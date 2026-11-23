// Command worker runs the background half of the service: it publishes outbox rows to
// Kafka and executes queued payment operations with retries and dead-lettering.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/sayandip18/settlement-engine/internal/command"
	"github.com/sayandip18/settlement-engine/internal/config"
	"github.com/sayandip18/settlement-engine/internal/deadletter"
	"github.com/sayandip18/settlement-engine/internal/outbox"
	"github.com/sayandip18/settlement-engine/internal/payment"
	"github.com/sayandip18/settlement-engine/internal/platform/db"
	kafkaplat "github.com/sayandip18/settlement-engine/internal/platform/kafka"
	"github.com/sayandip18/settlement-engine/internal/processor"
	"github.com/sayandip18/settlement-engine/migrations"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.AutoMigrate {
		if err := db.Migrate(cfg.DatabaseURL, migrations.FS); err != nil {
			return err
		}
	}
	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := kafkaplat.EnsureTopics(ctx, cfg.KafkaBrokers,
		kafkaplat.Topic{Name: cfg.CommandsTopic, Partitions: cfg.TopicPartitions},
		kafkaplat.Topic{Name: command.DLQTopic(cfg.CommandsTopic), Partitions: 1},
	); err != nil {
		return err
	}

	payments := payment.NewService(pool, processor.NewMock(), cfg.DefaultFeeBPS)
	policy := command.RetryPolicy{
		MaxAttempts: cfg.MaxAttempts, Base: cfg.RetryBaseDelay, Max: cfg.RetryMaxDelay, Jitter: 0.2}
	worker := command.NewWorker(pool, payments, cfg.CommandsTopic, policy, log)

	publisher := kafkaplat.NewPublisher(cfg.KafkaBrokers)
	defer publisher.Close()
	relay := outbox.NewRelay(pool, publisher, log, 100, cfg.OutboxPollInterval)
	consumer := kafkaplat.NewConsumer(cfg.KafkaBrokers, cfg.ConsumerGroup, cfg.CommandsTopic, log)
	defer consumer.Close()
	// Copies dead-lettered messages into the dead_letters table for the admin endpoints.
	dlq := deadletter.NewService(pool, command.NewService(pool, payments, cfg.CommandsTopic))
	dlqConsumer := kafkaplat.NewConsumer(cfg.KafkaBrokers, cfg.DLQConsumerGroup, command.DLQTopic(cfg.CommandsTopic), log)
	defer dlqConsumer.Close()

	errs := make(chan error, 3)
	var wg sync.WaitGroup
	run := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("started", "component", name)
			if err := fn(ctx); err != nil {
				log.Error("component stopped with error", "component", name, "err", err)
				errs <- err
				stop() // take the whole worker down; the orchestrator restarts it
			}
		}()
	}
	run("outbox-relay", relay.Run)
	run("command-consumer", func(ctx context.Context) error { return consumer.Run(ctx, worker.Handle) })
	run("dlq-store", func(ctx context.Context) error { return dlqConsumer.Run(ctx, dlq.Record) })

	<-ctx.Done()
	log.Info("shutting down")
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}
