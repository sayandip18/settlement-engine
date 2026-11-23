// Command worker runs the background half of the service: it publishes outbox rows to
// Kafka and executes queued payment operations with retries and dead-lettering.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sayandip18/settlement-engine/internal/command"
	"github.com/sayandip18/settlement-engine/internal/config"
	"github.com/sayandip18/settlement-engine/internal/deadletter"
	"github.com/sayandip18/settlement-engine/internal/httpapi"
	"github.com/sayandip18/settlement-engine/internal/obs"
	"github.com/sayandip18/settlement-engine/internal/outbox"
	"github.com/sayandip18/settlement-engine/internal/payment"
	"github.com/sayandip18/settlement-engine/internal/platform/db"
	kafkaplat "github.com/sayandip18/settlement-engine/internal/platform/kafka"
	"github.com/sayandip18/settlement-engine/internal/processor"
	"github.com/sayandip18/settlement-engine/internal/reconcile"
	"github.com/sayandip18/settlement-engine/internal/retention"
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

	shutdownTracing, err := obs.InitTracing(ctx, "settlement-worker")
	if err != nil {
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(sctx)
	}()

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

	// Loops that must keep beating for the worker to count as ready.
	beats := obs.NewHeartbeats()
	relay.SetHeartbeat(beats.Register("outbox-relay", 30*time.Second))
	reconciler := reconcile.NewService(pool, cfg.ReconcileThresholds, log)
	reconciler.SetHeartbeat(beats.Register("reconciler", 3*cfg.ReconcileInterval+time.Minute))
	settleBeat := beats.Register("settlement", 3*cfg.SettlementInterval+time.Minute)
	purger := retention.NewService(pool, retention.Config{OutboxAge: cfg.OutboxRetention, DeadLetterAge: cfg.DeadLetterRetention}, log)
	purgeBeat := beats.Register("retention", 3*cfg.RetentionInterval+time.Minute)
	obs.NewDBCollector(pool, reconcileTypes()).Register()

	errs := make(chan error, 7)
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
	run("reconciler", func(ctx context.Context) error { return reconciler.RunLoop(ctx, cfg.ReconcileInterval) })
	run("settlement", func(ctx context.Context) error {
		return payments.RunSettlement(ctx, cfg.SettlementInterval, cfg.SettlementDelay, log, settleBeat)
	})
	run("dlq-store", func(ctx context.Context) error { return dlqConsumer.Run(ctx, dlq.Record) })
	run("retention", func(ctx context.Context) error { return purger.RunLoop(ctx, cfg.RetentionInterval, purgeBeat) })

	// Health and metrics for the orchestrator and Prometheus. Unauthenticated: bind it to
	// a private network, not the public one.
	health := httpapi.NewHealth(map[string]httpapi.Check{
		"database":   func(ctx context.Context) error { return pool.Ping(ctx) },
		"kafka":      kafkaplat.Check(cfg.KafkaBrokers),
		"components": beats.Check,
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", health.Live)
	mux.HandleFunc("GET /readyz", health.Ready)
	mux.Handle("GET /metrics", obs.Handler())
	srv := &http.Server{Addr: cfg.WorkerHTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("worker http listening", "addr", cfg.WorkerHTTPAddr)
		serveErr := make(chan error, 1)
		go func() { serveErr <- srv.ListenAndServe() }()
		select {
		case err := <-serveErr:
			if !errors.Is(err, http.ErrServerClosed) {
				log.Error("worker http stopped", "err", err)
				errs <- err
				stop()
			}
		case <-ctx.Done():
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(sctx)
		}
	}()

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

func reconcileTypes() []string {
	var out []string
	for _, t := range reconcile.AllTypes() {
		out = append(out, string(t))
	}
	return out
}
