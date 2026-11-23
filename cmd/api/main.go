package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/sayandip18/settlement-engine/internal/command"
	"github.com/sayandip18/settlement-engine/internal/config"
	"github.com/sayandip18/settlement-engine/internal/deadletter"
	"github.com/sayandip18/settlement-engine/internal/httpapi"
	"github.com/sayandip18/settlement-engine/internal/ledger"
	"github.com/sayandip18/settlement-engine/internal/merchant"
	"github.com/sayandip18/settlement-engine/internal/obs"
	"github.com/sayandip18/settlement-engine/internal/payment"
	"github.com/sayandip18/settlement-engine/internal/platform/db"
	kafkaplat "github.com/sayandip18/settlement-engine/internal/platform/kafka"
	"github.com/sayandip18/settlement-engine/internal/processor"
	"github.com/sayandip18/settlement-engine/internal/reconcile"
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
	if err := cfg.RequireAdminKey(); err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := obs.InitTracing(ctx, "settlement-api")
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
		log.Info("migrations applied")
	}
	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	health := httpapi.NewHealth(map[string]httpapi.Check{
		"database": func(ctx context.Context) error { return pool.Ping(ctx) },
		"kafka":    kafkaplat.Check(cfg.KafkaBrokers),
	})
	// The API never calls the processor itself: payment actions are queued as
	// operations and executed by the worker. The service is used here only for
	// creating and reading payments.
	paymentSvc := payment.NewService(pool, processor.NewMock(), cfg.DefaultFeeBPS)
	payments := payment.NewHandler(paymentSvc, log)
	commandSvc := command.NewService(pool, paymentSvc, cfg.CommandsTopic)
	commands := command.NewHandler(commandSvc, log)
	deadLetters := deadletter.NewHandler(deadletter.NewService(pool, commandSvc), log)
	reconciliation := reconcile.NewHandler(reconcile.NewService(pool, cfg.ReconcileThresholds, log), log)
	payouts := ledger.NewHandler(pool, log)
	merchants := merchant.NewHandler(pool, cfg.DefaultFeeBPS, log)
	router := httpapi.NewRouter(log, health, cfg.AdminAPIKey,
		func(r chi.Router) { payments.Routes(r); commands.Routes(r); payouts.Routes(r) },
		func(r chi.Router) { merchants.Routes(r); deadLetters.Routes(r); reconciliation.Routes(r) },
		httpapi.WithMetrics(obs.Handler()))

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("api listening", "addr", cfg.HTTPAddr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
	return nil
}
