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

	"github.com/sayandip18/settlement-engine/internal/config"
	"github.com/sayandip18/settlement-engine/internal/httpapi"
	"github.com/sayandip18/settlement-engine/internal/payment"
	"github.com/sayandip18/settlement-engine/internal/platform/db"
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
		log.Info("migrations applied")
	}
	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	health := httpapi.NewHealth(map[string]httpapi.Check{
		"database": func(ctx context.Context) error { return pool.Ping(ctx) },
		"kafka":    httpapi.TCPCheck(cfg.KafkaBrokers[0]),
	})
	payments := payment.NewHandler(payment.NewService(pool), log)
	router := httpapi.NewRouter(log, health, cfg.AdminAPIKey, payments.Routes, nil)

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
