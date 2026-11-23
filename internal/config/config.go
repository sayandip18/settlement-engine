// Package config loads service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sayandip18/settlement-engine/internal/reconcile"
)

type Config struct {
	HTTPAddr     string
	DatabaseURL  string
	KafkaBrokers []string
	AdminAPIKey  string
	AutoMigrate  bool
	// Async processing.
	CommandsTopic      string
	ConsumerGroup      string
	DLQConsumerGroup   string
	TopicPartitions    int
	MaxAttempts        int
	RetryBaseDelay     time.Duration
	RetryMaxDelay      time.Duration
	OutboxPollInterval time.Duration
	// DefaultFeeBPS is the platform fee in basis points for merchants without their own (200 = 2%).
	DefaultFeeBPS int
	LogLevel      slog.Level
	// Reconciliation.
	ReconcileInterval   time.Duration
	ReconcileThresholds reconcile.Thresholds
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr:    env("HTTP_ADDR", ":8080"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		AdminAPIKey: os.Getenv("ADMIN_API_KEY"),
	}
	for _, b := range strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			c.KafkaBrokers = append(c.KafkaBrokers, b)
		}
	}
	auto, err := strconv.ParseBool(env("AUTO_MIGRATE", "true"))
	if err != nil {
		return c, errors.New("AUTO_MIGRATE must be a boolean")
	}
	c.AutoMigrate = auto
	if c.DefaultFeeBPS, err = strconv.Atoi(env("DEFAULT_FEE_BPS", "200")); err != nil || c.DefaultFeeBPS < 0 || c.DefaultFeeBPS > 10000 {
		return c, errors.New("DEFAULT_FEE_BPS must be an integer between 0 and 10000")
	}
	if err := c.LogLevel.UnmarshalText([]byte(env("LOG_LEVEL", "info"))); err != nil {
		return c, errors.New("LOG_LEVEL must be debug, info, warn or error")
	}
	c.CommandsTopic = env("COMMANDS_TOPIC", "payments.commands")
	c.ConsumerGroup = env("CONSUMER_GROUP", "settlement-worker")
	c.DLQConsumerGroup = env("DLQ_CONSUMER_GROUP", "settlement-dlq-store")
	if c.TopicPartitions, err = positiveInt("TOPIC_PARTITIONS", "3"); err != nil {
		return c, err
	}
	if c.MaxAttempts, err = positiveInt("MAX_ATTEMPTS", "5"); err != nil {
		return c, err
	}
	for _, d := range []struct {
		dst  *time.Duration
		name string
		def  string
	}{
		{&c.RetryBaseDelay, "RETRY_BASE_DELAY", "1s"},
		{&c.RetryMaxDelay, "RETRY_MAX_DELAY", "1m"},
		{&c.OutboxPollInterval, "OUTBOX_POLL_INTERVAL", "200ms"},
		{&c.ReconcileInterval, "RECON_INTERVAL", "5m"},
		{&c.ReconcileThresholds.PendingPayment, "RECON_PENDING_PAYMENT_AFTER", "1h"},
		{&c.ReconcileThresholds.AuthorizedPayment, "RECON_AUTHORIZED_PAYMENT_AFTER", "168h"},
		{&c.ReconcileThresholds.OutboxStale, "RECON_OUTBOX_STALE_AFTER", "5m"},
		{&c.ReconcileThresholds.OperationStuck, "RECON_OPERATION_STUCK_AFTER", "15m"},
	} {
		v, perr := time.ParseDuration(env(d.name, d.def))
		if perr != nil || v <= 0 {
			return c, fmt.Errorf("%s must be a positive duration like 500ms or 2s", d.name)
		}
		*d.dst = v
	}
	if c.DatabaseURL == "" {
		return c, errors.New("DATABASE_URL is required")
	}
	return c, nil
}

// RequireAdminKey is checked by the API process only; the worker has no admin endpoints.
func (c Config) RequireAdminKey() error {
	if c.AdminAPIKey == "" {
		return errors.New("ADMIN_API_KEY is required")
	}
	return nil
}

func positiveInt(name, def string) (int, error) {
	v, err := strconv.Atoi(env(name, def))
	if err != nil || v < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return v, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
