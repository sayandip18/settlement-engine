// Package config loads service configuration from environment variables.
package config

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	HTTPAddr     string
	DatabaseURL  string
	KafkaBrokers []string
	AdminAPIKey  string
	AutoMigrate  bool
	LogLevel     slog.Level
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
	if err := c.LogLevel.UnmarshalText([]byte(env("LOG_LEVEL", "info"))); err != nil {
		return c, errors.New("LOG_LEVEL must be debug, info, warn or error")
	}
	if c.DatabaseURL == "" {
		return c, errors.New("DATABASE_URL is required")
	}
	if c.AdminAPIKey == "" {
		return c, errors.New("ADMIN_API_KEY is required")
	}
	return c, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
