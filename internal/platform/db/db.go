// Package db provides the Postgres pool and schema migrations.
package db

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// Migrate applies all pending migrations from the given filesystem.
func Migrate(url string, migrations fs.FS) error {
	src, err := iofs.New(migrations, ".")
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}
	// golang-migrate's pgx/v5 driver registers the "pgx5" scheme.
	dbURL := url
	for _, p := range []string{"postgres://", "postgresql://"} {
		if strings.HasPrefix(url, p) {
			dbURL = "pgx5://" + strings.TrimPrefix(url, p)
		}
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, dbURL)
	if err != nil {
		return fmt.Errorf("init migrator: %w", err)
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}
