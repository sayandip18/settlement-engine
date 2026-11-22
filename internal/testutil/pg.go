// Package testutil provides integration-test helpers backed by real containers.
package testutil

import (
	"context"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/sayandip18/settlement-engine/internal/platform/db"
	"github.com/sayandip18/settlement-engine/migrations"
)

var (
	once    sync.Once
	pool    *pgxpool.Pool
	ctr     *tcpostgres.PostgresContainer
	initErr error
)

// Run is for TestMain: it runs the tests, then terminates the shared container.
// On Windows the testcontainers reaper (Ryuk) fails to start under Docker Desktop,
// so it is disabled there and this teardown does the cleanup instead.
func Run(m *testing.M) int {
	if runtime.GOOS == "windows" && os.Getenv("TESTCONTAINERS_RYUK_DISABLED") == "" {
		os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
	code := m.Run()
	if pool != nil {
		pool.Close()
	}
	if ctr != nil {
		_ = ctr.Terminate(context.Background())
	}
	return code
}

// NewPool returns a pool to a migrated Postgres container shared by the whole
// test binary, with all tables truncated. Tests using it must not run in parallel.
func NewPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	once.Do(func() {
		ctx := context.Background()
		c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
			tcpostgres.WithDatabase("settlement"),
			tcpostgres.WithUsername("settlement"),
			tcpostgres.WithPassword("settlement"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).WithStartupTimeout(90*time.Second)),
		)
		if err != nil {
			initErr = err
			return
		}
		ctr = c
		url, err := ctr.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			initErr = err
			return
		}
		if initErr = db.Migrate(url, migrations.FS); initErr != nil {
			return
		}
		pool, initErr = db.NewPool(ctx, url)
	})
	if initErr != nil {
		t.Skipf("postgres test container unavailable (is Docker running?): %v", initErr)
	}
	if _, err := pool.Exec(context.Background(),
		`TRUNCATE ledger_entries, ledger_transactions, ledger_accounts, refunds, idempotency_keys, payments, merchants CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return pool
}
