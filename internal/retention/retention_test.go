package retention_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sayandip18/settlement-engine/internal/outbox"
	"github.com/sayandip18/settlement-engine/internal/retention"
	"github.com/sayandip18/settlement-engine/internal/testutil"
)

func TestMain(m *testing.M) { os.Exit(testutil.Run(m)) }

var ctx = context.Background()

type env struct {
	pool *pgxpool.Pool
	svc  *retention.Service
}

func setup(t *testing.T) env {
	t.Helper()
	pool := testutil.NewPool(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return env{pool, retention.NewService(pool, retention.Config{OutboxAge: 7 * 24 * time.Hour, DeadLetterAge: 30 * 24 * time.Hour}, log)}
}

func (e env) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := e.pool.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func (e env) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (e env) outboxRow(t *testing.T, key string, published *time.Duration) {
	t.Helper()
	if published == nil {
		e.exec(t, `INSERT INTO outbox (topic, msg_key, payload, available_at) VALUES ('t', $1, 'x', now() - interval '60 days')`, key)
		return
	}
	e.exec(t, `INSERT INTO outbox (topic, msg_key, payload, available_at, published_at)
	           VALUES ('t', $1, 'x', now() - interval '60 days', now() - $2 * interval '1 microsecond')`, key, published.Microseconds())
}

func ago(d time.Duration) *time.Duration { return &d }

func (e env) deadLetter(t *testing.T, status string, closedAgo time.Duration, opID *uuid.UUID) uuid.UUID {
	t.Helper()
	id := uuid.New()
	var closed any
	if status != "PENDING" {
		closed = closedAgo.Microseconds()
	}
	e.exec(t, `INSERT INTO dead_letters (id, dedupe_key, msg_key, payload, error, status, operation_id, created_at, closed_at)
	           VALUES ($1, $2, 'k', 'x', 'e', $3, $4, now() - interval '90 days',
	                   CASE WHEN $5::bigint IS NULL THEN NULL ELSE now() - $5::bigint * interval '1 microsecond' END)`,
		id, uuid.NewString(), status, opID, closed)
	return id
}

func (e env) operation(t *testing.T, status string) uuid.UUID {
	t.Helper()
	pid := uuid.New()
	e.exec(t, `INSERT INTO payments (id, merchant_id, customer_id, amount_paise, currency, status) VALUES ($1,'m','c',100,'INR','PENDING')`, pid)
	id := uuid.New()
	e.exec(t, `INSERT INTO operations (id, payment_id, kind, status) VALUES ($1,$2,'AUTHORIZE',$3)`, id, pid, status)
	return id
}

const day = 24 * time.Hour

func TestPurgeDeletesOnlyOldFinishedOutboxRows(t *testing.T) {
	e := setup(t)
	e.outboxRow(t, "old-published", ago(8*day))
	e.outboxRow(t, "recent-published", ago(6*day))
	e.outboxRow(t, "old-unpublished", nil) // never published: must stay, however old
	e.exec(t, `INSERT INTO outbox (topic, msg_key, payload) VALUES ('t', 'fresh', 'x')`)

	res, err := e.svc.Purge(ctx)
	if err != nil || res.Outbox != 1 {
		t.Fatalf("purged %d (err %v), want 1", res.Outbox, err)
	}
	if n := e.count(t, `SELECT count(*) FROM outbox WHERE msg_key = 'old-published'`); n != 0 {
		t.Fatal("old published row survived")
	}
	if n := e.count(t, `SELECT count(*) FROM outbox`); n != 3 {
		t.Fatalf("%d rows left, want 3", n)
	}
	if n := e.count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 2 {
		t.Fatal("an unpublished row was deleted")
	}

	if res, _ := e.svc.Purge(ctx); res.Outbox != 0 { // idempotent
		t.Fatalf("second purge removed %d", res.Outbox)
	}
}

func TestPurgeDeadLetters(t *testing.T) {
	e := setup(t)
	oldDismissed := e.deadLetter(t, "DISMISSED", 40*day, nil)
	oldReplayed := e.deadLetter(t, "REPLAYED", 40*day, nil)
	recentDismissed := e.deadLetter(t, "DISMISSED", 10*day, nil)
	pendingForever := e.deadLetter(t, "PENDING", 0, nil) // 90 days old, but still needs an operator

	// A closed dead letter of an operation that is still dead-lettered is evidence that an
	// operator accepted that outcome (reconciliation reads it): keep it.
	stuck := e.operation(t, "DEAD_LETTERED")
	acceptedOutcome := e.deadLetter(t, "DISMISSED", 40*day, &stuck)
	// Once the operation has moved on, its old dead letter is just history.
	done := e.operation(t, "SUCCEEDED")
	replayedAndDone := e.deadLetter(t, "REPLAYED", 40*day, &done)

	res, err := e.svc.Purge(ctx)
	if err != nil || res.DeadLetters != 3 {
		t.Fatalf("purged %d (err %v), want 3", res.DeadLetters, err)
	}
	for id, wantGone := range map[uuid.UUID]bool{
		oldDismissed: true, oldReplayed: true, replayedAndDone: true,
		recentDismissed: false, pendingForever: false, acceptedOutcome: false,
	} {
		gone := e.count(t, `SELECT count(*) FROM dead_letters WHERE id = $1`, id) == 0
		if gone != wantGone {
			t.Errorf("dead letter %s: deleted=%v, want %v", id, gone, wantGone)
		}
	}
}

func TestPurgeWorksInBatches(t *testing.T) {
	e := setup(t)
	e.exec(t, `INSERT INTO outbox (topic, msg_key, payload, available_at, published_at)
	           SELECT 't', g::text, 'x', now() - interval '60 days', now() - interval '30 days' FROM generate_series(1, 2500) g`)
	res, err := e.svc.Purge(ctx)
	if err != nil || res.Outbox != 2500 {
		t.Fatalf("purged %d (err %v), want 2500", res.Outbox, err)
	}
}

// publisher that accepts everything.
type sink struct{}

func (sink) Publish(context.Context, []outbox.Message) error { return nil }

// Purging while the relay publishes and new messages arrive must never delete a message
// that has not been published, and concurrent purges must not conflict.
func TestPurgeConcurrentWithRelayLosesNothing(t *testing.T) {
	e := setup(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	relay := outbox.NewRelay(e.pool, sink{}, log, 50, time.Millisecond)
	// Rows the purger may take once published.
	e.exec(t, `INSERT INTO outbox (topic, msg_key, payload, available_at, published_at)
	           SELECT 't', 'old' || g, 'x', now() - interval '60 days', now() - interval '30 days' FROM generate_series(1, 1500) g`)

	// Use a tiny retention so rows published during the test become purgeable immediately.
	eager := retention.NewService(e.pool, retention.Config{OutboxAge: time.Nanosecond, DeadLetterAge: time.Nanosecond}, log)

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); relay.Run(runCtx) }() //nolint:errcheck

	const producers, perProducer = 4, 100
	var pwg sync.WaitGroup
	for p := 0; p < producers; p++ {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			for i := 0; i < perProducer; i++ {
				e.exec(t, `INSERT INTO outbox (topic, msg_key, payload) VALUES ('t', 'live', 'x')`)
			}
		}()
	}
	for p := 0; p < 3; p++ {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			for i := 0; i < 15; i++ {
				if _, err := eager.Purge(ctx); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	pwg.Wait()

	// Wait for the relay to publish everything that was produced, then purge once more.
	deadline := time.Now().Add(20 * time.Second)
	for e.count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("relay did not drain the outbox")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	if _, err := eager.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, `SELECT count(*) FROM outbox`); n != 0 {
		t.Fatalf("%d rows left after everything was published and purged", n)
	}
}

func TestRunLoopStopsOnCancel(t *testing.T) {
	e := setup(t)
	e.outboxRow(t, "old", ago(8*day))
	runCtx, cancel := context.WithCancel(ctx)
	beats := 0
	var mu sync.Mutex
	done := make(chan error, 1)
	go func() {
		done <- e.svc.RunLoop(runCtx, 20*time.Millisecond, func() { mu.Lock(); beats++; mu.Unlock() })
	}()
	deadline := time.Now().Add(5 * time.Second)
	for e.count(t, `SELECT count(*) FROM outbox`) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("loop never purged")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not stop")
	}
	mu.Lock()
	defer mu.Unlock()
	if beats < 2 {
		t.Fatalf("heartbeat called %d times", beats)
	}
}
