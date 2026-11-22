package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sayandip18/settlement-engine/internal/outbox"
	"github.com/sayandip18/settlement-engine/internal/testutil"
)

func TestMain(m *testing.M) { os.Exit(testutil.Run(m)) }

type fakePublisher struct {
	mu   sync.Mutex
	got  []outbox.Message
	fail error
	slow time.Duration
}

func (p *fakePublisher) Publish(_ context.Context, msgs []outbox.Message) error {
	time.Sleep(p.slow)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail != nil {
		return p.fail
	}
	p.got = append(p.got, msgs...)
	return nil
}

func (p *fakePublisher) payloads() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.got))
	for i, m := range p.got {
		out[i] = string(m.Payload)
	}
	return out
}

func relay(pool *pgxpool.Pool, pub outbox.Publisher, batch int) *outbox.Relay {
	return outbox.NewRelay(pool, pub, slog.New(slog.NewTextHandler(io.Discard, nil)), batch, time.Millisecond)
}

func enqueue(t *testing.T, pool *pgxpool.Pool, msgs ...outbox.Message) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	for _, m := range msgs {
		if err := outbox.Enqueue(ctx, tx, m); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func msg(payload string) outbox.Message {
	return outbox.Message{Topic: "t", Key: "k", Payload: []byte(payload)}
}

func pending(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestFlushPublishesInOrderOnce(t *testing.T) {
	pool := testutil.NewPool(t)
	pub := &fakePublisher{}
	r := relay(pool, pub, 100)
	enqueue(t, pool, msg("a"), msg("b"), msg("c"))

	n, err := r.Flush(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("Flush = %d, %v", n, err)
	}
	if got := pub.payloads(); fmt.Sprint(got) != "[a b c]" {
		t.Fatalf("published %v", got)
	}
	if n, _ := r.Flush(context.Background()); n != 0 || len(pub.payloads()) != 3 {
		t.Fatalf("published rows were sent again: n=%d total=%d", n, len(pub.payloads()))
	}
	if pending(t, pool) != 0 {
		t.Fatal("rows not marked published")
	}
}

func TestFlushCarriesTopicKeyAndHeaders(t *testing.T) {
	pool := testutil.NewPool(t)
	pub := &fakePublisher{}
	enqueue(t, pool,
		outbox.Message{Topic: "orders", Key: "p-1", Payload: []byte("x"), Headers: map[string]string{"error": "boom", "n": "3"}},
		outbox.Message{Topic: "orders", Key: "p-2", Payload: []byte("y")}) // nil headers
	if _, err := relay(pool, pub, 100).Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	a, b := pub.got[0], pub.got[1]
	if a.Topic != "orders" || a.Key != "p-1" || a.Headers["error"] != "boom" || a.Headers["n"] != "3" {
		t.Fatalf("first: %+v", a)
	}
	if b.Key != "p-2" || len(b.Headers) != 0 {
		t.Fatalf("second: %+v", b)
	}
}

func TestBatchSizeLimitsEachFlush(t *testing.T) {
	pool := testutil.NewPool(t)
	pub := &fakePublisher{}
	r := relay(pool, pub, 2)
	enqueue(t, pool, msg("1"), msg("2"), msg("3"), msg("4"), msg("5"))

	var sizes []int
	for {
		n, err := r.Flush(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		sizes = append(sizes, n)
	}
	if fmt.Sprint(sizes) != "[2 2 1]" || fmt.Sprint(pub.payloads()) != "[1 2 3 4 5]" {
		t.Fatalf("sizes=%v published=%v", sizes, pub.payloads())
	}
}

func TestDelayedMessageWaitsUntilDue(t *testing.T) {
	pool := testutil.NewPool(t)
	pub := &fakePublisher{}
	r := relay(pool, pub, 100)
	delay := 300 * time.Millisecond
	enqueue(t, pool,
		outbox.Message{Topic: "t", Key: "k", Payload: []byte("later"), Delay: delay},
		msg("now"))

	if n, _ := r.Flush(context.Background()); n != 1 || fmt.Sprint(pub.payloads()) != "[now]" {
		t.Fatalf("only the immediate message should go out: %v", pub.payloads())
	}
	if n, _ := r.Flush(context.Background()); n != 0 {
		t.Fatal("delayed message published early")
	}
	time.Sleep(delay + 100*time.Millisecond)
	if n, _ := r.Flush(context.Background()); n != 1 || fmt.Sprint(pub.payloads()) != "[now later]" {
		t.Fatalf("delayed message not published once due: %v", pub.payloads())
	}
}

func TestPublishFailureLeavesRowsPendingThenRecovers(t *testing.T) {
	pool := testutil.NewPool(t)
	pub := &fakePublisher{fail: errors.New("broker down")}
	r := relay(pool, pub, 100)
	enqueue(t, pool, msg("a"), msg("b"))

	if _, err := r.Flush(context.Background()); err == nil {
		t.Fatal("expected publish error")
	}
	if pending(t, pool) != 2 {
		t.Fatal("rows must stay pending when the broker rejects them")
	}

	pub.mu.Lock()
	pub.fail = nil
	pub.mu.Unlock()
	if n, err := r.Flush(context.Background()); err != nil || n != 2 {
		t.Fatalf("after recovery: %d, %v", n, err)
	}
	if fmt.Sprint(pub.payloads()) != "[a b]" || pending(t, pool) != 0 {
		t.Fatalf("published %v", pub.payloads())
	}
}

func TestRolledBackTransactionPublishesNothing(t *testing.T) {
	pool := testutil.NewPool(t)
	pub := &fakePublisher{}
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Enqueue(ctx, tx, msg("ghost")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := relay(pool, pub, 100).Flush(ctx); n != 0 || len(pub.payloads()) != 0 {
		t.Fatal("a message from a rolled-back transaction was published")
	}
}

func TestConcurrentRelaysNeverDuplicate(t *testing.T) {
	pool := testutil.NewPool(t)
	pub := &fakePublisher{slow: 5 * time.Millisecond} // hold row locks long enough to overlap
	const total = 60
	var msgs []outbox.Message
	for i := 0; i < total; i++ {
		msgs = append(msgs, msg(fmt.Sprintf("m%03d", i)))
	}
	enqueue(t, pool, msgs...)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := relay(pool, pub, 7)
			for {
				n, err := r.Flush(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 && pending(t, pool) == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	got := pub.payloads()
	sort.Strings(got)
	if len(got) != total {
		t.Fatalf("published %d messages, want %d (duplicates or losses)", len(got), total)
	}
	for i, p := range got {
		if p != fmt.Sprintf("m%03d", i) {
			t.Fatalf("message %d is %q", i, p)
		}
	}
}

func TestRunDrainsUntilCancelled(t *testing.T) {
	pool := testutil.NewPool(t)
	pub := &fakePublisher{}
	r := relay(pool, pub, 100)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	enqueue(t, pool, msg("a"))
	waitFor(t, func() bool { return len(pub.payloads()) == 1 })
	enqueue(t, pool, msg("b")) // picked up on a later poll
	waitFor(t, func() bool { return len(pub.payloads()) == 2 })

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
