package reconcile_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sayandip18/settlement-engine/internal/ledger"
	"github.com/sayandip18/settlement-engine/internal/payment"
	"github.com/sayandip18/settlement-engine/internal/processor"
	"github.com/sayandip18/settlement-engine/internal/reconcile"
	"github.com/sayandip18/settlement-engine/internal/testutil"
)

func TestMain(m *testing.M) { os.Exit(testutil.Run(m)) }

var ctx = context.Background()

type env struct {
	pool *pgxpool.Pool
	svc  *reconcile.Service
}

func setup(t *testing.T) env {
	t.Helper()
	pool := testutil.NewPool(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return env{pool, reconcile.NewService(pool, reconcile.DefaultThresholds(), log)}
}

func (e env) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := e.pool.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// payment inserts a payment directly in the given state.
func (e env) payment(t *testing.T, status string, amount, refunded, fee int64, age time.Duration) uuid.UUID {
	t.Helper()
	id := uuid.New()
	e.exec(t, `INSERT INTO payments (id, merchant_id, customer_id, amount_paise, currency, status, refunded_paise, fee_paise, created_at, updated_at)
	           VALUES ($1, 'm1', 'c1', $2, 'INR', $3, $4, $5, now() - $6 * interval '1 microsecond', now() - $6 * interval '1 microsecond')`,
		id, amount, status, refunded, fee, age.Microseconds())
	return id
}

func (e env) post(t *testing.T, txn ledger.Txn) {
	t.Helper()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := ledger.Post(ctx, tx, txn); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// captured is a consistent CAPTURED payment with its ledger transaction.
func (e env) captured(t *testing.T) uuid.UUID {
	t.Helper()
	id := e.payment(t, "CAPTURED", 100000, 0, 2000, 0)
	e.post(t, ledger.Capture(id, "m1", 100000, 2000))
	return id
}

// corrupt runs sql with the ledger's append-only and balance triggers switched off,
// simulating a schema or migration mistake.
func (e env) corrupt(t *testing.T, sql string, args ...any) {
	t.Helper()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Skipf("cannot disable triggers (need superuser): %v", err)
	}
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func (e env) run(t *testing.T) reconcile.RunResult {
	t.Helper()
	res, err := e.svc.Run(ctx)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, c := range res.Checks {
		if c.Error != "" {
			t.Fatalf("check %s failed: %s", c.Type, c.Error)
		}
	}
	return res
}

// active returns subjects of unresolved findings by type.
func (e env) active(t *testing.T) map[reconcile.Type][]string {
	t.Helper()
	rows, err := e.pool.Query(ctx, `SELECT type, subject FROM discrepancies WHERE status <> 'RESOLVED' ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[reconcile.Type][]string{}
	for rows.Next() {
		var typ reconcile.Type
		var subj string
		if err := rows.Scan(&typ, &subj); err != nil {
			t.Fatal(err)
		}
		out[typ] = append(out[typ], subj)
	}
	return out
}

func (e env) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func wantOnly(t *testing.T, got map[reconcile.Type][]string, typ reconcile.Type, subject string) {
	t.Helper()
	if len(got) != 1 || len(got[typ]) != 1 || got[typ][0] != subject {
		t.Fatalf("want exactly one %s finding for %s, got %v", typ, subject, got)
	}
}

func TestHealthyDataHasNoFindings(t *testing.T) {
	e := setup(t)
	svc := payment.NewService(e.pool, processor.NewMock(), 200)
	res, err := svc.Create(ctx, payment.CreateInput{MerchantID: "m1", CustomerID: "c1", AmountPaise: 100003, IdempotencyKey: uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	id := res.Payment.ID
	for _, step := range []func() error{
		func() error { _, err := svc.Authorize(ctx, id); return err },
		func() error { _, err := svc.Capture(ctx, id); return err },
		func() error {
			half := int64(40001)
			_, err := svc.Refund(ctx, payment.RefundInput{PaymentID: id, AmountPaise: &half, IdempotencyKey: "r1"})
			return err
		},
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	// A voided payment and a declined one are healthy too.
	r2, _ := svc.Create(ctx, payment.CreateInput{MerchantID: "m1", CustomerID: "c1", AmountPaise: 500, IdempotencyKey: uuid.NewString()})
	svc.Authorize(ctx, r2.Payment.ID) //nolint:errcheck
	svc.Void(ctx, r2.Payment.ID)      //nolint:errcheck
	r3, _ := svc.Create(ctx, payment.CreateInput{MerchantID: "m1", CustomerID: "decline_x", AmountPaise: 500, IdempotencyKey: uuid.NewString()})
	svc.Authorize(ctx, r3.Payment.ID) //nolint:errcheck

	e.run(t)
	if got := e.active(t); len(got) != 0 {
		t.Fatalf("healthy data produced findings: %v", got)
	}
}

func TestChecksDetect(t *testing.T) {
	cases := []struct {
		name  string
		typ   reconcile.Type
		setup func(t *testing.T, e env) string // returns the expected subject
		// notFlagged builds look-alike healthy rows that must not be reported.
		extra func(t *testing.T, e env)
	}{
		{"capture ledger missing", reconcile.TypeCaptureLedgerMissing,
			func(t *testing.T, e env) string { return e.payment(t, "CAPTURED", 100000, 0, 2000, 0).String() },
			nil},
		{"customer funds mismatch", reconcile.TypeCustomerFundsMismatch,
			func(t *testing.T, e env) string {
				id := e.captured(t)
				e.exec(t, `UPDATE payments SET refunded_paise = 30000, status = 'PARTIALLY_REFUNDED' WHERE id = $1`, id)
				return id.String()
			}, nil},
		{"payment stuck pending", reconcile.TypePaymentStuck,
			func(t *testing.T, e env) string { return e.payment(t, "PENDING", 100, 0, 0, 2*time.Hour).String() },
			func(t *testing.T, e env) {
				e.payment(t, "PENDING", 100, 0, 0, time.Minute)    // fresh
				e.payment(t, "AUTHORIZED", 100, 0, 0, 2*time.Hour) // holds are allowed to last
				e.payment(t, "VOIDED", 100, 0, 0, 30*24*time.Hour) // terminal
			}},
		{"payment stuck authorized", reconcile.TypePaymentStuck,
			func(t *testing.T, e env) string {
				return e.payment(t, "AUTHORIZED", 100, 0, 0, 8*24*time.Hour).String()
			},
			nil},
		{"refund exceeds capture", reconcile.TypeRefundExceedsCapture,
			func(t *testing.T, e env) string {
				id := e.captured(t)
				e.exec(t, `INSERT INTO refunds (id, payment_id, idem_key, amount_paise) VALUES ($1, $2, 'k', 150000)`, uuid.New(), id)
				return id.String()
			}, nil},
		{"ledger on uncaptured payment", reconcile.TypeLedgerOnUncaptured,
			func(t *testing.T, e env) string {
				id := e.payment(t, "AUTHORIZED", 100000, 0, 2000, 0)
				e.post(t, ledger.Capture(id, "m1", 100000, 2000))
				return id.String()
			}, nil},
		{"ledger on failed payment", reconcile.TypeLedgerOnUncaptured,
			func(t *testing.T, e env) string {
				id := e.payment(t, "FAILED", 100000, 0, 2000, 0)
				e.post(t, ledger.Capture(id, "m1", 100000, 2000))
				return id.String()
			}, nil},
		{"outbox stale", reconcile.TypeOutboxStale,
			func(t *testing.T, e env) string {
				var id int64
				if err := e.pool.QueryRow(ctx, `INSERT INTO outbox (topic, msg_key, payload, available_at)
					VALUES ('t', 'k', 'x', now() - interval '1 hour') RETURNING id`).Scan(&id); err != nil {
					t.Fatal(err)
				}
				return strconv.FormatInt(id, 10)
			},
			func(t *testing.T, e env) {
				e.exec(t, `INSERT INTO outbox (topic, msg_key, payload) VALUES ('t', 'fresh', 'x')`)
				e.exec(t, `INSERT INTO outbox (topic, msg_key, payload, available_at, published_at)
					VALUES ('t', 'done', 'x', now() - interval '1 hour', now() - interval '59 minutes')`)
				e.exec(t, `INSERT INTO outbox (topic, msg_key, payload, available_at)
					VALUES ('t', 'delayed-retry', 'x', now() + interval '1 hour')`) // scheduled for later: not stale
			}},
		{"operation pending too long", reconcile.TypeOperationStuck,
			func(t *testing.T, e env) string {
				return e.operation(t, "PENDING", time.Hour).String()
			},
			func(t *testing.T, e env) {
				e.operation(t, "PENDING", time.Minute)
				e.operation(t, "SUCCEEDED", 24*time.Hour)
			}},
		{"operation dead lettered", reconcile.TypeOperationStuck,
			func(t *testing.T, e env) string {
				return e.operation(t, "DEAD_LETTERED", time.Second).String()
			},
			func(t *testing.T, e env) {
				op := e.operation(t, "DEAD_LETTERED", time.Hour)
				e.deadLetter(t, op, "DISMISSED") // an operator accepted this one
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := setup(t)
			subject := c.setup(t, e)
			if c.extra != nil {
				c.extra(t, e)
			}
			e.run(t)
			wantOnly(t, e.active(t), c.typ, subject)
		})
	}
}

func (e env) operation(t *testing.T, status string, age time.Duration) uuid.UUID {
	t.Helper()
	pid := e.payment(t, "PENDING", 100, 0, 0, 0)
	id := uuid.New()
	e.exec(t, `INSERT INTO operations (id, payment_id, kind, status, updated_at)
	           VALUES ($1, $2, 'AUTHORIZE', $3, now() - $4 * interval '1 microsecond')`, id, pid, status, age.Microseconds())
	return id
}

func (e env) deadLetter(t *testing.T, op uuid.UUID, status string) {
	t.Helper()
	e.exec(t, `INSERT INTO dead_letters (id, dedupe_key, msg_key, payload, error, operation_id, status)
	           VALUES ($1, $2, 'k', 'x', 'boom', $3, $4)`, uuid.New(), uuid.NewString(), op, status)
}

// A ledger transaction that does not balance can only exist if the triggers are bypassed.
func TestUnbalancedLedgerDetected(t *testing.T) {
	e := setup(t)
	id := e.captured(t)
	txn := uuid.New()
	e.corrupt(t, `WITH t AS (
	                INSERT INTO ledger_transactions (id, payment_id, merchant_id, kind, ref_id) VALUES ($1, $2, 'm1', 'ADJUSTMENT', 'orphan'))
	              INSERT INTO ledger_entries (txn_id, account_code, direction, amount_paise) VALUES ($1, 'MERCHANT_PAYABLE:m1', 'DEBIT', 777)`,
		txn, id)
	e.run(t)
	got := e.active(t)
	if len(got) != 2 || len(got[reconcile.TypeUnbalancedLedgerTxn]) != 1 || got[reconcile.TypeUnbalancedLedgerTxn][0] != txn.String() ||
		len(got[reconcile.TypeLedgerGlobalImbalance]) != 1 || got[reconcile.TypeLedgerGlobalImbalance][0] != "INR" {
		t.Fatalf("want unbalanced txn + global imbalance for INR, got %v", got)
	}
}

func TestRerunDeduplicates(t *testing.T) {
	e := setup(t)
	id := e.payment(t, "CAPTURED", 100000, 0, 2000, 0)

	first := e.run(t)
	second := e.run(t)
	third := e.run(t)

	if n := newCount(first); n != 1 {
		t.Fatalf("first run should create 1 finding, created %d", n)
	}
	if newCount(second) != 0 || newCount(third) != 0 {
		t.Fatalf("re-runs created duplicates: %+v %+v", second, third)
	}
	if n := e.count(t, `SELECT count(*) FROM discrepancies`); n != 1 {
		t.Fatalf("want 1 row, got %d", n)
	}
	if n := e.count(t, `SELECT seen_count FROM discrepancies WHERE subject = $1`, id.String()); n != 3 {
		t.Fatalf("seen_count = %d, want 3", n)
	}
}

func newCount(r reconcile.RunResult) int {
	n := 0
	for _, c := range r.Checks {
		n += c.New
	}
	return n
}

func TestFindingClearsItself(t *testing.T) {
	e := setup(t)
	open := e.payment(t, "PENDING", 100, 0, 0, 2*time.Hour)
	acked := e.payment(t, "PENDING", 100, 0, 0, 2*time.Hour)
	e.run(t)

	var ackedID uuid.UUID
	if err := e.pool.QueryRow(ctx, `SELECT id FROM discrepancies WHERE subject = $1`, acked.String()).Scan(&ackedID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Acknowledge(ctx, ackedID, "looking"); err != nil {
		t.Fatal(err)
	}

	// Both payments move on.
	e.exec(t, `UPDATE payments SET status = 'AUTHORIZED', updated_at = now() WHERE id = ANY($1)`, []uuid.UUID{open, acked})
	res := e.run(t)

	resolved := 0
	for _, c := range res.Checks {
		resolved += c.AutoResolved
	}
	if resolved != 2 {
		t.Fatalf("auto-resolved %d, want 2", resolved)
	}
	if got := e.active(t); len(got) != 0 {
		t.Fatalf("still active: %v", got)
	}
	var resolution, note string
	if err := e.pool.QueryRow(ctx, `SELECT resolution, resolution_note FROM discrepancies WHERE id = $1`, ackedID).Scan(&resolution, &note); err != nil {
		t.Fatal(err)
	}
	if resolution != "AUTO" || note == "" {
		t.Fatalf("resolution=%q note=%q", resolution, note)
	}
}

func TestRecurrenceAfterResolveOpensNewFinding(t *testing.T) {
	e := setup(t)
	id := e.payment(t, "CAPTURED", 100000, 0, 2000, 0)
	e.run(t)
	var first uuid.UUID
	if err := e.pool.QueryRow(ctx, `SELECT id FROM discrepancies`).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Resolve(ctx, first, "false alarm"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // the run's snapshot must start after the resolve
	e.run(t)

	if n := e.count(t, `SELECT count(*) FROM discrepancies WHERE subject = $1`, id.String()); n != 2 {
		t.Fatalf("want old resolved + new open row, got %d rows", n)
	}
	if n := e.count(t, `SELECT count(*) FROM discrepancies WHERE subject = $1 AND status = 'OPEN'`, id.String()); n != 1 {
		t.Fatalf("want exactly one open row, got %d", n)
	}
}

// A run whose snapshot predates an operator's resolve must not reopen the finding.
func TestStaleSnapshotDoesNotReopenResolved(t *testing.T) {
	e := setup(t)
	id := e.payment(t, "CAPTURED", 100000, 0, 2000, 0)
	var snapshot time.Time
	if err := e.pool.QueryRow(ctx, `SELECT now()`).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	f := []reconcile.Finding{{Subject: id.String(), PaymentID: &id, Details: map[string]any{}}}
	if _, err := e.svc.PersistFindings(ctx, snapshot, reconcile.TypeCaptureLedgerMissing, f); err != nil {
		t.Fatal(err)
	}
	var did uuid.UUID
	if err := e.pool.QueryRow(ctx, `SELECT id FROM discrepancies`).Scan(&did); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Resolve(ctx, did, "fixed by hand"); err != nil {
		t.Fatal(err)
	}
	// Replay the stale snapshot.
	if _, err := e.svc.PersistFindings(ctx, snapshot, reconcile.TypeCaptureLedgerMissing, f); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, `SELECT count(*) FROM discrepancies`); n != 1 {
		t.Fatalf("stale snapshot reopened the finding: %d rows", n)
	}
}

// A check that errors must not take the others down, and must not auto-resolve its own findings.
func TestFailingCheckIsIsolated(t *testing.T) {
	e := setup(t)
	e.payment(t, "CAPTURED", 100000, 0, 2000, 0) // CAPTURE_LEDGER_MISSING
	e.exec(t, `INSERT INTO outbox (topic, msg_key, payload, available_at) VALUES ('t', 'k', 'x', now() - interval '1 hour')`)
	e.run(t)
	if len(e.active(t)) != 2 {
		t.Fatalf("setup: %v", e.active(t))
	}

	e.exec(t, `ALTER TABLE outbox RENAME TO outbox_gone`)
	defer e.exec(t, `ALTER TABLE outbox_gone RENAME TO outbox`)
	res, err := e.svc.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var failed, ok int
	for _, c := range res.Checks {
		if c.Error != "" {
			failed++
			if c.Type != reconcile.TypeOutboxStale {
				t.Fatalf("unexpected failing check %s: %s", c.Type, c.Error)
			}
		} else {
			ok++
		}
	}
	if failed != 1 || ok != len(res.Checks)-1 {
		t.Fatalf("failed=%d ok=%d", failed, ok)
	}
	if got := e.active(t); len(got[reconcile.TypeOutboxStale]) != 1 || len(got[reconcile.TypeCaptureLedgerMissing]) != 1 {
		t.Fatalf("findings changed while a check was failing: %v", got)
	}
}

// When a check hits the findings cap it cannot tell "fixed" from "beyond the cap".
func TestTruncatedCheckDoesNotAutoResolve(t *testing.T) {
	e := setup(t)
	e.exec(t, `INSERT INTO payments (id, merchant_id, customer_id, amount_paise, currency, status, created_at, updated_at)
	           SELECT gen_random_uuid(), 'm1', 'c1', 100, 'INR', 'PENDING',
	                  now() - interval '10 hours' + g * interval '1 second', now() - interval '10 hours' + g * interval '1 second'
	             FROM generate_series(1, 1002) g`)
	res := e.run(t)
	var c reconcile.CheckResult
	for _, x := range res.Checks {
		if x.Type == reconcile.TypePaymentStuck {
			c = x
		}
	}
	if !c.Truncated || c.Found != 1000 {
		t.Fatalf("want truncated at 1000, got %+v", c)
	}

	// The oldest payment is fixed; the 1001st now enters the top 1000 but 1001 are still stuck, so the check stays truncated.
	e.exec(t, `UPDATE payments SET status = 'AUTHORIZED', updated_at = now()
	            WHERE id = (SELECT id FROM payments ORDER BY created_at LIMIT 1)`)
	res = e.run(t)
	for _, x := range res.Checks {
		if x.Type == reconcile.TypePaymentStuck && (!x.Truncated || x.AutoResolved != 0) {
			t.Fatalf("truncated check auto-resolved: %+v", x)
		}
	}
	if n := e.count(t, `SELECT count(*) FROM discrepancies WHERE status = 'OPEN'`); n != 1001 {
		t.Fatalf("open findings = %d, want 1001", n)
	}
}

func TestOnlyOneRunAtATime(t *testing.T) {
	e := setup(t)

	// Another process holds the lock.
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(7210190110)`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Run(ctx); !errors.Is(err, reconcile.ErrAlreadyRunning) {
		t.Fatalf("want ErrAlreadyRunning, got %v", err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock(7210190110)`); err != nil {
		t.Fatal(err)
	}
	conn.Release()
	e.run(t) // lock is free again (and was not leaked by the failed attempt)
}

// Hammer Run from many goroutines: no overlap errors other than ErrAlreadyRunning,
// and still exactly one finding per problem.
func TestConcurrentRunsDoNotDuplicate(t *testing.T) {
	e := setup(t)
	for i := 0; i < 5; i++ {
		e.payment(t, "CAPTURED", 100000, 0, 2000, 0)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.svc.Run(ctx); err != nil && !errors.Is(err, reconcile.ErrAlreadyRunning) {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if n := e.count(t, `SELECT count(*) FROM discrepancies`); n != 5 {
		t.Fatalf("want 5 findings, got %d", n)
	}
}

// The reconciler itself must never write to money tables.
func TestRunLeavesMoneyTablesUntouched(t *testing.T) {
	e := setup(t)
	e.captured(t)
	e.payment(t, "CAPTURED", 100000, 0, 2000, 0) // a finding exists too
	digest := func() string {
		var s string
		if err := e.pool.QueryRow(ctx, `SELECT
		  (SELECT md5(string_agg(p::text, ',' ORDER BY p.id)) FROM payments p) || '/' ||
		  (SELECT md5(string_agg(t::text, ',' ORDER BY t.id)) FROM ledger_transactions t) || '/' ||
		  (SELECT md5(string_agg(l::text, ',' ORDER BY l.id)) FROM ledger_entries l) || '/' ||
		  (SELECT count(*) FROM outbox) || '/' || (SELECT count(*) FROM operations)`).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	before := digest()
	e.run(t)
	e.run(t)
	if after := digest(); after != before {
		t.Fatal("reconciliation modified money tables")
	}
}
