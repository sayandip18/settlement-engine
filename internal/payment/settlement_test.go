package payment_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/sayandip18/settlement-engine/internal/ledger"
	"github.com/sayandip18/settlement-engine/internal/payment"
)

const day = 24 * time.Hour

// age pretends the payment was captured d ago.
func (e env) age(t *testing.T, id uuid.UUID, d time.Duration) {
	t.Helper()
	if _, err := e.pool.Exec(context.Background(),
		`UPDATE payments SET captured_at = now() - $2 * interval '1 microsecond' WHERE id = $1`, id, d.Microseconds()); err != nil {
		t.Fatal(err)
	}
}

func (e env) count(t *testing.T, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := e.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// net is the credit-positive balance of an account across one payment's ledger.
func (e env) net(t *testing.T, id uuid.UUID, account string) int64 {
	return e.count(t, `SELECT COALESCE(SUM(CASE e.direction WHEN 'CREDIT' THEN e.amount_paise ELSE -e.amount_paise END), 0)
		FROM ledger_transactions t JOIN ledger_entries e ON e.txn_id = t.id WHERE t.payment_id = $1 AND e.account_code = $2`, id, account)
}

func (e env) settlements(t *testing.T, id uuid.UUID) int64 {
	return e.count(t, `SELECT count(*) FROM ledger_transactions WHERE payment_id = $1 AND kind = 'SETTLEMENT'`, id)
}

func TestSettlementWaitsForDelayThenPaysOut(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.capturedFor(t, "m1", 100000, "k1") // fee 2000

	if n, err := e.svc.SettleDue(ctx, day); err != nil || n != 0 {
		t.Fatalf("settled %d before the delay (err %v)", n, err)
	}
	e.age(t, p.ID, 25*time.Hour)
	if n, err := e.svc.SettleDue(ctx, day); err != nil || n != 1 {
		t.Fatalf("settled %d, want 1 (err %v)", n, err)
	}

	got, _ := e.svc.Get(ctx, p.ID)
	if got.Status != payment.StatusSettled || got.SettledAt == nil {
		t.Fatalf("payment after settlement: %+v", got)
	}
	if e.settlements(t, p.ID) != 1 {
		t.Fatal("want one SETTLEMENT transaction")
	}
	// Owed 98000 moved from payable to paid-out; payable is cleared, customer funds untouched.
	if n := e.net(t, p.ID, ledger.MerchantPayable("m1")); n != 0 {
		t.Fatalf("payable balance after payout = %d", n)
	}
	if n := e.net(t, p.ID, ledger.MerchantPaidOut("m1")); n != 98000 {
		t.Fatalf("paid out = %d", n)
	}
	if n := e.net(t, p.ID, ledger.AccountCustomerFunds); n != -100000 { // credit-positive: a 100000 debit
		t.Fatalf("customer funds = %d", n)
	}
}

func TestSettlementIsIdempotent(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.capturedFor(t, "m1", 100000, "k1")
	e.age(t, p.ID, 2*day)
	for i := 0; i < 3; i++ {
		e.svc.SettleDue(ctx, day) //nolint:errcheck
	}
	if ok, err := e.svc.Settle(ctx, p.ID, day); err != nil || ok {
		t.Fatalf("settling again: ok=%v err=%v", ok, err)
	}
	if e.settlements(t, p.ID) != 1 {
		t.Fatal("settled more than once")
	}
}

// Many workers racing: each payment is paid out exactly once.
func TestConcurrentSettlementPaysOutOnce(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	var ids []uuid.UUID
	for i := 0; i < 8; i++ {
		p := e.capturedFor(t, "m1", 100000, uuid.NewString())
		e.age(t, p.ID, 2*day)
		ids = append(ids, p.ID)
	}

	var wg sync.WaitGroup
	var settled atomic.Int32
	for i := 0; i < 6; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			n, err := e.svc.SettleDue(ctx, day)
			if err != nil {
				t.Error(err)
			}
			settled.Add(int32(n))
		}()
		go func(id uuid.UUID) {
			defer wg.Done()
			if ok, err := e.svc.Settle(ctx, id, day); err != nil {
				t.Error(err)
			} else if ok {
				settled.Add(1)
			}
		}(ids[i])
	}
	wg.Wait()
	if settled.Load() != 8 {
		t.Fatalf("settled %d payments in total, want 8", settled.Load())
	}
	for _, id := range ids {
		if e.settlements(t, id) != 1 {
			t.Fatalf("payment %s has %d settlements", id, e.settlements(t, id))
		}
	}
}

func TestOnlyCollectedPaymentsSettle(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	pending := e.newPaymentFor(t, "m1", 100, "k1")
	authorized := e.newPaymentFor(t, "m1", 100, "k2")
	if _, err := e.svc.Authorize(ctx, authorized.ID); err != nil {
		t.Fatal(err)
	}
	refunded := e.capturedFor(t, "m1", 100000, "k3")
	if _, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: refunded.ID, IdempotencyKey: "r"}); err != nil {
		t.Fatal(err)
	}
	e.pool.Exec(ctx, `UPDATE payments SET captured_at = now() - interval '9 days'`) //nolint:errcheck

	if n, err := e.svc.SettleDue(ctx, day); err != nil || n != 0 {
		t.Fatalf("settled %d, want 0 (err %v)", n, err)
	}
	for _, id := range []uuid.UUID{pending.ID, authorized.ID, refunded.ID} {
		if ok, _ := e.svc.Settle(ctx, id, day); ok || e.settlements(t, id) != 0 {
			t.Fatalf("payment %s must not settle", id)
		}
	}
	if ok, err := e.svc.Settle(ctx, uuid.New(), day); ok || err != nil {
		t.Fatalf("unknown payment: ok=%v err=%v", ok, err)
	}
}

// A partially refunded payment still pays out what is left; refunds after settlement
// come out of the merchant's payable, leaving a clawback balance.
func TestSettlementWithRefundsBeforeAndAfter(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.capturedFor(t, "m1", 100000, "k1") // fee 2000

	if _, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, AmountPaise: ptr(60000), IdempotencyKey: "r1"}); err != nil {
		t.Fatal(err)
	}
	// Refunded 60000 with fee share 1200: payable debit 58800, so 39200 is owed.
	e.age(t, p.ID, 2*day)
	if n, err := e.svc.SettleDue(ctx, day); err != nil || n != 1 {
		t.Fatalf("settled %d (err %v)", n, err)
	}
	got, _ := e.svc.Get(ctx, p.ID)
	if got.Status != payment.StatusPartiallyRefunded || got.SettledAt == nil {
		t.Fatalf("status must stay PARTIALLY_REFUNDED with settled_at set: %+v", got)
	}
	if n := e.net(t, p.ID, ledger.MerchantPaidOut("m1")); n != 39200 {
		t.Fatalf("paid out = %d, want 39200", n)
	}

	// Refunding the rest after the payout: payable goes negative by what was paid out.
	if _, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, IdempotencyKey: "r2"}); err != nil {
		t.Fatal(err)
	}
	if n := e.net(t, p.ID, ledger.MerchantPayable("m1")); n != -39200 {
		t.Fatalf("payable = %d, want -39200 (clawback)", n)
	}
	got, _ = e.svc.Get(ctx, p.ID)
	if got.Status != payment.StatusRefunded {
		t.Fatalf("status = %s", got.Status)
	}
	if n, _ := e.svc.SettleDue(ctx, day); n != 0 || e.settlements(t, p.ID) != 1 {
		t.Fatal("a refunded, already-settled payment must not settle again")
	}
}

func TestRefundAfterSettlementOfCapturedPayment(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.capturedFor(t, "m1", 100000, "k1")
	e.age(t, p.ID, 2*day)
	e.svc.SettleDue(ctx, day) //nolint:errcheck
	res, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, AmountPaise: ptr(10000), IdempotencyKey: "r"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Payment.Status != payment.StatusPartiallyRefunded || res.Payment.SettledAt == nil {
		t.Fatalf("%+v", res.Payment)
	}
}

func TestPayoutSummaryShowsSettledWithoutChangingNet(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	a := e.capturedFor(t, "m1", 100000, "k1")
	e.capturedFor(t, "m1", 50000, "k2") // stays unsettled
	e.age(t, a.ID, 2*day)
	e.svc.SettleDue(ctx, day) //nolint:errcheck

	s, err := ledger.Summary(ctx, e.pool, "m1", time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if s.NetPayoutPaise != 98000+49000 || s.SettledPaise != 98000 || s.GrossCollectedPaise != 150000 || s.FeesPaise != 3000 {
		t.Fatalf("%+v", s)
	}
	if s.GrossCollectedPaise-s.RefundedPaise-s.FeesPaise != s.NetPayoutPaise {
		t.Fatalf("identity broken by settlement: %+v", s)
	}
}

func TestRunSettlementLoop(t *testing.T) {
	e := setup(t)
	p := e.capturedFor(t, "m1", 100000, "k1")
	e.age(t, p.ID, 2*day)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- e.svc.RunSettlement(ctx, 20*time.Millisecond, day, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for e.settlements(t, p.ID) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("loop never settled the payment")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not stop on cancel")
	}
}
