package payment_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sayandip18/settlement-engine/internal/payment"
	"github.com/sayandip18/settlement-engine/internal/processor"
	"github.com/sayandip18/settlement-engine/internal/testutil"
)

// countingProc wraps the mock and counts calls per operation.
type countingProc struct {
	processor.Mock
	authorize, capture, void, refund atomic.Int32
}

func (c *countingProc) Authorize(ctx context.Context, r processor.Request) error {
	c.authorize.Add(1)
	return c.Mock.Authorize(ctx, r)
}
func (c *countingProc) Capture(ctx context.Context, r processor.Request) error {
	c.capture.Add(1)
	return c.Mock.Capture(ctx, r)
}
func (c *countingProc) Void(ctx context.Context, r processor.Request) error {
	c.void.Add(1)
	return c.Mock.Void(ctx, r)
}
func (c *countingProc) Refund(ctx context.Context, r processor.Request) error {
	c.refund.Add(1)
	return c.Mock.Refund(ctx, r)
}

type env struct {
	svc  *payment.Service
	proc *countingProc
	pool *pgxpool.Pool
}

func setup(t *testing.T) env {
	t.Helper()
	pool := testutil.NewPool(t)
	proc := &countingProc{}
	return env{svc: payment.NewService(pool, proc, 200), proc: proc, pool: pool}
}

// newPayment creates a payment (default amount 100000 paise) for the given customer.
func (e env) newPayment(t *testing.T, customer, key string) payment.Payment {
	t.Helper()
	res, err := e.svc.Create(context.Background(), payment.CreateInput{
		MerchantID: "m1", CustomerID: customer, AmountPaise: 100000, IdempotencyKey: key,
	})
	if err != nil {
		t.Fatal(err)
	}
	return res.Payment
}

// captured returns a payment that has been authorized and captured.
func (e env) captured(t *testing.T, key string) payment.Payment {
	t.Helper()
	p := e.newPayment(t, "c1", key)
	if _, err := e.svc.Authorize(context.Background(), p.ID); err != nil {
		t.Fatal(err)
	}
	p, err := e.svc.Capture(context.Background(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func ptr(v int64) *int64 { return &v }

func TestHappyPathAuthorizeCapture(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.newPayment(t, "c1", "k")

	p, err := e.svc.Authorize(ctx, p.ID)
	if err != nil || p.Status != payment.StatusAuthorized {
		t.Fatalf("authorize: %v, %+v", err, p)
	}
	p, err = e.svc.Capture(ctx, p.ID)
	if err != nil || p.Status != payment.StatusCaptured || p.CapturedAt == nil {
		t.Fatalf("capture: %v, %+v", err, p)
	}
	got, _ := e.svc.Get(ctx, p.ID)
	if got.Status != payment.StatusCaptured {
		t.Fatalf("not persisted: %+v", got)
	}
}

func TestRepeatedActionsAreNoOps(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.newPayment(t, "c1", "k")

	for i := 0; i < 3; i++ {
		if _, err := e.svc.Authorize(ctx, p.ID); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := e.svc.Capture(ctx, p.ID); err != nil {
			t.Fatal(err)
		}
	}
	if a, c := e.proc.authorize.Load(), e.proc.capture.Load(); a != 1 || c != 1 {
		t.Fatalf("processor called authorize=%d capture=%d times, want 1 each", a, c)
	}
}

func TestInvalidTransitionsAreRejected(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	var terr *payment.InvalidTransitionError

	// Capture or refund before authorizing.
	p := e.newPayment(t, "c1", "k1")
	if _, err := e.svc.Capture(ctx, p.ID); !errors.As(err, &terr) {
		t.Fatalf("capture of PENDING: got %v", err)
	}
	if _, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, IdempotencyKey: "r"}); !errors.As(err, &terr) {
		t.Fatalf("refund of PENDING: got %v", err)
	}

	// Refunding a payment that was authorized but never collected.
	e.svc.Authorize(ctx, p.ID)
	if _, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, IdempotencyKey: "r"}); !errors.As(err, &terr) {
		t.Fatalf("refund of AUTHORIZED: got %v", err)
	}

	// Voiding after capture.
	c := e.captured(t, "k2")
	if _, err := e.svc.Void(ctx, c.ID); !errors.As(err, &terr) {
		t.Fatalf("void of CAPTURED: got %v", err)
	}

	// Capturing a voided payment.
	v := e.newPayment(t, "c1", "k3")
	e.svc.Authorize(ctx, v.ID)
	if _, err := e.svc.Void(ctx, v.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Capture(ctx, v.ID); !errors.As(err, &terr) {
		t.Fatalf("capture of VOIDED: got %v", err)
	}
	if e.proc.refund.Load() != 0 {
		t.Fatal("processor must not be called for rejected refunds")
	}
}

func TestPermanentDeclineFailsPayment(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.newPayment(t, "decline_1", "k")

	got, err := e.svc.Authorize(ctx, p.ID)
	var derr *payment.DeclinedError
	if !errors.As(err, &derr) || derr.Code != "insufficient_funds" {
		t.Fatalf("want decline, got %v", err)
	}
	if got.Status != payment.StatusFailed || got.FailureReason == nil {
		t.Fatalf("payment not failed: %+v", got)
	}
	stored, _ := e.svc.Get(ctx, p.ID)
	if stored.Status != payment.StatusFailed || *stored.FailureReason != "insufficient funds" {
		t.Fatalf("FAILED not persisted: %+v", stored)
	}

	// A permanent decline is final: no retry, and no further actions.
	var terr *payment.InvalidTransitionError
	if _, err := e.svc.Authorize(ctx, p.ID); !errors.As(err, &terr) {
		t.Fatalf("retry after FAILED: got %v", err)
	}
	if n := e.proc.authorize.Load(); n != 1 {
		t.Fatalf("processor called %d times, want 1", n)
	}
}

func TestCaptureDeclineFailsPayment(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.newPayment(t, "capture_decline_1", "k")
	if _, err := e.svc.Authorize(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	got, err := e.svc.Capture(ctx, p.ID)
	var derr *payment.DeclinedError
	if !errors.As(err, &derr) || got.Status != payment.StatusFailed {
		t.Fatalf("got %v, %+v", err, got)
	}
}

func TestTemporaryErrorLeavesPaymentUntouched(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.newPayment(t, "timeout_1", "k")

	_, err := e.svc.Authorize(ctx, p.ID)
	if !errors.Is(err, payment.ErrProcessorUnavailable) {
		t.Fatalf("want ErrProcessorUnavailable, got %v", err)
	}
	stored, _ := e.svc.Get(ctx, p.ID)
	if stored.Status != payment.StatusPending {
		t.Fatalf("status changed on transient error: %s", stored.Status)
	}
}

func TestFullRefund(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.captured(t, "k")

	res, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, IdempotencyKey: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Refund.AmountPaise != 100000 || res.Payment.Status != payment.StatusRefunded || res.Payment.RefundedPaise != 100000 {
		t.Fatalf("unexpected: %+v", res)
	}
	// REFUNDED is terminal.
	var terr *payment.InvalidTransitionError
	if _, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, IdempotencyKey: "r2"}); !errors.As(err, &terr) {
		t.Fatalf("refund of REFUNDED: got %v", err)
	}
}

func TestPartialRefundsAccumulate(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.captured(t, "k")

	res, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, AmountPaise: ptr(25000), IdempotencyKey: "r1"})
	if err != nil || res.Payment.Status != payment.StatusPartiallyRefunded || res.Payment.RefundedPaise != 25000 {
		t.Fatalf("first: %v, %+v", err, res)
	}
	res, err = e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, AmountPaise: ptr(50000), IdempotencyKey: "r2"})
	if err != nil || res.Payment.RefundedPaise != 75000 || res.Payment.Status != payment.StatusPartiallyRefunded {
		t.Fatalf("second: %v, %+v", err, res)
	}

	// More than what is left.
	var rerr *payment.RefundExceedsError
	_, err = e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, AmountPaise: ptr(25001), IdempotencyKey: "r3"})
	if !errors.As(err, &rerr) || rerr.RemainingPaise != 25000 {
		t.Fatalf("over-refund: got %v", err)
	}

	// Omitting the amount refunds the remainder and closes the payment.
	res, err = e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, IdempotencyKey: "r4"})
	if err != nil || res.Refund.AmountPaise != 25000 || res.Payment.Status != payment.StatusRefunded {
		t.Fatalf("remainder: %v, %+v", err, res)
	}
}

func TestRefundIdempotency(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.captured(t, "k")

	first, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, AmountPaise: ptr(10000), IdempotencyKey: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, AmountPaise: ptr(10000), IdempotencyKey: "r1"})
	if err != nil || !again.Replayed || again.Refund.ID != first.Refund.ID {
		t.Fatalf("replay: %v, %+v", err, again)
	}
	if again.Payment.RefundedPaise != 10000 || e.proc.refund.Load() != 1 {
		t.Fatalf("refunded twice: refunded=%d calls=%d", again.Payment.RefundedPaise, e.proc.refund.Load())
	}

	_, err = e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, AmountPaise: ptr(20000), IdempotencyKey: "r1"})
	if !errors.Is(err, payment.ErrIdempotencyReused) {
		t.Fatalf("same key, new amount: got %v", err)
	}
	// "Remainder" is a different request from an explicit amount, even if equal today.
	_, err = e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, IdempotencyKey: "r1"})
	if !errors.Is(err, payment.ErrIdempotencyReused) {
		t.Fatalf("same key, remainder: got %v", err)
	}

	// A replayed full refund still answers after the payment is REFUNDED.
	full, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, IdempotencyKey: "r2"})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, IdempotencyKey: "r2"})
	if err != nil || !replayed.Replayed || replayed.Refund.ID != full.Refund.ID {
		t.Fatalf("replay after REFUNDED: %v, %+v", err, replayed)
	}
}

func TestRefundDeclineAndValidation(t *testing.T) {
	e := setup(t)
	ctx := context.Background()

	// Captured under a customer whose refunds are declined.
	p := e.newPayment(t, "refund_decline_1", "k")
	e.svc.Authorize(ctx, p.ID)
	e.svc.Capture(ctx, p.ID)

	_, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, IdempotencyKey: "r1"})
	var derr *payment.DeclinedError
	if !errors.As(err, &derr) || derr.Payment.Status != payment.StatusCaptured {
		t.Fatalf("want decline with payment unchanged, got %v", err)
	}
	stored, _ := e.svc.Get(ctx, p.ID)
	if stored.Status != payment.StatusCaptured || stored.RefundedPaise != 0 {
		t.Fatalf("state changed after declined refund: %+v", stored)
	}

	var verr *payment.ValidationError
	for name, in := range map[string]payment.RefundInput{
		"no key":      {PaymentID: p.ID},
		"zero amount": {PaymentID: p.ID, IdempotencyKey: "x", AmountPaise: ptr(0)},
		"negative":    {PaymentID: p.ID, IdempotencyKey: "x", AmountPaise: ptr(-1)},
	} {
		if _, err := e.svc.Refund(ctx, in); !errors.As(err, &verr) {
			t.Errorf("%s: want ValidationError, got %v", name, err)
		}
	}
}

func TestActionsOnUnknownPayment(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.newPayment(t, "c1", "k")
	if _, err := e.svc.Get(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	missing := p
	missing.ID[0] ^= 0xff
	if _, err := e.svc.Authorize(ctx, missing.ID); !errors.Is(err, payment.ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestConcurrentCapturesCallProcessorOnce(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.newPayment(t, "c1", "k")
	if _, err := e.svc.Authorize(ctx, p.ID); err != nil {
		t.Fatal(err)
	}

	const n = 15
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = e.svc.Capture(ctx, p.ID)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("capture %d: %v", i, err)
		}
	}
	if c := e.proc.capture.Load(); c != 1 {
		t.Fatalf("processor capture called %d times, want 1", c)
	}
}

func TestConcurrentRefundsNeverExceedCaptured(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.captured(t, "k")

	// 10 refunds of 30000 against 100000: exactly 3 can fit.
	const n = 10
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = e.svc.Refund(ctx, payment.RefundInput{
				PaymentID: p.ID, AmountPaise: ptr(30000), IdempotencyKey: string(rune('a' + i)),
			})
		}()
	}
	wg.Wait()

	ok := 0
	for _, err := range errs {
		var rerr *payment.RefundExceedsError
		switch {
		case err == nil:
			ok++
		case !errors.As(err, &rerr):
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if ok != 3 {
		t.Fatalf("%d refunds succeeded, want 3", ok)
	}
	var sum int64
	if err := e.pool.QueryRow(ctx, `SELECT COALESCE(SUM(amount_paise),0) FROM refunds`).Scan(&sum); err != nil {
		t.Fatal(err)
	}
	stored, _ := e.svc.Get(ctx, p.ID)
	if sum != 90000 || stored.RefundedPaise != 90000 {
		t.Fatalf("refund rows sum to %d, payment says %d, want 90000", sum, stored.RefundedPaise)
	}
}
