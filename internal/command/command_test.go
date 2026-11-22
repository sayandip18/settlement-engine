package command_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/sayandip18/settlement-engine/internal/command"
	"github.com/sayandip18/settlement-engine/internal/payment"
	"github.com/sayandip18/settlement-engine/internal/platform/kafka"
)

func TestSubmitRecordsOperationAndOutboxAtomically(t *testing.T) {
	h := newHarness(t)
	p := h.newPayment(t, "c1")

	op := h.submit(t, p.ID, command.KindAuthorize)
	if op.Status != command.StatusPending || op.Attempts != 0 {
		t.Fatalf("unexpected operation: %+v", op)
	}
	if n := h.count(t, `SELECT count(*) FROM operations`); n != 1 {
		t.Fatalf("%d operations", n)
	}
	if n := h.count(t, `SELECT count(*) FROM outbox WHERE topic = $1 AND msg_key = $2 AND published_at IS NULL`,
		topic, p.ID.String()); n != 1 {
		t.Fatalf("%d outbox rows keyed by payment id, want 1", n)
	}
	// Nothing has touched the processor or the payment: the API call is only a request.
	if h.proc.authorize.Load() != 0 || h.payment(t, p.ID).Status != payment.StatusPending {
		t.Fatal("submit must not run the action itself")
	}
}

func TestAuthorizeThenCaptureFlow(t *testing.T) {
	h := newHarness(t)
	p := h.newPayment(t, "c1")

	auth := h.submit(t, p.ID, command.KindAuthorize)
	h.pump(t, auth)
	if got := h.get(t, auth.ID); got.Status != command.StatusSucceeded || got.Attempts != 1 || got.Error != nil {
		t.Fatalf("authorize op: %+v", got)
	}
	if s := h.payment(t, p.ID).Status; s != payment.StatusAuthorized {
		t.Fatalf("payment status %s", s)
	}

	capture := h.submit(t, p.ID, command.KindCapture)
	h.pump(t, capture)
	got := h.payment(t, p.ID)
	if got.Status != payment.StatusCaptured || got.FeePaise != 2000 {
		t.Fatalf("payment after capture: %+v", got)
	}
	if n := h.count(t, `SELECT count(*) FROM ledger_transactions WHERE kind = 'CAPTURE'`); n != 1 {
		t.Fatalf("%d capture ledger transactions", n)
	}
}

func TestVoidFlow(t *testing.T) {
	h := newHarness(t)
	p := h.authorized(t)
	op := h.submit(t, p.ID, command.KindVoid)
	h.pump(t, op)
	if s := h.payment(t, p.ID).Status; s != payment.StatusVoided || h.get(t, op.ID).Status != command.StatusSucceeded {
		t.Fatalf("payment status %s", s)
	}
}

func TestPermanentDeclineFailsWithoutRetry(t *testing.T) {
	h := newHarness(t)
	p := h.newPayment(t, "decline_1")
	op := h.submit(t, p.ID, command.KindAuthorize)
	h.pump(t, op)

	got := h.get(t, op.ID)
	if got.Status != command.StatusFailed || got.Attempts != 1 ||
		got.Error == nil || got.Error.Code != "insufficient_funds" {
		t.Fatalf("operation: %+v", got)
	}
	if s := h.payment(t, p.ID).Status; s != payment.StatusFailed {
		t.Fatalf("payment status %s, want FAILED", s)
	}
	if n := len(h.broker.onTopic(topic)); n != 1 {
		t.Fatalf("%d messages published, want 1: a decline must not be retried", n)
	}
	if c := h.proc.authorize.Load(); c != 1 {
		t.Fatalf("processor called %d times", c)
	}
}

func TestTransientFailureRetriesThenSucceeds(t *testing.T) {
	h := newHarness(t)
	h.proc.failAuthorize.Store(2)
	p := h.newPayment(t, "c1")
	op := h.submit(t, p.ID, command.KindAuthorize)
	h.pump(t, op)

	got := h.get(t, op.ID)
	if got.Status != command.StatusSucceeded || got.Attempts != 3 || got.Error != nil {
		t.Fatalf("operation: %+v", got)
	}
	if h.proc.authorize.Load() != 3 {
		t.Fatalf("processor called %d times, want 3", h.proc.authorize.Load())
	}
	if s := h.payment(t, p.ID).Status; s != payment.StatusAuthorized {
		t.Fatalf("payment status %s", s)
	}
	// Attempt numbers on the wire go 1, 2, 3.
	var attempts []int
	for _, m := range h.broker.onTopic(topic) {
		_, a := msgOf(t, m)
		attempts = append(attempts, a)
	}
	if len(attempts) != 3 || attempts[0] != 1 || attempts[1] != 2 || attempts[2] != 3 {
		t.Fatalf("attempt numbers %v", attempts)
	}
}

func TestRetriesAreDelayedByBackoff(t *testing.T) {
	h := newHarness(t)
	h.proc.failAuthorize.Store(1)
	p := h.newPayment(t, "c1")
	op := h.submit(t, p.ID, command.KindAuthorize)
	ctx := context.Background()

	h.relay.Flush(ctx)
	for _, m := range h.broker.undelivered() {
		h.worker.Handle(ctx, toKafka(m, 1))
	}
	// The retry row exists but is scheduled in the future, so it is not yet due.
	var due, scheduled int
	h.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL AND available_at > now()`).Scan(&scheduled)
	h.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL AND available_at <= now()`).Scan(&due)
	if scheduled+due != 1 {
		t.Fatalf("want exactly one pending retry row, got scheduled=%d due=%d", scheduled, due)
	}
	if got := h.get(t, op.ID); got.Status != command.StatusPending || got.Attempts != 1 ||
		got.Error == nil || got.Error.Code != "processor_unavailable" {
		t.Fatalf("operation should show the transient error while pending: %+v", got)
	}
}

func TestRetriesExhaustedDeadLetter(t *testing.T) {
	h := newHarness(t)
	p := h.newPayment(t, "timeout_1") // the mock always times out for this customer
	op := h.submit(t, p.ID, command.KindAuthorize)
	h.pump(t, op)

	got := h.get(t, op.ID)
	if got.Status != command.StatusDeadLettered || got.Attempts != 3 ||
		got.Error == nil || got.Error.Code != "processor_unavailable" {
		t.Fatalf("operation: %+v", got)
	}
	if s := h.payment(t, p.ID).Status; s != payment.StatusPending {
		t.Fatalf("payment status %s: a dead-lettered operation must leave the payment untouched", s)
	}

	h.relay.Flush(context.Background())
	dlq := h.broker.onTopic(command.DLQTopic(topic))
	if len(dlq) != 1 {
		t.Fatalf("%d DLQ messages, want 1", len(dlq))
	}
	hd := dlq[0].Headers
	if hd["operation_id"] != op.ID.String() || hd["attempts"] != "3" || hd["source_topic"] != topic || hd["error"] == "" {
		t.Fatalf("DLQ headers: %v", hd)
	}
	if opID, attempt := msgOf(t, dlq[0]); opID != op.ID || attempt != 3 {
		t.Fatalf("DLQ payload should be the last failed message, got op=%s attempt=%d", opID, attempt)
	}
}

func TestDuplicateDeliveryRunsOnce(t *testing.T) {
	h := newHarness(t)
	p := h.newPayment(t, "c1")
	op := h.submit(t, p.ID, command.KindAuthorize)
	ctx := context.Background()
	h.relay.Flush(ctx)
	m := toKafka(h.broker.undelivered()[0], 1)

	for i := 0; i < 3; i++ { // at-least-once: the broker may redeliver
		if err := h.worker.Handle(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	if c := h.proc.authorize.Load(); c != 1 {
		t.Fatalf("processor called %d times, want 1", c)
	}
	if got := h.get(t, op.ID); got.Status != command.StatusSucceeded || got.Attempts != 1 {
		t.Fatalf("operation: %+v", got)
	}
}

func TestConcurrentDuplicateDeliveriesScheduleOneRetry(t *testing.T) {
	h := newHarness(t)
	p := h.newPayment(t, "timeout_1")
	h.submit(t, p.ID, command.KindAuthorize)
	ctx := context.Background()
	h.relay.Flush(ctx)
	m := toKafka(h.broker.undelivered()[0], 1)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := h.worker.Handle(ctx, m); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	// Several workers may each call the processor, but only one outcome is recorded.
	if n := h.count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 1 {
		t.Fatalf("%d retry messages scheduled, want 1", n)
	}
	if n := h.count(t, `SELECT attempts FROM operations`); n != 1 {
		t.Fatalf("attempts = %d, want 1", n)
	}
}

func TestPoisonMessagesGoStraightToDLQ(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	unknown := `{"operation_id":"` + uuid.NewString() + `","attempt":1}`
	for name, value := range map[string]string{
		"garbage":           `not json at all`,
		"empty":             ``,
		"unknown operation": unknown,
		"zero attempt":      `{"operation_id":"` + uuid.NewString() + `","attempt":0}`,
		"nil operation":     `{"operation_id":"00000000-0000-0000-0000-000000000000","attempt":1}`,
	} {
		err := h.worker.Handle(ctx, kafka.Message{Topic: topic, Partition: 2, Offset: 7, Key: "k", Value: []byte(value)})
		if err != nil {
			t.Errorf("%s: Handle returned %v; poison must be consumed, not retried", name, err)
		}
	}
	h.relay.Flush(ctx)
	dlq := h.broker.onTopic(command.DLQTopic(topic))
	if len(dlq) != 5 {
		t.Fatalf("%d DLQ messages, want 5", len(dlq))
	}
	preserved := false
	for _, m := range dlq {
		if m.Headers["source_partition"] != "2" || m.Headers["source_offset"] != "7" || m.Headers["error"] == "" {
			t.Fatalf("headers: %v", m.Headers)
		}
		preserved = preserved || string(m.Payload) == "not json at all"
	}
	if !preserved {
		t.Fatal("the original payload of the undecodable message must be preserved in the DLQ")
	}
}

func TestRefundFlow(t *testing.T) {
	h := newHarness(t)
	p := h.captured(t)

	res := h.refund(t, p.ID, ptr(25000), "r1")
	if res.Replayed || res.Operation.Kind != command.KindRefund || *res.Operation.RequestedPaise != 25000 {
		t.Fatalf("unexpected: %+v", res)
	}
	h.pump(t, res.Operation)
	got := h.payment(t, p.ID)
	if got.Status != payment.StatusPartiallyRefunded || got.RefundedPaise != 25000 {
		t.Fatalf("payment: %+v", got)
	}

	// "Refund the rest" with no amount.
	rest := h.refund(t, p.ID, nil, "r2")
	h.pump(t, rest.Operation)
	if got := h.payment(t, p.ID); got.Status != payment.StatusRefunded || got.RefundedPaise != 100000 {
		t.Fatalf("payment: %+v", got)
	}
}

func TestRefundRequestIdempotency(t *testing.T) {
	h := newHarness(t)
	p := h.captured(t)

	first := h.refund(t, p.ID, ptr(10000), "r1")
	again := h.refund(t, p.ID, ptr(10000), "r1")
	if !again.Replayed || again.Operation.ID != first.Operation.ID {
		t.Fatalf("expected replay of %s, got %+v", first.Operation.ID, again)
	}
	if n := h.count(t, `SELECT count(*) FROM operations WHERE kind = 'REFUND'`); n != 1 {
		t.Fatalf("%d refund operations, want 1", n)
	}
	if n := h.count(t, `SELECT count(*) FROM outbox WHERE topic = $1`, topic); n != 3 { // authorize, capture, one refund
		t.Fatalf("%d command messages, want 3", n)
	}

	_, err := h.svc.Submit(context.Background(), command.SubmitInput{
		PaymentID: p.ID, Kind: command.KindRefund, AmountPaise: ptr(20000), IdempotencyKey: "r1"})
	if !errors.Is(err, payment.ErrIdempotencyReused) {
		t.Fatalf("same key, different amount: got %v", err)
	}

	// Still answered from the stored operation after the refund finished, and after
	// the payment moved on to a status where new refunds would be rejected.
	h.pump(t, first.Operation)
	full := h.refund(t, p.ID, nil, "r2")
	h.pump(t, full.Operation)
	for _, key := range []string{"r1", "r2"} {
		var amount *int64
		if key == "r1" {
			amount = ptr(10000)
		}
		res := h.refund(t, p.ID, amount, key)
		if !res.Replayed || !res.Operation.Status.Terminal() {
			t.Fatalf("replay of %s after completion: %+v", key, res)
		}
	}
	if s := h.payment(t, p.ID).Status; s != payment.StatusRefunded {
		t.Fatalf("payment status %s", s)
	}
}

func TestConcurrentIdenticalRefundRequestsCreateOneOperation(t *testing.T) {
	h := newHarness(t)
	p := h.captured(t)

	const n = 12
	ids := make([]uuid.UUID, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := h.svc.Submit(context.Background(), command.SubmitInput{
				PaymentID: p.ID, Kind: command.KindRefund, AmountPaise: ptr(5000), IdempotencyKey: "same"})
			if err != nil {
				t.Error(err)
				return
			}
			ids[i] = res.Operation.ID
		}()
	}
	wg.Wait()
	for i := range ids {
		if ids[i] != ids[0] {
			t.Fatalf("request %d got operation %s, want %s", i, ids[i], ids[0])
		}
	}
	if c := h.count(t, `SELECT count(*) FROM operations WHERE kind = 'REFUND'`); c != 1 {
		t.Fatalf("%d refund operations", c)
	}
}

func TestSecondRefundThatNoLongerFitsFailsAtExecution(t *testing.T) {
	h := newHarness(t)
	p := h.captured(t)

	// Both pass the early check (60000 <= 100000 remaining) because neither has run yet.
	a := h.refund(t, p.ID, ptr(60000), "a")
	b := h.refund(t, p.ID, ptr(60000), "b")
	h.pump(t, a.Operation, b.Operation)

	ga, gb := h.get(t, a.Operation.ID), h.get(t, b.Operation.ID)
	if ga.Status != command.StatusSucceeded || gb.Status != command.StatusFailed ||
		gb.Error == nil || gb.Error.Code != "refund_exceeds_remaining" {
		t.Fatalf("a=%+v b=%+v", ga, gb)
	}
	if got := h.payment(t, p.ID); got.RefundedPaise != 60000 {
		t.Fatalf("refunded %d, want 60000", got.RefundedPaise)
	}
}

func TestAmountsAndStateAreCheckedAtSubmit(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	pending := h.newPayment(t, "c1")

	var (
		terr *payment.InvalidTransitionError
		rerr *payment.RefundExceedsError
		verr *payment.ValidationError
	)
	submit := func(in command.SubmitInput) error { _, err := h.svc.Submit(ctx, in); return err }

	if err := submit(command.SubmitInput{PaymentID: pending.ID, Kind: command.KindCapture}); !errors.As(err, &terr) {
		t.Errorf("capture of PENDING: %v", err)
	}
	if err := submit(command.SubmitInput{PaymentID: pending.ID, Kind: command.KindVoid}); !errors.As(err, &terr) {
		t.Errorf("void of PENDING: %v", err)
	}
	if err := submit(command.SubmitInput{PaymentID: pending.ID, Kind: command.KindRefund, IdempotencyKey: "k"}); !errors.As(err, &terr) {
		t.Errorf("refund of PENDING: %v", err)
	}
	if err := submit(command.SubmitInput{PaymentID: uuid.New(), Kind: command.KindAuthorize}); !errors.Is(err, payment.ErrNotFound) {
		t.Errorf("unknown payment: %v", err)
	}
	if err := submit(command.SubmitInput{PaymentID: pending.ID, Kind: command.KindRefund}); !errors.As(err, &verr) {
		t.Errorf("refund without key: %v", err)
	}
	for _, bad := range []int64{0, -1} {
		if err := submit(command.SubmitInput{PaymentID: pending.ID, Kind: command.KindRefund, AmountPaise: ptr(bad), IdempotencyKey: "k"}); !errors.As(err, &verr) {
			t.Errorf("refund of %d: %v", bad, err)
		}
	}

	c := h.captured(t)
	if err := submit(command.SubmitInput{PaymentID: c.ID, Kind: command.KindRefund, AmountPaise: ptr(100001), IdempotencyKey: "k"}); !errors.As(err, &rerr) || rerr.RemainingPaise != 100000 {
		t.Errorf("over-refund: %v", err)
	}
	if err := submit(command.SubmitInput{PaymentID: c.ID, Kind: command.KindVoid}); !errors.As(err, &terr) {
		t.Errorf("void of CAPTURED: %v", err)
	}
	if err := submit(command.SubmitInput{PaymentID: c.ID, Kind: command.KindAuthorize}); !errors.As(err, &terr) {
		t.Errorf("authorize of CAPTURED: %v", err)
	}

	// Rejected requests leave no trace.
	if n := h.count(t, `SELECT count(*) FROM operations WHERE payment_id = $1`, pending.ID); n != 0 {
		t.Fatalf("%d operations recorded for rejected requests", n)
	}
}

func TestRepeatedAuthorizeRequestIsHarmless(t *testing.T) {
	h := newHarness(t)
	p := h.newPayment(t, "c1")
	a := h.submit(t, p.ID, command.KindAuthorize)
	b := h.submit(t, p.ID, command.KindAuthorize)
	h.pump(t, a, b)
	if h.get(t, a.ID).Status != command.StatusSucceeded || h.get(t, b.ID).Status != command.StatusSucceeded {
		t.Fatal("both requests should succeed")
	}
	if c := h.proc.authorize.Load(); c != 1 {
		t.Fatalf("processor called %d times, want 1", c)
	}
}

func TestOperationNotFound(t *testing.T) {
	h := newHarness(t)
	if _, err := h.svc.Get(context.Background(), uuid.New()); !errors.Is(err, command.ErrOperationNotFound) {
		t.Fatalf("got %v", err)
	}
}
