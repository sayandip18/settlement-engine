package command_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sayandip18/settlement-engine/internal/command"
	"github.com/sayandip18/settlement-engine/internal/outbox"
	"github.com/sayandip18/settlement-engine/internal/payment"
	"github.com/sayandip18/settlement-engine/internal/platform/kafka"
	"github.com/sayandip18/settlement-engine/internal/processor"
	"github.com/sayandip18/settlement-engine/internal/testutil"
)

func TestMain(m *testing.M) { os.Exit(testutil.Run(m)) }

const topic = "payments.commands"

// fakeBroker stands in for Kafka: it records everything the relay publishes.
type fakeBroker struct {
	mu   sync.Mutex
	msgs []outbox.Message
	next int // index of the first message not yet delivered to the worker
}

func (b *fakeBroker) Publish(_ context.Context, msgs []outbox.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.msgs = append(b.msgs, msgs...)
	return nil
}

// undelivered returns commands-topic messages not yet handed to the worker.
func (b *fakeBroker) undelivered() []outbox.Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []outbox.Message
	for ; b.next < len(b.msgs); b.next++ {
		if b.msgs[b.next].Topic == topic {
			out = append(out, b.msgs[b.next])
		}
	}
	return out
}

func (b *fakeBroker) onTopic(t string) []outbox.Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []outbox.Message
	for _, m := range b.msgs {
		if m.Topic == t {
			out = append(out, m)
		}
	}
	return out
}

// flakyProc is the mock processor plus failure injection and call counting.
type flakyProc struct {
	processor.Mock
	failAuthorize atomic.Int32 // fail this many authorize calls with a temporary error first
	authorize     atomic.Int32
	capture       atomic.Int32
	refund        atomic.Int32
}

func (p *flakyProc) Authorize(ctx context.Context, r processor.Request) error {
	n := p.authorize.Add(1)
	if n <= p.failAuthorize.Load() {
		return fmt.Errorf("%w: injected failure %d", processor.ErrTemporary, n)
	}
	return p.Mock.Authorize(ctx, r)
}
func (p *flakyProc) Capture(ctx context.Context, r processor.Request) error {
	p.capture.Add(1)
	return p.Mock.Capture(ctx, r)
}
func (p *flakyProc) Refund(ctx context.Context, r processor.Request) error {
	p.refund.Add(1)
	return p.Mock.Refund(ctx, r)
}

type harness struct {
	pool     *pgxpool.Pool
	proc     *flakyProc
	payments *payment.Service
	svc      *command.Service
	worker   *command.Worker
	relay    *outbox.Relay
	broker   *fakeBroker
	offset   int // fake Kafka offset: like the real thing, it only ever grows
}

var fastPolicy = command.RetryPolicy{MaxAttempts: 3, Base: time.Millisecond, Max: 4 * time.Millisecond, Jitter: 0.2}

func newHarness(t *testing.T) *harness {
	t.Helper()
	pool := testutil.NewPool(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	proc := &flakyProc{}
	payments := payment.NewService(pool, proc, 200)
	broker := &fakeBroker{}
	return &harness{
		pool: pool, proc: proc, payments: payments, broker: broker,
		svc:    command.NewService(pool, payments, topic),
		worker: command.NewWorker(pool, payments, topic, fastPolicy, log),
		relay:  outbox.NewRelay(pool, broker, log, 100, time.Millisecond),
	}
}

// newPayment creates a PENDING payment of ₹1,000 for the given customer.
func (h *harness) newPayment(t *testing.T, customer string) payment.Payment {
	t.Helper()
	res, err := h.payments.Create(context.Background(), payment.CreateInput{
		MerchantID: "m1", CustomerID: customer, AmountPaise: 100000, IdempotencyKey: uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	return res.Payment
}

func (h *harness) submit(t *testing.T, id uuid.UUID, kind command.Kind) command.Operation {
	t.Helper()
	res, err := h.svc.Submit(context.Background(), command.SubmitInput{PaymentID: id, Kind: kind})
	if err != nil {
		t.Fatalf("submit %s: %v", kind, err)
	}
	return res.Operation
}

func (h *harness) refund(t *testing.T, id uuid.UUID, amount *int64, key string) command.SubmitResult {
	t.Helper()
	res, err := h.svc.Submit(context.Background(), command.SubmitInput{
		PaymentID: id, Kind: command.KindRefund, AmountPaise: amount, IdempotencyKey: key})
	if err != nil {
		t.Fatalf("submit refund: %v", err)
	}
	return res
}

func toKafka(m outbox.Message, offset int) kafka.Message {
	return kafka.Message{Topic: m.Topic, Key: m.Key, Value: m.Payload, Offset: int64(offset)}
}

// pump plays the part of broker + consumer: it publishes outbox rows and hands
// each message to the worker, until every given operation reaches a terminal state.
func (h *harness) pump(t *testing.T, ops ...command.Operation) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := h.relay.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		for _, m := range h.broker.undelivered() {
			h.offset++
			if err := h.worker.Handle(ctx, toKafka(m, h.offset)); err != nil {
				t.Fatalf("worker.Handle: %v", err)
			}
		}
		done := true
		for _, op := range ops {
			if !h.get(t, op.ID).Status.Terminal() {
				done = false
			}
		}
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for operations to finish")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// flush publishes pending outbox rows to the fake broker, failing the test on error.
func (h *harness) flush(t *testing.T) {
	t.Helper()
	if _, err := h.relay.Flush(context.Background()); err != nil {
		t.Fatalf("relay flush: %v", err)
	}
}

func (h *harness) get(t *testing.T, id uuid.UUID) command.Operation {
	t.Helper()
	op, err := h.svc.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func (h *harness) payment(t *testing.T, id uuid.UUID) payment.Payment {
	t.Helper()
	p, err := h.payments.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func (h *harness) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := h.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// authorized returns a payment that has been authorized through the async path.
func (h *harness) authorized(t *testing.T) payment.Payment {
	t.Helper()
	p := h.newPayment(t, "c1")
	h.pump(t, h.submit(t, p.ID, command.KindAuthorize))
	return h.payment(t, p.ID)
}

// captured returns a payment that has been authorized and captured.
func (h *harness) captured(t *testing.T) payment.Payment {
	t.Helper()
	p := h.authorized(t)
	h.pump(t, h.submit(t, p.ID, command.KindCapture))
	return h.payment(t, p.ID)
}

func msgOf(t *testing.T, m outbox.Message) (opID uuid.UUID, attempt int) {
	t.Helper()
	var v struct {
		OperationID uuid.UUID `json:"operation_id"`
		Attempt     int       `json:"attempt"`
	}
	if err := json.Unmarshal(m.Payload, &v); err != nil {
		t.Fatal(err)
	}
	return v.OperationID, v.Attempt
}

func ptr(v int64) *int64 { return &v }
