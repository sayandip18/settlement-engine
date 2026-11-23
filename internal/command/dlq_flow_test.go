package command_test

import (
	"context"
	"errors"
	"testing"
	"time"

	kg "github.com/segmentio/kafka-go"

	"github.com/sayandip18/settlement-engine/internal/command"
	"github.com/sayandip18/settlement-engine/internal/deadletter"
	"github.com/sayandip18/settlement-engine/internal/payment"
	"github.com/sayandip18/settlement-engine/internal/platform/kafka"
)

// storeDLQ plays the DLQ consumer: it copies everything the relay has published to the
// DLQ topic into the dead_letters table.
func storeDLQ(t *testing.T, h *harness, dl *deadletter.Service) {
	t.Helper()
	if _, err := h.relay.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i, m := range h.broker.onTopic(command.DLQTopic(topic)) {
		err := dl.Record(context.Background(), kafka.Message{
			Topic: m.Topic, Offset: int64(i), Key: m.Key, Value: m.Payload, Headers: m.Headers})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func pendingEntries(t *testing.T, dl *deadletter.Service) []deadletter.Entry {
	t.Helper()
	s := deadletter.StatusPending
	items, _, err := dl.List(context.Background(), deadletter.ListFilter{Status: &s})
	if err != nil {
		t.Fatal(err)
	}
	return items
}

func TestExhaustedOperationCanBeReplayedAfterTheFaultIsFixed(t *testing.T) {
	h := newHarness(t)
	dl := deadletter.NewService(h.pool, h.svc)
	ctx := context.Background()

	h.proc.failAuthorize.Store(3) // the processor is down for exactly the first run's 3 attempts
	p := h.newPayment(t, "c1")
	op := h.submit(t, p.ID, command.KindAuthorize)
	h.pump(t, op)
	if got := h.get(t, op.ID); got.Status != command.StatusDeadLettered {
		t.Fatalf("setup: %+v", got)
	}

	storeDLQ(t, h, dl)
	entries := pendingEntries(t, dl)
	if len(entries) != 1 || entries[0].OperationID == nil || *entries[0].OperationID != op.ID || entries[0].Attempts != 3 {
		t.Fatalf("dead letters: %+v", entries)
	}

	// The processor has recovered. Replay gives the operation a brand-new run.
	replayed, err := dl.Replay(ctx, entries[0].ID)
	if err != nil || replayed.Status != deadletter.StatusReplayed {
		t.Fatalf("replay: %+v %v", replayed, err)
	}
	if got := h.get(t, op.ID); got.Status != command.StatusPending || got.Attempts != 0 {
		t.Fatalf("operation after replay: %+v", got)
	}
	h.pump(t, op)

	got := h.get(t, op.ID)
	if got.Status != command.StatusSucceeded || got.Attempts != 1 {
		t.Fatalf("operation after replay run: %+v", got)
	}
	if s := h.payment(t, p.ID).Status; s != payment.StatusAuthorized {
		t.Fatalf("payment status %s", s)
	}
	if c := h.proc.authorize.Load(); c != 4 {
		t.Fatalf("processor called %d times, want 3 failures + 1 success", c)
	}
	// It cannot be replayed a second time.
	var handled *deadletter.AlreadyHandledError
	if _, err := dl.Replay(ctx, entries[0].ID); !errors.As(err, &handled) {
		t.Fatalf("second replay: %v", err)
	}
}

func TestReplayWhileFaultPersistsReturnsToDLQWithoutLooping(t *testing.T) {
	h := newHarness(t)
	dl := deadletter.NewService(h.pool, h.svc)
	ctx := context.Background()

	h.proc.failAuthorize.Store(6) // still down for the whole first replay run too
	p := h.newPayment(t, "c1")
	op := h.submit(t, p.ID, command.KindAuthorize)
	h.pump(t, op)
	storeDLQ(t, h, dl)
	first := pendingEntries(t, dl)[0]

	if _, err := dl.Replay(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	h.pump(t, op) // fails 3 more times and dead-letters again
	if got := h.get(t, op.ID); got.Status != command.StatusDeadLettered || got.Attempts != 3 {
		t.Fatalf("operation: %+v", got)
	}

	// The failure is a new pending entry; the replayed one stays as history. Nothing loops.
	storeDLQ(t, h, dl)
	pending := pendingEntries(t, dl)
	if len(pending) != 1 || pending[0].ID == first.ID {
		t.Fatalf("pending entries: %+v", pending)
	}
	old, _ := dl.Get(ctx, first.ID)
	if old.Status != deadletter.StatusReplayed {
		t.Fatalf("original entry: %+v", old)
	}

	// Fixed at last: the second replay completes it.
	if _, err := dl.Replay(ctx, pending[0].ID); err != nil {
		t.Fatal(err)
	}
	h.pump(t, op)
	if got := h.get(t, op.ID); got.Status != command.StatusSucceeded {
		t.Fatalf("operation: %+v", got)
	}
}

func TestKafkaDeadLettersAreStoredAndReplayable(t *testing.T) {
	p := startPipeline(t, command.RetryPolicy{MaxAttempts: 3, Base: 20 * time.Millisecond, Max: 100 * time.Millisecond, Jitter: 0.2})
	ctx := context.Background()
	p.proc.failAuthorize.Store(3)

	pay := p.newPayment(t, "c1")
	op := p.submit(t, pay.ID, command.KindAuthorize)
	p.waitTerminal(t, op.ID)

	w := &kg.Writer{Addr: kg.TCP(p.brokers...), Topic: p.topic, RequiredAcks: kg.RequireAll}
	defer w.Close()
	if err := w.WriteMessages(ctx, kg.Message{Key: []byte("poison"), Value: []byte("}{ not json")}); err != nil {
		t.Fatal(err)
	}

	// The DLQ consumer stores both without any help.
	var entries []deadletter.Entry
	waitUntil(t, "2 dead letters stored", func() bool {
		entries = pendingEntries(t, p.dlq)
		return len(entries) == 2
	})
	var exhausted, bad deadletter.Entry
	for _, e := range entries {
		if e.OperationID != nil {
			exhausted = e
		} else {
			bad = e
		}
	}
	if *exhausted.OperationID != op.ID || exhausted.Attempts != 3 || bad.Error != "undecodable message" {
		t.Fatalf("entries: exhausted=%+v bad=%+v", exhausted, bad)
	}

	// Replay the exhausted operation (the processor is healthy again): it completes.
	if _, err := p.dlq.Replay(ctx, exhausted.ID); err != nil {
		t.Fatal(err)
	}
	p.waitTerminal(t, op.ID)
	if got := p.get(t, op.ID); got.Status != command.StatusSucceeded || got.Attempts != 1 {
		t.Fatalf("operation after replay: %+v", got)
	}

	// Replaying the poison message cannot fix it, so it comes straight back to the DLQ as a
	// new entry. The original is marked replayed and the pipeline does not loop on it.
	if _, err := p.dlq.Replay(ctx, bad.ID); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "poison back in the DLQ as a new entry", func() bool {
		pending := pendingEntries(t, p.dlq)
		return len(pending) == 1 && pending[0].ID != bad.ID && pending[0].Error == "undecodable message"
	})
	time.Sleep(time.Second)
	if n := len(pendingEntries(t, p.dlq)); n != 1 {
		t.Fatalf("%d pending entries after settling; poison is looping", n)
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
