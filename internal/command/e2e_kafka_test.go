package command_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	kg "github.com/segmentio/kafka-go"

	"github.com/sayandip18/settlement-engine/internal/command"
	"github.com/sayandip18/settlement-engine/internal/outbox"
	"github.com/sayandip18/settlement-engine/internal/payment"
	kafkaplat "github.com/sayandip18/settlement-engine/internal/platform/kafka"
	"github.com/sayandip18/settlement-engine/internal/testutil"
)

// pipeline is the whole async path against a real broker: outbox relay -> Kafka ->
// consumer group -> worker -> processor, exactly as cmd/worker wires it.
type pipeline struct {
	*harness
	brokers []string
	topic   string
}

func startPipeline(t *testing.T, policy command.RetryPolicy) *pipeline {
	t.Helper()
	brokers := testutil.NewKafka(t)
	h := newHarness(t)
	id := uuid.NewString()[:8]
	topic := "e2e-" + id
	ctx, cancel := context.WithCancel(context.Background())

	if err := kafkaplat.EnsureTopics(ctx, brokers,
		kafkaplat.Topic{Name: topic, Partitions: 3},
		kafkaplat.Topic{Name: command.DLQTopic(topic), Partitions: 1}); err != nil {
		cancel()
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h.svc = command.NewService(h.pool, h.payments, topic)
	worker := command.NewWorker(h.pool, h.payments, topic, policy, log)

	pub := kafkaplat.NewPublisher(brokers)
	relay := outbox.NewRelay(h.pool, pub, log, 100, 20*time.Millisecond)
	consumer := kafkaplat.NewConsumer(brokers, "group-"+id, topic, log)

	var wg sync.WaitGroup
	for _, fn := range []func(){
		func() { relay.Run(ctx) },
		func() { consumer.Run(ctx, worker.Handle) },
	} {
		wg.Add(1)
		go func() { defer wg.Done(); fn() }()
	}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		consumer.Close()
		pub.Close()
	})
	return &pipeline{harness: h, brokers: brokers, topic: topic}
}

func (p *pipeline) waitTerminal(t *testing.T, ids ...uuid.UUID) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for _, id := range ids {
		for !p.get(t, id).Status.Terminal() {
			if time.Now().After(deadline) {
				t.Fatalf("operation %s still %s after 60s", id, p.get(t, id).Status)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestKafkaEndToEndPaymentLifecycle(t *testing.T) {
	p := startPipeline(t, fastPolicy)
	pay := p.newPayment(t, "c1")

	// Each step is requested only once the previous one finished, as a real client would.
	for _, kind := range []command.Kind{command.KindAuthorize, command.KindCapture} {
		op := p.submit(t, pay.ID, kind)
		p.waitTerminal(t, op.ID)
		if got := p.get(t, op.ID); got.Status != command.StatusSucceeded {
			t.Fatalf("%s: %+v", kind, got)
		}
	}
	if s := p.payment(t, pay.ID).Status; s != payment.StatusCaptured {
		t.Fatalf("payment status %s", s)
	}

	r1 := p.refund(t, pay.ID, ptr(30000), "r1").Operation
	r2 := p.refund(t, pay.ID, nil, "r2").Operation // the remainder; order is guaranteed by the shared key
	p.waitTerminal(t, r1.ID, r2.ID)
	if got := p.payment(t, pay.ID); got.Status != payment.StatusRefunded || got.RefundedPaise != 100000 {
		t.Fatalf("payment: %+v (r1=%+v r2=%+v)", got, p.get(t, r1.ID), p.get(t, r2.ID))
	}
	if n := p.count(t, `SELECT count(*) FROM ledger_transactions`); n != 3 { // capture + 2 refunds
		t.Fatalf("%d ledger transactions, want 3", n)
	}
	if n := p.count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 0 {
		t.Fatalf("%d outbox rows left unpublished", n)
	}
}

func TestKafkaRetriesThenRecoversAcrossRealDelays(t *testing.T) {
	p := startPipeline(t, command.RetryPolicy{MaxAttempts: 5, Base: 100 * time.Millisecond, Max: time.Second, Jitter: 0.2})
	p.proc.failAuthorize.Store(2)
	pay := p.newPayment(t, "c1")

	start := time.Now()
	op := p.submit(t, pay.ID, command.KindAuthorize)
	p.waitTerminal(t, op.ID)

	got := p.get(t, op.ID)
	if got.Status != command.StatusSucceeded || got.Attempts != 3 {
		t.Fatalf("operation: %+v", got)
	}
	// Two retries wait ~100ms and ~200ms (each +/-20%), so at least ~240ms in total.
	if elapsed := time.Since(start); elapsed < 240*time.Millisecond {
		t.Fatalf("finished in %v: retries were not delayed by backoff", elapsed)
	}
}

func TestKafkaDeadLetterQueue(t *testing.T) {
	p := startPipeline(t, command.RetryPolicy{MaxAttempts: 3, Base: 20 * time.Millisecond, Max: 100 * time.Millisecond, Jitter: 0.2})

	// 1. A payment whose processor never recovers.
	pay := p.newPayment(t, "timeout_1")
	op := p.submit(t, pay.ID, command.KindAuthorize)
	p.waitTerminal(t, op.ID)
	if got := p.get(t, op.ID); got.Status != command.StatusDeadLettered || got.Attempts != 3 {
		t.Fatalf("operation: %+v", got)
	}

	// 2. A poison message written straight to the commands topic.
	w := &kg.Writer{Addr: kg.TCP(p.brokers...), Topic: p.topic, RequiredAcks: kg.RequireAll}
	defer w.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, kg.Message{Key: []byte("poison"), Value: []byte("}{ not json")}); err != nil {
		t.Fatal(err)
	}

	// Both arrive on the DLQ topic, with the failure details as headers.
	r := kg.NewReader(kg.ReaderConfig{Brokers: p.brokers, Topic: command.DLQTopic(p.topic), Partition: 0,
		StartOffset: kg.FirstOffset, MaxWait: 200 * time.Millisecond})
	defer r.Close()
	byKey := map[string]kg.Message{}
	for len(byKey) < 2 {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("reading DLQ (have %d of 2): %v", len(byKey), err)
		}
		byKey[string(m.Key)] = m
	}
	header := func(m kg.Message, k string) string {
		for _, h := range m.Headers {
			if h.Key == k {
				return string(h.Value)
			}
		}
		return ""
	}
	exhausted := byKey[pay.ID.String()]
	if header(exhausted, "operation_id") != op.ID.String() || header(exhausted, "attempts") != "3" ||
		header(exhausted, "source_topic") != p.topic || header(exhausted, "error") == "" {
		t.Fatalf("exhausted-retries message headers: %+v", exhausted.Headers)
	}
	poison := byKey["poison"]
	if string(poison.Value) != "}{ not json" || header(poison, "error") != "undecodable message" {
		t.Fatalf("poison message: value=%q headers=%+v", poison.Value, poison.Headers)
	}

	// The poison message did not block the healthy traffic behind it.
	good := p.newPayment(t, "c1")
	gop := p.submit(t, good.ID, command.KindAuthorize)
	p.waitTerminal(t, gop.ID)
	if got := p.get(t, gop.ID); got.Status != command.StatusSucceeded {
		t.Fatalf("healthy operation after poison: %+v", got)
	}
}
