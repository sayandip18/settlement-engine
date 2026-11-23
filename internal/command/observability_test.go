package command_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/sayandip18/settlement-engine/internal/command"
	"github.com/sayandip18/settlement-engine/internal/obs"
)

// The registry is process-wide, so tests assert on how much a counter moved, not on its value.
func finished(kind, outcome string) float64 {
	return promtest.ToFloat64(obs.OperationsFinished.WithLabelValues(kind, outcome))
}
func retries(kind string) float64 {
	return promtest.ToFloat64(obs.OperationRetries.WithLabelValues(kind))
}

func histCount(t *testing.T, o prometheus.Observer) uint64 {
	t.Helper()
	m, ok := o.(prometheus.Metric)
	if !ok {
		t.Fatal("observer is not a metric")
	}
	var out dto.Metric
	if err := m.Write(&out); err != nil {
		t.Fatal(err)
	}
	return out.GetHistogram().GetSampleCount()
}

func TestMetricsCountEachOutcomeOnce(t *testing.T) {
	h := newHarness(t)

	// success
	before := finished("AUTHORIZE", obs.OutcomeSucceeded)
	latencyBefore := histCount(t, obs.OperationLatency.WithLabelValues("AUTHORIZE", obs.OutcomeSucceeded))
	p := h.newPayment(t, "c1")
	op := h.submit(t, p.ID, command.KindAuthorize)
	h.pump(t, op)
	if d := finished("AUTHORIZE", obs.OutcomeSucceeded) - before; d != 1 {
		t.Fatalf("succeeded counter moved by %v, want 1", d)
	}
	if d := histCount(t, obs.OperationLatency.WithLabelValues("AUTHORIZE", obs.OutcomeSucceeded)) - latencyBefore; d != 1 {
		t.Fatalf("latency histogram observed %d, want 1", d)
	}

	// A duplicate delivery of an already-finished operation changes nothing.
	for i := 0; i < 3; i++ {
		if err := h.worker.Handle(context.Background(), toKafka(h.broker.onTopic(topic)[0], 99+i)); err != nil {
			t.Fatal(err)
		}
	}
	if d := finished("AUTHORIZE", obs.OutcomeSucceeded) - before; d != 1 {
		t.Fatalf("duplicates inflated the counter: moved by %v", d)
	}

	// permanent decline -> failed, no retries
	failedBefore, retriesBefore := finished("AUTHORIZE", obs.OutcomeFailed), retries("AUTHORIZE")
	declined := h.newPayment(t, "decline_x")
	h.pump(t, h.submit(t, declined.ID, command.KindAuthorize))
	if d := finished("AUTHORIZE", obs.OutcomeFailed) - failedBefore; d != 1 {
		t.Fatalf("failed counter moved by %v", d)
	}
	if d := retries("AUTHORIZE") - retriesBefore; d != 0 {
		t.Fatalf("a permanent failure scheduled %v retries", d)
	}
}

func TestMetricsCountRetriesAndDeadLetters(t *testing.T) {
	h := newHarness(t)

	// two transient failures then success: 2 retries, 1 success
	h.proc.failAuthorize.Store(2)
	retriesBefore, okBefore := retries("AUTHORIZE"), finished("AUTHORIZE", obs.OutcomeSucceeded)
	h.pump(t, h.submit(t, h.newPayment(t, "c1").ID, command.KindAuthorize))
	if d := retries("AUTHORIZE") - retriesBefore; d != 2 {
		t.Fatalf("retries moved by %v, want 2", d)
	}
	if d := finished("AUTHORIZE", obs.OutcomeSucceeded) - okBefore; d != 1 {
		t.Fatalf("succeeded moved by %v, want 1", d)
	}

	// always transient (fastPolicy: 3 attempts): 2 retries, then dead-lettered once
	retriesBefore, dlBefore := retries("AUTHORIZE"), finished("AUTHORIZE", obs.OutcomeDeadLettered)
	h.pump(t, h.submit(t, h.newPayment(t, "timeout_x").ID, command.KindAuthorize))
	if d := retries("AUTHORIZE") - retriesBefore; d != 2 {
		t.Fatalf("retries moved by %v, want 2", d)
	}
	if d := finished("AUTHORIZE", obs.OutcomeDeadLettered) - dlBefore; d != 1 {
		t.Fatalf("dead_lettered moved by %v, want 1", d)
	}
}

// The trace started by a request must continue through the outbox row, the Kafka
// message, the consumer and the worker, and across a retry.
func TestTraceFollowsOperationThroughOutboxKafkaAndWorker(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	p := startPipeline(t, command.RetryPolicy{MaxAttempts: 5, Base: 20 * time.Millisecond, Max: 100 * time.Millisecond, Jitter: 0.2})
	p.proc.failAuthorize.Store(1) // one retry, so the chain has two attempts
	pay := p.newPayment(t, "c1")

	ctx, root := obs.Tracer().Start(context.Background(), "test request")
	res, err := p.svc.Submit(ctx, command.SubmitInput{PaymentID: pay.ID, Kind: command.KindAuthorize})
	root.End()
	if err != nil {
		t.Fatal(err)
	}
	p.waitTerminal(t, res.Operation.ID)
	if got := p.get(t, res.Operation.ID); got.Status != command.StatusSucceeded || got.Attempts != 2 {
		t.Fatalf("operation: %+v", got)
	}

	trace := root.SpanContext().TraceID()
	byName := map[string][]tracetest.SpanStub{}
	for _, s := range exp.GetSpans() {
		if s.SpanContext.TraceID() == trace {
			byName[s.Name] = append(byName[s.Name], s)
		}
	}
	publish, consume, operation := byName["outbox publish "+p.topic], byName["kafka consume "+p.topic], byName["operation AUTHORIZE"]
	if len(publish) != 2 || len(consume) != 2 || len(operation) != 2 {
		t.Fatalf("want 2 publish/consume/operation spans in the request's trace (attempt + retry), got %d/%d/%d",
			len(publish), len(consume), len(operation))
	}

	// Parent links: request -> publish -> consume -> operation, and the retry hangs off the first operation.
	ids := map[string]bool{}
	for _, s := range publish {
		ids[s.SpanContext.SpanID().String()] = true
	}
	for _, s := range consume {
		if !ids[s.Parent.SpanID().String()] {
			t.Fatalf("consume span %s is not a child of a publish span", s.Name)
		}
	}
	consumeIDs := map[string]bool{}
	for _, s := range consume {
		consumeIDs[s.SpanContext.SpanID().String()] = true
	}
	for _, s := range operation {
		if !consumeIDs[s.Parent.SpanID().String()] {
			t.Fatalf("operation span is not a child of a consume span")
		}
	}
	first := publish[0]
	if first.Parent.SpanID() != root.SpanContext().SpanID() && publish[1].Parent.SpanID() != root.SpanContext().SpanID() {
		t.Fatal("no publish span is a child of the request span")
	}
}
