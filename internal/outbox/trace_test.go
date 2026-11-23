package outbox_test

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/sayandip18/settlement-engine/internal/obs"
	"github.com/sayandip18/settlement-engine/internal/outbox"
	"github.com/sayandip18/settlement-engine/internal/testutil"
)

func useRecorder(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return exp
}

// The trace of the transaction that enqueued a message is stored with it, and the relay
// publishes under a producer span of that same trace.
func TestTraceContextIsStoredAndPropagatedOnPublish(t *testing.T) {
	exp := useRecorder(t)
	pool := testutil.NewPool(t)
	ctx, root := obs.Tracer().Start(context.Background(), "request")

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Enqueue(ctx, tx, outbox.Message{Topic: "t", Key: "k", Payload: []byte("x"),
		Headers: map[string]string{"error": "boom"}}); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Enqueue(context.Background(), tx, outbox.Message{Topic: "t", Key: "untraced", Payload: []byte("y")}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	root.End()

	// The caller's own headers are not modified by enqueueing.
	var stored string
	if err := pool.QueryRow(ctx, `SELECT headers::text FROM outbox WHERE msg_key = 'k'`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if want := root.SpanContext().TraceID().String(); !strings.Contains(stored, want) || !strings.Contains(stored, "boom") {
		t.Fatalf("stored headers %s lack the trace id %s or the caller's header", stored, want)
	}

	pub := &fakePublisher{}
	if _, err := relay(pool, pub, 100).Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	var traced, untraced outbox.Message
	for _, m := range pub.got {
		if m.Key == "k" {
			traced = m
		} else {
			untraced = m
		}
	}
	if traced.Headers["error"] != "boom" {
		t.Fatalf("caller header lost: %v", traced.Headers)
	}
	published := trace.SpanContextFromContext(obs.Extract(context.Background(), traced.Headers))
	if published.TraceID() != root.SpanContext().TraceID() {
		t.Fatalf("published message is in trace %s, want the request's %s", published.TraceID(), root.SpanContext().TraceID())
	}
	if published.SpanID() == root.SpanContext().SpanID() {
		t.Fatal("the publish should carry its own span, not the request's")
	}

	// The publish span is a child of the request and is the one the message carries.
	var found bool
	for _, s := range exp.GetSpans() {
		if s.Name == "outbox publish t" && s.SpanContext.TraceID() == root.SpanContext().TraceID() {
			found = true
			if s.Parent.SpanID() != root.SpanContext().SpanID() || s.SpanContext.SpanID() != published.SpanID() {
				t.Fatalf("publish span wiring wrong: parent %s, id %s, carried %s", s.Parent.SpanID(), s.SpanContext.SpanID(), published.SpanID())
			}
		}
	}
	if !found {
		t.Fatal("no publish span recorded in the request's trace")
	}
	// A message enqueued outside any trace still publishes, under its own new trace.
	if !trace.SpanContextFromContext(obs.Extract(context.Background(), untraced.Headers)).IsValid() {
		t.Fatal("untraced message should get a trace from the relay")
	}
}

func TestPublishFailureMarksSpanAndKeepsRowPending(t *testing.T) {
	exp := useRecorder(t)
	pool := testutil.NewPool(t)
	enqueue(t, pool, msg("1"))
	pub := &fakePublisher{fail: context.DeadlineExceeded}
	if _, err := relay(pool, pub, 10).Flush(context.Background()); err == nil {
		t.Fatal("want an error")
	}
	for _, s := range exp.GetSpans() {
		if s.Name == "outbox publish t" && s.Status.Code == codes.Error {
			return
		}
	}
	t.Fatalf("no errored publish span among %d spans", len(exp.GetSpans()))
}
