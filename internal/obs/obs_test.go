package obs_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/sayandip18/settlement-engine/internal/obs"
	"github.com/sayandip18/settlement-engine/internal/testutil"
)

func TestMain(m *testing.M) { os.Exit(testutil.Run(m)) }

func withTracer(t *testing.T) {
	t.Helper()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
}

func TestTraceContextSurvivesAMapRoundTrip(t *testing.T) {
	withTracer(t)
	ctx, span := obs.Tracer().Start(context.Background(), "request")
	defer span.End()

	carried := obs.Inject(ctx, nil)
	if carried["traceparent"] == "" {
		t.Fatalf("no traceparent injected: %v", carried)
	}
	got := trace.SpanContextFromContext(obs.Extract(context.Background(), carried))
	if got.TraceID() != span.SpanContext().TraceID() || got.SpanID() != span.SpanContext().SpanID() || !got.IsRemote() {
		t.Fatalf("extracted %v, want the original span's context", got)
	}
	if obs.TraceID(ctx) != span.SpanContext().TraceID().String() || obs.TraceID(context.Background()) != "" {
		t.Fatal("TraceID helper is wrong")
	}

	// Existing headers are kept; no span means nothing is added.
	m := obs.Inject(ctx, map[string]string{"error": "boom"})
	if m["error"] != "boom" || m["traceparent"] == "" {
		t.Fatalf("%v", m)
	}
	if out := obs.Inject(context.Background(), nil); out != nil {
		t.Fatalf("injected without a span: %v", out)
	}
	if ctx := obs.Extract(context.Background(), map[string]string{"traceparent": "garbage"}); trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("garbage traceparent must be ignored")
	}
}

func TestHeartbeatsReportOnlyStaleComponents(t *testing.T) {
	h := obs.NewHeartbeats()
	h.Register("fast", 100*time.Millisecond)
	beatSlow := h.Register("slow", time.Hour)
	if err := h.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	err := h.Check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "fast") || strings.Contains(err.Error(), "slow") {
		t.Fatalf("want only 'fast' reported, got %v", err)
	}
	h.Beat("fast")
	beatSlow()
	h.Beat("unknown") // must not panic
	if err := h.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func gaugeValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	mfs, _ := reg.Gather() // an error here is expected in the failure test; callers check presence
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if matches(m, labels) {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

func matches(m *dto.Metric, want map[string]string) bool {
	got := map[string]string{}
	for _, l := range m.GetLabel() {
		got[l.GetName()] = l.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

func TestDBCollectorReportsBacklogs(t *testing.T) {
	pool := testutil.NewPool(t)
	ctx := context.Background()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(obs.NewDBCollector(pool, []string{"TYPE_A", "TYPE_B"}))

	// Empty database: everything reads zero, including every finding type.
	for name, labels := range map[string]map[string]string{
		"settlement_outbox_pending_messages":           nil,
		"settlement_outbox_oldest_pending_age_seconds": nil,
		"settlement_dead_letters_pending":              nil,
		"settlement_operations_unfinished":             {"status": "PENDING"},
		"settlement_reconciliation_open_findings":      {"type": "TYPE_B", "status": "OPEN"},
	} {
		if v, ok := gaugeValue(t, reg, name, labels); !ok || v != 0 {
			t.Fatalf("%s on an empty database = %v (present %v)", name, v, ok)
		}
	}

	// Outbox: 2 due (oldest 30s), 1 scheduled for later, 1 already published.
	exec(`INSERT INTO outbox (topic, msg_key, payload, available_at) VALUES
	        ('t','a','x', now() - interval '30 seconds'), ('t','b','x', now() - interval '5 seconds'),
	        ('t','c','x', now() + interval '1 hour')`)
	exec(`INSERT INTO outbox (topic, msg_key, payload, available_at, published_at) VALUES ('t','d','x', now() - interval '1 hour', now())`)
	// Dead letters: 2 pending, 1 dismissed.
	for _, st := range []string{"PENDING", "PENDING", "DISMISSED"} {
		exec(`INSERT INTO dead_letters (id, dedupe_key, msg_key, payload, error, status) VALUES ($1, $2, 'k', 'x', 'e', $3)`,
			uuid.New(), uuid.NewString(), st)
	}
	// Operations: 1 pending, 2 dead-lettered, 1 succeeded (not counted).
	pid := uuid.New()
	exec(`INSERT INTO payments (id, merchant_id, customer_id, amount_paise, currency, status) VALUES ($1,'m','c',100,'INR','PENDING')`, pid)
	for _, st := range []string{"PENDING", "DEAD_LETTERED", "DEAD_LETTERED", "SUCCEEDED"} {
		exec(`INSERT INTO operations (id, payment_id, kind, status) VALUES ($1,$2,'AUTHORIZE',$3)`, uuid.New(), pid, st)
	}
	// Findings: A open x2, A acknowledged x1, A resolved (not counted).
	for _, st := range []string{"OPEN", "OPEN", "ACKNOWLEDGED"} {
		exec(`INSERT INTO discrepancies (id, type, subject, status) VALUES ($1,'TYPE_A',$2,$3)`, uuid.New(), uuid.NewString(), st)
	}
	exec(`INSERT INTO discrepancies (id, type, subject, status, resolved_at, resolution) VALUES ($1,'TYPE_A','r','RESOLVED', now(), 'AUTO')`, uuid.New())

	check := func(name string, labels map[string]string, want float64, atLeast bool) {
		t.Helper()
		v, ok := gaugeValue(t, reg, name, labels)
		if !ok || (!atLeast && v != want) || (atLeast && v < want) {
			t.Errorf("%s%v = %v (present %v), want %v", name, labels, v, ok, want)
		}
	}
	check("settlement_outbox_pending_messages", nil, 2, false)
	check("settlement_outbox_scheduled_messages", nil, 1, false)
	check("settlement_outbox_oldest_pending_age_seconds", nil, 30, true)
	check("settlement_dead_letters_pending", nil, 2, false)
	check("settlement_operations_unfinished", map[string]string{"status": "PENDING"}, 1, false)
	check("settlement_operations_unfinished", map[string]string{"status": "DEAD_LETTERED"}, 2, false)
	check("settlement_reconciliation_open_findings", map[string]string{"type": "TYPE_A", "status": "OPEN"}, 2, false)
	check("settlement_reconciliation_open_findings", map[string]string{"type": "TYPE_A", "status": "ACKNOWLEDGED"}, 1, false)
	check("settlement_reconciliation_open_findings", map[string]string{"type": "TYPE_B", "status": "OPEN"}, 0, false)

	// If one query fails, only its metrics disappear.
	exec(`ALTER TABLE dead_letters RENAME TO dead_letters_gone`)
	defer exec(`ALTER TABLE dead_letters_gone RENAME TO dead_letters`)
	if _, ok := gaugeValue(t, reg, "settlement_dead_letters_pending", nil); ok {
		t.Error("dead letter gauge should be absent while its query fails")
	}
	check("settlement_outbox_pending_messages", nil, 2, false)
}
