package httpapi_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/sayandip18/settlement-engine/internal/httpapi"
	"github.com/sayandip18/settlement-engine/internal/obs"
)

func newInstrumented() http.Handler {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return httpapi.NewRouter(log, httpapi.NewHealth(nil), "secret",
		func(r chi.Router) {
			r.Get("/v1/things/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
			r.Get("/v1/boom", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) })
		}, nil, httpapi.WithMetrics(obs.Handler()))
}

func get(h http.Handler, path string, hdr ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// Metrics are labelled by route pattern, so a million distinct IDs make one series.
func TestRequestMetricsUseRoutePatternNotRawPath(t *testing.T) {
	h := newInstrumented()
	ok := func() float64 {
		return promtest.ToFloat64(obs.HTTPRequests.WithLabelValues("GET", "/v1/things/{id}", "200"))
	}
	before := ok()
	for _, id := range []string{"1", "2", "abc", "3f9c"} {
		get(h, "/v1/things/"+id)
	}
	if d := ok() - before; d != 4 {
		t.Fatalf("pattern series moved by %v, want 4", d)
	}

	notFoundBefore := promtest.ToFloat64(obs.HTTPRequests.WithLabelValues("GET", "unmatched", "404"))
	get(h, "/nope/123")
	get(h, "/nope/456")
	if d := promtest.ToFloat64(obs.HTTPRequests.WithLabelValues("GET", "unmatched", "404")) - notFoundBefore; d != 2 {
		t.Fatalf("unmatched routes must share one series, moved by %v", d)
	}

	errBefore := promtest.ToFloat64(obs.HTTPRequests.WithLabelValues("GET", "/v1/boom", "500"))
	get(h, "/v1/boom")
	if d := promtest.ToFloat64(obs.HTTPRequests.WithLabelValues("GET", "/v1/boom", "500")) - errBefore; d != 1 {
		t.Fatalf("500s not counted: %v", d)
	}
}

func TestMetricsEndpointRequiresAdminKey(t *testing.T) {
	h := newInstrumented()
	if rec := get(h, "/metrics"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no key: %d", rec.Code)
	}
	rec := get(h, "/metrics", "Authorization", "Bearer secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("with key: %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"settlement_http_requests_total", "settlement_http_request_duration_seconds", "go_goroutines"} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %s", want)
		}
	}
}

func TestServerSpanJoinsIncomingTraceAndCarriesRoute(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	h := newInstrumented()
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	get(h, "/v1/things/42", "traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("%d spans", len(spans))
	}
	if got := spans[0].SpanContext.TraceID().String(); got != traceID {
		t.Fatalf("span did not join the caller's trace: %s", got)
	}
	if spans[0].Name != "GET /v1/things/{id}" {
		t.Fatalf("span name %q", spans[0].Name)
	}
}

func TestReadinessFailsWhenAComponentStopsBeating(t *testing.T) {
	beats := obs.NewHeartbeats()
	beat := beats.Register("outbox-relay", 150*time.Millisecond)
	h := httpapi.NewHealth(map[string]httpapi.Check{"components": beats.Check})
	ready := func() int {
		rec := httptest.NewRecorder()
		h.Ready(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		return rec.Code
	}
	if ready() != http.StatusOK {
		t.Fatal("a freshly registered component must count as alive")
	}
	time.Sleep(400 * time.Millisecond)
	if ready() != http.StatusServiceUnavailable {
		t.Fatal("a silent component must make the worker unready")
	}
	beat()
	if ready() != http.StatusOK {
		t.Fatal("beating again must restore readiness")
	}
}
