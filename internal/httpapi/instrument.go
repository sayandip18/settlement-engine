package httpapi

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/sayandip18/settlement-engine/internal/obs"
)

// Instrument records a server span and Prometheus metrics for every request. It
// continues a trace from an incoming traceparent header, so callers can join theirs.
//
// Metrics are labelled with the matched route pattern (/v1/payments/{id}), never the raw
// path, so the number of series stays bounded however many IDs are requested.
func Instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := obs.ExtractHTTP(r.Context(), r.Header)
		ctx, span := obs.Tracer().Start(ctx, r.Method, trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("http.request.method", r.Method)))
		defer span.End()

		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		start := time.Now()
		next.ServeHTTP(ww, r.WithContext(ctx))

		route := "unmatched"
		if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
			route = rc.RoutePattern()
		}
		status := ww.Status()
		if status == 0 {
			status = http.StatusOK
		}
		span.SetName(r.Method + " " + route)
		span.SetAttributes(attribute.String("http.route", route), attribute.Int("http.response.status_code", status))
		if status >= 500 {
			span.SetStatus(codes.Error, http.StatusText(status))
		}
		obs.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(status)).Inc()
		obs.HTTPDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
	})
}
