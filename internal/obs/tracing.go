package obs

import (
	"context"
	"net/http"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/sayandip18/settlement-engine"

// Tracer returns the service's tracer.
func Tracer() trace.Tracer { return otel.Tracer(tracerName) }

// InitTracing installs a tracer provider and returns its shutdown function.
//
// Spans are exported over OTLP/HTTP when OTEL_EXPORTER_OTLP_ENDPOINT (or the traces
// variant) is set; the standard OTEL_* variables configure the rest. Without an
// endpoint spans are still created, so trace IDs propagate across the outbox and
// Kafka and show up in logs, but nothing leaves the process.
func InitTracing(ctx context.Context, service string) (func(context.Context) error, error) {
	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service))),
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" {
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp.Shutdown, nil
}

var propagator = propagation.TraceContext{}

// Inject writes ctx's trace context (a W3C traceparent) into m and returns it, creating
// the map when needed. It leaves m alone when ctx carries no span. It is how a trace
// survives being stored in the outbox and sent through Kafka headers.
func Inject(ctx context.Context, m map[string]string) map[string]string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return m
	}
	if m == nil {
		m = map[string]string{}
	}
	propagator.Inject(ctx, propagation.MapCarrier(m))
	return m
}

// Extract returns ctx continued from the trace context stored in m, if any.
func Extract(ctx context.Context, m map[string]string) context.Context {
	if len(m) == 0 {
		return ctx
	}
	return propagator.Extract(ctx, propagation.MapCarrier(m))
}

// ExtractHTTP continues ctx from the traceparent header of an incoming request.
func ExtractHTTP(ctx context.Context, h http.Header) context.Context {
	return propagator.Extract(ctx, propagation.HeaderCarrier(h))
}

// TraceID is the trace ID in ctx, or "" when there is none (for log correlation).
func TraceID(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		return sc.TraceID().String()
	}
	return ""
}
