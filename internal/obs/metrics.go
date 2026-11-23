// Package obs holds the service's observability plumbing: Prometheus metrics,
// OpenTelemetry trace propagation helpers and component heartbeats. It depends on no
// other internal package, so everything else can import it.
package obs

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry is the process-wide metrics registry served at /metrics.
var Registry = prometheus.NewRegistry()

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
}

const ns = "settlement"

var f = promauto.With(Registry)

// Outcomes of an operation, used as the "outcome" label.
const (
	OutcomeSucceeded    = "succeeded"
	OutcomeFailed       = "failed"
	OutcomeDeadLettered = "dead_lettered"
)

var (
	HTTPRequests = f.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "http_requests_total",
		Help: "HTTP requests by method, route pattern and status code."}, []string{"method", "route", "status"})
	HTTPDuration = f.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "http_request_duration_seconds",
		Help: "HTTP request latency by method and route pattern.", Buckets: prometheus.DefBuckets}, []string{"method", "route"})

	OperationsFinished = f.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "operations_finished_total",
		Help: "Operations that reached a final outcome (succeeded, failed, dead_lettered)."}, []string{"kind", "outcome"})
	OperationRetries = f.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "operation_retries_total",
		Help: "Retries scheduled after a transient failure."}, []string{"kind"})
	OperationLatency = f.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "operation_duration_seconds",
		Help:    "Time from an operation being accepted to its final outcome.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 900}}, []string{"kind", "outcome"})

	OutboxPublished = f.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "outbox_published_total",
		Help: "Outbox messages published to Kafka."})
	OutboxPublishErrors = f.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "outbox_publish_errors_total",
		Help: "Failed outbox flushes."})
	KafkaHandlerErrors = f.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "kafka_handler_errors_total",
		Help: "Consumed messages whose handler returned an error (the message is retried)."}, []string{"topic"})
	DeadLettersRecorded = f.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "dead_letters_recorded_total",
		Help: "Dead letters stored from the DLQ topic."})

	SettlementsTotal = f.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "settlements_total",
		Help: "Payments settled (paid out to the merchant)."})

	ReconcileRuns = f.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "reconciliation_runs_total",
		Help: "Reconciliation runs by result (ok, failed, skipped)."}, []string{"result"})
	ReconcileNewFindings = f.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "reconciliation_new_findings_total",
		Help: "New discrepancies opened, by type."}, []string{"type"})
	ReconcileLastSuccess = f.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "reconciliation_last_success_timestamp_seconds",
		Help: "Unix time of the last reconciliation run in which every check ran."})

	RetentionPurged = f.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "retention_purged_total",
		Help: "Rows deleted by the retention job, by table."}, []string{"table"})
)

// Handler serves the registry. A collector that fails (e.g. the database is down)
// drops only its own metrics instead of failing the whole scrape.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError})
}
