package obs

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// DBCollector exposes backlog gauges that only the database knows: outbox lag, DLQ
// size, pending operations and open reconciliation findings. They are read at scrape
// time, so they are exact and need no bookkeeping in the write path.
type DBCollector struct {
	pool         *pgxpool.Pool
	findingTypes []string

	outboxPending, outboxLag, outboxScheduled, dlqPending *prometheus.Desc
	operations, findings                                  *prometheus.Desc
}

// NewDBCollector registers on Registry. findingTypes are the reconciliation finding
// types, so every one is reported (as 0) even when there are none.
func NewDBCollector(pool *pgxpool.Pool, findingTypes []string) *DBCollector {
	d := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(ns+"_"+name, help, labels, nil)
	}
	c := &DBCollector{
		pool: pool, findingTypes: findingTypes,
		outboxPending:   d("outbox_pending_messages", "Unpublished outbox messages that are due now."),
		outboxLag:       d("outbox_oldest_pending_age_seconds", "Age of the oldest due, unpublished outbox message (0 if none): the relay's lag."),
		outboxScheduled: d("outbox_scheduled_messages", "Unpublished outbox messages scheduled for later (retry backoff)."),
		dlqPending:      d("dead_letters_pending", "Dead letters waiting for an operator to replay or dismiss them."),
		operations:      d("operations_unfinished", "Operations not yet succeeded or failed, by status (PENDING or DEAD_LETTERED).", "status"),
		findings:        d("reconciliation_open_findings", "Unresolved reconciliation findings.", "type", "status"),
	}
	return c
}

// Register adds the collector to the process registry.
func (c *DBCollector) Register() *DBCollector {
	Registry.MustRegister(c)
	return c
}

func (c *DBCollector) Describe(ch chan<- *prometheus.Desc) { prometheus.DescribeByCollect(c, ch) }

func (c *DBCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	fail := func(d *prometheus.Desc, err error) { ch <- prometheus.NewInvalidMetric(d, err) }
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}

	var pending, scheduled int64
	var lag float64
	if err := c.pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE available_at <= now()),
		       COALESCE(EXTRACT(EPOCH FROM now() - MIN(available_at) FILTER (WHERE available_at <= now())), 0)::float8,
		       count(*) FILTER (WHERE available_at > now())
		  FROM outbox WHERE published_at IS NULL`).Scan(&pending, &lag, &scheduled); err != nil {
		fail(c.outboxPending, err)
	} else {
		gauge(c.outboxPending, float64(pending))
		gauge(c.outboxLag, lag)
		gauge(c.outboxScheduled, float64(scheduled))
	}

	var dlq int64
	if err := c.pool.QueryRow(ctx, `SELECT count(*) FROM dead_letters WHERE status = 'PENDING'`).Scan(&dlq); err != nil {
		fail(c.dlqPending, err)
	} else {
		gauge(c.dlqPending, float64(dlq))
	}

	ops := map[string]float64{"PENDING": 0, "DEAD_LETTERED": 0}
	if rows, err := c.pool.Query(ctx,
		`SELECT status, count(*) FROM operations WHERE status IN ('PENDING', 'DEAD_LETTERED') GROUP BY status`); err != nil {
		fail(c.operations, err)
	} else {
		for rows.Next() {
			var s string
			var n int64
			if err := rows.Scan(&s, &n); err == nil {
				ops[s] = float64(n)
			}
		}
		rows.Close()
		for s, n := range ops {
			gauge(c.operations, n, s)
		}
	}

	open := map[[2]string]float64{}
	for _, t := range c.findingTypes {
		open[[2]string{t, "OPEN"}], open[[2]string{t, "ACKNOWLEDGED"}] = 0, 0
	}
	if rows, err := c.pool.Query(ctx,
		`SELECT type, status, count(*) FROM discrepancies WHERE status <> 'RESOLVED' GROUP BY type, status`); err != nil {
		fail(c.findings, err)
	} else {
		for rows.Next() {
			var t, s string
			var n int64
			if err := rows.Scan(&t, &s, &n); err == nil {
				open[[2]string{t, s}] = float64(n)
			}
		}
		rows.Close()
		for k, n := range open {
			gauge(c.findings, n, k[0], k[1])
		}
	}
}
