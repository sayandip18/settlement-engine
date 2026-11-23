// Package outbox implements the transactional outbox: messages are inserted in the
// same database transaction as the state change that produced them, and a relay
// publishes them to the broker afterwards. This avoids the dual-write problem (a
// committed payment change whose message was never sent, or vice versa).
//
// Delivery is at-least-once: a crash after publishing but before marking a row
// published causes a re-publish, so consumers must tolerate duplicates.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/sayandip18/settlement-engine/internal/obs"
)

// Message is one outgoing broker message.
type Message struct {
	Topic   string
	Key     string // messages with the same key stay ordered
	Payload []byte
	Headers map[string]string
	// Delay postpones publication by this long, measured on the database's clock (not
	// the application's), so clock skew between the two cannot stretch or shrink it.
	Delay time.Duration
}

// Publisher sends a batch to the broker. It must return an error unless every
// message was durably accepted.
type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

// Enqueue adds a message using the caller's transaction.
func Enqueue(ctx context.Context, tx pgx.Tx, m Message) error {
	// Carry the caller's trace through the outbox so the eventual publish and the
	// consumer's work join the request that caused them.
	var carried map[string]string
	if len(m.Headers) > 0 {
		carried = make(map[string]string, len(m.Headers)+1)
		for k, v := range m.Headers {
			carried[k] = v
		}
	}
	carried = obs.Inject(ctx, carried)
	headers, err := json.Marshal(carried)
	if err != nil {
		return fmt.Errorf("marshal outbox headers: %w", err)
	}
	if carried == nil {
		headers = []byte("{}")
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox (topic, msg_key, payload, headers, available_at)
		 VALUES ($1, $2, $3, $4, now() + $5 * interval '1 microsecond')`,
		m.Topic, m.Key, m.Payload, headers, m.Delay.Microseconds()); err != nil {
		return fmt.Errorf("enqueue outbox message: %w", err)
	}
	return nil
}

// Relay publishes pending outbox rows.
type Relay struct {
	pool      *pgxpool.Pool
	pub       Publisher
	log       *slog.Logger
	batchSize int
	interval  time.Duration
	beat      func()
}

// SetHeartbeat makes Run call beat on every loop iteration (for readiness checks).
func (r *Relay) SetHeartbeat(beat func()) { r.beat = beat }

func NewRelay(pool *pgxpool.Pool, pub Publisher, log *slog.Logger, batchSize int, interval time.Duration) *Relay {
	if batchSize <= 0 {
		batchSize = 100
	}
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	return &Relay{pool: pool, pub: pub, log: log, batchSize: batchSize, interval: interval}
}

// Flush publishes one batch of due messages and returns how many were sent.
//
// Rows are locked with FOR UPDATE SKIP LOCKED for the duration of the publish, so
// several relay instances can run side by side without sending the same row twice.
// If publishing fails the transaction rolls back and the rows stay pending.
func (r *Relay) Flush(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rows, err := tx.Query(ctx,
		`SELECT id, topic, msg_key, payload, headers FROM outbox
		  WHERE published_at IS NULL AND available_at <= now()
		  ORDER BY id LIMIT $1
		  FOR UPDATE SKIP LOCKED`, r.batchSize)
	if err != nil {
		return 0, fmt.Errorf("select outbox: %w", err)
	}
	var (
		ids  []int64
		msgs []Message
	)
	for rows.Next() {
		var (
			id      int64
			m       Message
			headers []byte
		)
		if err := rows.Scan(&id, &m.Topic, &m.Key, &m.Payload, &headers); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan outbox: %w", err)
		}
		if err := json.Unmarshal(headers, &m.Headers); err != nil {
			rows.Close()
			return 0, fmt.Errorf("decode headers of outbox row %d: %w", id, err)
		}
		ids, msgs = append(ids, id), append(msgs, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read outbox: %w", err)
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	// One producer span per message, continuing the trace stored in its headers; its
	// context replaces the stored one so the consumer's span hangs off the publish.
	spans := make([]trace.Span, len(msgs))
	for i := range msgs {
		sctx, sp := obs.Tracer().Start(obs.Extract(ctx, msgs[i].Headers), "outbox publish "+msgs[i].Topic,
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(attribute.String("messaging.destination.name", msgs[i].Topic),
				attribute.String("messaging.kafka.message.key", msgs[i].Key), attribute.Int64("outbox.id", ids[i])))
		msgs[i].Headers = obs.Inject(sctx, msgs[i].Headers)
		spans[i] = sp
	}
	perr := r.pub.Publish(ctx, msgs)
	for _, sp := range spans {
		if perr != nil {
			sp.RecordError(perr)
			sp.SetStatus(codes.Error, "publish failed")
		}
		sp.End()
	}
	if perr != nil {
		obs.OutboxPublishErrors.Inc()
		return 0, fmt.Errorf("publish: %w", perr)
	}
	obs.OutboxPublished.Add(float64(len(msgs)))
	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return len(msgs), nil
}

// Run flushes until ctx is cancelled, sleeping only when there is nothing to do.
func (r *Relay) Run(ctx context.Context) error {
	for {
		if r.beat != nil {
			r.beat()
		}
		n, err := r.Flush(ctx)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			r.log.Error("outbox flush failed", "err", err)
		case n >= r.batchSize:
			continue // more is probably waiting
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.interval):
		}
	}
}
