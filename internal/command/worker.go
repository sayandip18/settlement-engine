package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/sayandip18/settlement-engine/internal/obs"
	"github.com/sayandip18/settlement-engine/internal/outbox"
	"github.com/sayandip18/settlement-engine/internal/payment"
	"github.com/sayandip18/settlement-engine/internal/platform/kafka"
)

// DLQTopic is where messages that cannot be processed end up.
func DLQTopic(commandsTopic string) string { return commandsTopic + ".dlq" }

// Worker executes queued operations.
//
// Delivery is at-least-once, so Handle must tolerate duplicates. Two things make
// that safe: the payment actions themselves are idempotent (a repeated authorize or
// capture is a no-op, a repeated refund is keyed), and every outcome is recorded with
// a fenced UPDATE that only succeeds for the next unseen attempt number.
type Worker struct {
	pool     *pgxpool.Pool
	payments *payment.Service
	policy   RetryPolicy
	topic    string
	log      *slog.Logger
	rnd      func() float64
}

func NewWorker(pool *pgxpool.Pool, payments *payment.Service, commandsTopic string, policy RetryPolicy, log *slog.Logger) *Worker {
	return &Worker{pool: pool, payments: payments, policy: policy, topic: commandsTopic,
		log: log, rnd: randFloat}
}

// Handle processes one queue message. It returns an error only for infrastructure
// trouble (e.g. the database is down), in which case the caller must redeliver; all
// business outcomes, including giving up, are recorded and return nil.
func (w *Worker) Handle(ctx context.Context, m kafka.Message) error {
	var msg message
	if err := json.Unmarshal(m.Value, &msg); err != nil || msg.OperationID == uuid.Nil || msg.Attempt < 1 {
		return w.deadLetterRaw(ctx, m, "undecodable message")
	}

	op, err := scanOperation(w.pool.QueryRow(ctx, `SELECT `+opCols+` FROM operations WHERE id = $1`, msg.OperationID))
	if errors.Is(err, pgx.ErrNoRows) {
		return w.deadLetterRaw(ctx, m, "unknown operation "+msg.OperationID.String())
	}
	if err != nil {
		return fmt.Errorf("load operation: %w", err)
	}

	// Duplicate or stale delivery: already finished, or this attempt was handled.
	if op.Status != StatusPending || msg.Attempt <= op.Attempts {
		w.log.Debug("skipping duplicate message", "operation_id", op.ID, "attempt", msg.Attempt)
		return nil
	}

	// Everything below, including the retry or DLQ message it may enqueue, belongs to
	// this span, so a retried operation stays in one trace.
	ctx, span := obs.Tracer().Start(ctx, "operation "+string(op.Kind), trace.WithAttributes(
		attribute.String("operation.id", op.ID.String()), attribute.String("payment.id", op.PaymentID.String()),
		attribute.Int("operation.attempt", msg.Attempt)))
	defer span.End()

	err = w.execute(ctx, op)
	if ctx.Err() != nil {
		return ctx.Err() // shutting down mid-call: leave it for redelivery
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	if err == nil {
		return w.finish(ctx, op, msg.Attempt, StatusSucceeded, nil)
	}
	if f, permanent := classify(err); permanent {
		w.log.Info("operation failed permanently", "operation_id", op.ID, "kind", op.Kind, "code", f.Code)
		return w.finish(ctx, op, msg.Attempt, StatusFailed, &f)
	}

	f := Failure{Code: "processor_unavailable", Message: err.Error()}
	if msg.Attempt >= w.policy.MaxAttempts {
		w.log.Warn("retries exhausted, dead-lettering", "operation_id", op.ID, "attempts", msg.Attempt, "err", err)
		return w.deadLetterOperation(ctx, m, op, msg.Attempt, f)
	}
	delay := w.policy.Delay(msg.Attempt, w.rnd)
	w.log.Info("transient failure, scheduling retry", "operation_id", op.ID, "attempt", msg.Attempt, "delay", delay, "err", err)
	return w.scheduleRetry(ctx, op, msg.Attempt, delay, f)
}

func (w *Worker) execute(ctx context.Context, op Operation) error {
	var err error
	switch op.Kind {
	case KindAuthorize:
		_, err = w.payments.Authorize(ctx, op.PaymentID)
	case KindCapture:
		_, err = w.payments.Capture(ctx, op.PaymentID)
	case KindVoid:
		_, err = w.payments.Void(ctx, op.PaymentID)
	case KindRefund:
		key := ""
		if op.idemKey != nil {
			key = *op.idemKey
		}
		_, err = w.payments.Refund(ctx, payment.RefundInput{
			PaymentID: op.PaymentID, AmountPaise: op.RequestedPaise, IdempotencyKey: key})
	default:
		return &payment.ValidationError{Msg: "unknown operation kind " + string(op.Kind)}
	}
	return err
}

// classify separates errors that will never succeed on retry (permanent) from those
// that might (transient). Unknown errors count as transient: retrying is safe because
// every action is idempotent, and the attempt cap stops it from looping forever.
func classify(err error) (Failure, bool) {
	var (
		declined *payment.DeclinedError
		invalid  *payment.InvalidTransitionError
		exceeds  *payment.RefundExceedsError
		verr     *payment.ValidationError
	)
	switch {
	case errors.As(err, &declined):
		return Failure{declined.Code, declined.Message}, true
	case errors.As(err, &invalid):
		return Failure{"invalid_state", invalid.Error()}, true
	case errors.As(err, &exceeds):
		return Failure{"refund_exceeds_remaining", exceeds.Error()}, true
	case errors.As(err, &verr):
		return Failure{"invalid_request", verr.Msg}, true
	case errors.Is(err, payment.ErrNotFound):
		return Failure{"not_found", "payment not found"}, true
	case errors.Is(err, payment.ErrIdempotencyReused):
		return Failure{"idempotency_key_reused", err.Error()}, true
	}
	return Failure{}, false
}

// fenced runs fn in a transaction after a conditional UPDATE that only matches while
// the operation is still PENDING and has not yet recorded this attempt. If another
// delivery got there first, nothing is written and fn is not called.
func (w *Worker) fenced(ctx context.Context, op Operation, attempt int, status Status, f *Failure,
	fn func(tx pgx.Tx) error) error {

	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var code, msg *string
	if f != nil {
		code, msg = &f.Code, &f.Message
	}
	tag, err := tx.Exec(ctx,
		`UPDATE operations
		    SET status = $2, attempts = $3, error_code = $4, error_message = $5, updated_at = now()
		  WHERE id = $1 AND status = 'PENDING' AND attempts < $3`,
		op.ID, status, attempt, code, msg)
	if err != nil {
		return fmt.Errorf("update operation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	if fn != nil {
		if err := fn(tx); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	// Counted only when this delivery's update applied, so duplicate deliveries do not
	// inflate the numbers.
	kind := string(op.Kind)
	switch status {
	case StatusPending:
		obs.OperationRetries.WithLabelValues(kind).Inc()
	case StatusSucceeded, StatusFailed, StatusDeadLettered:
		outcome := obs.OutcomeSucceeded
		if status == StatusFailed {
			outcome = obs.OutcomeFailed
		} else if status == StatusDeadLettered {
			outcome = obs.OutcomeDeadLettered
		}
		obs.OperationsFinished.WithLabelValues(kind, outcome).Inc()
		obs.OperationLatency.WithLabelValues(kind, outcome).Observe(time.Since(op.CreatedAt).Seconds())
	}
	return nil
}

func (w *Worker) finish(ctx context.Context, op Operation, attempt int, status Status, f *Failure) error {
	return w.fenced(ctx, op, attempt, status, f, nil)
}

func (w *Worker) scheduleRetry(ctx context.Context, op Operation, attempt int, delay time.Duration, f Failure) error {
	return w.fenced(ctx, op, attempt, StatusPending, &f, func(tx pgx.Tx) error {
		payload, err := json.Marshal(message{OperationID: op.ID, Attempt: attempt + 1})
		if err != nil {
			return err
		}
		return outbox.Enqueue(ctx, tx, outbox.Message{
			Topic: w.topic, Key: op.PaymentID.String(), Payload: payload,
			Delay: delay,
		})
	})
}

func (w *Worker) deadLetterOperation(ctx context.Context, m kafka.Message, op Operation, attempt int, f Failure) error {
	return w.fenced(ctx, op, attempt, StatusDeadLettered, &f, func(tx pgx.Tx) error {
		return w.sendToDLQ(ctx, tx, m, f.Message, op.ID.String(), attempt)
	})
}

// deadLetterRaw handles a message that cannot be tied to a processable operation.
func (w *Worker) deadLetterRaw(ctx context.Context, m kafka.Message, reason string) error {
	w.log.Warn("dead-lettering unprocessable message", "reason", reason, "partition", m.Partition, "offset", m.Offset)
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := w.sendToDLQ(ctx, tx, m, reason, "", 0); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (w *Worker) sendToDLQ(ctx context.Context, tx pgx.Tx, m kafka.Message, reason, opID string, attempts int) error {
	headers := map[string]string{
		"error":            reason,
		"attempts":         strconv.Itoa(attempts),
		"source_topic":     m.Topic,
		"source_partition": strconv.Itoa(m.Partition),
		"source_offset":    strconv.FormatInt(m.Offset, 10),
	}
	if opID != "" {
		headers["operation_id"] = opID
	}
	key := m.Key
	if key == "" {
		key = "unkeyed"
	}
	return outbox.Enqueue(ctx, tx, outbox.Message{
		Topic: DLQTopic(w.topic), Key: key, Payload: m.Value, Headers: headers})
}
