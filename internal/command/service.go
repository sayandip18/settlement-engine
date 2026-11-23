// Package command turns API requests (authorize, capture, void, refund) into
// asynchronous operations: the request is validated and recorded together with an
// outbox message in one transaction, and a worker later runs it against the
// processor. Clients poll the operation for the outcome.
package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sayandip18/settlement-engine/internal/outbox"
	"github.com/sayandip18/settlement-engine/internal/payment"
)

type Kind string

const (
	KindAuthorize Kind = "AUTHORIZE"
	KindCapture   Kind = "CAPTURE"
	KindVoid      Kind = "VOID"
	KindRefund    Kind = "REFUND"
)

func (k Kind) action() string { return strings.ToLower(string(k)) }

// target is the payment status an action moves to (refunds decide theirs later).
func (k Kind) target() payment.Status {
	switch k {
	case KindAuthorize:
		return payment.StatusAuthorized
	case KindCapture:
		return payment.StatusCaptured
	case KindVoid:
		return payment.StatusVoided
	}
	return ""
}

type Status string

const (
	StatusPending      Status = "PENDING"
	StatusSucceeded    Status = "SUCCEEDED"
	StatusFailed       Status = "FAILED"
	StatusDeadLettered Status = "DEAD_LETTERED"
)

// Terminal reports whether the operation will not change any more.
func (s Status) Terminal() bool { return s != StatusPending }

// Failure explains why an operation failed, or the latest transient error while
// it is still being retried.
type Failure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Operation struct {
	ID             uuid.UUID `json:"id"`
	PaymentID      uuid.UUID `json:"payment_id"`
	Kind           Kind      `json:"kind"`
	Status         Status    `json:"status"`
	Attempts       int       `json:"attempts"`
	RequestedPaise *int64    `json:"requested_paise,omitempty"`
	Error          *Failure  `json:"error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`

	idemKey *string
}

var ErrOperationNotFound = errors.New("operation not found")

const opCols = `id, payment_id, kind, status, attempts, requested_paise, idem_key,
	error_code, error_message, created_at, updated_at`

func scanOperation(row pgx.Row) (Operation, error) {
	var (
		o        Operation
		code, em *string
	)
	err := row.Scan(&o.ID, &o.PaymentID, &o.Kind, &o.Status, &o.Attempts, &o.RequestedPaise,
		&o.idemKey, &code, &em, &o.CreatedAt, &o.UpdatedAt)
	if code != nil {
		o.Error = &Failure{Code: *code}
		if em != nil {
			o.Error.Message = *em
		}
	}
	return o, err
}

// message is the queue payload. It carries only references; the operation row is the
// source of truth for what to do.
type message struct {
	OperationID uuid.UUID `json:"operation_id"`
	Attempt     int       `json:"attempt"`
}

type Service struct {
	pool     *pgxpool.Pool
	payments *payment.Service
	topic    string
}

func NewService(pool *pgxpool.Pool, payments *payment.Service, commandsTopic string) *Service {
	return &Service{pool: pool, payments: payments, topic: commandsTopic}
}

type SubmitInput struct {
	PaymentID      uuid.UUID
	Kind           Kind
	AmountPaise    *int64 // refunds only; nil refunds whatever remains
	IdempotencyKey string // refunds only; required
}

type SubmitResult struct {
	Operation Operation
	Replayed  bool
}

// Submit validates the request against the payment's current status (a cheap early
// rejection; the worker re-checks authoritatively), then records the operation and
// its outbox message atomically.
func (s *Service) Submit(ctx context.Context, in SubmitInput) (SubmitResult, error) {
	var idemKey *string
	if in.Kind == KindRefund {
		key := strings.TrimSpace(in.IdempotencyKey)
		switch {
		case key == "":
			return SubmitResult{}, &payment.ValidationError{Msg: "Idempotency-Key header is required"}
		case len(key) > 255:
			return SubmitResult{}, &payment.ValidationError{Msg: "Idempotency-Key must be at most 255 characters"}
		case in.AmountPaise != nil && *in.AmountPaise <= 0:
			return SubmitResult{}, &payment.ValidationError{Msg: "amount_paise must be greater than zero"}
		}
		idemKey = &key
	} else {
		in.AmountPaise = nil
	}

	// A retried refund request is answered from the stored operation, whatever the
	// payment's status is by now.
	if idemKey != nil {
		if existing, ok, err := s.byKey(ctx, s.pool, in.PaymentID, *idemKey); err != nil {
			return SubmitResult{}, err
		} else if ok {
			return replay(existing, in.AmountPaise)
		}
	}

	p, err := s.payments.Get(ctx, in.PaymentID)
	if err != nil {
		return SubmitResult{}, err
	}
	if err := precheck(p, in); err != nil {
		return SubmitResult{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SubmitResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	op := Operation{ID: uuid.New(), PaymentID: in.PaymentID, Kind: in.Kind, Status: StatusPending,
		RequestedPaise: in.AmountPaise, idemKey: idemKey}
	err = tx.QueryRow(ctx,
		`INSERT INTO operations (id, payment_id, kind, requested_paise, idem_key)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (payment_id, idem_key) WHERE idem_key IS NOT NULL DO NOTHING
		 RETURNING created_at, updated_at`,
		op.ID, op.PaymentID, op.Kind, op.RequestedPaise, op.idemKey,
	).Scan(&op.CreatedAt, &op.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// Lost a race with an identical concurrent request.
		existing, ok, lerr := s.byKey(ctx, tx, in.PaymentID, *idemKey)
		if lerr != nil {
			return SubmitResult{}, fmt.Errorf("reload racing operation: %w", lerr)
		}
		if !ok {
			return SubmitResult{}, errors.New("operation insert conflicted but no existing operation found")
		}
		return replay(existing, in.AmountPaise)
	}
	if err != nil {
		return SubmitResult{}, fmt.Errorf("insert operation: %w", err)
	}

	if err := s.enqueue(ctx, tx, op.ID, 1, in.PaymentID.String()); err != nil {
		return SubmitResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SubmitResult{}, fmt.Errorf("commit: %w", err)
	}
	return SubmitResult{Operation: op}, nil
}

func (s *Service) enqueue(ctx context.Context, tx pgx.Tx, opID uuid.UUID, attempt int, key string) error {
	payload, err := json.Marshal(message{OperationID: opID, Attempt: attempt})
	if err != nil {
		return err
	}
	return outbox.Enqueue(ctx, tx, outbox.Message{Topic: s.topic, Key: key, Payload: payload})
}

func replay(existing Operation, requested *int64) (SubmitResult, error) {
	same := (existing.RequestedPaise == nil && requested == nil) ||
		(existing.RequestedPaise != nil && requested != nil && *existing.RequestedPaise == *requested)
	if !same {
		return SubmitResult{}, payment.ErrIdempotencyReused
	}
	return SubmitResult{Operation: existing, Replayed: true}, nil
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *Service) byKey(ctx context.Context, q queryRower, paymentID uuid.UUID, key string) (Operation, bool, error) {
	o, err := scanOperation(q.QueryRow(ctx,
		`SELECT `+opCols+` FROM operations WHERE payment_id = $1 AND idem_key = $2`, paymentID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, false, nil
	}
	return o, err == nil, err
}

// Get returns an operation by ID.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (Operation, error) {
	o, err := scanOperation(s.pool.QueryRow(ctx, `SELECT `+opCols+` FROM operations WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, ErrOperationNotFound
	}
	if err != nil {
		return Operation{}, fmt.Errorf("get operation: %w", err)
	}
	return o, nil
}

// precheck rejects requests that cannot possibly succeed given the payment's
// current status. It is advisory: the status may still change before the worker runs.
func precheck(p payment.Payment, in SubmitInput) error {
	if in.Kind == KindRefund {
		if !payment.CanRefund(p.Status) {
			return &payment.InvalidTransitionError{Action: in.Kind.action(), From: p.Status}
		}
		if remaining := p.AmountPaise - p.RefundedPaise; in.AmountPaise != nil && *in.AmountPaise > remaining {
			return &payment.RefundExceedsError{RemainingPaise: remaining}
		}
		return nil
	}
	target := in.Kind.target()
	if p.Status != target && !payment.CanTransition(p.Status, target) {
		return &payment.InvalidTransitionError{Action: in.Kind.action(), From: p.Status}
	}
	return nil
}

// NotRequeueableError means the operation is not dead-lettered, so there is nothing to retry.
type NotRequeueableError struct{ Status Status }

func (e *NotRequeueableError) Error() string {
	return "operation is " + string(e.Status) + ", not " + string(StatusDeadLettered)
}

// Requeue gives a dead-lettered operation a fresh start inside the caller's transaction:
// it goes back to PENDING with its attempt counter reset, and a new first-attempt message
// is queued. The old attempt-N message cannot be reused because the worker would skip it
// as already handled. Used by dead-letter replay.
func (s *Service) Requeue(ctx context.Context, tx pgx.Tx, opID uuid.UUID) error {
	var paymentID uuid.UUID
	err := tx.QueryRow(ctx,
		`UPDATE operations
		    SET status = 'PENDING', attempts = 0, error_code = NULL, error_message = NULL, updated_at = now()
		  WHERE id = $1 AND status = 'DEAD_LETTERED'
		  RETURNING payment_id`, opID).Scan(&paymentID)
	if errors.Is(err, pgx.ErrNoRows) {
		var status Status
		serr := tx.QueryRow(ctx, `SELECT status FROM operations WHERE id = $1`, opID).Scan(&status)
		if errors.Is(serr, pgx.ErrNoRows) {
			return ErrOperationNotFound
		}
		if serr != nil {
			return fmt.Errorf("load operation: %w", serr)
		}
		return &NotRequeueableError{Status: status}
	}
	if err != nil {
		return fmt.Errorf("reset operation: %w", err)
	}
	return s.enqueue(ctx, tx, opID, 1, paymentID.String())
}
