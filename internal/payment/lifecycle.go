package payment

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/sayandip18/settlement-engine/internal/processor"
)

var ErrProcessorUnavailable = errors.New("payment processor temporarily unavailable")

// DeclinedError means the processor permanently refused an operation. Payment is
// the state after the attempt: FAILED for authorize/capture declines, unchanged
// for void/refund declines.
type DeclinedError struct {
	Code    string
	Message string
	Payment Payment
}

func (e *DeclinedError) Error() string { return e.Code + ": " + e.Message }

// RefundExceedsError is returned when a refund is larger than what is left to refund.
type RefundExceedsError struct{ RemainingPaise int64 }

func (e *RefundExceedsError) Error() string {
	return fmt.Sprintf("refund exceeds refundable amount; %d paise remaining", e.RemainingPaise)
}

// commitThen makes mutate commit the transaction before returning err. It lets a
// permanent decline persist the FAILED status while still reporting the error.
type commitThen struct{ err error }

func (c *commitThen) Error() string { return c.err.Error() }
func (c *commitThen) Unwrap() error { return c.err }

// mutate runs fn against the payment row locked FOR UPDATE, so concurrent actions
// on one payment are serialized and always see each other's committed result.
// It returns the payment as fn left it (even when fn fails).
//
// The processor is called while the row lock is held. That is fine for the
// synchronous mock; Phase 4 moves processor calls to async workers.
func (s *Service) mutate(ctx context.Context, id uuid.UUID, fn func(tx pgx.Tx, p *Payment) error) (Payment, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Payment{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	p, err := scanPayment(tx.QueryRow(ctx,
		`SELECT `+paymentCols+` FROM payments WHERE id = $1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, ErrNotFound
	}
	if err != nil {
		return Payment{}, fmt.Errorf("lock payment: %w", err)
	}

	if err := fn(tx, &p); err != nil {
		var ct *commitThen
		if !errors.As(err, &ct) {
			return p, err
		}
		if cerr := tx.Commit(ctx); cerr != nil {
			return p, fmt.Errorf("commit: %w", cerr)
		}
		return p, ct.err
	}
	if err := tx.Commit(ctx); err != nil {
		return p, fmt.Errorf("commit: %w", err)
	}
	return p, nil
}

// save persists the mutable fields of p and bumps its version.
func save(ctx context.Context, tx pgx.Tx, p *Payment) error {
	err := tx.QueryRow(ctx,
		`UPDATE payments
		    SET status = $2, failure_reason = $3, refunded_paise = $4,
		        captured_at = $5, version = version + 1, updated_at = now()
		  WHERE id = $1 AND version = $6
		  RETURNING version, updated_at`,
		p.ID, p.Status, p.FailureReason, p.RefundedPaise, p.CapturedAt, p.Version,
	).Scan(&p.Version, &p.UpdatedAt)
	if err != nil {
		return fmt.Errorf("save payment: %w", err)
	}
	return nil
}

type step struct {
	action string
	target Status
	call   func(ctx context.Context, r processor.Request) error
	// failOnDecline moves the payment to FAILED on a permanent decline.
	// Otherwise a decline leaves the payment untouched.
	failOnDecline bool
}

func (s *Service) advance(ctx context.Context, id uuid.UUID, st step) (Payment, error) {
	return s.mutate(ctx, id, func(tx pgx.Tx, p *Payment) error {
		if p.Status == st.target {
			return nil // repeat of an action that already happened: no-op
		}
		if !CanTransition(p.Status, st.target) {
			return &InvalidTransitionError{Action: st.action, From: p.Status}
		}

		err := st.call(ctx, processor.Request{
			PaymentID:   p.ID,
			ReferenceID: p.ID.String(),
			MerchantID:  p.MerchantID,
			CustomerID:  p.CustomerID,
			AmountPaise: p.AmountPaise,
		})
		if d, ok := processor.AsDecline(err); ok {
			declined := &DeclinedError{Code: d.Code, Message: d.Message}
			if !st.failOnDecline {
				declined.Payment = *p
				return declined
			}
			reason := d.Message
			p.Status, p.FailureReason = StatusFailed, &reason
			if err := save(ctx, tx, p); err != nil {
				return err
			}
			declined.Payment = *p
			return &commitThen{err: declined}
		}
		if err != nil {
			return fmt.Errorf("%w: %v", ErrProcessorUnavailable, err)
		}

		p.Status = st.target
		if st.target == StatusCaptured {
			now := time.Now().UTC()
			p.CapturedAt = &now
		}
		return save(ctx, tx, p)
	})
}

// Authorize places a hold on the customer's funds (PENDING -> AUTHORIZED).
// A permanent decline moves the payment to FAILED.
func (s *Service) Authorize(ctx context.Context, id uuid.UUID) (Payment, error) {
	return s.advance(ctx, id, step{"authorize", StatusAuthorized, s.proc.Authorize, true})
}

// Capture collects the held funds (AUTHORIZED -> CAPTURED).
func (s *Service) Capture(ctx context.Context, id uuid.UUID) (Payment, error) {
	return s.advance(ctx, id, step{"capture", StatusCaptured, s.proc.Capture, true})
}

// Void releases a hold that was never captured (AUTHORIZED -> VOIDED).
func (s *Service) Void(ctx context.Context, id uuid.UUID) (Payment, error) {
	return s.advance(ctx, id, step{"void", StatusVoided, s.proc.Void, false})
}

// Refund is a successful refund of part or all of a captured payment.
type Refund struct {
	ID          uuid.UUID `json:"id"`
	PaymentID   uuid.UUID `json:"payment_id"`
	AmountPaise int64     `json:"amount_paise"`
	CreatedAt   time.Time `json:"created_at"`
}

// RefundInput asks to refund AmountPaise, or whatever remains when it is nil.
type RefundInput struct {
	PaymentID      uuid.UUID
	AmountPaise    *int64
	IdempotencyKey string
}

type RefundResult struct {
	Refund   Refund
	Payment  Payment
	Replayed bool
}

// Refund returns money from a CAPTURED, SETTLED or PARTIALLY_REFUNDED payment.
// The idempotency key is scoped to the payment: a retry returns the original
// refund, while a retry with a different amount is rejected.
func (s *Service) Refund(ctx context.Context, in RefundInput) (RefundResult, error) {
	if in.IdempotencyKey == "" {
		return RefundResult{}, &ValidationError{"Idempotency-Key header is required"}
	}
	if len(in.IdempotencyKey) > maxIdempotencyKeyLen {
		return RefundResult{}, &ValidationError{"Idempotency-Key must be at most 255 characters"}
	}
	if in.AmountPaise != nil && *in.AmountPaise <= 0 {
		return RefundResult{}, &ValidationError{"amount_paise must be greater than zero"}
	}

	var res RefundResult
	p, err := s.mutate(ctx, in.PaymentID, func(tx pgx.Tx, p *Payment) error {
		// A retry is answered from the stored refund, whatever the payment's status is now.
		var existing Refund
		var requested *int64
		err := tx.QueryRow(ctx,
			`SELECT id, payment_id, amount_paise, requested_paise, created_at
			   FROM refunds WHERE payment_id = $1 AND idem_key = $2`,
			p.ID, in.IdempotencyKey,
		).Scan(&existing.ID, &existing.PaymentID, &existing.AmountPaise, &requested, &existing.CreatedAt)
		if err == nil {
			if !sameRequest(requested, in.AmountPaise) {
				return ErrIdempotencyReused
			}
			res = RefundResult{Refund: existing, Replayed: true}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("load refund: %w", err)
		}

		if p.Status != StatusCaptured && p.Status != StatusSettled && p.Status != StatusPartiallyRefunded {
			return &InvalidTransitionError{Action: "refund", From: p.Status}
		}
		remaining := p.AmountPaise - p.RefundedPaise
		amount := remaining
		if in.AmountPaise != nil {
			amount = *in.AmountPaise
		}
		if amount > remaining {
			return &RefundExceedsError{RemainingPaise: remaining}
		}

		refund := Refund{ID: uuid.New(), PaymentID: p.ID, AmountPaise: amount}
		err = s.proc.Refund(ctx, processor.Request{
			PaymentID:   p.ID,
			ReferenceID: refund.ID.String(),
			MerchantID:  p.MerchantID,
			CustomerID:  p.CustomerID,
			AmountPaise: amount,
		})
		if d, ok := processor.AsDecline(err); ok {
			return &DeclinedError{Code: d.Code, Message: d.Message, Payment: *p}
		}
		if err != nil {
			return fmt.Errorf("%w: %v", ErrProcessorUnavailable, err)
		}

		if err := tx.QueryRow(ctx,
			`INSERT INTO refunds (id, payment_id, idem_key, requested_paise, amount_paise)
			 VALUES ($1, $2, $3, $4, $5) RETURNING created_at`,
			refund.ID, p.ID, in.IdempotencyKey, in.AmountPaise, amount,
		).Scan(&refund.CreatedAt); err != nil {
			return fmt.Errorf("insert refund: %w", err)
		}

		p.RefundedPaise += amount
		if p.RefundedPaise == p.AmountPaise {
			p.Status = StatusRefunded
		} else {
			p.Status = StatusPartiallyRefunded
		}
		if err := save(ctx, tx, p); err != nil {
			return err
		}
		res = RefundResult{Refund: refund}
		return nil
	})
	if err != nil {
		return RefundResult{}, err
	}
	res.Payment = p
	return res, nil
}

func sameRequest(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
