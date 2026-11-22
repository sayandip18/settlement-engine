// Package processor defines the external payment processor boundary and a mock.
package processor

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Request identifies one operation against the processor. ReferenceID is a stable
// identifier a real processor would use for its own idempotency (the payment ID,
// or the refund ID for refunds).
type Request struct {
	PaymentID   uuid.UUID
	ReferenceID string
	MerchantID  string
	CustomerID  string
	AmountPaise int64
}

// Processor performs the money-moving calls. Implementations return:
//   - nil on success,
//   - *DeclineError for a permanent refusal (never retry),
//   - an error wrapping ErrTemporary for a transient failure (safe to retry).
type Processor interface {
	Authorize(ctx context.Context, r Request) error
	Capture(ctx context.Context, r Request) error
	Void(ctx context.Context, r Request) error
	Refund(ctx context.Context, r Request) error
}

// ErrTemporary marks a transient failure such as a timeout or brief outage.
var ErrTemporary = errors.New("processor temporarily unavailable")

// DeclineError is a permanent refusal, e.g. insufficient funds.
type DeclineError struct {
	Code    string
	Message string
}

func (e *DeclineError) Error() string { return e.Code + ": " + e.Message }

// AsDecline reports whether err is a permanent decline.
func AsDecline(err error) (*DeclineError, bool) {
	var d *DeclineError
	ok := errors.As(err, &d)
	return d, ok
}

// Mock is a deterministic processor for development and tests. Behaviour is chosen
// by a prefix on the customer ID:
//
//	decline_          authorize is declined (insufficient_funds)
//	capture_decline_  capture is declined
//	refund_decline_   refund is declined
//	timeout_          every operation fails with a temporary error
//
// Any other customer succeeds.
type Mock struct{}

func NewMock() Mock { return Mock{} }

func (Mock) Authorize(_ context.Context, r Request) error { return behave(r.CustomerID, "authorize") }
func (Mock) Capture(_ context.Context, r Request) error   { return behave(r.CustomerID, "capture") }
func (Mock) Void(_ context.Context, r Request) error      { return behave(r.CustomerID, "void") }
func (Mock) Refund(_ context.Context, r Request) error    { return behave(r.CustomerID, "refund") }

func behave(customer, op string) error {
	switch {
	case strings.HasPrefix(customer, "timeout_"):
		return fmt.Errorf("%w: mock timeout during %s", ErrTemporary, op)
	case op == "authorize" && strings.HasPrefix(customer, "decline_"):
		return &DeclineError{Code: "insufficient_funds", Message: "insufficient funds"}
	case op == "capture" && strings.HasPrefix(customer, "capture_decline_"):
		return &DeclineError{Code: "capture_declined", Message: "capture was declined"}
	case op == "refund" && strings.HasPrefix(customer, "refund_decline_"):
		return &DeclineError{Code: "refund_declined", Message: "refund was declined"}
	}
	return nil
}
