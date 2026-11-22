// Package payment implements payment creation, lookup and (later) lifecycle.
package payment

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

// CurrencyINR is the only supported currency. Amounts are integer paise.
const CurrencyINR = "INR"

const maxIdempotencyKeyLen = 255

type Status string

const (
	StatusPending           Status = "PENDING"
	StatusAuthorized        Status = "AUTHORIZED"
	StatusCaptured          Status = "CAPTURED"
	StatusSettled           Status = "SETTLED"
	StatusPartiallyRefunded Status = "PARTIALLY_REFUNDED"
	StatusRefunded          Status = "REFUNDED"
	StatusVoided            Status = "VOIDED"
	StatusFailed            Status = "FAILED"
)

type Payment struct {
	ID            uuid.UUID  `json:"id"`
	MerchantID    string     `json:"merchant_id"`
	CustomerID    string     `json:"customer_id"`
	AmountPaise   int64      `json:"amount_paise"`
	Currency      string     `json:"currency"`
	Status        Status     `json:"status"`
	FailureReason *string    `json:"failure_reason,omitempty"`
	RefundedPaise int64      `json:"refunded_paise"`
	CapturedAt    *time.Time `json:"captured_at,omitempty"`
	Version       int        `json:"-"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

var (
	ErrNotFound          = errors.New("payment not found")
	ErrIdempotencyReused = errors.New("idempotency key reused with different request")
)

// ValidationError describes a client-correctable input problem.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

type CreateInput struct {
	MerchantID     string `json:"merchant_id"`
	CustomerID     string `json:"customer_id"`
	AmountPaise    int64  `json:"amount_paise"`
	Currency       string `json:"currency"`
	IdempotencyKey string `json:"-"`
}

// Normalize trims input and applies defaults.
func (in *CreateInput) Normalize() {
	in.MerchantID = strings.TrimSpace(in.MerchantID)
	in.CustomerID = strings.TrimSpace(in.CustomerID)
	in.IdempotencyKey = strings.TrimSpace(in.IdempotencyKey)
	in.Currency = strings.ToUpper(strings.TrimSpace(in.Currency))
	if in.Currency == "" {
		in.Currency = CurrencyINR
	}
}

func (in CreateInput) Validate() error {
	switch {
	case in.IdempotencyKey == "":
		return &ValidationError{"Idempotency-Key header is required"}
	case len(in.IdempotencyKey) > maxIdempotencyKeyLen:
		return &ValidationError{"Idempotency-Key must be at most 255 characters"}
	case in.MerchantID == "":
		return &ValidationError{"merchant_id is required"}
	case in.CustomerID == "":
		return &ValidationError{"customer_id is required"}
	case in.AmountPaise <= 0:
		return &ValidationError{"amount_paise must be greater than zero"}
	case in.Currency != CurrencyINR:
		return &ValidationError{"only INR is supported"}
	}
	return nil
}

// Hash fingerprints the payment details (not the key) so a reused
// idempotency key with a different body can be detected.
func (in CreateInput) Hash() string {
	b, _ := json.Marshal(struct {
		MerchantID  string `json:"m"`
		CustomerID  string `json:"c"`
		AmountPaise int64  `json:"a"`
		Currency    string `json:"cur"`
	}{in.MerchantID, in.CustomerID, in.AmountPaise, in.Currency})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
