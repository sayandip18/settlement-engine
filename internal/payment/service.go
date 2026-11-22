package payment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Service struct {
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) *Service { return &Service{pool: pool} }

// CreateResult carries the payment plus whether it came from an earlier,
// identical request (an idempotent replay).
type CreateResult struct {
	Payment  Payment
	Replayed bool
}

// Create stores a new PENDING payment exactly once per (merchant, idempotency key).
//
// The key row is claimed with INSERT ... ON CONFLICT DO NOTHING inside the same
// transaction as the payment insert. A concurrent request with the same key blocks
// on the unique index until the first transaction finishes, then sees its result;
// if the first rolled back, the second proceeds and creates the payment.
func (s *Service) Create(ctx context.Context, in CreateInput) (CreateResult, error) {
	in.Normalize()
	if err := in.Validate(); err != nil {
		return CreateResult{}, err
	}
	hash := in.Hash()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return CreateResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx,
		`INSERT INTO idempotency_keys (merchant_id, idem_key, request_hash)
		 VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		in.MerchantID, in.IdempotencyKey, hash)
	if err != nil {
		return CreateResult{}, fmt.Errorf("claim idempotency key: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return replay(ctx, tx, in, hash)
	}

	p := Payment{
		ID:          uuid.New(),
		MerchantID:  in.MerchantID,
		CustomerID:  in.CustomerID,
		AmountPaise: in.AmountPaise,
		Currency:    in.Currency,
		Status:      StatusPending,
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO payments (id, merchant_id, customer_id, amount_paise, currency, status)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING created_at, updated_at`,
		p.ID, p.MerchantID, p.CustomerID, p.AmountPaise, p.Currency, p.Status,
	).Scan(&p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return CreateResult{}, fmt.Errorf("insert payment: %w", err)
	}

	body, err := json.Marshal(p)
	if err != nil {
		return CreateResult{}, fmt.Errorf("marshal payment: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE idempotency_keys
		    SET payment_id = $3, response_status = 201, response_body = $4
		  WHERE merchant_id = $1 AND idem_key = $2`,
		in.MerchantID, in.IdempotencyKey, p.ID, body); err != nil {
		return CreateResult{}, fmt.Errorf("record idempotent response: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return CreateResult{}, fmt.Errorf("commit: %w", err)
	}
	return CreateResult{Payment: p}, nil
}

func replay(ctx context.Context, tx pgx.Tx, in CreateInput, hash string) (CreateResult, error) {
	var storedHash string
	var body []byte
	err := tx.QueryRow(ctx,
		`SELECT request_hash, response_body FROM idempotency_keys
		  WHERE merchant_id = $1 AND idem_key = $2`,
		in.MerchantID, in.IdempotencyKey).Scan(&storedHash, &body)
	if err != nil {
		return CreateResult{}, fmt.Errorf("load idempotency key: %w", err)
	}
	if storedHash != hash {
		return CreateResult{}, ErrIdempotencyReused
	}
	var p Payment
	if err := json.Unmarshal(body, &p); err != nil {
		return CreateResult{}, fmt.Errorf("decode stored response: %w", err)
	}
	return CreateResult{Payment: p, Replayed: true}, nil
}

// Get returns the payment with its current status.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (Payment, error) {
	var p Payment
	err := s.pool.QueryRow(ctx,
		`SELECT id, merchant_id, customer_id, amount_paise, currency, status,
		        failure_reason, created_at, updated_at
		   FROM payments WHERE id = $1`, id,
	).Scan(&p.ID, &p.MerchantID, &p.CustomerID, &p.AmountPaise, &p.Currency,
		&p.Status, &p.FailureReason, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, ErrNotFound
	}
	if err != nil {
		return Payment{}, fmt.Errorf("get payment: %w", err)
	}
	return p, nil
}
