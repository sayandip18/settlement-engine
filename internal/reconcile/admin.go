package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/sayandip18/settlement-engine/internal/ledger"
)

// Discrepancy is one finding.
type Discrepancy struct {
	ID             uuid.UUID       `json:"id"`
	Type           Type            `json:"type"`
	Status         Status          `json:"status"`
	Subject        string          `json:"subject"`
	PaymentID      *uuid.UUID      `json:"payment_id,omitempty"`
	Details        json.RawMessage `json:"details"`
	SeenCount      int             `json:"seen_count"`
	DetectedAt     time.Time       `json:"detected_at"`
	LastSeenAt     time.Time       `json:"last_seen_at"`
	AcknowledgedAt *time.Time      `json:"acknowledged_at,omitempty"`
	AckNote        *string         `json:"ack_note,omitempty"`
	ResolvedAt     *time.Time      `json:"resolved_at,omitempty"`
	Resolution     *string         `json:"resolution,omitempty"` // AUTO, OPERATOR or CORRECTED
	ResolutionNote *string         `json:"resolution_note,omitempty"`
	LedgerTxnID    *uuid.UUID      `json:"ledger_txn_id,omitempty"`

	seq int64
}

var (
	ErrNotFound = errors.New("discrepancy not found")
	// ErrConflict means another correction for the same thing got there first.
	ErrConflict = errors.New("a conflicting ledger entry already exists")
)

// AlreadyHandledError: the finding is not in a state the action applies to.
type AlreadyHandledError struct{ Status Status }

func (e *AlreadyHandledError) Error() string { return "discrepancy is already " + string(e.Status) }

type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

// NotCorrectableError: the finding type cannot be fixed with a ledger correction.
type NotCorrectableError struct{ Reason string }

func (e *NotCorrectableError) Error() string { return e.Reason }

// CorrectionRejectedError: the proposed correction would not make the ledger agree
// with the payment, so nothing was posted.
type CorrectionRejectedError struct{ Reason string }

func (e *CorrectionRejectedError) Error() string { return e.Reason }

const discCols = `id, seq, type, status, subject, payment_id, details, seen_count, detected_at, last_seen_at,
	acknowledged_at, ack_note, resolved_at, resolution, resolution_note, ledger_txn_id`

func scanDiscrepancy(row pgx.Row) (Discrepancy, error) {
	var d Discrepancy
	err := row.Scan(&d.ID, &d.seq, &d.Type, &d.Status, &d.Subject, &d.PaymentID, &d.Details, &d.SeenCount,
		&d.DetectedAt, &d.LastSeenAt, &d.AcknowledgedAt, &d.AckNote, &d.ResolvedAt, &d.Resolution,
		&d.ResolutionNote, &d.LedgerTxnID)
	return d, err
}

type ListFilter struct {
	Status *Status
	Type   *Type
	Limit  int   // 1..MaxLimit; 0 = DefaultLimit
	Before int64 // cursor; 0 = newest
}

const (
	DefaultLimit = 50
	MaxLimit     = 200
	maxNoteLen   = 2000
)

// List returns findings newest first plus a cursor for the next page (0 when done).
func (s *Service) List(ctx context.Context, f ListFilter) ([]Discrepancy, int64, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultLimit
	}
	var status, typ *string
	if f.Status != nil {
		v := string(*f.Status)
		status = &v
	}
	if f.Type != nil {
		v := string(*f.Type)
		typ = &v
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+discCols+` FROM discrepancies
		  WHERE ($1::text IS NULL OR status = $1) AND ($2::text IS NULL OR type = $2)
		    AND ($3::bigint = 0 OR seq < $3)
		  ORDER BY seq DESC LIMIT $4`, status, typ, f.Before, limit+1)
	if err != nil {
		return nil, 0, fmt.Errorf("list discrepancies: %w", err)
	}
	defer rows.Close()
	var out []Discrepancy
	for rows.Next() {
		d, err := scanDiscrepancy(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan discrepancy: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	var next int64
	if len(out) > limit {
		out = out[:limit]
		next = out[limit-1].seq
	}
	return out, next, nil
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *Service) load(ctx context.Context, q queryRower, id uuid.UUID, lock bool) (Discrepancy, error) {
	sql := `SELECT ` + discCols + ` FROM discrepancies WHERE id = $1`
	if lock {
		sql += ` FOR UPDATE`
	}
	d, err := scanDiscrepancy(q.QueryRow(ctx, sql, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Discrepancy{}, ErrNotFound
	}
	if err != nil {
		return Discrepancy{}, fmt.Errorf("load discrepancy: %w", err)
	}
	return d, nil
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (Discrepancy, error) {
	return s.load(ctx, s.pool, id, false)
}

// Acknowledge marks an OPEN finding as seen and being looked at. It stays in place:
// later runs keep refreshing it, and it still auto-resolves if the condition clears.
func (s *Service) Acknowledge(ctx context.Context, id uuid.UUID, note string) (Discrepancy, error) {
	if err := checkNote(note, false); err != nil {
		return Discrepancy{}, err
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE discrepancies SET status = 'ACKNOWLEDGED', acknowledged_at = now(), ack_note = NULLIF($2, '')
		  WHERE id = $1 AND status = 'OPEN'`, id, strings.TrimSpace(note))
	if err != nil {
		return Discrepancy{}, fmt.Errorf("acknowledge discrepancy: %w", err)
	}
	return s.afterTransition(ctx, id, tag.RowsAffected())
}

// Resolve closes a finding as handled by an operator. A note is required: it is the
// audit trail of what was done. Resolving does not touch money; if the underlying
// condition persists, the next run opens a new finding.
func (s *Service) Resolve(ctx context.Context, id uuid.UUID, note string) (Discrepancy, error) {
	if err := checkNote(note, true); err != nil {
		return Discrepancy{}, err
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE discrepancies SET status = 'RESOLVED', resolution = 'OPERATOR', resolved_at = now(), resolution_note = $2
		  WHERE id = $1 AND status <> 'RESOLVED'`, id, strings.TrimSpace(note))
	if err != nil {
		return Discrepancy{}, fmt.Errorf("resolve discrepancy: %w", err)
	}
	return s.afterTransition(ctx, id, tag.RowsAffected())
}

func (s *Service) afterTransition(ctx context.Context, id uuid.UUID, affected int64) (Discrepancy, error) {
	d, err := s.load(ctx, s.pool, id, false)
	if err != nil {
		return Discrepancy{}, err
	}
	if affected == 0 {
		return Discrepancy{}, &AlreadyHandledError{Status: d.Status}
	}
	return d, nil
}

func checkNote(note string, required bool) error {
	note = strings.TrimSpace(note)
	if required && note == "" {
		return &ValidationError{"note is required"}
	}
	if len(note) > maxNoteLen {
		return &ValidationError{fmt.Sprintf("note must be at most %d characters", maxNoteLen)}
	}
	return nil
}

// CorrectionEntry is one line of an operator-supplied correcting transaction.
type CorrectionEntry struct {
	Account     string           `json:"account"`
	Direction   ledger.Direction `json:"direction"`
	AmountPaise int64            `json:"amount_paise"`
}

// Correct fixes a ledger finding by appending a new ledger transaction and resolving
// the finding with a link to it. History is never edited.
//
//   - CAPTURE_LEDGER_MISSING: no entries are accepted. The normal CAPTURE transaction is
//     re-posted from the payment's amount and frozen fee, so nobody hand-computes paise.
//   - CUSTOMER_FUNDS_MISMATCH and LEDGER_ON_UNCAPTURED_PAYMENT: the operator supplies the
//     entries, posted as an ADJUSTMENT whose ref is this finding's ID (so each finding
//     can be corrected once).
//
// Everything happens in one database transaction with the finding and the payment row
// locked (serialising against capture/refund). After posting, the payment's ledger is
// re-checked in that same transaction; if it still disagrees with the payment, the
// transaction rolls back and nothing is written.
func (s *Service) Correct(ctx context.Context, id uuid.UUID, note string, entries []CorrectionEntry) (Discrepancy, error) {
	if err := checkNote(note, true); err != nil {
		return Discrepancy{}, err
	}
	note = strings.TrimSpace(note)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Discrepancy{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	d, err := s.load(ctx, tx, id, true)
	if err != nil {
		return Discrepancy{}, err
	}
	if d.Status == StatusResolved {
		return Discrepancy{}, &AlreadyHandledError{Status: d.Status}
	}
	switch d.Type {
	case TypeCaptureLedgerMissing:
		if len(entries) != 0 {
			return Discrepancy{}, &ValidationError{"entries are not accepted for " + string(d.Type) + "; the capture is re-posted from the payment"}
		}
	case TypeCustomerFundsMismatch, TypeLedgerOnUncaptured:
		if len(entries) == 0 {
			return Discrepancy{}, &ValidationError{"entries are required for " + string(d.Type)}
		}
	default:
		return Discrepancy{}, &NotCorrectableError{"a " + string(d.Type) + " finding cannot be corrected with a ledger entry; resolve it with a note once handled"}
	}
	if d.PaymentID == nil {
		return Discrepancy{}, &NotCorrectableError{"the finding has no payment"}
	}

	var (
		merchant              string
		status                string
		amount, refunded, fee int64
	)
	err = tx.QueryRow(ctx,
		`SELECT merchant_id, status, amount_paise, refunded_paise, fee_paise
		   FROM payments WHERE id = $1 FOR UPDATE`, *d.PaymentID).Scan(&merchant, &status, &amount, &refunded, &fee)
	if err != nil {
		return Discrepancy{}, fmt.Errorf("lock payment: %w", err)
	}

	var txn ledger.Txn
	if d.Type == TypeCaptureLedgerMissing {
		txn = ledger.Capture(*d.PaymentID, merchant, amount, fee)
	} else {
		le, err := toLedgerEntries(entries, merchant)
		if err != nil {
			return Discrepancy{}, err
		}
		txn = ledger.Adjustment(*d.PaymentID, merchant, d.ID.String(), le)
	}
	if err := ledger.Post(ctx, tx, txn); err != nil {
		var pgErr *pgconn.PgError
		switch {
		case errors.As(err, &pgErr) && pgErr.Code == "23505":
			return Discrepancy{}, ErrConflict
		case errors.As(err, &pgErr):
			return Discrepancy{}, err
		default:
			return Discrepancy{}, &ValidationError{err.Error()} // ledger.Validate: unbalanced, bad amount, ...
		}
	}

	if err := verifyFixed(ctx, tx, *d.PaymentID, status, amount, refunded); err != nil {
		return Discrepancy{}, err
	}

	var txnID uuid.UUID
	if err := tx.QueryRow(ctx,
		`SELECT id FROM ledger_transactions WHERE kind = $1 AND ref_id = $2`, txn.Kind, txn.RefID).Scan(&txnID); err != nil {
		return Discrepancy{}, fmt.Errorf("find posted transaction: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE discrepancies
		    SET status = 'RESOLVED', resolution = 'CORRECTED', resolved_at = now(),
		        resolution_note = $2, ledger_txn_id = $3
		  WHERE id = $1`, id, note, txnID); err != nil {
		return Discrepancy{}, fmt.Errorf("resolve discrepancy: %w", err)
	}
	d, err = s.load(ctx, tx, id, false)
	if err != nil {
		return Discrepancy{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Discrepancy{}, fmt.Errorf("commit: %w", err)
	}
	return d, nil
}

func toLedgerEntries(in []CorrectionEntry, merchantID string) ([]ledger.Entry, error) {
	allowed := map[string]bool{
		ledger.AccountCustomerFunds:        true,
		ledger.AccountPlatformFees:         true,
		ledger.MerchantPayable(merchantID): true,
	}
	out := make([]ledger.Entry, 0, len(in))
	for _, e := range in {
		if !allowed[e.Account] {
			return nil, &ValidationError{fmt.Sprintf("account %q is not allowed for this payment; use CUSTOMER_FUNDS, PLATFORM_FEES or %s",
				e.Account, ledger.MerchantPayable(merchantID))}
		}
		if e.Direction != ledger.Debit && e.Direction != ledger.Credit {
			return nil, &ValidationError{"direction must be DEBIT or CREDIT"}
		}
		out = append(out, ledger.Entry{Account: e.Account, Direction: e.Direction, AmountPaise: e.AmountPaise})
	}
	return out, nil
}

// verifyFixed checks, inside the correcting transaction, that the payment's ledger now
// agrees with its state: CUSTOMER_FUNDS nets to captured minus refunded, and a payment
// that never collected money has no balance on any account.
func verifyFixed(ctx context.Context, tx pgx.Tx, paymentID uuid.UUID, status string, amount, refunded int64) error {
	rows, err := tx.Query(ctx,
		`SELECT e.account_code,
		        SUM(CASE e.direction WHEN 'DEBIT' THEN e.amount_paise ELSE -e.amount_paise END)
		   FROM ledger_transactions t JOIN ledger_entries e ON e.txn_id = t.id
		  WHERE t.payment_id = $1 GROUP BY e.account_code`, paymentID)
	if err != nil {
		return fmt.Errorf("verify correction: %w", err)
	}
	defer rows.Close()
	nets := map[string]int64{}
	for rows.Next() {
		var acct string
		var net int64
		if err := rows.Scan(&acct, &net); err != nil {
			return fmt.Errorf("verify correction: %w", err)
		}
		nets[acct] = net
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("verify correction: %w", err)
	}

	switch status {
	case "CAPTURED", "SETTLED", "PARTIALLY_REFUNDED", "REFUNDED":
		if want := amount - refunded; nets[ledger.AccountCustomerFunds] != want {
			return &CorrectionRejectedError{fmt.Sprintf(
				"after this correction CUSTOMER_FUNDS would net %d paise but the payment's captured minus refunded is %d; nothing was posted",
				nets[ledger.AccountCustomerFunds], want)}
		}
	default:
		for acct, net := range nets {
			if net != 0 {
				return &CorrectionRejectedError{fmt.Sprintf(
					"after this correction %s would still net %d paise on a payment in status %s; nothing was posted", acct, net, status)}
			}
		}
	}
	return nil
}
