// Package ledger is the append-only double-entry record of money movement.
//
// Every business event (a capture, a refund) is one transaction made of debit and
// credit entries in integer paise. The Go code refuses to post an unbalanced
// transaction, and the database independently enforces balance (at commit) and
// append-only history with triggers.
//
// Accounts:
//
//	CUSTOMER_FUNDS         debited when money is collected, credited when refunded
//	MERCHANT_PAYABLE:<id>  what the platform owes the merchant
//	PLATFORM_FEES          the platform's fee revenue
//
// A capture of ₹1,000 with a 2% fee:
//
//	DEBIT  CUSTOMER_FUNDS        100000
//	CREDIT MERCHANT_PAYABLE:m1    98000
//	CREDIT PLATFORM_FEES           2000
package ledger

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

type Kind string

const (
	KindCapture Kind = "CAPTURE"
	KindRefund  Kind = "REFUND"
	// KindAdjustment is an operator correction of a reconciliation finding; its RefID
	// is the discrepancy ID, so each finding can be corrected at most once.
	KindAdjustment Kind = "ADJUSTMENT"
	// KindSettlement pays out what is owed to a merchant for one payment.
	KindSettlement Kind = "SETTLEMENT"
)

const (
	AccountCustomerFunds = "CUSTOMER_FUNDS"
	AccountPlatformFees  = "PLATFORM_FEES"
)

// MerchantPayable is the account for what is owed to one merchant.
func MerchantPayable(merchantID string) string { return "MERCHANT_PAYABLE:" + merchantID }

// MerchantPaidOut is the account recording what has been paid out to one merchant.
func MerchantPaidOut(merchantID string) string { return "MERCHANT_PAID_OUT:" + merchantID }

type Entry struct {
	Account     string
	Direction   Direction
	AmountPaise int64
}

// Txn is one balanced business event. RefID makes the event unique per Kind.
type Txn struct {
	PaymentID  uuid.UUID
	MerchantID string
	Kind       Kind
	RefID      string
	Entries    []Entry
}

// Validate checks the transaction is well-formed and balanced.
func (t Txn) Validate() error {
	if len(t.Entries) < 2 {
		return errors.New("ledger transaction needs at least two entries")
	}
	var debits, credits int64
	for _, e := range t.Entries {
		if e.AmountPaise <= 0 {
			return fmt.Errorf("ledger entry for %s has non-positive amount %d", e.Account, e.AmountPaise)
		}
		switch e.Direction {
		case Debit:
			debits += e.AmountPaise
		case Credit:
			credits += e.AmountPaise
		default:
			return fmt.Errorf("ledger entry for %s has invalid direction %q", e.Account, e.Direction)
		}
	}
	if debits != credits {
		return fmt.Errorf("ledger transaction is unbalanced: debits=%d credits=%d", debits, credits)
	}
	return nil
}

// Capture builds the entries for collecting amount, of which fee is the platform's.
func Capture(paymentID uuid.UUID, merchantID string, amount, fee int64) Txn {
	entries := []Entry{
		{AccountCustomerFunds, Debit, amount},
		{MerchantPayable(merchantID), Credit, amount - fee},
		{AccountPlatformFees, Credit, fee},
	}
	return Txn{paymentID, merchantID, KindCapture, paymentID.String(), nonZero(entries)}
}

// Refund builds the entries for returning amount to the customer, of which feeShare
// is taken back from the platform's fee and the rest from the merchant.
func Refund(paymentID uuid.UUID, merchantID, refundID string, amount, feeShare int64) Txn {
	entries := []Entry{
		{MerchantPayable(merchantID), Debit, amount - feeShare},
		{AccountPlatformFees, Debit, feeShare},
		{AccountCustomerFunds, Credit, amount},
	}
	return Txn{paymentID, merchantID, KindRefund, refundID, nonZero(entries)}
}

// Settlement moves amount from what is owed to the merchant to what has been paid out.
func Settlement(paymentID uuid.UUID, merchantID string, amount int64) Txn {
	return Txn{paymentID, merchantID, KindSettlement, paymentID.String(), []Entry{
		{MerchantPayable(merchantID), Debit, amount},
		{MerchantPaidOut(merchantID), Credit, amount},
	}}
}

// Adjustment builds a correcting transaction for the discrepancy with the given ID.
func Adjustment(paymentID uuid.UUID, merchantID, discrepancyID string, entries []Entry) Txn {
	return Txn{paymentID, merchantID, KindAdjustment, discrepancyID, entries}
}

func nonZero(entries []Entry) []Entry {
	out := entries[:0]
	for _, e := range entries {
		if e.AmountPaise != 0 {
			out = append(out, e)
		}
	}
	return out
}

// Post appends the transaction using the caller's database transaction, so the
// ledger rows commit or roll back together with the payment state change.
func Post(ctx context.Context, tx pgx.Tx, t Txn) error {
	if err := t.Validate(); err != nil {
		return err
	}

	codes := make([]string, 0, len(t.Entries))
	for _, e := range t.Entries {
		codes = append(codes, e.Account)
	}
	sort.Strings(codes) // stable order avoids lock-order deadlocks on shared accounts
	if _, err := tx.Exec(ctx,
		`INSERT INTO ledger_accounts (code) SELECT unnest($1::text[]) ON CONFLICT DO NOTHING`,
		codes); err != nil {
		return fmt.Errorf("ensure ledger accounts: %w", err)
	}

	txnID := uuid.New()
	if _, err := tx.Exec(ctx,
		`INSERT INTO ledger_transactions (id, payment_id, merchant_id, kind, ref_id)
		 VALUES ($1, $2, $3, $4, $5)`,
		txnID, t.PaymentID, t.MerchantID, t.Kind, t.RefID); err != nil {
		return fmt.Errorf("insert ledger transaction: %w", err)
	}
	for _, e := range t.Entries {
		if _, err := tx.Exec(ctx,
			`INSERT INTO ledger_entries (txn_id, account_code, direction, amount_paise)
			 VALUES ($1, $2, $3, $4)`,
			txnID, e.Account, e.Direction, e.AmountPaise); err != nil {
			return fmt.Errorf("insert ledger entry: %w", err)
		}
	}
	return nil
}
