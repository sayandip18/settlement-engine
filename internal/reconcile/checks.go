package reconcile

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Statuses in which money has been collected (and possibly partly returned).
const capturedStatuses = `('CAPTURED', 'SETTLED', 'PARTIALLY_REFUNDED', 'REFUNDED')`

// Statuses in which no money should have moved, so no ledger activity is expected.
const uncapturedStatuses = `('PENDING', 'AUTHORIZED', 'FAILED', 'VOIDED')`

// fetchLimit is one more than the cap so a truncated result can be recognised.
const fetchLimit = maxFindingsPerCheck + 1

func micros(d time.Duration) int64 { return d.Microseconds() }

func (s *Service) checks() []check {
	return []check{
		{TypeCaptureLedgerMissing, checkCaptureLedgerMissing},
		{TypeCustomerFundsMismatch, checkCustomerFundsMismatch},
		{TypeUnbalancedLedgerTxn, checkUnbalancedLedgerTxn},
		{TypePaymentStuck, func(ctx context.Context, tx pgx.Tx) ([]Finding, error) {
			return checkPaymentStuck(ctx, tx, s.th)
		}},
		{TypeRefundExceedsCapture, checkRefundExceedsCapture},
		{TypeLedgerOnUncaptured, checkLedgerOnUncaptured},
		{TypeOutboxStale, func(ctx context.Context, tx pgx.Tx) ([]Finding, error) {
			return checkOutboxStale(ctx, tx, s.th)
		}},
		{TypeOperationStuck, func(ctx context.Context, tx pgx.Tx) ([]Finding, error) {
			return checkOperationStuck(ctx, tx, s.th)
		}},
		{TypeLedgerGlobalImbalance, checkLedgerGlobalImbalance},
		{TypeSettlementMismatch, checkSettlementMismatch},
	}
}

// collect runs a query and turns each row into a Finding.
func collect(ctx context.Context, tx pgx.Tx, sql string, args []any, scan func(pgx.Rows) (Finding, error)) ([]Finding, error) {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Finding
	for rows.Next() {
		f, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func paymentFinding(id uuid.UUID, details map[string]any) Finding {
	return Finding{Subject: id.String(), PaymentID: &id, Details: details}
}

// A collected payment must have its CAPTURE ledger transaction.
func checkCaptureLedgerMissing(ctx context.Context, tx pgx.Tx) ([]Finding, error) {
	return collect(ctx, tx, `
		SELECT p.id, p.status, p.amount_paise, p.fee_paise FROM payments p
		 WHERE p.status IN `+capturedStatuses+`
		   AND NOT EXISTS (SELECT 1 FROM ledger_transactions t WHERE t.payment_id = p.id AND t.kind = 'CAPTURE')
		 ORDER BY p.created_at, p.id LIMIT `+fmt.Sprint(fetchLimit), nil,
		func(r pgx.Rows) (Finding, error) {
			var (
				id          uuid.UUID
				status      string
				amount, fee int64
			)
			if err := r.Scan(&id, &status, &amount, &fee); err != nil {
				return Finding{}, err
			}
			return paymentFinding(id, map[string]any{"status": status, "amount_paise": amount, "fee_paise": fee}), nil
		})
}

// The net debited to CUSTOMER_FUNDS for a payment must be captured minus refunded.
// Payments with no CAPTURE transaction at all are left to checkCaptureLedgerMissing.
func checkCustomerFundsMismatch(ctx context.Context, tx pgx.Tx) ([]Finding, error) {
	return collect(ctx, tx, `
		SELECT p.id, p.status, p.amount_paise, p.refunded_paise, COALESCE(n.net, 0)
		  FROM payments p
		  CROSS JOIN LATERAL (
		       SELECT SUM(CASE e.direction WHEN 'DEBIT' THEN e.amount_paise ELSE -e.amount_paise END) AS net
		         FROM ledger_transactions t JOIN ledger_entries e ON e.txn_id = t.id
		        WHERE t.payment_id = p.id AND e.account_code = 'CUSTOMER_FUNDS') n
		 WHERE p.status IN `+capturedStatuses+`
		   AND EXISTS (SELECT 1 FROM ledger_transactions t WHERE t.payment_id = p.id AND t.kind = 'CAPTURE')
		   AND COALESCE(n.net, 0) <> p.amount_paise - p.refunded_paise
		 ORDER BY p.created_at, p.id LIMIT `+fmt.Sprint(fetchLimit), nil,
		func(r pgx.Rows) (Finding, error) {
			var (
				id                       uuid.UUID
				status                   string
				amount, refunded, actual int64
			)
			if err := r.Scan(&id, &status, &amount, &refunded, &actual); err != nil {
				return Finding{}, err
			}
			expected := amount - refunded
			return paymentFinding(id, map[string]any{
				"status": status, "captured_paise": amount, "refunded_paise": refunded,
				"expected_net_paise": expected, "actual_net_paise": actual, "delta_paise": expected - actual,
			}), nil
		})
}

// Every ledger transaction must have entries and balance. The database trigger makes
// this impossible; the check guards against schema or migration mistakes.
func checkUnbalancedLedgerTxn(ctx context.Context, tx pgx.Tx) ([]Finding, error) {
	return collect(ctx, tx, `
		SELECT t.id, t.payment_id, t.kind,
		       COALESCE(SUM(e.amount_paise) FILTER (WHERE e.direction = 'DEBIT'), 0),
		       COALESCE(SUM(e.amount_paise) FILTER (WHERE e.direction = 'CREDIT'), 0),
		       COUNT(e.id)
		  FROM ledger_transactions t LEFT JOIN ledger_entries e ON e.txn_id = t.id
		 GROUP BY t.id, t.payment_id, t.kind
		HAVING COUNT(e.id) = 0
		    OR COALESCE(SUM(e.amount_paise) FILTER (WHERE e.direction = 'DEBIT'), 0)
		    <> COALESCE(SUM(e.amount_paise) FILTER (WHERE e.direction = 'CREDIT'), 0)
		 ORDER BY t.created_at, t.id LIMIT `+fmt.Sprint(fetchLimit), nil,
		func(r pgx.Rows) (Finding, error) {
			var (
				id, paymentID   uuid.UUID
				kind            string
				debits, credits int64
				entries         int
			)
			if err := r.Scan(&id, &paymentID, &kind, &debits, &credits, &entries); err != nil {
				return Finding{}, err
			}
			return Finding{Subject: id.String(), PaymentID: &paymentID, Details: map[string]any{
				"kind": kind, "debits_paise": debits, "credits_paise": credits, "entries": entries}}, nil
		})
}

// A payment left PENDING or AUTHORIZED longer than its threshold. The AUTHORIZED
// threshold is much longer: holding funds until fulfilment is a normal use.
func checkPaymentStuck(ctx context.Context, tx pgx.Tx, th Thresholds) ([]Finding, error) {
	return collect(ctx, tx, `
		SELECT p.id, p.status, p.updated_at, EXTRACT(EPOCH FROM now() - p.updated_at)::bigint,
		       EXISTS (SELECT 1 FROM operations o WHERE o.payment_id = p.id AND o.status = 'PENDING')
		  FROM payments p
		 WHERE (p.status = 'PENDING'    AND p.updated_at < now() - $1 * interval '1 microsecond')
		    OR (p.status = 'AUTHORIZED' AND p.updated_at < now() - $2 * interval '1 microsecond')
		 ORDER BY p.updated_at, p.id LIMIT `+fmt.Sprint(fetchLimit),
		[]any{micros(th.PendingPayment), micros(th.AuthorizedPayment)},
		func(r pgx.Rows) (Finding, error) {
			var (
				id        uuid.UUID
				status    string
				updatedAt time.Time
				age       int64
				inFlight  bool
			)
			if err := r.Scan(&id, &status, &updatedAt, &age, &inFlight); err != nil {
				return Finding{}, err
			}
			return paymentFinding(id, map[string]any{"status": status, "last_updated_at": updatedAt,
				"age_seconds": age, "has_pending_operation": inFlight}), nil
		})
}

// Total refunded (per the refunds table, and per the ledger) must not exceed what was
// captured. A payment that was never captured has captured = 0.
func checkRefundExceedsCapture(ctx context.Context, tx pgx.Tx) ([]Finding, error) {
	return collect(ctx, tx, `
		SELECT p.id, p.status,
		       CASE WHEN p.status IN `+capturedStatuses+` THEN p.amount_paise ELSE 0 END AS captured,
		       COALESCE(r.total, 0), COALESCE(l.total, 0)
		  FROM payments p
		  LEFT JOIN (SELECT payment_id, SUM(amount_paise) AS total FROM refunds GROUP BY payment_id) r ON r.payment_id = p.id
		  LEFT JOIN (SELECT t.payment_id, SUM(e.amount_paise) AS total
		               FROM ledger_transactions t JOIN ledger_entries e ON e.txn_id = t.id
		              WHERE t.kind = 'REFUND' AND e.account_code = 'CUSTOMER_FUNDS' AND e.direction = 'CREDIT'
		              GROUP BY t.payment_id) l ON l.payment_id = p.id
		 WHERE COALESCE(r.total, 0) > CASE WHEN p.status IN `+capturedStatuses+` THEN p.amount_paise ELSE 0 END
		    OR COALESCE(l.total, 0) > CASE WHEN p.status IN `+capturedStatuses+` THEN p.amount_paise ELSE 0 END
		 ORDER BY p.created_at, p.id LIMIT `+fmt.Sprint(fetchLimit), nil,
		func(r pgx.Rows) (Finding, error) {
			var (
				id                          uuid.UUID
				status                      string
				captured, refunds, ledgered int64
			)
			if err := r.Scan(&id, &status, &captured, &refunds, &ledgered); err != nil {
				return Finding{}, err
			}
			return paymentFinding(id, map[string]any{"status": status, "captured_paise": captured,
				"refunds_table_paise": refunds, "ledger_refunded_paise": ledgered}), nil
		})
}

// A payment that never collected money must have no ledger activity. If it does, the
// finding persists until an ADJUSTMENT reverses it to a zero balance on every account
// (the original transactions stay in the append-only ledger forever).
func checkLedgerOnUncaptured(ctx context.Context, tx pgx.Tx) ([]Finding, error) {
	return collect(ctx, tx, `
		SELECT p.id, p.status, COUNT(DISTINCT t.id), ARRAY_AGG(DISTINCT t.kind ORDER BY t.kind)
		  FROM payments p JOIN ledger_transactions t ON t.payment_id = p.id
		 WHERE p.status IN `+uncapturedStatuses+`
		 GROUP BY p.id, p.status
		HAVING COUNT(DISTINCT t.id) FILTER (WHERE t.kind = 'ADJUSTMENT') = 0
		    OR EXISTS (SELECT 1 FROM ledger_transactions t2 JOIN ledger_entries e ON e.txn_id = t2.id
		                WHERE t2.payment_id = p.id GROUP BY e.account_code
		               HAVING SUM(CASE e.direction WHEN 'DEBIT' THEN e.amount_paise ELSE -e.amount_paise END) <> 0)
		 ORDER BY MIN(t.created_at), p.id LIMIT `+fmt.Sprint(fetchLimit), nil,
		func(r pgx.Rows) (Finding, error) {
			var (
				id     uuid.UUID
				status string
				n      int
				kinds  []string
			)
			if err := r.Scan(&id, &status, &n, &kinds); err != nil {
				return Finding{}, err
			}
			return paymentFinding(id, map[string]any{"status": status, "ledger_transactions": n, "kinds": kinds}), nil
		})
}

// An outbox row that should have been published long ago.
func checkOutboxStale(ctx context.Context, tx pgx.Tx, th Thresholds) ([]Finding, error) {
	return collect(ctx, tx, `
		SELECT id, topic, msg_key, available_at, EXTRACT(EPOCH FROM now() - available_at)::bigint
		  FROM outbox
		 WHERE published_at IS NULL AND available_at < now() - $1 * interval '1 microsecond'
		 ORDER BY id LIMIT `+fmt.Sprint(fetchLimit), []any{micros(th.OutboxStale)},
		func(r pgx.Rows) (Finding, error) {
			var (
				id          int64
				topic, key  string
				availableAt time.Time
				lag         int64
			)
			if err := r.Scan(&id, &topic, &key, &availableAt, &lag); err != nil {
				return Finding{}, err
			}
			return Finding{Subject: fmt.Sprint(id), Details: map[string]any{
				"topic": topic, "key": key, "available_at": availableAt, "lag_seconds": lag}}, nil
		})
}

// An operation that is not progressing: PENDING without an update for too long, or
// DEAD_LETTERED. A dead-lettered operation whose latest dead letter an operator
// dismissed is an accepted outcome, not a finding.
func checkOperationStuck(ctx context.Context, tx pgx.Tx, th Thresholds) ([]Finding, error) {
	return collect(ctx, tx, `
		SELECT o.id, o.payment_id, o.kind, o.status, o.attempts, o.updated_at, o.error_code
		  FROM operations o
		 WHERE (o.status = 'PENDING' AND o.updated_at < now() - $1 * interval '1 microsecond')
		    OR (o.status = 'DEAD_LETTERED' AND NOT COALESCE(
		          (SELECT d.status = 'DISMISSED' FROM dead_letters d
		            WHERE d.operation_id = o.id ORDER BY d.seq DESC LIMIT 1), false))
		 ORDER BY o.updated_at, o.id LIMIT `+fmt.Sprint(fetchLimit), []any{micros(th.OperationStuck)},
		func(r pgx.Rows) (Finding, error) {
			var (
				id, paymentID uuid.UUID
				kind, status  string
				attempts      int
				updatedAt     time.Time
				code          *string
			)
			if err := r.Scan(&id, &paymentID, &kind, &status, &attempts, &updatedAt, &code); err != nil {
				return Finding{}, err
			}
			return Finding{Subject: id.String(), PaymentID: &paymentID, Details: map[string]any{
				"kind": kind, "status": status, "attempts": attempts, "last_updated_at": updatedAt, "error_code": code}}, nil
		})
}

// Across the whole ledger, debits must equal credits within each currency.
func checkLedgerGlobalImbalance(ctx context.Context, tx pgx.Tx) ([]Finding, error) {
	return collect(ctx, tx, `
		SELECT p.currency,
		       COALESCE(SUM(e.amount_paise) FILTER (WHERE e.direction = 'DEBIT'), 0),
		       COALESCE(SUM(e.amount_paise) FILTER (WHERE e.direction = 'CREDIT'), 0)
		  FROM ledger_entries e
		  JOIN ledger_transactions t ON t.id = e.txn_id
		  JOIN payments p ON p.id = t.payment_id
		 GROUP BY p.currency
		HAVING COALESCE(SUM(e.amount_paise) FILTER (WHERE e.direction = 'DEBIT'), 0)
		    <> COALESCE(SUM(e.amount_paise) FILTER (WHERE e.direction = 'CREDIT'), 0)`, nil,
		func(r pgx.Rows) (Finding, error) {
			var (
				currency        string
				debits, credits int64
			)
			if err := r.Scan(&currency, &debits, &credits); err != nil {
				return Finding{}, err
			}
			return Finding{Subject: currency, Details: map[string]any{
				"currency": currency, "debits_paise": debits, "credits_paise": credits, "difference_paise": debits - credits}}, nil
		})
}

// settled_at, status SETTLED and the SETTLEMENT ledger transaction must agree.
func checkSettlementMismatch(ctx context.Context, tx pgx.Tx) ([]Finding, error) {
	return collect(ctx, tx, `
		SELECT p.id, p.status, p.settled_at,
		       EXISTS (SELECT 1 FROM ledger_transactions t WHERE t.payment_id = p.id AND t.kind = 'SETTLEMENT')
		  FROM payments p
		 WHERE (p.settled_at IS NOT NULL OR p.status = 'SETTLED')
		       <> EXISTS (SELECT 1 FROM ledger_transactions t WHERE t.payment_id = p.id AND t.kind = 'SETTLEMENT')
		 ORDER BY p.created_at, p.id LIMIT `+fmt.Sprint(fetchLimit), nil,
		func(r pgx.Rows) (Finding, error) {
			var (
				id        uuid.UUID
				status    string
				settledAt *time.Time
				hasLedger bool
			)
			if err := r.Scan(&id, &status, &settledAt, &hasLedger); err != nil {
				return Finding{}, err
			}
			return paymentFinding(id, map[string]any{"status": status, "settled_at": settledAt, "has_settlement_txn": hasLedger}), nil
		})
}
