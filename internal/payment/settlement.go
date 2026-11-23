package payment

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/sayandip18/settlement-engine/internal/ledger"
	"github.com/sayandip18/settlement-engine/internal/obs"
)

// settleBatch bounds how many payments one pass settles.
const settleBatch = 100

// Settle pays out one payment to its merchant if it is due: captured (and not fully
// refunded) at least delay ago and not yet settled. It reports whether it settled.
//
// It runs under the payment's row lock, so it serialises with refunds, and the ledger's
// UNIQUE (kind, ref_id) makes a second settlement of the same payment impossible even
// if the eligibility check were wrong. The payout is whatever is currently owed to the
// merchant on this payment (captured less fee, less refunds already taken back).
// A payment owing nothing is left alone.
func (s *Service) Settle(ctx context.Context, id uuid.UUID, delay time.Duration) (bool, error) {
	settled := false
	_, err := s.mutate(ctx, id, func(tx pgx.Tx, p *Payment) error {
		if p.SettledAt != nil || (p.Status != StatusCaptured && p.Status != StatusPartiallyRefunded) {
			return nil
		}
		// The delay is measured on the database clock, like every other delay here.
		var due bool
		var now time.Time
		if err := tx.QueryRow(ctx,
			`SELECT $1::timestamptz IS NOT NULL AND $1::timestamptz <= now() - $2 * interval '1 microsecond', now()`,
			p.CapturedAt, delay.Microseconds()).Scan(&due, &now); err != nil {
			return fmt.Errorf("check settlement due: %w", err)
		}
		if !due {
			return nil
		}

		var owed int64
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(SUM(CASE e.direction WHEN 'CREDIT' THEN e.amount_paise ELSE -e.amount_paise END), 0)
			   FROM ledger_transactions t JOIN ledger_entries e ON e.txn_id = t.id
			  WHERE t.payment_id = $1 AND t.kind <> 'SETTLEMENT' AND e.account_code = $2`,
			p.ID, ledger.MerchantPayable(p.MerchantID)).Scan(&owed); err != nil {
			return fmt.Errorf("compute amount owed: %w", err)
		}
		if owed <= 0 {
			return nil
		}
		if err := ledger.Post(ctx, tx, ledger.Settlement(p.ID, p.MerchantID, owed)); err != nil {
			return err
		}
		p.SettledAt = &now
		if p.Status == StatusCaptured {
			p.Status = StatusSettled
		}
		settled = true
		return save(ctx, tx, p)
	})
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if settled && err == nil {
		obs.SettlementsTotal.Inc()
	}
	return settled, err
}

// SettleDue settles every payment that is due, oldest first, and returns how many.
// One payment failing does not stop the rest; the errors are joined.
func (s *Service) SettleDue(ctx context.Context, delay time.Duration) (int, error) {
	var (
		total int
		errs  []error
		skip  = map[uuid.UUID]bool{} // payments that errored or owe nothing: do not loop on them
	)
	for {
		rows, err := s.pool.Query(ctx,
			`SELECT id FROM payments
			  WHERE settled_at IS NULL AND status IN ('CAPTURED', 'PARTIALLY_REFUNDED')
			    AND captured_at <= now() - $1 * interval '1 microsecond'
			  ORDER BY captured_at, id LIMIT $2`, delay.Microseconds(), settleBatch+len(skip))
		if err != nil {
			return total, errors.Join(append(errs, fmt.Errorf("list due payments: %w", err))...)
		}
		var ids []uuid.UUID
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return total, err
			}
			if !skip[id] {
				ids = append(ids, id)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return total, err
		}
		if len(ids) == 0 {
			return total, errors.Join(errs...)
		}
		if len(ids) > settleBatch {
			ids = ids[:settleBatch]
		}
		for _, id := range ids {
			if ctx.Err() != nil {
				return total, ctx.Err()
			}
			ok, err := s.Settle(ctx, id, delay)
			switch {
			case err != nil:
				errs = append(errs, fmt.Errorf("settle %s: %w", id, err))
				skip[id] = true
			case ok:
				total++
			default:
				skip[id] = true
			}
		}
	}
}

// RunSettlement settles due payments now and then every interval until ctx ends.
// beat (optional) is called every pass for readiness checks.
func (s *Service) RunSettlement(ctx context.Context, interval, delay time.Duration, log *slog.Logger, beat func()) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if beat != nil {
			beat()
		}
		n, err := s.SettleDue(ctx, delay)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			log.Error("settlement pass had errors", "settled", n, "err", err)
		case n > 0:
			log.Info("settled payments", "count", n)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
