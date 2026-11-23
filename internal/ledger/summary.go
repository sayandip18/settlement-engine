package ledger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	"github.com/sayandip18/settlement-engine/internal/httpapi"
)

// Querier is satisfied by both *pgxpool.Pool and pgx.Tx.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// PayoutSummary totals one merchant's money movement in [From, To), read purely
// from ledger entries. GrossCollected - Refunded - Fees == NetPayout holds whenever
// the window contains only CAPTURE and REFUND transactions. An ADJUSTMENT (a
// reconciliation correction) moves FeesPaise and NetPayoutPaise but not the gross and
// refunded figures, so the identity can differ by the adjusted amount. A re-posted
// capture is dated when it was posted, not when the money was originally collected.
type PayoutSummary struct {
	MerchantID          string    `json:"merchant_id"`
	Currency            string    `json:"currency"`
	From                time.Time `json:"from"`
	To                  time.Time `json:"to"`
	GrossCollectedPaise int64     `json:"gross_collected_paise"`
	RefundedPaise       int64     `json:"refunded_paise"`
	FeesPaise           int64     `json:"fees_paise"`
	NetPayoutPaise      int64     `json:"net_payout_paise"`
	// SettledPaise is what was paid out to the merchant in the window (SETTLEMENT
	// transactions). It does not change NetPayoutPaise, which is what the merchant earned.
	SettledPaise int64 `json:"settled_paise"`
}

func Summary(ctx context.Context, q Querier, merchantID string, from, to time.Time) (PayoutSummary, error) {
	s := PayoutSummary{MerchantID: merchantID, Currency: "INR", From: from, To: to}
	err := q.QueryRow(ctx, `
		SELECT
		  COALESCE(SUM(e.amount_paise) FILTER (WHERE t.kind = 'CAPTURE' AND e.account_code = 'CUSTOMER_FUNDS' AND e.direction = 'DEBIT'), 0),
		  COALESCE(SUM(e.amount_paise) FILTER (WHERE t.kind = 'REFUND'  AND e.account_code = 'CUSTOMER_FUNDS' AND e.direction = 'CREDIT'), 0),
		  COALESCE(SUM(e.amount_paise) FILTER (WHERE e.account_code = 'PLATFORM_FEES' AND e.direction = 'CREDIT'), 0)
		- COALESCE(SUM(e.amount_paise) FILTER (WHERE e.account_code = 'PLATFORM_FEES' AND e.direction = 'DEBIT'), 0),
		  COALESCE(SUM(e.amount_paise) FILTER (WHERE t.kind <> 'SETTLEMENT' AND e.account_code = $2 AND e.direction = 'CREDIT'), 0)
		- COALESCE(SUM(e.amount_paise) FILTER (WHERE t.kind <> 'SETTLEMENT' AND e.account_code = $2 AND e.direction = 'DEBIT'), 0),
		  COALESCE(SUM(e.amount_paise) FILTER (WHERE t.kind = 'SETTLEMENT' AND e.account_code = $2 AND e.direction = 'DEBIT'), 0)
		FROM ledger_entries e
		JOIN ledger_transactions t ON t.id = e.txn_id
		WHERE t.merchant_id = $1 AND t.created_at >= $3 AND t.created_at < $4`,
		merchantID, MerchantPayable(merchantID), from, to,
	).Scan(&s.GrossCollectedPaise, &s.RefundedPaise, &s.FeesPaise, &s.NetPayoutPaise, &s.SettledPaise)
	if err != nil {
		return s, fmt.Errorf("payout summary: %w", err)
	}
	return s, nil
}

type Handler struct {
	q   Querier
	log *slog.Logger
}

func NewHandler(q Querier, log *slog.Logger) *Handler { return &Handler{q: q, log: log} }

func (h *Handler) Routes(r chi.Router) {
	r.Get("/v1/merchants/{id}/payouts/summary", h.summary)
}

func (h *Handler) summary(w http.ResponseWriter, r *http.Request) {
	from, errFrom := time.Parse(time.RFC3339, r.URL.Query().Get("from"))
	to, errTo := time.Parse(time.RFC3339, r.URL.Query().Get("to"))
	if err := errors.Join(errFrom, errTo); err != nil {
		httpapi.WriteError(w, http.StatusBadRequest, "invalid_request",
			"from and to are required RFC 3339 timestamps, e.g. 2026-11-01T00:00:00Z")
		return
	}
	if !from.Before(to) {
		httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", "from must be before to")
		return
	}
	s, err := Summary(r.Context(), h.q, chi.URLParam(r, "id"), from.UTC(), to.UTC())
	if err != nil {
		h.log.Error("payout summary", "err", err)
		httpapi.WriteError(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, s)
}
