// Package merchant holds per-merchant settings (currently just the fee rate).
package merchant

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sayandip18/settlement-engine/internal/httpapi"
)

// MaxFeeBPS is 100%.
const MaxFeeBPS = 10000

// Querier is satisfied by both *pgxpool.Pool and pgx.Tx.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// FeeBPS returns the merchant's fee in basis points (200 = 2%), or def when the
// merchant has no setting of its own.
func FeeBPS(ctx context.Context, q Querier, merchantID string, def int) (int, error) {
	var bps int
	err := q.QueryRow(ctx, `SELECT fee_bps FROM merchants WHERE id = $1`, merchantID).Scan(&bps)
	if errors.Is(err, pgx.ErrNoRows) {
		return def, nil
	}
	return bps, err
}

// Handler serves the admin endpoints; mount it on the authenticated /admin router.
type Handler struct {
	pool       *pgxpool.Pool
	defaultBPS int
	log        *slog.Logger
}

func NewHandler(pool *pgxpool.Pool, defaultBPS int, log *slog.Logger) *Handler {
	return &Handler{pool: pool, defaultBPS: defaultBPS, log: log}
}

func (h *Handler) Routes(r chi.Router) {
	r.Get("/merchants/{id}/fee", h.getFee)
	r.Put("/merchants/{id}/fee", h.setFee)
}

type feeBody struct {
	MerchantID string `json:"merchant_id,omitempty"`
	FeeBPS     *int   `json:"fee_bps"`
}

func (h *Handler) getFee(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	bps, err := FeeBPS(r.Context(), h.pool, id, h.defaultBPS)
	if err != nil {
		h.fail(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, feeBody{MerchantID: id, FeeBPS: &bps})
}

// setFee changes the rate for future captures only; each payment keeps the fee
// frozen at its own capture.
func (h *Handler) setFee(w http.ResponseWriter, r *http.Request) {
	var body feeBody
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil || body.FeeBPS == nil {
		httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", `body must be {"fee_bps": <0-10000>}`)
		return
	}
	if *body.FeeBPS < 0 || *body.FeeBPS > MaxFeeBPS {
		httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", "fee_bps must be between 0 and 10000")
		return
	}
	id := chi.URLParam(r, "id")
	if _, err := h.pool.Exec(r.Context(),
		`INSERT INTO merchants (id, fee_bps) VALUES ($1, $2)
		 ON CONFLICT (id) DO UPDATE SET fee_bps = EXCLUDED.fee_bps, updated_at = now()`,
		id, *body.FeeBPS); err != nil {
		h.fail(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, feeBody{MerchantID: id, FeeBPS: body.FeeBPS})
}

func (h *Handler) fail(w http.ResponseWriter, err error) {
	h.log.Error("merchant fee", "err", err)
	httpapi.WriteError(w, http.StatusInternalServerError, "internal", "internal error")
}
