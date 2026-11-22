package command

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/sayandip18/settlement-engine/internal/httpapi"
	"github.com/sayandip18/settlement-engine/internal/payment"
)

type Handler struct {
	svc *Service
	log *slog.Logger
}

func NewHandler(svc *Service, log *slog.Logger) *Handler { return &Handler{svc: svc, log: log} }

func (h *Handler) Routes(r chi.Router) {
	r.Post("/v1/payments/{id}/authorize", h.submit(KindAuthorize))
	r.Post("/v1/payments/{id}/capture", h.submit(KindCapture))
	r.Post("/v1/payments/{id}/void", h.submit(KindVoid))
	r.Post("/v1/payments/{id}/refund", h.submit(KindRefund))
	r.Get("/v1/operations/{id}", h.get)
}

type refundRequest struct {
	AmountPaise *int64 `json:"amount_paise"`
}

// submit accepts a request for background processing: 202 with the operation to poll,
// or 200 if an identical refund request was already accepted.
func (h *Handler) submit(kind Kind) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", "payment id must be a UUID")
			return
		}
		in := SubmitInput{PaymentID: id, Kind: kind, IdempotencyKey: r.Header.Get("Idempotency-Key")}
		if kind == KindRefund && r.ContentLength != 0 { // an empty body means "refund the remainder"
			var req refundRequest
			dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&req); err != nil {
				httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", "malformed JSON body: "+err.Error())
				return
			}
			in.AmountPaise = req.AmountPaise
		}

		res, err := h.svc.Submit(r.Context(), in)
		if err != nil {
			h.writeError(w, err)
			return
		}
		w.Header().Set("Location", "/v1/operations/"+res.Operation.ID.String())
		status := http.StatusAccepted
		if res.Replayed {
			w.Header().Set("Idempotent-Replayed", "true")
			status = http.StatusOK
		}
		httpapi.WriteJSON(w, status, res.Operation)
	}
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", "operation id must be a UUID")
		return
	}
	op, err := h.svc.Get(r.Context(), id)
	if err != nil {
		h.writeError(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, op)
}

func (h *Handler) writeError(w http.ResponseWriter, err error) {
	var (
		verr *payment.ValidationError
		terr *payment.InvalidTransitionError
		rerr *payment.RefundExceedsError
	)
	switch {
	case errors.As(err, &verr):
		httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", verr.Msg)
	case errors.Is(err, payment.ErrNotFound):
		httpapi.WriteError(w, http.StatusNotFound, "not_found", "payment not found")
	case errors.Is(err, ErrOperationNotFound):
		httpapi.WriteError(w, http.StatusNotFound, "not_found", "operation not found")
	case errors.As(err, &terr):
		httpapi.WriteError(w, http.StatusConflict, "invalid_state", terr.Error())
	case errors.As(err, &rerr):
		httpapi.WriteError(w, http.StatusUnprocessableEntity, "refund_exceeds_remaining", rerr.Error())
	case errors.Is(err, payment.ErrIdempotencyReused):
		httpapi.WriteError(w, http.StatusUnprocessableEntity, "idempotency_key_reused",
			"this Idempotency-Key was already used with a different amount")
	default:
		h.log.Error("command request", "err", err)
		httpapi.WriteError(w, http.StatusInternalServerError, "internal", "internal error")
	}
}
