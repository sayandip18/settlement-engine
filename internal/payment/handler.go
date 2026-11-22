package payment

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/sayandip18/settlement-engine/internal/httpapi"
)

type Handler struct {
	svc *Service
	log *slog.Logger
}

func NewHandler(svc *Service, log *slog.Logger) *Handler { return &Handler{svc: svc, log: log} }

func (h *Handler) Routes(r chi.Router) {
	r.Post("/v1/payments", h.create)
	r.Get("/v1/payments/{id}", h.get)
	r.Post("/v1/payments/{id}/authorize", h.action(h.svc.Authorize))
	r.Post("/v1/payments/{id}/capture", h.action(h.svc.Capture))
	r.Post("/v1/payments/{id}/void", h.action(h.svc.Void))
	r.Post("/v1/payments/{id}/refund", h.refund)
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var in CreateInput
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", "malformed JSON body: "+err.Error())
		return
	}
	in.IdempotencyKey = r.Header.Get("Idempotency-Key")

	res, err := h.svc.Create(r.Context(), in)
	var verr *ValidationError
	switch {
	case errors.As(err, &verr):
		httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", verr.Msg)
	case errors.Is(err, ErrIdempotencyReused):
		httpapi.WriteError(w, http.StatusUnprocessableEntity, "idempotency_key_reused",
			"this Idempotency-Key was already used with different payment details")
	case err != nil:
		h.log.Error("create payment", "err", err)
		httpapi.WriteError(w, http.StatusInternalServerError, "internal", "internal error")
	case res.Replayed:
		w.Header().Set("Idempotent-Replayed", "true")
		httpapi.WriteJSON(w, http.StatusOK, res.Payment)
	default:
		httpapi.WriteJSON(w, http.StatusCreated, res.Payment)
	}
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", "payment id must be a UUID")
		return
	}
	p, err := h.svc.Get(r.Context(), id)
	switch {
	case errors.Is(err, ErrNotFound):
		httpapi.WriteError(w, http.StatusNotFound, "not_found", "payment not found")
	case err != nil:
		h.log.Error("get payment", "err", err)
		httpapi.WriteError(w, http.StatusInternalServerError, "internal", "internal error")
	default:
		httpapi.WriteJSON(w, http.StatusOK, p)
	}
}

// action adapts a payment lifecycle method (authorize, capture, void) to HTTP.
func (h *Handler) action(do func(context.Context, uuid.UUID) (Payment, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := parseID(w, r)
		if !ok {
			return
		}
		p, err := do(r.Context(), id)
		if err != nil {
			h.writeLifecycleError(w, err)
			return
		}
		httpapi.WriteJSON(w, http.StatusOK, p)
	}
}

type refundRequest struct {
	AmountPaise *int64 `json:"amount_paise"`
}

type refundResponse struct {
	Refund  Refund  `json:"refund"`
	Payment Payment `json:"payment"`
}

func (h *Handler) refund(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	var req refundRequest
	// An empty body means "refund everything that is left".
	if r.ContentLength != 0 {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", "malformed JSON body: "+err.Error())
			return
		}
	}
	res, err := h.svc.Refund(r.Context(), RefundInput{
		PaymentID:      id,
		AmountPaise:    req.AmountPaise,
		IdempotencyKey: strings.TrimSpace(r.Header.Get("Idempotency-Key")),
	})
	if err != nil {
		h.writeLifecycleError(w, err)
		return
	}
	status := http.StatusCreated
	if res.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
		status = http.StatusOK
	}
	httpapi.WriteJSON(w, status, refundResponse{Refund: res.Refund, Payment: res.Payment})
}

func parseID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", "payment id must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}

type declinedResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
	Payment Payment `json:"payment"`
}

func (h *Handler) writeLifecycleError(w http.ResponseWriter, err error) {
	var (
		verr *ValidationError
		terr *InvalidTransitionError
		rerr *RefundExceedsError
		derr *DeclinedError
	)
	switch {
	case errors.As(err, &verr):
		httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", verr.Msg)
	case errors.Is(err, ErrNotFound):
		httpapi.WriteError(w, http.StatusNotFound, "not_found", "payment not found")
	case errors.As(err, &terr):
		httpapi.WriteError(w, http.StatusConflict, "invalid_state", terr.Error())
	case errors.As(err, &rerr):
		httpapi.WriteError(w, http.StatusUnprocessableEntity, "refund_exceeds_remaining", rerr.Error())
	case errors.Is(err, ErrIdempotencyReused):
		httpapi.WriteError(w, http.StatusUnprocessableEntity, "idempotency_key_reused",
			"this Idempotency-Key was already used with a different amount")
	case errors.As(err, &derr):
		var body declinedResponse
		body.Error.Code, body.Error.Message, body.Payment = derr.Code, derr.Message, derr.Payment
		httpapi.WriteJSON(w, http.StatusPaymentRequired, body)
	case errors.Is(err, ErrProcessorUnavailable):
		h.log.Warn("processor unavailable", "err", err)
		w.Header().Set("Retry-After", "1")
		httpapi.WriteError(w, http.StatusServiceUnavailable, "processor_unavailable",
			"payment processor is temporarily unavailable; the request can be retried")
	default:
		h.log.Error("payment action", "err", err)
		httpapi.WriteError(w, http.StatusInternalServerError, "internal", "internal error")
	}
}
