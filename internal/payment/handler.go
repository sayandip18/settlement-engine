package payment

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

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
