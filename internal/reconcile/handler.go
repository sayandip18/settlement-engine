package reconcile

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/sayandip18/settlement-engine/internal/httpapi"
)

// Handler serves the admin endpoints. Mount Routes on the authenticated /admin router.
type Handler struct {
	svc *Service
	log *slog.Logger
}

func NewHandler(svc *Service, log *slog.Logger) *Handler { return &Handler{svc: svc, log: log} }

func (h *Handler) Routes(r chi.Router) {
	r.Get("/reconciliation/discrepancies", h.list)
	r.Get("/reconciliation/discrepancies/{id}", h.get)
	r.Post("/reconciliation/discrepancies/{id}/acknowledge", h.acknowledge)
	r.Post("/reconciliation/discrepancies/{id}/resolve", h.resolve)
	r.Post("/reconciliation/discrepancies/{id}/correct", h.correct)
	r.Post("/reconciliation/run", h.run)
}

type listResponse struct {
	Items      []Discrepancy `json:"items"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

var allTypes = map[Type]bool{
	TypeCaptureLedgerMissing: true, TypeCustomerFundsMismatch: true, TypeUnbalancedLedgerTxn: true,
	TypePaymentStuck: true, TypeRefundExceedsCapture: true, TypeLedgerOnUncaptured: true,
	TypeOutboxStale: true, TypeOperationStuck: true, TypeLedgerGlobalImbalance: true,
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var f ListFilter
	if v := q.Get("status"); v != "" {
		s := Status(v)
		if !s.valid() {
			httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", "status must be OPEN, ACKNOWLEDGED or RESOLVED")
			return
		}
		f.Status = &s
	}
	if v := q.Get("type"); v != "" {
		t := Type(v)
		if !allTypes[t] {
			httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", "unknown type")
			return
		}
		f.Type = &t
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > MaxLimit {
			httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", "limit must be between 1 and "+strconv.Itoa(MaxLimit))
			return
		}
		f.Limit = n
	}
	if v := q.Get("cursor"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1 {
			httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", "cursor is not valid")
			return
		}
		f.Before = n
	}
	items, next, err := h.svc.List(r.Context(), f)
	if err != nil {
		h.fail(w, err)
		return
	}
	resp := listResponse{Items: items}
	if resp.Items == nil {
		resp.Items = []Discrepancy{}
	}
	if next != 0 {
		resp.NextCursor = strconv.FormatInt(next, 10)
	}
	httpapi.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	d, err := h.svc.Get(r.Context(), id)
	if err != nil {
		h.fail(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, d)
}

type noteBody struct {
	Note    string            `json:"note"`
	Entries []CorrectionEntry `json:"entries"`
}

// decode reads the optional JSON body. withEntries says whether "entries" is accepted.
func decode(w http.ResponseWriter, r *http.Request, withEntries bool) (noteBody, bool) {
	var b noteBody
	if r.ContentLength != 0 {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&b); err != nil || (!withEntries && b.Entries != nil) {
			msg := `body must be {"note": "..."}`
			if withEntries {
				msg = `body must be {"note": "...", "entries": [{"account": "...", "direction": "DEBIT|CREDIT", "amount_paise": 0}]}`
			}
			httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", msg)
			return b, false
		}
	}
	return b, true
}

func (h *Handler) acknowledge(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	b, ok := decode(w, r, false)
	if !ok {
		return
	}
	d, err := h.svc.Acknowledge(r.Context(), id, b.Note)
	if err != nil {
		h.fail(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, d)
}

func (h *Handler) resolve(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	b, ok := decode(w, r, false)
	if !ok {
		return
	}
	d, err := h.svc.Resolve(r.Context(), id, b.Note)
	if err != nil {
		h.fail(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, d)
}

func (h *Handler) correct(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	b, ok := decode(w, r, true)
	if !ok {
		return
	}
	d, err := h.svc.Correct(r.Context(), id, b.Note, b.Entries)
	if err != nil {
		h.fail(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, d)
}

// run triggers a reconciliation immediately (the worker also runs it on a schedule).
func (h *Handler) run(w http.ResponseWriter, r *http.Request) {
	res, err := h.svc.Run(r.Context())
	if err != nil {
		h.fail(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, res)
}

func parseID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", "id must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}

func (h *Handler) fail(w http.ResponseWriter, err error) {
	var (
		handled  *AlreadyHandledError
		verr     *ValidationError
		nocorr   *NotCorrectableError
		rejected *CorrectionRejectedError
	)
	switch {
	case errors.Is(err, ErrNotFound):
		httpapi.WriteError(w, http.StatusNotFound, "not_found", "discrepancy not found")
	case errors.As(err, &handled):
		httpapi.WriteError(w, http.StatusConflict, "already_handled", handled.Error())
	case errors.Is(err, ErrAlreadyRunning):
		httpapi.WriteError(w, http.StatusConflict, "already_running", err.Error())
	case errors.Is(err, ErrConflict):
		httpapi.WriteError(w, http.StatusConflict, "conflict", err.Error())
	case errors.As(err, &verr):
		httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", verr.Msg)
	case errors.As(err, &nocorr):
		httpapi.WriteError(w, http.StatusUnprocessableEntity, "not_correctable", nocorr.Error())
	case errors.As(err, &rejected):
		httpapi.WriteError(w, http.StatusUnprocessableEntity, "correction_rejected", rejected.Error())
	default:
		h.log.Error("reconciliation admin", "err", err)
		httpapi.WriteError(w, http.StatusInternalServerError, "internal", "internal error")
	}
}
