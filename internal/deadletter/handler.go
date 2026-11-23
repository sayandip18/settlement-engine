package deadletter

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
	r.Get("/dlq", h.list)
	r.Get("/dlq/{id}", h.get)
	r.Post("/dlq/{id}/replay", h.replay)
	r.Post("/dlq/{id}/dismiss", h.dismiss)
}

type listResponse struct {
	Items      []Entry `json:"items"`
	NextCursor string  `json:"next_cursor,omitempty"`
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var f ListFilter
	if v := q.Get("status"); v != "" {
		s := Status(v)
		if !s.valid() {
			httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", "status must be PENDING, REPLAYED or DISMISSED")
			return
		}
		f.Status = &s
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
		resp.Items = []Entry{}
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
	e, err := h.svc.Get(r.Context(), id)
	if err != nil {
		h.fail(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, e)
}

func (h *Handler) replay(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	e, err := h.svc.Replay(r.Context(), id)
	if err != nil {
		h.fail(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, e)
}

func (h *Handler) dismiss(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	var body struct {
		Note string `json:"note"`
	}
	if r.ContentLength != 0 {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<14))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			httpapi.WriteError(w, http.StatusBadRequest, "invalid_request", `body must be {"note": "..."}`)
			return
		}
	}
	e, err := h.svc.Dismiss(r.Context(), id, body.Note)
	if err != nil {
		h.fail(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, e)
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
		handled *AlreadyHandledError
		nope    *NotReplayableError
	)
	switch {
	case errors.Is(err, ErrNotFound):
		httpapi.WriteError(w, http.StatusNotFound, "not_found", "dead letter not found")
	case errors.As(err, &handled):
		httpapi.WriteError(w, http.StatusConflict, "already_handled", handled.Error())
	case errors.As(err, &nope):
		httpapi.WriteError(w, http.StatusConflict, "not_replayable", nope.Error()+"; dismiss it instead")
	default:
		h.log.Error("dead letter admin", "err", err)
		httpapi.WriteError(w, http.StatusInternalServerError, "internal", "internal error")
	}
}
