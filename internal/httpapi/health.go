package httpapi

import (
	"context"
	"net/http"
	"time"
)

// Check reports whether a dependency is reachable.
type Check func(ctx context.Context) error

// Health serves liveness (/healthz) and readiness (/readyz).
type Health struct {
	checks map[string]Check
}

func NewHealth(checks map[string]Check) *Health { return &Health{checks: checks} }

func (h *Health) Live(w http.ResponseWriter, _ *http.Request) {
	WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Health) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	status, results := http.StatusOK, map[string]string{}
	for name, check := range h.checks {
		if err := check(ctx); err != nil {
			status, results[name] = http.StatusServiceUnavailable, err.Error()
		} else {
			results[name] = "ok"
		}
	}
	WriteJSON(w, status, results)
}
