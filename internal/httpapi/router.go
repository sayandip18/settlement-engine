package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// NewRouter wires shared middleware, health endpoints and an admin sub-router
// guarded by AdminAuth. register mounts public routes; registerAdmin mounts
// routes under /admin (either may be nil).
func NewRouter(log *slog.Logger, health *Health, adminKey string,
	register func(chi.Router), registerAdmin func(chi.Router)) http.Handler {

	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.Recoverer, RequestLogger(log))

	r.Get("/healthz", health.Live)
	r.Get("/readyz", health.Ready)

	if register != nil {
		register(r)
	}
	r.Route("/admin", func(ar chi.Router) {
		ar.Use(AdminAuth(adminKey))
		if registerAdmin != nil {
			registerAdmin(ar)
		}
	})
	return r
}
