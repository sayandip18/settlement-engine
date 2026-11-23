package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type routerConfig struct{ metrics http.Handler }

// RouterOption customises NewRouter.
type RouterOption func(*routerConfig)

// WithMetrics serves h at GET /metrics, protected by the admin key (Prometheus can send
// it as a bearer token).
func WithMetrics(h http.Handler) RouterOption { return func(c *routerConfig) { c.metrics = h } }

// NewRouter wires shared middleware, health endpoints and an admin sub-router
// guarded by AdminAuth. register mounts public routes; registerAdmin mounts
// routes under /admin (either may be nil).
func NewRouter(log *slog.Logger, health *Health, adminKey string,
	register func(chi.Router), registerAdmin func(chi.Router), opts ...RouterOption) http.Handler {

	var cfg routerConfig
	for _, o := range opts {
		o(&cfg)
	}

	r := chi.NewRouter()
	r.Use(Instrument, middleware.RequestID, middleware.Recoverer, RequestLogger(log))

	r.Get("/healthz", health.Live)
	r.Get("/readyz", health.Ready)
	if cfg.metrics != nil {
		r.With(AdminAuth(adminKey)).Method(http.MethodGet, "/metrics", cfg.metrics)
	}

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
