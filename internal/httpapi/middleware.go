package httpapi

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/sayandip18/settlement-engine/internal/obs"
)

// AdminAuth requires "Authorization: Bearer <key>" matching the admin key.
func AdminAuth(key string) func(http.Handler) http.Handler {
	want := []byte(key)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || subtle.ConstantTimeCompare([]byte(got), want) != 1 {
				WriteError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid admin credentials")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// RequestLogger logs one structured line per request.
func RequestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			start := time.Now()
			next.ServeHTTP(ww, r)
			log.Info("request",
				"request_id", middleware.GetReqID(r.Context()),
				"trace_id", obs.TraceID(r.Context()),
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"duration_ms", time.Since(start).Milliseconds(),
			)
		})
	}
}
