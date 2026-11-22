package httpapi_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sayandip18/settlement-engine/internal/httpapi"
)

func TestAdminAuth(t *testing.T) {
	h := httpapi.AdminAuth("secret")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	cases := map[string]struct {
		header string
		want   int
	}{
		"no header":    {"", http.StatusUnauthorized},
		"wrong key":    {"Bearer nope", http.StatusUnauthorized},
		"wrong scheme": {"secret", http.StatusUnauthorized},
		"valid":        {"Bearer secret", http.StatusNoContent},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/admin/x", nil)
			if c.header != "" {
				req.Header.Set("Authorization", c.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != c.want {
				t.Fatalf("got %d, want %d", rec.Code, c.want)
			}
		})
	}
}

func TestReadiness(t *testing.T) {
	ok := func(context.Context) error { return nil }
	bad := func(context.Context) error { return errors.New("down") }

	for name, c := range map[string]struct {
		checks map[string]httpapi.Check
		want   int
	}{
		"all healthy":    {map[string]httpapi.Check{"db": ok, "kafka": ok}, http.StatusOK},
		"one dependency": {map[string]httpapi.Check{"db": ok, "kafka": bad}, http.StatusServiceUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			httpapi.NewHealth(c.checks).Ready(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if rec.Code != c.want {
				t.Fatalf("got %d, want %d", rec.Code, c.want)
			}
		})
	}
}
