package payment_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/sayandip18/settlement-engine/internal/payment"
	"github.com/sayandip18/settlement-engine/internal/testutil"
)

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	svc := payment.NewService(testutil.NewPool(t))
	r := chi.NewRouter()
	payment.NewHandler(svc, slog.New(slog.NewTextHandler(io.Discard, nil))).Routes(r)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, srv *httptest.Server, key, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/payments", bytes.NewBufferString(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

const body = `{"merchant_id":"m1","customer_id":"c1","amount_paise":50000}`

func TestHTTPCreateReplayAndGet(t *testing.T) {
	srv := newServer(t)

	r1 := post(t, srv, "abc", body)
	if r1.StatusCode != http.StatusCreated {
		t.Fatalf("first: got %d", r1.StatusCode)
	}
	var p1 payment.Payment
	if err := json.NewDecoder(r1.Body).Decode(&p1); err != nil {
		t.Fatal(err)
	}

	r2 := post(t, srv, "abc", body)
	if r2.StatusCode != http.StatusOK || r2.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: got %d, replayed=%q", r2.StatusCode, r2.Header.Get("Idempotent-Replayed"))
	}
	var p2 payment.Payment
	if err := json.NewDecoder(r2.Body).Decode(&p2); err != nil {
		t.Fatal(err)
	}
	if p1.ID != p2.ID {
		t.Fatalf("replay returned different payment: %s vs %s", p1.ID, p2.ID)
	}

	resp, err := http.Get(srv.URL + "/v1/payments/" + p1.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get: got %d", resp.StatusCode)
	}
}

func TestHTTPErrors(t *testing.T) {
	srv := newServer(t)

	if r := post(t, srv, "abc", body); r.StatusCode != http.StatusCreated {
		t.Fatalf("setup: got %d", r.StatusCode)
	}
	cases := map[string]struct {
		key, body string
		want      int
	}{
		"reused key, new body": {"abc", `{"merchant_id":"m1","customer_id":"c1","amount_paise":1}`, http.StatusUnprocessableEntity},
		"missing key":          {"", body, http.StatusBadRequest},
		"bad json":             {"k2", `{`, http.StatusBadRequest},
		"unknown field":        {"k3", `{"merchant_id":"m1","customer_id":"c1","amount_paise":5,"x":1}`, http.StatusBadRequest},
		"bad amount":           {"k4", `{"merchant_id":"m1","customer_id":"c1","amount_paise":0}`, http.StatusBadRequest},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if got := post(t, srv, c.key, c.body).StatusCode; got != c.want {
				t.Fatalf("got %d, want %d", got, c.want)
			}
		})
	}

	for path, want := range map[string]int{
		"/v1/payments/not-a-uuid":                           http.StatusBadRequest,
		"/v1/payments/00000000-0000-0000-0000-000000000000": http.StatusNotFound,
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("%s: got %d, want %d", path, resp.StatusCode, want)
		}
	}
}
