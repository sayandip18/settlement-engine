package command_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/sayandip18/settlement-engine/internal/command"
)

func newAPI(t *testing.T, h *harness) *httptest.Server {
	t.Helper()
	r := chi.NewRouter()
	command.NewHandler(h.svc, slog.New(slog.NewTextHandler(io.Discard, nil))).Routes(r)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

func call(t *testing.T, method, url, key, body string) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func TestHTTPAcceptsThenReportsProgress(t *testing.T) {
	h := newHarness(t)
	srv := newAPI(t, h)
	p := h.newPayment(t, "c1")
	base := srv.URL + "/v1/payments/" + p.ID.String()

	resp, body := call(t, "POST", base+"/authorize", "", "")
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("got %d: %s", resp.StatusCode, body)
	}
	var op command.Operation
	if err := json.Unmarshal(body, &op); err != nil {
		t.Fatal(err)
	}
	loc := resp.Header.Get("Location")
	if loc != "/v1/operations/"+op.ID.String() || op.Status != command.StatusPending {
		t.Fatalf("location=%q op=%+v", loc, op)
	}

	_, body = call(t, "GET", srv.URL+loc, "", "")
	if !strings.Contains(string(body), `"PENDING"`) {
		t.Fatalf("before processing: %s", body)
	}
	h.pump(t, op)
	_, body = call(t, "GET", srv.URL+loc, "", "")
	if !strings.Contains(string(body), `"SUCCEEDED"`) {
		t.Fatalf("after processing: %s", body)
	}

	// A failed operation exposes the reason.
	d := h.newPayment(t, "decline_9")
	resp, body = call(t, "POST", srv.URL+"/v1/payments/"+d.ID.String()+"/authorize", "", "")
	json.Unmarshal(body, &op)
	h.pump(t, op)
	_, body = call(t, "GET", srv.URL+resp.Header.Get("Location"), "", "")
	if !strings.Contains(string(body), `"FAILED"`) || !strings.Contains(string(body), `"insufficient_funds"`) {
		t.Fatalf("failed op: %s", body)
	}
}

func TestHTTPStatusCodes(t *testing.T) {
	h := newHarness(t)
	srv := newAPI(t, h)
	pending := h.newPayment(t, "c1")
	captured := h.captured(t)
	pu := srv.URL + "/v1/payments/" + pending.ID.String()
	cu := srv.URL + "/v1/payments/" + captured.ID.String()

	for _, c := range []struct {
		name, method, url, key, body string
		want                         int
	}{
		{"capture before authorize", "POST", pu + "/capture", "", "", 409},
		{"refund before capture", "POST", pu + "/refund", "k", "", 409},
		{"void captured", "POST", cu + "/void", "", "", 409},
		{"unknown payment", "POST", srv.URL + "/v1/payments/00000000-0000-0000-0000-000000000000/capture", "", "", 404},
		{"bad payment id", "POST", srv.URL + "/v1/payments/nope/capture", "", "", 400},
		{"refund without key", "POST", cu + "/refund", "", "", 400},
		{"refund bad json", "POST", cu + "/refund", "k1", `{"amount_paise":"x"}`, 400},
		{"refund unknown field", "POST", cu + "/refund", "k1", `{"amount":5}`, 400},
		{"refund zero", "POST", cu + "/refund", "k1", `{"amount_paise":0}`, 400},
		{"refund too much", "POST", cu + "/refund", "k2", `{"amount_paise":100001}`, 422},
		{"refund accepted", "POST", cu + "/refund", "k3", `{"amount_paise":40000}`, 202},
		{"refund replay", "POST", cu + "/refund", "k3", `{"amount_paise":40000}`, 200},
		{"refund key reuse", "POST", cu + "/refund", "k3", `{"amount_paise":1}`, 422},
		{"refund remainder accepted", "POST", cu + "/refund", "k4", "", 202},
		{"unknown operation", "GET", srv.URL + "/v1/operations/00000000-0000-0000-0000-000000000000", "", "", 404},
		{"bad operation id", "GET", srv.URL + "/v1/operations/x", "", "", 400},
	} {
		if resp, body := call(t, c.method, c.url, c.key, c.body); resp.StatusCode != c.want {
			t.Errorf("%s: got %d, want %d (%s)", c.name, resp.StatusCode, c.want, body)
		}
	}
	// The replay flagged itself.
	resp, _ := call(t, "POST", cu+"/refund", "k3", `{"amount_paise":40000}`)
	if resp.Header.Get("Idempotent-Replayed") != "true" {
		t.Error("missing Idempotent-Replayed header")
	}
}
