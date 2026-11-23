package reconcile_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/sayandip18/settlement-engine/internal/httpapi"
	"github.com/sayandip18/settlement-engine/internal/ledger"
	"github.com/sayandip18/settlement-engine/internal/reconcile"
)

// finding runs the reconciler and returns the single open finding's ID.
func (e env) finding(t *testing.T) uuid.UUID {
	t.Helper()
	e.run(t)
	var id uuid.UUID
	if err := e.pool.QueryRow(ctx, `SELECT id FROM discrepancies WHERE status = 'OPEN'`).Scan(&id); err != nil {
		t.Fatalf("want exactly one open finding: %v", err)
	}
	return id
}

func TestAcknowledgeAndResolve(t *testing.T) {
	e := setup(t)
	e.payment(t, "CAPTURED", 100000, 0, 2000, 0)
	id := e.finding(t)

	if _, err := e.svc.Resolve(ctx, id, "  "); err == nil {
		t.Fatal("resolve without a note must be rejected")
	} else if ve := new(reconcile.ValidationError); !errors.As(err, &ve) {
		t.Fatalf("want ValidationError, got %v", err)
	}

	d, err := e.svc.Acknowledge(ctx, id, "on it")
	if err != nil || d.Status != reconcile.StatusAcknowledged || d.AckNote == nil || *d.AckNote != "on it" || d.AcknowledgedAt == nil {
		t.Fatalf("ack: %+v %v", d, err)
	}
	if _, err := e.svc.Acknowledge(ctx, id, ""); !isHandled(err) {
		t.Fatalf("second ack should conflict, got %v", err)
	}

	// An acknowledged finding is still refreshed by later runs rather than duplicated.
	e.run(t)
	if n := e.count(t, `SELECT count(*) FROM discrepancies`); n != 1 {
		t.Fatalf("rows = %d", n)
	}

	d, err = e.svc.Resolve(ctx, id, "booked manually in the bank ledger")
	if err != nil || d.Status != reconcile.StatusResolved || d.Resolution == nil || *d.Resolution != "OPERATOR" ||
		d.ResolutionNote == nil || d.ResolvedAt == nil {
		t.Fatalf("resolve: %+v %v", d, err)
	}
	if _, err := e.svc.Resolve(ctx, id, "again"); !isHandled(err) {
		t.Fatalf("second resolve should conflict, got %v", err)
	}
	if _, err := e.svc.Acknowledge(ctx, id, ""); !isHandled(err) {
		t.Fatalf("ack after resolve should conflict, got %v", err)
	}
	if _, err := e.svc.Get(ctx, uuid.New()); !errors.Is(err, reconcile.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if _, err := e.svc.Resolve(ctx, uuid.New(), "x"); !errors.Is(err, reconcile.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func isHandled(err error) bool {
	var h *reconcile.AlreadyHandledError
	return errors.As(err, &h)
}

func TestListFiltersAndPages(t *testing.T) {
	e := setup(t)
	for i := 0; i < 5; i++ {
		e.payment(t, "CAPTURED", 100, 0, 0, 0)
	}
	e.payment(t, "PENDING", 100, 0, 0, 5*3600*1e9) // stuck
	e.run(t)

	var seen int
	var cursor int64
	for {
		items, next, err := e.svc.List(ctx, reconcile.ListFilter{Limit: 2, Before: cursor})
		if err != nil {
			t.Fatal(err)
		}
		seen += len(items)
		if next == 0 {
			break
		}
		cursor = next
	}
	if seen != 6 {
		t.Fatalf("paged through %d, want 6", seen)
	}
	typ := reconcile.TypePaymentStuck
	items, _, err := e.svc.List(ctx, reconcile.ListFilter{Type: &typ})
	if err != nil || len(items) != 1 {
		t.Fatalf("type filter: %d %v", len(items), err)
	}
	st := reconcile.StatusResolved
	items, _, _ = e.svc.List(ctx, reconcile.ListFilter{Status: &st})
	if len(items) != 0 {
		t.Fatalf("status filter: %d", len(items))
	}
}

func (e env) ledgerTxns(t *testing.T, id uuid.UUID, kind string) int {
	return e.count(t, `SELECT count(*) FROM ledger_transactions WHERE payment_id = $1 AND kind = $2`, id, kind)
}

func TestCorrectMissingCaptureRepostsCapture(t *testing.T) {
	e := setup(t)
	pid := e.payment(t, "CAPTURED", 100000, 0, 2000, 0)
	id := e.finding(t)

	if _, err := e.svc.Correct(ctx, id, "x", []reconcile.CorrectionEntry{{Account: "CUSTOMER_FUNDS", Direction: ledger.Debit, AmountPaise: 1}}); !isValidation(err) {
		t.Fatalf("entries must be refused for a missing capture, got %v", err)
	}
	if _, err := e.svc.Correct(ctx, id, "", nil); !isValidation(err) {
		t.Fatalf("note is required, got %v", err)
	}
	if n := e.ledgerTxns(t, pid, "CAPTURE"); n != 0 {
		t.Fatalf("rejected corrections posted %d txns", n)
	}

	d, err := e.svc.Correct(ctx, id, "ledger post lost in the incident of 11-20", nil)
	if err != nil {
		t.Fatal(err)
	}
	if d.Status != reconcile.StatusResolved || d.Resolution == nil || *d.Resolution != "CORRECTED" || d.LedgerTxnID == nil {
		t.Fatalf("%+v", d)
	}
	if n := e.ledgerTxns(t, pid, "CAPTURE"); n != 1 {
		t.Fatalf("capture txns = %d", n)
	}
	// 98000 to the merchant, 2000 fee: exactly what a normal capture posts.
	if n := e.count(t, `SELECT amount_paise FROM ledger_entries WHERE txn_id = $1 AND account_code = 'MERCHANT_PAYABLE:m1' AND direction = 'CREDIT'`, *d.LedgerTxnID); n != 98000 {
		t.Fatalf("merchant credit = %d", n)
	}

	e.run(t)
	if got := e.active(t); len(got) != 0 {
		t.Fatalf("finding persists after correction: %v", got)
	}
	if _, err := e.svc.Correct(ctx, id, "again", nil); !isHandled(err) {
		t.Fatalf("second correction should conflict, got %v", err)
	}
}

func isValidation(err error) bool {
	var v *reconcile.ValidationError
	return errors.As(err, &v)
}

func TestCorrectMismatch(t *testing.T) {
	setupMismatch := func(t *testing.T) (env, uuid.UUID, uuid.UUID) {
		e := setup(t)
		pid := e.captured(t)
		// The refund was recorded on the payment but its ledger posting never happened.
		e.exec(t, `UPDATE payments SET refunded_paise = 30000, status = 'PARTIALLY_REFUNDED' WHERE id = $1`, pid)
		return e, pid, e.finding(t)
	}
	good := []reconcile.CorrectionEntry{
		{Account: "MERCHANT_PAYABLE:m1", Direction: ledger.Debit, AmountPaise: 29400},
		{Account: "PLATFORM_FEES", Direction: ledger.Debit, AmountPaise: 600},
		{Account: "CUSTOMER_FUNDS", Direction: ledger.Credit, AmountPaise: 30000},
	}

	t.Run("correct entries fix it", func(t *testing.T) {
		e, pid, id := setupMismatch(t)
		d, err := e.svc.Correct(ctx, id, "refund r-17 was never posted", good)
		if err != nil {
			t.Fatal(err)
		}
		if d.LedgerTxnID == nil || e.ledgerTxns(t, pid, "ADJUSTMENT") != 1 {
			t.Fatalf("no adjustment: %+v", d)
		}
		var ref string
		e.pool.QueryRow(ctx, `SELECT ref_id FROM ledger_transactions WHERE id = $1`, *d.LedgerTxnID).Scan(&ref) //nolint:errcheck
		if ref != id.String() {
			t.Fatalf("ref_id = %s, want the discrepancy id", ref)
		}
		e.run(t)
		if got := e.active(t); len(got) != 0 {
			t.Fatalf("still flagged: %v", got)
		}
	})

	t.Run("wrong amount is rejected and nothing is posted", func(t *testing.T) {
		e, pid, id := setupMismatch(t)
		bad := []reconcile.CorrectionEntry{
			{Account: "MERCHANT_PAYABLE:m1", Direction: ledger.Debit, AmountPaise: 20000},
			{Account: "CUSTOMER_FUNDS", Direction: ledger.Credit, AmountPaise: 20000},
		}
		_, err := e.svc.Correct(ctx, id, "oops", bad)
		var rej *reconcile.CorrectionRejectedError
		if !errors.As(err, &rej) {
			t.Fatalf("want CorrectionRejectedError, got %v", err)
		}
		if n := e.ledgerTxns(t, pid, "ADJUSTMENT"); n != 0 {
			t.Fatalf("rejected correction left %d adjustments", n)
		}
		if n := e.count(t, `SELECT count(*) FROM discrepancies WHERE id = $1 AND status = 'OPEN'`, id); n != 1 {
			t.Fatal("finding was resolved by a rejected correction")
		}
		// The right correction still works afterwards.
		if _, err := e.svc.Correct(ctx, id, "retry", good); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("invalid entries", func(t *testing.T) {
		e, _, id := setupMismatch(t)
		for name, entries := range map[string][]reconcile.CorrectionEntry{
			"missing":         nil,
			"unbalanced":      {{Account: "CUSTOMER_FUNDS", Direction: ledger.Credit, AmountPaise: 30000}, {Account: "PLATFORM_FEES", Direction: ledger.Debit, AmountPaise: 1}},
			"other merchant":  {{Account: "MERCHANT_PAYABLE:m2", Direction: ledger.Debit, AmountPaise: 30000}, {Account: "CUSTOMER_FUNDS", Direction: ledger.Credit, AmountPaise: 30000}},
			"unknown account": {{Account: "SUSPENSE", Direction: ledger.Debit, AmountPaise: 30000}, {Account: "CUSTOMER_FUNDS", Direction: ledger.Credit, AmountPaise: 30000}},
			"bad direction":   {{Account: "CUSTOMER_FUNDS", Direction: "SIDEWAYS", AmountPaise: 30000}, {Account: "PLATFORM_FEES", Direction: ledger.Debit, AmountPaise: 30000}},
			"zero amount":     {{Account: "CUSTOMER_FUNDS", Direction: ledger.Credit, AmountPaise: 0}, {Account: "PLATFORM_FEES", Direction: ledger.Debit, AmountPaise: 0}},
		} {
			if _, err := e.svc.Correct(ctx, id, "n", entries); !isValidation(err) {
				t.Errorf("%s: want ValidationError, got %v", name, err)
			}
		}
		if n := e.count(t, `SELECT count(*) FROM ledger_transactions WHERE kind = 'ADJUSTMENT'`); n != 0 {
			t.Fatalf("invalid corrections posted %d adjustments", n)
		}
	})
}

func TestCorrectLedgerOnUncapturedReversesIt(t *testing.T) {
	e := setup(t)
	pid := e.payment(t, "FAILED", 100000, 0, 2000, 0)
	e.post(t, ledger.Capture(pid, "m1", 100000, 2000))
	id := e.finding(t)

	partial := []reconcile.CorrectionEntry{
		{Account: "CUSTOMER_FUNDS", Direction: ledger.Credit, AmountPaise: 100000},
		{Account: "MERCHANT_PAYABLE:m1", Direction: ledger.Debit, AmountPaise: 100000},
	}
	if _, err := e.svc.Correct(ctx, id, "n", partial); err == nil {
		t.Fatal("a reversal leaving the fee behind must be rejected")
	}
	full := []reconcile.CorrectionEntry{
		{Account: "CUSTOMER_FUNDS", Direction: ledger.Credit, AmountPaise: 100000},
		{Account: "MERCHANT_PAYABLE:m1", Direction: ledger.Debit, AmountPaise: 98000},
		{Account: "PLATFORM_FEES", Direction: ledger.Debit, AmountPaise: 2000},
	}
	if _, err := e.svc.Correct(ctx, id, "reverse the stray capture", full); err != nil {
		t.Fatal(err)
	}
	e.run(t)
	if got := e.active(t); len(got) != 0 {
		t.Fatalf("a fully reversed payment is still flagged: %v", got)
	}
}

func TestCorrectNotAvailableForOperationalFindings(t *testing.T) {
	e := setup(t)
	e.payment(t, "PENDING", 100, 0, 0, 5*3600*1e9)
	id := e.finding(t)
	_, err := e.svc.Correct(ctx, id, "n", nil)
	var nc *reconcile.NotCorrectableError
	if !errors.As(err, &nc) {
		t.Fatalf("want NotCorrectableError, got %v", err)
	}
}

// Many operators clicking at once: exactly one wins, and the ledger is posted once.
func TestConcurrentCorrectPostsOnce(t *testing.T) {
	e := setup(t)
	pid := e.payment(t, "CAPTURED", 100000, 0, 2000, 0)
	id := e.finding(t)

	var wg sync.WaitGroup
	results := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.svc.Correct(ctx, id, "fix", nil)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	var ok int
	for err := range results {
		switch {
		case err == nil:
			ok++
		case !isHandled(err):
			t.Errorf("unexpected error: %v", err)
		}
	}
	if ok != 1 {
		t.Fatalf("%d corrections succeeded, want exactly 1", ok)
	}
	if n := e.ledgerTxns(t, pid, "CAPTURE"); n != 1 {
		t.Fatalf("capture posted %d times", n)
	}
}

func TestConcurrentAcknowledgeAndResolve(t *testing.T) {
	e := setup(t)
	e.payment(t, "CAPTURED", 100, 0, 0, 0)
	id := e.finding(t)

	var wg sync.WaitGroup
	acks, resolves := make(chan error, 8), make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _, err := e.svc.Acknowledge(ctx, id, ""); acks <- err }()
		go func() { defer wg.Done(); _, err := e.svc.Resolve(ctx, id, "done"); resolves <- err }()
	}
	wg.Wait()
	close(acks)
	close(resolves)
	count := func(c chan error, name string) int {
		n := 0
		for err := range c {
			switch {
			case err == nil:
				n++
			case !isHandled(err):
				t.Errorf("%s: %v", name, err)
			}
		}
		return n
	}
	if n := count(resolves, "resolve"); n != 1 {
		t.Fatalf("%d resolves succeeded, want 1", n)
	}
	if n := count(acks, "ack"); n > 1 {
		t.Fatalf("%d acks succeeded, want at most 1", n)
	}
	if n := e.count(t, `SELECT count(*) FROM discrepancies WHERE status = 'RESOLVED'`); n != 1 {
		t.Fatal("finding should end up resolved")
	}
}

// ---- HTTP ----

type api struct {
	t *testing.T
	h http.Handler
}

func newAPI(t *testing.T, e env) api {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := reconcile.NewHandler(e.svc, log)
	return api{t, httpapi.NewRouter(log, httpapi.NewHealth(nil), "secret", nil, h.Routes)}
}

func (a api) do(method, path string, body any, key string) (int, map[string]any) {
	a.t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, r)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rec := httptest.NewRecorder()
	a.h.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func TestHTTP(t *testing.T) {
	e := setup(t)
	a := newAPI(t, e)
	pid := e.payment(t, "CAPTURED", 100000, 0, 2000, 0)

	for _, p := range []string{"GET /admin/reconciliation/discrepancies", "POST /admin/reconciliation/run"} {
		method, path, _ := strings.Cut(p, " ")
		if code, _ := a.do(method, path, nil, ""); code != http.StatusUnauthorized {
			t.Errorf("%s without key: %d", p, code)
		}
		if code, _ := a.do(method, path, nil, "wrong"); code != http.StatusUnauthorized {
			t.Errorf("%s wrong key: %d", p, code)
		}
	}

	code, res := a.do("POST", "/admin/reconciliation/run", nil, "secret")
	if code != 200 || len(res["checks"].([]any)) != 10 {
		t.Fatalf("run: %d %v", code, res)
	}

	code, list := a.do("GET", "/admin/reconciliation/discrepancies?status=OPEN&type=CAPTURE_LEDGER_MISSING", nil, "secret")
	items := list["items"].([]any)
	if code != 200 || len(items) != 1 {
		t.Fatalf("list: %d %v", code, list)
	}
	first := items[0].(map[string]any)
	id := first["id"].(string)
	if first["payment_id"] != pid.String() || first["status"] != "OPEN" {
		t.Fatalf("%v", first)
	}

	for _, q := range []string{"status=NOPE", "type=NOPE", "limit=0", "limit=9999", "cursor=abc"} {
		if code, _ := a.do("GET", "/admin/reconciliation/discrepancies?"+q, nil, "secret"); code != 400 {
			t.Errorf("%s: %d", q, code)
		}
	}
	if code, _ := a.do("GET", "/admin/reconciliation/discrepancies/not-a-uuid", nil, "secret"); code != 400 {
		t.Errorf("bad id: %d", code)
	}
	if code, _ := a.do("GET", "/admin/reconciliation/discrepancies/"+uuid.NewString(), nil, "secret"); code != 404 {
		t.Errorf("unknown id: %d", code)
	}

	if code, _ := a.do("POST", "/admin/reconciliation/discrepancies/"+id+"/resolve", map[string]any{}, "secret"); code != 400 {
		t.Errorf("resolve without note: %d", code)
	}
	if code, _ := a.do("POST", "/admin/reconciliation/discrepancies/"+id+"/resolve", map[string]any{"note": "n", "bogus": 1}, "secret"); code != 400 {
		t.Errorf("unknown field: %d", code)
	}
	if code, _ := a.do("POST", "/admin/reconciliation/discrepancies/"+id+"/resolve", map[string]any{"note": "n", "entries": []any{}}, "secret"); code != 400 {
		t.Errorf("entries on resolve: %d", code)
	}
	if code, body := a.do("POST", "/admin/reconciliation/discrepancies/"+id+"/acknowledge", map[string]any{"note": "mine"}, "secret"); code != 200 || body["status"] != "ACKNOWLEDGED" {
		t.Errorf("ack: %d %v", code, body)
	}
	if code, _ := a.do("POST", "/admin/reconciliation/discrepancies/"+id+"/acknowledge", nil, "secret"); code != 409 {
		t.Errorf("second ack: %d", code)
	}
	if code, body := a.do("POST", "/admin/reconciliation/discrepancies/"+id+"/correct", map[string]any{"note": "repost"}, "secret"); code != 200 || body["resolution"] != "CORRECTED" {
		t.Errorf("correct: %d %v", code, body)
	}
	if code, _ := a.do("POST", "/admin/reconciliation/discrepancies/"+id+"/correct", map[string]any{"note": "repost"}, "secret"); code != 409 {
		t.Errorf("second correct: %d", code)
	}

	// A mismatch with a wrong correction is 422.
	e.exec(t, `UPDATE payments SET refunded_paise = 30000, status = 'PARTIALLY_REFUNDED' WHERE id = $1`, pid)
	e.run(t)
	var mid string
	if err := e.pool.QueryRow(ctx, `SELECT id FROM discrepancies WHERE status = 'OPEN'`).Scan(&mid); err != nil {
		t.Fatal(err)
	}
	wrong := map[string]any{"note": "n", "entries": []map[string]any{
		{"account": "CUSTOMER_FUNDS", "direction": "CREDIT", "amount_paise": 1},
		{"account": "PLATFORM_FEES", "direction": "DEBIT", "amount_paise": 1}}}
	if code, body := a.do("POST", "/admin/reconciliation/discrepancies/"+mid+"/correct", wrong, "secret"); code != 422 {
		t.Errorf("wrong correction: %d %v", code, body)
	}

	// Operational findings are 422 for correct.
	e.payment(t, "PENDING", 100, 0, 0, 5*3600*1e9)
	e.run(t)
	var sid string
	if err := e.pool.QueryRow(ctx, `SELECT id FROM discrepancies WHERE status = 'OPEN' AND type = 'PAYMENT_STUCK'`).Scan(&sid); err != nil {
		t.Fatal(err)
	}
	if code, _ := a.do("POST", "/admin/reconciliation/discrepancies/"+sid+"/correct", map[string]any{"note": "n"}, "secret"); code != 422 {
		t.Errorf("operational correct: %d", code)
	}
}
