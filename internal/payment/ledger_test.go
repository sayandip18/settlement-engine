package payment_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/sayandip18/settlement-engine/internal/httpapi"
	"github.com/sayandip18/settlement-engine/internal/ledger"
	"github.com/sayandip18/settlement-engine/internal/merchant"
	"github.com/sayandip18/settlement-engine/internal/payment"
)

// newPaymentFor creates a PENDING payment for an arbitrary merchant and amount.
func (e env) newPaymentFor(t *testing.T, merchantID string, amount int64, key string) payment.Payment {
	t.Helper()
	res, err := e.svc.Create(context.Background(), payment.CreateInput{
		MerchantID: merchantID, CustomerID: "c1", AmountPaise: amount, IdempotencyKey: key,
	})
	if err != nil {
		t.Fatal(err)
	}
	return res.Payment
}

func (e env) capturedFor(t *testing.T, merchantID string, amount int64, key string) payment.Payment {
	t.Helper()
	ctx := context.Background()
	p := e.newPaymentFor(t, merchantID, amount, key)
	if _, err := e.svc.Authorize(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	p, err := e.svc.Capture(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func (e env) setFee(t *testing.T, merchantID string, bps int) {
	t.Helper()
	if _, err := e.pool.Exec(context.Background(),
		`INSERT INTO merchants (id, fee_bps) VALUES ($1, $2)
		 ON CONFLICT (id) DO UPDATE SET fee_bps = EXCLUDED.fee_bps`, merchantID, bps); err != nil {
		t.Fatal(err)
	}
}

// ledgerLines returns "DIRECTION ACCOUNT amount" for every entry of the given
// kind for a payment, sorted so the assertion does not depend on insert order.
func (e env) ledgerLines(t *testing.T, paymentID uuid.UUID, kind string) []string {
	t.Helper()
	rows, err := e.pool.Query(context.Background(),
		`SELECT e.direction, e.account_code, e.amount_paise
		   FROM ledger_entries e JOIN ledger_transactions t ON t.id = e.txn_id
		  WHERE t.payment_id = $1 AND t.kind = $2`, paymentID, kind)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var dir, acct string
		var amt int64
		if err := rows.Scan(&dir, &acct, &amt); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%s %s %d", dir, acct, amt))
	}
	sort.Strings(out)
	return out
}

func (e env) ledgerTxnCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), `SELECT count(*) FROM ledger_transactions`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// netBalances returns debits - credits per account, excluding accounts that net to zero.
func (e env) netBalances(t *testing.T) map[string]int64 {
	t.Helper()
	rows, err := e.pool.Query(context.Background(),
		`SELECT account_code,
		        SUM(CASE direction WHEN 'DEBIT' THEN amount_paise ELSE -amount_paise END)
		   FROM ledger_entries GROUP BY account_code`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var code string
		var net int64
		if err := rows.Scan(&code, &net); err != nil {
			t.Fatal(err)
		}
		if net != 0 {
			out[code] = net
		}
	}
	return out
}

func assertLines(t *testing.T, got []string, want ...string) {
	t.Helper()
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ledger lines:\n got  %v\n want %v", got, want)
	}
}

func TestCaptureRecordsBalancedEntriesAtDefaultFee(t *testing.T) {
	e := setup(t)
	p := e.capturedFor(t, "m1", 100000, "k") // 2% default

	if p.FeePaise != 2000 {
		t.Fatalf("fee = %d, want 2000", p.FeePaise)
	}
	assertLines(t, e.ledgerLines(t, p.ID, "CAPTURE"),
		"DEBIT CUSTOMER_FUNDS 100000",
		"CREDIT MERCHANT_PAYABLE:m1 98000",
		"CREDIT PLATFORM_FEES 2000")
	stored, _ := e.svc.Get(context.Background(), p.ID)
	if stored.FeePaise != 2000 {
		t.Fatalf("fee not persisted: %d", stored.FeePaise)
	}
}

func TestMerchantSpecificFee(t *testing.T) {
	e := setup(t)
	e.setFee(t, "m1", 250)
	p := e.capturedFor(t, "m1", 100000, "k1")
	other := e.capturedFor(t, "m2", 100000, "k2")
	if p.FeePaise != 2500 || other.FeePaise != 2000 {
		t.Fatalf("m1 fee=%d (want 2500), m2 fee=%d (want 2000)", p.FeePaise, other.FeePaise)
	}
}

func TestZeroFeeMerchant(t *testing.T) {
	e := setup(t)
	e.setFee(t, "m1", 0)
	p := e.capturedFor(t, "m1", 100000, "k")
	assertLines(t, e.ledgerLines(t, p.ID, "CAPTURE"),
		"DEBIT CUSTOMER_FUNDS 100000", "CREDIT MERCHANT_PAYABLE:m1 100000")
	if _, err := e.svc.Refund(context.Background(), payment.RefundInput{PaymentID: p.ID, IdempotencyKey: "r"}); err != nil {
		t.Fatal(err)
	}
	if bal := e.netBalances(t); len(bal) != 0 {
		t.Fatalf("accounts should net to zero after full refund: %v", bal)
	}
}

func TestFeeRoundsUpInPlatformFavour(t *testing.T) {
	e := setup(t)
	// 2% of 333 paise = 6.66 -> 7
	p := e.capturedFor(t, "m1", 333, "k")
	if p.FeePaise != 7 {
		t.Fatalf("fee = %d, want 7", p.FeePaise)
	}
	// 2% of 10 paise = 0.2 -> 1: never rounds to a free payment while a fee is set.
	q := e.capturedFor(t, "m1", 10, "k2")
	if q.FeePaise != 1 {
		t.Fatalf("fee = %d, want 1", q.FeePaise)
	}
}

func TestPartialRefundEntriesSplitFeeProportionally(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.capturedFor(t, "m1", 100000, "k")

	res, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, AmountPaise: ptr(25000), IdempotencyKey: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	// 25% refunded: 500 of the 2000 fee comes back, 24500 from the merchant.
	assertLines(t, e.ledgerLines(t, res.Payment.ID, "REFUND"),
		"DEBIT MERCHANT_PAYABLE:m1 24500",
		"DEBIT PLATFORM_FEES 500",
		"CREDIT CUSTOMER_FUNDS 25000")
	assertLines(t, e.ledgerLines(t, p.ID, "CAPTURE"), // capture history untouched
		"DEBIT CUSTOMER_FUNDS 100000", "CREDIT MERCHANT_PAYABLE:m1 98000", "CREDIT PLATFORM_FEES 2000")
	want := map[string]int64{ // debit-positive net
		ledger.AccountCustomerFunds:  75000,
		ledger.MerchantPayable("m1"): -73500,
		ledger.AccountPlatformFees:   -1500,
	}
	if got := e.netBalances(t); !reflect.DeepEqual(got, want) {
		t.Fatalf("balances = %v, want %v", got, want)
	}
}

func TestRoundingNeverLeaksPaise(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.capturedFor(t, "m1", 333, "k") // fee 7
	for i, amt := range []int64{100, 100, 133} {
		if _, err := e.svc.Refund(ctx, payment.RefundInput{
			PaymentID: p.ID, AmountPaise: ptr(amt), IdempotencyKey: fmt.Sprint("r", i)}); err != nil {
			t.Fatal(err)
		}
	}
	// Cumulative shares floor(7*100/333)=2, floor(7*200/333)=4, 7: so 2 + 2 + 3.
	var feeShares []int64
	rows, err := e.pool.Query(ctx,
		`SELECT e.amount_paise FROM ledger_entries e JOIN ledger_transactions t ON t.id = e.txn_id
		  WHERE t.kind = 'REFUND' AND e.account_code = 'PLATFORM_FEES' ORDER BY e.id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var a int64
		rows.Scan(&a)
		feeShares = append(feeShares, a)
	}
	if !reflect.DeepEqual(feeShares, []int64{2, 2, 3}) {
		t.Fatalf("fee shares = %v, want [2 2 3]", feeShares)
	}
	// Fully refunded: every account is back to exactly zero.
	if bal := e.netBalances(t); len(bal) != 0 {
		t.Fatalf("accounts should net to zero: %v", bal)
	}
}

func TestRefundUsesFeeFrozenAtCapture(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	e.setFee(t, "m1", 500)
	p := e.capturedFor(t, "m1", 100000, "k") // fee 5000
	e.setFee(t, "m1", 0)                     // later rate change must not matter

	res, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, AmountPaise: ptr(50000), IdempotencyKey: "r"})
	if err != nil {
		t.Fatal(err)
	}
	assertLines(t, e.ledgerLines(t, res.Payment.ID, "REFUND"),
		"DEBIT MERCHANT_PAYABLE:m1 47500", "DEBIT PLATFORM_FEES 2500", "CREDIT CUSTOMER_FUNDS 50000")
}

func TestOnlyMoneyMovementsWriteToLedger(t *testing.T) {
	e := setup(t)
	ctx := context.Background()

	// authorize, then void
	v := e.newPayment(t, "c1", "k1")
	e.svc.Authorize(ctx, v.ID)
	e.svc.Void(ctx, v.ID)
	// authorize declined
	d := e.newPayment(t, "decline_1", "k2")
	e.svc.Authorize(ctx, d.ID)
	// transient failure
	tm := e.newPayment(t, "timeout_1", "k3")
	e.svc.Authorize(ctx, tm.ID)
	// capture declined
	cd := e.newPayment(t, "capture_decline_1", "k4")
	e.svc.Authorize(ctx, cd.ID)
	e.svc.Capture(ctx, cd.ID)
	// refund declined
	rd := e.newPayment(t, "refund_decline_1", "k5")
	e.svc.Authorize(ctx, rd.ID)
	e.svc.Capture(ctx, rd.ID)
	before := e.ledgerTxnCount(t) // only rd's capture
	e.svc.Refund(ctx, payment.RefundInput{PaymentID: rd.ID, IdempotencyKey: "r"})

	if before != 1 || e.ledgerTxnCount(t) != 1 {
		t.Fatalf("ledger transactions: before refund=%d after=%d, want 1 and 1", before, e.ledgerTxnCount(t))
	}
}

func TestRepeatedRequestsDoNotPostTwice(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.newPayment(t, "c1", "k")
	e.svc.Authorize(ctx, p.ID)
	for i := 0; i < 3; i++ {
		if _, err := e.svc.Capture(ctx, p.ID); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: p.ID, AmountPaise: ptr(1000), IdempotencyKey: "r"}); err != nil {
			t.Fatal(err)
		}
	}
	if n := e.ledgerTxnCount(t); n != 2 { // one capture + one refund
		t.Fatalf("%d ledger transactions, want 2", n)
	}
}

func TestLedgerFailureRollsBackCapture(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	p := e.newPayment(t, "c1", "k")
	e.svc.Authorize(ctx, p.ID)

	// Sabotage: occupy the (CAPTURE, payment id) slot so the real posting collides.
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.Post(ctx, tx, ledger.Capture(p.ID, "m1", 100000, 2000)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := e.svc.Capture(ctx, p.ID); err == nil {
		t.Fatal("capture succeeded although its ledger posting failed")
	}
	stored, _ := e.svc.Get(ctx, p.ID)
	if stored.Status != payment.StatusAuthorized || stored.FeePaise != 0 || stored.CapturedAt != nil {
		t.Fatalf("payment changed despite ledger failure: %+v", stored)
	}
}

func TestLedgerAlwaysBalancesAcrossManyPayments(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	e.setFee(t, "m2", 175)
	amounts := []int64{1, 99, 333, 1000, 12345, 99999}
	for i, amt := range amounts {
		m := []string{"m1", "m2"}[i%2]
		p := e.capturedFor(t, m, amt, fmt.Sprint("k", i))
		if amt > 2 {
			if _, err := e.svc.Refund(ctx, payment.RefundInput{
				PaymentID: p.ID, AmountPaise: ptr(amt / 3), IdempotencyKey: "r"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	var debits, credits int64
	if err := e.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(amount_paise) FILTER (WHERE direction='DEBIT'),0),
		        COALESCE(SUM(amount_paise) FILTER (WHERE direction='CREDIT'),0) FROM ledger_entries`,
	).Scan(&debits, &credits); err != nil {
		t.Fatal(err)
	}
	if debits == 0 || debits != credits {
		t.Fatalf("debits=%d credits=%d", debits, credits)
	}
}

func TestPayoutSummary(t *testing.T) {
	e := setup(t)
	ctx := context.Background()

	a := e.capturedFor(t, "m1", 100000, "k1") // fee 2000
	e.capturedFor(t, "m1", 50000, "k2")       // fee 1000
	e.capturedFor(t, "m2", 10000, "k3")       // fee 200
	if _, err := e.svc.Refund(ctx, payment.RefundInput{PaymentID: a.ID, AmountPaise: ptr(25000), IdempotencyKey: "r"}); err != nil {
		t.Fatal(err)
	}

	from, to := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	s, err := ledger.Summary(ctx, e.pool, "m1", from, to)
	if err != nil {
		t.Fatal(err)
	}
	// fees: 2000 + 1000 - 500 refunded share
	if s.GrossCollectedPaise != 150000 || s.RefundedPaise != 25000 || s.FeesPaise != 2500 || s.NetPayoutPaise != 122500 {
		t.Fatalf("m1 summary: %+v", s)
	}
	if s.GrossCollectedPaise-s.RefundedPaise-s.FeesPaise != s.NetPayoutPaise {
		t.Fatalf("summary does not add up: %+v", s)
	}

	m2, _ := ledger.Summary(ctx, e.pool, "m2", from, to)
	if m2.GrossCollectedPaise != 10000 || m2.FeesPaise != 200 || m2.NetPayoutPaise != 9800 || m2.RefundedPaise != 0 {
		t.Fatalf("m2 summary: %+v", m2)
	}

	// Outside the window and for an unknown merchant: all zero.
	for _, q := range []struct {
		m        string
		from, to time.Time
	}{
		{"m1", time.Now().Add(time.Hour), time.Now().Add(2 * time.Hour)},
		{"nobody", from, to},
	} {
		z, err := ledger.Summary(ctx, e.pool, q.m, q.from, q.to)
		if err != nil || z.GrossCollectedPaise != 0 || z.NetPayoutPaise != 0 || z.FeesPaise != 0 || z.RefundedPaise != 0 {
			t.Fatalf("expected zeros for %+v: %+v %v", q, z, err)
		}
	}
}

// --- HTTP: payout summary and admin fee endpoints ---

func fullServer(t *testing.T, e env) *httptest.Server {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	health := httpapi.NewHealth(nil)
	router := httpapi.NewRouter(log, health, "admin-secret",
		func(r chi.Router) {
			payment.NewHandler(e.svc, log).Routes(r)
			ledger.NewHandler(e.pool, log).Routes(r)
		},
		merchant.NewHandler(e.pool, 200, log).Routes)
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)
	return srv
}

func call(t *testing.T, method, url, auth, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestHTTPPayoutSummary(t *testing.T) {
	e := setup(t)
	srv := fullServer(t, e)
	e.capturedFor(t, "m1", 100000, "k")

	from := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	to := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	code, body := call(t, "GET", srv.URL+"/v1/merchants/m1/payouts/summary?from="+from+"&to="+to, "", "")
	if code != http.StatusOK {
		t.Fatalf("got %d: %s", code, body)
	}
	var s ledger.PayoutSummary
	if err := json.Unmarshal([]byte(body), &s); err != nil {
		t.Fatal(err)
	}
	if s.GrossCollectedPaise != 100000 || s.FeesPaise != 2000 || s.NetPayoutPaise != 98000 || s.Currency != "INR" {
		t.Fatalf("summary: %+v", s)
	}

	for name, q := range map[string]string{
		"missing params": "",
		"bad date":       "?from=yesterday&to=" + to,
		"reversed":       "?from=" + to + "&to=" + from,
	} {
		if code, _ := call(t, "GET", srv.URL+"/v1/merchants/m1/payouts/summary"+q, "", ""); code != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400", name, code)
		}
	}
}

func TestHTTPAdminFeeEndpoint(t *testing.T) {
	e := setup(t)
	srv := fullServer(t, e)
	u := srv.URL + "/admin/merchants/m1/fee"

	if code, _ := call(t, "GET", u, "", ""); code != http.StatusUnauthorized {
		t.Fatalf("no auth: got %d", code)
	}
	if code, _ := call(t, "PUT", u, "Bearer wrong", `{"fee_bps":100}`); code != http.StatusUnauthorized {
		t.Fatalf("bad auth: got %d", code)
	}
	auth := "Bearer admin-secret"

	if code, body := call(t, "GET", u, auth, ""); code != 200 || !strings.Contains(body, `"fee_bps":200`) {
		t.Fatalf("default fee: %d %s", code, body)
	}
	for name, b := range map[string]string{
		"missing":  `{}`,
		"negative": `{"fee_bps":-1}`,
		"over 100": `{"fee_bps":10001}`,
		"not int":  `{"fee_bps":"x"}`,
		"unknown":  `{"fee_bps":1,"extra":2}`,
	} {
		if code, _ := call(t, "PUT", u, auth, b); code != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400", name, code)
		}
	}
	if code, _ := call(t, "PUT", u, auth, `{"fee_bps":350}`); code != http.StatusOK {
		t.Fatalf("set fee: got %d", code)
	}
	bps, err := merchant.FeeBPS(context.Background(), e.pool, "m1", 200)
	if err != nil || bps != 350 {
		t.Fatalf("stored fee = %d, %v", bps, err)
	}

	// The new rate applies to the next capture.
	p := e.capturedFor(t, "m1", 100000, "k")
	if p.FeePaise != 3500 {
		t.Fatalf("fee = %d, want 3500", p.FeePaise)
	}
}
