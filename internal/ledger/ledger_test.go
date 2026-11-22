package ledger_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sayandip18/settlement-engine/internal/ledger"
	"github.com/sayandip18/settlement-engine/internal/testutil"
)

func ent(account string, d ledger.Direction, amount int64) ledger.Entry {
	return ledger.Entry{Account: account, Direction: d, AmountPaise: amount}
}

func TestMain(m *testing.M) { os.Exit(testutil.Run(m)) }

func TestTxnValidate(t *testing.T) {
	cases := map[string]struct {
		entries []ledger.Entry
		wantErr string
	}{
		"balanced":     {[]ledger.Entry{ent("A", ledger.Debit, 100), ent("B", ledger.Credit, 100)}, ""},
		"split credit": {[]ledger.Entry{ent("A", ledger.Debit, 100), ent("B", ledger.Credit, 98), ent("C", ledger.Credit, 2)}, ""},
		"unbalanced":   {[]ledger.Entry{ent("A", ledger.Debit, 100), ent("B", ledger.Credit, 99)}, "unbalanced"},
		"one entry":    {[]ledger.Entry{ent("A", ledger.Debit, 100)}, "at least two"},
		"zero amount":  {[]ledger.Entry{ent("A", ledger.Debit, 0), ent("B", ledger.Credit, 0)}, "non-positive"},
		"negative":     {[]ledger.Entry{ent("A", ledger.Debit, -5), ent("B", ledger.Credit, -5)}, "non-positive"},
		"bad dir":      {[]ledger.Entry{ent("A", "SIDEWAYS", 5), ent("B", ledger.Credit, 5)}, "invalid direction"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			err := ledger.Txn{Entries: c.entries}.Validate()
			switch {
			case c.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
				t.Fatalf("want error containing %q, got %v", c.wantErr, err)
			}
		})
	}
}

func TestBuildersBalanceAndSkipZeroLines(t *testing.T) {
	id := uuid.New()
	for _, tc := range []struct {
		name    string
		txn     ledger.Txn
		entries int
	}{
		{"capture with fee", ledger.Capture(id, "m", 100000, 2000), 3},
		{"capture zero fee", ledger.Capture(id, "m", 100000, 0), 2},
		{"capture all fee", ledger.Capture(id, "m", 100, 100), 2},
		{"refund with fee share", ledger.Refund(id, "m", "r", 25000, 500), 3},
		{"refund zero fee share", ledger.Refund(id, "m", "r", 25000, 0), 2},
	} {
		if err := tc.txn.Validate(); err != nil {
			t.Errorf("%s: %v", tc.name, err)
		}
		if len(tc.txn.Entries) != tc.entries {
			t.Errorf("%s: %d entries, want %d", tc.name, len(tc.txn.Entries), tc.entries)
		}
	}
}

// --- database-level guarantees: these hold even if Go code is bypassed ---

func newPayment(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO payments (id, merchant_id, customer_id, amount_paise, currency, status)
		 VALUES ($1, 'm1', 'c1', 1000, 'INR', 'CAPTURED')`, id); err != nil {
		t.Fatal(err)
	}
	return id
}

func inTx(t *testing.T, pool *pgxpool.Pool, fn func(tx pgx.Tx)) error {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	fn(tx)
	return tx.Commit(ctx)
}

func exec(t *testing.T, tx pgx.Tx, sql string, args ...any) {
	t.Helper()
	if _, err := tx.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func TestPostWritesBalancedEntries(t *testing.T) {
	pool := testutil.NewPool(t)
	pid := newPayment(t, pool)

	err := inTx(t, pool, func(tx pgx.Tx) {
		if err := ledger.Post(context.Background(), tx, ledger.Capture(pid, "m1", 1000, 20)); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	var debits, credits int64
	if err := pool.QueryRow(context.Background(),
		`SELECT COALESCE(SUM(amount_paise) FILTER (WHERE direction='DEBIT'),0),
		        COALESCE(SUM(amount_paise) FILTER (WHERE direction='CREDIT'),0)
		   FROM ledger_entries`).Scan(&debits, &credits); err != nil {
		t.Fatal(err)
	}
	if debits != 1000 || credits != 1000 {
		t.Fatalf("debits=%d credits=%d, want 1000/1000", debits, credits)
	}
}

func TestPostRefusesUnbalancedWithoutTouchingDB(t *testing.T) {
	pool := testutil.NewPool(t)
	pid := newPayment(t, pool)

	bad := ledger.Txn{PaymentID: pid, MerchantID: "m1", Kind: ledger.KindCapture, RefID: "x",
		Entries: []ledger.Entry{ent("A", ledger.Debit, 10), ent("B", ledger.Credit, 9)}}
	err := inTx(t, pool, func(tx pgx.Tx) {
		if err := ledger.Post(context.Background(), tx, bad); err == nil {
			t.Fatal("Post accepted an unbalanced transaction")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	var n int
	pool.QueryRow(context.Background(), `SELECT count(*) FROM ledger_transactions`).Scan(&n)
	if n != 0 {
		t.Fatalf("%d ledger transactions written", n)
	}
}

func TestDatabaseRejectsUnbalancedTransaction(t *testing.T) {
	pool := testutil.NewPool(t)
	pid := newPayment(t, pool)
	txnID := uuid.New()

	// Raw SQL, bypassing ledger.Post: balance is enforced at COMMIT.
	err := inTx(t, pool, func(tx pgx.Tx) {
		exec(t, tx, `INSERT INTO ledger_accounts (code) VALUES ('A'), ('B')`)
		exec(t, tx, `INSERT INTO ledger_transactions (id, payment_id, merchant_id, kind, ref_id)
		             VALUES ($1, $2, 'm1', 'CAPTURE', 'raw')`, txnID, pid)
		exec(t, tx, `INSERT INTO ledger_entries (txn_id, account_code, direction, amount_paise)
		             VALUES ($1, 'A', 'DEBIT', 100), ($1, 'B', 'CREDIT', 99)`, txnID)
	})
	if err == nil || !strings.Contains(err.Error(), "not balanced") {
		t.Fatalf("want 'not balanced' commit error, got %v", err)
	}
	var n int
	pool.QueryRow(context.Background(), `SELECT count(*) FROM ledger_transactions`).Scan(&n)
	if n != 0 {
		t.Fatal("unbalanced transaction was persisted")
	}
}

func TestDatabaseRejectsTransactionWithoutEntries(t *testing.T) {
	pool := testutil.NewPool(t)
	pid := newPayment(t, pool)

	err := inTx(t, pool, func(tx pgx.Tx) {
		exec(t, tx, `INSERT INTO ledger_transactions (id, payment_id, merchant_id, kind, ref_id)
		             VALUES ($1, $2, 'm1', 'CAPTURE', 'empty')`, uuid.New(), pid)
	})
	if err == nil || !strings.Contains(err.Error(), "not balanced") {
		t.Fatalf("want commit error, got %v", err)
	}
}

func TestDatabaseRejectsLateUnbalancingEntry(t *testing.T) {
	pool := testutil.NewPool(t)
	pid := newPayment(t, pool)
	ctx := context.Background()

	var txnID uuid.UUID
	if err := inTx(t, pool, func(tx pgx.Tx) {
		txn := ledger.Capture(pid, "m1", 1000, 0)
		if err := ledger.Post(ctx, tx, txn); err != nil {
			t.Fatal(err)
		}
		if err := tx.QueryRow(ctx, `SELECT id FROM ledger_transactions`).Scan(&txnID); err != nil {
			t.Fatal(err)
		}
	}); err != nil {
		t.Fatal(err)
	}

	// Appending one more line to an already-committed transaction would unbalance it.
	err := inTx(t, pool, func(tx pgx.Tx) {
		exec(t, tx, `INSERT INTO ledger_entries (txn_id, account_code, direction, amount_paise)
		             VALUES ($1, 'CUSTOMER_FUNDS', 'CREDIT', 5)`, txnID)
	})
	if err == nil || !strings.Contains(err.Error(), "not balanced") {
		t.Fatalf("want 'not balanced', got %v", err)
	}
}

func TestLedgerIsAppendOnly(t *testing.T) {
	pool := testutil.NewPool(t)
	pid := newPayment(t, pool)
	ctx := context.Background()
	if err := inTx(t, pool, func(tx pgx.Tx) {
		if err := ledger.Post(ctx, tx, ledger.Capture(pid, "m1", 1000, 20)); err != nil {
			t.Fatal(err)
		}
	}); err != nil {
		t.Fatal(err)
	}

	for _, stmt := range []string{
		`UPDATE ledger_entries SET amount_paise = 1`,
		`DELETE FROM ledger_entries`,
		`UPDATE ledger_transactions SET merchant_id = 'x'`,
		`DELETE FROM ledger_transactions`,
		`UPDATE ledger_accounts SET code = 'x'`,
		`DELETE FROM ledger_accounts`,
	} {
		_, err := pool.Exec(ctx, stmt)
		if err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Errorf("%s: want append-only error, got %v", stmt, err)
		}
	}
	var n int
	pool.QueryRow(ctx, `SELECT count(*) FROM ledger_entries`).Scan(&n)
	if n != 3 {
		t.Fatalf("ledger entries changed: %d rows", n)
	}
}

func TestEventCannotBePostedTwice(t *testing.T) {
	pool := testutil.NewPool(t)
	pid := newPayment(t, pool)
	ctx := context.Background()

	post := func() error {
		var perr error
		cerr := inTx(t, pool, func(tx pgx.Tx) {
			perr = ledger.Post(ctx, tx, ledger.Capture(pid, "m1", 1000, 20))
		})
		if perr != nil {
			return perr
		}
		return cerr
	}
	if err := post(); err != nil {
		t.Fatal(err)
	}
	if err := post(); err == nil {
		t.Fatal("same capture posted twice")
	}
}
