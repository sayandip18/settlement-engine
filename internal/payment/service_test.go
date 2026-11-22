package payment_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/sayandip18/settlement-engine/internal/payment"
	"github.com/sayandip18/settlement-engine/internal/testutil"
)

func input(key string) payment.CreateInput {
	return payment.CreateInput{
		MerchantID: "m1", CustomerID: "c1", AmountPaise: 150000, Currency: "INR",
		IdempotencyKey: key,
	}
}

func TestCreateAndGet(t *testing.T) {
	svc := payment.NewService(testutil.NewPool(t))
	ctx := context.Background()

	res, err := svc.Create(ctx, input("k1"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed || res.Payment.Status != payment.StatusPending || res.Payment.Currency != "INR" {
		t.Fatalf("unexpected result: %+v", res)
	}

	got, err := svc.Get(ctx, res.Payment.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != res.Payment.ID || got.AmountPaise != 150000 {
		t.Fatalf("unexpected payment: %+v", got)
	}

	if _, err := svc.Get(ctx, uuid.New()); !errors.Is(err, payment.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestIdempotentReplay(t *testing.T) {
	svc := payment.NewService(testutil.NewPool(t))
	ctx := context.Background()

	first, err := svc.Create(ctx, input("k1"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.Create(ctx, input("k1"))
	if err != nil {
		t.Fatal(err)
	}
	if !second.Replayed || second.Payment.ID != first.Payment.ID {
		t.Fatalf("expected replay of %s, got %+v", first.Payment.ID, second)
	}
}

func TestIdempotencyKeyReusedWithDifferentBody(t *testing.T) {
	svc := payment.NewService(testutil.NewPool(t))
	ctx := context.Background()

	if _, err := svc.Create(ctx, input("k1")); err != nil {
		t.Fatal(err)
	}
	changed := input("k1")
	changed.AmountPaise = 999
	if _, err := svc.Create(ctx, changed); !errors.Is(err, payment.ErrIdempotencyReused) {
		t.Fatalf("want ErrIdempotencyReused, got %v", err)
	}
}

func TestSameKeyDifferentMerchantsAreIndependent(t *testing.T) {
	svc := payment.NewService(testutil.NewPool(t))
	ctx := context.Background()

	a := input("shared")
	b := input("shared")
	b.MerchantID = "m2"
	ra, err := svc.Create(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := svc.Create(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if ra.Payment.ID == rb.Payment.ID || rb.Replayed {
		t.Fatal("different merchants must not share idempotency state")
	}
}

func TestConcurrentSameKeyCreatesOnePayment(t *testing.T) {
	pool := testutil.NewPool(t)
	svc := payment.NewService(pool)
	ctx := context.Background()

	const n = 20
	ids := make([]uuid.UUID, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := svc.Create(ctx, input("race"))
			ids[i], errs[i] = res.Payment.ID, err
		}()
	}
	wg.Wait()

	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("request %d failed: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("request %d got payment %s, want %s", i, ids[i], ids[0])
		}
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM payments`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("want exactly 1 payment row, got %d", count)
	}
}

func TestValidation(t *testing.T) {
	svc := payment.NewService(testutil.NewPool(t))
	cases := map[string]func(*payment.CreateInput){
		"missing key":      func(in *payment.CreateInput) { in.IdempotencyKey = "" },
		"missing merchant": func(in *payment.CreateInput) { in.MerchantID = "" },
		"missing customer": func(in *payment.CreateInput) { in.CustomerID = " " },
		"zero amount":      func(in *payment.CreateInput) { in.AmountPaise = 0 },
		"negative amount":  func(in *payment.CreateInput) { in.AmountPaise = -5 },
		"wrong currency":   func(in *payment.CreateInput) { in.Currency = "USD" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := input("kv")
			mutate(&in)
			_, err := svc.Create(context.Background(), in)
			var verr *payment.ValidationError
			if !errors.As(err, &verr) {
				t.Fatalf("want ValidationError, got %v", err)
			}
		})
	}
}

func TestCurrencyDefaultsToINR(t *testing.T) {
	svc := payment.NewService(testutil.NewPool(t))
	in := input("kd")
	in.Currency = ""
	res, err := svc.Create(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Payment.Currency != "INR" {
		t.Fatalf("got currency %q", res.Payment.Currency)
	}
}
