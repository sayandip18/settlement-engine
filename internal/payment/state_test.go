package payment_test

import (
	"testing"

	"github.com/sayandip18/settlement-engine/internal/payment"
)

func TestCanTransition(t *testing.T) {
	allowed := [][2]payment.Status{
		{payment.StatusPending, payment.StatusAuthorized},
		{payment.StatusPending, payment.StatusFailed},
		{payment.StatusAuthorized, payment.StatusCaptured},
		{payment.StatusAuthorized, payment.StatusVoided},
		{payment.StatusAuthorized, payment.StatusFailed},
		{payment.StatusCaptured, payment.StatusSettled},
		{payment.StatusCaptured, payment.StatusPartiallyRefunded},
		{payment.StatusCaptured, payment.StatusRefunded},
		{payment.StatusSettled, payment.StatusPartiallyRefunded},
		{payment.StatusSettled, payment.StatusRefunded},
		{payment.StatusPartiallyRefunded, payment.StatusPartiallyRefunded},
		{payment.StatusPartiallyRefunded, payment.StatusRefunded},
	}
	ok := map[[2]payment.Status]bool{}
	for _, a := range allowed {
		ok[a] = true
	}

	all := []payment.Status{
		payment.StatusPending, payment.StatusAuthorized, payment.StatusCaptured,
		payment.StatusSettled, payment.StatusPartiallyRefunded, payment.StatusRefunded,
		payment.StatusVoided, payment.StatusFailed,
	}
	// Exhaustive: every pair not explicitly allowed must be rejected.
	for _, from := range all {
		for _, to := range all {
			if got, want := payment.CanTransition(from, to), ok[[2]payment.Status{from, to}]; got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
}
