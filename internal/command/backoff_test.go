package command

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/sayandip18/settlement-engine/internal/payment"
	"github.com/sayandip18/settlement-engine/internal/processor"
)

func TestDelayGrowsExponentiallyAndIsCapped(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 10, Base: time.Second, Max: 10 * time.Second, Jitter: 0}
	mid := func() float64 { return 0.5 } // no jitter at the midpoint
	for attempt, want := range map[int]time.Duration{
		0: time.Second, // clamped to the first attempt
		1: time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
		4: 8 * time.Second,
		5: 10 * time.Second, // 16s capped
		9: 10 * time.Second,
	} {
		if got := p.Delay(attempt, mid); got != want {
			t.Errorf("Delay(%d) = %v, want %v", attempt, got, want)
		}
	}
}

func TestDelayJitterStaysWithinBounds(t *testing.T) {
	p := RetryPolicy{Base: time.Second, Max: time.Minute, Jitter: 0.2}
	if got := p.Delay(1, func() float64 { return 0 }); got != 800*time.Millisecond {
		t.Errorf("lowest jitter = %v, want 800ms", got)
	}
	if got := p.Delay(1, func() float64 { return 0.999999 }); got < 1190*time.Millisecond || got > 1200*time.Millisecond {
		t.Errorf("highest jitter = %v, want ~1.2s", got)
	}
	// Real randomness never leaves the band, and actually varies.
	seen := map[time.Duration]bool{}
	for i := 0; i < 500; i++ {
		d := p.Delay(3, rand.Float64) // un-jittered 4s
		if d < 3200*time.Millisecond || d > 4800*time.Millisecond {
			t.Fatalf("delay %v outside 4s +/- 20%%", d)
		}
		seen[d] = true
	}
	if len(seen) < 50 {
		t.Fatalf("only %d distinct delays in 500 draws: no jitter?", len(seen))
	}
}

func TestDelayCapAppliesBeforeJitterWithoutOverflow(t *testing.T) {
	p := RetryPolicy{Base: time.Second, Max: time.Minute, Jitter: 0.2}
	for _, attempt := range []int{20, 100, 10000} {
		if d := p.Delay(attempt, rand.Float64); d > 72*time.Second || d < 48*time.Second {
			t.Fatalf("Delay(%d) = %v", attempt, d)
		}
	}
}

func TestClassify(t *testing.T) {
	permanent := map[string]error{
		"declined":      &payment.DeclinedError{Code: "insufficient_funds", Message: "no"},
		"invalid state": &payment.InvalidTransitionError{Action: "capture", From: payment.StatusPending},
		"over refund":   &payment.RefundExceedsError{RemainingPaise: 1},
		"validation":    &payment.ValidationError{Msg: "bad"},
		"not found":     payment.ErrNotFound,
		"key reused":    payment.ErrIdempotencyReused,
		"wrapped":       fmt.Errorf("outer: %w", &payment.DeclinedError{Code: "x"}),
	}
	for name, err := range permanent {
		if _, ok := classify(err); !ok {
			t.Errorf("%s should be permanent", name)
		}
	}
	transient := map[string]error{
		"processor":  fmt.Errorf("%w: %v", payment.ErrProcessorUnavailable, processor.ErrTemporary),
		"unknown":    errors.New("connection reset by peer"),
		"db failure": fmt.Errorf("commit: %w", errors.New("conn closed")),
	}
	for name, err := range transient {
		if _, ok := classify(err); ok {
			t.Errorf("%s should be transient", name)
		}
	}
	if f, _ := classify(&payment.DeclinedError{Code: "insufficient_funds", Message: "insufficient funds"}); f.Code != "insufficient_funds" {
		t.Errorf("code = %q", f.Code)
	}
}
