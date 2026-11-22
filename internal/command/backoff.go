package command

import (
	"math/rand/v2"
	"time"
)

// RetryPolicy decides how many times a transient failure is retried and how long to
// wait between attempts: exponential growth capped at Max, with random jitter so many
// failing operations do not retry in lock-step and hammer a recovering processor.
type RetryPolicy struct {
	MaxAttempts int           // total attempts including the first
	Base        time.Duration // delay before the second attempt (before jitter)
	Max         time.Duration // upper bound on the un-jittered delay
	Jitter      float64       // 0.2 means the delay varies by up to +/-20%
}

// Delay is the wait after the given failed attempt (1-based) before the next one.
// rnd must return values in [0, 1); pass rand.Float64 in production.
func (p RetryPolicy) Delay(attempt int, rnd func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := p.Base
	for i := 1; i < attempt && d < p.Max; i++ {
		d *= 2
	}
	if d > p.Max {
		d = p.Max
	}
	factor := 1 + p.Jitter*(2*rnd()-1)
	return time.Duration(float64(d) * factor)
}

// DefaultRetryPolicy: up to 5 attempts, roughly 1s, 2s, 4s, 8s apart.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 5, Base: time.Second, Max: time.Minute, Jitter: 0.2}
}

var randFloat = rand.Float64
