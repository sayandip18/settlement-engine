package payment

import "testing"

func TestCalcFee(t *testing.T) {
	for _, c := range []struct {
		amount int64
		bps    int
		want   int64
	}{
		{100000, 200, 2000},
		{333, 200, 7}, // 6.66 rounds up
		{10, 200, 1},  // 0.2 rounds up
		{100, 0, 0},   // free merchant
		{100, 10000, 100},
		{1, 1, 1}, // smallest non-zero fee
		{MaxAmountPaise, 10000, MaxAmountPaise},
	} {
		if got := calcFee(c.amount, c.bps); got != c.want {
			t.Errorf("calcFee(%d, %d) = %d, want %d", c.amount, c.bps, got, c.want)
		}
	}
}

func TestFeeRefundedAfter(t *testing.T) {
	for _, c := range []struct{ fee, amount, refunded, want int64 }{
		{2000, 100000, 0, 0},
		{2000, 100000, 25000, 500},
		{2000, 100000, 100000, 2000},
		{7, 333, 100, 2},
		{7, 333, 200, 4},
		{7, 333, 333, 7},
		{0, 100, 50, 0},
		// Largest allowed values: fee*refunded ~ 1e24 would overflow int64.
		{MaxAmountPaise, MaxAmountPaise, MaxAmountPaise, MaxAmountPaise},
		{MaxAmountPaise / 2, MaxAmountPaise, MaxAmountPaise / 2, MaxAmountPaise / 4},
	} {
		if got := feeRefundedAfter(c.fee, c.amount, c.refunded); got != c.want {
			t.Errorf("feeRefundedAfter(%d, %d, %d) = %d, want %d", c.fee, c.amount, c.refunded, got, c.want)
		}
	}
}

// However a payment is split into refunds, the fee shares must sum to the fee
// exactly and never go negative.
func TestFeeSharesAlwaysSumToFee(t *testing.T) {
	const amount, fee = 333, 7
	for step := int64(1); step <= amount; step++ {
		var refunded, given int64
		for refunded < amount {
			next := min(refunded+step, amount)
			share := feeRefundedAfter(fee, amount, next) - feeRefundedAfter(fee, amount, refunded)
			if share < 0 {
				t.Fatalf("step %d: negative share %d", step, share)
			}
			given += share
			refunded = next
		}
		if given != fee {
			t.Fatalf("step %d: shares sum to %d, want %d", step, given, fee)
		}
	}
}
