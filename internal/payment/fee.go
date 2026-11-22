package payment

import "math/bits"

const bpsDenominator = 10000

// calcFee is the platform fee for amount at bps basis points, rounded UP so any
// fractional paisa goes to the platform. Safe from overflow because amounts are
// capped at MaxAmountPaise.
func calcFee(amount int64, bps int) int64 {
	return (amount*int64(bps) + bpsDenominator - 1) / bpsDenominator
}

// feeRefundedAfter is how much of the frozen fee has been given back once a total of
// refunded paise has been refunded out of amount: floor(fee * refunded / amount).
// Using the cumulative figure (rather than rounding each refund separately) means the
// shares always add up to exactly fee when the payment is fully refunded.
func feeRefundedAfter(fee, amount, refunded int64) int64 {
	if amount == 0 {
		return 0
	}
	hi, lo := bits.Mul64(uint64(fee), uint64(refunded))
	q, _ := bits.Div64(hi, lo, uint64(amount)) // refunded <= amount, so hi < amount
	return int64(q)
}
