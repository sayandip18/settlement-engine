package payment

import "fmt"

// transitions is the single source of truth for which status changes are legal.
// Terminal states (VOIDED, FAILED, REFUNDED) have no outgoing edges.
var transitions = map[Status][]Status{
	StatusPending:           {StatusAuthorized, StatusFailed},
	StatusAuthorized:        {StatusCaptured, StatusVoided, StatusFailed},
	StatusCaptured:          {StatusSettled, StatusPartiallyRefunded, StatusRefunded},
	StatusSettled:           {StatusPartiallyRefunded, StatusRefunded},
	StatusPartiallyRefunded: {StatusPartiallyRefunded, StatusRefunded},
}

// CanTransition reports whether a payment may move from one status to another.
func CanTransition(from, to Status) bool {
	for _, s := range transitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// CanRefund reports whether money can be refunded from a payment in this status:
// it must have been collected, and not already be fully refunded.
func CanRefund(s Status) bool {
	return s == StatusCaptured || s == StatusSettled || s == StatusPartiallyRefunded
}

// InvalidTransitionError is returned when an action is not allowed in the
// payment's current status, e.g. refunding a payment that was never captured.
type InvalidTransitionError struct {
	Action string
	From   Status
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("cannot %s a payment in status %s", e.Action, e.From)
}
