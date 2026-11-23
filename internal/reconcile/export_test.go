package reconcile

import (
	"context"
	"time"
)

// PersistFindings exposes persist so tests can replay a run whose snapshot is older
// than a concurrent operator action.
func (s *Service) PersistFindings(ctx context.Context, startedAt time.Time, typ Type, findings []Finding) (RunResult, error) {
	return s.persist(ctx, s.pool, startedAt, []outcome{{typ: typ, findings: findings}})
}
