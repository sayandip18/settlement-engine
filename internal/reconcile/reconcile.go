// Package reconcile looks for mismatches between payments, the ledger, the outbox and
// operations, and records what it finds as discrepancies for operators to handle.
//
// The reconciler is read-only against money tables: every check runs in one
// REPEATABLE READ, READ ONLY transaction (a single consistent snapshot) and only the
// discrepancies table is written afterwards. Fixing a finding is a separate, explicit
// operator action (see Service.Correct).
package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sayandip18/settlement-engine/internal/obs"
)

type Type string

const (
	TypeCaptureLedgerMissing  Type = "CAPTURE_LEDGER_MISSING"
	TypeCustomerFundsMismatch Type = "CUSTOMER_FUNDS_MISMATCH"
	TypeUnbalancedLedgerTxn   Type = "UNBALANCED_LEDGER_TXN"
	TypePaymentStuck          Type = "PAYMENT_STUCK"
	TypeRefundExceedsCapture  Type = "REFUND_EXCEEDS_CAPTURE"
	TypeLedgerOnUncaptured    Type = "LEDGER_ON_UNCAPTURED_PAYMENT"
	TypeOutboxStale           Type = "OUTBOX_STALE"
	TypeOperationStuck        Type = "OPERATION_STUCK"
	TypeLedgerGlobalImbalance Type = "LEDGER_GLOBAL_IMBALANCE"
	TypeSettlementMismatch    Type = "SETTLEMENT_MISMATCH"
)

type Status string

const (
	StatusOpen         Status = "OPEN"
	StatusAcknowledged Status = "ACKNOWLEDGED"
	StatusResolved     Status = "RESOLVED"
)

func (s Status) valid() bool {
	return s == StatusOpen || s == StatusAcknowledged || s == StatusResolved
}

// Thresholds say how long something may sit in an intermediate state before it is a finding.
type Thresholds struct {
	PendingPayment    time.Duration // PENDING payment with no progress
	AuthorizedPayment time.Duration // AUTHORIZED (held, never captured or voided)
	OutboxStale       time.Duration // unpublished outbox row, measured from its available_at
	OperationStuck    time.Duration // PENDING operation with no progress
}

func DefaultThresholds() Thresholds {
	return Thresholds{
		PendingPayment:    time.Hour,
		AuthorizedPayment: 7 * 24 * time.Hour,
		OutboxStale:       5 * time.Minute,
		OperationStuck:    15 * time.Minute,
	}
}

// maxFindingsPerCheck bounds how many findings one check reports per run, so a
// systemic problem (e.g. a bad migration) cannot flood the table. A truncated check
// does not auto-resolve anything: the missing findings may simply be beyond the cap.
const maxFindingsPerCheck = 1000

// lockKey is the Postgres advisory lock that keeps reconcilers from overlapping.
const lockKey int64 = 7210190110

var ErrAlreadyRunning = errors.New("a reconciliation run is already in progress")

// Finding is one problem a check detected. Subject identifies the thing that is wrong
// and is the deduplication key together with the check's Type.
type Finding struct {
	Subject   string
	PaymentID *uuid.UUID
	Details   map[string]any
}

type check struct {
	typ Type
	run func(ctx context.Context, tx pgx.Tx) ([]Finding, error)
}

// CheckResult summarises one check in a run.
type CheckResult struct {
	Type         Type   `json:"type"`
	Found        int    `json:"found"`
	New          int    `json:"new"`
	AutoResolved int    `json:"auto_resolved"`
	Truncated    bool   `json:"truncated,omitempty"`
	Error        string `json:"error,omitempty"`
}

type RunResult struct {
	Checks []CheckResult `json:"checks"`
}

// Failed reports whether any check could not run.
func (r RunResult) Failed() bool {
	for _, c := range r.Checks {
		if c.Error != "" {
			return true
		}
	}
	return false
}

type Service struct {
	pool *pgxpool.Pool
	th   Thresholds
	log  *slog.Logger
	beat func()
}

// SetHeartbeat makes RunLoop call beat every iteration (for readiness checks).
func (s *Service) SetHeartbeat(beat func()) { s.beat = beat }

// AllTypes lists every finding type.
func AllTypes() []Type {
	return []Type{TypeCaptureLedgerMissing, TypeCustomerFundsMismatch, TypeUnbalancedLedgerTxn, TypePaymentStuck,
		TypeRefundExceedsCapture, TypeLedgerOnUncaptured, TypeOutboxStale, TypeOperationStuck,
		TypeLedgerGlobalImbalance, TypeSettlementMismatch}
}

func NewService(pool *pgxpool.Pool, th Thresholds, log *slog.Logger) *Service {
	return &Service{pool: pool, th: th, log: log}
}

type outcome struct {
	typ       Type
	findings  []Finding
	truncated bool
	err       error
}

// Run executes every check once and records the findings. Only one run happens at a
// time across all processes; a concurrent caller gets ErrAlreadyRunning.
//
// A check that fails to run is reported in its CheckResult and leaves that check's
// existing findings untouched; the other checks are unaffected.
func (s *Service) Run(ctx context.Context) (RunResult, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return RunResult{}, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, lockKey).Scan(&locked); err != nil {
		return RunResult{}, fmt.Errorf("take reconciliation lock: %w", err)
	}
	if !locked {
		return RunResult{}, ErrAlreadyRunning
	}
	defer func() {
		uctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var ok bool
		if err := conn.QueryRow(uctx, `SELECT pg_advisory_unlock($1)`, lockKey).Scan(&ok); err != nil || !ok {
			// Never hand a connection that may still hold the lock back to the pool.
			s.log.Error("release reconciliation lock failed; closing connection", "err", err)
			_ = conn.Hijack().Close(uctx)
		}
	}()

	// Phase 1: read everything from one snapshot.
	rtx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return RunResult{}, fmt.Errorf("begin snapshot: %w", err)
	}
	var startedAt time.Time // the snapshot's own time, on the database clock
	if err := rtx.QueryRow(ctx, `SELECT now()`).Scan(&startedAt); err != nil {
		_ = rtx.Rollback(ctx)
		return RunResult{}, fmt.Errorf("read snapshot time: %w", err)
	}
	var outcomes []outcome
	for _, c := range s.checks() {
		o := outcome{typ: c.typ}
		sp, err := rtx.Begin(ctx) // savepoint: one failing check must not abort the rest
		if err == nil {
			o.findings, err = c.run(ctx, sp)
			_ = sp.Rollback(ctx)
		}
		if err != nil {
			if ctx.Err() != nil {
				_ = rtx.Rollback(ctx)
				return RunResult{}, ctx.Err()
			}
			o.err = err
		}
		if len(o.findings) > maxFindingsPerCheck {
			o.findings, o.truncated = o.findings[:maxFindingsPerCheck], true
		}
		outcomes = append(outcomes, o)
	}
	_ = rtx.Rollback(ctx)

	// Phase 2: write findings.
	return s.persist(ctx, conn, startedAt, outcomes)
}

type execer interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

const upsertSQL = `
INSERT INTO discrepancies (id, type, subject, payment_id, details)
SELECT $1::uuid, $2::text, $3::text, $4::uuid, $5::jsonb
 WHERE NOT EXISTS (
       SELECT 1 FROM discrepancies
        WHERE type = $2::text AND subject = $3::text AND status = 'RESOLVED' AND resolved_at >= $6::timestamptz)
ON CONFLICT (type, subject) WHERE status <> 'RESOLVED'
DO UPDATE SET last_seen_at = now(), seen_count = discrepancies.seen_count + 1, details = EXCLUDED.details
RETURNING (xmax = 0)`

// persist upserts each check's findings and auto-resolves the ones it no longer reports.
//
// A finding an operator resolved after the snapshot was taken is not reopened by this
// run (the snapshot may predate their fix); the next run decides.
func (s *Service) persist(ctx context.Context, db execer, startedAt time.Time, outcomes []outcome) (RunResult, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return RunResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var res RunResult
	for _, o := range outcomes {
		cr := CheckResult{Type: o.typ, Found: len(o.findings), Truncated: o.truncated}
		if o.err != nil {
			cr.Error = o.err.Error()
			s.log.Error("reconciliation check failed", "check", o.typ, "err", o.err)
			res.Checks = append(res.Checks, cr)
			continue
		}

		subjects := make([]string, 0, len(o.findings)) // non-nil: pgx would send NULL for a nil slice
		batch := &pgx.Batch{}
		for _, f := range o.findings {
			details, err := json.Marshal(f.Details)
			if err != nil {
				return RunResult{}, fmt.Errorf("marshal details: %w", err)
			}
			subjects = append(subjects, f.Subject)
			batch.Queue(upsertSQL, uuid.New(), string(o.typ), f.Subject, f.PaymentID, details, startedAt).
				QueryRow(func(row pgx.Row) error {
					var inserted bool
					switch err := row.Scan(&inserted); {
					case errors.Is(err, pgx.ErrNoRows): // skipped: resolved after the snapshot
						return nil
					case err != nil:
						return err
					case inserted:
						cr.New++
					}
					return nil
				})
		}
		if batch.Len() > 0 {
			if err := tx.SendBatch(ctx, batch).Close(); err != nil {
				return RunResult{}, fmt.Errorf("upsert %s findings: %w", o.typ, err)
			}
		}

		if !o.truncated {
			tag, err := tx.Exec(ctx,
				`UPDATE discrepancies
				    SET status = 'RESOLVED', resolution = 'AUTO', resolved_at = now(),
				        resolution_note = 'condition no longer detected'
				  WHERE type = $1 AND status <> 'RESOLVED' AND NOT (subject = ANY($2))`,
				string(o.typ), subjects)
			if err != nil {
				return RunResult{}, fmt.Errorf("auto-resolve %s findings: %w", o.typ, err)
			}
			cr.AutoResolved = int(tag.RowsAffected())
		} else {
			s.log.Warn("reconciliation check hit the findings cap; not auto-resolving", "check", o.typ)
		}
		res.Checks = append(res.Checks, cr)
	}
	if err := tx.Commit(ctx); err != nil {
		return RunResult{}, fmt.Errorf("commit: %w", err)
	}
	for _, c := range res.Checks {
		obs.ReconcileNewFindings.WithLabelValues(string(c.Type)).Add(float64(c.New))
	}
	if res.Failed() {
		obs.ReconcileRuns.WithLabelValues("failed").Inc()
	} else {
		obs.ReconcileRuns.WithLabelValues("ok").Inc()
		obs.ReconcileLastSuccess.SetToCurrentTime()
	}
	return res, nil
}

// RunLoop runs the checks now and then every interval until ctx is cancelled.
func (s *Service) RunLoop(ctx context.Context, interval time.Duration) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if s.beat != nil {
			s.beat()
		}
		res, err := s.Run(ctx)
		switch {
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, ErrAlreadyRunning):
			obs.ReconcileRuns.WithLabelValues("skipped").Inc()
			s.log.Debug("reconciliation skipped: another run is in progress")
		case err != nil:
			obs.ReconcileRuns.WithLabelValues("failed").Inc()
			s.log.Error("reconciliation run failed", "err", err)
		default:
			var found, created, resolved int
			for _, c := range res.Checks {
				found, created, resolved = found+c.Found, created+c.New, resolved+c.AutoResolved
			}
			s.log.Info("reconciliation run finished", "findings", found, "new", created,
				"auto_resolved", resolved, "failed_checks", res.Failed())
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
