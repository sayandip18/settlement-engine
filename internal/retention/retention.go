// Package retention deletes data that has served its purpose so the outbox and dead
// letter tables do not grow without bound.
//
// Only finished work is ever deleted:
//   - outbox rows that were published and are older than OutboxAge;
//   - dead letters that were replayed or dismissed more than DeadLetterAge ago.
//
// Pending dead letters are never touched, and neither are the dead letters of an
// operation that is still DEAD_LETTERED: reconciliation relies on a dismissed one to
// know an operator accepted that outcome.
package retention

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sayandip18/settlement-engine/internal/obs"
)

// batchSize bounds each DELETE so no single statement holds locks for long.
const batchSize = 1000

type Config struct {
	OutboxAge     time.Duration
	DeadLetterAge time.Duration
}

type Service struct {
	pool *pgxpool.Pool
	cfg  Config
	log  *slog.Logger
}

func NewService(pool *pgxpool.Pool, cfg Config, log *slog.Logger) *Service {
	return &Service{pool: pool, cfg: cfg, log: log}
}

type Result struct {
	Outbox      int64
	DeadLetters int64
}

// Purge deletes everything past retention, in batches, and returns the counts. Running
// it concurrently with the relay, replay or itself is safe: it only deletes rows that
// can no longer change.
func (s *Service) Purge(ctx context.Context) (Result, error) {
	var res Result
	var errs []error

	n, err := s.deleteBatches(ctx, `
		DELETE FROM outbox WHERE id IN (
		    SELECT id FROM outbox
		     WHERE published_at IS NOT NULL AND published_at < now() - $1 * interval '1 microsecond'
		     ORDER BY published_at LIMIT $2)`, s.cfg.OutboxAge)
	res.Outbox = n
	obs.RetentionPurged.WithLabelValues("outbox").Add(float64(n))
	if err != nil {
		errs = append(errs, fmt.Errorf("purge outbox: %w", err))
	}

	n, err = s.deleteBatches(ctx, `
		DELETE FROM dead_letters WHERE id IN (
		    SELECT d.id FROM dead_letters d
		     WHERE d.status IN ('REPLAYED', 'DISMISSED')
		       AND d.closed_at < now() - $1 * interval '1 microsecond'
		       AND NOT EXISTS (SELECT 1 FROM operations o WHERE o.id = d.operation_id AND o.status = 'DEAD_LETTERED')
		     ORDER BY d.closed_at LIMIT $2)`, s.cfg.DeadLetterAge)
	res.DeadLetters = n
	obs.RetentionPurged.WithLabelValues("dead_letters").Add(float64(n))
	if err != nil {
		errs = append(errs, fmt.Errorf("purge dead letters: %w", err))
	}
	return res, errors.Join(errs...)
}

func (s *Service) deleteBatches(ctx context.Context, sql string, age time.Duration) (int64, error) {
	var total int64
	for {
		tag, err := s.pool.Exec(ctx, sql, age.Microseconds(), batchSize)
		if err != nil {
			return total, err
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < batchSize {
			return total, nil
		}
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
	}
}

// RunLoop purges now and then every interval until ctx ends. beat (optional) is called
// each pass for readiness checks.
func (s *Service) RunLoop(ctx context.Context, interval time.Duration, beat func()) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if beat != nil {
			beat()
		}
		res, err := s.Purge(ctx)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			s.log.Error("retention purge failed", "err", err)
		case res.Outbox+res.DeadLetters > 0:
			s.log.Info("retention purge", "outbox_rows", res.Outbox, "dead_letters", res.DeadLetters)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
