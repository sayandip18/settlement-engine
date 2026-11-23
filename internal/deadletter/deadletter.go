// Package deadletter stores messages the worker gave up on and lets operators
// inspect, replay or dismiss them.
//
// The worker publishes failures to a Dead Letter Queue topic. A consumer copies each
// one into the dead_letters table (Record); admin endpoints then list them (List, Get)
// and act on them (Replay, Dismiss).
package deadletter

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sayandip18/settlement-engine/internal/command"
	"github.com/sayandip18/settlement-engine/internal/obs"
	"github.com/sayandip18/settlement-engine/internal/outbox"
	"github.com/sayandip18/settlement-engine/internal/platform/kafka"
)

type Status string

const (
	StatusPending   Status = "PENDING"
	StatusReplayed  Status = "REPLAYED"
	StatusDismissed Status = "DISMISSED"
)

func (s Status) valid() bool {
	return s == StatusPending || s == StatusReplayed || s == StatusDismissed
}

// Entry is one dead letter. Payload is only filled in by Get.
type Entry struct {
	ID              uuid.UUID  `json:"id"`
	Status          Status     `json:"status"`
	Error           string     `json:"error"`
	Attempts        int        `json:"attempts"`
	OperationID     *uuid.UUID `json:"operation_id,omitempty"`
	OperationStatus *string    `json:"operation_status,omitempty"` // live status of that operation
	SourceTopic     *string    `json:"source_topic,omitempty"`
	SourcePartition *int       `json:"source_partition,omitempty"`
	SourceOffset    *int64     `json:"source_offset,omitempty"`
	Key             string     `json:"key"`
	PayloadSize     int        `json:"payload_size"`
	Payload         *string    `json:"payload,omitempty"`
	PayloadEncoding string     `json:"payload_encoding,omitempty"` // "utf8" or "base64"
	ReplayCount     int        `json:"replay_count"`
	Note            *string    `json:"note,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	ReplayedAt      *time.Time `json:"replayed_at,omitempty"`

	seq int64
}

var ErrNotFound = errors.New("dead letter not found")

// AlreadyHandledError: the entry was already replayed or dismissed.
type AlreadyHandledError struct{ Status Status }

func (e *AlreadyHandledError) Error() string { return "dead letter is already " + string(e.Status) }

// NotReplayableError: the entry cannot be replayed, e.g. its operation has since
// finished some other way. Dismiss it instead.
type NotReplayableError struct{ Reason string }

func (e *NotReplayableError) Error() string { return e.Reason }

type Service struct {
	pool     *pgxpool.Pool
	commands *command.Service
}

func NewService(pool *pgxpool.Pool, commands *command.Service) *Service {
	return &Service{pool: pool, commands: commands}
}

// Record stores one DLQ message. It is the DLQ consumer's handler and is idempotent.
func (s *Service) Record(ctx context.Context, m kafka.Message) error {
	h := m.Headers
	var (
		srcTopic  *string
		srcPart   *int
		srcOffset *int64
		dedupe    = fmt.Sprintf("dlq:%s:%d:%d", m.Topic, m.Partition, m.Offset)
	)
	if t := h["source_topic"]; t != "" {
		p, perr := strconv.Atoi(h["source_partition"])
		o, oerr := strconv.ParseInt(h["source_offset"], 10, 64)
		if perr == nil && oerr == nil {
			srcTopic, srcPart, srcOffset = &t, &p, &o
			// The same failed message can reach the DLQ topic twice (at-least-once publishing);
			// its coordinates in the source topic identify it.
			dedupe = fmt.Sprintf("src:%s:%d:%d", t, p, o)
		}
	}
	attempts, _ := strconv.Atoi(h["attempts"])
	var opID *uuid.UUID
	if id, err := uuid.Parse(h["operation_id"]); err == nil {
		opID = &id
	}
	errText := h["error"]
	if errText == "" {
		errText = "unknown"
	}

	if _, err := s.pool.Exec(ctx,
		`INSERT INTO dead_letters
		   (id, dedupe_key, source_topic, source_partition, source_offset, msg_key, payload, error, attempts, operation_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		 ON CONFLICT (dedupe_key) DO NOTHING`,
		uuid.New(), dedupe, srcTopic, srcPart, srcOffset, m.Key, m.Value, errText, attempts, opID); err != nil {
		return fmt.Errorf("record dead letter: %w", err)
	}
	obs.DeadLettersRecorded.Inc()
	return nil
}

const entryCols = `d.id, d.seq, d.status, d.error, d.attempts, d.operation_id, o.status,
	d.source_topic, d.source_partition, d.source_offset, d.msg_key, octet_length(d.payload),
	d.replay_count, d.note, d.created_at, d.replayed_at`

const entryFrom = ` FROM dead_letters d LEFT JOIN operations o ON o.id = d.operation_id`

func scanEntry(row pgx.Row) (Entry, error) {
	var e Entry
	err := row.Scan(&e.ID, &e.seq, &e.Status, &e.Error, &e.Attempts, &e.OperationID, &e.OperationStatus,
		&e.SourceTopic, &e.SourcePartition, &e.SourceOffset, &e.Key, &e.PayloadSize,
		&e.ReplayCount, &e.Note, &e.CreatedAt, &e.ReplayedAt)
	return e, err
}

type ListFilter struct {
	Status *Status // nil = any
	Limit  int     // 1..MaxLimit; 0 = DefaultLimit
	Before int64   // cursor from a previous page; 0 = start at the newest
}

const (
	DefaultLimit = 50
	MaxLimit     = 200
)

// List returns entries newest first, plus a cursor for the next page (0 when done).
func (s *Service) List(ctx context.Context, f ListFilter) ([]Entry, int64, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultLimit
	}
	var status *string
	if f.Status != nil {
		v := string(*f.Status)
		status = &v
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+entryCols+entryFrom+`
		  WHERE ($1::text IS NULL OR d.status = $1) AND ($2::bigint = 0 OR d.seq < $2)
		  ORDER BY d.seq DESC LIMIT $3`, status, f.Before, limit+1)
	if err != nil {
		return nil, 0, fmt.Errorf("list dead letters: %w", err)
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan dead letter: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	var next int64
	if len(out) > limit {
		out = out[:limit]
		next = out[limit-1].seq
	}
	return out, next, nil
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *Service) load(ctx context.Context, q queryRower, id uuid.UUID, withPayload bool) (Entry, error) {
	e, err := scanEntry(q.QueryRow(ctx, `SELECT `+entryCols+entryFrom+` WHERE d.id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Entry{}, ErrNotFound
	}
	if err != nil {
		return Entry{}, fmt.Errorf("get dead letter: %w", err)
	}
	if withPayload {
		var raw []byte
		if err := q.QueryRow(ctx, `SELECT payload FROM dead_letters WHERE id = $1`, id).Scan(&raw); err != nil {
			return Entry{}, fmt.Errorf("get dead letter payload: %w", err)
		}
		// Show readable payloads as text; anything else (binary garbage) as base64.
		if utf8.Valid(raw) {
			p := string(raw)
			e.Payload, e.PayloadEncoding = &p, "utf8"
		} else {
			p := base64.StdEncoding.EncodeToString(raw)
			e.Payload, e.PayloadEncoding = &p, "base64"
		}
	}
	return e, nil
}

// Get returns one entry including its payload.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (Entry, error) {
	return s.load(ctx, s.pool, id, true)
}

// Replay re-runs a pending dead letter, in one transaction with marking it REPLAYED:
//
//   - If it belongs to an operation, the operation is reset and given a fresh first
//     attempt (see command.Service.Requeue).
//   - Otherwise (e.g. an unreadable message) the original payload is published again
//     to the topic it came from.
//
// Replaying the same entry twice is rejected, so one click cannot enqueue two retries.
func (s *Service) Replay(ctx context.Context, id uuid.UUID) (Entry, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Entry{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var (
		status  Status
		opID    *uuid.UUID
		topic   *string
		key     string
		payload []byte
	)
	err = tx.QueryRow(ctx,
		`SELECT status, operation_id, source_topic, msg_key, payload
		   FROM dead_letters WHERE id = $1 FOR UPDATE`, id).Scan(&status, &opID, &topic, &key, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return Entry{}, ErrNotFound
	}
	if err != nil {
		return Entry{}, fmt.Errorf("lock dead letter: %w", err)
	}
	if status != StatusPending {
		return Entry{}, &AlreadyHandledError{Status: status}
	}

	switch {
	case opID != nil:
		var nr *command.NotRequeueableError
		switch err := s.commands.Requeue(ctx, tx, *opID); {
		case errors.Is(err, command.ErrOperationNotFound):
			return Entry{}, &NotReplayableError{Reason: "its operation no longer exists"}
		case errors.As(err, &nr):
			return Entry{}, &NotReplayableError{Reason: "its operation is no longer dead-lettered: " + nr.Error()}
		case err != nil:
			return Entry{}, err
		}
	case topic != nil:
		if err := outbox.Enqueue(ctx, tx, outbox.Message{Topic: *topic, Key: key, Payload: payload}); err != nil {
			return Entry{}, err
		}
	default:
		return Entry{}, &NotReplayableError{Reason: "it records neither an operation nor a source topic to replay to"}
	}

	if _, err := tx.Exec(ctx,
		`UPDATE dead_letters SET status = 'REPLAYED', replay_count = replay_count + 1, replayed_at = now(), closed_at = now()
		  WHERE id = $1`, id); err != nil {
		return Entry{}, fmt.Errorf("mark replayed: %w", err)
	}
	e, err := s.load(ctx, tx, id, false)
	if err != nil {
		return Entry{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Entry{}, fmt.Errorf("commit: %w", err)
	}
	return e, nil
}

// Dismiss closes a pending entry without replaying it, optionally recording why.
func (s *Service) Dismiss(ctx context.Context, id uuid.UUID, note string) (Entry, error) {
	var n *string
	if note != "" {
		n = &note
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE dead_letters SET status = 'DISMISSED', note = $2, closed_at = now() WHERE id = $1 AND status = 'PENDING'`, id, n)
	if err != nil {
		return Entry{}, fmt.Errorf("dismiss dead letter: %w", err)
	}
	e, err := s.load(ctx, s.pool, id, false)
	if err != nil {
		return Entry{}, err
	}
	if tag.RowsAffected() == 0 {
		return Entry{}, &AlreadyHandledError{Status: e.Status}
	}
	return e, nil
}
