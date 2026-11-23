-- Messages the worker gave up on, copied from the DLQ topic so operators can inspect,
-- replay or dismiss them. dedupe_key makes the DLQ consumer idempotent: the same DLQ
-- message delivered twice (the broker is at-least-once) stores one row.
CREATE TABLE dead_letters (
    id               UUID PRIMARY KEY,
    seq              BIGSERIAL   NOT NULL UNIQUE,
    dedupe_key       TEXT        NOT NULL UNIQUE,
    source_topic     TEXT,
    source_partition INTEGER,
    source_offset    BIGINT,
    msg_key          TEXT        NOT NULL,
    payload          BYTEA       NOT NULL,
    error            TEXT        NOT NULL,
    attempts         INTEGER     NOT NULL DEFAULT 0,
    operation_id     UUID,
    status           TEXT        NOT NULL DEFAULT 'PENDING'
                     CHECK (status IN ('PENDING', 'REPLAYED', 'DISMISSED')),
    replay_count     INTEGER     NOT NULL DEFAULT 0,
    note             TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at      TIMESTAMPTZ
);
CREATE INDEX dead_letters_status_seq_idx ON dead_letters (status, seq DESC);
CREATE INDEX dead_letters_operation_idx  ON dead_letters (operation_id);
