-- Transactional outbox: rows are written in the same transaction as the state change
-- that caused them, then published to Kafka by the relay. available_at lets a row be
-- scheduled for later, which is how retries get their backoff delay.
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT        NOT NULL,
    msg_key      TEXT        NOT NULL,
    payload      BYTEA       NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '{}',
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (available_at, id) WHERE published_at IS NULL;

-- One row per asynchronous request (authorize, capture, void, refund). The status is
-- what API clients poll; attempts counts processing attempts made so far and fences
-- out duplicate or stale queue messages.
CREATE TABLE operations (
    id              UUID PRIMARY KEY,
    payment_id      UUID        NOT NULL REFERENCES payments (id),
    kind            TEXT        NOT NULL CHECK (kind IN ('AUTHORIZE', 'CAPTURE', 'VOID', 'REFUND')),
    status          TEXT        NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED', 'DEAD_LETTERED')),
    attempts        INTEGER     NOT NULL DEFAULT 0,
    requested_paise BIGINT,
    idem_key        TEXT,
    error_code      TEXT,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Refund requests are idempotent per (payment, Idempotency-Key).
CREATE UNIQUE INDEX operations_idem_idx ON operations (payment_id, idem_key) WHERE idem_key IS NOT NULL;
CREATE INDEX operations_payment_idx ON operations (payment_id);
CREATE INDEX operations_status_idx  ON operations (status, updated_at);
