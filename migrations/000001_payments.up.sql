CREATE TABLE payments (
    id             UUID PRIMARY KEY,
    merchant_id    TEXT        NOT NULL,
    customer_id    TEXT        NOT NULL,
    amount_paise   BIGINT      NOT NULL CHECK (amount_paise > 0),
    currency       CHAR(3)     NOT NULL CHECK (currency = 'INR'),
    status         TEXT        NOT NULL CHECK (status IN (
                       'PENDING', 'AUTHORIZED', 'CAPTURED', 'SETTLED',
                       'PARTIALLY_REFUNDED', 'REFUNDED', 'VOIDED', 'FAILED')),
    failure_reason TEXT,
    version        INTEGER     NOT NULL DEFAULT 1,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX payments_merchant_created_idx ON payments (merchant_id, created_at);
CREATE INDEX payments_status_updated_idx ON payments (status, updated_at);

-- One row per (merchant, Idempotency-Key). request_hash detects key reuse with a
-- different body; response_* lets a retry get back the original response verbatim.
CREATE TABLE idempotency_keys (
    merchant_id     TEXT        NOT NULL,
    idem_key        TEXT        NOT NULL,
    request_hash    TEXT        NOT NULL,
    payment_id      UUID REFERENCES payments (id),
    response_status INTEGER,
    response_body   JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (merchant_id, idem_key)
);
