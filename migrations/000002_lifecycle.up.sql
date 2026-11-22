ALTER TABLE payments
    ADD COLUMN refunded_paise BIGINT      NOT NULL DEFAULT 0,
    ADD COLUMN captured_at    TIMESTAMPTZ,
    ADD CONSTRAINT payments_refunded_within_amount
        CHECK (refunded_paise >= 0 AND refunded_paise <= amount_paise);

-- One row per successful refund. (payment_id, idem_key) makes a retried refund
-- request return the original refund; requested_paise is NULL for "refund the
-- remainder" so a retry can be compared against what the client actually sent.
CREATE TABLE refunds (
    id              UUID PRIMARY KEY,
    payment_id      UUID        NOT NULL REFERENCES payments (id),
    idem_key        TEXT        NOT NULL,
    requested_paise BIGINT,
    amount_paise    BIGINT      NOT NULL CHECK (amount_paise > 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (payment_id, idem_key)
);
