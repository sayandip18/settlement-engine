-- Corrections to the append-only ledger are new transactions of kind ADJUSTMENT.
-- Their ref_id is the discrepancy being corrected, so UNIQUE (kind, ref_id) allows
-- at most one adjustment per finding.
ALTER TABLE ledger_transactions DROP CONSTRAINT ledger_transactions_kind_check;
ALTER TABLE ledger_transactions
    ADD CONSTRAINT ledger_transactions_kind_check CHECK (kind IN ('CAPTURE', 'REFUND', 'ADJUSTMENT'));

-- Findings written by the reconciler. (type, subject) identifies the thing that is
-- wrong; at most one unresolved finding exists for it, so re-running the checks
-- refreshes the finding instead of duplicating it. Once resolved, the same problem
-- recurring opens a new row.
CREATE TABLE discrepancies (
    id              UUID PRIMARY KEY,
    seq             BIGSERIAL   NOT NULL UNIQUE,
    type            TEXT        NOT NULL,
    subject         TEXT        NOT NULL,
    payment_id      UUID REFERENCES payments (id),
    details         JSONB       NOT NULL DEFAULT '{}',
    status          TEXT        NOT NULL DEFAULT 'OPEN'
                    CHECK (status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')),
    seen_count      INTEGER     NOT NULL DEFAULT 1,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_at TIMESTAMPTZ,
    ack_note        TEXT,
    resolved_at     TIMESTAMPTZ,
    resolution      TEXT CHECK (resolution IN ('AUTO', 'OPERATOR', 'CORRECTED')),
    resolution_note TEXT,
    ledger_txn_id   UUID REFERENCES ledger_transactions (id),
    CHECK ((status = 'RESOLVED') = (resolved_at IS NOT NULL AND resolution IS NOT NULL))
);
CREATE UNIQUE INDEX discrepancies_active_idx ON discrepancies (type, subject) WHERE status <> 'RESOLVED';
CREATE INDEX discrepancies_status_seq_idx ON discrepancies (status, seq DESC);
CREATE INDEX discrepancies_payment_idx    ON discrepancies (payment_id);
