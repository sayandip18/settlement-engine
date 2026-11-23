-- Settlement: the payout of a captured payment to its merchant, recorded as a
-- SETTLEMENT ledger transaction (ref_id = payment id, so it can happen only once).
-- settled_at is the source of truth; status moves to SETTLED only from CAPTURED.
ALTER TABLE payments
    ADD COLUMN settled_at TIMESTAMPTZ,
    ADD CONSTRAINT payments_settled_has_timestamp CHECK (status <> 'SETTLED' OR settled_at IS NOT NULL);
CREATE INDEX payments_settlement_due_idx ON payments (captured_at) WHERE settled_at IS NULL AND status IN ('CAPTURED', 'PARTIALLY_REFUNDED');

ALTER TABLE ledger_transactions DROP CONSTRAINT ledger_transactions_kind_check;
ALTER TABLE ledger_transactions
    ADD CONSTRAINT ledger_transactions_kind_check CHECK (kind IN ('CAPTURE', 'REFUND', 'ADJUSTMENT', 'SETTLEMENT'));
