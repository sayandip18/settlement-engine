-- Fails if SETTLEMENT transactions exist: the ledger is append-only.
ALTER TABLE ledger_transactions DROP CONSTRAINT ledger_transactions_kind_check;
ALTER TABLE ledger_transactions
    ADD CONSTRAINT ledger_transactions_kind_check CHECK (kind IN ('CAPTURE', 'REFUND', 'ADJUSTMENT'));
DROP INDEX IF EXISTS payments_settlement_due_idx;
ALTER TABLE payments DROP CONSTRAINT IF EXISTS payments_settled_has_timestamp, DROP COLUMN IF EXISTS settled_at;
