DROP TABLE IF EXISTS discrepancies;
-- Fails if ADJUSTMENT transactions exist: the ledger is append-only, so they cannot be removed.
ALTER TABLE ledger_transactions DROP CONSTRAINT ledger_transactions_kind_check;
ALTER TABLE ledger_transactions
    ADD CONSTRAINT ledger_transactions_kind_check CHECK (kind IN ('CAPTURE', 'REFUND'));
