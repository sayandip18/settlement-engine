DROP TABLE IF EXISTS ledger_entries;
DROP TABLE IF EXISTS ledger_transactions;
DROP TABLE IF EXISTS ledger_accounts;
DROP FUNCTION IF EXISTS ledger_assert_balanced();
DROP FUNCTION IF EXISTS ledger_reject_mutation();
DROP TABLE IF EXISTS merchants;
ALTER TABLE payments
    DROP CONSTRAINT IF EXISTS payments_fee_within_amount,
    DROP COLUMN IF EXISTS fee_paise;
