DROP TABLE IF EXISTS refunds;
ALTER TABLE payments
    DROP CONSTRAINT IF EXISTS payments_refunded_within_amount,
    DROP COLUMN IF EXISTS captured_at,
    DROP COLUMN IF EXISTS refunded_paise;
