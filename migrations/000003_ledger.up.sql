ALTER TABLE payments
    ADD COLUMN fee_paise BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT payments_fee_within_amount CHECK (fee_paise >= 0 AND fee_paise <= amount_paise);

-- Per-merchant fee in basis points (200 = 2%). Merchants without a row use the
-- service default. The fee is frozen onto the payment at capture time.
CREATE TABLE merchants (
    id         TEXT PRIMARY KEY,
    fee_bps    INTEGER     NOT NULL CHECK (fee_bps BETWEEN 0 AND 10000),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ledger_accounts (
    code       TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per business event. (kind, ref_id) is unique so an event can never be
-- posted twice: ref_id is the payment ID for a capture and the refund ID for a refund.
CREATE TABLE ledger_transactions (
    id          UUID PRIMARY KEY,
    payment_id  UUID        NOT NULL REFERENCES payments (id),
    merchant_id TEXT        NOT NULL,
    kind        TEXT        NOT NULL CHECK (kind IN ('CAPTURE', 'REFUND')),
    ref_id      TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (kind, ref_id)
);
CREATE INDEX ledger_transactions_merchant_idx ON ledger_transactions (merchant_id, created_at);
CREATE INDEX ledger_transactions_payment_idx  ON ledger_transactions (payment_id);

CREATE TABLE ledger_entries (
    id           BIGSERIAL PRIMARY KEY,
    txn_id       UUID        NOT NULL REFERENCES ledger_transactions (id),
    account_code TEXT        NOT NULL REFERENCES ledger_accounts (code),
    direction    TEXT        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_paise BIGINT      NOT NULL CHECK (amount_paise > 0),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ledger_entries_txn_idx     ON ledger_entries (txn_id);
CREATE INDEX ledger_entries_account_idx ON ledger_entries (account_code);

-- Append-only: history is never rewritten. Corrections are new transactions.
CREATE FUNCTION ledger_reject_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'ledger is append-only: % on % is not allowed', TG_OP, TG_TABLE_NAME
        USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_accounts_append_only
    BEFORE UPDATE OR DELETE ON ledger_accounts
    FOR EACH ROW EXECUTE FUNCTION ledger_reject_mutation();
CREATE TRIGGER ledger_transactions_append_only
    BEFORE UPDATE OR DELETE ON ledger_transactions
    FOR EACH ROW EXECUTE FUNCTION ledger_reject_mutation();
CREATE TRIGGER ledger_entries_append_only
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_reject_mutation();

-- Balanced: checked at COMMIT (deferred), once all of a transaction's entries are in.
-- Fires from both tables so a transaction with no entries at all is rejected too.
CREATE FUNCTION ledger_assert_balanced() RETURNS trigger AS $$
DECLARE
    tid     UUID;
    debits  BIGINT;
    credits BIGINT;
BEGIN
    IF TG_TABLE_NAME = 'ledger_transactions' THEN
        tid := NEW.id;
    ELSE
        tid := NEW.txn_id;
    END IF;

    SELECT COALESCE(SUM(amount_paise) FILTER (WHERE direction = 'DEBIT'), 0),
           COALESCE(SUM(amount_paise) FILTER (WHERE direction = 'CREDIT'), 0)
      INTO debits, credits
      FROM ledger_entries WHERE txn_id = tid;

    IF debits = 0 OR debits <> credits THEN
        RAISE EXCEPTION 'ledger transaction % is not balanced: debits=% credits=%', tid, debits, credits
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER ledger_transactions_balanced
    AFTER INSERT ON ledger_transactions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_assert_balanced();
CREATE CONSTRAINT TRIGGER ledger_entries_balanced
    AFTER INSERT ON ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_assert_balanced();
