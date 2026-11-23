-- closed_at: when a dead letter was replayed or dismissed (retention counts from it).
ALTER TABLE dead_letters ADD COLUMN closed_at TIMESTAMPTZ;
UPDATE dead_letters SET closed_at = COALESCE(replayed_at, created_at) WHERE status <> 'PENDING';
CREATE INDEX dead_letters_closed_idx ON dead_letters (closed_at) WHERE closed_at IS NOT NULL;

-- Lets the retention job find old published outbox rows without scanning the table.
CREATE INDEX outbox_published_idx ON outbox (published_at) WHERE published_at IS NOT NULL;
