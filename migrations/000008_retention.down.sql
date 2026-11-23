DROP INDEX IF EXISTS outbox_published_idx;
DROP INDEX IF EXISTS dead_letters_closed_idx;
ALTER TABLE dead_letters DROP COLUMN IF EXISTS closed_at;
