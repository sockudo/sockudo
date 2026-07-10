-- =============================================================================
-- Sockudo PostgreSQL push publish-status CAS upgrade (schema version 2)
-- =============================================================================
-- Stop push admission and all old push workers before applying this migration.
-- A mixed deployment is unsafe because schema-version-1 workers use blind status writes.
-- The constant default initializes retained statuses at revision 1.

BEGIN;

ALTER TABLE push_publish_status
    ADD COLUMN revision BIGINT NOT NULL DEFAULT 1
    CHECK (revision > 0);

INSERT INTO push_schema_version (version, applied_at_ms)
VALUES (2, 0)
ON CONFLICT (version) DO NOTHING;

COMMIT;

-- Rollback requires stopping all schema-version-2 workers first, then running:
-- DELETE FROM push_schema_version WHERE version = 2;
-- ALTER TABLE push_publish_status DROP COLUMN revision;
