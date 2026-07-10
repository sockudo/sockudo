-- =============================================================================
-- Sockudo MySQL/MariaDB push publish-status CAS upgrade (schema version 2)
-- =============================================================================
-- Stop push admission and all old push workers before applying this migration.
-- A mixed deployment is unsafe because schema-version-1 workers use blind status writes.
-- MySQL DDL commits implicitly: add the column before recording schema version 2.

ALTER TABLE `push_publish_status`
    ADD COLUMN revision BIGINT UNSIGNED NOT NULL DEFAULT 1 AFTER updated_at_ms;

INSERT IGNORE INTO `push_schema_version` (version, applied_at_ms)
VALUES (2, 0);

-- Rollback requires stopping all schema-version-2 workers first, then running:
-- DELETE FROM `push_schema_version` WHERE version = 2;
-- ALTER TABLE `push_publish_status` DROP COLUMN revision;
