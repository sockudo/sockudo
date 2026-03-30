-- Backfill canonical app policy JSON from legacy Sockudo application columns.
--
-- Safe to run multiple times. Existing non-null `policy` values are preserved.
--
-- IMPORTANT:
-- - This migration reconstructs limits/features/channels/webhooks/idempotency/recovery
--   from legacy columns that actually existed in the MySQL schema.
-- - Legacy MySQL app storage did not persist `channel_namespaces`, so there is
--   nothing to backfill for that field from the database alone.

SET @policy_exists = (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'applications'
      AND COLUMN_NAME = 'policy'
);

SET @sql = IF(
    @policy_exists = 0,
    'ALTER TABLE applications ADD COLUMN policy JSON NULL',
    'SELECT "policy column already exists" AS message'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE applications
SET policy = JSON_OBJECT(
    'limits', JSON_OBJECT(
        'max_connections', max_connections,
        'max_backend_events_per_second', max_backend_events_per_second,
        'max_client_events_per_second', max_client_events_per_second,
        'max_read_requests_per_second', max_read_requests_per_second,
        'max_presence_members_per_channel', max_presence_members_per_channel,
        'max_presence_member_size_in_kb', max_presence_member_size_in_kb,
        'max_channel_name_length', max_channel_name_length,
        'max_event_channels_at_once', max_event_channels_at_once,
        'max_event_name_length', max_event_name_length,
        'max_event_payload_in_kb', max_event_payload_in_kb,
        'max_event_batch_size', max_event_batch_size
    ),
    'features', JSON_OBJECT(
        'enable_client_messages', enable_client_messages,
        'enable_user_authentication', enable_user_authentication,
        'enable_watchlist_events', enable_watchlist_events
    ),
    'channels', JSON_OBJECT(
        'allowed_origins', allowed_origins,
        'channel_delta_compression', channel_delta_compression,
        'channel_namespaces', CAST(NULL AS JSON)
    ),
    'webhooks', webhooks,
    'idempotency', idempotency,
    'connection_recovery', connection_recovery
)
WHERE policy IS NULL;

-- Optional verification:
-- SELECT id, policy FROM applications ORDER BY id LIMIT 10;
