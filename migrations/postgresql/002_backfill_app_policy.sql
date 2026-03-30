-- Backfill canonical app policy JSONB from legacy Sockudo application columns.
--
-- Safe to run multiple times. Existing non-null `policy` values are preserved.
--
-- IMPORTANT:
-- - This migration reconstructs limits/features/channels/webhooks/idempotency/recovery
--   from legacy columns that actually existed in the PostgreSQL schema.
-- - Legacy PostgreSQL app storage did not persist `channel_namespaces`, so there is
--   nothing to backfill for that field from the database alone.

ALTER TABLE applications
ADD COLUMN IF NOT EXISTS policy JSONB;

UPDATE applications
SET policy = jsonb_build_object(
    'limits',
    jsonb_build_object(
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
    'features',
    jsonb_build_object(
        'enable_client_messages', enable_client_messages,
        'enable_user_authentication', enable_user_authentication,
        'enable_watchlist_events', enable_watchlist_events
    ),
    'channels',
    jsonb_build_object(
        'allowed_origins', allowed_origins,
        'channel_delta_compression', channel_delta_compression,
        'channel_namespaces', NULL
    ),
    'webhooks', webhooks,
    'idempotency', idempotency,
    'connection_recovery', connection_recovery
)
WHERE policy IS NULL;

-- Optional verification:
-- SELECT id, policy FROM applications ORDER BY id LIMIT 10;
