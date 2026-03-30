-- Drop legacy Sockudo MySQL app columns after policy backfill.
--
-- PREREQUISITES:
-- 1. Run `002_backfill_app_policy.sql`.
-- 2. Verify every row has a non-null `policy`.
-- 3. Run this during a controlled maintenance window.

SET @drop_columns = '
ALTER TABLE applications
    DROP COLUMN max_connections,
    DROP COLUMN enable_client_messages,
    DROP COLUMN max_backend_events_per_second,
    DROP COLUMN max_client_events_per_second,
    DROP COLUMN max_read_requests_per_second,
    DROP COLUMN max_presence_members_per_channel,
    DROP COLUMN max_presence_member_size_in_kb,
    DROP COLUMN max_channel_name_length,
    DROP COLUMN max_event_channels_at_once,
    DROP COLUMN max_event_name_length,
    DROP COLUMN max_event_payload_in_kb,
    DROP COLUMN max_event_batch_size,
    DROP COLUMN enable_user_authentication,
    DROP COLUMN enable_watchlist_events,
    DROP COLUMN webhooks,
    DROP COLUMN allowed_origins,
    DROP COLUMN channel_delta_compression,
    DROP COLUMN idempotency,
    DROP COLUMN connection_recovery
';

PREPARE stmt FROM @drop_columns;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
