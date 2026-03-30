-- Drop legacy Sockudo PostgreSQL app columns after policy backfill.
--
-- PREREQUISITES:
-- 1. Run `002_backfill_app_policy.sql`.
-- 2. Verify every row has a non-null `policy`.
-- 3. Run this during a controlled maintenance window.

ALTER TABLE applications DROP COLUMN IF EXISTS max_connections;
ALTER TABLE applications DROP COLUMN IF EXISTS enable_client_messages;
ALTER TABLE applications DROP COLUMN IF EXISTS max_backend_events_per_second;
ALTER TABLE applications DROP COLUMN IF EXISTS max_client_events_per_second;
ALTER TABLE applications DROP COLUMN IF EXISTS max_read_requests_per_second;
ALTER TABLE applications DROP COLUMN IF EXISTS max_presence_members_per_channel;
ALTER TABLE applications DROP COLUMN IF EXISTS max_presence_member_size_in_kb;
ALTER TABLE applications DROP COLUMN IF EXISTS max_channel_name_length;
ALTER TABLE applications DROP COLUMN IF EXISTS max_event_channels_at_once;
ALTER TABLE applications DROP COLUMN IF EXISTS max_event_name_length;
ALTER TABLE applications DROP COLUMN IF EXISTS max_event_payload_in_kb;
ALTER TABLE applications DROP COLUMN IF EXISTS max_event_batch_size;
ALTER TABLE applications DROP COLUMN IF EXISTS enable_user_authentication;
ALTER TABLE applications DROP COLUMN IF EXISTS enable_watchlist_events;
ALTER TABLE applications DROP COLUMN IF EXISTS webhooks;
ALTER TABLE applications DROP COLUMN IF EXISTS allowed_origins;
ALTER TABLE applications DROP COLUMN IF EXISTS channel_delta_compression;
ALTER TABLE applications DROP COLUMN IF EXISTS idempotency;
ALTER TABLE applications DROP COLUMN IF EXISTS connection_recovery;
