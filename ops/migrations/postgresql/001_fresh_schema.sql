-- =============================================================================
-- Sockudo PostgreSQL fresh schema
-- =============================================================================
-- Canonical fresh-install schema for:
-- - applications table used by the PostgreSQL app manager
-- - durable history tables used by the PostgreSQL history backend
-- - versioned durable-message tables used by the release 4.3 mutable-message layer
--
-- Presence history does not have separate tables. When both `history.enabled`
-- and `presence_history.enabled` are true, retained presence transitions are
-- stored in the same durable history backend under internal channel names like:
--   [presence-history]presence-room
--
-- Backfill note:
-- Existing immutable history is not backfilled into the versioned-message tables.
-- Only release-4.3-aware mutable messages should populate those tables.

CREATE TABLE IF NOT EXISTS applications (
    id VARCHAR(255) PRIMARY KEY,
    key VARCHAR(255) UNIQUE NOT NULL,
    secret VARCHAR(255) NOT NULL,
    max_connections INTEGER NOT NULL,
    enable_client_messages BOOLEAN NOT NULL DEFAULT FALSE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    max_backend_events_per_second INTEGER,
    max_client_events_per_second INTEGER NOT NULL,
    max_read_requests_per_second INTEGER,
    max_presence_members_per_channel INTEGER,
    max_presence_member_size_in_kb INTEGER,
    max_channel_name_length INTEGER,
    max_event_channels_at_once INTEGER,
    max_event_name_length INTEGER,
    max_event_payload_in_kb INTEGER,
    max_event_batch_size INTEGER,
    enable_user_authentication BOOLEAN,
    enable_watchlist_events BOOLEAN,
    policy JSONB,
    webhooks JSONB,
    allowed_origins JSONB,
    channel_delta_compression JSONB,
    idempotency JSONB,
    connection_recovery JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_applications_key ON applications(key);
CREATE INDEX IF NOT EXISTS idx_applications_enabled ON applications(enabled);
CREATE INDEX IF NOT EXISTS idx_applications_created_at ON applications(created_at);

CREATE TABLE IF NOT EXISTS sockudo_history_streams (
    app_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    next_serial BIGINT NOT NULL,
    durable_state TEXT NOT NULL DEFAULT 'healthy',
    durable_state_reason TEXT NULL,
    durable_state_node_id TEXT NULL,
    durable_state_changed_at_ms BIGINT NULL,
    retained_messages BIGINT NOT NULL DEFAULT 0,
    retained_bytes BIGINT NOT NULL DEFAULT 0,
    oldest_available_serial BIGINT NULL,
    newest_available_serial BIGINT NULL,
    oldest_available_published_at_ms BIGINT NULL,
    newest_available_published_at_ms BIGINT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (app_id, channel)
);

CREATE TABLE IF NOT EXISTS sockudo_history_entries (
    app_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    serial BIGINT NOT NULL,
    published_at_ms BIGINT NOT NULL,
    message_id TEXT NULL,
    event_name TEXT NULL,
    operation_kind TEXT NOT NULL,
    payload_bytes BYTEA NOT NULL,
    payload_size_bytes BIGINT NOT NULL,
    metadata JSONB NULL,
    tombstone BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (app_id, channel, stream_id, serial)
);

CREATE INDEX IF NOT EXISTS sockudo_history_entries_app_channel_serial_idx
ON sockudo_history_entries (app_id, channel, serial DESC);

CREATE INDEX IF NOT EXISTS sockudo_history_entries_app_channel_time_idx
ON sockudo_history_entries (app_id, channel, published_at_ms DESC, serial DESC);

CREATE TABLE IF NOT EXISTS sockudo_history_version_streams (
    app_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    next_delivery_serial BIGINT NOT NULL,
    oldest_available_delivery_serial BIGINT NULL,
    newest_available_delivery_serial BIGINT NULL,
    migration_state TEXT NOT NULL DEFAULT 'native_only',
    migration_state_changed_at_ms BIGINT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (app_id, channel)
);

CREATE TABLE IF NOT EXISTS sockudo_history_version_messages (
    app_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    message_serial TEXT NOT NULL,
    history_serial BIGINT NOT NULL,
    original_client_id TEXT NULL,
    latest_version_serial TEXT NOT NULL,
    latest_delivery_serial BIGINT NOT NULL,
    latest_action TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (app_id, channel, message_serial)
);

CREATE UNIQUE INDEX IF NOT EXISTS sockudo_history_version_messages_history_serial_uidx
ON sockudo_history_version_messages (app_id, channel, history_serial);

CREATE INDEX IF NOT EXISTS sockudo_history_version_messages_latest_version_idx
ON sockudo_history_version_messages (app_id, channel, latest_version_serial);

CREATE TABLE IF NOT EXISTS sockudo_history_version_entries (
    app_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    message_serial TEXT NOT NULL,
    version_serial TEXT NOT NULL,
    delivery_serial BIGINT NOT NULL,
    history_serial BIGINT NOT NULL,
    action TEXT NOT NULL,
    client_id TEXT NULL,
    description TEXT NULL,
    operation_metadata JSONB NULL,
    event_name TEXT NULL,
    payload_bytes BYTEA NOT NULL,
    payload_size_bytes BIGINT NOT NULL,
    version_timestamp_ms BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (app_id, channel, message_serial, version_serial)
);

CREATE UNIQUE INDEX IF NOT EXISTS sockudo_history_version_entries_delivery_uidx
ON sockudo_history_version_entries (app_id, channel, delivery_serial);

CREATE INDEX IF NOT EXISTS sockudo_history_version_entries_message_version_idx
ON sockudo_history_version_entries (app_id, channel, message_serial, version_serial DESC);

CREATE INDEX IF NOT EXISTS sockudo_history_version_entries_replay_idx
ON sockudo_history_version_entries (app_id, channel, delivery_serial);

CREATE INDEX IF NOT EXISTS sockudo_history_version_entries_history_version_idx
ON sockudo_history_version_entries (app_id, channel, history_serial, version_serial DESC);

INSERT INTO applications (
    id,
    key,
    secret,
    max_connections,
    enable_client_messages,
    enabled,
    max_client_events_per_second
) VALUES (
    'app-id',
    'app-key',
    'app-secret',
    100000,
    TRUE,
    TRUE,
    1000
)
ON CONFLICT (id) DO NOTHING;
