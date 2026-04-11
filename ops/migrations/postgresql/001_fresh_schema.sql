-- =============================================================================
-- Sockudo PostgreSQL fresh schema
-- =============================================================================
-- Canonical fresh-install schema for:
-- - applications table used by the PostgreSQL app manager
-- - durable history tables used by the PostgreSQL history backend
--
-- Presence history does not have separate tables. When both `history.enabled`
-- and `presence_history.enabled` are true, retained presence transitions are
-- stored in the same durable history backend under internal channel names like:
--   [presence-history]presence-room

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
