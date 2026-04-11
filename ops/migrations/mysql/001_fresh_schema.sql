-- =============================================================================
-- Sockudo MySQL/MariaDB fresh schema
-- =============================================================================
-- Canonical fresh-install schema for:
-- - applications table used by the MySQL app manager
-- - durable history tables used by the MySQL history backend
--
-- Presence history does not have separate tables. When both `history.enabled`
-- and `presence_history.enabled` are true, retained presence transitions are
-- stored in the same durable history backend under internal channel names like:
--   [presence-history]presence-room

CREATE TABLE IF NOT EXISTS `applications` (
    id VARCHAR(255) PRIMARY KEY,
    `key` VARCHAR(255) UNIQUE NOT NULL,
    secret VARCHAR(255) NOT NULL,
    max_connections INT UNSIGNED NOT NULL,
    enable_client_messages BOOLEAN NOT NULL DEFAULT FALSE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    max_backend_events_per_second INT UNSIGNED NULL,
    max_client_events_per_second INT UNSIGNED NOT NULL,
    max_read_requests_per_second INT UNSIGNED NULL,
    max_presence_members_per_channel INT UNSIGNED NULL,
    max_presence_member_size_in_kb INT UNSIGNED NULL,
    max_channel_name_length INT UNSIGNED NULL,
    max_event_channels_at_once INT UNSIGNED NULL,
    max_event_name_length INT UNSIGNED NULL,
    max_event_payload_in_kb INT UNSIGNED NULL,
    max_event_batch_size INT UNSIGNED NULL,
    enable_user_authentication BOOLEAN NULL,
    enable_watchlist_events BOOLEAN NULL,
    policy JSON NULL,
    webhooks JSON NULL,
    allowed_origins JSON NULL,
    channel_delta_compression JSON NULL,
    idempotency JSON NULL,
    connection_recovery JSON NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_applications_key ON `applications` (`key`);
CREATE INDEX idx_applications_enabled ON `applications` (enabled);
CREATE INDEX idx_applications_created_at ON `applications` (created_at);

CREATE TABLE IF NOT EXISTS `sockudo_history_streams` (
    app_id VARCHAR(255) NOT NULL,
    channel VARCHAR(255) NOT NULL,
    stream_id VARCHAR(255) NOT NULL,
    next_serial BIGINT NOT NULL,
    durable_state VARCHAR(32) NOT NULL DEFAULT 'healthy',
    durable_state_reason TEXT NULL,
    durable_state_node_id VARCHAR(255) NULL,
    durable_state_changed_at_ms BIGINT NULL,
    retained_messages BIGINT NOT NULL DEFAULT 0,
    retained_bytes BIGINT NOT NULL DEFAULT 0,
    oldest_available_serial BIGINT NULL,
    newest_available_serial BIGINT NULL,
    oldest_available_published_at_ms BIGINT NULL,
    newest_available_published_at_ms BIGINT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (app_id, channel)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `sockudo_history_entries` (
    app_id VARCHAR(255) NOT NULL,
    channel VARCHAR(255) NOT NULL,
    stream_id VARCHAR(255) NOT NULL,
    serial BIGINT NOT NULL,
    published_at_ms BIGINT NOT NULL,
    message_id VARCHAR(255) NULL,
    event_name VARCHAR(255) NULL,
    operation_kind VARCHAR(64) NOT NULL,
    payload_bytes LONGBLOB NOT NULL,
    payload_size_bytes BIGINT NOT NULL,
    metadata JSON NULL,
    tombstone BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (app_id, channel, stream_id, serial)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX sockudo_history_entries_app_channel_serial_idx
ON `sockudo_history_entries` (app_id, channel, serial DESC);

CREATE INDEX sockudo_history_entries_app_channel_time_idx
ON `sockudo_history_entries` (app_id, channel, published_at_ms DESC, serial DESC);

INSERT INTO `applications` (
    id,
    `key`,
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
ON DUPLICATE KEY UPDATE id = VALUES(id);
