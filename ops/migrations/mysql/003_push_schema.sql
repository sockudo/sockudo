-- =============================================================================
-- Sockudo MySQL/MariaDB push storage schema
-- =============================================================================
-- Binding model:
-- - small production tier, generally under 10M devices without benchmark proof
-- - partition by app_id hash/key where supported by the selected engine/version
-- - use cursor-compatible indexes; hot paths must not use offset pagination
-- - delivery events and dead letters are time-partition candidates
--
-- Online rollout:
-- 1. Deploy schema with push disabled.
-- 2. Enable dual-write for activation/subscription/publish paths.
-- 3. Backfill in app_id/device_id/channel cursor order.
-- 4. Compare sampled token_hash and channel fanout projections.
-- 5. Switch reads per app, keep dual-write through rollback window.
--
-- Rollback:
-- - Switch reads back before disabling dual-write.
-- - Drop tables in reverse order with DROP TABLE IF EXISTS.
--
-- Credential security:
-- - Store only envelope-encrypted credential material.
-- - KEK source may be raw operator material, KMS key id, or Vault reference.
-- - Rotate by creating a new version, marking it active, draining old in-flight
--   jobs, then retiring the old version.

CREATE TABLE IF NOT EXISTS `push_devices` (
    app_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    device_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    client_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NULL,
    form_factor VARCHAR(32) NOT NULL,
    platform VARCHAR(32) NOT NULL,
    metadata JSON NOT NULL,
    device_secret_hash VARCHAR(255) NOT NULL,
    timezone VARCHAR(128) NOT NULL,
    locale VARCHAR(64) NOT NULL,
    last_active_at_ms BIGINT UNSIGNED NOT NULL,
    push_state VARCHAR(32) NOT NULL,
    failure_count INT UNSIGNED NOT NULL DEFAULT 0,
    error_reason TEXT NULL,
    transport_type VARCHAR(32) NOT NULL,
    token_hash VARCHAR(128) NOT NULL,
    recipient_json JSON NOT NULL,
    credential_version BIGINT UNSIGNED NULL,
    created_at_ms BIGINT UNSIGNED NOT NULL,
    updated_at_ms BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (app_id, device_id),
    KEY push_devices_client_idx (app_id, client_id, device_id),
    KEY push_devices_transport_idx (app_id, transport_type, device_id),
    KEY push_devices_token_idx (app_id, transport_type, token_hash, device_id),
    KEY push_devices_last_active_idx (app_id, last_active_at_ms, device_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `push_channel_subscribers` (
    app_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    channel VARCHAR(512) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    device_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    client_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NULL,
    provider VARCHAR(32) NOT NULL,
    token_hash VARCHAR(128) NOT NULL,
    credential_version BIGINT UNSIGNED NULL,
    recipient_json JSON NOT NULL,
    updated_at_ms BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (app_id, channel, device_id),
    KEY push_channels_by_device_idx (app_id, device_id, channel),
    KEY push_channels_by_client_idx (app_id, client_id, channel, device_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `push_provider_credentials` (
    app_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    credential_id VARCHAR(255) NOT NULL,
    provider VARCHAR(32) NOT NULL,
    version BIGINT UNSIGNED NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    encrypted_material LONGBLOB NOT NULL,
    dek_ciphertext VARBINARY(1024) NOT NULL,
    kek_ref VARCHAR(1024) NOT NULL,
    metadata JSON NOT NULL,
    created_at_ms BIGINT UNSIGNED NOT NULL,
    updated_at_ms BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (app_id, credential_id),
    KEY push_provider_credentials_provider_idx (app_id, provider, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `push_notification_templates` (
    app_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    template_id VARCHAR(255) NOT NULL,
    default_locale VARCHAR(64) NOT NULL,
    locales_json JSON NOT NULL,
    provider_overrides_json JSON NOT NULL,
    created_at_ms BIGINT UNSIGNED NOT NULL,
    updated_at_ms BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (app_id, template_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `push_scheduled_jobs` (
    app_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    publish_id VARCHAR(255) NOT NULL,
    due_minute_ms BIGINT UNSIGNED NOT NULL,
    due_at_ms BIGINT UNSIGNED NOT NULL,
    payload_json JSON NOT NULL,
    state VARCHAR(32) NOT NULL,
    created_at_ms BIGINT UNSIGNED NOT NULL,
    updated_at_ms BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (app_id, publish_id),
    KEY push_scheduled_jobs_due_idx (app_id, due_minute_ms, due_at_ms, publish_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `push_scheduler_locks` (
    app_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    publish_id VARCHAR(255) NOT NULL,
    owner_id VARCHAR(255) NOT NULL,
    expires_at_ms BIGINT UNSIGNED NOT NULL,
    updated_at_ms BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (app_id, publish_id),
    KEY push_scheduler_locks_expiry_idx (app_id, expires_at_ms, publish_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `push_publish_status` (
    app_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    publish_id VARCHAR(255) NOT NULL,
    state VARCHAR(32) NOT NULL,
    counters_json JSON NOT NULL,
    error_reason TEXT NULL,
    created_at_ms BIGINT UNSIGNED NOT NULL,
    updated_at_ms BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (app_id, publish_id),
    KEY push_publish_status_state_idx (app_id, state, updated_at_ms, publish_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `push_publish_log` (
    app_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    publish_id VARCHAR(255) NOT NULL,
    occurred_at_ms BIGINT UNSIGNED NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    event_json JSON NOT NULL,
    PRIMARY KEY (app_id, occurred_at_ms, event_id),
    KEY push_publish_log_publish_idx (app_id, publish_id, occurred_at_ms, event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `push_fanout_shards` (
    app_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    publish_id VARCHAR(255) NOT NULL,
    shard_id VARCHAR(255) NOT NULL,
    shard_json JSON NOT NULL,
    updated_at_ms BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (app_id, publish_id, shard_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `push_delivery_events` (
    app_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    publish_id VARCHAR(255) NOT NULL,
    occurred_at_ms BIGINT UNSIGNED NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    provider VARCHAR(32) NOT NULL,
    outcome VARCHAR(32) NOT NULL,
    result_json JSON NOT NULL,
    PRIMARY KEY (app_id, publish_id, occurred_at_ms, event_id),
    KEY push_delivery_events_time_idx (app_id, occurred_at_ms, event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `push_dead_letters` (
    app_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    publish_id VARCHAR(255) NOT NULL,
    dead_letter_id VARCHAR(255) NOT NULL,
    occurred_at_ms BIGINT UNSIGNED NOT NULL,
    stage VARCHAR(64) NOT NULL,
    reason TEXT NOT NULL,
    payload_json JSON NOT NULL,
    PRIMARY KEY (app_id, publish_id, dead_letter_id),
    KEY push_dead_letters_time_idx (app_id, occurred_at_ms, dead_letter_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `push_idempotency` (
    app_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    publish_id VARCHAR(255) NOT NULL,
    expires_at_ms BIGINT UNSIGNED NOT NULL,
    created_at_ms BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (app_id, idempotency_key),
    KEY push_idempotency_expiry_idx (app_id, expires_at_ms, idempotency_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `push_operator_invalidations` (
    app_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    occurred_at_ms BIGINT UNSIGNED NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    subject VARCHAR(1024) NOT NULL,
    PRIMARY KEY (app_id, occurred_at_ms, event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `push_schema_version` (
    version INT UNSIGNED NOT NULL PRIMARY KEY,
    applied_at_ms BIGINT UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO `push_schema_version` (version, applied_at_ms)
VALUES (1, 0);
