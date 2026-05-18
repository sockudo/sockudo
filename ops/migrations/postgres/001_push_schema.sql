-- =============================================================================
-- Sockudo PostgreSQL push storage schema
-- =============================================================================
-- Binding model:
-- - small production tier, generally under 10M devices without benchmark proof
-- - hash partition hot app-scoped tables by app_id
-- - use cursor-compatible indexes; hot paths must not use offset pagination
-- - delivery events and dead letters are time-partition candidates
--
-- Online rollout:
-- 1. Deploy this schema with push disabled.
-- 2. Enable dual-write from activation/subscription/publish paths while reads
--    continue using the previous source of truth, if any.
-- 3. Backfill devices, subscriptions, templates, credentials, scheduled jobs,
--    and idempotency records in app_id order using cursor pagination.
-- 4. Compare row counts and sampled token_hash/channel fanout projections.
-- 5. Switch reads to the push schema per app.
-- 6. Keep dual-write until rollback window expires, then remove old schema.
--
-- Rollback:
-- - Switch reads back to the old source before disabling dual-write.
-- - Drop tables in reverse dependency order with DROP TABLE IF EXISTS.
--
-- Credential security:
-- - Store only envelope-encrypted credential material.
-- - The DEK ciphertext and KEK reference/raw-KEK id live beside metadata.
-- - KEK may be operator-supplied raw material, KMS key id, or Vault reference.
-- - Rotation procedure: write new credential version, warm provider cache, mark
--   new version active, keep old version for in-flight jobs, then retire it.

CREATE TABLE IF NOT EXISTS push_devices (
    app_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    client_id TEXT NULL,
    form_factor TEXT NOT NULL,
    platform TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    device_secret_hash TEXT NOT NULL,
    timezone TEXT NOT NULL,
    locale TEXT NOT NULL,
    last_active_at_ms BIGINT NOT NULL,
    push_state TEXT NOT NULL,
    failure_count INTEGER NOT NULL DEFAULT 0,
    error_reason TEXT NULL,
    transport_type TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    recipient_json JSONB NOT NULL,
    credential_version BIGINT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (app_id, device_id)
) PARTITION BY HASH (app_id);

CREATE INDEX IF NOT EXISTS push_devices_client_idx
ON push_devices (app_id, client_id, device_id)
WHERE client_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS push_devices_transport_idx
ON push_devices (app_id, transport_type, device_id);

CREATE INDEX IF NOT EXISTS push_devices_token_idx
ON push_devices (app_id, transport_type, token_hash, device_id);

CREATE INDEX IF NOT EXISTS push_devices_last_active_idx
ON push_devices (app_id, ((last_active_at_ms / 86400000)), last_active_at_ms, device_id);

CREATE TABLE IF NOT EXISTS push_channel_subscribers (
    app_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    device_id TEXT NOT NULL,
    client_id TEXT NULL,
    provider TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    credential_version BIGINT NULL,
    recipient_json JSONB NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (app_id, channel, device_id)
) PARTITION BY HASH (app_id);

CREATE INDEX IF NOT EXISTS push_channel_subscribers_cursor_idx
ON push_channel_subscribers (app_id, channel, device_id);

CREATE INDEX IF NOT EXISTS push_channels_by_device_idx
ON push_channel_subscribers (app_id, device_id, channel);

CREATE INDEX IF NOT EXISTS push_channels_by_client_idx
ON push_channel_subscribers (app_id, client_id, channel, device_id)
WHERE client_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS push_provider_credentials (
    app_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    version BIGINT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    encrypted_material BYTEA NOT NULL,
    dek_ciphertext BYTEA NOT NULL,
    kek_ref TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (app_id, credential_id)
) PARTITION BY HASH (app_id);

CREATE INDEX IF NOT EXISTS push_provider_credentials_provider_idx
ON push_provider_credentials (app_id, provider, version DESC);

CREATE TABLE IF NOT EXISTS push_notification_templates (
    app_id TEXT NOT NULL,
    template_id TEXT NOT NULL,
    default_locale TEXT NOT NULL,
    locales_json JSONB NOT NULL,
    provider_overrides_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (app_id, template_id)
) PARTITION BY HASH (app_id);

CREATE TABLE IF NOT EXISTS push_scheduled_jobs (
    app_id TEXT NOT NULL,
    publish_id TEXT NOT NULL,
    due_minute_ms BIGINT NOT NULL,
    due_at_ms BIGINT NOT NULL,
    payload_json JSONB NOT NULL,
    state TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (app_id, publish_id)
) PARTITION BY HASH (app_id);

CREATE INDEX IF NOT EXISTS push_scheduled_jobs_due_idx
ON push_scheduled_jobs (app_id, due_minute_ms, due_at_ms, publish_id);

CREATE TABLE IF NOT EXISTS push_scheduler_locks (
    app_id TEXT NOT NULL,
    publish_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (app_id, publish_id)
) PARTITION BY HASH (app_id);

CREATE INDEX IF NOT EXISTS push_scheduler_locks_expiry_idx
ON push_scheduler_locks (app_id, expires_at_ms, publish_id);

CREATE TABLE IF NOT EXISTS push_publish_status (
    app_id TEXT NOT NULL,
    publish_id TEXT NOT NULL,
    state TEXT NOT NULL,
    counters_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_reason TEXT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (app_id, publish_id)
) PARTITION BY HASH (app_id);

CREATE INDEX IF NOT EXISTS push_publish_status_state_idx
ON push_publish_status (app_id, state, updated_at_ms, publish_id);

CREATE TABLE IF NOT EXISTS push_publish_log (
    app_id TEXT NOT NULL,
    publish_id TEXT NOT NULL,
    occurred_at_ms BIGINT NOT NULL,
    event_id TEXT NOT NULL,
    event_json JSONB NOT NULL,
    PRIMARY KEY (app_id, occurred_at_ms, event_id)
) PARTITION BY HASH (app_id);

CREATE INDEX IF NOT EXISTS push_publish_log_publish_idx
ON push_publish_log (app_id, publish_id, occurred_at_ms, event_id);

CREATE TABLE IF NOT EXISTS push_fanout_shards (
    app_id TEXT NOT NULL,
    publish_id TEXT NOT NULL,
    shard_id TEXT NOT NULL,
    shard_json JSONB NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (app_id, publish_id, shard_id)
) PARTITION BY HASH (app_id);

CREATE TABLE IF NOT EXISTS push_delivery_events (
    app_id TEXT NOT NULL,
    publish_id TEXT NOT NULL,
    occurred_at_ms BIGINT NOT NULL,
    event_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    outcome TEXT NOT NULL,
    result_json JSONB NOT NULL,
    PRIMARY KEY (app_id, publish_id, occurred_at_ms, event_id)
) PARTITION BY RANGE (occurred_at_ms);

CREATE INDEX IF NOT EXISTS push_delivery_events_publish_cursor_idx
ON push_delivery_events (app_id, publish_id, occurred_at_ms, event_id);

CREATE TABLE IF NOT EXISTS push_dead_letters (
    app_id TEXT NOT NULL,
    publish_id TEXT NOT NULL,
    dead_letter_id TEXT NOT NULL,
    occurred_at_ms BIGINT NOT NULL,
    stage TEXT NOT NULL,
    reason TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    PRIMARY KEY (app_id, publish_id, dead_letter_id)
) PARTITION BY HASH (app_id);

CREATE INDEX IF NOT EXISTS push_dead_letters_time_idx
ON push_dead_letters (app_id, occurred_at_ms, dead_letter_id);

CREATE TABLE IF NOT EXISTS push_idempotency (
    app_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    publish_id TEXT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (app_id, idempotency_key)
) PARTITION BY HASH (app_id);

CREATE INDEX IF NOT EXISTS push_idempotency_expiry_idx
ON push_idempotency (app_id, expires_at_ms, idempotency_key);

CREATE TABLE IF NOT EXISTS push_operator_invalidations (
    app_id TEXT NOT NULL,
    occurred_at_ms BIGINT NOT NULL,
    event_id TEXT NOT NULL,
    subject TEXT NOT NULL,
    PRIMARY KEY (app_id, occurred_at_ms, event_id)
) PARTITION BY HASH (app_id);

CREATE TABLE IF NOT EXISTS push_schema_version (
    version INTEGER PRIMARY KEY,
    applied_at_ms BIGINT NOT NULL
);

INSERT INTO push_schema_version (version, applied_at_ms)
VALUES (1, 0)
ON CONFLICT (version) DO NOTHING;
