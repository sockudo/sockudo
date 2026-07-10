-- =============================================================================
-- Sockudo ScyllaDB push storage schema
-- =============================================================================
-- Hyperscale recommended path for 10M to 1B+ devices with disciplined data
-- modeling. Runtime startup should create/evolve equivalent tables and fail
-- closed if schema validation fails.
--
-- Online migration:
-- 1. Create schema with push disabled.
-- 2. Enable dual-write for all push storage families.
-- 3. Backfill by app, bucket, and clustering-key cursor ranges.
-- 4. Compare sampled token hashes, channel fanout counts, due jobs, and status.
-- 5. Switch reads per app.
-- 6. Retire old schema after rollback window.
--
-- Rollback:
-- - Switch reads back before disabling dual-write.
-- - Drop tables only after event/status retention and forensic needs are met.
--
-- Credential security:
-- - Store envelope-encrypted material only.
-- - KEK source may be raw operator material, KMS key id, or Vault reference.
-- - Rotate by writing a new immutable version, warming provider caches, marking
--   it active, draining old in-flight jobs, then retiring the old version.

CREATE KEYSPACE IF NOT EXISTS sockudo_push
WITH replication = {'class': 'NetworkTopologyStrategy'};

CREATE TABLE IF NOT EXISTS sockudo_push.devices_by_id (
    app_id text,
    device_id text,
    client_id text,
    form_factor text,
    platform text,
    metadata_json text,
    device_secret_hash text,
    timezone text,
    locale text,
    last_active_at_ms bigint,
    push_state text,
    failure_count int,
    error_reason text,
    transport_type text,
    token_hash text,
    recipient_json text,
    credential_version bigint,
    created_at_ms bigint,
    updated_at_ms bigint,
    PRIMARY KEY ((app_id, device_id))
);

-- Runtime document store used by the ScyllaDB push backend. The denormalized
-- tables below remain the capacity-oriented model for future hot-path fanout
-- tuning; this table is the authoritative bootstrap store used by Sockudo.
CREATE TABLE IF NOT EXISTS sockudo_push.push_records (
    family_app text,
    pk text,
    sk text,
    app_id text,
    data text,
    PRIMARY KEY ((family_app), pk, sk)
) WITH CLUSTERING ORDER BY (pk ASC, sk ASC);

CREATE TABLE IF NOT EXISTS sockudo_push.devices_by_client (
    app_id text,
    client_id text,
    bucket int,
    device_id text,
    transport_type text,
    token_hash text,
    recipient_json text,
    PRIMARY KEY ((app_id, client_id, bucket), device_id)
) WITH CLUSTERING ORDER BY (device_id ASC);

CREATE TABLE IF NOT EXISTS sockudo_push.devices_by_transport (
    app_id text,
    transport_type text,
    bucket int,
    device_id text,
    token_hash text,
    recipient_json text,
    PRIMARY KEY ((app_id, transport_type, bucket), device_id)
) WITH CLUSTERING ORDER BY (device_id ASC);

CREATE TABLE IF NOT EXISTS sockudo_push.devices_by_token (
    app_id text,
    transport_type text,
    token_hash text,
    device_id text,
    PRIMARY KEY ((app_id, transport_type, token_hash), device_id)
) WITH CLUSTERING ORDER BY (device_id ASC);

CREATE TABLE IF NOT EXISTS sockudo_push.devices_by_last_active (
    app_id text,
    day_bucket text,
    bucket int,
    last_active_at_ms bigint,
    device_id text,
    PRIMARY KEY ((app_id, day_bucket, bucket), last_active_at_ms, device_id)
) WITH CLUSTERING ORDER BY (last_active_at_ms ASC, device_id ASC);

CREATE TABLE IF NOT EXISTS sockudo_push.channel_subscribers (
    app_id text,
    channel text,
    bucket int,
    device_id text,
    client_id text,
    provider text,
    token_hash text,
    credential_version bigint,
    recipient_json text,
    PRIMARY KEY ((app_id, channel, bucket), device_id)
) WITH CLUSTERING ORDER BY (device_id ASC);

CREATE TABLE IF NOT EXISTS sockudo_push.channels_by_device (
    app_id text,
    device_id text,
    channel text,
    PRIMARY KEY ((app_id, device_id), channel)
) WITH CLUSTERING ORDER BY (channel ASC);

CREATE TABLE IF NOT EXISTS sockudo_push.channels_by_client (
    app_id text,
    client_id text,
    bucket int,
    channel text,
    device_id text,
    PRIMARY KEY ((app_id, client_id, bucket), channel, device_id)
) WITH CLUSTERING ORDER BY (channel ASC, device_id ASC);

CREATE TABLE IF NOT EXISTS sockudo_push.provider_credentials (
    app_id text,
    credential_id text,
    version bigint,
    provider text,
    active boolean,
    encrypted_material blob,
    dek_ciphertext blob,
    kek_ref text,
    metadata_json text,
    created_at_ms bigint,
    updated_at_ms bigint,
    PRIMARY KEY ((app_id, credential_id), version)
) WITH CLUSTERING ORDER BY (version DESC);

CREATE TABLE IF NOT EXISTS sockudo_push.notification_templates (
    app_id text,
    template_id text,
    default_locale text,
    locales_json text,
    provider_overrides_json text,
    created_at_ms bigint,
    updated_at_ms bigint,
    PRIMARY KEY ((app_id, template_id))
);

CREATE TABLE IF NOT EXISTS sockudo_push.scheduled_jobs_by_due_minute (
    app_id text,
    due_minute_ms bigint,
    bucket int,
    due_at_ms bigint,
    publish_id text,
    payload_json text,
    state text,
    PRIMARY KEY ((app_id, due_minute_ms, bucket), due_at_ms, publish_id)
) WITH CLUSTERING ORDER BY (due_at_ms ASC, publish_id ASC);

CREATE TABLE IF NOT EXISTS sockudo_push.scheduled_jobs_by_id (
    app_id text,
    publish_id text,
    due_minute_ms bigint,
    due_at_ms bigint,
    payload_json text,
    state text,
    PRIMARY KEY ((app_id, publish_id))
);

CREATE TABLE IF NOT EXISTS sockudo_push.publish_status (
    app_id text,
    publish_id text,
    state text,
    counters_json text,
    error_reason text,
    revision bigint,
    created_at_ms bigint,
    updated_at_ms bigint,
    PRIMARY KEY ((app_id, publish_id))
);

CREATE TABLE IF NOT EXISTS sockudo_push.push_delivery_events (
    app_id text,
    publish_id text,
    occurred_at_ms bigint,
    event_id text,
    provider text,
    outcome text,
    result_json text,
    PRIMARY KEY ((app_id, publish_id), occurred_at_ms, event_id)
) WITH CLUSTERING ORDER BY (occurred_at_ms ASC, event_id ASC);

CREATE TABLE IF NOT EXISTS sockudo_push.push_dead_letters (
    app_id text,
    publish_id text,
    occurred_at_ms bigint,
    dead_letter_id text,
    stage text,
    reason text,
    payload_json text,
    PRIMARY KEY ((app_id, publish_id), occurred_at_ms, dead_letter_id)
) WITH CLUSTERING ORDER BY (occurred_at_ms ASC, dead_letter_id ASC);

CREATE TABLE IF NOT EXISTS sockudo_push.push_idempotency (
    app_id text,
    idempotency_key text,
    publish_id text,
    expires_at_ms bigint,
    created_at_ms bigint,
    PRIMARY KEY ((app_id, idempotency_key))
);
