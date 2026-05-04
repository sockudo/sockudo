-- Sockudo SurrealDB push storage schema.
--
-- Supported small deployment tier only unless benchmarks prove otherwise.
-- Runtime startup should apply equivalent DEFINE TABLE / DEFINE INDEX commands
-- and fail closed if schema evolution cannot be confirmed.
--
-- Online migration: deploy schema disabled, dual-write, backfill by app cursor,
-- switch reads per app, then drop old schema after rollback window.
-- Rollback: switch reads back before disabling dual-write.
--
-- Credential security: store envelope-encrypted provider material only. KEK
-- reference may be raw operator material, KMS id, or Vault path. Rotation writes
-- a new immutable credential version, warms provider caches, marks it active,
-- drains old in-flight jobs, and retires the old version.

DEFINE TABLE push_devices SCHEMAFULL;
DEFINE FIELD app_id ON push_devices TYPE string;
DEFINE FIELD device_id ON push_devices TYPE string;
DEFINE FIELD client_id ON push_devices TYPE option<string>;
DEFINE FIELD form_factor ON push_devices TYPE string;
DEFINE FIELD platform ON push_devices TYPE string;
DEFINE FIELD metadata ON push_devices TYPE object;
DEFINE FIELD device_secret_hash ON push_devices TYPE string;
DEFINE FIELD timezone ON push_devices TYPE string;
DEFINE FIELD locale ON push_devices TYPE string;
DEFINE FIELD last_active_at_ms ON push_devices TYPE int;
DEFINE FIELD push_state ON push_devices TYPE string;
DEFINE FIELD failure_count ON push_devices TYPE int;
DEFINE FIELD error_reason ON push_devices TYPE option<string>;
DEFINE FIELD transport_type ON push_devices TYPE string;
DEFINE FIELD token_hash ON push_devices TYPE string;
DEFINE FIELD recipient_json ON push_devices TYPE object;
DEFINE INDEX devices_by_id ON push_devices FIELDS app_id, device_id UNIQUE;
DEFINE INDEX devices_by_client ON push_devices FIELDS app_id, client_id, device_id;
DEFINE INDEX devices_by_transport ON push_devices FIELDS app_id, transport_type, device_id;
DEFINE INDEX devices_by_token ON push_devices FIELDS app_id, transport_type, token_hash, device_id;
DEFINE INDEX devices_by_last_active ON push_devices FIELDS app_id, last_active_at_ms, device_id;

-- Runtime document store used by the SurrealDB push backend. The schemafull
-- tables below remain the reference model for operator capacity planning; this
-- table is the authoritative bootstrap store used by Sockudo.
DEFINE TABLE push_records SCHEMALESS;
DEFINE INDEX push_records_key ON push_records FIELDS family_app, pk, sk UNIQUE;
DEFINE INDEX push_records_app ON push_records FIELDS family_app;

DEFINE TABLE push_channel_subscribers SCHEMAFULL;
DEFINE FIELD app_id ON push_channel_subscribers TYPE string;
DEFINE FIELD channel ON push_channel_subscribers TYPE string;
DEFINE FIELD device_id ON push_channel_subscribers TYPE string;
DEFINE FIELD client_id ON push_channel_subscribers TYPE option<string>;
DEFINE FIELD provider ON push_channel_subscribers TYPE string;
DEFINE FIELD token_hash ON push_channel_subscribers TYPE string;
DEFINE FIELD credential_version ON push_channel_subscribers TYPE option<int>;
DEFINE FIELD recipient_json ON push_channel_subscribers TYPE object;
DEFINE INDEX channel_subscribers ON push_channel_subscribers FIELDS app_id, channel, device_id UNIQUE;
DEFINE INDEX channels_by_device ON push_channel_subscribers FIELDS app_id, device_id, channel;
DEFINE INDEX channels_by_client ON push_channel_subscribers FIELDS app_id, client_id, channel, device_id;

DEFINE TABLE push_provider_credentials SCHEMAFULL;
DEFINE FIELD app_id ON push_provider_credentials TYPE string;
DEFINE FIELD credential_id ON push_provider_credentials TYPE string;
DEFINE FIELD provider ON push_provider_credentials TYPE string;
DEFINE FIELD version ON push_provider_credentials TYPE int;
DEFINE FIELD active ON push_provider_credentials TYPE bool;
DEFINE FIELD encrypted_material ON push_provider_credentials TYPE bytes;
DEFINE FIELD dek_ciphertext ON push_provider_credentials TYPE bytes;
DEFINE FIELD kek_ref ON push_provider_credentials TYPE string;
DEFINE FIELD metadata ON push_provider_credentials TYPE object;
DEFINE INDEX provider_credentials ON push_provider_credentials FIELDS app_id, credential_id UNIQUE;
DEFINE INDEX provider_credentials_by_provider ON push_provider_credentials FIELDS app_id, provider, version;

DEFINE TABLE push_notification_templates SCHEMAFULL;
DEFINE FIELD app_id ON push_notification_templates TYPE string;
DEFINE FIELD template_id ON push_notification_templates TYPE string;
DEFINE FIELD default_locale ON push_notification_templates TYPE string;
DEFINE FIELD locales_json ON push_notification_templates TYPE object;
DEFINE FIELD provider_overrides_json ON push_notification_templates TYPE object;
DEFINE INDEX notification_templates ON push_notification_templates FIELDS app_id, template_id UNIQUE;

DEFINE TABLE push_scheduled_jobs SCHEMAFULL;
DEFINE FIELD app_id ON push_scheduled_jobs TYPE string;
DEFINE FIELD publish_id ON push_scheduled_jobs TYPE string;
DEFINE FIELD due_minute_ms ON push_scheduled_jobs TYPE int;
DEFINE FIELD due_at_ms ON push_scheduled_jobs TYPE int;
DEFINE FIELD payload_json ON push_scheduled_jobs TYPE object;
DEFINE FIELD state ON push_scheduled_jobs TYPE string;
DEFINE INDEX scheduled_jobs_by_id ON push_scheduled_jobs FIELDS app_id, publish_id UNIQUE;
DEFINE INDEX scheduled_jobs_by_due_minute ON push_scheduled_jobs FIELDS app_id, due_minute_ms, due_at_ms, publish_id;

DEFINE TABLE push_publish_status SCHEMAFULL;
DEFINE FIELD app_id ON push_publish_status TYPE string;
DEFINE FIELD publish_id ON push_publish_status TYPE string;
DEFINE FIELD state ON push_publish_status TYPE string;
DEFINE FIELD counters_json ON push_publish_status TYPE object;
DEFINE FIELD error_reason ON push_publish_status TYPE option<string>;
DEFINE INDEX publish_status ON push_publish_status FIELDS app_id, publish_id UNIQUE;

DEFINE TABLE push_delivery_events SCHEMAFULL;
DEFINE FIELD app_id ON push_delivery_events TYPE string;
DEFINE FIELD publish_id ON push_delivery_events TYPE string;
DEFINE FIELD occurred_at_ms ON push_delivery_events TYPE int;
DEFINE FIELD event_id ON push_delivery_events TYPE string;
DEFINE FIELD provider ON push_delivery_events TYPE string;
DEFINE FIELD outcome ON push_delivery_events TYPE string;
DEFINE FIELD result_json ON push_delivery_events TYPE object;
DEFINE INDEX push_delivery_events_by_publish ON push_delivery_events FIELDS app_id, publish_id, occurred_at_ms, event_id UNIQUE;
DEFINE INDEX push_delivery_events_retention ON push_delivery_events FIELDS app_id, occurred_at_ms, event_id;

DEFINE TABLE push_dead_letters SCHEMAFULL;
DEFINE FIELD app_id ON push_dead_letters TYPE string;
DEFINE FIELD publish_id ON push_dead_letters TYPE string;
DEFINE FIELD dead_letter_id ON push_dead_letters TYPE string;
DEFINE FIELD occurred_at_ms ON push_dead_letters TYPE int;
DEFINE FIELD stage ON push_dead_letters TYPE string;
DEFINE FIELD reason ON push_dead_letters TYPE string;
DEFINE FIELD payload_json ON push_dead_letters TYPE object;
DEFINE INDEX push_dead_letters_by_publish ON push_dead_letters FIELDS app_id, publish_id, dead_letter_id UNIQUE;

DEFINE TABLE push_idempotency SCHEMAFULL;
DEFINE FIELD app_id ON push_idempotency TYPE string;
DEFINE FIELD idempotency_key ON push_idempotency TYPE string;
DEFINE FIELD publish_id ON push_idempotency TYPE string;
DEFINE FIELD expires_at_ms ON push_idempotency TYPE int;
DEFINE INDEX push_idempotency ON push_idempotency FIELDS app_id, idempotency_key UNIQUE;
DEFINE INDEX push_idempotency_expiry ON push_idempotency FIELDS app_id, expires_at_ms, idempotency_key;
