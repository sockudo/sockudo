# DynamoDB Push Storage Schema

Status: binding bootstrap model for the `dynamodb` push backend.

DynamoDB tables are created/evolved by the runtime, but the table model is
checked in here so startup validation and operator provisioning have one source
of truth.

The runtime backend creates one compact document table named
`<configured_table_name>_push_records` with partition key `family_app` and sort
key `doc_key`. Each item also stores `app_id`, logical `pk`, logical `sk`, and
serialized `data`. The logical families below map into that table and can be
split into the denormalized tables/GSIs later when an app needs hotter fanout
partitions.

## Tables And Keys

| Logical family | Table / GSI | Partition key | Sort key |
| --- | --- | --- | --- |
| `devices_by_id` | `push_devices` | `APP#<app_id>#DEVICE#<device_id>` | `META` |
| `devices_by_client` | `gsi_client_devices` | `APP#<app_id>#CLIENT#<client_id>` | `DEVICE#<device_id>` |
| `devices_by_transport` | `gsi_transport_devices` | `APP#<app_id>#TRANSPORT#<transport_type>` | `DEVICE#<device_id>` |
| `devices_by_token` | `gsi_tokens` | `APP#<app_id>#TRANSPORT#<transport_type>#TOKEN#<token_hash>` | `DEVICE#<device_id>` |
| `devices_by_last_active` | `gsi_last_active` | `APP#<app_id>#DAY#<day_bucket>` | `TS#<last_active_at_ms>#DEVICE#<device_id>` |
| `channel_subscribers` | `push_channel_subscribers` | `APP#<app_id>#CHANNEL#<channel>#BUCKET#<bucket>` | `DEVICE#<device_id>` |
| `channels_by_device` | `gsi_channels_by_device` | `APP#<app_id>#DEVICE#<device_id>` | `CHANNEL#<channel>` |
| `channels_by_client` | `gsi_channels_by_client` | `APP#<app_id>#CLIENT#<client_id>` | `CHANNEL#<channel>#DEVICE#<device_id>` |
| `provider_credentials` | `push_provider_credentials` | `APP#<app_id>#CREDENTIAL#<credential_id>` | `VERSION#<version>` |
| `notification_templates` | `push_notification_templates` | `APP#<app_id>#TEMPLATE#<template_id>` | `META` |
| `scheduled_jobs_by_due_minute` | `gsi_scheduled_due` | `APP#<app_id>#DUE#<due_minute_ms>` | `DUE#<due_at_ms>#PUBLISH#<publish_id>` |
| `scheduled_jobs_by_id` | `push_scheduled_jobs` | `APP#<app_id>#PUBLISH#<publish_id>` | `SCHEDULE` |
| `publish_status` | `push_publish_status` | `APP#<app_id>#PUBLISH#<publish_id>` | `STATUS` |
| `push_delivery_events` | `push_delivery_events` | `APP#<app_id>#PUBLISH#<publish_id>` | `TS#<occurred_at_ms>#EVENT#<event_id>` |
| `push_dead_letters` | `push_dead_letters` | `APP#<app_id>#PUBLISH#<publish_id>` | `DLQ#<occurred_at_ms>#<dead_letter_id>` |
| `push_idempotency` | `push_idempotency` | `APP#<app_id>#IDEMPOTENCY#<idempotency_key>` | `META` |

`publish_status` data is a versioned envelope containing the public status, a
strictly positive `revision`, and authoritative `updated_at_ms`. Creation uses a
conditional put and every replacement conditions on the complete prior
envelope. The update-time index is advisory; cleanup conditionally deletes only
the envelope revision and timestamp it inspected.

Hot channels must derive `bucket` from stable hashing of `device_id`. Bucket
counts are part of app-level capacity configuration and may increase only
through dual-write/backfill.

## Cursor Rules

List APIs use exclusive-start keys encoded in `PushCursor`. Offset pagination is
not allowed.

## Credential Security

Credential material is envelope encrypted:

- encrypted material stores provider credential ciphertext
- encrypted DEK stores the data key ciphertext
- `kek_ref` identifies raw operator KEK material, KMS key id, or Vault path
- credential versions are immutable; rotation creates a new version and then
  flips the active pointer after provider cache warmup

## Online Migration

1. Create tables and GSIs with push disabled.
2. Enable dual-write for device, channel, credential, template, schedule, status,
   idempotency, event, and dead-letter writes.
3. Backfill by app and by cursor-compatible key ranges.
4. Verify row counts, sampled token hashes, and sampled channel fanout counts.
5. Switch reads per app.
6. Retire old schema after the rollback window.

## Rollback

Switch reads back first, keep dual-write until the rollback is verified, then
delete push tables or leave them idle for forensic retention.
