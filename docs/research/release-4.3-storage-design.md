# Release 4.3 Storage Schema, Migration, And Backfill

## Status

Binding storage design for release 4.3.

Implementation anchor:

- `crates/sockudo-core/src/version_store.rs`

## Storage Model

Release 4.3 cannot safely overload the current durable history store alone.

Why:

- latest-view history needs one fixed row per logical message
- version history needs every accepted mutation
- replay needs a channel-wide continuity order for create, update, delete, and append

The durable storage model therefore needs three logical collections per backend:

1. `version_streams`
2. `version_messages`
3. `version_entries`

## Logical Collections

### `version_streams`

Channel-scoped replay continuity and retention metadata.

Required fields:

- `app_id`
- `channel`
- `next_delivery_serial`
- `oldest_available_delivery_serial`
- `newest_available_delivery_serial`
- migration or backfill status marker
- updated timestamps

Primary purpose:

- reserve or validate replay continuity for mutable-message delivery
- expose operator-visible replay retention state

### `version_messages`

One row per logical mutable message.

Required fields:

- `app_id`
- `channel`
- `message_serial`
- `history_serial`
- `original_client_id`
- `latest_version_serial`
- `latest_delivery_serial`
- `latest_action`
- `created_at_ms`
- `updated_at_ms`

Primary purpose:

- fixed history position
- fast latest-visible lookup
- ownership checks anchored to the original create identity

### `version_entries`

One row per accepted version.

Required fields:

- `app_id`
- `channel`
- `message_serial`
- `version_serial`
- `delivery_serial`
- `history_serial`
- `action`
- `client_id`
- `description`
- operation metadata
- fully materialized visible payload fields
- timestamps

Primary purpose:

- version-history retrieval
- deterministic latest-visible winner selection
- replay after disconnect by `delivery_serial`

## Durable Indexes

These indexes are required for production backends.

### Postgres / MySQL

`version_streams`

- primary key: `(app_id, channel)`
- index on `(app_id, oldest_available_delivery_serial, newest_available_delivery_serial)` for inspection and retention work

`version_messages`

- primary key: `(app_id, channel, message_serial)`
- unique index on `(app_id, channel, history_serial)`
- index on `(app_id, channel, latest_version_serial)`

`version_entries`

- primary key: `(app_id, channel, message_serial, version_serial)`
- unique index on `(app_id, channel, delivery_serial)`
- index on `(app_id, channel, message_serial, version_serial DESC)` for version-history paging
- index on `(app_id, channel, delivery_serial)` for replay
- index on `(app_id, channel, history_serial, version_serial DESC)` for latest-view joins or rebuilds

### DynamoDB

Tables:

- `<prefix>_version_streams`
- `<prefix>_version_messages`
- `<prefix>_version_entries`

Keys:

- `version_streams`: PK `app_channel`
- `version_messages`: PK `app_channel`, SK `message_serial`
- `version_entries`: PK `app_channel`, SK `message_serial#version_serial`

GSIs:

- replay GSI on `app_channel` + `delivery_serial`
- history-position GSI on `app_channel` + `history_serial`
- version-history GSI on `app_channel#message_serial` + `version_serial`

### SurrealDB

Tables:

- `<prefix>_version_streams`
- `<prefix>_version_messages`
- `<prefix>_version_entries`

Indexes:

- `version_streams` on `app_id`
- `version_messages` on `app_id, channel, message_serial`
- `version_messages` on `app_id, channel, history_serial`
- `version_entries` on `app_id, channel, message_serial, version_serial`
- `version_entries` on `app_id, channel, delivery_serial`

### ScyllaDB

Tables:

- `<prefix>_version_streams`
- `<prefix>_version_messages`
- `<prefix>_version_entries_by_message`
- `<prefix>_version_entries_by_delivery`

Reason:

- Scylla needs query-shaped tables instead of secondary indexes for all critical access paths.

## Migration Strategy

Release 4.3 migrations must be additive and side-by-side.

Rules:

- do not rewrite existing durable history tables in place
- create new versioned-message tables or collections alongside existing history storage
- keep old immutable history behavior intact for pre-4.3 data
- gate new writes so versioned-message rows are only created for 4.3-aware messages

This makes the migration reversible:

- disabling the feature stops new version-store writes
- existing history remains readable because the immutable history store is unchanged
- no destructive transform is required to roll back

## Backfill Assessment

Current durable history cannot be safely backfilled into 4.3 mutable-message storage.

Reason:

- pre-4.3 history rows do not have stable `message_serial`
- they do not have `version_serial`
- they do not have original create identity needed for own-scope mutations
- they do not preserve replay continuity for later mutations because those mutations did not exist
- synthesizing identifiers after the fact would create fake authority and break future ownership and recovery semantics

## Safest Migration Boundary

The safest boundary is:

- no historical backfill of existing immutable channel history into mutable-message version chains
- only messages created after release-4.3 feature enablement may participate in versioned durable message flows
- pre-existing history remains immutable and readable through current history APIs

This is the only safe boundary that does not fabricate mutable-message identity for old rows.

## Rollback And Reversibility

Rollback is operationally clean if migrations remain additive.

Required rollback posture:

- disable 4.3 versioned-message writes
- continue serving ordinary immutable history
- leave versioned-message tables in place for forensic inspection or later forward re-enable

What rollback must not do:

- delete or mutate existing immutable history rows
- rewrite old history payloads to remove synthetic metadata

## Implemented Scope In This Prompt

This prompt implements the storage abstraction and testable in-memory behavior in `sockudo-core`:

- append version record
- latest-visible lookup
- version-history paging
- replay lookup by delivery serial
- latest-by-history projection

This prompt also lands additive schema/bootstrap surfaces for persistent backends:

- SQL fresh-schema files include the 4.3 version-store tables and indexes
- runtime-provisioned backend bootstrap paths create the 4.3 version-store tables or indexes side by side with immutable history

Durable read/write version-store backends are still deferred to later release-4.3 runtime work. The migration and schema contract above is now the binding surface those later implementations must follow.
