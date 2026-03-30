# ScyllaDB App Policy Migration

Sockudo now uses a canonical `policy` document for app-level behavior.

## Why this is different on ScyllaDB

PostgreSQL and MySQL can backfill `policy` with pure SQL/JSON construction. ScyllaDB/Cassandra CQL does not provide an equivalent ergonomic JSON document backfill for this legacy schema, so the migration should be treated as an application-assisted data migration.

## Recommended production procedure

1. Upgrade Sockudo to a version that reads `policy` first and still supports legacy column fallback.
2. Let the application continue running so app reads/writes remain compatible.
3. Export legacy rows from the app table.
4. Reconstruct the canonical `policy` JSON for each row using the same mapping Sockudo uses in code:
   - `limits`: legacy numeric limit columns
   - `features`: `enable_client_messages`, `enable_user_authentication`, `enable_watchlist_events`
   - `channels`: `allowed_origins`, `channel_delta_compression`
   - `webhooks`
   - `idempotency`
   - `connection_recovery`
5. Write the resulting JSON into the `policy` column for every row.
6. Verify all rows have non-null `policy`.
7. During a maintenance window, alter the table to drop legacy columns if desired.

## Important limitation

Legacy ScyllaDB app storage did not persist `channel_namespaces`, so there is nothing to backfill for that field from the database alone.

## Example cleanup CQL

Run only after `policy` has been populated and verified:

```sql
ALTER TABLE sockudo.applications DROP max_connections;
ALTER TABLE sockudo.applications DROP enable_client_messages;
ALTER TABLE sockudo.applications DROP max_backend_events_per_second;
ALTER TABLE sockudo.applications DROP max_client_events_per_second;
ALTER TABLE sockudo.applications DROP max_read_requests_per_second;
ALTER TABLE sockudo.applications DROP max_presence_members_per_channel;
ALTER TABLE sockudo.applications DROP max_presence_member_size_in_kb;
ALTER TABLE sockudo.applications DROP max_channel_name_length;
ALTER TABLE sockudo.applications DROP max_event_channels_at_once;
ALTER TABLE sockudo.applications DROP max_event_name_length;
ALTER TABLE sockudo.applications DROP max_event_payload_in_kb;
ALTER TABLE sockudo.applications DROP max_event_batch_size;
ALTER TABLE sockudo.applications DROP enable_user_authentication;
ALTER TABLE sockudo.applications DROP enable_watchlist_events;
ALTER TABLE sockudo.applications DROP webhooks;
ALTER TABLE sockudo.applications DROP allowed_origins;
ALTER TABLE sockudo.applications DROP channel_delta_compression;
ALTER TABLE sockudo.applications DROP idempotency;
ALTER TABLE sockudo.applications DROP connection_recovery;
```
