# Migrations

This directory is the canonical schema/bootstrap surface for persistent
backends used by Sockudo.

How to use it:

- Fresh MySQL/MariaDB database: use
  [mysql/001_fresh_schema.sql](/Users/radudiaconu/Desktop/Code/Rust/sockudo/ops/migrations/mysql/001_fresh_schema.sql)
- Fresh PostgreSQL database: use
  [postgresql/001_fresh_schema.sql](/Users/radudiaconu/Desktop/Code/Rust/sockudo/ops/migrations/postgresql/001_fresh_schema.sql)
- Push PostgreSQL schema: use
  [postgres/001_push_schema.sql](/Users/radudiaconu/Desktop/Code/Rust/sockudo/ops/migrations/postgres/001_push_schema.sql)
- Push MySQL schema: use
  [mysql/003_push_schema.sql](/Users/radudiaconu/Desktop/Code/Rust/sockudo/ops/migrations/mysql/003_push_schema.sql)
- Existing push PostgreSQL schema version 1: apply
  [postgres/002_push_status_revision.sql](/Users/radudiaconu/Desktop/Code/Rust/sockudo/ops/migrations/postgres/002_push_status_revision.sql)
- Existing push MySQL/MariaDB schema version 1: apply
  [mysql/004_push_status_revision.sql](/Users/radudiaconu/Desktop/Code/Rust/sockudo/ops/migrations/mysql/004_push_status_revision.sql)
- Test-only MySQL grants/user setup: use
  [mysql/002_test_access.sql](/Users/radudiaconu/Desktop/Code/Rust/sockudo/ops/migrations/mysql/002_test_access.sql)

Backend notes:

- MySQL and PostgreSQL have checked-in fresh schema files.
- DynamoDB, SurrealDB, and ScyllaDB are provisioned by the runtime/backend and
  do not use checked-in SQL bootstrap files here.
- Push storage has checked-in bootstrap contracts for DynamoDB, SurrealDB, and
  ScyllaDB under their backend directories. Runtime provisioning must match
  those contracts or fail closed at startup.
- Release 4.3 mutable-message storage and release 4.4 annotation storage are
  additive and side by side with immutable history. Fresh schemas now include
  version-store and annotation tables for SQL backends, and runtime-provisioned
  backends are expected to create equivalent collections automatically.

Push publish-status CAS upgrade:

- The current push schema version is 2. Fresh push schemas already include the
  `push_publish_status.revision` column and both schema-version records.
- Re-running a fresh-schema script against version 1 does not mark version 2
  unless the revision column already exists; existing installations must still
  use the additive upgrade script.
- For an existing schema version 1 deployment, pause push admission, drain and
  stop every old push worker, apply the backend-specific upgrade, deploy the
  new binary to every node, and only then resume push work.
- Do not run schema-version-1 and schema-version-2 push workers together. Older
  workers use blind status writes and can overwrite a CAS-protected update.
- PostgreSQL applies the column and version record transactionally. MySQL and
  MariaDB commit DDL implicitly, so the version record is deliberately written
  only after the column addition succeeds. If the process stops between those
  statements, verify the column and then apply the version insert manually.
- Rollback requires stopping all schema-version-2 workers before removing the
  version-2 record and revision column. Do not drop the column while a new
  worker can still issue conditional status writes.

Presence history:

- Presence history does not have separate tables.
- When both durable history and presence history are enabled, retained presence
  transitions are stored through the same durable history backend on internal
  channels like `[presence-history]presence-room`.

Backfill boundary:

- Existing immutable history is not backfilled into release-4.3 mutable-message
  chains.
- Only messages created after 4.3-aware feature enablement may populate the
  version-store tables.
- Existing channels are not backfilled into release-4.4 annotation tables.
  Channels with no annotations have empty annotation event logs and no summary
  projection rows until the first annotation event is published.

Annotation retention and rollback:

- Annotation events follow the same retention boundary as the parent message's
  durable history. When an operator or retention worker evicts a parent message,
  its annotation events and derived projections may be evicted as well.
- Annotation event logs are canonical; summary projection rows are derived
  caches and may be rebuilt from the annotation event table.
- Rollback is additive: drop `*_annotation_events` and
  `*_annotation_projections`. Message/history/version tables are unaffected.
