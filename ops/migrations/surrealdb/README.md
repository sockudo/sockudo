# SurrealDB bootstrap notes

SurrealDB does not use checked-in SQL migrations in this repository.

Reason:

- the app-manager backend creates the applications table and key index at runtime
- the durable history backend creates its tables and indexes at runtime
- the release-4.3 version-store tables and indexes are also created at runtime
- presence history inherits the durable history backend and therefore uses the
  same runtime-managed history storage

- Existing immutable history is not backfilled into release-4.3 mutable-message
  chains. Only 4.3-aware mutable writes should populate the version-store tables.

There is intentionally no `001_fresh_schema.sql` here.
