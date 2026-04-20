# ScyllaDB bootstrap notes

ScyllaDB does not use checked-in SQL migrations in this repository.

Reason:

- the app-manager backend creates the keyspace, table, and indexes at runtime
- the durable history backend creates the history keyspace/tables at runtime
- the release-4.3 version-store tables are also created at runtime
- presence history inherits the durable history backend and therefore uses the
  same runtime-managed history storage

Operational note:

- durable history serial reservation depends on LWT and requires tablets
  disabled for the history keyspace
- Sockudo applies the required keyspace setting when it creates that keyspace
- Existing immutable history is not backfilled into release-4.3 mutable-message
  chains. Only new 4.3-aware mutable writes should populate the version-store tables.

There is intentionally no `001_fresh_schema.sql` here.
