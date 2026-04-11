# SurrealDB bootstrap notes

SurrealDB does not use checked-in SQL migrations in this repository.

Reason:

- the app-manager backend creates the applications table and key index at runtime
- the durable history backend creates its tables and indexes at runtime
- presence history inherits the durable history backend and therefore uses the
  same runtime-managed history storage

There is intentionally no `001_fresh_schema.sql` here.
