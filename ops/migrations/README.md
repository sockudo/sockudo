# Migrations

This directory is the canonical schema/bootstrap surface for persistent
backends used by Sockudo.

How to use it:

- Fresh MySQL/MariaDB database: use
  [mysql/001_fresh_schema.sql](/Users/radudiaconu/Desktop/Code/Rust/sockudo/ops/migrations/mysql/001_fresh_schema.sql)
- Fresh PostgreSQL database: use
  [postgresql/001_fresh_schema.sql](/Users/radudiaconu/Desktop/Code/Rust/sockudo/ops/migrations/postgresql/001_fresh_schema.sql)
- Test-only MySQL grants/user setup: use
  [mysql/002_test_access.sql](/Users/radudiaconu/Desktop/Code/Rust/sockudo/ops/migrations/mysql/002_test_access.sql)

Backend notes:

- MySQL and PostgreSQL have checked-in fresh schema files.
- DynamoDB, SurrealDB, and ScyllaDB are provisioned by the runtime/backend and
  do not use checked-in SQL bootstrap files here.

Presence history:

- Presence history does not have separate tables.
- When both durable history and presence history are enabled, retained presence
  transitions are stored through the same durable history backend on internal
  channels like `[presence-history]presence-room`.
