# DynamoDB bootstrap notes

DynamoDB does not use checked-in SQL migrations.

Reason:

- the app-manager backend is schemaless at the item level
- the durable history backend creates and manages its own DynamoDB tables
- the release-4.3 version-store tables and GSIs are also created at runtime
- presence history inherits the durable history backend and therefore also uses
  the runtime-managed history tables

What to do:

- configure the DynamoDB table or endpoint in Sockudo
- let Sockudo create or evolve the required tables at startup
- do not backfill pre-4.3 immutable history rows into the version-store tables;
  only new 4.3-aware mutable messages should populate them

There is intentionally no `001_fresh_schema.sql` here.
