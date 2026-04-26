# DynamoDB bootstrap notes

DynamoDB does not use checked-in SQL migrations.

Reason:

- the app-manager backend is schemaless at the item level
- the durable history backend creates and manages its own DynamoDB tables
- the release-4.3 version-store tables and GSIs are also created at runtime
- release-4.4 annotation storage follows the same runtime-managed table model:
  the annotation store must create/evolve its event/projection tables and GSIs
  before annotations are enabled
- presence history inherits the durable history backend and therefore also uses
  the runtime-managed history tables

What to do:

- configure the DynamoDB table or endpoint in Sockudo
- let Sockudo create or evolve the required tables at startup
- do not backfill pre-4.3 immutable history rows into the version-store tables;
  only new 4.3-aware mutable messages should populate them
- do not backfill existing channels into annotation tables; channels with no
  annotations have empty event logs and no projection rows
- annotation events follow the parent message history retention boundary; when
  parent messages are evicted, annotation events/projections may be evicted too

There is intentionally no `001_fresh_schema.sql` here.
