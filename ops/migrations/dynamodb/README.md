# DynamoDB bootstrap notes

DynamoDB does not use checked-in SQL migrations.

Reason:

- the app-manager backend is schemaless at the item level
- the durable history backend creates and manages its own DynamoDB tables
- presence history inherits the durable history backend and therefore also uses
  the runtime-managed history tables

What to do:

- configure the DynamoDB table or endpoint in Sockudo
- let Sockudo create or evolve the required tables at startup

There is intentionally no `001_fresh_schema.sql` here.
