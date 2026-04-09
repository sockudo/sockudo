# ADR 0002: Durable History Backend Scope After Full Backend Expansion

Status: Accepted

## Context

Sockudo durable history now depends on a stronger contract than simple message storage:

- monotonic `serial` reservation
- durable `stream_id` ownership
- ordered pagination by serial and time
- retention floor/ceiling tracking
- explicit `degraded` / `reset_required` state
- operator reset and purge controls
- fail-closed clustered recovery semantics

That contract is defined in:

- [`crates/sockudo-core/src/history.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-core/src/history.rs)
- [`crates/sockudo-core/src/history_conformance.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-core/src/history_conformance.rs)

And implemented in:

- [`crates/sockudo-server/src/history.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-server/src/history.rs) for PostgreSQL
- [`crates/sockudo-server/src/history_mysql.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-server/src/history_mysql.rs) for MySQL
- [`crates/sockudo-server/src/history_scylla.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-server/src/history_scylla.rs) for ScyllaDB
- [`crates/sockudo-server/src/history_surreal.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-server/src/history_surreal.rs) for SurrealDB
- [`crates/sockudo-server/src/history_dynamodb.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-server/src/history_dynamodb.rs) for DynamoDB

The wider repo still contains other database-backed surfaces for app metadata, but all officially exposed database backends now also have durable history implementations.

## Decision

Sockudo durable history production backends are currently:

- PostgreSQL
- MySQL
- ScyllaDB
- SurrealDB
- DynamoDB

## Why MySQL Was Added

MySQL is close enough to the PostgreSQL model to preserve the existing history contract with bounded engineering cost:

- one authoritative `(app_id, channel)` stream metadata row
- transactional append + retention updates
- ordered range reads by serial and time
- explicit stream reset/purge operations
- durable degraded-state storage

This is not zero-effort, but it is still a relational port of an already proven design rather than a fresh distributed-systems design.

## Why ScyllaDB Was Added

ScyllaDB can preserve the durable history contract with bounded extra backend-specific work:

- LWT-backed monotonic `serial` reservation on the authoritative stream row
- explicit durable `degraded` / `reset_required` stream state
- ordered range reads by `serial`
- operator reset/purge behavior aligned with the shared contract
- shared conformance coverage against a live Scylla test node

Important operational constraint:

- the history keyspace must have `tablets = {'enabled': false}` because this backend relies on LWT for serial reservation and current Scylla tablet-enabled tables do not support that path reliably enough for this contract

## Why SurrealDB Was Added

SurrealDB can satisfy the history contract with one authoritative per-channel stream record plus deterministic entry records:

- optimistic compare-and-set `next_serial` reservation on the stream record
- durable `stream_id` ownership and degraded/reset state
- ordered pagination over deterministic per-stream entries
- persisted retained-floor metadata on the stream record for fast inspection and recovery gating
- operator reset/purge controls
- live conformance coverage against a real SurrealDB server

The implementation is less naturally relational than PostgreSQL/MySQL, but it is still explicit enough to preserve the `HistoryStore` contract without falling back to best-effort continuity.

## Why DynamoDB Was Added

DynamoDB can satisfy the history contract with:

- one authoritative stream item per `(app_id, channel)`
- conditional writes for monotonic `next_serial` reservation
- one ordered partition per `(app_id, channel, stream_id)` for retained history
- persisted retained-floor metadata on the stream item for fast inspection and recovery gating
- explicit durable degraded/reset state
- operator reset/purge controls
- live conformance coverage against DynamoDB Local

## Conformance Requirement

Every future durable history backend must pass the shared `HistoryStore` conformance contract:

- serial monotonicity
- `stream_id` continuity
- cursor pagination
- purge semantics
- reset semantics

Reference:

- [`crates/sockudo-core/src/history_conformance.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-core/src/history_conformance.rs)

## Consequences

### Positive

- Sockudo now has five realistic production durable history backends.
- Backend additions are now anchored to one shared contract instead of ad hoc parity claims.

### Negative

- Full “all databases” history parity is still not achieved.
- ScyllaDB operators must provision the history keyspace with tablets disabled or allow Sockudo to create that keyspace with the required setting.
- DynamoDB currently favors correctness and explicit state transitions over aggressive read-path optimization for large streams.

## Future Revisit Gate

Add another durable history backend only when all of the following are true:

1. The backend can preserve the exact `HistoryStore` semantics.
2. A backend-specific implementation exists for:
   - append
   - pagination
   - degraded-state persistence
   - operator reset/purge
3. The backend passes the shared conformance suite plus backend-specific integration tests.
