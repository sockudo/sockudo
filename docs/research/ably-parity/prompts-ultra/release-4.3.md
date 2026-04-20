# Release 4.3 Ultra Prompt Pack

## Prompt 4.3-01: Architecture and release contract
```text
Implement Sockudo release 4.3: Versioned Durable Messages.
Create the binding release contract first. Define goals, non-goals, supported message actions, V1/V2 boundaries, acceptance criteria, and release blockers.
Use:
- /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/ably-message-annotations-research.md
- /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/sockudo-ably-parity-release-plan.md
Do not start coding until the contract exists in-repo and explains how latest-visible-version history works.
```

## Prompt 4.3-02: Domain model and invariants
```text
Design and implement the core domain model for release 4.3.
Define message identity, version identity, latest-visible-version rules, append semantics, winner selection, and replay invariants.
Write down the invariants explicitly, then implement them.
```

## Prompt 4.3-03: Storage schema, migrations, and backfill
```text
Design and implement the storage changes for release 4.3.
Cover:
- schema changes
- durable indexes
- migration strategy
- backfill strategy for existing history
- rollback and reversibility notes
If current data cannot be safely backfilled, say so precisely and propose the safest migration boundary.
```

## Prompt 4.3-04: Protocol, API, and wire contract
```text
Implement the Sockudo-native protocol and API contract for mutable messages.
Ship request/response shapes, realtime action semantics, validation, error behavior, and version retrieval surfaces.
Keep this V2-native unless a narrower compatibility path is justified.
```

## Prompt 4.3-05: Auth, capabilities, and security
```text
Implement own-versus-any mutation capabilities and identity-bound authorization.
Cover update, delete, and append.
Prove unauthorized paths fail and authenticated ownership paths succeed.
```

## Prompt 4.3-06: Server and runtime implementation
```text
Implement the runtime for update, delete, append, version assignment, history substitution, and version retrieval.
Do not stop at handlers. Complete the end-to-end behavior.
```

## Prompt 4.3-07: Cluster and distributed correctness
```text
Harden release 4.3 for clustered operation.
Implement and verify deterministic winning-version selection, cross-node convergence, and recovery compatibility under distributed races.
```

## Prompt 4.3-08: Client and SDK behavior
```text
Define and implement the client-visible behavior needed for release 4.3.
Document replace-versus-concatenate logic, action interpretation, and version-history consumption.
Add any minimal SDK or example support needed to make the release usable.
```

## Prompt 4.3-09: Framework integration and release engineering
```text
Prepare release 4.3 for downstream adoption.
Scope:
- example updates
- release-note input
- compatibility notes
- packaging or SDK follow-up markers
- release engineering checklist for this feature set
This is about making the release shippable, not only technically implemented.
```

## Prompt 4.3-10: Test matrix and automation
```text
Build and run the full test matrix for release 4.3.
Include unit, integration, cluster, auth, and replay/recovery cases.
Fix discovered defects before moving on.
```

## Prompt 4.3-11: Observability, docs, and operator guidance
```text
Add metrics, logs, docs, and operator guidance for release 4.3.
Make history/version anomalies observable.
Update protocol and HTTP docs comprehensively.
```

## Prompt 4.3-13: Persistent VersionStore backends and config
```text
The versioned messages runtime is complete but VersionStore is currently memory-only
(MemoryVersionStore hardwired in main.rs). All history backends already have the version
tables in their schema (version_streams, version_messages, version_entries) but nothing
reads from or writes to them. This prompt implements the full persistence layer.

--- 1. Config: add driver field to VersionedMessagesConfig ---

In `crates/sockudo-core/src/options.rs`:

Add a new driver enum following the exact HistoryBackend pattern:

  #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
  #[serde(rename_all = "lowercase")]
  pub enum VersionStoreDriver {
      #[default]
      Memory,
      Postgres,
      Mysql,
      DynamoDb,
      ScyllaDb,
      SurrealDb,
  }

  impl FromStr for VersionStoreDriver — accept aliases:
    "postgres" | "postgresql" | "pgsql" → Postgres
    "mysql"                             → Mysql
    "dynamodb"                          → DynamoDb
    "scylladb" | "scylla"               → ScyllaDb
    "surrealdb" | "surreal"             → SurrealDb
    "memory"                            → Memory

Extend VersionedMessagesConfig:

  pub struct VersionedMessagesConfig {
      pub enabled: bool,
      pub driver: VersionStoreDriver,   // NEW — default: memory
      pub max_page_size: usize,
  }

Do NOT add per-backend sub-config structs (postgres/mysql/etc.) to VersionedMessagesConfig.
The version store shares the existing top-level `[database]` config (same connection pools,
same credentials) that every other persistent backend already uses.
This is correct — it mirrors how HistoryConfig works: history backends read from
`DatabaseConfig` and `DatabasePooling`, not from their own connection settings.

Update config.toml and .env.example:

  [versioned_messages]
  enabled = true
  driver = "memory"     # memory | postgres | mysql | dynamodb | scylladb | surrealdb
  max_page_size = 100

Add the corresponding env var: VERSIONED_MESSAGES_DRIVER

--- 2. VersionStore trait (already exists in crates/sockudo-core/src/version_store.rs) ---

Verify the trait has all required methods before implementing backends:
  - reserve_delivery_position(channel_id) → Result<u64>
  - append_version(record: StoredVersionRecord) → Result<()>
  - get_latest(channel_id, message_serial) → Result<Option<StoredVersionRecord>>
  - get_versions(channel_id, message_serial, page) → Result<PaginatedVersions>
  - replay_after(channel_id, after_serial, limit) → Result<Vec<StoredVersionRecord>>
  - latest_by_history(channel_id, limit) → Result<Vec<StoredVersionRecord>>
  - stream_state(channel_id) → Result<VersionStreamState>

If any method signature needs adjusting to support persistent backends (e.g. async
connection handles), fix the trait now before implementing the backends.

--- 3. PostgreSQL VersionStore ---

Location: `crates/sockudo-server/src/history.rs` (add alongside existing Postgres history code)
or a new file `crates/sockudo-server/src/version_store_postgres.rs`.

Use the existing tables already created in the Postgres migration:
  - {prefix}_version_streams  — per-channel metadata (stream_id, next_delivery_serial)
  - {prefix}_version_messages — per-message identity (channel_id, message_serial, created_at)
  - {prefix}_version_entries  — per-version record (delivery_serial, message_serial,
                                 version_serial, action, data, client_id, timestamp, ...)

Use the same sqlx PgPool that the Postgres history store already uses.
Do not open a new connection pool — accept Arc<PgPool> in the constructor.

reserve_delivery_position:
  Use a database sequence or an atomic UPDATE ... RETURNING on version_streams.
  Must be safe under concurrent writers from multiple nodes.

append_version:
  INSERT INTO version_entries. If a version with the same version_serial already exists,
  treat as idempotent (ON CONFLICT DO NOTHING).

get_latest:
  SELECT the entry with the highest version_serial for this (channel_id, message_serial).
  Apply the shallow-mixin rules in Rust after retrieval (same as MemoryVersionStore).

get_versions:
  SELECT all entries for (channel_id, message_serial) ordered by version_serial, paginated.

replay_after:
  SELECT entries WHERE delivery_serial > after_serial ORDER BY delivery_serial LIMIT limit.

--- 4. MySQL VersionStore ---

Location: `crates/sockudo-server/src/history_mysql.rs` (add alongside existing MySQL history code).

Use the existing sqlx MySqlPool. Same method implementations as Postgres with MySQL syntax.
reserve_delivery_position: use `SELECT ... FOR UPDATE` on version_streams row + UPDATE.

--- 5. ScyllaDB VersionStore ---

Location: `crates/sockudo-server/src/history_scylla.rs`.

Use the existing scylla session. Key differences from SQL backends:
- reserve_delivery_position: use a lightweight transaction (IF) or a counter table.
- append_version: INSERT with IF NOT EXISTS for idempotency.
- get_latest: requires a read of all versions for (channel_id, message_serial) and
  picking the max version_serial in Rust — Scylla does not support ORDER BY on non-clustering keys
  unless the schema is designed for it. Check the existing version_entries table schema;
  if version_serial is not a clustering key, add it or adjust the query.

--- 6. DynamoDB VersionStore ---

Location: `crates/sockudo-server/src/history_dynamodb.rs`.

Use the existing AWS DynamoDB client. Key differences:
- reserve_delivery_position: use a DynamoDB atomic counter (UpdateItem with ADD on a counter attribute).
- append_version: PutItem with a condition expression to prevent overwrite on duplicate version_serial.
- get_latest: Query on (channel_id, message_serial) GSI, sort by version_serial descending, Limit 1.

--- 7. SurrealDB VersionStore ---

Location: `crates/sockudo-server/src/history_surreal.rs`.

Use the existing SurrealDB client.
- reserve_delivery_position: use SurrealDB's `UPDATE ... SET counter += 1 RETURN AFTER`.
- append_version: CREATE with a content block; use IF NOT EXISTS equivalent.
- get_latest: `SELECT * FROM version_entries WHERE channel_id = $c AND message_serial = $m
  ORDER BY version_serial DESC LIMIT 1`.

--- 8. Factory function ---

In `crates/sockudo-server/src/main.rs` (or extract to a helper module):

Replace the current hardwired:
  builder = builder.version_store(Arc::new(MemoryVersionStore::new()));

With a factory that matches on config.versioned_messages.driver:

  let version_store: Arc<dyn VersionStore + Send + Sync> = match config.versioned_messages.driver {
      VersionStoreDriver::Memory => Arc::new(MemoryVersionStore::new()),
      VersionStoreDriver::Postgres => {
          #[cfg(feature = "postgres")]
          { Arc::new(PostgresVersionStore::new(pg_pool.clone(), table_prefix).await?) }
          #[cfg(not(feature = "postgres"))]
          { return Err("postgres feature not enabled".into()); }
      }
      VersionStoreDriver::Mysql => { /* #[cfg(feature = "mysql")] ... */ }
      VersionStoreDriver::DynamoDb => { /* #[cfg(feature = "dynamodb")] ... */ }
      VersionStoreDriver::ScyllaDb => { /* #[cfg(feature = "scylladb")] ... */ }
      VersionStoreDriver::SurrealDb => { /* #[cfg(feature = "surrealdb")] ... */ }
  };
  builder = builder.version_store(version_store);

Log the selected driver: info!("VersionStore initialized with driver: {:?}", config.versioned_messages.driver);

--- 9. Cluster correctness ---

The persistent backends fix the single-node restart problem but introduce a new one:
multiple nodes writing versions concurrently.

reserve_delivery_position MUST be atomic at the database level — not just at the Rust level.
Verify each backend's implementation is safe under concurrent INSERT from N nodes.

winner-selection rule (from 4.3 domain model): the version with the lexicographically
highest version_serial wins as the latest visible version. This rule must hold correctly
when two nodes race to insert versions for the same message_serial. The database does not
need to enforce this — the Rust get_latest() implementation selects the max version_serial
after retrieval. Verify this is correct for each backend.

--- 10. Tests ---

Add tests to `crates/sockudo-adapter/tests/` or `crates/sockudo-server/tests/`:

- Memory backend: existing tests should still pass unchanged.
- For each persistent backend (use test containers or the existing CI Docker setup):
  - append_version and get_latest round-trip
  - reserve_delivery_position is monotonically increasing under sequential calls
  - concurrent reserve_delivery_position from two tasks produces no duplicates
  - get_versions returns all versions in correct order
  - replay_after returns only entries after the given serial
  - duplicate append_version (same version_serial) is idempotent

--- 11. Docs and ops ---

Update `ops/migrations/` for each backend if any schema changes are needed
(the tables already exist but verify column types match what the new code expects).

Update `docs/content/2.server/15.connection-recovery.md` or a new
`docs/content/2.server/16.versioned-messages.md` to document:
  - versioned_messages.driver options
  - which database backends support persistent versions
  - the shared [database] config dependency (no separate connection pool)
  - production recommendation: use the same backend as your history store
```

---

## Prompt 4.3-12: Final verification and release audit
```text
Run the final release audit for 4.3.

This audit must confirm that Prompt 4.3-13 (persistent VersionStore backends) is complete
before issuing a go decision. A memory-only version store is not acceptable for a release
that claims "durable" messages.

Verification checklist:
- VersionedMessagesConfig has a driver field with all five backends supported.
- config.toml and .env.example document the driver option.
- At least one persistent backend (Postgres or MySQL) is fully implemented and tested.
- All remaining backends are either implemented or have a tracked issue with a clear scope.
- reserve_delivery_position is atomic under concurrent writes for implemented backends.
- winner-selection (max version_serial) is correct in get_latest for all backends.
- Factory in main.rs wires the correct backend based on config.
- Memory backend still passes all existing tests.
- Cluster correctness: two nodes inserting versions for the same message_serial converge
  to the correct latest-visible version.

Deliver findings, go/no-go, blockers if no-go, release note draft, and readiness statement for 4.4 and 4.6.
```
