# Sockudo Modularization Plan (Cycle-Free)

This plan defines a crate graph and migration sequence to split Sockudo's monolith without introducing dependency cycles.

## Immediate Wins (already applied)

1. `src/main.rs` now imports from the library crate (`sockudo::...`) instead of redeclaring all modules.
2. `src/lib.rs` exports `middleware` so `main` can stay thin.
3. `src/cache/mod.rs` re-exports `CacheManagerFactory` for public composition.
4. `src/options.rs` no longer imports adapter modules for default prefixes (removed one direct config -> adapter edge).
5. Introduced `crates/sockudo-types` and moved:
   1. `SocketId` (`crates/sockudo-types/src/socket.rs`)
   2. webhook DTOs (`crates/sockudo-types/src/webhook.rs`)
   3. API auth query DTO (`EventQuery`, `crates/sockudo-types/src/api.rs`)
6. Introduced `crates/sockudo-core` with a minimal `ChannelStore` contract (`crates/sockudo-core/src/channel_store.rs`) and rewired `ChannelManager` to depend on it instead of `adapter::ConnectionManager`.
7. Moved `PresenceMemberInfo` into `sockudo-types` (`crates/sockudo-types/src/presence.rs`) to shrink cross-module coupling.
8. Broke `queue <-> webhook` coupling by moving queue processor callback contracts into `queue` (`src/queue/mod.rs`) and removing `queue` dependency on `webhook::sender`.
9. Added `ConnectionLifecycleStore` in `sockudo-core` (`crates/sockudo-core/src/connection_lifecycle.rs`) and rewired `cleanup` and `presence` to depend on core contracts instead of `adapter::ConnectionManager`.
10. Broke `options -> app` direct dependency by introducing `ConfiguredApp` in `options` and explicit conversion to runtime `App` (`src/app/config.rs`).
11. Broke `utils -> app` dependency by changing channel validation to accept channel limit directly instead of full `App`.
12. Broke `websocket -> app` dependency by introducing lightweight `ConnectionAppInfo` in `websocket` and storing only connection-level app identity.
13. Moved websocket buffer config types into `sockudo-types` (`crates/sockudo-types/src/websocket.rs`) and consumed them from `options`.
14. Introduced `crates/sockudo-protocol` and moved protocol constants/messages there, then updated call sites to import directly from `sockudo_protocol`.
15. Introduced `crates/sockudo-filter` and moved filter parser/matcher/index there, then updated call sites to import directly from `sockudo_filter`.
16. Introduced `crates/sockudo-config` and moved driver enums (`AdapterDriver`, `AppManagerDriver`, `CacheDriver`, `QueueDriver`, `MetricsDriver`, `LogOutputFormat`) there.
17. Removed compatibility re-exports for driver enums from `src/options.rs`; runtime and tests now import driver enums directly from `sockudo_config::drivers`.
18. Converted the repository to a workspace and created all target member crates from the plan: `sockudo-runtime`, `sockudo-adapter-*`, `sockudo-store-*`, `sockudo-queue-*`, `sockudo-webhook-*`, and `sockudo-server`.
19. Moved adapter configuration types into `sockudo-config` (`AdapterConfig`, `RedisAdapterConfig`, `RedisClusterAdapterConfig`, `NatsAdapterConfig`, `ClusterHealthConfig`) and rewired runtime/tests to import from `sockudo_config::adapter` directly.
20. Added initial module skeletons inside all newly created crates so each target package now has an internal migration surface (runtime modules + backend modules per implementation crate).
21. Replaced queue backend scaffolds with real implementations in `sockudo-queue-memory`, `sockudo-queue-redis`, `sockudo-queue-redis-cluster`, and `sockudo-queue-sqs`.
22. Rewired in-crate queue managers (`src/queue/*_queue_manager.rs`) to delegate to the new queue crates via adapter wrappers.
23. Added concrete webhook sender implementations in `sockudo-webhook-http` and `sockudo-webhook-lambda`, and rewired monolith webhook adapters to use them.
24. Moved queue/webhook config types into `sockudo-config` (`queue::{QueueConfig, RedisQueueConfig, RedisClusterQueueConfig, SqsQueueConfig}` and `webhook::{WebhooksConfig, BatchingConfig}`).
25. Replaced remaining `sockudo-runtime` scaffolds with concrete runtime exports that re-expose production app/channel/cleanup/http/metrics/middleware/presence/websocket modules.
26. Replaced adapter crate scaffolds (`sockudo-adapter-local`, `sockudo-adapter-redis`, `sockudo-adapter-redis-cluster`, `sockudo-adapter-nats`) with concrete backend constructors and trait-object conversion APIs.
27. Replaced store crate scaffolds (`sockudo-store-mysql`, `sockudo-store-postgres`, `sockudo-store-dynamodb`, `sockudo-store-scylla`) with concrete backend constructors and app-manager conversion APIs.
28. Replaced `sockudo-server` scaffold print entrypoint with a real composition root that loads config, assembles runtime components, performs health checks, and does graceful shutdown wiring.
29. Moved delta compression shared types (`DeltaAlgorithm`, `ChannelDeltaConfig`, `ChannelDeltaSimple`, `ChannelDeltaSettings`) into `sockudo-types` (`crates/sockudo-types/src/delta.rs`) and rewired `src/delta_compression.rs` to consume/re-export them.
30. Moved the canonical app domain model into `sockudo-types` (`crates/sockudo-types/src/app.rs`), kept `src/app/config.rs` as a thin conversion layer from `ConfiguredApp`, and rewired runtime modules to use `sockudo_types::app::App` directly.
31. Added a core app repository contract (`sockudo_core::app_store::AppStore`) and adapted monolith `AppManager` trait objects to that contract.
32. Replaced `sockudo-store-mysql` wrapper implementation with a real SQLx-backed MySQL app-store backend inside the crate (`crates/sockudo-store-mysql/src/backend.rs`).
33. Replaced `sockudo-store-postgres` wrapper implementation with a real SQLx-backed PostgreSQL app-store backend inside the crate (`crates/sockudo-store-postgres/src/backend.rs`).
34. Replaced `sockudo-store-dynamodb` wrapper implementation with a real AWS SDK DynamoDB app-store backend inside the crate (`crates/sockudo-store-dynamodb/src/backend.rs`).
35. Replaced `sockudo-store-scylla` wrapper implementation with a real ScyllaDB app-store backend inside the crate (`crates/sockudo-store-scylla/src/backend.rs`).
36. Rewired `sockudo-server` app-manager composition to instantiate modular store crates directly (`sockudo-store-mysql`, `sockudo-store-postgres`, `sockudo-store-dynamodb`, `sockudo-store-scylla`) instead of calling monolith `AppManagerFactory`.
37. Rewired `sockudo-server` connection-manager composition to instantiate modular adapter crates directly (`sockudo-adapter-local`, `sockudo-adapter-redis`, `sockudo-adapter-redis-cluster`, `sockudo-adapter-nats`) instead of calling monolith `AdapterFactory`.
38. Replaced `sockudo-adapter-local` wrapper implementation with the full local adapter backend code in-crate (`crates/sockudo-adapter-local/src/local_adapter.rs`), keeping constructor/trait-object APIs stable.
39. Replaced `sockudo-adapter-redis` wrapper implementation with in-crate horizontal adapter logic (`horizontal_adapter`, `horizontal_adapter_base`, `horizontal_transport`) plus concrete Redis transport (`crates/sockudo-adapter-redis/src/adapter/transports/redis_transport.rs`).
40. Replaced `sockudo-adapter-redis-cluster` wrapper implementation with in-crate horizontal adapter logic plus concrete Redis Cluster transport (`crates/sockudo-adapter-redis-cluster/src/adapter/transports/redis_cluster_transport.rs`).
41. Replaced `sockudo-adapter-nats` wrapper implementation with in-crate horizontal adapter logic plus concrete NATS transport (`crates/sockudo-adapter-nats/src/adapter/transports/nats_transport.rs`).
42. Removed direct `sockudo` dependency edges from `sockudo-store-*` and switched their app-manager/error/options contracts to `sockudo-runtime` (`sockudo_runtime::app::manager::AppManager`, `sockudo_runtime::error`, `sockudo_runtime::options`).
43. Removed direct `sockudo` dependency edges from `sockudo-adapter-*` and switched shared module imports/contracts to `sockudo-runtime` (`sockudo_runtime::{app, delta_compression, error, metrics, namespace, websocket}` and `sockudo_runtime::adapter::connection_manager`).
44. Replaced `sockudo-runtime` re-export facade with real runtime module ownership by wiring it to implementation modules (`crates/sockudo-runtime/src/lib.rs` now owns `adapter/app/cache/channel/cleanup/delta_compression/error/http_handler/metrics/middleware/namespace/options/presence/queue/rate_limiter/token/utils/watchlist/webhook/websocket/ws_handler`).
45. Added workspace feature-unification guardrail via `cargo hakari`: initialized and generated `crates/sockudo-workspace-hack`, added `.config/hakari.toml`, and wired workspace members to `sockudo-workspace-hack`.
46. Added dependency-policy guardrail via `cargo deny`: initialized `deny.toml`, configured transitive advisory ignores currently without upstream fixes, and validated `cargo deny check advisories bans sources`.
47. Added CI guardrail/build-matrix jobs in `.github/workflows/ci.yml` for:
   1. `guardrails` (`cargo hakari verify`, `cargo deny check advisories bans sources`)
   2. `modular-builds` matrix (`core-only`, each backend crate independently, `full-workspace` with `--all-features`)

Result: after a clean build, `cargo check --all-features --lib` followed by `cargo check --all-features --bin sockudo` dropped the second step from ~28s to ~2.8s.

## Target Workspace Layout

Move from one package to a workspace with explicit dependency direction:

1. `crates/sockudo-types`
2. `crates/sockudo-protocol`
3. `crates/sockudo-filter`
4. `crates/sockudo-config`
5. `crates/sockudo-core`
6. `crates/sockudo-runtime`
7. `crates/sockudo-adapter-local`
8. `crates/sockudo-adapter-redis`
9. `crates/sockudo-adapter-redis-cluster`
10. `crates/sockudo-adapter-nats`
11. `crates/sockudo-store-*` (mysql, postgres, dynamodb, scylla)
12. `crates/sockudo-queue-*` (memory, redis, redis-cluster, sqs)
13. `crates/sockudo-webhook-*` (http, lambda)
14. `crates/sockudo-server` (binary only)

## Allowed Dependency DAG

Use this direction only:

1. `sockudo-types` -> none
2. `sockudo-protocol` -> `sockudo-types`
3. `sockudo-filter` -> `sockudo-types`
4. `sockudo-config` -> `sockudo-types`
5. `sockudo-core` -> `sockudo-types`, `sockudo-protocol`
6. `sockudo-runtime` -> `sockudo-core`, `sockudo-config`, `sockudo-types`, `sockudo-protocol`, `sockudo-filter`
7. `sockudo-adapter-*` -> `sockudo-core`, `sockudo-types`, `sockudo-protocol`, `sockudo-filter`
8. `sockudo-store-*` / `sockudo-queue-*` / `sockudo-webhook-*` -> `sockudo-core`, `sockudo-types`
9. `sockudo-server` -> `sockudo-runtime` + chosen implementation crates

No reverse edges into `sockudo-core`.

## Cycle Cuts to Prioritize

Current high-value cycle cuts:

1. `app <-> webhook`: move shared webhook models from `src/webhook/types.rs` into `sockudo-types`.
2. `app -> http_handler`: move request DTOs (`EventQuery`) from `src/http_handler.rs` into a neutral API/types module.
3. `channel <-> adapter`: move `ConnectionManager` trait and related interfaces to `sockudo-core`.
4. `websocket` coupling: extract `SocketId` and lightweight socket metadata into `sockudo-types`.

## Migration Sequence

1. Extract pure types first (`SocketId`, webhook DTOs, protocol messages/constants, shared errors).
2. Extract core traits (AppRepository, ConnectionRegistry, Queue, WebhookDispatcher, RateLimiter).
3. Move adapter/store implementations into per-backend crates.
4. Keep all factory wiring in composition root only (`sockudo-server`), not in core.
5. Gate implementation crates with features at workspace member level.
6. Remove old in-crate modules after each extraction step passes tests.

## Compile-Time Strategy

1. Keep backends in separate crates so changing core/runtime does not recompile AWS/DB/Redis stacks.
2. Keep proc-macro-heavy dependencies away from hot edit crates.
3. Use feature flags only in implementation crates; avoid `cfg` branching in core logic.
4. Make `sockudo-server` a thin assembly layer.

## Guardrails

1. Add `cargo hakari` (workspace feature unification) and `cargo deny` checks.
2. Add CI jobs that compile:
   1. `core-only` path
   2. each backend crate independently
   3. full production feature set
3. Enforce layer rules with `cargo modules` or custom import checks (no adapter imports from core/config/types).

48. Fixed cyclic dependency: removed unused `sockudo-runtime` dependency from `sockudo-adapter-local` Cargo.toml (was declared but never imported).
49. Moved `error` module into `sockudo-core` (`crates/sockudo-core/src/error.rs`), removed `#[path]` shims from `sockudo-runtime` and `sockudo-adapter-local`, updated monolith `src/error/mod.rs` to re-export from core.
50. Moved `websocket` module into `sockudo-core` (`crates/sockudo-core/src/websocket.rs`), removed `#[path]` shims from `sockudo-runtime` and `sockudo-adapter-local`.
51. Moved `app::manager` trait into `sockudo-core` (`crates/sockudo-core/src/app_manager.rs`), removed `#[path]` shim from `sockudo-adapter-local/src/app.rs`.
52. Moved `delta_compression` module into `sockudo-core` (`crates/sockudo-core/src/delta_compression.rs`), added feature-gated `redis`/`nats` features to `sockudo-core` for cluster coordinator types, wired features through `sockudo-runtime` and root `Cargo.toml`.
53. Moved `namespace` module into `sockudo-core` (`crates/sockudo-core/src/namespace.rs`), removed `#[path]` shims from `sockudo-runtime` and `sockudo-adapter-local`.
54. Moved `connection_manager` into `sockudo-core` (`crates/sockudo-core/src/connection_manager.rs`), created `adapter_types` module in core for `DeadNodeEvent`/`OrphanedMember`, updated monolith `horizontal_adapter.rs` to re-export from core.
55. Physically moved all 16 remaining runtime modules (`app`, `cache`, `channel`, `cleanup`, `http_handler`, `metrics`, `middleware`, `options`, `presence`, `queue`, `rate_limiter`, `token`, `utils`, `watchlist`, `webhook`, `ws_handler`) plus `adapter/handler` into `sockudo-runtime/src/`, eliminating all `#[path]` shims from the workspace.

Result: Zero `#[path]` directives remain in any crate. All code now lives in its target crate. The monolith `src/` contains only re-exports for backward compatibility during transition. 98 unit tests pass; 23 failures are pre-existing (require external PG/Scylla databases).

56. Converted monolith `src/lib.rs` to a pure re-export layer from `sockudo-runtime`. All `pub mod` declarations replaced with `pub use sockudo_runtime::*` equivalents.
57. Moved remaining adapter files (factory, horizontal_adapter, horizontal_adapter_base, horizontal_transport, local_adapter, transports, concrete adapters) into `sockudo-runtime/src/adapter/`, completing the full adapter migration.
58. Changed `pub(crate)` fields in `JoinResponse` and `PresenceMember` to `pub` to allow cross-crate access via re-exports.
59. Deleted all duplicate source files from `src/`, leaving only `src/lib.rs` (22-line re-export layer) and `src/main.rs` (binary entry point).

Result: `src/` is now 2 files. All source code lives in `crates/`. 242 unit tests pass; 53 failures are all infrastructure-dependent (PG, Scylla, NATS, Redis — require external services). The `sockudo` binary builds and works via re-exports from `sockudo-runtime`.
