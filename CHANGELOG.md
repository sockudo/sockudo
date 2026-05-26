# Changelog

## [4.5.1] - 2026-05-26

### Added

- Redis Cluster transport now supports targeted node request routing, per-transport reply inboxes, and optional sharded Pub/Sub for cluster-aware fanout.
- Metrics recorder access was modernized and the documentation surface was migrated to the new docs structure.
- CI can now be triggered manually with `workflow_dispatch` for recovery when GitHub drops automatic push events.

### Changed

- Long-running push provider workers keep their authentication context available across dispatch loops.
- Push signing now selects an explicit JWT crypto provider before creating provider tokens.
- Redis Cluster listeners use publishable shard-aware subscriptions for reliable cluster Pub/Sub delivery.

### Fixed

- Redis Cluster CI test services now advertise only the ports exposed by `docker-compose.test.yml`, keeping host-side transport tests reachable.
- Redis Cluster sharded Pub/Sub code was kept clippy-clean after the transport merge.

## [4.5.0] - 2026-05-18

### Added

- Optional push notification platform behind the `push` feature, with provider feature gates for FCM, APNs, Web Push, HMS, and WNS.
- Push HTTP/admin surfaces for device activation, registry management, channel subscriptions, async publish admission, status lookup, credentials, templates, schedules, feedback, and cleanup workflows.
- Durable push pipeline primitives including publish-log admission, fanout planning, provider delivery queues, weighted-fair dispatch, circuit breakers, retry/DLQ state, quotas, and stale-device cleanup.
- Push storage migrations for PostgreSQL, MySQL, DynamoDB, ScyllaDB, and SurrealDB, plus Web Push and APNs probe tooling.
- Push metrics, dashboard, benchmark, canary, and verification scripts for release-candidate operations.

### Changed

- Added `/live` and configurable `/up` subsystem timeout behavior for health checks.
- Improved horizontal presence and adapter performance with batched disconnect queries, local fast paths, per-node NATS delivery, and opt-in chunked presence sync.
- Started configured monolith push provider workers during server bootstrap.

### Fixed

- Prevent unsafe demo app and webhook configuration persistence during enabled-only app bootstrap.
- Send WebSocket error messages before closing status transitions.
- Replace per-request NATS inbox subscriptions with a shared wildcard subscription.
- Downgrade expected channel-closed adapter logs from warn/error to debug.

## [4.4.0] - 2026-04-29

### Added

- Protocol V2 message annotations with summary projections, raw annotation streams, HTTP annotation APIs, and operator metrics for reactions, receipts, moderation, and audit workflows.
- Annotation policy controls at app and namespace scope, including retained message-state requirements for annotated channels.
- Apache Iggy adapter and queue support, including durable transport configuration, operational docs, and local multinode verification tooling.

### Fixed

- Hardened Apache Iggy runtime lifecycle handling and broker partition semantics.
- Tightened annotation authorization, projection rebuilds, delivery behavior, and release-matrix test coverage.

## [4.3.1] - 2026-04-26

### Fixed

- Include `subscription_count` in filtered channel list responses when `info=subscription_count` is requested, instead of dropping matching channels with otherwise empty info maps.
- Use native WebSocket ping/pong frames for Protocol V2 idle heartbeats while preserving Protocol V1 `pusher:ping` / `pusher:pong` compatibility.
- Avoid attaching V2 recovery metadata to fallback heartbeat messages.

### Performance

- Use local socket counts for broadcast latency metrics when available, avoiding unnecessary distributed adapter round-trips.

### Tests / Hardening

- Added clustered Redis coverage for delta compression combined with wildcard subscriptions, tag filtering, durable message history, and presence history across multiple Sockudo nodes.
- Tightened clustered fanout waiting, namespace lookups, wildcard matching, presence-history dedupe, and in-memory cleanup paths related to V2 delivery.

## [4.3.0] - 2026-04-20

### Added

- Sockudo-native Protocol V2 mutable messages with stable message identity, preserved version history, and latest-visible history substitution.
- Realtime mutation delivery for `sockudo:message.update`, `sockudo:message.delete`, and `sockudo:message.append`.
- Own-versus-any mutation authorization tied to authenticated V2 identities, gated by new `message_update_own`/`message_update_any`/`message_delete_own`/`message_delete_any`/`message_append_own`/`message_append_any` connection capabilities.
- Client-facing mutable-message consumption guidance across in-repo SDKs, plus JS reducer helpers for replace-versus-concatenate handling.
- Durable history backends for mutable messages: MySQL, PostgreSQL, DynamoDB, ScyllaDB, and SurrealDB with full schema migrations under `ops/migrations/`.

### Fixed

- Include subscribing member in `subscription_succeeded` response for presence channels.
- Sync `active_channels` gauge from DashMap instead of increment/decrement to avoid drift under load.
- Cancel shutdown token on writer death in a way that does not break graceful shutdown or duplicate close frames (PR #220 reverted in #222).

### CI / Build

- Consolidated `cargo audit` ignores into `.cargo/audit.toml` (RUSTSEC-2023-0071, RUSTSEC-2023-0089, RUSTSEC-2025-0134, RUSTSEC-2026-0049).
- Fixed Docker `Test Docker Image` job on push by wiring `prepare` job outputs into its `needs:` list.
- Fixed Docker `Security Scan` job by granting `security-events: write` so SARIF uploads succeed.

### Compatibility Notes

- Release 4.3 mutable messages are V2-only Sockudo-native behavior.
- Protocol V1 remains strictly Pusher-compatible and never receives mutable-message mutation envelopes.
- Existing immutable history is not backfilled into mutable-message chains.

## [4.2.0] - 2026-04-11

### Added

- Durable channel history, rewind, and persistence-backed recovery across the HTTP API, runtime, metrics, and operator docs.
- Presence history and presence snapshot APIs, including retention/continuity metadata and multi-node history operations.
- Queue and app-manager coverage needed by the new history/recovery stack across Redis, SQL, DynamoDB, ScyllaDB, SurrealDB, and broker-backed deployments.
- Official client and server SDK support for presence history and presence snapshots, with proxy-backed client access and typed server-side helpers.

### Changed

- Docker, migration, and monitoring references were moved under `ops/` to match the repository layout used by the new history/recovery documentation.
- Release workflows now trigger from `master` as well as `main`.

## [4.0.0] - 2026-03-30

### Breaking Changes

#### Protocol & Event Naming
- **V2 clients receive `sockudo:` / `sockudo_internal:` event prefixes** instead of `pusher:`. V1 (default) remains fully Pusher-compatible — only connections opting in via `?protocol=2` are affected.
- **V2 message format includes `serial` and `message_id`** on every broadcast. V2 clients must handle these additional fields.
- **Binary wire-format negotiation is V2-only.** Clients may now negotiate JSON, MessagePack, or Protobuf encoding; V1 connections always use plain JSON.

#### Configuration Format
- **TOML is now the primary config format.** The server loads `config/config.toml` first and falls back to `config/config.json`. Existing JSON configs continue to work.
- **New config sections** for v4 features must be present (or set via env) for those features to activate:

```toml
[idempotency]
enabled = true
ttl_seconds = 120
max_key_length = 128

[connection_recovery]
enabled = false
buffer_ttl_seconds = 120
max_buffer_size = 100

[delta_compression]
enabled = false

[tag_filtering]
enabled = false
```

#### Cargo Features
- The `v2` meta-feature is **enabled by default**. Build a pure Pusher V1 server with `--no-default-features`.
- New flags: `delta`, `tag-filtering`, `recovery` (included in `v2` and `full`).

---

### New Features

#### Dual Protocol Model
Per-connection protocol negotiation via `?protocol=` query parameter:

| | V1 (default) | V2 |
|---|---|---|
| Event prefix | `pusher:` / `pusher_internal:` | `sockudo:` / `sockudo_internal:` |
| `serial` field | No | Yes |
| `message_id` field | No | Yes |
| Connection recovery | No | Yes |
| Delta compression | No | Yes |
| Tag filtering | No | Yes |
| Idempotent publish | No | Yes |
| Wire-format negotiation | No | Yes (JSON / MessagePack / Protobuf) |
| Compatible SDKs | Official Pusher SDKs | Sockudo client SDKs |

#### Connection Recovery (V2)
Serial-based replay buffer for exactly-once delivery on reconnect. Clients send `sockudo:resume` with their last known serial and the server replays missed messages.

- Config: `[connection_recovery]` — `enabled`, `buffer_ttl_seconds`, `max_buffer_size`
- Per-app policy override supported
- Build flag: `--features recovery`

#### Idempotent Publishing
Server-side deduplication on the REST publish API via an `idempotency_key` field. Duplicate publishes within the TTL window are silently dropped without re-broadcasting.

- Config: `[idempotency]` — `enabled`, `ttl_seconds`, `max_key_length`
- Per-app policy override supported
- Metrics: `idempotency_publish_total`, `idempotency_duplicates_total`

#### Wire-Format Negotiation (V2)
V2 connections can negotiate encoding at connect time:
- JSON (default)
- MessagePack
- Protobuf

Server-side encode/decode handled by `sockudo-protocol/src/wire.rs`.

#### Extended Publishing Semantics
- **Extras envelope** — attach arbitrary metadata to a published event
- **Echo control** — suppress event echo back to the publishing connection
- **Ephemeral messages** — fire-and-forget events not stored in the replay buffer
- **Event-name filtering** — per-subscription filter by event name
- **Batch publish** — publish multiple events in a single HTTP API call

#### New Horizontal Scaling Adapters
- **Kafka** adapter and transport (`--features kafka`)
- **RabbitMQ** adapter and transport (`--features rabbitmq`)
- **Google Pub/Sub** adapter and transport (`--features google-pubsub`)

#### New App Manager Backend
- **SurrealDB** app manager (`--features surrealdb`)

#### Richer V2 Connection State
- Connection capabilities negotiated at handshake
- Connection metadata carried per-socket
- Namespace-aware validation rules
- Signed-in user info updates propagated through WebSocket state

#### Delta Compression & Tag Filtering Improvements
- Protocol-aware delta support — deltas only applied on V2 connections
- Delta cluster-coordination documentation and config for multi-node deployments
- Tag-filtering improvements with zero-allocation evaluation (~12–94 ns per filter)

#### Observability
- `/stats` endpoint expanded
- Additional Prometheus metrics across idempotency, recovery, and wire-format paths
- Improved error-code surface documented in reference docs

#### Client SDK Updates
- **JS SDK**: protocol v2 runtime, `react` and `vue` framework entrypoints, live wire-format tests
- **Python SDK**: v4 protocol support
- **C# SDK**: v4 protocol support

#### Expanded Platform Support
Pre-built binaries and Docker images:
- Linux x86_64 GNU and musl
- Linux ARM64 GNU and musl
- macOS x86_64 (Intel) and ARM64 (Apple Silicon)
- Windows x86_64
- Docker multi-platform manifest (`linux/amd64` + `linux/arm64`)

---

## [3.4.2] - 2026-03-10

- fix: close idle HTTP API connections via `Connection: close` header

## [3.4.1] - 2026-02-XX

- fix: decrement `sockudo_connected` metric on activity timeout cleanup
- fix: resolve DashMap deadlocks in namespace cleanup

## [3.4.0] - 2026-01-XX

- add: Sockudo dashboard Vue app
- fix: DashMap lock contention in channel cleanup
- fix: CORS handling consistency
- add: 404 fallback handler
