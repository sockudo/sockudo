# Changelog

## Unreleased

### Added

- Model Context Protocol server. The new `sockudo-mcp` crate (built on the official `rmcp` SDK)
  exposes scoped tools, `sockudo://` resources, prompts, and argument completion for channels,
  publishing, durable history, versioned messages, annotations, presence history, push, and
  operations. The `sockudo` binary embeds it behind the `mcp` Cargo feature and `[mcp]` config
  (Streamable HTTP on `/mcp` or a dedicated port, bearer tokens with `read`/`write`/`admin`
  scopes and app allow-lists, per-token rate limits, audit logs, `mcp_*` Prometheus metrics),
  driving its own API router in-process. The standalone `sockudo-mcp` binary serves stdio or HTTP
  against a remote deployment.

## [5.0.1] - 2026-08-21

### Fixed

- Started the Ably realtime push worker only when compatibility and its local provider capability
  are active, preventing repeated unsupported-stage warnings, and compiled the standard Docker
  image with the in-process `monolith` push workers. Fixes #400.

## [5.0.0] - 2026-08-17

### Breaking Changes

- AI Transport now uses the Session/Run public vocabulary. The TypeScript SDK renames the
  Turn-named APIs and types, writes `run-continue` instead of `turn-continue`, and exposes the
  four-arm run lifecycle (`start`, `suspend`, `resume`, and `end`). Existing `turn-continue`
  history remains readable for migration compatibility.
- The coordinated SDK release advances additive packages to 2.2.0. AI Transport and the Swift
  client advance to 3.0.0 for their breaking public API and actor-isolation changes.

Protocol V1 Pusher compatibility remains unchanged. The new Ably facade is opt-in and does not
alter the native `/app/{appKey}` route.

### Added

- Opt-in `ably-compat` REST and WebSocket facade with JSON and MessagePack realtime support,
  capability auth, presence, history, rewind, recovery, mutable messages, annotations, selected
  push-recipient APIs, and AI Transport interoperability. The supported claim excludes Live
  Objects and non-WebSocket realtime transports.
- Pinned Node, Chromium, Go, strict-completeness, and AI Transport compatibility suites plus a
  released-binary verification workflow and evidence scorecard.
- Deterministic Sockudo simulator, storage and upgrade fault injection, failure shrinking,
  outside-in binary chaos tooling, distributed correctness probes, and expanded fuzz targets for
  protocol and durable-state boundaries.
- `POST /apps/{appId}/users/{userId}/force_reconnect`, with matching helpers across all nine
  server SDKs.
- Per-node connection capacity limits, configuration-file `${VAR}` substitution, expanded
  deployment guides, Caddy support, and a broader operator dashboard.
- AI Transport steering, step lifecycle, run continuation, configurable header ceilings, and
  recovery-aware branching/tool-result handling in `@sockudo/ai-transport`.

### Changed

- Queue backends use the Queue V2 worker model, Redis blocking behavior is bounded, and Iggy
  batches offset commits for higher throughput.
- Push admission, retry, dead-letter handling, provider failure classification, queue-age
  backpressure, cleanup, repair, and durable status coordination now fail closed and expose
  stronger operational controls.
- Logging across the Rust workspace uses structured tracing fields with stable messages and safer
  content handling.
- Client SDKs share bounded reconnection behavior and corrected capability-token refresh,
  expiration, and revocation handling.
- Swift client coordination moved from `MainActor` to the dedicated `SockudoActor` to avoid
  blocking the main thread while preserving serialized state access.

### Fixed

- Applied `database.redis.master_tls` private-CA and mutual-TLS settings to direct Redis and Redis
  Cluster connections across the adapter, cache, queue, rate limiter, and delta coordinator; cache
  and rate limiting now use native Sentinel topology instead of passing an unsupported Sentinel URL.
- Made Redis sliding-window admission atomic and safely retried one dropped-connection failure with
  an idempotent member ID, preventing idle proxy disconnects from turning the next WebSocket
  handshake into HTTP 500.
- Updated the vulnerable `h2` dependency and made the NATS cross-node integration health assertion
  wait through transient reconnect states.
- Protocol V2 now uses the native nonce-based Ping/Pong heartbeat in `sockudo-ws` 2.0.1, lets the
  WebSocket engine provide native Pong responses, and closes missed Pong deadlines with code 4201,
  without running a duplicate application-level heartbeat. Protocol V1 and Ably compatibility keep
  their existing heartbeat behavior.
- Restored Protocol V1 frame handling, corrected native auth and presence behavior, and hardened
  Ably reconnect/recovery ordering and continuity failure handling. Explicit Ably channel serials
  remain authoritative on same-node resumed attachments, preventing already-seen messages from
  being mixed into durable recovery. Empty versioned channels now advertise position zero from the
  version store's stable stream identity instead of a history or hot-buffer stream that can
  conflict with the first versioned publish. Deployments without durable history filter the live
  recovery tail strictly after the client's channel serial and fail closed when buffered
  continuity cannot be proven.
- Suppressed repeated Ably ACKable protocol serials on the same resumed transport while allowing
  the first retry on a replacement transport, preventing duplicate processing and responses.
- Released socket rate-limit entries, token-expiry tasks, disconnect tasks, and stats-map guards
  promptly instead of retaining resources or holding guards across asynchronous work.
- Skipped expensive webhook channel-count queries when filters exclude the channel and fixed
  PostgreSQL user-status and JSON policy persistence edge cases.
- Corrected the React Native client entrypoint and stabilized SDK, parser, fanout, and compatibility
  CI lanes.
- Refreshed the JavaScript and PHP SDK release dependency graphs, documentation toolchain, and
  nested Rust SDK/benchmark lockfiles to patched versions with clean security audits.

### Performance

- Reduced avoidable clones and per-subscriber payload work in adapter fanout, recovery, presence,
  queues, webhooks, and the Ably compatibility facade.
- Added regression budgets for compatibility parsing, fanout grouping, recovery, and real-topology
  load evidence.

## [4.7.0] - 2026-06-28

### Added

- Coordinated SDK 2.1.0 release with Protocol V2 capability-token auth, presence updates,
  `until_attach` history, mutable-message helpers, append-rollup negotiation, and forward-compatible
  decoding across the official client SDKs.
- Forward-compatible webhook and mutation response handling across the official server SDKs.

### Fixed

- Kept V2-only versioned-message metadata out of Protocol V1 delivery.
- Corrected MySQL durable-history index sizing, Swift reconnect and delta-decoder behavior, and
  webhook hot-path allocation and filtering issues.

## [4.6.0] - 2026-06-17

### Added

- AI Transport GA readiness gates, including `scripts/ai-transport-ga-gate.sh`, the release
  readiness record in `docs/specs/ai-transport-ga-readiness.md`, and
  `config/ai-transport.example.toml` for release artifacts.
- Release-candidate evidence recorders for S14 fleet profiles, shared-Redis rolling upgrades, and
  the full in-repo SDK compatibility matrix.
- AI Transport wire-protocol v1 compatibility promise in
  `docs/specs/ai-transport-wire-protocol.md`.

### Changed

- Docker release builds now use the workspace Rust toolchain version and include the push,
  AI Transport, and benchmark workspace manifests during dependency-cache setup.
- Crates.io release publishing now includes the `sockudo-ai-transport` crate before downstream
  crates that can depend on it.
- The CI test suite now frees unused runner toolchains and avoids restoring cached `target/`
  artifacts so full-feature test builds do not exhaust GitHub runner disk space.

## [4.5.2] - 2026-06-13

### Fixed

- Published the `sockudo` binary crate release line after `4.5.x` library crates advanced independently on crates.io.
- Included the `sockudo-push` crate in the publish sequence so optional push features no longer block publishing `sockudo`.

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
