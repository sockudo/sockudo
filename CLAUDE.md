# CLAUDE.md

This file gives coding agents a compact, code-verified map of Sockudo. Trust the code over older
notes if this file drifts.

## Project Overview

Sockudo is a Pusher-compatible, high-performance Rust realtime server. Protocol V1 remains strict
Pusher wire compatibility. Protocol V2 adds Sockudo-native features: serials, message IDs, recovery,
durable history, mutable/versioned messages, annotations, tag filtering, delta compression,
presence-history, capability-token auth, and optional AI Transport conventions.

AI Transport is additive. Build it with Cargo feature `ai-transport`, enable it at runtime with
`[ai_transport] enabled = true`, and extend the existing versioned-message, history, recovery,
annotation, and push subsystems. Do not build parallel AI-only storage or fanout paths.

## Workspace Structure

The Cargo workspace has 14 members. Thirteen production/library crates live under `crates/`; the
fourteenth is the permanent AI benchmark crate under `benches/ai`.

```text
crates/
  sockudo-protocol        protocol message types, V1/V2 prefixes, wire payloads
  sockudo-filter          tag filter parsing and matching
  sockudo-core            shared traits, config, auth, history, version store, errors
  sockudo-app             app-manager backends
  sockudo-cache           cache backends
  sockudo-queue           queue backends
  sockudo-rate-limiter    rate limiters and middleware
  sockudo-metrics         Prometheus metrics driver
  sockudo-webhook         webhook delivery
  sockudo-delta           delta compression runtime
  sockudo-push            push domain, storage, queue, providers, scheduler
  sockudo-ai-transport    AI Transport validation, rollup, and conformance helpers
  sockudo-adapter         connection handling, presence, fanout, recovery, rollup egress
  sockudo-server          binary, HTTP/WS routes, bootstrap, durable stores

benches/ai               Criterion suite and budgets for AI hot paths
```

## Dependency DAG

```text
sockudo-protocol ──┐
sockudo-filter ────┼─→ sockudo-core ──┬─→ sockudo-app
                   │                  ├─→ sockudo-cache
                   │                  ├─→ sockudo-queue ──┬─→ sockudo-webhook
                   │                  │                   └─→ sockudo-push
                   │                  ├─→ sockudo-rate-limiter
                   │                  ├─→ sockudo-metrics
                   │                  └─→ sockudo-delta
                   ├─→ sockudo-ai-transport
                   └─→ sockudo-adapter ──→ sockudo-server
```

`sockudo-server` wires the implementation crates together. Backend-specific stores for durable
history and versioned messages live in the server crate because they depend on database clients and
runtime bootstrap.

## Crate Responsibilities

| Crate | Main responsibilities | Key files |
| --- | --- | --- |
| `sockudo-protocol` | Pusher-compatible messages, V2 prefixing, extras, versioned realtime frames | `src/messages.rs`, `src/protocol_version.rs`, `src/versioned_messages.rs` |
| `sockudo-filter` | Tag-filter expression model and matching | `src/node.rs`, `src/ops.rs` |
| `sockudo-core` | Traits, config, auth, errors, `HistoryStore`, `VersionStore`, annotations, presence-history | `src/options.rs`, `src/history.rs`, `src/version_store.rs`, `src/versioned_messages.rs`, `src/annotations.rs`, `src/presence_history.rs`, `src/capability_token.rs` |
| `sockudo-app` | App-manager backends | `src/*_app_manager.rs` |
| `sockudo-cache` | Memory/Redis cache managers | `src/*` |
| `sockudo-queue` | Memory and external queue managers | `src/*` |
| `sockudo-rate-limiter` | Rate limit drivers and Tower middleware | `src/*` |
| `sockudo-metrics` | Prometheus metrics exporter and metric catalog | `src/prometheus.rs` |
| `sockudo-webhook` | Webhook event transformation and delivery | `src/*` |
| `sockudo-delta` | Delta compression state and algorithms | `src/*` |
| `sockudo-push` | Push credentials, device registrations, channel subscriptions, publish planning, queues, provider dispatch, feedback, scheduler | `src/domain.rs`, `src/storage.rs`, `src/pipeline.rs`, `src/dispatch.rs`, `src/scheduler.rs` |
| `sockudo-ai-transport` | AI extras validation, lifecycle conventions, append rollup, conformance helpers | `src/lib.rs`, `tests/` |
| `sockudo-adapter` | Connection manager, local/horizontal fanout, subscriptions, presence, recovery, rollup egress | `src/handler/*`, `src/local_adapter.rs`, `src/replay_buffer.rs` |
| `sockudo-server` | HTTP/WS routes, app startup, database-backed history/version stores, push HTTP API | `src/main.rs`, `src/http_handler.rs`, `src/push_http.rs`, `src/history_*.rs` |

## Feature Flags

Features are crate-local and propagated through `crates/sockudo-server/Cargo.toml`.

Important server features:

- `v2`: Protocol V2 feature bundle.
- `ai-transport`: AI Transport validation, rollup, and conformance surfaces. Not default.
- `push`: push HTTP/runtime support through `sockudo-push`.
- `redis`, `redis-cluster`, `nats`, `pulsar`, `rabbitmq`, `google-pubsub`, `kafka`, `iggy`: adapter/cache/queue integrations as applicable.
- `mysql`, `postgres`, `dynamodb`, `surrealdb`, `scylladb`: app, history, version-store, and push storage backends as applicable.
- `sqs`, `sns`, `lambda`: queue/webhook/cloud integrations.
- `full`: all production integrations plus `ai-transport` and `push` where wired.

Build examples:

```bash
cargo build -p sockudo
cargo build -p sockudo --features "v2,ai-transport,redis,postgres,push"
cargo build -p sockudo --release --features full
cargo build --workspace
```

## Core Subsystems

### Protocol V1 and V2

V1 clients receive byte-compatible Pusher frames with Sockudo-only fields stripped. V2 clients get
`sockudo:` prefixes, `serial`, `message_id`, extras, recovery frames, delta/filter features,
mutable-message actions, annotations, and AI Transport conventions when enabled.

Key files: `sockudo-protocol/src/protocol_version.rs`,
`sockudo-adapter/src/local_adapter.rs`, `sockudo-server/src/ws_handler.rs`.

### Versioned Mutable Messages

`MessageAction` supports `create`, `update`, `delete`, `append`, and `summary`. `VersionStore`
reserves gapless delivery positions, appends versions, reads latest visible state, pages version
history, replays after delivery serial, projects latest-by-history, and purges expired records.

HTTP surfaces include publish-created mutable messages through `/events`, mutation endpoints under
`/channels/{channel}/messages/{message_serial}/{update|delete|append}`, latest reads, and version
history. Authorization uses `message_update_own/any`, `message_delete_own/any`, and
`message_append_own/any`; own-scoped grants require verified connection identity.

Key files: `sockudo-protocol/src/versioned_messages.rs`,
`sockudo-core/src/versioned_messages.rs`, `sockudo-core/src/version_store.rs`,
`sockudo-core/src/versioned_message_auth.rs`, `sockudo-server/src/http_handler.rs`.

### Durable History, Rewind, and Recovery

`HistoryStore` is distinct from the hot replay buffer. Durable history owns retention, cursors,
stream state, reset/purge, and cold recovery. Protocol V2 recovery first tries hot replay, then
durable history when continuity can be proven. Subscribe-time rewind reads recent history and
`until_attach` bounds client history pages at the attach serial so late joiners get gapless history
plus live delivery.

Key files: `sockudo-core/src/history.rs`, `sockudo-adapter/src/replay_buffer.rs`,
`sockudo-adapter/src/handler/recovery.rs`, `sockudo-adapter/src/handler/history_frames.rs`,
`sockudo-adapter/src/handler/subscription_management.rs`, `sockudo-server/src/history_*.rs`.

### Annotations

Annotations are V2 mutable side records with publish/delete, summary projection, raw subscription
mode, own/any delete authorization, and metrics. They are feature/config gated and delivered only
to V2 clients that request annotation mode.

Key files: `sockudo-core/src/annotations.rs`, `sockudo-adapter/src/handler/annotations.rs`,
`sockudo-server/src/http_handler.rs`.

### Push

`sockudo-push` handles provider credentials, device registrations, channel subscriptions, publish
requests, durable idempotency/status, fanout planning, queues, retries, circuit breakers,
provider dispatch, feedback, scheduled-job cancellation, and cleanup. Realtime publishes may also
trigger channel push through `extras.push` or `[[push_rules]]`.

Key files: `sockudo-push/src/domain.rs`, `sockudo-push/src/storage.rs`,
`sockudo-push/src/pipeline.rs`, `sockudo-push/src/dispatch.rs`,
`sockudo-server/src/push_http.rs`.

### Presence History

Presence history records authoritative joins/leaves separately from current membership. It can use
memory or durable history as storage and supports paging, snapshots, degraded/reset-required
state, retention, and metrics.

Key files: `sockudo-core/src/presence_history.rs`,
`sockudo-adapter/src/presence.rs`, `sockudo-server/src/presence_history.rs`.

### AI Transport

AI Transport defines a session-as-channel model, five lifecycle events (`ai-input`, `ai-output`,
`ai-turn-start`, `ai-turn-end`, `ai-cancel`), typed `extras.ai.transport` and `extras.ai.codec`,
turn/stream validation, append rollup, orphan cancellation, conformance tests, load/chaos tooling,
and capacity docs. It depends on versioned messages, durable history, recovery, presence, push, and
capability-token auth rather than replacing them.

Key files: `crates/sockudo-ai-transport/src/lib.rs`,
`crates/sockudo-ai-transport/tests/`, `test/ai-conformance/`, `test/load/`, `tools/chaos/`,
`docs/specs/ai-transport-wire-protocol.md`.

## Configuration Surfaces

Important sections:

- `[versioned_messages]`
- `[history]` and backend subsections
- `[presence_history]`
- `[annotations]`
- `[push]`, `[push.retry]`, `[push.circuit_breaker]`, `[push.default_quotas]`,
  `[push.payload_redaction]`
- `[[push_rules]]`
- `[ai_transport]`, `[[ai_transport.channels]]`, `[ai_transport.rollup]`
- Protocol V2 capability tokens are not a TOML feature switch; they are accepted by V2 connection
  query `token` and refreshed with `sockudo:auth`.

Reference docs: `docs/content/docs/reference/configuration.mdx`,
`docs/content/docs/reference/environment-variables.mdx`.

## Test Locations

- Rust unit tests: inline `#[cfg(test)]` modules in each crate.
- Rust integration tests: each crate's `tests/` directory.
- AI Transport Rust conformance: `crates/sockudo-ai-transport/tests/`.
- SDK-facing AI conformance: `test/ai-conformance/`.
- Scale and chaos: `test/load/`, `tools/chaos/`.
- AI Criterion budgets: `benches/ai/`.
- Push integration smoke: `crates/sockudo-push/tests/`.
- Legacy/SDK tests: `test/interactive/`, `test/automated/` where present.

## Development Commands

```bash
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo build -p sockudo --features "v2,ai-transport,redis,postgres,push"
scripts/ai-transport-bench-guard.sh
AIT_CONFORMANCE_OFFLINE=1 scripts/ai-conformance-node.sh
```

Docs:

```bash
cd docs
npm run types:check
npm run build
```

## Compatibility Rules

- Preserve V1 wire output unless the task explicitly changes Pusher compatibility.
- AI Transport, capability tokens, mutable messages, annotations, client history frames, and
  rollup are V2-only.
- No client-asserted `client_id` is authoritative; identity comes from signed-in socket state or
  trusted app-key HTTP context.
- No locks across `.await` in hot fanout paths, no unbounded queues, and no avoidable per-message
  allocations on broadcast paths.
- Update docs and tests whenever a protocol-visible shape, config key, feature flag, endpoint, or
  recovery/history semantic changes.
