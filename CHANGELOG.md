# Changelog

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
