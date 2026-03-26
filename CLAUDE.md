# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Sockudo is a simple, fast, and secure WebSocket server for real-time applications built in Rust. It supports dual protocol versioning: V1 (strict Pusher compatibility) and V2 (Sockudo-native with serial numbers, message IDs, connection recovery, delta compression, and tag filtering built-in).

## Workspace Structure

Sockudo is organized as a Cargo workspace with 12 crates under `crates/`:

```
crates/
├── sockudo-protocol/       # Pusher protocol messages and constants (leaf crate)
├── sockudo-filter/          # Zero-allocation tag-based publication filtering (leaf crate)
├── sockudo-core/            # Core traits, types, error handling, config, websocket types
├── sockudo-app/             # App manager implementations (Memory, MySQL, PG, DynamoDB, Scylla)
├── sockudo-cache/           # Cache implementations (Memory, Redis, Redis Cluster)
├── sockudo-queue/           # Queue implementations (Memory, Redis, SQS, SNS)
├── sockudo-rate-limiter/    # Rate limiter implementations + HTTP middleware
├── sockudo-metrics/         # Prometheus metrics driver
├── sockudo-webhook/         # Webhook integration + HTTP/Lambda senders
├── sockudo-delta/           # Delta compression manager (fossil_delta, xdelta3)
├── sockudo-adapter/         # Connection management, horizontal scaling, handler logic, replay buffer
└── sockudo-server/          # Binary: HTTP + WebSocket handlers, server bootstrap
```

### Dependency DAG

```
sockudo-protocol ──┐
                   ├─→ sockudo-core ──┬─→ sockudo-app
sockudo-filter ────┘                  ├─→ sockudo-cache
                                      ├─→ sockudo-queue ──→ sockudo-webhook
                                      ├─→ sockudo-rate-limiter
                                      ├─→ sockudo-metrics
                                      ├─→ sockudo-delta
                                      └─→ sockudo-adapter ──→ sockudo-server (binary)
```

### Crate Responsibilities

| Crate | What it contains | Key traits/types |
|---|---|---|
| `sockudo-protocol` | Protocol message types, constants, versioning | `PusherMessage`, `MessageData`, `ProtocolVersion`, `ErrorData` |
| `sockudo-filter` | Tag filter evaluation, zero-alloc matching | `FilterNode`, `CompareOp`, `LogicalOp`, `TagMap` |
| `sockudo-core` | All shared traits, types, config, error handling | `AppManager`, `CacheManager`, `QueueInterface`, `RateLimiter`, `MetricsInterface`, `App`, `SocketId`, `WebSocketRef`, `ServerOptions`, `Error`, `Result` |
| `sockudo-app` | App storage backends | `MemoryAppManager`, `CachedAppManager`, `AppManagerFactory` |
| `sockudo-cache` | Cache backends | `MemoryCacheManager`, `FallbackCacheManager`, `CacheManagerFactory` |
| `sockudo-queue` | Queue backends | `MemoryQueueManager`, `QueueManager`, `QueueManagerFactory` |
| `sockudo-rate-limiter` | Rate limiting + Tower middleware | `MemoryRateLimiter`, `RateLimitLayer`, `RateLimiterFactory` |
| `sockudo-metrics` | Metrics collection | `PrometheusMetricsDriver` |
| `sockudo-webhook` | Webhook delivery | `WebhookSender`, `WebhookIntegration` |
| `sockudo-delta` | Delta compression runtime | `DeltaCompressionManager`, `DeltaMessage` |
| `sockudo-adapter` | Connection handling, adapters, horizontal scaling, replay buffer | `ConnectionHandler`, `ConnectionManager`, `LocalAdapter`, `ChannelManager`, `PresenceManager`, `ReplayBuffer` |
| `sockudo-server` | Binary entry point, HTTP/WS handlers | `main()`, `http_handler`, `ws_handler`, cleanup workers |

## Feature Flags

Features are defined **per-crate** and propagated through `sockudo-server`. The server crate's features activate the corresponding features in downstream crates.

### Build Examples

```bash
# Local development (default - fastest compile times)
cargo build -p sockudo

# With Redis only
cargo build -p sockudo --features redis

# With Redis Cluster and PostgreSQL
cargo build -p sockudo --features "redis-cluster,postgres"

# Production build with all features
cargo build -p sockudo --release --features full

# Build entire workspace (all crates, default features)
cargo build --workspace

# Build specific crate
cargo build -p sockudo-adapter --features "redis,nats"
```

### Feature Propagation

Features on `sockudo-server` activate downstream crate features:
- `redis` → `sockudo-adapter/redis`, `sockudo-cache/redis`, `sockudo-queue/redis`, `sockudo-rate-limiter/redis`
- `mysql` → `sockudo-app/mysql`
- `nats` → `sockudo-adapter/nats`
- etc.

## Development Commands

### Quick Start
```bash
# Complete setup with Docker
make quick-start

# Development environment
make dev

# Run tests (entire workspace)
cargo test --workspace

# Run tests for a specific crate
cargo test -p sockudo-adapter

# Clippy (entire workspace)
cargo clippy --workspace

# Build the server binary (local development - fastest)
cargo build -p sockudo --release

# Run the server
./target/release/sockudo --config config/config.toml
```

### Docker Operations
```bash
make prod           # Production deployment
make scale REPLICAS=3
make logs-sockudo
make health
```

## Architecture

### Key Design Patterns

**Trait-based abstraction**: All major components define traits in `sockudo-core` with implementations in dedicated crates:
```rust
// sockudo-core defines the trait
pub trait CacheManager: Send + Sync { ... }

// sockudo-cache provides implementations
pub struct MemoryCacheManager { ... }
pub struct RedisCacheManager { ... }   // #[cfg(feature = "redis")]
```

**Factory pattern**: Each implementation crate has a factory for creating backends based on config:
```rust
// sockudo-cache::factory
CacheManagerFactory::create(&options) -> Arc<dyn CacheManager>
```

**Feature-gated compilation**: Backend implementations are behind Cargo features to minimize compile times and binary size.

**Configuration Hierarchy** (highest priority wins):
1. Default values (hardcoded)
2. Config file (`config/config.toml` or `config/config.json`)
3. Command-line arguments
4. Environment variables

**Config format**: TOML (preferred) or JSON (legacy). Server tries `config.toml` first, falls back to `config.json`.

### Protocol Versioning

Sockudo supports dual protocol versions, negotiated per-connection via `?protocol=` query param:

| | Protocol V1 (default) | Protocol V2 |
|---|---|---|
| Event prefix | `pusher:` / `pusher_internal:` | `sockudo:` / `sockudo_internal:` |
| `serial` | Never sent | Always on every message |
| `message_id` | Never sent | Always on every broadcast |
| Connection recovery | Not available | Always available |
| Delta compression | Not available | Native |
| Tag filtering | Not available | Native |
| Message idempotency | Not available | Native (`message_id` on every broadcast) |
| Compatible SDKs | Official Pusher SDKs | Sockudo client SDKs |

V1 connections receive plain Pusher-compatible messages with no extensions. All Sockudo features (delta, tag filtering, serial, message_id, recovery) are V2-only and each is individually configurable via config/env.

**Key files:**
- `crates/sockudo-protocol/src/protocol_version.rs` - `ProtocolVersion` enum, canonical event names, prefix translation
- `crates/sockudo-server/src/ws_handler.rs` - Parses `?protocol=2` from WebSocket URL
- `crates/sockudo-adapter/src/handler/mod.rs` - Routes events using canonical names (accepts both prefixes)
- `crates/sockudo-adapter/src/local_adapter.rs` - V1 gets plain Pusher JSON (no delta/tags/serial), V2 gets sockudo: prefix + all features

### WebSocket Protocol

- **Channel Types**: public, private, presence, private-encrypted
- **Authentication**: HMAC-SHA256 signatures for private/presence channels
- **Events**: Standard protocol events plus client events on private channels
- **Connection Recovery** (V2 native): Serial-based replay buffer for exactly-once delivery on reconnect
- **Message Idempotency** (V2 native): UUIDv4 `message_id` on every broadcast

**Key files:**
- `crates/sockudo-adapter/src/replay_buffer.rs` - Per-channel replay buffer (DashMap + VecDeque)
- `crates/sockudo-adapter/src/handler/recovery.rs` - Resume event handler
- `crates/sockudo-adapter/src/handler/connection_management.rs` - Serial assignment and message_id injection at broadcast time
- `crates/sockudo-core/src/cache.rs` - `CacheManager::set_if_not_exists()` for atomic idempotency

## Testing

### Running Tests
```bash
# All workspace tests
cargo test --workspace

# Specific crate
cargo test -p sockudo-core
cargo test -p sockudo-adapter
cargo test -p sockudo-filter

# Interactive frontend test
cd test/interactive && npm install && npm start

# Automated integration tests
cd test/automated && npm install && npm test
```

### Test Locations
- Each crate has unit tests inline (`#[cfg(test)]` modules)
- Integration tests live in each crate's `tests/` directory
- `sockudo-adapter/tests/` has the largest integration test suite (159 tests)
- `test/interactive/` - Browser-based WebSocket testing UI
- `test/automated/` - Node.js integration tests using Pusher SDK

## Configuration

### Environment Variables
Key variables (see `.env.example` for complete list):
- `PORT` - Server port (default: 6001)
- `HOST` - Server host (default: 0.0.0.0)
- `ADAPTER_DRIVER` - Connection adapter (local|redis|redis-cluster|nats)
- `APP_MANAGER_DRIVER` - App storage (memory|mysql|postgresql|dynamodb)
- `CACHE_DRIVER` - Cache backend (memory|redis|redis-cluster|none)
- `QUEUE_DRIVER` - Queue backend (memory|redis|redis-cluster|sqs|none)
- `RATE_LIMITER_DRIVER` - Rate limiter backend (memory|redis|redis-cluster|none)
- `DEBUG` or `DEBUG_MODE` - Enable debug logging (DEBUG takes precedence)
- `REDIS_URL` - Override all Redis configurations with single URL
- `LOG_OUTPUT_FORMAT` - Log format (human|json, default: human) **[Must be set at startup]**

Note: All Sockudo features are V2-only, gated by both **Cargo feature flags** (compile-time) and **config** (runtime):

Cargo features (on `sockudo-server` and `sockudo-adapter`):
- `v2` (default) — meta-feature enabling all V2 features
- `delta` — delta compression
- `tag-filtering` — server-side tag filtering
- `recovery` — connection recovery (serial + message_id + replay buffer)

Runtime config (only applies to V2 connections):
- `delta_compression.enabled` — delta compression
- `tag_filtering.enabled` — tag-based filtering
- `connection_recovery.enabled` — serial numbers + message_id + replay buffer

Build without V2: `cargo build -p sockudo --no-default-features` (pure Pusher-only server).
V1 connections get plain Pusher protocol with none of these features regardless of config.

Full configuration reference: see `docs/` (Nuxt-based documentation site).

## Development Guidelines

### Adding New Features

1. **New Adapter**: Add to `crates/sockudo-adapter/src/`, implement `ConnectionManager` trait
2. **New App Manager**: Add to `crates/sockudo-app/src/`, implement `AppManager` trait from `sockudo-core`
3. **New Cache/Queue Driver**: Add to `crates/sockudo-cache/` or `crates/sockudo-queue/`, implement the trait from `sockudo-core`
4. **New feature flag**: Define in the implementation crate's `Cargo.toml`, propagate through `sockudo-server/Cargo.toml`

### Import Conventions

When working across crates:
```rust
// Core types and traits
use sockudo_core::app::App;
use sockudo_core::error::{Error, Result};
use sockudo_core::websocket::SocketId;
use sockudo_core::cache::CacheManager;

// Protocol types
use sockudo_protocol::ProtocolVersion;
use sockudo_protocol::messages::PusherMessage;
use sockudo_protocol::constants::CLIENT_EVENT_PREFIX;
use sockudo_protocol::protocol_version::CANONICAL_SUBSCRIBE; // canonical event names

// Filter types
use sockudo_filter::FilterNode;

// Implementation types (from other crates)
use sockudo_adapter::ConnectionHandler;
use sockudo_delta::DeltaCompressionManager;
```

### Code Conventions

- Use `sockudo_core::error::Result` for error handling (backed by `thiserror`)
- Use `tracing` for logging (not `println!`)
- Keep async functions small and focused
- Validate all external inputs (especially WebSocket messages)
- Feature-gate optional backends with `#[cfg(feature = "...")]`
- Workspace dependency inheritance: declare shared dep versions in root `Cargo.toml` `[workspace.dependencies]`

### Performance Considerations

- Connection pools are managed automatically for database/Redis connections
- Rate limiting is enforced at both API and WebSocket levels
- Use batch operations when processing multiple channels/connections
- Metrics are exposed at `/metrics` for Prometheus scraping
- WebSocket buffers are bounded to prevent memory exhaustion from slow consumers
- Delta compression provides 60-90% bandwidth savings for similar consecutive messages
- Tag filtering uses zero-allocation evaluation in the broadcast path (~12-94ns per filter)

## Documentation

The `docs/` directory contains a Nuxt-based documentation site with:
- `docs/content/1.getting-started/` - Installation, first connection, auth, migration
- `docs/content/2.server/` - Configuration, env vars, HTTP API, scaling, security, observability, delta compression, tag filtering, app managers, queue, cache, webhooks, rate limiting, SSL
- `docs/content/3.client/` - Client-side integration
- `docs/content/4.integrations/` - Third-party integrations
- `docs/content/5.reference/` - Protocol spec, HTTP endpoints, config reference, compatibility

### Monitoring
- Health endpoint: `GET /up/{app_id}`
- Metrics endpoint: `GET /metrics` (Prometheus format on port 9601)
- WebSocket stats: `GET /apps/{app_id}/channels`

## Platform Support

- **Linux**: x86_64 (GNU and musl), ARM64/AArch64 (GNU and musl)
- **macOS**: x86_64 (Intel), ARM64 (Apple Silicon)
- **Windows**: x86_64

## Code Review Guidelines

- **Comments**:
  * Never add comment to note that you have removed a code block
  * Don't add comment with no value or that over explain what you are doing or what you changed
  * Add comments when it will help the developer understand your intent or complex code

## Development Principles

- **Implementation Guidelines**:
  * Do not add placeholder implementations
  * A comment that an implementation is not done yet or will be done later is fine
  * Do not add config / env values for features that do not exist yet, unless asked directly
