# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Sockudo is a simple, fast, and secure WebSocket server for real-time applications built in Rust. It implements the Pusher protocol with support for horizontal scaling, multiple backend adapters, and real-time communication features.

## Feature Flags

Sockudo uses Cargo feature flags to allow compiling only the backends you need, significantly reducing build times and binary size for local development.

### Available Features

**Meta Features:**
- `local` (default) - Includes only local/in-memory implementations (no external dependencies)
- `full` - Enables all available backends

**Backend Features:**
- `redis` - Redis adapter, cache, queue, and rate limiter
- `redis-cluster` - Redis Cluster support (implies `redis`)
- `nats` - NATS adapter for horizontal scaling
- `mysql` - MySQL app manager
- `postgres` - PostgreSQL app manager
- `dynamodb` - DynamoDB app manager
- `sqs` - AWS SQS queue backend
- `lambda` - AWS Lambda webhook support

### Build Examples

```bash
# Local development (default - fastest compile times)
cargo build

# Or explicitly
cargo build --no-default-features --features local

# With Redis only
cargo build --no-default-features --features "local,redis"

# With Redis Cluster and PostgreSQL
cargo build --no-default-features --features "local,redis-cluster,postgres"

# Production build with all features
cargo build --release --features full

# Custom feature combination
cargo build --features "redis,mysql,nats"
```

### Feature Benefits

- **Faster local builds**: Default `local` feature compiles in ~30-50% less time
- **Smaller binaries**: Exclude unused database drivers and cloud SDKs
- **Cleaner dependencies**: Only compile what you actually use
- **Automatic fallback**: If a feature is disabled but configured, falls back to local/memory implementation with a warning

## Development Commands

### Quick Start
```bash
# Complete setup with Docker
make quick-start

# Development environment
make dev

# Install Git hooks (recommended for all developers)
./scripts/install-hooks.sh

# Run tests
make test
cargo test

# Build from source (local development - fastest)
cargo build --release

# Build with specific features
cargo build --release --features "redis,postgres"

# Run the server
./target/release/sockudo --config config/config.json
```

### Docker Operations
```bash
# Production deployment
make prod

# Scale instances
make scale REPLICAS=3

# View logs
make logs-sockudo

# Health check
make health
```

## Architecture

### Core Module Structure
- `src/adapter/` - Connection management and horizontal scaling (Local, Redis, RedisCluster, NATS)
- `src/app/` - Application management with multiple backend support (Memory, MySQL, PostgreSQL, DynamoDB)
- `src/cache/` - Caching layer (Memory, Redis, RedisCluster)
- `src/channel/` - Channel types and subscription management
- `src/delta_compression.rs` - **NEW**: Delta compression for bandwidth optimization (fossil_delta)
- `src/http_handler.rs` - REST API endpoints for event triggering
- `src/websocket.rs` - WebSocket connection handling and message parsing
- `src/webhook/` - Event notification system (HTTP, Lambda)
- `src/queue/` - Background job processing (Memory, Redis, RedisCluster, SQS)
- `src/metrics/` - Prometheus metrics collection
- `src/watchlist/` - Watchlist event management
- `src/protocol/` - Pusher protocol constants and message definitions
- `src/rate_limiter/` - Rate limiting implementation

### Key Design Patterns

**Origin Validation**: Per-app origin validation provides additional WebSocket security by restricting which domains can connect:
```rust
// Example: App config with allowed origins
App {
    id: "my-app".to_string(),
    allowed_origins: Some(vec![
        "https://app.example.com".to_string(),
        "*.staging.example.com".to_string(),
        "http://localhost:3000".to_string()
    ]),
    // ... other fields
}
```

**Adapter Pattern**: All major components (connection management, caching, queuing) use an adapter pattern for backend flexibility:
```rust
// Example: adapter/mod.rs defines trait, implementations in adapter/{local,redis,nats}.rs
pub trait AdapterHandler: Send + Sync {
    async fn subscribe(&self, channel: &str) -> Result<()>;
    async fn unsubscribe(&self, channel: &str) -> Result<()>;
    // ...
}
```

**Configuration Hierarchy** (highest priority wins):
1. Default values (hardcoded)
2. Config file (`config/config.json`)
3. Command-line arguments
4. Environment variables

### WebSocket Protocol

Implements Pusher protocol with extensions:
- **Channel Types**: public, private, presence, private-encrypted
- **Authentication**: HMAC-SHA256 signatures for private/presence channels
- **Events**: Standard pusher events plus client events on private channels

Message format:
```json
{
  "event": "pusher:subscribe",
  "data": {
    "channel": "private-channel",
    "auth": "app_key:signature"
  }
}
```

## Testing Approach

### Running Tests
```bash
# Unit tests
cargo test

# Interactive frontend test
cd test/interactive && npm install && npm start

# Automated integration tests
cd test/automated && npm install && npm test

# Multi-node testing
docker-compose -f docker-compose.multinode.yml up
```

### Test Structure
- `tests/` - Rust unit tests with mocks
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
- `ENVIRONMENT` - Mode (production/development)
- `REDIS_URL` - Override all Redis configurations with single URL
- `SOCKUDO_DEFAULT_APP_ALLOWED_ORIGINS` - Comma-separated list of allowed origins for default app
- `UNIX_SOCKET_ENABLED` - Enable Unix socket server (true|false, default: false)
- `UNIX_SOCKET_PATH` - Unix socket file path (default: /var/run/sockudo/sockudo.sock)
- `UNIX_SOCKET_PERMISSION_MODE` - Unix socket file permissions in octal (default: 660)
- `WEBSOCKET_MAX_MESSAGES` - Max messages buffered per connection (default: 1000, "none" to disable)
- `WEBSOCKET_MAX_BYTES` - Max bytes buffered per connection (default: none, e.g., 1048576 for 1MB)
- `WEBSOCKET_DISCONNECT_ON_BUFFER_FULL` - Disconnect slow clients when buffer full (true|false, default: true)

#### Logging Configuration
**Environment Variables:**
- `LOG_OUTPUT_FORMAT` - Log format (human|json, default: human) **[Must be set at startup]**
- `LOG_COLORS_ENABLED` - Enable/disable colors in human format (true|false, default: true)
- `LOG_INCLUDE_TARGET` - Include module target in logs (true|false, default: true)

**Config File Options** (in `logging` section):
- `colors_enabled` - Enable/disable colors in human format (true|false, default: true)
- `include_target` - Include module target in logs (true|false, default: true)

**Important**: JSON format (`LOG_OUTPUT_FORMAT=json`) can only be configured via environment variable at startup due to tracing subscriber limitations. It cannot be set in config files.

### Redis/NATS Configuration
- Redis: Set `DATABASE_REDIS_HOST`, `DATABASE_REDIS_PORT`, `DATABASE_REDIS_PASSWORD`
- Redis Cluster: Set `REDIS_CLUSTER_NODES` as comma-separated list
- NATS: Set `NATS_SERVERS` as comma-separated list (e.g., "nats://localhost:4222")

## Development Guidelines

### Adding New Features

1. **New Adapter Implementation**: Create in `src/adapter/`, implement `AdapterHandler` trait
2. **New App Manager**: Create in `src/app/managers/`, implement `AppManagerInterface` trait
3. **New Cache/Queue Driver**: Follow existing patterns in `src/cache/` or `src/queue/`

### Code Conventions

- Use `anyhow::Result` for error handling
- Use `tracing` for logging (not `println!`)
- Keep async functions small and focused
- Use `Arc<RwLock<>>` for shared state across async tasks
- Validate all external inputs (especially WebSocket messages)

### Performance Considerations

- Connection pools are managed automatically for database/Redis connections
- Rate limiting is enforced at both API and WebSocket levels
- Use batch operations when processing multiple channels/connections
- Metrics are exposed at `/metrics` for Prometheus scraping
- WebSocket buffers are bounded to prevent memory exhaustion from slow consumers

### WebSocket Buffer Configuration (Slow Consumer Handling)

Sockudo uses bounded buffers for WebSocket connections to protect against slow consumers that can't keep up with message delivery. This prevents unbounded memory growth.

**Three Limit Modes:**
1. **Message count only** (default, fastest) - No size tracking overhead
2. **Byte size only** - Precise memory control
3. **Both limits** - Whichever triggers first (most precise)

**Configuration Examples:**

```json
// Mode 1: Message count only (fastest, default)
{
  "websocket": {
    "max_messages": 1000,
    "max_bytes": null,
    "disconnect_on_buffer_full": true
  }
}

// Mode 2: Byte size only (1MB limit)
{
  "websocket": {
    "max_messages": null,
    "max_bytes": 1048576,
    "disconnect_on_buffer_full": true
  }
}

// Mode 3: Both limits (whichever triggers first)
{
  "websocket": {
    "max_messages": 1000,
    "max_bytes": 1048576,
    "disconnect_on_buffer_full": true
  }
}
```

**Options:**
- `max_messages` - Maximum number of messages in buffer (default: 1000, set to `null` to disable)
- `max_bytes` - Maximum bytes in buffer (default: `null`/disabled, e.g., 1048576 for 1MB)
- `disconnect_on_buffer_full` - When `true`, disconnects slow clients. When `false`, drops messages instead (default: true)

**Behavior:**
- When a client cannot consume messages fast enough, messages queue up in the per-connection buffer
- Once any configured limit is reached:
  - If `disconnect_on_buffer_full: true` → Connection is closed with error code 4100 (reconnect with backoff)
  - If `disconnect_on_buffer_full: false` → New messages are dropped silently (logged as warning)

**Environment Variables:**
```bash
# Message limit (set to "none" or "0" to disable)
WEBSOCKET_MAX_MESSAGES=1000

# Byte limit (set to "none" or "0" to disable)
WEBSOCKET_MAX_BYTES=1048576

# Disconnect behavior
WEBSOCKET_DISCONNECT_ON_BUFFER_FULL=true
```

**Performance Characteristics:**
- **Message-only mode**: Zero overhead for size tracking, uses bounded channel capacity
- **Byte-only mode**: Atomic counter tracking (~1-2ns per message), large channel capacity (10,000)
- **Both modes**: Atomic counter + channel capacity check

**Memory Estimation:**
- Message-only: Depends on average message size (~1-2KB typical)
- Byte-only: Precise control (e.g., 1MB = exactly 1MB max)
- With 1MB byte limit per connection and 10,000 connections: ~10GB worst case

## Platform Support

### Supported Architectures
- **Linux**: x86_64 (GNU and musl), ARM64/AArch64 (GNU and musl)  
- **macOS**: x86_64 (Intel), ARM64 (Apple Silicon)
- **Windows**: x86_64

## Deployment

### Production Checklist
1. Set secure app credentials (`SOCKUDO_DEFAULT_APP_ID`, `SOCKUDO_DEFAULT_APP_KEY`, `SOCKUDO_DEFAULT_APP_SECRET`)
2. Configure SSL certificates (`SSL_CERT_PATH`, `SSL_KEY_PATH`, `SSL_ENABLED=true`)
3. Enable rate limiting (`RATE_LIMITER_ENABLED=true`, `RATE_LIMITER_DRIVER`)
4. Configure webhooks if needed (`WEBHOOK_BATCHING_ENABLED`, `WEBHOOK_BATCHING_DURATION`)
5. Set appropriate limits via app configuration
6. Configure structured logging for external systems (see [Production Logging](#production-logging))
7. Consider Unix socket deployment for reverse proxy scenarios (see [Unix Socket Configuration](#unix-socket-configuration))
8. Configure delta compression for bandwidth optimization (see [Delta Compression](#delta-compression))

### Unix Socket Configuration

Sockudo supports Unix domain sockets for communication when deployed behind a reverse proxy, providing performance benefits by avoiding TCP/IP stack overhead.

#### Configuration Options
```bash
# Enable Unix socket server
UNIX_SOCKET_ENABLED=true

# Custom socket path (optional, default: /var/run/sockudo/sockudo.sock)
UNIX_SOCKET_PATH=/var/run/sockudo/sockudo.sock

# Socket file permissions in octal (optional, default: 660)
UNIX_SOCKET_PERMISSION_MODE=660
```

#### Configuration File Example
```json
{
  "unix_socket": {
    "enabled": true,
    "path": "/var/run/sockudo/sockudo.sock",
    "permission_mode": "660"
  }
}
```

#### Usage with Reverse Proxies

**Nginx Configuration Example:**
```nginx
upstream sockudo {
    server unix:/var/run/sockudo/sockudo.sock;
}

server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://sockudo;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}
```

#### Benefits
- **Performance**: Eliminates TCP/IP stack overhead for local communication
- **Security**: Unix sockets are local filesystem objects, not network accessible
- **Flexibility**: Easier deployment behind reverse proxies without port conflicts

**Note**: Unix sockets are only available on Unix-like systems (Linux, macOS). On Windows, the Unix socket configuration is ignored.

### Production Logging
For production environments with external log aggregation systems (Fluentd, Logstash, etc.):

```bash
# JSON output for parsing-friendly logs (must be set via environment variable)
LOG_OUTPUT_FORMAT=json ./target/release/sockudo

# Human format with no colors (can be set via config file)
LOG_COLORS_ENABLED=false ./target/release/sockudo
```

**Configuration file example:**
```json
{
  "logging": {
    "colors_enabled": false,
    "include_target": true
  }
}
```

**Note**: To use JSON format, you must set `LOG_OUTPUT_FORMAT=json` as an environment variable at startup. JSON format cannot be configured via config files due to technical limitations in the tracing library.

**Benefits of JSON logging:**
- Single-line JSON objects per log entry
- No color codes that interfere with log parsing
- Structured data for better filtering and analysis
- Compatible with log aggregation tools

### Delta Compression

Sockudo implements delta compression using `fossil_delta` and `xdelta3` algorithms to reduce bandwidth usage by sending only message differences.

**Configuration:**
```json
{
  "delta_compression": {
    "enabled": true,
    "algorithm": "fossil",
    "full_message_interval": 10,
    "min_message_size": 100,
    "max_state_age_secs": 300,
    "max_channel_states_per_socket": 100,
    "cluster_coordination": false,
    "omit_delta_algorithm": false
  },
  "tag_filtering": {
    "enabled": false,
    "enable_tags": true
  }
}
```

**Supported Algorithms:**
- `fossil` (default) - Fast binary delta, excellent for most use cases
- `xdelta3` - Industry-standard VCDIFF (RFC 3284), better compression ratio

**Encrypted Channel Detection:**
Delta compression is **automatically disabled** for `private-encrypted-*` channels. Encrypted payloads have no similarity between messages (due to unique nonces), so delta compression would provide zero benefit and waste CPU cycles.

**Per-Publish Delta Control:**
Publishers can control delta compression per-message via the HTTP API:
```bash
# Force delta compression
curl -X POST http://localhost:6001/apps/my-app/events \
  -d '{"name": "update", "channel": "ticker", "data": "{...}", "delta": true}'

# Force full message (skip delta)
curl -X POST http://localhost:6001/apps/my-app/events \
  -d '{"name": "snapshot", "channel": "ticker", "data": "{...}", "delta": false}'
```

**Per-Subscription Delta Negotiation:**
Clients can negotiate delta compression settings per-channel during subscription:
```javascript
// Enable with specific algorithm
pusher.subscribe('ticker:BTC', { delta: { enabled: true, algorithm: 'fossil' } });

// Disable for specific channel
pusher.subscribe('snapshots', { delta: { enabled: false } });

// Combined with tag filtering
pusher.subscribe('events', {
  filter: Filter.eq('type', 'important'),
  delta: { enabled: true, algorithm: 'xdelta3' }
});
```

**Bandwidth Optimization Options:**
- `delta_compression.omit_delta_algorithm`: When `true`, omits the `algorithm` field from delta messages (saves ~20-30 bytes per message). Client must infer the algorithm. Default: `false`
- `tag_filtering.enable_tags`: When `false`, strips tags from all messages globally (saves 50-200 bytes per message depending on tag size). Can be overridden per-channel. Default: `true`
  - **Note:** Tags are stripped AFTER server-side filtering, so you can safely use `enable_tags: false` even when tag filtering is enabled. The server uses tags for routing decisions, then removes them before sending to clients.

**Client Usage:**
Clients can opt-in via:
1. Global: Send `pusher:enable_delta_compression` event after connection
2. Per-channel: Include `delta` settings in subscription request

See [DELTA_COMPRESSION.md](./DELTA_COMPRESSION.md) for full documentation.

**Performance:**
- Typical bandwidth savings: 60-90% for similar consecutive messages
- CPU overhead: ~5-20μs per message
- Memory: ~10-50KB per socket

**Horizontal Scaling:**
- Fully supported on Redis, Redis Cluster, and NATS adapters
- Node-local intervals by default (each node tracks independently)
- Optional cluster coordination for synchronized full message intervals (adds ~0.5-1.2ms latency)

**Cluster Coordination (Optional):**
Enable synchronized full message intervals across all nodes:
```json
{
  "delta_compression": {
    "cluster_coordination": true
  },
  "adapter": {
    "driver": "redis"  // or "redis-cluster" or "nats"
  }
}
```

See [docs/DELTA_COMPRESSION_HORIZONTAL_IMPLEMENTATION.md](./docs/DELTA_COMPRESSION_HORIZONTAL_IMPLEMENTATION.md) and [docs/DELTA_COMPRESSION_CLUSTER_COORDINATION.md](./docs/DELTA_COMPRESSION_CLUSTER_COORDINATION.md) for details.

### Publication Filtering by Tags

Sockudo implements server-side publication filtering by tags, allowing clients to subscribe to channels with filters that reduce bandwidth usage by 60-90% in high-volume scenarios.

**Configuration:**
Tag filtering is **disabled by default** for Pusher backward compatibility. Enable it via:
```bash
# Environment variable
TAG_FILTERING_ENABLED=true

# Or in config file
{
  "tag_filtering": {
    "enabled": true
  }
}
```

**Server-side (adding tags to publications):**
```bash
POST /apps/:app_id/events
{
  "name": "event_name",
  "channel": "channel_name",
  "data": "{...}",
  "tags": {
    "event_type": "goal",
    "priority": "high"
  }
}
```

**Client-side (subscribing with filters):**
```javascript
import { Filter } from 'sockudo-js';

// Simple filter
const channel = pusher.subscribe('match:123', Filter.eq('event_type', 'goal'));

// Complex filter
const filter = Filter.or(
  Filter.eq('event_type', 'goal'),
  Filter.and(
    Filter.eq('event_type', 'shot'),
    Filter.gte('xG', '0.8')
  )
);
const channel = pusher.subscribe('match:123', filter);
```

**Performance:**
- Zero-allocation evaluation in broadcast path (~12-94ns per filter)
- 10k subscriber broadcast: 112-924μs with zero allocations
- Typical bandwidth savings: 60-90% for filtered channels

**Documentation:** See [TAG_FILTERING.md](./docs/TAG_FILTERING.md) and [TAG_FILTERING_QUICKSTART.md](./docs/TAG_FILTERING_QUICKSTART.md)

### Monitoring
- Health endpoint: `GET /up/{app_id}` (WebSocket health check)
- Metrics endpoint: `GET /metrics` (Prometheus format on port 9601)
- WebSocket stats: `GET /apps/{app_id}/channels` (REST API)

## Common Tasks

### Debug WebSocket Issues
```bash
# Enable debug logging via environment variable
DEBUG=true ./target/release/sockudo  # Takes precedence over DEBUG_MODE

# Or use Rust log levels
RUST_LOG=debug ./target/release/sockudo

# Check specific module
RUST_LOG=sockudo::websocket=debug ./target/release/sockudo
```

### Test Channel Authentication
```bash
# Generate auth signature for testing
echo -n "socket_id:channel_name" | openssl dgst -sha256 -hmac "app_secret" -hex
```

### Scale Horizontally
1. Choose adapter: Redis, Redis Cluster, or NATS
2. Configure all instances with same adapter settings
3. Load balance WebSocket connections (use sticky sessions)
4. Share app configuration via database backend


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