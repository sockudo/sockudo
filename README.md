# Sockudo

A high-performance, scalable WebSocket server implementing the Pusher protocol in Rust.

[![Stars](https://img.shields.io/github/stars/sockudo/sockudo?style=social)](https://github.com/Sockudo/sockudo)
[![Build Status](https://img.shields.io/github/actions/workflow/status/sockudo/sockudo/ci.yml?branch=main)](https://github.com/sockudo/sockudo/actions)
[![License](https://img.shields.io/github/license/sockudo/sockudo)](LICENSE)

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=sockudo/sockudo&type=Date)](https://star-history.com/#Sockudo/sockudo&Date)

## Features

- **🚀 High Performance** - Handle 100K+ concurrent connections
- **🔄 Pusher Compatible** - Drop-in replacement for Pusher services
- **🏗️ Scalable Architecture** - Redis, Redis Cluster, NATS adapters
- **🛡️ Production Ready** - Rate limiting, SSL/TLS, metrics
- **⚡ Async Cleanup** - Non-blocking disconnect handling
- **📊 Real-time Metrics** - Prometheus integration
- **🧠 Delta Compression + Conflation** - Fossil and Xdelta3 (VCDIFF) with per-channel controls
- **🏷️ Tag Filtering** - High-performance server-side filtering with optional tag emission controls
- **🌐 Native WebSocket Engine** - `sockudo_ws` (replacing `fastwebsockets`) with advanced runtime tuning

## What Was Added In This Branch

This branch includes a major realtime pipeline upgrade:

- Delta compression enhancements (including late-subscriber sync, cluster coordination, state cleanup hardening, and sequence/base-index improvements)
- Channel-level and global tag-filtering optimizations
- Conflation key support to isolate delta state per logical stream/key
- Broadcast and adapter path optimizations (lock/contention, batching, buffer controls)
- Expanded benchmark/soak tooling and test assets
- Full WebSocket transport migration from `fastwebsockets` to `sockudo_ws`
- Xdelta3 backend migration from `xdelta3` crate to `oxidelta`

## Delta Compression, Conflation, and Tag Filtering

### Delta Algorithms

- `fossil` (fast, low overhead)
- `xdelta3` (VCDIFF/RFC 3284) powered by `oxidelta`

### Core Delta Options (`config/config.json`)

```json
{
  "delta_compression": {
    "enabled": true,
    "algorithm": "Fossil",
    "full_message_interval": 10,
    "min_message_size": 100,
    "max_state_age_secs": 300,
    "max_channel_states_per_socket": 100,
    "max_conflation_states_per_channel": 100,
    "conflation_key_path": null,
    "cluster_coordination": true,
    "omit_delta_algorithm": true
  }
}
```

### Tag Filtering (`config/config.json`)

```json
{
  "tag_filtering": {
    "enabled": true,
    "enable_tags": false
  }
}
```

### Channel-Level Delta Overrides

Per-app/per-channel overrides are available through `channel_delta_compression` in app config.  
Supported modes include:

- Simple: `inherit`, `disabled`, `fossil`, `xdelta3`
- Full object settings: `enabled`, `algorithm`, `conflation_key`, `max_messages_per_key`, `max_conflation_keys`, `enable_tags`

### Delta + Conflation Behavior

- Delta state is tracked per socket and channel
- Conflation key extraction isolates independent state streams (for example per `asset`, `symbol`, token id, etc.)
- Cluster coordination can synchronize full-message intervals across nodes (Redis/NATS)
- Cache-sync and late subscriber behavior were improved for reduced warm-up overhead

### Detailed Docs

- `DELTA_COMPRESSION.md`
- `docs/DELTA_COMPRESSION_BANDWIDTH_OPTIMIZATION.md`
- `docs/DELTA_COMPRESSION_CLUSTER_COORDINATION.md`
- `docs/DELTA_COMPRESSION_HORIZONTAL_IMPLEMENTATION.md`
- `docs/DELTA_COMPRESSION_LATE_SUBSCRIBERS.md`
- `docs/TAG_FILTERING.md`
- `docs/TAG_FILTERING_QUICKSTART.md`

## WebSocket Engine (`sockudo_ws`)

`fastwebsockets` was fully replaced with native `sockudo_ws` APIs:

- Axum integration via `sockudo_ws::axum_integration::WebSocketUpgrade`
- Split reader/writer model via `sockudo_ws::axum_integration::WebSocket::{split}`
- Message-based send/receive model (`Text`, `Binary`, `Close`, control handling)
- Native websocket runtime tuning mapped into `websocket` config

### WebSocket Runtime Options (`config/config.json`)

```json
{
  "websocket": {
    "max_messages": 1000,
    "max_bytes": 1048576,
    "disconnect_on_buffer_full": false,
    "max_message_size": 67108864,
    "max_frame_size": 16777216,
    "write_buffer_size": 16384,
    "max_backpressure": 1048576,
    "auto_ping": true,
    "ping_interval": 30,
    "idle_timeout": 120,
    "compression": "disabled"
  }
}
```

### WebSocket Environment Variables

```bash
WEBSOCKET_MAX_MESSAGES=1000
WEBSOCKET_MAX_BYTES=1048576
WEBSOCKET_DISCONNECT_ON_BUFFER_FULL=false
WEBSOCKET_MAX_MESSAGE_SIZE=67108864
WEBSOCKET_MAX_FRAME_SIZE=16777216
WEBSOCKET_WRITE_BUFFER_SIZE=16384
WEBSOCKET_MAX_BACKPRESSURE=1048576
WEBSOCKET_AUTO_PING=true
WEBSOCKET_PING_INTERVAL=30
WEBSOCKET_IDLE_TIMEOUT=120
WEBSOCKET_COMPRESSION=disabled
```

Compression values for `WEBSOCKET_COMPRESSION` / `websocket.compression`:

- `disabled`
- `dedicated`
- `shared`
- `window256b`
- `window1kb`
- `window2kb`
- `window4kb`
- `window8kb`
- `window16kb`
- `window32kb`

## Quick Start

### Docker (Recommended)

```bash
# Clone and start with Docker Compose
git clone https://github.com/sockudo/sockudo.git
cd sockudo
make up

# Server runs on http://localhost:6001
# Metrics on http://localhost:9601/metrics
```

### From Source

```bash
# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build and run
git clone https://github.com/sockudo/sockudo.git
cd sockudo

# Fast local development build (default - no external dependencies)
cargo run --release

# Production build with all features
cargo run --release --features full
```

### Feature Flags

Sockudo supports optional compilation of backends to speed up local development:

```bash
# Local development (fastest - default)
cargo build                                    # ~30-50% faster compile

# With specific backends
cargo build --features "redis,postgres"        # Redis + PostgreSQL
cargo build --features "redis-cluster,mysql"   # Redis Cluster + MySQL

# Full production build
cargo build --release --features full          # All backends
```

**Available Features:**
- `local` (default) - In-memory implementations only
- `redis` - Redis adapter, cache, queue, rate limiter
- `redis-cluster` - Redis Cluster support
- `nats` - NATS adapter
- `mysql` / `postgres` / `dynamodb` - Database backends
- `sqs` / `lambda` - AWS integrations
- `full` - All features enabled

## Basic Usage

Connect using any Pusher-compatible client:

```javascript
import Pusher from 'pusher-js';

const pusher = new Pusher('app-key', {
    wsHost: 'localhost',
    wsPort: 6001,
    cluster: '',
    forceTLS: false
});

const channel = pusher.subscribe('my-channel');
channel.bind('my-event', (data) => {
    console.log('Received:', data);
});
```

## Configuration

### Environment Variables

```bash
# Basic settings
PORT=6001
HOST=0.0.0.0
DEBUG=false

# Default app credentials
SOCKUDO_DEFAULT_APP_ID=app-id
SOCKUDO_DEFAULT_APP_KEY=app-key
SOCKUDO_DEFAULT_APP_SECRET=app-secret

# Scaling drivers
ADAPTER_DRIVER=redis          # local, redis, redis-cluster, nats
CACHE_DRIVER=redis           # memory, redis, redis-cluster, none
QUEUE_DRIVER=redis           # memory, redis, redis-cluster, sqs, none
```

### Performance Tuning

```bash
# Connection limits
SOCKUDO_DEFAULT_APP_MAX_CONNECTIONS=100000
SOCKUDO_DEFAULT_APP_MAX_CLIENT_EVENTS_PER_SECOND=10000

# Cleanup performance (for handling mass disconnects)
CLEANUP_QUEUE_BUFFER_SIZE=50000
CLEANUP_BATCH_SIZE=25
CLEANUP_WORKER_THREADS=auto

# CPU scaling
ADAPTER_BUFFER_MULTIPLIER_PER_CPU=128
```

### Database Pooling

- Global defaults (apply to all SQL DBs unless overridden):

```bash
DATABASE_POOLING_ENABLED=true
DATABASE_POOL_MIN=2
DATABASE_POOL_MAX=10
# Legacy cap if pooling disabled
DATABASE_CONNECTION_POOL_SIZE=10
```

- Per‑database overrides (take precedence over global when set):

```bash
# MySQL
DATABASE_MYSQL_POOL_MIN=4
DATABASE_MYSQL_POOL_MAX=32

# PostgreSQL
DATABASE_POSTGRES_POOL_MIN=2
DATABASE_POSTGRES_POOL_MAX=16
```

- config/config.json keys:

```json
{
  "database_pooling": { "enabled": true, "min": 2, "max": 10 },
  "database": {
    "mysql": { "pool_min": 2, "pool_max": 10, "connection_pool_size": 10 },
    "postgres": { "pool_min": 2, "pool_max": 10, "connection_pool_size": 10 }
  }
}
```

Behavior:
- When `database_pooling.enabled` is true, managers use per‑DB `pool_min/pool_max` if provided; otherwise they fall back to the global `database_pooling.min/max`.
- When disabled, managers use `connection_pool_size` as the max connections for backward compatibility.

## Deployment Scenarios

| Scenario | CPU/RAM | Adapter | Cache | Queue | Max Connections |
|----------|---------|---------|-------|-------|-----------------|
| **Development** | 1vCPU/1GB | local | memory | memory | 1K |
| **Small Production** | 2vCPU/2GB | redis | redis | redis | 10K |
| **High Traffic** | 4vCPU/4GB+ | redis | redis | redis | 50K+ |
| **Multi-Region** | 8vCPU/8GB+ | redis-cluster | redis-cluster | redis-cluster | 100K+ |

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Load Balancer │────│   Sockudo Node  │────│   Redis Cluster │
│    (Nginx)      │    │    (Rust/Tokio) │    │  (State Store)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │              ┌─────────────────┐             │
         └──────────────│   Sockudo Node  │─────────────┘
                        │    (Rust/Tokio) │
                        └─────────────────┘
```

## Documentation

- **[Full Documentation](docs/)** - Complete setup and configuration guide
- **[Performance Tuning](docs/QUEUE_CONFIG.md)** - Optimize for your workload
- **[Docker Deployment](docker-compose.yml)** - Production-ready containers
- **[API Reference](docs/API.md)** - WebSocket and HTTP API details

## Testing

```bash
# Run all tests
make test

# Interactive WebSocket testing
cd test/interactive && npm install && npm start
# Open http://localhost:3000

# Load testing
make benchmark
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

Licensed under the [MIT License](LICENSE).

## Support

- **Issues**: [GitHub Issues](https://github.com/sockudo/sockudo/issues)
- **Discussions**: [GitHub Discussions](https://github.com/sockudo/sockudo/discussions)
- **Documentation**: [sockudo.io](https://sockudo.io)
- **Discord**: [Join our Discord](https://discord.gg/ySfNxfh2gZ)
- **X**: [@sockudorealtime](https://x.com/sockudorealtime)
- **Email**: [sockudorealtime](mailto:office@sockudo.io)
