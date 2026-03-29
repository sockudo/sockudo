<p align="center">
  <img src="images/logo.svg" alt="Sockudo Logo" width="200">
  <h1 align="center">Sockudo</h1>
  <p align="center">A high-performance, scalable WebSocket server for real-time applications, built in Rust.</p>
  <p align="center">
    <a href="https://github.com/Sockudo/sockudo"><img src="https://img.shields.io/github/stars/sockudo/sockudo?style=social" alt="Stars"></a>
    <a href="https://github.com/sockudo/sockudo/actions"><img src="https://img.shields.io/github/actions/workflow/status/sockudo/sockudo/ci.yml?branch=main" alt="Build Status"></a>
    <a href="LICENSE"><img src="https://img.shields.io/github/license/sockudo/sockudo" alt="License"></a>
  </p>
</p>

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=sockudo/sockudo&type=Date)](https://star-history.com/#Sockudo/sockudo&Date)

## Features

- **🚀 High Performance** - Handle 100K+ concurrent connections
- **🔄 Dual Protocol** - V1 for Pusher compatibility, V2 for Sockudo-native with serial numbers, message IDs, and connection recovery
- **🔗 Connection Recovery** - Ably-style serial-based message replay for exactly-once delivery (V2)
- **🆔 Message Idempotency** - Automatic `message_id` on every broadcast, HTTP-level `idempotency_key` deduplication
- **🏗️ Scalable Architecture** - Redis, Redis Cluster, NATS adapters
- **🛡️ Production Ready** - Rate limiting, SSL/TLS, metrics
- **⚡ Async Cleanup** - Non-blocking disconnect handling
- **📊 Real-time Metrics** - Prometheus integration
- **🧠 Delta Compression + Conflation** - Fossil and Xdelta3 (VCDIFF) with per-channel controls
- **🏷️ Tag Filtering** - High-performance server-side filtering with optional tag emission controls
- **🌐 Native WebSocket Engine** - `sockudo_ws` with advanced runtime tuning
- **📦 Official SDKs** - Client and server SDKs for all major platforms

## What's New in v4

Sockudo v4 introduces **dual protocol versioning**, making Sockudo its own platform while retaining full Pusher compatibility:

| | Protocol V1 (default) | Protocol V2 |
|---|---|---|
| Event prefix | `pusher:` / `pusher_internal:` | `sockudo:` / `sockudo_internal:` |
| `serial` | Never sent | Always on every message |
| `message_id` | Never sent | Always on every broadcast |
| Connection recovery | Not available | Always available |
| Delta compression | Not available | Native |
| Tag filtering | Not available | Native |
| Compatible SDKs | Official Pusher SDKs | Sockudo client SDKs |

Additional v4 changes:
- **TOML configuration** (preferred over JSON) - `config/config.toml`
- **Rebranded SDKs** - All server and client SDKs renamed from `pusher-*` to `sockudo-*`
- **HTTP idempotency** - Atomic `idempotency_key` deduplication via `SET NX` (no race conditions)
- **Replay buffer** - Per-channel message buffer with configurable TTL and max size

## Official Client SDKs

| Client | Package | Default Protocol |
|---|---|---|
| JavaScript / TypeScript | `@sockudo/client` | V2 |
| Swift | `SockudoSwift` (SPM) | V2 |
| Kotlin | `io.sockudo:sockudo-kotlin` | V2 |
| Flutter / Dart | `sockudo_flutter` | V2 |

All Sockudo client SDKs default to protocol V2 (`sockudo:` prefix, serial tracking, connection recovery). Set `protocolVersion: 1` to use Pusher-compatible mode. For V1 you can also use the official Pusher SDKs directly.

## Official Server SDKs

| Language | Package |
|---|---|
| Node.js | `sockudo` |
| PHP | `sockudo/sockudo-php-server` |
| Ruby | `sockudo` gem |
| Go | `github.com/sockudo/sockudo-http-go` |
| Rust | `sockudo-http` |
| Java | `io.sockudo:sockudo-http-java` |
| .NET | `SockudoServer` |
| Swift | `Sockudo` (SPM) |

All server SDKs support `idempotency_key` for safe publish retries.

## Quick Start

### Docker (Recommended)

```bash
git clone https://github.com/sockudo/sockudo.git
cd sockudo
make up

# Server runs on http://localhost:6001
# Metrics on http://localhost:9601/metrics
```

### Kubernetes (Helm)

```bash
helm install sockudo ./charts/sockudo

# Production with Redis adapter and autoscaling
helm install sockudo ./charts/sockudo \
  --set config.adapterDriver=redis \
  --set redis.host=redis-master \
  --set redis.existingSecret=my-redis-secret \
  --set autoscaling.enabled=true \
  --set pdb.enabled=true \
  --set ingress.enabled=true \
  --set serviceMonitor.enabled=true
```

See [`charts/sockudo/values.yaml`](charts/sockudo/values.yaml) for all configurable options.

### From Source

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

git clone https://github.com/sockudo/sockudo.git
cd sockudo

# Fast local development build (default - no external dependencies)
cargo run --release

# Production build with all features
cargo run --release --features full
```

### Feature Flags

```bash
# Local development (fastest - default)
cargo build                                    # ~30-50% faster compile

# With specific backends
cargo build --features "redis,postgres"        # Redis + PostgreSQL
cargo build --features "redis-cluster,mysql"   # Redis Cluster + MySQL
cargo build --features "redis,surrealdb"       # Redis + SurrealDB 3
cargo build --features "rabbitmq,postgres"     # RabbitMQ + PostgreSQL
cargo build --features "google-pubsub,mysql"   # Google Pub/Sub + MySQL
cargo build --features "kafka,postgres"        # Kafka + PostgreSQL

# Full production build
cargo build --release --features full          # All backends
```

**Available Features:**
- `local` (default) - In-memory implementations only
- `v2` (default) - All Sockudo V2 features (delta, tag-filtering, recovery)
- `delta` - Delta compression (V2)
- `tag-filtering` - Server-side tag filtering (V2)
- `recovery` - Connection recovery with serial numbers and message IDs (V2)
- `redis` - Redis adapter, cache, queue, rate limiter
- `redis-cluster` - Redis Cluster support
- `nats` - NATS adapter
- `rabbitmq` - RabbitMQ adapter
- `google-pubsub` - Google Cloud Pub/Sub adapter
- `kafka` - Kafka adapter
- `mysql` / `postgres` / `dynamodb` / `surrealdb` / `scylladb` - App manager backends
- `sqs` / `lambda` - AWS integrations
- `full` - All features enabled

```bash
# Pure Pusher-only server (no V2 features, smallest binary)
cargo build --no-default-features

# V2 with only delta compression
cargo build --no-default-features --features delta

# V2 with recovery only (serial + message_id + replay buffer)
cargo build --no-default-features --features recovery
```

## Basic Usage

### Protocol V2 (Sockudo-native, default for Sockudo SDKs)

```javascript
import { SockudoClient } from '@sockudo/client';

const client = new SockudoClient('app-key', {
    wsHost: 'localhost',
    wsPort: 6001,
    forceTLS: false,
    // protocolVersion: 2 is the default
});

const channel = client.subscribe('my-channel');
channel.bind('my-event', (data) => {
    console.log('Received:', data);
});
```

```swift
import SockudoSwift

let client = try SockudoClient(
    "app-key",
    options: .init(
        cluster: "local",
        forceTLS: false,
        enabledTransports: [.ws],
        wsHost: "127.0.0.1",
        wsPort: 6001,
        wssPort: 6001
    )
)

let channel = client.subscribe("my-channel")
channel.bind("my-event") { data, _ in
    print(data ?? "")
}
client.connect()
```

### Protocol V1 (Pusher-compatible)

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

Sockudo uses TOML configuration (preferred) with JSON as a fallback. The server tries `config/config.toml` first, then `config/config.json`.

### Core Configuration (`config/config.toml`)

```toml
port = 6001
host = "0.0.0.0"
debug = false

[app_manager]
driver = "memory"

[app_manager.array]
[[app_manager.array.apps]]
id = "app-id"
key = "app-key"
secret = "app-secret"
enabled = true
[app_manager.array.apps.policy.limits]
max_connections = 100000
max_client_events_per_second = 1000

[app_manager.array.apps.policy.features]
enable_client_messages = true

[app_manager.array.apps.policy.channels]
allowed_origins = ["*"]

[[app_manager.array.apps.policy.channels.channel_namespaces]]
name = "ticker"
channel_name_pattern = "^ticker:[A-Za-z0-9._-]+$"
max_channel_name_length = 200

[[app_manager.array.apps.policy.channels.channel_namespaces]]
name = "chat"
channel_name_pattern = "^chat:[A-Za-z0-9._-]+$"
max_channel_name_length = 200
allow_user_limited_channels = true

[adapter]
driver = "local"

[cache]
driver = "memory"

[queue]
driver = "memory"
```

Per-app app-manager entries are where V2 channel semantics live, now grouped under `app.policy`: `policy.channels.allowed_origins`, `policy.channels.channel_namespaces`, app-level `policy.idempotency`, `policy.connection_recovery`, and `policy.channels.channel_delta_compression`.

For existing MySQL/PostgreSQL app tables, use the explicit migration scripts under [`migrations/mysql`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/migrations/mysql) and [`migrations/postgresql`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/migrations/postgresql) to backfill `policy` and then drop legacy columns during a maintenance window.

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
ADAPTER_DRIVER=redis          # local, redis, redis-cluster, nats, rabbitmq, google-pubsub, kafka
CACHE_DRIVER=redis           # memory, redis, redis-cluster, none
QUEUE_DRIVER=redis           # memory, redis, redis-cluster, sqs, none
APP_MANAGER_DRIVER=surrealdb # memory, mysql, postgres, dynamodb, surrealdb, scylladb

RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/%2f
GOOGLE_PUBSUB_PROJECT_ID=my-gcp-project
KAFKA_BROKERS=127.0.0.1:9092

DATABASE_SURREALDB_URL=ws://127.0.0.1:8000
DATABASE_SURREALDB_NAMESPACE=sockudo
DATABASE_SURREALDB_DATABASE=sockudo
DATABASE_SURREALDB_USERNAME=root
DATABASE_SURREALDB_PASSWORD=root
DATABASE_SURREALDB_TABLE_NAME=applications

# V2 feature gates
EPHEMERAL_ENABLED=true
ECHO_CONTROL_ENABLED=true
ECHO_CONTROL_DEFAULT_ECHO_MESSAGES=true
EVENT_NAME_FILTERING_ENABLED=true
```

### Delta Compression (`config/config.toml`)

```toml
[delta_compression]
enabled = true
algorithm = "Fossil"
full_message_interval = 10
min_message_size = 100
max_state_age_secs = 300
max_channel_states_per_socket = 100
max_conflation_states_per_channel = 100
cluster_coordination = true
omit_delta_algorithm = true
```

### Tag Filtering (`config/config.toml`)

```toml
[tag_filtering]
enabled = true
enable_tags = false
```

### V2 Message Features (`config/config.toml`)

```toml
[ephemeral]
enabled = true

[echo_control]
enabled = true
default_echo_messages = true

[event_name_filtering]
enabled = true
max_events_per_filter = 50
max_event_name_length = 200
```

### WebSocket Runtime (`config/config.toml`)

```toml
[websocket]
max_messages = 1000
max_bytes = 1048576
disconnect_on_buffer_full = false
max_message_size = 67108864
max_frame_size = 16777216
write_buffer_size = 16384
max_backpressure = 1048576
auto_ping = true
ping_interval = 30
idle_timeout = 120
compression = "disabled"
```

### Performance Tuning

```bash
SOCKUDO_DEFAULT_APP_MAX_CONNECTIONS=100000
SOCKUDO_DEFAULT_APP_MAX_CLIENT_EVENTS_PER_SECOND=10000

CLEANUP_QUEUE_BUFFER_SIZE=50000
CLEANUP_BATCH_SIZE=25
CLEANUP_WORKER_THREADS=auto

ADAPTER_BUFFER_MULTIPLIER_PER_CPU=128
```

### Database Pooling

```toml
[database_pooling]
enabled = true
min = 2
max = 10

[database.mysql]
pool_min = 4
pool_max = 32

[database.postgres]
pool_min = 2
pool_max = 16
```

## Deployment Scenarios

| Scenario | CPU/RAM | Adapter | Cache | Queue | Max Connections |
|----------|---------|---------|-------|-------|-----------------|
| **Development** | 1vCPU/1GB | local | memory | memory | 1K |
| **Small Production** | 2vCPU/2GB | redis | redis | redis | 10K |
| **High Traffic** | 4vCPU/4GB+ | redis | redis | redis | 50K+ |
| **Multi-Region** | 8vCPU/8GB+ | redis-cluster | redis-cluster | redis-cluster | 100K+ |

All scenarios can be deployed via Docker Compose or the included [Helm chart](charts/sockudo/) on Kubernetes with built-in HPA autoscaling.

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
- **[Client Overview](docs/content/3.client/1.overview.md)** - Official SDKs and runtime targets
- **[Docker Deployment](docker-compose.yml)** - Production-ready containers
- **[Helm Charts](charts/sockudo/)** - Kubernetes deployment with HPA, PDB, ServiceMonitor

## Testing

```bash
# Run all server tests
make test

# Interactive WebSocket testing
cd test/interactive && npm install && npm start
# Open http://localhost:3000

# Load testing
make benchmark
```

Client and server SDK tests are run from their respective repositories. See each SDK's README for instructions.

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
