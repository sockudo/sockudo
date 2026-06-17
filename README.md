<p align="center">
  <img src="images/logo.svg" alt="Sockudo Logo" width="180">
</p>

<h1 align="center">Sockudo</h1>

<p align="center">
  A self-hosted realtime platform: Pusher-compatible at the edge, Sockudo-native where you need
  recovery, history, mutable messages, push, AI Transport, and first-party SDKs.
</p>

<p align="center">
  <a href="https://github.com/sockudo/sockudo/actions"><img src="https://img.shields.io/github/actions/workflow/status/sockudo/sockudo/ci.yml?branch=main" alt="Build status"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/sockudo/sockudo" alt="License"></a>
  <a href="https://github.com/sockudo/sockudo"><img src="https://img.shields.io/github/stars/sockudo/sockudo?style=social" alt="GitHub stars"></a>
</p>

## What Sockudo Is

Sockudo is a high-performance Rust realtime server for WebSocket and HTTP publish workloads. It
keeps strict Protocol V1 compatibility with the Pusher protocol, then adds Protocol V2 features for
teams that need stronger delivery semantics and product-level control.

This repository is now the Sockudo monorepo. It contains the server, Rust workspace crates,
dashboard, deployment assets, docs, official realtime client SDKs, HTTP server SDKs, and the
TypeScript AI Transport SDK.

## Highlights

- Pusher-compatible WebSocket and HTTP APIs for Protocol V1 clients.
- Protocol V2 with `sockudo:` events, serials, message IDs, recovery, rewind, tags, deltas, and
  mutable message events.
- Horizontal fanout through Redis, Redis Cluster, NATS, RabbitMQ, Google Pub/Sub, Kafka, Pulsar, and
  Apache Iggy.
- Durable history, hot replay buffers, two-tier recovery, `until_attach` history reads, and
  degraded/reset-required continuity state.
- Versioned mutable messages with create, update, delete, append, summary, latest-visible reads, and
  version history.
- Presence membership plus retained presence-history transitions and snapshots.
- Message annotations for reactions, receipts, moderation, summary projection, and raw annotation
  streams.
- Push notification pipeline for FCM, APNs, Web Push, HMS, and WNS.
- Prometheus metrics, webhook delivery, rate limiting, app managers, TLS, Docker Compose, and Helm.
- Optional AI Transport built on the same versioned-message, history, recovery, push, and presence
  primitives instead of a parallel streaming path.

## Monorepo Layout

```text
crates/              Rust server crates and libraries
benches/ai/          Permanent Criterion benchmarks for AI hot paths
client-sdks/         Realtime client SDKs and AI Transport SDK
server-sdks/         HTTP/server SDKs for backend publishers
dashboard/           Operator API and Vue UI
docs/                Documentation site content
config/              Local configuration examples
ops/                 Migrations and operational assets
charts/              Helm chart
tests/               Conformance, dashboard, load, and binary fixtures
tools/               Probes, chaos tooling, and helper utilities
```

The SDK directories preserve their original repository names so package metadata, release scripts,
and issue references remain recognizable.

## Protocols

| Capability | Protocol V1 | Protocol V2 |
| --- | --- | --- |
| Event prefixes | `pusher:` / `pusher_internal:` | `sockudo:` / `sockudo_internal:` |
| Pusher compatibility | Strict compatibility target | Sockudo-native |
| `serial` and `message_id` | Stripped from delivery | Native on relevant frames |
| Recovery and rewind | Not available | Hot replay plus durable history |
| Mutable messages | Not available | `sockudo:message.*` |
| Annotations | Not available | Native |
| Tag filtering and deltas | Not available | Native |
| AI Transport | Not available | Optional feature and runtime config |

Use Protocol V1 when you need drop-in Pusher behavior. Use Protocol V2 when clients and server SDKs
can rely on Sockudo-native recovery, history, mutation, annotation, filtering, and AI Transport
semantics. Protocol defaults vary by SDK; check each package README before assuming V2 is enabled.

## Quick Start

### Docker Compose

```bash
git clone https://github.com/sockudo/sockudo.git
cd sockudo
make up
```

Default local services:

| Service | URL |
| --- | --- |
| Sockudo server | `http://localhost:6001` |
| Prometheus metrics | `http://localhost:9601/metrics` |
| Dashboard UI | `http://localhost:5174` |
| Dashboard API | `http://localhost:3460` |

### From Source

```bash
rustup toolchain install stable
git clone https://github.com/sockudo/sockudo.git
cd sockudo

cargo run -p sockudo
```

Build with a production-oriented feature set:

```bash
cargo build -p sockudo --release --features "v2,redis,postgres,push"
```

Build everything the server can expose:

```bash
cargo build -p sockudo --release --features full
```

### Kubernetes

```bash
helm install sockudo ./charts/sockudo
```

Example production shape with Redis, ingress, autoscaling, and monitoring:

```bash
helm install sockudo ./charts/sockudo \
  --set config.adapterDriver=redis \
  --set redis.host=redis-master \
  --set redis.existingSecret=my-redis-secret \
  --set defaultApp.existingSecret=my-default-app-secret \
  --set autoscaling.enabled=true \
  --set pdb.enabled=true \
  --set ingress.enabled=true \
  --set serviceMonitor.enabled=true
```

When `defaultApp.existingSecret` is set, the Secret must contain `default-app-secret` and can also
include `default-app-id` and `default-app-key`.

## Client SDKs

Realtime clients live under [client-sdks/](client-sdks/).

Until the package-manager publishing workflows are fully enabled, install SDKs from their GitHub
monorepo paths. The old per-SDK repositories should be treated as archived/legacy mirrors; this
repository is the source of truth. The package names below are the intended published names, but the
install examples in the SDK docs use local monorepo paths for now.

| Runtime | Directory | Package |
| --- | --- | --- |
| JavaScript / TypeScript | [`client-sdks/sockudo-js`](client-sdks/sockudo-js) | `@sockudo/client` |
| AI Transport TypeScript | [`client-sdks/sockudo-ai-transport-js`](client-sdks/sockudo-ai-transport-js) | `@sockudo/ai-transport` |
| Swift / Apple platforms | [`client-sdks/sockudo-swift`](client-sdks/sockudo-swift) | `SockudoSwift` |
| Kotlin / JVM / Android | [`client-sdks/sockudo-kotlin`](client-sdks/sockudo-kotlin) | `io.sockudo:sockudo-kotlin` |
| Flutter / Dart | [`client-sdks/sockudo-flutter`](client-sdks/sockudo-flutter) | `sockudo_flutter` |
| .NET | [`client-sdks/sockudo-dotnet`](client-sdks/sockudo-dotnet) | `Sockudo.Client` |
| Python | [`client-sdks/sockudo-python`](client-sdks/sockudo-python) | `sockudo-python` |

JavaScript example:

```ts
import Sockudo from "@sockudo/client";

const sockudo = new Sockudo("app-key", {
  wsHost: "127.0.0.1",
  wsPort: 6001,
  wssPort: 6001,
  forceTLS: false,
  enabledTransports: ["ws"],
  protocolVersion: 2,
});

const channel = sockudo.subscribe("public-updates");
channel.bind("price-updated", (payload) => {
  console.log(payload);
});
```

AI Transport example:

```ts
import { TransportProvider, useView } from "@sockudo/ai-transport/react";
```

See [`client-sdks/sockudo-ai-transport-js/README.md`](client-sdks/sockudo-ai-transport-js/README.md)
for React, Vue, Svelte, Vercel AI SDK, and direct provider helpers.

## Server SDKs

HTTP/server SDKs live under [server-sdks/](server-sdks/). They publish events, sign private and
presence channel auth, authenticate users, validate webhooks, query state, and expose
Sockudo-native APIs such as idempotent publishing, history, mutable messages, annotations, and push
where implemented.

Until registry publishing is fixed, install these SDKs from local paths in this monorepo. The old
per-SDK repositories should be treated as archived/legacy mirrors. Do not assume npm, PyPI, NuGet,
Maven Central, Packagist, RubyGems, crates.io, or pub.dev packages are available yet.

| Language | Directory | Package |
| --- | --- | --- |
| Node.js | [`server-sdks/sockudo-http-node`](server-sdks/sockudo-http-node) | `sockudo` |
| Python | [`server-sdks/sockudo-http-python`](server-sdks/sockudo-http-python) | `sockudo-http-python` |
| PHP | [`server-sdks/sockudo-http-php`](server-sdks/sockudo-http-php) | `sockudo/sockudo-php-server` |
| Ruby | [`server-sdks/sockudo-http-ruby`](server-sdks/sockudo-http-ruby) | `sockudo` |
| Go | [`server-sdks/sockudo-http-go`](server-sdks/sockudo-http-go) | `github.com/sockudo/sockudo-http-go/v5` |
| Rust | [`server-sdks/sockudo-http-rust`](server-sdks/sockudo-http-rust) | `sockudo-http` |
| Java | [`server-sdks/sockudo-http-java`](server-sdks/sockudo-http-java) | `io.sockudo:sockudo-http-java` |
| .NET | [`server-sdks/sockudo-http-dotnet`](server-sdks/sockudo-http-dotnet) | `SockudoServer` |
| Swift | [`server-sdks/sockudo-http-swift`](server-sdks/sockudo-http-swift) | `Sockudo` |

Node.js example:

```ts
import { Sockudo } from "sockudo";

const sockudo = new Sockudo({
  appId: "app-id",
  key: "app-key",
  secret: "app-secret",
  host: "127.0.0.1",
  port: 6001,
  useTLS: false,
});

await sockudo.trigger("orders", "order.created", { id: "ord_123" });
```

Python example:

```python
from sockudo_http import Config, Sockudo

sockudo = Sockudo(
    Config(
        app_id="app-id",
        key="app-key",
        secret="app-secret",
        host="127.0.0.1",
        port=6001,
    )
)

sockudo.trigger("orders", "order.created", {"id": "ord_123"})
```

## Server Workspace

The Rust workspace is split by responsibility:

| Crate | Responsibility |
| --- | --- |
| `sockudo-protocol` | Protocol message types, V1/V2 prefixes, extras, wire payloads |
| `sockudo-filter` | Tag filtering expressions and matching |
| `sockudo-core` | Shared traits, config, auth, errors, history, version store, annotations |
| `sockudo-app` | App-manager backends |
| `sockudo-cache` | Cache backends |
| `sockudo-queue` | Queue backends |
| `sockudo-rate-limiter` | Rate limiting and middleware |
| `sockudo-metrics` | Prometheus metrics |
| `sockudo-webhook` | Webhook delivery |
| `sockudo-delta` | Delta compression |
| `sockudo-push` | Push domain, storage, queues, providers, feedback, scheduler |
| `sockudo-ai-transport` | AI Transport validation, rollup, and conformance helpers |
| `sockudo-adapter` | Connections, presence, fanout, replay, recovery |
| `sockudo-server` | Binary, HTTP/WS routes, bootstrap, durable stores, push API |

The Rust SDK at `server-sdks/sockudo-http-rust` is intentionally excluded from the root Cargo
workspace so it can keep its own package and release lifecycle.

## Feature Flags

Common server features:

| Feature | Purpose |
| --- | --- |
| `local` | In-memory development defaults |
| `v2` | Protocol V2 bundle: deltas, tag filtering, recovery |
| `delta` | Delta compression |
| `tag-filtering` | Server-side tag filtering |
| `recovery` | Serial and message-id recovery |
| `ai-transport` | AI Transport validation and rollup surfaces |
| `push` | Push notification runtime and HTTP APIs |
| `redis`, `redis-cluster` | Redis-backed adapter/cache/queue/rate limit paths |
| `nats`, `pulsar`, `rabbitmq`, `google-pubsub`, `kafka`, `iggy` | Horizontal adapter and queue integrations |
| `mysql`, `postgres`, `dynamodb`, `surrealdb`, `scylladb` | App, history, version-store, and push storage backends |
| `sqs`, `sns`, `lambda` | AWS queue, notification, and webhook integrations |
| `full` | All production integrations wired by the server |

Examples:

```bash
cargo build
cargo build --no-default-features
cargo build --features "redis,postgres"
cargo build --features "v2,ai-transport,redis,postgres,push"
cargo build --release --features full
```

## Configuration

Sockudo prefers TOML and falls back to JSON. The default local config is
[`config/config.toml`](config/config.toml).

Minimal local shape:

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

[adapter]
driver = "local"

[cache]
driver = "memory"

[queue]
driver = "memory"
```

Major runtime sections:

| Section | Purpose |
| --- | --- |
| `[app_manager]` | App credentials, policies, origin rules, namespaces |
| `[adapter]` | Realtime fanout driver |
| `[cache]` | Shared cache for auth, recovery, and cross-node state |
| `[queue]` | Background queue driver |
| `[history]` | Durable channel history |
| `[versioned_messages]` | Mutable message storage and versioning |
| `[presence_history]` | Retained join/leave transitions |
| `[annotations]` | Annotation publish, delete, summaries, raw streams |
| `[push]` | Push providers, quotas, retries, feedback, scheduler |
| `[ai_transport]` | AI Transport validation and channel scoping |

Reference docs:

- [Configuration](docs/content/docs/reference/configuration.mdx)
- [Environment variables](docs/content/docs/reference/environment-variables.mdx)
- [HTTP endpoints](docs/content/docs/reference/http-endpoints.mdx)

## AI Transport

AI Transport is default-off and Protocol V2-only. Enable it with both Cargo and runtime config:

```bash
cargo build -p sockudo --features "v2,ai-transport,redis,postgres,push"
```

```toml
[versioned_messages]
enabled = true

[history]
enabled = true

[ai_transport]
enabled = true

[[ai_transport.channels]]
prefix = "ai:"
```

Operational rule: append rollup only changes WebSocket egress. Persistence, version storage,
history, recovery, push, and webhooks still see every original mutation.

Useful docs:

- [AI Transport overview](docs/content/docs/server/ai-transport-overview.mdx)
- [AI Transport conventions](docs/content/docs/server/ai-transport-conventions.mdx)
- [Token streaming rollup](docs/content/docs/server/token-streaming-rollup.mdx)
- [Production checklist](docs/content/docs/server/ai-transport-production-checklist.mdx)

## Development

Server checks:

```bash
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Focused server checks:

```bash
cargo test -p sockudo-core
cargo test -p sockudo-adapter
cargo test -p sockudo-ai-transport
cargo test -p sockudo-push
```

AI Transport checks:

```bash
scripts/ai-transport-bench-guard.sh
AIT_CONFORMANCE_OFFLINE=1 scripts/ai-conformance-node.sh
```

Docs checks:

```bash
cd docs
npm run types:check
npm run build
```

SDK checks are package-local. Start from each SDK README and use the native package manager:
`bun`, `pnpm`, `npm`, `pytest`, `cargo`, `go test`, `gradle`, `swift test`, `dotnet test`,
`composer`, or `bundle`.

## Deployment Profiles

| Profile | Adapter | Cache | Queue | App store | Typical use |
| --- | --- | --- | --- | --- | --- |
| Local development | `local` | `memory` | `memory` | `memory` | Fast single-node iteration |
| Small production | `redis` | `redis` | `redis` | `postgres` or `mysql` | Simple shared state |
| High traffic | `redis-cluster` | `redis-cluster` | external queue | SQL or DynamoDB | Larger fanout and retention |
| AI Transport | horizontal adapter | shared cache | shared queue | shared history and version store | Streaming recovery and rollup |

For horizontal adapters, do not assume memory history, memory version stores, or process-local cache
are safe. AI Transport startup intentionally rejects invalid horizontal/shared-state combinations.

## Documentation

- [Docs site source](docs/)
- [Getting started](docs/content/docs/getting-started/overview.mdx)
- [Realtime clients](docs/content/docs/clients/index.mdx)
- [Server SDKs](docs/content/docs/server-sdks/index.mdx)
- [Server configuration](docs/content/docs/server/configuration.mdx)
- [Scaling](docs/content/docs/server/scaling.mdx)
- [Security](docs/content/docs/server/security.mdx)
- [Observability](docs/content/docs/server/observability.mdx)

## Sponsors

<p align="center">
  <a href="https://swag.live/">
    <img src="https://swag.live/static/img/favicon.2991446b.png" alt="SWAG" width="84">
  </a>
  <a href="https://livecaller.io/">
    <img src="https://cdn.prod.website-files.com/69159207dbb70153a0260551/69159207dbb70153a02605cc_logo.svg" alt="LiveCaller" width="170">
  </a>
</p>

## Contributing

1. Fork the repository.
2. Create a focused branch.
3. Make the smallest coherent change.
4. Run the relevant checks.
5. Open a pull request with the behavior change, verification, and any follow-up risk.

For coding-agent work, read [AGENTS.md](AGENTS.md). For a compact technical map of the repo, read
[CLAUDE.md](CLAUDE.md).

## License

Sockudo is licensed under the [MIT License](LICENSE). Individual SDK packages may also carry their
own package metadata and notices; check the SDK directory before publishing.

## Support

- [GitHub Issues](https://github.com/sockudo/sockudo/issues)
- [GitHub Discussions](https://github.com/sockudo/sockudo/discussions)
- [Documentation](https://sockudo.io)
- [Discord](https://discord.gg/ySfNxfh2gZ)
- [X / Twitter](https://x.com/sockudorealtime)
- [Email](mailto:office@sockudo.io)
