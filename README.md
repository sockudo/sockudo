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
- Optional `ably-compat` facade exposing a reduced Ably AI Transport compatibility surface for
  tested AI Transport workloads.

## Monorepo Layout

```text
crates/              Rust server crates and libraries
benches/ai/          Permanent Criterion benchmarks for AI hot paths
benches/presence/    Presence performance budgets
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

SDK CI and publishing are managed from root workflows. See the
[2026 SDK publishing runbook](docs/sdk-publishing-2026.md) before releasing packages or changing
registry setup. Registry-native packages publish through protected workflows where supported.
SwiftPM packages are mirrored to standalone package repositories.

| Runtime | Package |
| --- | --- |
| JavaScript / TypeScript | `@sockudo/client` |
| AI Transport TypeScript | `@sockudo/ai-transport` |
| Swift / Apple platforms | `SockudoSwift` via SwiftPM (`https://github.com/sockudo/sockudo-swift`) |
| Kotlin / JVM / Android | `io.sockudo:sockudo-kotlin` |
| Flutter / Dart | `sockudo_flutter` |
| .NET | `Sockudo.Client` |
| Python | `sockudo-python` |

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

SDK CI and publishing are managed from root workflows. See the
[2026 SDK publishing runbook](docs/sdk-publishing-2026.md) before releasing packages or changing
registry setup. Registry-native packages publish through protected workflows where supported. Go
modules and SwiftPM publish from this monorepo with package-manager-native tags and manifests. The
PHP SDK is mirrored to `sockudo/sockudo-http-php` for Packagist.

| Language | Package |
| --- | --- |
| Node.js | `sockudo` |
| Python | `sockudo-http-python` |
| PHP | `sockudo/sockudo-php-server` |
| Ruby | `sockudo` |
| Go | `github.com/sockudo/sockudo/server-sdks/sockudo-http-go/v2` |
| Rust | `sockudo-http` |
| Java | `io.sockudo:sockudo-http-java` |
| .NET | `SockudoServer` |
| Swift | `Sockudo` via SwiftPM (`https://github.com/sockudo/sockudo-http-swift`) |

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
| `ably-compat` | Opt-in reduced Ably Realtime/REST facade for AI Transport compatibility; implies `ai-transport` |
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
- [Ably AI Transport compatibility](docs/content/docs/server/ably-ai-transport-compatibility.mdx)
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

Presence correctness and performance gate:

```bash
scripts/presence-bench-guard.sh
```

The gate compares authoritative enter/leave against the former facade-map
cycle in the same Criterion run. It fails if either the single-worker or
eight-worker disjoint-client confidence interval is slower.

AI Transport checks:

```bash
scripts/ai-transport-bench-guard.sh
AIT_CONFORMANCE_OFFLINE=1 scripts/ai-conformance-node.sh
make ably-compat-test
make ably-ai-transport-test
make ably-ai-demo
```

The Ably targets exercise a reduced Ably Pub/Sub subset for AI Transport using local Sockudo as the
endpoint. Start Sockudo with Cargo feature `ably-compat` plus the normal runtime `[ai_transport]`
config before running them. They do not imply full Ably platform compatibility; see the
compatibility scorecard in [`docs/ably-compat/`](docs/ably-compat/).

Docs checks:

```bash
cd docs
npm run types:check
npm run build
```

SDK checks are covered by `SDK CI` and are also package-local when iterating. Start from the
[2026 SDK publishing runbook](docs/sdk-publishing-2026.md), then use the native package manager:
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
