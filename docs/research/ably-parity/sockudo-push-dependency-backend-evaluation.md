# Sockudo Push Dependency And Backend Evaluation

Date: 2026-04-30
Status: ADR candidate for Sockudo 4.5 push implementation

This document evaluates provider crates and existing Sockudo backends before adding
`crates/sockudo-push`. It intentionally does not add dependencies. The dependency
decisions below approve implementation directions only; final dependency pins must
be introduced with feature-gated code and focused tests.

## Scope And Release Fit

Sockudo 4.5 push should live primarily in `crates/sockudo-push`, with a
`sockudo-server` feature named `push` and provider subfeatures:

- `push-fcm`
- `push-apns`
- `push-webpush`
- `push-hms`
- `push-wns`

Storage and queue features should reuse existing server features where feasible:
`postgres`, `mysql`, `dynamodb`, `surrealdb`, `scylladb`, `redis`,
`redis-cluster`, `nats`, `pulsar`, `rabbitmq`, `google-pubsub`, `kafka`,
`iggy`, `sqs`, and `sns`. A separate `monolith` feature may run accept, plan,
schedule, and dispatch workers in one process for development and small
deployments.

All existing Sockudo backends should be supported for push where feasible, but
not all should be recommended for hyperscale. The docs must distinguish
"supported" from "recommended for 100M fanout / 1B device registry" explicitly.

## Local Repository Findings

- Workspace version is `4.4.0`; `crates/sockudo-server` is the binary crate and
  the current feature composition point.
- Server defaults are `local` and `v2`; `full` enables all existing queue and
  storage backends plus V2 features.
- Queue feature names already match the release target: memory/local, Redis,
  Redis Cluster, NATS, Pulsar, RabbitMQ, Google Pub/Sub, Kafka, Iggy, SQS, SNS.
- Storage feature names already match the release target: memory/local,
  PostgreSQL, MySQL, DynamoDB, SurrealDB, ScyllaDB.
- `HistoryBackend` and `VersionStoreDriver` already model memory plus the five
  durable database families. `create_history_store` fails closed with a
  configuration error when a selected backend was not compiled in.
- SQL migrations are checked in for PostgreSQL and MySQL. DynamoDB, SurrealDB,
  and ScyllaDB are runtime-provisioned today, so push schema work must either
  add migrations/provisioning docs or explicitly follow runtime provisioning.
- Metrics are Prometheus-only today. Push metrics should extend the existing
  `sockudo_` prefix convention and use labels for `app_id`, provider, backend,
  queue/stage, status, and error class.

Primary local files inspected:

- `Cargo.toml`
- `crates/sockudo-server/Cargo.toml`
- `crates/sockudo-core/src/options.rs`
- `crates/sockudo-core/src/app.rs`
- `crates/sockudo-queue`
- `crates/sockudo-metrics`
- `crates/sockudo-server/src/history.rs`
- `ops/migrations`

## Provider Protocol Requirements

FCM HTTP v1 requires OAuth2 bearer tokens and POST requests to
`https://fcm.googleapis.com/v1/projects/{project}/messages:send`. The official
FCM docs show service account JSON / Workload Identity auth, HTTP v1 send
requests, and a JSON success response containing the message name. FCM error
handling must preserve provider codes such as `UNREGISTERED`, quota/rate errors,
and retryable 429/5xx responses. FCM scale guidance requires respecting
`retry-after`, using backoff with jitter, and aborting non-retryable 400/401/403/404
classes. Sources: <https://firebase.google.com/docs/cloud-messaging/send/v1-api>,
<https://firebase.google.com/docs/cloud-messaging/error-codes>,
<https://firebase.google.com/docs/cloud-messaging/scale-fcm>,
<https://firebase.google.com/docs/cloud-messaging/manage-tokens>.

APNs requires HTTP/2 provider connections, certificate or token authentication,
provider response status/reason parsing, 410 token cleanup, rate-limit handling,
and connection lifecycle handling including APNs server shutdown / GOAWAY
semantics. Sources: <https://developer.apple.com/documentation/usernotifications/establishing-a-connection-to-apns>,
<https://developer.apple.com/documentation/usernotifications/sending-notification-requests-to-apns>,
<https://developer.apple.com/documentation/usernotifications/handling-notification-responses-from-apns>.

Web Push requires endpoint-specific POST requests, VAPID application server
identity, and encrypted payloads using subscription `p256dh` and `auth` material.
RFC 8291 specifies P-256 ECDH, authentication secret mixing, and HKDF/SHA-256
derivation; RFC 8292 specifies VAPID JWTs signed with ES256, `aud`, `exp`, and
optional contact `sub`. Sources: <https://web.dev/articles/push-notifications-web-push-protocol>,
<https://developer.mozilla.org/en-US/docs/Web/API/Push_API>,
<https://www.rfc-editor.org/rfc/rfc8291>,
<https://datatracker.ietf.org/doc/html/rfc8292>.

HMS Push Kit server sends through HTTPS APIs using app credentials and an access
token. No mature Rust provider crate was found that meets Sockudo's scale,
error-surface, and feature-control requirements.

WNS requires OAuth2 client credentials using Package SID / client secret or
Windows App SDK Azure credentials, then HTTPS POST to the channel URI with WNS
headers and bearer authorization. Sources:
<https://learn.microsoft.com/en-us/windows/apps/design/shell/tiles-and-notifications/windows-push-notification-services--wns--overview>,
<https://learn.microsoft.com/en-us/windows/apps/windows-app-sdk/notifications/push-notifications/push-quickstart>.

## Provider Crate Decisions

Crate metadata was checked from crates.io, docs.rs, and downloaded crate source
on 2026-04-30.

| Provider | Candidate | Version | License | Maintenance signal | Runtime / HTTP / TLS | Protocol and errors | Decision |
| --- | ---: | ---: | --- | --- | --- | --- | --- |
| FCM | `fcm` | 0.9.2 | MIT | Last crate update 2022-07-27 | Async reqwest; feature can select rustls, but crate targets legacy FCM model | Legacy response model and retry-after support, not HTTP v1/service-account first | Reject for 4.5 HTTP v1 |
| FCM | `fcm-service` | 0.2.3 | MIT | Last crate update 2025-06-03 | Tokio + reqwest + `gcp_auth`; reqwest default TLS unless patched | Uses HTTP v1 endpoint and has broad message fields, but returns boxed/string errors and loses structured provider detail | Reject for core provider; useful as reference only |
| FCM | `oauth_fcm` | 0.3.0 | MIT | Last crate update 2024-12-15 | Tokio + reqwest 0.11 default TLS; custom token manager | HTTP v1 endpoint, mockable URL hooks, status/body errors; narrow message model and old reqwest | Wrap or fork only if `fcm-rs` is insufficient |
| FCM | `fcm-rs` | 0.2.0 | MIT | Last crate update 2024-07-20 | Tokio + reqwest 0.12 + `yup-oauth2` 9; reqwest default TLS needs patching | HTTP v1 endpoint, service account auth, typed FCM error response with code/message/status/details | Fork/wrap candidate; require `default-features = false`, `rustls-tls`, and typed error preservation |
| FCM | `fcm_v1` | 0.3.0 | MIT | Last crate update 2023-04-22 | reqwest 0.11 + `yup-oauth2` 8.3; default TLS | Type-safe message model, but FCM errors collapse into a formatted string | Reject unless used as a schema reference |
| APNs | `apple-apns` | 0.5.3 | MIT | Last crate update 2023-02-05 | reqwest; rustls feature exists | Token auth and reason enum, but no `.p12` support and no explicit HTTP/2 connection management surface | Reject for release core |
| APNs | `apns-h2` | 0.11.0 | MIT | Last crate update 2026-02-09; active owner org | Tokio + hyper 1 + hyper-rustls + rustls 0.23; default `aws-lc-rs`, optional OpenSSL | Async HTTP/2 APNs client, certificate and token auth, `.p12`, PEM, response status/reason/timestamp types | Adopt behind `push-apns`, wrapped for pooling, GOAWAY reconnect policy, and Sockudo error taxonomy |
| APNs | `apnoxide` | 0.1.1 | MIT | Last crate update 2025-04-20; very low adoption | reqwest with rustls builder | Token auth only; simple response mapping; no certificate/.p12 support | Reject |
| APNs | `apns2` | 0.1.0 | MIT/Apache-2.0 | Last crate update 2018-02-25 | curl-based synchronous client | `.p12` certificate only; obsolete sync dependency stack | Reject |
| APNs | `vru-apns` | 0.3.0 | MIT | Last crate update 2023-11-25; very low recent downloads | h2 + tokio-rustls | Token auth, low-level response, no certificate/.p12 support | Reject |
| Web Push | `web-push` | 0.11.0 | Apache-2.0 | Last crate update 2025-02-22; high usage | Async generic client; default `isahc`, optional hyper 0.14 + native TLS; depends on `ece` with OpenSSL default | Strong Web Push API, VAPID, retry-after, response parsing, encryption; TLS/dependency defaults conflict with Sockudo posture | Wrap or fork only if OpenSSL/native TLS can be removed; do not enable default client |
| Web Push | `web-push-native` | 0.4.0 | MIT OR Apache-2.0 | Last crate update 2023-12-25 | Library-only HTTP request builder; no baked-in client | RFC 8030 request construction, VAPID, pure Rust companion `ece-native`; weaker maturity than `web-push` | Wrap candidate for rustls-clean implementation |
| Web Push | `ece` | 2.3.1 | MPL-2.0 | Last crate update 2024-01-22; high usage | OpenSSL default backend | ECE encryption/decryption used by `web-push`; license and OpenSSL default need review | Reject as direct dependency unless accepted through `web-push` fork |
| Web Push | `ece-native` | 0.4.0 | MIT OR Apache-2.0 | Last crate update 2023-12-25 | Pure Rust crypto dependencies | RFC 8188 ECE primitives for `web-push-native` | Wrap candidate with additional RFC 8291 interoperability tests |
| Unified | `armature-push` | 0.1.1 | Apache-2.0 | Last crate update 2025-12-30; low downloads | reqwest 0.12; optional `web-push` 0.10 | Unified API is convenient but immature; APNs/FCM error mapping collapses provider detail into generic strings | Reject for core; may inform public trait shape |
| HMS | `hms` | 0.1.3 | MIT | Last crate update 2025-01-19 | Not a push crate | Crate is unrelated CLI/text utility | Reject |
| HMS | `hi-push` | 0.0.1 | MIT | Very early crate | Broad default features include email, MongoDB, gRPC, HTTP API, FCM/APNs/Xiaomi/Huawei | Too broad and immature for Sockudo; feature bloat is unacceptable | Reject; hand-written HMS adapter is justified after this rejection |
| HMS | `soflare` | 0.1.0 | Apache-2.0 | New Matrix push gateway; Rust 1.94 requirement exceeds Sockudo 1.90 | Service/gateway crate, not a provider client library | Includes domestic OEM providers but is not suitable as an embedded Sockudo provider | Reject |
| WNS | `wns` | 1.0.1 | MPL-2.0 | Last crate update 2020-07-03 | Not a push crate | Crate is unrelated CSS dialect | Reject |
| WNS | `wpush` | 0.1.1 | MPL-2.0 | Last crate update 2023-era from crate metadata; low relevance | Desktop toast helper | Local Windows notifications, not WNS cloud push | Reject; hand-written WNS adapter is justified after this rejection |

Recommended provider path:

- FCM: fork/wrap `fcm-rs` first because it is HTTP v1, uses reqwest 0.12, has
  service account auth, and exposes typed FCM error bodies. The wrapper must
  force rustls, preserve status/code/details, expose retry-after when present,
  support mock endpoints, and keep retries outside the crate.
- APNs: adopt `apns-h2` behind a Sockudo wrapper. The wrapper must own
  connection pooling, reconnect/GOAWAY behavior, provider-token caching,
  `.p12` and PEM loading policy, status/reason classification, and metrics.
- Web Push: prefer a rustls-clean wrapper around `web-push-native` +
  `ece-native`, with interoperability tests against RFC 8291/8292 vectors and
  major browser push endpoints. `web-push` remains a fallback only if we can
  remove OpenSSL/native TLS/default-client bloat through features or a fork.
- HMS: no acceptable Rust provider crate was found. A minimal Sockudo HTTP
  adapter is justified, but it must be explicitly typed, mockable, and based on
  official HMS Push Kit REST/auth/error docs before implementation.
- WNS: no acceptable Rust provider crate was found. A minimal Sockudo HTTP
  adapter is justified, but it must preserve WNS status headers/body details
  and model token refresh semantics from Microsoft docs.

## Queue Backend Suitability For Push

Push pipeline stages have different queue needs:

- accept queue: durable enough to return async `202` quickly
- planning queue: partitionable by tenant/channel and able to fan out large jobs
- dispatch queue: high throughput, retry/backoff, DLQ, provider-rate partitioning
- scheduled queue: delayed delivery and cancellation must be backed by storage,
  not only broker timers

| Backend | Current Sockudo behavior | Classification | Push suitability |
| --- | --- | --- | --- |
| memory | Bounded in-process `VecDeque`, drops oldest at 100,000 jobs, 500ms polling | tests/dev only | Good for unit tests and monolith demos only. Not durable and not multi-node. |
| redis | `RPUSH` + `BLPOP`; workers pop before processing | small production | Supported for small deployments, but not reliability mode because worker failure after pop can lose jobs. Needs visibility/retry wrapper or Redis Streams for durable push. |
| redis-cluster | Same list semantics as Redis over cluster | small/regional production | Supported for sharded small/regional deployments, but same pop-before-process reliability issue. Not hyperscale recommended for dispatch. |
| nats | JetStream stream per queue, explicit ack, max deliver 10, default max messages 100,000 | regional production; hyperscale candidate with config | Good for durable push stages after retention/stream limits become configurable. Recommended for high-throughput regional clusters. |
| pulsar | Shared subscription, explicit ack, producer cache | hyperscale recommended | Strong fit for planner/dispatch at scale if topics are partitioned by tenant/provider/channel and operational docs cover backlog and retention. |
| rabbitmq | Durable queue, persistent messages, ack on success | regional production | Good for traditional regional deployments. Not recommended for 100M fanout unless heavily sharded and DLQ/backpressure policies are configured. |
| google-pubsub | Managed topic/subscription, ack on success | regional/cloud production; hyperscale recommended on GCP | Strong cloud-native dispatch/planner queue with provider quotas and managed scaling. Recommended for GCP deployments, with docs on ordering, ack deadlines, and quota smoothing. |
| kafka | Producer/consumer with manual commit; topic auto-created with one partition today | hyperscale recommended after partition config | Strong fit for planner and dispatch, but push implementation must allow configurable partition counts and keys. One partition is unacceptable for 4.5 scale goals. |
| iggy | Stream/topic/group, offset commits, retry/DLQ handling in implementation | hyperscale candidate | Promising fit for high-throughput dispatch. Because ecosystem maturity is lower than Kafka/Pulsar, recommend as supported and potentially hyperscale after benchmark proof. |
| sqs | Managed queue, visibility timeout, long polling, delete on success | regional/cloud production | Good cloud-native dispatch queue, especially FIFO or per-provider shards. Not recommended for 100M fanout planning without aggressive sharding and quota docs. |
| sns | Publish-only; consumption handled elsewhere | unsupported as worker queue; useful fanout ingress | Support only as notification fanout into SQS/HTTP subscribers. Not valid as a complete push pipeline queue because `process_queue` is a no-op. |

Queue release blockers:

- Add retry, DLQ, idempotency key, and provider retry-after handling at the push
  layer instead of assuming every queue backend provides those semantics.
- Scheduled delivery and cancellation must be anchored in push storage. Broker
  delays may optimize dispatch but must not be the source of truth.
- Hyperscale docs must recommend Kafka, Pulsar, Google Pub/Sub, and possibly
  Iggy/NATS JetStream after benchmark evidence, while still documenting support
  for Redis, RabbitMQ, SQS, and SNS in smaller or cloud-specific modes.

## Storage Backend Suitability For Push

Device registry storage needs multi-tenant device records, provider token lookup,
topic/channel subscription indexes, stale-token cleanup, scheduled/cancelled job
state, publish status, and analytics rollups. Fanout planning needs efficient
range scans by `(app_id, channel/topic, shard)` and update/delete paths for
provider cleanup.

| Backend | Classification | Device count and fanout fit | Operational fit |
| --- | --- | --- | --- |
| memory | tests/dev only | Suitable only for unit tests and local development. No multi-node registry or durable status. | Keep implementation for tests and monolith demos. |
| postgres | small/regional production | Good for thousands to tens of millions of devices with partitioned tables, covering indexes, and background cleanup. Large fanout requires shard columns and careful query plans. | Strong correctness and migration story. Not recommended as default for 1B devices / 10TB registry without extensive partitioning and operational proof. |
| mysql | small/regional production | Similar to PostgreSQL but weaker fit for complex fanout/status analytics unless schema stays simple and partitioning is enforced. | Supported where MySQL is already used for app data. Not hyperscale recommended. |
| dynamodb | cloud production; hyperscale recommended | Strong fit for 1B+ device registry when partition keys distribute by tenant/provider/device shard and GSIs model topic/channel fanout. Writes and cleanup scale horizontally. | Recommended for AWS/cloud-native large deployments. Requires capacity, hot-key, TTL, and GSI cost documentation. |
| surrealdb | small production / experimental regional | Flexible model, but not proven here for 10TB device registry or 100M channel fanout. | Supported where current app-manager/history support exists, but not recommended for hyperscale push until load-tested. |
| scylladb | hyperscale recommended | Strong fit for 10TB+ registry and massive fanout indexes with wide rows/time buckets/shard keys. Handles high write rates and horizontal scale. | Recommended for largest self-managed deployments. Requires data-model discipline and compaction/tombstone documentation. |

Storage release blockers:

- Add push-specific schemas or runtime provisioning for every supported backend.
- Define canonical keys for `tenant/app`, `device_id`, `provider`, `token_hash`,
  `channel/topic`, `shard`, `expires_at`, and `updated_at`.
- Token cleanup must consume structured provider errors: FCM `UNREGISTERED`,
  APNs 410/unregistered, Web Push 404/410, HMS invalid token codes, and WNS
  expired/invalid channel responses.
- Status tracking must be append-friendly and bounded by retention. Analytics
  rollups should not require scanning raw delivery rows.

## Dependency Risk Summary

- No provider crate should be adopted without a Sockudo wrapper. The wrapper is
  where provider errors become cleanup/retry/circuit-breaker signals.
- Rustls posture is a hard gate. Crates using reqwest default TLS, hyper-tls, or
  OpenSSL defaults need feature patches, forks, or rejection.
- Retry behavior must stay outside provider crates so Sockudo can apply tenant
  quotas, provider circuit breakers, jitter, and noisy-tenant isolation.
- Mock endpoint support is mandatory for FCM/HMS/WNS and highly desirable for
  APNs/Web Push. Provider integration tests should not require live credentials.
- Hand-rolled provider code is justified only for HMS and WNS in this evaluation.
  For FCM/APNs/Web Push, existing crates cover enough protocol machinery that
  Sockudo should adopt, wrap, or fork rather than start from scratch.

## Implementation Guidance For Prompt 4.5+

1. Add `crates/sockudo-push` with no default provider dependencies.
2. Add `sockudo-server` feature `push` and provider subfeatures, but keep
   trimmed builds working.
3. Implement provider traits around Sockudo-owned error/status types before
   wiring HTTP API handlers.
4. Add memory storage/queue implementations first for deterministic tests.
5. Add durable backend schemas/provisioning one backend family at a time.
6. Add metrics before worker loops are considered production-ready.
7. Document supported vs hyperscale-recommended backends in release docs.
