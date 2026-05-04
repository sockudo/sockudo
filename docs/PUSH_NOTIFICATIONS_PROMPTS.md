# Release 4.5 Ultra Prompt Pack v3: Push Notification Platform

This pack combines the earlier Sockudo push-notification prompt pack with the hyperscale 4.5 v2 release plan. It is written as an execution-grade prompt sequence for building a production push platform in `sockudo-push`, behind feature flags, with Ably/Centrifugo parity plus Sockudo-native scale, observability, and operator controls.

Use these prompts in order. Each prompt assumes the previous prompt is complete, tested, documented, and committed. Do not merge placeholder provider behavior, TODO-driven production paths, or code that compiles only under one feature combination.

## Research Context

This prompt pack is self-contained. These local files are useful prior-art references when present, but they are not required for release 4.5 execution. If any are missing, continue from this document and the external sources below instead of blocking:

- `docs/research/ably-parity/ably-push-notifications-research.md`
- `docs/research/ably-parity/sockudo-ably-parity-release-plan.md`
- `docs/PRESENCE_HISTORY_PROMPTS.md`

Binding local implementation context:

- `Cargo.toml`
- `crates/sockudo-server/Cargo.toml`
- `crates/sockudo-core/src/options.rs`
- `crates/sockudo-core/src/app.rs`
- `crates/sockudo-queue`
- `crates/sockudo-metrics`
- `crates/sockudo-server/src/history.rs`
- `ops/migrations`

External sources checked for the original prompt pack:

- Centrifugo Pro push notifications: <https://centrifugal.dev/docs/5/pro/push_notifications>
- Centrifugo configuration model: <https://centrifugal.dev/docs/server/configuration>
- Ably push overview: <https://ably.com/docs/push>
- Ably publish push notifications: <https://ably.com/docs/push/publish>
- Ably device activation: <https://ably.com/docs/push/configure/device>
- Ably REST API push routes: <https://ably.com/docs/api/rest-api>
- FCM HTTP v1 send API: <https://firebase.google.com/docs/cloud-messaging/auth-server>
- FCM errors and retry guidance: <https://firebase.google.com/docs/cloud-messaging/error-codes>
- FCM scale guidance: <https://firebase.google.com/docs/cloud-messaging/scale-fcm>
- FCM token management: <https://firebase.google.com/docs/cloud-messaging/doc-revamp/optimize-delivery/manage-tokens>
- APNs connection guidance: <https://developer.apple.com/documentation/usernotifications/establishing-a-connection-to-apns>
- APNs notification request guidance: <https://developer.apple.com/documentation/usernotifications/sending-notification-requests-to-apns>
- APNs response handling: <https://developer.apple.com/documentation/usernotifications/handling-notification-responses-from-apns>
- Web Push overview: <https://web.dev/articles/push-notifications-overview>
- Web Push protocol details: <https://web.dev/articles/push-notifications-web-push-protocol>
- MDN Push API: <https://developer.mozilla.org/en-US/docs/Web/API/Push_API>
- RFC 8291 Web Push encryption: <https://www.rfc-editor.org/rfc/rfc8291>
- RFC 8292 VAPID: <https://datatracker.ietf.org/doc/html/rfc8292>

Rust provider crates known to exist and requiring evaluation before implementation:

- FCM: `fcm`, `fcm-service`, `oauth_fcm`, `fcm-rs`, `fcm_v1`
- APNs: `apple-apns`, `apns-h2`, `apnoxide`, `apns2`, `vru-apns`
- Web Push: `web-push`, `web-push-native`, `ece`, `ece-native`
- Unified or mixed crates: `armature-push`

The implementation must not hand-roll provider clients by default. It must first evaluate existing crates against Sockudo requirements: active maintenance, async Tokio compatibility, rustls/AWS-LC/ring story, HTTP/2 support, FCM HTTP v1 correctness, APNs auth modes and GOAWAY handling, Web Push RFC 8291/8292 support, dependency weight, license, feature flags, testability, and whether error surfaces expose enough provider detail for cleanup/retry/circuit-breaker logic.

## Release Contract

Sockudo release 4.5 adds an optional push notification platform. It must live primarily in `crates/sockudo-push` and be enabled through `sockudo-server` feature `push`. Provider and backend support must be controlled by subfeatures.

Parity target:

- Ably-style activation: direct activation and server-assisted activation.
- Ably-style delivery: direct publish, batch publish, async publish with `publish_id`, channel-based publish via `extras.push`, scheduled delivery, cancellation, and delivery status tracking.
- Centrifugo-style operability: fully configurable, durable queues, device/topic storage, scheduling, cancellation, expiry, dry-run, reliability mode, localization, templates, rate limits, stale-device cleanup, analytics, worker metrics, and clear deployment modes.

Provider scope:

- FCM via service account JSON and HTTP v1.
- APNs via `.p12` or PEM credentials, with token auth considered if crate/protocol support is stronger.
- Web Push via VAPID and RFC 8291 encryption.
- HMS via app credentials.
- WNS via package SID and client secret.

Backend scope:

- Storage: memory for tests/dev only; PostgreSQL, MySQL, DynamoDB, SurrealDB, ScyllaDB.
- Queue/broker: memory for tests/dev only; Redis, Redis Cluster, NATS JetStream, Pulsar, RabbitMQ, Google Pub/Sub, Kafka, Iggy, SQS, SNS.
- All existing Sockudo database and queue options must be wired, documented, and tested at their appropriate tier. The docs must distinguish supported from recommended-for-hyperscale. A backend can be supported for small or cloud-native deployments without being marketed as 100M-fanout capable.

Scale targets:

- Publish acceptance: 100K publishes/sec sustained, 1M/sec peak across a cluster.
- Delivery dispatch: 1M notifications/sec sustained across providers, 5M/sec peak across a large cluster, subject to provider quotas.
- Per-tenant default acceptance ceiling: 50K publishes/sec sustained, configurable per app.
- Device registry: architecture supports 1B+ devices across tenants by horizontal scaling.
- Largest channel fanout: 100M subscribers per publish.
- Token store size: design for 10TB+ registration/index data per cluster.
- p99 publish acceptance latency: under 50ms for async `202`.
- p99 fanout planner latency to first delivery job: under 100ms.
- p99 single-recipient end-to-end to provider ack: under 1s.
- p99 1M fanout completion: under 30s in the release candidate environment.
- A noisy tenant at 10x quota must not degrade other tenants' p99 by more than 20%.

Important realism:

- Sockudo can be engineered to accept, persist, plan, queue, and dispatch at hyperscale.
- Sockudo cannot force FCM/APNs/Web Push/HMS/WNS to exceed provider quotas. Quotas, smoothing, backpressure, circuit breakers, and capacity planning docs are release blockers.

Non-goals:

- No dashboard UI in 4.5, only metrics and Grafana JSON.
- No custom device transport replacing APNs/FCM/Web Push.
- No claim of Meta-MCS-class global private push infrastructure.
- No permanent raw payload storage by default.

## Feature And Crate Target

Recommended feature shape:

- Server feature: `push`
- Provider features: `push-fcm`, `push-apns`, `push-webpush`, `push-hms`, `push-wns`
- Storage features: reuse existing `postgres`, `mysql`, `dynamodb`, `surrealdb`, `scylladb`; add push-specific guards only if necessary.
- Queue features: reuse existing `redis`, `redis-cluster`, `nats`, `pulsar`, `rabbitmq`, `google-pubsub`, `kafka`, `iggy`, `sqs`, `sns`; add push-specific guards only if necessary.
- Deployment feature: `monolith` for all pipeline stages in the main process for dev/small deployments.

`push` may enable all provider subfeatures by default only if this does not bloat minimal builds unexpectedly. Trimmed builds must be possible, for example:

- `cargo build -p sockudo --no-default-features --features "v2"`
- `cargo build -p sockudo --features "push,push-fcm,postgres,kafka"`
- `cargo build -p sockudo --features "push,push-apns,scylladb,iggy"`
- `cargo build -p sockudo --features "full"`

## Prompt 4.5-00: Dependency And Backend Evaluation

```text
Before implementation, produce and commit:
`docs/research/ably-parity/sockudo-push-dependency-backend-evaluation.md`.

Inspect:
- Cargo.toml
- crates/sockudo-server/Cargo.toml
- crates/sockudo-queue
- crates/sockudo-core/src/options.rs
- crates/sockudo-server/src/history.rs
- ops/migrations

Evaluate existing Rust crates for:
- FCM HTTP v1
- APNs
- Web Push
- HMS
- WNS

For each candidate crate, record:
- version, license, repository, maintenance activity
- async runtime and HTTP client
- TLS stack and compatibility with Sockudo's rustls posture
- HTTP/2 support
- provider protocol coverage
- error detail exposure
- auth handling
- retry/backoff hooks
- testability with mock providers
- feature bloat and transitive dependencies
- decision: adopt, wrap, fork, or reject

Do not add dependencies yet unless the ADR approves them.

Also evaluate every existing Sockudo queue backend for push pipeline suitability:
- memory
- redis
- redis-cluster
- nats
- pulsar
- rabbitmq
- google-pubsub
- kafka
- iggy
- sqs
- sns

Classify each as:
- tests/dev only
- small production
- regional production
- hyperscale recommended
- unsupported for a specific pipeline stage, with reason

Also evaluate every existing Sockudo storage backend:
- memory
- postgres
- mysql
- dynamodb
- surrealdb
- scylladb

Classify each by device count, fanout size, write rate, query pattern, and operational fit.

Acceptance criteria:
- The document explicitly says all Sockudo backends are supported where feasible, but not all are recommended for hyperscale.
- The chosen provider crates are justified against official provider docs.
- Any hand-rolled provider code requires a specific rejection of available crates.
```

## Prompt 4.5-01: Architecture, Release Contract, And ADR

```text
Implement Sockudo release 4.5: Push Notification Platform.

Before writing code, produce and commit:
- `docs/research/ably-parity/sockudo-push-release-contract.md`
- an ADR under the repo's ADR/research convention

The release contract must cover:
1. Ably parity surface: activation, registry, direct publish, channel push, batch, async publish, scheduled delivery, cancellation, status.
2. Centrifugo-inspired operator controls: config, reliability mode, dry-run, stale cleanup, templating, localization, analytics, worker metrics.
3. Provider scope: FCM, APNs, Web Push, HMS, WNS.
4. Storage scope: memory, Postgres, MySQL, DynamoDB, SurrealDB, ScyllaDB.
5. Queue scope: memory, Redis, Redis Cluster, NATS JetStream, Pulsar, RabbitMQ, Google Pub/Sub, Kafka, Iggy, SQS, SNS.
6. Feature-flag matrix and compile targets.
7. Public API contract and auth model.
8. Internal store, queue, provider, worker, planner, feedback, metrics abstractions.
9. Credential encryption, rotation, redaction, and cache invalidation.
10. Idempotency, scheduling, cancellation, expiry, retries, dead letters, duplicate suppression.
11. Per-tenant quotas, fanout caps, weighted fair queueing, and backpressure.
12. Provider quota realism and the difference between Sockudo dispatch capacity and provider delivery limits.
13. Security and privacy rules for tokens, payloads, logs, metrics, and admin APIs.
14. Test, load, chaos, soak, docs, migration, and rollout plans.

Release blockers:
- Complete device registry and channel push subscriptions.
- Async publish returns `202 + publish_id`.
- Cursor pagination on every list endpoint.
- Payload transformation layer implemented, not pass-through JSON.
- Scheduled delivery cancellable before dispatch.
- Per-device state tracked and observable.
- Stale device cleanup configurable.
- At least one real end-to-end provider verified before shipping.
- Tenant fairness and backpressure verified under load.
- No unsupported scale claim without performance evidence.
```

## Prompt 4.5-01b: Capacity Model And Fanout Architecture

```text
Before implementing the runtime, commit:
`docs/research/ably-parity/sockudo-push-capacity-model.md`.

This document is binding for all subsequent prompts.

Architecture: durable queue-based pipeline.

Stages:
1. Publish API: validate, rate-limit, persist status, publish event, return 202.
2. Publish Log: durable topic partitioned by hash(app_id, publish_id).
3. Fanout Planner: stateless consumers; fast path or shard path.
4. Delivery Jobs Queue: provider-specific topics.
5. Provider Worker Pools: weighted-fair across tenants, per-provider connection pools.
6. Delivery Results Queue.
7. Feedback Processor: device state, token cleanup, status counters, metrics, `[meta]log:push`.

Fanout regimes:
- Fast path: fanout < `PUSH_FANOUT_FAST_THRESHOLD`, default 10K. Planner reads subscribers through paginated storage queries and emits provider batches.
- Shard path: fanout >= threshold. Planner emits shard jobs covering cursor/key ranges. Shard workers stream tokens in bounded pages and emit delivery jobs.
- Default `PUSH_FANOUT_SHARD_SIZE`: 100K. A 100M channel emits about 1000 shards.

Token store logical partitioning:
- `devices_by_id`: partition `(app_id, device_id)`.
- `devices_by_channel`: partition `(app_id, channel)`, clustering `device_id`, denormalized recipient data.
- `devices_by_client`: partition `(app_id, client_id)`, clustering `device_id`.
- `devices_by_token`: partition `(app_id, transport_type, token_hash)`.
- `devices_by_last_active`: partition `(app_id, day_bucket)`, clustering `(last_active_at, device_id)`.

Backend tier guidance:
- memory: tests/dev only.
- PostgreSQL/MySQL: small production, generally < 10M devices, hash-partitioned by app_id.
- SurrealDB: supported small deployment tier only unless benchmarks prove otherwise.
- DynamoDB: cloud-native production path with careful partition design.
- ScyllaDB: hyperscale recommended path for 10M to 1B+ devices.

Broker tier guidance:
- memory: tests/dev only.
- Redis/Redis Cluster: small production or low-latency local queue use; do not claim replay-heavy hyperscale unless stream semantics are implemented and tested.
- NATS JetStream, Kafka/Redpanda, Iggy, Pulsar: primary candidates for high-scale durable replayable stages.
- RabbitMQ: supported where ack/redelivery and retention fit; document replay limits.
- Google Pub/Sub, SQS, SNS: cloud-native supported paths; account for ordering, visibility timeout, fanout semantics, and cost.

Deliver:
1. Architecture diagram.
2. Storage partition-key/clustering-key tables per backend family.
3. Broker topic/stream/queue layout for every supported queue backend.
4. Worked capacity envelope.
5. Cluster sizing baseline and autoscale ceilings.
6. Provider concurrency targets.
7. Backpressure protocol.
8. Backend tier matrix with honest scale limits.
```

## Prompt 4.5-02: Domain Model And Invariants

```text
Design and implement the provider-independent push domain model in `crates/sockudo-push`.

Entities:
- DeviceDetails
- ChannelSubscription
- ProviderCredential
- NotificationTemplate
- PushRecipient
- PushPayload
- ProviderOverridePayload
- PublishIntent
- PublishStatus
- DeliveryJob
- DeliveryBatch
- DeliveryResult
- DeliveryEvent
- PushCursor

DeviceDetails fields:
- `id`: stable device id.
- `clientId`: optional Sockudo user/client id.
- `formFactor`: phone, tablet, desktop, tv, car, watch, embedded, other.
- `platform`: android, ios, browser, windows, macos, watchos, tvos, other.
- `metadata`: bounded JSON.
- `deviceSecret`: hashed server-side; never stored plaintext.
- `timezone`: IANA timezone.
- `locale`: BCP 47 tag.
- `push.recipient`:
  - FCM: `{ transportType: "gcm", registrationToken }`
  - APNs: `{ transportType: "apns", deviceToken }`
  - Web Push: `{ transportType: "web", endpoint, p256dh, auth }`
  - HMS: `{ transportType: "hms", registrationToken }`
  - WNS: `{ transportType: "wns", channelUri }`
- `pushRatePolicy`: optional per-device token bucket policy.
- `push.state`: ACTIVE, FAILING, FAILED.
- `push.errorReason`: redacted last known error.

Invariants:
- All rows and cursors are app-scoped.
- Re-registering identical device details is idempotent.
- Changing a provider token updates the registration, not a new device.
- A push channel subscription is separate from realtime websocket subscription.
- A device can receive channel push while offline.
- ACTIVE -> FAILING on first failure; FAILING -> FAILED after threshold; success from FAILING resets to ACTIVE.
- Deleting a missing device returns success.
- Tokens, endpoints, credentials, and raw payloads are redacted in Debug/logs/metrics.

Provider credentials:
- FCM service account JSON, encrypted.
- APNs `.p12` or PEM material, encrypted.
- Web Push VAPID keys, generated or imported per app, encrypted where private.
- HMS app id/client secret, encrypted.
- WNS package SID/client secret, encrypted.

Templates:
- Per-app template id.
- Locale map with title/body/icon/sound/collapseKey.
- Exact locale -> language prefix -> default locale fallback.
- Provider-specific overrides may combine with templates.

Tests:
- serde compatibility
- redaction
- bounds validation
- cursor encoding/decoding
- idempotency keys
- state transitions
- template fallback
```

## Prompt 4.5-03: Storage Traits, Schemas, Migrations, And Backfill

```text
Implement push storage abstractions and backend schemas.

Required traits:
- PushDeviceStore
- PushSubscriptionStore
- PushCredentialStore
- PushTemplateStore
- PushPublishStatusStore
- PushScheduleStore
- PushDeliveryEventStore
- PushIdempotencyStore

Supported storage backends:
- memory: tests/dev only
- postgres
- mysql
- dynamodb
- surrealdb
- scylladb

Every backend must implement the same semantic contract or explicitly return a feature-gated startup error. Do not silently fall back to memory in production.

Schema families:

Hyperscale denormalized layout for Scylla/Dynamo:
- `devices_by_id`
- `devices_by_client`
- `devices_by_transport`
- `devices_by_token`
- `devices_by_last_active`
- `channel_subscribers`
- `channels_by_device`
- `channels_by_client`
- `provider_credentials`
- `notification_templates`
- `scheduled_jobs_by_due_minute`
- `scheduled_jobs_by_id`
- `publish_status`
- `push_delivery_events`
- `push_dead_letters`
- `push_idempotency`

SQL small-tier layout:
- Use equivalent tables and indexes.
- PostgreSQL: hash partition by app_id; range/time partitions for events.
- MySQL: partition by key app_id where supported; time partition events.
- Use cursor-compatible indexes; never offset pagination for hot paths.

SurrealDB:
- Supported for small deployments.
- Implement if existing Sockudo patterns make it practical; otherwise the release contract must call out any deferred support as a blocker if "all backends" is non-negotiable.

Migrations:
- Add files under `ops/migrations/postgres`, `ops/migrations/mysql`, `ops/migrations/dynamodb`, `ops/migrations/surrealdb`, `ops/migrations/scylladb`.
- Include rollback/drop guidance.
- Include online migration procedure: dual-write, backfill, switch reads, drop old schema.

Credential security:
- Envelope encryption.
- Operator-supplied raw KEK or KMS/Vault reference.
- Rotation procedure documented.

Tests:
- conformance suite shared across all storage backends.
- migration smoke tests.
- concurrent registration/update.
- cursor pagination.
- channel fanout scans.
- stale cleanup scans.
- delivery event retention.
```

## Prompt 4.5-04: HTTP API, Protocol, Payload Contract

```text
Implement push administration and publishing HTTP APIs.

Credential APIs:
- `POST /apps/{app_id}/push/credentials/fcm`
- `POST /apps/{app_id}/push/credentials/apns`
- `POST /apps/{app_id}/push/credentials/webpush`
- `POST /apps/{app_id}/push/credentials/hms`
- `POST /apps/{app_id}/push/credentials/wns`
- `GET /apps/{app_id}/push/credentials`

Template APIs:
- `POST /apps/{app_id}/push/templates`
- `GET /apps/{app_id}/push/templates/{id}`
- `GET /apps/{app_id}/push/templates`
- `DELETE /apps/{app_id}/push/templates/{id}`

Device APIs:
- `POST /apps/{app_id}/push/deviceRegistrations`
- `GET /apps/{app_id}/push/deviceRegistrations/{id}`
- `GET /apps/{app_id}/push/deviceRegistrations`
- `DELETE /apps/{app_id}/push/deviceRegistrations/{id}`
- `DELETE /apps/{app_id}/push/deviceRegistrations` for removeWhere

Channel subscription APIs:
- `POST /apps/{app_id}/push/channelSubscriptions`
- `GET /apps/{app_id}/push/channelSubscriptions`
- `GET /apps/{app_id}/push/channelSubscriptions/channels`
- `DELETE /apps/{app_id}/push/channelSubscriptions`

Publish APIs:
- `POST /apps/{app_id}/push/publish`
- `POST /apps/{app_id}/push/batch/publish`
- `GET /apps/{app_id}/push/publish/{publish_id}/status`
- `DELETE /apps/{app_id}/push/scheduled/{job_id}`
- `POST /apps/{app_id}/push/deliveryStatus`

Cursor pagination:
- Mandatory for every list endpoint.
- Default limit 100, max 1000.
- Cursor is opaque, versioned, base64, app-scoped, and backend-aware.
- Response: `{ "items": [...], "next_cursor": "...", "has_more": true }`.
- Offset pagination is not allowed.

Async publish:
- Default response: `202 Accepted` with `{ "publish_id", "status": "accepted", "expected_recipients" }`.
- Sync mode allowed only when expected recipients <= `PUSH_FANOUT_SYNC_THRESHOLD`.
- If sync is requested above threshold, return 202 and `X-Sockudo-Forced-Async: fanout-exceeds-sync-threshold`.

Recipients:
- deviceId
- clientId
- channel
- registered topic/user topic if implemented
- raw FCM token/topic/condition
- raw APNs token
- raw Web Push subscription
- raw HMS token/topic/condition
- raw WNS channel URI
- bounded indexed filters

Payload mapping:
- Generic notification/data fields map to FCM/APNs/Web Push/HMS/WNS.
- Provider overrides take precedence.
- APNs push type, topic, priority, expiration, collapse id are validated.
- Web Push TTL, urgency, topic, and VAPID audience are validated.
- WNS toast/tile/raw envelope is built from generic fields or override.
- Payload transformation is real code, not pass-through JSON.

Channel push:
- `extras.push` on V2 message publish creates a push publish event.
- Channel push targets push subscribers, not live websocket subscribers.
- V1/Pusher-compatible publish remains unchanged.
```

## Prompt 4.5-05: Auth, Capabilities, Quotas, And Security

```text
Implement push authorization and abuse resistance.

Capabilities:
- `push-admin`: full app-scoped push administration and direct publish.
- `push-subscribe`: self-service device registration and subscriptions for the authenticated client/device only.

Device auth:
- Issue `deviceIdentityToken` after direct activation.
- Device updates authenticate with that token.
- Tokens are rotatable and revocable.

Channel push:
- Publishing a message with `extras.push` requires normal publish capability on that channel.
- No extra push capability is needed after server-side authorization succeeds.

Per-tenant quotas:
- `push.acceptance_rate_limit`
- `push.delivery_quota_daily`
- `push.fanout_max`
- `push.concurrent_inflight_max`
- per-provider ceilings
- per-device rate limits
- quota override header allowed only for `push-admin`, logged and metered

Enforcement:
- Publish API rate limit returns 429 with Retry-After.
- Planner quota check sets publish status `quota_exceeded`.
- Worker in-flight cap drives weighted fair queueing.
- Backpressure returns 503 with Retry-After.

Security:
- Provider credentials never appear in responses.
- Tokens/endpoints redacted everywhere.
- Device secrets stored hashed with a strong password hash.
- `removeWhere` with no filter returns 400.
- Raw-recipient publish requires `push-admin`.
- Web Push endpoint SSRF protection: HTTPS-only outside local tests, host allow/deny, no private IP ranges.
- Template substitution is sandboxed and output-bounded.
- Audit log every credential write, quota override, and removeWhere.
```

## Prompt 4.5-06: Runtime Pipeline And Queue Backends

```text
Implement the push runtime as the durable queue pipeline from 4.5-01b.

Crate layout:
- `api`
- `credentials`
- `registry`
- `activation`
- `subscription`
- `templates`
- `transformer`
- `planner`
- `dispatch`
- `feedback`
- `scheduler`
- `ratelimit`
- `cleanup`
- `status`
- `pipeline`
- `metrics`
- `testing`

Deployment modes:
- Distributed mode: separate binaries:
  - `sockudo-push-api`
  - `sockudo-push-planner`
  - `sockudo-push-worker-fcm`
  - `sockudo-push-worker-apns`
  - `sockudo-push-worker-webpush`
  - `sockudo-push-worker-hms`
  - `sockudo-push-worker-wns`
  - `sockudo-push-feedback`
  - `sockudo-push-scheduler`
  - `sockudo-push-cleanup`
- Monolith mode behind `monolith` for dev/small deployments.

Queue abstraction:
- publish log
- shard jobs
- delivery jobs per provider
- delivery results
- dead letters
- retry schedule

Required queue semantics:
- produce with key
- consume with consumer group / competing consumer
- ack/nack
- lease or visibility timeout
- retry at time
- dead-letter
- health
- lag/depth metrics
- idempotent producer where backend supports it

Supported queue backends:
- memory: tests/dev only.
- redis and redis-cluster: use streams/lists only if ack/replay semantics are correct; document limits.
- nats: JetStream required for durable pipeline stages.
- pulsar: durable topics and subscription modes.
- rabbitmq: quorum queues preferred; document replay/retention limits.
- google-pubsub: cloud-native mapping with ack deadlines and ordering keys where available.
- kafka: high-scale recommended.
- iggy: high-scale candidate; implement partitioned streams.
- sqs: visibility timeout and DLQ mapping.
- sns: publish fanout only; if used, pair with SQS consumers for delivery stages.

Stage behavior:
- Publish API persists status and emits publish_event, then returns 202.
- Planner expands recipients using fast path or shard path.
- Shard worker streams channel subscribers and emits provider batches.
- Provider workers dispatch and emit delivery_result.
- Feedback updates state, invalidates tokens, updates counters, writes events, emits metrics.

Backpressure:
- Publish API checks publish_log lag.
- Planner checks provider job lag and slows emission.
- Provider circuit breaker rejects/defers provider-specific jobs without stalling other providers.
```

## Prompt 4.5-06b: Provider Clients, Crates, Pools, Batching, Circuit Breakers

```text
Implement provider clients in `crates/sockudo-push/src/dispatch`.

Before coding each provider, use the dependency evaluation from 4.5-00. Prefer adopting or wrapping existing Rust crates when they meet Sockudo's production requirements. If rejecting all crates for a provider, document why and implement only the missing protocol layer.

Common trait:

trait PushDispatcher: Send + Sync {
    async fn dispatch(&self, batch: DeliveryBatch) -> Vec<DeliveryResult>;
    fn provider(&self) -> Provider;
    async fn health_check(&self) -> HealthStatus;
}

FCM:
- HTTP v1 `projects/{project_id}/messages:send`.
- OAuth2 service-account auth.
- Cache access token until five minutes before expiry.
- HTTP/2 connection pool.
- No legacy multicast dependency; batch means concurrent HTTP/2 sends unless an adopted crate safely abstracts this.
- Classify invalid token, invalid payload, auth failure, quota, 5xx, unavailable.
- Honor Retry-After and use jittered exponential backoff.

APNs:
- `.p12` or PEM certificate support per release scope; token auth may be added if approved.
- HTTP/2 persistent pool per tenant credential.
- Correct `apns-topic`, `apns-push-type`, priority, expiration, collapse id.
- Handle GOAWAY.
- 410 token cleanup, 429 retry, 5xx retry, permanent 4xx no retry.

Web Push:
- RFC 8291 encryption.
- VAPID per RFC 8292.
- Per-endpoint pool; per-recipient encryption.
- Use performant crypto and avoid pure slow hot paths.
- 404/410 cleanup, 413 permanent, 429 retry, 5xx retry.
- SSRF protections.

HMS:
- OAuth2 client credentials.
- Token list batching where supported.
- Per-token error parsing.
- Invalid token cleanup.

WNS:
- OAuth2 client credentials.
- POST to channel URI.
- Validate notification type and XML/raw payload shape.
- 410 cleanup, 429 retry.

Adaptive rate limiter:
- Per provider and tenant.
- Shrink on 429.
- Grow slowly after quiet period.

Circuit breaker:
- closed/open/half-open.
- Open on high failure rate.
- Probe before closing.
- Emit `[meta]log:push` and metrics.

Tests:
- local mock provider for every provider.
- auth cache refresh.
- header/payload generation.
- every error class.
- Retry-After.
- circuit breaker.
- token cleanup signal.
- no credentials/tokens in logs.
```

## Prompt 4.5-07: Cluster Correctness, Fairness, And Idempotency

```text
Harden clustered operation.

State:
- Registry, subscriptions, schedules, status, events in shared backing store.
- Provider credential cache per node; invalidate via cluster pub/sub.
- No per-node persistent source of truth.

Partitioning:
- publish_log partitioned by hash(app_id, publish_id).
- jobs.{provider} partitioned by hash(app_id) with enough partitions for parallelism.
- storage partitioning follows 4.5-01b and 4.5-03.

Tenant fairness:
- Weighted fair queueing or deficit round robin across app_id.
- Default weight 1.0.
- Over-quota tenants downgraded to 0.1.
- In-flight cap prevents one tenant occupying all workers.
- Noisy-neighbor test must pass.

Idempotency:
- Publish idempotency by app_id + publish_id / uid.
- Channel push idempotency by message_id + device_id.
- Worker duplicate suppression through existing cache manager where suitable.
- Duplicate suppression metrics.

Scheduler correctness:
- Every scheduler node polls due buckets.
- Per-job distributed lock.
- Scheduler emits publish_event, not direct provider sends.

Consistency:
- Token rotation last-writer-wins where provider is source of truth.
- Device state cache TTL <= 30s.
- Operator state changes publish cache invalidation events.
- removeWhere is paged and explicitly eventually consistent.
```

## Prompt 4.5-08: SDK And Client Behavior

```text
Implement push support across existing SDKs where present in the repository.

Client SDK targets:
- JavaScript/TypeScript browser and Node
- React Native / NativeScript if existing SDK surface supports it
- Kotlin Android
- Swift iOS/macOS
- Flutter
- .NET MAUI/Android/iOS/Windows

Server SDK targets:
- Node/TypeScript
- Python
- Go
- Java
- PHP
- Ruby
- Rust
- .NET
- Swift server-side

Required SDK behavior:
- device activation
- server-assisted registration helper
- device update token/deviceIdentityToken handling
- channel push subscription management
- admin registry APIs
- direct publish
- batch publish
- async publish default with `publish_id`
- `getPublishStatus(publishId)`
- cursor pagination for every list call
- provider payload overrides
- scheduled publish and cancellation
- delivery status update helper

Do not invent SDK directories that do not exist without first documenting the scope. If SDKs are outside this repo, produce exact implementation prompts and compatibility tests instead of pretending they were changed.
```

## Prompt 4.5-09: Configuration, Deployment, Docs, And Capacity Planning

```text
Prepare operator-facing release docs and config.

Config:
- `[push]` top-level config or `[push_notifications]` if approved by ADR.
- provider enables
- credential references
- storage backend
- queue backend
- worker counts
- partition/shard counts
- fanout thresholds
- sync threshold
- backpressure thresholds
- publish status TTL
- stale device age
- retry/circuit-breaker policy
- per-tenant default quotas
- encryption key/KMS/Vault config
- dry-run mode
- analytics retention
- payload redaction controls

Required env vars include:
- PUSH_FCM_ENABLED
- PUSH_APNS_ENABLED
- PUSH_WEBPUSH_ENABLED
- PUSH_HMS_ENABLED
- PUSH_WNS_ENABLED
- PUSH_CREDENTIAL_ENCRYPTION_KEY
- PUSH_FANOUT_FAST_THRESHOLD
- PUSH_FANOUT_SHARD_SIZE
- PUSH_FANOUT_SYNC_THRESHOLD
- PUSH_BACKPRESSURE_LAG_THRESHOLD_SECS
- PUSH_PUBLISH_STATUS_TTL_DAYS
- PUSH_FAILURE_THRESHOLD
- PUSH_SCHEDULER_INTERVAL_SECS
- PUSH_STALE_DEVICE_MAX_AGE_DAYS
- PUSH_ANALYTICS_ENABLED
- PUSH_DEFAULT_ACCEPTANCE_RPS
- PUSH_DEFAULT_DELIVERY_QUOTA_DAILY
- PUSH_DEFAULT_FANOUT_MAX
- PUSH_DEFAULT_INFLIGHT_MAX

Docs:
- `docs/content/2.server/23.push-notifications.md`
- `docs/content/2.server/24.push-capacity-planning.md`
- `docs/content/5.reference/3.config-reference.md`
- `docs/content/5.reference/2.http-endpoints.md`
- `FEATURE_COMPARISON.md`

Capacity planning doc:
- backend tier selection
- broker selection across every supported queue backend
- provider quota uplift guidance
- sizing formulas for API, planner, worker, store, broker
- cost model caveats
- failure runbook
- deployment modes: monolith and distributed

Docs must be honest:
- All backend support is documented.
- Hyperscale recommendations are clearly marked.
- Provider quota limits are called out.
```

## Prompt 4.5-10: Test Matrix, Load, Chaos, And Soak

```text
Build and run the full verification matrix.

Unit tests:
- domain invariants
- cursor pagination
- payload transforms
- template locale fallback
- per-tenant token bucket
- adaptive limiter
- circuit breaker
- weighted fair queue
- fast-path and shard-path planner

Integration tests:
- async publish 202 -> status polling -> completion
- sync publish under threshold
- forced async over threshold
- every list endpoint cursor pagination
- quota 429
- delivery quota exceeded
- fanout cap
- backpressure 503
- channel `extras.push`
- V1 publish unchanged

Backend conformance:
- memory
- postgres
- mysql
- dynamodb
- surrealdb
- scylladb

Queue conformance:
- memory
- redis
- redis-cluster
- nats
- pulsar
- rabbitmq
- google-pubsub
- kafka
- iggy
- sqs
- sns

Provider tests:
- FCM mock
- APNs mock HTTP/2
- Web Push mock
- HMS mock
- WNS mock
- at least one real provider smoke before release, with credentials supplied out of band

Fanout-at-scale:
- 10K fanout
- 100K fanout
- 1M fanout
- 10M smoke
- 100M smoke with mock provider and no OOM

Load:
- 50K publishes/sec sustained for 5 minutes where test cluster supports it.
- 100K burst for 30 seconds.
- mixed direct/small/medium/large fanout.
- noisy-neighbor test.

Chaos:
- provider 503
- provider 429
- broker node kill
- storage node kill
- planner node kill
- feedback partition

Soak:
- 24h where practical, otherwise documented shorter CI soak plus release-candidate soak.

Fix all failures before completing this prompt.
```

## Prompt 4.5-10b: Performance And SLO Gates

```text
Before release sign-off, run SLO validation and commit:
`docs/research/ably-parity/sockudo-push-perf-results-4.5.md`.

Release candidate baseline:
- Scylla cluster for hyperscale path.
- Kafka/Redpanda or Iggy/NATS JetStream according to ADR.
- publish API nodes.
- fanout planner nodes.
- provider worker pools.
- feedback consumers.
- mock providers with realistic latency distributions.

Gates:
1. Acceptance latency: p99 < 50ms.
2. Single-recipient e2e: p99 < 1s.
3. Mid-fanout: 1K publishes/sec at fanout 10K, p99 completion < 10s.
4. Mega-fanout: fanout 1M, p99 completion < 30s.
5. Sustained mixed throughput without lag accumulation.
6. Burst handling with no silent drops.
7. Tenant fairness, p99 degradation <= 20%.
8. Provider failure isolation.
9. Token-store stress.
10. Memory/task stability soak.

Results doc must include:
- topology
- exact commands
- feature flags
- workload generator config
- p50/p99/p99.9
- throughput
- resource utilization
- headroom
- bottlenecks
- pass/fail

If any gate fails, document the blocker and stop release progression.
```

## Prompt 4.5-11: Observability, Meta-Channel, Dashboards

```text
Add metrics, logs, push meta-channel events, and dashboards.

Metrics:
- `sockudo_push_dispatched_total{provider,status,app}`
- `sockudo_push_dispatch_duration_seconds{provider,app}`
- `sockudo_push_dispatch_inflight{provider,app}`
- `sockudo_push_publish_accepted_total{app,result}`
- `sockudo_push_publish_acceptance_duration_seconds{app}`
- `sockudo_push_publish_log_lag_seconds`
- `sockudo_push_planner_duration_seconds`
- `sockudo_push_fanout_size{app}`
- `sockudo_push_delivery_jobs_emitted_total{provider,app}`
- `sockudo_push_delivery_jobs_lag_seconds{provider}`
- `sockudo_push_worker_pool_size{provider}`
- `sockudo_push_worker_pool_busy{provider}`
- `sockudo_push_provider_connections{provider,tenant}`
- `sockudo_push_provider_streams_active{provider,tenant}`
- `sockudo_push_circuit_breaker_state{provider,tenant}`
- `sockudo_push_circuit_breaker_open_total{provider,tenant}`
- `sockudo_push_rate_limiter_throttled_total{provider,tenant}`
- `sockudo_push_duplicate_suppressed_total`
- `sockudo_push_devices_total{app,state}`
- `sockudo_push_device_state_transitions_total{from,to,app}`
- `sockudo_push_token_invalidations_total{provider,app}`
- `sockudo_push_quota_acceptance_rejections_total{app}`
- `sockudo_push_quota_delivery_rejections_total{app}`
- `sockudo_push_quota_consumed_acceptance{app}`
- `sockudo_push_quota_consumed_delivery{app}`
- `sockudo_push_channel_publish_total{channel}`
- `sockudo_push_scheduled_jobs_total{status}`
- `sockudo_push_scheduler_lag_seconds`
- `sockudo_push_stale_devices_removed_total{app}`
- `sockudo_push_rate_dropped_total{app}`
- `sockudo_push_rate_queued_total{app}`
- `sockudo_push_delivery_status_total{status,app}`
- `sockudo_push_wfq_dispatched_total{provider,app}`
- `sockudo_push_wfq_starvation_seconds{provider,app}`

Logs:
- publish accepted/completed
- dispatch success/failure
- first delivery failure
- state transition to FAILED
- circuit breaker state changes
- backpressure 503
- quota rejection
- credential load failure

Meta-channel:
- push accepted
- publish completed
- device state changed
- provider rejected
- token invalidated
- quota event
- scheduler event
- circuit breaker event
- dead letter

Dashboard:
- add `ops/dashboards/push.json`.
```

## Prompt 4.5-12: Final Audit And Release Decision

```text
Run the final release audit.

Functional:
- FCM end-to-end verified.
- Web Push end-to-end verified.
- APNs credential load and dispatch mock/real verified.
- HMS mock verified.
- WNS mock verified.
- channel push nonblocking.
- scheduled jobs fire, dedupe, and cancel.
- delivery status updates last_active_at.
- registry state transitions correct.
- credential GET never returns secrets.
- push-subscribe cannot access another client.
- push-admin has full access.

Backend:
- Every promised storage backend passes conformance or is documented as blocked before release.
- Every promised queue backend passes conformance or is documented as blocked before release.
- Memory backend clearly tests/dev only.

Scale:
- All 4.5-10b gates pass or release is no-go.
- Capacity model matches observations within +/-20% where measurable.
- async publish, status polling, cursor pagination, quotas, fanout cap, fairness, circuit breakers, backpressure all verified.
- distributed-mode deployment works end to end.

Observability:
- all metrics fire with correct labels under load.
- dashboard renders.
- `[meta]log:push` emits required event types.

Docs:
- provider setup guides accurate.
- config reference complete.
- API reference complete.
- capacity planning doc honest.
- troubleshooting covers provider and backend failure modes.

Deliver:
1. Findings and evidence.
2. Go/no-go decision.
3. Blockers if no-go.
4. Release note draft.
5. Capacity statement based on measured data.
6. Readiness statement for 4.6.
```

## Required Final Block For Every Implementation Prompt

Every implementation prompt must end with this checklist:

```text
Before finalizing:
- Run the narrowest meaningful tests first, then widen.
- Read every failure output.
- Fix failures instead of reporting around them.
- Confirm `push` disabled behavior.
- Confirm feature-gated compile behavior for touched features.
- Confirm provider credentials, tokens, endpoints, and payloads are redacted.
- Confirm all touched backends either pass conformance or fail with explicit startup/config errors.
- Confirm no unrelated user changes were reverted.
- Report changed files, verification, and remaining risks.
```

Every commit must follow the repository Lore Commit Protocol. Example trailers:

```text
Confidence: medium
Scope-risk: broad
Tested: cargo test -p sockudo-push
Not-tested: real APNs/FCM provider calls; mock provider tests only
Directive: Do not claim provider delivery throughput without provider quota and benchmark evidence
```
