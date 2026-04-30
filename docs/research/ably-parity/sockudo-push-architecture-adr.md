# ADR: Sockudo Push Notification Platform Architecture

Date: 2026-04-30
Status: Accepted for release 4.5 implementation planning

## Context

Sockudo release 4.5 adds an optional push notification platform with Ably-style
activation and publishing surfaces, Centrifugo-style operator controls, multiple
provider integrations, and support for all existing Sockudo storage and queue
families where feasible.

The platform must meet conflicting constraints:

- preserve minimal builds and V1 compatibility
- support small deployments and large multi-node deployments
- avoid provider-client rewrites where existing crates are suitable
- preserve provider error detail for stale cleanup, retry, status, and circuit
  breakers
- distinguish Sockudo dispatch capacity from provider delivery limits
- make reliability mode durable without requiring every backend to be
  hyperscale-recommended

This ADR binds the architecture before code is written.

## Decision

### Crate And Feature Boundaries

Create `crates/sockudo-push` as the primary implementation crate.

Expose `sockudo-server` feature `push` and provider subfeatures:

- `push-fcm`
- `push-apns`
- `push-webpush`
- `push-hms`
- `push-wns`

Reuse existing storage and queue features instead of inventing push-specific
backend feature names unless a backend requires a push-only guard.

`push` enables the platform surfaces but must not force every provider
dependency into trimmed builds. `full` may enable all provider and backend
features after dependency size and TLS posture are accepted.

### Public API Boundary

Expose push as HTTP/admin APIs, not as V1 WebSocket protocol extensions.

Required API groups:

- activation
- device registry
- channel push subscriptions
- direct publish
- batch publish
- async publish
- channel push publish
- scheduled publish
- cancellation
- status
- dead letters
- credential rotation
- stale cleanup

Every list endpoint uses opaque cursor pagination.

Async publish returns `202 Accepted` and an opaque `publish_id`.

### Internal Pipeline

The push pipeline is split into durable stages:

1. accept and validate request
2. transform payload into provider-neutral intent
3. persist publish record
4. plan direct or channel fanout
5. enqueue dispatch jobs
6. dispatch to provider
7. classify feedback
8. update per-device and publish status
9. retry, expire, cancel, or dead-letter
10. roll up analytics and metrics

Scheduled delivery is storage-led. Queue delay mechanisms are optimizations only.

### Required Abstractions

Define Sockudo-owned abstractions before backend implementations:

- `PushStore`
- `PushQueue`
- `PushProvider`
- `PushPlanner`
- `PayloadTransformer`
- `PushFeedback`
- `PushMetrics`
- worker role modules for accept, plan, schedule, dispatch, retry, cleanup, and
  DLQ replay

Provider crates must sit behind `PushProvider` wrappers. Backend-specific code
must sit behind `PushStore` or `PushQueue` conformance tests.

### Provider Decisions

Use the dependency evaluation ADR as the provider dependency gate.

Initial provider direction:

- FCM: wrap or fork a HTTP v1 crate that preserves typed provider errors,
  currently favoring `fcm-rs` over `fcm_v1` because cleanup/retry logic needs
  structured error detail.
- APNs: adopt `apns-h2` behind a Sockudo wrapper.
- Web Push: prefer a rustls-clean wrapper around `web-push-native` and
  `ece-native`; use `web-push` only if TLS and OpenSSL/default-client bloat are
  resolved.
- HMS: implement a minimal Sockudo HTTP adapter because no acceptable Rust push
  provider crate was found.
- WNS: implement a minimal Sockudo HTTP adapter because no acceptable Rust WNS
  push provider crate was found.

Provider wrappers own:

- credential loading and cache invalidation
- request construction
- provider payload validation
- response parsing
- retry-after extraction
- invalid-token detection
- provider-specific redaction
- metrics labels and error classes

Retry scheduling remains in Sockudo, not inside provider crates.

### Storage Decisions

Support memory, PostgreSQL, MySQL, DynamoDB, SurrealDB, and ScyllaDB where
feasible.

Classify storage support in docs:

- memory: tests/development only
- PostgreSQL and MySQL: small/regional production
- DynamoDB and ScyllaDB: hyperscale-recommended candidates
- SurrealDB: supported but not hyperscale-recommended without performance proof

Storage owns authoritative state for:

- devices
- provider endpoints
- channel subscriptions
- publish records
- scheduled records
- fanout plan shards
- dispatch attempts
- feedback events
- dead letters
- analytics rollups
- credential metadata

### Queue Decisions

Support memory, Redis, Redis Cluster, NATS JetStream, Pulsar, RabbitMQ, Google
Pub/Sub, Kafka, Iggy, SQS, and SNS where feasible.

Queue suitability is stage-dependent:

- memory is tests/dev only
- Redis and Redis Cluster are small/regional only unless safer retry semantics
  are implemented
- NATS JetStream, Pulsar, Kafka, Google Pub/Sub, and Iggy are the main
  high-throughput candidates
- RabbitMQ and SQS are supported production backends with documented scaling
  limits
- SNS is publish fanout only and cannot be a complete worker queue by itself

`PushQueue` must normalize ack, retry, nack, redelivery, and dead-letter
behavior enough that reliability mode can be tested across backends.

### Payload Transformation

Provider payloads are produced by `PayloadTransformer`. The transformer accepts
Sockudo push intent, template data, localization context, provider/device
attributes, and channel context, then emits provider-specific requests.

Pass-through provider JSON is not sufficient for release 4.5. Escape hatches may
exist later, but they must still pass validation, size checks, redaction hints,
and provider compatibility checks.

### Credentials

Credential material must be encrypted at rest if stored by Sockudo. Credential
versions are explicit and rotation-aware.

Credential cache invalidation is required on:

- rotation
- provider disablement
- app disablement
- admin revocation
- credential validation failure where the provider indicates permanent auth
  invalidation

Secrets must be redacted from logs, metrics, status APIs, dead letters, and test
snapshots.

### Reliability, Fairness, And Backpressure

Reliability mode persists every state transition needed to recover after worker
death. Memory backend is invalid for production reliability mode.

The planner enforces:

- app and tenant quotas
- provider quotas
- fanout caps
- weighted fair queueing
- duplicate suppression
- cancellation and expiry checks
- backpressure when storage, queues, workers, or providers are degraded

Provider throttling is not a Sockudo failure unless Sockudo ignores it. Provider
quota limits must appear distinctly in status, metrics, and docs.

## Consequences

### Positive

- Minimal non-push builds remain small and compatible.
- Provider error detail is preserved for stale cleanup and retry decisions.
- The same architecture supports monolith/dev mode and distributed worker mode.
- Storage and queue backends can be documented honestly as supported or
  hyperscale-recommended without pretending all backends have equal scale.
- Cursor pagination and status records are architectural requirements, not late
  API polish.

### Negative

- The release needs substantial conformance testing before provider work can be
  considered production-ready.
- Supporting all existing storage and queue families increases schema and
  operational documentation work.
- Provider wrappers add code even where an external crate exists.
- Hyperscale claims require load evidence and cannot be inferred from backend
  choice alone.

### Neutral

- HMS and WNS require Sockudo-owned HTTP adapters because no acceptable Rust
  provider crates were found.
- SNS is supported only as fanout ingress to another consumer path, not as a
  complete push worker queue.

## Rejected Alternatives

### Put Push Into `sockudo-server`

Rejected because provider dependencies, worker logic, and backend conformance
would bloat the binary crate and make feature isolation harder.

### Use One Unified Push Crate

Rejected because available unified crates hide provider-specific error detail,
lag provider-specific crates, or pull broad dependencies that conflict with
Sockudo's TLS and feature posture.

### Treat Provider JSON As The Public API

Rejected because release 4.5 requires templates, localization, redaction,
validation, provider portability, and operator-visible status. A transformation
layer is required.

### Broker-Led Scheduling

Rejected because cancellation, expiry, status, and recovery require storage to
be the authoritative schedule source.

### Claim Hyperscale For Every Supported Backend

Rejected because support and recommendation are different. Smaller backends
remain useful, but 100M fanout and 1B device registry claims require measured
proof.

## Verification Plan

Before release 4.5 ships:

- Run compile checks for all required feature combinations.
- Run `PushStore` conformance tests for every storage backend.
- Run `PushQueue` conformance tests for every queue backend and stage.
- Run provider mock tests for FCM, APNs, Web Push, HMS, and WNS.
- Run at least one real provider end-to-end validation.
- Run load tests proving acceptance, planner latency, dispatch throughput,
  fairness, and backpressure.
- Run chaos tests for worker death, provider throttling, provider outage, queue
  redelivery, storage failover, credential rotation, cancellation races, and
  cleanup races.
- Run soak tests that prove bounded memory, queue depth, retry backlog, and
  storage growth.

## Follow-Up Records

Expected follow-up ADRs:

- push API route and payload schema ADR
- push storage schema ADR
- push queue stage mapping ADR
- provider wrapper ADRs for FCM, APNs, Web Push, HMS, and WNS
- metrics and Grafana dashboard ADR
- rollout and migration ADR
