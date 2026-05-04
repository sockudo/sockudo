# Sockudo Push Performance Results 4.5

Date: 2026-04-30
Status: blocked - release progression stopped

## Decision

Release 4.5 push is not signed off for release. The required release-candidate
SLO validation was not run because this workspace does not include a provisioned
ScyllaDB hyperscale cluster, a durable high-scale broker cluster, publish API
nodes, planner nodes, provider worker pools, feedback consumers, or mock
providers with realistic latency distributions.

No public performance, hyperscale, or SLO pass claim is made by this document.
The blocker is release-candidate infrastructure and workload harness
availability, not a relaxation of the binding push architecture decisions.

## Binding Architecture Preserved

- Async publish must return `202 Accepted` only after the initial status record
  is persisted and the publish-log event is durable.
- The Publish API must not synchronously fan out to providers.
- Fast-path fanout remains `fanout < PUSH_FANOUT_FAST_THRESHOLD`; default
  `PUSH_FANOUT_FAST_THRESHOLD=10000`.
- Shard-path fanout remains `fanout >= PUSH_FANOUT_FAST_THRESHOLD`; default
  `PUSH_FANOUT_SHARD_SIZE=100000`.
- Shard-path fanout must be resumable and must stream bounded token pages.
- Provider dispatch must use weighted-fair tenant scheduling,
  provider-specific queues, bounded concurrency, retry-after handling, and
  circuit breakers.
- Provider feedback must update device state, stale-token cleanup, status
  counters, metrics, retry/DLQ state, and `[meta]log:push`.
- Backend claims remain limited to the tier matrix in the release contract,
  architecture ADR, dependency evaluation, and capacity model.

## Topology

Required release-candidate topology:

| Component | Required shape | Result |
| --- | --- | --- |
| Device registry | ScyllaDB cluster for the hyperscale self-managed path. | Not provisioned. |
| Durable broker | Kafka/Redpanda, Iggy, or NATS JetStream according to ADR limits. | Not provisioned. |
| Publish API | Independently scaled API nodes. | Not provisioned. |
| Fanout planning | Planner nodes plus shard workers. | Not provisioned. |
| Provider dispatch | Provider-specific worker pools with fair scheduling and circuit breakers. | Not provisioned. |
| Feedback | Durable result queues plus feedback consumers. | Not provisioned. |
| Providers | Mock FCM, APNs, Web Push, HMS, and WNS endpoints with realistic latency distributions and error classes. | Not provisioned. |
| Observability | Metrics capture for p50, p99, p99.9, throughput, lag, resource utilization, retries, DLQ, and headroom. | Not provisioned. |

## Feature Flags

Required release-candidate build families:

```bash
cargo build -p sockudo --features "push,push-apns,scylladb,iggy"
cargo build -p sockudo --features "push,push-fcm,scylladb,kafka"
cargo build -p sockudo --features "push,push-webpush,scylladb,nats"
cargo build -p sockudo --features "full"
```

These build commands are part of the required validation set. They are not a
substitute for SLO validation and do not establish performance pass/fail.

## Exact Commands

The release-candidate SLO run is blocked until the cluster and workload harness
exist. The expected command shape is:

```bash
scripts/push-verification-matrix.sh local

PUSH_FANOUT_FAST_THRESHOLD=10000 \
PUSH_FANOUT_SHARD_SIZE=100000 \
PUSH_FANOUT_PAGE_SIZE=1000 \
PUSH_PROVIDER_BATCH_SIZE=500 \
PUSH_STORE=scylladb \
PUSH_QUEUE=kafka \
PUSH_PROVIDER_MODE=mock-realistic-latency \
push-loadgen acceptance --rate 50000 --duration 5m

push-loadgen direct --recipients 1 --rate 50000 --duration 5m

push-loadgen fanout --publishes-per-sec 1000 --fanout 10000 --duration 5m

push-loadgen fanout --publishes 100 --fanout 1000000 --duration 30m

push-loadgen mixed --profile release-4.5-mixed --duration 30m

push-loadgen burst --profile release-4.5-burst --duration 5m

push-loadgen fairness --noisy-tenant-multiplier 10 --duration 10m

push-loadgen provider-failure --providers fcm,apns,webpush,hms,wns --duration 10m

push-loadgen token-store --devices 100000000 --channels 1000 --duration 30m

push-loadgen soak --profile release-4.5-mixed --duration 24h
```

`push-loadgen` is a required release-candidate workload generator interface for
this results contract. It is not present in this workspace, so none of the SLO
commands above were executed for this document.

## Workload Generator Config

Required config for the release-candidate run:

| Setting | Required value |
| --- | --- |
| Store | ScyllaDB, bucketed channel/device indexes, push schema applied. |
| Queue | Kafka/Redpanda, Iggy, or NATS JetStream with durable publish, shard, delivery, result, retry, and DLQ stages. |
| Fanout threshold | `PUSH_FANOUT_FAST_THRESHOLD=10000`. |
| Shard size | `PUSH_FANOUT_SHARD_SIZE=100000`. |
| Token page size | `PUSH_FANOUT_PAGE_SIZE=1000`. |
| Provider batch size | `PUSH_PROVIDER_BATCH_SIZE=500`. |
| Providers | Mock providers for FCM, APNs, Web Push, HMS, WNS. |
| Latency distributions | Provider-specific p50/p99/p99.9 distributions, retry-after, 429, 5xx, invalid-token, auth failure, and quota classes. |
| Tenants | Baseline tenants plus one noisy tenant at 10x quota for fairness testing. |
| Observability | Per-stage latency histograms, throughput, lag, retry/DLQ counters, resource utilization, and headroom. |

## Gate Results

| Gate | Required SLO | p50 | p99 | p99.9 | Throughput | Resource utilization | Headroom | Bottleneck | Result |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |
| Acceptance latency | p99 `< 50ms` | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | No RC topology or load generator. | Blocked |
| Single-recipient e2e | p99 `< 1s` | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | No RC topology or mock provider harness. | Blocked |
| Mid-fanout | 1K publishes/sec at fanout 10K, p99 completion `< 10s` | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | No Scylla/broker/load generator cluster. | Blocked |
| Mega-fanout | fanout 1M, p99 completion `< 30s` | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | No shard-path RC run. | Blocked |
| Sustained mixed throughput | No lag accumulation | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | No sustained mixed workload run. | Blocked |
| Burst handling | No silent drops | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | No burst run with durable loss accounting. | Blocked |
| Tenant fairness | p99 degradation `<= 20%` | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | No noisy-neighbor RC run. | Blocked |
| Provider failure isolation | Provider failures isolated by queue, retry-after, and circuit breaker state | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | No mock provider failure matrix. | Blocked |
| Token-store stress | Token scans, cleanup, and shard pagination stable | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | No Scylla token-store stress run. | Blocked |
| Memory/task stability soak | No memory/task leak during soak | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | No 24h soak run. | Blocked |

## Release Blocker

Release progression is stopped until the SLO matrix above is rerun on the
release-candidate topology and every gate records measured p50, p99, p99.9,
throughput, resource utilization, headroom, bottlenecks, and pass/fail.

The next valid update to this document must replace the blocked rows with
measured results or keep the release in no-go state with concrete failed-gate
blockers.
