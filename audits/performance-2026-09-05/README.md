# Sockudo performance audit — 2026-09-05

**All 20 Rust packages were assessed at commit `5613bb291032b6b7660352974b03da9eb0646da0`.** The strongest opportunities are repeated subscriber preparation, retention-dependent scans, unbounded work during dependency failures, and database pagination that does not bound the underlying work. This audit records 45 recommendations/cost findings across production and public-library paths, with workload-specific priorities. It does not claim 45 measured production bottlenecks.

No production behavior, configuration, dependency manifest or lockfile was changed. Everything added is under this audit directory: reports, a standalone non-published diagnostic package, its own lockfile, and raw results. No commits or publishing.

Read the [ranked findings below](#ranked-recommendations), then the detailed [core/server/SDK](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/CORE-SERVER.md), [fanout/protocol/filter/delta/AI](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/FANOUT.md), [app/cache/queue/limiter/metrics/webhook](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/SERVICES.md), and [push/Ably/tools](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/PUSH-TOOLS.md) reports. Each gives exact code references, mechanisms, smallest changes, correctness constraints and reproduction/verification requirements. [Validation](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/VALIDATION.md) distinguishes executed measurements from future work. [Manifest inventory](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/MANIFESTS.md) and [full manifest data](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/inventory.json) enumerate features and direct dependencies, including optional and auxiliary crates.

## What was measured

These are optimized **component/store** results on a shared Ryzen 9 7950X desktop, not production capacity numbers. Different rows are different workloads; do not combine them into a throughput estimate. No optimized production patch was applied.

| Current implementation / controlled workload | Evidence | Interpretation |
|---|---|---|
| Conflation extraction, 1,000 visits to one ~64KiB JSON payload | Original median 5.350ms when repeated, 5.723µs when extracted once and borrowed; rerun 5.485ms / 5.688µs | Actual extraction method; excludes sockets, delta algorithm, envelopes and network. Demonstrates avoidable repeated work, not a full-fanout gain. |
| Memory queue: 500 ordinary enqueues with 30k retained dedup IDs | 14.658–14.708ms versus 0.251–0.261ms for an equal-size queue without dedup state | The full-map dedup sweep dominates this controlled comparison. |
| Provider token source, 64 concurrent callers, scripted 5ms refresh | Healthy: one refresh/~5–6ms; failed: 64 refreshes/~387–389ms | Actual token cache code coalesces only success; failures serialize repeated work. |
| Annotation projection rebuild, 100 → 10k retained events | p50 18.8–19.0µs → 2.44–2.55ms across two runs | Full replay/sort and allocation cost grows with retained annotations. |
| Count-capped presence-history insert, cap 100 → 10k | p50 5.04µs → 1.114ms; uncapped control ~0.661µs | Eviction rebuilds the entire latest-user map. |
| Mutable update, 256B state, ~1,024 starting revisions | p50 77.8µs, ~112KB requested allocation bytes per operation | Chain checks repeatedly build indexes while holding a global store lock; sampled chain grows during the run. |
| 2,000 × 64B append fragments | Latest string 128,000B; retained historical strings total 128,064,000B | Actual version readback, excluding metadata/envelopes; full accumulated snapshots create quadratic storage. |
| Memory history, near-tail 100-item page over 100k retained rows | p50 141–143µs versus first-page 6.15–9.80µs | Deep cursor seeking is linear; first pages are bounded by requested results. |
| Memory cache: 10k entries × 4KiB, pages of 256 | Complete sweep 368–384ms | Each page rebuilds/sorts a full matching list. |
| 128 concurrent cold app lookups | 128 backend calls and 256 shared-cache SETs for an existing app | Mocked 20ms lookup/2ms SET; deterministic call amplification, not real DB latency. |
| Push identity hash/verify, 120k PBKDF2 iterations | Median 8.82ms / 8.79ms, 21 samples | Necessary cryptographic work runs synchronously in async HTTP/Ably handling; offload with bounded admission. |

Raw p50/p95/p99 store samples, allocation-request counters, process user/system time, peak RSS, repeat variability and environment are preserved. Short samples do not establish production tail latency. The existing version-streaming Criterion suite also ran successfully; timings below its named single-operation budgets do not cover retained-state scaling or constitute the full budget guard.

## Coverage: inspection versus execution

The root workspace has 17 members. `sockudo-http` is explicitly excluded; `bench-sender` and `sockudo-fuzz` declare separate workspaces. The audit diagnostic package is new and excluded from the count.

| Package | Responsibilities and inspected paths | Findings / conclusion | Runtime coverage and gaps |
|---|---|---|---|
| sockudo-protocol | messages, versioned payloads, JSON/MessagePack/Protobuf conversion, V1/V2 prefixing, AI headers and tests | F8; bounded header helpers otherwise no new substantive finding | 39 tests passed with AI feature; no live binary fanout validation |
| sockudo-filter | node/ops/predicate AST, EQ/IN/event/JMESPath, property tests, bench | No independent substantive defect established; F4 is its adapter caller | 54 tests passed; legacy unused FilterIndex probe excluded from production ranking |
| sockudo-core | auth/options/traits; namespace/presence; sender/buffers; history, versions, annotations, presence history, envelopes, idempotency, memory pressure | C1–C5, C7, C10; shared constraints throughout | 357 tests passed; actual memory store/presence probes and version Criterion; no multi-node store load |
| sockudo-app | factory, memory/cached app managers, MySQL/Postgres/Dynamo/Scylla/Surreal lookups/lists/pools | S2/S3 | Actual cached manager with controlled I/O; persistent DB timing absent |
| sockudo-cache | factory, Moka, Redis/Sentinel/Cluster, CAS/TTL/prefix scans, fallback | S4; fallback lock concern is not factory-wired | Actual memory pagination; no live Redis/cluster timing |
| sockudo-queue | memory supervisor/dedup; Redis backend; NATS/RabbitMQ/Kafka/Iggy/Pulsar/PubSub/SQS/SNS, retries/leases/shutdown | S1/S8–S10; SNS publish-only, no separate finding | Actual memory admission probe; no live broker callback/backpressure tests |
| sockudo-rate-limiter | memory sweep, Redis/cluster Lua admission, reconnect, IP middleware | S11; no new steady-state memory-limiter finding | Feature compilation; concurrent Redis outage not exercised |
| sockudo-metrics | recorder/exporters, handles/labels, interface methods, process/Tokio sampling, rendering | S5/S6 | Feature compilation/source call tracing; no cardinality/RSS/scrape load |
| sockudo-webhook | integration, event gates, batch buffer, sender/HMAC/HTTP retry, Lambda cache, bench | S7/S12/S13 | Full feature compilation; HTTP outage/Lambda validation pending |
| sockudo-delta | compression/conflation/state/messages, coordination Redis/NATS, tests and benches | F1/F3 | Current-source extraction probe; no distributed sequence/codec load |
| sockudo-push | all stores/providers, registration/targeting, pipeline, queues/status/feedback, scheduler/repair/cleanup, metrics, bench/conformance | P1–P8 and C12 | Actual token-cache and PBKDF2 probes; full feature compilation; providers/DBs not exercised |
| sockudo-ai-transport | rollup shards/deadline heaps/limits/tokens, validation/conformance and observability | No new independent engine defect established; F6 is adapter scheduler integration | 18 tests passed; offline fixtures; no timed live rollup delivery |
| sockudo-ably-compat | auth/ownership/revocation, WS/REST, native publishing bridge, stats, push, recovery/codec/outbound | A1–A4; P1/C12 cross-path | Full feature compilation; no live Ably/cache/delta load |
| sockudo-adapter | connection/auth/subscribe/disconnect, publish, fanout, presence, versions, history/rewind/recovery, annotations, AI; all transports | F1–F7; C/S/P cross-crate findings | Full feature compilation; component probes; real socket queue/slow-consumer load absent |
| sockudo (server) | router/auth middleware, HTTP/WS entry, bootstrap/shutdown/cleanup/logging; all five history/version/annotation DB stores; push bridge | C6–C9/C12, S2/S12 and queue payload costs | Minimal, selected V2+AI+Redis+Postgres+push and full feature checks passed; no end-to-end server capacity test |
| sockudo-simulator | deterministic workloads/fault scheduler/oracles, real memory subsystems, push lab/shrinker | No substantive standalone performance finding | Inspected; no simulator run; logical time is not throughput evidence |
| sockudo-ai-benches | all hot-path categories, intra-node models, capacity budgets and guard | No production runtime; synthetic benchmark limitations documented | Manifests/source inspected; full AI benchmark guard not run |
| bench-sender | complete paced HTTP sender, CLI/manifest | No defect in paced smoke purpose; cannot establish saturation or tail latency | Inspected; no sender load run |
| sockudo-fuzz | all 14 malformed-input/roundtrip/continuity harnesses, manifests/corpus | No substantive standalone optimization; oracle work intentional | Inspected; no fuzz campaign or sanitizer run |
| sockudo-http (Rust SDK) | public README/manifest; config/client/pool/retry, event/auth/history/presence/push/token/webhook APIs/tests | C11; existing client pooling retained | Tests attempted but offline `crypto_secretbox` dependency unavailable |

The feature map includes all provider/storage/broker flags and their propagation. `v2` = delta/tag filtering/recovery, while versioned messages, push and AI Transport have separate gates. `full` is not assumed to be the only build: the minimal and selected combinations were separately checked. Adapter `full` does not by itself enable AI Transport. Rust SDK encryption/TLS combinations remain uncompiled.

## End-to-end execution map and multiplying dimensions

Symbols: S subscribers, K subscriptions per socket, C channels, N nodes, H retained history, V revisions per message, A annotation events, D devices/dedup keys (context specific), Q queued work.

| Execution path | Flow and scaling-sensitive boundaries |
|---|---|
| WebSocket connect/auth | Server protocol/wire config and memory admission → adapter app lookup/origin/capability checks → indexed connection/namespace state → bounded sender/task lifecycle. Cold waves multiply app lookups (S3); Ably has extra per-connection ownership/revocation timers (A1). |
| Subscribe/disconnect | Channel auth/predicates → exact/wildcard membership → presence first/last transition → durable transition/history/annotation summary → attach serial/rewind gate → subscription response. Disconnect cancels/removes indexed state or enters bounded deferred cleanup. Work follows K, members and due leaves; durable history adds H-dependent costs (C4), summary rebuild adds A (C3). |
| HTTP/WS immutable publish | HTTP raw-body signing/auth or verified WS actor → validation/limits/idempotency → shared MessageService/acceptance → canonical serial/history/version evidence → local fanout and horizontal publish → subscriber predicate/protocol projection → bounded socket send. HTTP multi-channel work repeats per channel; idempotency remains canonical. C6 can limit durable writer throughput; C9 bounds input buffering. |
| Mutable create/update/delete/append | Actor/capability authorization → per-channel ordered atomic version-store commit → history/original mutation side effects → webhook/push → V2 full/delta append projection → local/cluster delivery. Four serial identities and original creator preserved. C1/C2; batching cannot erase originals. |
| Local fanout | Candidate exact+wildcard socket set → predicate checks → append-mode/V1/V2 partition → shared JSON bytes or delta grouping → bounded sender. Unavoidable O(S) delivery; F1 adds O(S*payload), F4 can add O(S*K), sparse replay allocation follows C*capacity (F7). |
| Distributed fanout | Shared HorizontalAdapterBase serializes broker envelope → Redis/Redis Cluster/Sentinel, NATS, Kafka, RabbitMQ, Pulsar, Google Pub/Sub or Iggy → receiver parse/app resolution → same local fanout. Traffic grows with N including nodes with no recipients. Redis/NATS detached handlers break ingress bounds (F2); other brokers have serial callback/prefetch/partition constraints. |
| Presence queries/history | Indexed membership and distributed first/last checks are distinct from history transition log. History wrapper checks dedupe/state → reserves/appends dedicated presence-history stream → snapshots replay with continuity checks. C4 plus backend I/O, count queries can materialize members; do not drop necessary transition order locks. |
| History/rewind/recovery/until_attach | Hot bounded replay by stream/serial → cold HistoryStore if needed → fail-closed contiguous validation → mutable latest substitution → filtering → gated replay/live drain. Page limit does not guarantee bounded work (C5/C7/C8). Attach serial and count/byte gate protect gaplessness; preserve them. |
| Annotation raw/summary | Separate actor authorization → idempotent annotation event store → projection rebuild/revision → raw delivery only to entitled V2 sockets and summary projection to subscribers. C3 and metric cardinality S5; all summarizers/deletes need parity. |
| Push | Native HTTP/event/Ably admission → immutable template/rendered validation/idempotency/status/log → target enumeration/shards → transport queue envelope → bounded provider chunks/token+HTTP → per-recipient feedback → status CAS, registry updates, retry/DLQ/scheduler/repair/cleanup. P1–P8/S1/S8–S10/C12; Q, D, batch size and campaign recipients interact. |
| AI Transport | Edge extras validation → ordinary durable version mutations → rollup engine keeps egress-only pending appends by shard/deadline → adapter timer claims token under channel ordering gate → deferred delivery/retry. F6 worker coupling, C1/C2 store costs; orphan sweeps amplify cache pagination (S4). |
| Shared services | App lookup/cache and rate admission precede work; metrics run throughout; logs are structured with a fixed subscriber; webhooks serialize/sign and queue/deliver using shared pools. S2/S3/S5–S13, P8; count/byte/latency bounds must span stages rather than one queue. |

## Ranked recommendations

Ranking combines likely affected workload, mechanism/evidence strength, implementation effort and regression risk. **H/M** in Impact mean high/medium; **S/M/L** effort mean small/medium/large; Risk L/M/H mean low/medium/high. **M** evidence includes controlled component/mocked-dependency measurements; **C** means code-backed only. A high-impact optional feature matters only if enabled. Implementation order can differ from severity because correctness work is a prerequisite.

| Rank | ID / recommendation | Impact | Evidence | Effort | Risk |
|---:|---|:---:|:---:|:---:|:---:|
| 1 | F1 Hoist conflation extraction and shared delta encoding | H | M/C | S–M | M |
| 2 | S1 Replace per-enqueue full dedup expiry scans | H | M | M | M |
| 3 | P3 Share failed token-refresh outcomes | H during outage | M | S–M | M |
| 4 | S12 Share Lambda regional client cache across deliveries | H if Lambda | C | S | L |
| 5 | C9 Enforce HTTP body limit before auth collection | H resource bound | C | S | M |
| 6 | F2 Bound distributed ingress tasks and queued bytes | H during overload | C | M | H |
| 7 | S7 Bound webhook batch admission and failed drain | H during outage | C | M | M |
| 8 | P1 Index client-scoped push targeting and Ably summary | H large registries | C | M | M |
| 9 | P2 Make push storage page work truly bounded | H large registries | C | M | M |
| 10 | C6 Incremental/bounded durable history maintenance | H durable workloads | C | L | H |
| 11 | C7 Implement selective latest-version batch reads | H durable history | C | M | M |
| 12 | C8 Bound Scylla history fetches before materialization | H Scylla recovery | C | M | M |
| 13 | C4 Remove presence-history replay from steady inserts | H churn | M/C | M–L | H |
| 14 | C1 Index version invariants/latest and partition state | H long revisions | M/C | M–L | H |
| 15 | C3 Incremental annotation summaries/duplicate outcome reuse | H busy projections | M/C | M–L | H |
| 16 | C12 Bound and offload device-identity KDF work | H crypto storms | M/C | S–M | M |
| 17 | P6 Retain/repair push work with bounded lifecycle scans | H lifetime scale | C | M | H |
| 18 | S2 Remove full-app rewrites from readiness | H many apps | C | M | M |
| 19 | A1 Share Ably revocation work with strict freshness | H idle clusters | C | L | H |
| 20 | S4 Bound memory cache pagination and pipeline Redis GETs | H many streams | M/C | M | M |
| 21 | S3 Coalesce cold app loads and bound negative caching | H reconnect storms | M/C | M | M |
| 22 | F6 Keep AI deadline polling progressing during slow egress | H tail latency | C | M–L | H |
| 23 | S5 Bound detailed annotation metric cardinality | H churn/scrapes | C | M | M |
| 24 | F3 Atomic Redis delta coordination without global I/O mutex | H if enabled | C | M | M–H |
| 25 | F7 Lazy replay buffer slot allocation | M sparse channels | C | S | L–M |
| 26 | C5 Indexed memory history serial seeking | M deep reads | M | M | M |
| 27 | C10 Reclaim expired idle memory history payloads | H write-once churn | C | M | M |
| 28 | A2 Reduce isolated stats ACK batching delay | H isolated latency | C | S–M | M |
| 29 | P4 Share common data in durable push batch envelopes | H broker bytes | C | M | H |
| 30 | P5 Batch durable successors/status deltas safely | H campaigns | C | L | H |
| 31 | P7 Stop rescanning entire memory push backlog per consume | M backlog | C | M | M |
| 32 | S8 Redis worker dispatch based on available capacity | M skewed jobs | C | M | M |
| 33 | S11 Single-flight Redis limiter reconnect | H failures | C | M | M |
| 34 | S13 Isolate webhook retry occupancy and bound error reads | M failures | C | M–L | M–H |
| 35 | A3 Range-bound stats reads and memory expiry | M long retention | C | M | M |
| 36 | S6 Cache metric handles and remove label-copy churn | M per-event CPU | C | S–M | L–M |
| 37 | P8 Numeric snapshot buckets/cached push metric labels | M per-recipient CPU | C | S–M | L–M |
| 38 | F5 Kafka per-channel partitioning; offload blocking health | H Kafka scale | C | L (health S) | H |
| 39 | S10 Broker-specific paced durable retries | H failures | C | M–L | H |
| 40 | S9 Partition-aware Kafka/Iggy queue workers | M–H | C | L | H |
| 41 | F4 Candidate/group routing for selective predicates | H selective rooms | C | L | H |
| 42 | A4 Share immutable projected recovery messages | H projected fanout | C | M | H |
| 43 | C2 Checkpointed original-operation storage representation | H long streams | M/C | L | H |
| 44 | C11 SDK serialized-body reuse and retry jitter | M publishers | C | S–M | M |
| 45 | F8 Borrowing binary encoding views | M direct binary | C | M | M |


## Correctness gates and practical implementation order

1. **Contained changes with strong evidence:** F1 extraction reuse, S1 expiry index, P3 failed-refresh coalescing, S12 shared client ownership, C9 ingress bounds and C12 bounded crypto offload. Benchmark actual call counts/bytes and test failures before broad load. These require no public wire redesign.
2. **Bound overload state:** F2 ingress and S7 webhook batches, then S2 readiness and S3 reconnect loads. Define admitted/failed/reset outcomes explicitly. A finite socket queue cannot compensate for unlimited detached work upstream.
3. **Make reads/writes proportional to useful work:** P1/P2 registry indexing, C7/C8 selective durable pages, C6 retention maintenance, S4 cache scans. Preserve cursor semantics and backend continuation/partial-failure behavior. Do not benchmark silently incomplete DynamoDB pages as an optimization.
4. **Reduce stateful critical-section work:** C1/C3/C4 and P6 lifecycle repair. Add incremental indexes before replacing synchronization. Then measure healthy channel progress while another channel stalls. Preserve first/last presence, exact actor authorization, idempotency and all stream/serial identities.
5. **Broader distributed designs:** A1 authorization freshness, F6 rollup scheduling, compact push queue schema/status batching, Kafka partition changes, candidate filtering and C2 snapshots. Require mixed-version rollout, retry/crash, live broker/storage and protocol conformance tests. Keep every original AI mutation in persistence/history/recovery/webhooks/push; rollup remains egress only.

V1 Pusher framing/signing and V2 field stripping remain mandatory across every change. until_attach/live gating, ordering, canonical serial continuity, mutation ownership and annotation authorization must be preserved. Neither shortening retention, weakening authentication, dropping originals, increasing unbounded queues, nor claiming a socket benchmark with the wrong negotiated wire format is an acceptable performance improvement.

Runtime gaps are substantial and explicit: no actual multi-node broker or durable DB loads, no production HTTP/WebSocket throughput/p99, no slow-reader/RSS soak, no real push-provider feedback, and no fleet-scale reconnect/mutation/chaos run. Those gaps do not erase measured local scaling, but they prevent production gain percentages or capacity claims. Start implementation with the contained changes above and use the matrix in VALIDATION.md to determine what dominates in the intended deployment.
