# Push and Ably implementation

Baseline: `5613bb291032b6b7660352974b03da9eb0646da0`, preserved under `implementation/baseline`. Nothing committed, published, or deployed. Only isolated local fixture schemas/data were migrated.

The user clarified that internal storage and queues should have one current write path, with automatic compatibility for old persisted records. The temporary operator flags for compact batches, nested cursors, lifecycle retention, and stats indexes have therefore been removed. Internal format discriminators remain necessary to decode old records; they do not select or change Pusher V1, Sockudo V2, or Ably public protocols. Root owns server/config propagation. The latest completed push library run passes 162 tests with five actual-backend tests run separately. New scheduled-work/publish-log repair and time-dependent lifecycle-proof regressions are being verified; all-backend library/test Clippy and the five live fixtures will be rerun after those final changes. Earlier evidence below is labeled accordingly.

| Finding | Implementation | Correctness / runtime evidence | Isolated before/after evidence | Remaining gates |
|---|---|---|---|---|
| P1 | Complete: client-indexed targeting for planner and Ably summaries; client-scoped pages in memory/SQL/document stores; bounded nested channel continuations; underestimated fast audiences continue without loss | Client reassignment, stale-index validation, nested continuation and exact recipient accounting tests. Actual Scylla cursor/channel conformance passed. Push/Ably full tests passed before final flag removal | 7 repetitions at registry sizes 1k/10k; same 10 recipients verified each run; table below | Indexed client pagination/reassignment now passes actual PostgreSQL/MySQL/Dynamo/Surreal fixtures; final Scylla run and network/storage-operation benchmarks remain |
| P2 | Complete: memory keyset registry/channel/log/maintenance pages with rotating progress; automatic ordered document subscription/log references with bounded resumable backfill and full compound continuations; bounded Surreal app queries; Dynamo continuation and unprocessed deletion retries | Compound cursor ordering and stale-index continuation tests; actual Scylla pagination passed. Document index migration is resumable and validates canonical rows | 7 repetitions at 1k/10k; every registry row returned; table below | Compound tuple-collision regression passes; actual backend pagination/operation-count measurements remain |
| P3 | Complete: shared failed refresh outcome for 250ms, cancellation-safe locking, explicit invalidation generation fencing | Success/failure coalescing, cancellation, invalidation races and cooldown tests | Baseline/after failure64 probe: 64 fetches/~389ms vs 1/~6ms (5 repeats); raw `results/push-ably/p3-*` | Real provider-facing outage integration and final feature/clippy checks |
| P4 | Complete: one compact delivery-batch writer; legacy/current readers; immutable original/rendered payload groups and per-recipient replay context | Legacy/current roundtrips, mixed recipient overrides, retry feedback, unknown format/group rejection, explicit offline legacy re-encoding | Seven repetitions per six batch shapes measured; large payload savings proven; small-payload regression corrected by omitting absent fields and reusing adjacent payload groups | Actual Redis producer/consumer reconnect and fresh decoder checks pass; seven provider constructor integrations reviewed |
| P5 | Complete in services-owned feedback work: bounded 16-way successor writes and grouped exactly-once status receipts | All four crash/restart boundaries exact on actual PostgreSQL, MySQL, DynamoDB Local, SurrealDB; fresh SQL database fixtures. Shared document path also passed all four boundaries on actual Scylla | Owned/tracked by services; do not infer benchmark completion from runtime checks | See `services.md`; rerun after final lifecycle API simplification |
| P6 | Implemented; final review corrections in progress: bounded rotating repair, restartable shard AND log proof scans, exact emitted-outcome completion proof, finite canonical retirement markers, bounded artifact deletion, SQL parent/child transaction fencing, durable document child admission pins, provider/shard replay guards | Missing canonical parent now returns a retriable child-write error; paused orphan/reused-parent race regression passes. Paused/cancelled/uncertain document writes cannot be overtaken by cleanup; 64-slot admission cap enforced. All five real backends passed restart/retention with 8 logs and 8 shards at two-row deletion budgets; provider guard tests verify active/unknown/retired outcomes. SQL test isolation resolved old-fixture scan backlog | Pending | Isolated retention/resource-growth/recovery measurements; final review caught time-dependent unsafe log proof starvation, now corrected in all stores with five-backend time-advance regression pending |
| P7 | Complete: ordered memory ready queue, delayed due-time index, lease-deadline index | Eligible FIFO, future bypass, duplicate key and expired lease redelivery behavior retained | 7 repetitions at 1k/10k backlog; output accounting asserted; table below | Future-heavy healthy-client latency and allocation measurements complete; table below |
| P8 | Complete: numeric histogram buckets and cached metric label/recorder keys; explicit app-removal API | Snapshot/bucket semantics, active recorder scope and label eviction tests; no reachable production app-deletion path exists; removal API is available but not wired | 7 repetitions at 1k/10k metric updates; exact count assertions; table below | Real Prometheus recorder/multitenant allocation measurements complete; no production lifecycle hookup claimed |
| A1 | Implemented, performance gate OPEN: shared 250ms active-session revocation snapshot, bounded 32 cached apps / 2MiB each / 4 scans, immediate fresh AUTH/attach checks, unchanged ownership fencing | 1,000 concurrent checks share one scan; dependency failure fails closed; remote changes/local invalidation and issuedBefore replacement tests. Actual Redis two-node tokens/revocation/ownership tests passed | Pending | Dense actual Redis cohort fails closed at 250ms in pinned runs. Unpinned identical GET-pipeline and MGET page probes both pass; transport/CPU-affinity diagnosis remains open. Do not claim TCP or MGET resolves it |
| A2 | Complete: critical stats observations flush immediately and interrupt optional batch wait; persistence still precedes ACK | Durable count visibility after record; critical command interrupts five-second optional wait; actual Redis cross-node stats merge/restart passed | 7 sequential 100-ACK runs: median 1,112,431µs → 205µs; full distributions below | Memory and actual Redis sequential ACK distributions complete; delayed/failing-store correctness remains covered by barrier tests |
| A3 | Complete: ordered memory range reads and retention; canonical legacy minute values plus automatic calendar indexes and bounded resumable backfill; index eviction falls back without omissions | Actual Redis and memory migration/read tests passed: 600 legacy minutes, 256-entry backfill steps, narrow pages, minute/hour/day/month rollups, both directions, cursor horizon, eviction fallback, canonical compatibility, inactive memory expiry | 7 narrow-memory reads at 1k/10k rows: 941/15,611µs → 7/7µs; actual Redis 1k/10k query pair passed; table below | Actual Redis range-read measurements and final 195-test suite pass; idle cleanup resource-growth timing remains separate |
| A4 | Complete: immutable Arc messages shared in attach/projected recovery gates; consumer-specific mutation still materializes an owned copy | Shared payload retention test plus scoped continuity invalidation; full 195-test Ably suite passed with actual Redis after final stats flag removal | Seven repetitions per 12 payload/message/subscriber shapes; isolated Arc ownership only, baseline sizing retained | Actual recovery/attach component allocations and CPU/RSS recorded; full compatibility tests pass |

## Recorded component measurements

Release profile on the recorded Ryzen 9 7950X host; seven repetitions per shape. Each pair uses the unchanged baseline and an isolated finding variant, with all returned items/counts asserted. These are component timings, not end-to-end throughput claims. Brackets show min/max; cold first repetitions are retained. Raw CSV and `/usr/bin/time -v` CPU/RSS output are in `results/push-ably/`. Environment/compiler metadata is in `implementation/environment.json`.

| Finding / workload | Baseline median µs [min,max] | After median µs [min,max] |
|---|---:|---:|
| P1: 1k registered / 10 selected | 405 [387,836] | 19 [18,457] |
| P1: 10k registered / 10 selected | 27,852 [26,604,30,968] | 19 [19,35] |
| P2: page every 1k device, size100 | 2,715 [2,690,3,185] | 222 [217,385] |
| P2: page every 10k device, size100 | 314,548 [303,312,318,688] | 2,226 [2,209,2,363] |
| P7: 1k backlog | 961 [925,1,184] | 829 [823,1,180] |
| P7: 10k backlog | 27,810 [26,333,32,846] | 9,086 [8,314,10,201] |
| P8: 1k metric updates | 554 [544,581] | 74 [72,186] |
| P8: 10k metric updates | 5,644 [5,552,5,792] | 752 [723,771] |

Reproduction source: `crates/sockudo-push/examples/performance_audit.rs`; baseline copy is retained in the baseline crate, and isolated copies live under `implementation/variants/p1`, `p2`, `p7`, and `p8`. Build the relevant variant manifest using `cargo build --release --example performance_audit`, then run its resulting binary with `p1`, `p2`, `p7`, or `p8`. Each raw `.time` records the exact binary command used. The original temporary executable paths are no longer available; source and baseline commit remain reproducible.

## Additional isolated Ably measurements

The stats probes compile the actual baseline/current stats module with identical dependencies. A2 replaces only the stats worker flush wait; A3 replaces only memory/cache range selection and calendar indexing, while preserving the baseline worker. Seven repetitions, release build, pinned to CPU3; fixtures are seeded outside timed queries. Raw `a2-*` and `a3-*` CSV/time files preserve all repetitions and allocation counts. These are in-memory component measurements; actual Redis results are recorded separately after completion.

| Workload | Baseline median µs [min,max] | After median µs [min,max] | Allocation change |
|---|---:|---:|---|
| A2: 100 sequential durable records | 1,112,431 [1,109,554,1,115,943] | 205 [195,215] | 2,500 allocations / 255,900 bytes unchanged in steady state |
| A2: per-record p50 | 11,088 [11,086,11,090] | 1 [1,2] | Same durability assertion |
| A2: per-record p95 / p99 | 11,499 / 12,028 | 2 / 4 | Full distributions in CSV |
| A3: two minutes from 1k stored minutes | 941 [902,985] | 7 [6,38] | 14,240→115 allocations; 1,490,428→13,873 bytes |
| A3: two minutes from 10k stored minutes | 15,611 [14,596,16,207] | 7 [6,791] | 141,743→115 allocations; 15,697,740→13,873 bytes |

The A3 first-run 791µs outlier is retained. Inactive-retention correctness is tested separately; this probe does not claim to measure idle cleanup. Reproduction manifests are `implementation/ably-stats-bench-{baseline,a2,a3}/Cargo.toml`; executable names are distinct to prevent accidental Cargo artifact reuse. Commands are recorded by each `.time` file. Later addition of the production Redis cache driver wrapper affects only fixture connection construction; its hot cache methods are copied unchanged and identical in both candidates.

## Actual Redis and final queue-envelope measurements

Seven repetitions per workload, release builds pinned to CPU3. Redis uses the actual production cache-driver hot methods with fresh unique fixture prefixes; the probe substitutes only direct connection construction, so no Sentinel/TLS/reconnect claim is made. The same cache methods and wrapper counters are used in both binaries. `cache_calls` counts cache-interface operations, and `read_bytes` counts materialized cache keys/values; neither is a Redis wire-packet count. Larger-fixture seeding and automatic migration finish before timed range queries. The unchanged baseline stats reader and candidate index reader see identical per-app minute fixtures. Raw files are `a{2,3}-redis-{baseline,after}.{csv,time}`.

| Workload | Baseline median µs [min,max] | After median µs [min,max] | Resource/accounting evidence |
|---|---:|---:|---|
| A2 Redis: 100 sequential persisted ACKs | 1,127,009 [1,125,156,1,129,578] | 7,052 [6,825,7,492] | Same200 cache operations; same7,305 median allocations; all700 counts visible |
| A2 Redis: p50 / p95 / p99 ACK | 11,252 / 11,366 / 12,367 | 67 / 88 / 120 | No acknowledgment before canonical count write |
| A3 Redis: 2 minutes from1k | 3,134 [3,075,3,707] | 313 [302,495] | 253,000→2,741 read bytes; 20,489→486 allocations; 4→11 cache calls |
| A3 Redis: 2 minutes from10k | 784,193 [770,409,788,134] | 287 [284,323] | 2,550,000→3,092 read bytes; 204,284→538 allocations; 43→11 cache calls |

P4 uses the unchanged baseline queue serialization and an isolated current batch encoder/decoder. `batch_envelope_audit.rs` asserts exact reconstructed jobs, payloads, recipients, and retry context for every repetition. Shared app/publish/batch identities are stored once, absent optional fields are omitted, and adjacent identical payload pointers reuse their group without repeated hashing. No live format selector exists. Final raw files are `p4-{baseline,after}.{csv,time}`; intermediate regression measurements remain as `p4-before-identity-sharing` and `p4-identity-only`.

| Payload / recipients | Encode median µs baseline→after | Decode median µs baseline→after | Queue bytes baseline→after | Encoding allocation bytes baseline→after |
|---|---:|---:|---:|---:|
| 256B /1 | 0→0 | 0→1 | 597→574 | 2,790→2,483 |
| 256B /100 | 17→8 | 38→28 | 49,386→12,931 | 120,218→40,461 |
| 256B /1k | 176→121 | 387→382 | 494,886→127,231 | 1,938,458→583,101 |
| 64KiB /1 | 2→2 | 5→7 | 65,877→65,854 | 394,470→394,163 |
| 64KiB /100 | 274→10 | 791→35 | 6,577,386→78,211 | 24,796,058→407,627 |
| 64KiB /1k | 20,223→130 | 22,450→385 | 65,774,886→192,511 | 201,117,210→530,027 |

Single-recipient decoding retains a small version-header overhead (1–2µs); aggregate batches remove the repeated payload allocation/serialization cost. Full min/max values and process CPU/RSS are retained in the raw files. Baseline/candidate source manifests are under `implementation/variants/p4-baseline` and `p4`.

## Final broker, queue, recorder and recovery measurements

All pairs below use seven repetitions, release binaries pinned to CPU3, exact output accounting, and unchanged baseline plus one-finding variants. Full distributions, CPU/RSS and allocation counts remain in raw CSV/time files.

P4 actual Redis: `implementation/run-p4-redis-roundtrip.py` encodes a batch, enqueues it to a unique fixture key, closes the producer, reconnects a consumer, verifies exact broker-held bytes and queue depth, then decodes in a fresh process. This tests consumer/worker restart, not Redis server restart or AOF durability. Raw `p4-redis-{baseline,after}.csv`.

| Broker batch | Roundtrip baseline median µs [min,max] | After median µs [min,max] | Broker bytes baseline→after |
|---|---:|---:|---:|
| 256B ×1k recipients | 1,204 [1,116,2,514] | 360 [308,409] | 494,886→127,231 |
| 64KiB ×100 recipients | 51,126 [7,871,91,297] | 311 [284,351] | 6,577,386→78,211 |

P7 seeds future jobs with `retry_at = u64::MAX`, then measures100 healthy produce/consume/ack operations while asserting exact IDs and retained delayed count. P8 installs a real Prometheus recorder, warms keys, and asserts both native snapshots and exported histogram counts. Reproduction manifests: `implementation/push-extra-bench-{baseline,p7,p8}`; raw `p{7,8}-extra-{baseline,after}.{csv,time}`.

| Workload | Baseline median µs [min,max] | After median µs [min,max] | Allocation evidence |
|---|---:|---:|---|
| P7: 1k future jobs /100 healthy jobs | 2,123 [1,937,2,246] | 139 [133,144] | 37,794,562→139,034 bytes; p99 healthy63→6µs |
| P7: 10k future jobs /100 healthy jobs | 19,325 [18,752,21,630] | 143 [138,164] | 603,044,762→141,434 bytes; p99 healthy616→5µs |
| P8: 1 app /1k observations | 793 [765,804] | 154 [148,178] | 31,015→7,015 allocations; 588,720→271,720 bytes |
| P8: 1 app /10k observations | 7,907 [7,780,8,039] | 1,576 [1,522,1,616] | 310,156→70,156 allocations; 5,893,488→2,723,488 bytes |
| P8: 128 apps /1k observations | 811 [779,820] | 159 [157,190] | 31,000→7,000 allocations; 577,480→258,240 bytes |
| P8: 128 apps /10k observations | 8,016 [7,970,8,078] | 1,635 [1,622,1,658] | 310,128→70,128 allocations; 5,909,720→2,716,932 bytes |

A4 copies the baseline actual protocol/codec/gate definitions and changes only recovery message ownership to `Arc`. Baseline repeated serialization for byte sizing is deliberately retained: its removal belongs to F1, not A4. Each run asserts gate counts, byte budgets and serial continuity; a consumer-specific mutation must leave other gates and the shared tail unchanged. Reproduction manifests: `implementation/ably-recovery-bench-{baseline,a4}`; raw `a4-{baseline,after}.{csv,time}`. Twelve shapes cover16/128 messages,1/16/64 subscribers,256B/64KiB payloads. Representative128-message results:

| Payload /subscribers | Baseline median µs [min,max] | After median µs [min,max] | Allocation count baseline→after |
|---|---:|---:|---:|
| 256B /1 | 37 [36,40] | 20 [19,28] | 1,159→391 |
| 256B /64 | 2,360 [2,321,4,174] | 1,627 [1,517,1,657] | 74,113→24,961 |
| 64KiB /1 | 317 [303,355] | 306 [263,593] | 1,159→391 |
| 64KiB /64 | 18,924 [18,649,20,610] | 17,299 [17,238,17,463] | 74,113→24,961 |

The64KiB/64-subscriber shape still allocates3,236,387,840→3,225,709,312 total bytes because baseline sizing serializes each message. Sonic values already share their DOM; A4 removes outer message/metadata copies. It does not independently remove gigabytes of sizing allocations.

A1 remains unresolved. Preserve `a1-only-failed`, `a1-baseline-final`, `a1-tcp`, `a1-a1-tcp` and the diagnostic logs. TCP-only is separately labeled and does not contaminate the unchanged baseline. A1+TCP still fails the dense cold-cohort deadline on fresh DB14; therefore unrelated DB0 stats fixtures cannot alone explain the failure. Exact1,024-record page probes in empty DB15 pass both GET pipeline and MGET unpinned; pinned comparison remains next. The production shared Redis factory TCP change is provisional pending attribution; no production MGET patch is justified yet.

## Runtime evidence and current limitations

- `results/push-ably/ably-current-tests.log`: 195/195 passed with `REDIS_URL=redis://127.0.0.1:16391/ cargo test -p sockudo-ably-compat --features 'push,delta,recovery' --lib -- --include-ignored`, before final internal flag removal. Port16379 was unavailable; all required tests were rerun against the available isolated Redis fixture.
- `results/push-ably/p6-current-test.log`: seven focused lifecycle tests passed before flag removal, including exact proof, bounded progressing log/shard scans, failure/cancellation admissions, and provider replay discrimination.
- `results/services/p5-live-restarts-final-11.log`: PostgreSQL/MySQL/DynamoDB Local/SurrealDB feedback+retention passed (19.43s), including all16 restart boundaries, indexed client-page conformance and duplicate insert expiry preservation. This predates the final missing-parent and time-advance review corrections.
- `results/push-ably/p6-scylla-final-live.log`: final actual Scylla shared path passed15.81s.
- `results/services/p5-live-restarts-final-7.log`: PostgreSQL, MySQL, DynamoDB Local, and SurrealDB feedback/restart/retention tests all passed (four tests, 7.96s). SQL fixtures now use fresh test databases; no pre-existing data was deleted.
- `results/push-ably/p6-scylla-live.log`: actual Scylla cursor/channel pagination, all four feedback crash boundaries, and lifecycle/replay checks passed (7.06s), using localhost19042 with fixture address translation and shard-aware-port discovery disabled.
- `results/push-ably/push-final-owned-tests.log`: 161/161 library tests passed across PostgreSQL/MySQL/DynamoDB/SurrealDB/Scylla features; five live tests run separately. `push-final-clippy.log`: library and test Clippy passed with warnings denied.
- `results/push-ably/p6-parent-fence-tests.log`: 162/162 library tests pass after requiring a canonical parent before child admission. Scheduled-work repair and time-advance proof regressions were added afterward and are pending verification.
- `results/push-ably/ably-final-shared-redis-tests.log`: 195/195 pass with actual Redis after the shared TCP factory change. This correctness suite does not establish dense snapshot deadline performance.
- `results/push-ably/ably-single-path-full.log`: 195/195 passed, including actual Redis, after removing the stats operator switch.
- `results/push-ably/push-bounded-maintenance-tests.log`: automatic legacy backfill resumes across restarts, reads at most 256 canonical rows per step, returns retryable failure until complete, and walks every 600-record page without full scans. Memory/document cleanup progresses beyond a live prefix with three-row scan limits.
- MySQL duplicate idempotency insert semantics were corrected after the P5 fast path exposed CLIENT_FOUND_ROWS ambiguity in a no-op ON DUPLICATE KEY write. Plain INSERT now returns false only for a unique violation and propagates other errors; duplicate insertion must preserve the original publish and expiry. Final rerun passed on all five actual backends.

Child log/shard writes without a canonical parent now fail retriably, preserving scheduled/queued work for status repair. The original publish-log planner also requires explicit retirement evidence before suppressing a replay with missing status; unknown status leaves the original queue item unacknowledged. Legacy orphan fanout requires restoring canonical accepted status from a consistent queue/database view after fencing prior writers; fabricated terminal counters are not safe repair evidence. Document child admissions intentionally have no expiry: an arbitrarily paused write must not be overtaken by retirement. Crashes can conservatively pin at most64 admissions per publish until original writers are fenced and uncertain operations are reconciled. Original delivery batches without explicit expiry have no absolute broker-redelivery horizon; after finite retirement evidence expires, missing status does not justify suppressing accepted work. These are documented in `docs/content/docs/server/push-operations.mdx`.

Cross-scope: C12 Ably device verification/hash uses the bounded crypto helpers with dependency/resource failure mapped to503. F2 Ably ingress reset invalidates shared tails, overflows affected gates and signals session closure. Root wired and this agent reviewed `.with_store(...)` in all seven provider constructors.

No finding is marked fully verified while its required isolated before/after or actual-backend gate remains open.
