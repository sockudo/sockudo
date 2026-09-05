# Shared services implementation

Baseline production source: `implementation/baseline`, commit `5613bb291032b6b7660352974b03da9eb0646da0`. All changes remain uncommitted. Baseline source was not modified; standalone diagnostic manifests reference it directly.

Refactor plan: preserve delivery/authentication/cache semantics, bound retained work and metadata, implement contained changes before distributed concurrency, and compare identical public API probes with the preserved source. Compilation and mocks do not close live-backend verification.

| Finding | Production change | Correctness evidence | Performance evidence | Remaining verification |
|---|---|---|---|---|
| S1 | Ordered expiration index, 64-entry cleanup budget, exact queried-key TTL checks and generation-safe stale cleanup | Default queue tests; new expired-backlog and refreshed-expiry regressions | `memory_queue-{baseline,s1-s4}-{0,1,2}.txt`, 3 process repeats ×3 samples, 500 accepted jobs at each retained cardinality | Larger mixed producer/consumer workload and retained RSS |
| S2 | Authoritative `has_apps` in every backend; cached enumeration stops shared rewrites; root wires `/up` | All five actual app backends preserve disabled-app existence and empty/create/delete behavior | Exact S2-only enumeration, 3 processes × 5 samples, 1000 apps: 1460→309 us and 2000→0 shared SETs | Backend readiness latency scaling is distinct from the enumeration component probe |
| S3 | 128 flight stripes; 1024 negative entries/250 ms; cancellation/mutation fencing | Cold/missing/cancellation tests; bounded exact outcomes | Exact S3-only 3-process pairs;128 concurrent cold requests:128→1 backend reads,256→2 shared writes; missing128→1 reads | All-driver outage/reconnect combinations remain broader runtime work |
| S4 | Ordered memory pages; Redis GET pipelines256; Cluster scans all primaries | 15 cache tests; standalone Redis10k exact sweep; Cluster3 exact1024×4KiB sweeps | Repeated memory and Redis before/after probes; Redis10k sweep362–435ms→43–50ms | Actual interior-slot reassignment rejects old cursor; A1 pinned snapshot diagnosis remains independent |
| S5 | Finite channel-class and summarizer annotation labels | Churn preserves exact111000 aggregate observations | Exact S5-only 3-process pair: annotation scrape910831→63 us;1887039→56 total fixture series | Exporter/application cardinality outside these labels remains externally determined |
| S6 | Borrowed stable labels and bounded1024 cached keys; active-recorder registration retained | Recorder changes, overflow, exactcounts | Exact S6-only 3-process×5sample pair:100k warmWS30462→15744 us;43→10 allocations/event | Recorder registration cost remains intentional for correctness |
| S7 | 2048 records/16MiB admission; retained stable failed batches; shutdown drain |100k offered:2048accepted+97952rejected; exact accepted drain and permit reclamation | Exact S7-only 3-process comparison;33797us admission vs75234us baseline accepting100k | Admission safety improvement, not equal accepted throughput; forced process termination remains loss boundary |
| S8 | Fair ready-worker `try_reserve` dispatch preserves durable claims without waiting on one full worker | Live skew callback test proves90 healthy completions before releasing first blocked callback,100 exact unique; other queue cases pass | Real Redis1000 jobs incl10 slow100ms callbacks;3 pairs baseline~1.04s→after0.49–0.58s | Cluster/Sentinel skew and rolling-disconnect fixture |
| S9 | Bounded serial group consumers; Kafka app partition keys with explicit epoch transition | Actual Kafka160 deliveries in per-app order,peak4 | Exact S9-only 3-process×3sample comparison:1826835→1307016us,peak1→4 | Iggy runtime blocked by io_uring fixture failure; broker crash/rebalance and commit faults unverified |
| S10 | Bounded paced broker retry, native SQS visibility and Pub/Sub retry policy/fallback | Actual Rabbit4attempts/1success; SQS3attempts/1success over3051ms; existing Pub/Sub fallback3attempts/1success over2154ms | Exact S10-only3-process×5sample pair:retry span384–582us→303522–305410us | Actual new Pub/Sub 1s/2s policy honored; native cloud/DLQ and transfer faults remain outside emulator coverage |
| S11 | Coalesced reconnect/cooldown, generation checks, network outside lock | Redis128 exact unique admissions during disconnect | Exact S11-only3-process×5sample pair:83553→83776us,2–5→2newconnections | No latency gain; actual Cluster failover unverified |
| S12 | Shared bounded64 regional OnceCells across clones | Cancellation/shared initialization and32actual local Lambda invocations | Final exact S12-only3-process×5sample pair:10753→3402us;98469→11396allocationrequests | Remote AWS latency not measured |
| S13 | Bounded retry jobs/body bytes;20active requests released duringbackoff; unused errorbody discarded | Actual healthy/error endpoint and exact41-request workload | Final exact S13-only3-process×5sample pair:500173→131us healthyrequest latency | Local HTTP failure isolation measured; arbitrary external network conditions not claimed |

The opening table is the current status. Subsequent sections preserve investigation history, including failed designs and then-pending checks; the final sections supersede earlier results.

## Raw results and methodology

All paths below are relative to `implementation/results/services`. Original failures are retained and do not count as successful validation.

- `regression-tests.log`:13 app+15 cache+2 metrics+6 queue+35 webhook tests passed (71 total). `regression-tests-final.log` reruns final edits; consult final result before reporting it.
- `redis-live-regressions.log`:2 queue+5 limiter live tests passed using isolated Redis7.4.5 at16391. Unique key prefixes; no FLUSHDB.
- `kafka-rabbit-live-regressions.log`:2 real broker tests passed against Kafka3.9.0 at19094 and RabbitMQ4.1.4 at15691. Kafka160 deliveries, peak4, every app ordered; Rabbit3 attempts,203ms span. Iggy test was skipped after startup failure, not counted.
- `http-retry-regression.log`: actual HTTP fault test passed. `lambda-regression-fixed.log`: cancellation-safe shared initialization test passed. Lambda latest bounded cache edit is covered by subsequent runs.
- `full-check.log` and `final-feature-check.log`: optional service backends compiled. `optional-check-final.log` covers subsequent edits; consult final result. Live app SQL/NoSQL, cloud brokers and Cluster runtime are not implied by compilation.
- `extra-{baseline,current}-{readiness,metrics,batch,redis-cache,redis-queue}-{0,1,2}.{jsonl,stderr}`: identical standalone harness (`implementation/services-bench/src/main.rs`) with manifests pointing at baseline/current production source. Three independent processes;5 internal samples for enumeration, warm metrics and Redis scans. Each validates actual returned/delivered counts. Allocation counts are allocator requests/bytes requested, not live retained bytes; `/usr/bin/time -v` outputs preserve process CPU/RSS. Timing on shared workstation can include overlapping compilation and is component evidence, not end-to-end production capacity.

Observed ranges over those current/baseline runs:

| Workload | Baseline | Changed | Interpretation |
|---|---:|---:|---|
| Cached enumeration1000 apps |1,410–2,127us,2000 shared SETs,~29k alloc requests |283–903us,0 SETs,~9k alloc requests | Authoritative enumeration without cache rewrite amplification |
|100k warm WS metrics |30,124–30,609us,4.3m allocations |15,377–16,362us,1.0m allocations | Stable keys/borrowed labels; recorder registration still costs allocations |
| Annotation scrape after111k unique channels |911,376–975,375us;189,060,409 bytes |55–340us;4912 bytes |56 total fixture series vs1,887,039; exact111k published aggregate |
| Redis10k×4KiB full sweep |362,452–435,348us |42,905–50,113us | Same10k returned values, pages256 |
| Redis1000 jobs,10 slow callbacks |1,039,914–1,043,479us |490,064–577,049us | Same1000 delivered,10 slow; healthy-worker progress reduces head-of-line delay |
| Batcher100k offered |100k accepted, no admission cap |2048 accepted+97,952 explicit rejected | All accepted jobs drained; byte/task/record safety, not equal accepted throughput |

Strict user requirement of repeated unchanged-before/after validation for **each individual fix** is not yet fully met. S5/S6 share the changed metric binary; S2/S3 share cached manager changes; S9–S13 lack repeated baseline pairs. No claim of all-findings closure is justified by these results.

## Operational contracts

- Annotation metric aggregate transition documented in `docs/content/docs/reference/configuration.mdx`; webhook resource/admission/shutdown behavior in `docs/content/docs/server/webhooks.mdx`; Kafka epoch migration in `docs/content/docs/server/scaling.mdx` (fanout owner).
- Failed buffered queue batches retain stable IDs and permits until queue acceptance; saturation returns `BufferFull`. Root must call integration shutdown before queue disconnect. Forced process termination remains a boundary for not-yet-durable batches.
- Kafka default remains legacy single-partition topic. Multi-partition configuration requires a fresh safe epoch; never enlarge existing topic in place. Quiesce and drain old generation before cutover. Iggy/Kafka workers remain serial per group assignment; retry-transfer failure never commits a later offset over the untransferred source.
- SQS and Pub/Sub finite max attempts/dead-letter forwarding remain externally owned native policies. Existing configurations are not rejected at startup. SQS uses receive-count visibility pacing; new Pub/Sub subscriptions receive retry policy, existing unpaced subscriptions use bounded worker-side pacing.

## Audit fixture inventory

Created isolated rootless Podman fixtures: `sockudo-perf-services-redis`(16391), `sockudo-perf-services-kafka`(19094), `sockudo-perf-services-rabbit`(15691), `sockudo-perf-services-iggy`(18092, failed startup). Iggy pinned SHA99b42016a898381d4bab3c2d4613456eb04ad06a7a0688314823d798a685636b fails creating io_uring shard executor with OS error12 even seccomp-unconfined and unlimited memlock. A cpuset attempt failed because the rootless cpuset controller is unavailable. No existing containers were changed; cleanup should remove only these owned fixtures after all agents finish using shared endpoints.


## Continuation review (2026-09-05)

- C4 belongs to services for this continuation. `c4-review-regressions-final-3.log` passes all8 durable presence tests after fixing normal `has_more` pages, immutable stream identity checks after cache eviction, and noncontiguous late-import fallback. At most256 unordered rows can be proven complete in one page; multiple pages require serial ordering and exact coverage, otherwise fail closed. Last cache design has a known performance gap: after its256KiB per-channel metadata limit, large retained streams rehydrate repeatedly. This is not closed by the safety test.
- C3 independent review found MySQL locking changed `updated_at_ms`, invalidating otherwise reusable summaries, and found rebuild permits released before network persistence could retain unbounded active accumulators. Both reported to root, which owns fixes. No C3 closure claim yet.
- P5 now includes pending feedback receipts, bounded group processing, transactional publish-status receipt/counter changes and restart-safe device/event effects. `p5-live-restarts-3.log` and `p5-mysql-live-final.log` collectively pass real local PostgreSQL/MySQL and DynamoDB/Surreal emulators across restart boundaries; `p5-broad-push-tests.log` passed152 push tests. Parent push agent owns subsequent shared storage edits; repeat after those edits.
- Redis Cluster `cluster-scan-live.log` passes three full sweeps of1024×4KiB values across every primary,61 pages each. This replaces the earlier missing basic Cluster runtime coverage; topology-change/failover gaps remain.
- `regression-tests-final-live.log` includes the final36 webhook tests and actual unhealthy/healthy HTTP behavior; `lambda-regression-final.log` covers the final bounded region-cache map. `optional-check-final.log` compiled optional service backends. Owned Clippy's latest logged failure was an unused import that has since been removed; a new run is needed.
- Recovery benchmark baseline dependencies were extended in the standalone harness only (immutable baseline source untouched). `extra-recovery-baseline-build-offline.log` records build progress; baseline and current lockfiles have identical package versions. P5/S11/S12/S13 repeated pairs remain pending.


### Latest verified continuation results

- C4 membership-filter design now passes10/10 focused tests (`c4-membership-tests-3.log`). Fixed128KiB filter plus at most128KiB recent payload-free indexes per cache;64 retained channel caches and128 stable serialization stripes. Coverage watermarks allow incremental fresh-tail reads after full proof. Pruning never clears membership bits, so retained expiration introduces false positives only; stream resets discard all evidence. A positive filter miss requires authoritative replay.2000 unique steady new-member transitions exceed recent-index capacity while staying below16 total reads; forced saturated-filter and reset tests preserve exact counts. Remaining: isolated unchanged-baseline C4 benchmark, particularly sparse old-user cache misses; compilation started in `c4-baseline-build.log`.
- Four actual feedback/lifecycle suites now pass after isolating PostgreSQL/MySQL test databases (`p5-live-restarts-final-7.log`,4tests7.96s). All16 restart boundaries preserve device failure_count=1, dispatched=1, failed=1, exactly1 event, and no pending receipt. Fresh SQL databases avoid unrelated synthetic-backlog work in bounded cleanup tests and are dropped after success. Agent push also reports Scylla shared document path passed (`push-ably/p6-scylla-live.log`).
- Isolated recovery measurements:24 processes, baseline/after order alternated,3process repeats×5inner samples, identical resolved dependency versions and releaseLTO/codegen1. Exact raw files are `extra-recovery-{baseline,current}-{limiter,lambda,http,feedback}-{0,1,2}-live.{jsonl,stderr}`. A first Redis connection-refused run is retained separately and not counted; audit-owned Redis fixture was restarted.

| Component workload | Baseline median (min–max) | Current median (min–max) | Verified result and limits |
|---|---:|---:|---|
| S11 reconnect storm128 requests |83579us(82340–85083)|83582us(82209–84519)|128 exact admitted unique members; new TCP connections2–5→2. No measured latency gain on this multiplexed reconnect proxy. |
| S12 32 Lambda sender-clone invocations |11048us(10838–16247)|3293us(3095–8881)|32 successful actual localhost Lambda HTTP requests; allocator requests98467→11394 median. AWS remote service latency is not measured. |
| S13 healthy request during20 failing endpoint retries |499307us(498763–501160)|138us(102–186)|Exactly41 HTTP requests (20×2 failed attempts+1 healthy). |
| P5 1024 memory feedback,64 results per campaign |3451us(3418–3549)|13197us(12715–13702)|1024 exact counters; status writes1024→32. Memory time regresses from new durable safety receipts; durable-backend latency measurement remains necessary. |
| P5 1024 memory feedback,1 per campaign |3980us(3855–4691)|15633us(15199–16957)|Status writes1024→2048; explicit correctness cost, no performance improvement claimed for singleton memory outcomes. |

These are component measurements with combined current source; strict isolated per-fix variants remain required where another finding affects the workload, especially P5 queue changes. S9/S10 unchanged baseline broker pairs and cloud runtime remain pending.


### C4 isolated measurement and follow-up

`presence-bench-{baseline,c4}` uses identical harness source and dependency versions. The `c4` variant copies only the current durable presence wrapper onto immutable baseline source (including unchanged baseline memory history). Raw first-pass files `c4-{baseline,current}-{0,1,2}.{jsonl,stderr}` each contain5samples×3retainedsizes×3traffic patterns; every sample checks exact final retained records. At10k retained records,100new transitions take baseline median1,059,654us (1,050,663–1,079,733) and reread1,005,050rows; changed267us (256–282),0rereads.100hot-user toggles take2875→250us. Cold100distinctold-user queries regressed2704→638098us because bounded recent metadata cannot retain all historical identities.

The sparse-query regression prompted a bounded64KiB queried-user cache within the existing256KiB budget (128KiB membership+64KiB recent transitions+64KiB query outcomes). It preserves verified query outcomes across conservative full reads, updates them from fresh tail events, and drops expired/generation-changed evidence. `c4-membership-queried-tests-2.log` passes10/10 including exact0 additional history reads for a repeated100old-user working set. Final harness now measures cold and warmed sparse queries separately; final repeated pair remains pending.

P5 follow-up: conditional pending-marker insertion now reuses its timestamp only when the storage backend reports a real insertion; restart replays still retrieve canonical persisted time. This exposed an existing MySQL contract bug (duplicate no-op UPSERT returned true under CLIENT_FOUND_ROWS), causing2 delivery events in `p5-live-restarts-final-8.log`. Push owner fixed SQL with plain INSERT and classified unique-conflict handling, plus duplicatefalse/unchangedrecord regression; latest live rerun remains pending. Also changed feedback completion to an owned-item async helper to satisfy monolith Send requirements; root reruns the monolith compilation.

Six explicit service variants now live in `variants/{s7,s9,s10,s11,s12,s13}` with `ISOLATION.txt` file inventories. `services-bench-{s7,s11,s12,s13}` and `broker-bench-{baseline,s9,s10}` are the reproduction manifests. Bench broker topics are test-owned four-partition topics on both sides, with identical16apps×10orderedjobs; Rabbit uses identical4attempts and one final successful job. Builds/timings pending; no results invented.


### Final isolated service pairs and renewed C4 investigation

The following results are repeated **single-finding** comparisons against immutable baseline source. Raw files: `isolated-final-{s7,s9,s10,s11,s12,s13}-{baseline,current}-{0,1,2}.{jsonl,stderr}`. Each side has three independent processes, alternating order; S9 has three internal samples, S11/S12/S13 and S10 have five. S7 has one admission stress sample per process. The manifest directories and `variants/*/ISOLATION.txt` identify exact changed files. Every workload validates returned/delivered counts. CPU and process peak RSS are retained in stderr; allocation counts mean allocator requests, not retained memory. These are component timings on one workstation, not production tail estimates.

| Finding and workload | Baseline median | Single-finding median | Correctness and interpretation |
|---|---:|---:|---|
| S7: 100,000 offered records under blocked queue | 75,234 us, 100,000 accepted | 33,797 us, 2,048 accepted + 97,952 rejected | All accepted records drain exactly. Different accepted work: this proves bounded admission, not equal-work throughput. |
| S9: Kafka, four partitions, 16 apps × 10 jobs | 1,826,835 us; peak callback concurrency 1 | 1,307,016 us; peak 4 | All 160 jobs delivered, each app exactly ordered 0–9. Both sides use identical four-partition owned topics. |
| S10: RabbitMQ, four attempts with one final success | 2,781 us total; retry span 384–582 us | 306,307 us; span 303,522–305,410 us | Exactly four attempts. Deliberate pacing trades retry latency for lower failure-loop load. |
| S11: Redis, 128-request forced reconnect | 83,553 us; 2–5 new TCP connections | 83,776 us; 2 connections | Exactly 128 unique admitted members. No measured latency improvement. |
| S12: 32 Lambda sender-clone HTTP invocations | 11,520 us; 98,468 allocation requests | 3,362 us; 11,395 allocations | 32 actual successful local HTTP invocations. Remote AWS latency unmeasured. |
| S13: one healthy endpoint amid 20 retrying endpoints | 500,417 us; 2,009 allocation requests | 134 us; 135 allocations | Exactly 41 requests: two failures per bad endpoint and one successful healthy request. |

C4 final queried-cache pair (`isolated-final-c4-*`, 3 processes × 5 samples) at 10,000 retained records: 100 new transitions 1,141,958 → 270 us, hot transitions 2,967 → 257 us, warmed sparse users 2,668 → 146 us. Cold 100 distinct old users remained 2,821 → 712,149 us with 990,099 reread rows. That regression is **not closed**: a subsequent implementation replaces the monolithic membership filter with at most 64 serial-range summaries totaling 128 KiB. Selective reads use existing serial-bounded history pages, verify exact row coverage, and retain final inspection checks. Adjacent summaries merge using bitwise OR; expiration never clears bits. No storage schema or trait changes. New focused tests and repeated exact-source measurements are pending.

Actual app readiness tests (`app-readiness-live-2.log`) pass PostgreSQL, MySQL, DynamoDB and SurrealDB, including disabled-app existence and empty/create/delete behavior. The first real Surreal run exposed flattened record decoding incompatible with the persisted flat fields; the production record now directly decodes id/key/payload, with unchanged stored format. Scylla constructor reaches the mapped fixture but its advertised shard ports are inaccessible; a fixture routing correction remains necessary.

Latest push rerun `p5-live-restarts-final-9.log`: all 16 feedback restart boundaries preserve one device failure, one dispatched/failed counter, one delivery event and zero pending receipts. PostgreSQL and MySQL lifecycle checks also pass. DynamoDB and Surreal lifecycle checks hit the bounded 80-pass receipt-scan assertion; push owner is inspecting progression. This is a lifecycle verification failure, not an all-four-suite pass. PostgreSQL P5/P6 integrated performance binaries are built; their pair remains pending. Integrated storage measurements will not close the strict P5-only isolation gate.


C4 follow-up source now passes 13 focused tests (`c4-range-tests-3.log`): 40,000-record filter merging, 25,000-transition retention churn with at most four surviving ranges, cold member lookups limited to candidate pages, three interrupted-range failures, saturated filters, warmed query reuse and maximum-serial eviction. Fully verified selective reads refine the corresponding filters. Wholly expired ranges release their slots; partial ranges preserve conservative bits. The current revision was rebuilt in `c4-range-final-build.log`; repeated timings remain pending.

Final app readiness closes the remaining basic backend runtime gap: `app-readiness-scylla-live-3.log` passes the actual Scylla constructor (0.47 s). A dedicated `sockudo-perf-services-scylla` fixture uses Scylla2025.3, two shards, 2 GiB, symmetric localhost native ports 19044/19045 and broadcast RPC address127.0.0.1; no product/test-only session translator was added. Together with `app-readiness-live-2.log`, all five app backend empty/disabled/create/delete paths pass.

`p5-live-restarts-final-10.log` passes all four complete feedback/lifecycle suites in19.99 s. The lifecycle test now accounts for fair one-app-per-tick scan rotation across five isolated fixture apps while retaining strict deletion budgets. All16 feedback boundaries and all lifecycle assertions pass. This supersedes the failed 80-tick test evidence, which remains retained. PostgreSQL P5/P6 benchmark locks have identical package/version/source sets.


### Final component attribution and actual presence integration

`components-final-{s2,s3,s5,s6,s12,s13}-{baseline,current}-{0,1,2}.{jsonl,stderr}` contains all36 successful processes. `components-final-manifest.json` records commands/binary SHA256, and `components-final-summary.json` records sample counts and ranges. S2/S3 and S5/S6 now have explicitly separated variants rather than shared changed binaries; all five component manifests resolve identical dependency package/version/source sets. The original immutable baseline remains unchanged. S2's benchmark isolates enumeration rewrite removal; all five authoritative existence implementations additionally have live correctness evidence.

C4 final serial-range design (`c4-range-final-*`) passes all repeated checks. At10k retained/100transitions, baseline/current medians: new1085060/291us, hot2921/279us, cold sparse2739/9262us, warmed sparse2657/140us. Cold sparse reads25344rows, down from990099 in the rejected monolithic-filter design. The bounded design still costs about3.4× baseline's unbounded exact-map lookup for this cold workload; it no longer rescans the full history on each query. `c4-range-final-summary.json` includes all100/1000/10000 retained shapes and observed ranges. Actual five-backend presence wrappers pass `c4-five-backend-live-2.log` (52.90s), including cold-page selection, other-node latest-state changes, retained dedupe, incomplete reads and stream reset races. Initial build logs exposed optional feature-gate and rand dependency issues reported to root, which owns fixes.

P5/P6 actual PostgreSQL integrated pairs now pass `p5-p6-sql-fair-{baseline,current}-{0,1,2}.{jsonl,stderr}`:9samples per side/group,256outcomes per sample, fresh identical schema per process. Singleton campaigns use64-record queue batches on both sides: median120896→207050us and256→512statusrevision increases (explicit safety cost). Shared campaigns of64outcomes use four-record queue batches on both sides:785472→451223us and256→128statusrevision increases. Every final counter and acknowledgement count is exact. The original64-concurrent-campaign baseline probe failed with56handled outcomes; its raw files remain separately retained. Source review shows the old receipt-before-CAS ordering can suppress a failed counter mutation on retry. The fair-batch probe does not relax final counter assertions. These integrated results do not alone close strict P5-only attribution.

Final default owned service Clippy passes `owned-clippy-final-3.log`; all optional owned service features/all targets pass `owned-optional-clippy-final-2.log`. Latest complete four-backend push suites including client-index reassignment and tuple-collision regressions pass `p5-live-restarts-final-11.log` (19.43s). Root owns required workspace/build/conformance verification.


### Cloud retry runtime and final optional verification

`cloud-retry-live-2.log` passes actual ElasticMQ1.7.1 and official Pub/Sub emulator0.8.35 tests (7.72s). SQS fails twice then succeeds once over3051ms, proving receive-count delays advance from1s to2s; a final receive verifies source acknowledgement. An existing Pub/Sub subscription without a retry policy preserves its externally owned policy and uses the bounded worker fallback: exactly3 attempts, one success,2154ms span. All resources are synthetic and uniquely named; image provenance is in `cloud-emulator-images.txt`. New-subscription native retry-policy verification is separate and pending. Native AWS/Google service-specific DLQ behavior remains untested; Iggy cannot start its io_uring executor on this host.

The expanded optional owned library test command compiled all optional features, then stopped in sockudo-app:32 passed,8 Scylla tests failed because their default localhost19042 fixture is unavailable (`owned-all-optional-tests-final.log`). The dedicated production-compatible Scylla19044 fixture already passed readiness and presence suites; rerun these existing tests with SCYLLADB_NODES explicitly set. Cloud queue Clippy with SQS/Google PubSub features and all targets passed `cloud-retry-clippy-final.log` before adding the new-subscription policy case.

C7 independent read-only review sent root one concrete correction: Scylla metadata message counts must not unconditionally include legacy pointers whose referenced entries have expired. Other active-count paths use bounded metadata pages, authoritative current state and version-fenced legacy repair consistent with their point-read retention semantics.


S4 review found a concrete Redis Cluster cursor gap: the topology generation hashed only each primary ID and its first slot, so an interior slot transfer could leave it unchanged. The current patch hashes every validated slot interval, rejects partial/overlapping maps, and compares topology again after reading a page, including the terminal page. Focused map regressions and an actual empty-slot reassignment test are added but have not run yet. The test restores the original owner before checking its outcome and requires explicit isolated-fixture opt-in. Standalone Redis MGET remains unadopted: the Ably owner reports equal-density unpinned scans pass both implementations; CPU-pinned diagnostics remain necessary to identify the earlier snapshot deadline failure.


### Controlled P5 algorithm attribution

All six processes in `p5-controlled-{baseline,current}-{0,1,2}.{jsonl,stderr}` passed; `p5-controlled-summary.json` contains9samples per side/group, each256 outcomes. Both variants contain the identical current PostgreSQL/P6 storage snapshot and identical dependency package/version/source sets. Only feedback.rs differs: before is byte-identical immutable baseline feedback.rs, after is the current feedback algorithm. Source/binary SHA256 values are recorded in `p5-controlled-source-manifest.json`. This controlled component pair supplements the unchanged whole-baseline integrated pair above; it does not replace that evidence. Subsequent P6 future-expiry cleanup edits are outside this fixed storage snapshot and require their own runtime verification.

Singleton campaigns (queue batch64 on both sides): median125410→216849us, ranges111623–129851 /211131–264873us, status revision increases256→512. Shared campaigns of64 outcomes (queue batch4 on both sides): median793076→445620us, ranges733521–874582 /435951–464269us, revision increases256→128. All final dispatched/failed counts, processed outcomes and acknowledgements are exact. The campaign improvement and singleton safety cost remain when P6 storage is held constant; no blanket feedback throughput gain is claimed.


Final optional library verification now passes `owned-all-optional-tests-final-2.log`:131 tests across app40/cache17/metrics2/queue15/limiter13/webhook44, five explicit fixture tests ignored. The eight Scylla tests pass with the production-compatible19044 fixture override. `cloud-retry-live-3.log` passes all three real emulator cases in10.27s, including the new Pub/Sub subscription with exact minimum1s/maximum2s policy and1955ms retry span; SQS receive-count pacing spans3045ms. `p5-live-restarts-final-12.log` passes all four actual backend feedback/lifecycle suites in21.91s, including the final time-advance log-retention proof. A subsequent full owned optional Clippy run and actual Cluster topology regression are pending.


S4 final topology regression passes `cluster-scan-live-final.log` (2tests,0.55s): verified-empty interior slot10924 moved between existing primaries, the old cursor was rejected, and the original owner was restored before the assertion. Three subsequent sweeps return exactly1024×4KiB records over62pages each. Unit map tests also passed within the optional library run. The standalone MGET candidate was rejected by the Ably owner's equal-density pinned comparison (1024matched+256other-app records): GET pipelines~416ms versus MGET~1455ms, both with TCP_NODELAY. The current standalone implementation remains bounded GET pipelines; the unexplained per-stage stall remains under investigation, with no claimed transport fix.
