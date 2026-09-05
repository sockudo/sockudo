# Validation, reproduction and remaining coverage

This audit assessed the source at commit `5613bb291032b6b7660352974b03da9eb0646da0`. It applied no production optimization. **Measurements establish current component costs and scaling; they do not establish server capacity, production p99 or an implemented speedup.** Feature compilation is distinct from runtime backend coverage.

## Environment and build

[Recorded environment](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/environment.json): AMD Ryzen 9 7950X, 16 cores/32 threads, approximately 30GiB RAM, Linux 7.2.3 Nobara, Rust/Cargo 1.98.1, x86_64. This was a shared desktop with frequency boost, no CPU affinity/governor isolation and no exclusive machine reservation. Compilation and some original subagent probes overlapped. Repeated store/service runs and a later Cargo rerun of the source-including probes expose some variation, not all environmental uncertainty.

The isolated [diagnostic manifest](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/diagnostics/Cargo.toml) has its own `[workspace]`, `publish=false`, release LTO and `codegen-units=1`. It depends on actual local crate APIs; auth/conflation/FilterIndex bins directly include current source with minimal surrounding types. It uses the system allocator; the server uses jemalloc. The store probe wraps system allocation to count successful allocation/reallocation requests, **not live bytes, deallocations or production allocator overhead**. Request bytes include constructing test requests and returned results within the timed operation. Instrumentation adds atomic-counter cost.

The diagnostic [lockfile](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/diagnostics/Cargo.lock) is separate from the unchanged root lockfile; some transitive versions differ (for example hyper 1.11.1 versus root 1.11.0). It is retained for repeatability. Root Criterion and crate checks/tests use the root dependency graph. Original auth/conflation/FilterIndex runs used `rustc -O` and existing dependency artifacts; their later Cargo reruns remove dependence on those artifact hashes. Compare runs only within their stated configuration.

## Executed checks

Run commands from the repository root. Logs include diagnostics, not just success summaries.

| Command | Result / evidence |
|---|---|
| `cargo metadata --no-deps --format-version 1 --offline` | Passed. The manifest-derived [inventory](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/inventory.json) retains all original package manifests plus the root manifest; [coverage](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/README.md#coverage-inspection-versus-execution) includes independent workspaces/excluded SDK. |
| `cargo check --offline -p sockudo --no-default-features` | Passed; [minimal build](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/check-minimal.txt). |
| `cargo check --offline -p sockudo --features v2,ai-transport,redis,postgres,push` | Passed; [selected combination](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/check-v2-ai-redis-postgres-push.txt). |
| `cargo check --offline -p sockudo --features full` | Passed; [full feature build](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/check-full.txt). This type-checks optional integrations, not their external services. |
| `cargo test --offline -p sockudo-core --lib` | **357 passed** in the completed run allowing localhost sockets; [successful log](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/core-tests-local-sockets.txt). The initial sandbox run passed 328 and failed 29 because loopback bind returned Operation not permitted; [initial diagnostics](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/core-tests.txt) retained. Repeating the same command with local socket access resolved all 29. |
| `cargo test --offline -p sockudo-protocol --features ai-transport --lib` | **39 passed**; [log](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/protocol-tests.txt). |
| `cargo test --offline -p sockudo-ai-transport --lib` | **18 passed**; [log](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/ai-tests.txt). |
| `cargo test --offline -p sockudo-filter --lib` | **54 passed**; [log](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/filter-tests.txt). |
| `AIT_CONFORMANCE_OFFLINE=1 scripts/ai-conformance-node.sh` | Passed; [offline fixture log](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/ai-conformance-offline.txt). It does not connect to a real Sockudo cluster. |
| `cargo test --locked --offline --manifest-path server-sdks/sockudo-http-rust/Cargo.toml --lib` | **Blocked before tests:** cached registry lacks `crypto_secretbox`; [failure](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/sdk-tests.txt). No SDK runtime coverage claimed and no dependencies downloaded. |
| `cargo build --release --offline --manifest-path audits/performance-2026-09-05/diagnostics/Cargo.toml --target-dir target -j2` | Passed for all eight bins; [build log](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/all-diagnostics-build.txt). |
| `cargo fmt --manifest-path audits/performance-2026-09-05/diagnostics/Cargo.toml` | Applied to isolated diagnostics only; final `--check` also run. |

There were **468 passing crate tests** in the four successful test runs. These were narrow existing tests, not 468 new performance tests. No production source changed, so full `cargo test --workspace`, workspace clippy, docs type/build checks and all feature-specific integration suites were not run. The full AI budget guard, simulator/fuzz campaigns and live backend/provider tests were also not run; source inspection and successful `cargo check --features full` do not substitute for them. Those checks remain necessary for future changes at the risk levels specified in AGENTS.md.

## Existing benchmark executed

Existing benchmark/build, without modifying its source:

```bash
cargo bench -p sockudo-core --bench versioned_message_streaming --offline --no-run
cargo bench -p sockudo-core --bench versioned_message_streaming --offline -- --warm-up-time 1 --measurement-time 2 --sample-size 20
```

[Criterion output](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/versioned-criterion.txt) used one-second warmup, two-second target measurement and 20 Criterion samples. The three times printed by Criterion are confidence-interval lower/point/upper estimates, **not p50/p95/p99**, regardless of a benchmark name containing `p50`.

| Benchmark | Lower / point / upper timing |
|---|---|
| Append 64B to 100KiB state | 2.6443 / 2.6516 / 2.6588µs |
| Warm get_latest after 2,000 appends | 7.3812 / 7.3960 / 7.4211µs |
| 200 leased delivery reservations | 12.836 / 12.940 / 13.049µs |
| Atomic memory append 64B to 100KiB state | 15.014 / 15.476 / 15.805µs |

The suite also printed 15 allocation calls / 7,837 requested bytes for its 32-revision append fixture. Named timing budgets were not exceeded in these samples; this is not a run of the full AI budget guard or evidence against retained-chain scaling. Existing adapter/delta/AI/push/Ably/load/conformance surfaces were inspected before adding probes; their limitations and relevant future commands appear in the detailed reports.

## Reproduce isolated measurements

Build once with the retained lockfile; invoke each bin separately to avoid overlapping measurements:

```bash
cargo build --locked --release --offline --manifest-path audits/performance-2026-09-05/diagnostics/Cargo.toml --target-dir target -j2
/usr/bin/time -v target/release/state_stores
/usr/bin/time -v target/release/services
/usr/bin/time -v target/release/memory_queue
/usr/bin/time -v target/release/presence_retention
target/release/auth_refresh
target/release/conflation
target/release/device_hash
target/release/filter_index_discarded
```

These commands need only cached Rust dependencies and local memory. They send no provider/broker/DB requests and use synthetic identifiers/payloads. Original captured stdout is in the following files; where available, paired `.stderr` files hold `/usr/bin/time -v` CPU and peak RSS. The discarded FilterIndex bin is explicitly not an active production bottleneck.

| Bin / evidence | Workload and interpretation |
|---|---|
| [state_stores source](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/diagnostics/src/bin/state_stores.rs), [run 1](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/state-stores-1.csv), [run 2](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/state-stores-2.csv), [process 1](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/state-stores-1.stderr), [process 2](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/state-stores-2.stderr) | Actual memory versions/annotations/history. Five warmups and 101 timed operations; empirical sorted sample indices 50/95/99 reported as p50/p95/p99. Versions: 16/128/1,024 starting revisions × 256B/4KiB/64KiB. `get_latest` retained size is fixed; mutations add 106 revisions including warmups and replace fixed-size data. Annotation duplicate append fixes projection size at 100/1k/10k. History 1k/10k/100k compares first page with near-tail cursor at limit 100. Append readback reports actual accumulated historical string bytes. Setup is outside individual operation timing but included in process RSS/CPU. |
| [presence_retention source](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/diagnostics/src/bin/presence_retention.rs), [samples](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/presence-retention.csv), [process](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/presence-retention.stderr) | 101 unique-user insert timings after filling count cap 100/1k/10k; uncapped control grows during sampling. Current-thread Tokio. Tests retention rebuild cost, not multi-node first/last timing. |
| [services source](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/diagnostics/src/bin/services.rs), [samples](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/services.txt), [process](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/services.stderr) | Actual memory cache, pages of 256, 1k/5k/10k entries × 64B/4KiB; asserts exact total rows. Three repetitions, first-page and full-sweep timings. Actual CachedAppManager with mocked 20ms app backend/2ms cache SET, concurrency 1/32/128, known/unknown keys, three repetitions and four warm followups. Call counts are deterministic amplification evidence; mock time is not database latency. |
| [memory_queue source](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/diagnostics/src/bin/memory_queue.rs), [samples](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/memory-queue.txt), [process](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/memory-queue.stderr) | Actual MemoryQueue, 500 ordinary enqueue timings after equal-size ready queues seeded with or without 1k/10k/30k dedup IDs, three repetitions. No workers/callbacks during timing; preload excluded. The output's `retained_dedup` field is the seeded row count; `dedup_seed=false` means those rows have **no dedup keys**, despite that field name. |
| [auth_refresh source](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/diagnostics/src/bin/auth_refresh.rs), [original samples](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/auth-refresh-original.csv), [Cargo rerun](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/auth-refresh.csv) | Exact production token-cache module with minimal domain substitutes and scripted source. Concurrent callers 1/16/64, healthy/failing source, five repetitions, current-thread Tokio, 5ms simulated refresh. Healthy call count is one; failing call count follows callers and serializes. Does not measure real TLS/provider latency. |
| [conflation source](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/diagnostics/src/bin/conflation.rs), [original samples](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/conflation-original.txt), [Cargo rerun](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/conflation.txt) | Exact current extraction module; 1/100/1k recipient visits × roughly 1/16/64KiB JSON. Nine samples of 20 component iterations; repeated extraction vs once-and-borrow equivalent extracted result. Does not include real subscribers, compression calculation, protocol envelopes, socket enqueue or network. |
| [device_hash source](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/diagnostics/src/bin/device_hash.rs), [samples](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/device-hash.txt) | Actual push identity hash/verify at the minimum 120k PBKDF2 iterations, 21 samples. Median 8.823ms hash / 8.785ms verify. Verifies per-call CPU cost; executor scheduling delay is source-backed, not load-measured. |
| [discarded source](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/diagnostics/src/bin/filter_index_discarded.rs), [original samples](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/filter-index-discarded-original.txt), [Cargo rerun](/home/radud/Desktop/Code/Rust/sockudo/audits/performance-2026-09-05/results/filter-index-discarded.txt) | Legacy FilterIndex cleanup, 100/1k/10k retained value buckets. Source tracing found no active server registration/lookup. Retained to explain rejection, **excluded from the production ranking**. |

C1 mutation p50 at 1,024 revisions/256B was 77.837µs and 77.807µs in two runs; annotation 10k was 2.546ms and 2.441ms; deep history 100k was 143.361µs and 141.187µs. First history page moved from 9.798µs to 6.151µs, illustrating meaningful run-to-run variance even without a source change. Use longer isolated runs before setting regression thresholds. The standalone reports preserve full sample ranges where measured; no profile-derived attribution or end-to-end p99 is claimed.

## Runtime matrix still needed for implementation

This is **future validation**, not a report of executed tests. Use existing load/conformance tooling and add fixture parameters/counting adapters only where missing. Keep offered and achieved load separate; report drops/rejections/reset outcomes so faster incomplete work cannot pass. Capture throughput, p50/p95/p99, user/system CPU, allocation rate, live/peak RSS, stage queue depth **and bytes/age**, task counts, backend calls/read/write bytes, retries and recovery outcomes. Record exact feature/config/runtime versions and broker prefetch/partition/retention policies.

| Workload | Required cases and observable question |
|---|---|
| Local fanout / predicate / delta | Real socket writer, 1/100/1k/10k recipients, 256B/1KiB/16KiB/64KiB, one hot vs many sparse channels, predicate hit 0/1/100%, delta 0/50/100%, both algorithms, JSON/MessagePack/Protobuf, mixed V1/V2 and append modes. Count serialization/extraction/hash calls and validate reconstructed payloads. |
| Slow consumers and reconnects | One stalled reader plus healthy readers, count/byte limits, 1k/10k connection waves, cold app keys, multiple channels/users per socket, duplicate disconnects. Bound RSS and timer lateness; measure first/last presence and healthy-reader p99. |
| Horizontal adapters | 1 and 3+ nodes for Redis/Sentinel/Cluster, NATS, RabbitMQ, Kafka, Pulsar, Google Pub/Sub and Iggy. Delay/drop backend calls, saturate ingress, fail nodes, restore service. Assert bounded tasks/bytes, ordering, control reserve, dedup and reset-required continuity. Include nodes with no local subscribers. |
| Durable history / mutations / annotations | All five DBs, H=1k/10k/100k, both page directions/deep cursors, message revisions=16/1k/10k, annotations=100/1k/10k, 1/8/32 concurrent publishers across same and independent channels. Record query plans/calls/rows/bytes/locks and async writer lag; cross-node idempotency and retention caps off/count/bytes. |
| Recovery / presence / AI | Hot/cold rewind, until_attach with concurrent mutation, new-user churn, gaps/reset, 0/40/500ms AI windows, one stalled channel among 17+, terminal storms, orphan cleanup on multiple nodes. Validate all original mutations and four serial identities; measure flush/transition deadline lateness. |
| Push / webhooks / queues | Device/client-targeted 1k/10k/100k registries, 64B/4KiB/64KiB templates, multi-provider batches, skewed 1s callbacks, token outage, 429/5xx, lost ACK, partial batch failure, status CAS contention, retry horizon and lifetime log/shard retention. Include every configured queue/store/provider and Lambda cache call counts. |
| Ably / metrics / lifecycle | Idle 1k/10k revocable sessions, 0/10k revocations and attached channels, slow cache, isolated vs saturated stats ACK, long minute-bucket retention, projected recovery/attach gates, write-once channels beyond TTL, annotation type/channel churn. Measure authorization freshness, metric series/RSS/scrape time and logging sink throughput without secrets. |
| Rust SDK | Restore dependencies, test default/no-default/native-tls, signed/encrypted golden requests and scripted HTTP retries before throughput comparison. Use equivalent bodies/idempotency and bounded offered load. |

For any future high-risk implementation, run formatting, focused subsystem tests, relevant integration/conformance, `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings` and affected feature combinations. These are implementation gates, not a claim that an unprovisioned backend was benchmarked during this audit.
