# Before/after benchmark results — PR snapshot

This draft implements changes for all 45 performance findings. It is **not merge-ready**: several final comparisons and compatibility gates remain open, and the C6 Surreal retention regression needs resolution. Missing comparisons are explicitly marked below; no results have been invented.

Baseline: `5613bb291032b6b7660352974b03da9eb0646da0`. Measurements are local component probes on the same Ryzen 7950X workstation with Rust 1.98.1; release probes use LTO and one codegen unit where recorded in their manifests. Rows summarize representative recorded cases, not production throughput promises. Repetition counts differ by probe; full samples, distributions, commands, fixture details, failures and intermediate candidates are retained in the evidence directories. Some comparisons isolate one algorithm using a controlled current store, as labeled. Historical candidate results are not blanket claims about the final combined tree.

| Finding | Workload | Before → after / qualification |
|---|---|---|
| C1 | Latest lookup, 1,024 revisions | 2,685 ns → 551 ns |
| C2 | 2,001 retained versions, memory allocations | 131,415,582 → 3,530,726 B; durable codec entries 128,976,158 → 1,198,424 B + 128,000 B snapshot; cumulative snapshot writes remain quadratic |
| C3 | 10k annotation duplicate, memory | 2,585,345 → 1,623 ns; durable PG 1,909 → 306 µs, MySQL 2,862 → 2,721, Dynamo 376 → 325, Scylla 221 → 251 (regression), Surreal 435 → 422 |
| C4 | Presence, 100 new users at 10k retained | 1.085 s → 291 µs; cold old-user lookup 2.739 → 9.262 ms (regression for bounded memory) |
| C5 | Deep history page at 100k rows | 141,899 → 5,891 ns |
| C6 | Retention, 8 appends, 1,024 × 16 KiB | PG 16.50 → 4.58 ms; Dynamo 2.740 s → 53.09 ms; MySQL 359.09 → 356.35 ms. Surreal smaller 64 × 16 KiB case 0.474 → 4.60 s (unresolved regression); final comparisons pending |
| C7 | Selective version batches and metadata | Final valid before/after comparison pending; all five durable backend correctness tests pass |
| C8 | Scylla newest 100, 10k × 16 KiB | 86.744 → 1.758 ms; oldest page 67.548 → 0.408 ms |
| C9 | 64 MiB offered to 64 KiB HTTP body limit | 67,108,864 → 69,632 B polled; after rejects with 413, not accepted-throughput comparison |
| C10 | 10k × 16 KiB expired version payloads | All retained owners → zero; allocator RSS unchanged |
| C11 | Rust HTTP SDK, 1 MiB request | 24,164,166 → 22,067,443 B allocated; 4,682 → 4,604 µs |
| C12 | 64 accepted PBKDF operations, healthy timer | 570,257 → 1,608 µs; overload rejects explicitly, not equal offered-throughput comparison |
| F1 | 512 subscribers × 64 KiB, mixed preparation | 16.437 → 2.312 ms; full receive validation 60.224 → 45.607 ms |
| F2 | Redis, 1,024 offered × 16 KiB | 1,024 → 1 active handlers; 46.6 → 8.4 MiB RSS; 65 accepted + 959 explicit gaps. NATS/Rabbit timing pending |
| F3 | Redis delta, 2,048 calls | Same channel 60.379 → 7.370 ms; independent channels 61.329 → 7.632 ms |
| F4 | 512 subscribers × 64 KiB, tag routing | One predicate 537.595 → 21.871 µs; 16 predicates 1,037.270 → 541.293 µs |
| F5 | Kafka, 16 channels, 1,024 exact deliveries | 1.064 s → 269.8 ms; blocked health timer 300.04 → 11.08 ms |
| F6 | AI delivery, healthy channels beside 150 ms stall | 156 → 10 ms |
| F7 | 1,000 empty replay checkpoints, capacity 10k | 800,381,808 → 380,904 B allocated; 2.436 → 0.115 ms |
| F8 | 16 KiB ordinary string encoding | MessagePack 805 → 275 ns; Protobuf 514 → 187 ns. Final versioned encoding rerun pending after copy fix |
| S1 | Memory queue, 500 jobs, 30k dedup entries | First process samples 14,377/14,357/14,349 → 442/393/371 µs; all three process logs included |
| S2 | Enumerate 1,000 apps | 1,460 → 309 µs; shared SETs 2,000 → 0 |
| S3 | 128 concurrent cold app requests | Backend reads 128 → 1; shared writes 256 → 2 |
| S4 | Redis 10k-key sweep | 362–435 → 43–50 ms; live cluster reshard correctness passes |
| S5 | Annotation metric fixture | 1,887,039 → 56 series; scrape 910,831 → 63 µs; exact aggregate retained |
| S6 | 100k WebSocket metric events | 30,462 → 15,744 µs; 43 → 10 allocations/event |
| S7 | 100k offered webhook jobs | 75,234 → 33,797 µs, but accepted count 100k → 2,048 + 97,952 rejected; bounded-resource result only |
| S8 | Redis 1,000 jobs including 10 slow jobs | ~1.04 s → 0.49–0.58 s; exact job counts |
| S9 | Kafka 160 ordered jobs | 1,826,835 → 1,307,016 µs; peak concurrency 1 → 4; Iggy fixture blocked by io_uring |
| S10 | Rabbit four retry attempts | 2,781 → 306,307 µs deliberate retry pacing; SQS/PubSub emulator checks pass |
| S11 | 128 reconnect admissions | 83,553 → 83,776 µs; connections 2–5 → 2; no latency gain |
| S12 | 32 local Lambda HTTP requests | 10,753 → 3,402 µs; 98,469 → 11,396 allocations; remote AWS unmeasured |
| S13 | Healthy endpoint alongside 20 retrying jobs | 500,173 → 131 µs; 41 exact requests |
| P1 | 10k devices, select 10 | 27,852 → 19 µs |
| P2 | Page all 10k devices, page size 100 | 314,548 → 2,226 µs; all-backend operation measurements pending |
| P3 | 64 concurrent credential refreshes during outage | 64 fetches/~389 ms → 1/~6 ms; actual OAuth HTTP probe pending |
| P4 | 64 KiB batch × 1k recipients | Encode 20,223 → 130 µs; decode 22,450 → 385 µs; queue bytes 65,774,886 → 192,511 |
| P5 | Controlled PostgreSQL feedback, current lifecycle store on both sides | 64-job campaign 793 → 446 ms; singleton 125 → 217 ms (durable-safety regression). Unchanged-baseline integrated results also retained |
| P6 | Push lifecycle retention | All five backend restart/time-advance/paused-child tests pass; isolated resource comparison pending |
| P7 | 10k future push jobs + 100 healthy jobs | 19,325 → 143 µs; 603,044,762 → 141,434 B allocated; healthy p99 616 → 5 µs |
| P8 | Prometheus, one app/10k push records | 7,907 → 1,576 µs; 310,156 → 70,156 allocations |
| A1 | Dense Ably connection cohort | Final same-network before/after pending; rootless forwarding stalls confound earlier deadline failures; diagnostic host-network scans 1–2 ms are not a baseline comparison |
| A2 | 100 sequential durable acknowledgements | Memory 1,112,431 → 205 µs; actual Redis 1,127,009 → 7,052 µs; exact persisted counts |
| A3 | Two-minute stats range among 10k records | Memory 15,611 → 7 µs; Redis 784,193 → 287 µs; bytes read 2,550,000 → 3,092 |
| A4 | 128 × 64 KiB messages, 64 subscribers | 18,924 → 17,299 µs; 74,113 → 24,961 allocations; allocated bytes still ~3.2 GB |

## Complete recorded evidence

- Core: [design and measurement log](implementation/core.md), [raw runs](implementation/results/core/).
- Fanout: [design and measurement log](implementation/fanout.md), [raw runs](implementation/results/fanout/).
- Services: [design and measurement log](implementation/services.md), [raw runs](implementation/results/services/).
- Push and Ably: [design and measurement log](implementation/push-ably.md), [raw runs](implementation/results/push-ably/).
- [Original audit and implementation tracker](IMPLEMENTATION.md) retain finding descriptions and historical progress. This snapshot supersedes stale final-workspace-pending notes in those logs.

## Verification at PR preparation

Passed: `cargo test --offline --workspace`; `cargo build --offline -p sockudo --features full`; `cargo clippy --offline --workspace --all-targets -- -D warnings` after correcting two test-only lints. The earlier Clippy failure is retained alongside [the passing rerun](implementation/results/core/pr-workspace-clippy.txt). See [workspace manifest](implementation/results/core/final-workspace-manifest.json).

Also recorded as passing: no-default-features server check; all 13 final live durable version-store tests across PostgreSQL/MySQL/DynamoDB/Scylla/Surreal; five-backend push lifecycle tests; focused push and Ably suites; docs type check and webpack production build; offline AI fixtures. The default docs Turbopack build stalled and was stopped; webpack passed instead.

Still open: final C6/C7/A1 comparisons, P2 backend measurements, P3 OAuth HTTP probe, P6 isolated measurements, final versioned F8 timings, NATS/Rabbit overload timings, AI benchmark guard execution, full final live AI and stock Ably JS conformance, Iggy runtime coverage, native cloud DLQ and broker rebalance/commit-failure scenarios. Passing builds and focused tests do not close those gates. C3 Scylla, C4 cold reads, C6 Surreal and P5 singleton regressions remain visible above.

## Evidence packaging and reproduction

All selected recorded text results, manifests, harnesses and logs are included. Build caches, executables, duplicated baseline/variant source trees, credential fixtures and generated payload fixtures are omitted. [Evidence inventory](implementation/evidence-package.json) records selected files and omitted artifact hashes/sizes. The local working audit also contains large build artifacts that are intentionally not committed.

Reconstruct the immutable baseline with `git archive 5613bb291032b6b7660352974b03da9eb0646da0`. [Variant source overlays](implementation/variant-patches/) preserve changed/added source for review and reconstruction; some original variants were partial workspace copies, so absent unrelated directories are not represented as deletions. Use the associated run manifest and harness, reviewing overlays before applying them. Historical absolute workstation paths need adjustment on another machine. Results from failed or superseded candidates are retained for transparency and must not be substituted for final results.

The implementation keeps one internal storage/queue write path, with automatic legacy readers and no new operator-facing format-version switches. Public Pusher V1, Sockudo V2 and Ably remain in scope. No merge or deployment is part of this PR preparation.
