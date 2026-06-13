# AI Transport Capacity And Benchmark Plan

Status: benchmark guard live; S14 scale/chaos rig committed; headline capacity runs require
dedicated hardware and are tracked by explicit result manifests.
Scope: S12 performance optimization plus S14 chaos, soak, and scale validation for AI Transport hot
paths.

## Permanent Benchmark Suite

The aggregate Criterion suite lives in `benches/ai` and composes the hot-path crates without
introducing a production dependency:

- `extras_header_validate_budget_mean_10us`: `extras.ai.transport` and `extras.ai.codec` validation.
- `ai_event_name_check_budget_mean_500ns`: AI event-name classification.
- `memory_reserve_local_budget_mean_25us`: local `MemoryVersionStore` serial reservation.
- `leased_clustered_reserve_budget_mean_25us`: clustered-store reservation through `LeasedVersionStore`.
- `versioned_append_memory_budget_mean_75us`: in-memory mutable-message append application.
- `memory_latest_by_history_1000_budget_mean_2ms`: aggregated latest projection/read.
- `rollup_decision_append_budget_mean_25us`: append rollup decision path.
- `rollup_flush_2000_budget_mean_10ms`: rollup decision plus flush of a 2k-append stream.
- `capability_publish_match_budget_mean_1us`: publish capability match.
- `capability_append_any_match_budget_mean_1us`: mutation capability match.
- `discrete_ai_input_publish_to_fanout_budget_mean_10ms`: one serialized discrete AI input fanned to 1k in-process subscribers.
- `streaming_append_rollup_on_1k_budget_mean_10ms`: append stream with rollup enabled and 1k in-process subscribers.
- `streaming_append_rollup_off_1k_budget_mean_10ms`: append stream with rollup disabled and 1k in-process subscribers.

Run the guard:

```bash
scripts/ai-transport-bench-guard.sh
```

The guard records hardware metadata in `target/criterion/ai-transport-hardware.txt`, runs
`cargo bench -p sockudo-ai-benches --bench ai_hot_paths`, parses Criterion mean estimates, and
fails when any checked-in budget in `benches/ai/baselines/ai_hot_paths.budgets.json` is exceeded
by more than 10 percent. The CI job is `.github/workflows/ci.yml` job
`ai-benchmark-regression`.

## Hot-Path Optimization Notes

The S12 pass removed an unnecessary per-append copy in `RollupEngine`: pending rollup state now
keeps only timing metadata and the latest aggregated `PusherMessage`. The versioned-message store
already materializes append state before egress, so rollup does not need to copy each fragment into
`Bytes` for a second fold step. This preserves the store-receives-everything invariant and reduces
rollup egress allocation pressure.

Areas to re-profile before changing production behavior:

- Opaque payload passthrough: do not add serde round-trips to `MessageData` or history `Bytes`.
- Append fanout: one ingest-side message materialization, no per-subscriber payload copies.
- Version-store reads: watch per-channel lock contention in `latest_by_history` and replay scans.
- Timers: rollup flush scheduling should coalesce by tick and avoid per-stream wakeups.
- Extras parsing: validate once at the edge and pass typed borrowed views where possible.

## Cluster Workload

The existing three-node cluster workload is:

```bash
docker compose -f docker-compose.ai-transport.yml up -d --build
node scripts/ai-transport-3node-bench.mjs \
  --streams 10000 \
  --appendsPerStream 3000 \
  --maxAppendAddedP99Ms 5
```

For failure-path and S8-style cluster profiling:

```bash
STREAMS=10000 APPENDS_PER_STREAM=3000 scripts/ai-transport-jepsen-lite.sh
```

Use lower values for developer smoke runs. The full target run needs an 8-core host with enough
file descriptors and ephemeral ports for the connection target.

## S14 Scale Rig

Permanent load tooling lives under `test/load`:

- `test/load/ai-scale-runner.mjs`: dependency-free signed HTTP runner and fleet planner.
- `test/load/profiles/smoke.json`: local executable smoke.
- `test/load/profiles/headline-1m.json`: 1M-connection, 50k-stream, 30-minute target.
- `test/load/profiles/reconnect-storm.json`: 20 percent simultaneous reconnect storm.
- `test/load/profiles/cancel-storm.json`: 10k cancels in 10 seconds.
- `test/load/profiles/soak-20pct.json`: 24-hour soak at 20 percent headline load.
- `test/load/docker-compose.ai-transport-scale.yml`: compose overlay extending the existing
  AI Transport stack to five Sockudo nodes plus a Node load-generator service.

Developer smoke:

```bash
make ai-scale-smoke
```

Five-node cluster:

```bash
docker compose -f docker-compose.ai-transport.yml -f test/load/docker-compose.ai-transport-scale.yml up -d --build
node test/load/ai-scale-runner.mjs --profile test/load/profiles/smoke.json
```

Headline plan:

```bash
node test/load/ai-scale-runner.mjs --profile test/load/profiles/headline-1m.json --plan
```

Production execution:

```bash
node test/load/ai-scale-runner.mjs \
  --profile test/load/profiles/headline-1m.json \
  --execute \
  --urls "$SOCKUDO_NODE_URLS" \
  --metricsUrls "$SOCKUDO_METRICS_URLS" \
  --output docs/specs/ai-transport-results/headline-1m.json
```

Profiles with `planOnly: true` are intentionally protected from accidental laptop execution.
Passing `--execute` is the explicit operator action that starts the workload.

## S14 Chaos Rig

Chaos tooling lives under `tools/chaos`:

```bash
tools/chaos/ai-chaos-runner.sh all
```

The harness covers node kill mid-stream, Redis restart/failover proxy, inter-node partition,
slow-subscriber pressure, and signed client timestamp skew. It writes JSON artifacts to
`target/ai-chaos` by default; set `RESULT_DIR` to preserve artifacts elsewhere.

Accepted limitation: local Docker cannot safely mutate node clocks without privileged containers.
The checked-in `clock-skew` scenario validates signed client timestamp skew and serial monotonicity;
production readiness sign-off still requires privileged node clock skew in the external chaos
environment.

## Profiling Commands

Flamegraph, S3 streaming scenario:

```bash
cargo flamegraph --bin sockudo --features v2,ai-transport,redis,postgres -- \
  --config config/config.ai-transport-compose.toml
```

Tokio console:

```bash
RUSTFLAGS="--cfg tokio_unstable" \
RUST_LOG=info,tokio=trace,runtime=trace \
cargo run -p sockudo --features v2,ai-transport,redis,postgres -- \
  --config config/config.ai-transport-compose.toml
tokio-console
```

Heap tracking:

```bash
heaptrack target/release/sockudo --config config/config.ai-transport-compose.toml
```

## Capacity Targets

Target hardware: one 8-core node unless the scenario explicitly says cluster.

| Scenario | Budget |
| --- | --- |
| 500k idle V2 connections | Memory per connection within +5% of main baseline |
| 10k active streams at 100 tok/s | CPU < 70%, no memory growth over 30 min |
| Rollup window 40 ms | >= 99.9% flushes within window + 5 ms |
| Append fanout | p99 <= 10 ms |
| 50k history queries/min with active streams | p99 <= 25 ms |
| Rollup and tracking memory | <= 4 KiB per active stream, excluding payload |

## S14 Headline Targets

| Scenario | Budget |
| --- | --- |
| 1M concurrent connections, 50k active streams, 30-minute soak | Zero sampled transcript loss, serial monotonicity clean, p99 append-to-delivery < 25 ms intra-region |
| Reconnect storm | 20 percent simultaneous drop resumes within 60 s, no thundering herd, recovery samples clean |
| Cancel storm | 10k cancels in 10 s routed correctly, no stuck active streams after TTL |
| 24h soak at 20 percent load | RSS, file descriptors, Tokio tasks, and store size flat against retention math |
| Chaos | Every finding fixed with a regression test or accepted in this document/runbook |

## Result Recording

Store raw capacity JSON under `docs/specs/ai-transport-results` when the numbers are intended to
be part of a reviewed capacity claim. Keep oversized flamegraphs, tokio-console snapshots, and heap
profiles as PR artifacts. For local runs, keep raw Criterion output under `target/criterion` and
paste the hardware note from `target/criterion/ai-transport-hardware.txt` into the PR. Do not
commit generated flamegraph SVGs or heap profiles unless a release artifact explicitly requires
them.

Current committed result manifests:

| Manifest | Status |
| --- | --- |
| `docs/specs/ai-transport-results/headline-1m.not-run.json` | Not executed locally; production fleet required |
| `docs/specs/ai-transport-results/reconnect-storm.not-run.json` | Not executed locally; production fleet required |
| `docs/specs/ai-transport-results/cancel-storm.not-run.json` | Not executed locally; production fleet required |
| `docs/specs/ai-transport-results/soak-20pct.not-run.json` | Not executed locally; 24-hour fleet run required |

Do not replace these with synthetic numbers. A passing result must come from the runner output and
include hardware notes, Prometheus snapshots, transcript audit summaries, serial audit summaries,
and leak evidence.

## Production Ops Runbook

The operator runbook is `docs/content/docs/server/ai-transport-production-ops.mdx`. It includes the
capacity formulae, alert thresholds tied to S9 metrics, chaos playbooks, and the troubleshooting
table for policy errors, short retention, never-ending turns, suspended-state publishes, bad-token
reconnect loops, and rollup tuning.
