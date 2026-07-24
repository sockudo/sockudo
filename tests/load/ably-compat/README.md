# Ably compatibility capacity evidence

This harness starts real one-node and Redis/Postgres-backed two-node Sockudo
topologies, opens real WebSocket subscribers, and publishes through the real
Ably and native HTTP APIs. Plan output is never accepted as release evidence.

Developer smoke (both one-node and Redis/Postgres-backed two-node topologies):

```bash
node tests/load/ably-compat/capacity-runner.mjs \
  --profile tests/load/ably-compat/profiles/smoke.json \
  --binary target/debug/sockudo --execute \
  --output target/ably-compat-load/smoke.json
```

Release runs use `profiles/release.json`, a release binary built with
`v2,ai-transport,ably-compat,redis,postgres,push,monolith`, and at least three
independent runs on the same controlled host. Two-node runs start the existing
`redis-test` and `postgres-test` Compose services unless
`--manage-services=false` is passed.

Each two-node run uses a run-specific Redis key prefix and PostgreSQL history
table prefix. This prevents retained rows from an earlier run from changing a
later run's query plan or latency while keeping the backing services and all
durability paths real.

The result records the commit, binary hash, exact config hash, redacted runtime
overrides, hardware/tool versions, per-scenario latency/throughput/correctness,
RSS/CPU samples, Linux process-data (heap) bytes when available, allocator bytes
when available, queue and compatibility counters, and whether memory plateaued
while peers were stalled and after disconnect. A per-run payload sentinel and
the harness credentials are checked against every node log without writing the
sentinel to the result. The runner, streaming evidence helper, selected profile,
and budget file are each hashed into the result so dirty-tree evidence still
identifies the exact harness inputs.

Delivery correctness is accounted for exactly as messages arrive with one
bounded bitmap per subscriber. Latency evidence uses a deterministic reservoir
capped at 100,000 samples by default (`maxLatencySamples` in a profile may set
a different positive bound); reports include both the total observation count
and retained sample count. Full protocol payloads are never retained by the
load driver, so the 64-KiB and stalled-peer scenarios cannot make the evidence
process grow in proportion to delivered payload bytes.

Validate machine-readable evidence and Criterion comparisons with:

```bash
python3 scripts/ably-compat-release-guard.py \
  --budgets tests/load/ably-compat/budgets.json \
  --criterion-root target/criterion \
  --require-independent-runs \
  --load-result target/ably-compat-load/run-1.json \
  --load-result target/ably-compat-load/run-2.json \
  --load-result target/ably-compat-load/run-3.json
```

Pass matching `--baseline-load-result` files to enable the one-sided
statistical regression gate. Criterion's own confidence interval is used when
`change/estimates.json` exists. CI benchmarks the pull request base and head on
the same runner, then passes `--require-criterion-comparison` so missing
same-run comparison evidence fails closed. When the first comparison exceeds a
budget, CI takes one bounded confirmation sample of the head revision against
the same base before enforcing the final result.
