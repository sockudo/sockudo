# AI Transport Chaos Runs

## Outside-In Binary Chaos

`sockudo-binary-chaos.mjs` is a manual local harness for running real Sockudo
server binaries as child processes while real clients publish and subscribe over
HTTP/WebSocket. It is intentionally outside-in and separate from the deterministic
simulator.

```bash
make binary-chaos CHAOS_SEED=42 CHAOS_DURATION_MS=12000
```

The runner writes a generated server config, process log, and JSON artifact under
`target/outside-in-chaos/<timestamp>-seed-<seed>/`. The artifact records the seed,
effective config, replay command, process restarts/kills, client reconnects, local
TCP proxy delay/drop faults, duplicate publish idempotency probes, push-provider mock
profile when enabled, and observed delivery/recovery counters.

The default `--network-fault-mode proxy` starts Sockudo on an internal local port and
listens on the requested public port with an unprivileged TCP proxy in between.
`--network-fault-mode publisher` keeps the original publisher-side HTTP delay/drop
faults, and `--network-fault-mode off` disables network fault injection.

Push-provider outcomes are optional because they require a push-capable binary. This
manual target starts the fake FCM provider, publishes through Sockudo, and requires
the mock provider to produce at least one seeded fake outcome:

```bash
make binary-chaos-push CHAOS_SEED=42 CHAOS_DURATION_MS=12000
```

Sockudo's provider dispatch layer rejects private/local provider URLs by design. When
that guard prevents the server process from calling the local mock provider, the
artifact records the guarded dispatch attempt and the harness performs a direct
provider probe so the fake provider outcome is still captured with the same seed.

Do not add this runner to CI, scheduled jobs, or GitHub Actions. It is for local
operator and developer experiments where the generated artifact is attached to a
bug report or follow-up deterministic simulator seed.

## Distributed Correctness Release Lane

`distributed-correctness.mjs` is the outside-in rolling-deployment and presence
chaos lane. Unlike the in-process cluster tests, it requires two separately built
Sockudo executables, starts two independent OS processes behind an HTTP/WebSocket
load balancer, and keeps publish plus presence churn active while it:

1. replaces each old process with the new package using rolling `SIGKILL` faults;
2. partitions one node from Redis through a per-node TCP fault proxy;
3. kills that partitioned node while it owns a live presence member;
4. waits for surviving-node dead-owner cleanup, heals the partition, and restarts;
5. audits shared PostgreSQL history and presence history from outside the processes.

The run fails unless old/new executable hashes differ, every logical idempotent
publish is accepted and exists exactly once in durable history, live delivery
contains no duplicate sequence, the final presence set has no ghosts, the dead
owner has exactly one durable and wire-visible `member_added` plus
`member_removed`, and presence-history continuity is complete and non-degraded.
Publish requests use bounded retries with the same idempotency key for retryable
`5xx` responses, including fail-closed coordination errors and a load balancer
observing a killed upstream. The artifact records each raw transient failure and
retry so crash-window repair is both exercised and auditable.

```bash
make distributed-correctness-chaos \
  DISTRIBUTED_CHAOS_OLD_BIN=/path/to/previous-release/sockudo \
  DISTRIBUTED_CHAOS_NEW_BIN=/path/to/release-candidate/sockudo \
  DISTRIBUTED_CHAOS_DURATION_MS=600000
```

The target starts the lightweight Redis and PostgreSQL fixtures from
`docker-compose.test.yml` but leaves them running. Artifacts and per-process logs
are written under `target/distributed-correctness/`; each artifact contains a replay
command, binary SHA-256 identities, the fault timeline, and all audit results. Use a
ten-minute or longer duration for scheduled/release evidence; a 30-second minimum is
enforced for local debugging. The nightly and manual
`.github/workflows/distributed-correctness-chaos.yml` lane builds `HEAD^` (or an
explicit `old_ref`) in an isolated worktree, rolls it to the checked-out candidate,
runs for ten minutes by default, and retains the evidence bundle for 30 days.

For deterministic production-upgrade risk replay, use the simulator upgrade profile instead of this
outside-in binary harness:

```bash
make simulator-upgrade SIM_SEED=48879 SIM_TICKS=1200

cargo run -p sockudo-simulator --bin sockudo-sim -- \
  --seed 43981 \
  --ticks 320 \
  --nodes 4 \
  --upgrade-risk-profile \
  --upgrade-require-coverage \
  --upgrade-schema-prepare-tick 8 \
  --upgrade-start-tick 12 \
  --upgrade-schema-activate-tick 18 \
  --upgrade-restart-duration-ticks 2 \
  --upgrade-interval-ticks 8 \
  --json
```

Run chaos against the AI Transport compose cluster:

```bash
tools/chaos/ai-chaos-runner.sh all
```

Available scenarios:

- `node-kill`: kill one Sockudo node mid-stream and verify sampled transcripts after restart.
- `redis-failover`: restart Redis during appends and verify bounded recovery.
- `partition`: disconnect one Sockudo node from the compose network, heal it, then audit.
- `slow-subscriber`: run a high-fanout smoke profile to verify the existing slow-consumer policy under pressure.
- `clock-skew`: run the scale audit with signed client timestamp skew. Host/container clock mutation requires privileged infrastructure and is documented as a production-run requirement.

Set `RESULT_DIR` to retain JSON artifacts outside `target/ai-chaos`.
The runner publishes its local Redis and PostgreSQL fixtures on ports `26379` and
`25433` by default so it can coexist with the compatibility fixtures. Override
`AI_REDIS_HOST_PORT` or `AI_POSTGRES_HOST_PORT` when those ports are occupied.
The five-node topology uses a bounded 16-connection PostgreSQL pool per store and
a 300-connection fixture ceiling; those values are part of the reproducible chaos
topology, not production sizing guidance.
The local Compose image defaults to non-LTO release codegen with eight codegen units
to keep linking inside bounded Docker Desktop memory. This affects build resource
usage only and is not performance evidence. Set `AI_DOCKER_RELEASE_LTO=true` and
`AI_DOCKER_RELEASE_CODEGEN_UNITS=1` to reproduce the production release profile on
a builder with sufficient memory; the standalone Dockerfile keeps those production
defaults.
