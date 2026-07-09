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
