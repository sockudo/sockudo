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
effective config, command, process restarts/kills, client reconnects, publisher-side
delay/drop/duplication, push-provider mock profile when enabled, and observed
delivery/recovery counters.

Push-provider outcomes are optional because they require a push-capable binary:

```bash
make binary-chaos \
  CHAOS_FEATURES="v2,monolith,push-fcm" \
  CHAOS_ARGS="--push-provider-profile flaky --exercise-push true"
```

Do not add this runner to CI, scheduled jobs, or GitHub Actions. It is for local
operator and developer experiments where the generated artifact is attached to a
bug report or follow-up deterministic simulator seed.

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
