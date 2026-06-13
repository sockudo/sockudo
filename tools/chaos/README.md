# AI Transport Chaos Runs

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
