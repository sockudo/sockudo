# AI Transport Load Validation

This directory contains the permanent S14 scale rig for AI Transport. It is
deliberately SDK-free: the runner signs HTTP requests directly and keeps a
logical connection/session model for fleet-sized runs.

Developer smoke:

```bash
make ai-scale-smoke
```

Five-node local cluster:

```bash
docker compose -f docker-compose.ai-transport.yml -f test/load/docker-compose.ai-transport-scale.yml up -d --build
node test/load/ai-scale-runner.mjs --profile test/load/profiles/smoke.json
```

Headline and soak profiles are checked in as production-run configurations and
default to `planOnly` to prevent accidental 1M-connection runs from a laptop:

```bash
node test/load/ai-scale-runner.mjs --profile test/load/profiles/headline-1m.json --plan
node test/load/ai-scale-runner.mjs --profile test/load/profiles/soak-20pct.json --execute
```

Use `--execute` only on a prepared fleet with raised file-descriptor limits,
ephemeral-port capacity, and load generator sharding.
