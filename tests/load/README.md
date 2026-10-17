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

## Pong-timeout cleanup leak

`pong-timeout-leak.mjs` reproduces the Protocol V1 leak where connections that
hit the pong timeout (close 4201) stayed in the adapter and their presence
channels after their sockets were gone, so `sockudo_connected` drifted above the
real socket count. It opens raw WebSocket clients that each join a
`presence-user-*` channel, stop answering `pusher:ping`, and after every round
compares the server's view (connected gauge, presence-channel gauge,
connections minus disconnections) with reality (no client sockets left).

Peer models (`--peer`):

- `throttled-tab` (default): a background browser tab. Page JS never answers
  the ping, but the browser answers the server's Close frame at once, so the
  reader's cleanup races the timeout task's cleanup. This is what triggers the
  leak.
- `dead`: the peer stops reading entirely.
- `clean`: every client closes normally at once; the round time is how long the
  server needs to count all the disconnects. Use it to compare cleanup cost.

```bash
redis-server --port 6379 &
ADAPTER_DRIVER=redis CACHE_DRIVER=redis \
  ./target/release/sockudo --config tests/load/pong-timeout-leak.toml &
node tests/load/pong-timeout-leak.mjs --connections 8000 --rounds 3 --pid "$(pgrep -n sockudo)"
```

Raise the open-file limit first (`ulimit -n 65536`). The config sets
`activity_timeout = 5`, so a timeout round takes about 35 s (5 s plus the 30 s
pong timeout). `tokio_active_tasks` is about four per live connection and is
not a leak signal by itself. RSS stays high after every client is gone because
the allocator keeps freed memory, so compare `leakedConnections` and
`leakedPresenceChannels` instead.

Results on one macOS node with the Redis adapter, 8,000 connections per round
(timeout scenarios: 3 rounds; `clean`: 5 rounds, run twice per build):

| Scenario | 5.1.0 | master with #470 | cancel-safe cleanup |
| --- | --- | --- | --- |
| `throttled-tab`, 24,000 pong timeouts | 13 connections and 13 presence channels leaked (5, 8, 13 after each round) | 0 | 0 |
| `dead`, 8,000 pong timeouts | 0 | 0 | 0 |
| `clean`, median time to count 8,000 disconnects | n/a | 260.5 ms | 259.0 ms |
