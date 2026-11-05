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

## V2 capacity probe

`v2-capacity-probe.mjs` measures one node in the shape reported in
sockudo/sockudo#533: Protocol V2 clients on private channels, durable Postgres
history, connection recovery, the Redis adapter and rate limiter. It reports
memory per idle connection at each connection step, then publishes one event
per channel every `TICK_MS` and reports server CPU, publish latency by HTTP
status (`0` is a client timeout, `503` is publish load shedding), and delivery
latency with a count of every expected delivery.

```bash
docker compose -f tests/load/docker-compose.v2-capacity.yml up -d --build
CONTAINER=v2-capacity-sockudo-1 STEPS=500,1000,2000 TICK_MS=500 \
  node tests/load/v2-capacity-probe.mjs
```

The compose file limits Sockudo to `SOCKUDO_CPUS` (default `0.25`) and builds
this checkout for the host architecture with `SOCKUDO_FEATURES` (default
`full,monolith`, as in the published image). With 2,000 clients on 20 channels,
`TICK_MS` of 2000, 500 and 250 offer 1,000, 4,000 and 8,000 deliveries per
second. Against a local binary instead, run it with
`tests/load/v2-capacity-probe.toml` and pass `PID` in place of `CONTAINER`.

On Docker Desktop (macOS) the numbers need two corrections before they can be
compared with a Linux host:

- A fractional CPU limit makes the same work bill more CPU time. A fixed
  sha256 of 64 MiB costs about 0.14 s of CPU with `--cpus 1` or more, 0.15 s at
  0.5 and 0.24 s at 0.25. Runs at 0.25 CPU also switch between two modes about
  1.5x apart in user and system time alike, for the same deliveries, faults and
  context switches. Compare runs only within one mode, and repeat each step.
- Through the published port, about half of Sockudo's CPU is the VM's network
  path (bridge, netfilter, conntrack and virtio in softirq), which is charged to
  the sending container. Sockudo makes one `sendto` per delivered frame. To
  take that path out, run the probe in Sockudo's network namespace:

```bash
docker run --rm --network container:v2-capacity-sockudo-1 \
  -v "$PWD/tests/load:/probe:ro" -e STEPS=2000 -e TICK_MS=500 \
  node:22-slim node /probe/v2-capacity-probe.mjs
```

That form has no `docker` CLI for `CONTAINER`, so read `cpu.stat` from the host
around the publish phase. Measured on an Apple Silicon Mac at 0.25 CPU, 2,000
clients, both feature sets within the same mode (`full,monolith` and
`redis,postgres` were within 5% of each other):

| Path | 1,000/s | 4,000/s | 8,000/s |
| --- | --- | --- | --- |
| published port | 27-41m | 99-168m | 174-238m, 503s in 5 of 6 runs |
| loopback, slow mode | 25-27m | 74-86m | 162-163m |
| loopback, fast mode | 17m | 50-54m | 100-114m, all delivered |
