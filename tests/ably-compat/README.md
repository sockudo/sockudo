# Ably Compatibility Smoke And Demo Harness

This harness points the unmodified Ably JavaScript SDK, and one stock
`@ably/ai-transport` smoke demo, at a local Sockudo server.

Required local service:

```bash
cargo run -p sockudo --features "v2,ai-transport,ably-compat,redis,postgres,push" -- \
  --config config/config.toml
```

The default `config/config.toml` app credentials match the harness defaults:
`app-key:app-secret`. AI Transport channels must match `private-ai-`. The Ably
facade is not present unless the server is built with Cargo feature
`ably-compat`.

Environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `ABLY_KEY` | `app-key:app-secret` | Ably SDK key auth value. |
| `ABLY_ENDPOINT` | `127.0.0.1` | Sockudo host exposed through the Ably facade. |
| `ABLY_PORT` | `6001` | Sockudo HTTP/WebSocket port. |
| `ABLY_TLS` | `false` | Set `true` for TLS endpoints. |
| `ABLY_CLIENT_ID` | script-specific | Client identity stamped by the Ably SDK. |
| `ABLY_AGENT_CLIENT_ID` | `sockudo-ably-ait-agent` | Agent identity for the stock AIT chat demo. |
| `ABLY_CHANNEL` | generated | Override the test/demo channel. |
| `ABLY_DEMO_PROMPT` | `Say hello from Sockudo AI Transport.` | Prompt text for the stock AIT chat demo. |
| `ABLY_PROTOCOL_RUN_ID` | timestamp | Stable suffix for protocol discovery channels and client IDs. |
| `ABLY_PROTOCOL_TIMEOUT_MS` | `15000` | Per-lane timeout for protocol discovery. |
| `ABLY_PROTOCOL_OUTPUT` | unset | Optional JSON output path for protocol discovery results. |
| `ABLY_PROTOCOL_STRICT` | `0` | Set `1` to fail when optional broader-protocol lanes fail. |
| `ABLY_BROWSER_ORIGIN_PORTS` | `4173,3001,5174` | Allowed localhost origin ports tried by the Chromium discovery lane. |

```bash
cd tests/ably-compat
npm ci
npm run smoke
npm run protocol:discovery
npm run pubsub:chat
npm run ait:mutable
npm run ait:recovery
npm run ait:chat
```

Repository-level commands:

```bash
make ably-compat-test
make ably-protocol-discovery
make ably-ai-transport-test
make ably-ai-demo
```

`npm run smoke` verifies that the Ably SDK connects, attaches, subscribes,
publishes, receives the same message, reads history, and detaches.

`npm run protocol:discovery` probes a broader stock `ably` SDK surface and emits
a machine-readable scorecard. The required lane is the JSON Realtime Pub/Sub
baseline already covered by the compatibility claim. Optional lanes currently
cover REST time, REST publish/history in JSON and MsgPack, Realtime presence,
token requests, token capability enforcement, Realtime MsgPack Pub/Sub, and
Chromium browser Pub/Sub/history. Optional failures are reported as gaps unless
`ABLY_PROTOCOL_STRICT=1` is set.

`make ably-protocol-discovery` installs the Playwright Chromium browser before
running the discovery script. On minimal Linux CI images, install the Playwright
system dependencies first or run `npx playwright install --with-deps chromium`
in the job setup.

`npm run pubsub:chat` verifies plain Ably Pub/Sub chat behavior with two
separate stock Ably Realtime clients on a normal `chat:` channel. Alice publishes,
Bob receives, Bob replies, Alice receives, and history contains both messages.

`npm run demo:pubsub-chat` starts a browser chat demo at
`http://127.0.0.1:4173/`. Open it in two tabs with different client IDs to test
live chat over Sockudo's Ably-compatible Pub/Sub subset.

The mutable AI Transport harness uses the stock Ably Realtime mutable-message
methods that the Ably AI Transport encoder calls: `appendMessage`,
`updateMessage`, `deleteMessage`, `getMessage`, `getMessageVersions`, and
`history`.

The stock AI Transport chat demo uses `@ably/ai-transport@0.4.0` and
`@ably/ai-transport/vercel` against Sockudo. It creates a client session, sends
one Vercel-style user message, creates an agent run, streams text chunks, ends
the run, and asserts that the client view sees the assistant response.

The recovery harness captures an Ably SDK recovery key during a streamed AI
response, publishes additional append mutations while that client is offline,
then starts a new SDK instance with `recover` and verifies that only the missed
append fragments are replayed and the final aggregate view is complete.
