# Ably Compatibility Smoke And Demo Harness

This harness points the unmodified Ably JavaScript SDKs, and one stock
`@ably/ai-transport` smoke demo, at a local Sockudo server. Ably publishes two
SDK lines: the `ably` package, which `@ably/ai-transport` still peers on, and
the 3.x `@ably/pubsub-device` and `@ably/pubsub-server` packages. Both are
covered.

Required local service:

```bash
PUSH_STORAGE_DRIVER=memory PUSH_QUEUE_DRIVER=memory PUSH_ALLOW_MEMORY_DRIVERS=true \
  cargo run -p sockudo --features "v2,ai-transport,ably-compat,redis,postgres,push" -- \
  --config config/config.toml
```

The push overrides are needed because the default config selects a `mysql` push
store. The default `config/config.toml` app credentials match the harness defaults:
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
| `ABLY_AGENT_CLIENT_ID` | unset | Agent identity for the stock AIT chat demo. |
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
npm run pubsub:v3
npm run multi-key
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

`npm run pubsub:v3` does the same across the Ably 3.x clients: a
`@ably/pubsub-device` client and a `@ably/pubsub-server` realtime client exchange
messages on one channel, and a `@ably/pubsub-server` HTTP client publishes and
reads history.

`npm run multi-key` consumes the generated six-key conformance fixture. It
proves that a capability-scoped key can publish and enter presence while the
primary key observes the same live delivery, history, and presence state, and
that a subscribe-only key cannot publish. Generate the fixture by provisioning
the upstream `ably-common` test app through the local sandbox, which starts a
dedicated Sockudo for it (`test-app-setup.json` ships in an `ably-js` checkout
under `packages/core-impl/test/common/ably-common/test-resources/`):

```bash
node tests/ably-compat/upstream-sandbox.mjs --sockudo-bin target/debug/sockudo \
  --config config/config.toml --listen 127.0.0.1:9080 &
jq .post_apps <ably-js>/packages/core-impl/test/common/ably-common/test-resources/test-app-setup.json |
  curl -s -X POST -H 'content-type: application/json' --data @- \
    http://127.0.0.1:9080/apps > /tmp/ably-test-app.json
cd tests/ably-compat
ABLY_TEST_APP_FILE=/tmp/ably-test-app.json \
  ABLY_PORT="$(jq -r .port /tmp/ably-test-app.json)" npm run multi-key
```

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

The stock AI Transport chat demo uses `@ably/ai-transport@0.10.0` and the
`@ably/ai-transport/vercel` codec against Sockudo. A client and an agent each
create a transport on one channel. The client sends a Vercel-style user message,
the agent receives it and pipes text chunks back, and the client asserts that it
received the whole reply.

The recovery harness captures an Ably SDK recovery key during a streamed AI
response, publishes additional append mutations while that client is offline,
then starts a new SDK instance with `recover` and verifies that only the missed
append fragments are replayed and the final aggregate view is complete.
