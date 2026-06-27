# Sockudo Dashboard Demo Guide

This guide is for an end-to-end demo call using the test/demo console in
`tests/dashboard`. It is written for a live walkthrough on Thursday,
June 25, 2026.

The strongest story to tell:

> Sockudo is a Pusher-compatible realtime server first, so existing clients can
> connect, subscribe, authenticate, and publish in familiar ways. On top of that
> compatibility layer, Sockudo adds Protocol V2 features for durable history,
> recovery, mutable/versioned messages, annotations, push, filtering, delta
> compression, observability, and AI Transport.

## Before The Call

Use a clean local demo app. The defaults in the dashboard match
`config/config.toml`:

- app id: `app-id`
- app key: `app-key`
- app secret: `app-secret`
- Sockudo HTTP/WebSocket: `127.0.0.1:6001`
- metrics: `127.0.0.1:9601`
- dashboard auth helper: `127.0.0.1:3457`
- dashboard UI: `127.0.0.1:3456`

Recommended local commands:

```bash
cargo run -p sockudo --features "versioned-messages,ai-transport,push-webpush,push-apns,monolith" -- --config config/config.toml
```

For the default AI Chat path, keep Ollama running locally and pull the model the
dashboard uses by default:

```bash
ollama serve
ollama pull llama3.2
```

In another terminal:

```bash
cd tests/dashboard
bun install
bun run dev:all
```

Quick checks:

```bash
curl -f http://127.0.0.1:6001/up/app-id
curl -f http://127.0.0.1:9601/metrics | head
curl -f http://127.0.0.1:3457/health
```

Prepare two browser tabs if you want to show presence join/leave. Keep the
Event Log tab available because it is the best "proof surface" during the demo.

Important framing:

- `tests/dashboard` is a testing/demo console, not the production operator UI.
- App secrets and provider credentials are entered in the browser only for this
  local demo. In production, trusted backends/server SDKs should sign requests.
- With the default config, history, versioned messages, push, and presence
  history use memory storage. They prove API behavior, but they do not survive a
  server restart. For restart-proof or multi-node proof, use Redis/Postgres
  backed configuration such as the AI Transport compose setup.
- Real Web Push requires valid VAPID keys, browser permission, and provider
  dispatch workers. You can still demo push admission/status APIs without real
  provider delivery.
- Rate limiting is disabled in the default config, so the rate probe normally
  shows successful requests unless you enable limits.
- Webhook trigger proves the publish path that would enqueue webhook delivery;
  you need webhook targets configured if you want to show outbound delivery.
- AI Chat uses the local auth helper to proxy Ollama at
  `http://127.0.0.1:11434` by default. Set `OLLAMA_URL` or `OLLAMA_MODEL`
  before `bun run dev:all` if your local model is elsewhere.

## Recommended Live Flow

### 20 Minute Version

1. Demo Console: explain the storyline and use the guided senders to seed
   realtime, history, mutable, push, and AI evidence.
2. Connection: connect, ping, and sign in.
3. Channels + Presence: subscribe public/private/presence, use the channel
   sender, show auth and member activity.
4. HTTP API + Event Log: show signed API responses and arriving live events.
5. Recovery: fetch durable history, show serials/cursors.
6. Mutable Messages: show the created/updated/appended/annotated record.
7. AI Chat: run an Ollama prompt, show streaming appends, push publish, and
   history recovery.
8. Ops: scrape health/stats/metrics.

### 45-60 Minute Version

Use the 20 minute version, then add:

- Presence History snapshot/reset.
- Tag Filters builder.
- Delta compression burst.
- Push lifecycle.
- AI Transport internals.
- Rate probe and webhook trigger.

## Exact Opening Script

"I will use this test dashboard as a single end-to-end console. It connects like
a Pusher-compatible client, signs HTTP API requests like a server SDK would, and
then exercises Sockudo V2 features: durable history, recovery, versioned
messages, annotations, delta compression, push, and AI Transport. The UI is not
the commercial operator dashboard; it is a compact demo surface for proving the
runtime behavior."

## Panel By Panel

### Demo

What it shows:

- A feature map for the whole console.
- A recommended sequence: Connect, Channels, Publish, History, Messages, Push,
  AI Chat, AI Internals, Ops, Delta, Events.
- Guided demo senders for realtime, history, mutable messages, push, and AI
  Transport.
- Runtime evidence: connection state, subscribed channel count, event count,
  wire format, delta message count.

How to present:

- Start here for 30 seconds, then run the guided senders if you want the rest of
  the dashboard preloaded with evidence.
- Say: "This page is the map. I will start with the compatibility path, then
  move into stateful V2 features."

### Connection

What it shows:

- WebSocket connection to Sockudo using `@sockudo/client`.
- Protocol V2 over Pusher protocol v7.
- Configurable app key, secret, app id, host, port, TLS, wire format, auth
  endpoints, and AI append rollup window.
- Quick actions: ping, user sign-in, reconnect.
- Local Bun auth server health.

How it works:

- The dashboard creates a Sockudo/Pusher client with `protocolVersion = 2`.
- V2 events use `sockudo:` and `sockudo_internal:` prefixes. V1-compatible
  clients continue to use Pusher-shaped behavior.
- Private and presence subscriptions call `/pusher/auth`.
- User sign-in calls `/pusher/user-auth`.
- The local auth helper uses the Node `pusher` SDK to generate HMAC signatures
  accepted by Sockudo.

Demo clicks:

1. Confirm Auth Server is online.
2. Click Connect.
3. Point to socket id and endpoint.
4. Click Ping and show `sockudo:pong` in Event Log.
5. Click Sign In and explain user authentication.

Talk track:

"The first thing to prove is the compatibility surface: a normal realtime client
connects over WebSocket, gets a socket id, can ping, and can authenticate a user
or protected channel using Pusher-style signed auth. From there we can opt into
Sockudo-native V2 features."

### Channels

What it shows:

- Public channels.
- Private channels.
- Presence channels.
- Private encrypted channel names.
- Cache channels.
- Namespaced channels such as `ticker:btc`.
- Wildcard channels such as `ticker:*`.
- Meta channels such as `[meta]ticker:btc`.
- User-limited names such as `presence-room#demo-user`.
- Event binding and client events on private/presence channels.

How it works:

- Channel type is inferred from the name prefix or pattern.
- Private/presence channels require the auth helper.
- Presence channels include member data from the auth response.
- Client events are only shown for private/presence channels because they are a
  trusted authenticated client path.
- Namespaces and user-limited channels are policy-controlled in `config.toml`.

Demo clicks:

1. Subscribe to `my-channel`.
2. Subscribe to `private-chat`.
3. Subscribe to `presence-room`.
4. Subscribe to `ticker:btc`, `ticker:*`, and `[meta]ticker:btc`.
5. Select `private-chat`, bind an event, then trigger `client-message`.
6. Show the event log frames.

Talk track:

"Channels are still familiar, but Sockudo also lets us express product structure
through namespaces, wildcard subscriptions, meta channels, and user-limited
channel names. That becomes useful for market data, multi-tenant rooms, and
stateful features."

Caveat:

- Do not spend much time on encrypted channels unless you have prepared the
  encryption-key flow. It is enough to mention that protected channel naming and
  auth are present.

### Events

What it shows:

- Live inbound, outbound, and system events.
- Search/filter by event name, channel, or payload.
- Pause and clear controls.
- Expandable JSON payloads.

How to present:

- Keep this tab open after each operation.
- Use it as runtime proof: "Here is the WebSocket frame/event that came back."

Talk track:

"This event log is useful during the demo because it shows the realtime side of
the system, not just HTTP responses."

### Presence

What it shows:

- Joining presence channels.
- Current members and member count.
- Member add/remove events.
- HTTP API query for presence users.

How it works:

- The auth server assigns a demo user per browser session.
- Presence auth returns `user_id` and `user_info`.
- Sockudo tracks active membership and exposes current users through the HTTP
  API.

Demo clicks:

1. Join `presence-room`.
2. Open a second browser tab and join the same room.
3. Show `member_added`.
4. Close or disconnect the second tab and show `member_removed`.
5. Query channel users via HTTP API.

Talk track:

"Presence is more than pub/sub. The server knows who is currently in a channel,
emits membership transitions, and exposes current membership through the HTTP
API for backends."

### HTTP API

What it shows:

- Pusher-compatible signed REST API.
- Publish single event.
- Publish batch events.
- List channels and read channel info.
- Read channel history, state, and reset history.
- Read presence history, state, historical snapshots, and reset presence
  history.
- Health check.
- Terminate user connections.

How it works:

- The dashboard signs requests with HMAC SHA-256 using app secret.
- For body requests it computes `body_md5`, then signs
  `METHOD\n/apps/{appId}/path\nsorted_query`.
- Production clients should not do this from a browser. Your backend or server
  SDK owns the secret.

Demo clicks:

1. Subscribe to `my-channel` first.
2. Publish to `my-channel` with event `my-event`.
3. Show HTTP response and then Event Log arrival.
4. Send a batch.
5. List channels.
6. Use History or Presence Hist tabs if you have already produced data.

Talk track:

"The HTTP API is the trusted backend side. A backend publishes events, lists
state, reads history, manages users, and controls push. The signing model
matches the Pusher/server-SDK style, which helps migration."

### History, Rewind, Recovery

What it shows:

- Durable history writes.
- Message, history, and delivery serials.
- Paginated history reads with direction and cursor.
- History state.
- Manual V2 resume positions.
- Drop/reconnect helper.

How it works:

- Publish uses `/events` with `info:
  message_serial,history_serial,delivery_serial`.
- History is distinct from the hot connection recovery buffer.
- Cursors are opaque and page through retained history.
- Resume positions contain a serial, optionally a stream id and last message id.
- If continuity cannot be proven, production recovery should fail closed rather
  than silently skipping messages.

Demo clicks:

1. Connect and subscribe to `history-room`.
2. Publish 5 events.
3. Fetch history.
4. Show serials and `next_cursor`.
5. Click State.
6. Click Use Position.
7. Optionally click Drop, Reconnect, then Send Resume.

Talk track:

"This is one of the most important V2 features. Connected clients get live
delivery, but backends and reconnecting clients can also read durable channel
history by cursor and resume from explicit positions. This is the foundation for
rewind, recovery, and AI stream reconstruction."

Caveat:

- Default demo storage is memory. It is durable inside this running process, not
  across server restarts. For restart-proof demos use Postgres-backed history.

### Versioned Messages And Annotations

What it shows:

- Create a message with a stable `message_id`.
- Receive a `message_serial`.
- Read latest message state.
- Update message data.
- Append incremental data.
- Delete/tombstone message state.
- Read full version history.
- Publish/list/delete annotations.

How it works:

- Versioned messages build on the version store and durable history.
- Mutations are actions: create, update, append, delete.
- Each mutation can include `op_id` for idempotency.
- Appends are important for streaming workloads, especially AI output.
- Annotations are separate from message mutation auth and can represent
  reactions, receipts, labels, moderation state, or summaries.

Demo clicks:

1. Subscribe to `versioned-room`.
2. Click Create and copy/notice the returned message serial.
3. Click Latest.
4. Click Update.
5. Click Append with status `streaming` or `complete`.
6. Click Versions.
7. Add an annotation, list it, optionally delete it.

Talk track:

"Instead of treating events as disposable, Sockudo can retain a versioned
message object. Applications can update, append, delete, and annotate the
message while preserving a version trail. That is useful for collaborative apps,
moderation, chat corrections, and AI streams."

### Tag Filters

What it shows:

- A builder for all 13 filter operators:
  `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `nin`, `exists`, `nexists`,
  `prefix`, `suffix`, `contains`.
- AND/OR composition.
- Generated `@sockudo/client/filter` code.
- Filter JSON shape.
- Local test evaluation against sample tags.

How it works:

- Tag filters let clients receive only messages matching metadata/tags.
- This panel is a builder and local evaluator. Pair it with tagged publishes
  and SDK filtered subscriptions when showing actual delivery behavior.

Demo clicks:

1. Add `symbol eq BTC`.
2. Add `volume gt 1000`.
3. Test against the default tags.
4. Show generated client code and JSON.

Talk track:

"Tag filtering is for high-volume channels where clients want subsets without
creating thousands of tiny channels. For example: one market data stream, but
clients select symbols, exchanges, or thresholds."

### Delta Compression

What it shows:

- Client-side delta statistics.
- Full messages vs delta messages.
- Original bytes vs wire bytes.
- Bandwidth saved and saved percent.
- Burst of large similar market data payloads.

How it works:

- The dashboard enables delta compression in `@sockudo/client`.
- The default config enables delta compression for `ticker:*`.
- `ticker:*` uses Fossil delta with `item_id` as the conflation key.
- First messages are full; later similar messages can be sent as deltas.
- Full snapshots are still sent periodically to keep state safe.

Demo clicks:

1. Connect.
2. Subscribe to `ticker:btc`.
3. Send 20 messages.
4. Show delta message count, bytes saved, and percentage.
5. Open Event Log and show `sockudo:delta*` events if visible.

Talk track:

"This is useful when the same object changes frequently: market prices,
presence/cursor state, dashboards, or streaming text. We avoid sending the whole
payload every time when the server and client can safely exchange deltas."

### Operations

What it shows:

- `/live`, `/up`, and app-specific `/up/{appId}` probes.
- Usage and stats.
- Prometheus `/metrics`.
- Signed burst rate probe.
- Webhook publish trigger path.

How it works:

- Health endpoints are unauthenticated operational probes.
- Stats and usage expose runtime state.
- Metrics are exposed on the metrics port with the configured prefix.
- The rate probe repeatedly sends signed API requests.
- The webhook panel publishes an event with an idempotency key to exercise the
  path that can drive webhook delivery.

Demo clicks:

1. Click Live, Up, App.
2. Click Usage and Stats.
3. Scrape Metrics.
4. Run Signed Burst.
5. Publish webhook probe if you want to show idempotent publish input.

Talk track:

"For production, the question is not only whether messages flow. Operators need
health checks, metrics, usage visibility, and clear failure signals. Sockudo
keeps those as first-class surfaces."

Caveats:

- Rate limit behavior needs rate limiting enabled.
- Webhook delivery needs webhook targets configured.

### Push Notifications

What it shows:

- Push provider credentials: Web Push and APNs.
- Push templates.
- Device registration.
- Browser Web Push subscription helper.
- Channel subscriptions for devices.
- Publish to channel, client, or device.
- Publish status lookup.
- Delivery feedback ingestion.

How it works:

- Push API calls use signed HTTP requests plus `x-sockudo-push-capability`.
- Device registration can rotate a device identity token.
- Channel subscriptions map devices/clients to realtime channels.
- Publish returns an admission/status record; dispatch and feedback can be
  tracked separately.
- Provider credentials and payload redaction are production security concerns.

Demo clicks for API lifecycle:

1. Save/list a template.
2. Register a demo device.
3. Upsert a subscription for `orders`.
4. Publish to channel.
5. Check status.
6. Post delivery feedback.

Demo clicks for real browser push, if prepared:

1. Paste valid Web Push VAPID public/private keys.
2. Click Browser to create a browser push subscription.
3. Register the device.
4. Subscribe it to `orders`.
5. Publish to channel or device.
6. Show browser receipt.

Talk track:

"Push is not just a one-off send call. Sockudo models the full lifecycle:
provider credentials, devices, channel subscriptions, publish admission,
status retention, retries, and provider feedback. That matters once push becomes
operational, not just a demo notification."

Caveats:

- Never paste production credentials in a shared demo.
- Real delivery needs provider credentials and worker/provider setup.
- Browser push requires permission and service worker support.

### AI Chat

What it shows:

- A user-facing AI chat demo.
- Demo agent mode that streams through Sockudo.
- External agent mode that waits for another service to respond on the channel.
- Streaming assistant output through versioned appends.
- Stop/cancel behavior.
- Recover history from retained channel history.
- Configurable append rollup window.

How it works:

- User input is published as `ai-input`.
- Assistant output is created as `ai-output`.
- Assistant tokens/chunks are appended to the output message.
- The terminal update marks the stream `complete` or `cancelled`.
- AI metadata lives under `extras.ai.transport` with turn id, role, and status.
- History can rebuild the chat after refresh/reconnect.

Demo clicks:

1. Connect.
2. Use channel `private-ai-demo`.
3. Keep mode on Demo agent.
4. Click one suggested prompt.
5. Show text streaming into the assistant bubble.
6. Click Recover history.
7. Optionally start another prompt and click Stop.

Talk track:

"This looks like a normal chat UI, but the important part is underneath. The
stream is made of durable, versioned appends. That means live users see the
stream, history can rebuild it, recovery can resume it, and backends can inspect
the original mutations."

Caveat:

- Demo agent is local dashboard logic. It proves the transport path, not an LLM
  integration.
- External agent mode needs an outside agent service listening on the channel.

### AI Transport

What it shows:

- The low-level AI lifecycle:
  `ai-input`, `ai-output`, append chunks, terminal status.
- Latest aggregate read.
- Version history read.
- Channel/history evidence.
- Append rollup window.

How it works:

- `ai-input` records the user turn.
- `ai-output` creates the assistant message with streaming status.
- Append calls add chunks to the same message serial.
- Terminal update marks complete or cancelled.
- Append rollup only affects WebSocket egress coalescing. Persistence, history,
  version storage, webhooks, push, and recovery still see the original
  mutations.

Demo clicks:

1. Subscribe to `private-ai-demo`.
2. Click Run Turn.
3. Show Lifecycle response.
4. Show Latest Aggregate.
5. Show Versions.
6. Show Channel/History.

Talk track:

"This is the internal proof for the AI Chat panel. Sockudo does not need a
separate AI-specific persistence model. It uses the same durable history and
versioned-message primitives, plus AI metadata and append rollup for efficient
streaming."

## Good Answers To Likely Questions

Q: Is this Pusher compatible?

A: Yes. Sockudo preserves Pusher-compatible connection, auth, channel, event,
and HTTP API behavior. The demo uses Protocol V2 so we can show Sockudo-native
features, but V1 compatibility remains the migration path.

Q: What is Protocol V2?

A: V2 keeps the realtime model but adds Sockudo-native semantics: V2 event
prefixes, durable history/recovery, versioned messages, annotations, delta,
filters, push helper workflows, and AI Transport. V2-only fields are gated and
should not leak to V1 clients.

Q: Is history actually durable?

A: The API and semantics are durable-history semantics. The default local demo
uses memory storage, so it is retained only while the server process runs. In
production or a stronger demo, use Postgres/MySQL/Surreal/Scylla/Dynamo-style
backends according to the configured driver.

Q: How does this scale horizontally?

A: The adapter layer handles fanout and recovery coordination. The local demo is
single node, but Sockudo has adapter backends such as Redis, NATS, Kafka, Iggy,
and others. For stateful V2 features, use shared cache/queue/history/version
stores so nodes can prove continuity and share state.

Q: Why versioned messages instead of just sending another event?

A: Some application state has identity: chat messages, AI responses, moderated
content, collaborative objects. Versioned messages let clients receive live
mutations while backends can read latest state, version history, and annotations
by stable message serial.

Q: Does AI rollup lose data?

A: No. Append rollup is egress-only. It can coalesce rapid appends for WebSocket
delivery, but persistence, history, recovery, webhooks, push, and version store
state still see the original mutations.

Q: Is push part of realtime or separate?

A: It is complementary. WebSockets are for connected clients. Push is for
offline/background devices and uses retained status, retries, credentials,
subscriptions, and provider feedback.

Q: Why should someone choose Sockudo?

A: The practical pitch is migration plus growth. Existing Pusher-style apps can
move without rewriting their realtime model, then adopt stateful features when
they need recovery, history, mutable messages, annotations, push, efficient
high-volume streams, or AI streaming.

## Demo Risks And How To Avoid Them

- If private or presence channels fail, check the auth server at
  `http://127.0.0.1:3457/health`.
- If HTTP API calls fail, confirm app id/key/secret match `config.toml`.
- If AI panels fail, confirm the server was built with `ai-transport` and
  `versioned-messages` and that `[ai_transport]` is enabled.
- If mutable message endpoints fail, confirm `versioned_messages.enabled = true`
  and the binary includes `versioned-messages`.
- If push endpoints are missing, confirm the binary includes `push` provider
  features.
- If delta stats stay at zero, confirm you are connected, subscribed to
  `ticker:btc`, using V2, and sending enough similar messages.
- If presence has only one user, open a second browser tab or profile.
- If rate probe does not show 429s, that is expected with default rate limiting
  disabled.
- If Web Push does not deliver, fall back to explaining the lifecycle API and
  show status/feedback.

## Closing Script

"The main thing I wanted to show is that Sockudo is not only a fast Pusher-style
WebSocket server. It keeps the compatibility path, then adds durable state:
history, recovery, versioned messages, annotations, push, compression, filters,
and AI streaming on the same primitives. That gives applications a migration
path today and a way to build more stateful realtime workflows without adding a
separate event/history system."
