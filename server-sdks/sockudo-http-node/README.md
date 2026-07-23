# sockudo

Official Node.js server SDK for [Sockudo](https://github.com/sockudo/sockudo) — a fast, self-hosted WebSocket server with full Pusher HTTP API compatibility.

## Supported Platforms

This SDK supports **Node.js 16+**.

## Installation

For apps, install the published package:

```bash
npm install sockudo
# or: yarn add sockudo
# or: bun add sockudo
```

For contributors working inside this repository, build the SDK from its package directory:

```bash
cd server-sdks/sockudo-http-node
npm install
npm run build
```

## Importing

```javascript
// CommonJS
const { Sockudo } = require("sockudo")

// ESM / TypeScript
import { Sockudo } from "sockudo"
```

All external APIs have TypeScript definitions included — no separate `@types` package required.

## Configuration

```javascript
const sockudo = new Sockudo({
  appId: "your-app-id",
  key: "your-app-key",
  secret: "your-app-secret",
  host: "127.0.0.1",    // self-hosted Sockudo instance
  port: 6001,
  useTLS: false,
})
```

| Option    | Type    | Default      | Description                           |
|-----------|---------|--------------|---------------------------------------|
| `appId`   | string  | —            | Application ID                        |
| `key`     | string  | —            | Application key                       |
| `secret`  | string  | —            | Application secret                    |
| `host`    | string  | `127.0.0.1` | Sockudo server host                   |
| `port`    | number  | `6001`       | Sockudo server port                   |
| `useTLS`  | boolean | `false`      | Use HTTPS/WSS                         |
| `timeout` | number  | `30000`      | Request timeout in milliseconds       |
| `proxy`   | string  | —            | HTTP proxy URL                        |

### Instantiation from URL

```javascript
const sockudo = Sockudo.forURL("http://key:secret@127.0.0.1:6001/apps/app-id")
```

## Publishing Events

### Single Channel

```javascript
await sockudo.trigger("my-channel", "my-event", { message: "hello world" })
```

### Multiple Channels

```javascript
await sockudo.trigger(
  ["channel-1", "channel-2", "channel-3"],
  "my-event",
  { message: "hello world" }
)
```

You can trigger on up to 100 channels in a single call.

### Batch Events

Send multiple events in a single HTTP request (up to 10 events per call):

```javascript
await sockudo.triggerBatch([
  { channel: "channel-1", name: "event-1", data: { x: 1 } },
  { channel: "channel-2", name: "event-2", data: { x: 2 } },
  { channel: "channel-3", name: "event-3", data: { x: 3 } },
])
```

### Excluding a Socket Recipient

Pass `socket_id` to prevent the triggering connection from receiving its own event:

```javascript
await sockudo.trigger("my-channel", "my-event", { message: "hello" }, {
  socket_id: "123.456",
})
```

## Idempotent Publishing

Use `idempotency_key` to safely retry publishes without causing duplicate deliveries. The server deduplicates events with the same key within the configured window.

### Explicit key

```javascript
await sockudo.trigger("my-channel", "my-event", { message: "hello" }, {
  idempotency_key: "order-shipped-order-789",
})
```

### Auto-generated UUID

Pass `true` to have the SDK generate a UUID automatically:

```javascript
await sockudo.trigger("my-channel", "my-event", { message: "hello" }, {
  idempotency_key: true,
})
```

## Channel Authorization

### Private Channel

```javascript
const auth = sockudo.authorizeChannel(socketId, channelName)
// Returns: { auth: "key:signature" }
```

### Presence Channel

Pass user data as the third argument to authorize a presence channel subscription:

```javascript
const auth = sockudo.authorizeChannel(socketId, channelName, {
  user_id: "user-123",
  user_info: {
    name: "Jane Doe",
    email: "jane@example.com",
  },
})
// Returns: { auth: "key:signature", channel_data: "..." }
```

## User Authentication

Authenticate a user connection for user-targeted events:

```javascript
const auth = sockudo.authenticateUser(socketId, {
  id: "user-123",
  user_info: {
    name: "Jane Doe",
    role: "admin",
  },
})
// Returns: { auth: "key:signature", user_data: "..." }
```

## Terminating User Connections

Disconnect all active connections for a given user:

```javascript
await sockudo.terminateUserConnections("user-123")
```

### Force Reconnect User

Close all active connections for a user with code `4200`, prompting clients to reconnect:

```javascript
await sockudo.forceReconnectUser("user-123");
```

## Webhooks

Verify and parse incoming Sockudo webhooks:

```javascript
const webhook = sockudo.webhook({
  rawBody: req.rawBody,     // raw string body
  headers: req.headers,
})

if (webhook.isValid()) {
  const data = webhook.getData()
  const events = webhook.getEvents()
  const time = webhook.getTime()

  for (const event of events) {
    console.log(event.name, event.channel, event.data)
  }
}
```

`isValid()` also accepts additional tokens to check against (useful during key rotation):

```javascript
webhook.isValid([
  { key: "old-key", secret: "old-secret" },
])
```

## Application State

```javascript
// List all channels
const response = await sockudo.get({ path: "/channels", params: {} })
const body = await response.json()

// Get a specific channel
const response = await sockudo.get({ path: "/channels/my-channel", params: {} })

// List users in a presence channel
const response = await sockudo.get({ path: "/channels/presence-room/users" })
```

## Express Integration

### Channel Authorization Endpoint

```javascript
import express from "express"
import { Sockudo } from "sockudo"

const app = express()
const sockudo = new Sockudo({
  appId: process.env.SOCKUDO_APP_ID,
  key: process.env.SOCKUDO_KEY,
  secret: process.env.SOCKUDO_SECRET,
  host: "127.0.0.1",
  port: 6001,
})

app.post("/sockudo/auth", express.urlencoded({ extended: false }), (req, res) => {
  const { socket_id, channel_name } = req.body

  if (!isUserAuthorized(req, channel_name)) {
    return res.status(403).json({ error: "Forbidden" })
  }

  let presenceData
  if (channel_name.startsWith("presence-")) {
    presenceData = {
      user_id: req.user.id,
      user_info: { name: req.user.name },
    }
  }

  const auth = sockudo.authorizeChannel(socket_id, channel_name, presenceData)
  res.json(auth)
})
```

### Webhook Endpoint

```javascript
app.post(
  "/sockudo/webhook",
  express.raw({ type: "application/json" }),
  (req, res) => {
    const webhook = sockudo.webhook({
      rawBody: req.body.toString(),
      headers: req.headers,
    })

    if (!webhook.isValid()) {
      return res.status(401).send("Invalid signature")
    }

    for (const event of webhook.getEvents()) {
      console.log("Webhook event:", event)
    }

    res.sendStatus(200)
  }
)
```

## Fastify Integration

```javascript
import Fastify from "fastify"
import { Sockudo } from "sockudo"

const app = Fastify()
const sockudo = new Sockudo({
  appId: process.env.SOCKUDO_APP_ID,
  key: process.env.SOCKUDO_KEY,
  secret: process.env.SOCKUDO_SECRET,
})

app.addContentTypeParser(
  "application/x-www-form-urlencoded",
  { parseAs: "string" },
  (req, body, done) => {
    done(null, Object.fromEntries(new URLSearchParams(body)))
  }
)

app.post("/sockudo/auth", (req, reply) => {
  const { socket_id, channel_name } = req.body
  const auth = sockudo.authorizeChannel(socket_id, channel_name)
  reply.send(auth)
})
```

## TypeScript

```typescript
import { Sockudo, TriggerOptions, BatchEvent } from "sockudo"

const sockudo = new Sockudo({
  appId: process.env.SOCKUDO_APP_ID!,
  key: process.env.SOCKUDO_KEY!,
  secret: process.env.SOCKUDO_SECRET!,
})

const options: TriggerOptions = {
  socket_id: "123.456",
  idempotency_key: "my-idempotency-key",
}

await sockudo.trigger("my-channel", "my-event", { data: true }, options)

const batch: BatchEvent[] = [
  { channel: "ch-1", name: "event-a", data: { value: 1 } },
  { channel: "ch-2", name: "event-b", data: { value: 2 } },
]

await sockudo.triggerBatch(batch)
```

## Testing

```bash
npm install
npm test
```

## Channel History

```javascript
const page = await sockudo.channelHistory("my-channel", {
  limit: 50,
  direction: "newest_first",
})

const nextPage = await sockudo.channelHistory("my-channel", {
  cursor: "opaque-cursor-from-previous-page",
})
```

## Pusher SDK Compatibility

Sockudo implements the full Pusher HTTP API. If you prefer to use the official `pusher` npm package or are migrating from Pusher, point it at your Sockudo instance without any other changes:

```javascript
const Pusher = require("pusher")

const client = new Pusher({
  appId: "app-id",
  key: "app-key",
  secret: "app-secret",
  host: "127.0.0.1",
  port: "6001",
  useTLS: false,
})
```

All standard Pusher SDK calls work against a self-hosted Sockudo server without modification.

## License

MIT
