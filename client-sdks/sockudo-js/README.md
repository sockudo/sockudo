# @sockudo/client

Sockudo JavaScript client SDK with modern runtime targets:
- Web
- Node.js
- Web Worker
- React hooks
- Vue composables
- React Native
- NativeScript

## Install

For apps, install the published package:

```bash
npm install @sockudo/client
# or: bun add @sockudo/client
# or: pnpm add @sockudo/client
```

For contributors working inside this repository, build the SDK from its package directory:

```bash
cd client-sdks/sockudo-js
bun install
bun run build:all
```

## Runtime Imports

Use the entrypoint that matches your runtime:

```ts
// Browser / default
import Sockudo from "@sockudo/client";

// Filter helper entrypoint
import { Filter } from "@sockudo/client/filter";

// With encryption
import SockudoEncrypted from "@sockudo/client/with-encryption";

// Worker
import WorkerSockudo from "@sockudo/client/worker";

// Worker with encryption
import WorkerEncrypted from "@sockudo/client/worker/with-encryption";

// React hooks
import { SockudoProvider, useChannel } from "@sockudo/client/react";

// Vue composables
import { createSockudoPlugin, useChannel as useSockudoChannel } from "@sockudo/client/vue";

// React Native
import ReactNativeSockudo from "@sockudo/client/react-native";

// NativeScript
import NativeScriptSockudo from "@sockudo/client/nativescript";
```

The package also exposes runtime-aware fields in `package.json` (`main`, `browser`, `react-native`, `nativescript`, `exports`) so bundlers can resolve correctly.

## Quick Start

```ts
import Sockudo from "@sockudo/client";

const sockudo = new Sockudo("app-key", {
  wsHost: "your-sockudo-host",
  wsPort: 6001,
  wssPort: 6001,
  forceTLS: true,
  enabledTransports: ["ws", "wss"],
});

const channel = sockudo.subscribe("public-updates");
channel.bind("message", (payload: unknown) => {
  console.log(payload);
});
```

The default client mode is Protocol V1 compatibility (`protocol=7`). Opt into Protocol V2 explicitly when you want Sockudo-native event prefixes and V2-only features.

```ts
const sockudoV2 = new Sockudo("app-key", {
  wsHost: "your-sockudo-host",
  wsPort: 6001,
  forceTLS: true,
  protocolVersion: 2,
});
```

Protocol V2 heartbeat behavior:

- the server uses native WebSocket ping/pong frames for automatic liveness
- browser-style runtimes that do not expose native ping APIs may still use lightweight `sockudo:ping` / `sockudo:pong` fallback messages for client-side activity checks
- fallback heartbeat messages are not part of V2 broadcast continuity and do not carry `message_id`, `serial`, or `stream_id`

Protocol V2 also supports `wireFormat: "messagepack"` and
`wireFormat: "protobuf"`. Both preserve `Uint8Array` message data as native
binary values. The MessagePack representation is the additive tagged variant
`["binary", <bin>]`; existing wire variants remain unchanged.

### Capability-token authentication

Protocol V2 can authenticate the WebSocket with a scoped capability token.
Use `authCallback` (or `authUrl`) when the client must refresh credentials:

```ts
const client = new Sockudo("app-key", {
  cluster: "local",
  protocolVersion: 2,
  token: initialToken,
  authCallback: async ({ socketId, reason }) => {
    return fetchCapabilityToken({ socketId, reason });
  },
});
```

The optional static `token` is used for the first connection. Provider-backed
tokens are fetched again before reconnects and refreshed in place with
`sockudo:auth` at 80% of a JWT's `iat`–`exp` lifetime. `authUrl` receives a
JSON `POST` containing `{ socket_id, reason }` and must return either a token
string as JSON or an object containing `token` and optional expiry fields.

Code `40142` refreshes only when a provider is configured; static tokens are
never resent in a refresh loop. Code `40160` emits `TokenRevokedError` and is
not retried in place. Token auth with Protocol V1 is rejected during client
construction.

## Reconnection

Unexpected disconnects emit the distinct `reconnecting` state and retry with a
bounded quadratic delay: 0s, 1s, 4s, 9s, up to 120s. Protocol retry and
TLS-upgrade close codes reconnect immediately. The default retry limit is six;
set `maxReconnectAttempts: null` for unlimited retries.

Because that delay is the same for every client, clients dropped by a single
event retry in lockstep. Set `reconnectJitter` to randomize each delay by that
fraction and spread the retries out: `0.5` picks uniformly from 50–100% of
the delay, `1` from 0–100%. It defaults to `0`, which keeps the delays exact.

```ts
const client = new Sockudo("app-key", {
  cluster: "local",
  maxReconnectAttempts: 10,
  maxReconnectGapInSeconds: 60,
  reconnectJitter: 0.5,
});

client.connection.bind("reconnecting", () => {
  console.log("reconnecting");
});
```

The attempt counter resets after a successful connection and on explicit
`connect()` or `disconnect()` calls.

## Features

Protocol V2 subscriptions can combine event names, tag filters, and a bounded
JMESPath expression. All supplied components must match:

```ts
client.subscribe("orders.*", {
  events: ["order.updated"],
  filter: Filter.eq("region", "eu"),
  expression: 'data.total >= `100` && headers.priority == `"high"`',
});
```

- Pusher-protocol-compatible client surface
- Protocol V1 compatibility by default
- WebSocket-first connection strategy
- Fetch-first auth/timeline integrations
- ESM-first package outputs
- Runtime-specific builds for web/node/worker/react-native/nativescript
- Built-in React hooks via `@sockudo/client/react`
- Built-in Vue composables via `@sockudo/client/vue`
- Continuity-aware recovery positions and subscribe-time rewind in Protocol V2

## Recovery And Rewind

```ts
const client = new Sockudo("app-key", {
  wsHost: "127.0.0.1",
  wsPort: 6001,
  forceTLS: false,
  protocolVersion: 2,
  connectionRecovery: true,
});

const channel = client.subscribe("market:BTC", {
  rewind: { seconds: 30 },
});

channel.bind("message", () => {
  console.log(client.getRecoveryPosition("market:BTC"));
});

client.bind("sockudo:resume_success", (payload) => {
  console.log(payload.recovered, payload.failed);
});

channel.bind("sockudo:rewind_complete", (payload) => {
  console.log(payload.historical_count, payload.complete);
});
```

## Mutable Messages (Release 4.3)

Protocol V2 mutable messages use explicit action events:

- `sockudo:message.update`
- `sockudo:message.delete`
- `sockudo:message.append`

Client interpretation rules:

- `message.update`: replace local state with the full payload in the event
- `message.delete`: treat the event as the latest visible version; the payload may be `null`
- `message.append`: concatenate the incoming string fragment onto your current local string state

If you do not already have a string base for `message.append`, fetch the latest visible message first and seed local state before applying more appends.

The JS SDK exports helpers for this:

```ts
import Sockudo, {
  isMutableMessageEvent,
  reduceMutableMessageEvent,
  type MutableMessageState,
} from "@sockudo/client";

const client = new Sockudo("app-key", {
  wsHost: "127.0.0.1",
  wsPort: 6001,
  forceTLS: false,
  protocolVersion: 2,
});

let state: MutableMessageState | null = null;

client.subscribe("chat:room-1").bind_global((eventName, payload) => {
  if (!isMutableMessageEvent(payload)) {
    return;
  }
  state = reduceMutableMessageEvent(state, payload);
  console.log(state?.messageSerial, state?.action, state?.data);
});
```

Version-history consumption:

- consume `GET /apps/{appId}/channels/{channelName}/messages/{messageSerial}` when you need the latest visible state
- consume `GET /apps/{appId}/channels/{channelName}/messages/{messageSerial}/versions` when you need preserved historical versions
- process versions in `version_serial` order; the last version is the winning visible state

Proxy-backed read helpers:

Configure `versionedMessages.endpoint` when you want first-class helper methods without exposing server credentials in the client. The endpoint should accept a JSON POST body of the form `{ channel, messageSerial?, params?, action }` and proxy the request to Sockudo's REST API using server-side HMAC auth.

```ts
const client = new Sockudo("app-key", {
  cluster: "local",
  wsHost: "127.0.0.1",
  wsPort: 6001,
  forceTLS: false,
  versionedMessages: {
    endpoint: "/sockudo/versioned",
  },
});

const channel = client.subscribe("chat:room-1");

const latest = await channel.getMessage("message-serial");
const versions = await channel.getMessageVersions("message-serial", {
  limit: 20,
  direction: "oldest_first",
});
const history = await channel.channelHistory({
  limit: 50,
  direction: "newest_first",
});
```

## React Hooks

Install the framework peer dependencies:

```bash
npm install @sockudo/client react react-dom
```

```ts
import React from "react";
import Sockudo from "@sockudo/client";
import { SockudoProvider, useChannel } from "@sockudo/client/react";

const client = new Sockudo("app-key", {
  wsHost: "127.0.0.1",
  wsPort: 6001,
  forceTLS: false,
});

client.connect();

function PresencePanel() {
  const { subscribed, members } = useChannel("presence-room");

  return (
    <div>
      <div>Subscribed: {String(subscribed)}</div>
      <div>Members: {members?.length ?? 0}</div>
    </div>
  );
}

export function App() {
  return (
    <SockudoProvider client={client}>
      <PresencePanel />
    </SockudoProvider>
  );
}
```

Available React exports:
- `SockudoProvider`
- `useSockudo`
- `useSockudoEvent`
- `useChannel`
- `usePresenceChannel`

## Vue Composables

Install the framework peer dependency:

```bash
npm install @sockudo/client vue
```

```ts
import { createApp, defineComponent, h } from "vue";
import Sockudo from "@sockudo/client";
import { createSockudoPlugin, useChannel } from "@sockudo/client/vue";

const client = new Sockudo("app-key", {
  wsHost: "127.0.0.1",
  wsPort: 6001,
  forceTLS: false,
});

client.connect();

const PresencePanel = defineComponent({
  setup() {
    const { subscribed, members } = useChannel("presence-room");
    return () =>
      h("div", [
        h("div", `Subscribed: ${String(subscribed.value)}`),
        h("div", `Members: ${members.value?.length ?? 0}`),
      ]);
  },
});

createApp(PresencePanel).use(createSockudoPlugin(client)).mount("#app");
```

Available Vue exports:
- `createSockudoPlugin`
- `provideSockudo`
- `useSockudo`
- `useSockudoEvent`
- `useChannel`
- `usePresenceChannel`

## Push Proxy and Apple Live Activities

`SockudoPushRegistration` is exported from the root package with public TypeScript declarations.
Point it at an authenticated application backend; never put a Sockudo app secret, APNs provider key,
or unrestricted `push-admin` capability in browser, React Native, or NativeScript code.

React Native and NativeScript applications observe ActivityKit tokens through their app-specific
iOS bridge because `ActivityKit.Activity` is generic over the Widget Extension's custom attributes.
Upload every push-to-start and activity-token rotation to your backend. A narrowly authorized proxy
can then use the typed request:

```ts
import { SockudoPushRegistration } from "@sockudo/client";

const push = new SockudoPushRegistration({
  endpoint: "https://api.example.com/sockudo/push",
  headers: async () => ({ Authorization: `Bearer ${await sessionToken()}` }),
});

await push.publishLiveActivity({
  publishId: "ride-184-update-42",
  recipients: [{
    type: "recipient",
    recipient: {
      transportType: "apnsLiveActivity",
      activityToken: currentActivityToken,
    },
  }],
  liveActivity: {
    event: "update",
    timestamp: 1725000042,
    contentState: { status: "arriving", etaMinutes: 1 },
    priority: "conservePower",
  },
});
```

For one-to-many iOS 18 delivery, use `transportType: "apnsLiveActivityBroadcast"` with a channel ID
and its immutable `storagePolicy`. Channel creation and deletion remain server-admin operations.
See the [Apple Live Activities guide](../../docs/content/docs/server/apple-live-activities.mdx).

## React Native Notes

- React Native build output is `dist/react-native/sockudo.js`
- Package exposes both:
  - root `react-native` resolution
  - explicit `@sockudo/client/react-native` subpath
- `@react-native-community/netinfo` is an optional peer dependency
- React Native support remains inside `@sockudo/client`; React and Vue integrations stay as subpath exports in the same package

## NativeScript Notes

- Install the NativeScript websocket polyfill:

```bash
npm install @sockudo/client @valor/nativescript-websockets
```

- Import `@sockudo/client/nativescript` to automatically load the websocket polyfill before the SDK.
- NativeScript build output is `dist/nativescript/sockudo.js`
- Package exposes both:
  - root `nativescript` resolution
  - explicit `@sockudo/client/nativescript` subpath

## Development

### Requirements

- Bun `>=1.0.0`
- Node.js `>=22`

### Commands

```bash
# typecheck + lint + tests
bun run check

# typecheck only
bun run typecheck

# lint
bun run lint

# format
bun run format
bun run format:check

# tests
bun test
bun run test:watch

# builds
bun run build
bun run build:all
```

## Release Process

GitHub Actions are managed from the monorepo root:

- CI: `.github/workflows/sdk-ci.yml`
- Publish: `.github/workflows/sdk-release.yml` with tag `client-js-vX.Y.Z`
- Setup: see `docs/sdk-publishing-2026.md` for npm trusted publishing and provenance.

The release gate runs format checks, type checks, linting, tests, `build:all`, and `npm pack
--dry-run` before publishing.

## Repository

- Source: `https://github.com/sockudo/sockudo/tree/main/client-sdks/sockudo-js`
- Legacy repo: `https://github.com/sockudo/sockudo-js` (archived)
- npm: `https://www.npmjs.com/package/@sockudo/client`
