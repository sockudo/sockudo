# SockudoSwift

Official Swift client for Sockudo.

`SockudoSwift` is a Pusher-compatible realtime client for Apple platforms. It preserves the familiar subscribe/bind/channel model while adding Sockudo-native features such as filter-aware subscriptions, delta reconstruction, and encrypted channel handling.

## Platforms

- iOS 13+
- macOS 10.15+
- tvOS 13+
- watchOS 6+
- visionOS 1+

## Features

- Public, private, presence, and encrypted channels
- Proxy-backed presence history and presence snapshot helpers
- Channel authorization and user authentication
- Client events on private channels
- User sign-in and watchlist event handling
- Filter-aware subscriptions
- Fossil and Xdelta3/VCDIFF delta reconstruction
- Encrypted channel payload decryption with `swift-sodium`
- Continuity-aware connection recovery and subscribe-time rewind on Protocol V2
- Protocol V2 capability tokens with initial URL auth and `sockudo:auth` refresh
- Protocol V2 presence updates and append rollup window selection
- Proxy-backed mutable message create/update/append/delete helpers
- Live integration tests against Sockudo on `127.0.0.1:6001`
- Swift Package Manager distribution with root GitHub Actions CI

## Installation

Use Swift Package Manager with the monorepo package URL:

```swift
.package(url: "https://github.com/sockudo/sockudo", from: "2.0.0")
```

Then depend on the `SockudoSwift` product:

```swift
.target(
    name: "YourApp",
    dependencies: [
        .product(name: "SockudoSwift", package: "sockudo"),
    ]
)
```

For local development:

```swift
.package(path: "../sockudo")
```

## Quick Start

```swift
import SockudoSwift

let client = try SockudoClient(
    "app-key",
    options: .init(
        cluster: "local",
        forceTLS: false,
        enabledTransports: [.ws],
        wsHost: "127.0.0.1",
        wsPort: 6001,
        wssPort: 6001
    )
)

let channel = client.subscribe("public-updates")
channel.bind("price-updated") { data, _ in
    print(data ?? "")
}

client.connect()
```

The default client mode is Protocol V1 compatibility (`protocol=7`). Opt into Protocol V2 explicitly when you want Sockudo-native event prefixes and V2-only features.

```swift
let v2Client = try SockudoClient(
    "app-key",
    options: .init(
        cluster: "local",
        forceTLS: false,
        wsHost: "127.0.0.1",
        wsPort: 6001,
        protocolVersion: 2
    )
)
```

Protocol V2 heartbeat behavior:

- Sockudo servers use native WebSocket ping/pong frames for automatic heartbeat traffic
- the Swift client uses `URLSessionWebSocketTask.sendPing` for native V2 liveness checks
- lightweight `sockudo:ping` / `sockudo:pong` fallback messages remain reserved for compatibility and diagnostics, and they do not carry `message_id`, `serial`, or `stream_id`

Protocol V2 capability tokens can be supplied statically or through a provider. The initial token is sent as the WebSocket `token` query parameter. Provider-backed JWTs with `exp` are refreshed proactively at 80% of their lifetime (`iat` to `exp`, or current time to `exp` when `iat` is absent). Opaque tokens and static-only tokens are reactive only: when Sockudo emits `sockudo:token_expired` with code `40142` or `40160`, the provider is called if one exists and the SDK sends a `sockudo:auth` refresh frame with the new token.

```swift
let client = try SockudoClient(
    "app-key",
    options: .init(
        cluster: "local",
        protocolVersion: 2,
        forceTLS: false,
        wsHost: "127.0.0.1",
        wsPort: 6001,
        capabilityToken: .init(asyncProvider: {
            try await tokenService.fetchSockudoToken()
        }),
        appendRollupWindow: 40
    )
)
```

## Advanced Usage

### Channel Authorization

```swift
let client = try SockudoClient(
    "app-key",
    options: .init(
        cluster: "local",
        forceTLS: false,
        wsHost: "127.0.0.1",
        wsPort: 6001,
        channelAuthorization: .init(
            endpoint: "https://api.example.com/pusher/auth"
        )
    )
)
```

### Filters and Delta Compression

```swift
let channel = client.subscribe(
    "price:btc",
    options: .init(
        filter: .eq("market", "spot"),
        delta: .init(enabled: true, algorithm: .xdelta3)
    )
)
```

### Recovery And Rewind

```swift
let client = try SockudoClient(
    "app-key",
    options: .init(
        cluster: "local",
        protocolVersion: 2,
        forceTLS: false,
        wsHost: "127.0.0.1",
        wsPort: 6001,
        connectionRecovery: true
    )
)

let channel = client.subscribe(
    "market:BTC",
    options: .init(rewind: .seconds(30))
)

channel.bind("message") { _, _ in
    print(client.recoveryPosition(for: "market:BTC") as Any)
}

client.bind("sockudo:resume_success") { data, _ in
    print(data as Any)
}

channel.bind("sockudo:rewind_complete") { data, _ in
    print(data as Any)
}

print(channel.attachSerial as Any)
channel.channelHistory(.init(limit: 50, untilAttach: true)) { result in
    print(result)
}
```

### Mutable Messages (Release 4.3)

Protocol V2 mutable messages use:

- `sockudo:message.update`
- `sockudo:message.delete`
- `sockudo:message.append`

Client rule:

- `message.update` replaces local content with the full event payload
- `message.delete` is the latest visible version and may carry `nil` data
- `message.append` concatenates onto the current local string state

If you receive `message.append` before you have a string base, fetch the latest visible message first and seed local state before applying more appends.

For historical inspection, use:

- `GET /apps/{appId}/channels/{channelName}/messages/{messageSerial}` for the latest visible version
- `GET /apps/{appId}/channels/{channelName}/messages/{messageSerial}/versions` for preserved versions in `version_serial` order

Configure `versionedMessages.endpoint` to use proxy-backed write helpers without exposing Sockudo app secrets in the mobile client. The proxy should accept JSON `{ channel, messageSerial?, action, payload }` and call Sockudo's signed REST API server-side. `createMessage` should proxy `/events`; mutation helpers should proxy the HTTP update/delete/append endpoints. The Swift SDK does not send unsupported websocket mutation frames.

```swift
import SockudoSwift

let client = try SockudoClient(
    "app-key",
    options: .init(
        cluster: "local",
        forceTLS: false,
        wsHost: "127.0.0.1",
        wsPort: 6001,
        protocolVersion: 2,
        versionedMessages: .init(
            endpoint: "https://api.example.com/sockudo/versioned"
        )
    )
)

var state: MutableMessageState? = nil

let channel = client.subscribe("chat:room-1")
channel.bindGlobal { eventName, data in
    guard
        let event = data as? SockudoEvent,
        isMutableMessageEvent(event)
    else { return }
    do {
        state = try reduceMutableMessageEvent(current: state, event: event)
        print(state?.messageSerial as Any, state?.action as Any, state?.data as Any)
    } catch {
        print("mutable message reduction failed:", error)
    }
}

client.connect()

channel.createMessage(
    .init(name: "chat.message", data: ["text": "hello"], messageID: "msg-1")
) { result in
    print(result)
}

channel.appendMessage(
    "msg-1",
    request: .init(data: " world", opID: "append-1")
) { result in
    print(result)
}
```

### Presence Updates

Presence member data can be updated in-place on Protocol V2 presence channels. Incoming `sockudo_internal:presence_update` events update `members` and emit `sockudo:presence_update`.

```swift
let presence = client.subscribe("presence-agent:session-123") as! PresenceChannel

try presence.update(data: ["status": "thinking"])

presence.bind("sockudo:presence_update") { data, _ in
    print(data as Any)
}
```

### Presence History

Client-side presence history is proxy-backed. `SockudoSwift` does not sign the server REST API directly; configure `presenceHistory` with a backend endpoint that accepts `{channel, params, action}` and forwards the request using server credentials.

```swift
let client = try SockudoClient(
    "app-key",
    options: .init(
        cluster: "local",
        forceTLS: false,
        wsHost: "127.0.0.1",
        wsPort: 6001,
        presenceHistory: .init(
            endpoint: "https://api.example.com/sockudo/presence-history"
        )
    )
)

let channel = client.subscribe("presence-lobby") as! PresenceChannel
channel.history(.init(limit: 50, direction: "newest_first")) { result in
    print(result)
}

channel.snapshot(.init(atSerial: 4)) { result in
    print(result)
}
```

### Push Proxy Helpers

Push registration and publish helpers are HTTP/proxy surfaces. Do not ship Sockudo app secrets in the mobile client; call your own backend/proxy endpoint instead.

- `publish` and `publishBatch` always send `sync = false`
- publish calls should expect `202 Accepted` responses with a `publish_id`
- list helpers use `limit` and `cursor` query parameters

```swift
import SockudoSwift

let push = SockudoPushRegistration(
    options: .init(
        endpoint: "https://api.example.com/sockudo/push",
        headers: ["Authorization": "Bearer session-token"]
    )
)

push.publish(
    [
        "recipients": [["type": "channel", "channel": "orders"]],
        "payload": ["title": "Order updated", "body": "Ready for pickup"],
    ]
) { result in
    if case .success(let payload) = result {
        print(payload["publish_id"] as Any)
    }
}

push.listChannelSubscriptions(
    params: .init(deviceID: "device-1", limit: 20)
) { result in
    if case .success(let page) = result {
        print(page["next_cursor"] as Any)
    }
}
```

### Encrypted Channels

`private-encrypted-*` channels use the `shared_secret` returned by your auth endpoint or custom auth handler. Payload decryption is handled automatically.

## Testing

Standard tests:

```bash
swift test
```

Live integration tests against a local Sockudo server on port `6001`:

```bash
SOCKUDO_LIVE_TESTS=1 swift test
```

The live suite covers:

- public subscribe + publish round-trip
- filter validation and delta option serialization
- encrypted, private, and delta-enabled runtime paths through the client core

## Release Model

Swift packages are distributed by git tag rather than a central package registry by default.

- CI: root workflow `.github/workflows/sdk-ci.yml`
- Release gate: root workflow `.github/workflows/sdk-release.yml` with root tag `vX.Y.Z`
- Distribution: SwiftPM package in the root `Package.swift`

## Status

The package covers the core Sockudo feature set used by the official JavaScript client, including encrypted channels and both supported delta algorithms.
