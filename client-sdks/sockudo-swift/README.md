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
- Live integration tests against Sockudo on `127.0.0.1:6001`
- Swift Package Manager distribution with root GitHub Actions CI

## Installation

For local monorepo development, use a Swift Package Manager path:

```swift
.package(path: "../sockudo/client-sdks/sockudo-swift")
```

Public SwiftPM distribution requires the package repository or subtree split described in
`docs/sdk-publishing-2026.md`, because SwiftPM expects `Package.swift` at the package repository
root.

Then depend on `SockudoSwift`:

```swift
.target(
    name: "YourApp",
    dependencies: [
        .product(name: "SockudoSwift", package: "sockudo-swift"),
    ]
)
```

For local development:

```swift
.package(path: "../sockudo-swift")
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

```swift
import SockudoSwift

let client = try SockudoClient(
    "app-key",
    options: .init(
        cluster: "local",
        forceTLS: false,
        wsHost: "127.0.0.1",
        wsPort: 6001,
        protocolVersion: 2
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
- Release gate: root workflow `.github/workflows/sdk-release.yml` with tag `client-swift-vX.Y.Z`
- Distribution: SwiftPM still requires a package repository or subtree split whose root contains
  `Package.swift`

## Status

The package covers the core Sockudo feature set used by the official JavaScript client, including encrypted channels and both supported delta algorithms.
