# sockudo-kotlin

Official Kotlin client for Sockudo.

`sockudo-kotlin` is a Pusher-compatible realtime client for Android and JVM applications. It preserves the familiar subscribe/bind/channel model while adding Sockudo-native features such as filter-aware subscriptions, delta reconstruction, and encrypted channel handling.

## Features

- WebSocket-based `SockudoClient` built on OkHttp
- Public, private, presence, and encrypted channels
- Proxy-backed presence history and presence snapshot helpers
- Channel authorization and user authentication
- Client events on private channels
- Filter-aware subscriptions
- Fossil and Xdelta3/VCDIFF delta reconstruction
- User sign-in and watchlist event handling
- Continuity-aware connection recovery and subscribe-time rewind on Protocol V2
- Protocol V2 capability-token auth refresh, presence updates, channel history, and mutable-message proxy helpers
- Live integration tests against Sockudo on `127.0.0.1:6001`
- Gradle CI and Maven Central publication workflow

## Installation

Install the published package from Maven Central:

```kotlin
dependencies {
    implementation("io.sockudo:sockudo-kotlin:2.0.0")
}
```

For Maven projects:

```xml
<dependency>
  <groupId>io.sockudo</groupId>
  <artifactId>sockudo-kotlin</artifactId>
  <version>2.0.0</version>
</dependency>
```

For local monorepo development, run `./gradlew :lib:publishToMavenLocal` from this directory.

## Quick Start

```kotlin
import io.sockudo.client.SockudoClient
import io.sockudo.client.SockudoOptions
import io.sockudo.client.SockudoTransport

val client =
    SockudoClient(
        "app-key",
        SockudoOptions(
            cluster = "local",
            forceTls = false,
            enabledTransports = listOf(SockudoTransport.ws),
            wsHost = "127.0.0.1",
            wsPort = 6001,
            wssPort = 6001,
        ),
    )

val channel = client.subscribe("public-updates")
channel.bind("price-updated") { data, _ ->
    println(data)
}

client.connect()
```

Protocol V2 heartbeat behavior:

- Sockudo servers use native WebSocket ping/pong frames for automatic heartbeat traffic
- Kotlin/JVM runtimes may still use lightweight `sockudo:ping` / `sockudo:pong` fallback messages for client-side activity checks when the underlying transport does not expose a direct ping API
- fallback heartbeat messages are intentionally excluded from V2 recovery metadata such as `message_id`, `serial`, and `stream_id`

## Advanced Usage

### Channel Auth

```kotlin
import io.sockudo.client.*

val client =
    SockudoClient(
        "app-key",
        SockudoOptions(
            cluster = "local",
            forceTls = false,
            wsHost = "127.0.0.1",
            wsPort = 6001,
            channelAuthorization =
                ChannelAuthorizationOptions(
                    customHandler =
                        ChannelAuthorizationHandler { request ->
                            ChannelAuthorizationData(
                                auth = "signed-auth-token",
                                channelData = """{"user_id":"42"}""",
                            )
                        },
                ),
        ),
    )
```

### Filters and Delta Compression

```kotlin
val channel =
    client.subscribe(
        "price:btc",
        SubscriptionOptions(
            filter = Filter.eq("market", "spot"),
            delta = ChannelDeltaSettings(enabled = true, algorithm = DeltaAlgorithm.xdelta3),
        ),
)
```

### Recovery And Rewind

```kotlin
val client =
    SockudoClient(
        "app-key",
        SockudoOptions(
            cluster = "local",
            protocolVersion = 2,
            forceTls = false,
            wsHost = "127.0.0.1",
            wsPort = 6001,
            connectionRecovery = true,
        ),
    )

val channel =
    client.subscribe(
        "market:BTC",
        SubscriptionOptions(rewind = SubscriptionRewind.Seconds(30)),
    )

channel.bind("message") { _, _ ->
    println(client.getRecoveryPosition("market:BTC"))
}

client.bind("sockudo:resume_success") { data, _ ->
    println(data)
}

channel.bind("sockudo:rewind_complete") { data, _ ->
    println(data)
}
```

### Protocol V2 Auth And History

```kotlin
val client =
    SockudoClient(
        "app-key",
        SockudoOptions(
            cluster = "local",
            protocolVersion = 2,
            authTokenProvider =
                ClientAuthTokenProvider { request ->
                    fetchCapabilityToken(reason = request.reason, socketId = request.socketId)
                },
            appendRollupWindow = 100,
            versionedMessages =
                VersionedMessagesOptions(
                    endpoint = "https://api.example.com/sockudo/versioned",
                ),
        ),
    )

val channel = client.subscribe("chat:room-1")
val history = channel.channelHistory(ChannelHistoryParams(limit = 50, untilAttach = true))
println(channel.attachSerial)
```

For JWTs returned by `authTokenProvider`, the client reads `iat` / `exp` without validating the signature and schedules a proactive refresh at 80% of the token lifetime. When the server sends `sockudo:token_expired` or error code `40142` / `40160`, the client asks `authTokenProvider` for a fresh token and sends it with `sockudo:auth`.

Opaque provider tokens and static `authToken` values are reactive-only and rely on `sockudo:token_expired`.

### Mutable Messages (Release 4.3)

Protocol V2 mutable messages use:

- `sockudo:message.update`
- `sockudo:message.delete`
- `sockudo:message.append`

Client rule:

- `message.update` replaces local content with the full event payload
- `message.delete` is the latest visible version and may carry `null` data
- `message.append` concatenates onto the current local string state

If you receive `message.append` before you have a string base, fetch the latest visible message first and seed local state before applying more appends.

For historical inspection, use:

- `GET /apps/{appId}/channels/{channelName}/messages/{messageSerial}` for the latest visible version
- `GET /apps/{appId}/channels/{channelName}/messages/{messageSerial}/versions` for preserved versions in `version_serial` order
- proxy-backed helpers: `createMessage`, `appendMessage`, `updateMessage`, and `deleteMessage`

```kotlin
import io.sockudo.client.*

val client =
    SockudoClient(
        "app-key",
        SockudoOptions(
            cluster = "local",
            ws_host = "127.0.0.1",
            ws_port = 6001,
            protocolVersion = 2,
        ),
    )

var state: MutableMessageState? = null

val channel = client.subscribe("chat:room-1")
channel.bindGlobal { eventName, data ->
    val event = data as? SockudoEvent ?: return@bindGlobal
    if (!isMutableMessageEvent(event)) return@bindGlobal
    try {
        state = reduceMutableMessageEvent(state, event)
        println("${state?.messageSerial} ${state?.action} ${state?.data}")
    } catch (e: IllegalStateException) {
        println("mutable message reduction failed: ${e.message}")
    }
}

client.connect()
```

Proxy-backed write helpers use `VersionedMessagesOptions.endpoint`; the mobile client still does not sign server REST requests directly.

```kotlin
val ack = channel.createMessage("chat-message", mapOf("text" to "hello"))
channel.appendMessage(ack.messageSerial, " world")
channel.updateMessage(ack.messageSerial, mapOf("text" to "edited"))
channel.deleteMessage(ack.messageSerial)
```

### Presence History

Client-side presence history is proxy-backed. The Kotlin client does not sign the server REST API directly; configure a backend endpoint that accepts `{channel, params, action}` and forwards the request with server credentials.

```kotlin
val client =
    SockudoClient(
        "app-key",
        SockudoOptions(
            cluster = "local",
            forceTls = false,
            wsHost = "127.0.0.1",
            wsPort = 6001,
            presenceHistory =
                PresenceHistoryOptions(
                    endpoint = "https://api.example.com/sockudo/presence-history",
                ),
        ),
    )

val channel = client.subscribe("presence-lobby") as PresenceChannel
channel.update(mapOf("status" to "typing"))

val page = channel.history(PresenceHistoryParams(limit = 50, direction = "newest_first"))
if (page.hasNext()) {
    val nextPage = page.next()
}

val snapshot = channel.snapshot(PresenceSnapshotParams(atSerial = 4))
```

### Push Proxy Helpers

Push registration and publish helpers are HTTP/proxy surfaces. Keep app secrets on your backend, not in the mobile client, and point the helper at your own proxy/admin endpoint.

- `publish()` and `publishBatch()` always force async delivery with `sync = false`
- expect `202 Accepted` responses with a `publish_id` for publish calls
- list helpers use cursor pagination via `limit` and `cursor`

```kotlin
import io.sockudo.client.PushRegistrationOptions
import io.sockudo.client.PushSubscriptionParams
import io.sockudo.client.SockudoPushRegistration
import kotlinx.coroutines.runBlocking

val push =
    SockudoPushRegistration(
        PushRegistrationOptions(
            endpoint = "https://api.example.com/sockudo/push",
            headers = mapOf("Authorization" to "Bearer session-token"),
        ),
    )

runBlocking {
    val publish =
        push.publish(
            mapOf(
                "recipients" to listOf(mapOf("type" to "channel", "channel" to "orders")),
                "payload" to mapOf("title" to "Order updated", "body" to "Ready for pickup"),
            ),
        )

    println(publish["publish_id"])

    val page = push.listChannelSubscriptions(PushSubscriptionParams(deviceId = "device-1", limit = 20))
    println(page["next_cursor"])
}
```

### Encrypted Channels

`private-encrypted-*` channels use the `shared_secret` returned by your auth handler. Payload decryption is handled automatically.

## Testing

Standard tests:

```bash
./gradlew :lib:test
```

Live integration tests against a local Sockudo on port `6001`:

```bash
SOCKUDO_LIVE_TESTS=1 ./gradlew :lib:test --tests io.sockudo.client.LiveIntegrationTest
```

The live suite covers:

- public subscribe + publish round-trip
- delta-enabled channel delivery
- encrypted channel decryption

## Publishing

This package is configured for Maven-style publishing.

```bash
./gradlew :lib:publishToMavenLocal
./gradlew :lib:publish
```

GitHub Actions are managed from the monorepo root:

- CI: `.github/workflows/sdk-ci.yml`
- Publish: `.github/workflows/sdk-release.yml` with tag `client-kotlin-vX.Y.Z`
- Setup: see `docs/sdk-publishing-2026.md` for Maven Central credentials and signing.

## Status

The client currently covers the core Sockudo feature set used by the official JS and Swift clients, including encrypted channels and both supported delta algorithms.
