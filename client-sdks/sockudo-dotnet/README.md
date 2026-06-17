# Sockudo.Client

Official .NET client SDK for Sockudo.

`Sockudo.Client` is a Pusher-compatible realtime client for .NET applications. It preserves the familiar subscribe/bind/channel model while adding Sockudo-native features such as filter-aware subscriptions, delta reconstruction, and encrypted channel handling.

## Features

- Protocol V2 by default, with V1 compatibility
- Public, private, presence, and encrypted channel types
- Proxy-backed presence history and presence snapshot helpers
- Tag filters and per-subscription event filters
- Continuity-aware connection recovery (`stream_id` + `serial`)
- Message deduplication
- User sign-in and watchlist event handling
- JSON, MessagePack, and Protobuf codecs
- Fossil and Xdelta3/VCDIFF delta support
- Encrypted channel shared-secret payload decryption

## Installation

Clone the Sockudo monorepo and reference the project directly until the NuGet package is published:

```bash
git clone https://github.com/sockudo/sockudo.git
```

```xml
<ProjectReference Include="../sockudo/client-sdks/sockudo-dotnet/src/Sockudo.Client/Sockudo.Client.csproj" />
```

From this monorepo, use `client-sdks/sockudo-dotnet/src/Sockudo.Client/Sockudo.Client.csproj`.

## Quick Start

```csharp
using Sockudo.Client;

var client = new SockudoClient(
    "app-key",
    new SockudoOptions
    {
        Cluster = "local",
        ForceTls = false,
        WsHost = "127.0.0.1",
        WsPort = 6001,
    }
);

var channel = client.Subscribe("public-updates");
channel.Bind("price-updated", (data, meta) => Console.WriteLine(data));

await client.ConnectAsync();
await Task.Delay(TimeSpan.FromSeconds(30));
await client.DisconnectAsync();
```

Protocol V2 heartbeat behavior:

- Sockudo servers use native WebSocket ping/pong frames for automatic heartbeat traffic
- .NET runtimes may still use lightweight `sockudo:ping` / `sockudo:pong` fallback messages for client-side activity checks when native ping APIs are not exposed through the active transport surface
- fallback heartbeat messages are intentionally excluded from V2 recovery metadata such as `message_id`, `serial`, and `stream_id`

## Advanced Usage

### Private Channel Authorization

Use an endpoint URL (the default) or supply a fully custom async handler:

```csharp
using Sockudo.Client;

var client = new SockudoClient(
    "app-key",
    new SockudoOptions
    {
        Cluster = "local",
        WsHost = "127.0.0.1",
        WsPort = 6001,
        ChannelAuthorization = new ChannelAuthorizationOptions(
            Endpoint: "https://api.example.com/sockudo/auth",
            // Or override entirely:
            CustomHandler: async request =>
            {
                // Call your own backend to produce a signed auth token.
                return new ChannelAuthorizationData(
                    Auth: "app-key:hmac-sha256-signature",
                    ChannelData: """{"user_id":"42"}"""
                );
            }
        ),
    }
);

var channel = client.Subscribe("private-orders");
channel.Bind("order-placed", (data, meta) => Console.WriteLine(data));

await client.ConnectAsync();
```

### Presence Channels

```csharp
var channel = client.Subscribe("presence-lobby");

channel.Bind("pusher:subscription_succeeded", (data, meta) =>
    Console.WriteLine($"members: {data}"));
channel.Bind("pusher:member_added", (data, meta) =>
    Console.WriteLine($"joined: {data}"));
channel.Bind("pusher:member_removed", (data, meta) =>
    Console.WriteLine($"left: {data}"));

await client.ConnectAsync();
```

### Presence History

Client-side presence history is proxy-backed. `Sockudo.Client` does not sign the server REST API directly; configure `PresenceHistory.Endpoint` to call your own backend proxy, which then signs and forwards the request to Sockudo.

```csharp
var client = new SockudoClient(
    "app-key",
    new SockudoOptions(
        Cluster: "local",
        WsHost: "127.0.0.1",
        WsPort: 6001,
        PresenceHistory: new PresenceHistoryOptions(
            Endpoint: "https://api.example.com/sockudo/presence-history"
        )
    )
);

var channel = (PresenceChannel)client.Subscribe("presence-lobby");
var page = await channel.HistoryAsync(
    new PresenceHistoryParams(Limit: 50, Direction: "newest_first")
);
if (page.HasNext())
{
    var nextPage = await page.NextAsync();
}

var snapshot = await channel.SnapshotAsync(
    new PresenceSnapshotParams(AtSerial: 4)
);
```

### Push Proxy Helpers

Push registration and publish helpers are HTTP/proxy surfaces. Keep Sockudo app secrets on your backend and point the client helper at your own proxy/admin endpoint.

- `PublishAsync` and `PublishBatchAsync` always send `sync = false`
- publish calls should expect `202 Accepted` responses with a `publish_id`
- list helpers use `limit` and `cursor` query parameters

```csharp
using Sockudo.Client;

var push = new SockudoPushRegistration(
    new PushRegistrationOptions(
        Endpoint: "https://api.example.com/sockudo/push",
        Headers: new Dictionary<string, string>
        {
            ["Authorization"] = "Bearer session-token",
        }
    )
);

var publish = await push.PublishAsync(
    new Dictionary<string, object?>
    {
        ["recipients"] = new[]
        {
            new Dictionary<string, object?> { ["type"] = "channel", ["channel"] = "orders" },
        },
        ["payload"] = new Dictionary<string, object?>
        {
            ["title"] = "Order updated",
            ["body"] = "Ready for pickup",
        },
    }
);
Console.WriteLine(publish["publish_id"]);

var page = await push.ListChannelSubscriptionsAsync(
    new PushSubscriptionParams(DeviceId: "device-1", Limit: 20)
);
Console.WriteLine(page["next_cursor"]);
```

### Tag Filter Subscriptions

Server-side tag filtering is a V2 feature. Only messages whose tags match the filter expression are delivered to this subscription.

```csharp
using Sockudo.Client;

var channel = client.Subscribe(
    "price:btc",
    new SubscriptionOptions(
        Filter: Filter.Eq("market", "spot")
    )
);

// Compound filters
var channel2 = client.Subscribe(
    "price:btc",
    new SubscriptionOptions(
        Filter: Filter.And(
            Filter.Eq("market", "spot"),
            Filter.Gt("spread", "0")
        )
    )
);
```

### Delta Compression And Rewind

Request delta-compressed delivery to reduce bandwidth for channels that carry frequently-updated payloads:

```csharp
var channel = client.Subscribe(
    "orderbook:btc-usd",
    new SubscriptionOptions(
        Delta: new ChannelDeltaSettings(Enabled: true, Algorithm: DeltaAlgorithm.Xdelta3)
    )
);
channel.Bind("snapshot", (data, meta) => Console.WriteLine(data));

var rewindChannel = client.Subscribe(
    "market:btc",
    new SubscriptionOptions(
        Rewind: new SubscriptionRewind.Seconds(30)
    )
);

client.Bind("sockudo:resume_success", (data, _) => Console.WriteLine(data));
rewindChannel.Bind("sockudo:rewind_complete", (data, _) => Console.WriteLine(data));
```

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

```csharp
using Sockudo.Client;

var client = new SockudoClient(
    "app-key",
    new SockudoOptions { Cluster = "local", WsHost = "127.0.0.1", WsPort = 6001, Protocol = 2 }
);

MutableMessageState? state = null;

var channel = client.Subscribe("chat:room-1");
channel.BindGlobal((eventName, data) =>
{
    if (data is not SockudoEvent ev || !MutableMessageReducer.IsMutableMessageEvent(ev))
        return;
    try
    {
        state = MutableMessageReducer.ReduceMutableMessageEvent(state, ev);
        Console.WriteLine($"{state.MessageSerial} {state.Action} {state.Data}");
    }
    catch (InvalidOperationException ex)
    {
        Console.Error.WriteLine($"mutable message reduction failed: {ex.Message}");
    }
});

await client.ConnectAsync();
```

### Encrypted Channels

`private-encrypted-*` channels decrypt payloads automatically using the `SharedSecret` returned by your auth endpoint or custom handler.

```csharp
var channel = client.Subscribe("private-encrypted-documents");
channel.Bind("doc-updated", (data, meta) => Console.WriteLine(data)); // data is already decrypted
```

Your auth handler must populate `SharedSecret` in `ChannelAuthorizationData`:

```csharp
CustomHandler: async request => new ChannelAuthorizationData(
    Auth: "app-key:hmac-sha256-signature",
    SharedSecret: "base64-encoded-32-byte-secret"
)
```

### User Sign-In

```csharp
var client = new SockudoClient(
    "app-key",
    new SockudoOptions
    {
        Cluster = "local",
        WsHost = "127.0.0.1",
        WsPort = 6001,
        UserAuthentication = new UserAuthenticationOptions(
            Endpoint: "https://api.example.com/sockudo/user-auth"
        ),
    }
);

await client.ConnectAsync();
await client.User.SignInAsync();
```

### Connection State Events

```csharp
client.Connection.StateChanged += (sender, change) =>
    Console.WriteLine($"connection: {change.Previous} -> {change.Current}");

client.Connection.Connected += (sender, data) =>
    Console.WriteLine($"connected, socket id: {data.SocketId}");

client.Connection.Disconnected += (sender, _) =>
    Console.WriteLine("disconnected");

client.Connection.Reconnecting += (sender, _) =>
    Console.WriteLine("reconnecting...");

await client.ConnectAsync();
```

### Protocol V2

V2 is the default. To explicitly request it or to downgrade to V1 for strict Pusher SDK compatibility:

```csharp
// V2 (default) — enables continuity tokens, message_id, recovery, filters, delta
var client = new SockudoClient("app-key", new SockudoOptions { Protocol = 2 });

// V1 — plain Pusher protocol, compatible with official Pusher SDKs
var client = new SockudoClient("app-key", new SockudoOptions { Protocol = 1 });
```

## Requirements

- .NET 6+ or .NET 8+

## Testing

Run the unit and integration test suite:

```bash
dotnet test client-sdks/sockudo-dotnet/tests/Sockudo.Client.Tests/Sockudo.Client.Tests.csproj
```

Live integration tests against a local Sockudo server on port `6001`:

```bash
SOCKUDO_LIVE_TESTS=1 dotnet test client-sdks/sockudo-dotnet/tests/Sockudo.Client.Tests/Sockudo.Client.Tests.csproj
```

The live suite covers:

- public subscribe + publish round-trip
- delta-enabled channel delivery
- encrypted channel decryption

## CI/CD

GitHub Actions:

- CI: `.github/workflows/ci.yml`
- Publish: `.github/workflows/publish.yml`

## Status

The package covers the core Sockudo feature set, including encrypted channels and both supported delta algorithms, and is suitable for publishing as the official .NET SDK.
