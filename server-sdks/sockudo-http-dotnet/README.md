# Sockudo .NET HTTP Server SDK

A .NET server SDK for interacting with the [Sockudo](https://github.com/sockudo/sockudo) WebSocket server HTTP API. Publish events, authorize channels, handle webhooks, and query application state from your .NET applications.

## Supported platforms

- .NET 6+
- .NET Standard 2.0

## Contents

- [Installation](#installation)
- [Getting started](#getting-started)
- [Configuration](#configuration)
- [Triggering events](#triggering-events)
  - [Single channel](#single-channel)
  - [Multiple channels](#multiple-channels)
  - [Batches](#batches)
  - [Detecting event data that exceeds the 10KB threshold](#detecting-event-data-that-exceeds-the-10kb-threshold)
  - [Excluding event recipients](#excluding-event-recipients)
  - [Idempotency key](#idempotency-key)
- [Authenticating channel subscription](#authenticating-channel-subscription)
  - [Authenticating Private channels](#authenticating-private-channels)
  - [Authenticating Presence channels](#authenticating-presence-channels)
- [End-to-end encryption](#end-to-end-encryption)
- [Querying application state](#querying-application-state)
  - [Getting information for all channels](#getting-information-for-all-channels)
  - [Getting information for a channel](#getting-information-for-a-channel)
  - [Getting user information for a presence channel](#getting-user-information-for-a-presence-channel)
- [Webhooks](#webhooks)
- [License](#license)

## Installation

Install the published NuGet package:

```bash
dotnet add package SockudoServer --version 2.0.0
```

Or add it directly to your project file:

```xml
<PackageReference Include="SockudoServer" Version="2.0.0" />
```

For local monorepo development, reference
`server-sdks/sockudo-http-dotnet/SockudoServer/SockudoServer.csproj` directly.

## Getting started

The minimum configuration required to use the `Sockudo` object are the three constructor arguments that identify your app: app id, app key, and app secret.

```csharp
using Sockudo;

var options = new SockudoOptions
{
    Host = "127.0.0.1",
    Port = 6001,
    Encrypted = false,
};

var sockudo = new Sockudo(APP_ID, APP_KEY, APP_SECRET, options);
```

For best practice, create a `Sockudo` singleton and reuse it across your application.

**Note:** The `Host` property overrides the `Cluster` property. If `Host` is set, `Cluster` is ignored.

## Configuration

In addition to the three app identifiers, you can specify other options via `SockudoOptions`:

| Property                | Type             | Description                                                                                                                                                       |
|-------------------------|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Host`                  | String           | The Sockudo server host name, excluding the scheme; for example, `"127.0.0.1"`. Overrides `Cluster` if specified.                                                |
| `Port`                  | Integer          | The HTTP API port. If `Encrypted` is `true`, defaults to 443. If `Encrypted` is `false`, defaults to 80.                                                         |
| `Encrypted`             | Boolean          | Indicates whether calls to the HTTP API use HTTPS. Default: `false`.                                                                                              |
| `Cluster`               | String           | The server cluster name. Overridden by `Host` if both are set.                                                                                                    |
| `BatchEventDataSizeLimit` | Nullable Integer | Optional client-side size limit for event data. If specified, events exceeding this threshold throw before the request is sent. Normally 10KB.                  |
| `EncryptionMasterKey`   | Byte Array       | Optional 32-byte key for end-to-end encryption of private channels.                                                                                               |
| `RestClientTimeout`     | TimeSpan         | HTTP API request timeout. Default: 30 seconds.                                                                                                                    |

## Triggering events

To trigger an event on one or more channels use the `TriggerAsync` method.

### Single channel

```csharp
ITriggerResult result = await sockudo.TriggerAsync("channel-1", "test_event", new
{
    message = "hello world"
}).ConfigureAwait(false);
```

### Multiple channels

```csharp
ITriggerResult result = await sockudo.TriggerAsync(
    new string[]
    {
        "channel-1", "channel-2"
    },
    "test_event",
    new
    {
        message = "hello world"
    }).ConfigureAwait(false);
```

### Batches

```csharp
var events = new[]
{
    new Event { Channel = "channel-1", EventName = "event-1", Data = "hello world" },
    new Event { Channel = "channel-2", EventName = "event-2", Data = "my name is bob" }
};

ITriggerResult result = await sockudo.TriggerAsync(events).ConfigureAwait(false);
```

### Detecting event data that exceeds the 10KB threshold

Rather than relying on the server to reject oversized messages, you can validate client-side before sending:

```csharp
var sockudo = new Sockudo(Config.AppId, Config.AppKey, Config.AppSecret, new SockudoOptions
{
    Host = Config.HttpHost,
    BatchEventDataSizeLimit = SockudoOptions.DEFAULT_BATCH_EVENT_DATA_SIZE_LIMIT, // 10KB
});

try
{
    var events = new[]
    {
        new Event { Channel = "channel-1", EventName = "event-1", Data = "hello world" },
        new Event { Channel = "channel-2", EventName = "event-2", Data = new string('Q', 10 * 1024 + 1) },
    };
    await sockudo.TriggerAsync(events).ConfigureAwait(false);
}
catch (EventDataSizeExceededException eventDataSizeError)
{
    // Handle the error when event data exceeds 10KB
}
```

### Excluding event recipients

Pass an `ITriggerOptions` with a `SocketId` to prevent the triggering client from also receiving the event:

```csharp
ITriggerResult result = await sockudo.TriggerAsync(channel, eventName, data, new TriggerOptions
{
    SocketId = "1234.56"
}).ConfigureAwait(false);
```

### Idempotency key

Attach an idempotency key so the server deduplicates events on retries:

```csharp
ITriggerResult result = await sockudo.TriggerAsync(channel, eventName, data, new TriggerOptions
{
    IdempotencyKey = "unique-key-for-this-event"
}).ConfigureAwait(false);
```

## Authenticating channel subscription

### Authenticating Private channels

Return the authentication token to the client requesting subscription:

```csharp
var auth = sockudo.Authenticate(channelName, socketId);
var json = auth.ToJson();
```

The `json` can then be returned to the client, which uses it to validate the subscription.

### Authenticating Presence channels

For presence channels, include user identity data:

```csharp
var channelData = new PresenceChannelData
{
    user_id = "unique_user_id",
    user_info = new
    {
        name = "Jane Smith",
        role = "admin",
    }
};
var auth = sockudo.Authenticate(channelName, socketId, channelData);
var json = auth.ToJson();
```

## End-to-end encryption

This library supports end-to-end encryption of private channels. Only you and your connected clients can read the messages.

Encrypted channels must be prefixed with `private-encrypted-`. Only private channels can be encrypted.

1. Set up private channel authentication on your server.

2. Generate a 32-byte master encryption key. Store this securely and never share it.

   ```csharp
   byte[] encryptionMasterKey = new byte[32];
   using (RandomNumberGenerator random = RandomNumberGenerator.Create())
   {
       random.GetBytes(encryptionMasterKey);
   }
   ```

3. Pass the key to the SDK constructor:

   ```csharp
   var sockudo = new Sockudo(Config.AppId, Config.AppKey, Config.AppSecret, new SockudoOptions
   {
       Host = "127.0.0.1",
       Port = 6001,
       EncryptionMasterKey = encryptionMasterKey,
   });

   await sockudo.TriggerAsync("private-encrypted-my-channel", "my-event", new
   {
       message = "hello world"
   }).ConfigureAwait(false);
   ```

4. Subscribe to these channels in your client. Only the client (with the matching key) can decrypt the messages.

## Querying application state

Query the state of your Sockudo application using `GetAsync` or the typed helper methods.

### Getting information for all channels

```csharp
IGetResult<ChannelsList> result = await sockudo.GetAsync<ChannelsList>("/channels");
```

or

```csharp
IGetResult<ChannelsList> result = await sockudo.FetchStateForChannelsAsync<ChannelsList>();
```

Filter by prefix:

```csharp
IGetResult<ChannelsList> result = await sockudo.FetchStateForChannelsAsync<ChannelsList>(new
{
    filter_by_prefix = "presence-"
}).ConfigureAwait(false);
```

### Getting information for a channel

```csharp
IGetResult<object> result = await sockudo.GetAsync<object>("/channels/my_channel");
```

or

```csharp
IGetResult<object> result = await sockudo.FetchStateForChannelAsync<object>("my_channel");
```

### Getting user information for a presence channel

```csharp
IGetResult<object> result =
    await sockudo.FetchUsersFromPresenceChannelAsync<object>("my_channel");
```

### Getting channel history

```csharp
IGetResult<object> page =
    await sockudo.FetchHistoryForChannelAsync<object>(
        "my_channel",
        new { limit = 50, direction = "newest_first" });

IGetResult<object> nextPage =
    await sockudo.FetchHistoryForChannelAsync<object>(
        "my_channel",
        new { cursor = "opaque-cursor-from-previous-page" });
```

### Getting presence history and snapshots

```csharp
IGetResult<object> presencePage =
    await sockudo.FetchPresenceHistoryForChannelAsync<object>(
        "presence_my_channel",
        new { limit = 50, direction = "newest_first" });

IGetResult<object> presenceNextPage =
    await sockudo.FetchPresenceHistoryForChannelAsync<object>(
        "presence_my_channel",
        new { cursor = "opaque-cursor-from-previous-page" });

IGetResult<object> presenceSnapshot =
    await sockudo.FetchPresenceSnapshotForChannelAsync<object>(
        "presence_my_channel",
        new { at_serial = 4 });
```

## Webhooks

Sockudo sends webhooks based on your application's configuration. Validate and consume them as follows:

```csharp
// HTTP_X_PUSHER_SIGNATURE from HTTP header
var receivedSignature = "value";

// Body of the HTTP request
var receivedBody = "value";

var sockudo = new Sockudo(...);
var webHook = sockudo.ProcessWebHook(receivedSignature, receivedBody);

if (webHook.IsValid)
{
    // Dictionary<string,string>[]
    var events = webHook.Events;

    foreach (var webHookEvent in webHook.Events)
    {
        var eventType = webHookEvent["name"];
        var channelName = webHookEvent["channel"];

        // Additional keys may be present depending on the event type
    }
}
else
{
    // Inspect webHook.ValidationErrors to diagnose the problem
}
```

## License

This code is free to use under the terms of the MIT license.
