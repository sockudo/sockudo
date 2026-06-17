# Sockudo Swift HTTP Server SDK

A Swift server SDK for interacting with the [Sockudo](https://github.com/sockudo/sockudo) WebSocket server HTTP API. Publish events, authorize channels, authenticate users, and handle webhooks from your Swift applications.

- [Supported platforms](#supported-platforms)
- [Installation](#installation)
- [Usage](#usage)
- [License](#license)

## Supported platforms

- Swift 5.9 and above

### Deployment targets

- iOS 13.0 and above
- macOS 10.15 and above
- tvOS 13.0 and above
- watchOS 6.0 and above

## Installation

Clone the Sockudo monorepo and use a local Swift Package Manager path until package publishing is
enabled.

Alternatively, add it as a dependency in your `Package.swift` file:

```swift
// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "YourPackage",
    products: [
        .library(
            name: "YourPackage",
            targets: ["YourPackage"]),
    ],
    dependencies: [
        .package(path: "../sockudo/server-sdks/sockudo-http-swift"),
    ],
    targets: [
        .target(
            name: "YourPackage",
            dependencies: [
                .product(name: "Sockudo", package: "sockudo-http-swift")
            ]),
    ]
)
```

Then include `import Sockudo` in any source file where you want to use the native Sockudo module.

This package exports both products:

- `Sockudo`: native Sockudo module and recommended default for new integrations
- `Pusher`: compatibility module for existing Pusher-shaped integrations

Examples in this README use the native `Sockudo` module. If you are migrating an existing Pusher integration, you can depend on `.product(name: "Pusher", package: "sockudo-http-swift")` and keep `import Pusher`.

## Usage

**Note:** Certain initializers or methods throw an error if invalid parameters are provided or an operation fails. The use of `try!` in the examples below is for brevity and is not recommended for production code.

### Configuration

Create a `Sockudo` instance using your app credentials and point it at your self-hosted server:

```swift
import Sockudo

let sockudo = Sockudo(options: try! SockudoClientOptions(
    appId: 123456,
    key: "YOUR_APP_KEY",
    secret: "YOUR_APP_SECRET",
    host: "127.0.0.1",
    port: 6001
))
```

For end-to-end encrypted channels, pass an `encryptionMasterKey`:

```swift
let sockudo = Sockudo(options: try! SockudoClientOptions(
    appId: 123456,
    key: "YOUR_APP_KEY",
    secret: "YOUR_APP_SECRET",
    host: "127.0.0.1",
    port: 6001,
    encryptionMasterKey: "YOUR_BASE64_ENCODED_MASTER_KEY"
))
```

### Triggering events

Use the `trigger(event:callback:)` method to trigger an event on one or more channels.

#### A single channel

```swift
let publicChannel = Channel(name: "my-channel", type: .public)
let publicEvent = try! Event(name: "my-event",
                             data: "hello world!",
                             channel: publicChannel)

sockudo.trigger(event: publicEvent) { result in
    switch result {
        case .success(let channelSummaries):
            // Inspect `channelSummaries`
        case .failure(let error):
            // Handle error
    }
}
```

#### Multiple channels

```swift
let channelOne = Channel(name: "my-channel", type: .public)
let channelTwo = Channel(name: "my-other-channel", type: .public)
let multichannelEvent = try! Event(name: "my-multichannel-event",
                                   data: "hello world!",
                                   channels: [channelOne, channelTwo])

sockudo.trigger(event: multichannelEvent) { result in
    switch result {
        case .success(let channelSummaries):
            // Inspect `channelSummaries`
        case .failure(let error):
            // Handle error
    }
}
```

#### Event batches

Send multiple events in a single API call (max 10 per call) using `trigger(events:callback:)`:

```swift
let eventOne = try! Event(name: "my-event",
                          data: "hello world!",
                          channel: Channel(name: "my-channel", type: .public))
let eventTwo = try! Event(name: "my-other-event",
                          data: "hello world, again!",
                          channel: Channel(name: "my-other-channel", type: .public))

sockudo.trigger(events: [eventOne, eventTwo]) { result in
    switch result {
        case .success(let channelInfoList):
            // Inspect `channelInfoList`
        case .failure(let error):
            // Handle error
    }
}
```

#### Excluding recipients

Prevent the triggering client from receiving its own event by specifying its `socketId`:

```swift
let excludedClientEvent = try! Event(name: "my-event",
                                     data: "hello world!",
                                     channel: Channel(name: "my-channel", type: .public),
                                     socketId: "123.456")

sockudo.trigger(event: excludedClientEvent) { result in
    switch result {
        case .success(let channelSummaries):
            // Inspect `channelSummaries`
        case .failure(let error):
            // Handle error
    }
}
```

#### Idempotency key

Attach an idempotency key so the server deduplicates the event on retries:

```swift
let event = try! Event(name: "my-event",
                       data: "hello world!",
                       channel: Channel(name: "my-channel", type: .public),
                       idempotencyKey: "unique-key-for-this-event")

sockudo.trigger(event: event) { result in
    switch result {
        case .success(let channelSummaries):
            // Inspect `channelSummaries`
        case .failure(let error):
            // Handle error
    }
}
```

**Note:** The `trigger(…)` method is asynchronous. In non-GUI contexts, use a semaphore if you need to wait for the result:

```swift
let sema = DispatchSemaphore(value: 0)
sockudo.trigger(event: publicEvent) { result in
    // Handle result
    sema.signal()
}
sema.wait()
```

### Authenticating channel subscriptions

Users attempting to subscribe to a private or presence channel must first be authenticated. Generate an authentication token to return to the subscribing client.

#### Private channels

```swift
let userSocketId = "123.456"
let privateChannel = Channel(name: "my-channel", type: .private)

sockudo.authenticate(channel: privateChannel,
                     socketId: userSocketId) { result in
    switch result {
        case .success(let authToken):
            // Return `authToken` to the client
        case .failure(let error):
            // Handle error
    }
}
```

#### Presence channels

For presence channels, include user identity data:

```swift
let userData = PresenceUserAuthData(userId: "USER_ID", userInfo: ["name": "Jane Smith"])
let presenceChannel = Channel(name: "my-channel", type: .presence)

sockudo.authenticate(channel: presenceChannel,
                     socketId: "USER_SOCKET_ID",
                     userData: userData) { result in
    switch result {
        case .success(let authToken):
            // Return `authToken` to the client
        case .failure(let error):
            // Handle error
    }
}
```

### User authentication

Authenticate a user for server-to-user event delivery:

```swift
let userAuthData = UserAuthData(userId: "USER_ID", userInfo: ["name": "Jane Smith"])

sockudo.authenticateUser(socketId: "USER_SOCKET_ID",
                         userData: userAuthData) { result in
    switch result {
        case .success(let authToken):
            // Return `authToken` to the client
        case .failure(let error):
            // Handle error
    }
}
```

### Verifying webhooks

Verify that a received webhook request originated from your Sockudo server. Valid webhooks contain special headers with your application key and an HMAC signature of the payload:

```swift
sockudo.verifyWebhook(request: receivedWebhookRequest) { result in
    switch result {
        case .success(let webhook):
            // Inspect `webhook`
        case .failure(let error):
            // Handle error
    }
}
```

### End-to-end encryption

This library supports end-to-end encryption of private channels. Only you and your connected clients can read the messages.

1. Set up private channel authentication on your server.

2. Generate a 32-byte master encryption key encoded as Base64. **Never share this key.**

   ```bash
   openssl rand -base64 32
   ```

3. Pass the key to `SockudoClientOptions`:

   ```swift
   let options = try! SockudoClientOptions(
       appId: 123456,
       key: "YOUR_APP_KEY",
       secret: "YOUR_APP_SECRET",
       host: "127.0.0.1",
       port: 6001,
       encryptionMasterKey: "<MASTER KEY FROM PREVIOUS COMMAND>"
   )
   ```

4. Use channels of type `encrypted`. Encrypted channel names must be prefixed with `private-encrypted-`.

5. Subscribe to these channels in your client. Only clients with the matching key can decrypt messages.

**Note:** You cannot trigger a single event on a mix of encrypted and unencrypted channels in one call. Each requires a separate API request.

### Application state queries

#### Fetch all occupied channels

```swift
// All occupied channels
sockudo.channels { result in
    switch result {
        case .success(let channelSummaries):
            // Inspect `channelSummaries`
        case .failure(let error):
            // Handle error
    }
}

// Only occupied presence channels (with user counts)
sockudo.channels(withFilter: .presence,
                 attributeOptions: .userCount) { result in
    switch result {
        case .success(let channelSummaries):
            // Inspect `channelSummaries`
        case .failure(let error):
            // Handle error
    }
}
```

#### Fetch information about a channel

```swift
let presenceChannel = Channel(name: "my-channel", type: .presence)
sockudo.channelInfo(for: presenceChannel,
                    attributeOptions: [.userCount]) { result in
    switch result {
        case .success(let channelInfo):
            // Inspect `channelInfo`
        case .failure(let error):
            // Handle error
    }
}
```

**Note:** If the channel is not occupied, the returned `ChannelInfo` will have `isOccupied` set to `false` and no attributes will be populated.

#### Fetch users subscribed to a presence channel

```swift
let presenceChannel = Channel(name: "my-channel", type: .presence)
sockudo.users(for: presenceChannel) { result in
    switch result {
        case .success(let users):
            // Inspect `users`
        case .failure(let error):
            // Handle error
    }
}
```

#### Fetch channel history

```swift
let channel = Channel(name: "my-channel", type: .public)
sockudo.history(
    for: channel,
    options: .init(limit: 50, direction: "newest_first")
) { result in
    print(result)
}

sockudo.history(
    for: channel,
    options: .init(cursor: "opaque-cursor-from-previous-page")
) { result in
    print(result)
}
```

#### Fetch presence history and snapshots

```swift
let presenceChannel = Channel(name: "my-channel", type: .presence)
sockudo.presenceHistory(
    for: presenceChannel,
    options: .init(limit: 50, direction: "newest_first")
) { result in
    print(result)
}

sockudo.presenceHistory(
    for: presenceChannel,
    options: .init(cursor: "opaque-cursor-from-previous-page")
) { result in
    print(result)
}

sockudo.presenceSnapshot(
    for: presenceChannel,
    options: .init(atSerial: 4)
) { result in
    print(result)
}
```

## License

The library is completely open source and released under the MIT license.
