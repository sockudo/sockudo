# Sockudo Java HTTP Server SDK

A Java server SDK for interacting with the [Sockudo](https://github.com/sockudo/sockudo) WebSocket server HTTP API. Publish events, authorize channels, authenticate users, and handle webhooks from your Java applications.

## Supported platforms

- Java 11, 17, and 21
- Oracle JDK and OpenJDK
- Thread-safe with persistent connection pooling

## Installation

Clone the Sockudo monorepo and publish the SDK to your local Maven repository until the package is
published:

```bash
git clone https://github.com/sockudo/sockudo.git
cd sockudo/server-sdks/sockudo-http-java
./gradlew publishToMavenLocal
```

### Maven

```xml
<dependency>
  <groupId>io.sockudo</groupId>
  <artifactId>sockudo-http-java</artifactId>
  <version>1.0.0</version>
</dependency>
```

### Gradle

```kotlin
implementation("io.sockudo:sockudo-http-java:1.0.0")
```

## Synchronous vs asynchronous

This library provides two APIs:

- `io.sockudo.Sockudo` — synchronous, based on Apache HTTP Client
- `io.sockudo.SockudoAsync` — asynchronous, returns `CompletableFuture<T>` for every operation

The examples below use `Sockudo`, but `SockudoAsync` exposes the same API returning `CompletableFuture<T>` instead of `T`.

## Configuration

Construct a `Sockudo` instance with your app credentials, then point it at your self-hosted server:

```java
Sockudo sockudo = new Sockudo(appId, apiKey, apiSecret);
sockudo.setHost("127.0.0.1");
sockudo.setPort(6001);
```

### From URL

You can also initialize from a URL:

```java
Sockudo sockudo = new Sockudo("http://<key>:<secret>@127.0.0.1:6001/apps/<app_id>");
```

This sets `key`, `secret`, `appId`, `host`, `port`, and `secure` (based on the URL scheme) all at once.

### SSL

Enable HTTPS transport with `setEncrypted(true)`. Use this when your messages contain sensitive content and you want transport-level encryption in addition to any end-to-end encryption.

### HTTP proxy (synchronous)

```java
HttpClientBuilder builder = Sockudo.defaultHttpClientBuilder();
builder.setProxy(new HttpHost("proxy.example.com"));
sockudo.configureHttpClient(builder);
```

### HTTP proxy (asynchronous)

```java
sockudoAsync.configureHttpClient(
    config()
        .setProxyServer(proxyServer("127.0.0.1", 38080))
        .setMaxRequestRetry(5)
);
```

## Usage

### Responses

All requests return a `Result` object. Call `getStatus()` to get a `Status` enum value such as `Status.SUCCESS` or `Status.AUTHENTICATION_ERROR`. On error, `getMessage()` provides a description.

### Publishing events

Data is serialised using the GSON library by default. You can provide your own marshalling library via `setDataMarshaller`. POJOs and `java.util.Map` are suitable for marshalling.

#### Single channel

```java
sockudo.trigger("channel-one", "test_event", Collections.singletonMap("message", "hello world"));
```

#### Multiple channels

```java
List<String> channels = new ArrayList<>();
channels.add("channel-one");
channels.add("channel-two");

sockudo.trigger(channels, "test_event", Collections.singletonMap("message", "hello world"));
```

You can trigger an event to at most 10 channels at once. Passing more than 10 channels will throw an exception.

#### Batch events

```java
List<Event> batch = new ArrayList<>();
batch.add(new Event("channel-one", "event-1", Collections.singletonMap("value", 1)));
batch.add(new Event("channel-two", "event-2", Collections.singletonMap("value", 2)));

sockudo.triggerBatch(batch);
```

#### Excluding event recipients

Pass a `socketId` to prevent the triggering client from also receiving the event:

```java
sockudo.trigger(channel, event, data, "1302.1081607");
```

#### Idempotency key

Use `TriggerOptions` to attach an idempotency key. The server will deduplicate events with the same key, ensuring at-most-once delivery even on retries:

```java
TriggerOptions options = new TriggerOptions();
options.setIdempotencyKey("unique-key-for-this-event");

sockudo.trigger("channel-one", "test_event", data, options);
```

### Authenticating private channels

Return the authentication response body to the client requesting subscription:

```java
String authBody = sockudo.authenticate(socketId, channel);
```

### Authenticating presence channels

For presence channels, include user identity data alongside the socket authentication:

```java
String userId = "unique_user_id";
Map<String, String> userInfo = new HashMap<>();
userInfo.put("name", "Jane Smith");
userInfo.put("role", "admin");

String authBody = sockudo.authenticate(socketId, channel, new PresenceUser(userId, userInfo));
```

### User authentication

Authenticate a user for server-to-user event delivery:

```java
Map<String, Object> userData = new HashMap<>();
userData.put("id", "user-123");
userData.put("name", "Jane Smith");

String authBody = sockudo.authenticateUser(socketId, userData);
```

### Application state

Query the state of your application using the `get` method.

#### List all channels

```java
Result result = sockudo.get("/channels", params);
if (result.getStatus() == Status.SUCCESS) {
    String channelListJson = result.getMessage();
    // Parse and act upon list
}
```

#### Get the state of a channel

```java
Result result = sockudo.get("/channels/[channel_name]");
```

#### Get users in a presence channel

```java
Result result = sockudo.get("/channels/[channel_name]/users");
```

### Webhooks

Validate the authenticity of incoming webhooks using the `X-Pusher-Key` and `X-Pusher-Signature` headers:

```java
Validity validity = sockudo.validateWebhookSignature(xPusherKey, xPusherSignature, body);
```

### Generating signed URIs

If you want to issue HTTP API requests manually using a different HTTP client, generate a signed URI:

```java
URI requestUri = sockudo.signedUri("GET", "/apps/<appId>/channels", null);
```

For requests with a body, the signature covers the body content:

```java
URI requestUri = sockudo.signedUri("POST", "/apps/<appId>/events", body);
```

Additional query parameters can be appended:

```java
URI requestUri = sockudo.signedUri("GET", "/apps/<appId>/channels", null,
    Collections.singletonMap("filter_by_prefix", "presence-"));
```

The following query parameter keys are reserved for signing and must not be used:

- `auth_key`
- `auth_timestamp`
- `auth_version`
- `auth_signature`
- `body_md5`

### Multi-threaded usage

The library is thread-safe and intended for concurrent use. HTTP connections are persistent and pooled, reducing TCP overhead across repeated requests. By default, at most 2 concurrent connections are maintained. To increase this:

```java
PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
connManager.setDefaultMaxPerRoute(maxConns);

sockudo.configureHttpClient(
    Sockudo.defaultHttpClientBuilder()
           .setConnectionManager(connManager)
);
```

### End-to-end encryption

This library supports end-to-end encryption of private channels. Only you and your connected clients can read the messages — the server cannot decrypt them.

1. Set up private channel authentication on your server.

2. Generate a 32-byte master encryption key and encode it as Base64:

   ```bash
   openssl rand -base64 32
   ```

3. Pass the key to the constructor:

   ```java
   Sockudo sockudo = new Sockudo(APP_ID, API_KEY, API_SECRET, ENCRYPTION_MASTER_KEY_BASE64);
   sockudo.setHost("127.0.0.1");
   sockudo.setPort(6001);
   ```

4. Prefix encrypted channels with `private-encrypted-`:

   ```java
   sockudo.trigger("private-encrypted-my-channel", "my-event",
       Collections.singletonMap("message", "secret content"));
   ```

**Note:** A single `trigger` call cannot target both encrypted and unencrypted channels simultaneously.

### Channel history

```java
Map<String, String> params = new HashMap<>();
params.put("limit", "50");
params.put("direction", "newest_first");

Result page = sockudo.getChannelHistory("my-channel", params);
Result nextPage = sockudo.getChannelHistory(
    "my-channel",
    Collections.singletonMap("cursor", "opaque-cursor-from-previous-page")
);
```

### Presence history

```java
Map<String, String> params = new HashMap<>();
params.put("limit", "50");
params.put("direction", "newest_first");

Result page = sockudo.getChannelPresenceHistory("presence-room", params);
Result nextPage = sockudo.getChannelPresenceHistory(
    "presence-room",
    Collections.singletonMap("cursor", "opaque-cursor-from-previous-page")
);

Result snapshot = sockudo.getChannelPresenceSnapshot(
    "presence-room",
    Collections.singletonMap("at_serial", "4")
);
```

## License

This code is free to use under the terms of the MIT license.
