# sockudo-client

The official async Rust realtime client for [Sockudo](https://github.com/sockudo/sockudo).

`sockudo-client` speaks strict Pusher-compatible Protocol V1 and the
Sockudo-native Protocol V2. It is built on [`sockudo-ws`](https://crates.io/crates/sockudo-ws)
for WebSockets and [`sonic-rs`](https://crates.io/crates/sonic-rs) for JSON, runs on
Tokio, and keeps the hot path allocation-light: one task owns the socket,
outbound frames are written in batches, payloads are decoded lazily, and no
lock is held across an `.await`.

## Features

- Protocol V2 by default, with strict V1 (Pusher) compatibility
- Public, private, presence and end-to-end encrypted (`private-encrypted-*`) channels
- Channel authorization and user authentication via HTTP endpoints or custom async handlers
- Capability tokens (static, callback or URL) with proactive refresh at 80% of lifetime,
  reactive refresh on `40142`, and fail-closed revocation on `40160`
- Presence membership, `presence_update`, proxy-backed presence history and snapshots
- Tag filters, event-name filters and JMESPath expressions
- Delta compression: Fossil and Xdelta3/VCDIFF, conflation keys, cache sync, automatic resync
- Continuity-aware recovery (`stream_id` + `serial`) with persistable positions, plus rewind
- Message deduplication by `message_id`
- JSON, MessagePack and Protobuf wire formats
- Proxy-backed channel history (including gap-free `until_attach`), message versions,
  annotations, and versioned-message create/append/update/delete
- Client-side mutable-message reducer
- User sign-in, server-to-user events and watchlist events
- Push registration and publishing client
- Typed callbacks and bounded async `Stream`s for every event surface
- Reconnect with quadratic backoff, jitter, a bounded budget and Pusher close-code semantics
- permessage-deflate WebSocket compression, negotiated when the server enables it
- Opt-in connection timeline telemetry (`enable_stats`), compatible with the JS client

## Install

The crate lives in the Sockudo monorepo. Until it is published, depend on it by
path or git:

```toml
[dependencies]
sockudo-client = { git = "https://github.com/sockudo/sockudo", package = "sockudo-client" }
# or, inside a checkout:
# sockudo-client = { path = "client-sdks/sockudo-rust" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Minimum supported Rust version: 1.88.

## Quick start

```rust,no_run
use serde::Deserialize;
use sockudo_client::{Result, SockudoClient};

#[derive(Debug, Deserialize)]
struct Price {
    symbol: String,
    price: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = SockudoClient::builder("app-key")
        .ws_host("127.0.0.1")
        .ws_port(6001)
        .force_tls(false)
        .build()?;

    let channel = client.subscribe("prices");
    channel.bind("price-updated", |event| {
        if let Ok(price) = event.json::<Price>() {
            println!("{price:?}");
        }
    });

    client.connect()?;
    let socket_id = client.connected().await?;
    println!("connected as {socket_id}");

    tokio::signal::ctrl_c().await?;
    client.disconnect().await;
    Ok(())
}
```

`connect()` starts connecting in the background and is idempotent;
`connected().await` resolves with the socket id, or fails once the client stops
trying. Channels can be subscribed before or after connecting and are
resubscribed after every reconnect.

### Callbacks and streams

Callbacks (`bind`, `bind_global`) run on the connection task in wire order and
receive `&Event`; keep them short. For heavier work use a bounded stream:

```rust,no_run
# async fn demo(client: sockudo_client::SockudoClient) {
use futures_util::StreamExt;

let channel = client.subscribe("orders");
let mut orders = channel.stream_event("order-placed", 1024);
while let Some(event) = orders.next().await {
    // `Arc<Event>`; events beyond the capacity are dropped and counted.
    println!("{:?} (dropped so far: {})", event.value(), orders.dropped());
}
# }
```

`Event::json::<T>()` deserializes the payload straight into your type,
`Event::value()` gives a dynamic `sonic_rs::Value`, and `Event::text()` the
raw payload text. `Event::name` / `Event::logical_name()` carry the publisher's
event name for versioned-message envelopes (`sockudo:message.create`, …).

## Authorization

Private, presence and encrypted channels need a signature from **your**
backend. Use an HTTP endpoint (form POST of `socket_id` and `channel_name`,
returning `{"auth", "channel_data"?, "shared_secret"?}`):

```rust,no_run
# fn demo() -> sockudo_client::Result<()> {
use sockudo_client::{Endpoint, SockudoClient};

let client = SockudoClient::builder("app-key")
    .cluster("eu")
    .channel_auth_endpoint(
        Endpoint::new("https://api.example.com/sockudo/auth")
            .header("X-Tenant", "acme")
            .headers_provider(|| vec![("Authorization".into(), "Bearer <session>".into())]),
    )
    .user_auth_endpoint("https://api.example.com/sockudo/user-auth")
    .build()?;
# Ok(()) }
```

or any async closure:

```rust,no_run
# fn demo() -> sockudo_client::Result<()> {
use sockudo_client::{ChannelAuthData, ChannelAuthRequest, SockudoClient};

let client = SockudoClient::builder("app-key")
    .cluster("eu")
    .channel_authorizer(|request: ChannelAuthRequest| async move {
        // Call your backend however you like.
        Ok(ChannelAuthData::new(format!("app-key:{}", request.socket_id)))
    })
    .build()?;
# Ok(()) }
```

Failures emit `sockudo:subscription_error` on the channel
(`{"type":"AuthError","error":…,"status"?}`) and reach `on_error`.

## Capability tokens (Protocol V2)

```rust,no_run
# fn demo() -> sockudo_client::Result<()> {
use std::time::Duration;
use sockudo_client::{SockudoClient, TokenAuthData, TokenRequest};

let client = SockudoClient::builder("app-key")
    .cluster("eu")
    .token_provider(|request: TokenRequest| async move {
        let token = fetch_token_from_backend(request.reason.as_str()).await;
        Ok(TokenAuthData::new(token).with_expires_in(Duration::from_secs(300)))
    })
    .build()?;
# Ok(()) }
# async fn fetch_token_from_backend(_: &str) -> String { String::new() }
```

The token goes in the connection URL, is refreshed with `sockudo:auth` at 80% of
its lifetime (no later than `auth_refresh_leeway` before expiry), again on
`40142`, and before each reconnect. JWT `exp`/`iat` claims are read when no
lifetime is given. A static `.token(..)` is presented on every connection and
never refreshed. `.auth_url(..)` POSTs `{"socket_id","reason"}` and accepts
`{"token","expires_in"?,…}`. Revocation (`40160`) is reported and not retried.

## Presence

```rust,no_run
# async fn demo(client: sockudo_client::SockudoClient) -> sockudo_client::Result<()> {
let lobby = client.subscribe("presence-lobby");
lobby.bind("sockudo:member_added", |event| println!("joined {}", event.value()));
lobby.bind("sockudo:member_removed", |event| println!("left {}", event.value()));

if let Some(members) = lobby.members() {
    println!("{} online, me = {:?}", members.count(), members.me());
}
lobby.update_presence(&sonic_rs::json!({"status": "away"})).await?;
# Ok(()) }
```

## Filters, delta compression and rewind

```rust,no_run
# fn demo(client: sockudo_client::SockudoClient) -> sockudo_client::Result<()> {
use sockudo_client::{
    ChannelDeltaSettings, DeltaAlgorithm, DeltaOptions, Filter, Rewind, SubscribeOptions,
};

let ticker = client.subscribe_with(
    "ticker:btc",
    SubscribeOptions::new()
        .filter(Filter::and([Filter::eq("market", "spot"), Filter::gt("spread", "0")]))
        .events(["price.updated"])
        .expression("data.price >= `100`")
        .delta(ChannelDeltaSettings::with_algorithm(DeltaAlgorithm::Fossil))
        .rewind(Rewind::Seconds(30)),
)?;
ticker.bind("sockudo:rewind_complete", |event| {
    let done = sockudo_client::recovery::RewindComplete::from_event(event);
    println!("replayed {}", done.historical_count);
});
# Ok(()) }
```

Enable delta handling on the client with
`.delta_compression(DeltaOptions::default())`. Bases are tracked per
`(event, conflation key)` exactly like the server cache; a missing or
corrupt base triggers `delta_sync_error` and the server resends a full message.
`client.delta_stats()` reports bandwidth saved. Delta compression requires the
JSON wire format (bases are the server's JSON bytes).

## Recovery

```rust,no_run
# fn demo(client: sockudo_client::SockudoClient) {
// With `.connection_recovery(true)`, positions are tracked per channel and a
// `resume` is sent after every reconnect (after all subscribes).
let saved = client.recovery_positions();
// ...persist `saved` (it is Serialize) and restore after a restart:
client.set_recovery_positions(saved);
client.bind("sockudo:resume_failed", |event| {
    let failed = sockudo_client::recovery::ResumeFailed::from_event(event);
    eprintln!("reload state for {}: {}", failed.channel, failed.reason);
});
# }
```

A failed resume drops the channel's position and resubscribes it.

## History, versioned messages and annotations

The client never holds the app secret, so these calls go through a proxy on
your backend (`ProxyOptions::endpoint(url)` or `ProxyOptions::handler(closure)`).
Each call POSTs `{"channel","action",…}`; forward it to the REST API and return
the response unchanged.

```rust,no_run
# async fn demo() -> sockudo_client::Result<()> {
use sockudo_client::rest::{ChannelHistoryParams, Direction, PublishAnnotation};
use sockudo_client::{ProxyOptions, SockudoClient};

let client = SockudoClient::builder("app-key")
    .cluster("eu")
    .versioned_messages(ProxyOptions::endpoint("https://api.example.com/sockudo/messages"))
    .presence_history(ProxyOptions::endpoint("https://api.example.com/sockudo/presence"))
    .build()?;
let room = client.subscribe("chat:room-1");

let mut page = room
    .history(ChannelHistoryParams::new().limit(50).direction(Direction::OldestFirst).until_attach())
    .await?;
while page.has_next() {
    page = page.next().await?;
}
let state = sockudo_client::versioned::reduce_mutable_message_events(&page.events())?;

let ack = room.create_versioned_message("chat.message", &sonic_rs::json!({"text": "hi"}), None).await?;
room.publish_annotation("message-serial", PublishAnnotation::new("reaction:distinct.v1").name("👍")).await?;
# let _ = (state, ack); Ok(()) }
```

| Method | `action` |
| --- | --- |
| `history` | `channel_history` (or `history` on `.channel_history(..)`) |
| `get_message` / `get_message_versions` | `get_message` / `get_message_versions` |
| `publish_annotation` / `delete_annotation` / `list_annotations` | same names |
| `presence_history` / `presence_snapshot` | `history` / `snapshot` (presence proxy) |
| `versioned_messages().create/append/update/delete` | `create` / `append` / `update` / `delete` |

## Encrypted channels

`private-encrypted-*` payloads are decrypted automatically with the
`shared_secret` returned by your auth endpoint. When a payload fails to
decrypt (key rotation), the client re-authorizes once and retries. Client
events are not supported on encrypted channels.

## User sign-in and watchlist

```rust,no_run
# fn demo(client: sockudo_client::SockudoClient) -> sockudo_client::Result<()> {
let user = client.user();
user.bind("invoice-ready", |event| println!("for me: {}", event.value()));
user.watchlist().bind("online", |event| println!("online: {}", event.value()));
client.sign_in()?; // runs now if connected, and after every reconnect
# Ok(()) }
```

## WebSocket compression and telemetry

The client offers permessage-deflate on every connection; it is used only when
the server sets `[websocket] compression`, and `client.is_compressed()` reports
the outcome. Disable the offer with `.permessage_deflate(false)`.

`.enable_stats(true)` sends a bounded timeline of connection events (state
changes, retries, errors; never payloads or credentials) to `stats_host` on
connect and every 60 seconds, in the JS client's format. It is off by default.

## Push notifications

```rust,no_run
# async fn demo() -> sockudo_client::Result<()> {
use sockudo_client::push::*;

let push = PushRegistration::new("https://api.example.com/sockudo/push")?;
push.activate_device(&PushDeviceDetails::new(
    "device-1",
    "ios",
    "phone",
    PushRecipient::Apns { device_token: "<token>".into() },
))
.await?;
push.publish(&PushPublishRequest::new(
    vec![PushPublishTarget::Device { device_id: "device-1".into() }],
    PushPayload::notification("Hello", "World"),
))
.await?;
# Ok(()) }
```

## Connection lifecycle

```rust,no_run
# fn demo(client: sockudo_client::SockudoClient) {
client.on_state_change(|change| println!("{} -> {}", change.previous, change.current));
client.on_error(|error| eprintln!("{error} (code {:?})", error.code()));
let mut watcher = client.state_watcher(); // tokio::sync::watch
# }
```

States: `initialized`, `connecting`, `connected`, `reconnecting`, `unavailable`
(connecting for longer than `unavailable_timeout`), `disconnected`, `failed`.

Unexpected drops reconnect with a quadratic delay (0s, 1s, 4s, 9s … capped by
`max_reconnect_gap`, default 120s) for up to `max_reconnect_attempts` (default
6; `None` retries forever). `reconnect_jitter(f)` randomizes each delay by
fraction `f` so clients dropped together do not return in lockstep. Close codes
follow Pusher: `4000` reconnects over TLS, `4001–4099` and `≥4300` stop,
`4100–4199` back off, `4200–4299` reconnect immediately. The client pings after
`activity_timeout` of silence (the server's value wins when lower) and
reconnects when no pong arrives within `pong_timeout`.

## Protocol V1 and wire formats

```rust,no_run
# fn demo() -> sockudo_client::Result<()> {
use sockudo_client::{ProtocolVersion, SockudoClient, WireFormat};

let v1 = SockudoClient::builder("app-key").cluster("eu").protocol_version(ProtocolVersion::V1).build()?;
let binary = SockudoClient::builder("app-key").cluster("eu").wire_format(WireFormat::Protobuf).build()?;
# Ok(()) }
```

V2-only options (tokens, binary formats, subscription options) are rejected
under V1 at build time.

## Cargo features

| Feature | Default | Enables |
| --- | --- | --- |
| `tls` | yes | `wss://` via rustls (aws-lc-rs) and the OS trust store |
| `tls-webpki-roots` | no | bundled Mozilla roots instead of the OS store |
| `http` | yes | auth/token endpoints, HTTP proxies, push client |
| `encryption` | yes | `private-encrypted-*` channels |
| `xdelta3` | yes | Xdelta3/VCDIFF deltas (Fossil is always available) |
| `msgpack` | yes | MessagePack wire format |
| `protobuf` | yes | Protobuf wire format |
| `deflate` | yes | permessage-deflate WebSocket compression |

Use `.tls_config(Arc<rustls::ClientConfig>)` for custom roots or client
certificates, and `.http_client(reqwest::Client)` to share an HTTP client.

## Performance

Measured with `cargo bench --bench client_hot_paths` (x86-64, release):

| Path | Time |
| --- | --- |
| JSON event decode (230-byte frame) | ~330 ns |
| MessagePack / Protobuf event decode | ~205 ns / ~147 ns |
| JSON client-event encode | ~74 ns |
| Deduplication check | ~12 ns |
| Fossil delta apply | ~37 ns |
| End to end over loopback (socket → callback) | ~2.5 M events/s |

## Testing

```bash
cargo test                       # unit, mock-server, and server-codec conformance tests
cargo test --no-default-features # minimal build
SOCKUDO_LIVE_TESTS=1 cargo test --test live   # against a running server
cargo bench --bench client_hot_paths
```

The mock-server suites cover lifecycle, reconnect policy, keepalive, auth,
presence, encryption, sign-in, recovery, delta, tokens and proxies. The
conformance suite encodes and decodes frames with the server's own
`sockudo-protocol` crate in every wire format and checks that delta bases match
the server byte for byte. The live suite runs against `config/config.toml`
defaults (`app-key` on `127.0.0.1:6001`).

## Compatibility notes

- `presence_update` data is sent as a JSON-encoded string on the JSON wire
  format. Current servers also accept the object form, but servers released
  before that fix reject it; the string form works with every version.
- Delta compression requires the JSON wire format.
- `extras.idempotency_key` is serialized with the server's spelling,
  `idempotencyKey`, and accepted in either spelling.

## License

MIT
