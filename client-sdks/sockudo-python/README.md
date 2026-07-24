# sockudo-python

Async Sockudo client SDK for Python.

`sockudo-python` is a Pusher-compatible realtime client for Python applications. It preserves the familiar subscribe/bind/channel model while adding Sockudo-native features such as filter-aware subscriptions, delta reconstruction, and encrypted channel handling.

MessagePack and Protobuf preserve `bytes` message data as native binary. The
MessagePack representation uses the additive `["binary", <bin>]` tagged value;
existing string and JSON variants are unchanged.

## Features

- Protocol V2 by default, with V1 compatibility
- Public, private, presence, and encrypted channels
- Proxy-backed presence history and presence snapshot helpers
- Proxy-backed channel history and versioned message helpers
- Capability token auth with websocket refresh
- Tag filter and per-subscription event filter helpers
- Continuity-aware connection recovery (`stream_id` + `serial`)
- Message deduplication
- JSON, MessagePack, and Protobuf wire formats
- Fossil and Xdelta3/VCDIFF delta compression support
- User sign-in and watchlist event handling

## Install

For apps, install the published package:

```bash
pip install sockudo-python
```

For contributors working inside this repository:

```bash
pip install -e client-sdks/sockudo-python
```

Using `pyproject.toml` for local development:

```toml
[project]
dependencies = [
  "sockudo-python @ file:///absolute/path/to/sockudo/client-sdks/sockudo-python",
]
```

From this workspace:

```bash
pip install -e client-sdks/sockudo-python
```

## Quick Start

```python
import asyncio

from sockudo_python import SockudoClient, SockudoOptions


async def main() -> None:
    client = SockudoClient(
        "app-key",
        SockudoOptions(
            cluster="local",
            force_tls=False,
            ws_host="127.0.0.1",
            ws_port=6001,
        ),
    )

    channel = client.subscribe("public-updates")
    channel.bind("price-updated", lambda payload, meta: print(payload))

    await client.connect()
    await asyncio.sleep(30)
    await client.disconnect()


asyncio.run(main())
```

## Advanced Usage

### Private Channel Authorization

Use an endpoint URL (the default) or supply a fully custom async handler:

```python
from sockudo_python import (
    SockudoClient,
    SockudoOptions,
    ChannelAuthorizationOptions,
    ChannelAuthorizationData,
    ChannelAuthorizationRequest,
)


async def my_auth_handler(request: ChannelAuthorizationRequest) -> ChannelAuthorizationData:
    # Call your own backend to produce a signed auth token.
    return ChannelAuthorizationData(
        auth="app-key:hmac-sha256-signature",
        channel_data='{"user_id":"42"}',
    )


client = SockudoClient(
    "app-key",
    SockudoOptions(
        cluster="local",
        ws_host="127.0.0.1",
        ws_port=6001,
        channel_authorization=ChannelAuthorizationOptions(
            endpoint="https://api.example.com/sockudo/auth",
            # Or override entirely:
            custom_handler=my_auth_handler,
        ),
    ),
)

channel = client.subscribe("private-orders")
channel.bind("order-placed", lambda data, meta: print(data))

await client.connect()
```

### Capability Token Auth

Protocol V2 connections can include an initial capability token and refresh it with the server-supported `sockudo:auth` flow. Use a static token or an async callback; callbacks may return `TokenAuthData` with expiry metadata so the client can schedule refreshes at 80% of the token lifetime. Opaque tokens without expiry metadata rely on `sockudo:token_expired`.

```python
from sockudo_python import SockudoClient, SockudoOptions, TokenAuthData


async def auth_callback() -> TokenAuthData:
    token = await fetch_token_from_your_backend()
    return TokenAuthData(token=token, expires_in=300)


client = SockudoClient(
    "app-key",
    SockudoOptions(
        cluster="local",
        ws_host="127.0.0.1",
        ws_port=6001,
        auth_callback=auth_callback,
    ),
)
```

### Presence Channels

```python
channel = client.subscribe("presence-lobby")

channel.bind(
    "pusher:subscription_succeeded",
    lambda data, meta: print("members:", data),
)
channel.bind(
    "pusher:member_added",
    lambda data, meta: print("joined:", data),
)
channel.bind(
    "pusher:member_removed",
    lambda data, meta: print("left:", data),
)

await client.connect()

await channel.update({"status": "editing"})
```

### Presence History

Client-side presence history is proxy-backed. The Python client does not sign the server REST API directly; configure a backend endpoint that accepts `{channel, params, action}` and proxies the request with server credentials.

```python
from sockudo_python import PresenceHistoryOptions, PresenceHistoryParams, PresenceSnapshotParams

client = SockudoClient(
    "app-key",
    SockudoOptions(
        cluster="local",
        ws_host="127.0.0.1",
        ws_port=6001,
        presence_history=PresenceHistoryOptions(
            endpoint="https://api.example.com/sockudo/presence-history",
        ),
    ),
)

channel = client.subscribe("presence-lobby")

page = await channel.history(
    PresenceHistoryParams(limit=50, direction="newest_first")
)
if page.has_next():
    next_page = await page.next()

snapshot = await channel.snapshot(PresenceSnapshotParams(at_serial=4))
```

### Channel History

Message history is proxy-backed. Configure a backend endpoint that accepts `{channel, params, action}` and proxies to the Sockudo server with server credentials. `until_attach=True` uses the `attach_serial` received with `subscription_succeeded` when available.

```python
from sockudo_python import ChannelHistoryOptions, ChannelHistoryParams

client = SockudoClient(
    "app-key",
    SockudoOptions(
        cluster="local",
        ws_host="127.0.0.1",
        ws_port=6001,
        channel_history=ChannelHistoryOptions(
            endpoint="https://api.example.com/sockudo/channel-history",
        ),
    ),
)

channel = client.subscribe("room")
page = await channel.history(ChannelHistoryParams(limit=50, until_attach=True))
```

### Versioned Messages

Versioned message create and mutation helpers are also proxy-backed. The Python client does not send websocket mutation frames; configure a backend endpoint that performs the HTTP `/events` or mutation request and returns the server ack.

```python
from sockudo_python import VersionedMessageOptions

client = SockudoClient(
    "app-key",
    SockudoOptions(
        cluster="local",
        versioned_messages=VersionedMessageOptions(
            endpoint="https://api.example.com/sockudo/versioned-messages",
        ),
    ),
)

ack = await client.versioned_messages.create(
    "room",
    "message.created",
    {"text": "hello"},
    extras={"ai": {"transport": {"turn-id": "turn-1"}}},
)
await client.versioned_messages.append("room", ack.message_id, {"text": " world"})
await client.versioned_messages.update("room", ack.message_id, {"text": "hello world"})
await client.versioned_messages.delete("room", ack.message_id)
```

### Filter-Aware Subscriptions

Server-side tag filtering is a V2 feature. Only messages whose tags match the filter expression are delivered to this subscription.

```python
from sockudo_python import SubscriptionOptions, Filter

channel = client.subscribe(
    "price:btc",
    options=SubscriptionOptions(
        filter=Filter.eq("market", "spot"),
        events=["price.updated"],
        expression="data.price >= `100`",
    ),
)

# Compound filters
channel = client.subscribe(
    "price:btc",
    options=SubscriptionOptions(
        filter=Filter.and_(
            Filter.eq("market", "spot"),
            Filter.gt("spread", "0"),
        ),
    ),
)
```

### Delta Compression And Rewind

Request delta-compressed delivery to reduce bandwidth for channels that carry frequently-updated payloads:

```python
from sockudo_python import SubscriptionOptions, ChannelDeltaSettings, DeltaAlgorithm

channel = client.subscribe(
    "orderbook:btc-usd",
    options=SubscriptionOptions(
        delta=ChannelDeltaSettings(
            enabled=True,
            algorithm=DeltaAlgorithm.XDELTA3,
        ),
    ),
)
channel.bind("snapshot", lambda data, meta: print(data))

channel = client.subscribe(
    "market:btc",
    options=SubscriptionOptions(
        rewind=SubscriptionRewind.seconds_back(30),
    ),
)

client.bind("sockudo:resume_success", lambda data, _: print(data))
channel.bind("sockudo:rewind_complete", lambda data, _: print(data))
```

### Encrypted Channels

`private-encrypted-*` channels decrypt payloads automatically using the `shared_secret` returned by your auth endpoint or custom handler.

```python
channel = client.subscribe("private-encrypted-documents")
channel.bind("doc-updated", lambda data, meta: print(data))  # data is already decrypted
```

Your auth handler must populate `shared_secret` in `ChannelAuthorizationData`:

```python
async def encrypted_auth(request: ChannelAuthorizationRequest) -> ChannelAuthorizationData:
    return ChannelAuthorizationData(
        auth="app-key:hmac-sha256-signature",
        shared_secret="base64-encoded-32-byte-secret",
    )
```

### User Sign-In

```python
from sockudo_python import UserAuthenticationOptions


client = SockudoClient(
    "app-key",
    SockudoOptions(
        cluster="local",
        ws_host="127.0.0.1",
        ws_port=6001,
        user_authentication=UserAuthenticationOptions(
            endpoint="https://api.example.com/sockudo/user-auth",
        ),
    ),
)

await client.connect()
await client.user.sign_in()
```

### Connection Lifecycle

Bind to connection state changes to react to connect, disconnect, and reconnect events:

```python
def on_state_change(change) -> None:
    print(f"connection: {change.previous} -> {change.current}")

client.connection.bind("state_change", on_state_change)
client.connection.bind("connected", lambda data, _: print("socket id:", data.get("socket_id")))
client.connection.bind("disconnected", lambda data, _: print("disconnected"))
client.connection.bind("error", lambda data, _: print("error:", data))

await client.connect()
```

### Protocol V2

V2 is the default. To explicitly request it or to downgrade to V1 for strict Pusher SDK compatibility:

```python
# V2 (default) — enables continuity tokens, message_id, recovery, filters, delta
client = SockudoClient(
    "app-key",
    SockudoOptions(
        cluster="local",
        protocol_version=2,
        append_rollup_window=100,
    ),
)

# V1 — plain Pusher protocol, compatible with official Pusher SDKs
client = SockudoClient("app-key", SockudoOptions(cluster="local", protocol_version=1))
```

## Requirements

- Python 3.11+
- `asyncio`-based; designed for use with `async`/`await`

## Testing

Run the unit and integration test suite:

```bash
pytest client-sdks/sockudo-python/tests
```

Live integration tests against a local Sockudo server on port `6001`:

```bash
SOCKUDO_LIVE_TESTS=1 pytest client-sdks/sockudo-python/tests
```

The live suite covers:

- public subscribe + publish round-trip
- delta-enabled channel delivery
- encrypted channel decryption

## CI/CD

GitHub Actions are managed from the monorepo root:

- CI: `.github/workflows/sdk-ci.yml`
- Publish: `.github/workflows/sdk-release.yml` with tag `client-python-vX.Y.Z`
- Setup: see `docs/sdk-publishing-2026.md` for PyPI trusted publishing.

## Status

The package covers the core Sockudo feature set, including VCDIFF decoding, encrypted channel handling, and both supported delta algorithms, and is suitable for publishing as the official Python SDK.
