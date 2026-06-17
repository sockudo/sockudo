# Sockudo Python HTTP Server SDK

High-performance Python server SDK for the Sockudo HTTP API. It publishes events, signs channel and user authentication payloads, validates webhooks, queries channel state, reads durable history, mutates versioned messages, and manages annotations.

## Features

- Sync and asyncio clients: `Sockudo` and `AsyncSockudo`
- Persistent HTTP connection pooling via `httpx`, with HTTP/2 enabled by default
- Pusher-compatible signed REST requests
- Single, multi-channel, and batch publishing
- Explicit and automatic idempotency keys for safe publish retries
- Private, presence, user, and webhook authentication helpers
- Channel state, presence users, durable history, and presence history APIs
- Versioned message APIs: get, versions, update, delete, append
- Annotation APIs: publish, list, delete
- Operator controls for history reset/purge and presence-history reset
- End-to-end encrypted channel auth, publish, batch publish, and webhook decrypt support for `private-encrypted-*` channels

## Install

Clone the Sockudo monorepo and install from the local path until the PyPI package is published:

```bash
git clone https://github.com/sockudo/sockudo.git
pip install -e sockudo/server-sdks/sockudo-http-python
```

For local development from this monorepo:

```bash
pip install -e server-sdks/sockudo-http-python[dev]
```

## Quick Start

```python
from sockudo_http import Config, Sockudo

sockudo = Sockudo(Config(app_id="app-id", key="app-key", secret="app-secret", host="127.0.0.1", port=6001))

result = sockudo.trigger("orders", "order.created", {"id": "ord_123"})
assert result.ok
sockudo.close()
```

Async:

```python
from sockudo_http_python import AsyncSockudo, SockudoOptions

async with AsyncSockudo(
    "app-id",
    "app-key",
    "app-secret",
    options=SockudoOptions(host="127.0.0.1", port=6001),
) as sockudo:
    await sockudo.trigger("orders", "order.created", {"id": "ord_123"})
```

## Idempotent Publishing

```python
from sockudo_http_python import TriggerOptions

sockudo.trigger(
    "orders",
    "order.created",
    {"id": "ord_123"},
    TriggerOptions(idempotency_key="order-created-ord_123"),
)

sockudo.trigger("orders", "order.created", {"id": "ord_124"}, TriggerOptions(idempotency_key=True))
```

Set `SockudoOptions(auto_idempotency=True)` to generate keys for publish and batch publish calls that omit one.

Target a single user channel:

```python
sockudo.send_to_user("user-123", "notice", {"body": "hello"})
```

## Authentication Helpers

```python
from sockudo_http_python import PresenceUser

private_body = sockudo.authenticate("123.456", "private-orders")

presence_body = sockudo.authenticate(
    "123.456",
    "presence-room",
    PresenceUser("user-1", {"name": "Ada"}),
)

user_body = sockudo.authenticate_user("123.456", {"id": "user-1", "name": "Ada"})
```

Encrypted channel auth responses include `shared_secret` when `encryption_master_key_base64` is configured:

```python
encrypted = Sockudo(
    "app-id",
    "app-key",
    "app-secret",
    encryption_master_key_base64="base64-encoded-32-byte-key",
)

body = encrypted.authenticate("123.456", "private-encrypted-room")
```

## Channel And History APIs

```python
from sockudo_http_python import ChannelsParams, HistoryParams, PresenceHistoryParams

sockudo.list_channels(ChannelsParams(filter_by_prefix="presence-", info=["subscription_count", "user_count"]))
sockudo.get_channel_users("presence-room")
sockudo.get_channel_history("orders", HistoryParams(limit=50, direction="newest_first"))
sockudo.get_channel_presence_history("presence-room", PresenceHistoryParams(limit=50))
```

## Versioned Messages And Annotations

```python
from sockudo_http_python import MessageMutation, PublishAnnotationRequest

sockudo.get_message("orders", "msg:1")
sockudo.get_message_versions("orders", "msg:1")
sockudo.update_message("orders", "msg:1", MessageMutation(data={"status": "paid"}))
sockudo.append_message("orders", "msg:1", " appended text")
sockudo.delete_message("orders", "msg:1", MessageMutation(description="moderated"))

sockudo.publish_annotation(
    "orders",
    "msg:1",
    PublishAnnotationRequest(type="reactions:distinct.v1", name="like", client_id="user-1", count=1),
)
sockudo.list_annotations("orders", "msg:1")
```

## Webhooks

```python
validity = sockudo.validate_webhook_signature(x_pusher_key, x_pusher_signature, raw_body)
webhook = sockudo.parse_webhook(x_pusher_key, x_pusher_signature, raw_body)
```

If a webhook contains encrypted channel events and the client has an encryption master key, `parse_webhook` decrypts those event payloads.

## Signed URIs

```python
uri = sockudo.signed_uri("GET", "/apps/app-id/channels", parameters={"filter_by_prefix": "presence-"})
```

The signing format matches Sockudo/Pusher REST auth: `auth_key`, `auth_timestamp`, `auth_version`, optional `body_md5`, and `auth_signature` over `{METHOD}\n{PATH}\n{SORTED_QUERY}`.

## URL Configuration

```python
sockudo = Sockudo.from_url("http://app-key:app-secret@127.0.0.1:6001/apps/app-id")
```
