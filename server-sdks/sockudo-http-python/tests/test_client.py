import json
import base64
import hashlib
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from nacl.secret import SecretBox

from sockudo_http_python import (
    AnnotationEventsParams,
    AsyncSockudo,
    Config,
    Event,
    HistoryParams,
    PresenceUser,
    PushCursorParams,
    PushSubscriptionParams,
    PublishAnnotationRequest,
    Sockudo,
    SockudoError,
    SockudoOptions,
    Status,
    TriggerOptions,
    Validity,
    Webhook,
    sign,
)


TEST_MASTER_KEY = base64.b64encode(b"01234567890123456789012345678901").decode("ascii")


def make_client(handler):
    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)
    return Sockudo(
        "app-id",
        "app-key",
        "app-secret",
        options=SockudoOptions(max_retries=1),
        client=http_client,
    )


def load_forward_fixture(name):
    for parent in Path(__file__).resolve().parents:
        candidate = (
            parent / "tests" / "ai-conformance" / "fixtures" / "forward-compat" / name
        )
        if candidate.exists():
            return candidate.read_text(encoding="utf-8")
    raise AssertionError(f"forward-compat fixture not found: {name}")


def test_signed_uri_contains_expected_signature_params():
    sockudo = Sockudo(
        "app-id", "key", "secret", options=SockudoOptions(host="localhost", port=6001)
    )
    uri = sockudo.signed_uri(
        "GET", "/apps/app-id/channels", parameters={"filter_by_prefix": "presence-"}
    )
    parsed = urlparse(uri)
    query = parse_qs(parsed.query)

    assert parsed.path == "/apps/app-id/channels"
    assert query["auth_key"] == ["key"]
    assert query["auth_version"] == ["1.0"]
    assert "auth_timestamp" in query
    assert "auth_signature" in query
    sockudo.close()


def test_webhook_accepts_future_event_fixture_and_preserves_unknown_fields():
    body = load_forward_fixture("future-webhook-events.json")
    sockudo = Sockudo(
        "app-id", "app-key", "app-secret", options=SockudoOptions(max_retries=1)
    )

    webhook = sockudo.parse_webhook("app-key", sign(body, "app-secret"), body)

    assert webhook.time_ms == 1710000000000
    assert [event.name for event in webhook.events] == [
        "member_updated",
        "ai_run_started",
        "message_version_created",
    ]
    assert webhook.events[0].channel == "presence-ai-forward"
    assert webhook.events[0].user_id == "user-1"
    assert webhook.events[0].raw["future_field"] == "must-pass-through"
    assert webhook.events[1].raw["run_id"] == "run-1"
    assert Webhook.parse(body).events[2].raw["version_serial"] == "ver-1"
    sockudo.close()


def test_webhook_preserves_nested_future_event_values():
    body = json.dumps(
        {
            "time_ms": 1710000000000,
            "events": [
                {
                    "name": "ai_turn_started",
                    "channel": "private-ai-forward",
                    "data": {
                        "turn_id": "turn-1",
                        "tokens": ["hello", "world"],
                        "done": False,
                        "nullable": None,
                    },
                    "future_field": {"nested": True},
                }
            ],
        },
        separators=(",", ":"),
    )
    sockudo = Sockudo(
        "app-id", "app-key", "app-secret", options=SockudoOptions(max_retries=1)
    )

    webhook = sockudo.parse_webhook("app-key", sign(body, "app-secret"), body)

    assert webhook.events[0].name == "ai_turn_started"
    assert webhook.events[0].data == {
        "turn_id": "turn-1",
        "tokens": ["hello", "world"],
        "done": False,
        "nullable": None,
    }
    assert webhook.events[0].raw["future_field"] == {"nested": True}
    sockudo.close()


def test_signed_uri_signs_lowercase_query_keys_but_keeps_original_request_keys():
    sockudo = Sockudo(
        "app-id", "key", "secret", options=SockudoOptions(host="localhost", port=6001)
    )
    uri = sockudo.signed_uri(
        "GET",
        "/apps/app-id/push/channelSubscriptions",
        parameters={"deviceId": "device-1", "limit": 10},
    )
    parsed = urlparse(uri)
    query = parse_qs(parsed.query)

    expected_to_sign = (
        f"GET\n/apps/app-id/push/channelSubscriptions\n"
        f"auth_key=key&auth_timestamp={query['auth_timestamp'][0]}&auth_version=1.0&deviceid=device-1&limit=10"
    )
    assert query["deviceId"] == ["device-1"]
    assert "deviceid" not in query
    assert query["auth_signature"] == [sign(expected_to_sign, "secret")]
    sockudo.close()


def test_trigger_posts_signed_event_payload_and_idempotency_header():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["query"] = dict(request.url.params)
        seen["body"] = json.loads(request.content.decode())
        seen["idempotency"] = request.headers.get("X-Idempotency-Key")
        return httpx.Response(200, json={})

    sockudo = make_client(handler)
    result = sockudo.trigger(
        ["orders", "audit"],
        "order.created",
        {"id": 1},
        TriggerOptions(
            socket_id="123.456", idempotency_key="idem-1", tags={"tenant": "acme"}
        ),
    )

    assert result.ok
    assert seen["path"] == "/apps/app-id/events"
    assert seen["body"] == {
        "name": "order.created",
        "data": '{"id":1}',
        "channels": ["orders", "audit"],
        "socket_id": "123.456",
        "idempotency_key": "idem-1",
        "tags": {"tenant": "acme"},
    }
    assert seen["idempotency"] == "idem-1"
    assert seen["query"]["body_md5"]
    sockudo.close()


def test_idempotency_true_generates_key():
    seen = {}

    def handler(request):
        seen["body"] = json.loads(request.content.decode())
        seen["idempotency"] = request.headers.get("X-Idempotency-Key")
        return httpx.Response(200, json={})

    sockudo = make_client(handler)
    result = sockudo.trigger(
        "orders", "created", {}, TriggerOptions(idempotency_key=True)
    )

    assert result.ok
    assert seen["body"]["idempotency_key"]
    assert seen["idempotency"] == seen["body"]["idempotency_key"]
    sockudo.close()


def test_batch_payload_uses_batch_events_endpoint():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={"batch": [{}, {}]})

    sockudo = make_client(handler)
    result = sockudo.trigger_batch(
        [
            Event("orders", "created", {"id": 1}),
            Event("orders", "paid", {"id": 1}),
        ]
    )

    assert result.ok
    assert seen["path"] == "/apps/app-id/batch_events"
    assert seen["body"] == {
        "batch": [
            {"name": "created", "data": '{"id":1}', "channel": "orders"},
            {"name": "paid", "data": '{"id":1}', "channel": "orders"},
        ]
    }
    sockudo.close()


def test_batch_auto_idempotency_adds_per_event_keys():
    seen = {}

    def handler(request):
        seen["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={"batch": [{}, {}]})

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)
    sockudo = Sockudo(
        "app-id",
        "key",
        "secret",
        options=SockudoOptions(max_retries=1, auto_idempotency=True),
        client=http_client,
    )
    result = sockudo.trigger_batch(
        [
            Event("orders", "created", {"id": 1}),
            Event("orders", "paid", {"id": 1}, idempotency_key="explicit"),
            Event("orders", "shipped", {"id": 1}, idempotency_key=True),
        ]
    )

    keys = [item["idempotency_key"] for item in seen["body"]["batch"]]
    assert result.ok
    assert keys[0].endswith(":1:0")
    assert keys[1] == "explicit"
    assert keys[2] != "True"
    sockudo.close()


def test_history_and_annotation_paths_are_encoded():
    paths = []

    def handler(request):
        paths.append((request.method, request.url.path, dict(request.url.params)))
        return httpx.Response(200, json={})

    sockudo = make_client(handler)
    sockudo.get_channel_history(
        "market:BTC/USD", HistoryParams(limit=50, direction="newest_first")
    )
    sockudo.get_message_versions("chat room", "msg:1")
    sockudo.publish_annotation(
        "chat room",
        "msg:1",
        PublishAnnotationRequest(
            type="reactions:distinct.v1", name="like", client_id="u1"
        ),
    )
    sockudo.list_annotations(
        "chat room", "msg:1", AnnotationEventsParams(type="reactions:distinct.v1")
    )
    sockudo.delete_annotation("chat room", "msg:1", "ann:1", socket_id="123.456")

    assert paths[0][1] == "/apps/app-id/channels/market:BTC/USD/history"
    assert paths[0][2]["limit"] == "50"
    assert paths[1][1] == "/apps/app-id/channels/chat room/messages/msg:1/versions"
    assert paths[2][0] == "POST"
    assert paths[3][2]["type"] == "reactions:distinct.v1"
    assert paths[4][0] == "DELETE"
    assert paths[4][2]["socket_id"] == "123.456"
    sockudo.close()


def test_push_helpers_use_signed_push_admin_endpoints_and_async_publish_default():
    seen = []

    def handler(request):
        seen.append(
            (
                request.method,
                request.url.path,
                dict(request.url.params),
                json.loads(request.content.decode() or "{}"),
                dict(request.headers),
            )
        )
        if request.url.path.endswith("/push/publish"):
            return httpx.Response(
                202, json={"publish_id": "pub_123", "status": "queued"}
            )
        return httpx.Response(
            200, json={"items": [], "has_more": False, "next_cursor": None}
        )

    sockudo = make_client(handler)
    list_result = sockudo.list_device_registrations(
        PushCursorParams(limit=10, cursor="c1")
    )
    publish_result = sockudo.publish_push(
        {
            "recipients": [{"type": "channel", "channel": "orders"}],
            "payload": {"title": "Order", "body": "Updated"},
            "providerOverrides": [{"provider": "fcm", "payload": {"android": {}}}],
        }
    )

    assert list_result.ok
    assert publish_result.ok
    assert seen[0][0] == "GET"
    assert seen[0][1] == "/apps/app-id/push/deviceRegistrations"
    assert seen[0][2]["limit"] == "10"
    assert seen[0][2]["cursor"] == "c1"
    assert seen[0][4]["x-sockudo-push-capability"] == "push-admin"
    assert seen[1][0] == "POST"
    assert seen[1][1] == "/apps/app-id/push/publish"
    assert seen[1][3]["sync"] is False
    assert seen[1][4]["x-sockudo-push-capability"] == "push-admin"
    sockudo.close()


def test_push_subscribe_helpers_pass_device_identity_token():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["query"] = dict(request.url.params)
        seen["headers"] = dict(request.headers)
        return httpx.Response(
            200, json={"items": [], "has_more": False, "next_cursor": None}
        )

    sockudo = make_client(handler)
    result = sockudo.list_channel_push_subscriptions(
        PushSubscriptionParams(device_id="device-1", limit=5),
        device_identity_token="identity",
    )

    assert result.ok
    assert seen["path"] == "/apps/app-id/push/channelSubscriptions"
    assert seen["query"]["deviceId"] == "device-1"
    assert seen["query"]["limit"] == "5"
    assert seen["headers"]["x-sockudo-push-capability"] == "push-subscribe"
    assert seen["headers"]["x-sockudo-device-identity-token"] == "identity"
    sockudo.close()


def test_authenticate_and_webhook_validation():
    sockudo = Sockudo("app-id", "key", "secret", options=SockudoOptions())
    auth = json.loads(
        sockudo.authenticate(
            "123.456", "presence-room", PresenceUser("u1", {"name": "Ada"})
        )
    )

    assert auth["auth"].startswith("key:")
    assert json.loads(auth["channel_data"]) == {
        "user_id": "u1",
        "user_info": {"name": "Ada"},
    }

    body = '{"time_ms":1,"events":[{"name":"channel_occupied","channel":"orders"}]}'
    assert (
        sockudo.validate_webhook_signature("key", sign(body, "secret"), body)
        is Validity.VALID
    )
    assert (
        sockudo.validate_webhook_signature("wrong", sign(body, "secret"), body)
        is Validity.SIGNED_WITH_WRONG_KEY
    )
    webhook = sockudo.parse_webhook("key", sign(body, "secret"), body)
    assert webhook.events[0].name == "channel_occupied"
    sockudo.close()


def test_encrypted_channel_auth_includes_shared_secret():
    sockudo = Sockudo(
        "1234",
        "f00d",
        "tofu",
        encryption_master_key_base64="zyrm8pvV2C9fJcBfhyXzvxbJVN/H7QLmbe0xJi1GhPU=",
        options=SockudoOptions(),
    )
    auth = json.loads(
        sockudo.authenticate("123.456", "private-encrypted-bla", PresenceUser("foo"))
    )

    assert auth == {
        "auth": "f00d:0a1e5e682436bc35f8abd6b1b22df7bcee7e436ec9801f15fb2e61500f39ba19",
        "channel_data": '{"user_id":"foo","user_info":{}}',
        "shared_secret": "nlr49ISQHz91yS3cy/yWmW8wFMNeTnNL5tNHnbPJcLQ=",
    }
    sockudo.close()


def test_encrypted_channel_payloads_are_secretbox_encrypted():
    seen = {}

    def handler(request):
        seen["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={})

    sockudo = Sockudo(
        "app-id",
        "key",
        "secret",
        encryption_master_key_base64=TEST_MASTER_KEY,
        options=SockudoOptions(max_retries=1),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    result = sockudo.trigger("private-encrypted-room", "event", "Hello!")

    encrypted = json.loads(seen["body"]["data"])
    shared_secret = hashlib.sha256(
        b"private-encrypted-room" + b"01234567890123456789012345678901"
    ).digest()
    plaintext = SecretBox(shared_secret).decrypt(
        base64.b64decode(encrypted["ciphertext"]),
        base64.b64decode(encrypted["nonce"]),
    )
    assert result.ok
    assert seen["body"]["channels"] == ["private-encrypted-room"]
    assert json.loads(plaintext.decode("utf-8")) == "Hello!"
    sockudo.close()


def test_batch_encrypts_private_encrypted_events():
    seen = {}

    def handler(request):
        seen["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={})

    sockudo = Sockudo(
        "app-id",
        "key",
        "secret",
        encryption_master_key_base64=TEST_MASTER_KEY,
        options=SockudoOptions(max_retries=1),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    result = sockudo.trigger_batch(
        [
            Event("orders", "plain", "test"),
            Event("private-encrypted-orders", "secret", {"ok": True}),
        ]
    )

    encrypted = json.loads(seen["body"]["batch"][1]["data"])
    shared_secret = hashlib.sha256(
        b"private-encrypted-orders" + b"01234567890123456789012345678901"
    ).digest()
    plaintext = SecretBox(shared_secret).decrypt(
        base64.b64decode(encrypted["ciphertext"]),
        base64.b64decode(encrypted["nonce"]),
    )
    assert result.ok
    assert seen["body"]["batch"][0]["data"] == "test"
    assert json.loads(plaintext.decode("utf-8")) == {"ok": True}
    sockudo.close()


def test_encrypted_webhook_events_are_decrypted():
    sockudo = Sockudo(
        "app-id",
        "key",
        "secret",
        encryption_master_key_base64=TEST_MASTER_KEY,
        options=SockudoOptions(),
    )
    encrypted = json.loads(
        sockudo._encrypt_payload("private-encrypted-room", {"secret": "value"})
    )
    body = json.dumps(
        {
            "time_ms": 1,
            "events": [
                {
                    "name": "event",
                    "channel": "private-encrypted-room",
                    "data": json.dumps(encrypted, separators=(",", ":")),
                }
            ],
        },
        separators=(",", ":"),
    )

    webhook = sockudo.parse_webhook("key", sign(body, "secret"), body)

    assert webhook.events[0].data == '{"secret":"value"}'
    sockudo.close()


def test_encrypted_channel_guardrails():
    sockudo = Sockudo(
        "app-id",
        "key",
        "secret",
        encryption_master_key_base64=TEST_MASTER_KEY,
        options=SockudoOptions(),
    )
    with pytest.raises(SockudoError):
        sockudo.trigger(["private-encrypted-a", "public-a"], "event", {})
    sockudo.close()


def test_send_to_user_uses_user_channel():
    seen = {}

    def handler(request):
        seen["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={})

    sockudo = make_client(handler)
    result = sockudo.send_to_user("user-123", "notice", {"ok": True})

    assert result.ok
    assert seen["body"]["channel"] == "#server-to-user-user-123"
    sockudo.close()


def test_config_and_sockudo_http_alias_are_supported():
    from sockudo_http import Sockudo as AliasSockudo

    sockudo = AliasSockudo(
        Config(
            app_id="app-id",
            key="key",
            secret="secret",
            host="example.com",
            port=443,
            use_tls=True,
        )
    )

    assert sockudo.base_url == "https://example.com:443"
    sockudo.close()


def test_result_status_mapping_and_retry():
    calls = {"count": 0}

    def handler(_request):
        calls["count"] += 1
        if calls["count"] == 1:
            return httpx.Response(503, text="try later")
        return httpx.Response(200, text="{}")

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)
    sockudo = Sockudo(
        "app-id",
        "key",
        "secret",
        options=SockudoOptions(max_retries=2, retry_base_delay=0),
        client=http_client,
    )
    result = sockudo.list_channels()

    assert result.status is Status.SUCCESS
    assert calls["count"] == 2
    sockudo.close()


@pytest.mark.asyncio
async def test_async_client_trigger():
    seen = {}

    async def handler(request):
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport)
    sockudo = AsyncSockudo(
        "app-id",
        "key",
        "secret",
        options=SockudoOptions(max_retries=1),
        client=http_client,
    )
    result = await sockudo.trigger("orders", "created", {"id": 1})

    assert result.ok
    assert seen["path"] == "/apps/app-id/events"
    assert seen["body"]["channel"] == "orders"
    await sockudo.close()


@pytest.mark.asyncio
async def test_async_operator_controls_match_sync_surface():
    paths = []

    async def handler(request):
        paths.append((request.method, request.url.path))
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport)
    sockudo = AsyncSockudo(
        "app-id",
        "key",
        "secret",
        options=SockudoOptions(max_retries=1),
        client=http_client,
    )

    await sockudo.get_channel_history_state("orders")
    await sockudo.reset_channel_history("orders", "repair")
    await sockudo.purge_channel_history(
        "orders", "before_serial", "trim", before_serial=10
    )
    await sockudo.get_channel_presence_history_state("presence-orders")
    await sockudo.reset_channel_presence_history("presence-orders", "repair")

    assert paths == [
        ("GET", "/apps/app-id/channels/orders/history/state"),
        ("POST", "/apps/app-id/channels/orders/history/reset"),
        ("POST", "/apps/app-id/channels/orders/history/purge"),
        ("GET", "/apps/app-id/channels/presence-orders/presence/history/state"),
        ("POST", "/apps/app-id/channels/presence-orders/presence/history/reset"),
    ]
    await sockudo.close()


def test_force_reconnect_user_posts_to_correct_path():
    seen = {}

    def handler(request):
        seen["method"] = request.method
        seen["path"] = request.url.path
        return httpx.Response(200, json={})

    sockudo = make_client(handler)
    result = sockudo.force_reconnect_user("user-123")

    assert result.ok
    assert seen["method"] == "POST"
    assert seen["path"] == "/apps/app-id/users/user-123/force_reconnect"
    sockudo.close()


def test_force_reconnect_user_url_encodes_user_id():
    seen = {}

    def handler(request):
        # raw_path preserves percent-encoding; strip the query string
        seen["raw_path"] = request.url.raw_path.decode().split("?")[0]
        return httpx.Response(200, json={})

    sockudo = make_client(handler)
    sockudo.force_reconnect_user("user/with spaces")

    assert seen["raw_path"] == "/apps/app-id/users/user%2Fwith%20spaces/force_reconnect"
    sockudo.close()


@pytest.mark.asyncio
async def test_async_force_reconnect_user_posts_to_correct_path():
    seen = {}

    async def handler(request):
        seen["method"] = request.method
        seen["path"] = request.url.path
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(transport=transport)
    sockudo = AsyncSockudo(
        "app-id",
        "key",
        "secret",
        options=SockudoOptions(max_retries=1),
        client=http_client,
    )
    result = await sockudo.force_reconnect_user("user-123")

    assert result.ok
    assert seen["method"] == "POST"
    assert seen["path"] == "/apps/app-id/users/user-123/force_reconnect"
    await sockudo.close()
