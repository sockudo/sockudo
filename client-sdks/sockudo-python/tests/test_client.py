import asyncio
import base64
import json
from typing import Optional

import httpx
import pytest

from sockudo_python.client import (
    AppendMode,
    ChannelHistoryOptions,
    ChannelHistoryPage,
    ChannelHistoryParams,
    ConnectionState,
    DeltaOptions,
    InvalidOptions,
    PresenceHistoryOptions,
    PresenceHistoryPage,
    PresenceHistoryParams,
    PresenceSnapshotParams,
    ProtocolPrefix,
    RecoveryPosition,
    SockudoClient,
    SockudoOptions,
    SockudoTransport,
    SubscriptionOptions,
    SubscriptionRewind,
    TokenAuthData,
    VersionedMessageAck,
    VersionedMessageOptions,
)


def _unsigned_jwt(claims: dict[str, object]) -> str:
    def encode(payload: dict[str, object]) -> str:
        raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    return f"{encode({'alg': 'none', 'typ': 'JWT'})}.{encode(claims)}."


def test_socket_url_includes_append_mode_for_v2() -> None:
    client = SockudoClient(
        "app-key",
        SockudoOptions(
            cluster="local",
            force_tls=False,
            append_mode=AppendMode.FULL,
        ),
    )

    assert "append_mode=full" in client._socket_url(SockudoTransport.WS)


def test_socket_url_includes_static_token_for_v2() -> None:
    client = SockudoClient(
        "app-key",
        SockudoOptions(cluster="local", force_tls=False, token="capability-token"),
    )

    assert "token=capability-token" in client._socket_url(SockudoTransport.WS)


def test_auth_refresh_delay_uses_jwt_iat_exp_at_80_percent() -> None:
    token = _unsigned_jwt({"iat": 1000, "exp": 1600})
    client = SockudoClient(
        "app-key",
        SockudoOptions(cluster="local", force_tls=False, token=token),
    )

    assert client._auth_token_issued_at == 1000
    assert client._auth_token_expires_at == 1600
    assert client._auth_refresh_delay(now=1000) == pytest.approx(480)


def test_auth_refresh_delay_uses_remaining_lifetime_without_iat() -> None:
    token = _unsigned_jwt({"exp": 1600})
    client = SockudoClient(
        "app-key",
        SockudoOptions(cluster="local", force_tls=False, token=token),
    )

    assert client._auth_token_issued_at is None
    assert client._auth_token_expires_at == 1600
    assert client._auth_refresh_delay(now=1000) == pytest.approx(480)


def test_auth_refresh_delay_uses_token_result_iat_and_expires_in() -> None:
    client = SockudoClient("app-key", SockudoOptions(cluster="local", force_tls=False))
    token, expires_at, issued_at = client._normalize_auth_token(
        TokenAuthData(token="opaque", iat=100, expires_in=1000)
    )
    client._auth_token = token
    client._auth_token_expires_at = expires_at
    client._auth_token_issued_at = issued_at

    assert issued_at == 100
    assert expires_at == 1100
    assert client._auth_refresh_delay(now=100) == pytest.approx(800)


def test_auth_refresh_delay_uses_exp_from_now_without_iat() -> None:
    client = SockudoClient(
        "app-key",
        SockudoOptions(
            cluster="local",
            force_tls=False,
            auth_refresh_leeway_seconds=1,
        ),
    )
    token, expires_at, issued_at = client._normalize_auth_token(
        TokenAuthData(token="opaque", exp=1100)
    )
    client._auth_token = token
    client._auth_token_expires_at = expires_at
    client._auth_token_issued_at = issued_at

    assert issued_at is None
    assert expires_at == 1100
    assert client._auth_refresh_delay(now=100) == pytest.approx(800)


def test_opaque_token_without_exp_does_not_schedule_refresh() -> None:
    client = SockudoClient(
        "app-key",
        SockudoOptions(cluster="local", force_tls=False, token="opaque"),
    )
    client._update_state(ConnectionState.CONNECTED)

    client._schedule_auth_refresh()

    assert client._auth_refresh_delay(now=100) is None
    assert client._auth_refresh_task is None


def test_socket_url_includes_append_rollup_window_for_v2_only() -> None:
    v2_client = SockudoClient(
        "app-key",
        SockudoOptions(
            cluster="local",
            force_tls=False,
            append_rollup_window=100,
        ),
    )
    v1_client = SockudoClient(
        "app-key",
        SockudoOptions(
            cluster="local",
            force_tls=False,
            protocol_version=1,
            append_rollup_window=100,
        ),
    )

    assert "append_rollup_window=100" in v2_client._socket_url(SockudoTransport.WS)
    assert "append_rollup_window" not in v1_client._socket_url(SockudoTransport.WS)


def test_rejects_invalid_append_rollup_window() -> None:
    with pytest.raises(InvalidOptions):
        SockudoClient(
            "app-key",
            SockudoOptions(
                cluster="local",
                force_tls=False,
                append_rollup_window=10,
            ),
        )


def test_subscribe_tracks_event_filters() -> None:
    client = SockudoClient("app-key", SockudoOptions(cluster="local", force_tls=False))

    channel = client.subscribe(
        "orders",
        SubscriptionOptions(
            events=["order.created", "order.cancelled"],
            expression="data.total >= `100`",
        ),
    )

    assert channel.events_filter == ["order.created", "order.cancelled"]
    assert channel.expression_filter == "data.total >= `100`"


@pytest.mark.asyncio
async def test_resume_success_applies_authoritative_channel_position() -> None:
    client = SockudoClient(
        "app-key",
        SockudoOptions(
            cluster="local",
            force_tls=False,
            connection_recovery=True,
        ),
    )
    payload = json.dumps(
        {
            "event": "sockudo:resume_success",
            "data": json.dumps(
                {
                    "recovered": [
                        {
                            "channel": "orders",
                            "source": "durable",
                            "replayed": 0,
                            "position": {
                                "stream_id": "stream-2",
                                "serial": 42,
                                "last_message_id": "message-42",
                            },
                        }
                    ],
                    "failed": [],
                }
            ),
        }
    )

    await client._handle_raw_message(payload)

    assert client._channel_positions["orders"] == RecoveryPosition(
        serial=42,
        stream_id="stream-2",
        last_message_id="message-42",
    )
    client._cancel_activity_timer()


def test_reset_delta_stats_and_clear_channel_state() -> None:
    client = SockudoClient(
        "app-key",
        SockudoOptions(
            cluster="local",
            force_tls=False,
            delta_compression=DeltaOptions(enabled=True),
        ),
    )

    assert client._delta_manager is not None
    client._delta_manager.handle_full_message("orders", '{"data":{"id":1}}', 1, None)
    assert client.get_delta_stats() is not None
    assert client.get_delta_stats().full_messages == 1

    client.reset_delta_stats()

    assert client.get_delta_stats().full_messages == 0

    client._delta_manager.clear_channel_state("orders")
    assert "orders" not in client._delta_manager._channel_states


def test_signin_forwards_to_user_facade() -> None:
    client = SockudoClient("app-key", SockudoOptions(cluster="local", force_tls=False))
    called = False

    async def fake_sign_in() -> None:
        nonlocal called
        called = True

    client.user.sign_in = fake_sign_in  # type: ignore[method-assign]
    asyncio.run(client.signin())

    assert called is True


@pytest.mark.asyncio
async def test_auth_callback_initial_token_and_refresh_frames() -> None:
    calls = 0

    async def auth_callback() -> TokenAuthData:
        nonlocal calls
        calls += 1
        return TokenAuthData(token=f"token-{calls}", expires_in=60)

    client = SockudoClient(
        "app-key",
        SockudoOptions(
            cluster="local",
            force_tls=False,
            auth_callback=auth_callback,
        ),
    )
    sent: list[tuple[str, object, Optional[str]]] = []

    async def fake_send_event(
        name: str, data: object, channel_name: Optional[str]
    ) -> bool:
        sent.append((name, data, channel_name))
        return True

    client.send_event = fake_send_event  # type: ignore[method-assign]

    await client._prepare_connection_auth_token()
    assert "token=token-1" in client._socket_url(SockudoTransport.WS)

    client._update_state(ConnectionState.CONNECTED)
    await client._handle_raw_message(
        json.dumps(
            {
                "event": ProtocolPrefix(2).event("token_expired"),
                "data": {"code": 40160},
            }
        )
    )
    await client._handle_raw_message(
        json.dumps(
            {
                "event": ProtocolPrefix(2).event("error"),
                "data": {"code": 40142},
            }
        )
    )
    client._cancel_timers()

    assert sent == [
        ("sockudo:auth", {"token": "token-2"}, None),
        ("sockudo:auth", {"token": "token-3"}, None),
    ]
    assert client._auth_refresh_task is None


def test_connection_recovery_uses_channel_positions_payload() -> None:
    client = SockudoClient(
        "app-key",
        SockudoOptions(cluster="local", force_tls=False, connection_recovery=True),
    )
    sent: list[tuple[str, object, str | None]] = []

    async def fake_send_event(name: str, data: object, channel: Optional[str]) -> bool:
        sent.append((name, data, channel))
        return True

    async def fake_handle_connected() -> None:
        return None

    client.send_event = fake_send_event  # type: ignore[method-assign]
    client.user.handle_connected = fake_handle_connected  # type: ignore[method-assign]

    client._channel_positions["chat"] = RecoveryPosition(
        serial=42,
        stream_id="stream-1",
        last_message_id="msg-42",
    )

    payload = json.dumps(
        {
            "event": ProtocolPrefix(2).event("connection_established"),
            "data": {"socket_id": "123.456"},
        }
    )

    asyncio.run(client._handle_raw_message(payload))

    resume_event = next(item for item in sent if item[0] == "sockudo:resume")
    assert resume_event[1] == {
        "channel_positions": {
            "chat": {
                "serial": 42,
                "stream_id": "stream-1",
                "last_message_id": "msg-42",
            }
        }
    }


def test_resume_failed_clears_channel_position() -> None:
    client = SockudoClient("app-key", SockudoOptions(cluster="local", force_tls=False))
    client._channel_positions["chat"] = RecoveryPosition(
        serial=7,
        stream_id="stream-1",
        last_message_id="msg-7",
    )

    async def run() -> None:
        await client._handle_raw_message(
            json.dumps(
                {
                    "event": ProtocolPrefix(2).event("resume_failed"),
                    "channel": "chat",
                    "data": {"channel": "chat", "code": "stream_reset"},
                }
            )
        )

    asyncio.run(run())

    assert "chat" not in client._channel_positions


def test_subscribe_serializes_rewind_option() -> None:
    client = SockudoClient("app-key", SockudoOptions(cluster="local", force_tls=False))
    channel = client.subscribe(
        "history-room",
        SubscriptionOptions(rewind=SubscriptionRewind.seconds_back(30)),
    )
    sent: list[tuple[str, object, Optional[str]]] = []

    async def fake_send_event(
        name: str, data: object, channel_name: Optional[str]
    ) -> bool:
        sent.append((name, data, channel_name))
        return True

    client.send_event = fake_send_event  # type: ignore[method-assign]
    client._update_state(ConnectionState.CONNECTED)

    asyncio.run(channel.subscribe())

    subscribe = next(item for item in sent if item[0] == "sockudo:subscribe")
    assert subscribe[1]["rewind"] == {"seconds": 30}


@pytest.mark.asyncio
async def test_subscription_succeeded_exposes_attach_serial() -> None:
    client = SockudoClient("app-key", SockudoOptions(cluster="local", force_tls=False))
    channel = client.subscribe("history-room")

    await client._handle_raw_message(
        json.dumps(
            {
                "event": ProtocolPrefix(2).internal("subscription_succeeded"),
                "channel": "history-room",
                "data": {"attach_serial": 88},
            }
        )
    )
    client._cancel_timers()

    assert channel.attach_serial == 88


@pytest.mark.asyncio
async def test_channel_history_uses_until_attach_with_attach_serial() -> None:
    client = SockudoClient(
        "app-key",
        SockudoOptions(
            cluster="local",
            force_tls=False,
            channel_history=ChannelHistoryOptions(
                endpoint="https://example.test/channel-history",
            ),
        ),
    )
    channel = client.subscribe("history-room")
    channel.attach_serial = 42
    captured: list[tuple[str, ChannelHistoryParams, Optional[int]]] = []

    async def fake_fetch_channel_history(
        channel_name: str,
        params: ChannelHistoryParams,
        attach_serial: Optional[int],
    ) -> ChannelHistoryPage:
        captured.append((channel_name, params, attach_serial))
        return ChannelHistoryPage(
            items=[],
            direction="oldest_first",
            limit=10,
            has_more=False,
            next_cursor=None,
            attach_serial=attach_serial,
        )

    client.config.fetch_channel_history = fake_fetch_channel_history  # type: ignore[method-assign]

    page = await channel.history(ChannelHistoryParams(limit=10, until_attach=True))

    assert page.attach_serial == 42
    assert captured[0][0] == "history-room"
    assert captured[0][1].to_payload(captured[0][2]) == {
        "limit": 10,
        "until_attach": True,
        "attach_serial": 42,
    }


def test_presence_history_params_normalize_ably_aliases() -> None:
    params = PresenceHistoryParams(
        direction="newest_first",
        limit=50,
        start=1000,
        end=2000,
    )

    assert params.to_payload() == {
        "direction": "newest_first",
        "limit": 50,
        "start_time_ms": 1000,
        "end_time_ms": 2000,
    }


@pytest.mark.asyncio
async def test_presence_history_page_next_uses_next_cursor() -> None:
    client = SockudoClient(
        "app-key",
        SockudoOptions(
            cluster="local",
            force_tls=False,
            presence_history=PresenceHistoryOptions(
                endpoint="https://example.test/presence-history",
            ),
        ),
    )
    channel = client.subscribe("presence-lobby")
    history_channel = channel  # type: ignore[assignment]

    captured: list[PresenceHistoryParams] = []

    async def fake_fetch_presence_history(
        channel_name: str, params: PresenceHistoryParams
    ) -> PresenceHistoryPage:
        assert channel_name == "presence-lobby"
        captured.append(params)
        return PresenceHistoryPage(
            items=[],
            direction="newest_first",
            limit=50,
            has_more=False,
            next_cursor=None,
            bounds=client.config._decode_presence_history_bounds({}),
            continuity=client.config._decode_presence_history_continuity({}),
        )

    client.config.fetch_presence_history = fake_fetch_presence_history  # type: ignore[method-assign]

    page = await history_channel.history(
        PresenceHistoryParams(limit=50, direction="newest_first", cursor="cursor-1")
    )
    assert len(captured) == 1
    assert captured[0].cursor == "cursor-1"
    assert page.has_next() is False


@pytest.mark.asyncio
async def test_presence_update_sends_platform_event() -> None:
    client = SockudoClient("app-key", SockudoOptions(cluster="local", force_tls=False))
    channel = client.subscribe("presence-lobby")
    sent: list[tuple[str, object, Optional[str]]] = []

    async def fake_send_event(
        name: str, data: object, channel_name: Optional[str]
    ) -> bool:
        sent.append((name, data, channel_name))
        return True

    client.send_event = fake_send_event  # type: ignore[method-assign]

    assert await channel.update({"role": "writer"}) is True  # type: ignore[attr-defined]

    assert sent == [
        (
            "sockudo:presence_update",
            {"user_info": {"role": "writer"}},
            "presence-lobby",
        )
    ]


def test_presence_snapshot_params_normalize_ably_alias() -> None:
    params = PresenceSnapshotParams(at=3000, at_serial=7)

    assert params.to_payload() == {
        "at_time_ms": 3000,
        "at_serial": 7,
    }


@pytest.mark.asyncio
async def test_versioned_message_facade_exposes_all_proxy_actions() -> None:
    client = SockudoClient(
        "app-key",
        SockudoOptions(
            cluster="local",
            force_tls=False,
            versioned_messages=VersionedMessageOptions(
                endpoint="https://example.test/versioned-messages",
            ),
        ),
    )
    calls: list[tuple[str, str, dict[str, object], Optional[float]]] = []

    async def fake_perform_versioned_message_action(
        channel_name: str,
        action: str,
        params: dict[str, object],
        timeout: Optional[float] = None,
    ) -> VersionedMessageAck:
        calls.append((channel_name, action, params, timeout))
        return VersionedMessageAck(
            action=action,
            channel=channel_name,
            event=params.get("event") if isinstance(params.get("event"), str) else None,
            message_id=params.get("message_id")
            if isinstance(params.get("message_id"), str)
            else "msg-1",
            version_id=None,
            message_serial=1,
            version_serial=2,
            history_serial=3,
            delivery_serial=4,
        )

    client.config.perform_versioned_message_action = (  # type: ignore[method-assign]
        fake_perform_versioned_message_action
    )

    await client.versioned_messages.create("chat", "message.created", {"text": "hi"})
    await client.versioned_messages.append("chat", "msg-1", {"delta": "!"})
    await client.versioned_messages.update("chat", "msg-1", {"text": "hello"})
    await client.versioned_messages.delete("chat", "msg-1", reason="moderated")

    assert [call[1] for call in calls] == ["create", "append", "update", "delete"]
    assert calls[0] == (
        "chat",
        "create",
        {"event": "message.created", "data": {"text": "hi"}},
        None,
    )
    assert calls[3] == (
        "chat",
        "delete",
        {"message_id": "msg-1", "reason": "moderated"},
        None,
    )


@pytest.mark.asyncio
async def test_versioned_message_proxy_request_decodes_typed_ack() -> None:
    requests: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content.decode("utf-8")))
        return httpx.Response(
            200,
            json={
                "ack": {
                    "action": "create",
                    "channel": "chat",
                    "event": "message.created",
                    "message_id": "msg-1",
                    "version_id": "v1",
                    "message_serial": 11,
                    "version_serial": 12,
                    "history_serial": 13,
                    "delivery_serial": 14,
                }
            },
        )

    client = SockudoClient(
        "app-key",
        SockudoOptions(
            cluster="local",
            force_tls=False,
            versioned_messages=VersionedMessageOptions(
                endpoint="https://example.test/versioned-messages",
            ),
        ),
    )
    await client.config._http_client.aclose()
    client.config._http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler)
    )

    ack = await client.versioned_messages.create(
        "chat",
        "message.created",
        {"text": "hi"},
        extras={"ai": {"transport": {"turn-id": "turn-1"}}},
    )
    await client.config.close()

    assert requests == [
        {
            "channel": "chat",
            "params": {
                "event": "message.created",
                "data": {"text": "hi"},
                "extras": {"ai": {"transport": {"turn-id": "turn-1"}}},
            },
            "action": "create",
        }
    ]
    assert ack.message_id == "msg-1"
    assert ack.version_id == "v1"
    assert ack.message_serial == 11
    assert ack.version_serial == 12
    assert ack.history_serial == 13
    assert ack.delivery_serial == 14
