import asyncio
import json
from typing import Optional

import pytest

from sockudo_python.client import (
    ConnectionState,
    DeltaOptions,
    PresenceHistoryOptions,
    PresenceHistoryPage,
    PresenceHistoryParams,
    PresenceSnapshotParams,
    ProtocolPrefix,
    RecoveryPosition,
    SockudoClient,
    SockudoOptions,
    SubscriptionOptions,
    SubscriptionRewind,
)


def test_subscribe_tracks_event_filters() -> None:
    client = SockudoClient("app-key", SockudoOptions(cluster="local", force_tls=False))

    channel = client.subscribe(
        "orders",
        SubscriptionOptions(events=["order.created", "order.cancelled"]),
    )

    assert channel.events_filter == ["order.created", "order.cancelled"]


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


def test_connection_recovery_uses_channel_positions_payload() -> None:
    client = SockudoClient("app-key", SockudoOptions(cluster="local", force_tls=False, connection_recovery=True))
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

    payload = json.dumps({
        "event": ProtocolPrefix(2).event("connection_established"),
        "data": {"socket_id": "123.456"},
    })

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
        await client._handle_raw_message(json.dumps({
            "event": ProtocolPrefix(2).event("resume_failed"),
            "channel": "chat",
            "data": {"channel": "chat", "code": "stream_reset"},
        }))

    asyncio.run(run())

    assert "chat" not in client._channel_positions


def test_subscribe_serializes_rewind_option() -> None:
    client = SockudoClient("app-key", SockudoOptions(cluster="local", force_tls=False))
    channel = client.subscribe(
        "history-room",
        SubscriptionOptions(rewind=SubscriptionRewind.seconds_back(30)),
    )
    sent: list[tuple[str, object, Optional[str]]] = []

    async def fake_send_event(name: str, data: object, channel_name: Optional[str]) -> bool:
        sent.append((name, data, channel_name))
        return True

    client.send_event = fake_send_event  # type: ignore[method-assign]
    client._update_state(ConnectionState.CONNECTED)

    asyncio.run(channel.subscribe())

    subscribe = next(item for item in sent if item[0] == "sockudo:subscribe")
    assert subscribe[1]["rewind"] == {"seconds": 30}


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


def test_presence_snapshot_params_normalize_ably_alias() -> None:
    params = PresenceSnapshotParams(at=3000, at_serial=7)

    assert params.to_payload() == {
        "at_time_ms": 3000,
        "at_serial": 7,
    }
