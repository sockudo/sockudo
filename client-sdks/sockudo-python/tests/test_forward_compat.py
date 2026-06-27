import json
from pathlib import Path

import msgpack
import pytest

from sockudo_python.client import (
    PresenceChannel,
    ProtocolCodec,
    SockudoClient,
    SockudoOptions,
    SockudoWireFormat,
)


FIXTURES = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "ai-conformance"
    / "fixtures"
    / "forward-compat"
)


def fixture_raw(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_replays_forward_compat_fixtures_without_corrupting_channel_state() -> None:
    client = SockudoClient(
        "app-key",
        SockudoOptions(
            cluster="local",
            force_tls=False,
            connection_recovery=True,
        ),
    )
    channel = client.subscribe("private-ai-forward")
    known: list[object] = []
    observed: list[tuple[str, object]] = []
    errors: list[object] = []

    channel.bind("app-known", lambda data, _meta: known.append(data))
    channel.bind_global(lambda event, data: observed.append((event, data)))
    client.bind("error", lambda data, _meta: errors.append(data))

    await client._handle_raw_message(
        json.dumps(
            {
                "event": "app-known",
                "channel": "private-ai-forward",
                "data": {"before": True},
            }
        )
    )
    before_state = (
        channel.is_subscribed,
        channel.subscription_pending,
        channel.subscription_cancelled,
    )

    for name in (
        "future-v2-frame.json",
        "future-versioned-action.json",
        "future-webhook-events.json",
        "unknown-ai-extras.json",
    ):
        await client._handle_raw_message(fixture_raw(name))

    await client._handle_raw_message(
        json.dumps(
            {
                "event": "app-known",
                "channel": "private-ai-forward",
                "data": {"after": True},
            }
        )
    )
    client._cancel_timers()

    assert known == [{"before": True}, {"after": True}]
    assert (
        channel.is_subscribed,
        channel.subscription_pending,
        channel.subscription_cancelled,
    ) == before_state
    assert errors == []
    assert client._channel_positions["private-ai-forward"].serial == 9007199254740993
    assert (
        "sockudo:future_event",
        {"known": True, "futureObject": {"nested": "ignored"}},
    ) in observed
    assert ("sockudo:message.future", "opaque") in observed
    assert ("ai-output", "content") in observed


def test_json_forward_compat_fixture_preserves_ai_and_raw_extras() -> None:
    decoded = ProtocolCodec.decode_event(
        fixture_raw("unknown-ai-extras.json"), SockudoWireFormat.JSON
    )

    assert decoded.extras is not None
    assert decoded.extras.ai == {
        "transport": {
            "turn-id": "turn-1",
            "status": "streaming",
        },
        "codec": {
            "provider-future-key": "opaque",
            "x-custom": "opaque",
        },
    }
    assert decoded.extras["futureExtrasField"] is True
    assert decoded.extras.raw["futureExtrasField"] is True


def test_messagepack_decodes_uint64_serials_and_unknown_extras() -> None:
    payload = msgpack.packb(
        [
            "sockudo:test",
            "private-ai-forward",
            ["string", "content"],
            None,
            None,
            None,
            None,
            None,
            None,
            "stream-1",
            9007199254740993,
            None,
            {
                "headers": {
                    "sockudo_history_serial": ["number", 9007199254740994],
                },
                "ai": {
                    "transport": {
                        "turn-id": "turn-1",
                        "status": "future-status",
                    },
                    "codec": {
                        "provider-future-key": "opaque",
                    },
                },
                "futureExtrasField": True,
            },
        ],
        use_bin_type=True,
    )

    decoded = ProtocolCodec.decode_event(payload, SockudoWireFormat.MESSAGEPACK)

    assert decoded.stream_id == "stream-1"
    assert decoded.serial == 9007199254740993
    assert decoded.extras is not None
    assert decoded.extras.headers == {
        "sockudo_history_serial": 9007199254740994,
    }
    assert decoded.extras.ai == {
        "transport": {
            "turn-id": "turn-1",
            "status": "future-status",
        },
        "codec": {
            "provider-future-key": "opaque",
        },
    }
    assert decoded.extras.raw["futureExtrasField"] is True


def test_protobuf_decodes_uint64_serials_and_ai_extras() -> None:
    payload = ProtocolCodec.encode_envelope(
        {
            "event": "sockudo:test",
            "channel": "private-ai-forward",
            "data": "content",
            "stream_id": "stream-1",
            "serial": 9007199254740993,
            "__delta_seq": 9007199254740994,
            "extras": {
                "headers": {
                    "sockudo_history_serial": 9007199254740994,
                },
                "ai": {
                    "transport": {
                        "turn-id": "turn-1",
                        "status": "future-status",
                    },
                    "codec": {
                        "provider-future-key": "opaque",
                    },
                },
            },
        },
        SockudoWireFormat.PROTOBUF,
    )

    decoded = ProtocolCodec.decode_event(payload, SockudoWireFormat.PROTOBUF)

    assert decoded.stream_id == "stream-1"
    assert decoded.serial == 9007199254740993
    assert decoded.sequence == 9007199254740994
    assert decoded.extras is not None
    assert decoded.extras.headers == {
        "sockudo_history_serial": 9007199254740994.0,
    }
    assert decoded.extras.ai == {
        "transport": {
            "turn-id": "turn-1",
            "status": "future-status",
        },
        "codec": {
            "provider-future-key": "opaque",
        },
    }


@pytest.mark.asyncio
async def test_unknown_presence_events_and_malformed_members_do_not_corrupt_map() -> None:
    client = SockudoClient("app-key", SockudoOptions(cluster="local", force_tls=False))
    channel = client.subscribe("presence-ai-forward")
    assert isinstance(channel, PresenceChannel)
    observed: list[tuple[str, object]] = []
    errors: list[object] = []

    channel.bind_global(lambda event, data: observed.append((event, data)))
    client.bind("error", lambda data, _meta: errors.append(data))
    channel.members.remember_my_id("user-1")
    channel.members.apply_subscription_data(
        {
            "presence": {
                "hash": {"user-1": {"role": "reader"}},
                "count": 1,
            }
        }
    )

    await client._handle_raw_message(
        json.dumps(
            {
                "event": "sockudo_internal:presence_update",
                "channel": "presence-ai-forward",
                "data": {
                    "user_id": "user-1",
                    "user_info": {"role": "writer"},
                },
            }
        )
    )
    await client._handle_raw_message(
        json.dumps(
            {
                "event": "sockudo_internal:member_added",
                "channel": "presence-ai-forward",
                "data": {"user_info": {"malformed": True}},
            }
        )
    )
    await client._handle_raw_message(
        json.dumps(
            {
                "event": "sockudo_internal:member_removed",
                "channel": "presence-ai-forward",
                "data": {"user_id": {"malformed": True}},
            }
        )
    )
    channel.members.apply_subscription_data(
        {
            "presence": {
                "hash": ["malformed"],
                "count": "unknown",
            }
        }
    )
    client._cancel_timers()

    assert errors == []
    assert (
        "sockudo_internal:presence_update",
        {
            "user_id": "user-1",
            "user_info": {"role": "writer"},
        },
    ) in observed
    assert channel.members.count == 1
    assert channel.members.member("user-1") is not None
    assert channel.members.member("user-1").info == {"role": "writer"}
    assert channel.members.member("undefined") is None


def test_serial_coercion_accepts_integer_strings_and_rejects_unsafe_floats() -> None:
    string_serial = ProtocolCodec.decode_event(
        json.dumps(
            {
                "event": "sockudo:test",
                "channel": "private-ai-forward",
                "serial": "9007199254740993",
            }
        ),
        SockudoWireFormat.JSON,
    )
    unsafe_float_serial = ProtocolCodec.decode_event(
        json.dumps(
            {
                "event": "sockudo:test",
                "channel": "private-ai-forward",
                "serial": 9007199254740993.0,
            }
        ),
        SockudoWireFormat.JSON,
    )

    assert string_serial.serial == 9007199254740993
    assert unsafe_float_serial.serial is None
