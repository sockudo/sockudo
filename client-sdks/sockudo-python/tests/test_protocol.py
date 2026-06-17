import vcdiff_decoder

from sockudo_python.client import (
    FossilDelta,
    ProtocolCodec,
    SockudoClient,
    SockudoOptions,
    SockudoTransport,
    SockudoWireFormat,
)


def test_encodes_websocket_url_with_v2_format_query() -> None:
    client = SockudoClient(
        "app-key",
        SockudoOptions(
            cluster="local",
            force_tls=False,
            enabled_transports=[SockudoTransport.WS],
            ws_host="ws.example.com",
            ws_port=6001,
            wss_port=6002,
            wire_format=SockudoWireFormat.MESSAGEPACK,
        ),
    )

    url = client._socket_url(SockudoTransport.WS)

    assert "protocol=2" in url
    assert "format=messagepack" in url


def test_round_trips_messagepack() -> None:
    payload = ProtocolCodec.encode_envelope(
        {
            "event": "sockudo:test",
            "channel": "chat:room-1",
            "data": {"hello": "world", "count": 3},
            "stream_id": "stream-1",
            "serial": 7,
            "__delta_seq": 7,
            "__conflation_key": "room",
        },
        SockudoWireFormat.MESSAGEPACK,
    )

    decoded = ProtocolCodec.decode_event(payload, SockudoWireFormat.MESSAGEPACK)

    assert decoded.event == "sockudo:test"
    assert decoded.channel == "chat:room-1"
    assert decoded.data == {"hello": "world", "count": 3}
    assert decoded.stream_id == "stream-1"
    assert decoded.serial == 7
    assert decoded.sequence == 7
    assert decoded.conflation_key == "room"


def test_round_trips_protobuf() -> None:
    payload = ProtocolCodec.encode_envelope(
        {
            "event": "sockudo:test",
            "channel": "chat:room-1",
            "data": {"hello": "world"},
            "stream_id": "stream-2",
            "serial": 9,
            "__delta_seq": 11,
            "__conflation_key": "btc",
            "extras": {
                "headers": {"region": "eu", "ttl": 5, "replay": True},
                "echo": False,
            },
        },
        SockudoWireFormat.PROTOBUF,
    )

    decoded = ProtocolCodec.decode_event(payload, SockudoWireFormat.PROTOBUF)

    assert decoded.event == "sockudo:test"
    assert decoded.channel == "chat:room-1"
    assert decoded.data == {"hello": "world"}
    assert decoded.stream_id == "stream-2"
    assert decoded.serial == 9
    assert decoded.sequence == 11
    assert decoded.conflation_key == "btc"
    assert decoded.extras is not None
    assert decoded.extras.headers["region"] == "eu"
    assert decoded.extras.headers["ttl"] == 5.0
    assert decoded.extras.headers["replay"] is True
    assert decoded.extras.echo is False


def test_applies_insert_only_fossil_delta() -> None:
    assert FossilDelta.apply(b"", b"5\n5:hello3NPMmh;") == b"hello"


def test_decodes_xdelta3_delta() -> None:
    original = b'{"data":{"price":100,"volume":5}}'
    updated = b'{"data":{"price":101,"volume":7}}'
    delta = b'\xd6\xc3\xc4\x00\x00\x01!\x00(!\x00!\x02\x00{"data":{"price":101,"volume":7}}\x01!'

    assert vcdiff_decoder.decode(original, delta) == updated
