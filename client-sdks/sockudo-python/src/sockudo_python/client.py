from __future__ import annotations

import asyncio
import base64
import binascii
import inspect
import json
import struct
import time
import urllib.parse
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

import httpx
import msgpack
import vcdiff_decoder
from nacl.secret import SecretBox
from nacl.utils import random as nacl_random
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed


class SockudoException(RuntimeError):
    pass


class InvalidAppKey(SockudoException):
    pass


class InvalidOptions(SockudoException):
    pass


class UnsupportedFeature(SockudoException):
    pass


class BadEventName(SockudoException):
    pass


class AuthFailure(SockudoException):
    def __init__(self, status_code: Optional[int], message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


class DeltaFailure(SockudoException):
    pass


_ALLOWED_APPEND_ROLLUP_WINDOWS = {0, 20, 40, 100, 500}
_TOKEN_REFRESH_CODES = {40142, 40160}


class ConnectionState(str, Enum):
    INITIALIZED = "initialized"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    UNAVAILABLE = "unavailable"
    FAILED = "failed"


class SockudoTransport(str, Enum):
    WS = "ws"
    WSS = "wss"


class SockudoWireFormat(str, Enum):
    JSON = "json"
    MESSAGEPACK = "messagepack"
    PROTOBUF = "protobuf"

    @property
    def is_binary(self) -> bool:
        return self is not SockudoWireFormat.JSON


class AppendMode(str, Enum):
    DELTA = "delta"
    FULL = "full"


class DeltaAlgorithm(str, Enum):
    FOSSIL = "fossil"
    XDELTA3 = "xdelta3"


@dataclass
class FilterNode:
    op: Optional[str] = None
    key: Optional[str] = None
    cmp: Optional[str] = None
    val: Optional[str] = None
    vals: Optional[List[str]] = None
    nodes: Optional[List["FilterNode"]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            key: value
            for key, value in {
                "op": self.op,
                "key": self.key,
                "cmp": self.cmp,
                "val": self.val,
                "vals": self.vals,
                "nodes": [node.to_dict() for node in self.nodes]
                if self.nodes
                else None,
            }.items()
            if value is not None
        }


class Filter:
    @staticmethod
    def eq(key: str, value: str) -> FilterNode:
        return FilterNode(key=key, cmp="eq", val=value)

    @staticmethod
    def neq(key: str, value: str) -> FilterNode:
        return FilterNode(key=key, cmp="neq", val=value)

    @staticmethod
    def inside(key: str, values: List[str]) -> FilterNode:
        return FilterNode(key=key, cmp="in", vals=values)

    @staticmethod
    def not_in(key: str, values: List[str]) -> FilterNode:
        return FilterNode(key=key, cmp="nin", vals=values)

    @staticmethod
    def exists(key: str) -> FilterNode:
        return FilterNode(key=key, cmp="ex")

    @staticmethod
    def not_exists(key: str) -> FilterNode:
        return FilterNode(key=key, cmp="nex")

    @staticmethod
    def starts_with(key: str, value: str) -> FilterNode:
        return FilterNode(key=key, cmp="sw", val=value)

    @staticmethod
    def ends_with(key: str, value: str) -> FilterNode:
        return FilterNode(key=key, cmp="ew", val=value)

    @staticmethod
    def contains(key: str, value: str) -> FilterNode:
        return FilterNode(key=key, cmp="ct", val=value)

    @staticmethod
    def gt(key: str, value: str) -> FilterNode:
        return FilterNode(key=key, cmp="gt", val=value)

    @staticmethod
    def gte(key: str, value: str) -> FilterNode:
        return FilterNode(key=key, cmp="gte", val=value)

    @staticmethod
    def lt(key: str, value: str) -> FilterNode:
        return FilterNode(key=key, cmp="lt", val=value)

    @staticmethod
    def lte(key: str, value: str) -> FilterNode:
        return FilterNode(key=key, cmp="lte", val=value)

    @staticmethod
    def and_(*nodes: FilterNode) -> FilterNode:
        return FilterNode(op="and", nodes=list(nodes))

    @staticmethod
    def or_(*nodes: FilterNode) -> FilterNode:
        return FilterNode(op="or", nodes=list(nodes))

    @staticmethod
    def not_(node: FilterNode) -> FilterNode:
        return FilterNode(op="not", nodes=[node])


def validate_filter(filter_node: FilterNode) -> Optional[str]:
    if filter_node.op:
        if filter_node.op not in {"and", "or", "not"}:
            return f"Invalid logical operator: {filter_node.op}"
        if filter_node.nodes is None:
            return f"Logical operation '{filter_node.op}' requires nodes array"
        if filter_node.op == "not" and len(filter_node.nodes) != 1:
            return f"NOT operation requires exactly one child node, got {len(filter_node.nodes)}"
        if filter_node.op in {"and", "or"} and not filter_node.nodes:
            return (
                f"{filter_node.op.upper()} operation requires at least one child node"
            )
        for index, child in enumerate(filter_node.nodes):
            error = validate_filter(child)
            if error is not None:
                return f"Child node {index}: {error}"
        return None

    if not filter_node.key:
        return "Leaf node requires a key"
    if not filter_node.cmp:
        return "Leaf node requires a comparison operator"
    if filter_node.cmp not in {
        "eq",
        "neq",
        "in",
        "nin",
        "ex",
        "nex",
        "sw",
        "ew",
        "ct",
        "gt",
        "gte",
        "lt",
        "lte",
    }:
        return f"Invalid comparison operator: {filter_node.cmp}"
    if filter_node.cmp in {"in", "nin"}:
        if not filter_node.vals:
            return f"{filter_node.cmp} operation requires non-empty vals array"
    elif filter_node.cmp not in {"ex", "nex"} and not filter_node.val:
        return f"{filter_node.cmp} operation requires a val"
    return None


@dataclass
class ChannelDeltaSettings:
    enabled: Optional[bool] = None
    algorithm: Optional[DeltaAlgorithm] = None

    def subscription_value(self) -> Any:
        if self.enabled is None and self.algorithm is not None:
            return self.algorithm.value
        if self.enabled is False and self.algorithm is None:
            return False
        if self.enabled is True and self.algorithm is None:
            return True
        payload: Dict[str, Any] = {}
        if self.enabled is not None:
            payload["enabled"] = self.enabled
        if self.algorithm is not None:
            payload["algorithm"] = self.algorithm.value
        return payload


@dataclass
class MessageExtras:
    headers: Optional[Dict[str, Any]] = None
    ephemeral: Optional[bool] = None
    idempotency_key: Optional[str] = None
    echo: Optional[bool] = None
    ai: Optional[Dict[str, Any]] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload = dict(self.raw)
        if self.headers is not None:
            payload["headers"] = self.headers
        if self.ephemeral is not None:
            payload["ephemeral"] = self.ephemeral
        if self.idempotency_key is not None:
            payload["idempotency_key"] = self.idempotency_key
        if self.echo is not None:
            payload["echo"] = self.echo
        if self.ai is not None:
            payload["ai"] = self.ai
        return payload

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)

    def __contains__(self, key: str) -> bool:
        return key in self.to_dict()

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]


@dataclass
class SubscriptionOptions:
    filter: Optional[FilterNode] = None
    delta: Optional[ChannelDeltaSettings] = None
    events: Optional[List[str]] = None
    rewind: Optional["SubscriptionRewind"] = None


@dataclass
class SubscriptionRewind:
    count: Optional[int] = None
    seconds: Optional[int] = None

    @classmethod
    def count_messages(cls, count: int) -> "SubscriptionRewind":
        return cls(count=count)

    @classmethod
    def seconds_back(cls, seconds: int) -> "SubscriptionRewind":
        return cls(seconds=seconds)

    def subscription_value(self) -> Any:
        if self.count is not None:
            return self.count
        if self.seconds is not None:
            return {"seconds": self.seconds}
        raise SockudoException("SubscriptionRewind requires count or seconds")


@dataclass
class DeltaStats:
    total_messages: int = 0
    delta_messages: int = 0
    full_messages: int = 0
    total_bytes_without_compression: int = 0
    total_bytes_with_compression: int = 0
    errors: int = 0

    @property
    def bandwidth_saved(self) -> int:
        return self.total_bytes_without_compression - self.total_bytes_with_compression

    @property
    def bandwidth_saved_percent(self) -> float:
        if self.total_bytes_without_compression == 0:
            return 0.0
        return self.bandwidth_saved / self.total_bytes_without_compression * 100.0


@dataclass
class DeltaOptions:
    enabled: Optional[bool] = None
    algorithms: List[DeltaAlgorithm] = field(
        default_factory=lambda: [DeltaAlgorithm.FOSSIL, DeltaAlgorithm.XDELTA3]
    )
    debug: bool = False
    on_stats: Optional[Callable[[DeltaStats], None]] = None
    on_error: Optional[Callable[[BaseException], None]] = None


@dataclass
class PresenceMember:
    id: str
    info: Any


AuthValue = Union[str, int, float, bool]
HeadersProvider = Callable[[], Dict[str, str]]
ParamsProvider = Callable[[], Dict[str, AuthValue]]
ChannelAuthHandler = Callable[
    ["ChannelAuthorizationRequest"], Awaitable["ChannelAuthorizationData"]
]
UserAuthHandler = Callable[
    ["UserAuthenticationRequest"], Awaitable["UserAuthenticationData"]
]
PresenceHistoryHeadersProvider = Callable[[], Dict[str, str]]
ProxyHeadersProvider = Callable[[], Dict[str, str]]


@dataclass
class ChannelAuthorizationData:
    auth: str
    channel_data: Optional[str] = None
    shared_secret: Optional[str] = None


@dataclass
class UserAuthenticationData:
    auth: str
    user_data: str


@dataclass
class TokenAuthData:
    token: str
    expires_at: Optional[float] = None
    expires_at_ms: Optional[int] = None
    expires_in: Optional[float] = None
    exp: Optional[float] = None
    iat: Optional[float] = None
    issued_at: Optional[float] = None
    issued_at_ms: Optional[int] = None


TokenAuthResult = Union[str, TokenAuthData]
TokenAuthCallback = Callable[[], Union[TokenAuthResult, Awaitable[TokenAuthResult]]]


@dataclass
class ChannelAuthorizationRequest:
    socket_id: str
    channel_name: str


@dataclass
class UserAuthenticationRequest:
    socket_id: str


@dataclass
class ChannelAuthorizationOptions:
    endpoint: str = "/sockudo/auth"
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, AuthValue] = field(default_factory=dict)
    headers_provider: Optional[HeadersProvider] = None
    params_provider: Optional[ParamsProvider] = None
    custom_handler: Optional[ChannelAuthHandler] = None


@dataclass
class UserAuthenticationOptions:
    endpoint: str = "/sockudo/user-auth"
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, AuthValue] = field(default_factory=dict)
    headers_provider: Optional[HeadersProvider] = None
    params_provider: Optional[ParamsProvider] = None
    custom_handler: Optional[UserAuthHandler] = None


@dataclass
class PresenceHistoryOptions:
    endpoint: str
    headers: Dict[str, str] = field(default_factory=dict)
    headers_provider: Optional[PresenceHistoryHeadersProvider] = None


@dataclass
class ChannelHistoryOptions:
    endpoint: str
    headers: Dict[str, str] = field(default_factory=dict)
    headers_provider: Optional[ProxyHeadersProvider] = None


@dataclass
class VersionedMessageOptions:
    endpoint: str
    headers: Dict[str, str] = field(default_factory=dict)
    headers_provider: Optional[ProxyHeadersProvider] = None
    timeout: float = 10.0


@dataclass
class SockudoOptions:
    cluster: str
    protocol_version: int = 2
    activity_timeout: float = 120.0
    force_tls: Optional[bool] = None
    enabled_transports: Optional[List[SockudoTransport]] = None
    disabled_transports: Optional[List[SockudoTransport]] = None
    ws_host: Optional[str] = None
    ws_port: int = 80
    wss_port: int = 443
    ws_path: str = ""
    http_host: Optional[str] = None
    http_port: int = 80
    https_port: int = 443
    http_path: str = "/sockudo"
    pong_timeout: float = 30.0
    unavailable_timeout: float = 10.0
    enable_stats: bool = False
    stats_host: str = "stats.sockudo.io"
    timeline_params: Dict[str, AuthValue] = field(default_factory=dict)
    channel_authorization: ChannelAuthorizationOptions = field(
        default_factory=ChannelAuthorizationOptions
    )
    user_authentication: UserAuthenticationOptions = field(
        default_factory=UserAuthenticationOptions
    )
    presence_history: Optional[PresenceHistoryOptions] = None
    delta_compression: Optional[DeltaOptions] = None
    message_deduplication: bool = True
    message_deduplication_capacity: int = 1000
    connection_recovery: bool = False
    echo_messages: bool = True
    wire_format: SockudoWireFormat = SockudoWireFormat.JSON
    append_mode: AppendMode = AppendMode.DELTA
    append_rollup_window: Optional[int] = None
    token: Optional[str] = None
    auth_callback: Optional[TokenAuthCallback] = None
    auth_refresh_leeway_seconds: float = 30.0
    channel_history: Optional[ChannelHistoryOptions] = None
    versioned_messages: Optional[VersionedMessageOptions] = None


@dataclass
class EventMetadata:
    user_id: Optional[str] = None


@dataclass
class PresenceHistoryParams:
    direction: Optional[str] = None
    limit: Optional[int] = None
    cursor: Optional[str] = None
    start_serial: Optional[int] = None
    end_serial: Optional[int] = None
    start_time_ms: Optional[int] = None
    end_time_ms: Optional[int] = None
    start: Optional[int] = None
    end: Optional[int] = None

    def to_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if self.direction is not None:
            payload["direction"] = self.direction
        if self.limit is not None:
            payload["limit"] = self.limit
        if self.cursor is not None:
            payload["cursor"] = self.cursor
        if self.start_serial is not None:
            payload["start_serial"] = self.start_serial
        if self.end_serial is not None:
            payload["end_serial"] = self.end_serial
        if self.start_time_ms is not None:
            payload["start_time_ms"] = self.start_time_ms
        elif self.start is not None:
            payload["start_time_ms"] = self.start
        if self.end_time_ms is not None:
            payload["end_time_ms"] = self.end_time_ms
        elif self.end is not None:
            payload["end_time_ms"] = self.end
        return payload


@dataclass
class PresenceHistoryBounds:
    start_serial: Optional[int]
    end_serial: Optional[int]
    start_time_ms: Optional[int]
    end_time_ms: Optional[int]


@dataclass
class PresenceHistoryContinuity:
    stream_id: Optional[str]
    oldest_available_serial: Optional[int]
    newest_available_serial: Optional[int]
    oldest_available_published_at_ms: Optional[int]
    newest_available_published_at_ms: Optional[int]
    retained_events: int
    retained_bytes: int
    degraded: bool
    complete: bool
    truncated_by_retention: bool


@dataclass
class PresenceHistoryItem:
    stream_id: str
    serial: int
    published_at_ms: int
    event: str
    cause: str
    user_id: str
    connection_id: Optional[str]
    dead_node_id: Optional[str]
    payload_size_bytes: int
    presence_event: Dict[str, Any]


@dataclass
class PresenceSnapshotParams:
    at_time_ms: Optional[int] = None
    at: Optional[int] = None
    at_serial: Optional[int] = None

    def to_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if self.at_time_ms is not None:
            payload["at_time_ms"] = self.at_time_ms
        elif self.at is not None:
            payload["at_time_ms"] = self.at
        if self.at_serial is not None:
            payload["at_serial"] = self.at_serial
        return payload


@dataclass
class PresenceSnapshotMember:
    user_id: str
    last_event: str
    last_event_serial: int
    last_event_at_ms: int


@dataclass
class PresenceSnapshot:
    channel: str
    members: List[PresenceSnapshotMember]
    member_count: int
    events_replayed: int
    snapshot_serial: Optional[int]
    snapshot_time_ms: Optional[int]
    continuity: PresenceHistoryContinuity


@dataclass
class ChannelHistoryParams:
    direction: Optional[str] = None
    limit: Optional[int] = None
    cursor: Optional[str] = None
    start_serial: Optional[int] = None
    end_serial: Optional[int] = None
    until_attach: bool = False
    attach_serial: Optional[int] = None

    def to_payload(self, default_attach_serial: Optional[int] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if self.direction is not None:
            payload["direction"] = self.direction
        if self.limit is not None:
            payload["limit"] = self.limit
        if self.cursor is not None:
            payload["cursor"] = self.cursor
        if self.start_serial is not None:
            payload["start_serial"] = self.start_serial
        if self.end_serial is not None:
            payload["end_serial"] = self.end_serial
        if self.until_attach:
            payload["until_attach"] = True
            attach_serial = (
                self.attach_serial
                if self.attach_serial is not None
                else default_attach_serial
            )
            if attach_serial is not None:
                payload["attach_serial"] = attach_serial
        elif self.attach_serial is not None:
            payload["attach_serial"] = self.attach_serial
        return payload


@dataclass
class ChannelHistoryPage:
    items: List[Dict[str, Any]]
    direction: str
    limit: int
    has_more: bool
    next_cursor: Optional[str]
    attach_serial: Optional[int]
    _fetch_next: Optional[Callable[[str], Awaitable["ChannelHistoryPage"]]] = None

    def has_next(self) -> bool:
        return self.has_more and self.next_cursor is not None

    async def next(self) -> "ChannelHistoryPage":
        if not self.has_next() or self._fetch_next is None:
            raise SockudoException("No more pages available")
        return await self._fetch_next(self.next_cursor)


@dataclass
class VersionedMessageAck:
    action: str
    channel: str
    event: Optional[str]
    message_id: Optional[str]
    version_id: Optional[str]
    message_serial: Optional[int]
    version_serial: Optional[int]
    history_serial: Optional[int]
    delivery_serial: Optional[int]
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PresenceHistoryPage:
    items: List[PresenceHistoryItem]
    direction: str
    limit: int
    has_more: bool
    next_cursor: Optional[str]
    bounds: PresenceHistoryBounds
    continuity: PresenceHistoryContinuity
    _fetch_next: Optional[Callable[[str], Awaitable["PresenceHistoryPage"]]] = None

    def has_next(self) -> bool:
        return self.has_more and self.next_cursor is not None

    async def next(self) -> "PresenceHistoryPage":
        if not self.has_next() or self._fetch_next is None:
            raise SockudoException("No more pages available")
        return await self._fetch_next(self.next_cursor)


@dataclass
class SockudoEvent:
    event: str
    channel: Optional[str]
    data: Any
    user_id: Optional[str]
    message_id: Optional[str]
    stream_id: Optional[str]
    raw_message: str
    sequence: Optional[int] = None
    conflation_key: Optional[str] = None
    serial: Optional[int] = None
    extras: Optional[MessageExtras] = None


@dataclass
class RecoveryPosition:
    serial: int
    stream_id: Optional[str] = None
    last_message_id: Optional[str] = None


class ProtocolPrefix:
    def __init__(self, version: int) -> None:
        self.version = "2" if version >= 2 else "7"
        self.event_prefix = "sockudo:" if version >= 2 else "pusher:"
        self.internal_prefix = (
            "sockudo_internal:" if version >= 2 else "pusher_internal:"
        )

    def event(self, name: str) -> str:
        return f"{self.event_prefix}{name}"

    def internal(self, name: str) -> str:
        return f"{self.internal_prefix}{name}"

    def is_internal_event(self, name: str) -> bool:
        return name.startswith(self.internal_prefix)

    def is_platform_event(self, name: str) -> bool:
        return name.startswith(self.event_prefix)


class EventDispatcher:
    def __init__(
        self, failthrough: Optional[Callable[[str, Any], None]] = None
    ) -> None:
        self._callbacks: Dict[
            str, "OrderedDict[str, Callable[[Any, Optional[EventMetadata]], None]]"
        ] = {}
        self._global_callbacks: "OrderedDict[str, Callable[[str, Any], None]]" = (
            OrderedDict()
        )
        self._failthrough = failthrough

    def bind(
        self, event_name: str, callback: Callable[[Any, Optional[EventMetadata]], None]
    ) -> str:
        token = base64.urlsafe_b64encode(nacl_random(9)).decode("ascii")
        self._callbacks.setdefault(event_name, OrderedDict())[token] = callback
        return token

    def bind_global(self, callback: Callable[[str, Any], None]) -> str:
        token = base64.urlsafe_b64encode(nacl_random(9)).decode("ascii")
        self._global_callbacks[token] = callback
        return token

    def unbind_global(self, token: Optional[str] = None) -> None:
        if token is None:
            self._global_callbacks.clear()
            return
        self._global_callbacks.pop(token, None)

    def unbind(
        self, event_name: Optional[str] = None, token: Optional[str] = None
    ) -> None:
        if event_name is not None and token is None:
            self._callbacks.pop(event_name, None)
            return
        if event_name is not None and token is not None:
            callbacks = self._callbacks.get(event_name)
            if callbacks is None:
                return
            callbacks.pop(token, None)
            if not callbacks:
                self._callbacks.pop(event_name, None)
            return
        if token is not None:
            for name in list(self._callbacks):
                self._callbacks[name].pop(token, None)
                if not self._callbacks[name]:
                    self._callbacks.pop(name, None)
            self._global_callbacks.pop(token, None)
            return
        self._callbacks.clear()
        self._global_callbacks.clear()

    def emit(
        self, event_name: str, data: Any, metadata: Optional[EventMetadata] = None
    ) -> None:
        for callback in self._global_callbacks.values():
            callback(event_name, data)
        callbacks = self._callbacks.get(event_name)
        if not callbacks:
            if self._failthrough is not None:
                self._failthrough(event_name, data)
            return
        for callback in callbacks.values():
            callback(data, metadata)


class MessageDeduplicator:
    def __init__(self, capacity: int = 1000) -> None:
        self._capacity = capacity
        self._seen: "OrderedDict[str, bool]" = OrderedDict()

    def is_duplicate(self, message_id: str) -> bool:
        return message_id in self._seen

    def track(self, message_id: str) -> None:
        self._seen.pop(message_id, None)
        self._seen[message_id] = True
        while len(self._seen) > self._capacity:
            self._seen.popitem(last=False)


class FossilDelta:
    _digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~"
    _values = {ord(ch): index for index, ch in enumerate(_digits)}

    class _Reader:
        def __init__(self, data: bytes) -> None:
            self.data = data
            self.position = 0

        @property
        def has_bytes(self) -> bool:
            return self.position < len(self.data)

        def byte(self) -> int:
            if self.position >= len(self.data):
                raise DeltaFailure("out of bounds")
            value = self.data[self.position]
            self.position += 1
            return value

        def character(self) -> str:
            return chr(self.byte())

        def integer(self) -> int:
            value = 0
            while self.has_bytes:
                raw = self.byte()
                mapped = FossilDelta._values.get(raw, -1)
                if mapped < 0:
                    self.position -= 1
                    break
                value = (value << 6) + mapped
            return value

    @staticmethod
    def apply(base: bytes, delta: bytes) -> bytes:
        reader = FossilDelta._Reader(delta)
        output_size = reader.integer()
        if reader.character() != "\n":
            raise DeltaFailure("size integer not terminated by newline")
        output = bytearray()
        total = 0
        while reader.has_bytes:
            count = reader.integer()
            op = reader.character()
            if op == "@":
                offset = reader.integer()
                if reader.has_bytes and reader.character() != ",":
                    raise DeltaFailure("copy command not terminated by comma")
                total += count
                if total > output_size:
                    raise DeltaFailure("copy exceeds output file size")
                if offset + count > len(base):
                    raise DeltaFailure("copy extends past end of input")
                output.extend(base[offset : offset + count])
            elif op == ":":
                total += count
                if total > output_size:
                    raise DeltaFailure(
                        "insert command gives an output larger than predicted"
                    )
                if reader.position + count > len(delta):
                    raise DeltaFailure("insert count exceeds size of delta")
                output.extend(delta[reader.position : reader.position + count])
                reader.position += count
            elif op == ";":
                payload = bytes(output)
                if count != FossilDelta._checksum(payload):
                    raise DeltaFailure("bad checksum")
                if total != output_size:
                    raise DeltaFailure("generated size does not match predicted size")
                return payload
            else:
                raise DeltaFailure("unknown delta operator")
        raise DeltaFailure("unterminated delta")

    @staticmethod
    def _checksum(data: bytes) -> int:
        n_hash = 16
        sum0 = sum1 = sum2 = sum3 = 0
        index = 0
        remaining = len(data)
        while remaining >= n_hash:
            sum0 += (
                data[index + 0] + data[index + 4] + data[index + 8] + data[index + 12]
            )
            sum1 += (
                data[index + 1] + data[index + 5] + data[index + 9] + data[index + 13]
            )
            sum2 += (
                data[index + 2] + data[index + 6] + data[index + 10] + data[index + 14]
            )
            sum3 += (
                data[index + 3] + data[index + 7] + data[index + 11] + data[index + 15]
            )
            index += n_hash
            remaining -= n_hash
        while remaining >= 4:
            sum0 += data[index + 0]
            sum1 += data[index + 1]
            sum2 += data[index + 2]
            sum3 += data[index + 3]
            index += 4
            remaining -= 4
        sum3 += (sum2 << 8) + (sum1 << 16) + (sum0 << 24)
        if remaining == 3:
            sum3 += data[index + 2] << 8
            sum3 += data[index + 1] << 16
            sum3 += data[index + 0] << 24
        elif remaining == 2:
            sum3 += data[index + 1] << 16
            sum3 += data[index + 0] << 24
        elif remaining == 1:
            sum3 += data[index + 0] << 24
        return sum3


class ProtocolCodec:
    _messagepack_fields = [
        "event",
        "channel",
        "data",
        "name",
        "user_id",
        "tags",
        "sequence",
        "conflation_key",
        "message_id",
        "stream_id",
        "serial",
        "idempotency_key",
        "extras",
        "__delta_seq",
        "__conflation_key",
    ]

    @staticmethod
    def encode_envelope(
        envelope: Dict[str, Any], wire_format: SockudoWireFormat
    ) -> Union[str, bytes]:
        if wire_format is SockudoWireFormat.JSON:
            return json.dumps(envelope, separators=(",", ":"))
        if wire_format is SockudoWireFormat.MESSAGEPACK:
            payload = [
                envelope.get("event"),
                envelope.get("channel"),
                ProtocolCodec._encode_messagepack_data(envelope.get("data")),
                envelope.get("name"),
                envelope.get("user_id"),
                envelope.get("tags"),
                envelope.get("sequence"),
                envelope.get("conflation_key"),
                envelope.get("message_id"),
                envelope.get("stream_id"),
                envelope.get("serial"),
                envelope.get("idempotency_key"),
                ProtocolCodec._encode_messagepack_extras(envelope.get("extras")),
                envelope.get("__delta_seq"),
                envelope.get("__conflation_key"),
            ]
            return msgpack.packb(payload, use_bin_type=True)
        return ProtocolCodec._encode_protobuf(envelope)

    @staticmethod
    def decode_event(
        raw_message: Union[str, bytes], wire_format: SockudoWireFormat
    ) -> SockudoEvent:
        envelope, raw_text = ProtocolCodec.decode_envelope(raw_message, wire_format)
        raw_data = envelope.get("data")
        data = raw_data
        if isinstance(raw_data, str):
            try:
                data = json.loads(raw_data)
            except json.JSONDecodeError:
                data = raw_data
        event_name = envelope.get("event")
        channel = envelope.get("channel")
        user_id = envelope.get("user_id")
        message_id = envelope.get("message_id")
        stream_id = envelope.get("stream_id")
        conflation_key = envelope.get(
            "__conflation_key", envelope.get("conflation_key")
        )
        return SockudoEvent(
            event=event_name if isinstance(event_name, str) else "",
            channel=channel if isinstance(channel, str) else None,
            data=data,
            user_id=user_id if isinstance(user_id, str) else None,
            message_id=message_id if isinstance(message_id, str) else None,
            stream_id=stream_id if isinstance(stream_id, str) else None,
            raw_message=raw_text,
            sequence=_coerce_int(envelope.get("__delta_seq", envelope.get("sequence"))),
            conflation_key=conflation_key if isinstance(conflation_key, str) else None,
            serial=_coerce_int(envelope.get("serial")),
            extras=ProtocolCodec._decode_extras(envelope.get("extras")),
        )

    @staticmethod
    def decode_envelope(
        raw_message: Union[str, bytes], wire_format: SockudoWireFormat
    ) -> Tuple[Dict[str, Any], str]:
        if wire_format is SockudoWireFormat.JSON:
            text = (
                raw_message.decode("utf-8")
                if isinstance(raw_message, (bytes, bytearray))
                else raw_message
            )
            decoded = json.loads(text)
            if not isinstance(decoded, dict):
                raise SockudoException("Unable to decode event envelope")
            return decoded, text
        if wire_format is SockudoWireFormat.MESSAGEPACK:
            unpacked = msgpack.unpackb(
                raw_message
                if isinstance(raw_message, bytes)
                else raw_message.encode("utf-8"),
                raw=False,
            )
            if isinstance(unpacked, list):
                envelope = {}
                for index, field in enumerate(ProtocolCodec._messagepack_fields):
                    if index < len(unpacked):
                        value = ProtocolCodec._decode_messagepack_value(unpacked[index])
                        if value is not None:
                            envelope[field] = value
            elif isinstance(unpacked, dict):
                envelope = {
                    str(key): ProtocolCodec._decode_messagepack_value(value)
                    for key, value in unpacked.items()
                }
            else:
                raise SockudoException("Unable to decode event envelope")
            return envelope, json.dumps(envelope, separators=(",", ":"))
        envelope = ProtocolCodec._decode_protobuf(
            raw_message
            if isinstance(raw_message, bytes)
            else raw_message.encode("utf-8")
        )
        return envelope, json.dumps(envelope, separators=(",", ":"))

    @staticmethod
    def _encode_messagepack_data(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            return ["string", value]
        return ["json", json.dumps(value, separators=(",", ":"))]

    @staticmethod
    def _encode_messagepack_extras(raw_extras: Any) -> Any:
        extras = ProtocolCodec._decode_extras(raw_extras)
        if extras is None:
            return None
        encoded: Dict[str, Any] = dict(extras.to_dict())
        if extras.headers is not None:
            encoded_headers = {}
            for key, value in extras.headers.items():
                if isinstance(value, bool):
                    encoded_headers[key] = ["bool", value]
                elif isinstance(value, int):
                    encoded_headers[key] = ["number", value]
                elif isinstance(value, float):
                    encoded_headers[key] = ["number", value]
                else:
                    encoded_headers[key] = ["string", str(value)]
            encoded["headers"] = encoded_headers
        if extras.ephemeral is not None:
            encoded["ephemeral"] = extras.ephemeral
        if extras.idempotency_key is not None:
            encoded["idempotency_key"] = extras.idempotency_key
        if extras.echo is not None:
            encoded["echo"] = extras.echo
        return encoded

    @staticmethod
    def _decode_messagepack_value(value: Any) -> Any:
        if isinstance(value, list):
            if len(value) == 2 and isinstance(value[0], str):
                tag = value[0]
                if tag in {"string", "json", "number", "bool"}:
                    return value[1]
            return [ProtocolCodec._decode_messagepack_value(item) for item in value]
        if isinstance(value, dict):
            return {
                str(key): ProtocolCodec._decode_messagepack_value(item)
                for key, item in value.items()
            }
        return value

    @staticmethod
    def _decode_extras(raw_extras: Any) -> Optional[MessageExtras]:
        if raw_extras is None:
            return None
        if isinstance(raw_extras, MessageExtras):
            return raw_extras
        if not isinstance(raw_extras, dict):
            return None
        raw = {
            str(key): ProtocolCodec._decode_messagepack_value(value)
            for key, value in raw_extras.items()
        }
        headers = raw.get("headers")
        if isinstance(headers, dict):
            decoded_headers = {}
            for key, value in headers.items():
                if isinstance(value, list) and len(value) == 2:
                    decoded_headers[key] = value[1]
                else:
                    decoded_headers[key] = value
            headers = decoded_headers
            raw["headers"] = decoded_headers
        ai = raw.get("ai")
        return MessageExtras(
            headers=headers if isinstance(headers, dict) else None,
            ephemeral=raw.get("ephemeral"),
            idempotency_key=raw.get("idempotency_key"),
            echo=raw.get("echo"),
            ai=ai if isinstance(ai, dict) else None,
            raw=raw,
        )

    @staticmethod
    def _encode_protobuf(envelope: Dict[str, Any]) -> bytes:
        output = bytearray()
        _write_string_field(output, 1, envelope.get("event"))
        _write_string_field(output, 2, envelope.get("channel"))
        if "data" in envelope and envelope.get("data") is not None:
            nested = bytearray()
            data = envelope["data"]
            if isinstance(data, str):
                _write_string_field(nested, 1, data)
            else:
                _write_string_field(nested, 3, json.dumps(data, separators=(",", ":")))
            _write_bytes_field(output, 3, bytes(nested))
        _write_string_field(output, 5, envelope.get("user_id"))
        _write_uint_field(output, 7, envelope.get("sequence"))
        _write_string_field(output, 8, envelope.get("conflation_key"))
        _write_string_field(output, 9, envelope.get("message_id"))
        _write_string_field(output, 10, envelope.get("stream_id"))
        _write_uint_field(output, 11, envelope.get("serial"))
        _write_string_field(output, 12, envelope.get("idempotency_key"))
        extras = ProtocolCodec._encode_protobuf_extras(envelope.get("extras"))
        if extras is not None:
            _write_bytes_field(output, 13, extras)
        _write_uint_field(output, 14, envelope.get("__delta_seq"))
        _write_string_field(output, 15, envelope.get("__conflation_key"))
        return bytes(output)

    @staticmethod
    def _encode_protobuf_extras(raw_extras: Any) -> Optional[bytes]:
        extras = ProtocolCodec._decode_extras(raw_extras)
        if extras is None:
            return None
        output = bytearray()
        if extras.headers:
            for key, value in extras.headers.items():
                entry = bytearray()
                _write_string_field(entry, 1, key)
                value_bytes = bytearray()
                if isinstance(value, bool):
                    _write_bool_field(value_bytes, 3, value)
                elif isinstance(value, (int, float)):
                    _write_double_field(value_bytes, 2, float(value))
                else:
                    _write_string_field(value_bytes, 1, str(value))
                _write_bytes_field(entry, 2, bytes(value_bytes))
                _write_bytes_field(output, 1, bytes(entry))
        _write_optional_bool_field(output, 2, extras.ephemeral)
        _write_string_field(output, 3, extras.idempotency_key)
        _write_optional_bool_field(output, 4, extras.echo)
        if extras.ai is not None:
            ai_payload = ProtocolCodec._encode_proto_ai_extras(extras.ai)
            if ai_payload:
                _write_bytes_field(output, 5, ai_payload)
        return bytes(output)

    @staticmethod
    def _encode_proto_ai_extras(ai: Dict[str, Any]) -> bytes:
        output = bytearray()
        transport = ai.get("transport")
        if isinstance(transport, dict):
            for key, value in transport.items():
                entry = ProtocolCodec._encode_proto_string_map_entry(key, value)
                if entry:
                    _write_bytes_field(output, 1, entry)
        codec = ai.get("codec")
        if isinstance(codec, dict):
            for key, value in codec.items():
                entry = ProtocolCodec._encode_proto_string_map_entry(key, value)
                if entry:
                    _write_bytes_field(output, 2, entry)
        return bytes(output)

    @staticmethod
    def _encode_proto_string_map_entry(key: Any, value: Any) -> bytes:
        if not isinstance(key, str):
            return b""
        entry = bytearray()
        _write_string_field(entry, 1, key)
        _write_string_field(entry, 2, str(value))
        return bytes(entry)

    @staticmethod
    def _decode_protobuf(payload: bytes) -> Dict[str, Any]:
        index = 0
        envelope: Dict[str, Any] = {}
        while index < len(payload):
            tag, index = _read_varint(payload, index)
            field = tag >> 3
            wire = tag & 0x7
            if field in {1, 2, 5, 8, 9, 10, 12, 15} and wire == 2:
                value, index = _read_length_delimited(payload, index)
                envelope[
                    {
                        1: "event",
                        2: "channel",
                        5: "user_id",
                        8: "conflation_key",
                        9: "message_id",
                        10: "stream_id",
                        12: "idempotency_key",
                        15: "__conflation_key",
                    }[field]
                ] = value.decode("utf-8")
            elif field in {7, 11, 14} and wire == 0:
                value, index = _read_varint(payload, index)
                envelope[{7: "sequence", 11: "serial", 14: "__delta_seq"}[field]] = (
                    value
                )
            elif field == 10 and wire == 0:
                value, index = _read_varint(payload, index)
                envelope["serial"] = value
            elif field == 13 and wire == 0:
                value, index = _read_varint(payload, index)
                envelope["__delta_seq"] = value
            elif field == 3 and wire == 2:
                value, index = _read_length_delimited(payload, index)
                envelope["data"] = ProtocolCodec._decode_proto_data(value)
            elif field == 13 and wire == 2:
                value, index = _read_length_delimited(payload, index)
                envelope["extras"] = ProtocolCodec._decode_proto_extras(value)
            else:
                index = _skip_unknown(payload, index, wire)
        return envelope

    @staticmethod
    def _decode_proto_data(payload: bytes) -> Any:
        index = 0
        data: Dict[int, Any] = {}
        while index < len(payload):
            tag, index = _read_varint(payload, index)
            field = tag >> 3
            wire = tag & 0x7
            if field in {1, 3} and wire == 2:
                value, index = _read_length_delimited(payload, index)
                data[field] = value.decode("utf-8")
            else:
                index = _skip_unknown(payload, index, wire)
        if 1 in data:
            return data[1]
        if 3 in data:
            return data[3]
        return None

    @staticmethod
    def _decode_proto_extras(payload: bytes) -> Dict[str, Any]:
        index = 0
        result: Dict[str, Any] = {}
        headers: Dict[str, Any] = {}
        while index < len(payload):
            tag, index = _read_varint(payload, index)
            field = tag >> 3
            wire = tag & 0x7
            if field == 1 and wire == 2:
                entry, index = _read_length_delimited(payload, index)
                key, value = ProtocolCodec._decode_proto_header_entry(entry)
                if key is not None:
                    headers[key] = value
            elif field == 2 and wire == 0:
                value, index = _read_varint(payload, index)
                result["ephemeral"] = bool(value)
            elif field == 3 and wire == 2:
                value, index = _read_length_delimited(payload, index)
                result["idempotency_key"] = value.decode("utf-8")
            elif field == 4 and wire == 0:
                value, index = _read_varint(payload, index)
                result["echo"] = bool(value)
            elif field == 5 and wire == 2:
                value, index = _read_length_delimited(payload, index)
                ai = ProtocolCodec._decode_proto_ai_extras(value)
                if ai:
                    result["ai"] = ai
            else:
                index = _skip_unknown(payload, index, wire)
        if headers:
            result["headers"] = headers
        return result

    @staticmethod
    def _decode_proto_header_entry(payload: bytes) -> Tuple[Optional[str], Any]:
        index = 0
        key = None
        value = None
        while index < len(payload):
            tag, index = _read_varint(payload, index)
            field = tag >> 3
            wire = tag & 0x7
            if field == 1 and wire == 2:
                raw, index = _read_length_delimited(payload, index)
                key = raw.decode("utf-8")
            elif field == 2 and wire == 2:
                raw, index = _read_length_delimited(payload, index)
                value = ProtocolCodec._decode_proto_extra_value(raw)
            else:
                index = _skip_unknown(payload, index, wire)
        return key, value

    @staticmethod
    def _decode_proto_extra_value(payload: bytes) -> Any:
        index = 0
        while index < len(payload):
            tag, index = _read_varint(payload, index)
            field = tag >> 3
            wire = tag & 0x7
            if field == 1 and wire == 2:
                raw, index = _read_length_delimited(payload, index)
                return raw.decode("utf-8")
            if field == 2 and wire == 1:
                return struct.unpack("<d", payload[index : index + 8])[0]
            if field == 3 and wire == 0:
                raw, index = _read_varint(payload, index)
                return bool(raw)
            index = _skip_unknown(payload, index, wire)
        return None

    @staticmethod
    def _decode_proto_ai_extras(payload: bytes) -> Dict[str, Any]:
        index = 0
        transport: Dict[str, str] = {}
        codec: Dict[str, str] = {}
        while index < len(payload):
            tag, index = _read_varint(payload, index)
            field = tag >> 3
            wire = tag & 0x7
            if field in {1, 2} and wire == 2:
                raw, index = _read_length_delimited(payload, index)
                key, value = ProtocolCodec._decode_proto_string_map_entry(raw)
                if key is not None and value is not None:
                    if field == 1:
                        transport[key] = value
                    else:
                        codec[key] = value
            else:
                index = _skip_unknown(payload, index, wire)
        result: Dict[str, Any] = {}
        if transport:
            result["transport"] = transport
        if codec:
            result["codec"] = codec
        return result

    @staticmethod
    def _decode_proto_string_map_entry(
        payload: bytes,
    ) -> Tuple[Optional[str], Optional[str]]:
        index = 0
        key = None
        value = None
        while index < len(payload):
            tag, index = _read_varint(payload, index)
            field = tag >> 3
            wire = tag & 0x7
            if field == 1 and wire == 2:
                raw, index = _read_length_delimited(payload, index)
                key = raw.decode("utf-8")
            elif field == 2 and wire == 2:
                raw, index = _read_length_delimited(payload, index)
                value = raw.decode("utf-8")
            else:
                index = _skip_unknown(payload, index, wire)
        return key, value


class DeltaCompressionManager:
    def __init__(
        self,
        options: DeltaOptions,
        send_event: Callable[[str, Any, Optional[str]], Awaitable[bool]],
        prefix: ProtocolPrefix,
    ) -> None:
        self._options = options
        self._send_event = send_event
        self._prefix = prefix
        self._enabled = False
        self._default_algorithm = DeltaAlgorithm.FOSSIL
        self._stats = DeltaStats()
        self._channel_states: Dict[str, Dict[str, Any]] = {}

    async def enable(self) -> None:
        if self._enabled:
            return
        await self._send_event(
            self._prefix.event("enable_delta_compression"),
            {"algorithms": [algorithm.value for algorithm in self._options.algorithms]},
            None,
        )

    def handle_enabled(self, data: Any) -> None:
        payload = data if isinstance(data, dict) else {}
        self._enabled = payload.get("enabled", True)
        if "algorithm" in payload:
            try:
                self._default_algorithm = DeltaAlgorithm(payload["algorithm"])
            except ValueError:
                pass

    def handle_cache_sync(self, channel: str, data: Any) -> None:
        payload = data if isinstance(data, dict) else {}
        self._channel_states[channel] = {
            "conflation_key": payload.get("conflation_key"),
            "states": payload.get("states", {}),
            "base_message": None,
        }

    async def handle_delta_message(
        self, channel: str, data: Any
    ) -> Optional[SockudoEvent]:
        payload = data if isinstance(data, dict) else {}
        event_name = payload.get("event")
        delta_payload = payload.get("delta")
        if not isinstance(event_name, str) or not isinstance(delta_payload, str):
            return None
        algorithm = payload.get("algorithm", self._default_algorithm.value)
        sequence = _coerce_int(payload.get("seq"))
        base_state = self._channel_states.get(channel)
        if base_state is None or base_state.get("base_message") is None:
            await self._send_event(
                self._prefix.event("delta_sync_error"), {"channel": channel}, None
            )
            self._channel_states.pop(channel, None)
            return None
        try:
            delta_bytes = base64.b64decode(delta_payload)
            if algorithm == DeltaAlgorithm.XDELTA3.value:
                reconstructed = vcdiff_decoder.decode(
                    base_state["base_message"].encode("utf-8"), delta_bytes
                ).decode("utf-8")
            else:
                reconstructed = FossilDelta.apply(
                    base_state["base_message"].encode("utf-8"), delta_bytes
                ).decode("utf-8")
            parsed = json.loads(reconstructed)
            event_data = (
                parsed.get("data")
                if isinstance(parsed, dict) and "data" in parsed
                else parsed
            )
            self.handle_full_message(
                channel, reconstructed, sequence, payload.get("conflation_key")
            )
            self._stats.delta_messages += 1
            self._stats.total_messages += 1
            self._stats.total_bytes_without_compression += len(reconstructed)
            self._stats.total_bytes_with_compression += len(delta_bytes)
            if self._options.on_stats:
                self._options.on_stats(self._stats)
            return SockudoEvent(
                event=event_name,
                channel=channel,
                data=event_data,
                user_id=None,
                message_id=None,
                raw_message=reconstructed,
                sequence=sequence,
                conflation_key=payload.get("conflation_key"),
            )
        except BaseException as exc:
            self._stats.errors += 1
            if self._options.on_error:
                self._options.on_error(exc)
            return None

    def handle_full_message(
        self,
        channel: str,
        raw_message: str,
        sequence: Optional[int],
        conflation_key: Optional[str],
    ) -> None:
        self._channel_states.setdefault(channel, {})["base_message"] = raw_message
        self._stats.full_messages += 1
        self._stats.total_messages += 1
        self._stats.total_bytes_without_compression += len(raw_message)
        self._stats.total_bytes_with_compression += len(raw_message)
        if self._options.on_stats:
            self._options.on_stats(self._stats)

    def get_stats(self) -> DeltaStats:
        return self._stats

    def reset_stats(self) -> None:
        self._stats = DeltaStats()

    def clear_channel_state(self, channel: str) -> None:
        self._channel_states.pop(channel, None)


class PresenceMembers:
    def __init__(self) -> None:
        self._members: Dict[str, Any] = {}
        self.count: int = 0
        self.my_id: Optional[str] = None
        self.me: Optional[PresenceMember] = None

    def member(self, member_id: str) -> Optional[PresenceMember]:
        if member_id not in self._members:
            return None
        return PresenceMember(member_id, self._members[member_id])

    def remember_my_id(self, member_id: str) -> None:
        self.my_id = member_id

    def apply_subscription_data(self, data: Dict[str, Any]) -> None:
        if not isinstance(data, dict):
            return
        presence = data.get("presence")
        if not isinstance(presence, dict):
            return
        hash_data = presence.get("hash", {})
        if not isinstance(hash_data, dict):
            return
        self._members = dict(hash_data)
        count = _coerce_int(presence.get("count"))
        self.count = count if count is not None else len(self._members)
        self.me = self.member(self.my_id) if self.my_id else None

    def add(self, data: Dict[str, Any]) -> Optional[PresenceMember]:
        user_id = data.get("user_id")
        if not isinstance(user_id, str):
            return None
        if user_id not in self._members:
            self.count += 1
        self._members[user_id] = data.get("user_info")
        return PresenceMember(user_id, self._members[user_id])

    def update(self, data: Dict[str, Any]) -> Optional[PresenceMember]:
        user_id = data.get("user_id")
        if not isinstance(user_id, str) or user_id not in self._members:
            return None
        self._members[user_id] = data.get("user_info")
        self.me = self.member(self.my_id) if self.my_id else None
        return PresenceMember(user_id, self._members[user_id])

    def remove(self, data: Dict[str, Any]) -> Optional[PresenceMember]:
        user_id = data.get("user_id")
        if not isinstance(user_id, str) or user_id not in self._members:
            return None
        info = self._members.pop(user_id)
        self.count = max(0, self.count - 1)
        return PresenceMember(user_id, info)

    def reset(self) -> None:
        self._members.clear()
        self.count = 0
        self.my_id = None
        self.me = None


class SockudoChannel:
    def __init__(self, name: str, client: "SockudoClient") -> None:
        self.name = name
        self.client = client
        self.dispatcher = EventDispatcher()
        self.is_subscribed = False
        self.subscription_pending = False
        self.subscription_cancelled = False
        self.subscription_count: Optional[int] = None
        self.filter: Optional[FilterNode] = None
        self.delta_settings: Optional[ChannelDeltaSettings] = None
        self.events_filter: Optional[List[str]] = None
        self.rewind: Optional[SubscriptionRewind] = None
        self.attach_serial: Optional[int] = None

    def bind(
        self, event_name: str, callback: Callable[[Any, Optional[EventMetadata]], None]
    ) -> str:
        return self.dispatcher.bind(event_name, callback)

    def bind_global(self, callback: Callable[[str, Any], None]) -> str:
        return self.dispatcher.bind_global(callback)

    def unbind(
        self, event_name: Optional[str] = None, token: Optional[str] = None
    ) -> None:
        self.dispatcher.unbind(event_name, token)

    async def trigger(self, event: str, data: Any) -> bool:
        if not event.startswith("client-"):
            raise BadEventName(f"Event '{event}' does not start with 'client-'")
        return await self.client.send_event(event, data, self.name)

    async def history(
        self, params: Optional[ChannelHistoryParams] = None
    ) -> ChannelHistoryPage:
        return await self.channel_history(params)

    async def channel_history(
        self, params: Optional[ChannelHistoryParams] = None
    ) -> ChannelHistoryPage:
        return await self.client.config.fetch_channel_history(
            self.name, params or ChannelHistoryParams(), self.attach_serial
        )

    async def create_versioned_message(
        self,
        event: str,
        data: Any,
        extras: Optional[Union[MessageExtras, Dict[str, Any]]] = None,
        timeout: Optional[float] = None,
    ) -> VersionedMessageAck:
        return await self.client.versioned_messages.create(
            self.name, event, data, extras=extras, timeout=timeout
        )

    async def append_versioned_message(
        self,
        message_id: str,
        data: Any,
        extras: Optional[Union[MessageExtras, Dict[str, Any]]] = None,
        event: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> VersionedMessageAck:
        return await self.client.versioned_messages.append(
            self.name, message_id, data, extras=extras, event=event, timeout=timeout
        )

    async def update_versioned_message(
        self,
        message_id: str,
        data: Any,
        extras: Optional[Union[MessageExtras, Dict[str, Any]]] = None,
        event: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> VersionedMessageAck:
        return await self.client.versioned_messages.update(
            self.name, message_id, data, extras=extras, event=event, timeout=timeout
        )

    async def delete_versioned_message(
        self,
        message_id: str,
        reason: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> VersionedMessageAck:
        return await self.client.versioned_messages.delete(
            self.name, message_id, reason=reason, timeout=timeout
        )

    async def authorize(self, socket_id: str) -> ChannelAuthorizationData:
        return ChannelAuthorizationData(auth="")

    def subscribe_if_possible(self) -> None:
        if self.subscription_pending and self.subscription_cancelled:
            self.subscription_cancelled = False
        elif (
            not self.subscription_pending
            and self.client.connection_state is ConnectionState.CONNECTED
        ):
            asyncio.create_task(self.subscribe())

    async def subscribe(self) -> None:
        if self.is_subscribed:
            return
        self.subscription_pending = True
        self.subscription_cancelled = False
        try:
            auth = await self.authorize(self.client.socket_id or "")
            payload: Dict[str, Any] = {"auth": auth.auth, "channel": self.name}
            if auth.channel_data is not None:
                payload["channel_data"] = auth.channel_data
            if self.filter is not None:
                payload["tags_filter"] = self.filter.to_dict()
            if self.delta_settings is not None:
                payload["delta"] = self.delta_settings.subscription_value()
            if self.events_filter is not None:
                payload["events"] = self.events_filter
            if self.rewind is not None:
                payload["rewind"] = self.rewind.subscription_value()
            await self.client.send_event(
                self.client.prefix.event("subscribe"), payload, None
            )
        except BaseException as exc:
            self.subscription_pending = False
            self.dispatcher.emit(
                self.client.prefix.event("subscription_error"),
                {"type": "AuthError", "error": str(exc)},
            )

    async def unsubscribe(self) -> None:
        self.is_subscribed = False
        await self.client.send_event(
            self.client.prefix.event("unsubscribe"), {"channel": self.name}, None
        )

    def disconnect(self) -> None:
        self.is_subscribed = False
        self.subscription_pending = False
        self.attach_serial = None

    def handle(self, event: SockudoEvent) -> None:
        p = self.client.prefix
        if event.event == p.internal("subscription_succeeded"):
            self.subscription_pending = False
            self.is_subscribed = True
            payload = event.data if isinstance(event.data, dict) else {}
            self.attach_serial = _coerce_int(payload.get("attach_serial"))
            if self.subscription_cancelled:
                asyncio.create_task(self.client.unsubscribe(self.name))
            else:
                self.dispatcher.emit(p.event("subscription_succeeded"), event.data)
        elif event.event == p.internal("subscription_count"):
            if isinstance(event.data, dict):
                self.subscription_count = _coerce_int(
                    event.data.get("subscription_count")
                )
            self.dispatcher.emit(p.event("subscription_count"), event.data)
        elif p.is_internal_event(event.event):
            self.dispatcher.emit(
                event.event, event.data, EventMetadata(user_id=event.user_id)
            )
        elif not p.is_internal_event(event.event):
            self.dispatcher.emit(
                event.event, event.data, EventMetadata(user_id=event.user_id)
            )


class PrivateChannel(SockudoChannel):
    async def authorize(self, socket_id: str) -> ChannelAuthorizationData:
        return await self.client.config.authorize_channel(
            ChannelAuthorizationRequest(socket_id, self.name)
        )


class PresenceChannel(PrivateChannel):
    def __init__(self, name: str, client: "SockudoClient") -> None:
        super().__init__(name, client)
        self.members = PresenceMembers()

    async def authorize(self, socket_id: str) -> ChannelAuthorizationData:
        response = await super().authorize(socket_id)
        if response.channel_data:
            parsed = json.loads(response.channel_data)
            if isinstance(parsed, dict) and isinstance(parsed.get("user_id"), str):
                self.members.remember_my_id(parsed["user_id"])
                return response
        if self.client.user.user_id:
            self.members.remember_my_id(self.client.user.user_id)
            return response
        raise AuthFailure(
            None, f"Invalid auth response for presence channel '{self.name}'"
        )

    def handle(self, event: SockudoEvent) -> None:
        p = self.client.prefix
        if event.event == p.internal("subscription_succeeded"):
            self.subscription_pending = False
            self.is_subscribed = True
            payload = event.data if isinstance(event.data, dict) else {}
            self.attach_serial = _coerce_int(payload.get("attach_serial"))
            self.members.apply_subscription_data(payload)
            self.dispatcher.emit(p.event("subscription_succeeded"), self.members)
        elif event.event == p.internal("member_added") and isinstance(event.data, dict):
            member = self.members.add(event.data)
            if member is not None:
                self.dispatcher.emit(p.event("member_added"), member)
        elif event.event == p.internal("member_removed") and isinstance(
            event.data, dict
        ):
            member = self.members.remove(event.data)
            if member is not None:
                self.dispatcher.emit(p.event("member_removed"), member)
        elif event.event == p.internal("presence_update") and isinstance(
            event.data, dict
        ):
            member = self.members.update(event.data)
            if member is not None:
                self.dispatcher.emit(p.event("presence_update"), member)
                self.dispatcher.emit(event.event, event.data)
        else:
            super().handle(event)

    def disconnect(self) -> None:
        self.members.reset()
        super().disconnect()

    async def history(
        self, params: Optional[PresenceHistoryParams] = None
    ) -> PresenceHistoryPage:
        return await self.client.config.fetch_presence_history(
            self.name, params or PresenceHistoryParams()
        )

    async def snapshot(
        self, params: Optional[PresenceSnapshotParams] = None
    ) -> PresenceSnapshot:
        return await self.client.config.fetch_presence_snapshot(
            self.name, params or PresenceSnapshotParams()
        )

    async def update(self, data: Any) -> bool:
        return await self.client.send_event(
            self.client.prefix.event("presence_update"),
            {"user_info": data},
            self.name,
        )


class EncryptedChannel(PrivateChannel):
    def __init__(self, name: str, client: "SockudoClient") -> None:
        super().__init__(name, client)
        self.shared_secret: Optional[bytes] = None

    async def authorize(self, socket_id: str) -> ChannelAuthorizationData:
        response = await super().authorize(socket_id)
        if not response.shared_secret:
            raise AuthFailure(
                None,
                f"No shared_secret key in auth payload for encrypted channel: {self.name}",
            )
        self.shared_secret = base64.b64decode(response.shared_secret)
        return ChannelAuthorizationData(
            auth=response.auth, channel_data=response.channel_data
        )

    async def trigger(self, event: str, data: Any) -> bool:
        raise UnsupportedFeature(
            "Client events are not currently supported for encrypted channels"
        )

    def handle(self, event: SockudoEvent) -> None:
        if self.client.prefix.is_internal_event(
            event.event
        ) or self.client.prefix.is_platform_event(event.event):
            super().handle(event)
            return
        if self.shared_secret is None or not isinstance(event.data, dict):
            return
        cipher_text = event.data.get("ciphertext")
        nonce = event.data.get("nonce")
        if not isinstance(cipher_text, str) or not isinstance(nonce, str):
            return
        box = SecretBox(self.shared_secret)
        decrypted = box.decrypt(
            base64.b64decode(cipher_text), base64.b64decode(nonce)
        ).decode("utf-8")
        parsed = json.loads(decrypted)
        self.dispatcher.emit(event.event, parsed, EventMetadata(user_id=event.user_id))


class _ResolvedConfiguration:
    def __init__(self, options: SockudoOptions) -> None:
        self.cluster = options.cluster
        self.activity_timeout = options.activity_timeout
        self.use_tls = options.force_tls is not False
        self.ws_host = options.ws_host or f"ws-{options.cluster}.sockudo.io"
        self.ws_port = options.ws_port
        self.wss_port = options.wss_port
        self.ws_path = options.ws_path
        self.http_host = options.http_host or f"sockjs-{options.cluster}.sockudo.io"
        self.http_port = options.http_port
        self.https_port = options.https_port
        self.http_path = options.http_path
        self.pong_timeout = options.pong_timeout
        self.unavailable_timeout = options.unavailable_timeout
        self.enabled_transports = options.enabled_transports
        self.disabled_transports = options.disabled_transports
        self.channel_options = options.channel_authorization
        self.user_options = options.user_authentication
        self.presence_history = options.presence_history
        self.channel_history = options.channel_history
        self.versioned_messages = options.versioned_messages
        self._http_client = httpx.AsyncClient()

    async def authorize_channel(
        self, request: ChannelAuthorizationRequest
    ) -> ChannelAuthorizationData:
        if self.channel_options.custom_handler is not None:
            return await self.channel_options.custom_handler(request)
        params = dict(self.channel_options.params)
        if self.channel_options.params_provider:
            params.update(self.channel_options.params_provider())
        params["socket_id"] = request.socket_id
        params["channel_name"] = request.channel_name
        headers = dict(self.channel_options.headers)
        if self.channel_options.headers_provider:
            headers.update(self.channel_options.headers_provider())
        payload = await self._perform_auth_request(
            self.channel_options.endpoint, headers, params
        )
        auth = payload.get("auth")
        if not isinstance(auth, str):
            raise AuthFailure(200, "JSON returned from auth endpoint was invalid")
        return ChannelAuthorizationData(
            auth=auth,
            channel_data=payload.get("channel_data"),
            shared_secret=payload.get("shared_secret"),
        )

    async def authenticate_user(
        self, request: UserAuthenticationRequest
    ) -> UserAuthenticationData:
        if self.user_options.custom_handler is not None:
            return await self.user_options.custom_handler(request)
        params = dict(self.user_options.params)
        if self.user_options.params_provider:
            params.update(self.user_options.params_provider())
        params["socket_id"] = request.socket_id
        headers = dict(self.user_options.headers)
        if self.user_options.headers_provider:
            headers.update(self.user_options.headers_provider())
        payload = await self._perform_auth_request(
            self.user_options.endpoint, headers, params
        )
        auth = payload.get("auth")
        user_data = payload.get("user_data")
        if not isinstance(auth, str) or not isinstance(user_data, str):
            raise AuthFailure(200, "JSON returned from auth endpoint was invalid")
        return UserAuthenticationData(auth=auth, user_data=user_data)

    async def close(self) -> None:
        await self._http_client.aclose()

    async def fetch_presence_history(
        self, channel_name: str, params: PresenceHistoryParams
    ) -> PresenceHistoryPage:
        config = self.presence_history
        if config is None:
            raise UnsupportedFeature(
                "presence_history.endpoint must be configured to use presence.history(). "
                "This endpoint should proxy requests to the Sockudo server REST API."
            )

        payload = await self._perform_presence_history_request(
            config.endpoint,
            config.headers,
            config.headers_provider,
            channel_name,
            params.to_payload(),
            "history",
        )
        return self._decode_presence_history_page(
            payload,
            lambda cursor: self.fetch_presence_history(
                channel_name,
                PresenceHistoryParams(
                    direction=params.direction,
                    limit=params.limit,
                    cursor=cursor,
                    start_serial=params.start_serial,
                    end_serial=params.end_serial,
                    start_time_ms=params.start_time_ms,
                    end_time_ms=params.end_time_ms,
                    start=params.start,
                    end=params.end,
                ),
            ),
        )

    async def fetch_presence_snapshot(
        self, channel_name: str, params: PresenceSnapshotParams
    ) -> PresenceSnapshot:
        config = self.presence_history
        if config is None:
            raise UnsupportedFeature(
                "presence_history.endpoint must be configured to use presence.snapshot(). "
                "This endpoint should proxy requests to the Sockudo server REST API."
            )

        payload = await self._perform_presence_history_request(
            config.endpoint,
            config.headers,
            config.headers_provider,
            channel_name,
            params.to_payload(),
            "snapshot",
        )
        return self._decode_presence_snapshot(payload)

    async def fetch_channel_history(
        self,
        channel_name: str,
        params: ChannelHistoryParams,
        attach_serial: Optional[int],
    ) -> ChannelHistoryPage:
        config = self.channel_history
        if config is None:
            raise UnsupportedFeature(
                "channel_history.endpoint must be configured to use channel.history(). "
                "This endpoint should proxy requests to the Sockudo server REST API."
            )

        payload = await self._perform_proxy_request(
            config.endpoint,
            config.headers,
            config.headers_provider,
            channel_name,
            params.to_payload(attach_serial),
            "history",
        )
        return self._decode_channel_history_page(
            payload,
            lambda cursor: self.fetch_channel_history(
                channel_name,
                ChannelHistoryParams(
                    direction=params.direction,
                    limit=params.limit,
                    cursor=cursor,
                    start_serial=params.start_serial,
                    end_serial=params.end_serial,
                    until_attach=params.until_attach,
                    attach_serial=params.attach_serial,
                ),
                attach_serial,
            ),
        )

    async def perform_versioned_message_action(
        self,
        channel_name: str,
        action: str,
        params: Dict[str, Any],
        timeout: Optional[float] = None,
    ) -> VersionedMessageAck:
        config = self.versioned_messages
        if config is None:
            raise UnsupportedFeature(
                "versioned_messages.endpoint must be configured to use "
                "client.versioned_messages. This endpoint should proxy requests "
                "to the Sockudo server REST API."
            )
        payload = await self._perform_proxy_request(
            config.endpoint,
            config.headers,
            config.headers_provider,
            channel_name,
            params,
            action,
            timeout=config.timeout if timeout is None else timeout,
        )
        return self._decode_versioned_message_ack(payload, action, channel_name)

    async def _perform_auth_request(
        self, endpoint: str, headers: Dict[str, str], params: Dict[str, AuthValue]
    ) -> Dict[str, Any]:
        response = await self._http_client.post(
            endpoint,
            headers=headers,
            content=urllib.parse.urlencode(
                {
                    key: str(value).lower() if isinstance(value, bool) else value
                    for key, value in params.items()
                }
            ),
        )
        if response.status_code >= 400:
            raise AuthFailure(
                response.status_code,
                f"Could not get auth info from endpoint, status: {response.status_code}",
            )
        payload = response.json()
        if not isinstance(payload, dict):
            raise AuthFailure(
                response.status_code, "JSON returned from auth endpoint was invalid"
            )
        return payload

    async def _perform_presence_history_request(
        self,
        endpoint: str,
        headers: Dict[str, str],
        headers_provider: Optional[PresenceHistoryHeadersProvider],
        channel_name: str,
        params: Dict[str, Any],
        action: str,
    ) -> Dict[str, Any]:
        merged_headers = {"Content-Type": "application/json", **headers}
        if headers_provider:
            merged_headers.update(headers_provider())
        response = await self._http_client.post(
            endpoint,
            headers=merged_headers,
            content=json.dumps(
                {
                    "channel": channel_name,
                    "params": params,
                    "action": action,
                }
            ),
        )
        if response.status_code >= 400:
            raise SockudoException(
                f"Presence {action} request failed ({response.status_code}): "
                f"{response.text}"
            )
        payload = response.json()
        if not isinstance(payload, dict):
            raise SockudoException(f"Presence {action} endpoint returned invalid JSON")
        return payload

    async def _perform_proxy_request(
        self,
        endpoint: str,
        headers: Dict[str, str],
        headers_provider: Optional[ProxyHeadersProvider],
        channel_name: str,
        params: Dict[str, Any],
        action: str,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        merged_headers = {"Content-Type": "application/json", **headers}
        if headers_provider:
            merged_headers.update(headers_provider())
        response = await self._http_client.post(
            endpoint,
            headers=merged_headers,
            content=json.dumps(
                {
                    "channel": channel_name,
                    "params": params,
                    "action": action,
                }
            ),
            timeout=timeout,
        )
        if response.status_code >= 400:
            raise SockudoException(
                f"Proxy {action} request failed ({response.status_code}): "
                f"{response.text}"
            )
        payload = response.json()
        if not isinstance(payload, dict):
            raise SockudoException(f"Proxy {action} endpoint returned invalid JSON")
        return payload

    def _decode_presence_history_page(
        self,
        payload: Dict[str, Any],
        fetch_next: Callable[[str], Awaitable[PresenceHistoryPage]],
    ) -> PresenceHistoryPage:
        return PresenceHistoryPage(
            items=[
                PresenceHistoryItem(
                    stream_id=str(item["stream_id"]),
                    serial=int(item["serial"]),
                    published_at_ms=int(item["published_at_ms"]),
                    event=str(item["event"]),
                    cause=str(item["cause"]),
                    user_id=str(item["user_id"]),
                    connection_id=(
                        str(item["connection_id"])
                        if item.get("connection_id") is not None
                        else None
                    ),
                    dead_node_id=(
                        str(item["dead_node_id"])
                        if item.get("dead_node_id") is not None
                        else None
                    ),
                    payload_size_bytes=int(item["payload_size_bytes"]),
                    presence_event=dict(item.get("presence_event") or {}),
                )
                for item in payload.get("items", [])
                if isinstance(item, dict)
            ],
            direction=str(payload.get("direction", "oldest_first")),
            limit=int(payload.get("limit", 0)),
            has_more=bool(payload.get("has_more", False)),
            next_cursor=(
                str(payload["next_cursor"])
                if payload.get("next_cursor") is not None
                else None
            ),
            bounds=self._decode_presence_history_bounds(payload.get("bounds")),
            continuity=self._decode_presence_history_continuity(
                payload.get("continuity")
            ),
            _fetch_next=fetch_next,
        )

    def _decode_channel_history_page(
        self,
        payload: Dict[str, Any],
        fetch_next: Callable[[str], Awaitable[ChannelHistoryPage]],
    ) -> ChannelHistoryPage:
        return ChannelHistoryPage(
            items=[
                dict(item)
                for item in payload.get("items", [])
                if isinstance(item, dict)
            ],
            direction=str(payload.get("direction", "oldest_first")),
            limit=int(payload.get("limit", 0)),
            has_more=bool(payload.get("has_more", False)),
            next_cursor=(
                str(payload["next_cursor"])
                if payload.get("next_cursor") is not None
                else None
            ),
            attach_serial=_coerce_int(payload.get("attach_serial")),
            _fetch_next=fetch_next,
        )

    def _decode_versioned_message_ack(
        self, payload: Dict[str, Any], action: str, channel_name: str
    ) -> VersionedMessageAck:
        raw_ack = payload.get("ack", payload)
        ack = raw_ack if isinstance(raw_ack, dict) else {}
        raw = dict(ack)
        return VersionedMessageAck(
            action=str(ack.get("action", action)),
            channel=str(ack.get("channel", channel_name)),
            event=ack.get("event") if isinstance(ack.get("event"), str) else None,
            message_id=self._string_field(ack, "message_id", "id"),
            version_id=self._string_field(ack, "version_id", "version"),
            message_serial=_coerce_int(ack.get("message_serial")),
            version_serial=_coerce_int(ack.get("version_serial")),
            history_serial=_coerce_int(ack.get("history_serial")),
            delivery_serial=_coerce_int(ack.get("delivery_serial")),
            raw=raw,
        )

    @staticmethod
    def _string_field(payload: Dict[str, Any], *names: str) -> Optional[str]:
        for name in names:
            value = payload.get(name)
            if isinstance(value, str):
                return value
        return None

    def _decode_presence_snapshot(self, payload: Dict[str, Any]) -> PresenceSnapshot:
        return PresenceSnapshot(
            channel=str(payload.get("channel", "")),
            members=[
                PresenceSnapshotMember(
                    user_id=str(member["user_id"]),
                    last_event=str(member["last_event"]),
                    last_event_serial=int(member["last_event_serial"]),
                    last_event_at_ms=int(member["last_event_at_ms"]),
                )
                for member in payload.get("members", [])
                if isinstance(member, dict)
            ],
            member_count=int(payload.get("member_count", 0)),
            events_replayed=int(payload.get("events_replayed", 0)),
            snapshot_serial=(
                int(payload["snapshot_serial"])
                if payload.get("snapshot_serial") is not None
                else None
            ),
            snapshot_time_ms=(
                int(payload["snapshot_time_ms"])
                if payload.get("snapshot_time_ms") is not None
                else None
            ),
            continuity=self._decode_presence_history_continuity(
                payload.get("continuity")
            ),
        )

    def _decode_presence_history_bounds(self, payload: Any) -> PresenceHistoryBounds:
        if not isinstance(payload, dict):
            payload = {}
        return PresenceHistoryBounds(
            start_serial=(
                int(payload["start_serial"])
                if payload.get("start_serial") is not None
                else None
            ),
            end_serial=(
                int(payload["end_serial"])
                if payload.get("end_serial") is not None
                else None
            ),
            start_time_ms=(
                int(payload["start_time_ms"])
                if payload.get("start_time_ms") is not None
                else None
            ),
            end_time_ms=(
                int(payload["end_time_ms"])
                if payload.get("end_time_ms") is not None
                else None
            ),
        )

    def _decode_presence_history_continuity(
        self, payload: Any
    ) -> PresenceHistoryContinuity:
        if not isinstance(payload, dict):
            payload = {}
        return PresenceHistoryContinuity(
            stream_id=(
                str(payload["stream_id"])
                if payload.get("stream_id") is not None
                else None
            ),
            oldest_available_serial=(
                int(payload["oldest_available_serial"])
                if payload.get("oldest_available_serial") is not None
                else None
            ),
            newest_available_serial=(
                int(payload["newest_available_serial"])
                if payload.get("newest_available_serial") is not None
                else None
            ),
            oldest_available_published_at_ms=(
                int(payload["oldest_available_published_at_ms"])
                if payload.get("oldest_available_published_at_ms") is not None
                else None
            ),
            newest_available_published_at_ms=(
                int(payload["newest_available_published_at_ms"])
                if payload.get("newest_available_published_at_ms") is not None
                else None
            ),
            retained_events=int(payload.get("retained_events", 0)),
            retained_bytes=int(payload.get("retained_bytes", 0)),
            degraded=bool(payload.get("degraded", False)),
            complete=bool(payload.get("complete", False)),
            truncated_by_retention=bool(payload.get("truncated_by_retention", False)),
        )


class VersionedMessagesFacade:
    def __init__(self, client: "SockudoClient") -> None:
        self.client = client

    async def create(
        self,
        channel: str,
        event: str,
        data: Any,
        extras: Optional[Union[MessageExtras, Dict[str, Any]]] = None,
        timeout: Optional[float] = None,
    ) -> VersionedMessageAck:
        params: Dict[str, Any] = {"event": event, "data": data}
        self._add_extras(params, extras)
        return await self.client.config.perform_versioned_message_action(
            channel, "create", params, timeout
        )

    async def append(
        self,
        channel: str,
        message_id: str,
        data: Any,
        extras: Optional[Union[MessageExtras, Dict[str, Any]]] = None,
        event: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> VersionedMessageAck:
        params: Dict[str, Any] = {"message_id": message_id, "data": data}
        if event is not None:
            params["event"] = event
        self._add_extras(params, extras)
        return await self.client.config.perform_versioned_message_action(
            channel, "append", params, timeout
        )

    async def update(
        self,
        channel: str,
        message_id: str,
        data: Any,
        extras: Optional[Union[MessageExtras, Dict[str, Any]]] = None,
        event: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> VersionedMessageAck:
        params: Dict[str, Any] = {"message_id": message_id, "data": data}
        if event is not None:
            params["event"] = event
        self._add_extras(params, extras)
        return await self.client.config.perform_versioned_message_action(
            channel, "update", params, timeout
        )

    async def delete(
        self,
        channel: str,
        message_id: str,
        reason: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> VersionedMessageAck:
        params: Dict[str, Any] = {"message_id": message_id}
        if reason is not None:
            params["reason"] = reason
        return await self.client.config.perform_versioned_message_action(
            channel, "delete", params, timeout
        )

    @staticmethod
    def _add_extras(
        params: Dict[str, Any], extras: Optional[Union[MessageExtras, Dict[str, Any]]]
    ) -> None:
        if extras is None:
            return
        if isinstance(extras, MessageExtras):
            params["extras"] = extras.to_dict()
        elif isinstance(extras, dict):
            params["extras"] = extras


class SockudoClient:
    def __init__(self, key: str, options: SockudoOptions) -> None:
        if not key:
            raise InvalidAppKey(
                "You must pass your app key when you instantiate SockudoClient."
            )
        if not options.cluster:
            raise InvalidOptions("Options must provide a cluster.")
        if (
            options.append_rollup_window is not None
            and options.append_rollup_window not in _ALLOWED_APPEND_ROLLUP_WINDOWS
        ):
            allowed = ", ".join(
                str(value) for value in sorted(_ALLOWED_APPEND_ROLLUP_WINDOWS)
            )
            raise InvalidOptions(f"append_rollup_window must be one of: {allowed}")
        self.key = key
        self.options = options
        self.prefix = ProtocolPrefix(options.protocol_version)
        self.config = _ResolvedConfiguration(options)
        self.dispatcher = EventDispatcher()
        self.channels: Dict[str, SockudoChannel] = {}
        self.socket = None
        self.connection_state = ConnectionState.INITIALIZED
        self.socket_id: Optional[str] = None
        self._receive_task: Optional[asyncio.Task[Any]] = None
        self._activity_task: Optional[asyncio.Task[Any]] = None
        self._retry_task: Optional[asyncio.Task[Any]] = None
        self._unavailable_task: Optional[asyncio.Task[Any]] = None
        self._auth_refresh_task: Optional[asyncio.Task[Any]] = None
        self._manually_disconnected = False
        self._current_transport: Optional[SockudoTransport] = None
        self._attempted_fallback = False
        self._auth_token: Optional[str] = options.token
        (
            self._auth_token_expires_at,
            self._auth_token_issued_at,
        ) = self._extract_token_times(options.token)
        self._channel_positions: Dict[str, RecoveryPosition] = {}
        self._deduplicator = (
            MessageDeduplicator(options.message_deduplication_capacity)
            if options.message_deduplication
            else None
        )
        self._delta_manager = (
            DeltaCompressionManager(
                options.delta_compression, self.send_event, self.prefix
            )
            if options.delta_compression
            else None
        )
        self.user = self.UserFacade(self)
        self.watchlist = self.WatchlistFacade()
        self.versioned_messages = VersionedMessagesFacade(self)

    def bind(
        self, event_name: str, callback: Callable[[Any, Optional[EventMetadata]], None]
    ) -> str:
        return self.dispatcher.bind(event_name, callback)

    def bind_global(self, callback: Callable[[str, Any], None]) -> str:
        return self.dispatcher.bind_global(callback)

    def channel(self, name: str) -> Optional[SockudoChannel]:
        return self.channels.get(name)

    def subscribe(
        self, channel_name: str, options: Optional[SubscriptionOptions] = None
    ) -> SockudoChannel:
        channel = self.channels.get(channel_name)
        if channel is None:
            channel = self._create_channel(channel_name)
            self.channels[channel_name] = channel
        if options is not None:
            channel.filter = options.filter
            channel.delta_settings = options.delta
            channel.events_filter = options.events
            channel.rewind = options.rewind
        channel.subscribe_if_possible()
        return channel

    async def unsubscribe(self, channel_name: str) -> None:
        channel = self.channels.get(channel_name)
        if channel is None:
            return
        if channel.subscription_pending:
            channel.subscription_cancelled = True
        elif channel.is_subscribed:
            self.channels.pop(channel_name, None)
            await channel.unsubscribe()
        else:
            self.channels.pop(channel_name, None)
        self._channel_positions.pop(channel_name, None)
        if self._delta_manager is not None:
            self._delta_manager.clear_channel_state(channel_name)

    async def connect(self) -> None:
        if self.socket is not None:
            return
        transports = self._transport_sequence()
        if not transports:
            self._update_state(ConnectionState.FAILED)
            return
        self._manually_disconnected = False
        self._attempted_fallback = False
        self._update_state(ConnectionState.CONNECTING)
        await self._open_websocket(transports[0])
        self._set_unavailable_timer()

    async def disconnect(self) -> None:
        self._manually_disconnected = True
        self._cancel_timers()
        if self.socket is not None:
            await self.socket.close()
        self.socket = None
        for channel in self.channels.values():
            channel.disconnect()
        self._update_state(ConnectionState.DISCONNECTED)
        await self.config.close()

    async def close(self) -> None:
        await self.disconnect()

    async def signin(self) -> None:
        await self.user.sign_in()

    async def send_event(self, name: str, data: Any, channel: Optional[str]) -> bool:
        if self.socket is None:
            return False
        payload: Dict[str, Any] = {"event": name, "data": data}
        if channel is not None:
            payload["channel"] = channel
        encoded = ProtocolCodec.encode_envelope(payload, self.options.wire_format)
        await self.socket.send(encoded)
        return True

    def get_delta_stats(self) -> Optional[DeltaStats]:
        return self._delta_manager.get_stats() if self._delta_manager else None

    async def _open_websocket(self, transport: SockudoTransport) -> None:
        self._current_transport = transport
        await self._prepare_connection_auth_token()
        self.socket = await ws_connect(self._socket_url(transport))
        self._receive_task = asyncio.create_task(self._receive_loop())

    async def _receive_loop(self) -> None:
        try:
            assert self.socket is not None
            async for raw_message in self.socket:
                await self._handle_raw_message(raw_message)
        except ConnectionClosed as exc:
            await self._handle_socket_closed(exc.code, exc.reason)

    async def _handle_raw_message(self, raw_message: Union[str, bytes]) -> None:
        try:
            event = ProtocolCodec.decode_event(raw_message, self.options.wire_format)
            if not event.event:
                return
            if event.message_id and self._deduplicator:
                if self._deduplicator.is_duplicate(event.message_id):
                    return
                self._deduplicator.track(event.message_id)
            self._reset_activity_timer()
            if (
                self.options.connection_recovery
                and event.channel
                and event.serial is not None
            ):
                self._channel_positions[event.channel] = RecoveryPosition(
                    serial=event.serial,
                    stream_id=event.stream_id,
                    last_message_id=event.message_id,
                )
            event_name = event.event
            if event_name == self.prefix.event("connection_established"):
                payload = event.data if isinstance(event.data, dict) else {}
                self.socket_id = payload.get("socket_id")
                if not isinstance(self.socket_id, str):
                    raise SockudoException("Invalid handshake")
                self._update_state(
                    ConnectionState.CONNECTED, {"socket_id": self.socket_id}
                )
                for channel in self.channels.values():
                    channel.subscribe_if_possible()
                if self.options.connection_recovery and self._channel_positions:
                    await self.send_event(
                        self.prefix.event("resume"),
                        {
                            "channel_positions": {
                                channel_name: {
                                    key: value
                                    for key, value in {
                                        "serial": position.serial,
                                        "stream_id": position.stream_id,
                                        "last_message_id": position.last_message_id,
                                    }.items()
                                    if value is not None
                                }
                                for channel_name, position in self._channel_positions.items()
                            }
                        },
                        None,
                    )
                if (
                    self.options.delta_compression
                    and self.options.delta_compression.enabled
                ):
                    assert self._delta_manager is not None
                    await self._delta_manager.enable()
                self._schedule_auth_refresh()
                await self.user.handle_connected()
            elif event_name == self.prefix.event("error"):
                self.dispatcher.emit("error", event.data)
                if self._is_token_refresh_payload(event.data):
                    await self._refresh_auth_token()
            elif event_name == self.prefix.event("ping"):
                await self.send_event(self.prefix.event("pong"), {}, None)
            elif event_name == self.prefix.event("auth_success"):
                self._handle_auth_success(event.data)
                self.dispatcher.emit(event_name, event.data)
            elif event_name == self.prefix.event("token_expired"):
                self.dispatcher.emit(event_name, event.data)
                await self._refresh_auth_token()
            elif event_name == self.prefix.event("signin_success"):
                await self.user.handle_sign_in_success(event.data)
            elif event_name == self.prefix.event("resume_failed"):
                payload = event.data if isinstance(event.data, dict) else {}
                failed_channel_name = payload.get("channel")
                if isinstance(failed_channel_name, str):
                    self._channel_positions.pop(failed_channel_name, None)
                    if self._delta_manager is not None:
                        self._delta_manager.clear_channel_state(failed_channel_name)
                    failed_channel = self.channels.get(failed_channel_name)
                    if failed_channel is not None:
                        failed_channel.is_subscribed = False
                        failed_channel.subscription_pending = False
                        failed_channel.subscribe_if_possible()
                self.dispatcher.emit(event_name, event.data)
            elif event_name == self.prefix.internal("watchlist_events"):
                self.watchlist.handle(event.data)
            elif (
                event_name == self.prefix.event("delta_compression_enabled")
                and self._delta_manager
            ):
                self._delta_manager.handle_enabled(event.data)
                self.dispatcher.emit(event_name, event.data)
            elif (
                event_name == self.prefix.event("delta_cache_sync")
                and self._delta_manager
                and event.channel
            ):
                self._delta_manager.handle_cache_sync(event.channel, event.data)
            elif (
                event_name == self.prefix.event("delta")
                and self._delta_manager
                and event.channel
            ):
                reconstructed = await self._delta_manager.handle_delta_message(
                    event.channel, event.data
                )
                if reconstructed is not None:
                    channel = self.channels.get(event.channel)
                    if channel is not None:
                        channel.handle(reconstructed)
                    self.dispatcher.emit(reconstructed.event, reconstructed.data)
            else:
                if event.channel and event.channel in self.channels:
                    self.channels[event.channel].handle(event)
                    if (
                        not self.prefix.is_platform_event(event_name)
                        and not self.prefix.is_internal_event(event_name)
                        and event.sequence is not None
                        and self._delta_manager is not None
                    ):
                        self._delta_manager.handle_full_message(
                            event.channel,
                            self._strip_delta_metadata(event.raw_message),
                            event.sequence,
                            event.conflation_key,
                        )
                if not self.prefix.is_internal_event(event_name):
                    self.dispatcher.emit(
                        event_name, event.data, EventMetadata(user_id=event.user_id)
                    )
        except BaseException as exc:
            self.dispatcher.emit("error", exc)

    async def _handle_socket_closed(self, code: int, reason: str) -> None:
        self.socket = None
        self._cancel_activity_timer()
        self._clear_unavailable_timer()
        self._cancel_auth_refresh_task()
        for channel in self.channels.values():
            channel.disconnect()
        if not self._manually_disconnected:
            await self._schedule_retry(1.0)
        if reason:
            self.dispatcher.emit("error", reason)

    async def _schedule_retry(self, after_seconds: float) -> None:
        if self._manually_disconnected:
            return
        if self._retry_task:
            self._retry_task.cancel()

        async def _retry() -> None:
            await asyncio.sleep(after_seconds)
            self._update_state(ConnectionState.CONNECTING)
            transports = self._transport_sequence()
            if (
                self._current_transport is SockudoTransport.WS
                and not self._attempted_fallback
                and SockudoTransport.WSS in transports
            ):
                self._attempted_fallback = True
                await self._open_websocket(SockudoTransport.WSS)
            else:
                self._attempted_fallback = False
                await self._open_websocket(
                    transports[0] if transports else SockudoTransport.WSS
                )
            self._set_unavailable_timer()

        self._retry_task = asyncio.create_task(_retry())

    def _socket_url(self, transport: SockudoTransport) -> str:
        scheme = "wss" if transport is SockudoTransport.WSS else "ws"
        host = self.config.ws_host
        port = (
            self.config.wss_port
            if transport is SockudoTransport.WSS
            else self.config.ws_port
        )
        path = f"{self.config.ws_path}/app/{self.key}"
        query = {
            "protocol": self.prefix.version,
            "client": "python",
            "version": "2.0.0",
            "flash": "false",
        }
        if self.options.protocol_version >= 2:
            query["format"] = self.options.wire_format.value
            query["echo_messages"] = "true" if self.options.echo_messages else "false"
            query["append_mode"] = self.options.append_mode.value
            if self.options.append_rollup_window is not None:
                query["append_rollup_window"] = self.options.append_rollup_window
            if self._auth_token is not None:
                query["token"] = self._auth_token
        return urllib.parse.urlunsplit(
            (scheme, f"{host}:{port}", path, urllib.parse.urlencode(query), "")
        )

    async def _prepare_connection_auth_token(self) -> None:
        if self.options.auth_callback is None and self.options.token is None:
            return
        await self._resolve_auth_token()

    async def _refresh_auth_token(self) -> None:
        token = await self._resolve_auth_token()
        if token is None:
            return
        await self.send_event(self.prefix.event("auth"), {"token": token}, None)
        self._schedule_auth_refresh()

    async def _resolve_auth_token(self) -> Optional[str]:
        result: Optional[TokenAuthResult]
        if self.options.auth_callback is not None:
            value = self.options.auth_callback()
            result = await value if inspect.isawaitable(value) else value
        else:
            result = self.options.token
        token, expires_at, issued_at = self._normalize_auth_token(result)
        self._auth_token = token
        self._auth_token_expires_at = expires_at
        self._auth_token_issued_at = issued_at
        return token

    def _normalize_auth_token(
        self, result: Optional[TokenAuthResult]
    ) -> Tuple[Optional[str], Optional[float], Optional[float]]:
        if result is None:
            return None, None, None
        if isinstance(result, str):
            expires_at, issued_at = self._extract_token_times(result)
            return result, expires_at, issued_at
        token = result.token
        issued_at_ms = _coerce_int(result.issued_at_ms)
        issued_at = (
            issued_at_ms / 1000.0
            if issued_at_ms is not None
            else self._timestamp(None, result.issued_at, result.iat)
        )
        expires_at = self._timestamp(None, result.expires_at, result.exp)
        now = time.time()
        expires_at_ms = _coerce_int(result.expires_at_ms)
        if expires_at_ms is not None:
            expires_at = expires_at_ms / 1000.0
        elif isinstance(result.expires_in, (int, float)) and not isinstance(
            result.expires_in, bool
        ):
            if issued_at is None:
                issued_at = now
            expires_at = issued_at + float(result.expires_in)
        token_expires_at, token_issued_at = self._extract_token_times(token)
        if expires_at is None:
            expires_at = token_expires_at
        if issued_at is None:
            issued_at = token_issued_at
        return token, expires_at, issued_at

    def _handle_auth_success(self, data: Any) -> None:
        payload = data if isinstance(data, dict) else {}
        token = payload.get("token")
        if isinstance(token, str):
            self._auth_token = token
        issued_at_ms = _coerce_int(payload.get("issued_at_ms"))
        issued_at = (
            issued_at_ms / 1000.0
            if issued_at_ms is not None
            else self._timestamp(None, payload.get("issued_at"), payload.get("iat"))
        )
        expires_at = self._timestamp(
            None, payload.get("expires_at"), payload.get("exp")
        )
        expires_at_ms = _coerce_int(payload.get("expires_at_ms"))
        expires_in = payload.get("expires_in")
        if expires_at_ms is not None:
            expires_at = expires_at_ms / 1000.0
        elif isinstance(expires_in, (int, float)) and not isinstance(expires_in, bool):
            if issued_at is None:
                issued_at = time.time()
            expires_at = issued_at + float(expires_in)
        if expires_at is None and isinstance(token, str):
            expires_at, token_issued_at = self._extract_token_times(token)
            if issued_at is None:
                issued_at = token_issued_at
        if expires_at is not None:
            self._auth_token_expires_at = expires_at
        if issued_at is not None:
            self._auth_token_issued_at = issued_at
        self._schedule_auth_refresh()

    def _schedule_auth_refresh(self) -> None:
        self._cancel_auth_refresh_task()
        if (
            self.connection_state is not ConnectionState.CONNECTED
            or self._auth_token is None
        ):
            return
        delay = self._auth_refresh_delay()
        if delay is None:
            return

        async def _timer() -> None:
            await asyncio.sleep(delay)
            await self._refresh_auth_token()

        self._auth_refresh_task = asyncio.create_task(_timer())

    def _auth_refresh_delay(self, now: Optional[float] = None) -> Optional[float]:
        if self._auth_token_expires_at is None:
            return None
        current_time = time.time() if now is None else now
        issued_at = (
            self._auth_token_issued_at
            if self._auth_token_issued_at is not None
            else current_time
        )
        lifetime = self._auth_token_expires_at - issued_at
        if lifetime <= 0:
            return 1.0
        refresh_at = issued_at + (lifetime * 0.8)
        leeway = max(0.0, self.options.auth_refresh_leeway_seconds)
        latest_refresh_at = self._auth_token_expires_at - leeway
        if latest_refresh_at > current_time:
            refresh_at = min(refresh_at, latest_refresh_at)
        return max(1.0, refresh_at - current_time)

    def _cancel_auth_refresh_task(self) -> None:
        if self._auth_refresh_task:
            self._auth_refresh_task.cancel()
            self._auth_refresh_task = None

    @staticmethod
    def _extract_token_times(
        token: Optional[str],
    ) -> Tuple[Optional[float], Optional[float]]:
        if token is None or token.count(".") < 2:
            return None, None
        try:
            payload_part = token.split(".")[1]
            padded = payload_part + "=" * (-len(payload_part) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
        except (binascii.Error, UnicodeDecodeError, ValueError, json.JSONDecodeError):
            return None, None
        if not isinstance(payload, dict):
            return None, None
        return (
            SockudoClient._timestamp(None, payload.get("exp")),
            SockudoClient._timestamp(None, payload.get("iat")),
        )

    @staticmethod
    def _timestamp(*values: Any) -> Optional[float]:
        for value in values:
            coerced = _coerce_int(value)
            if coerced is not None:
                return float(coerced)
            if isinstance(value, float) and not isinstance(value, bool):
                return value
        return None

    @staticmethod
    def _is_token_refresh_payload(data: Any) -> bool:
        payload = data if isinstance(data, dict) else {}
        code = _coerce_int(payload.get("code"))
        return code in _TOKEN_REFRESH_CODES

    def _transport_sequence(self) -> List[SockudoTransport]:
        transports = (
            [SockudoTransport.WSS]
            if self.config.use_tls
            else [SockudoTransport.WS, SockudoTransport.WSS]
        )
        if self.config.enabled_transports is not None:
            transports = [
                transport
                for transport in transports
                if transport in self.config.enabled_transports
            ]
        if self.config.disabled_transports is not None:
            transports = [
                transport
                for transport in transports
                if transport not in self.config.disabled_transports
            ]
        return transports

    def _create_channel(self, name: str) -> SockudoChannel:
        if name.startswith("private-encrypted-"):
            return EncryptedChannel(name, self)
        if name.startswith("presence-"):
            return PresenceChannel(name, self)
        if name.startswith("private-"):
            return PrivateChannel(name, self)
        return SockudoChannel(name, self)

    def _update_state(
        self, state: ConnectionState, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        previous = self.connection_state
        self.connection_state = state
        self.dispatcher.emit(
            "state_change", {"previous": previous.value, "current": state.value}
        )
        self.dispatcher.emit(state.value, metadata)

    def _cancel_activity_timer(self) -> None:
        if self._activity_task:
            self._activity_task.cancel()
            self._activity_task = None

    def _reset_activity_timer(self) -> None:
        self._cancel_activity_timer()

        async def _timer() -> None:
            await asyncio.sleep(self.config.activity_timeout)
            await self.send_event(self.prefix.event("ping"), {}, None)

        self._activity_task = asyncio.create_task(_timer())

    def _set_unavailable_timer(self) -> None:
        self._clear_unavailable_timer()

        async def _timer() -> None:
            await asyncio.sleep(self.config.unavailable_timeout)
            self._update_state(ConnectionState.UNAVAILABLE)

        self._unavailable_task = asyncio.create_task(_timer())

    def _clear_unavailable_timer(self) -> None:
        if self._unavailable_task:
            self._unavailable_task.cancel()
            self._unavailable_task = None

    def _cancel_timers(self) -> None:
        self._cancel_activity_timer()
        self._clear_unavailable_timer()
        if self._retry_task:
            self._retry_task.cancel()
            self._retry_task = None
        self._cancel_auth_refresh_task()

    @staticmethod
    def _strip_delta_metadata(raw_message: str) -> str:
        return raw_message.replace(',"__delta_seq"', "").replace(
            ',"__conflation_key"', ""
        )

    def reset_delta_stats(self) -> None:
        if self._delta_manager is not None:
            self._delta_manager.reset_stats()

    class UserFacade:
        def __init__(self, client: "SockudoClient") -> None:
            self.client = client
            self.dispatcher = EventDispatcher()
            self.is_sign_in_requested = False
            self.user_data: Optional[Dict[str, Any]] = None
            self.server_channel: Optional[SockudoChannel] = None

        @property
        def user_id(self) -> Optional[str]:
            if self.user_data is None:
                return None
            value = self.user_data.get("id")
            return value if isinstance(value, str) else None

        def bind(
            self,
            event_name: str,
            callback: Callable[[Any, Optional[EventMetadata]], None],
        ) -> str:
            return self.dispatcher.bind(event_name, callback)

        async def sign_in(self) -> None:
            self.is_sign_in_requested = True
            await self._attempt_sign_in()

        async def handle_connected(self) -> None:
            await self._attempt_sign_in()

        async def handle_sign_in_success(self, data: Any) -> None:
            payload = data if isinstance(data, dict) else {}
            user_data = payload.get("user_data")
            if not isinstance(user_data, str):
                self._cleanup()
                return
            parsed = json.loads(user_data)
            if not isinstance(parsed, dict) or not isinstance(parsed.get("id"), str):
                self._cleanup()
                return
            self.user_data = parsed
            await self._subscribe_server_channel(parsed["id"])

        async def _attempt_sign_in(self) -> None:
            if (
                not self.is_sign_in_requested
                or self.client.connection_state is not ConnectionState.CONNECTED
            ):
                return
            if not self.client.socket_id:
                return
            try:
                auth = await self.client.config.authenticate_user(
                    UserAuthenticationRequest(self.client.socket_id)
                )
                await self.client.send_event(
                    self.client.prefix.event("signin"),
                    {"auth": auth.auth, "user_data": auth.user_data},
                    None,
                )
            except BaseException:
                self._cleanup()

        async def _subscribe_server_channel(self, user_id: str) -> None:
            channel = SockudoChannel(f"#server-to-user-{user_id}", self.client)
            channel.bind_global(
                lambda event_name, data: (
                    self.dispatcher.emit(event_name, data)
                    if not self.client.prefix.is_internal_event(event_name)
                    and not self.client.prefix.is_platform_event(event_name)
                    else None
                )
            )
            self.server_channel = channel
            channel.subscribe_if_possible()

        def _cleanup(self) -> None:
            self.user_data = None
            if self.server_channel is not None:
                self.server_channel.unbind()
                self.server_channel.disconnect()
                self.server_channel = None

    class WatchlistFacade:
        def __init__(self) -> None:
            self.dispatcher = EventDispatcher()

        def bind(
            self,
            event_name: str,
            callback: Callable[[Any, Optional[EventMetadata]], None],
        ) -> str:
            return self.dispatcher.bind(event_name, callback)

        def handle(self, data: Any) -> None:
            payload = data if isinstance(data, dict) else {}
            events = payload.get("events", [])
            if not isinstance(events, list):
                return
            for event in events:
                if isinstance(event, dict) and isinstance(event.get("name"), str):
                    self.dispatcher.emit(event["name"], event)


_MAX_SAFE_FLOAT_INT = 2**53 - 1


def _coerce_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("-"):
            return int(text) if text[1:].isdigit() else None
        return int(text) if text.isdigit() else None
    if isinstance(value, float):
        if value.is_integer() and abs(value) <= _MAX_SAFE_FLOAT_INT:
            return int(value)
        return None
    return None


def _write_varint(buffer: bytearray, value: int) -> None:
    while True:
        if value < 0x80:
            buffer.append(value)
            return
        buffer.append((value & 0x7F) | 0x80)
        value >>= 7


def _write_key(buffer: bytearray, field: int, wire_type: int) -> None:
    _write_varint(buffer, (field << 3) | wire_type)


def _write_string_field(buffer: bytearray, field: int, value: Any) -> None:
    if not isinstance(value, str):
        return
    encoded = value.encode("utf-8")
    _write_key(buffer, field, 2)
    _write_varint(buffer, len(encoded))
    buffer.extend(encoded)


def _write_bytes_field(buffer: bytearray, field: int, payload: bytes) -> None:
    _write_key(buffer, field, 2)
    _write_varint(buffer, len(payload))
    buffer.extend(payload)


def _write_uint_field(buffer: bytearray, field: int, value: Any) -> None:
    coerced = _coerce_int(value)
    if coerced is None:
        return
    _write_key(buffer, field, 0)
    _write_varint(buffer, coerced)


def _write_optional_bool_field(
    buffer: bytearray, field: int, value: Optional[bool]
) -> None:
    if value is None:
        return
    _write_key(buffer, field, 0)
    _write_varint(buffer, 1 if value else 0)


def _write_bool_field(buffer: bytearray, field: int, value: bool) -> None:
    _write_key(buffer, field, 0)
    _write_varint(buffer, 1 if value else 0)


def _write_double_field(buffer: bytearray, field: int, value: float) -> None:
    _write_key(buffer, field, 1)
    buffer.extend(struct.pack("<d", value))


def _read_varint(data: bytes, index: int) -> Tuple[int, int]:
    shift = 0
    result = 0
    while True:
        byte = data[index]
        index += 1
        result |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            return result, index
        shift += 7


def _read_length_delimited(data: bytes, index: int) -> Tuple[bytes, int]:
    length, index = _read_varint(data, index)
    return data[index : index + length], index + length


def _skip_unknown(data: bytes, index: int, wire: int) -> int:
    if wire == 0:
        _, index = _read_varint(data, index)
        return index
    if wire == 1:
        return index + 8
    if wire == 2:
        payload, index = _read_length_delimited(data, index)
        return index
    if wire == 5:
        return index + 4
    raise SockudoException(f"Unsupported protobuf wire type: {wire}")
