from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)
from urllib.parse import quote, urlencode, urlparse

import httpx
from nacl import exceptions as nacl_exceptions
from nacl import secret as nacl_secret
from nacl import utils as nacl_utils


JsonDict = Dict[str, Any]
JsonMarshaller = Callable[[Any], str]
ENCRYPTED_CHANNEL_PREFIX = "private-encrypted-"

RESERVED_QUERY_KEYS = {
    "auth_key",
    "auth_timestamp",
    "auth_version",
    "auth_signature",
    "body_md5",
}


class SockudoError(RuntimeError):
    pass


class Status(str, Enum):
    SUCCESS = "success"
    CLIENT_ERROR = "client_error"
    AUTHENTICATION_ERROR = "authentication_error"
    SERVER_ERROR = "server_error"
    NETWORK_ERROR = "network_error"

    def should_retry(self) -> bool:
        return self in {Status.SERVER_ERROR, Status.NETWORK_ERROR}


class Validity(str, Enum):
    VALID = "valid"
    INVALID = "invalid"
    SIGNED_WITH_WRONG_KEY = "signed_with_wrong_key"


@dataclass(frozen=True)
class Result:
    status: Status
    message: str
    status_code: Optional[int] = None

    @property
    def ok(self) -> bool:
        return self.status is Status.SUCCESS

    @staticmethod
    def from_response(response: httpx.Response) -> "Result":
        if 200 <= response.status_code < 300:
            status = Status.SUCCESS
        elif response.status_code in {401, 403}:
            status = Status.AUTHENTICATION_ERROR
        elif 400 <= response.status_code < 500:
            status = Status.CLIENT_ERROR
        else:
            status = Status.SERVER_ERROR
        return Result(
            status=status, message=response.text, status_code=response.status_code
        )

    @staticmethod
    def from_exception(exc: Exception) -> "Result":
        return Result(status=Status.NETWORK_ERROR, message=str(exc), status_code=None)

    def json(self) -> Any:
        return json.loads(self.message) if self.message else None


@dataclass
class SockudoOptions:
    host: str = "127.0.0.1"
    port: int = 6001
    use_tls: bool = False
    timeout: float = 4.0
    max_retries: int = 3
    retry_base_delay: float = 0.1
    auto_idempotency: bool = False
    http2: bool = True
    verify_tls: Union[bool, str] = True


@dataclass
class Config:
    app_id: str
    key: str
    secret: str
    host: str = "127.0.0.1"
    port: int = 6001
    use_tls: bool = False
    timeout: float = 4.0
    max_retries: int = 3
    retry_base_delay: float = 0.1
    auto_idempotency: bool = False
    http2: bool = True
    verify_tls: Union[bool, str] = True
    encryption_master_key_base64: Optional[str] = None

    def options(self) -> SockudoOptions:
        return SockudoOptions(
            host=self.host,
            port=self.port,
            use_tls=self.use_tls,
            timeout=self.timeout,
            max_retries=self.max_retries,
            retry_base_delay=self.retry_base_delay,
            auto_idempotency=self.auto_idempotency,
            http2=self.http2,
            verify_tls=self.verify_tls,
        )


@dataclass
class MessageExtras:
    headers: Optional[Dict[str, Any]] = None
    ephemeral: Optional[bool] = None
    idempotency_key: Optional[str] = None
    echo: Optional[bool] = None
    tags: Optional[Dict[str, str]] = None

    def to_payload(self) -> JsonDict:
        payload: JsonDict = {}
        if self.headers is not None:
            payload["headers"] = self.headers
        if self.ephemeral is not None:
            payload["ephemeral"] = self.ephemeral
        if self.idempotency_key is not None:
            payload["idempotency_key"] = self.idempotency_key
        if self.echo is not None:
            payload["echo"] = self.echo
        if self.tags is not None:
            payload["tags"] = self.tags
        return payload


@dataclass
class TriggerOptions:
    socket_id: Optional[str] = None
    info: Optional[Union[str, Sequence[str]]] = None
    idempotency_key: Optional[Union[str, bool]] = None
    extras: Optional[MessageExtras] = None
    tags: Optional[Dict[str, str]] = None


@dataclass
class Event:
    channel: Optional[str]
    name: str
    data: Any
    channels: Optional[Sequence[str]] = None
    socket_id: Optional[str] = None
    info: Optional[Union[str, Sequence[str]]] = None
    idempotency_key: Optional[Union[str, bool]] = None
    extras: Optional[MessageExtras] = None
    tags: Optional[Dict[str, str]] = None

    def to_payload(self, marshaller: JsonMarshaller) -> JsonDict:
        payload: JsonDict = {
            "name": self.name,
            "data": _encode_data(self.data, marshaller),
        }
        if self.channels is not None:
            payload["channels"] = list(self.channels)
        elif self.channel is not None:
            payload["channel"] = self.channel
        else:
            raise SockudoError("Event requires channel or channels")
        _apply_common_event_fields(
            payload,
            self.socket_id,
            self.info,
            self.idempotency_key,
            self.extras,
            self.tags,
        )
        return payload


@dataclass
class PresenceUser:
    user_id: str
    user_info: Optional[Mapping[str, Any]] = None

    def channel_data(self, marshaller: JsonMarshaller) -> str:
        return marshaller(
            {"user_id": self.user_id, "user_info": dict(self.user_info or {})}
        )


@dataclass(frozen=True)
class AuthData:
    auth: str
    channel_data: Optional[str] = None
    shared_secret: Optional[str] = None

    def to_json(self) -> str:
        payload: JsonDict = {"auth": self.auth}
        if self.channel_data is not None:
            payload["channel_data"] = self.channel_data
        if self.shared_secret is not None:
            payload["shared_secret"] = self.shared_secret
        return json.dumps(payload, separators=(",", ":"))


@dataclass
class ChannelsParams:
    filter_by_prefix: Optional[str] = None
    info: Optional[Union[str, Sequence[str]]] = None

    def to_query(self) -> Dict[str, str]:
        return _clean_query(
            {"filter_by_prefix": self.filter_by_prefix, "info": _join_info(self.info)}
        )


@dataclass
class ChannelParams:
    info: Optional[Union[str, Sequence[str]]] = None

    def to_query(self) -> Dict[str, str]:
        return _clean_query({"info": _join_info(self.info)})


@dataclass
class HistoryParams:
    limit: Optional[int] = None
    direction: Optional[str] = None
    cursor: Optional[str] = None
    start_serial: Optional[int] = None
    end_serial: Optional[int] = None
    start_time_ms: Optional[int] = None
    end_time_ms: Optional[int] = None

    def to_query(self) -> Dict[str, str]:
        return _clean_query(self.__dict__)


@dataclass
class PresenceHistoryParams(HistoryParams):
    pass


@dataclass
class PresenceSnapshotParams:
    at_serial: Optional[int] = None
    at_time_ms: Optional[int] = None

    def to_query(self) -> Dict[str, str]:
        return _clean_query(self.__dict__)


@dataclass
class MessageVersionsParams:
    limit: Optional[int] = None
    direction: Optional[str] = None
    cursor: Optional[str] = None

    def to_query(self) -> Dict[str, str]:
        return _clean_query(self.__dict__)


@dataclass
class PushCursorParams:
    limit: Optional[int] = None
    cursor: Optional[str] = None

    def to_query(self) -> Dict[str, str]:
        return _clean_query(self.__dict__)


@dataclass
class PushSubscriptionParams(PushCursorParams):
    channel: Optional[str] = None
    device_id: Optional[str] = None

    def to_query(self) -> Dict[str, str]:
        return _clean_query(
            {
                "limit": self.limit,
                "cursor": self.cursor,
                "channel": self.channel,
                "deviceId": self.device_id,
            }
        )


@dataclass
class MessageMutation:
    name: Optional[str] = None
    data: Optional[Any] = None
    extras: Optional[Mapping[str, Any]] = None
    clear_fields: Optional[Sequence[str]] = None
    client_id: Optional[str] = None
    socket_id: Optional[str] = None
    description: Optional[str] = None
    metadata: Optional[Mapping[str, Any]] = None

    def to_payload(self, marshaller: JsonMarshaller) -> JsonDict:
        payload = _clean_raw(
            {
                "name": self.name,
                "data": _encode_data(self.data, marshaller)
                if self.data is not None
                else None,
                "extras": dict(self.extras) if self.extras is not None else None,
                "clear_fields": list(self.clear_fields)
                if self.clear_fields is not None
                else None,
                "client_id": self.client_id,
                "socket_id": self.socket_id,
                "description": self.description,
                "metadata": dict(self.metadata) if self.metadata is not None else None,
            }
        )
        if not payload:
            raise SockudoError("Message mutation requires at least one field")
        return payload


@dataclass
class PublishAnnotationRequest:
    type: str
    name: Optional[str] = None
    client_id: Optional[str] = None
    socket_id: Optional[str] = None
    count: Optional[int] = None
    data: Optional[Any] = None
    encoding: Optional[str] = None

    def to_payload(self) -> JsonDict:
        return _clean_raw(
            {
                "type": self.type,
                "name": self.name,
                "clientId": self.client_id,
                "socketId": self.socket_id,
                "count": self.count,
                "data": self.data,
                "encoding": self.encoding,
            }
        )


@dataclass
class AnnotationEventsParams:
    type: Optional[str] = None
    from_serial: Optional[str] = None
    limit: Optional[int] = None
    socket_id: Optional[str] = None

    def to_query(self) -> Dict[str, str]:
        return _clean_query(self.__dict__)


@dataclass(frozen=True)
class WebhookEvent:
    name: str
    channel: Optional[str] = None
    event: Optional[str] = None
    data: Optional[Any] = None
    socket_id: Optional[str] = None
    user_id: Optional[str] = None


@dataclass(frozen=True)
class Webhook:
    time_ms: Optional[int]
    events: Tuple[WebhookEvent, ...] = field(default_factory=tuple)

    @staticmethod
    def parse(body: Union[str, bytes]) -> "Webhook":
        text = body.decode("utf-8") if isinstance(body, bytes) else body
        raw = json.loads(text)
        events = tuple(WebhookEvent(**event) for event in raw.get("events", []))
        return Webhook(time_ms=raw.get("time_ms"), events=events)


def body_md5(body: str) -> str:
    return hashlib.md5(body.encode("utf-8")).hexdigest()


def sign(input_value: str, secret: str) -> str:
    return hmac.new(
        secret.encode("utf-8"), input_value.encode("utf-8"), hashlib.sha256
    ).hexdigest()


class _BaseSockudo:
    def __init__(
        self,
        app_id: Union[str, Config],
        key: Optional[str] = None,
        secret: Optional[str] = None,
        encryption_master_key_base64: Optional[str] = None,
        options: Optional[SockudoOptions] = None,
    ) -> None:
        if isinstance(app_id, Config):
            config = app_id
            app_id = config.app_id
            key = config.key
            secret = config.secret
            encryption_master_key_base64 = (
                encryption_master_key_base64 or config.encryption_master_key_base64
            )
            options = options or config.options()
        if not app_id or not key or not secret:
            raise SockudoError("app_id, key, and secret are required")
        self.app_id = app_id
        self.key = key
        self.secret = secret
        self.options = options or SockudoOptions()
        self.encryption_master_key_base64 = encryption_master_key_base64
        self._encryption_master_key: Optional[bytes] = None
        self._marshaller: JsonMarshaller = _default_marshaller
        self._idempotency_base = uuid.uuid4().hex
        self._idempotency_serial = 0
        if encryption_master_key_base64 is not None:
            try:
                decoded = base64.b64decode(encryption_master_key_base64, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise SockudoError(
                    "encryption_master_key_base64 must be valid base64"
                ) from exc
            if len(decoded) != 32:
                raise SockudoError(
                    "encryption_master_key_base64 must decode to 32 bytes"
                )
            self._encryption_master_key = decoded

    @classmethod
    def from_url(cls, url: str) -> "_BaseSockudo":
        parsed = urlparse(url)
        if not parsed.username or not parsed.password:
            raise SockudoError("URL must include key and secret credentials")
        path_parts = [part for part in parsed.path.split("/") if part]
        if len(path_parts) != 2 or path_parts[0] != "apps":
            raise SockudoError("URL path must be /apps/{app_id}")
        options = SockudoOptions(
            host=parsed.hostname or "127.0.0.1",
            port=parsed.port or (443 if parsed.scheme == "https" else 80),
            use_tls=parsed.scheme == "https",
        )
        return cls(path_parts[1], parsed.username, parsed.password, options=options)  # type: ignore[return-value]

    def set_host(self, host: str) -> None:
        self.options.host = host

    def set_port(self, port: int) -> None:
        self.options.port = port

    def set_encrypted(self, encrypted: bool) -> None:
        self.options.use_tls = encrypted

    def set_auto_idempotency_key(self, auto: bool) -> None:
        self.options.auto_idempotency = auto

    def set_data_marshaller(self, marshaller: JsonMarshaller) -> None:
        self._marshaller = marshaller

    @property
    def base_url(self) -> str:
        scheme = "https" if self.options.use_tls else "http"
        return f"{scheme}://{self.options.host}:{self.options.port}"

    def signed_uri(
        self,
        method: str,
        path: str,
        body: Optional[str] = None,
        parameters: Optional[Mapping[str, Any]] = None,
    ) -> str:
        params = _clean_query(parameters or {})
        reserved = RESERVED_QUERY_KEYS.intersection(params)
        if reserved:
            raise SockudoError(
                f"Reserved signing parameters are not allowed: {sorted(reserved)}"
            )
        params.update(
            {
                "auth_key": self.key,
                "auth_timestamp": str(int(time.time())),
                "auth_version": "1.0",
            }
        )
        if body is not None:
            params["body_md5"] = body_md5(body)
        signature_string = _signature_string(method.upper(), path, params)
        params["auth_signature"] = sign(signature_string, self.secret)
        return f"{self.base_url}{path}?{urlencode(params)}"

    def authenticate(
        self, socket_id: str, channel: str, presence_user: Optional[PresenceUser] = None
    ) -> str:
        channel_data = (
            presence_user.channel_data(self._marshaller)
            if presence_user is not None
            else None
        )
        string_to_sign = (
            f"{socket_id}:{channel}"
            if channel_data is None
            else f"{socket_id}:{channel}:{channel_data}"
        )
        shared_secret = None
        if _is_encrypted_channel(channel):
            shared_secret = base64.b64encode(
                self.channel_shared_secret(channel)
            ).decode("ascii")
        return AuthData(
            auth=f"{self.key}:{sign(string_to_sign, self.secret)}",
            channel_data=channel_data,
            shared_secret=shared_secret,
        ).to_json()

    def authenticate_user(self, socket_id: str, user_data: Mapping[str, Any]) -> str:
        user_data_json = self._marshaller(dict(user_data))
        string_to_sign = f"{socket_id}::user::{user_data_json}"
        return json.dumps(
            {
                "auth": f"{self.key}:{sign(string_to_sign, self.secret)}",
                "user_data": user_data_json,
            },
            separators=(",", ":"),
        )

    def validate_webhook_signature(
        self, key_header: str, signature_header: str, body: Union[str, bytes]
    ) -> Validity:
        if key_header != self.key:
            return Validity.SIGNED_WITH_WRONG_KEY
        raw = body.decode("utf-8") if isinstance(body, bytes) else body
        expected = sign(raw, self.secret)
        return (
            Validity.VALID
            if hmac.compare_digest(expected, signature_header)
            else Validity.INVALID
        )

    def parse_webhook(
        self, key_header: str, signature_header: str, body: Union[str, bytes]
    ) -> Webhook:
        if (
            self.validate_webhook_signature(key_header, signature_header, body)
            is not Validity.VALID
        ):
            raise SockudoError("Invalid webhook signature")
        webhook = Webhook.parse(body)
        return self._decrypt_webhook(webhook)

    def channel_shared_secret(self, channel: str) -> bytes:
        if not _is_encrypted_channel(channel):
            raise SockudoError(
                f"Encrypted channel name must start with {ENCRYPTED_CHANNEL_PREFIX!r}"
            )
        if self._encryption_master_key is None:
            raise SockudoError(
                "Cannot generate shared_secret because encryption_master_key_base64 is not set"
            )
        return hashlib.sha256(
            channel.encode("utf-8") + self._encryption_master_key
        ).digest()

    def _next_idempotency_key(self) -> str:
        self._idempotency_serial += 1
        return f"{self._idempotency_base}:{self._idempotency_serial}"

    def _event_payload(
        self,
        channels: Union[str, Sequence[str]],
        event: str,
        data: Any,
        options: Optional[TriggerOptions],
    ) -> Tuple[JsonDict, Dict[str, str]]:
        channel_list = [channels] if isinstance(channels, str) else list(channels)
        if not channel_list:
            raise SockudoError("At least one channel is required")
        if len(channel_list) > 100:
            raise SockudoError("Sockudo supports at most 100 channels per trigger")
        encrypted_channels = [
            channel for channel in channel_list if _is_encrypted_channel(channel)
        ]
        if encrypted_channels and len(channel_list) > 1:
            raise SockudoError(
                "You cannot trigger to multiple channels when using encrypted channels"
            )
        opts = options or TriggerOptions()
        idempotency_key = _resolve_idempotency_key(opts.idempotency_key)
        if idempotency_key is None and self.options.auto_idempotency:
            idempotency_key = self._next_idempotency_key()
        if encrypted_channels:
            payload: JsonDict = {
                "name": event,
                "data": self._encrypt_payload(channel_list[0], data),
                "channels": channel_list,
            }
        else:
            payload = {"name": event, "data": _encode_data(data, self._marshaller)}
            payload["channel"] = channel_list[0]
            if len(channel_list) > 1:
                payload.pop("channel")
                payload["channels"] = channel_list
        _apply_common_event_fields(
            payload, opts.socket_id, opts.info, idempotency_key, opts.extras, opts.tags
        )
        headers = {"X-Idempotency-Key": idempotency_key} if idempotency_key else {}
        return payload, headers

    def _batch_payload(self, batch: Sequence[Event]) -> JsonDict:
        batch_serial: Optional[int] = None
        items: List[JsonDict] = []
        for index, event in enumerate(batch):
            payload = self._event_to_payload(event)
            idempotency_key = _resolve_idempotency_key(payload.get("idempotency_key"))
            if idempotency_key is None and self.options.auto_idempotency:
                if batch_serial is None:
                    self._idempotency_serial += 1
                    batch_serial = self._idempotency_serial
                idempotency_key = f"{self._idempotency_base}:{batch_serial}:{index}"
            if idempotency_key is not None:
                payload["idempotency_key"] = idempotency_key
            items.append(payload)
        return {"batch": items}

    def _event_to_payload(self, event: Event) -> JsonDict:
        payload = event.to_payload(self._marshaller)
        channel = event.channel
        if event.channels is not None:
            encrypted_channels = [
                item for item in event.channels if _is_encrypted_channel(item)
            ]
            if encrypted_channels and len(event.channels) > 1:
                raise SockudoError(
                    "You cannot trigger to multiple channels when using encrypted channels"
                )
            if len(event.channels) == 1:
                channel = event.channels[0]
        if channel is not None and _is_encrypted_channel(channel):
            payload["data"] = self._encrypt_payload(channel, event.data)
        return payload

    def _encrypt_payload(self, channel: str, data: Any) -> str:
        plaintext = self._marshaller(data).encode("utf-8")
        box = nacl_secret.SecretBox(self.channel_shared_secret(channel))
        nonce = nacl_utils.random(nacl_secret.SecretBox.NONCE_SIZE)
        encrypted = box.encrypt(plaintext, nonce)
        return json.dumps(
            {
                "nonce": base64.b64encode(encrypted.nonce).decode("ascii"),
                "ciphertext": base64.b64encode(encrypted.ciphertext).decode("ascii"),
            },
            separators=(",", ":"),
        )

    def _decrypt_webhook(self, webhook: Webhook) -> Webhook:
        events = tuple(self._decrypt_webhook_event(event) for event in webhook.events)
        return Webhook(time_ms=webhook.time_ms, events=events)

    def _decrypt_webhook_event(self, event: WebhookEvent) -> WebhookEvent:
        if not event.channel or not _is_encrypted_channel(event.channel):
            return event
        if not isinstance(event.data, str):
            raise SockudoError("Encrypted webhook event data must be a string")
        try:
            encrypted = json.loads(event.data)
            nonce = base64.b64decode(encrypted["nonce"], validate=True)
            ciphertext = base64.b64decode(encrypted["ciphertext"], validate=True)
            plaintext = nacl_secret.SecretBox(
                self.channel_shared_secret(event.channel)
            ).decrypt(ciphertext, nonce)
        except (
            KeyError,
            TypeError,
            ValueError,
            binascii.Error,
            nacl_exceptions.CryptoError,
        ) as exc:
            raise SockudoError("Failed to decrypt encrypted webhook event") from exc
        return WebhookEvent(
            name=event.name,
            channel=event.channel,
            event=event.event,
            data=plaintext.decode("utf-8"),
            socket_id=event.socket_id,
            user_id=event.user_id,
        )

    def _serialize(self, payload: Mapping[str, Any]) -> str:
        return json.dumps(payload, separators=(",", ":"), sort_keys=False)

    def _path(self, suffix: str) -> str:
        return f"/apps/{quote(self.app_id, safe='')}{suffix}"

    def _channel_path(self, channel: str, suffix: str = "") -> str:
        return self._path(f"/channels/{quote(channel, safe='')}{suffix}")

    def _push_headers(
        self,
        capability: str = "push-admin",
        device_identity_token: Optional[str] = None,
    ) -> Dict[str, str]:
        headers = {"X-Sockudo-Push-Capability": capability}
        if device_identity_token is not None:
            headers["X-Sockudo-Device-Identity-Token"] = device_identity_token
        return headers

    def _push_path(self, suffix: str) -> str:
        return self._path(f"/push{suffix}")


class Sockudo(_BaseSockudo):
    def __init__(
        self,
        app_id: Union[str, Config],
        key: Optional[str] = None,
        secret: Optional[str] = None,
        encryption_master_key_base64: Optional[str] = None,
        options: Optional[SockudoOptions] = None,
        client: Optional[httpx.Client] = None,
    ) -> None:
        super().__init__(app_id, key, secret, encryption_master_key_base64, options)
        self._client = client or _make_sync_client(self.options)
        self._owns_client = client is None

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "Sockudo":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def trigger(
        self,
        channels: Union[str, Sequence[str]],
        event: str,
        data: Any,
        options: Optional[TriggerOptions] = None,
    ) -> Result:
        payload, headers = self._event_payload(channels, event, data, options)
        return self._post(self._path("/events"), payload, headers=headers)

    def trigger_batch(self, batch: Sequence[Event]) -> Result:
        return self._post(self._path("/batch_events"), self._batch_payload(batch))

    def send_to_user(
        self,
        user_id: str,
        event: str,
        data: Any,
        options: Optional[TriggerOptions] = None,
    ) -> Result:
        if not user_id:
            raise SockudoError("user_id is required")
        return self.trigger(f"#server-to-user-{user_id}", event, data, options)

    def get(self, path: str, params: Optional[Mapping[str, Any]] = None) -> Result:
        full_path = (
            path
            if path.startswith("/apps/")
            else self._path(path if path.startswith("/") else f"/{path}")
        )
        return self._request("GET", full_path, None, params=params)

    def list_channels(self, params: Optional[ChannelsParams] = None) -> Result:
        return self.get("/channels", params.to_query() if params else None)

    def get_channel(
        self, channel: str, params: Optional[ChannelParams] = None
    ) -> Result:
        return self.get(
            self._channel_path(channel), params.to_query() if params else None
        )

    def get_channel_users(self, channel: str) -> Result:
        return self.get(self._channel_path(channel, "/users"))

    def get_channel_history(
        self, channel: str, params: Optional[HistoryParams] = None
    ) -> Result:
        return self.get(
            self._channel_path(channel, "/history"),
            params.to_query() if params else None,
        )

    def get_channel_history_state(self, channel: str) -> Result:
        return self.get(self._channel_path(channel, "/history/state"))

    def reset_channel_history(
        self, channel: str, reason: str, requested_by: Optional[str] = None
    ) -> Result:
        return self._post(
            self._channel_path(channel, "/history/reset"),
            _operator_payload(channel, "reset", reason, requested_by),
        )

    def purge_channel_history(
        self,
        channel: str,
        mode: str,
        reason: str,
        requested_by: Optional[str] = None,
        before_serial: Optional[int] = None,
        before_time_ms: Optional[int] = None,
    ) -> Result:
        payload = _operator_payload(channel, "purge", reason, requested_by)
        payload.update(
            _clean_raw(
                {
                    "mode": mode,
                    "before_serial": before_serial,
                    "before_time_ms": before_time_ms,
                }
            )
        )
        return self._post(self._channel_path(channel, "/history/purge"), payload)

    def get_channel_presence_history(
        self, channel: str, params: Optional[PresenceHistoryParams] = None
    ) -> Result:
        return self.get(
            self._channel_path(channel, "/presence/history"),
            params.to_query() if params else None,
        )

    def get_channel_presence_history_state(self, channel: str) -> Result:
        return self.get(self._channel_path(channel, "/presence/history/state"))

    def reset_channel_presence_history(
        self, channel: str, reason: str, requested_by: Optional[str] = None
    ) -> Result:
        return self._post(
            self._channel_path(channel, "/presence/history/reset"),
            _operator_payload(channel, "reset", reason, requested_by),
        )

    def get_channel_presence_snapshot(
        self, channel: str, params: Optional[PresenceSnapshotParams] = None
    ) -> Result:
        return self.get(
            self._channel_path(channel, "/presence/history/snapshot"),
            params.to_query() if params else None,
        )

    def get_message(self, channel: str, message_serial: str) -> Result:
        return self.get(self._message_path(channel, message_serial))

    def get_message_versions(
        self,
        channel: str,
        message_serial: str,
        params: Optional[MessageVersionsParams] = None,
    ) -> Result:
        return self.get(
            f"{self._message_path(channel, message_serial)}/versions",
            params.to_query() if params else None,
        )

    def update_message(
        self,
        channel: str,
        message_serial: str,
        mutation: Union[MessageMutation, Mapping[str, Any]],
    ) -> Result:
        return self._post(
            f"{self._message_path(channel, message_serial)}/update",
            _mutation_payload(mutation, self._marshaller),
        )

    def delete_message(
        self,
        channel: str,
        message_serial: str,
        mutation: Optional[Union[MessageMutation, Mapping[str, Any]]] = None,
    ) -> Result:
        return self._post(
            f"{self._message_path(channel, message_serial)}/delete",
            _mutation_payload(mutation, self._marshaller)
            if mutation is not None
            else {},
        )

    def append_message(
        self,
        channel: str,
        message_serial: str,
        data: str,
        socket_id: Optional[str] = None,
    ) -> Result:
        if not data:
            raise SockudoError("append_message data must be non-empty")
        return self._post(
            f"{self._message_path(channel, message_serial)}/append",
            _clean_raw({"data": data, "socket_id": socket_id}),
        )

    def publish_annotation(
        self,
        channel: str,
        message_serial: str,
        request: Union[PublishAnnotationRequest, Mapping[str, Any]],
    ) -> Result:
        payload = (
            request.to_payload()
            if isinstance(request, PublishAnnotationRequest)
            else dict(request)
        )
        return self._post(
            f"{self._message_path(channel, message_serial)}/annotations", payload
        )

    def list_annotations(
        self,
        channel: str,
        message_serial: str,
        params: Optional[AnnotationEventsParams] = None,
    ) -> Result:
        return self.get(
            f"{self._message_path(channel, message_serial)}/annotations",
            params.to_query() if params else None,
        )

    def delete_annotation(
        self,
        channel: str,
        message_serial: str,
        annotation_serial: str,
        socket_id: Optional[str] = None,
    ) -> Result:
        return self._request(
            "DELETE",
            f"{self._message_path(channel, message_serial)}/annotations/{quote(annotation_serial, safe='')}",
            None,
            params=_clean_query({"socket_id": socket_id}),
        )

    def terminate_user_connections(self, user_id: str) -> Result:
        return self._post(
            self._path(f"/users/{quote(user_id, safe='')}/terminate_connections"), {}
        )

    def activate_device(
        self, device: Mapping[str, Any], rotate_device_identity_token: bool = False
    ) -> Result:
        headers = self._push_headers("push-admin")
        if rotate_device_identity_token:
            headers["X-Sockudo-Rotate-Device-Identity-Token"] = "true"
        return self._post(
            self._push_path("/deviceRegistrations"), dict(device), headers=headers
        )

    def create_device_activation(self, device: Mapping[str, Any]) -> Result:
        return self.activate_device(device)

    def update_device_registration(
        self, device: Mapping[str, Any], device_identity_token: str
    ) -> Result:
        return self._post(
            self._push_path("/deviceRegistrations"),
            dict(device),
            headers=self._push_headers("push-subscribe", device_identity_token),
        )

    def list_device_registrations(
        self, params: Optional[PushCursorParams] = None
    ) -> Result:
        return self._request(
            "GET",
            self._push_path("/deviceRegistrations"),
            None,
            params=params.to_query() if params else None,
            headers=self._push_headers("push-admin"),
        )

    def get_device_registration(
        self, device_id: str, device_identity_token: Optional[str] = None
    ) -> Result:
        headers = (
            self._push_headers("push-subscribe", device_identity_token)
            if device_identity_token
            else self._push_headers("push-admin")
        )
        return self._request(
            "GET",
            self._push_path(f"/deviceRegistrations/{quote(device_id, safe='')}"),
            None,
            headers=headers,
        )

    def delete_device_registration(
        self, device_id: str, device_identity_token: Optional[str] = None
    ) -> Result:
        headers = (
            self._push_headers("push-subscribe", device_identity_token)
            if device_identity_token
            else self._push_headers("push-admin")
        )
        return self._request(
            "DELETE",
            self._push_path(f"/deviceRegistrations/{quote(device_id, safe='')}"),
            None,
            headers=headers,
        )

    def remove_device_registrations_by_client(self, client_id: str) -> Result:
        return self._request(
            "DELETE",
            self._push_path("/deviceRegistrations"),
            None,
            params={"clientId": client_id},
            headers=self._push_headers("push-admin"),
        )

    def upsert_channel_push_subscription(
        self,
        subscription: Mapping[str, Any],
        device_identity_token: Optional[str] = None,
    ) -> Result:
        headers = (
            self._push_headers("push-subscribe", device_identity_token)
            if device_identity_token
            else self._push_headers("push-admin")
        )
        return self._post(
            self._push_path("/channelSubscriptions"),
            dict(subscription),
            headers=headers,
        )

    def list_channel_push_subscriptions(
        self,
        params: Optional[PushSubscriptionParams] = None,
        device_identity_token: Optional[str] = None,
    ) -> Result:
        headers = (
            self._push_headers("push-subscribe", device_identity_token)
            if device_identity_token
            else self._push_headers("push-admin")
        )
        return self._request(
            "GET",
            self._push_path("/channelSubscriptions"),
            None,
            params=params.to_query() if params else None,
            headers=headers,
        )

    def delete_channel_push_subscriptions(
        self,
        params: PushSubscriptionParams,
        device_identity_token: Optional[str] = None,
    ) -> Result:
        headers = (
            self._push_headers("push-subscribe", device_identity_token)
            if device_identity_token
            else self._push_headers("push-admin")
        )
        return self._request(
            "DELETE",
            self._push_path("/channelSubscriptions"),
            None,
            params=params.to_query(),
            headers=headers,
        )

    def list_channel_push_subscription_channels(
        self, params: Optional[PushCursorParams] = None
    ) -> Result:
        return self._request(
            "GET",
            self._push_path("/channelSubscriptions/channels"),
            None,
            params=params.to_query() if params else None,
            headers=self._push_headers("push-admin"),
        )

    def list_push_credentials(
        self, params: Optional[PushCursorParams] = None
    ) -> Result:
        return self._request(
            "GET",
            self._push_path("/credentials"),
            None,
            params=params.to_query() if params else None,
            headers=self._push_headers("push-admin"),
        )

    def put_push_credential(
        self, provider: str, credential: Mapping[str, Any]
    ) -> Result:
        return self._post(
            self._push_path(f"/credentials/{quote(provider, safe='')}"),
            dict(credential),
            headers=self._push_headers("push-admin"),
        )

    def publish_push(self, request: Mapping[str, Any]) -> Result:
        payload = dict(request)
        payload["sync"] = False
        return self._post(
            self._push_path("/publish"),
            payload,
            headers=self._push_headers("push-admin"),
        )

    def publish_push_direct(self, request: Mapping[str, Any]) -> Result:
        return self.publish_push(request)

    def publish_push_batch(self, requests: Sequence[Mapping[str, Any]]) -> Result:
        payload = []
        for request in requests:
            item = dict(request)
            item["sync"] = False
            payload.append(item)
        return self._post(
            self._push_path("/batch/publish"),
            payload,
            headers=self._push_headers("push-admin"),
        )

    def schedule_push(self, request: Mapping[str, Any]) -> Result:
        if "notBeforeMs" not in request:
            raise SockudoError("scheduled push requires notBeforeMs")
        return self.publish_push(request)

    def get_publish_status(self, publish_id: str) -> Result:
        return self._request(
            "GET",
            self._push_path(f"/publish/{quote(publish_id, safe='')}/status"),
            None,
            headers=self._push_headers("push-admin"),
        )

    def cancel_scheduled_push(self, publish_id: str) -> Result:
        return self._request(
            "DELETE",
            self._push_path(f"/scheduled/{quote(publish_id, safe='')}"),
            None,
            headers=self._push_headers("push-admin"),
        )

    def post_push_delivery_status(self, event: Mapping[str, Any]) -> Result:
        return self._post(
            self._push_path("/deliveryStatus"),
            dict(event),
            headers=self._push_headers("push-admin"),
        )

    def _message_path(self, channel: str, message_serial: str) -> str:
        return self._channel_path(
            channel, f"/messages/{quote(message_serial, safe='')}"
        )

    def _post(
        self,
        path: str,
        payload: Mapping[str, Any],
        headers: Optional[Mapping[str, str]] = None,
    ) -> Result:
        return self._request("POST", path, self._serialize(payload), headers=headers)

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[str],
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Result:
        url = self.signed_uri(method, path, body, params)
        last = Result(Status.NETWORK_ERROR, "request was not attempted")
        for attempt in range(1, self.options.max_retries + 1):
            try:
                response = self._client.request(
                    method,
                    url,
                    content=body,
                    headers={"Content-Type": "application/json", **dict(headers or {})},
                )
                last = Result.from_response(response)
            except httpx.HTTPError as exc:
                last = Result.from_exception(exc)
            if not last.status.should_retry() or attempt == self.options.max_retries:
                return last
            time.sleep(self.options.retry_base_delay * (2 ** (attempt - 1)))
        return last


class AsyncSockudo(_BaseSockudo):
    def __init__(
        self,
        app_id: Union[str, Config],
        key: Optional[str] = None,
        secret: Optional[str] = None,
        encryption_master_key_base64: Optional[str] = None,
        options: Optional[SockudoOptions] = None,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        super().__init__(app_id, key, secret, encryption_master_key_base64, options)
        self._client = client or _make_async_client(self.options)
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "AsyncSockudo":
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()

    async def trigger(
        self,
        channels: Union[str, Sequence[str]],
        event: str,
        data: Any,
        options: Optional[TriggerOptions] = None,
    ) -> Result:
        payload, headers = self._event_payload(channels, event, data, options)
        return await self._post(self._path("/events"), payload, headers=headers)

    async def trigger_batch(self, batch: Sequence[Event]) -> Result:
        return await self._post(self._path("/batch_events"), self._batch_payload(batch))

    async def send_to_user(
        self,
        user_id: str,
        event: str,
        data: Any,
        options: Optional[TriggerOptions] = None,
    ) -> Result:
        if not user_id:
            raise SockudoError("user_id is required")
        return await self.trigger(f"#server-to-user-{user_id}", event, data, options)

    async def get(
        self, path: str, params: Optional[Mapping[str, Any]] = None
    ) -> Result:
        full_path = (
            path
            if path.startswith("/apps/")
            else self._path(path if path.startswith("/") else f"/{path}")
        )
        return await self._request("GET", full_path, None, params=params)

    async def list_channels(self, params: Optional[ChannelsParams] = None) -> Result:
        return await self.get("/channels", params.to_query() if params else None)

    async def get_channel(
        self, channel: str, params: Optional[ChannelParams] = None
    ) -> Result:
        return await self.get(
            self._channel_path(channel), params.to_query() if params else None
        )

    async def get_channel_users(self, channel: str) -> Result:
        return await self.get(self._channel_path(channel, "/users"))

    async def get_channel_history(
        self, channel: str, params: Optional[HistoryParams] = None
    ) -> Result:
        return await self.get(
            self._channel_path(channel, "/history"),
            params.to_query() if params else None,
        )

    async def get_channel_history_state(self, channel: str) -> Result:
        return await self.get(self._channel_path(channel, "/history/state"))

    async def reset_channel_history(
        self, channel: str, reason: str, requested_by: Optional[str] = None
    ) -> Result:
        return await self._post(
            self._channel_path(channel, "/history/reset"),
            _operator_payload(channel, "reset", reason, requested_by),
        )

    async def purge_channel_history(
        self,
        channel: str,
        mode: str,
        reason: str,
        requested_by: Optional[str] = None,
        before_serial: Optional[int] = None,
        before_time_ms: Optional[int] = None,
    ) -> Result:
        payload = _operator_payload(channel, "purge", reason, requested_by)
        payload.update(
            _clean_raw(
                {
                    "mode": mode,
                    "before_serial": before_serial,
                    "before_time_ms": before_time_ms,
                }
            )
        )
        return await self._post(self._channel_path(channel, "/history/purge"), payload)

    async def get_channel_presence_history(
        self, channel: str, params: Optional[PresenceHistoryParams] = None
    ) -> Result:
        return await self.get(
            self._channel_path(channel, "/presence/history"),
            params.to_query() if params else None,
        )

    async def get_channel_presence_history_state(self, channel: str) -> Result:
        return await self.get(self._channel_path(channel, "/presence/history/state"))

    async def reset_channel_presence_history(
        self, channel: str, reason: str, requested_by: Optional[str] = None
    ) -> Result:
        return await self._post(
            self._channel_path(channel, "/presence/history/reset"),
            _operator_payload(channel, "reset", reason, requested_by),
        )

    async def get_channel_presence_snapshot(
        self, channel: str, params: Optional[PresenceSnapshotParams] = None
    ) -> Result:
        return await self.get(
            self._channel_path(channel, "/presence/history/snapshot"),
            params.to_query() if params else None,
        )

    async def get_message(self, channel: str, message_serial: str) -> Result:
        return await self.get(self._message_path(channel, message_serial))

    async def get_message_versions(
        self,
        channel: str,
        message_serial: str,
        params: Optional[MessageVersionsParams] = None,
    ) -> Result:
        return await self.get(
            f"{self._message_path(channel, message_serial)}/versions",
            params.to_query() if params else None,
        )

    async def update_message(
        self,
        channel: str,
        message_serial: str,
        mutation: Union[MessageMutation, Mapping[str, Any]],
    ) -> Result:
        return await self._post(
            f"{self._message_path(channel, message_serial)}/update",
            _mutation_payload(mutation, self._marshaller),
        )

    async def delete_message(
        self,
        channel: str,
        message_serial: str,
        mutation: Optional[Union[MessageMutation, Mapping[str, Any]]] = None,
    ) -> Result:
        return await self._post(
            f"{self._message_path(channel, message_serial)}/delete",
            _mutation_payload(mutation, self._marshaller)
            if mutation is not None
            else {},
        )

    async def append_message(
        self,
        channel: str,
        message_serial: str,
        data: str,
        socket_id: Optional[str] = None,
    ) -> Result:
        if not data:
            raise SockudoError("append_message data must be non-empty")
        return await self._post(
            f"{self._message_path(channel, message_serial)}/append",
            _clean_raw({"data": data, "socket_id": socket_id}),
        )

    async def publish_annotation(
        self,
        channel: str,
        message_serial: str,
        request: Union[PublishAnnotationRequest, Mapping[str, Any]],
    ) -> Result:
        payload = (
            request.to_payload()
            if isinstance(request, PublishAnnotationRequest)
            else dict(request)
        )
        return await self._post(
            f"{self._message_path(channel, message_serial)}/annotations", payload
        )

    async def list_annotations(
        self,
        channel: str,
        message_serial: str,
        params: Optional[AnnotationEventsParams] = None,
    ) -> Result:
        return await self.get(
            f"{self._message_path(channel, message_serial)}/annotations",
            params.to_query() if params else None,
        )

    async def delete_annotation(
        self,
        channel: str,
        message_serial: str,
        annotation_serial: str,
        socket_id: Optional[str] = None,
    ) -> Result:
        return await self._request(
            "DELETE",
            f"{self._message_path(channel, message_serial)}/annotations/{quote(annotation_serial, safe='')}",
            None,
            params=_clean_query({"socket_id": socket_id}),
        )

    async def terminate_user_connections(self, user_id: str) -> Result:
        return await self._post(
            self._path(f"/users/{quote(user_id, safe='')}/terminate_connections"), {}
        )

    async def activate_device(
        self, device: Mapping[str, Any], rotate_device_identity_token: bool = False
    ) -> Result:
        headers = self._push_headers("push-admin")
        if rotate_device_identity_token:
            headers["X-Sockudo-Rotate-Device-Identity-Token"] = "true"
        return await self._post(
            self._push_path("/deviceRegistrations"), dict(device), headers=headers
        )

    async def create_device_activation(self, device: Mapping[str, Any]) -> Result:
        return await self.activate_device(device)

    async def update_device_registration(
        self, device: Mapping[str, Any], device_identity_token: str
    ) -> Result:
        return await self._post(
            self._push_path("/deviceRegistrations"),
            dict(device),
            headers=self._push_headers("push-subscribe", device_identity_token),
        )

    async def list_device_registrations(
        self, params: Optional[PushCursorParams] = None
    ) -> Result:
        return await self._request(
            "GET",
            self._push_path("/deviceRegistrations"),
            None,
            params=params.to_query() if params else None,
            headers=self._push_headers("push-admin"),
        )

    async def get_device_registration(
        self, device_id: str, device_identity_token: Optional[str] = None
    ) -> Result:
        headers = (
            self._push_headers("push-subscribe", device_identity_token)
            if device_identity_token
            else self._push_headers("push-admin")
        )
        return await self._request(
            "GET",
            self._push_path(f"/deviceRegistrations/{quote(device_id, safe='')}"),
            None,
            headers=headers,
        )

    async def delete_device_registration(
        self, device_id: str, device_identity_token: Optional[str] = None
    ) -> Result:
        headers = (
            self._push_headers("push-subscribe", device_identity_token)
            if device_identity_token
            else self._push_headers("push-admin")
        )
        return await self._request(
            "DELETE",
            self._push_path(f"/deviceRegistrations/{quote(device_id, safe='')}"),
            None,
            headers=headers,
        )

    async def remove_device_registrations_by_client(self, client_id: str) -> Result:
        return await self._request(
            "DELETE",
            self._push_path("/deviceRegistrations"),
            None,
            params={"clientId": client_id},
            headers=self._push_headers("push-admin"),
        )

    async def upsert_channel_push_subscription(
        self,
        subscription: Mapping[str, Any],
        device_identity_token: Optional[str] = None,
    ) -> Result:
        headers = (
            self._push_headers("push-subscribe", device_identity_token)
            if device_identity_token
            else self._push_headers("push-admin")
        )
        return await self._post(
            self._push_path("/channelSubscriptions"),
            dict(subscription),
            headers=headers,
        )

    async def list_channel_push_subscriptions(
        self,
        params: Optional[PushSubscriptionParams] = None,
        device_identity_token: Optional[str] = None,
    ) -> Result:
        headers = (
            self._push_headers("push-subscribe", device_identity_token)
            if device_identity_token
            else self._push_headers("push-admin")
        )
        return await self._request(
            "GET",
            self._push_path("/channelSubscriptions"),
            None,
            params=params.to_query() if params else None,
            headers=headers,
        )

    async def delete_channel_push_subscriptions(
        self,
        params: PushSubscriptionParams,
        device_identity_token: Optional[str] = None,
    ) -> Result:
        headers = (
            self._push_headers("push-subscribe", device_identity_token)
            if device_identity_token
            else self._push_headers("push-admin")
        )
        return await self._request(
            "DELETE",
            self._push_path("/channelSubscriptions"),
            None,
            params=params.to_query(),
            headers=headers,
        )

    async def list_channel_push_subscription_channels(
        self, params: Optional[PushCursorParams] = None
    ) -> Result:
        return await self._request(
            "GET",
            self._push_path("/channelSubscriptions/channels"),
            None,
            params=params.to_query() if params else None,
            headers=self._push_headers("push-admin"),
        )

    async def list_push_credentials(
        self, params: Optional[PushCursorParams] = None
    ) -> Result:
        return await self._request(
            "GET",
            self._push_path("/credentials"),
            None,
            params=params.to_query() if params else None,
            headers=self._push_headers("push-admin"),
        )

    async def put_push_credential(
        self, provider: str, credential: Mapping[str, Any]
    ) -> Result:
        return await self._post(
            self._push_path(f"/credentials/{quote(provider, safe='')}"),
            dict(credential),
            headers=self._push_headers("push-admin"),
        )

    async def publish_push(self, request: Mapping[str, Any]) -> Result:
        payload = dict(request)
        payload["sync"] = False
        return await self._post(
            self._push_path("/publish"),
            payload,
            headers=self._push_headers("push-admin"),
        )

    async def publish_push_direct(self, request: Mapping[str, Any]) -> Result:
        return await self.publish_push(request)

    async def publish_push_batch(self, requests: Sequence[Mapping[str, Any]]) -> Result:
        payload = []
        for request in requests:
            item = dict(request)
            item["sync"] = False
            payload.append(item)
        return await self._post(
            self._push_path("/batch/publish"),
            payload,
            headers=self._push_headers("push-admin"),
        )

    async def schedule_push(self, request: Mapping[str, Any]) -> Result:
        if "notBeforeMs" not in request:
            raise SockudoError("scheduled push requires notBeforeMs")
        return await self.publish_push(request)

    async def get_publish_status(self, publish_id: str) -> Result:
        return await self._request(
            "GET",
            self._push_path(f"/publish/{quote(publish_id, safe='')}/status"),
            None,
            headers=self._push_headers("push-admin"),
        )

    async def cancel_scheduled_push(self, publish_id: str) -> Result:
        return await self._request(
            "DELETE",
            self._push_path(f"/scheduled/{quote(publish_id, safe='')}"),
            None,
            headers=self._push_headers("push-admin"),
        )

    async def post_push_delivery_status(self, event: Mapping[str, Any]) -> Result:
        return await self._post(
            self._push_path("/deliveryStatus"),
            dict(event),
            headers=self._push_headers("push-admin"),
        )

    def _message_path(self, channel: str, message_serial: str) -> str:
        return self._channel_path(
            channel, f"/messages/{quote(message_serial, safe='')}"
        )

    async def _post(
        self,
        path: str,
        payload: Mapping[str, Any],
        headers: Optional[Mapping[str, str]] = None,
    ) -> Result:
        return await self._request(
            "POST", path, self._serialize(payload), headers=headers
        )

    async def _request(
        self,
        method: str,
        path: str,
        body: Optional[str],
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> Result:
        url = self.signed_uri(method, path, body, params)
        last = Result(Status.NETWORK_ERROR, "request was not attempted")
        for attempt in range(1, self.options.max_retries + 1):
            try:
                response = await self._client.request(
                    method,
                    url,
                    content=body,
                    headers={"Content-Type": "application/json", **dict(headers or {})},
                )
                last = Result.from_response(response)
            except httpx.HTTPError as exc:
                last = Result.from_exception(exc)
            if not last.status.should_retry() or attempt == self.options.max_retries:
                return last
            await _async_sleep(self.options.retry_base_delay * (2 ** (attempt - 1)))
        return last


async def _async_sleep(delay: float) -> None:
    import asyncio

    await asyncio.sleep(delay)


def _signature_string(method: str, path: str, query_params: Mapping[str, str]) -> str:
    params = {
        key.lower(): value
        for key, value in query_params.items()
        if key != "auth_signature"
    }
    query = "&".join(f"{key}={params[key]}" for key in sorted(params))
    return f"{method}\n{path}\n{query}"


def _default_marshaller(data: Any) -> str:
    return json.dumps(data, separators=(",", ":"))


def _encode_data(data: Any, marshaller: JsonMarshaller) -> str:
    return data if isinstance(data, str) else marshaller(data)


def _resolve_idempotency_key(value: Optional[Union[str, bool]]) -> Optional[str]:
    if value is True:
        return str(uuid.uuid4())
    if value in (False, None):
        return None
    return str(value)


def _is_encrypted_channel(channel: str) -> bool:
    return channel.startswith(ENCRYPTED_CHANNEL_PREFIX)


def _join_info(info: Optional[Union[str, Sequence[str]]]) -> Optional[str]:
    if info is None or isinstance(info, str):
        return info
    return ",".join(info)


def _clean_raw(values: Mapping[str, Any]) -> JsonDict:
    return {key: value for key, value in values.items() if value is not None}


def _clean_query(values: Mapping[str, Any]) -> Dict[str, str]:
    return {key: str(value) for key, value in values.items() if value is not None}


def _apply_common_event_fields(
    payload: JsonDict,
    socket_id: Optional[str],
    info: Optional[Union[str, Sequence[str]]],
    idempotency_key: Optional[str],
    extras: Optional[MessageExtras],
    tags: Optional[Dict[str, str]],
) -> None:
    if socket_id is not None:
        payload["socket_id"] = socket_id
    joined_info = _join_info(info)
    if joined_info is not None:
        payload["info"] = joined_info
    if idempotency_key is not None:
        payload["idempotency_key"] = idempotency_key
    if extras is not None:
        extras_payload = extras.to_payload()
        if extras_payload:
            payload["extras"] = extras_payload
    if tags is not None:
        payload["tags"] = tags


def _mutation_payload(
    mutation: Union[MessageMutation, Mapping[str, Any]], marshaller: JsonMarshaller
) -> JsonDict:
    return (
        mutation.to_payload(marshaller)
        if isinstance(mutation, MessageMutation)
        else dict(mutation)
    )


def _operator_payload(
    channel: str, operation: str, reason: str, requested_by: Optional[str]
) -> JsonDict:
    if not reason:
        raise SockudoError("reason is required for operator history controls")
    return _clean_raw(
        {
            "confirm_channel": channel,
            "confirm_operation": operation,
            "reason": reason,
            "requested_by": requested_by,
        }
    )


def _make_sync_client(options: SockudoOptions) -> httpx.Client:
    try:
        return httpx.Client(
            timeout=options.timeout, http2=options.http2, verify=options.verify_tls
        )
    except ImportError:
        if not options.http2:
            raise
        return httpx.Client(
            timeout=options.timeout, http2=False, verify=options.verify_tls
        )


def _make_async_client(options: SockudoOptions) -> httpx.AsyncClient:
    try:
        return httpx.AsyncClient(
            timeout=options.timeout, http2=options.http2, verify=options.verify_tls
        )
    except ImportError:
        if not options.http2:
            raise
        return httpx.AsyncClient(
            timeout=options.timeout, http2=False, verify=options.verify_tls
        )
