//! Ably wire DTOs, actions, and projection formats.

use base64::Engine as _;
use serde::{Deserialize, Deserializer, Serialize};
use sonic_rs::Value;
use std::collections::HashMap;
#[cfg(feature = "delta")]
use std::sync::Arc;

use crate::codec::WireValue;

pub(crate) const ACTION_HEARTBEAT: u8 = 0;
pub(crate) const ACTION_ACK: u8 = 1;
pub(crate) const ACTION_NACK: u8 = 2;
pub(crate) const ACTION_CONNECT: u8 = 3;
pub(crate) const ACTION_CONNECTED: u8 = 4;
pub(crate) const ACTION_DISCONNECT: u8 = 5;
pub(crate) const ACTION_DISCONNECTED: u8 = 6;
pub(crate) const ACTION_CLOSE: u8 = 7;
pub(crate) const ACTION_CLOSED: u8 = 8;
pub(crate) const ACTION_ERROR: u8 = 9;
pub(crate) const ACTION_ATTACH: u8 = 10;
pub(crate) const ACTION_ATTACHED: u8 = 11;
pub(crate) const ACTION_DETACH: u8 = 12;
pub(crate) const ACTION_DETACHED: u8 = 13;
pub(crate) const ACTION_PRESENCE: u8 = 14;
pub(crate) const ACTION_MESSAGE: u8 = 15;
pub(crate) const ACTION_SYNC: u8 = 16;
pub(crate) const ACTION_AUTH: u8 = 17;
pub(crate) const ACTION_ANNOTATION: u8 = 21;

pub(crate) const FLAG_HAS_PRESENCE: u64 = 1 << 0;
pub(crate) const FLAG_HAS_BACKLOG: u64 = 1 << 1;
pub(crate) const FLAG_RESUMED: u64 = 1 << 2;
pub(crate) const FLAG_ATTACH_RESUME: u64 = 1 << 5;
pub(crate) const DEFAULT_CONNECTION_STATE_TTL_MS: u64 = 120_000;
pub(crate) const DEFAULT_MAX_IDLE_INTERVAL_MS: u64 = 15_000;
pub(crate) const DEFAULT_MAX_MESSAGE_SIZE: u64 = 64 * 1024;
pub(crate) const DEFAULT_TOKEN_TTL_MS: i64 = 60 * 60 * 1000;
pub(crate) const ABLY_COMPAT_MAX_REPLAY_MESSAGES: usize = 4096;
pub(crate) const ABLY_COMPAT_MAX_SESSIONS: usize = 100_000;
pub(crate) const ABLY_COMPAT_MAX_TOKENS: usize = 100_000;
pub(crate) const ABLY_COMPAT_EXPIRY_SWEEP_MS: u64 = 30_000;

pub(crate) const MESSAGE_CREATE: u8 = 0;
pub(crate) const MESSAGE_UPDATE: u8 = 1;
pub(crate) const MESSAGE_DELETE: u8 = 2;
pub(crate) const MESSAGE_SUMMARY: u8 = 4;
pub(crate) const MESSAGE_APPEND: u8 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AblyMessageProjection {
    Mutation,
    Aggregate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum AblyFormat {
    Json,
    MsgPack,
}

pub(crate) fn empty_protocol_message(action: u8) -> AblyProtocolMessage {
    AblyProtocolMessage {
        action,
        id: None,
        flags: None,
        timestamp: None,
        count: None,
        error: None,
        connection_id: None,
        channel: None,
        channel_serial: None,
        msg_serial: None,
        messages: None,
        presence: None,
        annotations: None,
        auth: None,
        connection_details: None,
        params: None,
        res: None,
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct AblyConnectQuery {
    pub(crate) key: Option<String>,
    #[serde(rename = "access_token", alias = "accessToken")]
    pub(crate) access_token: Option<String>,
    #[serde(rename = "client_id", alias = "clientId")]
    pub(crate) client_id: Option<String>,
    pub(crate) resume: Option<String>,
    pub(crate) recover: Option<String>,
    pub(crate) format: Option<String>,
    #[serde(default = "default_echo", alias = "echoMessages")]
    pub(crate) echo: bool,
    #[serde(rename = "remainPresentFor", alias = "remain_present_for")]
    pub(crate) remain_present_for: Option<u64>,
}

const fn default_echo() -> bool {
    true
}

#[derive(Debug, Deserialize, Default)]
pub struct AblyRestQuery {
    pub key: Option<String>,
    #[serde(rename = "access_token", alias = "accessToken")]
    pub access_token: Option<String>,
    #[serde(rename = "client_id", alias = "clientId")]
    pub client_id: Option<String>,
    pub format: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) cursor: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct AblyHistoryQuery {
    pub(crate) key: Option<String>,
    #[serde(rename = "access_token", alias = "accessToken")]
    pub(crate) access_token: Option<String>,
    #[serde(rename = "client_id", alias = "clientId")]
    pub(crate) client_id: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) direction: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) start: Option<i64>,
    pub(crate) end: Option<i64>,
    #[serde(rename = "until_attach", alias = "untilAttach")]
    pub(crate) until_attach: Option<bool>,
    #[serde(rename = "from_serial")]
    pub(crate) from_serial: Option<String>,
    pub(crate) format: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct AblyStatsQuery {
    pub key: Option<String>,
    #[serde(rename = "access_token", alias = "accessToken")]
    pub access_token: Option<String>,
    #[serde(rename = "client_id", alias = "clientId")]
    pub client_id: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub direction: Option<String>,
    pub unit: Option<String>,
    pub by: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub format: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyTokenRequest {
    pub(crate) key_name: Option<String>,
    pub(crate) client_id: Option<String>,
    pub(crate) ttl: Option<serde_json::Value>,
    pub(crate) capability: Option<serde_json::Value>,
    pub(crate) timestamp: Option<serde_json::Value>,
    pub(crate) nonce: Option<String>,
    pub(crate) mac: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyErrorInfo {
    pub(crate) message: String,
    pub(crate) code: u32,
    pub(crate) status_code: u16,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyConnectionDetails {
    pub(crate) client_id: Option<String>,
    pub(crate) connection_key: String,
    pub(crate) connection_state_ttl: u64,
    pub(crate) max_idle_interval: u64,
    pub(crate) max_message_size: u64,
    pub(crate) max_frame_size: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyProtocolMessage {
    pub(crate) action: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) flags: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timestamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<AblyErrorInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) connection_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) channel_serial: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) msg_serial: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) messages: Option<Vec<AblyMessage>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) presence: Option<Vec<AblyPresenceMessage>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) annotations: Option<Vec<AblyAnnotation>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, deserialize_with = "deserialize_optional_wire_value")]
    pub(crate) auth: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) connection_details: Option<AblyConnectionDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, deserialize_with = "deserialize_optional_params")]
    pub(crate) params: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, deserialize_with = "deserialize_optional_wire_value")]
    pub(crate) res: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyAnnotation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) action: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) serial: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) message_serial: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub(crate) annotation_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) encoding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timestamp: Option<i64>,
}

#[derive(Debug, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) data: Option<Value>,
    /// Exact encoded JSON bytes retained for delta chaining. This is projection
    /// metadata only and is never exposed on the Ably wire.
    #[cfg(feature = "delta")]
    #[serde(skip)]
    pub(crate) encoded_json: Option<Arc<[u8]>>,
    // Ably's decoded Message shape uses an explicit null when an encoding
    // chain has been fully consumed (for example `json` or `utf-8/base64`).
    pub(crate) encoding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) connection_id: Option<String>,
    #[serde(skip)]
    pub(crate) connection_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timestamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) extras: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) annotations: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) serial: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) action: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) version: Option<AblyMessageVersion>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct RawAblyMessage {
    id: Option<String>,
    name: Option<String>,
    data: Option<WireValue>,
    encoding: Option<String>,
    client_id: Option<String>,
    connection_id: Option<String>,
    connection_key: Option<String>,
    timestamp: Option<i64>,
    extras: Option<WireValue>,
    serial: Option<String>,
    action: Option<MessageActionWire>,
    #[serde(alias = "operation")]
    version: Option<AblyMessageVersion>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum MessageActionWire {
    Number(u8),
    Name(String),
}

impl MessageActionWire {
    fn value(self) -> Option<u8> {
        match self {
            Self::Number(value) => Some(value),
            Self::Name(value) => match value.as_str() {
                "message.create" => Some(MESSAGE_CREATE),
                "message.update" => Some(MESSAGE_UPDATE),
                "message.delete" => Some(MESSAGE_DELETE),
                "message.summary" => Some(MESSAGE_SUMMARY),
                "message.append" => Some(MESSAGE_APPEND),
                _ => None,
            },
        }
    }
}

impl<'de> Deserialize<'de> for AblyMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawAblyMessage::deserialize(deserializer)?;
        let (data, binary) = match raw.data {
            Some(WireValue::Binary(bytes)) => (
                Some(sonic_rs::json!(
                    base64::engine::general_purpose::STANDARD.encode(bytes)
                )),
                true,
            ),
            Some(value) => (
                Some(sonic_rs::to_value(&value).map_err(serde::de::Error::custom)?),
                false,
            ),
            None => (None, false),
        };
        let encoding = if binary {
            Some(append_encoding(raw.encoding.as_deref(), "base64"))
        } else {
            raw.encoding
        };
        let mut version = raw.version;
        if let Some(version) = version.as_mut()
            && version.serial.is_empty()
            && let Some(serial) = raw.serial.as_ref()
        {
            // Ably's decoded Message projection exposes the source serial on
            // an operation that omitted its own serial. Mutation commit code
            // distinguishes this fallback from an explicit next-version
            // serial by comparing it with the target message serial.
            version.serial.clone_from(serial);
        }
        Ok(Self {
            id: raw.id,
            name: raw.name,
            data,
            #[cfg(feature = "delta")]
            encoded_json: None,
            encoding,
            client_id: raw.client_id,
            connection_id: raw.connection_id,
            connection_key: raw.connection_key,
            timestamp: raw.timestamp,
            extras: raw
                .extras
                .map(|value| sonic_rs::to_value(&value).map_err(serde::de::Error::custom))
                .transpose()?,
            annotations: None,
            serial: raw.serial,
            action: raw.action.and_then(MessageActionWire::value),
            version,
        })
    }
}

fn append_encoding(existing: Option<&str>, component: &str) -> String {
    match existing.filter(|value| !value.is_empty()) {
        Some(existing)
            if existing
                .split('/')
                .any(|part| part.eq_ignore_ascii_case(component)) =>
        {
            existing.to_string()
        }
        Some(existing) => format!("{existing}/{component}"),
        None => component.to_string(),
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyMessageVersion {
    #[serde(default)]
    pub(crate) serial: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timestamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, deserialize_with = "deserialize_optional_wire_value")]
    pub(crate) metadata: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyPublishResponse {
    pub(crate) serials: Vec<Option<String>>,
}

#[derive(Debug, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyPresenceMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) action: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) connection_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) encoding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timestamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) extras: Option<Value>,
}

impl<'de> Deserialize<'de> for AblyPresenceMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase")]
        struct Raw {
            id: Option<String>,
            action: Option<u8>,
            client_id: Option<String>,
            connection_id: Option<String>,
            data: Option<WireValue>,
            encoding: Option<String>,
            timestamp: Option<i64>,
            extras: Option<WireValue>,
        }
        let raw = Raw::deserialize(deserializer)?;
        let (data, encoding) = match raw.data {
            Some(WireValue::Binary(bytes)) => (
                Some(sonic_rs::json!(
                    base64::engine::general_purpose::STANDARD.encode(bytes)
                )),
                Some(append_encoding(raw.encoding.as_deref(), "base64")),
            ),
            Some(value) => (
                Some(sonic_rs::to_value(&value).map_err(serde::de::Error::custom)?),
                raw.encoding,
            ),
            None => (None, raw.encoding),
        };
        Ok(Self {
            id: raw.id,
            action: raw.action,
            client_id: raw.client_id,
            connection_id: raw.connection_id,
            data,
            encoding,
            timestamp: raw.timestamp,
            extras: raw
                .extras
                .map(|value| sonic_rs::to_value(&value).map_err(serde::de::Error::custom))
                .transpose()?,
        })
    }
}

fn deserialize_optional_wire_value<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<WireValue>::deserialize(deserializer)?
        .map(|value| sonic_rs::to_value(&value).map_err(serde::de::Error::custom))
        .transpose()
}

fn deserialize_optional_params<'de, D>(
    deserializer: D,
) -> Result<Option<HashMap<String, String>>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(params) = Option::<HashMap<String, WireValue>>::deserialize(deserializer)? else {
        return Ok(None);
    };
    params
        .into_iter()
        .map(|(key, value)| {
            let value = match value {
                WireValue::String(value) => value,
                WireValue::Bool(value) => value.to_string(),
                WireValue::I64(value) => value.to_string(),
                WireValue::U64(value) => value.to_string(),
                WireValue::F64(value) if value.is_finite() => value.to_string(),
                WireValue::Null
                | WireValue::F64(_)
                | WireValue::Binary(_)
                | WireValue::Array(_)
                | WireValue::Map(_) => {
                    return Err(serde::de::Error::custom(
                        "Ably protocol params must contain scalar values",
                    ));
                }
            };
            Ok((key, value))
        })
        .collect::<Result<HashMap<_, _>, D::Error>>()
        .map(Some)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AblyTokenDetails {
    pub(crate) token: String,
    pub(crate) key_name: String,
    pub(crate) issued: i64,
    pub(crate) expires: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) client_id: Option<String>,
    pub(crate) capability: String,
}
