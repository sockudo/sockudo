use ahash::AHashMap;
use prost::Message;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::collections::{BTreeMap, HashMap};

use crate::messages::{ExtrasValue, MessageData, MessageExtras, PusherMessage};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum WireFormat {
    #[default]
    Json,
    MessagePack,
    Protobuf,
}

impl WireFormat {
    pub fn from_query_param(value: Option<&str>) -> Self {
        Self::parse_query_param(value).unwrap_or(Self::Json)
    }

    pub fn parse_query_param(value: Option<&str>) -> Result<Self, String> {
        match value.map(|v| v.trim().to_ascii_lowercase()) {
            None => Ok(Self::Json),
            Some(v) if v.is_empty() || v == "json" => Ok(Self::Json),
            Some(v) if v == "msgpack" || v == "messagepack" => Ok(Self::MessagePack),
            Some(v) if v == "protobuf" || v == "proto" => Ok(Self::Protobuf),
            Some(v) => Err(format!("unsupported wire format '{v}'")),
        }
    }

    pub const fn is_binary(self) -> bool {
        !matches!(self, Self::Json)
    }
}

pub fn serialize_message(message: &PusherMessage, format: WireFormat) -> Result<Vec<u8>, String> {
    match format {
        WireFormat::Json => {
            sonic_rs::to_vec(message).map_err(|e| format!("JSON serialization failed: {e}"))
        }
        WireFormat::MessagePack => rmp_serde::to_vec(&MsgpackPusherMessage::from(message.clone()))
            .map_err(|e| format!("MessagePack serialization failed: {e}")),
        WireFormat::Protobuf => {
            let proto = ProtoPusherMessage::from(message.clone());
            let mut buf = Vec::with_capacity(proto.encoded_len());
            proto
                .encode(&mut buf)
                .map_err(|e| format!("Protobuf serialization failed: {e}"))?;
            Ok(buf)
        }
    }
}

pub fn deserialize_message(bytes: &[u8], format: WireFormat) -> Result<PusherMessage, String> {
    match format {
        WireFormat::Json => sonic_rs::from_slice(bytes)
            .map_err(|e| format!("JSON deserialization failed: {e}")),
        WireFormat::MessagePack => {
            let msg: MsgpackPusherMessage = rmp_serde::from_slice(bytes)
                .map_err(|e| format!("MessagePack deserialization failed: {e}"))?;
            Ok(msg.into())
        }
        WireFormat::Protobuf => {
            let proto = ProtoPusherMessage::decode(bytes)
                .map_err(|e| format!("Protobuf deserialization failed: {e}"))?;
            Ok(proto.into())
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct ProtoPusherMessage {
    #[prost(string, optional, tag = "1")]
    event: Option<String>,
    #[prost(string, optional, tag = "2")]
    channel: Option<String>,
    #[prost(message, optional, tag = "3")]
    data: Option<ProtoMessageData>,
    #[prost(string, optional, tag = "4")]
    name: Option<String>,
    #[prost(string, optional, tag = "5")]
    user_id: Option<String>,
    #[prost(map = "string, string", tag = "6")]
    tags: HashMap<String, String>,
    #[prost(uint64, optional, tag = "7")]
    sequence: Option<u64>,
    #[prost(string, optional, tag = "8")]
    conflation_key: Option<String>,
    #[prost(string, optional, tag = "9")]
    message_id: Option<String>,
    #[prost(uint64, optional, tag = "10")]
    serial: Option<u64>,
    #[prost(string, optional, tag = "11")]
    idempotency_key: Option<String>,
    #[prost(message, optional, tag = "12")]
    extras: Option<ProtoMessageExtras>,
    #[prost(uint64, optional, tag = "13")]
    delta_sequence: Option<u64>,
    #[prost(string, optional, tag = "14")]
    delta_conflation_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MsgpackPusherMessage {
    event: Option<String>,
    channel: Option<String>,
    data: Option<MsgpackMessageData>,
    name: Option<String>,
    user_id: Option<String>,
    tags: Option<BTreeMap<String, String>>,
    sequence: Option<u64>,
    conflation_key: Option<String>,
    message_id: Option<String>,
    serial: Option<u64>,
    idempotency_key: Option<String>,
    extras: Option<MsgpackMessageExtras>,
    delta_sequence: Option<u64>,
    delta_conflation_key: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoMessageData {
    #[prost(oneof = "proto_message_data::Kind", tags = "1, 2, 3")]
    kind: Option<proto_message_data::Kind>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum MsgpackMessageData {
    String(String),
    Structured(MsgpackStructuredData),
    Json(String),
}

mod proto_message_data {
    use super::ProtoStructuredData;
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Kind {
        #[prost(string, tag = "1")]
        String(String),
        #[prost(message, tag = "2")]
        Structured(ProtoStructuredData),
        #[prost(string, tag = "3")]
        Json(String),
    }
}

#[derive(Clone, PartialEq, Message)]
struct ProtoStructuredData {
    #[prost(string, optional, tag = "1")]
    channel_data: Option<String>,
    #[prost(string, optional, tag = "2")]
    channel: Option<String>,
    #[prost(string, optional, tag = "3")]
    user_data: Option<String>,
    #[prost(map = "string, string", tag = "4")]
    extra: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MsgpackStructuredData {
    channel_data: Option<String>,
    channel: Option<String>,
    user_data: Option<String>,
    extra: HashMap<String, String>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoMessageExtras {
    #[prost(map = "string, message", tag = "1")]
    headers: HashMap<String, ProtoExtrasValue>,
    #[prost(bool, optional, tag = "2")]
    ephemeral: Option<bool>,
    #[prost(string, optional, tag = "3")]
    idempotency_key: Option<String>,
    #[prost(bool, optional, tag = "4")]
    echo: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MsgpackMessageExtras {
    headers: Option<HashMap<String, MsgpackExtrasValue>>,
    ephemeral: Option<bool>,
    idempotency_key: Option<String>,
    echo: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoExtrasValue {
    #[prost(oneof = "proto_extras_value::Kind", tags = "1, 2, 3")]
    kind: Option<proto_extras_value::Kind>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum MsgpackExtrasValue {
    String(String),
    Number(f64),
    Bool(bool),
}

mod proto_extras_value {
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Kind {
        #[prost(string, tag = "1")]
        String(String),
        #[prost(double, tag = "2")]
        Number(f64),
        #[prost(bool, tag = "3")]
        Bool(bool),
    }
}

impl From<PusherMessage> for ProtoPusherMessage {
    fn from(value: PusherMessage) -> Self {
        Self {
            event: value.event,
            channel: value.channel,
            data: value.data.map(Into::into),
            name: value.name,
            user_id: value.user_id,
            tags: value.tags.map(|m| m.into_iter().collect()).unwrap_or_default(),
            sequence: value.sequence,
            conflation_key: value.conflation_key,
            message_id: value.message_id,
            serial: value.serial,
            idempotency_key: value.idempotency_key,
            extras: value.extras.map(Into::into),
            delta_sequence: value.delta_sequence,
            delta_conflation_key: value.delta_conflation_key,
        }
    }
}

impl From<PusherMessage> for MsgpackPusherMessage {
    fn from(value: PusherMessage) -> Self {
        Self {
            event: value.event,
            channel: value.channel,
            data: value.data.map(Into::into),
            name: value.name,
            user_id: value.user_id,
            tags: value.tags,
            sequence: value.sequence,
            conflation_key: value.conflation_key,
            message_id: value.message_id,
            serial: value.serial,
            idempotency_key: value.idempotency_key,
            extras: value.extras.map(Into::into),
            delta_sequence: value.delta_sequence,
            delta_conflation_key: value.delta_conflation_key,
        }
    }
}

impl From<ProtoPusherMessage> for PusherMessage {
    fn from(value: ProtoPusherMessage) -> Self {
        Self {
            event: value.event,
            channel: value.channel,
            data: value.data.map(Into::into),
            name: value.name,
            user_id: value.user_id,
            tags: (!value.tags.is_empty()).then_some(value.tags.into_iter().collect::<BTreeMap<_, _>>()),
            sequence: value.sequence,
            conflation_key: value.conflation_key,
            message_id: value.message_id,
            serial: value.serial,
            idempotency_key: value.idempotency_key,
            extras: value.extras.map(Into::into),
            delta_sequence: value.delta_sequence,
            delta_conflation_key: value.delta_conflation_key,
        }
    }
}

impl From<MsgpackPusherMessage> for PusherMessage {
    fn from(value: MsgpackPusherMessage) -> Self {
        Self {
            event: value.event,
            channel: value.channel,
            data: value.data.map(Into::into),
            name: value.name,
            user_id: value.user_id,
            tags: value.tags,
            sequence: value.sequence,
            conflation_key: value.conflation_key,
            message_id: value.message_id,
            serial: value.serial,
            idempotency_key: value.idempotency_key,
            extras: value.extras.map(Into::into),
            delta_sequence: value.delta_sequence,
            delta_conflation_key: value.delta_conflation_key,
        }
    }
}

impl From<MessageData> for ProtoMessageData {
    fn from(value: MessageData) -> Self {
        let kind = match value {
            MessageData::String(s) => Some(proto_message_data::Kind::String(s)),
            MessageData::Structured {
                channel_data,
                channel,
                user_data,
                extra,
            } => Some(proto_message_data::Kind::Structured(ProtoStructuredData {
                channel_data,
                channel,
                user_data,
                extra: extra
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            sonic_rs::to_string(&v).unwrap_or_else(|_| "null".to_string()),
                        )
                    })
                    .collect(),
            })),
            MessageData::Json(v) => Some(proto_message_data::Kind::Json(
                sonic_rs::to_string(&v).unwrap_or_else(|_| "null".to_string()),
            )),
        };

        Self { kind }
    }
}

impl From<MessageData> for MsgpackMessageData {
    fn from(value: MessageData) -> Self {
        match value {
            MessageData::String(s) => Self::String(s),
            MessageData::Structured {
                channel_data,
                channel,
                user_data,
                extra,
            } => Self::Structured(MsgpackStructuredData {
                channel_data,
                channel,
                user_data,
                extra: extra
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            sonic_rs::to_string(&v).unwrap_or_else(|_| "null".to_string()),
                        )
                    })
                    .collect(),
            }),
            MessageData::Json(v) => Self::Json(
                sonic_rs::to_string(&v).unwrap_or_else(|_| "null".to_string()),
            ),
        }
    }
}

impl From<ProtoMessageData> for MessageData {
    fn from(value: ProtoMessageData) -> Self {
        match value.kind {
            Some(proto_message_data::Kind::String(s)) => MessageData::String(s),
            Some(proto_message_data::Kind::Structured(s)) => MessageData::Structured {
                channel_data: s.channel_data,
                channel: s.channel,
                user_data: s.user_data,
                extra: s
                    .extra
                    .into_iter()
                    .map(|(k, v)| {
                        let parsed = sonic_rs::from_str(&v).unwrap_or_else(|_| Value::from(v.as_str()));
                        (k, parsed)
                    })
                    .collect::<AHashMap<_, _>>(),
            },
            Some(proto_message_data::Kind::Json(v)) => {
                MessageData::Json(sonic_rs::from_str(&v).unwrap_or_else(|_| Value::from(v.as_str())))
            }
            None => MessageData::Json(Value::new_null()),
        }
    }
}

impl From<MsgpackMessageData> for MessageData {
    fn from(value: MsgpackMessageData) -> Self {
        match value {
            MsgpackMessageData::String(s) => MessageData::String(s),
            MsgpackMessageData::Structured(s) => MessageData::Structured {
                channel_data: s.channel_data,
                channel: s.channel,
                user_data: s.user_data,
                extra: s
                    .extra
                    .into_iter()
                    .map(|(k, v)| {
                        let parsed =
                            sonic_rs::from_str(&v).unwrap_or_else(|_| Value::from(v.as_str()));
                        (k, parsed)
                    })
                    .collect::<AHashMap<_, _>>(),
            },
            MsgpackMessageData::Json(v) => {
                MessageData::Json(sonic_rs::from_str(&v).unwrap_or_else(|_| Value::from(v.as_str())))
            }
        }
    }
}

impl From<MessageExtras> for ProtoMessageExtras {
    fn from(value: MessageExtras) -> Self {
        Self {
            headers: value
                .headers
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            ephemeral: value.ephemeral,
            idempotency_key: value.idempotency_key,
            echo: value.echo,
        }
    }
}

impl From<MessageExtras> for MsgpackMessageExtras {
    fn from(value: MessageExtras) -> Self {
        Self {
            headers: value.headers.map(|headers| {
                headers
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect()
            }),
            ephemeral: value.ephemeral,
            idempotency_key: value.idempotency_key,
            echo: value.echo,
        }
    }
}

impl From<ProtoMessageExtras> for MessageExtras {
    fn from(value: ProtoMessageExtras) -> Self {
        Self {
            headers: (!value.headers.is_empty()).then_some(
                value.headers.into_iter().map(|(k, v)| (k, v.into())).collect(),
            ),
            ephemeral: value.ephemeral,
            idempotency_key: value.idempotency_key,
            echo: value.echo,
        }
    }
}

impl From<MsgpackMessageExtras> for MessageExtras {
    fn from(value: MsgpackMessageExtras) -> Self {
        Self {
            headers: value.headers.map(|headers| {
                headers
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect()
            }),
            ephemeral: value.ephemeral,
            idempotency_key: value.idempotency_key,
            echo: value.echo,
        }
    }
}

impl From<ExtrasValue> for ProtoExtrasValue {
    fn from(value: ExtrasValue) -> Self {
        let kind = match value {
            ExtrasValue::String(s) => Some(proto_extras_value::Kind::String(s)),
            ExtrasValue::Number(n) => Some(proto_extras_value::Kind::Number(n)),
            ExtrasValue::Bool(b) => Some(proto_extras_value::Kind::Bool(b)),
        };
        Self { kind }
    }
}

impl From<ExtrasValue> for MsgpackExtrasValue {
    fn from(value: ExtrasValue) -> Self {
        match value {
            ExtrasValue::String(s) => Self::String(s),
            ExtrasValue::Number(n) => Self::Number(n),
            ExtrasValue::Bool(b) => Self::Bool(b),
        }
    }
}

impl From<ProtoExtrasValue> for ExtrasValue {
    fn from(value: ProtoExtrasValue) -> Self {
        match value.kind {
            Some(proto_extras_value::Kind::String(s)) => ExtrasValue::String(s),
            Some(proto_extras_value::Kind::Number(n)) => ExtrasValue::Number(n),
            Some(proto_extras_value::Kind::Bool(b)) => ExtrasValue::Bool(b),
            None => ExtrasValue::String(String::new()),
        }
    }
}

impl From<MsgpackExtrasValue> for ExtrasValue {
    fn from(value: MsgpackExtrasValue) -> Self {
        match value {
            MsgpackExtrasValue::String(s) => ExtrasValue::String(s),
            MsgpackExtrasValue::Number(n) => ExtrasValue::Number(n),
            MsgpackExtrasValue::Bool(b) => ExtrasValue::Bool(b),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_message() -> PusherMessage {
        PusherMessage {
            event: Some("sockudo:test".to_string()),
            channel: Some("chat:room-1".to_string()),
            data: Some(MessageData::Json(sonic_rs::json!({
                "hello": "world",
                "count": 3,
                "nested": { "ok": true },
                "items": [1, 2, 3]
            }))),
            name: None,
            user_id: Some("user-1".to_string()),
            tags: Some(BTreeMap::from([
                ("region".to_string(), "eu".to_string()),
                ("tier".to_string(), "gold".to_string()),
            ])),
            sequence: Some(7),
            conflation_key: Some("room".to_string()),
            message_id: Some("mid-1".to_string()),
            serial: Some(9),
            idempotency_key: Some("idem-1".to_string()),
            extras: Some(MessageExtras {
                headers: Some(HashMap::from([
                    ("priority".to_string(), ExtrasValue::String("high".to_string())),
                    ("ttl".to_string(), ExtrasValue::Number(5.0)),
                ])),
                ephemeral: Some(true),
                idempotency_key: Some("extra-idem".to_string()),
                echo: Some(false),
            }),
            delta_sequence: Some(11),
            delta_conflation_key: Some("btc".to_string()),
        }
    }

    #[test]
    fn round_trip_messagepack() {
        let msg = sample_message();
        let bytes = serialize_message(&msg, WireFormat::MessagePack).unwrap();
        let decoded = deserialize_message(&bytes, WireFormat::MessagePack).unwrap();
        assert_eq!(decoded.event, msg.event);
        assert_eq!(decoded.delta_sequence, msg.delta_sequence);
    }

    #[test]
    fn round_trip_protobuf() {
        let msg = sample_message();
        let bytes = serialize_message(&msg, WireFormat::Protobuf).unwrap();
        let decoded = deserialize_message(&bytes, WireFormat::Protobuf).unwrap();
        assert_eq!(decoded.event, msg.event);
        assert_eq!(decoded.channel, msg.channel);
        assert_eq!(decoded.message_id, msg.message_id);
        assert_eq!(decoded.delta_conflation_key, msg.delta_conflation_key);
    }

    #[test]
    fn parse_query_param_accepts_known_values() {
        assert_eq!(WireFormat::parse_query_param(None).unwrap(), WireFormat::Json);
        assert_eq!(
            WireFormat::parse_query_param(Some("json")).unwrap(),
            WireFormat::Json
        );
        assert_eq!(
            WireFormat::parse_query_param(Some("messagepack")).unwrap(),
            WireFormat::MessagePack
        );
        assert_eq!(
            WireFormat::parse_query_param(Some("msgpack")).unwrap(),
            WireFormat::MessagePack
        );
        assert_eq!(
            WireFormat::parse_query_param(Some("protobuf")).unwrap(),
            WireFormat::Protobuf
        );
        assert_eq!(
            WireFormat::parse_query_param(Some("proto")).unwrap(),
            WireFormat::Protobuf
        );
    }

    #[test]
    fn parse_query_param_rejects_unknown_value() {
        assert!(WireFormat::parse_query_param(Some("avro")).is_err());
    }
}
