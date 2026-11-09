//! MessagePack codec.
//!
//! The server serializes its wire structs with `rmp_serde::to_vec`, which
//! writes structs as positional arrays and adjacently-tagged enums as
//! `[kind, value]`. These mirror types reproduce that layout exactly. Every
//! field has `#[serde(default)]` so frames from older servers with fewer
//! trailing fields still decode.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;

use crate::codec::{Outbound, ai_to_value, structured_to_json};
use crate::error::{Error, Result};
use crate::event::{Event, Extras, HeaderValue, Payload};

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct WireMessage {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    data: Option<WireData>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    tags: Option<BTreeMap<String, String>>,
    #[serde(default)]
    sequence: Option<u64>,
    #[serde(default)]
    conflation_key: Option<String>,
    #[serde(default)]
    message_id: Option<String>,
    #[serde(default)]
    stream_id: Option<String>,
    #[serde(default)]
    serial: Option<u64>,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    extras: Option<WireExtras>,
    #[serde(default)]
    delta_sequence: Option<u64>,
    #[serde(default)]
    delta_conflation_key: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum WireData {
    String(String),
    Binary(#[serde(with = "serde_bytes")] Vec<u8>),
    Structured(WireStructured),
    Json(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct WireStructured {
    #[serde(default)]
    channel_data: Option<String>,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    user_data: Option<String>,
    #[serde(default)]
    extra: HashMap<String, String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct WireExtras {
    #[serde(default)]
    /// A null header is native msgpack nil.
    headers: Option<HashMap<String, Option<WireHeader>>>,
    #[serde(default)]
    ephemeral: Option<bool>,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    echo: Option<bool>,
    #[serde(default)]
    ai: Option<WireAi>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum WireHeader {
    String(String),
    Number(f64),
    Bool(bool),
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct WireAi {
    #[serde(default)]
    transport: Option<HashMap<String, String>>,
    #[serde(default)]
    /// A header map (Sockudo) or a codec id string (Ably).
    codec: Option<AnyValue>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    opaque: BTreeMap<String, AnyValue>,
}

/// A self-describing value that deserializes from any serde format.
/// (`sonic_rs::Value` only deserializes from sonic's own deserializer.)
#[derive(Debug)]
enum AnyValue {
    Null,
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    Text(String),
    Array(Vec<AnyValue>),
    Object(Vec<(String, AnyValue)>),
}

impl AnyValue {
    fn into_value(self) -> Value {
        match self {
            Self::Null => Value::new_null(),
            Self::Bool(flag) => Value::from(flag),
            Self::Signed(number) => Value::from(number),
            Self::Unsigned(number) => Value::from(number),
            Self::Float(number) => sonic_rs::to_value(&number).unwrap_or_default(),
            Self::Text(text) => Value::from(text.as_str()),
            Self::Array(items) => {
                let mut array = sonic_rs::Array::new();
                for item in items {
                    array.push(item.into_value());
                }
                Value::from(array)
            }
            Self::Object(entries) => {
                let mut object = sonic_rs::Object::new();
                for (key, item) in entries {
                    object.insert(&key, item.into_value());
                }
                Value::from(object)
            }
        }
    }
}

impl Serialize for AnyValue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{SerializeMap, SerializeSeq};
        match self {
            Self::Null => serializer.serialize_unit(),
            Self::Bool(flag) => serializer.serialize_bool(*flag),
            Self::Signed(number) => serializer.serialize_i64(*number),
            Self::Unsigned(number) => serializer.serialize_u64(*number),
            Self::Float(number) => serializer.serialize_f64(*number),
            Self::Text(text) => serializer.serialize_str(text),
            Self::Array(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            Self::Object(entries) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (key, item) in entries {
                    map.serialize_entry(key, item)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for AnyValue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = AnyValue;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("any value")
            }
            fn visit_unit<E>(self) -> Result<AnyValue, E> {
                Ok(AnyValue::Null)
            }
            fn visit_none<E>(self) -> Result<AnyValue, E> {
                Ok(AnyValue::Null)
            }
            fn visit_some<D: serde::Deserializer<'de>>(self, d: D) -> Result<AnyValue, D::Error> {
                AnyValue::deserialize(d)
            }
            fn visit_bool<E>(self, v: bool) -> Result<AnyValue, E> {
                Ok(AnyValue::Bool(v))
            }
            fn visit_i64<E>(self, v: i64) -> Result<AnyValue, E> {
                Ok(AnyValue::Signed(v))
            }
            fn visit_u64<E>(self, v: u64) -> Result<AnyValue, E> {
                Ok(AnyValue::Unsigned(v))
            }
            fn visit_f64<E>(self, v: f64) -> Result<AnyValue, E> {
                Ok(AnyValue::Float(v))
            }
            fn visit_str<E>(self, v: &str) -> Result<AnyValue, E> {
                Ok(AnyValue::Text(v.to_owned()))
            }
            fn visit_string<E>(self, v: String) -> Result<AnyValue, E> {
                Ok(AnyValue::Text(v))
            }
            fn visit_bytes<E>(self, v: &[u8]) -> Result<AnyValue, E> {
                Ok(AnyValue::Array(
                    v.iter()
                        .map(|b| AnyValue::Unsigned(u64::from(*b)))
                        .collect(),
                ))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<AnyValue, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element()? {
                    items.push(item);
                }
                Ok(AnyValue::Array(items))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<AnyValue, A::Error> {
                let mut entries = Vec::new();
                while let Some((key, value)) = map.next_entry::<String, AnyValue>()? {
                    entries.push((key, value));
                }
                Ok(AnyValue::Object(entries))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

/// Encode an outbound frame. `data` travels as the `json` kind, which the
/// server parses back into a structured value.
pub(crate) fn encode(frame: Outbound<'_>) -> Result<Vec<u8>> {
    let data = if frame.data.is_empty() {
        "{}".to_owned()
    } else {
        std::str::from_utf8(frame.data)
            .map_err(|error| Error::Encode(error.to_string()))?
            .to_owned()
    };
    let message = WireMessage {
        event: Some(frame.event.to_owned()),
        channel: frame.channel.map(str::to_owned),
        data: Some(WireData::Json(data)),
        ..WireMessage::default()
    };
    rmp_serde::to_vec(&message).map_err(|error| Error::Encode(error.to_string()))
}

/// Decode a MessagePack frame.
pub(crate) fn decode(raw: Bytes) -> Result<Event> {
    let message: WireMessage =
        rmp_serde::from_slice(&raw).map_err(|error| Error::Decode(error.to_string()))?;
    let data = match message.data {
        None => Payload::None,
        Some(WireData::String(text)) => Payload::Text(text),
        Some(WireData::Json(text)) => Payload::Json(text),
        Some(WireData::Binary(bytes)) => Payload::Binary(Bytes::from(bytes)),
        Some(WireData::Structured(structured)) => Payload::Json(structured_to_json(
            structured.channel_data,
            structured.channel,
            structured.user_data,
            structured.extra,
        )?),
    };
    Ok(Event {
        event: message.event.unwrap_or_default(),
        channel: message.channel,
        name: message.name,
        data,
        user_id: message.user_id,
        message_id: message.message_id,
        stream_id: message.stream_id,
        serial: message.serial,
        sequence: message.delta_sequence.or(message.sequence),
        conflation_key: message.delta_conflation_key.or(message.conflation_key),
        extras: message.extras.map(extras_from_wire),
        tags: message.tags,
        raw,
    })
}

fn extras_from_wire(wire: WireExtras) -> Extras {
    Extras {
        headers: wire.headers.map(|headers| {
            headers
                .into_iter()
                .map(|(key, value)| {
                    let value = match value {
                        Some(WireHeader::String(text)) => HeaderValue::String(text),
                        Some(WireHeader::Number(number)) => HeaderValue::Number(number),
                        Some(WireHeader::Bool(flag)) => HeaderValue::Bool(flag),
                        None => HeaderValue::Null,
                    };
                    (key, value)
                })
                .collect()
        }),
        ephemeral: wire.ephemeral,
        idempotency_key: wire.idempotency_key,
        echo: wire.echo,
        ai: wire.ai.map(|ai| {
            let opaque = ai
                .opaque
                .into_iter()
                .map(|(key, value)| (key, value.into_value()))
                .collect();
            ai_to_value(ai.transport, ai.codec.map(AnyValue::into_value), opaque)
        }),
        push: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_outbound_frame() {
        let encoded = encode(Outbound {
            event: "sockudo:subscribe",
            channel: Some("c"),
            data: br#"{"channel":"c"}"#,
        })
        .unwrap();
        let decoded = decode(Bytes::from(encoded)).unwrap();
        assert_eq!(decoded.event, "sockudo:subscribe");
        assert_eq!(decoded.channel.as_deref(), Some("c"));
        assert_eq!(decoded.data, Payload::Json(r#"{"channel":"c"}"#.into()));
    }

    #[test]
    fn decodes_short_arrays_from_older_servers() {
        // Only event + channel + data, no trailing V2 fields.
        let bytes = rmp_serde::to_vec(&(Some("e"), Some("c"), Some(("string", "hello")))).unwrap();
        let event = decode(Bytes::from(bytes)).unwrap();
        assert_eq!(event.event, "e");
        assert_eq!(event.data, Payload::Text("hello".into()));
    }

    #[test]
    fn rejects_garbage() {
        assert!(decode(Bytes::from_static(&[0xc1, 0x00])).is_err());
    }
}
