//! Binary-safe JSON and MessagePack wire codecs.

use crate::protocol::{AblyFormat, AblyProtocolMessage};
use bytes::Bytes;
use serde::{Deserialize, Serialize, de::Visitor};
use std::collections::BTreeMap;

const MAX_WIRE_COLLECTION_ENTRIES: usize = 4_096;

/// A format-neutral wire value. Unlike `serde_json::Value`, this retains the
/// distinction between MessagePack strings and binary values and between
/// signed and unsigned integers.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WireValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Binary(Vec<u8>),
    Array(Vec<Self>),
    Map(BTreeMap<String, Self>),
}

impl Serialize for WireValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Null => serializer.serialize_unit(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::I64(value) => serializer.serialize_i64(*value),
            Self::U64(value) => serializer.serialize_u64(*value),
            Self::F64(value) => serializer.serialize_f64(*value),
            Self::String(value) => serializer.serialize_str(value),
            Self::Binary(value) => serializer.serialize_bytes(value),
            Self::Array(value) => value.serialize(serializer),
            Self::Map(value) => value.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for WireValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(WireValueVisitor)
    }
}

struct WireValueVisitor;

impl<'de> Visitor<'de> for WireValueVisitor {
    type Value = WireValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON or MessagePack wire value")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(WireValue::Null)
    }
    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(WireValue::Null)
    }
    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(WireValue::Bool(value))
    }
    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(WireValue::I64(value))
    }
    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(WireValue::U64(value))
    }
    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
        Ok(WireValue::F64(value))
    }
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(WireValue::String(value.to_owned()))
    }
    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(WireValue::String(value))
    }
    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E> {
        Ok(WireValue::Binary(value.to_vec()))
    }
    fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E> {
        Ok(WireValue::Binary(value))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let size_hint = access.size_hint().unwrap_or(0);
        if size_hint > MAX_WIRE_COLLECTION_ENTRIES {
            return Err(serde::de::Error::custom("wire array exceeds entry limit"));
        }
        let mut values = Vec::with_capacity(size_hint);
        while let Some(value) = access.next_element()? {
            if values.len() == MAX_WIRE_COLLECTION_ENTRIES {
                return Err(serde::de::Error::custom("wire array exceeds entry limit"));
            }
            values.push(value);
        }
        Ok(WireValue::Array(values))
    }

    fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        if access
            .size_hint()
            .is_some_and(|size_hint| size_hint > MAX_WIRE_COLLECTION_ENTRIES)
        {
            return Err(serde::de::Error::custom("wire map exceeds entry limit"));
        }
        let mut values = BTreeMap::new();
        while let Some((key, value)) = access.next_entry::<String, WireValue>()? {
            if values.len() == MAX_WIRE_COLLECTION_ENTRIES && !values.contains_key(&key) {
                return Err(serde::de::Error::custom("wire map exceeds entry limit"));
            }
            values.insert(key, value);
        }
        Ok(WireValue::Map(values))
    }
}

pub(crate) fn decode_protocol_bytes(
    bytes: &[u8],
    format: AblyFormat,
) -> Result<AblyProtocolMessage, String> {
    match format {
        AblyFormat::Json => sonic_rs::from_slice(bytes).map_err(|error| error.to_string()),
        AblyFormat::MsgPack => decode_msgpack_exact(bytes),
    }
}

pub(crate) fn decode_value<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    format: AblyFormat,
) -> Result<T, String> {
    match format {
        AblyFormat::Json => sonic_rs::from_slice(bytes).map_err(|error| error.to_string()),
        AblyFormat::MsgPack => decode_msgpack_exact(bytes),
    }
}

fn decode_msgpack_exact<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let mut cursor = std::io::Cursor::new(bytes);
    let value = T::deserialize(&mut rmp_serde::Deserializer::new(&mut cursor))
        .map_err(|error| error.to_string())?;
    if cursor.position() != bytes.len() as u64 {
        return Err("trailing bytes after MessagePack value".to_string());
    }
    Ok(value)
}

pub(crate) fn encode_protocol_bytes(
    message: &AblyProtocolMessage,
    format: AblyFormat,
) -> Result<Bytes, String> {
    match format {
        AblyFormat::Json => sonic_rs::to_vec(message)
            .map(Bytes::from)
            .map_err(|error| error.to_string()),
        AblyFormat::MsgPack => rmp_serde::to_vec_named(message)
            .map(Bytes::from)
            .map_err(|error| error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ACTION_MESSAGE, AblyMessage, empty_protocol_message};
    use proptest::prelude::*;
    use sonic_rs::JsonValueTrait;

    fn wire_values(include_binary: bool) -> impl Strategy<Value = WireValue> {
        let binary = prop::collection::vec(any::<u8>(), 0..32)
            .prop_map(WireValue::Binary)
            .boxed();
        let leaf = prop_oneof![
            Just(WireValue::Null),
            any::<bool>().prop_map(WireValue::Bool),
            any::<i64>().prop_map(WireValue::I64),
            any::<u64>().prop_map(WireValue::U64),
            any::<f64>()
                .prop_filter("finite", |value| value.is_finite())
                .prop_map(WireValue::F64),
            ".{0,32}".prop_map(WireValue::String),
            if include_binary {
                binary
            } else {
                Just(WireValue::Null).boxed()
            },
        ];
        leaf.prop_recursive(3, 64, 8, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..8).prop_map(WireValue::Array),
                prop::collection::btree_map("[a-z]{1,8}", inner, 0..8).prop_map(WireValue::Map),
            ]
        })
    }

    fn normalize_positive_signed(value: WireValue) -> WireValue {
        match value {
            WireValue::I64(value) if value >= 0 => WireValue::U64(value as u64),
            WireValue::Array(values) => {
                WireValue::Array(values.into_iter().map(normalize_positive_signed).collect())
            }
            WireValue::Map(values) => WireValue::Map(
                values
                    .into_iter()
                    .map(|(key, value)| (key, normalize_positive_signed(value)))
                    .collect(),
            ),
            value => value,
        }
    }

    #[test]
    fn msgpack_rejects_oversized_array_before_allocating_declared_capacity() {
        let declared_array = [0xdd, 0x00, 0x00, 0x10, 0x01];
        let error = decode_value::<WireValue>(&declared_array, AblyFormat::MsgPack).unwrap_err();
        assert_eq!(error, "wire array exceeds entry limit");
    }

    #[test]
    fn msgpack_rejects_oversized_map_before_reading_entries() {
        let declared_map = [0xdf, 0x00, 0x00, 0x10, 0x01];
        let error = decode_value::<WireValue>(&declared_map, AblyFormat::MsgPack).unwrap_err();
        assert_eq!(error, "wire map exceeds entry limit");
    }

    proptest! {
        #[test]
        fn msgpack_round_trip_preserves_every_wire_variant(value in wire_values(true)) {
            let encoded = rmp_serde::to_vec_named(&value).unwrap();
            let decoded: WireValue = rmp_serde::from_slice(&encoded).unwrap();
            prop_assert_eq!(normalize_positive_signed(decoded), normalize_positive_signed(value));
        }

        #[test]
        fn json_round_trip_preserves_json_semantics(value in wire_values(false)) {
            let encoded = sonic_rs::to_vec(&value).unwrap();
            let decoded: WireValue = sonic_rs::from_slice(&encoded).unwrap();
            prop_assert_eq!(normalize_positive_signed(decoded), normalize_positive_signed(value));
        }
    }

    #[test]
    fn binary_data_is_normalized_once_and_keeps_unknown_encoding_chain() {
        #[derive(Serialize)]
        struct Frame<'a> {
            action: u8,
            messages: Vec<Message<'a>>,
        }
        #[derive(Serialize)]
        struct Message<'a> {
            data: &'a [u8],
            encoding: &'a str,
        }
        let frame = Frame {
            action: ACTION_MESSAGE,
            messages: vec![Message {
                data: &[0xde, 0xad, 0xbe, 0xef],
                encoding: "cipher+aes-256-cbc",
            }],
        };
        let mut bytes = Vec::new();
        let mut serializer = rmp_serde::Serializer::new(&mut bytes)
            .with_struct_map()
            .with_bytes(rmp_serde::config::BytesMode::ForceAll);
        frame.serialize(&mut serializer).unwrap();
        let decoded = decode_protocol_bytes(&bytes, AblyFormat::MsgPack).unwrap();
        let message = &decoded.messages.unwrap()[0];
        assert_eq!(
            message.data.as_ref().and_then(sonic_rs::Value::as_str),
            Some("3q2+7w==")
        );
        assert_eq!(
            message.encoding.as_deref(),
            Some("cipher+aes-256-cbc/base64")
        );
    }

    #[test]
    fn msgpack_protocol_rejects_trailing_bytes() {
        let mut message = empty_protocol_message(ACTION_MESSAGE);
        message.messages = Some(vec![AblyMessage::default()]);
        let mut encoded = encode_protocol_bytes(&message, AblyFormat::MsgPack)
            .unwrap()
            .to_vec();
        encoded.push(0xc0);
        assert!(decode_protocol_bytes(&encoded, AblyFormat::MsgPack).is_err());
    }

    #[test]
    fn attach_params_normalize_numeric_scalars_in_json_and_msgpack() {
        #[derive(Serialize)]
        struct Attach<'a> {
            action: u8,
            channel: &'a str,
            params: BTreeMap<&'a str, u64>,
        }
        let attach = Attach {
            action: crate::protocol::ACTION_ATTACH,
            channel: "rewind",
            params: BTreeMap::from([("rewind", 1)]),
        };
        let json = sonic_rs::to_vec(&attach).unwrap();
        let msgpack = rmp_serde::to_vec_named(&attach).unwrap();
        for (bytes, format) in [
            (json.as_slice(), AblyFormat::Json),
            (msgpack.as_slice(), AblyFormat::MsgPack),
        ] {
            let decoded = decode_protocol_bytes(bytes, format).unwrap();
            assert_eq!(
                decoded
                    .params
                    .as_ref()
                    .and_then(|params| params.get("rewind"))
                    .map(String::as_str),
                Some("1")
            );
        }
    }

    #[test]
    fn attach_params_reject_nested_values() {
        let bytes = br#"{"action":10,"channel":"invalid","params":{"rewind":[1]}}"#;
        assert!(decode_protocol_bytes(bytes, AblyFormat::Json).is_err());
    }
}
