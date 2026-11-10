//! Wire codecs: JSON (sonic-rs), MessagePack and Protobuf.
//!
//! The binary codecs mirror the server's `sockudo-protocol` wire types field
//! for field; the conformance tests encode and decode against that crate.

use bytes::Bytes;
use sockudo_ws::Message;

use crate::error::{Error, Result};
use crate::event::Event;

pub(crate) mod json;
#[cfg(feature = "msgpack")]
pub(crate) mod msgpack;
#[cfg(feature = "protobuf")]
pub(crate) mod protobuf;

/// Serialization format negotiated through the `format` query parameter
/// (Protocol V2 only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum WireFormat {
    /// JSON text frames (default, the only V1 format).
    #[default]
    Json,
    /// MessagePack binary frames.
    #[cfg(feature = "msgpack")]
    MessagePack,
    /// Protobuf binary frames.
    #[cfg(feature = "protobuf")]
    Protobuf,
}

impl WireFormat {
    /// Value of the `format` query parameter.
    pub const fn query_value(self) -> &'static str {
        match self {
            Self::Json => "json",
            #[cfg(feature = "msgpack")]
            Self::MessagePack => "messagepack",
            #[cfg(feature = "protobuf")]
            Self::Protobuf => "protobuf",
        }
    }

    /// Whether frames are sent as binary WebSocket messages.
    pub const fn is_binary(self) -> bool {
        !matches!(self, Self::Json)
    }
}

/// An outbound frame. `data` is already-serialized JSON.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Outbound<'a> {
    pub event: &'a str,
    pub channel: Option<&'a str>,
    pub data: &'a [u8],
}

/// Encode a frame for the wire.
pub(crate) fn encode(frame: Outbound<'_>, format: WireFormat) -> Result<Message> {
    match format {
        WireFormat::Json => Ok(Message::Text(Bytes::from(json::encode(frame)?))),
        #[cfg(feature = "msgpack")]
        WireFormat::MessagePack => Ok(Message::Binary(Bytes::from(msgpack::encode(frame)?))),
        #[cfg(feature = "protobuf")]
        WireFormat::Protobuf => Ok(Message::Binary(Bytes::from(protobuf::encode(frame)?))),
    }
}

/// Decode a server frame. Text frames are always JSON; binary frames use the
/// negotiated format.
pub(crate) fn decode(raw: Bytes, is_text: bool, format: WireFormat) -> Result<Event> {
    if is_text {
        return json::decode(raw);
    }
    match format {
        WireFormat::Json => json::decode(raw),
        #[cfg(feature = "msgpack")]
        WireFormat::MessagePack => msgpack::decode(raw),
        #[cfg(feature = "protobuf")]
        WireFormat::Protobuf => protobuf::decode(raw),
    }
}

/// Serialize a value to JSON bytes for use as frame `data`.
pub(crate) fn to_json<T: serde::Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    sonic_rs::to_vec(value).map_err(|error| Error::Encode(error.to_string()))
}

#[cfg(any(feature = "msgpack", feature = "protobuf"))]
mod binary_shared {
    use std::collections::{BTreeMap, HashMap};

    use sonic_rs::Value;

    use crate::error::{Error, Result};

    /// Rebuild the JSON object the server flattened into structured data.
    pub(crate) fn structured_to_json(
        channel_data: Option<String>,
        channel: Option<String>,
        user_data: Option<String>,
        extra: HashMap<String, String>,
    ) -> Result<String> {
        let mut object = sonic_rs::Object::new();
        if let Some(value) = channel_data {
            object.insert("channel_data", Value::from(value.as_str()));
        }
        if let Some(value) = channel {
            object.insert("channel", Value::from(value.as_str()));
        }
        if let Some(value) = user_data {
            object.insert("user_data", Value::from(value.as_str()));
        }
        for (key, raw) in extra {
            let value =
                sonic_rs::from_str::<Value>(&raw).unwrap_or_else(|_| Value::from(raw.as_str()));
            object.insert(&key, value);
        }
        sonic_rs::to_string(&object).map_err(|error| Error::Decode(error.to_string()))
    }

    /// Merge typed AI transport/codec maps and extension keys into one object.
    pub(crate) fn ai_to_value(
        transport: Option<HashMap<String, String>>,
        codec: Option<Value>,
        opaque: BTreeMap<String, Value>,
    ) -> Value {
        let mut object = sonic_rs::Object::new();
        for (key, value) in opaque {
            object.insert(&key, value);
        }
        if let Some(map) = transport {
            object.insert("transport", string_map_value(map));
        }
        if let Some(codec) = codec {
            object.insert("codec", codec);
        }
        Value::from(object)
    }

    pub(crate) fn string_map_value(map: HashMap<String, String>) -> Value {
        let mut object = sonic_rs::Object::new();
        for (key, value) in map {
            object.insert(&key, Value::from(value.as_str()));
        }
        Value::from(object)
    }
}
#[cfg(any(feature = "msgpack", feature = "protobuf"))]
pub(crate) use binary_shared::{ai_to_value, string_map_value, structured_to_json};
