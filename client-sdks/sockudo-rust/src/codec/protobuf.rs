//! Protobuf codec mirroring the server's prost schema.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use prost::Message as _;
use sonic_rs::Value;

use crate::codec::{Outbound, ai_to_value, string_map_value, structured_to_json};
use crate::error::{Error, Result};
use crate::event::{Event, Extras, HeaderValue, Payload};

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct WireMessage {
    #[prost(string, optional, tag = "1")]
    event: Option<String>,
    #[prost(string, optional, tag = "2")]
    channel: Option<String>,
    #[prost(message, optional, tag = "3")]
    data: Option<WireData>,
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
    #[prost(string, optional, tag = "10")]
    stream_id: Option<String>,
    #[prost(uint64, optional, tag = "11")]
    serial: Option<u64>,
    #[prost(string, optional, tag = "12")]
    idempotency_key: Option<String>,
    #[prost(message, optional, tag = "13")]
    extras: Option<WireExtras>,
    #[prost(uint64, optional, tag = "14")]
    delta_sequence: Option<u64>,
    #[prost(string, optional, tag = "15")]
    delta_conflation_key: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireData {
    #[prost(oneof = "wire_data::Kind", tags = "1, 2, 3, 4")]
    kind: Option<wire_data::Kind>,
}

mod wire_data {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub(super) enum Kind {
        #[prost(string, tag = "1")]
        String(String),
        #[prost(message, tag = "2")]
        Structured(super::WireStructured),
        #[prost(string, tag = "3")]
        Json(String),
        #[prost(bytes = "vec", tag = "4")]
        Binary(Vec<u8>),
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireStructured {
    #[prost(string, optional, tag = "1")]
    channel_data: Option<String>,
    #[prost(string, optional, tag = "2")]
    channel: Option<String>,
    #[prost(string, optional, tag = "3")]
    user_data: Option<String>,
    #[prost(map = "string, string", tag = "4")]
    extra: HashMap<String, String>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireExtras {
    #[prost(map = "string, message", tag = "1")]
    headers: HashMap<String, WireHeader>,
    #[prost(bool, optional, tag = "2")]
    ephemeral: Option<bool>,
    #[prost(string, optional, tag = "3")]
    idempotency_key: Option<String>,
    #[prost(bool, optional, tag = "4")]
    echo: Option<bool>,
    #[prost(message, optional, tag = "5")]
    ai: Option<WireAi>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireAi {
    #[prost(map = "string, string", tag = "1")]
    transport: HashMap<String, String>,
    #[prost(map = "string, string", tag = "2")]
    codec: HashMap<String, String>,
    #[prost(map = "string, string", tag = "3")]
    opaque_json: HashMap<String, String>,
    /// Ably AI Transport codec id, set instead of `codec`.
    #[prost(string, optional, tag = "4")]
    codec_id: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireHeader {
    #[prost(oneof = "wire_header::Kind", tags = "1, 2, 3")]
    kind: Option<wire_header::Kind>,
}

mod wire_header {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub(super) enum Kind {
        #[prost(string, tag = "1")]
        String(String),
        #[prost(double, tag = "2")]
        Number(f64),
        #[prost(bool, tag = "3")]
        Bool(bool),
    }
}

/// Encode an outbound frame with `data` as the `json` kind.
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
        data: Some(WireData {
            kind: Some(wire_data::Kind::Json(data)),
        }),
        ..WireMessage::default()
    };
    Ok(message.encode_to_vec())
}

/// Decode a Protobuf frame.
pub(crate) fn decode(raw: Bytes) -> Result<Event> {
    let message =
        WireMessage::decode(raw.as_ref()).map_err(|error| Error::Decode(error.to_string()))?;
    let data = match message.data.and_then(|data| data.kind) {
        None => Payload::None,
        Some(wire_data::Kind::String(text)) => Payload::Text(text),
        Some(wire_data::Kind::Json(text)) => Payload::Json(text),
        Some(wire_data::Kind::Binary(bytes)) => Payload::Binary(Bytes::from(bytes)),
        Some(wire_data::Kind::Structured(structured)) => Payload::Json(structured_to_json(
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
        tags: (!message.tags.is_empty()).then(|| message.tags.into_iter().collect()),
        raw,
    })
}

fn extras_from_wire(wire: WireExtras) -> Extras {
    let headers = (!wire.headers.is_empty()).then(|| {
        wire.headers
            .into_iter()
            .filter_map(|(key, value)| {
                let value = match value.kind? {
                    wire_header::Kind::String(text) => HeaderValue::String(text),
                    wire_header::Kind::Number(number) => HeaderValue::Number(number),
                    wire_header::Kind::Bool(flag) => HeaderValue::Bool(flag),
                };
                Some((key, value))
            })
            .collect()
    });
    let ai = wire.ai.map(|ai| {
        let opaque: BTreeMap<String, Value> = ai
            .opaque_json
            .into_iter()
            .filter(|(key, _)| key != "transport" && key != "codec")
            .filter_map(|(key, raw)| sonic_rs::from_str(&raw).ok().map(|value| (key, value)))
            .collect();
        ai_to_value(
            (!ai.transport.is_empty()).then_some(ai.transport),
            match ai.codec_id {
                Some(id) => Some(Value::from(id.as_str())),
                None => (!ai.codec.is_empty()).then(|| string_map_value(ai.codec)),
            },
            opaque,
        )
    });
    Extras {
        headers,
        ephemeral: wire.ephemeral,
        idempotency_key: wire.idempotency_key,
        echo: wire.echo,
        ai,
        push: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_outbound_frame() {
        let encoded = encode(Outbound {
            event: "client-move",
            channel: Some("private-game"),
            data: br#"{"x":1}"#,
        })
        .unwrap();
        let decoded = decode(Bytes::from(encoded)).unwrap();
        assert_eq!(decoded.event, "client-move");
        assert_eq!(decoded.channel.as_deref(), Some("private-game"));
        assert_eq!(decoded.data, Payload::Json(r#"{"x":1}"#.into()));
    }

    #[test]
    fn skips_unknown_fields() {
        let mut bytes = encode(Outbound {
            event: "e",
            channel: None,
            data: b"1",
        })
        .unwrap();
        // field 99, varint 5
        bytes.extend_from_slice(&[0x98, 0x06, 0x05]);
        assert_eq!(decode(Bytes::from(bytes)).unwrap().event, "e");
    }
}
