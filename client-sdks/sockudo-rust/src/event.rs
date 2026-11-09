//! Decoded events and message extras.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use sonic_rs::{JsonValueTrait, Value};

use crate::error::{Error, Result};

/// The `data` field of an event, kept in its wire form until you ask for it.
///
/// Parsing is deferred: [`Event::json`] deserializes straight into your type
/// without building an intermediate tree, so events you ignore cost nothing.
#[derive(Clone, PartialEq, Eq, Default)]
pub enum Payload {
    /// The frame had no `data` field (or it was `null`).
    #[default]
    None,
    /// A string payload. Pusher-compatible servers JSON-encode application
    /// data into a string, so this usually holds JSON text.
    Text(String),
    /// A structured JSON payload, kept as its raw JSON text.
    Json(String),
    /// Native binary data (MessagePack/Protobuf wire formats).
    Binary(Bytes),
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::Text(text) => f.debug_tuple("Text").field(&text.len()).finish(),
            Self::Json(text) => f.debug_tuple("Json").field(&text.len()).finish(),
            Self::Binary(bytes) => f.debug_tuple("Binary").field(&bytes.len()).finish(),
        }
    }
}

impl Payload {
    /// Whether there is no payload.
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// The textual form: the string for [`Payload::Text`], the raw JSON for
    /// [`Payload::Json`].
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Text(text) | Self::Json(text) => Some(text),
            _ => None,
        }
    }

    /// The raw bytes of the payload, whatever its kind.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::None => &[],
            Self::Text(text) | Self::Json(text) => text.as_bytes(),
            Self::Binary(bytes) => bytes,
        }
    }

    /// Deserialize the payload into `T`.
    ///
    /// Text payloads are parsed as JSON. If `T` is `String` and the text is
    /// not JSON, the text itself is returned.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T> {
        match self {
            Self::None => Ok(sonic_rs::from_str("null")?),
            Self::Json(raw) => Ok(sonic_rs::from_str(raw)?),
            Self::Text(text) => match sonic_rs::from_str(text) {
                Ok(value) => Ok(value),
                Err(error) => {
                    // Plain (non-JSON) strings deserialize as themselves.
                    let quoted = sonic_rs::to_string(text)?;
                    sonic_rs::from_str(&quoted).map_err(|_| Error::Json(error))
                }
            },
            Self::Binary(bytes) => Ok(sonic_rs::from_slice(bytes)?),
        }
    }

    /// The payload as a dynamic JSON value. Non-JSON text becomes a JSON
    /// string; binary becomes an array of numbers.
    pub fn value(&self) -> Value {
        match self {
            Self::None => Value::new_null(),
            Self::Json(raw) => {
                sonic_rs::from_str(raw).unwrap_or_else(|_| Value::from(raw.as_str()))
            }
            Self::Text(text) => {
                sonic_rs::from_str(text).unwrap_or_else(|_| Value::from(text.as_str()))
            }
            Self::Binary(bytes) => sonic_rs::to_value(bytes.as_ref()).unwrap_or_default(),
        }
    }
}

/// A value inside `extras.headers`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HeaderValue {
    /// Boolean header.
    Bool(bool),
    /// Numeric header.
    Number(f64),
    /// String header.
    String(String),
    /// Null header (admitted for Ably compatibility).
    Null,
}

impl HeaderValue {
    /// Whether this is a null header.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// The string value, if this is a string header.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    /// The numeric value, if this is a number header.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Number(value) => Some(*value),
            _ => None,
        }
    }

    /// The boolean value, if this is a bool header.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }
}

impl From<&str> for HeaderValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for HeaderValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<f64> for HeaderValue {
    fn from(value: f64) -> Self {
        Self::Number(value)
    }
}

impl From<i64> for HeaderValue {
    fn from(value: i64) -> Self {
        Self::Number(value as f64)
    }
}

impl From<bool> for HeaderValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

/// The Protocol V2 `extras` envelope.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Extras {
    /// Flat metadata used for filtering and mutable-message bookkeeping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, HeaderValue>>,
    /// Ephemeral messages skip recovery buffers and webhooks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ephemeral: Option<bool>,
    /// Server-side idempotency key. The server's JSON spelling is
    /// `idempotencyKey`; `idempotency_key` is accepted on input.
    #[serde(
        default,
        rename = "idempotencyKey",
        alias = "idempotency_key",
        skip_serializing_if = "Option::is_none"
    )]
    pub idempotency_key: Option<String>,
    /// Per-message echo override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub echo: Option<bool>,
    /// AI Transport metadata (`transport`, `codec`, and codec extensions).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ai: Option<Value>,
    /// Push payload, when the server forwards it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push: Option<Value>,
}

impl Extras {
    /// Empty extras.
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up a header.
    pub fn header(&self, name: &str) -> Option<&HeaderValue> {
        self.headers.as_ref()?.get(name)
    }

    /// Look up a string header.
    pub fn header_str(&self, name: &str) -> Option<&str> {
        self.header(name)?.as_str()
    }

    /// Set a header (builder style).
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<HeaderValue>) -> Self {
        self.headers
            .get_or_insert_with(HashMap::new)
            .insert(name.into(), value.into());
        self
    }

    /// Set the ephemeral flag (builder style).
    pub fn with_ephemeral(mut self, ephemeral: bool) -> Self {
        self.ephemeral = Some(ephemeral);
        self
    }

    /// Set the idempotency key (builder style).
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Set the echo override (builder style).
    pub fn with_echo(mut self, echo: bool) -> Self {
        self.echo = Some(echo);
        self
    }

    /// Set AI Transport metadata (builder style).
    pub fn with_ai(mut self, ai: Value) -> Self {
        self.ai = Some(ai);
        self
    }

    /// A string entry of `ai.transport`, e.g. `ai_transport("turn-id")`.
    pub fn ai_transport(&self, key: &str) -> Option<&str> {
        self.ai.as_ref()?.get("transport")?.get(key)?.as_str()
    }

    /// A string entry of `ai.codec` when the codec is a header map
    /// (Sockudo AI Transport).
    pub fn ai_codec(&self, key: &str) -> Option<&str> {
        self.ai.as_ref()?.get("codec")?.get(key)?.as_str()
    }

    /// The codec id when `ai.codec` is a string (Ably AI Transport), e.g.
    /// `vercel-ai-sdk-ui-message`.
    pub fn ai_codec_id(&self) -> Option<&str> {
        self.ai.as_ref()?.get("codec")?.as_str()
    }
}

/// A decoded server frame.
///
/// Events are shared between every callback and stream that receives them,
/// so handlers get `&Event` and streams yield `Arc<Event>`.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct Event {
    /// Event name, e.g. `price-updated` or `sockudo:subscription_succeeded`.
    pub event: String,
    /// Channel the event belongs to, if any.
    pub channel: Option<String>,
    /// Logical event name of a V2 versioned-message envelope. Durable
    /// history, rewind and mutations arrive as `sockudo:message.create` /
    /// `.update` / `.delete` / `.append` with the publisher's event name here.
    pub name: Option<String>,
    /// The payload.
    pub data: Payload,
    /// Publishing user (client events on presence channels).
    pub user_id: Option<String>,
    /// Server-assigned unique message id (deduplication).
    pub message_id: Option<String>,
    /// Opaque continuity token for recovery and history.
    pub stream_id: Option<String>,
    /// Per-channel serial (recovery position).
    pub serial: Option<u64>,
    /// Delta sequence number on full messages of a delta stream.
    pub sequence: Option<u64>,
    /// Delta conflation key.
    pub conflation_key: Option<String>,
    /// Protocol V2 extras.
    pub extras: Option<Extras>,
    /// Publication tags (when the server forwards them).
    pub tags: Option<BTreeMap<String, String>>,
    /// The original frame. Cheap to clone (reference counted).
    pub raw: Bytes,
}

impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Event")
            .field("event", &self.event)
            .field("channel", &self.channel)
            .field("name", &self.name)
            .field("data", &self.data)
            .field("user_id", &self.user_id)
            .field("message_id", &self.message_id)
            .field("stream_id", &self.stream_id)
            .field("serial", &self.serial)
            .field("sequence", &self.sequence)
            .field("has_extras", &self.extras.is_some())
            .finish()
    }
}

impl Event {
    /// Build an event by name (useful for tests and for replaying history
    /// items through [`crate::versioned::reduce_mutable_message_event`]).
    pub fn new(event: impl Into<String>) -> Self {
        Self {
            event: event.into(),
            ..Self::default()
        }
    }

    /// Set the channel (builder style).
    pub fn with_channel(mut self, channel: impl Into<String>) -> Self {
        self.channel = Some(channel.into());
        self
    }

    /// Set the payload (builder style).
    pub fn with_data(mut self, data: Payload) -> Self {
        self.data = data;
        self
    }

    /// Set extras (builder style).
    pub fn with_extras(mut self, extras: Extras) -> Self {
        self.extras = Some(extras);
        self
    }

    /// Set the serial (builder style).
    pub fn with_serial(mut self, serial: u64) -> Self {
        self.serial = Some(serial);
        self
    }

    /// Set the message id (builder style).
    pub fn with_message_id(mut self, id: impl Into<String>) -> Self {
        self.message_id = Some(id.into());
        self
    }

    /// Set the stream id (builder style).
    pub fn with_stream_id(mut self, id: impl Into<String>) -> Self {
        self.stream_id = Some(id.into());
        self
    }

    /// The publisher's event name: [`Event::name`] for versioned-message
    /// envelopes, otherwise the wire event name.
    pub fn logical_name(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.event)
    }

    /// Deserialize the payload into `T` (see [`Payload::json`]).
    pub fn json<T: DeserializeOwned>(&self) -> Result<T> {
        self.data.json()
    }

    /// The payload as a dynamic JSON value (see [`Payload::value`]).
    pub fn value(&self) -> Value {
        self.data.value()
    }

    /// The payload text, when it is textual.
    pub fn text(&self) -> Option<&str> {
        self.data.as_str()
    }

    /// Look up an `extras.headers` entry.
    pub fn header(&self, name: &str) -> Option<&HeaderValue> {
        self.extras.as_ref()?.header(name)
    }

    /// Clone this event under a different name, keeping every other field.
    pub(crate) fn renamed(&self, event: String) -> Self {
        Self {
            event,
            ..self.clone()
        }
    }
}

/// Deserialize a serial that may arrive as a JSON number or a numeric string.
///
/// A direct visitor (rather than an untagged enum) avoids buffering every
/// serial through serde's intermediate `Content` representation.
pub(crate) fn de_opt_u64<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<u64>, D::Error> {
    struct SerialVisitor;

    impl<'de> serde::de::Visitor<'de> for SerialVisitor {
        type Value = Option<u64>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("an unsigned integer, an integral float, a numeric string or null")
        }
        fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
            Ok(Some(value))
        }
        fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
            Ok(u64::try_from(value).ok())
        }
        fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E> {
            Ok(float_to_u64(value))
        }
        fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E> {
            Ok(value.trim().parse().ok())
        }
        fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_some<D: Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> std::result::Result<Self::Value, D::Error> {
            deserializer.deserialize_any(SerialVisitor)
        }
        fn visit_bool<E>(self, _value: bool) -> std::result::Result<Self::Value, E> {
            Ok(None)
        }
    }

    deserializer.deserialize_any(SerialVisitor)
}

/// Integral, non-negative floats within the exactly-representable range.
pub(crate) fn float_to_u64(value: f64) -> Option<u64> {
    const MAX_SAFE: f64 = 9_007_199_254_740_991.0;
    (value.is_finite() && value >= 0.0 && value.fract() == 0.0 && value <= MAX_SAFE)
        .then_some(value as u64)
}

/// Coerce a dynamic JSON value into a `u64` serial.
pub(crate) fn value_to_u64(value: &Value) -> Option<u64> {
    if let Some(unsigned) = value.as_u64() {
        return Some(unsigned);
    }
    if let Some(float) = value.as_f64() {
        return float_to_u64(float);
    }
    value.as_str().and_then(|text| text.trim().parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize, PartialEq)]
    struct Price {
        price: f64,
    }

    #[test]
    fn payload_json_parses_text_and_raw_json() {
        let text = Payload::Text(r#"{"price":1.5}"#.into());
        assert_eq!(text.json::<Price>().unwrap(), Price { price: 1.5 });
        let raw = Payload::Json(r#"{"price":2.0}"#.into());
        assert_eq!(raw.json::<Price>().unwrap(), Price { price: 2.0 });
    }

    #[test]
    fn plain_text_payload_deserializes_as_string() {
        let text = Payload::Text("hello world".into());
        assert_eq!(text.json::<String>().unwrap(), "hello world");
        assert!(text.json::<Price>().is_err());
        assert_eq!(text.value().as_str(), Some("hello world"));
    }

    #[test]
    fn serials_accept_numbers_and_strings() {
        #[derive(Deserialize)]
        struct S {
            #[serde(default, deserialize_with = "de_opt_u64")]
            serial: Option<u64>,
        }
        let parse = |raw: &str| sonic_rs::from_str::<S>(raw).unwrap().serial;
        assert_eq!(parse(r#"{"serial":42}"#), Some(42));
        assert_eq!(parse(r#"{"serial":"43"}"#), Some(43));
        assert_eq!(parse(r#"{"serial":44.0}"#), Some(44));
        assert_eq!(parse(r#"{"serial":1.5}"#), None);
        assert_eq!(parse(r#"{"serial":null}"#), None);
        assert_eq!(parse(r#"{}"#), None);
    }

    #[test]
    fn extras_headers_round_trip() {
        let extras = Extras::new()
            .with_header("sockudo_action", "message.create")
            .with_header("ttl", 5.0)
            .with_header("flag", true)
            .with_echo(false);
        let json = sonic_rs::to_string(&extras).unwrap();
        let back: Extras = sonic_rs::from_str(&json).unwrap();
        assert_eq!(back, extras);
        assert_eq!(back.header_str("sockudo_action"), Some("message.create"));
        assert_eq!(back.header("ttl").and_then(HeaderValue::as_f64), Some(5.0));
        assert_eq!(
            back.header("flag").and_then(HeaderValue::as_bool),
            Some(true)
        );
    }

    #[test]
    fn ai_helpers_read_transport_and_codec() {
        let extras = Extras::new().with_ai(sonic_rs::json!({
            "transport": {"turn-id": "t1"},
            "codec": {"format": "ai-sdk"}
        }));
        assert_eq!(extras.ai_transport("turn-id"), Some("t1"));
        assert_eq!(extras.ai_codec("format"), Some("ai-sdk"));
        assert_eq!(extras.ai_transport("missing"), None);
    }
}
