//! JSON codec built on sonic-rs.

use std::collections::BTreeMap;

use bytes::Bytes;
use sonic_rs::{JsonType, JsonValueTrait};

use crate::codec::Outbound;
use crate::error::{Error, Result};
use crate::event::{Event, Extras, Payload, float_to_u64};

fn decode_error(error: impl std::fmt::Display) -> Error {
    Error::Decode(error.to_string())
}

/// JSON value kinds the envelope cares about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    String,
    Number,
    Null,
    Bool,
    Object,
    Array,
}

impl From<JsonType> for Kind {
    fn from(kind: JsonType) -> Self {
        match kind {
            JsonType::String => Self::String,
            JsonType::Number => Self::Number,
            JsonType::Null => Self::Null,
            JsonType::Boolean => Self::Bool,
            JsonType::Object => Self::Object,
            JsonType::Array => Self::Array,
        }
    }
}

#[derive(Default)]
struct Fields {
    event: Event,
    sequence: Option<u64>,
    delta_sequence: Option<u64>,
    conflation_key: Option<String>,
    delta_conflation_key: Option<String>,
}

impl Fields {
    /// Apply one top-level field. `raw` is the value's exact JSON text.
    fn apply(&mut self, key: &str, kind: Kind, raw: &str) -> Result<()> {
        let event = &mut self.event;
        match key {
            "event" => event.event = string_field(kind, raw)?.unwrap_or_default(),
            "channel" => event.channel = string_field(kind, raw)?,
            "name" => event.name = string_field(kind, raw)?,
            "data" => {
                event.data = match kind {
                    Kind::Null => Payload::None,
                    Kind::String => Payload::Text(unescape(raw)?),
                    _ => Payload::Json(raw.to_owned()),
                }
            }
            "user_id" => event.user_id = string_field(kind, raw)?,
            "message_id" => event.message_id = string_field(kind, raw)?,
            "stream_id" => event.stream_id = string_field(kind, raw)?,
            "serial" => event.serial = serial_field(kind, raw)?,
            "sequence" => self.sequence = serial_field(kind, raw)?,
            "__delta_seq" => self.delta_sequence = serial_field(kind, raw)?,
            "conflation_key" => self.conflation_key = string_field(kind, raw)?,
            "__conflation_key" => self.delta_conflation_key = string_field(kind, raw)?,
            // Lenient: an unexpected shape from a newer server must not make
            // the whole frame undeliverable.
            "extras" if kind == Kind::Object => {
                event.extras = sonic_rs::from_str::<Extras>(raw).ok()
            }
            "tags" if kind == Kind::Object => {
                event.tags = sonic_rs::from_str::<BTreeMap<String, String>>(raw).ok();
            }
            _ => {}
        }
        Ok(())
    }

    fn finish(mut self, raw: Bytes) -> Event {
        self.event.sequence = self.delta_sequence.or(self.sequence);
        self.event.conflation_key = self.delta_conflation_key.or(self.conflation_key);
        self.event.raw = raw;
        self.event
    }
}

/// Decode a JSON frame.
///
/// A hand-rolled scanner walks the top-level envelope in one pass (copying
/// escape-free strings straight out of the frame). Anything it does not
/// recognise falls back to sonic-rs' lazy object iterator, which also
/// performs full validation of the envelope structure. Unknown fields are
/// skipped, and fields with an unexpected type are ignored.
pub(crate) fn decode(raw: Bytes) -> Result<Event> {
    let text = std::str::from_utf8(&raw).map_err(decode_error)?;
    if let Some(fields) = fast_scan(text) {
        return Ok(fields?.finish(raw));
    }
    let fields = sonic_scan(text)?;
    Ok(fields.finish(raw))
}

fn sonic_scan(text: &str) -> Result<Fields> {
    if !text.trim_start().starts_with('{') {
        return Err(Error::Decode("frame is not a JSON object".into()));
    }
    // The lazy iterator tolerates trailing bytes; this rare path can afford
    // a full validation pass first.
    sonic_rs::from_str::<serde::de::IgnoredAny>(text).map_err(decode_error)?;
    let mut fields = Fields::default();
    for entry in sonic_rs::to_object_iter(text) {
        let (key, value) = entry.map_err(decode_error)?;
        fields.apply(&key, value.get_type().into(), value.as_raw_str())?;
    }
    Ok(fields)
}

/// Fast path. `None` means "not handled here" (fall back), never "invalid".
fn fast_scan(text: &str) -> Option<Result<Fields>> {
    let mut scanner = Scanner {
        bytes: text.as_bytes(),
        at: 0,
    };
    scanner.whitespace();
    scanner.eat(b'{')?;
    let mut fields = Fields::default();
    scanner.whitespace();
    if scanner.peek()? == b'}' {
        scanner.at += 1;
    } else {
        loop {
            scanner.whitespace();
            let (key_start, key_end, key_escaped) = scanner.string()?;
            if key_escaped {
                return None;
            }
            scanner.whitespace();
            scanner.eat(b':')?;
            scanner.whitespace();
            let (kind, value_start, value_end) = scanner.value()?;
            let key = &text[key_start + 1..key_end - 1];
            if let Err(error) = fields.apply(key, kind, &text[value_start..value_end]) {
                return Some(Err(error));
            }
            scanner.whitespace();
            match scanner.peek()? {
                b',' => scanner.at += 1,
                b'}' => {
                    scanner.at += 1;
                    break;
                }
                _ => return None,
            }
        }
    }
    scanner.whitespace();
    (scanner.at == scanner.bytes.len()).then_some(Ok(fields))
}

struct Scanner<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Scanner<'_> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    fn eat(&mut self, byte: u8) -> Option<()> {
        (self.peek()? == byte).then(|| self.at += 1)
    }

    fn whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.at += 1;
        }
    }

    /// A string literal starting at `at`: `(start, end, has_escapes)` with
    /// `end` one past the closing quote.
    fn string(&mut self) -> Option<(usize, usize, bool)> {
        let start = self.at;
        self.eat(b'"')?;
        let mut escaped = false;
        loop {
            // Raw control characters (invalid JSON) are tolerated as literal
            // characters rather than paying a second pass over every string.
            let offset = memchr::memchr2(b'"', b'\\', &self.bytes[self.at..])?;
            self.at += offset;
            match self.bytes[self.at] {
                b'"' => {
                    self.at += 1;
                    return Some((start, self.at, escaped));
                }
                b'\\' => {
                    escaped = true;
                    self.at += 2;
                    if self.at > self.bytes.len() {
                        return None;
                    }
                }
                _ => return None,
            }
        }
    }

    fn value(&mut self) -> Option<(Kind, usize, usize)> {
        let start = self.at;
        let kind = match self.peek()? {
            b'"' => {
                self.string()?;
                Kind::String
            }
            b'{' | b'[' => {
                let kind = if self.peek()? == b'{' {
                    Kind::Object
                } else {
                    Kind::Array
                };
                self.container()?;
                kind
            }
            b'-' | b'0'..=b'9' => {
                self.at += 1;
                while matches!(
                    self.peek(),
                    Some(b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
                ) {
                    self.at += 1;
                }
                Kind::Number
            }
            b't' => {
                self.literal(b"true")?;
                Kind::Bool
            }
            b'f' => {
                self.literal(b"false")?;
                Kind::Bool
            }
            b'n' => {
                self.literal(b"null")?;
                Kind::Null
            }
            _ => return None,
        };
        Some((kind, start, self.at))
    }

    fn literal(&mut self, word: &[u8]) -> Option<()> {
        self.bytes
            .get(self.at..)?
            .starts_with(word)
            .then(|| self.at += word.len())
    }

    /// Skip a balanced object/array, honouring strings. Structure inside is
    /// checked for balance only; nested values are validated when parsed.
    fn container(&mut self) -> Option<()> {
        let mut stack: Vec<u8> = Vec::new();
        loop {
            match self.peek()? {
                b'"' => {
                    self.string()?;
                    continue;
                }
                open @ (b'{' | b'[') => stack.push(if open == b'{' { b'}' } else { b']' }),
                close @ (b'}' | b']') => {
                    if stack.pop()? != close {
                        return None;
                    }
                    if stack.is_empty() {
                        self.at += 1;
                        return Some(());
                    }
                }
                _ => {}
            }
            self.at += 1;
        }
    }
}

fn string_field(kind: Kind, raw: &str) -> Result<Option<String>> {
    match kind {
        Kind::String => unescape(raw).map(Some),
        _ => Ok(None),
    }
}

fn serial_field(kind: Kind, raw: &str) -> Result<Option<u64>> {
    Ok(match kind {
        Kind::Number => match raw.parse::<u64>() {
            Ok(number) => Some(number),
            Err(_) => raw.parse::<f64>().ok().and_then(float_to_u64),
        },
        Kind::String => unescape(raw)?.trim().parse().ok(),
        _ => None,
    })
}

/// Unescape a raw JSON string literal (including its quotes).
pub(crate) fn unescape(literal: &str) -> Result<String> {
    let inner = literal
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .ok_or_else(|| Error::Decode("malformed JSON string".into()))?;
    let bytes = inner.as_bytes();
    let Some(first) = memchr::memchr(b'\\', bytes) else {
        return Ok(inner.to_owned());
    };
    let mut out = String::with_capacity(inner.len());
    out.push_str(&inner[..first]);
    let mut index = first;
    while index < bytes.len() {
        // `index` always points at a backslash here.
        let escape = *bytes
            .get(index + 1)
            .ok_or_else(|| Error::Decode("truncated escape".into()))?;
        index += 2;
        match escape {
            b'"' => out.push('"'),
            b'\\' => out.push('\\'),
            b'/' => out.push('/'),
            b'b' => out.push('\u{8}'),
            b'f' => out.push('\u{c}'),
            b'n' => out.push('\n'),
            b'r' => out.push('\r'),
            b't' => out.push('\t'),
            b'u' => {
                let high = hex4(bytes, index)?;
                index += 4;
                let code = if (0xD800..0xDC00).contains(&high) {
                    // A high surrogate must be followed by `\uDC00`–`\uDFFF`.
                    if bytes.get(index) == Some(&b'\\') && bytes.get(index + 1) == Some(&b'u') {
                        let low = hex4(bytes, index + 2)?;
                        if (0xDC00..0xE000).contains(&low) {
                            index += 6;
                            0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00)
                        } else {
                            0xFFFD
                        }
                    } else {
                        0xFFFD
                    }
                } else if (0xDC00..0xE000).contains(&high) {
                    0xFFFD
                } else {
                    high
                };
                out.push(char::from_u32(code).unwrap_or('\u{FFFD}'));
            }
            _ => return Err(Error::Decode("invalid escape in JSON string".into())),
        }
        match memchr::memchr(b'\\', &bytes[index..]) {
            Some(offset) => {
                out.push_str(&inner[index..index + offset]);
                index += offset;
            }
            None => {
                out.push_str(&inner[index..]);
                break;
            }
        }
    }
    Ok(out)
}

fn hex4(bytes: &[u8], at: usize) -> Result<u32> {
    let digits = bytes
        .get(at..at + 4)
        .ok_or_else(|| Error::Decode("truncated unicode escape".into()))?;
    digits.iter().try_fold(0u32, |acc, digit| {
        let value = (*digit as char)
            .to_digit(16)
            .ok_or_else(|| Error::Decode("invalid unicode escape".into()))?;
        Ok(acc * 16 + value)
    })
}

/// Encode an outbound frame: `{"event":…,"channel":…,"data":…}`.
pub(crate) fn encode(frame: Outbound<'_>) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(
        frame.event.len() + frame.channel.map_or(0, str::len) + frame.data.len() + 40,
    );
    out.extend_from_slice(b"{\"event\":");
    write_json_str(&mut out, frame.event)?;
    if let Some(channel) = frame.channel {
        out.extend_from_slice(b",\"channel\":");
        write_json_str(&mut out, channel)?;
    }
    out.extend_from_slice(b",\"data\":");
    if frame.data.is_empty() {
        out.extend_from_slice(b"{}");
    } else {
        out.extend_from_slice(frame.data);
    }
    out.push(b'}');
    Ok(out)
}

fn write_json_str(out: &mut Vec<u8>, value: &str) -> Result<()> {
    sonic_rs::to_writer(&mut *out, value).map_err(|error| Error::Encode(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_str(raw: &str) -> Event {
        decode(Bytes::copy_from_slice(raw.as_bytes())).unwrap()
    }

    #[test]
    fn decodes_pusher_style_string_data() {
        let event =
            decode_str(r#"{"event":"price","channel":"btc","data":"{\"p\":1}","user_id":"u1"}"#);
        assert_eq!(event.event, "price");
        assert_eq!(event.channel.as_deref(), Some("btc"));
        assert_eq!(event.data, Payload::Text(r#"{"p":1}"#.into()));
        assert_eq!(event.user_id.as_deref(), Some("u1"));
    }

    #[test]
    fn decodes_object_data_as_raw_json() {
        let event = decode_str(r#"{"event":"x","data":{"a":[1,2],"b":"c"}}"#);
        assert_eq!(event.data, Payload::Json(r#"{"a":[1,2],"b":"c"}"#.into()));
    }

    #[test]
    fn decodes_v2_metadata_and_delta_markers() {
        let event = decode_str(
            r#"{"event":"e","channel":"c","data":"d","message_id":"m1","stream_id":"s1","serial":"7","extras":{"headers":{"k":"v","n":1},"echo":false},"__delta_seq":3,"__conflation_key":"btc"}"#,
        );
        assert_eq!(event.message_id.as_deref(), Some("m1"));
        assert_eq!(event.stream_id.as_deref(), Some("s1"));
        assert_eq!(event.serial, Some(7));
        assert_eq!(event.sequence, Some(3));
        assert_eq!(event.conflation_key.as_deref(), Some("btc"));
        let extras = event.extras.unwrap();
        assert_eq!(extras.header_str("k"), Some("v"));
        assert_eq!(extras.echo, Some(false));
    }

    #[test]
    fn tolerates_unknown_fields_and_malformed_extras() {
        let event =
            decode_str(r#"{"event":"e","future":{"x":1},"extras":{"headers":[1,2]},"data":null}"#);
        assert_eq!(event.event, "e");
        assert!(event.extras.is_none());
        assert!(event.data.is_none());
    }

    #[test]
    fn escaped_string_payload_is_unescaped() {
        let event = decode_str(r#"{"event":"e","data":"line\nnext \"q\" é"}"#);
        assert_eq!(event.data, Payload::Text("line\nnext \"q\" é".into()));
    }

    #[test]
    fn unescape_matches_sonic_on_tricky_strings() {
        let cases = [
            r#""""#,
            r#""plain""#,
            r#""a\"b\\c\/d""#,
            r#""\b\f\n\r\t""#,
            r#""\u00e9\u4e2d\ud83d\ude00 tail""#,
            r#""lone \ud83d high""#,
            r#""lone \ude00 low""#,
            r#""\ud83dx""#,
            r#""mixed é 中 \"quoted\" \\""#,
        ];
        for case in cases {
            let ours = unescape(case).unwrap();
            let theirs: String = sonic_rs::from_str(case).unwrap_or_else(|_| ours.clone());
            assert_eq!(ours, theirs, "{case}");
        }
        assert!(unescape(r#""bad \x escape""#).is_err());
        assert!(unescape(r#""trunc \u12""#).is_err());
        assert!(unescape(r#""ends with \""#).is_err());
        assert!(unescape("no quotes").is_err());
    }

    #[test]
    fn ignores_mistyped_fields_and_reads_numeric_strings() {
        let event = decode_str(
            r#"{"event":"e","channel":7,"serial":"12","__delta_seq":3.0,"sequence":-1}"#,
        );
        assert_eq!(event.channel, None);
        assert_eq!(event.serial, Some(12));
        assert_eq!(event.sequence, Some(3));
    }

    #[test]
    fn fast_path_agrees_with_sonic_path() {
        let frames = [
            r#"{"event":"e","channel":"c","data":"{\"p\":1}","serial":3,"message_id":"m"}"#,
            r#" { "event" : "e" , "data" : { "a" : [1, {"b": "]}"}] } , "x": [[], {}], "n": null } "#,
            r#"{"event":"e","data":[1,"two",{"three":3}],"extras":{"headers":{"k":"v"}},"tags":{"t":"1"}}"#,
            r#"{"event":"e\u00e9","data":"\ud83d\ude00","__delta_seq":7,"__conflation_key":"k\"q"}"#,
            r#"{"event":"e","data":true,"serial":"9","sequence":-1.5,"stream_id":null}"#,
            r#"{}"#,
            r#"{"event":"x","data":"with\ttab and \\ backslash"}"#,
        ];
        for frame in frames {
            let fast = fast_scan(frame)
                .expect("fast path handles well-formed frames")
                .unwrap();
            let slow = sonic_scan(frame).unwrap();
            let (fast, slow) = (fast.finish(Bytes::new()), slow.finish(Bytes::new()));
            assert_eq!(format!("{fast:?}"), format!("{slow:?}"), "{frame}");
            assert_eq!(fast.data, slow.data, "{frame}");
            assert_eq!(fast.extras, slow.extras, "{frame}");
            assert_eq!(fast.tags, slow.tags, "{frame}");
            assert_eq!(fast.conflation_key, slow.conflation_key, "{frame}");
        }
    }

    #[test]
    fn fast_path_defers_on_anything_unusual() {
        for frame in [
            r#"{"ev\u0065nt":"e"}"#,     // escaped key
            r#"{"event":"e"} trailing"#, // trailing garbage
            r#"{"event":"e",}"#,         // trailing comma
            r#"{"event":"e" "data":1}"#, // missing comma
            r#"{"data":{"a":1]}"#,       // mismatched brackets
            r#"{"event":"e""#,           // unterminated
        ] {
            assert!(fast_scan(frame).is_none(), "{frame}");
        }
        // The fallback still rejects what is actually invalid.
        assert!(decode(Bytes::from_static(br#"{"event":"e",}"#)).is_err());
        assert!(decode(Bytes::from_static(br#"{"event":"e"} trailing"#)).is_err());
        // ...and accepts what is valid but unusual.
        assert_eq!(
            decode(Bytes::from_static(br#"{"ev\u0065nt":"e"}"#))
                .unwrap()
                .event,
            "e"
        );
    }

    #[test]
    fn rejects_non_json() {
        assert!(decode(Bytes::from_static(b"not json")).is_err());
        assert!(decode(Bytes::from_static(b"[1,2]")).is_err());
        assert!(decode(Bytes::from_static(b"{\"event\":")).is_err());
        assert!(decode(Bytes::from_static(&[b'{', 0xff, b'}'])).is_err());
    }

    #[test]
    fn encodes_frames() {
        let out = encode(Outbound {
            event: "sockudo:subscribe",
            channel: None,
            data: br#"{"channel":"a\"b"}"#,
        })
        .unwrap();
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            r#"{"event":"sockudo:subscribe","data":{"channel":"a\"b"}}"#
        );
        let out = encode(Outbound {
            event: "client-x",
            channel: Some("private-\"c"),
            data: b"",
        })
        .unwrap();
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            r#"{"event":"client-x","channel":"private-\"c","data":{}}"#
        );
    }
}
