//! Wire conformance against the server's own `sockudo-protocol` crate:
//! server-encoded frames decode correctly here, client-encoded frames decode
//! correctly on the server, and delta bases match the server byte for byte.
#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use sockudo_client::__private as client;
use sockudo_client::{HeaderValue, Payload, WireFormat};
use sockudo_protocol::messages::{
    AiExtras, ExtrasValue, MessageData, MessageExtras, PusherMessage,
};
use sockudo_protocol::wire::{self, WireFormat as ServerFormat};
use sonic_rs::JsonValueTrait;

fn formats() -> Vec<(WireFormat, ServerFormat)> {
    #[allow(unused_mut)]
    let mut formats = vec![(WireFormat::Json, ServerFormat::Json)];
    #[cfg(feature = "msgpack")]
    formats.push((WireFormat::MessagePack, ServerFormat::MessagePack));
    #[cfg(feature = "protobuf")]
    formats.push((WireFormat::Protobuf, ServerFormat::Protobuf));
    formats
}

fn base_message(data: MessageData) -> PusherMessage {
    let mut message: PusherMessage = sonic_rs::from_str(r#"{"event":"price.updated"}"#).unwrap();
    message.channel = Some("market:btc".into());
    message.data = Some(data);
    message
}

fn rich_message() -> PusherMessage {
    let mut message = base_message(MessageData::String(
        r#"{"price":101.25,"note":"é \"q\""}"#.into(),
    ));
    message.user_id = Some("u1".into());
    message.message_id = Some("msg-1".into());
    message.stream_id = Some("stream-1".into());
    message.serial = Some(42);
    message.tags = Some(BTreeMap::from([("market".into(), "spot".into())]));
    let ai: AiExtras = serde_json::from_value(serde_json::json!({
        "transport": {"turn-id": "t1"},
        "codec": {"format": "ai-sdk"},
        "type": "text-delta",
        "stream": true
    }))
    .unwrap();
    message.extras = Some(MessageExtras {
        headers: Some(HashMap::from([
            (
                "sockudo_action".into(),
                ExtrasValue::String("message.create".into()),
            ),
            ("ttl".into(), ExtrasValue::Number(5.0)),
            ("pinned".into(), ExtrasValue::Bool(true)),
        ])),
        ephemeral: Some(true),
        idempotency_key: Some("idem-1".into()),
        echo: Some(false),
        ai: Some(ai),
        ..MessageExtras::default()
    });
    message
}

fn decode(format: (WireFormat, ServerFormat), message: &PusherMessage) -> sockudo_client::Event {
    let bytes = wire::serialize_message(message, format.1).unwrap();
    client::decode(format.0, Bytes::from(bytes), !format.0.is_binary()).unwrap()
}

#[test]
fn server_frames_decode_in_every_format() {
    for format in formats() {
        let event = decode(format, &rich_message());
        assert_eq!(event.event, "price.updated", "{format:?}");
        assert_eq!(event.channel.as_deref(), Some("market:btc"));
        assert_eq!(event.user_id.as_deref(), Some("u1"));
        assert_eq!(event.message_id.as_deref(), Some("msg-1"));
        assert_eq!(event.stream_id.as_deref(), Some("stream-1"));
        assert_eq!(event.serial, Some(42));
        assert_eq!(event.value()["price"].as_f64(), Some(101.25));
        assert_eq!(event.value()["note"].as_str(), Some("é \"q\""));
        assert_eq!(
            event
                .tags
                .as_ref()
                .unwrap()
                .get("market")
                .map(String::as_str),
            Some("spot"),
            "{format:?}"
        );

        let extras = event.extras.as_ref().unwrap();
        assert_eq!(
            extras.header_str("sockudo_action"),
            Some("message.create"),
            "{format:?}"
        );
        assert_eq!(extras.header("ttl"), Some(&HeaderValue::Number(5.0)));
        assert_eq!(extras.header("pinned"), Some(&HeaderValue::Bool(true)));
        assert_eq!(extras.ephemeral, Some(true));
        assert_eq!(
            extras.idempotency_key.as_deref(),
            Some("idem-1"),
            "{format:?}"
        );
        assert_eq!(extras.echo, Some(false));
        assert_eq!(extras.ai_transport("turn-id"), Some("t1"), "{format:?}");
        assert_eq!(extras.ai_codec("format"), Some("ai-sdk"));
        assert_eq!(
            extras.ai.as_ref().unwrap()["type"].as_str(),
            Some("text-delta"),
            "AI codec extensions survive {format:?}"
        );
    }
}

#[test]
fn every_data_variant_decodes() {
    for format in formats() {
        let text = decode(format, &base_message(MessageData::String("plain".into())));
        assert_eq!(text.data, Payload::Text("plain".into()), "{format:?}");

        let json = decode(
            format,
            &base_message(MessageData::Json(sonic_rs::json!({"a": [1, 2]}))),
        );
        assert_eq!(json.value(), sonic_rs::json!({"a": [1, 2]}), "{format:?}");

        let mut extra = ahash::AHashMap::new();
        extra.insert("auth".to_owned(), sonic_rs::json!("k:s"));
        let structured = decode(
            format,
            &base_message(MessageData::Structured {
                channel_data: Some(r#"{"user_id":"1"}"#.into()),
                channel: Some("presence-x".into()),
                user_data: None,
                extra,
            }),
        );
        let value = structured.value();
        assert_eq!(value["channel"].as_str(), Some("presence-x"), "{format:?}");
        assert_eq!(value["auth"].as_str(), Some("k:s"), "{format:?}");
        assert_eq!(value["channel_data"].as_str(), Some(r#"{"user_id":"1"}"#));
    }
    #[cfg(any(feature = "msgpack", feature = "protobuf"))]
    for format in formats().into_iter().filter(|f| f.0.is_binary()) {
        let binary = decode(
            format,
            &base_message(MessageData::Binary(vec![0, 1, 0xfe, 0xff])),
        );
        assert_eq!(
            binary.data,
            Payload::Binary(Bytes::from_static(&[0, 1, 0xfe, 0xff])),
            "{format:?}"
        );
    }
}

#[test]
fn delta_markers_decode_in_every_format() {
    for format in formats() {
        let mut message = base_message(MessageData::String("{}".into()));
        message.delta_sequence = Some(9);
        message.delta_conflation_key = Some("BTC".into());
        let event = decode(format, &message);
        assert_eq!(event.sequence, Some(9), "{format:?}");
        assert_eq!(event.conflation_key.as_deref(), Some("BTC"), "{format:?}");
    }
}

#[test]
fn client_frames_decode_on_the_server() {
    let data = br#"{"channel":"private-x","auth":"k:s","channel_data":"{\"user_id\":\"1\"}"}"#;
    for format in formats() {
        let bytes = client::encode(format.0, "sockudo:subscribe", None, data).unwrap();
        let message = wire::deserialize_message(&bytes, format.1).unwrap();
        assert_eq!(
            message.event.as_deref(),
            Some("sockudo:subscribe"),
            "{format:?}"
        );
        assert_eq!(message.channel, None);
        let value = match message.data.unwrap() {
            MessageData::Json(value) => value,
            MessageData::Structured { channel, extra, .. } => {
                assert_eq!(channel.as_deref(), Some("private-x"));
                assert_eq!(extra.get("auth").and_then(|v| v.as_str()), Some("k:s"));
                continue;
            }
            other => panic!("unexpected subscribe data {other:?} for {format:?}"),
        };
        assert_eq!(value["channel"].as_str(), Some("private-x"), "{format:?}");
        assert_eq!(value["auth"].as_str(), Some("k:s"));

        let bytes =
            client::encode(format.0, "client-move", Some("private-x"), br#"{"x":1}"#).unwrap();
        let message = wire::deserialize_message(&bytes, format.1).unwrap();
        assert_eq!(message.event.as_deref(), Some("client-move"));
        assert_eq!(message.channel.as_deref(), Some("private-x"), "{format:?}");
    }
}

/// The server stores `serialize(base)` as the delta base and sends
/// `serialize(base + markers)`. Stripping the markers must reproduce the
/// stored base exactly, or every subsequent delta fails its checksum.
#[test]
fn stripping_markers_reproduces_the_server_delta_base() {
    let mut cases = vec![
        base_message(MessageData::String(r#"{"price":100.10,"qty":3}"#.into())),
        base_message(MessageData::Json(
            sonic_rs::json!({"p": 1.0e-7, "s": "x\"y", "n": null}),
        )),
        rich_message(),
    ];
    let mut tricky = base_message(MessageData::String(
        r#"{"__delta_seq":5,"__conflation_key":"x"}"#.into(),
    ));
    tricky.message_id = Some("m".into());
    cases.push(tricky);

    for base in cases {
        let base_bytes = wire::serialize_message(&base, ServerFormat::Json).unwrap();
        for (sequence, key) in [
            (0u64, None),
            (17, Some("BTC")),
            (u64::from(u32::MAX), Some("a\"b\\c")),
        ] {
            let mut full = base.clone();
            full.delta_sequence = Some(sequence);
            full.delta_conflation_key = key.map(str::to_owned);
            let full_bytes = wire::serialize_message(&full, ServerFormat::Json).unwrap();
            assert_eq!(
                client::strip_delta_markers(&full_bytes),
                base_bytes,
                "sequence {sequence} key {key:?}\nfull: {}",
                String::from_utf8_lossy(&full_bytes)
            );
        }
    }
}

#[test]
fn client_extras_serialize_with_the_server_spelling() {
    let extras = sockudo_client::Extras::new()
        .with_idempotency_key("idem-9")
        .with_header("k", "v")
        .with_echo(true);
    let json = sonic_rs::to_string(&extras).unwrap();
    let server: MessageExtras = serde_json::from_str(&json).unwrap();
    assert_eq!(server.idempotency_key.as_deref(), Some("idem-9"), "{json}");
    assert!(
        server.opaque.is_empty(),
        "no field fell into the opaque map: {json}"
    );
    assert_eq!(server.echo, Some(true));
}

#[test]
fn null_headers_and_codec_ids_decode_in_every_format() {
    for format in formats() {
        let mut message = base_message(MessageData::String("{}".into()));
        let ai: AiExtras = serde_json::from_value(serde_json::json!({
            "transport": {"turn-id": "t9"},
            "codec": "vercel-ai-sdk-ui-message"
        }))
        .unwrap();
        message.extras = Some(MessageExtras {
            headers: Some(HashMap::from([
                ("cleared".into(), ExtrasValue::Null),
                ("kept".into(), ExtrasValue::String("v".into())),
            ])),
            ai: Some(ai),
            ..MessageExtras::default()
        });
        let event = decode(format, &message);
        let extras = event.extras.as_ref().unwrap();
        assert_eq!(
            extras.ai_codec_id(),
            Some("vercel-ai-sdk-ui-message"),
            "{format:?}"
        );
        assert_eq!(extras.ai_transport("turn-id"), Some("t9"), "{format:?}");
        assert_eq!(extras.header_str("kept"), Some("v"), "{format:?}");
        let cleared = extras.header("cleared");
        if format.1 == ServerFormat::Protobuf {
            // Protobuf has no null header: the server omits the key.
            assert_eq!(cleared, None);
        } else {
            assert_eq!(cleared, Some(&HeaderValue::Null), "{format:?}");
        }
    }
}
