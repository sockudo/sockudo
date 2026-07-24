#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_protocol::messages::{MessageData, PusherMessage};
use sockudo_protocol::wire::{WireFormat, deserialize_message, serialize_message};

const MAX_INPUT_BYTES: usize = 64 * 1024;
const FORMATS: [WireFormat; 3] = [
    WireFormat::Json,
    WireFormat::MessagePack,
    WireFormat::Protobuf,
];

fn assert_stable(message: &PusherMessage, format: WireFormat) {
    let encoded = serialize_message(message, format).expect("typed messages must serialize");
    let decoded =
        deserialize_message(&encoded, format).expect("serialized messages must deserialize");
    let reencoded = serialize_message(&decoded, format).expect("decoded messages must serialize");
    let redecode =
        deserialize_message(&reencoded, format).expect("re-serialized messages must deserialize");
    assert_eq!(redecode, decoded, "wire normalization was not stable");
}

fn message_with_data(data: MessageData) -> PusherMessage {
    PusherMessage {
        event: Some("client-event".to_string()),
        channel: Some("public-room".to_string()),
        data: Some(data),
        name: None,
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: None,
        stream_id: None,
        serial: None,
        idempotency_key: None,
        extras: None,
        delta_sequence: None,
        delta_conflation_key: None,
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    if let Ok(message) = serde_json::from_slice::<PusherMessage>(data) {
        for format in FORMATS {
            assert_stable(&message, format);
        }
    }

    let text = String::from_utf8_lossy(data).into_owned();
    let text_message = message_with_data(MessageData::String(text));
    let binary_message = message_with_data(MessageData::Binary(data.to_vec()));
    for format in FORMATS {
        assert_stable(&text_message, format);
        assert_stable(&binary_message, format);
    }

    if let Ok(value) = sonic_rs::from_slice(data) {
        let json_message = message_with_data(MessageData::Json(value));
        for format in FORMATS {
            assert_stable(&json_message, format);
        }
    }
});
