#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_core::message_envelope::{
    EncodingChain, MessageContent, decode_stored_message_payload,
};
use sockudo_protocol::messages::MessageData;

const MAX_INPUT_BYTES: usize = 64 * 1024;

fn json_semantically_equal(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    match (left, right) {
        (serde_json::Value::Number(left), serde_json::Value::Number(right)) => {
            match (left.as_i64(), right.as_i64()) {
                (Some(left), Some(right)) => left == right,
                _ => match (left.as_u64(), right.as_u64()) {
                    (Some(left), Some(right)) => left == right,
                    _ => match (left.as_f64(), right.as_f64()) {
                        (Some(left), Some(right)) => {
                            left == right
                                || (left - right).abs()
                                    <= left.abs().max(right.abs()).max(1.0) * f64::EPSILON
                        }
                        _ => false,
                    },
                },
            }
        }
        (serde_json::Value::Array(left), serde_json::Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| json_semantically_equal(left, right))
        }
        (serde_json::Value::Object(left), serde_json::Value::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, left)| {
                    right
                        .get(key)
                        .is_some_and(|right| json_semantically_equal(left, right))
                })
        }
        _ => left == right,
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    let text = String::from_utf8_lossy(data).into_owned();
    let encoding = EncodingChain::new(text.clone());
    if text.is_empty() {
        assert!(encoding.is_none());
    } else {
        assert_eq!(
            encoding.as_ref().map(EncodingChain::as_str),
            Some(text.as_str())
        );
    }

    let string_data = MessageData::String(text.clone());
    assert_eq!(string_data.as_string(), Some(text.as_str()));
    assert_eq!(string_data.clone().into_string(), Some(text));
    assert!(matches!(
        MessageContent::from_message_data(&string_data),
        Ok(MessageContent::Text(_))
    ));

    let binary_data = MessageData::Binary(data.to_vec());
    assert!(binary_data.as_string().is_none());
    assert!(matches!(
        MessageContent::from_message_data(&binary_data),
        Ok(MessageContent::Binary(value)) if value == data
    ));

    if let Ok(message_data) = serde_json::from_slice::<MessageData>(data) {
        let encoded = serde_json::to_vec(&message_data).expect("message data must serialize");
        let decoded = serde_json::from_slice::<MessageData>(&encoded)
            .expect("serialized message data must deserialize");
        let reencoded = serde_json::to_vec(&decoded).expect("message data must re-serialize");
        let _: MessageData = serde_json::from_slice(&reencoded)
            .expect("re-serialized message data must deserialize");
        let normalized = serde_json::from_slice::<serde_json::Value>(&encoded)
            .expect("serialized message data must remain valid JSON");
        let renormalized = serde_json::from_slice::<serde_json::Value>(&reencoded)
            .expect("re-serialized message data must remain valid JSON");
        assert!(
            json_semantically_equal(&renormalized, &normalized),
            "message data normalization changed JSON semantics"
        );
        let _ = MessageContent::from_message_data(&decoded);
    }

    let _ = decode_stored_message_payload(data);
});
