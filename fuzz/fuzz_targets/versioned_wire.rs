#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_protocol::versioned_messages::VersionedRealtimeMessage;
use sockudo_protocol::wire::{
    WireFormat, deserialize_versioned_message, serialize_versioned_message,
};

const MAX_INPUT_BYTES: usize = 64 * 1024;
const FORMATS: [WireFormat; 3] = [
    WireFormat::Json,
    WireFormat::MessagePack,
    WireFormat::Protobuf,
];

fn assert_stable(message: &VersionedRealtimeMessage, format: WireFormat) {
    let encoded = serialize_versioned_message(message, format)
        .expect("typed versioned message must serialize");
    let decoded = deserialize_versioned_message(&encoded, format)
        .expect("serialized versioned message must deserialize");
    let reencoded = serialize_versioned_message(&decoded, format)
        .expect("decoded versioned message must serialize");
    let redecode = deserialize_versioned_message(&reencoded, format)
        .expect("re-serialized versioned message must deserialize");
    assert_eq!(
        redecode, decoded,
        "versioned wire normalization was not stable"
    );
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > MAX_INPUT_BYTES {
        return;
    }

    for source_format in FORMATS {
        let Ok(message) = deserialize_versioned_message(data, source_format) else {
            continue;
        };
        for target_format in FORMATS {
            assert_stable(&message, target_format);
        }
    }
});
