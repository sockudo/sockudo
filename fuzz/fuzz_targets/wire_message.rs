#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_protocol::wire::{WireFormat, deserialize_message, serialize_message};

const MAX_INPUT_BYTES: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > MAX_INPUT_BYTES {
        return;
    }

    let format = match data[0] % 3 {
        0 => WireFormat::Json,
        1 => WireFormat::MessagePack,
        _ => WireFormat::Protobuf,
    };
    let Ok(message) = deserialize_message(&data[1..], format) else {
        return;
    };
    let encoded = serialize_message(&message, format).expect("decoded messages must serialize");
    let decoded = deserialize_message(&encoded, format).expect("serialized messages must decode");
    assert!(decoded == message, "wire round trip changed the message");
});
