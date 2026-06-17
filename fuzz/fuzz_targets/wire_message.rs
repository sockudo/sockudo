#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_protocol::wire::{WireFormat, deserialize_message};

fuzz_target!(|data: &[u8]| {
    let _ = deserialize_message(data, WireFormat::Json);
});
