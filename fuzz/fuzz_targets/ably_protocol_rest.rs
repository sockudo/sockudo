#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_ably_compat::fuzzing;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    fuzzing::protocol_and_rest(&data[1..], data[0] & 1 == 1);
});
