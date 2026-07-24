#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_ably_compat::fuzzing;

fuzz_target!(|data: &[u8]| fuzzing::continuity_and_state(data));
