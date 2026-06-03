#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_core::history::HistoryCursor;
use sockudo_core::presence_history::PresenceHistoryCursor;

const MAX_INPUT_BYTES: usize = 16 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    let Ok(input) = std::str::from_utf8(data) else {
        return;
    };

    let _ = HistoryCursor::decode(input);
    let _ = PresenceHistoryCursor::decode(input);
});
