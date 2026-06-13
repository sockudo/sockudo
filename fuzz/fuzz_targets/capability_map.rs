#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_core::websocket::ConnectionCapabilities;

const MAX_INPUT_BYTES: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    let Ok(capabilities) = serde_json::from_slice::<ConnectionCapabilities>(data) else {
        return;
    };

    for channel in ["public-room", "private-room", "presence-room", "ai-room"] {
        let _ = capabilities.allows_subscribe(channel);
        let _ = capabilities.allows_publish(channel);
        let _ = capabilities.allows_history(channel);
        let _ = capabilities.allows_annotation_subscribe(channel);
        let _ = capabilities.allows_annotation_publish(channel);
    }
});
