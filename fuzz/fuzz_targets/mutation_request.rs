#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_protocol::versioned_messages::{
    AppendMessageRequest, DeleteMessageRequest, UpdateMessageRequest,
};

const MAX_INPUT_BYTES: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    if let Ok(request) = serde_json::from_slice::<UpdateMessageRequest>(data) {
        let _ = request.validate();
        if let Some(extras) = request.extras.as_ref() {
            let _ = extras.validate_ai_headers();
        }
    }

    if let Ok(request) = serde_json::from_slice::<DeleteMessageRequest>(data) {
        let _ = request.validate();
        if let Some(extras) = request.extras.as_ref() {
            let _ = extras.validate_ai_headers();
        }
    }

    if let Ok(request) = serde_json::from_slice::<AppendMessageRequest>(data) {
        let _ = request.validate();
        if let Some(extras) = request.extras.as_ref() {
            let _ = extras.validate_ai_headers();
        }
    }
});
