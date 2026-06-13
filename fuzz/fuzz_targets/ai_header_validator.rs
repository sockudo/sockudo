#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_protocol::messages::{AiExtras, MessageExtras};
use std::collections::HashMap;

fuzz_target!(|data: &[u8]| {
    let Ok(input) = std::str::from_utf8(data) else {
        return;
    };

    let mut transport = HashMap::new();
    for pair in input.split('&').take(40) {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        transport.insert(key.to_string(), value.to_string());
    }

    let extras = MessageExtras {
        ai: Some(AiExtras {
            transport: Some(transport),
            codec: None,
        }),
        ..Default::default()
    };
    let _ = extras.validate_ai_headers();
});
