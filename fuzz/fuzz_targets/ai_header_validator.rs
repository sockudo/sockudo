#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_protocol::messages::{
    AI_CODEC_PROVIDER_METADATA_MAX_BYTES, AI_TRANSPORT_KEY_MAX_BYTES, AI_TRANSPORT_TIER_LIMIT,
    AI_TRANSPORT_VALUE_MAX_BYTES, AiExtras, MessageExtras,
};
use std::collections::HashMap;

const MAX_INPUT_BYTES: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    if let Ok(extras) = serde_json::from_slice::<MessageExtras>(data) {
        let _ = extras.validate_ai_headers();
        if let Ok(Some(headers)) = extras.validated_ai_transport_headers() {
            let _ = headers.run_id();
            let _ = headers.run_client_identity();
            let _ = headers.status();
            let _ = headers.iter().count();
        }
    }

    let Ok(input) = std::str::from_utf8(data) else {
        return;
    };

    let mut transport = HashMap::new();
    let mut codec = HashMap::new();
    for pair in input.split('&').take(40) {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        if let Some(key) = key.strip_prefix("codec.") {
            codec.insert(key.to_string(), value.to_string());
        } else {
            transport.insert(key.to_string(), value.to_string());
        }
    }

    let extras = MessageExtras {
        ai: Some(AiExtras {
            transport: Some(transport.clone()),
            codec: Some(codec.clone()),
        }),
        ..Default::default()
    };
    let result = extras.validate_ai_headers();
    let exceeds_public_bound = transport.len() > AI_TRANSPORT_TIER_LIMIT
        || codec.len() > AI_TRANSPORT_TIER_LIMIT
        || transport.iter().any(|(key, value)| {
            key.len() > AI_TRANSPORT_KEY_MAX_BYTES || value.len() > AI_TRANSPORT_VALUE_MAX_BYTES
        })
        || codec.iter().any(|(key, value)| {
            key.len() > AI_TRANSPORT_KEY_MAX_BYTES
                || value.len()
                    > if key == "providerMetadata" {
                        AI_CODEC_PROVIDER_METADATA_MAX_BYTES
                    } else {
                        AI_TRANSPORT_VALUE_MAX_BYTES
                    }
        });
    if exceeds_public_bound {
        assert!(
            result.is_err(),
            "AI headers exceeding a public bound were accepted"
        );
    }
});
