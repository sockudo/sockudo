#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_core::versioned_message_auth::MutationKind;
use sockudo_core::websocket::ConnectionCapabilities;

const MAX_INPUT_BYTES: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    let Ok(capabilities) = serde_json::from_slice::<ConnectionCapabilities>(data) else {
        return;
    };

    let encoded = serde_json::to_vec(&capabilities).expect("capabilities must serialize");
    let decoded = serde_json::from_slice::<ConnectionCapabilities>(&encoded)
        .expect("serialized capabilities must deserialize");
    assert_eq!(decoded, capabilities);

    for channel in [
        "",
        "public-room",
        "private-room",
        "presence-room",
        "ai-room",
        "unicode-☃",
    ] {
        let _ = capabilities.allows_subscribe(channel);
        let _ = capabilities.allows_publish(channel);
        let _ = capabilities.allows_history(channel);
        let _ = capabilities.allows_annotation_subscribe(channel);
        let _ = capabilities.allows_annotation_publish(channel);
        let _ = capabilities.allows_annotation_delete_own(channel);
        let _ = capabilities.allows_annotation_delete_any(channel);
        let _ = capabilities.allows_push_admin(channel);
        let _ = capabilities.allows_push_subscribe(channel);
        for kind in [
            MutationKind::Update,
            MutationKind::Delete,
            MutationKind::Append,
        ] {
            let _ = capabilities.allows_message_mutation_own(kind, channel);
            let _ = capabilities.allows_message_mutation_any(kind, channel);
        }
    }
});
