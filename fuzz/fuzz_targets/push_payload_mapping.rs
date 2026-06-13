#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_push::{
    ChannelPushRule, ProviderOverridePayload, PushPayload, PushProviderKind,
    render_provider_payload,
};

const MAX_INPUT_BYTES: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    if let Ok(payload) = serde_json::from_slice::<PushPayload>(data) {
        let _ = payload.validate();
        for provider in [
            PushProviderKind::Fcm,
            PushProviderKind::Apns,
            PushProviderKind::WebPush,
            PushProviderKind::Hms,
            PushProviderKind::Wns,
        ] {
            let _ = render_provider_payload(provider, &payload, &[]);
        }
    }

    if let Ok(overrides) = serde_json::from_slice::<Vec<ProviderOverridePayload>>(data) {
        let payload = PushPayload {
            template_id: None,
            template_data: serde_json::json!({}),
            title: None,
            body: None,
            icon: None,
            sound: None,
            collapse_key: None,
        };
        for provider in [
            PushProviderKind::Fcm,
            PushProviderKind::Apns,
            PushProviderKind::WebPush,
            PushProviderKind::Hms,
            PushProviderKind::Wns,
        ] {
            let _ = render_provider_payload(provider, &payload, &overrides);
        }
    }

    if let Ok(rule) = serde_json::from_slice::<ChannelPushRule>(data) {
        let message = serde_json::json!({
            "title": "hello",
            "body": "world",
            "extra": "value"
        });
        let _ = rule.validate();
        let _ = rule.map_payload(&message);
    }
});
