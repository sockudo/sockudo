#![no_main]

use libfuzzer_sys::fuzz_target;
use sockudo_push::{
    ChannelPushRule, MAX_PUSH_BODY_BYTES, MAX_PUSH_ICON_BYTES, MAX_PUSH_TARGETS,
    MAX_PUSH_TITLE_BYTES, ProviderOverridePayload, PublishIntent, PushCursor, PushPayload,
    PushProviderKind, render_provider_payload,
};
use sonic_rs::json;

const MAX_INPUT_BYTES: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }

    if let Ok(payload) = serde_json::from_slice::<PushPayload>(data) {
        let validation = payload.validate();
        if payload
            .title
            .as_ref()
            .is_some_and(|value| value.len() > MAX_PUSH_TITLE_BYTES)
            || payload
                .body
                .as_ref()
                .is_some_and(|value| value.len() > MAX_PUSH_BODY_BYTES)
            || payload
                .icon
                .as_ref()
                .is_some_and(|value| value.len() > MAX_PUSH_ICON_BYTES)
        {
            assert!(
                validation.is_err(),
                "oversized push payload field was accepted"
            );
        }
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
            template_data: json!({}),
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
        for override_payload in overrides {
            let _ = override_payload.validate();
        }
    }

    if let Ok(rule) = serde_json::from_slice::<ChannelPushRule>(data) {
        let message = json!({
            "title": "hello",
            "body": "world",
            "extra": "value"
        });
        let _ = rule.validate();
        let _ = rule.map_payload(&message);
    }

    if let Ok(intent) = serde_json::from_slice::<PublishIntent>(data) {
        let validation = intent.validate();
        if intent.targets.len() > MAX_PUSH_TARGETS {
            assert!(
                validation.is_err(),
                "push intent with too many targets was accepted"
            );
        }
        if validation.is_ok() {
            let key = intent.idempotency_key();
            let encoded = sonic_rs::to_vec(&intent).expect("push intent must serialize");
            let decoded = sonic_rs::from_slice::<PublishIntent>(&encoded)
                .expect("serialized push intent must deserialize");
            assert_eq!(decoded, intent);
            assert_eq!(decoded.idempotency_key(), key);
        }
    }

    if let Ok(cursor) = serde_json::from_slice::<PushCursor>(data)
        && let Ok(encoded) = cursor.encode()
    {
        let decoded = PushCursor::decode(&encoded, &cursor.app_id)
            .expect("encoded push cursor must decode for its app");
        assert_eq!(decoded, cursor);
    }

    if let Ok(input) = std::str::from_utf8(data) {
        let _ = PushCursor::decode(input, "app");
    }
});
