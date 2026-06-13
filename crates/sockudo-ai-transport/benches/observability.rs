use criterion::{Criterion, criterion_group, criterion_main};
use sockudo_ai_transport::observability::AiObservabilityTracker;
use sockudo_protocol::messages::{AiExtras, MessageData, MessageExtras, PusherMessage};
use std::collections::HashMap;

fn message_with_headers(headers: &[(&str, &str)]) -> PusherMessage {
    let mut transport = HashMap::new();
    for (key, value) in headers {
        transport.insert((*key).to_string(), (*value).to_string());
    }
    PusherMessage {
        event: Some("sockudo:message.append".to_string()),
        channel: Some("ai:bench".to_string()),
        data: Some(MessageData::String("x".repeat(64))),
        name: None,
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: None,
        stream_id: None,
        serial: None,
        idempotency_key: None,
        extras: Some(MessageExtras {
            ai: Some(AiExtras {
                transport: Some(transport),
                codec: None,
            }),
            ..MessageExtras::default()
        }),
        delta_sequence: None,
        delta_conflation_key: None,
    }
}

fn plain_message() -> PusherMessage {
    PusherMessage {
        event: Some("event".to_string()),
        channel: Some("chat".to_string()),
        data: Some(MessageData::String("x".repeat(64))),
        name: None,
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: None,
        stream_id: None,
        serial: None,
        idempotency_key: None,
        extras: None,
        delta_sequence: None,
        delta_conflation_key: None,
    }
}

fn bench_observability(c: &mut Criterion) {
    c.bench_function("observability_no_ai_headers_budget_50ns", |b| {
        let tracker = AiObservabilityTracker::default();
        let message = plain_message();
        b.iter(|| {
            let _ = tracker.observe("app", "chat", &message, 1);
        });
    });

    c.bench_function("observability_stream_update_budget_1000ns", |b| {
        let tracker = AiObservabilityTracker::default();
        let message = message_with_headers(&[
            ("codec-message-id", "msg-1"),
            ("status", "streaming"),
            ("stream", "true"),
        ]);
        b.iter(|| {
            let _ = tracker.observe("app", "ai:bench", &message, 1);
        });
    });
}

criterion_group!(benches, bench_observability);
criterion_main!(benches);
