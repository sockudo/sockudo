use criterion::{Criterion, criterion_group, criterion_main};
use sockudo_protocol::messages::{
    AI_EVENT_INPUT, AiExtras, MessageExtras, PusherMessage, is_ai_event,
};
use sonic_rs::json;
use std::collections::HashMap;
use std::hint::black_box;

fn sample_ai_extras() -> MessageExtras {
    MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([
                ("turn-id".to_string(), "turn-1".to_string()),
                ("invocation-id".to_string(), "invoke-1".to_string()),
                ("status".to_string(), "streaming".to_string()),
                ("role".to_string(), "assistant".to_string()),
                ("stream".to_string(), "true".to_string()),
            ])),
            codec: Some(HashMap::from([(
                "content-type".to_string(),
                "application/json".to_string(),
            )])),
        }),
        ..Default::default()
    }
}

fn bench_ai_headers(c: &mut Criterion) {
    let extras = sample_ai_extras();
    c.bench_function("ai_transport_header_validate_p50_budget_lt_2us", |b| {
        b.iter(|| black_box(&extras).validate_ai_headers().unwrap())
    });

    let message = PusherMessage::channel_event(AI_EVENT_INPUT, "private-ai", json!({"x":1}));
    c.bench_function("ai_event_name_check", |b| {
        b.iter(|| black_box(is_ai_event(black_box(message.event.as_deref().unwrap()))))
    });

    c.bench_function("non_ai_event_name_check", |b| {
        b.iter(|| black_box(is_ai_event(black_box("client-typing"))))
    });
}

criterion_group!(benches, bench_ai_headers);
criterion_main!(benches);
