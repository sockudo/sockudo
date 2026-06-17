use criterion::{Criterion, criterion_group, criterion_main};
use sockudo_ai_transport::{RollupConfig, RollupEngine};
use sockudo_protocol::messages::{MessageData, PusherMessage};
use sockudo_protocol::versioned_messages::{
    MessageAction, MessageVersionMetadata, apply_runtime_metadata,
};

fn append(serial: &str, data: String, now: i64) -> PusherMessage {
    let mut message = PusherMessage {
        event: Some(MessageAction::Append.v2_event_name()),
        channel: Some("ai:bench".to_string()),
        data: Some(MessageData::String(data)),
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
    };
    apply_runtime_metadata(
        &mut message,
        MessageAction::Append,
        serial,
        &MessageVersionMetadata {
            serial: format!("v{now}"),
            client_id: None,
            timestamp_ms: now,
            description: None,
            metadata: None,
        },
        Some(now as u64),
    );
    message
}

fn bench_rollup_engine(c: &mut Criterion) {
    c.bench_function("rollup_decision_64b_append", |b| {
        let engine = RollupEngine::new(RollupConfig::default());
        let first = append("bench", "x".repeat(64), 0);
        let _ = engine.ingest("app", "ai:bench", first, 0);
        let messages = (1_i64..=4096)
            .map(|index| append("bench", "x".repeat(64), index))
            .collect::<Vec<_>>();
        let mut serial = 1_i64;
        b.iter(|| {
            let message = messages[(serial as usize) & 4095].clone();
            serial = serial.wrapping_add(1);
            let _ = engine.ingest("app", "ai:bench", message, 1);
        });
    });

    c.bench_function("rollup_flush_2000_append_warm", |b| {
        b.iter_batched(
            || {
                let engine = RollupEngine::new(RollupConfig::default());
                let _ = engine.ingest("app", "ai:bench", append("bench", "x".repeat(64), 0), 0);
                for index in 1..=2000 {
                    let _ =
                        engine.ingest("app", "ai:bench", append("bench", "x".repeat(64), index), 1);
                }
                engine
            },
            |engine| {
                let _ = engine.flush_due(40);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    c.bench_function("rollup_build_2000_append_stream", |b| {
        b.iter(|| {
            let engine = RollupEngine::new(RollupConfig::default());
            let _ = engine.ingest("app", "ai:bench", append("bench", "x".repeat(64), 0), 0);
            for index in 1..=2000 {
                let _ = engine.ingest("app", "ai:bench", append("bench", "x".repeat(64), index), 1);
            }
            let _ = engine.flush_due(40);
        });
    });
}

criterion_group!(benches, bench_rollup_engine);
criterion_main!(benches);
