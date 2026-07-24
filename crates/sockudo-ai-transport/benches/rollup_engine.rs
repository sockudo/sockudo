use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sockudo_ai_transport::{RollupConfig, RollupEngine};
use sockudo_protocol::messages::{AiExtras, MessageData, MessageExtras, PusherMessage};
use sockudo_protocol::versioned_messages::{
    MessageAction, MessageVersionMetadata, apply_runtime_metadata,
};
use std::collections::HashMap;
use std::hint::black_box;

const CARDINALITIES: [usize; 2] = [2_000, 50_000];

fn append(serial: &str, data: &str, version: i64, terminal: bool) -> PusherMessage {
    let status = if terminal { "complete" } else { "streaming" };
    let mut message = PusherMessage {
        event: Some(MessageAction::Append.v2_event_name()),
        channel: Some("ai:bench".to_string()),
        data: Some(MessageData::String(data.to_string())),
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
                transport: Some(HashMap::from([("status".to_string(), status.to_string())])),
                codec: None,
            }),
            ..MessageExtras::default()
        }),
        delta_sequence: None,
        delta_conflation_key: None,
    };
    apply_runtime_metadata(
        &mut message,
        MessageAction::Append,
        serial,
        &MessageVersionMetadata {
            serial: format!("v{version}"),
            client_id: None,
            timestamp_ms: version,
            description: None,
            metadata: None,
        },
        Some(version.max(0) as u64),
    );
    message
}

fn engine() -> RollupEngine {
    RollupEngine::new(RollupConfig {
        max_active_streams_per_channel: 50_000,
        ..RollupConfig::default()
    })
}

fn seed(engine: &RollupEngine, streams: usize, due_streams: usize) {
    for index in 0..streams {
        let channel = format!("ai:bench:{index}");
        let serial = format!("msg:{index}");
        let _ = engine.ingest(
            "app",
            &channel,
            append(&serial, "x", (index * 2) as i64, false),
            0,
        );
        if index < due_streams {
            let _ = engine.ingest(
                "app",
                &channel,
                append(&serial, "xx", (index * 2 + 1) as i64, false),
                1,
            );
        }
    }
}

fn terminal_fixture(streams: usize) -> (RollupEngine, Vec<(String, PusherMessage)>) {
    let engine = engine();
    seed(&engine, streams, streams);
    let terminals = (0..streams)
        .map(|index| {
            let channel = format!("ai:bench:{index}");
            let serial = format!("msg:{index}");
            (
                channel,
                append(&serial, "done", (streams * 2 + index) as i64, true),
            )
        })
        .collect();
    (engine, terminals)
}

fn bench_rollup_engine(c: &mut Criterion) {
    let mut group = c.benchmark_group("rollup_independent_streams");
    group.sample_size(10);

    for streams in CARDINALITIES {
        group.throughput(Throughput::Elements(streams as u64));
        group.bench_with_input(
            BenchmarkId::new("empty_tick", streams),
            &streams,
            |b, &streams| {
                b.iter_batched_ref(
                    || {
                        let engine = engine();
                        seed(&engine, streams, 0);
                        engine
                    },
                    |engine| black_box(engine.poll_due(1, streams)),
                    BatchSize::LargeInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("one_percent_due", streams),
            &streams,
            |b, &streams| {
                b.iter_batched_ref(
                    || {
                        let engine = engine();
                        seed(&engine, streams, streams.div_ceil(100));
                        engine
                    },
                    |engine| black_box(engine.poll_due(40, streams)),
                    BatchSize::LargeInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("all_due", streams),
            &streams,
            |b, &streams| {
                b.iter_batched_ref(
                    || {
                        let engine = engine();
                        seed(&engine, streams, streams);
                        engine
                    },
                    |engine| black_box(engine.poll_due(40, streams)),
                    BatchSize::LargeInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("terminal_storm", streams),
            &streams,
            |b, &streams| {
                b.iter_batched_ref(
                    || terminal_fixture(streams),
                    |(engine, terminals)| {
                        for (channel, terminal) in std::mem::take(terminals) {
                            black_box(engine.ingest("app", &channel, terminal, 2));
                        }
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_rollup_engine);
criterion_main!(benches);
