use async_trait::async_trait;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sockudo_ai_transport::{RollupConfig, RollupEngine};
use sockudo_core::error::Result;
use sockudo_core::version_store::{
    LeasedVersionStore, MemoryVersionStore, StoredVersionRecord, VersionReplayRequest,
    VersionStore, VersionStorePage, VersionStoreReadRequest, VersionStreamState,
    VersionWriteReservation, VersionWriteReservationBlock,
};
use sockudo_core::versioned_message_auth::MutationKind;
use sockudo_core::versioned_messages::{
    MessageAppend, MessageSerial, VersionMetadata, VersionSerial, VersionedMessage,
};
use sockudo_core::websocket::ConnectionCapabilities;
use sockudo_protocol::messages::{
    AI_EVENT_INPUT, AiExtras, MessageData, MessageExtras, PusherMessage, is_ai_event,
};
use sockudo_protocol::versioned_messages::{
    MessageAction, MessageVersionMetadata, apply_runtime_metadata,
};
use sonic_rs::json;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const APP_ID: &str = "app";
const CHANNEL: &str = "ai:bench";
const MESSAGE_SERIAL: &str = "msg:bench";

struct CountingBlockVersionStore {
    inner: MemoryVersionStore,
    block_calls: AtomicU64,
}

impl CountingBlockVersionStore {
    fn new() -> Self {
        Self {
            inner: MemoryVersionStore::new(),
            block_calls: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl VersionStore for CountingBlockVersionStore {
    async fn reserve_delivery_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<VersionWriteReservation> {
        self.inner.reserve_delivery_position(app_id, channel).await
    }

    async fn reserve_delivery_positions(
        &self,
        app_id: &str,
        channel: &str,
        block_size: u64,
    ) -> Result<VersionWriteReservationBlock> {
        self.block_calls.fetch_add(1, Ordering::Relaxed);
        self.inner
            .reserve_delivery_positions(app_id, channel, block_size)
            .await
    }

    async fn append_version(&self, record: StoredVersionRecord) -> Result<()> {
        self.inner.append_version(record).await
    }

    async fn get_latest(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        self.inner.get_latest(app_id, channel, message_serial).await
    }

    async fn get_versions(&self, request: VersionStoreReadRequest) -> Result<VersionStorePage> {
        self.inner.get_versions(request).await
    }

    async fn replay_after(
        &self,
        request: VersionReplayRequest,
    ) -> Result<Vec<StoredVersionRecord>> {
        self.inner.replay_after(request).await
    }

    async fn latest_by_history(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Vec<StoredVersionRecord>> {
        self.inner.latest_by_history(app_id, channel).await
    }

    async fn stream_state(&self, app_id: &str, channel: &str) -> Result<VersionStreamState> {
        self.inner.stream_state(app_id, channel).await
    }
}

fn sample_ai_extras() -> MessageExtras {
    MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([
                ("run-id".to_string(), "turn-1".to_string()),
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

fn version(serial: u64) -> VersionMetadata {
    VersionMetadata {
        serial: VersionSerial::new(format!("ver:{serial:020}")).unwrap(),
        client_id: Some("agent-1".to_string()),
        timestamp_ms: serial as i64,
        description: None,
        metadata: None,
    }
}

fn stored_create(
    message_serial: &str,
    history_serial: u64,
    delivery_serial: u64,
) -> StoredVersionRecord {
    StoredVersionRecord {
        app_id: APP_ID.to_string(),
        channel: CHANNEL.to_string(),
        original_client_id: Some("agent-1".to_string()),
        envelope: None,
        message: VersionedMessage::new_create(
            MessageSerial::new(message_serial).unwrap(),
            version(delivery_serial),
            history_serial,
            delivery_serial,
            Some("ai-output".to_string()),
            Some(MessageData::String(String::new())),
            Some(sample_ai_extras()),
        ),
    }
}

fn append_wire_message(serial: &str, data: String, now: i64, terminal: bool) -> PusherMessage {
    let mut message = PusherMessage {
        event: Some(MessageAction::Append.v2_event_name()),
        channel: Some(CHANNEL.to_string()),
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
        extras: Some(sample_ai_extras()),
        delta_sequence: None,
        delta_conflation_key: None,
    };
    if terminal
        && let Some(extras) = message.extras.as_mut()
        && let Some(ai) = extras.ai.as_mut()
        && let Some(transport) = ai.transport.as_mut()
    {
        transport.insert("status".to_string(), "complete".to_string());
    }
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

fn bench_header_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("ai_hot_paths_headers");
    let extras = sample_ai_extras();
    group.bench_function("extras_header_validate_budget_mean_10us", |b| {
        b.iter(|| black_box(&extras).validate_ai_headers().unwrap())
    });

    let event = PusherMessage::channel_event(AI_EVENT_INPUT, CHANNEL, json!({"x":1}));
    group.bench_function("ai_event_name_check_budget_mean_500ns", |b| {
        b.iter(|| black_box(is_ai_event(black_box(event.event.as_deref().unwrap()))))
    });
    group.finish();
}

fn bench_serial_reservation(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("ai_hot_paths_serial_reservation");

    group.bench_function("memory_reserve_local_budget_mean_25us", |b| {
        let store = MemoryVersionStore::new();
        b.iter(|| {
            runtime
                .block_on(store.reserve_delivery_position(APP_ID, CHANNEL))
                .unwrap();
        });
    });

    group.bench_function("leased_clustered_reserve_budget_mean_25us", |b| {
        let inner = Arc::new(CountingBlockVersionStore::new());
        let store = LeasedVersionStore::new(inner, 128);
        b.iter(|| {
            runtime
                .block_on(store.reserve_delivery_position(APP_ID, CHANNEL))
                .unwrap();
        });
    });
    group.finish();
}

fn bench_versioned_store(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("ai_hot_paths_versioned_store");

    group.bench_function("versioned_append_memory_budget_mean_75us", |b| {
        b.iter_batched(
            || stored_create(MESSAGE_SERIAL, 1, 1),
            |current| {
                black_box(
                    current
                        .message
                        .apply_append(
                            version(2),
                            2,
                            MessageAppend {
                                data_fragment: "x".repeat(64),
                                extras: Some(sample_ai_extras()),
                            },
                        )
                        .unwrap(),
                );
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("memory_latest_by_history_1000_budget_mean_2ms", |b| {
        let store = runtime.block_on(async {
            let store = MemoryVersionStore::new();
            for index in 0..1000 {
                store
                    .append_version(stored_create(
                        &format!("msg:{index:04}"),
                        index + 1,
                        index + 1,
                    ))
                    .await
                    .unwrap();
            }
            store
        });
        b.iter(|| {
            let rows = runtime
                .block_on(store.latest_by_history(APP_ID, CHANNEL))
                .unwrap();
            black_box(rows);
        });
    });
    group.finish();
}

fn bench_rollup(c: &mut Criterion) {
    let mut group = c.benchmark_group("ai_hot_paths_rollup");
    group.throughput(Throughput::Elements(1));
    group.bench_function("rollup_decision_append_budget_mean_25us", |b| {
        let engine = RollupEngine::new(RollupConfig::default());
        let _ = engine.ingest(
            APP_ID,
            CHANNEL,
            append_wire_message(MESSAGE_SERIAL, "x".repeat(64), 0, false),
            0,
        );
        let mut serial = 1_i64;
        b.iter(|| {
            let message = append_wire_message(MESSAGE_SERIAL, "x".repeat(64), serial, false);
            serial = serial.wrapping_add(1);
            black_box(engine.ingest(APP_ID, CHANNEL, message, 1));
        });
    });

    group.bench_function("rollup_flush_2000_budget_mean_10ms", |b| {
        b.iter_batched(
            || {
                let engine = RollupEngine::new(RollupConfig::default());
                let _ = engine.ingest(
                    APP_ID,
                    CHANNEL,
                    append_wire_message(MESSAGE_SERIAL, "x".repeat(64), 0, false),
                    0,
                );
                for index in 1..=2000 {
                    let _ = engine.ingest(
                        APP_ID,
                        CHANNEL,
                        append_wire_message(MESSAGE_SERIAL, "x".repeat(64), index, false),
                        1,
                    );
                }
                engine
            },
            |engine| black_box(engine.flush_due(40)),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_capabilities(c: &mut Criterion) {
    let capabilities = ConnectionCapabilities {
        publish: Some(vec!["ai:*".to_string(), "public".to_string()]),
        subscribe: Some(vec!["*".to_string()]),
        history: Some(vec!["ai:*".to_string()]),
        message_append_any: Some(vec!["ai:*".to_string()]),
        ..Default::default()
    };

    let mut group = c.benchmark_group("ai_hot_paths_capability");
    group.bench_function("capability_publish_match_budget_mean_1us", |b| {
        b.iter(|| black_box(&capabilities).allows_publish(black_box(CHANNEL)))
    });
    group.bench_function("capability_append_any_match_budget_mean_1us", |b| {
        b.iter(|| {
            black_box(&capabilities)
                .allows_message_mutation_any(MutationKind::Append, black_box(CHANNEL))
        })
    });
    group.finish();
}

fn bench_e2e_intra_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("ai_hot_paths_e2e_intra_node");
    let subscriber_counts = [1_u64, 1_000];
    for subscribers in subscriber_counts {
        group.throughput(Throughput::Elements(subscribers));
        group.bench_with_input(
            BenchmarkId::new(
                "discrete_ai_input_publish_to_fanout_budget_mean_10ms",
                subscribers,
            ),
            &subscribers,
            |b, subscriber_count| {
                let mut message =
                    PusherMessage::channel_event(AI_EVENT_INPUT, CHANNEL, json!({"text":"hello"}));
                message.extras = Some(sample_ai_extras());
                let wire = sonic_rs::to_string(&message).unwrap();
                b.iter(|| {
                    for _ in 0..*subscriber_count {
                        black_box(wire.as_bytes());
                    }
                });
            },
        );
    }

    group.bench_function("streaming_append_rollup_on_1k_budget_mean_10ms", |b| {
        b.iter(|| {
            let engine = RollupEngine::new(RollupConfig::default());
            let _ = engine.ingest(
                APP_ID,
                CHANNEL,
                append_wire_message(MESSAGE_SERIAL, "x".repeat(64), 0, false),
                0,
            );
            for index in 1..=100 {
                let _ = engine.ingest(
                    APP_ID,
                    CHANNEL,
                    append_wire_message(MESSAGE_SERIAL, "x".repeat(64), index, false),
                    1,
                );
            }
            let mut deliveries = engine.flush_due(40);
            deliveries.push(
                engine
                    .ingest(
                        APP_ID,
                        CHANNEL,
                        append_wire_message(MESSAGE_SERIAL, "x".repeat(64), 101, true),
                        41,
                    )
                    .pop()
                    .unwrap(),
            );
            for delivery in deliveries {
                let wire = sonic_rs::to_string(&delivery.message).unwrap();
                for _ in 0..1000 {
                    black_box(wire.as_bytes());
                }
            }
        });
    });

    group.bench_function("streaming_append_rollup_off_1k_budget_mean_10ms", |b| {
        b.iter(|| {
            let engine = RollupEngine::new(RollupConfig {
                window_ms: 0,
                ..RollupConfig::default()
            });
            for index in 0..=100 {
                let deliveries = engine.ingest(
                    APP_ID,
                    CHANNEL,
                    append_wire_message(MESSAGE_SERIAL, "x".repeat(64), index, index == 100),
                    index as u64,
                );
                for delivery in deliveries {
                    let wire = sonic_rs::to_string(&delivery.message).unwrap();
                    for _ in 0..1000 {
                        black_box(wire.as_bytes());
                    }
                }
            }
        });
    });
    group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_header_validation(c);
    bench_serial_reservation(c);
    bench_versioned_store(c);
    bench_rollup(c);
    bench_capabilities(c);
    bench_e2e_intra_node(c);
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20);
    targets = criterion_benchmark
}
criterion_main!(benches);
