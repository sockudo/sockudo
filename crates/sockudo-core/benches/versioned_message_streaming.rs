use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use sockudo_core::error::Result;
use sockudo_core::version_store::{
    LeasedVersionStore, MemoryVersionStore, StoredVersionRecord, VersionReplayRequest,
    VersionStore, VersionStorePage, VersionStoreReadRequest, VersionStreamState,
    VersionWriteReservation, VersionWriteReservationBlock,
};
use sockudo_core::versioned_messages::{
    MessageAppend, MessageSerial, VersionMetadata, VersionSerial, VersionedMessage,
};
use sockudo_protocol::messages::MessageData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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

fn version(serial: u64) -> VersionMetadata {
    VersionMetadata {
        serial: VersionSerial::new(format!("ver:{serial:020}")).unwrap(),
        client_id: Some("agent-1".to_string()),
        timestamp_ms: serial as i64,
        description: None,
        metadata: None,
    }
}

fn record_with_data(data: String) -> StoredVersionRecord {
    StoredVersionRecord {
        app_id: "app".to_string(),
        channel: "ai-room".to_string(),
        original_client_id: Some("agent-1".to_string()),
        message: VersionedMessage::new_create(
            MessageSerial::new("msg:1").unwrap(),
            version(1),
            1,
            1,
            Some("ai-output".to_string()),
            Some(MessageData::String(data)),
            None,
        ),
    }
}

fn bench_append_64b_to_100k(c: &mut Criterion) {
    let current = record_with_data("x".repeat(100 * 1024));
    let fragment = "y".repeat(64);

    c.bench_function("versioned_append_64b_to_100k_budget_10us_p50", |b| {
        b.iter(|| {
            let _ = current
                .message
                .apply_append(
                    version(2),
                    2,
                    MessageAppend {
                        data_fragment: fragment.clone(),
                        extras: None,
                    },
                )
                .unwrap();
        });
    });
}

fn bench_warm_get_latest_2000_appends(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let store = runtime.block_on(async {
        let store = MemoryVersionStore::new();
        let mut current = record_with_data(String::new());
        store.append_version(current.clone()).await.unwrap();
        for index in 0..2000 {
            let next = StoredVersionRecord {
                message: current
                    .message
                    .apply_append(
                        version(index + 2),
                        index + 2,
                        MessageAppend {
                            data_fragment: "x".repeat(64),
                            extras: None,
                        },
                    )
                    .unwrap(),
                ..current.clone()
            };
            store.append_version(next.clone()).await.unwrap();
            current = next;
        }
        store
    });
    let serial = MessageSerial::new("msg:1").unwrap();

    c.bench_function("memory_get_latest_2000_appends_warm_budget_50us_p50", |b| {
        b.iter(|| {
            runtime
                .block_on(store.get_latest("app", "ai-room", &serial))
                .unwrap()
                .unwrap();
        });
    });
}

fn bench_leased_delivery_reservations(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    c.bench_function(
        "leased_delivery_reserve_200_appends_budget_lt_0_1_store_round_trips_per_append",
        |b| {
            b.iter(|| {
                let inner = Arc::new(CountingBlockVersionStore::new());
                let store = LeasedVersionStore::new(inner.clone(), 128);
                runtime.block_on(async {
                    for _ in 0..200 {
                        store
                            .reserve_delivery_position("app", "ai-room")
                            .await
                            .unwrap();
                    }
                });
                let calls = inner.block_calls.load(Ordering::Relaxed);
                assert!(
                    calls <= 20,
                    "expected <= 0.1 store round-trips per append, got {calls} calls for 200 appends"
                );
            });
        },
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_append_64b_to_100k(c);
    bench_warm_get_latest_2000_appends(c);
    bench_leased_delivery_reservations(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
