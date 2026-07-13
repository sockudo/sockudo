use super::*;
use crate::error::Result;
use crate::versioned_messages::{
    FieldPatch, MessageAction, MessageAppend, MessageFieldDelta, MessageSerial, VersionMetadata,
    VersionSerial, VersionedMessage,
};
use async_trait::async_trait;
use sockudo_protocol::messages::{MessageData, MessageExtras};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

fn version(serial: &str, timestamp_ms: i64) -> VersionMetadata {
    VersionMetadata {
        serial: VersionSerial::new(serial).unwrap(),
        client_id: Some("user-1".to_string()),
        timestamp_ms,
        description: None,
        metadata: None,
    }
}

fn base_record(
    message_serial: &str,
    history_serial: u64,
    delivery_serial: u64,
) -> StoredVersionRecord {
    StoredVersionRecord {
        app_id: "app".to_string(),
        channel: "chat".to_string(),
        original_client_id: Some("user-1".to_string()),
        envelope: None,
        message: VersionedMessage::new_create(
            MessageSerial::new(message_serial).unwrap(),
            version("ver:1", 1),
            history_serial,
            delivery_serial,
            Some("chat.message".to_string()),
            Some(MessageData::String("hello".to_string())),
            Some(MessageExtras {
                headers: None,
                ephemeral: Some(false),
                idempotency_key: None,
                push: None,
                echo: None,
                ai: None,
                opaque: Default::default(),
            }),
        ),
    }
}

struct CountingBlockVersionStore {
    inner: MemoryVersionStore,
    single_calls: AtomicU64,
    block_calls: AtomicU64,
    latest_row_calls: AtomicU64,
    latest_history_calls: AtomicU64,
}

impl CountingBlockVersionStore {
    fn new() -> Self {
        Self {
            inner: MemoryVersionStore::new(),
            single_calls: AtomicU64::new(0),
            block_calls: AtomicU64::new(0),
            latest_row_calls: AtomicU64::new(0),
            latest_history_calls: AtomicU64::new(0),
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
        self.single_calls.fetch_add(1, Ordering::Relaxed);
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
        self.latest_row_calls.fetch_add(1, Ordering::Relaxed);
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
        self.latest_history_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.latest_by_history(app_id, channel).await
    }

    async fn stream_state(&self, app_id: &str, channel: &str) -> Result<VersionStreamState> {
        self.inner.stream_state(app_id, channel).await
    }
}

#[tokio::test]
async fn default_batch_projection_uses_one_history_read_and_zero_row_reads() {
    let store = CountingBlockVersionStore::new();
    store
        .append_version(base_record("msg:1", 10, 1))
        .await
        .unwrap();
    store
        .append_version(base_record("msg:2", 20, 2))
        .await
        .unwrap();

    let requested = vec![
        MessageSerial::new("msg:2").unwrap(),
        MessageSerial::new("missing").unwrap(),
        MessageSerial::new("msg:1").unwrap(),
    ];
    let projected = store
        .get_latest_batch("app", "chat", &requested)
        .await
        .unwrap();

    assert_eq!(projected.len(), 2);
    assert_eq!(projected[&requested[0]].history_serial(), 20);
    assert_eq!(projected[&requested[2]].history_serial(), 10);
    assert_eq!(store.latest_history_calls.load(Ordering::Relaxed), 1);
    assert_eq!(store.latest_row_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn memory_batch_projection_returns_latest_records_for_requested_serials() {
    let store = MemoryVersionStore::new();
    let create = base_record("msg:1", 10, 1);
    store.append_version(create.clone()).await.unwrap();
    store
        .append_version(StoredVersionRecord {
            message: create
                .message
                .apply_mutation(
                    MessageAction::Update,
                    version("ver:2", 2),
                    2,
                    MessageFieldDelta::default(),
                )
                .unwrap(),
            ..create
        })
        .await
        .unwrap();

    let serial = MessageSerial::new("msg:1").unwrap();
    let missing = MessageSerial::new("msg:missing").unwrap();
    let projected = store
        .get_latest_batch("app", "chat", &[serial.clone(), missing.clone()])
        .await
        .unwrap();

    assert_eq!(projected[&serial].version_serial().as_str(), "ver:2");
    assert!(!projected.contains_key(&missing));
}

#[tokio::test]
async fn memory_store_returns_latest_visible_by_version_serial() {
    let store = MemoryVersionStore::new();
    let create = base_record("msg:1", 10, 1);
    store.append_version(create.clone()).await.unwrap();

    let update = StoredVersionRecord {
        message: create
            .message
            .apply_mutation(
                MessageAction::Update,
                version("ver:9", 2),
                2,
                MessageFieldDelta {
                    data: FieldPatch::Replace(MessageData::String("patched".to_string())),
                    ..Default::default()
                },
            )
            .unwrap(),
        ..create.clone()
    };
    store.append_version(update.clone()).await.unwrap();

    let latest = store
        .get_latest("app", "chat", &MessageSerial::new("msg:1").unwrap())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(latest.version_serial().as_str(), "ver:9");
    assert_eq!(
        latest.message.data.unwrap().into_string().as_deref(),
        Some("patched")
    );
}

#[tokio::test]
async fn memory_store_pages_version_history() {
    let store = MemoryVersionStore::new();
    let create = base_record("msg:1", 10, 1);
    store.append_version(create.clone()).await.unwrap();

    let update_1 = StoredVersionRecord {
        message: create
            .message
            .apply_mutation(
                MessageAction::Update,
                version("ver:2", 2),
                2,
                MessageFieldDelta::default(),
            )
            .unwrap(),
        ..create.clone()
    };
    let update_2 = StoredVersionRecord {
        message: update_1
            .message
            .apply_mutation(
                MessageAction::Delete,
                version("ver:3", 3),
                3,
                MessageFieldDelta::default(),
            )
            .unwrap(),
        ..create.clone()
    };

    store.append_version(update_1).await.unwrap();
    store.append_version(update_2).await.unwrap();

    let page = store
        .get_versions(VersionStoreReadRequest {
            app_id: "app".to_string(),
            channel: "chat".to_string(),
            message_serial: MessageSerial::new("msg:1").unwrap(),
            direction: VersionStoreDirection::NewestFirst,
            limit: 2,
            cursor: None,
        })
        .await
        .unwrap();

    assert_eq!(page.items.len(), 2);
    assert!(page.has_more);
    assert_eq!(page.items[0].version_serial().as_str(), "ver:3");
    assert_eq!(page.items[1].version_serial().as_str(), "ver:2");
    assert!(page.next_cursor.is_some());
}

#[tokio::test]
async fn memory_store_projects_latest_by_history_order() {
    let store = MemoryVersionStore::new();
    let first = base_record("msg:1", 10, 1);
    let second = base_record("msg:2", 20, 2);
    store.append_version(second.clone()).await.unwrap();
    store.append_version(first.clone()).await.unwrap();

    let latest = store.latest_by_history("app", "chat").await.unwrap();
    assert_eq!(latest.len(), 2);
    assert_eq!(latest[0].message_serial().as_str(), "msg:1");
    assert_eq!(latest[1].message_serial().as_str(), "msg:2");
}

#[tokio::test]
async fn memory_store_replays_in_delivery_order() {
    let store = MemoryVersionStore::new();
    let first = base_record("msg:1", 10, 1);
    let second = base_record("msg:2", 20, 2);
    store.append_version(first).await.unwrap();
    store.append_version(second).await.unwrap();

    let replay = store
        .replay_after(VersionReplayRequest {
            app_id: "app".to_string(),
            channel: "chat".to_string(),
            after_delivery_serial: 0,
            limit: 10,
        })
        .await
        .unwrap();

    assert_eq!(replay.len(), 2);
    assert_eq!(replay[0].delivery_serial(), 1);
    assert_eq!(replay[1].delivery_serial(), 2);
}

#[tokio::test]
async fn memory_store_reserves_delivery_positions_with_stable_stream_id() {
    let store = MemoryVersionStore::new();
    let first = store
        .reserve_delivery_position("app", "chat")
        .await
        .unwrap();
    let second = store
        .reserve_delivery_position("app", "chat")
        .await
        .unwrap();

    assert_eq!(first.stream_id, second.stream_id);
    assert_eq!(first.delivery_serial, 1);
    assert_eq!(second.delivery_serial, 2);
}

#[tokio::test]
async fn leased_store_reserves_gapless_serials_with_amortized_backend_calls() {
    let inner = Arc::new(CountingBlockVersionStore::new());
    let store = Arc::new(LeasedVersionStore::new(inner.clone(), 128));
    let handles = (0..1_000)
        .map(|_| {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .reserve_delivery_position("app", "chat")
                    .await
                    .unwrap()
                    .delivery_serial
            })
        })
        .collect::<Vec<_>>();

    let mut serials = Vec::with_capacity(handles.len());
    for handle in handles {
        serials.push(handle.await.unwrap());
    }
    serials.sort_unstable();

    assert_eq!(serials.len(), 1_000);
    for (index, serial) in serials.into_iter().enumerate() {
        assert_eq!(serial, index as u64 + 1);
    }
    assert_eq!(inner.single_calls.load(Ordering::Relaxed), 0);
    assert_eq!(inner.block_calls.load(Ordering::Relaxed), 8);
}

#[tokio::test]
async fn leased_store_discards_stale_local_range_when_reserving_after_latest() {
    let inner = Arc::new(CountingBlockVersionStore::new());
    let node_a = LeasedVersionStore::new(inner.clone(), 128);
    let node_b = LeasedVersionStore::new(inner.clone(), 128);

    let first = node_a
        .reserve_delivery_position("app", "chat")
        .await
        .unwrap();
    let advanced = node_b
        .reserve_delivery_position_after("app", "chat", first.delivery_serial)
        .await
        .unwrap();
    let after_advanced = node_a
        .reserve_delivery_position_after("app", "chat", advanced.delivery_serial)
        .await
        .unwrap();

    assert_eq!(first.delivery_serial, 1);
    assert_eq!(advanced.delivery_serial, 129);
    assert!(after_advanced.delivery_serial > advanced.delivery_serial);
    assert_eq!(inner.single_calls.load(Ordering::Relaxed), 0);
    assert_eq!(inner.block_calls.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn memory_store_rejects_duplicate_channel_delivery_serial() {
    let store = MemoryVersionStore::new();
    let first = base_record("msg:1", 10, 1);
    let second = base_record("msg:2", 20, 1);
    store.append_version(first).await.unwrap();

    let error = store.append_version(second).await.unwrap_err();
    assert!(
        error.to_string().contains("duplicate delivery_serial"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn memory_store_rejects_invalid_append_without_corrupting_chain() {
    let store = MemoryVersionStore::new();
    let create = base_record("msg:1", 10, 1);
    store.append_version(create.clone()).await.unwrap();

    let mut invalid = StoredVersionRecord {
        message: create
            .message
            .apply_mutation(
                MessageAction::Update,
                version("ver:2", 2),
                2,
                MessageFieldDelta::default(),
            )
            .unwrap(),
        ..create.clone()
    };
    invalid.message.identity.history_serial = 99;

    let error = store.append_version(invalid).await.unwrap_err();
    assert!(
        error.to_string().contains("mixed history_serial"),
        "unexpected error: {error}"
    );

    let latest = store
        .get_latest("app", "chat", &MessageSerial::new("msg:1").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.version_serial().as_str(), "ver:1");
    assert_eq!(latest.history_serial(), 10);

    let replay = store
        .replay_after(VersionReplayRequest {
            app_id: "app".to_string(),
            channel: "chat".to_string(),
            after_delivery_serial: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].version_serial().as_str(), "ver:1");
}

#[tokio::test]
async fn memory_store_aggregates_many_appends_for_latest_and_history_reads() {
    let store = MemoryVersionStore::new();
    let mut current = base_record("msg:1", 10, 1);
    current.message.data = Some(MessageData::String("start".to_string()));
    current.message.version.serial = VersionSerial::new("ver:00000000000000000001").unwrap();
    store.append_version(current.clone()).await.unwrap();

    for index in 0..128 {
        let next = StoredVersionRecord {
            message: current
                .message
                .apply_append(
                    version(&format!("ver:{:020}", index + 2), index + 2),
                    index as u64 + 2,
                    MessageAppend {
                        data_fragment: format!(":{index}"),
                        extras: None,
                    },
                )
                .unwrap(),
            ..current.clone()
        };
        store.append_version(next.clone()).await.unwrap();
        current = next;
    }

    let latest = store
        .get_latest("app", "chat", &MessageSerial::new("msg:1").unwrap())
        .await
        .unwrap()
        .unwrap();
    let projected = store.latest_by_history("app", "chat").await.unwrap();

    assert_eq!(projected.len(), 1);
    assert_eq!(latest.version_serial(), projected[0].version_serial());
    assert_eq!(
        latest.message.data.unwrap().into_string(),
        projected[0].message.data.clone().unwrap().into_string()
    );
    assert_eq!(
        projected[0]
            .message
            .data
            .clone()
            .unwrap()
            .into_string()
            .unwrap(),
        (0..128).fold("start".to_string(), |mut acc, index| {
            acc.push_str(&format!(":{index}"));
            acc
        })
    );
}

#[tokio::test]
async fn memory_store_preserves_channel_delivery_order_under_concurrent_appends() {
    let store = MemoryVersionStore::new();
    for index in 0..100 {
        let reservation = store
            .reserve_delivery_position("app", "chat")
            .await
            .unwrap();
        let mut create = base_record(
            &format!("msg:{index}"),
            index as u64 + 1,
            reservation.delivery_serial,
        );
        create.message.version.serial = VersionSerial::new(format!("ver:{index}:0")).unwrap();
        store.append_version(create).await.unwrap();
    }

    let handles = (0..100)
        .map(|index| {
            let store = store.clone();
            tokio::spawn(async move {
                for append_index in 0..3 {
                    let serial = MessageSerial::new(format!("msg:{index}")).unwrap();
                    let current = store
                        .get_latest("app", "chat", &serial)
                        .await
                        .unwrap()
                        .unwrap();
                    let reservation = store
                        .reserve_delivery_position("app", "chat")
                        .await
                        .unwrap();
                    let next = StoredVersionRecord {
                        message: current
                            .message
                            .apply_append(
                                version(
                                    &format!("ver:{index}:{}", append_index + 1),
                                    append_index + 1,
                                ),
                                reservation.delivery_serial,
                                MessageAppend {
                                    data_fragment: format!(":{append_index}"),
                                    extras: None,
                                },
                            )
                            .unwrap(),
                        ..current
                    };
                    store.append_version(next).await.unwrap();
                }
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.await.unwrap();
    }

    let replay = store
        .replay_after(VersionReplayRequest {
            app_id: "app".to_string(),
            channel: "chat".to_string(),
            after_delivery_serial: 0,
            limit: 1000,
        })
        .await
        .unwrap();
    assert_eq!(replay.len(), 400);
    for pair in replay.windows(2) {
        assert!(pair[0].delivery_serial() < pair[1].delivery_serial());
    }
}

#[tokio::test]
async fn memory_store_replay_log_rebuilds_identical_aggregates() {
    let source = MemoryVersionStore::new();
    let first = base_record("msg:1", 10, 1);
    let second = base_record("msg:2", 20, 2);
    source.append_version(first.clone()).await.unwrap();
    source.append_version(second.clone()).await.unwrap();

    let first_append = StoredVersionRecord {
        message: first
            .message
            .apply_append(
                version("ver:2", 2),
                3,
                MessageAppend {
                    data_fragment: " world".to_string(),
                    extras: None,
                },
            )
            .unwrap(),
        ..first
    };
    let second_update = StoredVersionRecord {
        message: second
            .message
            .apply_mutation(
                MessageAction::Update,
                version("ver:2", 2),
                4,
                MessageFieldDelta {
                    data: FieldPatch::Replace(MessageData::String("patched".to_string())),
                    ..Default::default()
                },
            )
            .unwrap(),
        ..second
    };
    source.append_version(first_append).await.unwrap();
    source.append_version(second_update).await.unwrap();

    let log = source
        .replay_after(VersionReplayRequest {
            app_id: "app".to_string(),
            channel: "chat".to_string(),
            after_delivery_serial: 0,
            limit: 100,
        })
        .await
        .unwrap();
    let rebuilt = MemoryVersionStore::new();
    for record in log {
        rebuilt.append_version(record).await.unwrap();
    }

    let source_latest = source.latest_by_history("app", "chat").await.unwrap();
    let rebuilt_latest = rebuilt.latest_by_history("app", "chat").await.unwrap();
    assert_eq!(source_latest.len(), rebuilt_latest.len());
    for (left, right) in source_latest.iter().zip(rebuilt_latest.iter()) {
        assert_eq!(left.message_serial(), right.message_serial());
        assert_eq!(left.version_serial(), right.version_serial());
        assert_eq!(
            left.message.data.clone().unwrap().into_string(),
            right.message.data.clone().unwrap().into_string()
        );
    }
}
