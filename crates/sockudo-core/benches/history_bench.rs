use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use sockudo_core::history::{
    HistoryAppendRecord, HistoryDirection, HistoryQueryBounds, HistoryReadRequest, HistoryStore,
    MemoryHistoryStore, MemoryHistoryStoreConfig,
};
use sockudo_core::presence_history::{
    MemoryPresenceHistoryStore, PresenceHistoryDirection, PresenceHistoryEventCause,
    PresenceHistoryEventKind, PresenceHistoryReadRequest, PresenceHistoryRetentionPolicy,
    PresenceHistoryStore, PresenceHistoryTransitionRecord,
};
use tokio::runtime::Runtime;

fn seed_history(store: &MemoryHistoryStore, rt: &Runtime, count: u64) {
    let reservation = rt
        .block_on(store.reserve_publish_position("bench-app", "bench-channel"))
        .expect("reservation");
    let stream_id = reservation.stream_id;
    let base_ts = sockudo_core::history::now_ms();

    for serial in 1..=count {
        rt.block_on(store.append(HistoryAppendRecord {
            app_id: "bench-app".to_string(),
            channel: "bench-channel".to_string(),
            stream_id: stream_id.clone(),
            serial,
            published_at_ms: base_ts + serial as i64,
            message_id: Some(format!("msg-{serial}")),
            event_name: Some("evt".to_string()),
            operation_kind: "append".to_string(),
            payload_bytes: Bytes::from_static(br#"{"event":"evt","data":"payload"}"#),
            retention: sockudo_core::history::HistoryRetentionPolicy {
                retention_window_seconds: 3600,
                max_messages_per_channel: None,
                max_bytes_per_channel: None,
            },
        }))
        .expect("append");
    }
}

fn bench_history_reads(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let store = MemoryHistoryStore::new(MemoryHistoryStoreConfig::default());
    seed_history(&store, &rt, 10_000);

    let mut group = c.benchmark_group("history_reads");

    group.bench_function("newest_first_page_100", |b| {
        b.iter(|| {
            rt.block_on(store.read_page(HistoryReadRequest {
                app_id: "bench-app".to_string(),
                channel: "bench-channel".to_string(),
                direction: HistoryDirection::NewestFirst,
                limit: 100,
                cursor: None,
                bounds: HistoryQueryBounds::default(),
            }))
            .expect("read page")
        });
    });

    group.bench_function("oldest_first_page_100", |b| {
        b.iter(|| {
            rt.block_on(store.read_page(HistoryReadRequest {
                app_id: "bench-app".to_string(),
                channel: "bench-channel".to_string(),
                direction: HistoryDirection::OldestFirst,
                limit: 100,
                cursor: None,
                bounds: HistoryQueryBounds::default(),
            }))
            .expect("read page")
        });
    });

    group.bench_function("oldest_first_large_gap_page_100", |b| {
        b.iter(|| {
            rt.block_on(store.read_page(HistoryReadRequest {
                app_id: "bench-app".to_string(),
                channel: "bench-channel".to_string(),
                direction: HistoryDirection::OldestFirst,
                limit: 100,
                cursor: None,
                bounds: HistoryQueryBounds {
                    start_serial: Some(9_000),
                    end_serial: None,
                    start_time_ms: None,
                    end_time_ms: None,
                },
            }))
            .expect("read page")
        });
    });

    group.finish();
}

fn seed_presence_history(store: &MemoryPresenceHistoryStore, rt: &Runtime, count: u64) {
    let base_ts = sockudo_core::history::now_ms();

    for index in 1..=count {
        rt.block_on(store.record_transition(PresenceHistoryTransitionRecord {
            app_id: "bench-app".to_string(),
            channel: "presence-bench-channel".to_string(),
            event_kind: if index % 2 == 0 {
                PresenceHistoryEventKind::MemberRemoved
            } else {
                PresenceHistoryEventKind::MemberAdded
            },
            cause: if index % 2 == 0 {
                PresenceHistoryEventCause::Disconnect
            } else {
                PresenceHistoryEventCause::Join
            },
            user_id: format!("user-{index}"),
            connection_id: Some(format!("socket-{index}")),
            user_info: None,
            dead_node_id: None,
            dedupe_key: format!("presence-transition-{index}"),
            published_at_ms: base_ts + index as i64,
            retention: PresenceHistoryRetentionPolicy {
                retention_window_seconds: 3600,
                max_events_per_channel: None,
                max_bytes_per_channel: None,
            },
        }))
        .expect("presence append");
    }
}

fn bench_presence_history_reads(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let store = MemoryPresenceHistoryStore::new(Default::default());
    seed_presence_history(&store, &rt, 10_000);

    let mut group = c.benchmark_group("presence_history_reads");

    group.bench_function("newest_first_page_100", |b| {
        b.iter(|| {
            rt.block_on(store.read_page(PresenceHistoryReadRequest {
                app_id: "bench-app".to_string(),
                channel: "presence-bench-channel".to_string(),
                direction: PresenceHistoryDirection::NewestFirst,
                limit: 100,
                cursor: None,
                bounds: Default::default(),
            }))
            .expect("presence read page")
        });
    });

    group.bench_function("oldest_first_page_100", |b| {
        b.iter(|| {
            rt.block_on(store.read_page(PresenceHistoryReadRequest {
                app_id: "bench-app".to_string(),
                channel: "presence-bench-channel".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 100,
                cursor: None,
                bounds: Default::default(),
            }))
            .expect("presence read page")
        });
    });

    group.finish();
}

fn bench_presence_history_writes(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("presence_history_writes");

    group.bench_function("rapid_join_leave_churn_1000", |b| {
        let store = MemoryPresenceHistoryStore::new(Default::default());
        let base_ts = sockudo_core::history::now_ms();
        let mut serial = 0u64;
        b.iter(|| {
            for i in 0..1000 {
                serial += 1;
                let is_join = i % 2 == 0;
                rt.block_on(store.record_transition(PresenceHistoryTransitionRecord {
                    app_id: "bench-app".to_string(),
                    channel: "presence-hot-channel".to_string(),
                    event_kind: if is_join {
                        PresenceHistoryEventKind::MemberAdded
                    } else {
                        PresenceHistoryEventKind::MemberRemoved
                    },
                    cause: if is_join {
                        PresenceHistoryEventCause::Join
                    } else {
                        PresenceHistoryEventCause::Disconnect
                    },
                    user_id: format!("user-{}", i % 50),
                    connection_id: Some(format!("socket-{serial}")),
                    user_info: None,
                    dead_node_id: None,
                    dedupe_key: format!("churn-{serial}"),
                    published_at_ms: base_ts + serial as i64,
                    retention: PresenceHistoryRetentionPolicy {
                        retention_window_seconds: 3600,
                        max_events_per_channel: None,
                        max_bytes_per_channel: None,
                    },
                }))
                .expect("write");
            }
        });
    });

    group.bench_function("write_with_retention_eviction_cap_500", |b| {
        let store = MemoryPresenceHistoryStore::new(Default::default());
        let base_ts = sockudo_core::history::now_ms();
        let mut serial = 0u64;
        b.iter(|| {
            for i in 0..1000 {
                serial += 1;
                rt.block_on(store.record_transition(PresenceHistoryTransitionRecord {
                    app_id: "bench-app".to_string(),
                    channel: "presence-evict-channel".to_string(),
                    event_kind: if i % 2 == 0 {
                        PresenceHistoryEventKind::MemberAdded
                    } else {
                        PresenceHistoryEventKind::MemberRemoved
                    },
                    cause: PresenceHistoryEventCause::Join,
                    user_id: format!("user-{serial}"),
                    connection_id: Some(format!("socket-{serial}")),
                    user_info: None,
                    dead_node_id: None,
                    dedupe_key: format!("evict-{serial}"),
                    published_at_ms: base_ts + serial as i64,
                    retention: PresenceHistoryRetentionPolicy {
                        retention_window_seconds: 3600,
                        max_events_per_channel: Some(500),
                        max_bytes_per_channel: None,
                    },
                }))
                .expect("write");
            }
        });
    });

    group.finish();
}

fn bench_presence_history_snapshot(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");

    let mut group = c.benchmark_group("presence_history_snapshot");

    // Snapshot reconstruction from 1000 events
    let store_1k = MemoryPresenceHistoryStore::new(Default::default());
    seed_presence_history(&store_1k, &rt, 1_000);

    group.bench_function("snapshot_from_1000_events", |b| {
        b.iter(|| {
            rt.block_on(store_1k.snapshot_at(sockudo_core::presence_history::PresenceSnapshotRequest {
                app_id: "bench-app".to_string(),
                channel: "presence-bench-channel".to_string(),
                at_time_ms: None,
                at_serial: None,
            }))
            .expect("snapshot")
        });
    });

    // Snapshot reconstruction from 10000 events
    let store_10k = MemoryPresenceHistoryStore::new(Default::default());
    seed_presence_history(&store_10k, &rt, 10_000);

    group.bench_function("snapshot_from_10000_events", |b| {
        b.iter(|| {
            rt.block_on(store_10k.snapshot_at(sockudo_core::presence_history::PresenceSnapshotRequest {
                app_id: "bench-app".to_string(),
                channel: "presence-bench-channel".to_string(),
                at_time_ms: None,
                at_serial: None,
            }))
            .expect("snapshot")
        });
    });

    // Snapshot bounded at serial midpoint (only replays half)
    group.bench_function("snapshot_bounded_at_serial_5000", |b| {
        b.iter(|| {
            rt.block_on(store_10k.snapshot_at(sockudo_core::presence_history::PresenceSnapshotRequest {
                app_id: "bench-app".to_string(),
                channel: "presence-bench-channel".to_string(),
                at_time_ms: None,
                at_serial: Some(5000),
            }))
            .expect("snapshot")
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_history_reads,
    bench_presence_history_reads,
    bench_presence_history_writes,
    bench_presence_history_snapshot
);
criterion_main!(benches);
