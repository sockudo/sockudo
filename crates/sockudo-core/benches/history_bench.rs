use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use sockudo_core::history::{
    HistoryAppendRecord, HistoryDirection, HistoryQueryBounds, HistoryReadRequest,
    HistoryStore, MemoryHistoryStore, MemoryHistoryStoreConfig,
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

    group.finish();
}

criterion_group!(benches, bench_history_reads);
criterion_main!(benches);
