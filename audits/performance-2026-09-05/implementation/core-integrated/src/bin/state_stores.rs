//! Audit-only probes of actual public store APIs. No alternate implementation.
use bytes::Bytes;
use sockudo_core::annotations::*;
use sockudo_core::history::*;
use sockudo_core::version_store::*;
use sockudo_core::versioned_messages::*;
use sockudo_protocol::messages::MessageData;
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

struct CountAlloc;
static TRACK: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
// SAFETY: all operations forward their original valid arguments to System.
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() && TRACK.load(Ordering::Relaxed) {
            CALLS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(l.size() as u64, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        let p = unsafe { System.realloc(p, l, n) };
        if !p.is_null() && TRACK.load(Ordering::Relaxed) {
            CALLS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(n as u64, Ordering::Relaxed);
        }
        p
    }
}
#[global_allocator]
static ALLOC: CountAlloc = CountAlloc;
fn sample<T>(name: &str, retained: usize, payload: usize, mut op: impl FnMut() -> T) {
    for _ in 0..5 {
        black_box(op());
    }
    let mut times = Vec::with_capacity(101);
    CALLS.store(0, Ordering::SeqCst);
    BYTES.store(0, Ordering::SeqCst);
    TRACK.store(true, Ordering::SeqCst);
    for _ in 0..101 {
        let t = Instant::now();
        black_box(op());
        times.push(t.elapsed().as_nanos() as u64);
    }
    TRACK.store(false, Ordering::SeqCst);
    times.sort_unstable();
    println!(
        "{name},{retained},{payload},101,{},{},{},{},{}",
        times[50],
        times[95],
        times[99],
        CALLS.load(Ordering::SeqCst) / 101,
        BYTES.load(Ordering::SeqCst) / 101
    );
}
fn version(n: u64) -> VersionMetadata {
    VersionMetadata {
        serial: VersionSerial::new(format!("ver:{n:020}")).unwrap(),
        client_id: Some("actor".into()),
        timestamp_ms: n as i64,
        description: None,
        metadata: None,
    }
}
fn record(n: u64, size: usize) -> StoredVersionRecord {
    StoredVersionRecord {
        app_id: "audit".into(),
        channel: "room".into(),
        original_client_id: Some("actor".into()),
        envelope: None,
        message: VersionedMessage::new_create(
            MessageSerial::new("msg:1").unwrap(),
            version(n),
            1,
            n,
            Some("evt".into()),
            Some(MessageData::String("x".repeat(size))),
            None,
        ),
    }
}
fn annotation(n: usize) -> StoredAnnotationEvent {
    StoredAnnotationEvent {
        app_id: "audit".into(),
        channel_id: "room".into(),
        stored_at_ms: now_ms(),
        annotation: Annotation {
            id: AnnotationId::new(format!("id:{n:020}")).unwrap(),
            action: AnnotationAction::Create,
            serial: AnnotationSerial::new(format!("ann:{n:020}")).unwrap(),
            message_serial: MessageSerial::new("msg:1").unwrap(),
            annotation_type: AnnotationType::new("reaction:total.v1").unwrap(),
            name: None,
            client_id: None,
            count: None,
            data: None,
            encoding: None,
            timestamp: n as i64,
        },
    }
}
fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    println!(
        "operation,retained,payload_bytes,samples,p50_ns,p95_ns,p99_ns,alloc_calls_per_op,requested_alloc_bytes_per_op"
    );
    for payload in [256, 4096, 65536] {
        for count in [16, 128, 1024] {
            let store = MemoryVersionStore::new();
            rt.block_on(async {
                for n in 1..=count {
                    store
                        .append_version(record(n as u64, payload))
                        .await
                        .unwrap();
                }
            });
            let serial = MessageSerial::new("msg:1").unwrap();
            sample("get_latest", count, payload, || {
                rt.block_on(store.get_latest("audit", "room", &serial))
                    .unwrap()
            });
            let mut current = rt
                .block_on(store.get_latest("audit", "room", &serial))
                .unwrap()
                .unwrap();
            let mut n = count as u64;
            sample("compare_apply_fixed_size", count, payload, || {
                n += 1;
                let result = rt
                    .block_on(store.compare_and_apply(VersionMutationRequest {
                        app_id: "audit".into(),
                        channel: "room".into(),
                        message_serial: serial.clone(),
                        expected: VersionPrecondition::from_record(&current),
                        version: version(n),
                        mutation: VersionMutation::Update(MessageFieldDelta {
                            data: FieldPatch::Replace(MessageData::String("x".repeat(payload))),
                            ..Default::default()
                        }),
                        idempotency: None,
                        limits: VersionMutationLimits::default(),
                    }))
                    .unwrap();
                if let VersionMutationResult::Applied { record, .. } = result {
                    current = record;
                } else {
                    panic!("mutation rejected");
                }
            });
        }
    }
    for count in [100, 1000, 10000] {
        let store = MemoryAnnotationStore::new();
        rt.block_on(async {
            for n in 0..count {
                store.append_event(annotation(n)).await.unwrap();
            }
        });
        // Re-appending the same serial exercises a fixed-size projection rebuild.
        sample("annotation_duplicate_rebuild", count, 0, || {
            rt.block_on(store.append_event(annotation(count - 1)))
                .unwrap()
        });
    }
    for count in [1000, 10000, 100000] {
        let store = MemoryHistoryStore::new(Default::default());
        let stream = rt
            .block_on(store.reserve_publish_position("audit", "room"))
            .unwrap()
            .stream_id;
        rt.block_on(async {
            for n in 1..=count {
                store
                    .append(HistoryAppendRecord {
                        app_id: "audit".into(),
                        channel: "room".into(),
                        stream_id: stream.clone(),
                        serial: n as u64,
                        published_at_ms: now_ms(),
                        message_id: None,
                        event_name: Some("evt".into()),
                        operation_kind: "append".into(),
                        payload_bytes: Bytes::from(vec![b'x'; 256]),
                        retention: HistoryRetentionPolicy {
                            retention_window_seconds: 3600,
                            max_messages_per_channel: None,
                            max_bytes_per_channel: None,
                        },
                    })
                    .await
                    .unwrap();
            }
        });
        for (label, start) in [
            ("history_first_page", None),
            ("history_deep_page", Some((count - 100) as u64)),
        ] {
            sample(label, count, 256, || {
                rt.block_on(store.read_page(HistoryReadRequest {
                    app_id: "audit".into(),
                    channel: "room".into(),
                    direction: HistoryDirection::OldestFirst,
                    limit: 100,
                    cursor: None,
                    bounds: HistoryQueryBounds {
                        start_serial: start,
                        ..Default::default()
                    },
                }))
                .unwrap()
            });
        }
    }
    // Confirm actual retained cumulative full-state bytes, excluding metadata/alloc overhead.
    for count in [128u64, 512, 2000] {
        let store = MemoryVersionStore::new();
        let mut current = record(1, 0);
        rt.block_on(store.append_version(current.clone())).unwrap();
        rt.block_on(async {
            for n in 2..=count + 1 {
                current.message = current
                    .message
                    .apply_append(
                        version(n),
                        n,
                        MessageAppend {
                            data_fragment: "x".repeat(64),
                            extras: None,
                        },
                    )
                    .unwrap();
                store.append_version(current.clone()).await.unwrap();
            }
        });
        let mut cursor = None;
        let mut bytes = 0;
        let mut records = 0;
        loop {
            let page = rt
                .block_on(store.get_versions(VersionStoreReadRequest {
                    app_id: "audit".into(),
                    channel: "room".into(),
                    message_serial: MessageSerial::new("msg:1").unwrap(),
                    direction: VersionStoreDirection::OldestFirst,
                    limit: 100,
                    cursor,
                }))
                .unwrap();
            for r in page.items {
                bytes += r.data_bytes().unwrap();
                records += 1;
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        eprintln!(
            "snapshot_storage appends={count} fragment_bytes=64 retained_versions={records} latest_bytes={} summed_data_bytes={bytes}",
            current.data_bytes().unwrap()
        );
    }
}
