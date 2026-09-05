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
    for count in [100, 1000, 10000] {
        let store = MemoryAnnotationStore::new();
        rt.block_on(async {
            for n in 0..count {
                store.append_event(annotation(n)).await.unwrap();
            }
        });
        // Re-appending the same serial exercises a fixed-size projection rebuild.
        sample("annotation_duplicate_rebuild", count, 0, || {
            let projection = rt.block_on(store.append_event(annotation(count - 1))).unwrap();
            assert_eq!(projection.summary, AnnotationSummary::Total(TotalAnnotationSummary { total: count as u64 }));
            assert_eq!(projection.last_annotation_serial, Some(annotation(count - 1).annotation.serial));
            projection
        });
    }
}
