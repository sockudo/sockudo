//! Reproducible P4 component probe; `monolith` selects the V2 candidate in the isolated variant.
use sockudo_push::*;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

struct CountingAllocator;
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: the exact caller-provided layout is forwarded to the system allocator.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: allocation and deallocation use the same system allocator and layout.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(size as u64, Ordering::Relaxed);
        // SAFETY: the caller's pointer and layout are forwarded without modification.
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[cfg(feature = "monolith")]
fn encode(payload: &PushQueuePayload) -> String {
    sockudo_push::batch_wire::encode_queue_payload(payload).unwrap()
}
#[cfg(not(feature = "monolith"))]
fn encode(payload: &PushQueuePayload) -> String {
    sonic_rs::to_string(payload).unwrap()
}

#[cfg(feature = "monolith")]
fn decode(encoded: &str) -> DeliveryBatch {
    let PushQueuePayload::DeliveryBatch(batch) =
        sockudo_push::batch_wire::decode_queue_payload(encoded).unwrap()
    else {
        panic!("invalid decoded variant")
    };
    *batch
}
#[cfg(not(feature = "monolith"))]
fn decode(encoded: &str) -> DeliveryBatch {
    sonic_rs::from_str(encoded).unwrap()
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let broker_mode = args
        .get(1)
        .map(String::as_str)
        .filter(|mode| matches!(*mode, "emit" | "verify"));
    for bytes in [256, 65_536] {
        for recipients in [1, 100, 1_000] {
            if broker_mode.is_some()
                && (args[2].parse::<usize>().unwrap() != bytes
                    || args[3].parse::<usize>().unwrap() != recipients)
            {
                continue;
            }
            let payload = Arc::new(PushPayload {
                template_id: None,
                template_data: sonic_rs::json!({}),
                title: Some("audit".to_owned()),
                body: Some("x".repeat(bytes)),
                icon: None,
                sound: None,
                collapse_key: None,
            });
            let jobs = (0..recipients)
                .map(|id| DeliveryJob {
                    app_id: "audit".to_owned(),
                    publish_id: "publish".to_owned(),
                    provider: PushProviderKind::Fcm,
                    batch_id: "batch".to_owned(),
                    device_id: Some(format!("device-{id}")),
                    recipient: PushRecipient::Fcm {
                        registration_token: SecretString::new(format!("recipient-{id}")).unwrap(),
                    },
                    payload: payload.clone(),
                    rendered_payload: None,
                    attempt: 1,
                    first_attempt_at_ms: None,
                    not_before_ms: None,
                    expires_at_ms: None,
                })
                .collect();
            let batch = DeliveryBatch {
                app_id: "audit".to_owned(),
                publish_id: "publish".to_owned(),
                provider: PushProviderKind::Fcm,
                batch_id: "batch".to_owned(),
                jobs,
            };
            let message = PushQueuePayload::DeliveryBatch(Box::new(batch.clone()));
            if let Some(mode) = broker_mode {
                if mode == "emit" {
                    std::fs::write(&args[4], encode(&message)).unwrap();
                } else {
                    assert_eq!(decode(&std::fs::read_to_string(&args[4]).unwrap()), batch);
                }
                return;
            }
            for rep in 0..7 {
                ALLOCATIONS.store(0, Ordering::Relaxed);
                ALLOCATED_BYTES.store(0, Ordering::Relaxed);
                let start = Instant::now();
                let encoded = encode(std::hint::black_box(&message));
                let encode_us = start.elapsed().as_micros();
                let allocations = ALLOCATIONS.load(Ordering::Relaxed);
                let allocated_bytes = ALLOCATED_BYTES.load(Ordering::Relaxed);
                let start = Instant::now();
                let decoded = decode(&encoded);
                let decode_us = start.elapsed().as_micros();
                assert_eq!(decoded, batch);
                println!(
                    "p4,bytes={bytes},recipients={recipients},rep={rep},encode_us={encode_us},decode_us={decode_us},wire_bytes={},allocations={allocations},allocated_bytes={allocated_bytes}",
                    encoded.len()
                );
            }
        }
    }
}
