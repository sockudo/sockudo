#![allow(dead_code)]
#[path = "../../baseline/crates/sockudo-adapter/src/replay_buffer.rs"]
mod replay;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::hint::black_box;
use std::time::{Instant, Duration};
use sockudo_protocol::{messages::{PusherMessage, MessageData}, wire::{serialize_message, deserialize_message, WireFormat}};
struct Allocator;
static ALLOC: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 { ALLOC.fetch_add(layout.size(), Ordering::Relaxed); unsafe { System.alloc(layout) } }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) { unsafe { System.dealloc(ptr, layout) } }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 { ALLOC.fetch_add(size, Ordering::Relaxed); unsafe { System.realloc(ptr, layout, size) } }
}
#[global_allocator] static GLOBAL: Allocator = Allocator;
fn main() {
    if std::env::var_os("SOCKUDO_F8_VERSIONED_ONLY").is_some() { versioned(); return; }
    for capacity in [100, 10_000] {
        for sample in 0..9 {
            let buffer = replay::ReplayBuffer::new(capacity, Duration::from_secs(120));
            let allocated = ALLOC.load(Ordering::Relaxed);
            let start = Instant::now();
            for channel in 0..1_000 { let p = buffer.current_position("app", &channel.to_string()); assert_eq!(p.serial, 0); black_box(p); }
            println!("F7 capacity={capacity} sample={sample} ns={} requested_bytes={}", start.elapsed().as_nanos(), ALLOC.load(Ordering::Relaxed) - allocated);
        }
    }
    for format in [WireFormat::MessagePack, WireFormat::Protobuf] {
        for size in [1024, 16384, 65536] {
            for kind in ["string", "binary", "json"] {
                let data = match kind { "binary" => MessageData::Binary(vec![42;size]), "json" => MessageData::Json(sonic_rs::json!({"value":"x".repeat(size),"integer":u64::MAX})), _ => MessageData::String("x".repeat(size)) };
                let message = PusherMessage { event: Some("event".into()), channel: Some("channel".into()), data: Some(data), ..PusherMessage::ping() };
                let decoded = deserialize_message(&serialize_message(&message, format).unwrap(), format).unwrap();
                assert_eq!(sonic_rs::to_string(&decoded).unwrap(), sonic_rs::to_string(&message).unwrap());
                for sample in 0..9 {
                    let allocated = ALLOC.load(Ordering::Relaxed);
                    let start = Instant::now();
                    for _ in 0..1000 { black_box(serialize_message(black_box(&message), format).unwrap()); }
                    println!("F8 format={format:?} kind={kind} size={size} sample={sample} ns={} requested_bytes={}",start.elapsed().as_nanos()/1000,(ALLOC.load(Ordering::Relaxed)-allocated)/1000);
                }
            }
        }
    }
}

fn versioned() {
    use sockudo_protocol::wire::{serialize_versioned_message, deserialize_versioned_message};
    use sockudo_protocol::versioned_messages::{VersionedRealtimeMessage, MessageAction, MessageVersionMetadata};
    for format in [WireFormat::MessagePack, WireFormat::Protobuf] {
        for size in [1024, 16384, 65536] {
            for kind in ["string", "binary", "json"] {
                let data = match kind { "binary" => MessageData::Binary(vec![42;size]), "json" => MessageData::Json(sonic_rs::json!({"value":"x".repeat(size),"integer":u64::MAX})), _ => MessageData::String("x".repeat(size)) };
                let message = VersionedRealtimeMessage {
                    message: PusherMessage { event: Some("sockudo:message.update".into()), channel: Some("channel".into()), name: Some("event".into()), data: Some(data), serial: Some(9), ..PusherMessage::ping() },
                    action: MessageAction::Update, message_serial: "message:1".into(), history_serial: Some(7), delivery_serial: Some(9),
                    version: Some(MessageVersionMetadata { serial: "version:2".into(), client_id: Some("client".into()), timestamp_ms: 1713100805000, description: Some("replacement".into()), metadata: Some(sonic_rs::json!({"counter":u64::MAX})) }),
                };
                let decoded = deserialize_versioned_message(&serialize_versioned_message(&message, format).unwrap(), format).unwrap();
                assert_eq!(sonic_rs::to_string(&decoded).unwrap(), sonic_rs::to_string(&message).unwrap());
                for sample in 0..9 {
                    let allocated = ALLOC.load(Ordering::Relaxed);
                    let start = Instant::now();
                    for _ in 0..1000 { black_box(serialize_versioned_message(black_box(&message), format).unwrap()); }
                    println!("F8-versioned format={format:?} kind={kind} size={size} sample={sample} ns={} requested_bytes={}",start.elapsed().as_nanos()/1000,(ALLOC.load(Ordering::Relaxed)-allocated)/1000);
                }
            }
        }
    }
}
