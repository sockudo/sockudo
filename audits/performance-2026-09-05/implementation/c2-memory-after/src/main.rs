use sockudo_core::version_store::*;
use sockudo_core::versioned_messages::*;
use sockudo_protocol::messages::MessageData;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

struct Alloc;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static REQUESTED: AtomicUsize = AtomicUsize::new(0);
// SAFETY: the allocator forwards the unchanged allocation contract to System.
unsafe impl GlobalAlloc for Alloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
            REQUESTED.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let ptr = unsafe { System.realloc(ptr, layout, size) };
        if !ptr.is_null() {
            if size >= layout.size() { LIVE.fetch_add(size - layout.size(), Ordering::Relaxed); }
            else { LIVE.fetch_sub(layout.size() - size, Ordering::Relaxed); }
            REQUESTED.fetch_add(size, Ordering::Relaxed);
        }
        ptr
    }
}
#[global_allocator]
static ALLOCATOR: Alloc = Alloc;

fn version(n: u64) -> VersionMetadata {
    VersionMetadata { serial: VersionSerial::new(format!("v:{n:020}")).unwrap(),
        client_id: Some("actor".into()), timestamp_ms: n as i64, description: None, metadata: None }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("appends,fragment_bytes,retained_alloc_bytes,requested_alloc_bytes,append_us,verified_versions,public_data_bytes");
    for count in [128u64, 512, 2000] {
        let before = LIVE.load(Ordering::Relaxed);
        let requested = REQUESTED.load(Ordering::Relaxed);
        let store = MemoryVersionStore::new();
        let serial = MessageSerial::new("message").unwrap();
        let mut current = StoredVersionRecord { app_id: "audit".into(), channel: "room".into(),
            original_client_id: Some("actor".into()), envelope: None,
            message: VersionedMessage::new_create(serial.clone(), version(1), 1, 1,
                Some("event".into()), Some(MessageData::String(String::new())), None) };
        store.append_version(current.clone()).await.unwrap();
        let fragment = "é🙂".repeat(10) + "abcd";
        assert_eq!(fragment.len(), 64);
        let start = Instant::now();
        for n in 2..=count + 1 {
            current.message = current.message.apply_append(version(n), n,
                MessageAppend { data_fragment: fragment.clone(), extras: None }).unwrap();
            store.append_version(current.clone()).await.unwrap();
        }
        let elapsed = start.elapsed().as_micros();
        let retained = LIVE.load(Ordering::Relaxed) - before;
        let requested = REQUESTED.load(Ordering::Relaxed) - requested;
        assert_eq!(sonic_rs::to_vec(&store.get_latest("audit", "room", &serial).await.unwrap().unwrap()).unwrap(), sonic_rs::to_vec(&current).unwrap());
        let mut cursor = None;
        let mut verified = 0u64;
        let mut public_bytes = 0usize;
        loop {
            let page = store.get_versions(VersionStoreReadRequest { app_id: "audit".into(), channel: "room".into(),
                message_serial: serial.clone(), direction: VersionStoreDirection::OldestFirst, limit: 37, cursor }).await.unwrap();
            assert!(page.items.len() <= 37);
            for record in page.items {
                assert_eq!(record.message_serial(), &serial);
                assert_eq!(record.version_serial(), &version(verified + 1).serial);
                assert_eq!(record.history_serial(), 1);
                assert_eq!(record.delivery_serial(), verified + 1);
                assert_eq!(record.original_client_id.as_deref(), Some("actor"));
                assert_eq!(record.message.data, Some(MessageData::String(fragment.repeat(verified as usize))));
                assert_eq!(record.message.append_fragment.as_deref(), (verified > 0).then_some(fragment.as_str()));
                public_bytes += record.data_bytes().unwrap();
                verified += 1;
            }
            cursor = page.next_cursor;
            if cursor.is_none() { break; }
        }
        assert_eq!(verified, count + 1);
        assert_eq!(public_bytes, 64 * count as usize * (count as usize + 1) / 2);
        println!("{count},64,{retained},{requested},{elapsed},{verified},{public_bytes}");
    }
}
