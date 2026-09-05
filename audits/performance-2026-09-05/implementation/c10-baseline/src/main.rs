use bytes::Bytes;
use sockudo_core::history::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status").unwrap().lines()
        .find_map(|line| line.strip_prefix("VmRSS:").map(|value| value.split_whitespace().next().unwrap().parse().unwrap()))
        .unwrap()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let expected_cleanup = std::env::var("EXPECT_IDLE_CLEANUP").unwrap() == "1";
    println!("channels,payload_bytes,retained_before,retained_after,purge_calls,deleted,cleanup_us,rss_before_kib,rss_seeded_kib,rss_after_kib");
    for channels in [1000, 10000] {
        let store = MemoryHistoryStore::default();
        let mut owners = Vec::new();
        let rss_before = rss_kib();
        let mut first_stream = None;
        for index in 0..channels {
            let channel = format!("idle-{index}");
            let position = store.reserve_publish_position("audit", &channel).await.unwrap();
            if index == 0 { first_stream = Some(position.stream_id.clone()); }
            let payload: Arc<[u8]> = vec![b'x'; 16384].into();
            owners.push(Arc::downgrade(&payload));
            store.append(HistoryAppendRecord {
                app_id: "audit".into(), channel, stream_id: position.stream_id, serial: position.serial,
                published_at_ms: now_ms(), message_id: None, event_name: Some("event".into()), operation_kind: "append".into(),
                payload_bytes: Bytes::from_owner(payload),
                retention: HistoryRetentionPolicy {retention_window_seconds: 1, max_messages_per_channel: None, max_bytes_per_channel: None},
            }).await.unwrap();
        }
        assert!(owners.iter().all(|owner| owner.upgrade().is_some()));
        let rss_seeded = rss_kib();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let retained_before = owners.iter().filter(|owner| owner.upgrade().is_some()).count();
        assert_eq!(retained_before, channels);
        let start = Instant::now();
        let mut calls = 0;
        let mut deleted = 0;
        for _ in 0..channels.div_ceil(128) + 1 {
            calls += 1;
            let (count, more) = store.purge_before(now_ms(), 128).await.unwrap();
            assert!(count <= 128);
            deleted += count;
            if !more { break; }
        }
        let elapsed = start.elapsed().as_micros();
        // Weak owners observe actual retention before any read can lazily evict.
        let retained_after = owners.iter().filter(|owner| owner.upgrade().is_some()).count();
        assert_eq!(retained_after, if expected_cleanup {0} else {channels});
        assert_eq!(deleted, if expected_cleanup {channels as u64} else {0});
        let rss_after = rss_kib();
        let next = store.reserve_publish_position("audit", "idle-0").await.unwrap();
        assert_eq!(Some(next.stream_id), first_stream);
        assert_eq!(next.serial, 2);
        println!("{channels},16384,{retained_before},{retained_after},{calls},{deleted},{elapsed},{rss_before},{rss_seeded},{rss_after}");
    }
}
