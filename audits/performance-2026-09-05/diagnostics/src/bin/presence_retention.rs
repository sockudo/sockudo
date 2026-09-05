//! Count-capped retained presence history, compared with equal-length uncapped history.
use sockudo_core::history::now_ms;
use sockudo_core::presence_history::*;
use std::time::Instant;
fn event(n: usize, cap: Option<usize>) -> PresenceHistoryTransitionRecord {
    PresenceHistoryTransitionRecord {
        app_id: "audit".into(),
        channel: "presence-audit".into(),
        event_kind: PresenceHistoryEventKind::MemberAdded,
        cause: PresenceHistoryEventCause::Join,
        user_id: format!("user-{n}"),
        connection_id: Some(format!("socket-{n}")),
        user_info: None,
        dead_node_id: None,
        dedupe_key: format!("dedupe-{n}"),
        published_at_ms: now_ms(),
        retention: PresenceHistoryRetentionPolicy {
            retention_window_seconds: 3600,
            max_events_per_channel: cap,
            max_bytes_per_channel: None,
        },
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("retained,capped,samples,p50_ns,p95_ns,p99_ns");
    for count in [100, 1000, 10000] {
        for capped in [false, true] {
            let store = MemoryPresenceHistoryStore::new(Default::default());
            let cap = capped.then_some(count);
            for i in 0..count {
                store.record_transition(event(i, cap)).await.unwrap();
            }
            let mut ns = Vec::with_capacity(101);
            for i in count..count + 101 {
                let t = Instant::now();
                store.record_transition(event(i, cap)).await.unwrap();
                ns.push(t.elapsed().as_nanos());
            }
            ns.sort_unstable();
            println!("{count},{capped},101,{},{},{}", ns[50], ns[95], ns[99]);
        }
    }
}
