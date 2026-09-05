use super::*;
use crate::history::{
    HistoryPage, HistoryStreamInspection, HistoryWriteReservation, MemoryHistoryStore,
    MemoryHistoryStoreConfig, now_ms,
};
use crate::presence_history::test_support::transition;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

struct InterruptedHistory {
    inner: Arc<MemoryHistoryStore>,
    fault: AtomicU8,
    reads: AtomicUsize,
}
#[async_trait]
impl HistoryStore for InterruptedHistory {
    async fn reserve_publish_position(&self, a: &str, c: &str) -> Result<HistoryWriteReservation> {
        self.inner.reserve_publish_position(a, c).await
    }
    async fn append(&self, r: HistoryAppendRecord) -> Result<()> {
        self.inner.append(r).await
    }
    async fn stream_inspection(&self, a: &str, c: &str) -> Result<HistoryStreamInspection> {
        self.inner.stream_inspection(a, c).await
    }
    async fn read_page(&self, r: HistoryReadRequest) -> Result<HistoryPage> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let mut page = self.inner.read_page(r.clone()).await?;
        match self.fault.swap(0, Ordering::SeqCst) {
            1 => {
                page.complete = false;
                page.truncated_by_retention = true;
            }
            2 => {
                page.items.clear();
                page.has_more = false;
                page.next_cursor = None;
            }
            3 => {
                self.inner
                    .reset_stream(&r.app_id, &r.channel, "synthetic reset race", None)
                    .await?;
            }
            _ => {}
        }
        Ok(page)
    }
}
#[tokio::test]
async fn incomplete_empty_and_reset_races_never_prove_absence() {
    for fault in 1..=3 {
        let inner = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let seed = DurablePresenceHistoryStore::new(inner.clone(), None);
        seed.record_transition(transition(
            now_ms(),
            "join",
            PresenceHistoryEventKind::MemberAdded,
            "alice",
        ))
        .await
        .unwrap();
        let wrapper = Arc::new(InterruptedHistory {
            inner: inner.clone(),
            fault: AtomicU8::new(fault),
            reads: AtomicUsize::new(0),
        });
        let store = DurablePresenceHistoryStore::new(wrapper, None);
        let result = store
            .record_transition(transition(
                now_ms(),
                "new-join",
                PresenceHistoryEventKind::MemberAdded,
                "bob",
            ))
            .await;
        assert!(result.is_err(), "fault {fault} must fail closed");
        let retained = inner
            .stream_inspection(
                "app",
                &DurablePresenceHistoryStore::durable_channel_name("presence-room"),
            )
            .await
            .unwrap()
            .retained
            .retained_messages;
        assert_eq!(retained, if fault == 3 { 0 } else { 1 });
        store
            .record_transition(transition(
                now_ms(),
                "new-join",
                PresenceHistoryEventKind::MemberAdded,
                "bob",
            ))
            .await
            .unwrap();
    }
}
#[tokio::test]
async fn channel_churn_and_large_retention_bound_disposable_metadata() {
    let inner = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
    let store = DurablePresenceHistoryStore::new(inner.clone(), None);
    for channel in 0..1000 {
        store
            .channel_cache("app", &format!("presence-{channel}"))
            .await;
    }
    assert_eq!(
        store.transition_cache.read().await.len(),
        TRANSITION_CACHE_CHANNELS
    );
    let count = 600;
    for serial in 0..count {
        let mut record = transition(
            now_ms(),
            &format!("join-{serial}"),
            PresenceHistoryEventKind::MemberAdded,
            &format!("user-{serial}"),
        );
        record.retention.max_events_per_channel = Some(2000);
        let reservation = inner
            .reserve_publish_position(
                "app",
                &DurablePresenceHistoryStore::durable_channel_name("presence-room"),
            )
            .await
            .unwrap();
        inner
            .append(HistoryAppendRecord {
                app_id: record.app_id.clone(),
                channel: DurablePresenceHistoryStore::durable_channel_name(&record.channel),
                stream_id: reservation.stream_id,
                serial: reservation.serial,
                published_at_ms: record.published_at_ms,
                message_id: None,
                event_name: None,
                operation_kind: "append".into(),
                payload_bytes: DurablePresenceHistoryStore::encode_payload(&record).unwrap(),
                retention: DurablePresenceHistoryStore::history_retention(&record),
            })
            .await
            .unwrap();
    }
    for serial in [0, 599] {
        store
            .record_transition(transition(
                now_ms(),
                &format!("repeat-{serial}"),
                PresenceHistoryEventKind::MemberAdded,
                &format!("user-{serial}"),
            ))
            .await
            .unwrap();
        let cached = store.channel_cache("app", "presence-room").await;
        assert!(cached.lock().await.accounted_bytes <= TRANSITION_CACHE_BYTES);
    }
    assert_eq!(
        inner
            .stream_inspection(
                "app",
                &DurablePresenceHistoryStore::durable_channel_name("presence-room")
            )
            .await
            .unwrap()
            .retained
            .retained_messages,
        count
    );
}

#[test]
fn membership_merging_and_pruning_never_forget_retained_identity() {
    let mut cache = DurablePresenceTransitionCache::default();
    for serial in 1..40_000 {
        cache.insert(
            format!("dedupe-{serial}"),
            format!("user-{serial}"),
            PresenceHistoryEventKind::MemberAdded,
            serial,
        );
        assert!(cache.accounted_bytes <= TRANSITION_CACHE_BYTES);
    }
    assert_eq!(cache.membership.len(), TRANSITION_MEMBERSHIP_RANGES);
    cache.prune_before(20_000);
    assert!(cache.membership.iter().all(|range| range.last >= 20_000));
    for serial in 20_000..40_000 {
        assert!(cache.may_contain(0, &format!("dedupe-{serial}")));
        assert!(cache.may_contain(1, &format!("user-{serial}")));
    }
    let empty = DurablePresenceTransitionCache::default();
    assert!(!empty.may_contain(0, "dedupe-1"));
    assert!(!empty.may_contain(1, "user-1"));
}

#[test]
fn expired_ranges_do_not_consume_membership_capacity_under_retention_churn() {
    let mut cache = DurablePresenceTransitionCache::default();
    for serial in 1_u64..25_000 {
        let oldest = serial.saturating_sub(512);
        cache.prune_before(oldest);
        cache.insert(
            format!("dedupe-{serial}"),
            format!("user-{serial}"),
            PresenceHistoryEventKind::MemberAdded,
            serial,
        );
        assert!(cache.membership.len() <= 4);
        assert!(cache.membership.iter().all(|range| range.last >= oldest));
    }
    for serial in 24_487..25_000 {
        assert!(cache.may_contain(0, &format!("dedupe-{serial}")));
        assert!(cache.may_contain(1, &format!("user-{serial}")));
    }
}

#[test]
fn recent_eviction_at_maximum_serial_remains_bounded() {
    let mut cache = DurablePresenceTransitionCache::default();
    cache.insert(
        "maximum".into(),
        "maximum".into(),
        PresenceHistoryEventKind::MemberAdded,
        u64::MAX,
    );
    cache.insert(
        "x".repeat(TRANSITION_CACHE_BYTES),
        "oversized".into(),
        PresenceHistoryEventKind::MemberAdded,
        u64::MAX,
    );
    assert!(cache.serial_entries.is_empty());
    assert!(cache.accounted_bytes <= TRANSITION_CACHE_BYTES);
    assert!(cache.may_contain(1, "maximum"));
}

#[tokio::test]
async fn cold_member_lookup_reads_only_candidate_ranges_and_fails_closed() {
    for fault in 0..=3 {
        let inner = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let history = Arc::new(InterruptedHistory {
            inner: inner.clone(),
            fault: AtomicU8::new(0),
            reads: AtomicUsize::new(0),
        });
        let store = DurablePresenceHistoryStore::new(history.clone(), None);
        for serial in 0..2000 {
            let mut record = transition(
                now_ms(),
                &format!("join-{serial}"),
                PresenceHistoryEventKind::MemberAdded,
                &format!("user-{serial}"),
            );
            record.retention.max_events_per_channel = Some(5000);
            store.record_transition(record).await.unwrap();
        }
        history.fault.store(fault, Ordering::SeqCst);
        let before = history.reads.load(Ordering::SeqCst);
        let result = store
            .record_transition(transition(
                now_ms(),
                "cold-query",
                PresenceHistoryEventKind::MemberAdded,
                "user-0",
            ))
            .await;
        if fault == 0 {
            result.unwrap();
            let reads = history.reads.load(Ordering::SeqCst) - before;
            assert!(reads <= 2, "cold lookup unnecessarily read {reads} pages");
        } else {
            assert!(result.is_err(), "range fault {fault} must fail closed");
        }
        assert_eq!(
            inner
                .stream_inspection("app", "[presence-history]presence-room")
                .await
                .unwrap()
                .retained
                .retained_messages,
            if fault == 3 { 0 } else { 2000 }
        );
    }
}

#[tokio::test]
async fn steady_new_members_do_not_replay_history_beyond_metadata_capacity() {
    let inner = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
    let history = Arc::new(InterruptedHistory {
        inner: inner.clone(),
        fault: AtomicU8::new(0),
        reads: AtomicUsize::new(0),
    });
    let store = DurablePresenceHistoryStore::new(history.clone(), None);
    for serial in 0..2000 {
        let mut record = transition(
            now_ms(),
            &format!("join-{serial}"),
            PresenceHistoryEventKind::MemberAdded,
            &format!("user-{serial}"),
        );
        record.retention.max_events_per_channel = Some(5000);
        store.record_transition(record).await.unwrap();
    }
    let cache = store.channel_cache("app", "presence-room").await;
    let mut cached = cache.lock().await;
    assert!(cached.accounted_bytes <= TRANSITION_CACHE_BYTES);
    assert!(cached.serial_entries.len() < 500);
    assert_eq!(cached.covered_through, Some(2000));
    // Force false positives. They must only trigger authoritative fallback,
    // never invent a duplicate or permit a known duplicate through.
    for range in &mut cached.membership {
        range.words.fill(u64::MAX);
    }
    drop(cached);
    let reads = history.reads.load(Ordering::SeqCst);
    assert!(
        reads < 16,
        "steady transitions performed {reads} history reads"
    );
    store
        .record_transition(transition(
            now_ms(),
            "join-0",
            PresenceHistoryEventKind::MemberAdded,
            "user-0",
        ))
        .await
        .unwrap();
    assert!(history.reads.load(Ordering::SeqCst) > reads);
    assert_eq!(
        inner
            .stream_inspection(
                "app",
                &DurablePresenceHistoryStore::durable_channel_name("presence-room")
            )
            .await
            .unwrap()
            .retained
            .retained_messages,
        2000
    );
    for pass in 0..2 {
        let before = history.reads.load(Ordering::SeqCst);
        for user in 0..100 {
            store
                .record_transition(transition(
                    now_ms(),
                    &format!("query-{user}"),
                    PresenceHistoryEventKind::MemberAdded,
                    &format!("user-{}", user * 10),
                ))
                .await
                .unwrap();
        }
        if pass == 1 {
            assert_eq!(
                history.reads.load(Ordering::SeqCst),
                before,
                "verified old-member query outcomes must remain reusable"
            );
        }
    }
    let cached = cache.lock().await;
    assert!(cached.accounted_bytes + cached.queried_user_bytes <= TRANSITION_CACHE_BYTES);
    drop(cached);
    inner
        .reset_stream(
            "app",
            &DurablePresenceHistoryStore::durable_channel_name("presence-room"),
            "synthetic reset",
            None,
        )
        .await
        .unwrap();
    store
        .record_transition(transition(
            now_ms(),
            "join-0",
            PresenceHistoryEventKind::MemberAdded,
            "user-0",
        ))
        .await
        .unwrap();
    assert_eq!(
        inner
            .stream_inspection(
                "app",
                &DurablePresenceHistoryStore::durable_channel_name("presence-room")
            )
            .await
            .unwrap()
            .retained
            .retained_messages,
        1
    );
}

#[test]
fn positional_index_locates_user_rows_and_disables_for_merged_or_wide_ranges() {
    let mut cache = DurablePresenceTransitionCache::default();
    for serial in 1..=300_u64 {
        cache.insert(
            format!("dedupe-{serial}"),
            format!("user-{}", serial % 150),
            PresenceHistoryEventKind::MemberAdded,
            serial,
        );
    }
    assert_eq!(cache.membership.len(), 2);
    let first = &cache.membership[0];
    assert!(first.positional && first.width() <= TRANSITION_MEMBERSHIP_RANGE_ROWS);
    // user-10 appears at serials 10 and 160 inside the first range (1..=256).
    let positions = first.user_positions("user-10").unwrap();
    assert!(
        positions.contains(&10) && positions.contains(&160),
        "{positions:?}"
    );
    assert_eq!(positions.first(), Some(&160), "candidates are newest first");
    assert!(
        positions.len() <= 4,
        "tag collisions must stay rare: {positions:?}"
    );
    assert!(
        first.user_positions("user-never").unwrap().is_empty(),
        "an unseen user has no candidates"
    );
    // Inserting the same identity twice is idempotent.
    let occupied = first.positions.iter().filter(|slot| **slot != 0).count();
    cache.insert(
        "dedupe-10".into(),
        "user-10".into(),
        PresenceHistoryEventKind::MemberAdded,
        10,
    );
    assert_eq!(
        cache.membership[0]
            .positions
            .iter()
            .filter(|slot| **slot != 0)
            .count(),
        occupied
    );
    // A range widened past 256 rows can no longer answer positionally.
    let mut wide = TransitionMembershipRange::new(1_000, 1_000);
    wide.add_position("user-a", 1_000);
    wide.last = 1_300;
    wide.add_position("user-b", 1_300);
    assert!(!wide.positional && wide.user_positions("user-a").is_none());
    // Merging fills all slots and drops positional evidence.
    let mut merged = DurablePresenceTransitionCache::default();
    for serial in 1..40_000_u64 {
        merged.insert(
            format!("dedupe-{serial}"),
            format!("user-{serial}"),
            PresenceHistoryEventKind::MemberAdded,
            serial,
        );
    }
    assert!(merged.membership.iter().any(|range| !range.positional));
    for range in &merged.membership {
        if range.width() > TRANSITION_MEMBERSHIP_RANGE_ROWS {
            assert!(!range.positional);
        }
    }
    assert!(merged.accounted_bytes <= TRANSITION_CACHE_BYTES);
}

struct RowCountingHistory {
    inner: Arc<MemoryHistoryStore>,
    pages: AtomicUsize,
    rows: AtomicUsize,
}
#[async_trait]
impl HistoryStore for RowCountingHistory {
    async fn reserve_publish_position(&self, a: &str, c: &str) -> Result<HistoryWriteReservation> {
        self.inner.reserve_publish_position(a, c).await
    }
    async fn append(&self, r: HistoryAppendRecord) -> Result<()> {
        self.inner.append(r).await
    }
    async fn stream_inspection(&self, a: &str, c: &str) -> Result<HistoryStreamInspection> {
        self.inner.stream_inspection(a, c).await
    }
    async fn read_page(&self, r: HistoryReadRequest) -> Result<HistoryPage> {
        let page = self.inner.read_page(r).await?;
        self.pages.fetch_add(1, Ordering::SeqCst);
        self.rows.fetch_add(page.items.len(), Ordering::SeqCst);
        Ok(page)
    }
}

#[tokio::test]
async fn cold_distinct_user_lookups_read_single_rows_instead_of_ranges() {
    let inner = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
    let history = Arc::new(RowCountingHistory {
        inner: inner.clone(),
        pages: AtomicUsize::new(0),
        rows: AtomicUsize::new(0),
    });
    let store = DurablePresenceHistoryStore::new(history.clone(), None);
    for serial in 0..3000 {
        let mut record = transition(
            now_ms(),
            &format!("join-{serial}"),
            PresenceHistoryEventKind::MemberAdded,
            &format!("user-{serial}"),
        );
        record.retention.max_events_per_channel = Some(10_000);
        store.record_transition(record).await.unwrap();
    }
    let before_rows = history.rows.load(Ordering::SeqCst);
    let before_pages = history.pages.load(Ordering::SeqCst);
    // 100 distinct old users, each already a member: the transition is
    // suppressed as a duplicate state, and each lookup reads its own row plus
    // the one fresh tail row the previous call appended (none here).
    for user in 0..100 {
        let mut record = transition(
            now_ms(),
            &format!("cold-{user}"),
            PresenceHistoryEventKind::MemberAdded,
            &format!("user-{}", user * 17),
        );
        record.retention.max_events_per_channel = Some(10_000);
        store.record_transition(record).await.unwrap();
    }
    let rows = history.rows.load(Ordering::SeqCst) - before_rows;
    let pages = history.pages.load(Ordering::SeqCst) - before_pages;
    assert!(
        rows <= 150,
        "100 cold users must read about one row each, read {rows} rows over {pages} pages"
    );
    assert_eq!(
        inner
            .stream_inspection("app", "[presence-history]presence-room")
            .await
            .unwrap()
            .retained
            .retained_messages,
        3000,
        "already-present users do not append duplicate transitions"
    );
    // A member that really left is found positionally and re-added.
    let mut leave = transition(
        now_ms(),
        "leave-42",
        PresenceHistoryEventKind::MemberRemoved,
        "user-42",
    );
    leave.retention.max_events_per_channel = Some(10_000);
    store.record_transition(leave).await.unwrap();
    let before_rows = history.rows.load(Ordering::SeqCst);
    let mut rejoin = transition(
        now_ms(),
        "rejoin-42",
        PresenceHistoryEventKind::MemberAdded,
        "user-42",
    );
    rejoin.retention.max_events_per_channel = Some(10_000);
    store.record_transition(rejoin).await.unwrap();
    assert!(history.rows.load(Ordering::SeqCst) - before_rows <= 3);
    assert_eq!(
        inner
            .stream_inspection("app", "[presence-history]presence-room")
            .await
            .unwrap()
            .retained
            .retained_messages,
        3002
    );
}
