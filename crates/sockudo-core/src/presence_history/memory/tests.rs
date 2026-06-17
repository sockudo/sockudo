use super::*;
use crate::presence_history::PresenceSnapshotRequest;
use crate::presence_history::test_support::transition;

#[tokio::test]
async fn memory_presence_history_orders_oldest_and_newest_first() {
    let store = MemoryPresenceHistoryStore::new(Default::default());
    let base = now_ms();
    store
        .record_transition(transition(
            base,
            "join-1",
            PresenceHistoryEventKind::MemberAdded,
            "u1",
        ))
        .await
        .unwrap();
    store
        .record_transition(transition(
            base + 1,
            "leave-1",
            PresenceHistoryEventKind::MemberRemoved,
            "u1",
        ))
        .await
        .unwrap();

    let newest = store
        .read_page(PresenceHistoryReadRequest {
            app_id: "app".to_string(),
            channel: "presence-room".to_string(),
            direction: PresenceHistoryDirection::NewestFirst,
            limit: 10,
            cursor: None,
            bounds: PresenceHistoryQueryBounds::default(),
        })
        .await
        .unwrap();
    assert_eq!(newest.items.len(), 2);
    assert_eq!(newest.items[0].serial, 2);
    assert_eq!(newest.items[1].serial, 1);

    let oldest = store
        .read_page(PresenceHistoryReadRequest {
            app_id: "app".to_string(),
            channel: "presence-room".to_string(),
            direction: PresenceHistoryDirection::OldestFirst,
            limit: 10,
            cursor: None,
            bounds: PresenceHistoryQueryBounds::default(),
        })
        .await
        .unwrap();
    assert_eq!(oldest.items.len(), 2);
    assert_eq!(oldest.items[0].serial, 1);
    assert_eq!(oldest.items[1].serial, 2);
}

#[tokio::test]
async fn memory_presence_history_deduplicates_same_transition_key() {
    let store = MemoryPresenceHistoryStore::new(Default::default());
    let record = transition(
        now_ms(),
        "join-1",
        PresenceHistoryEventKind::MemberAdded,
        "u1",
    );
    store.record_transition(record.clone()).await.unwrap();
    store.record_transition(record).await.unwrap();

    let page = store
        .read_page(PresenceHistoryReadRequest {
            app_id: "app".to_string(),
            channel: "presence-room".to_string(),
            direction: PresenceHistoryDirection::OldestFirst,
            limit: 10,
            cursor: None,
            bounds: PresenceHistoryQueryBounds::default(),
        })
        .await
        .unwrap();

    assert_eq!(page.items.len(), 1);
    assert_eq!(page.retained.retained_events, 1);
}

#[tokio::test]
async fn memory_presence_history_applies_time_and_count_retention() {
    let store = MemoryPresenceHistoryStore::new(MemoryPresenceHistoryStoreConfig {
        retention_window: Duration::from_secs(5),
        max_events_per_channel: Some(2),
        max_bytes_per_channel: None,
        metrics: None,
    });

    let now = now_ms();
    let capped_retention = PresenceHistoryRetentionPolicy {
        retention_window_seconds: 5,
        max_events_per_channel: Some(2),
        max_bytes_per_channel: None,
    };

    let mut old = transition(
        now - 10_000,
        "join-1",
        PresenceHistoryEventKind::MemberAdded,
        "u1",
    );
    old.retention = capped_retention.clone();
    store.record_transition(old).await.unwrap();

    let mut newer = transition(
        now - 2_000,
        "join-2",
        PresenceHistoryEventKind::MemberAdded,
        "u2",
    );
    newer.retention = capped_retention.clone();
    store.record_transition(newer).await.unwrap();

    let mut newest = transition(
        now - 1_000,
        "join-3",
        PresenceHistoryEventKind::MemberAdded,
        "u3",
    );
    newest.retention = capped_retention;
    store.record_transition(newest).await.unwrap();

    let page = store
        .read_page(PresenceHistoryReadRequest {
            app_id: "app".to_string(),
            channel: "presence-room".to_string(),
            direction: PresenceHistoryDirection::OldestFirst,
            limit: 10,
            cursor: None,
            bounds: PresenceHistoryQueryBounds::default(),
        })
        .await
        .unwrap();

    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].user_id, "u2");
    assert_eq!(page.items[1].user_id, "u3");
}

#[tokio::test]
async fn memory_presence_history_suppresses_consecutive_duplicate_user_transitions() {
    let store = MemoryPresenceHistoryStore::new(Default::default());
    let base = now_ms();

    let mut first_join = transition(
        base,
        "join-node-a",
        PresenceHistoryEventKind::MemberAdded,
        "u1",
    );
    first_join.connection_id = Some("socket-a".to_string());
    store.record_transition(first_join).await.unwrap();

    let mut duplicate_join = transition(
        base + 1,
        "join-node-b",
        PresenceHistoryEventKind::MemberAdded,
        "u1",
    );
    duplicate_join.connection_id = Some("socket-b".to_string());
    store.record_transition(duplicate_join).await.unwrap();

    let mut first_leave = transition(
        base + 2,
        "leave-disconnect",
        PresenceHistoryEventKind::MemberRemoved,
        "u1",
    );
    first_leave.connection_id = Some("socket-a".to_string());
    store.record_transition(first_leave).await.unwrap();

    let mut duplicate_leave = transition(
        base + 3,
        "leave-orphan-cleanup",
        PresenceHistoryEventKind::MemberRemoved,
        "u1",
    );
    duplicate_leave.connection_id = None;
    duplicate_leave.cause = PresenceHistoryEventCause::OrphanCleanup;
    duplicate_leave.dead_node_id = Some("dead-node".to_string());
    store.record_transition(duplicate_leave).await.unwrap();

    let page = store
        .read_page(PresenceHistoryReadRequest {
            app_id: "app".to_string(),
            channel: "presence-room".to_string(),
            direction: PresenceHistoryDirection::OldestFirst,
            limit: 10,
            cursor: None,
            bounds: PresenceHistoryQueryBounds::default(),
        })
        .await
        .unwrap();

    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
    assert_eq!(page.items[1].event, PresenceHistoryEventKind::MemberRemoved);
    assert_eq!(page.items[1].cause, PresenceHistoryEventCause::Disconnect);
}

// --- Prompt 13: Chaos, load, and failover tests ---

#[tokio::test]
async fn rapid_join_leave_churn_maintains_correct_count() {
    let store = MemoryPresenceHistoryStore::new(Default::default());
    let base = now_ms();
    let num_users = 100u64;

    // All users join
    for i in 0..num_users {
        store
            .record_transition(PresenceHistoryTransitionRecord {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                event_kind: PresenceHistoryEventKind::MemberAdded,
                cause: PresenceHistoryEventCause::Join,
                user_id: format!("u{i}"),
                connection_id: Some(format!("s{i}")),
                user_info: None,
                dead_node_id: None,
                dedupe_key: format!("join-{i}"),
                published_at_ms: base + i as i64,
                retention: PresenceHistoryRetentionPolicy {
                    retention_window_seconds: 3600,
                    max_events_per_channel: None,
                    max_bytes_per_channel: None,
                },
            })
            .await
            .unwrap();
    }

    // Half the users leave
    for i in 0..num_users / 2 {
        store
            .record_transition(PresenceHistoryTransitionRecord {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                event_kind: PresenceHistoryEventKind::MemberRemoved,
                cause: PresenceHistoryEventCause::Disconnect,
                user_id: format!("u{i}"),
                connection_id: Some(format!("s{i}")),
                user_info: None,
                dead_node_id: None,
                dedupe_key: format!("leave-{i}"),
                published_at_ms: base + num_users as i64 + i as i64,
                retention: PresenceHistoryRetentionPolicy {
                    retention_window_seconds: 3600,
                    max_events_per_channel: None,
                    max_bytes_per_channel: None,
                },
            })
            .await
            .unwrap();
    }

    let snapshot = store
        .snapshot_at(PresenceSnapshotRequest {
            app_id: "app".to_string(),
            channel: "presence-room".to_string(),
            at_time_ms: None,
            at_serial: None,
        })
        .await
        .unwrap();

    assert_eq!(
        snapshot.members.len(),
        50,
        "50 users should remain after half left"
    );
    assert_eq!(snapshot.events_replayed, 150);
    assert!(snapshot.complete);
}

#[tokio::test]
async fn retention_eviction_under_count_cap_preserves_newest() {
    let store = MemoryPresenceHistoryStore::new(Default::default());
    let base = now_ms();

    // Write 200 events with cap of 50
    for i in 0..200u64 {
        store
            .record_transition(PresenceHistoryTransitionRecord {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                event_kind: if i % 2 == 0 {
                    PresenceHistoryEventKind::MemberAdded
                } else {
                    PresenceHistoryEventKind::MemberRemoved
                },
                cause: PresenceHistoryEventCause::Join,
                user_id: format!("u{i}"),
                connection_id: Some(format!("s{i}")),
                user_info: None,
                dead_node_id: None,
                dedupe_key: format!("evt-{i}"),
                published_at_ms: base + i as i64,
                retention: PresenceHistoryRetentionPolicy {
                    retention_window_seconds: 3600,
                    max_events_per_channel: Some(50),
                    max_bytes_per_channel: None,
                },
            })
            .await
            .unwrap();
    }

    let page = store
        .read_page(PresenceHistoryReadRequest {
            app_id: "app".to_string(),
            channel: "presence-room".to_string(),
            direction: PresenceHistoryDirection::NewestFirst,
            limit: 100,
            cursor: None,
            bounds: Default::default(),
        })
        .await
        .unwrap();

    assert!(page.items.len() <= 50, "should not exceed retention cap");
    // Newest event should still be present
    let newest_serial = page.items.first().map(|i| i.serial).unwrap_or(0);
    assert!(
        newest_serial >= 150,
        "newest events should survive eviction"
    );
    assert!(page.truncated_by_retention, "should flag truncation");
}

#[tokio::test]
async fn multinode_dedupe_collapses_fanout_duplicates() {
    let store = MemoryPresenceHistoryStore::new(Default::default());
    let base = now_ms();

    // Simulate 3 nodes publishing the same join with the same dedupe_key
    for node in 0..3 {
        store
            .record_transition(PresenceHistoryTransitionRecord {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                event_kind: PresenceHistoryEventKind::MemberAdded,
                cause: PresenceHistoryEventCause::Join,
                user_id: "u1".to_string(),
                connection_id: Some("s1".to_string()),
                user_info: None,
                dead_node_id: None,
                dedupe_key: "join-u1-same-key".to_string(),
                published_at_ms: base + node,
                retention: PresenceHistoryRetentionPolicy {
                    retention_window_seconds: 3600,
                    max_events_per_channel: None,
                    max_bytes_per_channel: None,
                },
            })
            .await
            .unwrap();
    }

    let page = store
        .read_page(PresenceHistoryReadRequest {
            app_id: "app".to_string(),
            channel: "presence-room".to_string(),
            direction: PresenceHistoryDirection::OldestFirst,
            limit: 10,
            cursor: None,
            bounds: Default::default(),
        })
        .await
        .unwrap();

    assert_eq!(
        page.items.len(),
        1,
        "dedupe should collapse 3 identical transitions to 1"
    );
}

#[tokio::test]
async fn orphan_cleanup_records_dead_node_id() {
    let store = MemoryPresenceHistoryStore::new(Default::default());
    let base = now_ms();

    // User joins normally
    store
        .record_transition(transition(
            base,
            "join-u1",
            PresenceHistoryEventKind::MemberAdded,
            "u1",
        ))
        .await
        .unwrap();

    // Node dies, orphan cleanup removes the user
    store
        .record_transition(PresenceHistoryTransitionRecord {
            app_id: "app".to_string(),
            channel: "presence-room".to_string(),
            event_kind: PresenceHistoryEventKind::MemberRemoved,
            cause: PresenceHistoryEventCause::OrphanCleanup,
            user_id: "u1".to_string(),
            connection_id: None,
            user_info: None,
            dead_node_id: Some("dead-node-abc".to_string()),
            dedupe_key: "orphan-u1".to_string(),
            published_at_ms: base + 1,
            retention: PresenceHistoryRetentionPolicy {
                retention_window_seconds: 3600,
                max_events_per_channel: None,
                max_bytes_per_channel: None,
            },
        })
        .await
        .unwrap();

    let page = store
        .read_page(PresenceHistoryReadRequest {
            app_id: "app".to_string(),
            channel: "presence-room".to_string(),
            direction: PresenceHistoryDirection::NewestFirst,
            limit: 10,
            cursor: None,
            bounds: Default::default(),
        })
        .await
        .unwrap();

    let orphan_event = &page.items[0];
    assert_eq!(orphan_event.cause, PresenceHistoryEventCause::OrphanCleanup);
    assert_eq!(orphan_event.dead_node_id.as_deref(), Some("dead-node-abc"));

    // Snapshot should show u1 as removed
    let snapshot = store
        .snapshot_at(PresenceSnapshotRequest {
            app_id: "app".to_string(),
            channel: "presence-room".to_string(),
            at_time_ms: None,
            at_serial: None,
        })
        .await
        .unwrap();

    assert_eq!(
        snapshot.members.len(),
        0,
        "orphan-cleaned user should not appear in snapshot"
    );
}

#[tokio::test]
async fn paging_through_large_retained_window_returns_all_events() {
    let store = MemoryPresenceHistoryStore::new(Default::default());
    let base = now_ms();

    // Write 500 events
    for i in 0..500u64 {
        store
            .record_transition(PresenceHistoryTransitionRecord {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                event_kind: if i % 2 == 0 {
                    PresenceHistoryEventKind::MemberAdded
                } else {
                    PresenceHistoryEventKind::MemberRemoved
                },
                cause: PresenceHistoryEventCause::Join,
                user_id: format!("u{i}"),
                connection_id: Some(format!("s{i}")),
                user_info: None,
                dead_node_id: None,
                dedupe_key: format!("evt-{i}"),
                published_at_ms: base + i as i64,
                retention: PresenceHistoryRetentionPolicy {
                    retention_window_seconds: 3600,
                    max_events_per_channel: None,
                    max_bytes_per_channel: None,
                },
            })
            .await
            .unwrap();
    }

    // Page through with page size 50
    let mut total_items = 0u64;
    let mut cursor: Option<PresenceHistoryCursor> = None;
    let mut pages = 0u64;

    loop {
        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 50,
                cursor: cursor.clone(),
                bounds: Default::default(),
            })
            .await
            .unwrap();

        total_items += page.items.len() as u64;
        pages += 1;

        if !page.has_more || page.next_cursor.is_none() {
            break;
        }
        cursor = page.next_cursor;

        // Safety valve
        if pages > 20 {
            panic!("too many pages, possible infinite loop");
        }
    }

    assert_eq!(total_items, 500, "paging should return all 500 events");
    assert_eq!(pages, 10, "500 events / 50 per page = 10 pages");
}

#[tokio::test]
async fn memory_presence_history_read_page_does_not_materialize_absent_channels() {
    let store = MemoryPresenceHistoryStore::new(MemoryPresenceHistoryStoreConfig::default());

    let page = store
        .read_page(PresenceHistoryReadRequest {
            app_id: "app".to_string(),
            channel: "missing".to_string(),
            direction: PresenceHistoryDirection::OldestFirst,
            limit: 10,
            cursor: None,
            bounds: PresenceHistoryQueryBounds::default(),
        })
        .await
        .unwrap();

    assert!(page.items.is_empty());
    assert_eq!(store.channels.read().await.len(), 0);
}
