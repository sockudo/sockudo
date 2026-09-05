use super::super::constants::*;
use super::*;
use crate::domain::ChannelSubscription;
use crate::storage::PushSubscriptionStore;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn automatic_backfill_is_bounded_resumable_and_never_returns_partial_complete_pages() {
    let store = test_store();
    for index in 0..600 {
        let subscription =
            ChannelSubscription::from_client("app", "wide", format!("client-{index:04}"));
        store
            .backend
            .put(
                FAMILY_SUBSCRIPTION,
                "app",
                "wide",
                &subscription.device_id,
                to_json_string(&subscription).unwrap(),
            )
            .await
            .unwrap();
    }
    let mut attempts = 0;
    let first = loop {
        attempts += 1;
        assert!(attempts < 10);
        let restarted = DocumentPushStore::with_backend(store.backend.clone());
        store.backend.page_rows.store(0, Ordering::Relaxed);
        let result = restarted.list_subscriptions("app", 7, None).await;
        assert!(store.backend.page_rows.load(Ordering::Relaxed) <= 264);
        if let Ok(page) = result {
            break page;
        }
    };
    assert!(attempts >= 3);
    assert_eq!(first.items.len(), 7);
    let mut ids = first
        .items
        .into_iter()
        .map(|row| row.device_id)
        .collect::<std::collections::BTreeSet<_>>();
    let mut cursor = first.next_cursor;
    while cursor.is_some() {
        store.backend.page_rows.store(0, Ordering::Relaxed);
        let page = store.list_subscriptions("app", 7, cursor).await.unwrap();
        assert!(store.backend.page_rows.load(Ordering::Relaxed) <= 8);
        for row in page.items {
            assert!(ids.insert(row.device_id));
        }
        cursor = page.next_cursor;
    }
    assert_eq!(ids.len(), 600);
    assert_eq!(store.backend.full_scans.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn ordered_cursor_preserves_distinct_logs_with_the_same_timestamp_and_event_id() {
    use crate::domain::{FanoutRegime, PublishIntent, PublishLogEvent, PushPayload};
    use crate::storage::PushPublishLogStore;
    let store = test_store();
    for index in 0..7 {
        let publish_id = format!("publish-{index}");
        let intent = PublishIntent {
            app_id: "app".to_owned(),
            publish_id: publish_id.clone(),
            targets: Vec::new(),
            payload: PushPayload {
                template_id: None,
                template_data: sonic_rs::json!({}),
                title: None,
                body: None,
                icon: None,
                sound: None,
                collapse_key: None,
            },
            provider_overrides: Vec::new(),
            not_before_ms: None,
            expires_at_ms: None,
        };
        store
            .create_publish_status_if_absent(crate::domain::PublishStatus {
                app_id: "app".to_owned(),
                publish_id: publish_id.clone(),
                state: crate::domain::PublishLifecycleState::Queued,
                counters: Default::default(),
                fanout_regime: None,
                retry_after_ms: None,
                error_reason: None,
            })
            .await
            .unwrap();
        store
            .append_publish_log_event(PublishLogEvent {
                app_id: "app".to_owned(),
                publish_id,
                event_id: "accepted".to_owned(),
                occurred_at_ms: 42,
                intent,
                fanout_regime: FanoutRegime::FastPath,
                expected_recipients: 0,
                fast_threshold: 1,
                shard_size: 1,
            })
            .await
            .unwrap();
    }
    let mut cursor = None;
    let mut seen = std::collections::BTreeSet::new();
    loop {
        let page = store
            .list_publish_log_events("app", 2, cursor)
            .await
            .unwrap();
        assert!(page.items.len() <= 2);
        for event in page.items {
            assert!(seen.insert(event.publish_id));
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen.len(), 7);
    assert_eq!(store.backend.full_scans.load(Ordering::Relaxed), 0);
}
