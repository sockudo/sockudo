use super::*;
use crate::http_handler::test_support::*;
use sockudo_protocol::messages::{ApiMessageData, PusherApiMessage};
use sockudo_push::{
    ChannelSubscription, MemoryPushQueue, MemoryPushStore, PushDeviceStore, PushPublishLogStore,
    PushSubscriptionStore,
};

#[tokio::test]
async fn matching_channel_push_rule_accepts_existing_channel_subscription_fanout() {
    let handler = test_handler_with_push_rules(vec![sockudo_core::options::PushRuleConfig {
        channel_pattern: "notifications:*".to_string(),
        event_filter: vec!["agent-complete".to_string()],
        ..Default::default()
    }]);
    let app = test_notifications_app();
    let store = Arc::new(MemoryPushStore::new());
    let queue = Arc::new(MemoryPushQueue::new());
    let device = test_push_device("device-1");
    store.upsert_device(device.clone()).await.unwrap();
    store
        .upsert_subscription(ChannelSubscription::from_device(
            "notifications:user-1",
            &device,
        ))
        .await
        .unwrap();

    let response = events(
        Path("app-1".to_string()),
        Query(empty_event_query()),
        Extension(app),
        #[cfg(feature = "push")]
        Extension(store.clone() as sockudo_push::DynPushStore),
        #[cfg(feature = "push")]
        Extension(queue.clone() as sockudo_push::DynPushQueue),
        State(handler),
        HeaderMap::new(),
        Uri::from_static("/apps/app-1/events"),
        RawQuery(None),
        Json(PusherApiMessage {
            name: Some("agent-complete".to_string()),
            data: Some(ApiMessageData::Json(json!({
                "title": "Agent complete",
                "body": "Your answer is ready",
                "sessionId": "sess-1"
            }))),
            channel: Some("notifications:user-1".to_string()),
            channels: None,
            socket_id: None,
            info: None,
            tags: None,
            delta: None,
            idempotency_key: None,
            message_id: None,
            extras: None,
        }),
    )
    .await
    .unwrap()
    .into_response();
    assert_eq!(response.status(), StatusCode::OK);

    let mut log_event = None;
    for _ in 0..20 {
        let page = store
            .list_publish_log_events("app-1", 10, None)
            .await
            .unwrap();
        if let Some(event) = page.items.into_iter().next() {
            log_event = Some(event);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let log_event = log_event.expect("rule-triggered push publish log event");
    assert_eq!(log_event.expected_recipients, 1);
    assert_eq!(
        log_event.intent.targets,
        vec![sockudo_push::PublishTarget::Channel {
            channel: "notifications:user-1".to_string()
        }]
    );
    assert_eq!(
        log_event.intent.payload.title.as_deref(),
        Some("Agent complete")
    );
    assert_eq!(
        log_event.intent.payload.template_data["data"]["sessionId"],
        "sess-1"
    );
}

#[tokio::test]
async fn non_matching_channel_push_rule_does_not_enqueue_push() {
    let handler = test_handler_with_push_rules(vec![sockudo_core::options::PushRuleConfig {
        channel_pattern: "notifications:*".to_string(),
        event_filter: vec!["agent-complete".to_string()],
        ..Default::default()
    }]);
    let app = test_notifications_app();
    let store = Arc::new(MemoryPushStore::new());
    let queue = Arc::new(MemoryPushQueue::new());

    let response = events(
        Path("app-1".to_string()),
        Query(empty_event_query()),
        Extension(app),
        #[cfg(feature = "push")]
        Extension(store.clone() as sockudo_push::DynPushStore),
        #[cfg(feature = "push")]
        Extension(queue.clone() as sockudo_push::DynPushQueue),
        State(handler),
        HeaderMap::new(),
        Uri::from_static("/apps/app-1/events"),
        RawQuery(None),
        Json(PusherApiMessage {
            name: Some("agent-start".to_string()),
            data: Some(ApiMessageData::Json(json!({
                "title": "Agent started",
                "body": "Working",
                "sessionId": "sess-1"
            }))),
            channel: Some("notifications:user-1".to_string()),
            channels: None,
            socket_id: None,
            info: None,
            tags: None,
            delta: None,
            idempotency_key: None,
            message_id: None,
            extras: None,
        }),
    )
    .await
    .unwrap()
    .into_response();
    assert_eq!(response.status(), StatusCode::OK);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let page = store
        .list_publish_log_events("app-1", 10, None)
        .await
        .unwrap();
    assert!(page.items.is_empty());
}
