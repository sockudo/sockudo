use super::*;
use crate::http_handler::test_support::*;
use sockudo_adapter::ConnectionHandlerBuilder;
use sockudo_adapter::local_adapter::LocalAdapter;
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_cache::memory_cache_manager::MemoryCacheManager;
use sockudo_core::app::AppManager;
use sockudo_core::options::MemoryCacheOptions;
use sockudo_core::presence_history::{
    MemoryPresenceHistoryStore, PresenceHistoryDurableState, PresenceHistoryEventCause,
    PresenceHistoryEventKind, PresenceHistoryRetentionPolicy, PresenceHistoryStore,
    PresenceHistoryTransitionRecord, TrackingPresenceHistoryStore,
};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};

#[tokio::test]
async fn channel_presence_history_state_endpoint_returns_authoritative_stream_state() {
    let inner = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
    seed_presence_history(
        &inner,
        "app-1",
        "presence-room",
        2,
        sockudo_core::history::now_ms(),
    )
    .await;
    let stateful_store = Arc::new(InspectablePresenceHistoryStore {
        inner,
        state: PresenceHistoryStreamRuntimeState {
            app_id: "app-1".to_string(),
            channel: "presence-room".to_string(),
            stream_id: Some("presence-stream-9".to_string()),
            durable_state: PresenceHistoryDurableState::ResetRequired,
            continuity_proven: false,
            reset_required: true,
            reason: Some("presence_history_reset_required_after_write_failure".to_string()),
            node_id: Some("node-a".to_string()),
            last_transition_at_ms: Some(4321),
            authoritative_source: "test_state".to_string(),
            observed_source: "test_state".to_string(),
        },
    });
    let handler = test_presence_history_handler_with_store(100, stateful_store);
    let app = test_app();

    let response = channel_presence_history_state(
        Path(("app-1".to_string(), "presence-room".to_string())),
        Extension(app),
        State(handler),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();

    assert_eq!(
        json["stream"]["state"]["durable_state"].as_str(),
        Some("reset_required")
    );
    assert_eq!(
        json["stream"]["state"]["continuity_proven"].as_bool(),
        Some(false)
    );
    assert_eq!(
        json["stream"]["state"]["reset_required"].as_bool(),
        Some(true)
    );
    assert_eq!(
        json["stream"]["state"]["reason"].as_str(),
        Some("presence_history_reset_required_after_write_failure")
    );
    assert_eq!(json["stream"]["next_serial"].as_u64(), Some(10));
}

#[tokio::test]
async fn channel_presence_history_reports_degraded_stream_fail_closed() {
    let inner = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
    seed_presence_history(
        &inner,
        "app-1",
        "presence-room",
        2,
        sockudo_core::history::now_ms(),
    )
    .await;
    let stateful_store = Arc::new(InspectablePresenceHistoryStore {
        inner,
        state: PresenceHistoryStreamRuntimeState {
            app_id: "app-1".to_string(),
            channel: "presence-room".to_string(),
            stream_id: Some("presence-stream-9".to_string()),
            durable_state: PresenceHistoryDurableState::Degraded,
            continuity_proven: false,
            reset_required: false,
            reason: Some("presence_history_write_failed".to_string()),
            node_id: Some("node-a".to_string()),
            last_transition_at_ms: Some(1234),
            authoritative_source: "test_state".to_string(),
            observed_source: "test_state".to_string(),
        },
    });
    let handler = test_presence_history_handler_with_store(100, stateful_store);
    let app = test_app();

    let response = channel_presence_history(
        Path(("app-1".to_string(), "presence-room".to_string())),
        Query(HistoryQuery {
            limit: Some(10),
            direction: Some("newest_first".to_string()),
            cursor: None,
            start_serial: None,
            end_serial: None,
            start_time_ms: None,
            end_time_ms: None,
            ..Default::default()
        }),
        Extension(app),
        State(handler),
    )
    .await
    .unwrap()
    .into_response();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(json["continuity"]["degraded"].as_bool(), Some(true));
    assert_eq!(json["continuity"]["complete"].as_bool(), Some(false));
    assert_eq!(
        json["stream_state"]["durable_state"].as_str(),
        Some("degraded")
    );
    assert_eq!(
        json["stream_state"]["continuity_proven"].as_bool(),
        Some(false)
    );
}

#[tokio::test]
async fn channel_presence_history_reset_rotates_stream_and_purges_history() {
    let inner = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
    seed_presence_history(
        &inner,
        "app-1",
        "presence-room",
        2,
        sockudo_core::history::now_ms(),
    )
    .await;
    let tracked = Arc::new(TrackingPresenceHistoryStore::new(
        inner.clone(),
        None,
        "in_memory",
    ));
    let handler = test_presence_history_handler_with_store(100, tracked.clone());
    let app = test_app();

    let before = tracked
        .stream_inspection("app-1", "presence-room")
        .await
        .unwrap();
    let previous_stream_id = before.stream_id.clone().unwrap();

    let response = channel_presence_history_reset(
        Path(("app-1".to_string(), "presence-room".to_string())),
        Extension(app),
        State(handler),
        Json(HistoryResetRequestBody {
            confirm_channel: "presence-room".to_string(),
            confirm_operation: "reset".to_string(),
            reason: "operator cleanup".to_string(),
            requested_by: Some("ops".to_string()),
        }),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(json["operation"].as_str(), Some("reset"));
    assert_eq!(
        json["previous_stream_id"].as_str(),
        Some(previous_stream_id.as_str())
    );
    assert_ne!(
        json["new_stream_id"].as_str(),
        Some(previous_stream_id.as_str())
    );
    assert_eq!(json["purged_events"].as_u64(), Some(2));
    assert_eq!(
        json["stream"]["retained"]["retained_events"].as_u64(),
        Some(0)
    );
    assert_eq!(
        json["stream"]["state"]["durable_state"].as_str(),
        Some("healthy")
    );
}

#[tokio::test]
async fn dead_node_cleanup_replay_does_not_duplicate_presence_history_rows() {
    let app = test_app();
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(app.clone()).await.unwrap();
    let adapter =
        Arc::new(LocalAdapter::new()) as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
    let cache = Arc::new(MemoryCacheManager::new(
        "test".to_string(),
        MemoryCacheOptions::default(),
    ));
    let store = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));

    let mut options = sockudo_core::options::ServerOptions::default();
    options.presence_history.enabled = true;

    let handler = Arc::new(
        ConnectionHandlerBuilder::new(app_manager, adapter, cache, options)
            .presence_history_store(store.clone())
            .build(),
    );

    store
        .record_transition(PresenceHistoryTransitionRecord {
            app_id: "app-1".to_string(),
            channel: "presence-room".to_string(),
            event_kind: PresenceHistoryEventKind::MemberAdded,
            cause: PresenceHistoryEventCause::Join,
            user_id: "user-1".to_string(),
            connection_id: Some("socket-1".to_string()),
            user_info: Some(sonic_rs::json!({ "name": "Ada" })),
            dead_node_id: None,
            dedupe_key: "join-1".to_string(),
            published_at_ms: sockudo_core::history::now_ms(),
            retention: PresenceHistoryRetentionPolicy {
                retention_window_seconds: 3600,
                max_events_per_channel: None,
                max_bytes_per_channel: None,
            },
        })
        .await
        .unwrap();

    let cleanup_event = sockudo_adapter::horizontal_adapter::DeadNodeEvent {
        dead_node_id: "dead-node".to_string(),
        orphaned_members: vec![sockudo_adapter::horizontal_adapter::OrphanedMember {
            app_id: "app-1".to_string(),
            channel: "presence-room".to_string(),
            user_id: "user-1".to_string(),
            user_info: Some(sonic_rs::json!({ "name": "Ada" })),
        }],
    };

    handler
        .handle_dead_node_cleanup(cleanup_event.clone())
        .await
        .unwrap();
    handler
        .handle_dead_node_cleanup(cleanup_event)
        .await
        .unwrap();

    let page = store
        .read_page(PresenceHistoryReadRequest {
            app_id: "app-1".to_string(),
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
    assert_eq!(
        page.items[1].cause,
        PresenceHistoryEventCause::OrphanCleanup
    );
}

#[tokio::test]
async fn channel_presence_history_returns_presence_events() {
    let (handler, store) = test_presence_history_handler_with_memory_store(100);
    let app = test_app();
    let base_ts = sockudo_core::history::now_ms();
    seed_presence_history(&store, "app-1", "presence-room", 2, base_ts).await;

    let response = channel_presence_history(
        Path(("app-1".to_string(), "presence-room".to_string())),
        Query(HistoryQuery {
            limit: Some(10),
            direction: Some("newest_first".to_string()),
            cursor: None,
            start_serial: None,
            end_serial: None,
            start_time_ms: None,
            end_time_ms: None,
            ..Default::default()
        }),
        Extension(app),
        State(handler),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(json["items"].as_array().unwrap().len(), 2);
    assert_eq!(json["items"][0]["event"].as_str(), Some("member_removed"));
    assert_eq!(
        json["items"][0]["presence_event"]["user_id"].as_str(),
        Some("user-2")
    );
    assert_eq!(
        json["continuity"]["stream_id"]
            .as_str()
            .map(|value| !value.is_empty()),
        Some(true)
    );
    assert_eq!(json["continuity"]["retained_events"].as_u64(), Some(2));
}

#[tokio::test]
async fn channel_presence_history_paginates_newest_first_and_oldest_first() {
    let (handler, store) = test_presence_history_handler_with_memory_store(2);
    let app = test_app();
    let base_ts = sockudo_core::history::now_ms();
    seed_presence_history(&store, "app-1", "presence-room", 4, base_ts).await;

    let newest = channel_presence_history(
        Path(("app-1".to_string(), "presence-room".to_string())),
        Query(HistoryQuery {
            limit: Some(2),
            direction: Some("newest_first".to_string()),
            cursor: None,
            start_serial: None,
            end_serial: None,
            start_time_ms: None,
            end_time_ms: None,
            ..Default::default()
        }),
        Extension(app.clone()),
        State(handler.clone()),
    )
    .await
    .unwrap()
    .into_response();
    let newest_json: Value = sonic_rs::from_slice(
        &axum::body::to_bytes(newest.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(newest_json["items"][0]["serial"].as_u64(), Some(4));
    assert_eq!(newest_json["items"][1]["serial"].as_u64(), Some(3));
    let newest_cursor = newest_json["next_cursor"].as_str().unwrap().to_string();

    let newest_page_2 = channel_presence_history(
        Path(("app-1".to_string(), "presence-room".to_string())),
        Query(HistoryQuery {
            limit: Some(2),
            direction: Some("newest_first".to_string()),
            cursor: Some(newest_cursor),
            start_serial: None,
            end_serial: None,
            start_time_ms: None,
            end_time_ms: None,
            ..Default::default()
        }),
        Extension(app.clone()),
        State(handler.clone()),
    )
    .await
    .unwrap()
    .into_response();
    let newest_page_2_json: Value = sonic_rs::from_slice(
        &axum::body::to_bytes(newest_page_2.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(newest_page_2_json["items"][0]["serial"].as_u64(), Some(2));
    assert_eq!(newest_page_2_json["items"][1]["serial"].as_u64(), Some(1));

    let oldest = channel_presence_history(
        Path(("app-1".to_string(), "presence-room".to_string())),
        Query(HistoryQuery {
            limit: Some(2),
            direction: Some("oldest_first".to_string()),
            cursor: None,
            start_serial: None,
            end_serial: None,
            start_time_ms: None,
            end_time_ms: None,
            ..Default::default()
        }),
        Extension(app),
        State(handler),
    )
    .await
    .unwrap()
    .into_response();
    let oldest_json: Value = sonic_rs::from_slice(
        &axum::body::to_bytes(oldest.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(oldest_json["items"][0]["serial"].as_u64(), Some(1));
    assert_eq!(oldest_json["items"][1]["serial"].as_u64(), Some(2));
}

#[tokio::test]
async fn channel_presence_history_filters_by_serial_and_time() {
    let (handler, store) = test_presence_history_handler_with_memory_store(100);
    let app = test_app();
    let base_ts = sockudo_core::history::now_ms();
    seed_presence_history(&store, "app-1", "presence-room", 5, base_ts).await;

    let response = channel_presence_history(
        Path(("app-1".to_string(), "presence-room".to_string())),
        Query(HistoryQuery {
            limit: Some(10),
            direction: Some("oldest_first".to_string()),
            cursor: None,
            start_serial: Some(2),
            end_serial: Some(4),
            start_time_ms: Some(base_ts + 2),
            end_time_ms: Some(base_ts + 4),
            ..Default::default()
        }),
        Extension(app),
        State(handler),
    )
    .await
    .unwrap()
    .into_response();
    let json: Value = sonic_rs::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();

    assert_eq!(json["items"].as_array().unwrap().len(), 3);
    assert_eq!(json["items"][0]["serial"].as_u64(), Some(2));
    assert_eq!(json["items"][2]["serial"].as_u64(), Some(4));
    assert_eq!(json["bounds"]["start_serial"].as_u64(), Some(2));
    assert_eq!(json["bounds"]["end_serial"].as_u64(), Some(4));
}

#[tokio::test]
async fn channel_presence_history_rejects_non_presence_channels() {
    let handler = test_presence_history_handler(100);
    let app = test_app();

    let response = channel_presence_history(
        Path(("app-1".to_string(), "public-room".to_string())),
        Query(HistoryQuery {
            limit: Some(10),
            direction: Some("newest_first".to_string()),
            cursor: None,
            start_serial: None,
            end_serial: None,
            start_time_ms: None,
            end_time_ms: None,
            ..Default::default()
        }),
        Extension(app),
        State(handler),
    )
    .await;

    let response = match response {
        Ok(_) => panic!("expected non-presence channel request to fail"),
        Err(err) => err.into_response(),
    };

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn channel_presence_history_rejects_cursor_with_mismatched_bounds() {
    let (handler, store) = test_presence_history_handler_with_memory_store(2);
    let app = test_app();
    let base_ts = sockudo_core::history::now_ms();
    seed_presence_history(&store, "app-1", "presence-room", 3, base_ts).await;

    let first_page = channel_presence_history(
        Path(("app-1".to_string(), "presence-room".to_string())),
        Query(HistoryQuery {
            limit: Some(2),
            direction: Some("newest_first".to_string()),
            cursor: None,
            start_serial: None,
            end_serial: None,
            start_time_ms: None,
            end_time_ms: None,
            ..Default::default()
        }),
        Extension(app.clone()),
        State(handler.clone()),
    )
    .await
    .unwrap()
    .into_response();
    let first_page_json: Value = sonic_rs::from_slice(
        &axum::body::to_bytes(first_page.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let cursor = first_page_json["next_cursor"].as_str().unwrap().to_string();

    let response = channel_presence_history(
        Path(("app-1".to_string(), "presence-room".to_string())),
        Query(HistoryQuery {
            limit: Some(2),
            direction: Some("newest_first".to_string()),
            cursor: Some(cursor),
            start_serial: Some(2),
            end_serial: None,
            start_time_ms: None,
            end_time_ms: None,
            ..Default::default()
        }),
        Extension(app),
        State(handler),
    )
    .await;

    let response = match response {
        Ok(_) => panic!("expected mismatched cursor bounds to fail"),
        Err(err) => err.into_response(),
    };

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn channel_presence_history_snapshot_reconstructs_membership() {
    let (handler, store) = test_presence_history_handler_with_memory_store(100);
    let app = test_app();
    let base_ts = sockudo_core::history::now_ms();

    // Seed: u1 joins, u2 joins, u1 leaves
    seed_presence_history(&store, "app-1", "presence-room", 3, base_ts).await;

    let response = channel_presence_history_snapshot(
        Path(("app-1".to_string(), "presence-room".to_string())),
        Query(PresenceSnapshotQuery::default()),
        Extension(app),
        State(handler),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 64)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(json["channel"].as_str(), Some("presence-room"));
    assert!(json["member_count"].as_u64().unwrap() > 0);
    assert!(json["events_replayed"].as_u64().unwrap() > 0);
    assert!(json["continuity"]["stream_id"].as_str().is_some());
}

#[tokio::test]
async fn channel_presence_history_snapshot_at_serial_bound() {
    let (handler, store) = test_presence_history_handler_with_memory_store(100);
    let app = test_app();
    let base_ts = sockudo_core::history::now_ms();

    // Seed: alternating joins/leaves for u1, u2
    seed_presence_history(&store, "app-1", "presence-room", 4, base_ts).await;

    // Snapshot at serial 2 only replays events with serial <= 2
    let response = channel_presence_history_snapshot(
        Path(("app-1".to_string(), "presence-room".to_string())),
        Query(PresenceSnapshotQuery {
            at_serial: Some(2),
            ..Default::default()
        }),
        Extension(app),
        State(handler),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 64)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(json["snapshot_serial"].as_u64(), Some(2));
}

#[tokio::test]
async fn channel_presence_history_snapshot_rejects_non_presence_channel() {
    let handler = test_presence_history_handler(100);
    let app = test_app();

    let response = channel_presence_history_snapshot(
        Path(("app-1".to_string(), "private-room".to_string())),
        Query(PresenceSnapshotQuery::default()),
        Extension(app),
        State(handler),
    )
    .await;

    let response = match response {
        Ok(_) => panic!("expected non-presence channel to fail"),
        Err(err) => err.into_response(),
    };
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn continuity_includes_degraded_field() {
    let (handler, store) = test_presence_history_handler_with_memory_store(100);
    let app = test_app();
    let base_ts = sockudo_core::history::now_ms();
    seed_presence_history(&store, "app-1", "presence-room", 3, base_ts).await;

    let response = channel_presence_history(
        Path(("app-1".to_string(), "presence-room".to_string())),
        Query(HistoryQuery {
            limit: Some(10),
            direction: Some("newest_first".to_string()),
            ..Default::default()
        }),
        Extension(app),
        State(handler),
    )
    .await
    .unwrap()
    .into_response();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert!(
        json["continuity"]["degraded"].is_boolean(),
        "continuity must include 'degraded' boolean field"
    );
}

#[tokio::test]
async fn ably_time_aliases_resolve_correctly() {
    let (handler, store) = test_presence_history_handler_with_memory_store(100);
    let app = test_app();
    let base_ts = sockudo_core::history::now_ms();
    seed_presence_history(&store, "app-1", "presence-room", 5, base_ts).await;

    // Use start/end (Ably aliases) instead of start_time_ms/end_time_ms
    let response = channel_presence_history(
        Path(("app-1".to_string(), "presence-room".to_string())),
        Query(HistoryQuery {
            limit: Some(10),
            direction: Some("oldest_first".to_string()),
            start: Some(base_ts + 2),
            end: Some(base_ts + 4),
            ..Default::default()
        }),
        Extension(app),
        State(handler),
    )
    .await
    .unwrap()
    .into_response();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    let items = json["items"].as_array().unwrap();
    // All returned items should fall within the time bounds
    for item in items {
        let ts = item["published_at_ms"].as_i64().unwrap();
        assert!(
            ts >= base_ts + 2 && ts <= base_ts + 4,
            "item at ts={ts} outside Ably alias bounds [{}, {}]",
            base_ts + 2,
            base_ts + 4
        );
    }
}
