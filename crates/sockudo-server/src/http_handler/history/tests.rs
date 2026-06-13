use super::*;
use crate::http_handler::test_support::*;
use async_trait::async_trait;
use sockudo_core::app::{
    AppChannelsPolicy, AppHistoryConfig, AppPolicy, ChannelNamespace, NamespaceHistoryConfig,
};
use sockudo_core::history::{
    HistoryDurableState, HistoryPage, HistoryPurgeResult, HistoryReadRequest, HistoryResetResult,
    HistoryRuntimeStatus, HistoryStore, HistoryStreamInspection, HistoryStreamRuntimeState,
    HistoryWriteReservation, MemoryHistoryStore, MemoryHistoryStoreConfig,
};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use std::time::Instant;

#[derive(Clone)]
struct InspectableHistoryStore {
    inner: Arc<MemoryHistoryStore>,
    state: HistoryStreamRuntimeState,
}

#[async_trait]
impl HistoryStore for InspectableHistoryStore {
    async fn reserve_publish_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> sockudo_core::error::Result<HistoryWriteReservation> {
        self.inner.reserve_publish_position(app_id, channel).await
    }

    async fn append(
        &self,
        record: sockudo_core::history::HistoryAppendRecord,
    ) -> sockudo_core::error::Result<()> {
        self.inner.append(record).await
    }

    async fn read_page(
        &self,
        request: HistoryReadRequest,
    ) -> sockudo_core::error::Result<HistoryPage> {
        if !self.state.recovery_allowed {
            return Err(sockudo_core::error::Error::Internal(
                "history_stream_state_blocks_reads".to_string(),
            ));
        }
        self.inner.read_page(request).await
    }

    async fn runtime_status(&self) -> sockudo_core::error::Result<HistoryRuntimeStatus> {
        Ok(HistoryRuntimeStatus {
            enabled: true,
            backend: "memory".to_string(),
            state_authority: "test_state".to_string(),
            degraded_channels: usize::from(!self.state.recovery_allowed),
            reset_required_channels: usize::from(self.state.reset_required),
            queue_depth: 0,
        })
    }

    async fn stream_runtime_state(
        &self,
        _app_id: &str,
        _channel: &str,
    ) -> sockudo_core::error::Result<HistoryStreamRuntimeState> {
        Ok(self.state.clone())
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> sockudo_core::error::Result<HistoryStreamInspection> {
        Ok(HistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: self.state.stream_id.clone(),
            next_serial: Some(10),
            retained: self
                .inner
                .read_page(HistoryReadRequest {
                    app_id: app_id.to_string(),
                    channel: channel.to_string(),
                    direction: HistoryDirection::NewestFirst,
                    limit: 1,
                    cursor: None,
                    bounds: HistoryQueryBounds::default(),
                })
                .await
                .map(|page| page.retained)
                .unwrap_or_default(),
            state: self.state.clone(),
        })
    }

    async fn reset_stream(
        &self,
        app_id: &str,
        channel: &str,
        reason: &str,
        requested_by: Option<&str>,
    ) -> sockudo_core::error::Result<HistoryResetResult> {
        self.inner
            .reset_stream(app_id, channel, reason, requested_by)
            .await
    }

    async fn purge_stream(
        &self,
        app_id: &str,
        channel: &str,
        request: HistoryPurgeRequest,
    ) -> sockudo_core::error::Result<HistoryPurgeResult> {
        self.inner.purge_stream(app_id, channel, request).await
    }
}

#[tokio::test]
async fn channel_history_state_endpoint_returns_authoritative_stream_state() {
    let stateful_store = Arc::new(InspectableHistoryStore {
        inner: Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default())),
        state: HistoryStreamRuntimeState {
            app_id: "app-1".to_string(),
            channel: "public-room".to_string(),
            stream_id: Some("stream-9".to_string()),
            durable_state: HistoryDurableState::Degraded,
            recovery_allowed: false,
            reset_required: false,
            reason: Some("durable_history_write_failed".to_string()),
            node_id: Some("node-a".to_string()),
            last_transition_at_ms: Some(1234),
            authoritative_source: "durable_store".to_string(),
            observed_source: "shared_cache_hint".to_string(),
        },
    });
    let handler = test_history_handler_with_store(100, stateful_store);
    let app = test_annotation_app();

    let response = channel_history_state(
        Path(("app-1".to_string(), "public-room".to_string())),
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
        Some("degraded")
    );
    assert_eq!(
        json["stream"]["state"]["recovery_allowed"].as_bool(),
        Some(false)
    );
    assert_eq!(
        json["stream"]["state"]["authoritative_source"].as_str(),
        Some("durable_store")
    );
    assert_eq!(
        json["stream"]["state"]["observed_source"].as_str(),
        Some("shared_cache_hint")
    );
    assert_eq!(json["stream"]["next_serial"].as_u64(), Some(10));
}

#[tokio::test]
async fn channel_history_reset_rotates_stream_and_purges_history() {
    let (handler, store) = test_history_handler_with_memory_store(100);
    let app = test_app();

    handler
        .broadcast_to_channel(
            &app,
            "public-room",
            test_history_message("public-room", 1),
            None,
        )
        .await
        .unwrap();
    handler
        .broadcast_to_channel(
            &app,
            "public-room",
            test_history_message("public-room", 2),
            None,
        )
        .await
        .unwrap();

    let before = store
        .stream_inspection("app-1", "public-room")
        .await
        .unwrap();
    let previous_stream_id = before.stream_id.clone().unwrap();
    let response = channel_history_reset(
        Path(("app-1".to_string(), "public-room".to_string())),
        Extension(app.clone()),
        State(handler.clone()),
        Json(HistoryResetRequestBody {
            confirm_channel: "public-room".to_string(),
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
    assert_eq!(json["purged_messages"].as_u64(), Some(2));
    assert_eq!(
        json["stream"]["retained"]["retained_messages"].as_u64(),
        Some(0)
    );

    handler
        .broadcast_to_channel(
            &app,
            "public-room",
            test_history_message("public-room", 3),
            None,
        )
        .await
        .unwrap();
    let after = store
        .stream_inspection("app-1", "public-room")
        .await
        .unwrap();
    assert_ne!(
        after.stream_id.as_deref(),
        Some(previous_stream_id.as_str())
    );
    assert_eq!(after.next_serial, Some(2));
    assert_eq!(after.retained.retained_messages, 1);
}

#[tokio::test]
async fn channel_history_purge_before_serial_advances_retained_floor_without_rotation() {
    let (handler, store) = test_history_handler_with_memory_store(100);
    let app = test_app();

    for index in 1..=4 {
        handler
            .broadcast_to_channel(
                &app,
                "public-room",
                test_history_message("public-room", index),
                None,
            )
            .await
            .unwrap();
    }

    let before = store
        .stream_inspection("app-1", "public-room")
        .await
        .unwrap();
    let response = channel_history_purge(
        Path(("app-1".to_string(), "public-room".to_string())),
        Extension(app),
        State(handler),
        Json(HistoryPurgeRequestBody {
            confirm_channel: "public-room".to_string(),
            confirm_operation: "purge".to_string(),
            mode: HistoryPurgeMode::BeforeSerial,
            before_serial: Some(3),
            before_time_ms: None,
            reason: "trim old backlog".to_string(),
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
    assert_eq!(json["operation"].as_str(), Some("purge"));
    assert_eq!(json["mode"].as_str(), Some("before_serial"));
    assert_eq!(json["purged_messages"].as_u64(), Some(2));
    assert_eq!(
        json["stream"]["retained"]["oldest_available_serial"].as_u64(),
        Some(3)
    );

    let after = store
        .stream_inspection("app-1", "public-room")
        .await
        .unwrap();
    assert_eq!(after.stream_id, before.stream_id);
    assert_eq!(after.retained.oldest_serial, Some(3));
    assert_eq!(after.retained.newest_serial, Some(4));
    assert_eq!(after.next_serial, before.next_serial);
}

#[tokio::test]
async fn channel_history_reset_rejects_mismatched_confirmation() {
    let handler = test_history_handler(100);
    let app = test_app();

    let result = channel_history_reset(
        Path(("app-1".to_string(), "public-room".to_string())),
        Extension(app),
        State(handler),
        Json(HistoryResetRequestBody {
            confirm_channel: "wrong-room".to_string(),
            confirm_operation: "reset".to_string(),
            reason: "operator cleanup".to_string(),
            requested_by: None,
        }),
    )
    .await;

    match result {
        Ok(_) => panic!("expected mismatched confirmation to fail"),
        Err(AppError::InvalidInput(message)) => {
            assert!(message.contains("confirm_channel"));
        }
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn channel_history_returns_published_messages() {
    let handler = test_history_handler(100);
    let app = test_app();

    handler
        .broadcast_to_channel(
            &app,
            "public-room",
            test_history_message("public-room", 1),
            None,
        )
        .await
        .unwrap();

    let response = channel_history(
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
    .await
    .unwrap()
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(json["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        json["items"][0]["message"]["event"].as_str(),
        Some("message-created-1")
    );
    assert_eq!(json["items"][0]["serial"].as_u64(), Some(1));
    assert_eq!(json["has_more"].as_bool(), Some(false));
    assert_eq!(
        json["continuity"]["stream_id"]
            .as_str()
            .map(|value| !value.is_empty()),
        Some(true)
    );
}

#[tokio::test]
async fn channel_history_paginates_newest_first_and_oldest_first() {
    let handler = test_history_handler(2);
    let app = test_app();

    for index in 1..=4 {
        handler
            .broadcast_to_channel(
                &app,
                "public-room",
                test_history_message("public-room", index),
                None,
            )
            .await
            .unwrap();
    }

    let newest = channel_history(
        Path(("app-1".to_string(), "public-room".to_string())),
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

    let newest_page_2 = channel_history(
        Path(("app-1".to_string(), "public-room".to_string())),
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

    let oldest = channel_history(
        Path(("app-1".to_string(), "public-room".to_string())),
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
async fn channel_history_rejects_invalid_cursor() {
    let handler = test_history_handler(2);
    let app = test_app();

    handler
        .broadcast_to_channel(
            &app,
            "public-room",
            test_history_message("public-room", 1),
            None,
        )
        .await
        .unwrap();

    let result = channel_history(
        Path(("app-1".to_string(), "public-room".to_string())),
        Query(HistoryQuery {
            limit: Some(2),
            direction: Some("newest_first".to_string()),
            cursor: Some("not-a-valid-cursor".to_string()),
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

    let response = match result {
        Ok(_) => panic!("expected invalid cursor request to fail"),
        Err(err) => err.into_response(),
    };

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn channel_history_filters_by_serial_range() {
    let handler = test_history_handler(100);
    let app = test_app();

    for index in 1..=5 {
        handler
            .broadcast_to_channel(
                &app,
                "public-room",
                test_history_message("public-room", index),
                None,
            )
            .await
            .unwrap();
    }

    let response = channel_history(
        Path(("app-1".to_string(), "public-room".to_string())),
        Query(HistoryQuery {
            limit: Some(10),
            direction: Some("oldest_first".to_string()),
            cursor: None,
            start_serial: Some(2),
            end_serial: Some(4),
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
async fn channel_history_large_read_is_bounded_and_fast() {
    let handler = test_history_handler(100);
    let app = test_app();

    for index in 1..=250 {
        handler
            .broadcast_to_channel(
                &app,
                "public-room",
                test_history_message("public-room", index),
                None,
            )
            .await
            .unwrap();
    }

    let started = Instant::now();
    let response = channel_history(
        Path(("app-1".to_string(), "public-room".to_string())),
        Query(HistoryQuery {
            limit: Some(100),
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
    let elapsed = started.elapsed();
    let json: Value = sonic_rs::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();

    assert_eq!(json["items"].as_array().unwrap().len(), 100);
    assert_eq!(json["has_more"].as_bool(), Some(true));
    assert!(elapsed.as_secs_f64() < 2.0);
}

#[tokio::test]
async fn channel_history_rejects_when_history_is_disabled_by_app_policy() {
    let handler = test_history_handler(100);
    let app = test_app_with_policy(AppPolicy {
        history: Some(AppHistoryConfig {
            enabled: Some(false),
            rewind_enabled: None,
            retention_window_seconds: None,
            max_messages_per_channel: None,
            max_bytes_per_channel: None,
        }),
        ..Default::default()
    });

    let response = channel_history(
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
        Ok(_) => panic!("expected policy-disabled history request to fail"),
        Err(err) => err.into_response(),
    };

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn namespace_history_retention_override_beats_app_default() {
    let handler = test_history_handler(100);
    let app = test_app_with_policy(AppPolicy {
        history: Some(AppHistoryConfig {
            enabled: Some(true),
            rewind_enabled: None,
            retention_window_seconds: None,
            max_messages_per_channel: Some(1),
            max_bytes_per_channel: None,
        }),
        channels: AppChannelsPolicy {
            channel_namespaces: Some(vec![ChannelNamespace {
                name: "chat".to_string(),
                channel_name_pattern: None,
                max_channel_name_length: None,
                annotations_enabled: None,
                allow_user_limited_channels: None,
                allow_subscribe_for_client: None,
                allow_publish_for_client: None,
                allow_presence_for_client: None,
                history: Some(NamespaceHistoryConfig {
                    rewind_enabled: None,
                    retention_window_seconds: None,
                    max_messages_per_channel: Some(3),
                    max_bytes_per_channel: None,
                }),
                presence_history: None,
            }]),
            ..Default::default()
        },
        ..Default::default()
    });

    for index in 1..=3 {
        handler
            .broadcast_to_channel(
                &app,
                "public-room",
                test_history_message("public-room", index),
                None,
            )
            .await
            .unwrap();
        handler
            .broadcast_to_channel(
                &app,
                "chat:room-1",
                test_history_message("chat:room-1", index),
                None,
            )
            .await
            .unwrap();
    }

    let public_response = channel_history(
        Path(("app-1".to_string(), "public-room".to_string())),
        Query(HistoryQuery {
            limit: Some(10),
            direction: Some("oldest_first".to_string()),
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
    let public_json: Value = sonic_rs::from_slice(
        &axum::body::to_bytes(public_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();

    let namespaced_response = channel_history(
        Path(("app-1".to_string(), "chat:room-1".to_string())),
        Query(HistoryQuery {
            limit: Some(10),
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
    let namespaced_json: Value = sonic_rs::from_slice(
        &axum::body::to_bytes(namespaced_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();

    assert_eq!(public_json["items"].as_array().unwrap().len(), 1);
    assert_eq!(namespaced_json["items"].as_array().unwrap().len(), 3);
}
