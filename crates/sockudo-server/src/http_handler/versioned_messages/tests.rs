use super::*;
use crate::http_handler::test_support::*;
use crate::http_handler::{HistoryQuery, channel_history, events};
use axum::extract::RawQuery;
use axum::http::{HeaderMap, Uri};
use sockudo_core::auth::EventQuery;
use sockudo_core::version_store::{MemoryVersionStore, VersionStore};
use sockudo_protocol::messages::{ApiMessageData, MessageData, PusherApiMessage};
use sockudo_protocol::versioned_messages::{
    AppendMessageRequest, DeleteMessageRequest, UpdateMessageRequest,
};
use sonic_rs::{JsonContainerTrait, JsonValueTrait, json};

#[tokio::test]
async fn channel_message_returns_latest_versioned_item() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_versioned_handler_with_store(100, store.clone());
    let app = test_app();

    let original = test_versioned_record(
        "msg:1",
        "00000000000000000001:test:00000000000000000001",
        10,
        1,
        "hello",
    );
    store.append_version(original.clone()).await.unwrap();
    store
        .append_version(StoredVersionRecord {
            message: original
                .message
                .apply_mutation(
                    sockudo_core::versioned_messages::MessageAction::Update,
                    VersionMetadata {
                        serial: VersionSerial::new(
                            "00000000000000000002:test:00000000000000000002".to_string(),
                        )
                        .unwrap(),
                        client_id: Some("user-1".to_string()),
                        timestamp_ms: 2,
                        description: Some("patched".to_string()),
                        metadata: None,
                    },
                    2,
                    sockudo_core::versioned_messages::MessageFieldDelta {
                        data: sockudo_core::versioned_messages::FieldPatch::Replace(
                            MessageData::String("{\"text\":\"patched\"}".to_string()),
                        ),
                        ..Default::default()
                    },
                )
                .unwrap(),
            ..original
        })
        .await
        .unwrap();

    let response = channel_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
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
    assert_eq!(json["item"]["message_serial"], "msg:1");
    assert_eq!(json["item"]["action"], "update");
    assert_eq!(
        json["item"]["version"]["serial"],
        "00000000000000000002:test:00000000000000000002"
    );
    assert_eq!(json["item"]["event"], "sockudo:message.update");
}

#[tokio::test]
async fn events_create_then_update_substitutes_latest_visible_history() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_versioned_handler_with_store(100, store.clone());
    let app = test_app();

    let create_response = events(
        Path("app-1".to_string()),
        Query(EventQuery {
            auth_key: String::new(),
            auth_timestamp: String::new(),
            auth_version: String::new(),
            body_md5: String::new(),
            auth_signature: String::new(),
        }),
        Extension(app.clone()),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        State(handler.clone()),
        HeaderMap::new(),
        Uri::from_static("/apps/app-1/events"),
        RawQuery(None),
        Json(PusherApiMessage {
            name: Some("chat.message".to_string()),
            data: Some(ApiMessageData::Json(json!({"text": "hello"}))),
            channel: Some("versioned-room".to_string()),
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

    assert_eq!(create_response.status(), StatusCode::OK);

    let latest = handler
        .version_store()
        .latest_by_history("app-1", "versioned-room")
        .await
        .unwrap();
    assert_eq!(latest.len(), 1);
    let message_serial = latest[0].message_serial().as_str().to_string();

    let update_response = update_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: message_serial.clone(),
        }),
        Extension(app.clone()),
        State(handler.clone()),
        Json(UpdateMessageRequest {
            name: None,
            data: Some(MessageData::String("{\"text\":\"patched\"}".to_string())),
            extras: None,
            clear_fields: Vec::new(),
            client_id: None,
            socket_id: None,
            description: Some("patch".to_string()),
            metadata: None,
            op_id: None,
        }),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(update_response.status(), StatusCode::OK);

    let history_response = channel_history(
        Path(("app-1".to_string(), "versioned-room".to_string())),
        Query(HistoryQuery {
            limit: Some(10),
            direction: Some("oldest_first".to_string()),
            ..Default::default()
        }),
        Extension(app),
        State(handler),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(history_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(history_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(json["items"].as_array().unwrap().len(), 1);
    assert_eq!(json["items"][0]["serial"], 1);
    assert_eq!(
        json["items"][0]["message"]["message_serial"],
        message_serial
    );
    assert_eq!(json["items"][0]["message"]["action"], "update");
    assert_eq!(
        json["items"][0]["message"]["data"],
        "{\"text\":\"patched\"}"
    );
    assert_eq!(json["items"][0]["event_name"], "sockudo:message.update");
    assert_eq!(json["items"][0]["operation_kind"], "message.update");
}

#[tokio::test]
async fn events_create_then_delete_substitutes_latest_visible_history() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_versioned_handler_with_store(100, store.clone());
    let app = test_app();

    let create_response = events(
        Path("app-1".to_string()),
        Query(EventQuery {
            auth_key: String::new(),
            auth_timestamp: String::new(),
            auth_version: String::new(),
            body_md5: String::new(),
            auth_signature: String::new(),
        }),
        Extension(app.clone()),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        State(handler.clone()),
        HeaderMap::new(),
        Uri::from_static("/apps/app-1/events"),
        RawQuery(None),
        Json(PusherApiMessage {
            name: Some("chat.message".to_string()),
            data: Some(ApiMessageData::Json(json!({"text": "hello"}))),
            channel: Some("versioned-room".to_string()),
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

    assert_eq!(create_response.status(), StatusCode::OK);

    let latest = handler
        .version_store()
        .latest_by_history("app-1", "versioned-room")
        .await
        .unwrap();
    let message_serial = latest[0].message_serial().as_str().to_string();

    let delete_response = delete_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: message_serial.clone(),
        }),
        Extension(app.clone()),
        State(handler.clone()),
        Json(DeleteMessageRequest {
            data: None,
            extras: None,
            clear_fields: vec![
                sockudo_protocol::versioned_messages::ClearField::Data,
                sockudo_protocol::versioned_messages::ClearField::Extras,
            ],
            client_id: None,
            socket_id: None,
            description: Some("deleted".to_string()),
            metadata: None,
            op_id: None,
        }),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(delete_response.status(), StatusCode::OK);

    let history_response = channel_history(
        Path(("app-1".to_string(), "versioned-room".to_string())),
        Query(HistoryQuery {
            limit: Some(10),
            direction: Some("oldest_first".to_string()),
            ..Default::default()
        }),
        Extension(app),
        State(handler),
    )
    .await
    .unwrap()
    .into_response();

    let body = axum::body::to_bytes(history_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(json["items"][0]["serial"], 1);
    assert_eq!(
        json["items"][0]["message"]["message_serial"],
        message_serial
    );
    assert_eq!(json["items"][0]["message"]["action"], "delete");
    assert!(json["items"][0]["message"]["data"].is_null());
    assert_eq!(json["items"][0]["event_name"], "sockudo:message.delete");
    assert_eq!(json["items"][0]["operation_kind"], "message.delete");
}

#[tokio::test]
async fn events_create_then_append_substitutes_latest_visible_history() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_versioned_handler_with_store(100, store.clone());
    let app = test_app();

    let create_response = events(
        Path("app-1".to_string()),
        Query(EventQuery {
            auth_key: String::new(),
            auth_timestamp: String::new(),
            auth_version: String::new(),
            body_md5: String::new(),
            auth_signature: String::new(),
        }),
        Extension(app.clone()),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        State(handler.clone()),
        HeaderMap::new(),
        Uri::from_static("/apps/app-1/events"),
        RawQuery(None),
        Json(PusherApiMessage {
            name: Some("chat.message".to_string()),
            data: Some(ApiMessageData::String("hello".to_string())),
            channel: Some("versioned-room".to_string()),
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

    assert_eq!(create_response.status(), StatusCode::OK);

    let latest = handler
        .version_store()
        .latest_by_history("app-1", "versioned-room")
        .await
        .unwrap();
    let message_serial = latest[0].message_serial().as_str().to_string();

    let append_response = append_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: message_serial.clone(),
        }),
        Extension(app.clone()),
        State(handler.clone()),
        Json(AppendMessageRequest {
            data: " world".to_string(),
            extras: None,
            client_id: None,
            socket_id: None,
            description: Some("append".to_string()),
            metadata: None,
            op_id: None,
        }),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(append_response.status(), StatusCode::OK);

    let history_response = channel_history(
        Path(("app-1".to_string(), "versioned-room".to_string())),
        Query(HistoryQuery {
            limit: Some(10),
            direction: Some("oldest_first".to_string()),
            ..Default::default()
        }),
        Extension(app),
        State(handler),
    )
    .await
    .unwrap()
    .into_response();

    let body = axum::body::to_bytes(history_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(json["items"][0]["serial"], 1);
    assert_eq!(
        json["items"][0]["message"]["message_serial"],
        message_serial
    );
    assert_eq!(json["items"][0]["message"]["action"], "append");
    assert_eq!(json["items"][0]["message"]["data"], "hello world");
    assert_eq!(json["items"][0]["event_name"], "sockudo:message.append");
    assert_eq!(json["items"][0]["operation_kind"], "message.append");
}

#[tokio::test]
async fn channel_message_versions_pages_newest_first() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_versioned_handler_with_store(2, store.clone());
    let app = test_app();

    let original = test_versioned_record(
        "msg:1",
        "00000000000000000001:test:00000000000000000001",
        10,
        1,
        "hello",
    );
    let update_1 = StoredVersionRecord {
        message: original
            .message
            .apply_mutation(
                sockudo_core::versioned_messages::MessageAction::Update,
                VersionMetadata {
                    serial: VersionSerial::new(
                        "00000000000000000002:test:00000000000000000002".to_string(),
                    )
                    .unwrap(),
                    client_id: Some("user-1".to_string()),
                    timestamp_ms: 2,
                    description: None,
                    metadata: None,
                },
                2,
                sockudo_core::versioned_messages::MessageFieldDelta::default(),
            )
            .unwrap(),
        ..original.clone()
    };
    let update_2 = StoredVersionRecord {
        message: update_1
            .message
            .apply_mutation(
                sockudo_core::versioned_messages::MessageAction::Delete,
                VersionMetadata {
                    serial: VersionSerial::new(
                        "00000000000000000003:test:00000000000000000003".to_string(),
                    )
                    .unwrap(),
                    client_id: Some("user-1".to_string()),
                    timestamp_ms: 3,
                    description: None,
                    metadata: None,
                },
                3,
                sockudo_core::versioned_messages::MessageFieldDelta::default(),
            )
            .unwrap(),
        ..original.clone()
    };

    store.append_version(original).await.unwrap();
    store.append_version(update_1).await.unwrap();
    store.append_version(update_2).await.unwrap();

    let response = channel_message_versions(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Query(MessageVersionsQuery {
            limit: Some(2),
            direction: Some(VersionDirection::NewestFirst),
            cursor: None,
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
    assert_eq!(
        json["items"][0]["version"]["serial"],
        "00000000000000000003:test:00000000000000000003"
    );
    assert_eq!(
        json["items"][1]["version"]["serial"],
        "00000000000000000002:test:00000000000000000002"
    );
    assert_eq!(json["direction"], "newest_first");
    assert!(json["has_more"].as_bool().unwrap());
    assert_eq!(
        json["next_cursor"],
        "00000000000000000002:test:00000000000000000002"
    );
}

#[tokio::test]
async fn versioned_message_endpoints_require_feature_flag() {
    let handler = test_history_handler(100);
    let app = test_app();

    let response = match channel_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app),
        State(handler),
    )
    .await
    {
        Err(err) => err.into_response(),
        Ok(_) => panic!("expected feature-disabled error"),
    };

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}
