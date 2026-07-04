use super::*;
use crate::http_handler::test_support::*;
use sockudo_core::version_store::{
    MemoryVersionStore, VersionStore, VersionStoreDirection, VersionStoreReadRequest,
};
use sockudo_core::versioned_messages::MessageSerial;
use sockudo_core::websocket::{ConnectionCapabilities, SocketId};
use sockudo_protocol::messages::MessageData;
use sonic_rs::{JsonValueTrait, Value};

#[tokio::test]
async fn append_message_op_id_replay_returns_duplicate_serial_ack() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_ai_versioned_handler_with_store(100, store.clone(), 1024, 4096, 1024);
    let app = test_app();

    store
        .append_version(test_versioned_record(
            "msg:1",
            "00000000000000000001:test:00000000000000000001",
            10,
            1,
            "hello",
        ))
        .await
        .unwrap();

    let request = AppendMessageRequest {
        data: " world".to_string(),
        extras: Some(ai_extras("complete")),
        client_id: None,
        socket_id: None,
        description: Some("append".to_string()),
        metadata: None,
        op_id: Some("append-op-1".to_string()),
    };

    let first = append_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app.clone()),
        State(handler.clone()),
        Json(request.clone()),
    )
    .await
    .unwrap()
    .into_response();
    assert_eq!(first.status(), StatusCode::OK);
    let first_json: Value = sonic_rs::from_slice(
        &axum::body::to_bytes(first.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(first_json["status"], "applied");
    assert_eq!(first_json["history_serial"].as_u64(), Some(10));
    assert_eq!(first_json["delivery_serial"].as_u64(), Some(2));

    let duplicate = append_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app),
        State(handler),
        Json(request),
    )
    .await
    .unwrap()
    .into_response();
    let duplicate_json: Value = sonic_rs::from_slice(
        &axum::body::to_bytes(duplicate.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();

    assert_eq!(duplicate_json["status"], "duplicate");
    assert_eq!(
        duplicate_json["version_serial"],
        first_json["version_serial"]
    );
    assert_eq!(
        duplicate_json["history_serial"],
        first_json["history_serial"]
    );
    assert_eq!(
        duplicate_json["delivery_serial"],
        first_json["delivery_serial"]
    );
    assert_eq!(
        store
            .get_versions(VersionStoreReadRequest {
                app_id: "app-1".to_string(),
                channel: "versioned-room".to_string(),
                message_serial: MessageSerial::new("msg:1").unwrap(),
                direction: VersionStoreDirection::OldestFirst,
                limit: 10,
                cursor: None,
            })
            .await
            .unwrap()
            .items
            .len(),
        2
    );
}

#[tokio::test]
async fn update_message_validates_request_body_before_runtime() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_versioned_handler_with_store(100, store);
    let app = test_app();

    let response = match update_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app),
        State(handler),
        Json(UpdateMessageRequest::default()),
    )
    .await
    {
        Err(err) => err.into_response(),
        Ok(_) => panic!("expected validation error"),
    };

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn update_message_denies_authenticated_socket_without_mutation_capability() {
    let store = Arc::new(MemoryVersionStore::new());
    let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store.clone());
    let app = test_app();

    store
        .append_version(test_versioned_record(
            "msg:1",
            "00000000000000000001:test:00000000000000000001",
            10,
            1,
            "hello",
        ))
        .await
        .unwrap();

    let socket_id = SocketId::from_string("11.11").unwrap();
    attach_signed_in_mutation_actor(
        &handler,
        app_manager.clone(),
        &app,
        socket_id,
        "user-2",
        ConnectionCapabilities::default(),
    )
    .await;

    let response = match update_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app),
        State(handler),
        Json(UpdateMessageRequest {
            name: None,
            data: Some(MessageData::String("{\"text\":\"blocked\"}".to_string())),
            extras: None,
            clear_fields: Vec::new(),
            client_id: Some("user-2".to_string()),
            socket_id: Some(socket_id.to_string()),
            description: Some("blocked".to_string()),
            metadata: None,
            op_id: None,
        }),
    )
    .await
    {
        Err(err) => err.into_response(),
        Ok(_) => panic!("expected capability check to fail"),
    };

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(json["ai_code"], 93002);
    assert_eq!(json["error"], "mutations not permitted on this channel");
}

#[tokio::test]
async fn update_message_allows_authenticated_owner_with_own_capability() {
    let store = Arc::new(MemoryVersionStore::new());
    let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store.clone());
    let app = test_app();

    store
        .append_version(test_versioned_record(
            "msg:1",
            "00000000000000000001:test:00000000000000000001",
            10,
            1,
            "hello",
        ))
        .await
        .unwrap();

    let socket_id = SocketId::from_string("12.12").unwrap();
    attach_signed_in_mutation_actor(
        &handler,
        app_manager.clone(),
        &app,
        socket_id,
        "user-1",
        ConnectionCapabilities {
            message_update_own: Some(vec!["versioned-room".to_string()]),
            ..Default::default()
        },
    )
    .await;

    let response = update_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app.clone()),
        State(handler.clone()),
        Json(UpdateMessageRequest {
            name: None,
            data: Some(MessageData::String("{\"text\":\"patched\"}".to_string())),
            extras: None,
            clear_fields: Vec::new(),
            client_id: Some("user-1".to_string()),
            socket_id: Some(socket_id.to_string()),
            description: Some("owner patch".to_string()),
            metadata: None,
            op_id: None,
        }),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);

    let latest = handler
        .version_store()
        .get_latest(
            "app-1",
            "versioned-room",
            &MessageSerial::new("msg:1").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.message.version.client_id.as_deref(), Some("user-1"));
    assert_eq!(
        latest.message.data.unwrap().into_string().as_deref(),
        Some("{\"text\":\"patched\"}")
    );
}

#[tokio::test]
async fn delete_message_allows_authenticated_any_capability() {
    let store = Arc::new(MemoryVersionStore::new());
    let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store.clone());
    let app = test_app();

    store
        .append_version(test_versioned_record(
            "msg:1",
            "00000000000000000001:test:00000000000000000001",
            10,
            1,
            "hello",
        ))
        .await
        .unwrap();

    let socket_id = SocketId::from_string("13.13").unwrap();
    attach_signed_in_mutation_actor(
        &handler,
        app_manager.clone(),
        &app,
        socket_id,
        "moderator",
        ConnectionCapabilities {
            message_delete_any: Some(vec!["versioned-room".to_string()]),
            ..Default::default()
        },
    )
    .await;

    let response = delete_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app),
        State(handler),
        Json(DeleteMessageRequest {
            data: None,
            extras: None,
            clear_fields: vec![sockudo_protocol::versioned_messages::ClearField::Data],
            client_id: Some("moderator".to_string()),
            socket_id: Some(socket_id.to_string()),
            description: Some("moderation".to_string()),
            metadata: None,
            op_id: None,
        }),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn append_message_denies_non_owner_with_own_capability() {
    let store = Arc::new(MemoryVersionStore::new());
    let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store.clone());
    let app = test_app();

    store
        .append_version(test_versioned_record(
            "msg:1",
            "00000000000000000001:test:00000000000000000001",
            10,
            1,
            "hello",
        ))
        .await
        .unwrap();

    let socket_id = SocketId::from_string("14.14").unwrap();
    attach_signed_in_mutation_actor(
        &handler,
        app_manager.clone(),
        &app,
        socket_id,
        "user-2",
        ConnectionCapabilities {
            message_append_own: Some(vec!["versioned-room".to_string()]),
            ..Default::default()
        },
    )
    .await;

    let response = match append_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app),
        State(handler),
        Json(AppendMessageRequest {
            data: " world".to_string(),
            extras: None,
            client_id: Some("user-2".to_string()),
            socket_id: Some(socket_id.to_string()),
            description: Some("unauthorized append".to_string()),
            metadata: None,
            op_id: None,
        }),
    )
    .await
    {
        Err(err) => err.into_response(),
        Ok(_) => panic!("expected own-scope append to be denied"),
    };

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(json["ai_code"], 93002);
    assert_eq!(json["error"], "mutations not permitted on this channel");
}

#[tokio::test]
async fn append_message_reports_not_implemented_after_validation() {
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
    store.append_version(original).await.unwrap();

    let response = append_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app),
        State(handler),
        Json(AppendMessageRequest {
            data: "hello".to_string(),
            extras: None,
            client_id: None,
            socket_id: None,
            description: None,
            metadata: None,
            op_id: None,
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
    assert_eq!(json["accepted"], true);
    assert_eq!(json["action"], "append");
}

#[tokio::test]
async fn append_message_accepts_empty_terminal_ai_append_and_retains_fragment() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_ai_versioned_handler_with_store(100, store.clone(), 1024, 4096, 1024);
    let app = test_app();

    store
        .append_version(test_versioned_record(
            "msg:1",
            "00000000000000000001:test:00000000000000000001",
            10,
            1,
            "hello",
        ))
        .await
        .unwrap();

    let response = append_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app),
        State(handler.clone()),
        Json(AppendMessageRequest {
            data: String::new(),
            extras: Some(ai_extras("complete")),
            client_id: None,
            socket_id: None,
            description: Some("terminal".to_string()),
            metadata: None,
            op_id: Some("terminal-empty".to_string()),
        }),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    let latest = handler
        .version_store()
        .get_latest(
            "app-1",
            "versioned-room",
            &MessageSerial::new("msg:1").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        latest.message.data.unwrap().into_string().as_deref(),
        Some("{\"text\":\"hello\"}")
    );
    assert_eq!(latest.message.append_fragment.as_deref(), Some(""));
}
