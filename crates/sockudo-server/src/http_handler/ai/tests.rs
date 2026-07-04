use super::*;
use crate::http_handler::test_support::*;
use crate::http_handler::{
    ChannelQuery, VersionMutationPath, append_message, batch_events, channel, events,
};
use axum::Json;
use axum::extract::{Extension, Path, Query, RawQuery, State};
use axum::http::{HeaderMap, Uri};
use axum::response::IntoResponse;
use sockudo_core::version_store::{MemoryVersionStore, VersionStore};
use sockudo_core::versioned_messages::{
    MessageAppend, MessageSerial, VersionMetadata, VersionSerial,
};
use sockudo_protocol::messages::{ApiMessageData, BatchPusherApiMessage, PusherApiMessage};
use sockudo_protocol::versioned_messages::AppendMessageRequest;
use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use std::collections::HashMap;

#[tokio::test]
async fn ai_http_publish_returns_serial_ack_and_message_id_dedupes() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_ai_versioned_handler_with_store(100, store.clone(), 1024, 4096, 1024);
    let app = test_app();

    let first = publish_ai_http_event(
        handler.clone(),
        app.clone(),
        "ai-output",
        Some("sdk-msg-1"),
        None,
        "streaming",
        "hello",
    )
    .await;
    let first_ack = &first["channels"]["versioned-room"];
    assert!(first_ack["message_serial"].as_str().is_some());
    assert_eq!(first_ack["history_serial"].as_u64(), Some(1));
    assert_eq!(first_ack["delivery_serial"].as_u64(), Some(1));
    assert!(first_ack["version_serial"].as_str().is_some());

    let duplicate = publish_ai_http_event(
        handler.clone(),
        app,
        "ai-output",
        Some("sdk-msg-1"),
        None,
        "streaming",
        "hello again",
    )
    .await;
    assert_eq!(
        duplicate["channels"]["versioned-room"],
        first["channels"]["versioned-room"]
    );

    let latest = store
        .latest_by_history("app-1", "versioned-room")
        .await
        .unwrap();
    assert_eq!(latest.len(), 1);
    assert_eq!(
        latest[0].message.data.as_ref().unwrap().as_string(),
        Some("hello")
    );
}

#[tokio::test]
async fn ai_http_publish_idempotency_key_header_returns_cached_serial_ack() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_ai_versioned_handler_with_store(100, store.clone(), 1024, 4096, 1024);
    let app = test_app();

    let first = publish_ai_http_event(
        handler.clone(),
        app.clone(),
        "ai-run-start",
        None,
        Some("run-start-request-1"),
        "streaming",
        "{\"run\":\"1\"}",
    )
    .await;
    let duplicate = publish_ai_http_event(
        handler,
        app,
        "ai-run-start",
        None,
        Some("run-start-request-1"),
        "streaming",
        "{\"run\":\"1-retry\"}",
    )
    .await;

    assert_eq!(
        duplicate["channels"]["versioned-room"],
        first["channels"]["versioned-room"]
    );
    assert_eq!(
        store
            .latest_by_history("app-1", "versioned-room")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn ai_batch_http_publish_returns_serial_acks_in_request_order() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_ai_versioned_handler_with_store(100, store.clone(), 1024, 4096, 1024);
    let app = test_app();

    let response = batch_events(
        Path("app-1".to_string()),
        Query(empty_event_query()),
        Extension(app),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        #[cfg(feature = "push")]
        test_push_admission(),
        State(handler),
        HeaderMap::new(),
        Uri::from_static("/apps/app-1/batch_events"),
        RawQuery(None),
        Json(BatchPusherApiMessage {
            batch: vec![
                PusherApiMessage {
                    name: Some("ai-output".to_string()),
                    data: Some(ApiMessageData::String("first".to_string())),
                    channel: Some("versioned-room".to_string()),
                    channels: None,
                    socket_id: None,
                    info: None,
                    tags: None,
                    delta: None,
                    idempotency_key: None,
                    message_id: Some("batch-msg-1".to_string()),
                    extras: Some(ai_extras("streaming")),
                },
                PusherApiMessage {
                    name: Some("ai-output".to_string()),
                    data: Some(ApiMessageData::String("second".to_string())),
                    channel: Some("versioned-room".to_string()),
                    channels: None,
                    socket_id: None,
                    info: None,
                    tags: None,
                    delta: None,
                    idempotency_key: None,
                    message_id: Some("batch-msg-2".to_string()),
                    extras: Some(ai_extras("complete")),
                },
            ],
        }),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    let json: Value = sonic_rs::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(json["batch"].as_array().unwrap().len(), 2);
    assert_eq!(json["batch"][0]["history_serial"].as_u64(), Some(1));
    assert_eq!(json["batch"][0]["delivery_serial"].as_u64(), Some(1));
    assert_eq!(json["batch"][1]["history_serial"].as_u64(), Some(2));
    assert_eq!(json["batch"][1]["delivery_serial"].as_u64(), Some(2));
    assert_ne!(
        json["batch"][0]["message_serial"],
        json["batch"][1]["message_serial"]
    );
    assert_eq!(
        store
            .latest_by_history("app-1", "versioned-room")
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn channel_state_includes_ai_block_for_ai_transport_channels() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_ai_versioned_handler_with_store(100, store, 1024, 4096, 1024);
    let app = test_app();

    publish_ai_http_event(
        handler.clone(),
        app.clone(),
        "ai-output",
        Some("sdk-msg-streaming"),
        None,
        "streaming",
        "streaming",
    )
    .await;
    publish_ai_http_event(
        handler.clone(),
        app.clone(),
        "ai-output",
        Some("sdk-msg-complete"),
        None,
        "complete",
        "complete",
    )
    .await;

    let response = channel(
        Path(("app-1".to_string(), "versioned-room".to_string())),
        Query(ChannelQuery {
            info: Some("subscription_count".to_string()),
            auth_params: empty_event_query(),
        }),
        Extension(app),
        State(handler),
        Uri::from_static("/apps/app-1/channels/versioned-room?info=subscription_count"),
        RawQuery(Some("info=subscription_count".to_string())),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    let json: Value = sonic_rs::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(json["ai"]["active_streams"].as_u64(), Some(1));
    assert_eq!(json["ai"]["last_history_serial"].as_u64(), Some(2));
    assert_eq!(json["ai"]["message_count"].as_u64(), Some(2));
}

#[tokio::test]
async fn append_message_rejects_ai_accumulated_content_over_cap() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_ai_versioned_handler_with_store(100, store.clone(), 8, 4096, 1024);
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

    let response = match append_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app),
        State(handler),
        Json(AppendMessageRequest {
            data: " oversized".to_string(),
            extras: None,
            client_id: None,
            socket_id: None,
            description: Some("too large".to_string()),
            metadata: None,
            op_id: None,
        }),
    )
    .await
    {
        Err(err) => err.into_response(),
        Ok(_) => panic!("expected accumulated content cap rejection"),
    };

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(json["ai_code"], 40009);
    assert_eq!(json["code"], "payload_too_large");
}

#[tokio::test]
async fn append_message_rejects_ai_append_count_over_cap() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_ai_versioned_handler_with_store(100, store.clone(), 1024, 1, 1024);
    let app = test_app();
    let original = test_versioned_record(
        "msg:1",
        "00000000000000000001:test:00000000000000000001",
        10,
        1,
        "hello",
    );
    let first_append = StoredVersionRecord {
        message: original
            .message
            .apply_append(
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
                MessageAppend {
                    data_fragment: "a".to_string(),
                    extras: None,
                },
            )
            .unwrap(),
        ..original.clone()
    };
    store.append_version(original).await.unwrap();
    store.append_version(first_append).await.unwrap();

    let response = match append_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app),
        State(handler),
        Json(AppendMessageRequest {
            data: "b".to_string(),
            extras: None,
            client_id: None,
            socket_id: None,
            description: Some("over cap".to_string()),
            metadata: None,
            op_id: None,
        }),
    )
    .await
    {
        Err(err) => err.into_response(),
        Ok(_) => panic!("expected append count cap rejection"),
    };

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(json["ai_code"], 40009);
}

#[tokio::test]
async fn append_message_persists_terminal_status_in_latest_record() {
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

    let extras = MessageExtras {
        ai: Some(sockudo_protocol::messages::AiExtras {
            transport: Some(HashMap::from([(
                "status".to_string(),
                "complete".to_string(),
            )])),
            codec: None,
        }),
        ..Default::default()
    };

    let response = append_message(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app),
        State(handler.clone()),
        Json(AppendMessageRequest {
            data: " world".to_string(),
            extras: Some(extras),
            client_id: None,
            socket_id: None,
            description: Some("terminal append".to_string()),
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
    assert_eq!(
        latest
            .message
            .extras
            .as_ref()
            .and_then(|extras| extras.ai_transport_headers())
            .and_then(|headers| headers.status()),
        Some("complete")
    );
    assert_eq!(
        latest.message.data.unwrap().into_string().as_deref(),
        Some("{\"text\":\"hello\"} world")
    );
}

#[tokio::test]
async fn events_rejects_ai_create_when_open_stream_cap_is_reached() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_ai_versioned_handler_with_store(100, store, 1024, 4096, 1);
    let app = test_app();
    let streaming_extras = || MessageExtras {
        ai: Some(sockudo_protocol::messages::AiExtras {
            transport: Some(HashMap::from([(
                "status".to_string(),
                "streaming".to_string(),
            )])),
            codec: None,
        }),
        ..Default::default()
    };

    let first = events(
        Path("app-1".to_string()),
        Query(empty_event_query()),
        Extension(app.clone()),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        #[cfg(feature = "push")]
        test_push_admission(),
        State(handler.clone()),
        HeaderMap::new(),
        Uri::from_static("/apps/app-1/events"),
        RawQuery(None),
        Json(PusherApiMessage {
            name: Some("ai-output".to_string()),
            data: Some(ApiMessageData::String("hello".to_string())),
            channel: Some("versioned-room".to_string()),
            channels: None,
            socket_id: None,
            info: None,
            tags: None,
            delta: None,
            idempotency_key: None,
            message_id: None,
            extras: Some(streaming_extras()),
        }),
    )
    .await
    .unwrap()
    .into_response();
    assert_eq!(first.status(), StatusCode::OK);

    let second = match events(
        Path("app-1".to_string()),
        Query(empty_event_query()),
        Extension(app),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        #[cfg(feature = "push")]
        test_push_admission(),
        State(handler),
        HeaderMap::new(),
        Uri::from_static("/apps/app-1/events"),
        RawQuery(None),
        Json(PusherApiMessage {
            name: Some("ai-output".to_string()),
            data: Some(ApiMessageData::String("hello again".to_string())),
            channel: Some("versioned-room".to_string()),
            channels: None,
            socket_id: None,
            info: None,
            tags: None,
            delta: None,
            idempotency_key: None,
            message_id: None,
            extras: Some(streaming_extras()),
        }),
    )
    .await
    {
        Err(err) => err.into_response(),
        Ok(_) => panic!("expected open-stream cap rejection"),
    };

    assert_eq!(second.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = axum::body::to_bytes(second.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert_eq!(json["ai_code"], 40009);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum MatrixOperation {
    SubscribePublic,
    SubscribePrivate,
    SubscribePresence,
    PublishClientEvent,
    PublishAiInput,
    PublishAiOutput,
    PublishAiRun,
    PublishAiCancel,
    AppendOwn,
    AppendOther,
    UpdateOwn,
    UpdateOther,
    DeleteOwn,
    DeleteOther,
    HistoryWs,
    HistoryHttp,
    Rewind,
    UntilAttach,
    PresenceEnter,
    PresenceUpdate,
    MutationEndpoint,
    PushRulePublish,
    PushAdmin,
    PushSubscribe,
    RevocationAdmin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum MatrixPrincipal {
    Anonymous,
    V1Hmac,
    V2Hmac,
    TokenSubscribe,
    TokenPublish,
    TokenHistory,
    TokenPresence,
    TokenMutationOwn,
    TokenMutationAny,
    ExpiredToken,
    RevokedToken,
    ServerKey,
    WrongAppKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatrixDecision {
    Allow,
    Deny(u32),
}

const MATRIX_OPERATIONS: [MatrixOperation; 25] = [
    MatrixOperation::SubscribePublic,
    MatrixOperation::SubscribePrivate,
    MatrixOperation::SubscribePresence,
    MatrixOperation::PublishClientEvent,
    MatrixOperation::PublishAiInput,
    MatrixOperation::PublishAiOutput,
    MatrixOperation::PublishAiRun,
    MatrixOperation::PublishAiCancel,
    MatrixOperation::AppendOwn,
    MatrixOperation::AppendOther,
    MatrixOperation::UpdateOwn,
    MatrixOperation::UpdateOther,
    MatrixOperation::DeleteOwn,
    MatrixOperation::DeleteOther,
    MatrixOperation::HistoryWs,
    MatrixOperation::HistoryHttp,
    MatrixOperation::Rewind,
    MatrixOperation::UntilAttach,
    MatrixOperation::PresenceEnter,
    MatrixOperation::PresenceUpdate,
    MatrixOperation::MutationEndpoint,
    MatrixOperation::PushRulePublish,
    MatrixOperation::PushAdmin,
    MatrixOperation::PushSubscribe,
    MatrixOperation::RevocationAdmin,
];

const MATRIX_PRINCIPALS: [MatrixPrincipal; 13] = [
    MatrixPrincipal::Anonymous,
    MatrixPrincipal::V1Hmac,
    MatrixPrincipal::V2Hmac,
    MatrixPrincipal::TokenSubscribe,
    MatrixPrincipal::TokenPublish,
    MatrixPrincipal::TokenHistory,
    MatrixPrincipal::TokenPresence,
    MatrixPrincipal::TokenMutationOwn,
    MatrixPrincipal::TokenMutationAny,
    MatrixPrincipal::ExpiredToken,
    MatrixPrincipal::RevokedToken,
    MatrixPrincipal::ServerKey,
    MatrixPrincipal::WrongAppKey,
];

fn authz_matrix_decision(operation: MatrixOperation, principal: MatrixPrincipal) -> MatrixDecision {
    use MatrixDecision::{Allow, Deny};
    use MatrixOperation::*;
    use MatrixPrincipal::*;

    match (operation, principal) {
        (_, ExpiredToken | RevokedToken) => Deny(4009),
        (_, WrongAppKey) => Deny(4010),
        (SubscribePublic, Anonymous | V1Hmac | V2Hmac | TokenSubscribe) => Allow,
        (SubscribePrivate, V1Hmac | V2Hmac | TokenSubscribe) => Allow,
        (SubscribePresence, V1Hmac | V2Hmac | TokenPresence) => Allow,
        (PublishClientEvent, V1Hmac | V2Hmac | TokenPublish) => Allow,
        (PublishAiInput | PublishAiCancel, V2Hmac | TokenPublish | ServerKey) => Allow,
        (PublishAiOutput | PublishAiRun, ServerKey) => Allow,
        (
            AppendOwn | UpdateOwn | DeleteOwn,
            V2Hmac | TokenMutationOwn | TokenMutationAny | ServerKey,
        ) => Allow,
        (AppendOther | UpdateOther | DeleteOther, TokenMutationAny | ServerKey) => Allow,
        (HistoryWs | Rewind | UntilAttach, V2Hmac | TokenHistory) => Allow,
        (HistoryHttp, V1Hmac | V2Hmac | ServerKey) => Allow,
        (PresenceEnter | PresenceUpdate, V2Hmac | TokenPresence) => Allow,
        (MutationEndpoint, V2Hmac | TokenMutationOwn | TokenMutationAny | ServerKey) => Allow,
        (PushRulePublish | PushAdmin | PushSubscribe | RevocationAdmin, ServerKey) => Allow,
        (PushSubscribe, TokenPublish) => Deny(403),
        (SubscribePublic, _) => Deny(4009),
        (SubscribePrivate | SubscribePresence, _) => Deny(4009),
        (PublishClientEvent, _) => Deny(4301),
        (PublishAiInput | PublishAiOutput | PublishAiRun | PublishAiCancel, _) => {
            Deny(AI_ERROR_EVENT_NOT_PERMITTED)
        }
        (
            AppendOwn | AppendOther | UpdateOwn | UpdateOther | DeleteOwn | DeleteOther
            | MutationEndpoint,
            _,
        ) => Deny(AI_ERROR_MUTABLE_NOT_PERMITTED),
        (HistoryWs | HistoryHttp | Rewind | UntilAttach, _) => Deny(403),
        (PresenceEnter | PresenceUpdate, _) => Deny(104009),
        (PushRulePublish | PushAdmin | PushSubscribe | RevocationAdmin, _) => Deny(403),
    }
}

#[test]
fn ai_transport_authz_matrix_enumerates_every_operation_and_principal() {
    let mut checked = 0usize;
    for operation in MATRIX_OPERATIONS {
        for principal in MATRIX_PRINCIPALS {
            let decision = authz_matrix_decision(operation, principal);
            match decision {
                MatrixDecision::Allow => {}
                MatrixDecision::Deny(code) => assert_ne!(
                    code, 0,
                    "deny decisions must carry a stable error code for {operation:?}/{principal:?}"
                ),
            }
            checked += 1;
        }
    }

    assert_eq!(checked, MATRIX_OPERATIONS.len() * MATRIX_PRINCIPALS.len());
    assert_eq!(checked, 325);
}
