use super::*;
use crate::http_handler::test_support::*;
use sockudo_core::annotations::{
    AnnotationId, AnnotationProjectionRequest, AnnotationSummary, StoredAnnotationEvent,
    TotalAnnotationSummary,
};
use sockudo_core::version_store::{MemoryVersionStore, VersionStore};
use sockudo_core::versioned_messages::MessageSerial;
use sockudo_core::websocket::ConnectionCapabilities;
use sonic_rs::{JsonValueTrait, json};

#[tokio::test]
async fn annotation_publish_requires_feature_flag() {
    let store = Arc::new(MemoryVersionStore::new());
    let (handler, _adapter, _app_manager) =
        test_versioned_handler_harness_with_annotations(100, store.clone(), false);
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

    let response = match publish_annotation(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app),
        State(handler),
        Json(PublishAnnotationRequest {
            annotation_type: "reactions:total.v1".to_string(),
            name: None,
            client_id: None,
            socket_id: None,
            count: None,
            data: None,
            encoding: None,
        }),
    )
    .await
    {
        Err(err) => err.into_response(),
        Ok(_) => panic!("expected annotations feature-disabled error"),
    };

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn annotation_publish_requires_channel_policy_flag() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_versioned_handler_with_store(100, store.clone());
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

    let response = match publish_annotation(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app),
        State(handler),
        Json(PublishAnnotationRequest {
            annotation_type: "reactions:total.v1".to_string(),
            name: None,
            client_id: None,
            socket_id: None,
            count: None,
            data: None,
            encoding: None,
        }),
    )
    .await
    {
        Err(err) => err.into_response(),
        Ok(_) => panic!("expected annotation channel policy denial"),
    };

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    assert!(
        json["error"]
            .as_str()
            .unwrap()
            .contains("Annotations are disabled by channel policy")
    );
}

#[tokio::test]
async fn annotation_publish_denies_socket_without_annotation_publish_capability() {
    let store = Arc::new(MemoryVersionStore::new());
    let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store.clone());
    let app = test_annotation_app();

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

    let socket_id = SocketId::from_string("21.21").unwrap();
    attach_signed_in_mutation_actor(
        &handler,
        app_manager,
        &app,
        socket_id,
        "user-1",
        ConnectionCapabilities {
            publish: Some(vec!["versioned-room".to_string()]),
            ..Default::default()
        },
    )
    .await;

    let response = match publish_annotation(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app),
        State(handler),
        Json(PublishAnnotationRequest {
            annotation_type: "reactions:distinct.v1".to_string(),
            name: Some("thumbsup".to_string()),
            client_id: Some("user-1".to_string()),
            socket_id: Some(socket_id.to_string()),
            count: None,
            data: None,
            encoding: None,
        }),
    )
    .await
    {
        Err(err) => err.into_response(),
        Ok(_) => panic!("expected annotation-publish capability denial"),
    };

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn annotation_publish_rejects_unidentified_ownership_summarizer() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_versioned_handler_with_store(100, store.clone());
    let app = test_annotation_app();

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

    let response = match publish_annotation(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app),
        State(handler),
        Json(PublishAnnotationRequest {
            annotation_type: "reactions:flag.v1".to_string(),
            name: None,
            client_id: None,
            socket_id: None,
            count: None,
            data: None,
            encoding: None,
        }),
    )
    .await
    {
        Err(err) => err.into_response(),
        Ok(_) => panic!("expected missing client_id validation"),
    };

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn annotation_publish_and_delete_update_summary_projection() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_versioned_handler_with_store(100, store.clone());
    let app = test_annotation_app();

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

    let publish_response = publish_annotation(
        Path(VersionMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
        }),
        Extension(app.clone()),
        State(handler.clone()),
        Json(PublishAnnotationRequest {
            annotation_type: "reactions:total.v1".to_string(),
            name: None,
            client_id: None,
            socket_id: None,
            count: None,
            data: Some(json!({"emoji": "thumbsup"})),
            encoding: None,
        }),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(publish_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(publish_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = sonic_rs::from_slice(&body).unwrap();
    let annotation_serial = json["annotationSerial"].as_str().unwrap().to_string();

    let projection_request = AnnotationProjectionRequest {
        app_id: "app-1".to_string(),
        channel_id: "versioned-room".to_string(),
        message_serial: MessageSerial::new("msg:1").unwrap(),
        annotation_type: AnnotationType::new("reactions:total.v1").unwrap(),
    };
    let projection = handler
        .annotation_store()
        .get_projection(projection_request.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        projection.summary,
        AnnotationSummary::Total(TotalAnnotationSummary { total: 1 })
    );

    let delete_response = delete_annotation(
        Path(AnnotationMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
            annotation_serial,
        }),
        Query(HashMap::new()),
        Extension(app),
        State(handler.clone()),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(delete_response.status(), StatusCode::OK);
    let projection = handler
        .annotation_store()
        .get_projection(projection_request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        projection.summary,
        AnnotationSummary::Total(TotalAnnotationSummary { total: 0 })
    );
}

#[tokio::test]
async fn annotation_delete_missing_annotation_returns_404() {
    let store = Arc::new(MemoryVersionStore::new());
    let handler = test_versioned_handler_with_store(100, store);
    let app = test_annotation_app();

    let response = match delete_annotation(
        Path(AnnotationMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
            annotation_serial: "ann:missing".to_string(),
        }),
        Query(HashMap::new()),
        Extension(app),
        State(handler),
    )
    .await
    {
        Err(err) => err.into_response(),
        Ok(_) => panic!("expected missing annotation 404"),
    };

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn annotation_delete_own_denies_identity_mismatch() {
    let store = Arc::new(MemoryVersionStore::new());
    let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store);
    let app = test_annotation_app();

    handler
        .annotation_store()
        .append_event(StoredAnnotationEvent {
            app_id: "app-1".to_string(),
            channel_id: "versioned-room".to_string(),
            stored_at_ms: sockudo_core::history::now_ms(),
            annotation: Annotation {
                id: AnnotationId::new("ann-1").unwrap(),
                action: AnnotationAction::Create,
                serial: AnnotationSerial::new("ann:1").unwrap(),
                message_serial: MessageSerial::new("msg:1").unwrap(),
                annotation_type: AnnotationType::new("reactions:distinct.v1").unwrap(),
                name: Some("thumbsup".to_string()),
                client_id: Some("user-1".to_string()),
                count: None,
                data: None,
                encoding: None,
                timestamp: sockudo_core::history::now_ms(),
            },
        })
        .await
        .unwrap();

    let socket_id = SocketId::from_string("22.22").unwrap();
    attach_signed_in_mutation_actor(
        &handler,
        app_manager,
        &app,
        socket_id,
        "user-2",
        ConnectionCapabilities {
            annotation_delete_own: Some(vec!["versioned-room".to_string()]),
            ..Default::default()
        },
    )
    .await;

    let response = match delete_annotation(
        Path(AnnotationMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
            annotation_serial: "ann:1".to_string(),
        }),
        Query(HashMap::from([(
            "socket_id".to_string(),
            socket_id.to_string(),
        )])),
        Extension(app),
        State(handler),
    )
    .await
    {
        Err(err) => err.into_response(),
        Ok(_) => panic!("expected annotation-delete-own identity mismatch"),
    };

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn annotation_delete_own_allows_annotation_owner() {
    let store = Arc::new(MemoryVersionStore::new());
    let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store);
    let app = test_annotation_app();

    handler
        .annotation_store()
        .append_event(StoredAnnotationEvent {
            app_id: "app-1".to_string(),
            channel_id: "versioned-room".to_string(),
            stored_at_ms: sockudo_core::history::now_ms(),
            annotation: Annotation {
                id: AnnotationId::new("ann-own").unwrap(),
                action: AnnotationAction::Create,
                serial: AnnotationSerial::new("ann:own").unwrap(),
                message_serial: MessageSerial::new("msg:1").unwrap(),
                annotation_type: AnnotationType::new("reactions:distinct.v1").unwrap(),
                name: Some("thumbsup".to_string()),
                client_id: Some("user-1".to_string()),
                count: None,
                data: None,
                encoding: None,
                timestamp: sockudo_core::history::now_ms(),
            },
        })
        .await
        .unwrap();

    let socket_id = SocketId::from_string("23.23").unwrap();
    attach_signed_in_mutation_actor(
        &handler,
        app_manager,
        &app,
        socket_id,
        "user-1",
        ConnectionCapabilities {
            annotation_delete_own: Some(vec!["versioned-room".to_string()]),
            ..Default::default()
        },
    )
    .await;

    let response = delete_annotation(
        Path(AnnotationMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
            annotation_serial: "ann:own".to_string(),
        }),
        Query(HashMap::from([(
            "socket_id".to_string(),
            socket_id.to_string(),
        )])),
        Extension(app),
        State(handler),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn annotation_delete_any_allows_moderator() {
    let store = Arc::new(MemoryVersionStore::new());
    let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store);
    let app = test_annotation_app();

    handler
        .annotation_store()
        .append_event(StoredAnnotationEvent {
            app_id: "app-1".to_string(),
            channel_id: "versioned-room".to_string(),
            stored_at_ms: sockudo_core::history::now_ms(),
            annotation: Annotation {
                id: AnnotationId::new("ann-any").unwrap(),
                action: AnnotationAction::Create,
                serial: AnnotationSerial::new("ann:any").unwrap(),
                message_serial: MessageSerial::new("msg:1").unwrap(),
                annotation_type: AnnotationType::new("reactions:distinct.v1").unwrap(),
                name: Some("thumbsup".to_string()),
                client_id: Some("user-1".to_string()),
                count: None,
                data: None,
                encoding: None,
                timestamp: sockudo_core::history::now_ms(),
            },
        })
        .await
        .unwrap();

    let socket_id = SocketId::from_string("24.24").unwrap();
    attach_signed_in_mutation_actor(
        &handler,
        app_manager,
        &app,
        socket_id,
        "moderator",
        ConnectionCapabilities {
            annotation_delete_any: Some(vec!["versioned-room".to_string()]),
            ..Default::default()
        },
    )
    .await;

    let response = delete_annotation(
        Path(AnnotationMutationPath {
            app_id: "app-1".to_string(),
            channel_name: "versioned-room".to_string(),
            message_serial: "msg:1".to_string(),
            annotation_serial: "ann:any".to_string(),
        }),
        Query(HashMap::from([(
            "socket_id".to_string(),
            socket_id.to_string(),
        )])),
        Extension(app),
        State(handler),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
}
