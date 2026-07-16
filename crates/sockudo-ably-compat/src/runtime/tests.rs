//! Runtime behavior and compatibility regression tests.

use super::*;
use base64::engine::general_purpose;
use sockudo_cache::{MemoryCacheManager, RedisCacheManager};
use sockudo_core::{app::AppPolicy, options::MemoryCacheOptions};
use sockudo_protocol::messages::{AiExtras, MessageExtras};
use sockudo_protocol::versioned_messages::apply_runtime_metadata;
use sockudo_ws::{
    Config as WsConfig, Http1, Stream as WsStream, WebSocketStream, axum_integration::WebSocket,
    client::WebSocketClient,
};
use tokio::net::{TcpListener, TcpStream};

async fn full_websocket_pair() -> (WebSocket, WebSocketStream<WsStream<Http1>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        sockudo_ws::handshake::server_handshake(&mut stream)
            .await
            .unwrap();
        WebSocket::from_tcp(stream, WsConfig::default())
    });

    let stream = TcpStream::connect(address).await.unwrap();
    let client = WebSocketClient::<Http1>::new(WsConfig::default());
    let (client, _): (WebSocketStream<WsStream<Http1>>, _) = client
        .connect(stream, &address.to_string(), "/", None)
        .await
        .unwrap();

    (server.await.unwrap(), client)
}

#[tokio::test]
async fn fatal_socket_error_is_delivered_before_orderly_close() {
    let (server, client) = full_websocket_pair().await;
    let (mut client, mut client_writer) = client.split();
    let server = tokio::spawn(send_fatal_ably_socket_error(
        server,
        AblyFormat::Json,
        AblyAuthError::invalid_credentials(),
    ));

    let error_frame = tokio::time::timeout(Duration::from_secs(1), client.next())
        .await
        .expect("timed out waiting for Ably error frame")
        .expect("connection ended before Ably error frame")
        .expect("failed to read Ably error frame");
    let Message::Text(payload) = error_frame else {
        panic!("expected text Ably error frame, got {error_frame:?}");
    };
    let protocol = decode_protocol_bytes(&payload, AblyFormat::Json).unwrap();
    assert_eq!(protocol.action, ACTION_ERROR);
    assert_eq!(protocol.error.unwrap().code, 40101);

    client_writer
        .close(1000, "client acknowledged error")
        .await
        .unwrap();
    let close_frame = tokio::time::timeout(Duration::from_secs(1), client.next())
        .await
        .expect("timed out waiting for orderly close frame")
        .expect("connection ended without an orderly close frame")
        .expect("failed to read orderly close frame");
    assert!(matches!(close_frame, Message::Close(_)));
    server.await.unwrap();
}

#[test]
fn publish_validation_accepts_nameless_and_dataless_messages() {
    validate_ably_publish_message(&AblyMessage::default(), false)
        .expect("Ably permits a message without name or data");
}

#[test]
fn publish_validation_rejects_reserved_and_unknown_fields() {
    let reserved = AblyMessage {
        connection_id: Some("client-supplied".to_string()),
        ..AblyMessage::default()
    };
    assert!(validate_ably_publish_message(&reserved, true).is_err());

    let unknown = AblyMessage {
        extras: Some(json!({ "ephemeral": true })),
        ..AblyMessage::default()
    };
    assert!(validate_ably_publish_message(&unknown, false).is_err());
}

#[test]
fn wire_connection_key_is_not_treated_as_delivered_connection_id() {
    let message: AblyMessage =
        sonic_rs::from_str(r#"{"connectionKey":"app.key.secret","clientId":"publisher"}"#)
            .expect("valid message");

    assert_eq!(message.connection_key.as_deref(), Some("app.key.secret"));
    assert!(message.connection_id.is_none());
}

#[tokio::test]
async fn connection_key_resolves_only_a_live_same_app_identity() {
    let hub = AblyCompatHub::default();
    let connection_key = "app:key";
    hub.remember_connection(
        connection_key.to_string(),
        "app",
        "connection-a",
        Some("publisher".to_string()),
    )
    .await;
    let message = AblyMessage {
        client_id: Some("publisher".to_string()),
        connection_key: Some(connection_key.to_string()),
        ..AblyMessage::default()
    };

    assert!(rest_publish_identity(&hub, "app", Some("publisher"), &message).is_err());

    let (command_tx, _command_rx) = tokio::sync::mpsc::channel(1);
    hub.live_sessions.insert(
        "connection-a".to_string(),
        AblyLiveSession {
            session_id: "transport-a".to_string(),
            app_id: "app".to_string(),
            authorization: Arc::new(RwLock::new(ConnectionAuthorization {
                generation: 1,
                client_id: Some("publisher".to_string()),
                connection_client_id: Some("publisher".to_string()),
                capabilities: None,
                issued_ms: now_ms(),
                expires_ms: None,
                credential_id: "test".to_string(),
                revocable: false,
                revocation_key: None,
            })),
            command_tx,
        },
    );

    assert_eq!(
        rest_publish_identity(&hub, "app", Some("publisher"), &message).unwrap(),
        (
            Some("connection-a".to_string()),
            Some("publisher".to_string())
        )
    );
    assert!(rest_publish_identity(&hub, "other-app", Some("publisher"), &message).is_err());
    assert!(rest_publish_identity(&hub, "app", Some("impostor"), &message).is_err());
}

#[tokio::test]
async fn recovered_transport_supersedes_without_unregistering_replacement() {
    let hub = AblyCompatHub::default();
    let authorization = Arc::new(RwLock::new(ConnectionAuthorization {
        generation: 1,
        client_id: None,
        connection_client_id: None,
        capabilities: None,
        issued_ms: now_ms(),
        expires_ms: None,
        credential_id: "test".to_string(),
        revocable: false,
        revocation_key: None,
    }));
    let (old_tx, mut old_rx) = tokio::sync::mpsc::channel(1);
    assert!(
        hub.register_live_session(
            "connection-a".to_string(),
            AblyLiveSession {
                session_id: "transport-old".to_string(),
                app_id: "app".to_string(),
                authorization: Arc::clone(&authorization),
                command_tx: old_tx,
            },
        )
        .is_none()
    );

    let (new_tx, _new_rx) = tokio::sync::mpsc::channel(1);
    let previous = hub
        .register_live_session(
            "connection-a".to_string(),
            AblyLiveSession {
                session_id: "transport-new".to_string(),
                app_id: "app".to_string(),
                authorization,
                command_tx: new_tx,
            },
        )
        .expect("replacement returns the old transport command sender");
    previous
        .send(AblySessionCommand::Superseded)
        .await
        .expect("old transport still receives supersession");
    assert!(matches!(
        old_rx.recv().await,
        Some(AblySessionCommand::Superseded)
    ));

    hub.unregister_live_session("connection-a", "transport-old");
    assert_eq!(
        hub.live_sessions
            .get("connection-a")
            .map(|session| session.session_id.clone())
            .as_deref(),
        Some("transport-new")
    );
    hub.unregister_live_session("connection-a", "transport-new");
    assert!(!hub.live_sessions.contains_key("connection-a"));
}

#[test]
fn client_id_header_requires_base64_utf8() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-ably-clientid",
        axum::http::HeaderValue::from_static("cHVibGlzaGVy"),
    );
    assert_eq!(
        decode_ably_client_id_header(&headers)
            .expect("valid header")
            .as_deref(),
        Some("publisher")
    );

    headers.insert(
        "x-ably-clientid",
        axum::http::HeaderValue::from_static("not-base64"),
    );
    assert!(decode_ably_client_id_header(&headers).is_err());
}

#[test]
fn authenticated_identity_rejects_mutation_actor_spoofing() {
    let message = AblyMessage {
        action: Some(MESSAGE_UPDATE),
        serial: Some("message-serial".to_string()),
        version: Some(AblyMessageVersion {
            serial: "version-serial".to_string(),
            timestamp: None,
            client_id: Some("impostor".to_string()),
            description: None,
            metadata: None,
        }),
        ..AblyMessage::default()
    };

    assert!(effective_ably_client_id(Some("authenticated"), &message).is_err());
}

#[test]
fn ack_count_covers_the_inbound_protocol_serial_range() {
    let inbound = AblyProtocolMessage {
        action: ACTION_MESSAGE,
        count: Some(3),
        messages: Some(vec![AblyMessage::default(); 7]),
        ..empty_protocol_message(ACTION_MESSAGE)
    };
    assert_eq!(publish_ack_count(&inbound), 3);
}

#[test]
fn echo_filter_only_suppresses_the_originating_connection() {
    assert!(!should_deliver_to_subscriber(
        Some("connection-a"),
        "connection-a",
        false,
        None
    ));
    assert!(should_deliver_to_subscriber(
        Some("connection-a"),
        "connection-a",
        false,
        Some(true)
    ));
    assert!(should_deliver_to_subscriber(
        Some("connection-a"),
        "connection-b",
        false,
        Some(false)
    ));
}

#[test]
fn history_next_link_preserves_page_shape_without_credentials() {
    let query = AblyHistoryQuery {
        key: Some("key:secret".to_string()),
        access_token: Some("token-secret".to_string()),
        limit: Some(2),
        direction: Some("forwards".to_string()),
        start: Some(10),
        end: Some(20),
        ..AblyHistoryQuery::default()
    };
    let cursor = HistoryCursor {
        version: 1,
        app_id: "app".to_string(),
        channel: "space channel".to_string(),
        stream_id: "stream".to_string(),
        serial: 11,
        direction: HistoryDirection::OldestFirst,
        bounds: HistoryQueryBounds {
            start_time_ms: Some(10),
            end_time_ms: Some(20),
            ..HistoryQueryBounds::default()
        },
    };

    let link = ably_history_next_link(&query, HistoryDirection::OldestFirst, 2, &cursor)
        .expect("valid Link header");
    assert!(link.starts_with("<./messages?limit=2&direction=forwards&cursor="));
    assert!(link.contains("&start=10&end=20"));
    assert!(link.ends_with(">; rel=\"next\""));
    assert!(!link.contains("secret"));
    assert!(!link.contains("access_token"));
    assert!(!link.contains("key="));
}

#[test]
fn rewind_parser_accepts_count_and_seconds_minutes_hours() {
    assert_eq!(
        parse_ably_rewind_param("12"),
        Some(SubscriptionRewind::Count(12))
    );
    assert_eq!(
        parse_ably_rewind_param("15s"),
        Some(SubscriptionRewind::Seconds(15))
    );
    assert_eq!(
        parse_ably_rewind_param("2m"),
        Some(SubscriptionRewind::Seconds(120))
    );
    assert_eq!(
        parse_ably_rewind_param("3h"),
        Some(SubscriptionRewind::Seconds(10_800))
    );
}

#[test]
fn rewind_parser_rejects_zero_invalid_and_overflowing_values() {
    assert_eq!(parse_ably_rewind_param("0"), None);
    assert_eq!(parse_ably_rewind_param("0s"), None);
    assert_eq!(parse_ably_rewind_param("forever"), None);
    assert_eq!(parse_ably_rewind_param("18446744073709551615h"), None);
}

#[test]
fn attach_resume_suppresses_rewind_params() {
    let params = HashMap::from([("rewind".to_string(), "1m".to_string())]);

    assert_eq!(resolve_ably_rewind(&params, true), None);
    assert_eq!(
        resolve_ably_rewind(&params, false),
        Some(SubscriptionRewind::Seconds(60))
    );
}

#[test]
fn rewind_history_request_is_bounded_and_ends_at_attach_high_water() {
    let position = AblyChannelPosition {
        stream_id: "stream".to_string(),
        serial: 41,
    };

    let count = build_ably_rewind_history_request(
        "app",
        "channel",
        &SubscriptionRewind::Count(500),
        100,
        Some(&position),
        1_000_000,
    );
    assert_eq!(count.limit, 100);
    assert_eq!(count.direction, HistoryDirection::NewestFirst);
    assert_eq!(count.bounds.end_serial, Some(41));

    let duration = build_ably_rewind_history_request(
        "app",
        "channel",
        &SubscriptionRewind::Seconds(60),
        100,
        Some(&position),
        1_000_000,
    );
    assert_eq!(duration.limit, 100);
    assert_eq!(duration.direction, HistoryDirection::OldestFirst);
    assert_eq!(duration.bounds.start_time_ms, Some(940_000));
    assert_eq!(duration.bounds.end_serial, Some(41));
}

#[test]
fn history_link_header_includes_stable_first_and_next_relations() {
    let query = AblyHistoryQuery {
        key: Some("key:secret".to_string()),
        access_token: Some("token-secret".to_string()),
        limit: Some(2),
        direction: Some("forwards".to_string()),
        start: Some(10),
        end: Some(20),
        until_attach: Some(true),
        from_serial: Some("stream:7".to_string()),
        format: Some("msgpack".to_string()),
        ..AblyHistoryQuery::default()
    };
    let cursor = HistoryCursor {
        version: 1,
        app_id: "app".to_string(),
        channel: "space channel".to_string(),
        stream_id: "stream".to_string(),
        serial: 11,
        direction: HistoryDirection::OldestFirst,
        bounds: HistoryQueryBounds {
            start_time_ms: Some(10),
            end_time_ms: Some(20),
            ..HistoryQueryBounds::default()
        },
    };

    let header = ably_history_link_header(&query, HistoryDirection::OldestFirst, 2, Some(&cursor))
        .expect("valid Link header");

    assert!(header.contains("rel=\"first\""));
    assert!(header.contains("rel=\"next\""));
    assert!(header.contains("limit=2&direction=forwards"));
    assert!(header.contains("start=10&end=20&untilAttach=true"));
    assert!(header.contains("from_serial=stream%3A7&format=msgpack"));
    assert!(!header.contains("secret"));
    assert!(!header.contains("access_token"));
    assert!(!header.contains("key="));
}

#[test]
fn annotation_delete_capabilities_are_independent_operations() {
    let capabilities = ably_capability_value_to_sockudo(&serde_json::json!({
        "mutable:*": ["annotation-delete-own"],
        "moderated:*": ["annotation-delete-any"]
    }))
    .expect("annotation delete capabilities should parse");

    assert!(capabilities.allows_annotation_delete_own("mutable:room"));
    assert!(!capabilities.allows_annotation_delete_any("mutable:room"));
    assert!(capabilities.allows_annotation_delete_any("moderated:room"));
    assert!(
        ensure_ably_channel_capability(
            Some(&capabilities),
            &AblyChannelName::parse("mutable:room".to_string()).unwrap(),
            AblyCapabilityCheck::AnnotationMutate,
        )
        .is_ok()
    );
    assert!(!capabilities.allows_message_mutation_any(
        sockudo_core::versioned_message_auth::MutationKind::Delete,
        "moderated:room"
    ));
}

#[test]
fn annotation_cursor_is_opaque_and_scoped_to_app_channel_and_message() {
    let message_serial = MessageSerial::new("msg:1").unwrap();
    let annotation_serial = AnnotationSerial::new("ann:2").unwrap();
    let cursor =
        encode_ably_annotation_cursor("app", "mutable:room", &message_serial, &annotation_serial)
            .unwrap();

    assert!(!cursor.contains("ann:2"));
    assert_eq!(
        decode_ably_annotation_cursor(&cursor, "app", "mutable:room", &message_serial,).unwrap(),
        annotation_serial
    );
    assert!(
        decode_ably_annotation_cursor(&cursor, "app", "mutable:other", &message_serial,).is_err()
    );
    assert!(
        decode_ably_annotation_cursor(
            &cursor,
            "app",
            "mutable:room",
            &MessageSerial::new("msg:other").unwrap(),
        )
        .is_err()
    );
}

#[test]
fn multiple_summary_renames_native_client_counts_only_at_ably_edge() {
    let native = json!({
        "annotations": {
            "summary": {
                "reaction:multiple.v1": {
                    "vote": {
                        "total": 3,
                        "clientCounts": { "client-1": 2 },
                        "totalUnidentified": 1,
                        "clipped": false,
                        "totalClientIds": 1
                    }
                },
                "reaction:distinct.v1": {
                    "like": { "total": 1, "clientIds": ["client-1"], "clipped": false }
                }
            }
        }
    });

    let projected = ably_summary_annotations(&native).unwrap();
    let multiple = &projected["summary"]["reaction:multiple.v1"]["vote"];
    assert_eq!(multiple["clientIds"]["client-1"].as_u64(), Some(2));
    assert!(multiple.get("clientCounts").is_none());
    assert_eq!(
        projected["summary"]["reaction:distinct.v1"]["like"]["clientIds"][0].as_str(),
        Some("client-1")
    );
    assert!(native["summary"].is_null());
    assert_eq!(
            native["annotations"]["summary"]["reaction:multiple.v1"]["vote"]
                ["clientCounts"]["client-1"]
                .as_u64(),
            Some(2)
        );
}

#[test]
fn delete_own_and_delete_any_authorize_independently_of_message_mutation() {
    let channel = AblyChannelName::parse("mutable:room".to_string()).unwrap();
    let own = ably_capability_value_to_sockudo(&serde_json::json!({
        "mutable:*": ["annotation-delete-own"]
    }))
    .unwrap();
    assert!(
        authorize_ably_annotation_delete(Some(&own), &channel, Some("client-1"), Some("client-1"),)
            .is_ok()
    );
    assert!(
        authorize_ably_annotation_delete(Some(&own), &channel, Some("client-2"), Some("client-1"),)
            .is_err()
    );

    let any = ably_capability_value_to_sockudo(&serde_json::json!({
        "mutable:*": ["annotation-delete-any"]
    }))
    .unwrap();
    assert!(
            authorize_ably_annotation_delete(
                Some(&any),
                &channel,
                Some("moderator"),
                Some("client-1"),
            )
            .is_ok()
        );
    assert!(
        authorize_ably_annotation_delete(Some(&any), &channel, None, Some("client-1")).is_err()
    );
}

#[test]
fn delete_annotation_dto_keeps_native_target_selector_fields() {
    let command = parse_ably_annotation_command(
        AblyAnnotation {
            action: Some(1),
            id: Some("annotation-id".to_string()),
            serial: Some("ann:7".to_string()),
            message_serial: Some("msg:1".to_string()),
            annotation_type: Some("reaction:distinct.v1".to_string()),
            name: Some("like".to_string()),
            client_id: Some("client-1".to_string()),
            ..AblyAnnotation::default()
        },
        None,
        Some("moderator"),
    )
    .unwrap();

    let AblyAnnotationCommand::Delete(selector) = command else {
        panic!("expected delete command");
    };
    assert_eq!(selector.message_serial.as_str(), "msg:1");
    assert_eq!(selector.annotation_type.as_str(), "reaction:distinct.v1");
    assert_eq!(selector.id.unwrap().as_str(), "annotation-id");
    assert_eq!(selector.target_serial.unwrap().as_str(), "ann:7");
    assert_eq!(selector.name.as_deref(), Some("like"));
    assert_eq!(selector.client_id.as_deref(), Some("client-1"));
}

#[test]
fn create_annotation_rejects_authenticated_client_id_spoofing() {
    let result = parse_ably_annotation_command(
        AblyAnnotation {
            action: Some(0),
            message_serial: Some("msg:1".to_string()),
            annotation_type: Some("reaction:flag.v1".to_string()),
            client_id: Some("other-client".to_string()),
            ..AblyAnnotation::default()
        },
        None,
        Some("authenticated-client"),
    );

    assert!(result.is_err());
}

#[test]
fn recovery_projects_stored_annotation_as_action_21() {
    let event = AnnotationEventData {
        action: AnnotationEventAction::Create,
        id: Some("annotation-id".to_string()),
        serial: "ann:2".to_string(),
        message_serial: "msg:1".to_string(),
        annotation_type: "reaction:distinct.v1".to_string(),
        name: Some("like".to_string()),
        client_id: Some("client-1".to_string()),
        count: None,
        data: Some(json!({ "source": "recovery" })),
        encoding: Some("json".to_string()),
        timestamp: 123,
    };
    let message = PusherMessage {
        event: Some(ANNOTATION_EVENT_NAME.to_string()),
        channel: Some("mutable:room".to_string()),
        data: Some(MessageData::Json(sonic_rs::to_value(&event).unwrap())),
        name: None,
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: None,
        stream_id: Some("stream".to_string()),
        serial: Some(7),
        idempotency_key: None,
        extras: None,
        delta_sequence: None,
        delta_conflation_key: None,
    };

    let projected = ably_protocol_message_from_envelope(
        "mutable:room",
        &message,
        &MessageEnvelope::default(),
        AblyMessageProjection::Mutation,
        Some("stream:7".to_string()),
    )
    .expect("annotation recovery projection");

    assert_eq!(projected.action, ACTION_ANNOTATION);
    assert_eq!(projected.channel_serial.as_deref(), Some("stream:7"));
    let annotation = &projected.annotations.unwrap()[0];
    assert_eq!(annotation.serial.as_deref(), Some("ann:2"));
    assert_eq!(annotation.message_serial.as_deref(), Some("msg:1"));
    assert_eq!(annotation.data, Some(json!({ "source": "recovery" })));
    assert_eq!(annotation.encoding.as_deref(), Some("json"));
    assert_eq!(annotation.timestamp, Some(123));
}

#[test]
fn recovery_projects_annotation_summary_with_original_message_serial() {
    let message = PusherMessage {
        event: Some(MESSAGE_SUMMARY_EVENT_NAME.to_string()),
        channel: Some("mutable:room".to_string()),
        data: Some(MessageData::Json(json!({
            "action": "message.summary",
            "serial": "msg:original",
            "annotations": {
                "summary": {
                    "reaction:distinct.v1": {
                        "like": { "total": 2, "clientIds": ["a", "b"], "clipped": false }
                    }
                }
            }
        }))),
        name: None,
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: None,
        stream_id: Some("stream".to_string()),
        serial: Some(8),
        idempotency_key: None,
        extras: None,
        delta_sequence: None,
        delta_conflation_key: None,
    };

    let projected = ably_protocol_message_from_envelope(
        "mutable:room",
        &message,
        &MessageEnvelope {
            published_at_ms: Some(456),
            ..MessageEnvelope::default()
        },
        AblyMessageProjection::Mutation,
        Some("stream:8".to_string()),
    )
    .expect("annotation summary recovery projection");

    assert_eq!(projected.action, ACTION_MESSAGE);
    assert_eq!(projected.channel_serial.as_deref(), Some("stream:8"));
    assert_eq!(projected.timestamp, Some(456));
    let summary = &projected.messages.unwrap()[0];
    assert_eq!(summary.action, Some(MESSAGE_SUMMARY));
    assert_eq!(summary.serial.as_deref(), Some("msg:original"));
    assert_eq!(
        summary.annotations.as_ref().unwrap()["summary"]["reaction:distinct.v1"]["like"]["total"]
            .as_u64(),
        Some(2)
    );
}

#[test]
fn attach_options_default_to_supported_channel_modes() {
    let options = AblyAttachOptions::from_wire(None, None);

    assert_eq!(options.mode_flags, ABLY_DEFAULT_MODE_FLAGS);
    assert!(options.params.is_empty());
}

#[test]
fn attach_params_modes_override_protocol_mode_flags() {
    let options = AblyAttachOptions::from_wire(
        Some(ABLY_MODE_PUBLISH),
        Some(HashMap::from([
            ("modes".to_string(), "presence,subscribe".to_string()),
            ("delta".to_string(), "vcdiff".to_string()),
        ])),
    );

    assert_eq!(options.mode_flags, ABLY_MODE_PRESENCE | ABLY_MODE_SUBSCRIBE);
    assert_eq!(
        options.params.get("delta").map(String::as_str),
        Some("vcdiff")
    );
}

#[test]
fn attach_options_drop_unknown_params_and_unknown_mode_bits() {
    let options = AblyAttachOptions::from_wire(
        Some(u64::MAX),
        Some(HashMap::from([(
            "nonexistent".to_string(),
            "value".to_string(),
        )])),
    );

    assert_eq!(options.mode_flags, ABLY_MODE_MASK);
    assert!(options.params.is_empty());
}

#[test]
fn attach_options_accept_only_vcdiff_delta_mode() {
    let unsupported = AblyAttachOptions::from_wire(
        None,
        Some(HashMap::from([(
            "delta".to_string(),
            "unsupported".to_string(),
        )])),
    );
    let accepted = AblyAttachOptions::from_wire(
        None,
        Some(HashMap::from([("delta".to_string(), "VCDIFF".to_string())])),
    );

    assert!(!unsupported.params.contains_key("delta"));
    assert_eq!(
        accepted.params.get("delta").map(String::as_str),
        Some("vcdiff")
    );
}

#[test]
fn attached_channel_modes_deny_only_explicitly_missing_operations() {
    let modes = HashMap::from([(
        "channel".to_string(),
        AblyConnectionAttachment {
            channel: AblyChannelName::parse("channel".to_string()).unwrap(),
            params: HashMap::new(),
            mode_flags: ABLY_MODE_SUBSCRIBE,
            explicit_modes: true,
            filter: None,
            attach_position: None,
            has_presence: false,
        },
    )]);

    assert!(attached_channel_mode_denies(
        &modes,
        Some("channel"),
        ABLY_MODE_PUBLISH
    ));
    assert!(!attached_channel_mode_denies(
        &modes,
        Some("channel"),
        ABLY_MODE_SUBSCRIBE
    ));
    assert!(!attached_channel_mode_denies(
        &modes,
        Some("unattached"),
        ABLY_MODE_PUBLISH
    ));
}

#[test]
fn derived_filter_cache_reports_hits_and_misses() {
    let hub = AblyCompatHub::default();
    let source = "headers.kind == `\"accepted\"`";
    let encoded = base64::engine::general_purpose::STANDARD.encode(source);
    let channel = AblyChannelName::parse(format!("[filter={encoded}]cache-channel")).unwrap();

    assert!(hub.message_filter(&channel).unwrap().is_some());
    assert!(hub.message_filter(&channel).unwrap().is_some());

    let metrics = hub.metrics.snapshot();
    assert_eq!(metrics.filter_cache_misses, 1);
    assert_eq!(metrics.filter_cache_hits, 1);
    assert_eq!(metrics.filter_cache_entries, 1);
    assert!(metrics.filter_cache_bytes >= source.len());
}

#[test]
fn derived_filter_cache_evicts_at_the_entry_bound() {
    let hub = AblyCompatHub::default();
    for index in 0..=ABLY_FILTER_CACHE_MAX_ENTRIES {
        let source = format!("headers.cacheKey == `{index}`");
        let encoded = base64::engine::general_purpose::STANDARD.encode(source);
        let channel = AblyChannelName::parse(format!("[filter={encoded}]cache-channel")).unwrap();
        assert!(hub.message_filter(&channel).unwrap().is_some());
    }

    let metrics = hub.metrics.snapshot();
    assert_eq!(metrics.filter_cache_entries, ABLY_FILTER_CACHE_MAX_ENTRIES);
    assert!(metrics.filter_cache_bytes <= ABLY_FILTER_CACHE_MAX_BYTES);
    assert_eq!(metrics.filter_cache_evictions, 1);
}

#[test]
fn heartbeat_response_echoes_correlation_id() {
    let response = heartbeat_response(AblyProtocolMessage {
        action: ACTION_HEARTBEAT,
        id: Some("ping-1".to_string()),
        ..empty_protocol_message(ACTION_HEARTBEAT)
    });

    assert_eq!(response.action, ACTION_HEARTBEAT);
    assert_eq!(response.id.as_deref(), Some("ping-1"));
}

#[test]
fn realtime_message_ids_are_stable_across_unacked_retries() {
    assert_eq!(
        ably_realtime_message_id("connection-a", 7, 2),
        "connection-a:7:2"
    );
}

#[test]
fn close_and_disconnect_actions_have_distinct_terminal_outcomes() {
    assert_eq!(
        ably_protocol_control(ACTION_CLOSE),
        AblyProtocolControl::Close
    );
    assert_eq!(
        ably_protocol_control(ACTION_DISCONNECT),
        AblyProtocolControl::Disconnect
    );
    assert_eq!(
        ably_protocol_control(ACTION_HEARTBEAT),
        AblyProtocolControl::Continue
    );
}

#[test]
fn presence_reentry_preserves_only_valid_same_connection_ids() {
    let connection_id = "connection-a";
    assert_eq!(
        ably_presence_message_id(Some("connection-a:4:2"), connection_id, 9, 1),
        "connection-a:4:2"
    );
    assert_eq!(
        ably_presence_message_id(Some("connection-b:4:2"), connection_id, 9, 1),
        "connection-a:9:1"
    );
    assert_eq!(
        ably_presence_message_id(Some("connection-a:not-a-serial:2"), connection_id, 9, 1),
        "connection-a:9:1"
    );
    assert_eq!(
        ably_presence_message_id(None, connection_id, 9, 1),
        "connection-a:9:1"
    );
}

#[test]
fn durable_presence_payload_decodes_nested_json_data() {
    let record = PresenceRecord {
        id: "connection-a:client-a:0".to_string(),
        client_id: "client-a".to_string(),
        connection_id: "connection-a".to_string(),
        data: Some(json!({"profile": {"name": "Ada"}})),
        encoding: None,
        extras: None,
        timestamp_ms: 42,
    };
    let encoded = sonic_rs::to_vec(&record).unwrap();
    let value: Value = sonic_rs::from_slice(&encoded).unwrap();

    assert_eq!(decode_presence_record(&value), Some(record));
}

#[tokio::test]
async fn malformed_recover_and_resume_keys_use_invalid_format_error() {
    let hub = AblyCompatHub::default();

    let AblyConnectionStart::Failed { error: recover } = hub
        .begin_connection("app", None, None, Some("not-a-connection-key"))
        .await
    else {
        panic!("malformed recovery key must fail");
    };
    let AblyConnectionStart::Failed { error: resume } = hub
        .begin_connection("app", None, Some("not-a-connection-key"), None)
        .await
    else {
        panic!("malformed resume key must fail");
    };

    assert_eq!(recover.code, 80018);
    assert_eq!(resume.code, 80018);
}

#[tokio::test]
async fn missing_well_formed_recover_and_resume_keys_use_expired_error() {
    let hub = AblyCompatHub::default();
    let missing_key = "app:00000000000000000000000000000000";

    let AblyConnectionStart::Failed { error: recover } = hub
        .begin_connection("app", None, None, Some(missing_key))
        .await
    else {
        panic!("missing recovery key must fail");
    };
    let AblyConnectionStart::Failed { error: resume } = hub
        .begin_connection("app", None, Some(missing_key), None)
        .await
    else {
        panic!("missing resume key must fail");
    };

    assert_eq!(recover.code, 80008);
    assert_eq!(resume.code, 80008);
}

#[test]
fn connection_key_validation_is_bounded_and_matches_server_issued_shape() {
    assert!(is_well_formed_connection_key(
        "app:0123456789abcdef0123456789abcdef"
    ));
    assert!(!is_well_formed_connection_key("app"));
    assert!(!is_well_formed_connection_key("app:not-hex"));
    assert!(!is_well_formed_connection_key(
        &"a".repeat(ABLY_CONNECTION_KEY_MAX_BYTES + 1)
    ));
}

#[tokio::test]
async fn recover_key_survives_handoff_to_another_runtime_node() {
    let cache: Arc<dyn CacheManager> = Arc::new(MemoryCacheManager::new(
        "ably-session-recovery".to_string(),
        MemoryCacheOptions::default(),
    ));
    let first_node = AblyCompatHub {
        cache: Some(Arc::clone(&cache)),
        ..AblyCompatHub::default()
    };
    let second_node = AblyCompatHub {
        cache: Some(cache),
        ..AblyCompatHub::default()
    };
    first_node
        .remember_connection(
            "app:00000000000000000000000000000001".to_string(),
            "app",
            "connection-a",
            Some("client-a".to_string()),
        )
        .await;

    let recovered = second_node
        .begin_connection(
            "app",
            Some("client-a"),
            None,
            Some("app:00000000000000000000000000000001"),
        )
        .await;
    assert!(matches!(
        recovered,
        AblyConnectionStart::Resumed { connection_id }
            if connection_id == "connection-a"
    ));
}

#[tokio::test]
async fn auth_key_rotation_invalidates_previous_key_on_every_runtime() {
    let cache: Arc<dyn CacheManager> = Arc::new(MemoryCacheManager::new(
        "ably-session-key-rotation".to_string(),
        MemoryCacheOptions::default(),
    ));
    let first_node = AblyCompatHub {
        cache: Some(Arc::clone(&cache)),
        ..AblyCompatHub::default()
    };
    let second_node = AblyCompatHub {
        cache: Some(cache),
        ..AblyCompatHub::default()
    };
    first_node
        .remember_connection(
            "app:00000000000000000000000000000001".to_string(),
            "app",
            "connection-a",
            Some("client-a".to_string()),
        )
        .await;
    assert!(matches!(
        second_node
            .begin_connection(
                "app",
                Some("client-a"),
                None,
                Some("app:00000000000000000000000000000001"),
            )
            .await,
        AblyConnectionStart::Resumed { .. }
    ));
    first_node
        .replace_connection_key(
            "app:00000000000000000000000000000001",
            "app:00000000000000000000000000000002".to_string(),
            "app",
            "connection-a",
            Some("client-a".to_string()),
        )
        .await;

    assert!(matches!(
        second_node
            .begin_connection(
                "app",
                Some("client-a"),
                None,
                Some("app:00000000000000000000000000000001"),
            )
            .await,
        AblyConnectionStart::Failed { .. }
    ));
    assert!(matches!(
        second_node
            .begin_connection(
                "app",
                Some("client-a"),
                None,
                Some("app:00000000000000000000000000000002"),
            )
            .await,
        AblyConnectionStart::Resumed { .. }
    ));
}

#[tokio::test]
async fn recovered_session_owner_supersedes_the_previous_node_atomically() {
    let cache: Arc<dyn CacheManager> = Arc::new(MemoryCacheManager::new(
        "ably-session-owner".to_string(),
        MemoryCacheOptions::default(),
    ));
    let first_node = AblyCompatHub {
        cache: Some(Arc::clone(&cache)),
        ..AblyCompatHub::default()
    };
    let second_node = AblyCompatHub {
        cache: Some(cache),
        ..AblyCompatHub::default()
    };

    first_node
        .claim_session_owner("app", "connection-a", "session-a")
        .await
        .unwrap();
    assert!(
        first_node
            .session_is_current("app", "connection-a", "session-a")
            .await
    );

    second_node
        .claim_session_owner("app", "connection-a", "session-b")
        .await
        .unwrap();
    assert!(
        !first_node
            .session_is_current("app", "connection-a", "session-a")
            .await
    );
    assert!(
        second_node
            .session_is_current("app", "connection-a", "session-b")
            .await
    );
    assert!(
        !first_node
            .refresh_session_owner("app", "connection-a", "session-a")
            .await
    );

    first_node
        .release_session_owner("app", "connection-a", "session-a")
        .await;
    assert!(
        second_node
            .session_is_current("app", "connection-a", "session-b")
            .await
    );
    second_node
        .release_session_owner("app", "connection-a", "session-b")
        .await;
    assert!(
        !first_node
            .session_is_current("app", "connection-a", "session-b")
            .await
    );
}

#[tokio::test]
async fn active_connection_lease_refreshes_and_graceful_close_invalidates_it() {
    let hub = AblyCompatHub::default();
    hub.remember_connection(
        "app:00000000000000000000000000000001".to_string(),
        "app",
        "connection-a",
        Some("client-a".to_string()),
    )
    .await;
    hub.sessions
        .get_mut("app:00000000000000000000000000000001")
        .expect("stored lease")
        .expires_at_ms = 0;

    hub.remember_connection(
        "app:00000000000000000000000000000001".to_string(),
        "app",
        "connection-a",
        Some("client-a".to_string()),
    )
    .await;
    assert!(matches!(
        hub.begin_connection(
            "app",
            Some("client-a"),
            None,
            Some("app:00000000000000000000000000000001"),
        )
        .await,
        AblyConnectionStart::Resumed { .. }
    ));

    hub.forget_connection("app:00000000000000000000000000000001")
        .await;
    assert!(matches!(
        hub.begin_connection(
            "app",
            Some("client-a"),
            None,
            Some("app:00000000000000000000000000000001"),
        )
        .await,
        AblyConnectionStart::Failed { .. }
    ));
}

#[test]
fn batch_publish_body_decodes_single_message_without_expansion() {
    let request: AblyBatchPublishRequest = decode_value(
        br#"{"channels":["one","two"],"messages":{"name":"event","data":"value"}}"#,
        AblyFormat::Json,
    )
    .expect("valid batch body");

    assert_eq!(request.channels, ["one", "two"]);
    let messages = request.messages.into_messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].name.as_deref(), Some("event"));
}

#[test]
fn batch_publish_accepts_single_and_many_request_shapes() {
    let single: AblyBatchPublishBody = decode_value(
        br#"{"channels":["one"],"messages":{"name":"event"}}"#,
        AblyFormat::Json,
    )
    .expect("single request object");
    assert!(matches!(single, AblyBatchPublishBody::One(_)));

    let many: AblyBatchPublishBody = decode_value(
        br#"[{"channels":["one"],"messages":{"name":"event"}}]"#,
        AblyFormat::Json,
    )
    .expect("request array");
    assert!(matches!(many, AblyBatchPublishBody::Many(requests) if requests.len() == 1));
}

#[tokio::test]
async fn legacy_batch_publish_partial_failure_uses_browser_decodable_40020_envelope() {
    for requested_format in [AblyFormat::Json, AblyFormat::MsgPack] {
        let responses = vec![
            AblyBatchPublishChannelResponse::Success {
                channel: "valid".to_string(),
                message_id: "message-id".to_string(),
                serials: vec!["serial-1".to_string()],
            },
            AblyBatchPublishChannelResponse::Failure {
                channel: "[invalid".to_string(),
                error: error_info(StatusCode::BAD_REQUEST, 40010, "invalid channel"),
            },
        ];

        let response = encode_ably_legacy_batch_publish_response(requested_format, responses)
            .expect("partial response encodes");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response
                .headers()
                .get("x-ably-errorcode")
                .and_then(|value| value.to_str().ok()),
            Some("40020")
        );
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/json")
        );
        let body = axum::body::to_bytes(response.into_body(), 4 * 1024)
            .await
            .expect("response body");
        let value: Value = decode_value(body.as_ref(), AblyFormat::Json).expect("JSON response");
        assert_eq!(value["error"]["code"].as_u64(), Some(40020));
        assert_eq!(value["error"]["statusCode"].as_u64(), Some(400));
        assert_eq!(
            value["batchResponse"].as_array().map(|items| items.len()),
            Some(2)
        );
        assert_eq!(value["batchResponse"][0]["channel"].as_str(), Some("valid"));
        assert_eq!(
            value["batchResponse"][1]["error"]["code"].as_u64(),
            Some(40010)
        );
    }
}

#[tokio::test]
async fn bounded_ordered_work_limits_inflight_and_preserves_input_order() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let results = run_bounded_ordered((0usize..12).collect(), 3, {
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);
        move |item| {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            async move {
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::SeqCst);
                item
            }
        }
    })
    .await
    .expect("bounded work completes");

    assert_eq!(results, (0usize..12).collect::<Vec<_>>());
    assert!(peak.load(Ordering::SeqCst) <= 3);
}

#[test]
fn presence_registry_projects_present_and_removes_leave_from_base_state() {
    let hub = AblyCompatHub::default();
    let member = AblyPresenceMessage {
        id: Some("connection:1:0".to_string()),
        action: Some(2),
        client_id: Some("client".to_string()),
        connection_id: Some("connection".to_string()),
        data: Some(json!("data")),
        encoding: None,
        timestamp: Some(1),
        extras: Some(json!({ "headers": { "key": "value" } })),
    };

    hub.presence_registry
        .register_connection("app", "connection");
    hub.presence_registry
        .enter(
            "app",
            "base",
            presence_record_from_ably(&member).expect("valid member"),
        )
        .expect("enter accepted");
    let snapshot = hub.presence_snapshot("app", "base");
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].action, Some(1));
    assert_eq!(snapshot[0].extras, member.extras);

    hub.presence_registry
        .leave("app", "base", "connection", "client")
        .expect("leave accepted");
    assert!(hub.presence_snapshot("app", "base").is_empty());
}

#[tokio::test]
async fn attach_gate_delivers_only_messages_after_captured_high_water() {
    let hub = AblyCompatHub::default();
    let (sender, mut receiver) = AblyOutbound::channel(
        AblyFormat::Json,
        OutboundLimits::default(),
        Arc::clone(&hub.metrics),
    );
    let channel = AblyChannelName::parse("attach-race".to_string()).unwrap();
    let attachment = AblyAttachment {
        connection_id: "connection",
        session_id: "session",
        sender: Arc::clone(&sender),
        filter: None,
        params: HashMap::new(),
        mode_flags: ABLY_DEFAULT_MODE_FLAGS,
        echo: true,
        presence: Vec::new(),
    };
    hub.begin_attach("app", &channel, &attachment)
        .await
        .unwrap();
    for serial in [1, 2] {
        hub.broadcast(
            "app",
            channel.base(),
            AblyProtocolMessage {
                action: ACTION_MESSAGE,
                channel: Some(channel.base().to_string()),
                channel_serial: Some(encode_ably_channel_serial("stream-1", serial)),
                messages: Some(vec![AblyMessage {
                    id: Some(format!("message-{serial}")),
                    ..AblyMessage::default()
                }]),
                ..empty_protocol_message(ACTION_MESSAGE)
            },
            None,
            None,
        );
    }
    hub.attach_clean(
        "app",
        &channel,
        attachment,
        Some(encode_ably_channel_serial("stream-1", 1)),
        Vec::new(),
    );

    let attached = receiver.recv().await.expect("ATTACHED frame");
    let attached = decode_protocol_bytes(attached.bytes.as_ref(), AblyFormat::Json).unwrap();
    assert_eq!(attached.action, ACTION_ATTACHED);
    let delivered = receiver.recv().await.expect("post-high-water message");
    let delivered = decode_protocol_bytes(delivered.bytes.as_ref(), AblyFormat::Json).unwrap();
    assert_eq!(delivered.channel_serial.as_deref(), Some("stream-1:2"));
    assert_eq!(
        delivered
            .messages
            .as_ref()
            .and_then(|messages| messages.first())
            .and_then(|message| message.id.as_deref()),
        Some("message-2")
    );
}

#[tokio::test]
async fn duplicate_remote_delivery_position_reaches_local_ably_subscriber_once() {
    let hub = AblyCompatHub::default();
    let (sender, _receiver) = AblyOutbound::channel(
        AblyFormat::Json,
        OutboundLimits::default(),
        Arc::clone(&hub.metrics),
    );
    let channel = AblyChannelName::parse("distributed-room".to_string()).unwrap();
    hub.attach_clean(
        "app",
        &channel,
        AblyAttachment {
            connection_id: "subscriber-connection",
            session_id: "subscriber-session",
            sender,
            filter: None,
            params: HashMap::new(),
            mode_flags: ABLY_DEFAULT_MODE_FLAGS,
            echo: true,
            presence: Vec::new(),
        },
        None,
        Vec::new(),
    );
    let message = PusherMessage {
        event: Some("chat.message".to_string()),
        channel: Some(channel.base().to_string()),
        data: Some(MessageData::String("hello".to_string())),
        name: None,
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: Some("message-1".to_string()),
        stream_id: Some("stream-1".to_string()),
        serial: Some(7),
        idempotency_key: None,
        extras: None,
        delta_sequence: None,
        delta_conflation_key: None,
    };
    let mut envelope = MessageEnvelope::from_message(&message, None, None, 1).unwrap();
    envelope.set_commit_positions(Some("stream-1".to_string()), Some(7), Some(7));

    RealtimeEgressTap::deliver(&hub, "app", channel.base(), &message, &envelope).unwrap();
    RealtimeEgressTap::deliver(&hub, "app", channel.base(), &message, &envelope).unwrap();

    assert_eq!(hub.metrics.snapshot().fanout, 1);
    assert_eq!(hub.metrics.snapshot().data_encoded, 1);
    assert_eq!(hub.metrics.snapshot().duplicate_suppression, 1);
}

#[test]
fn contiguous_delivery_frontier_suppresses_old_duplicates_without_growing_memory() {
    let hub = AblyCompatHub::default();
    let envelope = |serial| MessageEnvelope {
        stream_id: Some("stream-1".to_string()),
        delivery_serial: Some(serial),
        ..MessageEnvelope::default()
    };

    for serial in 1..=(ABLY_DELIVERY_DEDUPE_MAX_ENTRIES as u64 + 128) {
        assert!(
            hub.accept_delivery_position("app", "channel", &envelope(serial))
                .unwrap()
        );
    }
    assert!(
        !hub.accept_delivery_position("app", "channel", &envelope(1))
            .unwrap()
    );

    let state = hub.channel_state("app", "channel");
    let state = lock_channel_state(&state);
    assert_eq!(
        state.delivery_frontier,
        ABLY_DELIVERY_DEDUPE_MAX_ENTRIES as u64 + 128
    );
    assert!(state.pending_deliveries.is_empty());
    assert!(!state.delivery_reset_required);
}

#[test]
fn out_of_order_delivery_is_emitted_once_then_folded_into_the_frontier() {
    let hub = AblyCompatHub::default();
    let envelope = |serial| MessageEnvelope {
        stream_id: Some("stream-1".to_string()),
        delivery_serial: Some(serial),
        ..MessageEnvelope::default()
    };

    assert!(
        hub.accept_delivery_position("app", "channel", &envelope(1))
            .unwrap()
    );
    assert!(
        hub.accept_delivery_position("app", "channel", &envelope(3))
            .unwrap()
    );
    assert!(
        !hub.accept_delivery_position("app", "channel", &envelope(3))
            .unwrap()
    );
    assert!(
        hub.accept_delivery_position("app", "channel", &envelope(2))
            .unwrap()
    );
    assert!(
        !hub.accept_delivery_position("app", "channel", &envelope(3))
            .unwrap()
    );

    let state = hub.channel_state("app", "channel");
    let state = lock_channel_state(&state);
    assert_eq!(state.delivery_frontier, 3);
    assert!(state.pending_deliveries.is_empty());
}

#[test]
fn excessive_delivery_reordering_fails_closed_and_requires_reset() {
    let hub = AblyCompatHub::default();
    let envelope = |serial| MessageEnvelope {
        stream_id: Some("stream-1".to_string()),
        delivery_serial: Some(serial),
        ..MessageEnvelope::default()
    };

    for serial in 2..=(ABLY_DELIVERY_DEDUPE_MAX_ENTRIES as u64 + 1) {
        assert!(
            hub.accept_delivery_position("app", "channel", &envelope(serial))
                .unwrap()
        );
    }
    let error = hub
        .accept_delivery_position(
            "app",
            "channel",
            &envelope(ABLY_DELIVERY_DEDUPE_MAX_ENTRIES as u64 + 2),
        )
        .unwrap_err();
    assert!(matches!(error, SockudoError::BufferFull(_)));
    assert!(
        hub.accept_delivery_position("app", "channel", &envelope(1))
            .is_err()
    );
    let metrics = hub.metrics.snapshot();
    assert_eq!(metrics.continuity_lost, 1);
    assert_eq!(metrics.reset_required, 1);
}

#[tokio::test]
async fn presence_sync_chunks_110_members_in_stable_order_with_continuation() {
    let metrics = Arc::new(OutboundMetrics::default());
    let (sender, mut receiver) =
        AblyOutbound::channel(AblyFormat::Json, OutboundLimits::default(), metrics);
    let members = (0..110)
        .map(|index| AblyPresenceMessage {
            id: Some(format!("connection:{index}:0")),
            action: Some(1),
            client_id: Some(format!("client-{index:03}")),
            connection_id: Some("connection".to_string()),
            timestamp: Some(index),
            ..AblyPresenceMessage::default()
        })
        .collect::<Vec<_>>();

    send_presence_sync(&sender, "room", members);

    let first = receiver.recv().await.expect("first SYNC frame");
    let first = decode_protocol_bytes(first.bytes.as_ref(), AblyFormat::Json).unwrap();
    let second = receiver.recv().await.expect("final SYNC frame");
    let second = decode_protocol_bytes(second.bytes.as_ref(), AblyFormat::Json).unwrap();
    assert_eq!(first.action, ACTION_SYNC);
    assert_eq!(first.channel_serial.as_deref(), Some("presence:1"));
    assert_eq!(first.presence.as_ref().map(Vec::len), Some(100));
    assert_eq!(second.channel_serial.as_deref(), Some("presence:"));
    assert_eq!(second.presence.as_ref().map(Vec::len), Some(10));
    assert_eq!(
        second
            .presence
            .as_ref()
            .and_then(|presence| presence.first())
            .and_then(|member| member.client_id.as_deref()),
        Some("client-100")
    );
}

#[tokio::test]
async fn attached_sets_has_presence_even_when_no_other_flags_are_present() {
    let metrics = Arc::new(OutboundMetrics::default());
    let (sender, mut receiver) =
        AblyOutbound::channel(AblyFormat::Json, OutboundLimits::default(), metrics);
    let member = AblyPresenceMessage {
        id: Some("connection:1:0".to_string()),
        action: Some(1),
        client_id: Some("client".to_string()),
        connection_id: Some("connection".to_string()),
        ..AblyPresenceMessage::default()
    };

    send_ably_attached(
        &sender,
        "room",
        None,
        None,
        None,
        vec![member],
        None,
        Vec::new(),
        None,
    );

    let attached = receiver.recv().await.expect("ATTACHED frame");
    let attached = decode_protocol_bytes(attached.bytes.as_ref(), AblyFormat::Json).unwrap();
    assert_eq!(attached.flags, Some(FLAG_HAS_PRESENCE));
    let sync = receiver.recv().await.expect("SYNC frame");
    let sync = decode_protocol_bytes(sync.bytes.as_ref(), AblyFormat::Json).unwrap();
    assert_eq!(sync.action, ACTION_SYNC);
}

#[test]
fn presence_replication_converges_two_isolated_node_registries() {
    let node_a = AblyCompatHub::default();
    let node_b = AblyCompatHub::default();
    let record = |connection_id: &str| PresenceRecord {
        connection_id: connection_id.to_string(),
        client_id: "shared-client".to_string(),
        id: format!("{connection_id}:1:0"),
        data: Some(json!({ "node": connection_id })),
        encoding: Some("json".to_string()),
        extras: None,
        timestamp_ms: 1,
    };
    let enter = |connection_id: &str| PresenceChange {
        action: PresenceChangeAction::Enter,
        member: record(connection_id),
        wire_id: Some(format!("{connection_id}:1:0")),
    };
    let enters = PresenceReplication {
        changes: vec![enter("connection-a"), enter("connection-b")],
        unregister_connection: None,
    };

    node_a.replicate_presence("app", "room", &enters).unwrap();
    node_b.replicate_presence("app", "room", &enters).unwrap();
    assert_eq!(node_a.presence_registry.snapshot("app", "room").len(), 2);
    assert_eq!(
        node_a.presence_registry.snapshot("app", "room"),
        node_b.presence_registry.snapshot("app", "room")
    );

    let leaves = PresenceReplication {
        changes: ["connection-a", "connection-b"]
            .into_iter()
            .map(|connection_id| PresenceChange {
                action: PresenceChangeAction::Leave,
                member: record(connection_id),
                wire_id: Some(format!("{connection_id}:2:0")),
            })
            .collect(),
        unregister_connection: None,
    };
    node_a.replicate_presence("app", "room", &leaves).unwrap();
    node_b.replicate_presence("app", "room", &leaves).unwrap();
    assert!(node_a.presence_registry.snapshot("app", "room").is_empty());
    assert!(node_b.presence_registry.snapshot("app", "room").is_empty());
}

#[tokio::test]
async fn qualified_and_base_subscriptions_share_state_but_keep_wire_names() {
    let hub = AblyCompatHub::default();
    let metrics = Arc::clone(&hub.metrics);
    let (sender, mut receiver) =
        AblyOutbound::channel(AblyFormat::Json, OutboundLimits::default(), metrics);
    let base = AblyChannelName::parse("rooms:all".to_string()).expect("valid base channel");
    let qualified = AblyChannelName::parse("[filter=name == `message`]rooms:all".to_string())
        .expect("valid qualified channel");

    hub.attach_clean(
        "app",
        &base,
        AblyAttachment {
            connection_id: "connection",
            session_id: "session",
            sender: Arc::clone(&sender),
            filter: None,
            params: HashMap::new(),
            mode_flags: ABLY_DEFAULT_MODE_FLAGS,
            echo: true,
            presence: Vec::new(),
        },
        None,
        Vec::new(),
    );
    hub.attach_clean(
        "app",
        &qualified,
        AblyAttachment {
            connection_id: "connection",
            session_id: "session",
            sender: Arc::clone(&sender),
            filter: None,
            params: HashMap::new(),
            mode_flags: ABLY_DEFAULT_MODE_FLAGS,
            echo: true,
            presence: Vec::new(),
        },
        None,
        Vec::new(),
    );
    assert_eq!(
        hub.channels.len(),
        1,
        "qualifier must not create channel state"
    );
    assert_eq!(
        lock_channel_state(
            hub.channels
                .get(&channel_key("app", "rooms:all"))
                .expect("base state exists")
                .value()
        )
        .subscribers
        .len(),
        2,
        "one session may attach both base and qualified identities"
    );

    for expected in [base.requested(), qualified.requested()] {
        let frame = receiver.recv().await.expect("ATTACHED frame");
        let decoded = decode_protocol_bytes(frame.bytes.as_ref(), AblyFormat::Json)
            .expect("valid JSON protocol frame");
        assert_eq!(decoded.channel.as_deref(), Some(expected));
    }

    hub.broadcast(
        "app",
        "rooms:all",
        AblyProtocolMessage {
            action: ACTION_MESSAGE,
            channel: Some("rooms:all".to_string()),
            messages: Some(Vec::new()),
            ..empty_protocol_message(ACTION_MESSAGE)
        },
        None,
        None,
    );
    let mut delivered_channels = Vec::new();
    for _ in 0..2 {
        let frame = receiver.recv().await.expect("MESSAGE frame");
        let decoded = decode_protocol_bytes(frame.bytes.as_ref(), AblyFormat::Json)
            .expect("valid JSON protocol frame");
        delivered_channels.push(decoded.channel.expect("delivery channel"));
    }
    delivered_channels.sort();
    let mut expected = vec![
        base.requested().to_string(),
        qualified.requested().to_string(),
    ];
    expected.sort();
    assert_eq!(delivered_channels, expected);
}

#[test]
fn ably_key_parses_key_and_secret() {
    assert_eq!(
        parse_ably_key("app-key:secret"),
        ("app-key", Some("secret"))
    );
    assert_eq!(parse_ably_key("app-key"), ("app-key", None));
}

#[test]
fn qualifier_wildcard_capability_matches_qualified_and_base_channels() {
    let capabilities = ably_capability_value_to_sockudo(&serde_json::json!({
        "[*]*": ["subscribe"]
    }))
    .expect("valid qualifier capability");
    let qualified = AblyChannelName::parse("[filter=name == `message`]chan".to_string())
        .expect("valid qualified channel");
    assert!(
        ensure_ably_channel_capability(
            Some(&capabilities),
            &qualified,
            AblyCapabilityCheck::Subscribe,
        )
        .is_ok()
    );
    let base = AblyChannelName::parse("chan".to_string()).expect("valid base channel");
    assert!(
        ensure_ably_channel_capability(Some(&capabilities), &base, AblyCapabilityCheck::Subscribe,)
            .is_ok(),
        "Ably's [*]* resource is the fixture's global channel wildcard"
    );
}

#[test]
fn token_request_mac_uses_canonical_newline_terminated_input() {
    type HmacSha256 = Hmac<Sha256>;
    let input = token_request_signing_input(
        "app.key",
        Some(1234),
        Some(r#"{"chat":["subscribe"]}"#),
        Some("client"),
        1_700_000_000_000,
        "nonce",
    );
    assert_eq!(
        input,
        "app.key\n1234\n{\"chat\":[\"subscribe\"]}\nclient\n1700000000000\nnonce\n"
    );
    let mut signer = HmacSha256::new_from_slice(b"secret").unwrap();
    signer.update(input.as_bytes());
    let mac = general_purpose::STANDARD.encode(signer.finalize().into_bytes());
    assert!(verify_token_request_mac("secret", &input, &mac));
    assert!(!verify_token_request_mac("wrong", &input, &mac));
    assert!(!verify_token_request_mac("secret", "different", &mac));
}

#[test]
fn jwt_timestamp_conversion_rejects_seconds_that_overflow_milliseconds() {
    let maximum_valid = i64::MAX / 1_000;
    let minimum_valid = i64::MIN / 1_000;
    assert_eq!(
        jwt_seconds_to_millis(maximum_valid).unwrap(),
        maximum_valid * 1_000
    );
    assert_eq!(
        jwt_seconds_to_millis(minimum_valid).unwrap(),
        minimum_valid * 1_000
    );

    for overflowing in [maximum_valid + 1, minimum_valid - 1, i64::MAX, i64::MIN] {
        let error = jwt_seconds_to_millis(overflowing).unwrap_err();
        assert_eq!(error.status, StatusCode::UNAUTHORIZED);
        assert_eq!(error.code, 40140);
        assert_eq!(error.message, "JWT claims are invalid");
    }
}

fn sign_test_jwt(header: serde_json::Value, claims: serde_json::Value, secret: &str) -> String {
    type HmacSha256 = Hmac<Sha256>;
    let header = general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap());
    let claims = general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
    let input = format!("{header}.{claims}");
    let mut signer = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    signer.update(input.as_bytes());
    format!(
        "{input}.{}",
        general_purpose::URL_SAFE_NO_PAD.encode(signer.finalize().into_bytes())
    )
}

#[test]
fn embedded_jwt_verifies_signature_key_and_inner_credential() {
    let inner = sign_test_jwt(
        serde_json::json!({"typ":"JWT","alg":"HS256","kid":"app.key"}),
        serde_json::json!({"iat":1_700_000_000_i64,"exp":1_700_000_600_i64}),
        "secret",
    );
    let outer = sign_test_jwt(
        serde_json::json!({
            "typ":"JWT",
            "alg":"HS256",
            "kid":"app.key",
            "x-ably-token":inner,
        }),
        serde_json::json!({"iat":1_700_000_000_i64,"exp":1_700_000_600_i64}),
        "secret",
    );

    let verified = verify_ably_signed_jwt(&outer, "app.key", "secret").unwrap();
    assert_eq!(verified.embedded_token.as_deref(), Some(inner.as_str()));
    assert!(verify_ably_signed_jwt(&outer, "other.key", "secret").is_err());
    assert!(verify_ably_signed_jwt(&outer, "app.key", "wrong").is_err());
}

#[test]
fn compact_jwe_decrypts_nested_jwt_and_rejects_tampering() {
    use aes_gcm::{
        Aes256Gcm, Nonce,
        aead::{Aead, Payload},
    };

    let nested = "header.payload.signature";
    let protected = general_purpose::URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&serde_json::json!({
            "typ":"JWT","alg":"dir","enc":"A256GCM","cty":"JWT","kid":"app.key"
        }))
        .unwrap(),
    );
    let key = Sha256::digest(b"secret");
    let cipher = <Aes256Gcm as AeadKeyInit>::new_from_slice(&key).unwrap();
    let nonce = [7_u8; 12];
    let encrypted = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: nested.as_bytes(),
                aad: protected.as_bytes(),
            },
        )
        .unwrap();
    let split = encrypted.len() - 16;
    let token = format!(
        "{}..{}.{}.{}",
        protected,
        general_purpose::URL_SAFE_NO_PAD.encode(nonce),
        general_purpose::URL_SAFE_NO_PAD.encode(&encrypted[..split]),
        general_purpose::URL_SAFE_NO_PAD.encode(&encrypted[split..]),
    );
    assert_eq!(
        decrypt_ably_compact_jwe(&token, "app.key", "secret").unwrap(),
        nested
    );
    assert!(decrypt_ably_compact_jwe(&token, "other.key", "secret").is_err());
    let tampered = format!("{token}x");
    assert!(decrypt_ably_compact_jwe(&tampered, "app.key", "secret").is_err());
}

#[test]
fn jwt_ttl_is_bounded_by_revocation_retention() {
    let claims = AblyJwtClaims {
        iat: 100,
        exp: 161,
        client_id: None,
        capability: None,
        revocation_key: None,
        embedded_token: None,
    };
    assert!(validate_ably_jwt_times(&claims, 100, 60_000, false).is_err());
    let valid = AblyJwtClaims { exp: 160, ..claims };
    assert_eq!(
        validate_ably_jwt_times(&valid, 100, 60_000, false).unwrap(),
        (100_000, 160_000)
    );
}

#[test]
fn jwt_lifetime_cannot_outlive_local_revocation_retention() {
    assert!(validate_jwt_lifetime_ms(1_000, 61_000, 60_000).is_ok());
    for (issued, expires) in [(1_000, 1_000), (2_000, 1_000), (1_000, 61_001)] {
        let error = validate_jwt_lifetime_ms(issued, expires, 60_000).unwrap_err();
        assert_eq!(error.status, StatusCode::UNAUTHORIZED);
        assert_eq!(error.message, "JWT claims are invalid");
    }
}

#[test]
fn rsa6_capability_intersection_preserves_equal_and_intersects_ops_and_paths() {
    let key = r#"{"channel0":["publish"],"channel2":["publish","subscribe"],"channel6":["*"]}"#;
    let (equal, _) = intersect_ably_capability(key, Some(key)).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&equal).unwrap(),
        serde_json::from_str::<serde_json::Value>(key).unwrap()
    );
    let ordered_key = r#"{"channel":["presence","publish"]}"#;
    let (reordered_equal, _) =
        intersect_ably_capability(ordered_key, Some(r#"{"channel":["publish","presence"]}"#))
            .unwrap();
    assert_eq!(reordered_equal, r#"{"channel":["publish","presence"]}"#);

    let (intersected, capabilities) = intersect_ably_capability(
            key,
            Some(r#"{"channel2":["presence","subscribe"],"missing":["publish"],"channel6":["publish","subscribe"]}"#),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&intersected).unwrap(),
        serde_json::json!({
            "channel2": ["subscribe"],
            "channel6": ["publish", "subscribe"]
        })
    );
    let capabilities = capabilities.unwrap();
    assert!(capabilities.allows_subscribe("channel2"));
    assert!(!capabilities.allows_publish("channel2"));
}

#[test]
fn rsa6_rejects_empty_unknown_and_mixed_wildcard_operations() {
    let key = r#"{"*":["*"]}"#;
    for invalid in [
        r#"{"channel":[]}"#,
        r#"{"channel":["publish_"]}"#,
        r#"{"channel":["*","publish"]}"#,
    ] {
        assert!(matches!(
            intersect_ably_capability(key, Some(invalid)),
            Err(CapabilityIntersectionError::Invalid(_))
        ));
    }
}

#[test]
fn key_rotation_metadata_controls_key_activity() {
    let mut key = AblyCompatKeyConfig {
        enabled: true,
        created_at_ms: Some(100),
        expires_at_ms: Some(300),
        ..Default::default()
    };
    assert!(!key_is_active(&key, 99));
    assert!(key_is_active(&key, 100));
    assert!(!key_is_active(&key, 300));
    key.revoked_at_ms = Some(200);
    assert!(!key_is_active(&key, 200));
    key.enabled = false;
    assert!(!key_is_active(&key, 150));
}

#[test]
fn revocable_token_tracks_key_rotation_and_revocation() {
    let mut hub = AblyCompatHub::default();
    hub.key_registry.insert(
        "extra".to_string(),
        AblyCompatKeyConfig {
            app_id: "app".to_string(),
            key_name: "extra".to_string(),
            secret: "secret".to_string(),
            enabled: true,
            rotation_id: Some("v1".to_string()),
            ..Default::default()
        },
    );
    let record = AblyTokenRecord {
        app_id: "app".to_string(),
        key_name: "extra".to_string(),
        client_id: None,
        issued_ms: 0,
        expires_ms: 1_000,
        capabilities: None,
        revocable: true,
        rotation_id: Some("v1".to_string()),
        revocation_key: None,
    };
    assert!(token_key_is_current(&hub, &record, 100));
    hub.key_registry.get_mut("extra").unwrap().rotation_id = Some("v2".to_string());
    assert!(!token_key_is_current(&hub, &record, 100));
    hub.key_registry.get_mut("extra").unwrap().rotation_id = Some("v1".to_string());
    hub.key_registry.get_mut("extra").unwrap().revoked_at_ms = Some(100);
    assert!(!token_key_is_current(&hub, &record, 100));
}

#[tokio::test]
async fn two_runtimes_share_nonce_replay_and_issued_tokens_through_cache() {
    let cache: Arc<dyn CacheManager> = Arc::new(MemoryCacheManager::new(
        "ably-two-node".to_string(),
        MemoryCacheOptions::default(),
    ));
    let dependencies = AblyCompatDependencies {
        cache: Some(cache),
        ..Default::default()
    };
    let first = AblyCompatRuntime::new(dependencies.clone());
    let second = AblyCompatRuntime::new(dependencies);

    assert!(first.hub.claim_nonce("key", "nonce").await.unwrap());
    assert!(!second.hub.claim_nonce("key", "nonce").await.unwrap());

    let key = ResolvedAblyKey {
        app: App::from_policy(
            "shared-app".to_string(),
            "primary".to_string(),
            "primary-secret".to_string(),
            true,
            AppPolicy::default(),
        ),
        key_name: "extra-key".to_string(),
        secret: "secret".to_string(),
        capability: default_ably_capability(),
        capabilities: None,
        revocable_tokens: false,
        rotation_id: Some("v1".to_string()),
    };
    let details = first
        .hub
        .issue_token(
            &key,
            Some("client".to_string()),
            60_000,
            default_ably_capability(),
            None,
        )
        .await
        .unwrap();
    let record = second.hub.resolve_token(&details.token).await.unwrap();
    assert_eq!(record.app_id, "shared-app");
    assert_eq!(record.key_name, "extra-key");
    assert_eq!(record.client_id.as_deref(), Some("client"));
}

#[tokio::test]
async fn two_redis_backed_nodes_share_tokens_revocation_and_session_ownership() {
    let prefix = format!("ably-distributed-test-{}", uuid::Uuid::new_v4().simple());
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:16379/".to_string());
    let first_cache: Arc<dyn CacheManager> = Arc::new(
        RedisCacheManager::with_url(&redis_url, Some(&prefix))
            .await
            .expect("configured Redis test service must be available"),
    );
    let second_cache: Arc<dyn CacheManager> = Arc::new(
        RedisCacheManager::with_url(&redis_url, Some(&prefix))
            .await
            .expect("configured Redis test service must be available"),
    );
    let first = AblyCompatRuntime::new(AblyCompatDependencies {
        cache: Some(first_cache),
        ..Default::default()
    });
    let second = AblyCompatRuntime::new(AblyCompatDependencies {
        cache: Some(second_cache),
        ..Default::default()
    });

    assert!(first.hub.claim_nonce("extra-key", "nonce").await.unwrap());
    assert!(!second.hub.claim_nonce("extra-key", "nonce").await.unwrap());

    let key = ResolvedAblyKey {
        app: App::from_policy(
            "shared-app".to_string(),
            "primary".to_string(),
            "primary-secret".to_string(),
            true,
            AppPolicy::default(),
        ),
        key_name: "extra-key".to_string(),
        secret: "secret".to_string(),
        capability: default_ably_capability(),
        capabilities: None,
        revocable_tokens: true,
        rotation_id: Some("v1".to_string()),
    };
    let details = first
        .hub
        .issue_token(
            &key,
            Some("client".to_string()),
            60_000,
            default_ably_capability(),
            None,
        )
        .await
        .unwrap();
    let record = second.hub.resolve_token(&details.token).await.unwrap();
    assert_eq!(record.app_id, "shared-app");
    assert_eq!(record.client_id.as_deref(), Some("client"));

    second
        .hub
        .store_revocation(
            "shared-app",
            "clientId",
            "client",
            AblyRevocationRecord {
                target_type: "clientId".to_string(),
                target_value: "client".to_string(),
                issued_before: record.issued_ms.saturating_add(1),
                applies_at: 0,
            },
        )
        .await
        .unwrap();
    let old_authorization = ConnectionAuthorization {
        generation: 1,
        client_id: record.client_id.clone(),
        connection_client_id: record.client_id.clone(),
        capabilities: record.capabilities.clone(),
        issued_ms: record.issued_ms,
        expires_ms: Some(record.expires_ms),
        credential_id: details.token.clone(),
        revocable: record.revocable,
        revocation_key: record.revocation_key.clone(),
    };
    assert!(
        first
            .hub
            .authorization_is_revoked("shared-app", &old_authorization, &HashMap::new(),)
            .await
    );

    let mut renewed = old_authorization.clone();
    renewed.generation = 2;
    renewed.issued_ms = record.issued_ms.saturating_add(2);
    renewed.credential_id = "renewed-token".to_string();
    assert!(
        !first
            .hub
            .authorization_is_revoked("shared-app", &renewed, &HashMap::new())
            .await
    );

    first
        .hub
        .claim_session_owner("shared-app", "connection-a", "node-a")
        .await
        .unwrap();
    second
        .hub
        .claim_session_owner("shared-app", "connection-a", "node-b")
        .await
        .unwrap();
    assert!(
        !first
            .hub
            .session_is_current("shared-app", "connection-a", "node-a")
            .await
    );
    assert!(
        second
            .hub
            .session_is_current("shared-app", "connection-a", "node-b")
            .await
    );
    assert!(
        !first
            .hub
            .refresh_session_owner("shared-app", "connection-a", "node-a")
            .await
    );

    second
        .hub
        .remember_connection(
            "shared-app:00000000000000000000000000000001".to_string(),
            "shared-app",
            "connection-a",
            Some("client".to_string()),
        )
        .await;
    drop(first);
    let restarted_cache: Arc<dyn CacheManager> = Arc::new(
        RedisCacheManager::with_url(&redis_url, Some(&prefix))
            .await
            .unwrap(),
    );
    let restarted = AblyCompatRuntime::new(AblyCompatDependencies {
        cache: Some(restarted_cache),
        ..Default::default()
    });
    assert!(matches!(
        restarted
            .hub
            .begin_connection(
                "shared-app",
                Some("client"),
                None,
                Some("shared-app:00000000000000000000000000000001"),
            )
            .await,
        AblyConnectionStart::Resumed { connection_id }
            if connection_id == "connection-a"
    ));
    restarted
        .hub
        .claim_session_owner("shared-app", "connection-a", "node-c")
        .await
        .unwrap();
    assert!(
        !second
            .hub
            .session_is_current("shared-app", "connection-a", "node-b")
            .await
    );
}

#[tokio::test]
async fn ably_errors_use_browser_decodable_json_for_all_request_formats() {
    for requested_format in [AblyFormat::Json, AblyFormat::MsgPack] {
        let response = ably_error_response_format(
            StatusCode::UNAUTHORIZED,
            40105,
            "Nonce value replayed",
            requested_format,
        );
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let body: AblyErrorBody = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.error.status_code, 401);
        assert_eq!(body.error.code, 40105);
        assert_eq!(body.error.message, "Nonce value replayed");
        assert_eq!(content_type, "application/json");
    }
}

#[test]
fn bearer_token_accepts_raw_and_ably_base64_values() {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        "Bearer sockudo-ably-raw".parse().unwrap(),
    );
    assert_eq!(bearer_token(&headers).as_deref(), Some("sockudo-ably-raw"));

    let encoded = general_purpose::STANDARD.encode("sockudo-ably-encoded");
    headers.insert(
        header::AUTHORIZATION,
        format!("Bearer {encoded}").parse().unwrap(),
    );
    assert_eq!(
        bearer_token(&headers).as_deref(),
        Some("sockudo-ably-encoded")
    );
}

#[test]
fn pusher_to_ably_keeps_ai_extras_and_hides_runtime_headers() {
    let message = PusherMessage {
        event: Some("ai-input".to_string()),
        channel: Some("chat".to_string()),
        data: Some(MessageData::String("hello".to_string())),
        name: None,
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: None,
        stream_id: None,
        serial: Some(42),
        idempotency_key: None,
        extras: Some(MessageExtras {
            ai: Some(AiExtras {
                transport: Some(HashMap::from([(
                    "input-client-id".to_string(),
                    "client-1".to_string(),
                )])),
                codec: None,
            }),
            ..Default::default()
        }),
        delta_sequence: None,
        delta_conflation_key: None,
    };

    let converted = pusher_to_ably_message(&message, AblyMessageProjection::Mutation).unwrap();
    assert_eq!(converted.name.as_deref(), Some("ai-input"));
    assert_eq!(converted.serial.as_deref(), Some("42"));
    assert_eq!(converted.client_id.as_deref(), Some("client-1"));
    assert!(converted.extras.unwrap().get("ai").is_some());
}

#[test]
fn stamp_ai_identity_rejects_client_id_spoofing() {
    let mut extras = Some(MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([(
                AI_HEADER_INPUT_CLIENT_ID.to_string(),
                "other-client".to_string(),
            )])),
            codec: None,
        }),
        ..Default::default()
    });

    let error = stamp_ai_identity(&mut extras, AI_EVENT_INPUT, "client-1").unwrap_err();
    assert!(error.to_string().contains("authenticated clientId"));
}

#[test]
fn rest_publish_payload_decodes_json_and_msgpack_arrays() {
    let messages = vec![AblyMessage {
        name: Some("chat".to_string()),
        data: Some(json!({ "ok": true })),
        encoding: Some("json".to_string()),
        client_id: Some("client-1".to_string()),
        ..Default::default()
    }];

    let json_body = sonic_rs::to_vec(&messages).unwrap();
    let decoded_json = decode_ably_publish_payload(&json_body, AblyFormat::Json).unwrap();
    assert_eq!(decoded_json[0].name.as_deref(), Some("chat"));
    assert_eq!(decoded_json[0].client_id.as_deref(), Some("client-1"));

    let msgpack_value = serde_json::json!([
        {
            "name": "chat",
            "data": { "ok": true },
            "encoding": "json",
            "clientId": "client-1"
        }
    ]);
    let msgpack_body = rmp_serde::to_vec(&msgpack_value).unwrap();
    let decoded_msgpack = decode_ably_publish_payload(&msgpack_body, AblyFormat::MsgPack).unwrap();
    assert_eq!(decoded_msgpack[0].name.as_deref(), Some("chat"));
    assert_eq!(decoded_msgpack[0].client_id.as_deref(), Some("client-1"));
}

#[test]
fn rest_publish_payload_decodes_single_json_object() {
    let message = AblyMessage {
        name: Some("chat".to_string()),
        data: Some(json!("hello")),
        ..Default::default()
    };
    let body = sonic_rs::to_vec(&message).unwrap();
    let decoded = decode_ably_publish_payload(&body, AblyFormat::Json).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].name.as_deref(), Some("chat"));
}

#[cfg(feature = "push")]
#[test]
fn push_msgpack_payload_uses_binary_safe_wire_values() {
    let body = rmp_serde::to_vec_named(&serde_json::json!({
        "recipient": {
            "transportType": "ablyChannel",
            "channel": "device-inbox"
        },
        "notification": { "title": "hello" },
        "data": { "count": 1 }
    }))
    .unwrap();

    let request = decode_value::<AblyPushPublishRequest>(&body, AblyFormat::MsgPack).unwrap();
    let recipient = ably_push_wire_value(request.recipient).unwrap();
    assert_eq!(recipient["channel"], "device-inbox");
    assert!(matches!(
        ably_push_recipient(&recipient).unwrap(),
        PushRecipient::Realtime { channel } if channel == "device-inbox"
    ));
}

#[cfg(feature = "push")]
#[tokio::test]
async fn push_adapter_admits_to_native_pipeline_and_deduplicates() {
    use sockudo_push::{
        MemoryPushQueue, MemoryPushStore, PublishLifecycleState, PublishTarget, PushQueueStage,
    };

    let store: DynPushStore = Arc::new(MemoryPushStore::new());
    let queue: sockudo_push::DynPushQueue = Arc::new(MemoryPushQueue::new());
    let payload = ably_push_payload(
        Some(json!({ "title": "hello", "body": "world" })),
        Some(json!({ "message": "durable" })),
    )
    .unwrap();
    let target = PublishTarget::Recipient {
        recipient: PushRecipient::Realtime {
            channel: "device-inbox".to_string(),
        },
    };
    let providers = BTreeSet::from([PushProviderKind::Realtime]);

    let first = accept_ably_push_intent(
        store.clone(),
        queue.clone(),
        None,
        AblyPushIntentRequest {
            app_id: "app".to_string(),
            publish_id: "ably-publish-1".to_string(),
            targets: vec![target.clone()],
            required_providers: providers.clone(),
            payload: payload.clone(),
            expected_recipients: 1,
        },
    )
    .await
    .unwrap();
    let duplicate = accept_ably_push_intent(
        store.clone(),
        queue.clone(),
        None,
        AblyPushIntentRequest {
            app_id: "app".to_string(),
            publish_id: "ably-publish-1".to_string(),
            targets: vec![target],
            required_providers: providers,
            payload,
            expected_recipients: 1,
        },
    )
    .await
    .unwrap();

    assert_eq!(first.status.state, PublishLifecycleState::Queued);
    assert!(!first.duplicate);
    assert!(duplicate.duplicate);
    assert_eq!(first.publish_id, duplicate.publish_id);
    assert_eq!(
        queue
            .lag(PushQueueStage::PublishLog)
            .await
            .unwrap()
            .ready_depth,
        1
    );
    assert!(
        store
            .get_publish_status("app", &first.publish_id)
            .await
            .unwrap()
            .is_some()
    );
}

#[cfg(feature = "push")]
#[test]
fn realtime_delivery_id_is_retry_stable_and_wire_bounded() {
    let job = DeliveryJob {
        app_id: "app".repeat(40),
        publish_id: "publish".repeat(40),
        provider: PushProviderKind::Realtime,
        batch_id: "batch-1".to_string(),
        device_id: Some("device-1".to_string()),
        recipient: PushRecipient::Realtime {
            channel: "recipient-channel".to_string(),
        },
        payload: Arc::new(PushPayload {
            template_id: None,
            template_data: json!({}),
            title: Some("title".to_string()),
            body: None,
            icon: None,
            sound: None,
            collapse_key: None,
        }),
        rendered_payload: None,
        attempt: 1,
        first_attempt_at_ms: None,
        not_before_ms: None,
        expires_at_ms: None,
    };
    let first = ably_realtime_delivery_id(&job);
    let mut retry = job;
    retry.batch_id = "batch-1-retry-2".to_string();
    retry.attempt = 2;

    assert_eq!(first, ably_realtime_delivery_id(&retry));
    assert!(first.len() <= 128);
}

#[cfg(feature = "push")]
#[test]
fn device_identity_token_is_scoped_and_secret_verified() {
    let token = generate_ably_device_identity_token("app/one", "device/one");
    assert_eq!(
        parse_ably_device_identity_token(token.expose_secret()),
        Some(("app/one".to_string(), "device/one".to_string()))
    );
    let hash = hash_device_identity_token(&token);
    assert!(verify_device_identity_token(token.expose_secret(), &hash));
    assert!(!verify_device_identity_token("wrong-token", &hash));
    assert!(!format!("{hash:?}").contains(hash.expose_secret()));
}

#[cfg(feature = "push")]
#[tokio::test]
async fn updating_device_preserves_identity_and_does_not_rotate_token() {
    use sockudo_push::MemoryPushStore;

    let store: DynPushStore = Arc::new(MemoryPushStore::new());
    let hub = AblyCompatHub {
        push_store: Some(store.clone()),
        ..Default::default()
    };
    let request = || {
        serde_json::from_value::<AblyPushDeviceRequest>(serde_json::json!({
            "id": "device-1",
            "clientId": "client-1",
            "deviceSecret": "ably-device-secret",
            "platform": "android",
            "formFactor": "phone",
            "push": {
                "recipient": {
                    "transportType": "ablyChannel",
                    "channel": "device-inbox"
                }
            }
        }))
        .unwrap()
    };

    let first = save_ably_push_device(&hub, "app-1", None, request())
        .await
        .unwrap();
    let first_hash = store
        .get_device("app-1", "device-1")
        .await
        .unwrap()
        .unwrap()
        .device_secret;
    let second = save_ably_push_device(&hub, "app-1", None, request())
        .await
        .unwrap();
    let second_hash = store
        .get_device("app-1", "device-1")
        .await
        .unwrap()
        .unwrap()
        .device_secret;

    assert!(first.get("deviceIdentityToken").is_some());
    assert!(second.get("deviceIdentityToken").is_none());
    assert_eq!(first_hash.expose_secret(), second_hash.expose_secret());
}

#[cfg(feature = "delta")]
#[test]
fn delta_projection_preserves_exact_encoded_json_bytes() {
    let raw = r#"{"foo":"bar","count":2,"status":"active"}"#;
    let protocol = AblyProtocolMessage {
        action: ACTION_MESSAGE,
        messages: Some(vec![AblyMessage {
            id: Some("message-1".to_string()),
            data: Some(json!({ "status": "active", "foo": "bar", "count": 2 })),
            encoded_json: Some(Arc::<[u8]>::from(raw.as_bytes())),
            ..AblyMessage::default()
        }]),
        ..empty_protocol_message(ACTION_MESSAGE)
    };

    let (projected, next) = project_ably_delta_message(protocol, &AblyDeltaState::default());
    let projected = &projected.messages.unwrap()[0];
    assert_eq!(projected.data.as_ref(), Some(&json!(raw)));
    assert_eq!(projected.encoding.as_deref(), Some("json"));
    assert_eq!(next.previous_payload.as_deref(), Some(raw.as_bytes()));
}

#[cfg(feature = "delta")]
#[test]
fn delta_projection_delivers_null_as_a_valid_baseline() {
    let protocol = AblyProtocolMessage {
        action: ACTION_MESSAGE,
        messages: Some(vec![AblyMessage {
            id: Some("message-null".to_string()),
            data: None,
            ..AblyMessage::default()
        }]),
        ..empty_protocol_message(ACTION_MESSAGE)
    };

    let (projected, next) = project_ably_delta_message(protocol, &AblyDeltaState::default());
    let projected = &projected.messages.unwrap()[0];
    assert_eq!(projected.data.as_ref(), Some(&json!("null")));
    assert_eq!(projected.encoding.as_deref(), Some("json"));
    assert_eq!(next.previous_payload.as_deref(), Some(b"null".as_slice()));
}

#[cfg(feature = "delta")]
#[test]
fn delta_projection_does_not_expand_small_payloads() {
    let protocol = AblyProtocolMessage {
        action: ACTION_MESSAGE,
        messages: Some(vec![AblyMessage {
            id: Some("message-2".to_string()),
            data: None,
            ..AblyMessage::default()
        }]),
        ..empty_protocol_message(ACTION_MESSAGE)
    };
    let state = AblyDeltaState {
        previous_id: Some(Arc::from("message-1")),
        previous_payload: Some(Arc::<[u8]>::from(b"null".as_slice())),
        previous_at: Some(Instant::now()),
    };

    let (projected, _) = project_ably_delta_message(protocol, &state);
    let projected = &projected.messages.unwrap()[0];
    assert_eq!(projected.data.as_ref(), Some(&json!("null")));
    assert_eq!(projected.encoding.as_deref(), Some("json"));
    assert!(projected.extras.is_none());
}

#[cfg(feature = "delta")]
#[test]
fn delta_projection_preserves_unsupported_encoding_and_resets_base() {
    let encoded = base64::engine::general_purpose::STANDARD.encode(b"ciphertext");
    let protocol = AblyProtocolMessage {
        action: ACTION_MESSAGE,
        messages: Some(vec![AblyMessage {
            id: Some("message-cipher".to_string()),
            data: Some(json!(encoded)),
            encoding: Some("cipher+aes-256-cbc/base64".to_string()),
            ..AblyMessage::default()
        }]),
        ..empty_protocol_message(ACTION_MESSAGE)
    };
    let state = AblyDeltaState {
        previous_id: Some(Arc::from("message-1")),
        previous_payload: Some(Arc::<[u8]>::from(b"previous".as_slice())),
        previous_at: Some(Instant::now()),
    };

    let (projected, next) = project_ably_delta_message(protocol, &state);
    let projected = &projected.messages.unwrap()[0];
    assert_eq!(projected.data.as_ref(), Some(&json!(encoded)));
    assert_eq!(
        projected.encoding.as_deref(),
        Some("cipher+aes-256-cbc/base64")
    );
    assert!(projected.extras.is_none());
    assert!(next.previous_id.is_none());
    assert!(next.previous_payload.is_none());
}

#[cfg(feature = "delta")]
#[test]
fn delta_projection_without_message_id_falls_back_to_full_delivery() {
    let protocol = AblyProtocolMessage {
        action: ACTION_MESSAGE,
        messages: Some(vec![AblyMessage {
            data: Some(json!({ "safe": true })),
            ..AblyMessage::default()
        }]),
        ..empty_protocol_message(ACTION_MESSAGE)
    };

    let (projected, next) = project_ably_delta_message(protocol, &AblyDeltaState::default());
    let projected = &projected.messages.unwrap()[0];
    assert_eq!(projected.data.as_ref(), Some(&json!({ "safe": true })));
    assert!(projected.encoding.is_none());
    assert!(next.previous_id.is_none());
    assert!(next.previous_payload.is_none());
}

#[cfg(feature = "delta")]
#[test]
fn delta_projection_does_not_retain_oversized_base() {
    let raw = "x".repeat(64 * 1024 + 1);
    let protocol = AblyProtocolMessage {
        action: ACTION_MESSAGE,
        messages: Some(vec![AblyMessage {
            id: Some("message-large".to_string()),
            data: Some(json!(raw)),
            ..AblyMessage::default()
        }]),
        ..empty_protocol_message(ACTION_MESSAGE)
    };

    let (projected, next) = project_ably_delta_message(protocol, &AblyDeltaState::default());
    assert_eq!(projected.messages.unwrap()[0].data, Some(json!(raw)));
    assert!(next.previous_id.is_none());
    assert!(next.previous_payload.is_none());
}

#[cfg(feature = "delta")]
#[test]
fn delta_projection_does_not_use_expired_base() {
    let raw = r#"{"foo":"bar","count":2,"status":"active"}"#;
    let protocol = AblyProtocolMessage {
        action: ACTION_MESSAGE,
        messages: Some(vec![AblyMessage {
            id: Some("message-2".to_string()),
            data: Some(json!({ "foo": "bar", "count": 2, "status": "active" })),
            encoded_json: Some(Arc::<[u8]>::from(raw.as_bytes())),
            ..AblyMessage::default()
        }]),
        ..empty_protocol_message(ACTION_MESSAGE)
    };
    let state = AblyDeltaState {
        previous_id: Some(Arc::from("message-1")),
        previous_payload: Some(Arc::<[u8]>::from(raw.as_bytes())),
        previous_at: Some(Instant::now() - ABLY_DELTA_BASE_MAX_AGE - Duration::from_secs(1)),
    };

    let (projected, next) = project_ably_delta_message(protocol, &state);
    let projected = &projected.messages.unwrap()[0];
    assert_eq!(projected.data.as_ref(), Some(&json!(raw)));
    assert_eq!(projected.encoding.as_deref(), Some("json"));
    assert!(projected.extras.is_none());
    assert_eq!(next.previous_id.as_deref(), Some("message-2"));
}

#[cfg(feature = "delta")]
#[test]
fn delta_projection_is_equivalent_on_json_and_msgpack_wire() {
    let body = "a".repeat(1024);
    let base = format!(r#"{{"body":"{body}","count":1}}"#);
    let target = format!(r#"{{"body":"{body}","count":2}}"#);
    let protocol = AblyProtocolMessage {
        action: ACTION_MESSAGE,
        channel: Some("room".to_string()),
        messages: Some(vec![AblyMessage {
            id: Some("message-2".to_string()),
            data: Some(json!({ "body": body, "count": 2 })),
            encoded_json: Some(Arc::<[u8]>::from(target.as_bytes())),
            ..AblyMessage::default()
        }]),
        ..empty_protocol_message(ACTION_MESSAGE)
    };
    let state = AblyDeltaState {
        previous_id: Some(Arc::from("message-1")),
        previous_payload: Some(Arc::<[u8]>::from(base.as_bytes())),
        previous_at: Some(Instant::now()),
    };

    let (projected, _) = project_ably_delta_message(protocol, &state);
    for format in [AblyFormat::Json, AblyFormat::MsgPack] {
        let encoded = encode_protocol_bytes(&projected, format).unwrap();
        let decoded = decode_protocol_bytes(&encoded, format).unwrap();
        let message = &decoded.messages.unwrap()[0];
        assert_eq!(
            message.encoding.as_deref(),
            Some("json/utf-8/vcdiff/base64")
        );
        assert_eq!(
            message.extras.as_ref().unwrap()["delta"]["from"],
            "message-1"
        );
        assert_eq!(
            message.extras.as_ref().unwrap()["delta"]["format"],
            "vcdiff"
        );
        assert!(message.data.as_ref().is_some_and(Value::is_str));
    }
}

#[test]
fn rest_msgpack_response_uses_named_message_fields() {
    let messages = vec![AblyMessage {
        name: Some("chat".to_string()),
        data: Some(json!("hello")),
        ..Default::default()
    }];
    let body = rmp_serde::to_vec_named(&messages).unwrap();
    let decoded: serde_json::Value = rmp_serde::from_slice(&body).unwrap();
    assert_eq!(decoded[0]["name"], "chat");
    assert_eq!(decoded[0]["data"], "hello");
}

#[test]
fn realtime_msgpack_protocol_message_round_trips_named_fields() {
    let wire = serde_json::json!({
        "action": ACTION_MESSAGE,
        "channel": "chat",
        "msgSerial": 7,
        "messages": [
            {
                "name": "chat-message",
                "data": { "ok": true },
                "encoding": "json"
            }
        ]
    });
    let body = rmp_serde::to_vec(&wire).unwrap();
    let decoded = decode_ably_protocol_message(&body, AblyFormat::MsgPack).unwrap();
    assert_eq!(decoded.action, ACTION_MESSAGE);
    assert_eq!(decoded.channel.as_deref(), Some("chat"));
    assert_eq!(decoded.msg_serial, Some(7));
    assert_eq!(
        decoded
            .messages
            .as_ref()
            .and_then(|messages| messages.first())
            .and_then(|message| message.name.as_deref()),
        Some("chat-message")
    );

    let encoded_body = encode_protocol_bytes(&decoded, AblyFormat::MsgPack).unwrap();
    let encoded_value: serde_json::Value = rmp_serde::from_slice(encoded_body.as_ref()).unwrap();
    assert_eq!(encoded_value["action"], ACTION_MESSAGE);
    assert_eq!(encoded_value["channel"], "chat");
    assert_eq!(encoded_value["msgSerial"], 7);
    assert_eq!(encoded_value["messages"][0]["name"], "chat-message");
}

#[test]
fn ably_protocol_format_defaults_to_json_and_accepts_msgpack() {
    assert_eq!(parse_ably_format(None).unwrap(), AblyFormat::Json);
    assert_eq!(parse_ably_format(Some("json")).unwrap(), AblyFormat::Json);
    assert_eq!(
        parse_ably_format(Some("msgpack")).unwrap(),
        AblyFormat::MsgPack
    );
    assert!(parse_ably_format(Some("xml")).is_err());
}

#[test]
fn rest_publish_rejects_message_client_id_spoofing() {
    let message = AblyMessage {
        client_id: Some("other-client".to_string()),
        ..Default::default()
    };
    let error = effective_ably_client_id(Some("client-1"), &message).unwrap_err();
    assert!(error.to_string().contains("authenticated clientId"));
}

#[test]
fn ably_token_capability_maps_to_sockudo_capabilities() {
    let (_, capabilities) = normalise_ably_token_capability(Some(serde_json::json!({
        "chat:*": ["publish", "subscribe", "history"],
        "presence-chat:*": ["presence"],
        "mutable:*": ["message-update-any", "message-delete-own"],
        "object:*": ["object-subscribe"]
    })))
    .unwrap();
    let capabilities = capabilities.unwrap();

    assert!(capabilities.allows_publish("chat:one"));
    assert!(capabilities.allows_subscribe("chat:one"));
    assert!(capabilities.allows_history("chat:one"));
    assert!(!capabilities.allows_publish("other:one"));
    assert!(capabilities.presence.as_deref().is_some_and(|patterns| {
        ConnectionCapabilities::matches_any(patterns, "presence-chat:one")
    }));
    assert!(capabilities.allows_message_mutation_any(
        sockudo_core::versioned_message_auth::MutationKind::Update,
        "mutable:one"
    ));
    assert!(capabilities.allows_message_mutation_own(
        sockudo_core::versioned_message_auth::MutationKind::Delete,
        "mutable:one"
    ));
    assert!(
        ensure_ably_capability(
            Some(&capabilities),
            "other:one",
            AblyCapabilityCheck::Publish
        )
        .is_err()
    );
}

#[test]
fn ably_token_capability_accepts_json_string_and_wildcard() {
    let (capability, capabilities) = normalise_ably_token_capability(Some(
        serde_json::Value::String(r#"{"*":["*"]}"#.to_string()),
    ))
    .unwrap();
    let capabilities = capabilities.unwrap();

    assert_eq!(capability.as_deref(), Some(r#"{"*":["*"]}"#));
    assert!(capabilities.allows_publish("any-channel"));
    assert!(capabilities.allows_subscribe("any-channel"));
    assert!(capabilities.allows_history("any-channel"));
    assert!(
        ensure_ably_capability(
            Some(&capabilities),
            "any-channel",
            AblyCapabilityCheck::AnyChannelAccess
        )
        .is_ok()
    );
}

#[test]
fn ably_token_client_id_cannot_be_overridden() {
    assert_eq!(
        resolve_ably_token_client_id(Some("client-1".to_string()), Some("client-1")).unwrap(),
        Some("client-1".to_string())
    );
    assert!(
        resolve_ably_token_client_id(Some("client-1".to_string()), Some("other-client")).is_err()
    );
    assert_eq!(
        resolve_ably_token_client_id(None, Some("client-1")).unwrap(),
        Some("client-1".to_string())
    );
    assert_eq!(
        resolve_ably_token_client_id(Some("*".to_string()), Some("client-2")).unwrap(),
        Some("client-2".to_string())
    );
    assert_eq!(
        resolve_ably_token_client_id(Some("*".to_string()), None).unwrap(),
        None
    );
}

#[test]
fn connection_query_accepts_canonical_names_aliases_and_boolean_echo() {
    let canonical: AblyConnectQuery = serde_json::from_value(serde_json::json!({
        "access_token": "redacted",
        "client_id": "client",
        "echo": false
    }))
    .unwrap();
    assert_eq!(canonical.access_token.as_deref(), Some("redacted"));
    assert_eq!(canonical.client_id.as_deref(), Some("client"));
    assert!(!canonical.echo);

    let aliases: AblyConnectQuery = serde_json::from_value(serde_json::json!({
        "accessToken": "redacted",
        "clientId": "client",
        "echoMessages": true
    }))
    .unwrap();
    assert!(aliases.echo);
}

#[test]
fn authorization_replacement_is_atomic_and_invalidates_stale_deadlines() {
    let app = App::from_policy(
        "app".to_string(),
        "key".to_string(),
        "secret".to_string(),
        true,
        AppPolicy::default(),
    );
    let first = ResolvedAblyAuth {
        app: app.clone(),
        client_id: Some("client".to_string()),
        connection_client_id: Some("client".to_string()),
        capabilities: None,
        issued_ms: 100,
        expires_ms: Some(200),
        credential_id: "first".to_string(),
        revocable: true,
        revocation_key: None,
        #[cfg(feature = "push")]
        push_device_id: None,
    };
    let second = ResolvedAblyAuth {
        app,
        client_id: Some("client".to_string()),
        connection_client_id: Some("client".to_string()),
        capabilities: Some(restricted_ably_capabilities()),
        issued_ms: 150,
        expires_ms: Some(500),
        credential_id: "second".to_string(),
        revocable: true,
        revocation_key: Some("group".to_string()),
        #[cfg(feature = "push")]
        push_device_id: None,
    };
    let mut authorization = ConnectionAuthorization::from_resolved(&first);
    let stale_generation = authorization.generation;
    authorization.replace_from(&second);
    assert_ne!(authorization.generation, stale_generation);
    assert_eq!(authorization.credential_id, "second");
    assert_eq!(authorization.expires_ms, Some(500));
    assert_eq!(authorization.revocation_key.as_deref(), Some("group"));
}

#[tokio::test]
async fn issued_before_revocation_does_not_disconnect_a_renewed_generation() {
    let hub = AblyCompatHub::default();
    let app_id = "app";
    hub.store_revocation(
        app_id,
        "clientId",
        "client",
        AblyRevocationRecord {
            target_type: "clientId".to_string(),
            target_value: "client".to_string(),
            issued_before: 200,
            applies_at: 0,
        },
    )
    .await
    .unwrap();
    let channels = HashMap::new();
    let old = ConnectionAuthorization {
        generation: 1,
        client_id: Some("client".to_string()),
        connection_client_id: Some("client".to_string()),
        capabilities: None,
        issued_ms: 100,
        expires_ms: Some(1_000),
        credential_id: "old".to_string(),
        revocable: true,
        revocation_key: None,
    };
    let mut renewed = old.clone();
    renewed.generation = 2;
    renewed.issued_ms = 201;
    renewed.credential_id = "new".to_string();
    assert!(hub.authorization_is_revoked(app_id, &old, &channels).await);
    assert!(
        !hub.authorization_is_revoked(app_id, &renewed, &channels)
            .await
    );
}

#[tokio::test]
async fn revocation_is_observed_by_an_independent_runtime_through_shared_cache() {
    let cache: Arc<dyn CacheManager> = Arc::new(MemoryCacheManager::new(
        "ably-revocation-two-node".to_string(),
        MemoryCacheOptions::default(),
    ));
    let first = AblyCompatRuntime::new(AblyCompatDependencies {
        cache: Some(Arc::clone(&cache)),
        ..Default::default()
    });
    let second = AblyCompatRuntime::new(AblyCompatDependencies {
        cache: Some(cache),
        ..Default::default()
    });
    first
        .hub
        .store_revocation(
            "app",
            "clientId",
            "client",
            AblyRevocationRecord {
                target_type: "clientId".to_string(),
                target_value: "client".to_string(),
                issued_before: 200,
                applies_at: 0,
            },
        )
        .await
        .unwrap();
    let authorization = ConnectionAuthorization {
        generation: 1,
        client_id: Some("client".to_string()),
        connection_client_id: Some("client".to_string()),
        capabilities: None,
        issued_ms: 100,
        expires_ms: Some(1_000),
        credential_id: "credential".to_string(),
        revocable: true,
        revocation_key: None,
    };
    assert!(
        second
            .hub
            .authorization_is_revoked("app", &authorization, &HashMap::new())
            .await
    );
}

struct PagedRevocationCache {
    entries: Vec<(String, String)>,
}

#[async_trait::async_trait]
impl CacheManager for PagedRevocationCache {
    async fn has(&self, _key: &str) -> sockudo_core::error::Result<bool> {
        Ok(false)
    }

    async fn get(&self, _key: &str) -> sockudo_core::error::Result<Option<String>> {
        Ok(None)
    }

    async fn set(
        &self,
        _key: &str,
        _value: &str,
        _ttl_seconds: u64,
    ) -> sockudo_core::error::Result<()> {
        Ok(())
    }

    async fn remove(&self, _key: &str) -> sockudo_core::error::Result<()> {
        Ok(())
    }

    async fn disconnect(&self) -> sockudo_core::error::Result<()> {
        Ok(())
    }

    async fn ttl(&self, _key: &str) -> sockudo_core::error::Result<Option<std::time::Duration>> {
        Ok(None)
    }

    async fn scan_prefix(
        &self,
        _prefix: &str,
        limit: usize,
    ) -> sockudo_core::error::Result<Vec<(String, String)>> {
        Ok(self.entries.iter().take(limit).cloned().collect())
    }

    async fn scan_prefix_page(
        &self,
        _prefix: &str,
        cursor: Option<String>,
        limit: usize,
    ) -> sockudo_core::error::Result<sockudo_core::cache::CacheScanPage> {
        let start = cursor
            .as_deref()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        let end = start.saturating_add(limit).min(self.entries.len());
        Ok(sockudo_core::cache::CacheScanPage {
            entries: self.entries[start..end].to_vec(),
            next_cursor: (end < self.entries.len()).then(|| end.to_string()),
        })
    }
}

#[tokio::test]
async fn shared_cache_channel_revocation_scan_pages_past_one_thousand_records() {
    let mut entries = (0..1_100)
        .map(|index| {
            let record = AblyRevocationRecord {
                target_type: "channel".to_string(),
                target_value: format!("channel-{index}"),
                issued_before: 0,
                applies_at: 0,
            };
            (
                format!("ably-compat:revocation:app:{index}"),
                serde_json::to_string(&record).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    entries.push((
        "ably-compat:revocation:app:target".to_string(),
        serde_json::to_string(&AblyRevocationRecord {
            target_type: "channel".to_string(),
            target_value: "target-channel".to_string(),
            issued_before: 200,
            applies_at: 0,
        })
        .unwrap(),
    ));
    let cache: Arc<dyn CacheManager> = Arc::new(PagedRevocationCache { entries });
    let hub = AblyCompatRuntime::new(AblyCompatDependencies {
        cache: Some(cache),
        ..Default::default()
    });
    let authorization = ConnectionAuthorization {
        generation: 1,
        client_id: None,
        connection_client_id: None,
        capabilities: None,
        issued_ms: 100,
        expires_ms: Some(1_000),
        credential_id: "credential".to_string(),
        revocable: true,
        revocation_key: None,
    };

    assert!(
        hub.hub
            .authorization_is_revoked("app", &authorization, &HashMap::new())
            .await
    );
}

#[test]
fn local_revocation_store_preserves_live_evidence_when_capacity_is_exhausted() {
    let mut store = AblyRevocationStore::new(1, usize::MAX);
    let first = AblyRevocationRecord {
        target_type: "clientId".to_string(),
        target_value: "first".to_string(),
        issued_before: 10,
        applies_at: 10,
    };
    let second = AblyRevocationRecord {
        target_type: "clientId".to_string(),
        target_value: "second".to_string(),
        issued_before: 11,
        applies_at: 11,
    };

    store
        .insert("app", "first-key".to_string(), first.clone(), 100, 10)
        .unwrap();
    let failure = store
        .insert("app", "second-key".to_string(), second.clone(), 101, 11)
        .unwrap_err();

    assert_eq!(failure.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(failure.message, "Token revocation capacity exceeded");
    assert_eq!(store.get("first-key", 99), Some(first));
    assert_eq!(store.get("second-key", 99), None);

    store
        .insert("app", "second-key".to_string(), second.clone(), 200, 100)
        .unwrap();
    assert_eq!(store.get("first-key", 100), None);
    assert_eq!(store.get("second-key", 100), Some(second));
}

#[test]
fn local_revocation_store_enforces_its_byte_limit() {
    let mut store = AblyRevocationStore::new(10, 1);
    let failure = store
        .insert(
            "app",
            "key".to_string(),
            AblyRevocationRecord {
                target_type: "clientId".to_string(),
                target_value: "client".to_string(),
                issued_before: 10,
                applies_at: 10,
            },
            100,
            10,
        )
        .unwrap_err();

    assert_eq!(failure.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(store.entries.is_empty());
    assert_eq!(store.bytes, 0);
}

#[tokio::test]
async fn local_revocation_retention_covers_the_configured_maximum_token_lifetime() {
    let issued_before = now_ms();
    let hub = AblyCompatHub {
        config: AblyCompatConfig {
            max_token_ttl_ms: 60_000,
            ..AblyCompatConfig::default()
        },
        ..AblyCompatHub::default()
    };
    hub.store_revocation(
        "app",
        "clientId",
        "client",
        AblyRevocationRecord {
            target_type: "clientId".to_string(),
            target_value: "client".to_string(),
            issued_before,
            applies_at: issued_before,
        },
    )
    .await
    .unwrap();
    let key = revocation_cache_key("app", "clientId", "client");
    let expires_at_ms = lock_revocations(&hub.revocations)
        .entries
        .get(&key)
        .unwrap()
        .expires_at_ms;

    assert!(expires_at_ms >= issued_before + 60_000);
    assert!(
        lock_revocations(&hub.revocations)
            .get(&key, expires_at_ms - 1)
            .is_some()
    );
    assert!(
        lock_revocations(&hub.revocations)
            .get(&key, expires_at_ms)
            .is_none()
    );
}

#[tokio::test]
async fn local_channel_revocations_are_scoped_to_the_issuing_app() {
    let hub = AblyCompatHub::default();
    hub.store_revocation(
        "app-a",
        "channel",
        "shared-channel",
        AblyRevocationRecord {
            target_type: "channel".to_string(),
            target_value: "shared-channel".to_string(),
            issued_before: now_ms(),
            applies_at: 0,
        },
    )
    .await
    .unwrap();
    let capabilities = ably_capability_value_to_sockudo(&serde_json::json!({
        "shared-channel": ["subscribe"]
    }))
    .unwrap();
    let authorization = ConnectionAuthorization {
        generation: 1,
        client_id: None,
        connection_client_id: None,
        capabilities: Some(capabilities),
        issued_ms: 0,
        expires_ms: Some(i64::MAX),
        credential_id: "app-b-token".to_string(),
        revocable: true,
        revocation_key: None,
    };

    assert!(
        !hub.authorization_is_revoked("app-b", &authorization, &HashMap::new())
            .await
    );
    assert!(
        hub.authorization_is_revoked("app-a", &authorization, &HashMap::new())
            .await
    );
}

struct UnavailableCache;

#[async_trait::async_trait]
impl CacheManager for UnavailableCache {
    async fn has(&self, _key: &str) -> sockudo_core::error::Result<bool> {
        Err(sockudo_core::error::Error::Cache(
            "coordination unavailable".to_string(),
        ))
    }

    async fn get(&self, _key: &str) -> sockudo_core::error::Result<Option<String>> {
        Err(sockudo_core::error::Error::Cache(
            "coordination unavailable".to_string(),
        ))
    }

    async fn set(
        &self,
        _key: &str,
        _value: &str,
        _ttl_seconds: u64,
    ) -> sockudo_core::error::Result<()> {
        Err(sockudo_core::error::Error::Cache(
            "coordination unavailable".to_string(),
        ))
    }

    async fn remove(&self, _key: &str) -> sockudo_core::error::Result<()> {
        Err(sockudo_core::error::Error::Cache(
            "coordination unavailable".to_string(),
        ))
    }

    async fn disconnect(&self) -> sockudo_core::error::Result<()> {
        Ok(())
    }

    async fn ttl(&self, _key: &str) -> sockudo_core::error::Result<Option<Duration>> {
        Err(sockudo_core::error::Error::Cache(
            "coordination unavailable".to_string(),
        ))
    }
}

#[tokio::test]
async fn revocable_authorization_fails_closed_when_shared_backend_is_unavailable() {
    let hub = AblyCompatRuntime::new(AblyCompatDependencies {
        cache: Some(Arc::new(UnavailableCache)),
        ..Default::default()
    });
    let authorization = ConnectionAuthorization {
        generation: 1,
        client_id: Some("client".to_string()),
        connection_client_id: Some("client".to_string()),
        capabilities: None,
        issued_ms: 100,
        expires_ms: Some(1_000),
        credential_id: "credential".to_string(),
        revocable: true,
        revocation_key: None,
    };

    assert!(
        hub.hub
            .authorization_is_revoked("app", &authorization, &HashMap::new())
            .await
    );
}

#[tokio::test]
async fn authentication_cache_failures_do_not_reach_client_error_messages() {
    let hub = AblyCompatRuntime::new(AblyCompatDependencies {
        cache: Some(Arc::new(UnavailableCache)),
        ..Default::default()
    });

    let error = hub.hub.claim_nonce("key", "nonce").await.unwrap_err();

    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.code, 50000);
    assert_eq!(
        error.message,
        "Authentication service temporarily unavailable"
    );
    assert!(!error.message.contains("coordination unavailable"));
}

#[tokio::test]
async fn invalid_cached_revocation_is_reported_without_serialization_details() {
    let cache: Arc<dyn CacheManager> = Arc::new(MemoryCacheManager::new(
        "ably-revocation-redaction".to_string(),
        MemoryCacheOptions::default(),
    ));
    cache
        .set(
            &revocation_cache_key("app", "clientId", "client"),
            "{sensitive backend record",
            60,
        )
        .await
        .unwrap();
    let hub = AblyCompatRuntime::new(AblyCompatDependencies {
        cache: Some(cache),
        ..Default::default()
    });

    let error = hub
        .hub
        .revocation("app", "clientId", "client")
        .await
        .unwrap_err();

    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        error.message,
        "Authentication service temporarily unavailable"
    );
    assert!(!error.message.contains("sensitive backend record"));
    assert!(!error.message.contains("expected"));
}

#[test]
fn capability_downgrade_removes_subscribe_without_affecting_publish_upgrade() {
    let (_, subscribe_only) = normalise_ably_token_capability(Some(serde_json::json!({
        "channel": ["subscribe"]
    })))
    .unwrap();
    let subscribe_only = subscribe_only.unwrap();
    assert!(subscribe_only.allows_subscribe("channel"));
    assert!(!subscribe_only.allows_publish("channel"));

    let (_, other_channel) = normalise_ably_token_capability(Some(serde_json::json!({
        "other": ["subscribe", "publish"]
    })))
    .unwrap();
    let other_channel = other_channel.unwrap();
    assert!(!other_channel.allows_subscribe("channel"));
    assert!(other_channel.allows_publish("other"));
}

#[test]
fn append_projection_uses_delta_for_mutations_and_aggregate_for_history() {
    let mut message = PusherMessage {
        event: Some("sockudo:message.append".to_string()),
        channel: Some("chat".to_string()),
        data: Some(MessageData::String("hello world".to_string())),
        name: Some("ai-output".to_string()),
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: None,
        stream_id: Some("stream-1".to_string()),
        serial: Some(2),
        idempotency_key: None,
        extras: Some(MessageExtras {
            ai: Some(AiExtras {
                transport: None,
                codec: Some(HashMap::from([(
                    "status".to_string(),
                    "complete".to_string(),
                )])),
            }),
            ..Default::default()
        }),
        delta_sequence: None,
        delta_conflation_key: None,
    };
    apply_runtime_metadata(
        &mut message,
        ProtocolMessageAction::Append,
        "msg:1",
        &sockudo_protocol::versioned_messages::MessageVersionMetadata {
            serial: "ver:2".to_string(),
            client_id: Some("client-1".to_string()),
            timestamp_ms: 2,
            description: None,
            metadata: None,
        },
        Some(10),
    );
    sockudo_protocol::versioned_messages::set_runtime_append_fragment(&mut message, " world");

    let mutation = pusher_to_ably_message(&message, AblyMessageProjection::Mutation).unwrap();
    assert_eq!(mutation.action, Some(MESSAGE_APPEND));
    assert_eq!(
        mutation.data.as_ref().and_then(Value::as_str),
        Some(" world")
    );
    assert_eq!(mutation.serial.as_deref(), Some("msg:1"));
    assert_eq!(
        mutation
            .version
            .as_ref()
            .map(|version| version.serial.as_str()),
        Some("ver:2")
    );

    let aggregate = pusher_to_ably_message(&message, AblyMessageProjection::Aggregate).unwrap();
    assert_eq!(aggregate.action, Some(MESSAGE_UPDATE));
    assert_eq!(
        aggregate.data.as_ref().and_then(Value::as_str),
        Some("hello world")
    );
    assert_eq!(aggregate.serial.as_deref(), Some("msg:1"));
}

#[test]
fn ably_channel_serial_round_trips_stream_position() {
    let encoded = encode_ably_channel_serial("stream-1", 42);
    let parsed = parse_ably_channel_serial(&encoded).unwrap();
    assert_eq!(parsed.stream_id, "stream-1");
    assert_eq!(parsed.serial, 42);

    let failure = parse_ably_channel_serial("not-a-position").unwrap_err();
    assert_eq!(failure.code, 90005);
}

#[test]
fn interest_check_does_not_create_compatibility_channel_state() {
    let hub = AblyCompatHub::default();
    assert!(!hub.has_subscribers("app", "chat"));
    assert!(hub.channels.is_empty());
}

#[test]
fn constructed_runtimes_do_not_share_compatibility_state() {
    let first = Arc::new(AblyCompatRuntime::new(AblyCompatDependencies::default()));
    let second = Arc::new(AblyCompatRuntime::new(AblyCompatDependencies::default()));
    let first_state = first.hub.channel_state("app", "channel");
    let second_state = second.hub.channel_state("app", "channel");

    assert!(!Arc::ptr_eq(&first.hub, &second.hub));
    assert!(!Arc::ptr_eq(&first_state, &second_state));
}

#[tokio::test]
async fn expiry_sweep_removes_expired_sessions_and_tokens() {
    let hub = AblyCompatHub::default();
    hub.sessions.insert(
        "expired-session".to_string(),
        AblySessionRecord {
            app_id: "app".to_string(),
            connection_id: "connection".to_string(),
            client_id: None,
            expires_at_ms: 10,
        },
    );
    hub.tokens.insert(
        "expired-token".to_string(),
        AblyTokenRecord {
            app_id: "app".to_string(),
            key_name: "key".to_string(),
            client_id: None,
            issued_ms: 0,
            expires_ms: 10,
            capabilities: None,
            revocable: false,
            rotation_id: None,
            revocation_key: None,
        },
    );

    hub.expire(11).await;

    assert!(hub.sessions.is_empty());
    assert!(hub.tokens.is_empty());
}

#[test]
fn mutation_operation_without_serial_uses_message_serial() {
    let message: AblyMessage = sonic_rs::from_str(
        r#"{
            "serial": "stream:1",
            "action": "message.update",
            "version": {
                "clientId": "updater",
                "description": "changed",
                "metadata": {"reason": "test"}
            }
        }"#,
    )
    .unwrap();

    let version = message.version.unwrap();
    assert_eq!(version.serial, "stream:1");
    assert_eq!(version.client_id.as_deref(), Some("updater"));
    assert_eq!(version.description.as_deref(), Some("changed"));
}

#[test]
fn mutation_path_is_authoritative_for_message_serial() {
    let mut absent = AblyMessage::default();
    reconcile_mutation_message_serial(&mut absent, "stream:1").unwrap();
    assert_eq!(absent.serial.as_deref(), Some("stream:1"));

    let mut matching = AblyMessage {
        serial: Some("stream:1".to_string()),
        ..AblyMessage::default()
    };
    reconcile_mutation_message_serial(&mut matching, "stream:1").unwrap();

    let mut mismatched = AblyMessage {
        serial: Some("stream:2".to_string()),
        ..AblyMessage::default()
    };
    assert!(reconcile_mutation_message_serial(&mut mismatched, "stream:1").is_err());
}

#[test]
fn msgpack_binary_payload_is_projected_to_base64_without_rejection() {
    #[derive(Serialize)]
    struct BinaryMessage {
        name: &'static str,
        data: Vec<u8>,
    }
    #[derive(Serialize)]
    struct Protocol {
        action: u8,
        messages: Vec<BinaryMessage>,
    }
    let wire = Protocol {
        action: ACTION_MESSAGE,
        messages: vec![BinaryMessage {
            name: "binary",
            data: vec![0xde, 0xad, 0xbe, 0xef],
        }],
    };
    let mut body = Vec::new();
    let mut serializer = rmp_serde::Serializer::new(&mut body)
        .with_struct_map()
        .with_bytes(rmp_serde::config::BytesMode::ForceAll);
    wire.serialize(&mut serializer).unwrap();
    let decoded = decode_ably_protocol_message(&body, AblyFormat::MsgPack).unwrap();
    let message = decoded.messages.unwrap().pop().unwrap();
    assert_eq!(message.encoding.as_deref(), Some("base64"));
    assert_eq!(message.data.unwrap().as_str(), Some("3q2+7w=="));
}
