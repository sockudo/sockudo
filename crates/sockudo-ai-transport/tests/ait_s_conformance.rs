use sockudo_ai_transport::{
    RollupConfig, RollupDeliveryReason, RollupEngine, is_allowed_append_rollup_window_ms,
};
use sockudo_core::history::{
    HistoryCursor, HistoryDirection, HistoryQueryBounds, HistoryReadRequest,
};
use sockudo_core::options::{AiTransportChannelConfig, AiTransportRollupConfig, PresenceConfig};
use sockudo_core::version_store::{MemoryVersionStore, VersionStore};
use sockudo_core::versioned_messages::{
    MessageAppend, MessageSerial, VersionMetadata, VersionSerial, VersionedMessage,
    validate_replay_continuity, validate_version_chain,
};
use sockudo_protocol::messages::{
    AI_EVENT_CANCEL, AI_EVENT_INPUT, AI_EVENT_LEGACY_TURN_END, AI_EVENT_LEGACY_TURN_START,
    AI_EVENT_OUTPUT, AI_EVENT_RUN_END, AI_EVENT_RUN_RESUME, AI_EVENT_RUN_START,
    AI_EVENT_RUN_SUSPEND, AI_TRANSPORT_KEY_MAX_BYTES, AI_TRANSPORT_TIER_LIMIT,
    AI_TRANSPORT_VALUE_MAX_BYTES, AiExtras, MessageData, MessageExtras, PusherMessage,
    is_ai_client_publish_event, is_ai_event,
};
use sockudo_protocol::versioned_messages::{
    MessageAction, MessageVersionMetadata, VersionedRealtimeMessage, apply_runtime_metadata,
    extract_runtime_action, extract_runtime_message_serial,
};
use sonic_rs::JsonValueTrait;
use std::collections::HashMap;

fn version(serial: &str, timestamp_ms: i64) -> VersionMetadata {
    VersionMetadata {
        serial: VersionSerial::new(serial).unwrap(),
        client_id: Some("client-1".to_string()),
        timestamp_ms,
        description: None,
        metadata: None,
    }
}

fn create_message(
    message_serial: &str,
    version_serial: &str,
    history_serial: u64,
    delivery_serial: u64,
    data: &str,
) -> VersionedMessage {
    VersionedMessage::new_create(
        MessageSerial::new(message_serial).unwrap(),
        version(version_serial, delivery_serial as i64),
        history_serial,
        delivery_serial,
        Some("ai-output".to_string()),
        Some(MessageData::String(data.to_string())),
        Some(ai_extras(&[("status", "streaming")])),
    )
}

fn ai_extras(headers: &[(&str, &str)]) -> MessageExtras {
    MessageExtras {
        ai: Some(AiExtras {
            transport: Some(
                headers
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                    .collect(),
            ),
            codec: Some(HashMap::from([(
                "content-type".to_string(),
                "text/plain".to_string(),
            )])),
        }),
        ..Default::default()
    }
}

fn append_wire(serial: &str, data: &str, now: i64, status: Option<&str>) -> PusherMessage {
    let mut message = PusherMessage {
        event: Some(MessageAction::Append.v2_event_name()),
        channel: Some("ai:room".to_string()),
        data: Some(MessageData::String(data.to_string())),
        name: None,
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: None,
        stream_id: None,
        serial: None,
        idempotency_key: None,
        extras: Some(ai_extras(&[("status", status.unwrap_or("streaming"))])),
        delta_sequence: None,
        delta_conflation_key: None,
    };
    apply_runtime_metadata(
        &mut message,
        MessageAction::Append,
        serial,
        &MessageVersionMetadata {
            serial: format!("v{now}"),
            client_id: None,
            timestamp_ms: now,
            description: None,
            metadata: None,
        },
        Some(now as u64),
    );
    message
}

fn string_data(message: &PusherMessage) -> &str {
    message
        .data
        .as_ref()
        .and_then(MessageData::as_string)
        .unwrap()
}

#[test]
fn ait_s001_002_message_actions_and_v2_event_names_match_spec() {
    let actions = [
        (MessageAction::Create, "message.create"),
        (MessageAction::Update, "message.update"),
        (MessageAction::Delete, "message.delete"),
        (MessageAction::Append, "message.append"),
        (MessageAction::Summary, "message.summary"),
    ];

    for (action, operation) in actions {
        assert_eq!(action.as_str(), operation);
        assert_eq!(action.v2_event_name(), format!("sockudo:{operation}"));
    }
}

#[test]
fn ait_s003_versioned_realtime_message_flattens_payload_and_validates_serials() {
    let message = PusherMessage {
        event: Some(MessageAction::Append.v2_event_name()),
        channel: Some("ai:room".to_string()),
        data: Some(MessageData::String("hello".to_string())),
        name: Some("ai-output".to_string()),
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: Some("client-message-1".to_string()),
        stream_id: Some("stream-1".to_string()),
        serial: Some(44),
        idempotency_key: None,
        extras: Some(ai_extras(&[("status", "streaming")])),
        delta_sequence: None,
        delta_conflation_key: None,
    };
    let frame = VersionedRealtimeMessage {
        message,
        action: MessageAction::Append,
        message_serial: "msg-1".to_string(),
        history_serial: Some(33),
        delivery_serial: Some(44),
        version: Some(MessageVersionMetadata {
            serial: "ver-1".to_string(),
            client_id: Some("client-1".to_string()),
            timestamp_ms: 1000,
            description: None,
            metadata: None,
        }),
    };

    frame.validate_v2().unwrap();
    let wire = sonic_rs::to_value(&frame).unwrap();
    assert_eq!(wire["event"].as_str(), Some("sockudo:message.append"));
    assert_eq!(wire["channel"].as_str(), Some("ai:room"));
    assert_eq!(wire["action"].as_str(), Some("append"));
    assert_eq!(wire["message_serial"].as_str(), Some("msg-1"));
    assert_eq!(wire["history_serial"].as_u64(), Some(33));
    assert_eq!(wire["delivery_serial"].as_u64(), Some(44));
    assert_eq!(wire["version"]["serial"].as_str(), Some("ver-1"));
}

#[test]
fn ait_s004_005_serial_newtypes_reject_empty_whitespace_and_overlong_values() {
    for invalid in ["", "   ", "has whitespace", &"x".repeat(129)] {
        assert!(MessageSerial::new(invalid).is_err());
        assert!(VersionSerial::new(invalid).is_err());
    }

    assert!(MessageSerial::new("msg-1").is_ok());
    assert!(VersionSerial::new("ver-1").is_ok());
}

#[test]
fn ait_s006_to_s010_version_chain_and_replay_validation_fail_closed() {
    let first = create_message("msg-1", "ver-1", 10, 20, "hello");
    let append = first
        .apply_append(
            version("ver-2", 21),
            21,
            MessageAppend {
                data_fragment: " world".to_string(),
                extras: None,
            },
        )
        .unwrap();

    validate_version_chain(&[first.clone(), append.clone()]).unwrap();
    validate_replay_continuity(&[first.clone(), append.clone()], 19).unwrap();

    let mixed_message = create_message("msg-2", "ver-3", 10, 22, "other");
    assert!(validate_version_chain(&[first.clone(), mixed_message]).is_err());

    let mixed_history = create_message("msg-1", "ver-3", 11, 22, "other");
    assert!(validate_version_chain(&[first.clone(), mixed_history]).is_err());

    let duplicate_version = first
        .apply_append(
            version("ver-2", 22),
            22,
            MessageAppend {
                data_fragment: "!".to_string(),
                extras: None,
            },
        )
        .unwrap();
    assert!(validate_version_chain(&[append.clone(), duplicate_version]).is_err());

    let duplicate_delivery = first
        .apply_append(
            version("ver-3", 21),
            21,
            MessageAppend {
                data_fragment: "!".to_string(),
                extras: None,
            },
        )
        .unwrap();
    assert!(validate_version_chain(&[append.clone(), duplicate_delivery]).is_err());

    assert!(validate_replay_continuity(&[append], 19).is_err());
}

#[tokio::test]
async fn ait_s011_012_memory_version_store_projects_latest_visible_state() {
    let store = MemoryVersionStore::new();
    let first = create_message("msg-1", "ver-1", 10, 20, "a");
    let second = first
        .apply_append(
            version("ver-2", 21),
            21,
            MessageAppend {
                data_fragment: "b".to_string(),
                extras: Some(ai_extras(&[("status", "complete")])),
            },
        )
        .unwrap();
    let other = create_message("msg-2", "ver-1", 11, 22, "z");

    for message in [first, second.clone(), other.clone()] {
        store
            .append_version(sockudo_core::version_store::StoredVersionRecord {
                app_id: "app".to_string(),
                channel: "ai:room".to_string(),
                original_client_id: Some("client-1".to_string()),
                envelope: None,
                message,
            })
            .await
            .unwrap();
    }

    let latest = store
        .get_latest("app", "ai:room", &MessageSerial::new("msg-1").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.version_serial().as_str(), "ver-2");
    assert_eq!(
        latest.message.data.as_ref().unwrap().as_string(),
        Some("ab")
    );

    let projected = store.latest_by_history("app", "ai:room").await.unwrap();
    assert_eq!(projected.len(), 2);
    assert_eq!(projected[0].message_serial().as_str(), "msg-1");
    assert_eq!(projected[1].message_serial().as_str(), "msg-2");
}

#[test]
fn ait_s026_to_s030_history_cursor_and_bounds_validation_are_strict() {
    let cursor = HistoryCursor {
        version: 1,
        app_id: "app".to_string(),
        channel: "ai:room".to_string(),
        stream_id: "stream-1".to_string(),
        serial: 9,
        direction: HistoryDirection::NewestFirst,
        bounds: HistoryQueryBounds {
            start_serial: Some(1),
            end_serial: Some(10),
            start_time_ms: None,
            end_time_ms: None,
        },
    };
    let encoded = cursor.encode().unwrap();
    assert_eq!(HistoryCursor::decode(&encoded).unwrap(), cursor);

    let valid = HistoryReadRequest {
        app_id: "app".to_string(),
        channel: "ai:room".to_string(),
        direction: HistoryDirection::NewestFirst,
        limit: 10,
        cursor: Some(cursor.clone()),
        bounds: cursor.bounds.clone(),
    };
    valid.validate().unwrap();

    assert!(
        HistoryReadRequest {
            limit: 0,
            cursor: None,
            ..valid.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        HistoryReadRequest {
            bounds: HistoryQueryBounds {
                start_serial: Some(10),
                end_serial: Some(1),
                ..HistoryQueryBounds::default()
            },
            cursor: None,
            ..valid.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        HistoryReadRequest {
            bounds: HistoryQueryBounds {
                start_time_ms: Some(10),
                end_time_ms: Some(1),
                ..HistoryQueryBounds::default()
            },
            cursor: None,
            ..valid.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        HistoryReadRequest {
            channel: "other".to_string(),
            ..valid
        }
        .validate()
        .is_err()
    );
}

#[test]
fn ait_s053_054_ai_feature_and_runtime_channel_gate_defaults_are_closed() {
    let mut config = sockudo_core::options::AiTransportConfig {
        channels: vec![AiTransportChannelConfig {
            prefix: "ai:".to_string(),
        }],
        ..Default::default()
    };

    assert!(!config.enabled);
    assert!(!config.matches_channel("ai:room"));
    config.enabled = true;
    assert!(config.matches_channel("ai:room"));
    assert!(!config.matches_channel("chat"));
}

#[test]
fn ait_s055_to_s064_ai_event_and_header_rules_match_registry() {
    for event in [
        AI_EVENT_INPUT,
        AI_EVENT_OUTPUT,
        AI_EVENT_RUN_START,
        AI_EVENT_RUN_SUSPEND,
        AI_EVENT_RUN_RESUME,
        AI_EVENT_RUN_END,
        AI_EVENT_CANCEL,
    ] {
        assert!(is_ai_event(event));
    }
    for legacy_event in [AI_EVENT_LEGACY_TURN_START, AI_EVENT_LEGACY_TURN_END] {
        assert!(is_ai_event(legacy_event));
    }
    assert!(is_ai_client_publish_event(AI_EVENT_INPUT));
    assert!(is_ai_client_publish_event(AI_EVENT_CANCEL));
    assert!(!is_ai_client_publish_event(AI_EVENT_OUTPUT));
    assert!(!is_ai_event("ai-unknown"));

    ai_extras(&[
        ("run-id", "run-1"),
        ("run-client-id", "client-1"),
        ("run-reason", "complete"),
        ("codec-message-id", "msg-1"),
        ("input-codec-message-id", "input-msg-1"),
        ("parent", "msg-0"),
        ("fork-of", "msg-old"),
        ("msg-regenerate", "msg-old"),
        ("stream", "true"),
        ("stream-id", "stream-1"),
        ("status", "streaming"),
        ("discrete", "false"),
        ("invocation-id", "invoke-1"),
        ("event-id", "event-1"),
        ("input-client-id", "client-1"),
        ("role", "assistant"),
        ("error-code", "tool-timeout"),
        ("error-message", "bounded message"),
    ])
    .validate_ai_headers()
    .unwrap();

    assert!(
        ai_extras(&[("unknown", "x")])
            .validate_ai_headers()
            .is_err()
    );
    assert!(
        ai_extras(&[("status", "done")])
            .validate_ai_headers()
            .is_err()
    );
    assert!(
        ai_extras(&[("run-reason", "paused")])
            .validate_ai_headers()
            .is_err()
    );
    assert!(
        ai_extras(&[("run-reason", "suspended")])
            .validate_ai_headers()
            .is_err()
    );
    ai_extras(&[
        ("turn-id", "legacy-turn-1"),
        ("turn-client-id", "client-1"),
        ("turn-reason", "suspended"),
        ("turn-continue", "false"),
    ])
    .validate_ai_headers()
    .unwrap();
    assert!(ai_extras(&[("role", "bot")]).validate_ai_headers().is_err());
    assert!(
        ai_extras(&[("stream", "yes")])
            .validate_ai_headers()
            .is_err()
    );

    let too_many = MessageExtras {
        ai: Some(AiExtras {
            transport: Some(
                (0..=AI_TRANSPORT_TIER_LIMIT)
                    .map(|index| (format!("x{index}"), "v".to_string()))
                    .collect(),
            ),
            codec: None,
        }),
        ..Default::default()
    };
    assert!(too_many.validate_ai_headers().is_err());

    let long_key = "a".repeat(AI_TRANSPORT_KEY_MAX_BYTES + 1);
    assert!(
        ai_extras(&[(long_key.as_str(), "v")])
            .validate_ai_headers()
            .is_err()
    );
    let long_value = "v".repeat(AI_TRANSPORT_VALUE_MAX_BYTES + 1);
    assert!(
        ai_extras(&[("status", long_value.as_str())])
            .validate_ai_headers()
            .is_err()
    );
}

#[test]
fn ait_s069_to_s073_rollup_table_and_reduced_state_are_deterministic() {
    let config = AiTransportRollupConfig::default();
    for window in [0, 20, 40, 100, 500] {
        assert!(config.allows_window(window));
        assert!(is_allowed_append_rollup_window_ms(window));
    }
    for window in [1, 19, 39, 250, 501] {
        assert!(!config.allows_window(window));
        assert!(!is_allowed_append_rollup_window_ms(window));
    }

    let engine = RollupEngine::new(RollupConfig::default());
    let first = engine.ingest("app", "ai:room", append_wire("msg-1", "a", 1, None), 0);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].reason, RollupDeliveryReason::Immediate);

    assert!(
        engine
            .ingest("app", "ai:room", append_wire("msg-1", "ab", 2, None), 10)
            .is_empty()
    );
    assert!(
        engine
            .ingest("app", "ai:room", append_wire("msg-1", "abc", 3, None), 20)
            .is_empty()
    );

    let due = engine.flush_due(40);
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].reason, RollupDeliveryReason::Deadline);
    assert_eq!(string_data(&due[0].message), "abc");

    let engine = RollupEngine::new(RollupConfig::default());
    let _ = engine.ingest("app", "ai:room", append_wire("msg-2", "x", 1, None), 0);
    let _ = engine.ingest("app", "ai:room", append_wire("msg-2", "xy", 2, None), 10);
    let terminal = engine.ingest(
        "app",
        "ai:room",
        append_wire("msg-2", "xyz", 3, Some("complete")),
        11,
    );
    assert_eq!(terminal.len(), 2);
    assert_eq!(terminal[0].reason, RollupDeliveryReason::TerminalFlush);
    assert_eq!(terminal[1].reason, RollupDeliveryReason::Terminal);
    assert_eq!(string_data(&terminal[1].message), "xyz");
}

#[test]
fn ait_s083_to_s086_presence_timeout_defaults_off_and_can_be_configured() {
    let default = PresenceConfig::default();
    assert_eq!(default.ungraceful_timeout_seconds, 0);

    let enabled = PresenceConfig {
        ungraceful_timeout_seconds: 15,
        ..PresenceConfig::default()
    };
    assert_eq!(enabled.ungraceful_timeout_seconds, 15);
}

#[test]
fn ait_s094_runtime_metadata_tracks_terminal_append_status() {
    let terminal = append_wire("msg-1", "done", 10, Some("complete"));
    assert_eq!(
        extract_runtime_action(&terminal),
        Some(MessageAction::Append)
    );
    assert_eq!(extract_runtime_message_serial(&terminal), Some("msg-1"));
    assert_eq!(
        terminal
            .ai_transport_headers()
            .and_then(|headers| headers.status()),
        Some("complete")
    );
}
