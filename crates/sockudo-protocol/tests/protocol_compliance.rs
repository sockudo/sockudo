use sockudo_protocol::messages::{
    AI_CODEC_PROVIDER_METADATA_MAX_BYTES, AI_ERROR_HEADER_TOO_LARGE,
    AI_ERROR_INVALID_TRANSPORT_HEADER, AI_EVENT_CANCEL, AI_EVENT_INPUT, AI_EVENT_LEGACY_TURN_END,
    AI_EVENT_LEGACY_TURN_START, AI_EVENT_OUTPUT, AI_EVENT_RUN_END, AI_EVENT_RUN_RESUME,
    AI_EVENT_RUN_START, AI_EVENT_RUN_SUSPEND, AI_HEADER_LEGACY_TURN_ID, AI_HEADER_MSG_REGENERATE,
    AI_HEADER_RUN_CLIENT_ID, AI_HEADER_RUN_ID, AI_TRANSPORT_VALUE_MAX_BYTES, AiExtras, ExtrasValue,
    MessageData, MessageExtras, PusherMessage, is_ai_event,
};
use sonic_rs::prelude::*;
use sonic_rs::{Value, json};
use std::collections::HashMap;

// Helper function to serialize message and parse as JSON for testing
fn message_to_json(message: &PusherMessage) -> Value {
    sonic_rs::to_value(message).expect("Failed to serialize message")
}

#[test]
fn test_connection_established_format() {
    // According to spec: data should be a String (JSON-encoded object)
    let message = PusherMessage::connection_established("test-socket-123".to_string(), 120);
    let json = message_to_json(&message);

    // Assert event field exists and has correct value
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "pusher:connection_established");

    // Assert data field exists and is a string (per Pusher spec)
    assert!(json.get("data").is_some(), "Should have 'data' field");
    assert!(
        json["data"].is_str(),
        "Data field should be a String (JSON-encoded)"
    );

    // Verify the string contains valid JSON
    let data_str = json["data"].as_str().expect("Data should be a string");
    let parsed_data: Value =
        sonic_rs::from_str(data_str).expect("Data string should contain valid JSON");

    // Assert parsed data has correct structure
    assert!(parsed_data.is_object(), "Parsed data should be an object");
    assert!(
        parsed_data.get("socket_id").is_some(),
        "Should have socket_id field"
    );
    assert!(
        parsed_data["socket_id"].is_str(),
        "socket_id should be a string"
    );
    assert_eq!(parsed_data["socket_id"], "test-socket-123");

    assert!(
        parsed_data.get("activity_timeout").is_some(),
        "Should have activity_timeout field"
    );
    assert!(
        parsed_data["activity_timeout"].is_number(),
        "activity_timeout should be a number"
    );
    assert_eq!(parsed_data["activity_timeout"], 120);
}

#[test]
fn test_error_format() {
    // According to spec: data should be an Object with message and code
    let message = PusherMessage::error(4001, "Application does not exist".to_string(), None);
    let json = message_to_json(&message);

    // Assert event field exists and has correct value
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "pusher:error");

    // Assert data field exists and is an object (per Pusher spec)
    assert!(json.get("data").is_some(), "Should have 'data' field");
    assert!(json["data"].is_object(), "Data field should be an Object");

    let data = json["data"].as_object().expect("Data should be an object");

    // Assert data object has correct structure
    assert!(data.get(&"code").is_some(), "Should have 'code' field");
    assert!(data["code"].is_number(), "Code should be a number");
    assert_eq!(data["code"], 4001);

    assert!(
        data.get(&"message").is_some(),
        "Should have 'message' field"
    );
    assert!(data["message"].is_str(), "Message should be a string");
    assert_eq!(data["message"], "Application does not exist");
}

#[test]
fn test_v1_channel_event_json_snapshot_is_stable_without_extras() {
    let message = PusherMessage::channel_event("client-test", "private-room", json!({"x":1}));
    let json = sonic_rs::to_string(&message).expect("serialize");
    assert_eq!(
        json,
        r#"{"event":"client-test","channel":"private-room","data":"{\"x\":1}"}"#
    );
}

#[test]
fn test_ai_event_constants_and_predicate() {
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
    assert!(!is_ai_event("client-typing"));
}

#[test]
fn test_ai_transport_headers_are_borrowed_views() {
    let mut transport = HashMap::new();
    transport.insert(AI_HEADER_RUN_ID.to_string(), "run-1".to_string());
    transport.insert("status".to_string(), "streaming".to_string());
    transport.insert("parent".to_string(), "msg-0".to_string());
    transport.insert("fork-of".to_string(), "msg-old".to_string());

    let extras = sockudo_protocol::messages::MessageExtras {
        ai: Some(AiExtras {
            transport: Some(transport),
            codec: Some(HashMap::from([(
                "encoding".to_string(),
                "json".to_string(),
            )])),
        }),
        ..Default::default()
    };

    let headers = extras.ai_transport_headers().expect("transport headers");
    assert_eq!(headers.run_id(), Some("run-1"));
    assert_eq!(headers.turn_id(), Some("run-1"));
    assert_eq!(headers.status(), Some("streaming"));
    assert_eq!(headers.parent(), Some("msg-0"));
    assert_eq!(headers.fork_of(), Some("msg-old"));
    assert_eq!(
        extras
            .ai_codec_headers()
            .and_then(|codec| codec.get("encoding"))
            .map(String::as_str),
        Some("json")
    );
}

#[test]
fn test_ai_header_validation_rejects_limits_and_domains() {
    let oversized = sockudo_protocol::messages::MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([(
                AI_HEADER_RUN_ID.to_string(),
                "x".repeat(257),
            )])),
            codec: None,
        }),
        ..Default::default()
    };
    assert_eq!(
        oversized.validate_ai_headers().unwrap_err().code,
        AI_ERROR_HEADER_TOO_LARGE
    );

    let bad_key = sockudo_protocol::messages::MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([("TurnId".to_string(), "x".to_string())])),
            codec: None,
        }),
        ..Default::default()
    };
    assert_eq!(
        bad_key.validate_ai_headers().unwrap_err().code,
        AI_ERROR_INVALID_TRANSPORT_HEADER
    );

    let bad_status = sockudo_protocol::messages::MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([("status".to_string(), "done".to_string())])),
            codec: None,
        }),
        ..Default::default()
    };
    assert_eq!(
        bad_status.validate_ai_headers().unwrap_err().code,
        AI_ERROR_INVALID_TRANSPORT_HEADER
    );
}

#[test]
fn test_ai_codec_headers_accept_ably_camel_case_keys() {
    let extras = MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([(
                AI_HEADER_RUN_ID.to_string(),
                "run-1".to_string(),
            )])),
            codec: Some(HashMap::from([
                ("messageId".to_string(), "msg-1".to_string()),
                ("finishReason".to_string(), "stop".to_string()),
                ("providerMetadata".to_string(), "{}".to_string()),
                ("toolCallId".to_string(), "tool-1".to_string()),
            ])),
        }),
        ..Default::default()
    };

    assert!(extras.validate_ai_headers().is_ok());
}

#[test]
fn test_ai_codec_provider_metadata_accepts_ably_sized_payloads() {
    let provider_metadata = format!(
        "{{\"gateway\":\"{}\"}}",
        "x".repeat(AI_TRANSPORT_VALUE_MAX_BYTES + 1)
    );
    assert!(provider_metadata.len() < AI_CODEC_PROVIDER_METADATA_MAX_BYTES);

    let extras = MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([(
                AI_HEADER_RUN_ID.to_string(),
                "run-1".to_string(),
            )])),
            codec: Some(HashMap::from([(
                "providerMetadata".to_string(),
                provider_metadata,
            )])),
        }),
        ..Default::default()
    };

    assert!(extras.validate_ai_headers().is_ok());
}

#[test]
fn test_ai_codec_provider_metadata_remains_bounded() {
    let extras = MessageExtras {
        ai: Some(AiExtras {
            transport: None,
            codec: Some(HashMap::from([(
                "providerMetadata".to_string(),
                "x".repeat(AI_CODEC_PROVIDER_METADATA_MAX_BYTES + 1),
            )])),
        }),
        ..Default::default()
    };

    let error = extras.validate_ai_headers().unwrap_err();
    assert_eq!(error.code, AI_ERROR_HEADER_TOO_LARGE);
    assert!(
        error.message.contains("providerMetadata"),
        "unexpected error: {}",
        error.message
    );
}

#[test]
fn test_ai_codec_other_values_stay_small() {
    let extras = MessageExtras {
        ai: Some(AiExtras {
            transport: None,
            codec: Some(HashMap::from([(
                "finishReason".to_string(),
                "x".repeat(AI_TRANSPORT_VALUE_MAX_BYTES + 1),
            )])),
        }),
        ..Default::default()
    };

    assert_eq!(
        extras.validate_ai_headers().unwrap_err().code,
        AI_ERROR_HEADER_TOO_LARGE
    );
}

#[test]
fn test_ai_codec_headers_reject_unsafe_keys() {
    let extras = MessageExtras {
        ai: Some(AiExtras {
            transport: None,
            codec: Some(HashMap::from([(
                "message.id".to_string(),
                "msg-1".to_string(),
            )])),
        }),
        ..Default::default()
    };

    assert_eq!(
        extras.validate_ai_headers().unwrap_err().code,
        AI_ERROR_INVALID_TRANSPORT_HEADER
    );
}

#[test]
fn test_ai_transport_model_key_accepted() {
    let extras = MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([
                (AI_HEADER_RUN_ID.to_string(), "run-1".to_string()),
                ("model".to_string(), "gpt-4o".to_string()),
            ])),
            codec: None,
        }),
        ..Default::default()
    };
    assert!(extras.validate_ai_headers().is_ok());

    let headers = extras.ai_transport_headers().expect("transport headers");
    assert_eq!(headers.model(), Some("gpt-4o"));
    assert_eq!(headers.run_id(), Some("run-1"));
}

#[test]
fn test_ai_transport_model_key_rejects_empty() {
    let extras = MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([("model".to_string(), "".to_string())])),
            codec: None,
        }),
        ..Default::default()
    };
    assert_eq!(
        extras.validate_ai_headers().unwrap_err().code,
        AI_ERROR_INVALID_TRANSPORT_HEADER
    );
}

#[test]
fn test_ai_transport_identity_keys_reject_empty_except_native_unknown_owner() {
    let identity_keys = [
        (AI_HEADER_RUN_ID, false),
        (AI_HEADER_RUN_CLIENT_ID, true),
        ("turn-client-id", false),
        ("input-client-id", false),
        ("input-codec-message-id", false),
        ("codec-message-id", false),
        ("parent", false),
        ("fork-of", false),
        ("stream-id", false),
        ("invocation-id", false),
        ("event-id", false),
        ("step-id", false),
        ("step-client-id", false),
        ("start-serial", false),
        ("msg-regenerate", false),
        ("model", false),
    ];

    for (key, allowed) in identity_keys {
        let extras = MessageExtras {
            ai: Some(AiExtras {
                transport: Some(HashMap::from([(key.to_string(), String::new())])),
                codec: None,
            }),
            ..Default::default()
        };

        assert_eq!(extras.validate_ai_headers().is_ok(), allowed, "key={key}");
    }
}

#[test]
fn test_ai_transport_unknown_owner_sentinel_is_wire_preserved_but_not_identity() {
    let extras = MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([
                (AI_HEADER_RUN_ID.to_string(), "run-1".to_string()),
                (AI_HEADER_RUN_CLIENT_ID.to_string(), String::new()),
            ])),
            codec: None,
        }),
        ..Default::default()
    };

    let headers = extras.ai_transport_headers().expect("transport headers");
    assert_eq!(headers.run_client_id(), Some(""));
    assert_eq!(headers.run_client_identity(), None);
    let wire = sonic_rs::to_value(&extras).expect("serialize extras");
    assert_eq!(wire["ai"]["transport"][AI_HEADER_RUN_CLIENT_ID], "");
}

#[test]
fn test_ai_transport_model_key_optional() {
    let extras = MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([(
                AI_HEADER_RUN_ID.to_string(),
                "run-1".to_string(),
            )])),
            codec: None,
        }),
        ..Default::default()
    };
    assert!(extras.validate_ai_headers().is_ok());

    let headers = extras.ai_transport_headers().expect("transport headers");
    assert_eq!(headers.model(), None);
}

#[test]
fn test_ai_transport_msg_regenerate_accepts_ably_message_id() {
    let extras = MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([
                (AI_HEADER_RUN_ID.to_string(), "run-1".to_string()),
                (
                    AI_HEADER_MSG_REGENERATE.to_string(),
                    "7f997e32-3a4d-4635-bb34-778c6952946b".to_string(),
                ),
            ])),
            codec: None,
        }),
        ..Default::default()
    };

    assert!(extras.validate_ai_headers().is_ok());
    let headers = extras.ai_transport_headers().expect("transport headers");
    assert_eq!(
        headers.msg_regenerate(),
        Some("7f997e32-3a4d-4635-bb34-778c6952946b")
    );
}

#[test]
fn test_ai_transport_msg_regenerate_rejects_empty() {
    let extras = MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([(
                AI_HEADER_MSG_REGENERATE.to_string(),
                "".to_string(),
            )])),
            codec: None,
        }),
        ..Default::default()
    };

    assert_eq!(
        extras.validate_ai_headers().unwrap_err().code,
        AI_ERROR_INVALID_TRANSPORT_HEADER
    );
}

#[test]
fn test_ai_transport_legacy_turn_id_alias_is_still_readable() {
    let extras = MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([(
                AI_HEADER_LEGACY_TURN_ID.to_string(),
                "legacy-turn-1".to_string(),
            )])),
            codec: None,
        }),
        ..Default::default()
    };

    let headers = extras.ai_transport_headers().expect("transport headers");
    assert_eq!(headers.run_id(), Some("legacy-turn-1"));
    assert_eq!(headers.turn_id(), Some("legacy-turn-1"));
    assert!(extras.validate_ai_headers().is_ok());
}

#[test]
fn test_signin_success_format() {
    // According to spec: data should be an Object with only user_data field
    let user_data = r#"{"id":"123","name":"John"}"#.to_string();

    let message = PusherMessage::signin_success(user_data);

    let json = message_to_json(&message);

    // Assert event field exists and has correct value
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "pusher:signin_success");

    // Assert data field exists and is an object (per Pusher spec)
    assert!(json.get("data").is_some(), "Should have 'data' field");
    assert!(json["data"].is_object(), "Data field should be an Object");

    let data = json["data"].as_object().expect("Data should be an object");

    // Assert data object has correct structure
    assert!(
        data.contains_key(&"user_data"),
        "Should have user_data field"
    );
    assert!(!data.contains_key(&"auth"), "Should NOT have auth field");
    assert!(data["user_data"].is_str(), "user_data should be a string");
    assert_eq!(data["user_data"], r#"{"id":"123","name":"John"}"#);
}

#[test]
fn test_ping_format() {
    // According to spec: should have NO data field at all
    let message = PusherMessage::ping();
    let json = message_to_json(&message);

    // Assert event field exists and has correct value
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "pusher:ping");

    // Assert no data field exists (per Pusher spec)
    assert!(
        json.get("data").is_none(),
        "Ping should not have 'data' field"
    );
}

#[test]
fn test_pong_format() {
    // According to spec: should have NO data field at all
    let message = PusherMessage::pong();
    let json = message_to_json(&message);

    // Assert event field exists and has correct value
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "pusher:pong");

    // Assert no data field exists (per Pusher spec)
    assert!(
        json.get("data").is_none(),
        "Pong should not have 'data' field"
    );
}

#[test]
fn test_subscription_succeeded_format() {
    // According to spec: data should be a String (JSON-encoded object)
    // For presence channels, it contains presence data
    use ahash::AHashMap;
    use sockudo_protocol::messages::PresenceData;

    let mut hash = AHashMap::new();
    hash.insert("user1".to_string(), Some(json!({"name": "Alice"})));
    hash.insert("user2".to_string(), Some(json!({"name": "Bob"})));

    let presence_data = PresenceData {
        ids: vec!["user1".to_string(), "user2".to_string()],
        hash: hash.clone(),
        count: 2,
    };

    let message =
        PusherMessage::subscription_succeeded("presence-room".to_string(), Some(presence_data));
    let json = message_to_json(&message);

    // Assert event and channel fields exist and have correct values
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "pusher_internal:subscription_succeeded");

    assert!(json.get("channel").is_some(), "Should have 'channel' field");
    assert!(json["channel"].is_str(), "Channel field should be a string");
    assert_eq!(json["channel"], "presence-room");

    // Assert data field exists and is a string (per Pusher spec)
    assert!(json.get("data").is_some(), "Should have 'data' field");
    assert!(
        json["data"].is_str(),
        "Data field should be a String (JSON-encoded)"
    );

    // Verify the string contains valid JSON with presence data
    let data_str = json["data"].as_str().expect("Data should be a string");
    let parsed_data: Value =
        sonic_rs::from_str(data_str).expect("Data string should contain valid JSON");

    // Assert parsed data has correct structure
    assert!(parsed_data.is_object(), "Parsed data should be an object");
    assert!(
        parsed_data.get("presence").is_some(),
        "Should have 'presence' field"
    );

    // Verify the presence data structure (not double-wrapped)
    let presence = &parsed_data["presence"];
    assert_eq!(presence["count"], 2);
    assert_eq!(presence["ids"], json!(["user1", "user2"]));
    assert_eq!(presence["hash"]["user1"], json!({"name": "Alice"}));
    assert_eq!(presence["hash"]["user2"], json!({"name": "Bob"}));
}

#[test]
fn test_subscription_succeeded_non_presence_format() {
    // For non-presence channels, data should be empty object as string
    let message = PusherMessage::subscription_succeeded("private-channel".to_string(), None);
    let json = message_to_json(&message);

    // Assert event and channel fields exist and have correct values
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "pusher_internal:subscription_succeeded");

    assert!(json.get("channel").is_some(), "Should have 'channel' field");
    assert!(json["channel"].is_str(), "Channel field should be a string");
    assert_eq!(json["channel"], "private-channel");

    // Assert data field exists and is a string (per Pusher spec)
    assert!(json.get("data").is_some(), "Should have 'data' field");
    assert!(
        json["data"].is_str(),
        "Data field should be a String (JSON-encoded)"
    );

    // Verify the string contains empty JSON object
    let data_str = json["data"].as_str().expect("Data should be a string");
    let parsed_data: Value =
        sonic_rs::from_str(data_str).expect("Data string should contain valid JSON");

    // Assert parsed data is an empty object
    assert!(parsed_data.is_object(), "Parsed data should be an object");
    assert_eq!(parsed_data, json!({}));
}

#[test]
fn test_member_added_format() {
    // According to spec: data should be a String (JSON-encoded object)
    let user_info = json!({"name": "Alice", "email": "alice@example.com"});

    let message = PusherMessage::member_added(
        "presence-room".to_string(),
        "user123".to_string(),
        Some(user_info.clone()),
    );
    let json = message_to_json(&message);

    // Assert event and channel fields exist and have correct values
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "pusher_internal:member_added");

    assert!(json.get("channel").is_some(), "Should have 'channel' field");
    assert!(json["channel"].is_str(), "Channel field should be a string");
    assert_eq!(json["channel"], "presence-room");

    // Assert data field exists and is a string (per Pusher spec)
    assert!(json.get("data").is_some(), "Should have 'data' field");
    assert!(
        json["data"].is_str(),
        "Data field should be a String (JSON-encoded)"
    );

    // Verify the string contains valid JSON
    let data_str = json["data"].as_str().expect("Data should be a string");
    let parsed_data: Value =
        sonic_rs::from_str(data_str).expect("Data string should contain valid JSON");

    // Assert parsed data has correct structure
    assert!(parsed_data.is_object(), "Parsed data should be an object");
    assert!(
        parsed_data.get("user_id").is_some(),
        "Should have 'user_id' field"
    );
    assert!(
        parsed_data["user_id"].is_str(),
        "user_id should be a string"
    );
    assert_eq!(parsed_data["user_id"], "user123");

    assert!(
        parsed_data.get("user_info").is_some(),
        "Should have 'user_info' field"
    );
    assert_eq!(parsed_data["user_info"], user_info);
}

#[test]
fn test_member_removed_format() {
    // According to spec: data should be a String (JSON-encoded object)
    let message = PusherMessage::member_removed("presence-room".to_string(), "user123".to_string());
    let json = message_to_json(&message);

    // Assert event and channel fields exist and have correct values
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "pusher_internal:member_removed");

    assert!(json.get("channel").is_some(), "Should have 'channel' field");
    assert!(json["channel"].is_str(), "Channel field should be a string");
    assert_eq!(json["channel"], "presence-room");

    // Assert data field exists and is a string (per Pusher spec)
    assert!(json.get("data").is_some(), "Should have 'data' field");
    assert!(
        json["data"].is_str(),
        "Data field should be a String (JSON-encoded)"
    );

    // Verify the string contains valid JSON
    let data_str = json["data"].as_str().expect("Data should be a string");
    let parsed_data: Value =
        sonic_rs::from_str(data_str).expect("Data string should contain valid JSON");

    // Assert parsed data has correct structure
    assert!(parsed_data.is_object(), "Parsed data should be an object");
    assert!(
        parsed_data.get("user_id").is_some(),
        "Should have 'user_id' field"
    );
    assert!(
        parsed_data["user_id"].is_str(),
        "user_id should be a string"
    );
    assert_eq!(parsed_data["user_id"], "user123");

    assert!(
        parsed_data.get("user_info").is_none(),
        "Should not have user_info"
    );
}

#[test]
fn test_channel_event_format() {
    // According to spec: data should be a String for channel events
    let event_data = json!({"message": "Hello", "timestamp": 1234567890});

    let message = PusherMessage::channel_event("my-event", "my-channel", event_data.clone());
    let json = message_to_json(&message);

    // Assert event and channel fields exist and have correct values
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "my-event");

    assert!(json.get("channel").is_some(), "Should have 'channel' field");
    assert!(json["channel"].is_str(), "Channel field should be a string");
    assert_eq!(json["channel"], "my-channel");

    // Assert data field exists and is a string (per Pusher spec)
    assert!(json.get("data").is_some(), "Should have 'data' field");
    assert!(json["data"].is_str(), "Data field should be a String");

    // Verify the string contains the JSON data
    let data_str = json["data"].as_str().expect("Data should be a string");
    let parsed_data: Value =
        sonic_rs::from_str(data_str).expect("Data string should contain valid JSON");

    // Assert parsed data matches expected event data
    assert_eq!(parsed_data, event_data);
}

#[test]
fn test_channel_event_with_user_id() {
    // Test that channel events can include optional user_id field (per Pusher spec)
    let event_data = json!({"message": "Hello from Alice"});

    let message = PusherMessage {
        event: Some("my-event".to_string()),
        channel: Some("presence-room".to_string()),
        data: Some(MessageData::String(event_data.to_string())),
        name: None,
        user_id: Some("user123".to_string()),
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: None,
        stream_id: None,
        serial: None,
        idempotency_key: None,
        extras: None,
        delta_sequence: None,
        delta_conflation_key: None,
    };
    let json = message_to_json(&message);

    // Assert event and channel fields exist and have correct values
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "my-event");

    assert!(json.get("channel").is_some(), "Should have 'channel' field");
    assert!(json["channel"].is_str(), "Channel field should be a string");
    assert_eq!(json["channel"], "presence-room");

    // Assert data field exists and is a string (per Pusher spec)
    assert!(json.get("data").is_some(), "Should have 'data' field");
    assert!(json["data"].is_str(), "Data field should be a String");

    // Assert user_id field exists and has correct value (per Pusher spec)
    assert!(json.get("user_id").is_some(), "Should have 'user_id' field");
    assert!(json["user_id"].is_str(), "user_id should be a string");
    assert_eq!(json["user_id"], "user123");
}

#[test]
fn test_encrypted_channel_event_format() {
    // According to spec: data should be a String containing JSON with ciphertext and nonce
    // This tests that our format can handle encrypted data properly
    let encrypted_data = json!({
        "ciphertext": "encrypted_content_here",
        "nonce": "random_nonce_value"
    });

    let message = PusherMessage::channel_event(
        "my-event",
        "private-encrypted-channel",
        encrypted_data.clone(),
    );
    let json = message_to_json(&message);

    // Assert event and channel fields exist and have correct values
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "my-event");

    assert!(json.get("channel").is_some(), "Should have 'channel' field");
    assert!(json["channel"].is_str(), "Channel field should be a string");
    assert_eq!(json["channel"], "private-encrypted-channel");

    // Assert data field exists and is a string (per Pusher spec)
    assert!(json.get("data").is_some(), "Should have 'data' field");
    assert!(
        json["data"].is_str(),
        "Data field should be a String (JSON-encoded)"
    );

    // Verify the string contains the encrypted data JSON
    let data_str = json["data"].as_str().expect("Data should be a string");
    let parsed_data: Value =
        sonic_rs::from_str(data_str).expect("Data string should contain valid JSON");

    // Assert parsed data has correct encrypted structure
    assert!(parsed_data.is_object(), "Parsed data should be an object");
    assert!(
        parsed_data.get("ciphertext").is_some(),
        "Should have 'ciphertext' field"
    );
    assert!(
        parsed_data["ciphertext"].is_str(),
        "ciphertext should be a string"
    );
    assert_eq!(parsed_data["ciphertext"], "encrypted_content_here");

    assert!(
        parsed_data.get("nonce").is_some(),
        "Should have 'nonce' field"
    );
    assert!(parsed_data["nonce"].is_str(), "nonce should be a string");
    assert_eq!(parsed_data["nonce"], "random_nonce_value");
}

#[test]
fn test_client_event_accepts_string() {
    // Client events should accept String data
    let message = PusherMessage {
        channel: Some("private-channel".to_string()),
        event: Some("client-typing".to_string()),
        data: Some(MessageData::String("user is typing...".to_string())),
        name: None,
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: None,
        stream_id: None,
        serial: None,
        idempotency_key: None,
        extras: None,
        delta_sequence: None,
        delta_conflation_key: None,
    };

    let json = message_to_json(&message);

    // Assert event and channel fields exist and have correct values
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "client-typing");

    assert!(json.get("channel").is_some(), "Should have 'channel' field");
    assert!(json["channel"].is_str(), "Channel field should be a string");
    assert_eq!(json["channel"], "private-channel");

    // Assert data field exists and is a string
    assert!(json.get("data").is_some(), "Should have 'data' field");
    assert!(json["data"].is_str(), "Data should be a string");
    assert_eq!(json["data"], "user is typing...");
}

#[test]
fn test_client_event_accepts_json() {
    // Client events should also accept JSON object data
    let message = PusherMessage {
        channel: Some("private-channel".to_string()),
        event: Some("client-typing".to_string()),
        data: Some(MessageData::Json(
            json!({"user": "alice", "status": "typing"}),
        )),
        name: None,
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: None,
        stream_id: None,
        serial: None,
        idempotency_key: None,
        extras: None,
        delta_sequence: None,
        delta_conflation_key: None,
    };

    let json = message_to_json(&message);

    // Assert event and channel fields exist and have correct values
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "client-typing");

    assert!(json.get("channel").is_some(), "Should have 'channel' field");
    assert!(json["channel"].is_str(), "Channel field should be a string");
    assert_eq!(json["channel"], "private-channel");

    // Assert data field exists and is an object
    assert!(json.get("data").is_some(), "Should have 'data' field");
    assert!(json["data"].is_object(), "Data should be an object");

    // Assert data object has correct structure
    assert!(
        json["data"].get("user").is_some(),
        "Should have 'user' field in data"
    );
    assert!(json["data"]["user"].is_str(), "User should be a string");
    assert_eq!(json["data"]["user"], "alice");

    assert!(
        json["data"].get("status").is_some(),
        "Should have 'status' field in data"
    );
    assert!(json["data"]["status"].is_str(), "Status should be a string");
    assert_eq!(json["data"]["status"], "typing");
}

#[test]
fn test_cache_miss_event_format() {
    // According to spec: cache_miss event should send an empty JSON object as a string
    let message = PusherMessage::cache_miss_event("cache-channel".to_string());
    let json = message_to_json(&message);

    // Assert event field exists and has correct value
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "pusher:cache_miss");

    // Assert channel field exists and has correct value
    assert!(json.get("channel").is_some(), "Should have 'channel' field");
    assert!(json["channel"].is_str(), "Channel field should be a string");
    assert_eq!(json["channel"], "cache-channel");

    // Assert data field exists and is a string containing empty JSON object
    assert!(json.get("data").is_some(), "Should have 'data' field");
    assert!(
        json["data"].is_str(),
        "Data field should be a String (empty JSON object)"
    );

    // Verify the string is exactly "{}"
    let data_str = json["data"].as_str().expect("Data should be a string");
    assert_eq!(data_str, "{}", "Data should be exactly '{{}}'");

    // Also verify it parses to an empty JSON object
    let parsed_data: Value =
        sonic_rs::from_str(data_str).expect("Data string should contain valid JSON");
    assert!(parsed_data.is_object(), "Parsed data should be an object");
    assert_eq!(
        parsed_data,
        json!({}),
        "Parsed data should be an empty object"
    );
}

#[test]
fn test_channel_info_minimal_format() {
    // Test channel_info with only occupied field (minimal case)
    let response = PusherMessage::channel_info(false, None, None, None);

    // Assert response is a valid JSON object
    assert!(response.is_object(), "Response should be an object");

    // Assert occupied field exists and is correct type
    assert!(
        response.get("occupied").is_some(),
        "Should have 'occupied' field"
    );
    assert!(
        response["occupied"].is_boolean(),
        "'occupied' should be a boolean"
    );
    assert_eq!(response["occupied"], false);

    // Assert no optional fields are present
    assert!(
        response.get("subscription_count").is_none(),
        "Should not have 'subscription_count' when None"
    );
    assert!(
        response.get("user_count").is_none(),
        "Should not have 'user_count' when None"
    );
    assert!(
        response.get("cache").is_none(),
        "Should not have 'cache' when None"
    );

    // Assert exactly 1 field
    let obj = response.as_object().expect("Response should be an object");
    assert_eq!(obj.len(), 1, "Should have exactly 1 field when minimal");
}

#[test]
fn test_channel_info_with_subscription_count() {
    // Test channel_info with subscription_count
    let response = PusherMessage::channel_info(true, Some(42), None, None);

    // Assert response is a valid JSON object
    assert!(response.is_object(), "Response should be an object");

    // Assert occupied field
    assert!(
        response.get("occupied").is_some(),
        "Should have 'occupied' field"
    );
    assert!(
        response["occupied"].is_boolean(),
        "'occupied' should be a boolean"
    );
    assert_eq!(response["occupied"], true);

    // Assert subscription_count field
    assert!(
        response.get("subscription_count").is_some(),
        "Should have 'subscription_count' field"
    );
    assert!(
        response["subscription_count"].is_u64(),
        "'subscription_count' should be a number"
    );
    assert_eq!(response["subscription_count"], 42);

    // Assert other optional fields are not present
    assert!(
        response.get("user_count").is_none(),
        "Should not have 'user_count' when None"
    );
    assert!(
        response.get("cache").is_none(),
        "Should not have 'cache' when None"
    );
}

#[test]
fn test_channel_info_with_user_count() {
    // Test channel_info with user_count (for presence channels)
    let response = PusherMessage::channel_info(true, Some(10), Some(8), None);

    // Assert response is a valid JSON object
    assert!(response.is_object(), "Response should be an object");

    // Assert all fields
    assert_eq!(response["occupied"], true);
    assert_eq!(response["subscription_count"], 10);
    assert_eq!(response["user_count"], 8);

    // Verify types
    assert!(
        response["occupied"].is_boolean(),
        "'occupied' should be a boolean"
    );
    assert!(
        response["subscription_count"].is_u64(),
        "'subscription_count' should be a number"
    );
    assert!(
        response["user_count"].is_u64(),
        "'user_count' should be a number"
    );

    // Assert cache is not present
    assert!(
        response.get("cache").is_none(),
        "Should not have 'cache' when None"
    );
}

#[test]
fn test_channel_info_with_cache_data() {
    use std::time::Duration;

    // Test channel_info with cache data
    let cache_content = r#"{"event":"test","data":"cached"}"#.to_string();
    let ttl = Duration::from_secs(3600); // 1 hour

    let response =
        PusherMessage::channel_info(true, Some(5), None, Some((cache_content.clone(), ttl)));

    // Assert response is a valid JSON object
    assert!(response.is_object(), "Response should be an object");

    // Assert basic fields
    assert_eq!(response["occupied"], true);
    assert_eq!(response["subscription_count"], 5);

    // Assert cache field exists and has correct structure
    assert!(response.get("cache").is_some(), "Should have 'cache' field");
    assert!(response["cache"].is_object(), "'cache' should be an object");

    // Assert cache subfields
    let cache_obj = response["cache"]
        .as_object()
        .expect("Cache should be an object");
    assert!(
        cache_obj.contains_key(&"data"),
        "Cache should have 'data' field"
    );
    assert!(
        cache_obj.contains_key(&"ttl"),
        "Cache should have 'ttl' field"
    );

    // Verify cache data field
    assert!(
        response["cache"]["data"].is_str(),
        "Cache data should be a string"
    );
    assert_eq!(response["cache"]["data"], cache_content);

    // Verify cache ttl field
    assert!(
        response["cache"]["ttl"].is_u64(),
        "Cache ttl should be a number"
    );
    assert_eq!(response["cache"]["ttl"], 3600);

    // Assert user_count is not present
    assert!(
        response.get("user_count").is_none(),
        "Should not have 'user_count' when None"
    );
}

#[test]
fn test_channel_info_full_format() {
    use std::time::Duration;

    // Test channel_info with all fields populated
    let cache_content = r#"{"message":"Hello from cache"}"#.to_string();
    let ttl = Duration::from_secs(7200); // 2 hours

    let response = PusherMessage::channel_info(
        true,
        Some(100),
        Some(75),
        Some((cache_content.clone(), ttl)),
    );

    // Assert response is a valid JSON object
    assert!(response.is_object(), "Response should be an object");

    // Assert all fields are present and have correct values
    assert_eq!(response["occupied"], true);
    assert_eq!(response["subscription_count"], 100);
    assert_eq!(response["user_count"], 75);

    // Assert cache structure
    assert!(response["cache"].is_object(), "'cache' should be an object");
    assert_eq!(response["cache"]["data"], cache_content);
    assert_eq!(response["cache"]["ttl"], 7200);

    // Verify exact field count
    let obj = response.as_object().expect("Response should be an object");
    assert_eq!(
        obj.len(),
        4,
        "Should have exactly 4 fields when all are populated"
    );

    // Verify field types
    assert!(
        response["occupied"].is_boolean(),
        "'occupied' should be a boolean"
    );
    assert!(
        response["subscription_count"].is_u64(),
        "'subscription_count' should be a number"
    );
    assert!(
        response["user_count"].is_u64(),
        "'user_count' should be a number"
    );
    assert!(
        response["cache"]["data"].is_str(),
        "Cache data should be a string"
    );
    assert!(
        response["cache"]["ttl"].is_u64(),
        "Cache ttl should be a number"
    );
}

#[test]
fn test_channel_info_zero_values() {
    // Test that zero values are properly included (not filtered out)
    let response = PusherMessage::channel_info(false, Some(0), Some(0), None);

    // Assert response is a valid JSON object
    assert!(response.is_object(), "Response should be an object");

    // Assert all fields with zero values are present
    assert_eq!(response["occupied"], false);
    assert_eq!(response["subscription_count"], 0);
    assert_eq!(response["user_count"], 0);

    // Verify that zero counts are included (not filtered)
    assert!(
        response.get("subscription_count").is_some(),
        "Should include subscription_count even when 0"
    );
    assert!(
        response.get("user_count").is_some(),
        "Should include user_count even when 0"
    );
}

#[test]
fn test_watchlist_events_format() {
    // Watchlist events are custom Sockudo extensions, not in Pusher spec
    // They can keep their current format
    let message =
        PusherMessage::watchlist_online_event(vec!["user1".to_string(), "user2".to_string()]);
    let json = message_to_json(&message);

    // Assert event field exists and has correct value
    assert!(json.get("event").is_some(), "Should have 'event' field");
    assert!(json["event"].is_str(), "Event field should be a string");
    assert_eq!(json["event"], "online");

    // Assert channel field is omitted for watchlist events (due to skip_serializing_if)
    assert!(
        json.get("channel").is_none(),
        "Channel field should be omitted for watchlist events"
    );

    // Watchlist events can remain as JSON objects since they're custom
    assert!(json.get("data").is_some(), "Should have 'data' field");
    assert!(json["data"].is_object(), "Data should be an object");

    // Assert data object has correct structure
    assert!(
        json["data"].get("user_ids").is_some(),
        "Should have 'user_ids' field in data"
    );
    assert!(
        json["data"]["user_ids"].is_array(),
        "user_ids should be an array"
    );
    assert_eq!(json["data"]["user_ids"], json!(["user1", "user2"]));
}

// ── MessageExtras tests ──────────────────────────────────────────────

#[test]
fn test_extras_round_trip_serialize_deserialize() {
    let mut headers = HashMap::new();
    headers.insert(
        "x-region".to_string(),
        ExtrasValue::String("us-east".to_string()),
    );
    headers.insert("priority".to_string(), ExtrasValue::Number(1.0));
    headers.insert("urgent".to_string(), ExtrasValue::Bool(true));

    let extras = MessageExtras {
        headers: Some(headers),
        ephemeral: Some(true),
        idempotency_key: Some("abc-123".to_string()),
        push: None,
        echo: Some(false),
        ai: None,
        opaque: Default::default(),
    };

    let json_str = sonic_rs::to_string(&extras).expect("serialize");
    let round_tripped: MessageExtras = sonic_rs::from_str(&json_str).expect("deserialize");

    assert_eq!(round_tripped.ephemeral, Some(true));
    assert_eq!(round_tripped.idempotency_key.as_deref(), Some("abc-123"));
    assert_eq!(round_tripped.echo, Some(false));
    let h = round_tripped.headers.unwrap();
    assert_eq!(
        h.get("x-region"),
        Some(&ExtrasValue::String("us-east".to_string()))
    );
    assert_eq!(h.get("priority"), Some(&ExtrasValue::Number(1.0)));
    assert_eq!(h.get("urgent"), Some(&ExtrasValue::Bool(true)));
}

#[test]
fn test_extras_default_is_all_none() {
    let extras = MessageExtras::default();
    assert!(extras.headers.is_none());
    assert!(extras.ephemeral.is_none());
    assert!(extras.idempotency_key.is_none());
    assert!(extras.push.is_none());
    assert!(extras.echo.is_none());
}

#[test]
fn test_is_ephemeral_false_when_extras_none() {
    let msg = PusherMessage::channel_event("test", "ch", json!({}));
    assert!(!msg.is_ephemeral());
}

#[test]
fn test_is_ephemeral_true_when_set() {
    let mut msg = PusherMessage::channel_event("test", "ch", json!({}));
    msg.extras = Some(MessageExtras {
        ephemeral: Some(true),
        ..Default::default()
    });
    assert!(msg.is_ephemeral());
}

#[test]
fn test_is_ephemeral_false_when_extras_present_but_not_set() {
    let mut msg = PusherMessage::channel_event("test", "ch", json!({}));
    msg.extras = Some(MessageExtras::default());
    assert!(!msg.is_ephemeral());
}

#[test]
fn test_should_echo_returns_connection_default_when_extras_none() {
    let msg = PusherMessage::channel_event("test", "ch", json!({}));
    assert!(msg.should_echo(true));
    assert!(!msg.should_echo(false));
}

#[test]
fn test_should_echo_true_overrides_connection_default() {
    let mut msg = PusherMessage::channel_event("test", "ch", json!({}));
    msg.extras = Some(MessageExtras {
        echo: Some(true),
        ..Default::default()
    });
    assert!(msg.should_echo(false));
    assert!(msg.should_echo(true));
}

#[test]
fn test_should_echo_false_overrides_connection_default() {
    let mut msg = PusherMessage::channel_event("test", "ch", json!({}));
    msg.extras = Some(MessageExtras {
        echo: Some(false),
        ..Default::default()
    });
    assert!(!msg.should_echo(true));
    assert!(!msg.should_echo(false));
}

#[test]
fn test_filter_headers_returns_none_when_extras_none() {
    let msg = PusherMessage::channel_event("test", "ch", json!({}));
    assert!(msg.filter_headers().is_none());
}

#[test]
fn test_filter_headers_returns_headers_when_set() {
    let mut headers = HashMap::new();
    headers.insert("env".to_string(), ExtrasValue::String("prod".to_string()));

    let mut msg = PusherMessage::channel_event("test", "ch", json!({}));
    msg.extras = Some(MessageExtras {
        headers: Some(headers),
        ..Default::default()
    });

    let h = msg.filter_headers().unwrap();
    assert_eq!(h.get("env"), Some(&ExtrasValue::String("prod".to_string())));
}

#[test]
fn test_v1_delivery_strips_extras() {
    let mut msg =
        PusherMessage::channel_event("test-event", "test-channel", json!({"hello": "world"}));
    msg.extras = Some(MessageExtras {
        ephemeral: Some(true),
        echo: Some(false),
        ..Default::default()
    });
    msg.serial = Some(42);
    msg.message_id = Some("msg-123".to_string());

    // Simulate V1 stripping (same as send_to_v1_sockets)
    msg.serial = None;
    msg.message_id = None;
    msg.extras = None;

    let json = message_to_json(&msg);
    assert!(
        json.get("extras").is_none(),
        "V1 delivery must not include extras"
    );
    assert!(
        json.get("serial").is_none(),
        "V1 delivery must not include serial"
    );
    assert!(
        json.get("messageId").is_none(),
        "V1 delivery must not include message_id"
    );
}

#[test]
fn test_v2_delivery_includes_extras() {
    let mut msg =
        PusherMessage::channel_event("test-event", "test-channel", json!({"hello": "world"}));
    msg.extras = Some(MessageExtras {
        ephemeral: Some(true),
        echo: Some(false),
        ..Default::default()
    });

    let json = message_to_json(&msg);
    assert!(
        json.get("extras").is_some(),
        "V2 delivery must include extras"
    );
    let extras = json.get("extras").unwrap();
    assert_eq!(extras["ephemeral"], true);
    assert_eq!(extras["echo"], false);
}

#[test]
fn test_v2_extras_push_is_serialized_inside_extras() {
    let mut msg =
        PusherMessage::channel_event("test-event", "test-channel", json!({"hello": "world"}));
    msg.extras = Some(MessageExtras {
        push: Some(json!({
            "title": "Hello",
            "body": "Push body",
            "templateData": {"id": "42"}
        })),
        ..Default::default()
    });

    let json = message_to_json(&msg);
    assert_eq!(json["extras"]["push"]["title"], "Hello");
    assert_eq!(json["extras"]["push"]["templateData"]["id"], "42");
}

#[test]
fn test_should_include_extras_by_protocol() {
    use sockudo_protocol::ProtocolVersion;
    assert!(!PusherMessage::should_include_extras(&ProtocolVersion::V1));
    assert!(PusherMessage::should_include_extras(&ProtocolVersion::V2));
}

#[test]
fn test_validate_headers_rejects_nested_objects() {
    let raw = json!({
        "extras": {
            "headers": {
                "valid_key": "string_value",
                "bad_key": {"nested": true}
            }
        }
    });
    let result = MessageExtras::validate_headers_from_json(&raw);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .contains("nested objects and arrays are not allowed")
    );
}

#[test]
fn test_validate_headers_rejects_arrays() {
    let raw = json!({
        "extras": {
            "headers": {
                "bad_key": [1, 2, 3]
            }
        }
    });
    let result = MessageExtras::validate_headers_from_json(&raw);
    assert!(result.is_err());
}

#[test]
fn test_validate_headers_accepts_flat_scalars() {
    let raw = json!({
        "extras": {
            "headers": {
                "str_key": "hello",
                "num_key": 42,
                "bool_key": true
            }
        }
    });
    let result = MessageExtras::validate_headers_from_json(&raw);
    assert!(result.is_ok());
}

#[test]
fn test_validate_headers_ok_when_no_extras() {
    let raw = json!({"name": "test-event"});
    let result = MessageExtras::validate_headers_from_json(&raw);
    assert!(result.is_ok());
}

#[test]
fn test_extras_skipped_in_serialization_when_none() {
    let msg = PusherMessage::channel_event("test", "ch", json!({}));
    let json = message_to_json(&msg);
    assert!(
        json.get("extras").is_none(),
        "extras should be omitted when None"
    );
}

#[test]
fn test_extras_idempotency_key() {
    let mut msg = PusherMessage::channel_event("test", "ch", json!({}));
    assert!(msg.extras_idempotency_key().is_none());

    msg.extras = Some(MessageExtras {
        idempotency_key: Some("dedup-key-1".to_string()),
        ..Default::default()
    });
    assert_eq!(msg.extras_idempotency_key(), Some("dedup-key-1"));
}
