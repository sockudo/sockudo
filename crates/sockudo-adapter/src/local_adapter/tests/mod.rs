use super::LocalAdapter;
use crate::ConnectionManager;
use sockudo_protocol::messages::{ExtrasValue, MessageData, MessageExtras, PusherMessage};
use std::collections::HashMap;

#[test]
fn v1_compatible_message_strips_v2_only_fields_for_plain_messages() {
    let message = PusherMessage {
        event: Some("chat.message".to_string()),
        channel: Some("room".to_string()),
        data: Some(MessageData::String("hello".to_string())),
        name: Some("chat.message".to_string()),
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: Some("mid-1".to_string()),
        stream_id: Some("stream-1".to_string()),
        serial: Some(9),
        idempotency_key: Some("idem-1".to_string()),
        extras: Some(MessageExtras {
            headers: Some(HashMap::from([(
                "note".to_string(),
                ExtrasValue::String("ok".to_string()),
            )])),
            ephemeral: Some(true),
            idempotency_key: Some("extra-idem".to_string()),
            push: None,
            echo: Some(false),
            ai: None,
        }),
        delta_sequence: None,
        delta_conflation_key: None,
    };

    let v1 = LocalAdapter::v1_compatible_message(&message).unwrap();
    assert_eq!(v1.event.as_deref(), Some("chat.message"));
    assert!(v1.serial.is_none());
    assert!(v1.message_id.is_none());
    assert!(v1.stream_id.is_none());
    assert!(v1.idempotency_key.is_none());
    assert!(v1.extras.is_none());
}

#[test]
fn v1_compatible_message_drops_versioned_mutation_events() {
    let mut message = PusherMessage::channel_event(
        "sockudo:message.update",
        "room",
        sonic_rs::json!({"text": "patched"}),
    );
    message.extras = Some(MessageExtras {
        headers: Some(HashMap::from([
            (
                "sockudo_action".to_string(),
                ExtrasValue::String("message.update".to_string()),
            ),
            (
                "sockudo_message_serial".to_string(),
                ExtrasValue::String("msg:1".to_string()),
            ),
        ])),
        ..Default::default()
    });
    message.serial = Some(11);
    message.stream_id = Some("stream-1".to_string());

    assert!(LocalAdapter::v1_compatible_message(&message).is_none());
}

#[test]
fn v2_runtime_message_id_skips_protocol_heartbeats() {
    assert!(!LocalAdapter::should_assign_v2_message_id(
        &PusherMessage::ping()
    ));
    assert!(!LocalAdapter::should_assign_v2_message_id(
        &PusherMessage::pong()
    ));
}

#[test]
fn v2_runtime_message_id_still_assigns_regular_messages() {
    let message =
        PusherMessage::channel_event("chat.message", "room", sonic_rs::json!({"text": "hello"}));

    assert!(LocalAdapter::should_assign_v2_message_id(&message));
}

#[tokio::test]
async fn read_only_queries_do_not_create_empty_namespaces() {
    let adapter = LocalAdapter::new();

    assert_eq!(
        ConnectionManager::get_channel_socket_count(&adapter, "missing-app", "room").await,
        0
    );
    assert!(
        ConnectionManager::get_channel_sockets(&adapter, "missing-app", "room")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(adapter.namespaces.len(), 0);
}
