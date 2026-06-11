use super::buffer::{RewindGate, SizedMessage};
use super::*;
use bytes::Bytes;
use sockudo_protocol::messages::PusherMessage;
use sockudo_ws::axum_integration::WebSocketWriter;

#[test]
fn test_socket_id_generation() {
    let id1 = SocketId::new();
    let id2 = SocketId::new();

    assert_ne!(id1, id2);
    let id1_str = id1.to_string();
    let id2_str = id2.to_string();
    assert!(id1_str.contains('.'));
    assert!(id2_str.contains('.'));
}

#[test]
fn test_connection_state() {
    let mut state = ConnectionState::new();

    assert!(!state.is_subscribed("test-channel"));
    state.add_subscription("test-channel".to_string());
    assert!(state.is_subscribed("test-channel"));
    assert!(state.remove_subscription("test-channel"));
    assert!(!state.is_subscribed("test-channel"));
}

#[test]
fn test_connection_capabilities_allow_matching_channels() {
    let capabilities = ConnectionCapabilities {
        subscribe: Some(vec!["chat:*".to_string()]),
        publish: Some(vec!["private-chat:*".to_string()]),
        presence: Some(vec!["presence-chat:*".to_string()]),
        ..Default::default()
    };

    assert!(capabilities.allows_subscribe("chat:room-1"));
    assert!(capabilities.allows_subscribe("presence-chat:room-1"));
    assert!(capabilities.allows_publish("private-chat:room-1"));
    assert!(!capabilities.allows_publish("private-news:room-1"));
    assert!(!capabilities.allows_annotation_publish("chat:room-1"));
}

#[test]
fn test_connection_capabilities_default_to_unrestricted_when_missing() {
    let capabilities = ConnectionCapabilities::default();

    assert!(capabilities.allows_subscribe("chat:room-1"));
    assert!(capabilities.allows_publish("private-chat:room-1"));
    assert!(!capabilities.allows_message_mutation_any(
        crate::versioned_message_auth::MutationKind::Update,
        "chat:room-1"
    ));
    assert!(!capabilities.allows_message_mutation_own(
        crate::versioned_message_auth::MutationKind::Update,
        "chat:room-1"
    ));
}

#[test]
fn test_connection_capabilities_allow_matching_mutation_channels() {
    let capabilities = ConnectionCapabilities {
        subscribe: None,
        publish: None,
        presence: None,
        message_update_own: Some(vec!["chat:*".to_string()]),
        message_update_any: Some(vec!["admin:*".to_string()]),
        message_delete_own: Some(vec!["chat:*".to_string()]),
        message_delete_any: None,
        message_append_own: None,
        message_append_any: Some(vec!["stream:*".to_string()]),
        ..Default::default()
    };

    assert!(capabilities.allows_message_mutation_own(
        crate::versioned_message_auth::MutationKind::Update,
        "chat:room-1"
    ));
    assert!(capabilities.allows_message_mutation_any(
        crate::versioned_message_auth::MutationKind::Update,
        "admin:room-1"
    ));
    assert!(capabilities.allows_message_mutation_own(
        crate::versioned_message_auth::MutationKind::Delete,
        "chat:room-1"
    ));
    assert!(capabilities.allows_message_mutation_any(
        crate::versioned_message_auth::MutationKind::Append,
        "stream:room-1"
    ));
    assert!(!capabilities.allows_message_mutation_any(
        crate::versioned_message_auth::MutationKind::Delete,
        "chat:room-1"
    ));
}

#[test]
fn test_connection_capabilities_parse_hyphenated_annotation_grants() {
    let capabilities: ConnectionCapabilities = sonic_rs::from_str(
        r#"{
                "annotation-publish":["chat:*"],
                "annotation-delete-own":["chat:*"],
                "annotation-delete-any":["admin:*"],
                "annotation-subscribe":["chat:*"]
            }"#,
    )
    .unwrap();

    assert!(capabilities.allows_annotation_publish("chat:room-1"));
    assert!(capabilities.allows_annotation_delete_own("chat:room-1"));
    assert!(capabilities.allows_annotation_delete_any("admin:room-1"));
    assert!(capabilities.allows_annotation_subscribe("chat:room-1"));
    assert!(!capabilities.allows_annotation_publish("news:room-1"));
}

#[test]
fn test_socket_id_display() {
    let id = SocketId::from_string("123.456").unwrap();
    assert_eq!(format!("{id}"), "123.456");
}

#[test]
fn test_buffer_limit_messages_only() {
    let limit = BufferLimit::Messages(1000);
    assert_eq!(limit.channel_capacity(), 1000);
    assert!(!limit.tracks_bytes());
    assert_eq!(limit.message_limit(), Some(1000));
    assert_eq!(limit.byte_limit(), None);
}

#[test]
fn test_buffer_limit_bytes_only() {
    let limit = BufferLimit::Bytes(1_048_576);
    assert_eq!(limit.channel_capacity(), 10_000);
    assert!(limit.tracks_bytes());
    assert_eq!(limit.message_limit(), None);
    assert_eq!(limit.byte_limit(), Some(1_048_576));
}

#[test]
fn test_buffer_limit_both() {
    let limit = BufferLimit::Both {
        messages: 1000,
        bytes: 1_048_576,
    };
    assert_eq!(limit.channel_capacity(), 1000);
    assert!(limit.tracks_bytes());
    assert_eq!(limit.message_limit(), Some(1000));
    assert_eq!(limit.byte_limit(), Some(1_048_576));
}

#[test]
fn test_websocket_buffer_config_default() {
    let config = WebSocketBufferConfig::default();
    assert_eq!(config.limit, BufferLimit::Messages(1000));
    assert!(config.disconnect_on_full);
    assert!(!config.tracks_bytes());
}

#[test]
fn test_byte_counter_basic() {
    let counter = ByteCounter::new();
    assert_eq!(counter.get(), 0);

    assert_eq!(counter.add(100), 100);
    assert_eq!(counter.get(), 100);

    assert_eq!(counter.add(50), 150);
    assert_eq!(counter.get(), 150);

    counter.sub(30);
    assert_eq!(counter.get(), 120);
}

#[test]
fn test_byte_counter_would_exceed() {
    let counter = ByteCounter::new();
    counter.add(900);

    assert!(!counter.would_exceed(100, 1000));
    assert!(counter.would_exceed(101, 1000));
    assert!(counter.would_exceed(200, 1000));
}

#[test]
fn test_sized_message() {
    let bytes = Bytes::from("hello world");
    let msg = SizedMessage::new(bytes.clone());
    assert_eq!(msg.size, 11);
    assert_eq!(msg.bytes, bytes);
}

#[test]
fn test_rewind_gate_buffers_and_drains_messages() {
    let mut gate = RewindGate::default();
    let message = BufferedRewindMessage {
        serial: Some(1),
        message_id: Some("msg-1".to_string()),
        message: PusherMessage {
            event: Some("evt".to_string()),
            channel: Some("chat".to_string()),
            data: None,
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: Some("msg-1".to_string()),
            stream_id: Some("stream-1".to_string()),
            serial: Some(1),
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        },
    };
    gate.buffered.push(message.clone());
    let drained = std::mem::take(&mut gate.buffered);
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].serial, Some(1));
    assert_eq!(drained[0].message_id.as_deref(), Some("msg-1"));
    assert!(gate.buffered.is_empty());
}

#[test]
fn test_websocket_buffer_config_message_limit() {
    let config = WebSocketBufferConfig::with_message_limit(500, false);
    assert_eq!(config.channel_capacity(), 500);
    assert!(!config.disconnect_on_full);
    assert!(!config.tracks_bytes());
}

#[test]
fn test_websocket_buffer_config_byte_limit() {
    let config = WebSocketBufferConfig::with_byte_limit(1_048_576, true);
    assert_eq!(config.channel_capacity(), 10_000);
    assert!(config.disconnect_on_full);
    assert!(config.tracks_bytes());
}

#[test]
fn test_websocket_buffer_config_both_limits() {
    let config = WebSocketBufferConfig::with_both_limits(1000, 1_048_576, true);
    assert_eq!(config.channel_capacity(), 1000);
    assert!(config.disconnect_on_full);
    assert!(config.tracks_bytes());
}

#[test]
fn test_websocket_buffer_config_legacy_new() {
    let config = WebSocketBufferConfig::new(500, false);
    assert_eq!(config.channel_capacity(), 500);
    assert!(!config.disconnect_on_full);
}

use futures_util::StreamExt;

type ClientWs = sockudo_ws::WebSocketStream<sockudo_ws::Stream<sockudo_ws::Http1>>;

async fn create_server_writer_with_client() -> (WebSocketWriter, ClientWs) {
    use sockudo_ws::Config as WsConfig;
    use sockudo_ws::Http1;
    use sockudo_ws::axum_integration::WebSocket;
    use sockudo_ws::client::WebSocketClient;
    use tokio::net::{TcpListener, TcpStream};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_task: tokio::task::JoinHandle<WebSocketWriter> = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = sockudo_ws::handshake::server_handshake(&mut stream)
            .await
            .unwrap();
        let ws = WebSocket::from_tcp(stream, WsConfig::default());
        let (_reader, writer) = ws.split();
        writer
    });

    let client_stream = TcpStream::connect(local_addr).await.unwrap();
    let client = WebSocketClient::<Http1>::new(WsConfig::default());
    let (client_ws, _): (ClientWs, _) = client
        .connect(client_stream, &local_addr.to_string(), "/", None)
        .await
        .unwrap();

    let writer = server_task.await.unwrap();
    (writer, client_ws)
}

#[tokio::test]
async fn close_with_error_code_sends_error_then_close_frame() {
    use sockudo_ws::Message;

    let socket_id = SocketId::new();
    let (writer, mut client) = create_server_writer_with_client().await;
    let mut ws = WebSocket::new(socket_id, writer);

    assert!(ws.is_connected());
    let result = ws.close(4200, "Server shutting down".to_string()).await;
    assert!(result.is_ok(), "close() should succeed: {result:?}");
    assert_eq!(ws.state.status, ConnectionStatus::Closed);

    // Give the background writer task time to flush both frames.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // First frame received by the client must be the text error message.
    let first = tokio::time::timeout(std::time::Duration::from_secs(1), client.next())
        .await
        .expect("timed out waiting for first frame")
        .expect("client stream ended unexpectedly")
        .expect("frame read error");

    assert!(
        matches!(first, Message::Text(_)),
        "expected text error frame first, got: {first:?}"
    );
    if let Message::Text(payload) = &first {
        let text = std::str::from_utf8(payload).expect("error frame is not UTF-8");
        assert!(
            text.contains("4200"),
            "error frame should contain code 4200, got: {text}"
        );
    }

    // Second frame must be the close frame.
    let second = tokio::time::timeout(std::time::Duration::from_secs(1), client.next())
        .await
        .expect("timed out waiting for close frame")
        .expect("client stream ended unexpectedly")
        .expect("frame read error");

    assert!(
        matches!(second, Message::Close(_)),
        "expected close frame second, got: {second:?}"
    );
}
