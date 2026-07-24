use super::buffer::{RewindGate, RewindGateAdmission, RewindGateDrain, SizedMessage};
use super::*;
use bytes::Bytes;
use sockudo_filter::node::FilterNodeBuilder;
use sockudo_protocol::messages::PusherMessage;
use sockudo_ws::axum_integration::WebSocketWriter;
use std::sync::Arc;

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
fn test_byte_counter_reservation_is_atomic_at_limit() {
    let counter = Arc::new(ByteCounter::new());
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let mut threads = Vec::with_capacity(8);

    for _ in 0..8 {
        let counter = Arc::clone(&counter);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            counter.try_reserve(6, 10)
        }));
    }

    let admitted = threads
        .into_iter()
        .map(|thread| thread.join().expect("reservation thread panicked"))
        .filter(|admitted| *admitted)
        .count();

    assert_eq!(admitted, 1);
    assert_eq!(counter.get(), 6);
    counter.sub(6);
    assert_eq!(counter.get(), 0);
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
        message: Arc::new(PusherMessage {
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
        }),
    };
    assert_eq!(gate.try_buffer(message, 64), RewindGateAdmission::Buffered);
    let RewindGateDrain::Buffered(drained) = gate.finish() else {
        panic!("bounded gate should drain without overflow");
    };
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].serial, Some(1));
    assert_eq!(drained[0].message_id.as_deref(), Some("msg-1"));
}

#[test]
fn rewind_gate_fails_closed_when_count_or_byte_limit_overflows() {
    let message = || BufferedRewindMessage {
        serial: Some(1),
        message_id: Some("msg-1".to_string()),
        message: Arc::new(PusherMessage::pong()),
    };

    let mut count_gate = RewindGate::new(WebSocketBufferConfig::with_both_limits(1, 1_024, true));
    assert_eq!(
        count_gate.try_buffer(message(), 1),
        RewindGateAdmission::Buffered
    );
    assert!(matches!(
        count_gate.try_buffer(message(), 1),
        RewindGateAdmission::Overflowed(_)
    ));
    assert!(matches!(
        count_gate.finish(),
        RewindGateDrain::Overflowed(_)
    ));

    let mut byte_gate = RewindGate::new(WebSocketBufferConfig::with_both_limits(10, 4, true));
    assert!(matches!(
        byte_gate.try_buffer(message(), 5),
        RewindGateAdmission::Overflowed(_)
    ));
    assert!(matches!(byte_gate.finish(), RewindGateDrain::Overflowed(_)));
}

#[test]
fn rewind_gate_rejects_late_admission_after_drain_for_live_delivery() {
    let mut gate = RewindGate::new(WebSocketBufferConfig::with_both_limits(10, 1_024, true));
    assert!(matches!(gate.finish(), RewindGateDrain::Buffered(messages) if messages.is_empty()));
    let admission = gate.try_buffer(
        BufferedRewindMessage {
            serial: Some(1),
            message_id: Some("msg-1".to_string()),
            message: Arc::new(PusherMessage::pong()),
        },
        1,
    );

    assert_eq!(admission, RewindGateAdmission::Closed);
}

#[test]
fn rewind_gates_share_one_message_allocation_across_subscribers() {
    let shared = Arc::new(PusherMessage::pong());
    let buffered = || BufferedRewindMessage {
        serial: Some(1),
        message_id: Some("msg-1".to_string()),
        message: Arc::clone(&shared),
    };
    let mut first = RewindGate::new(WebSocketBufferConfig::with_both_limits(10, 1_024, true));
    let mut second = RewindGate::new(WebSocketBufferConfig::with_both_limits(10, 1_024, true));

    assert_eq!(
        first.try_buffer(buffered(), 1),
        RewindGateAdmission::Buffered
    );
    assert_eq!(
        second.try_buffer(buffered(), 1),
        RewindGateAdmission::Buffered
    );
    let RewindGateDrain::Buffered(first) = first.finish() else {
        panic!("first gate should drain");
    };
    let RewindGateDrain::Buffered(second) = second.finish() else {
        panic!("second gate should drain");
    };

    assert!(Arc::ptr_eq(&first[0].message, &second[0].message));
    assert!(Arc::ptr_eq(&first[0].message, &shared));
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

async fn create_websocket_ref() -> WebSocketRef {
    create_websocket_ref_with_buffer_config(WebSocketBufferConfig::default()).await
}

async fn create_websocket_ref_with_buffer_config(
    buffer_config: WebSocketBufferConfig,
) -> WebSocketRef {
    let socket_id = SocketId::new();
    let (writer, _client) = create_server_writer_with_client().await;
    WebSocketRef::new(WebSocket::with_buffer_config(
        socket_id,
        writer,
        buffer_config,
    ))
}

fn create_test_pong_message(channel: &str, serial: u64, message_id: &str) -> PusherMessage {
    let mut message = PusherMessage::pong();
    message.channel = Some(channel.to_string());
    message.serial = Some(serial);
    message.message_id = Some(message_id.to_string());
    message
}

async fn buffer_test_rewind_message(
    ws_ref: &WebSocketRef,
    channel: &str,
    message: PusherMessage,
) -> crate::error::Result<bool> {
    let message_size = sonic_rs::to_vec(&message)
        .expect("test message should serialize")
        .len();
    ws_ref
        .buffer_rewind_message(channel, Arc::new(message), message_size)
        .await
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

#[tokio::test]
async fn test_send_message_serializes_and_delivers() {
    use sockudo_ws::Message;

    let socket_id = SocketId::new();
    let (writer, mut client) = create_server_writer_with_client().await;
    let ws = WebSocket::new(socket_id, writer);
    let ws_ref = WebSocketRef::new(ws);

    let msg = PusherMessage::pong();
    let result = ws_ref.send_message(&msg);
    assert!(
        result.is_ok(),
        "send_message should succeed on active connection: {result:?}"
    );

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let frame = tokio::time::timeout(std::time::Duration::from_secs(1), client.next())
        .await
        .expect("timed out waiting for frame")
        .expect("client stream ended unexpectedly")
        .expect("frame read error");

    assert!(
        matches!(frame, Message::Text(_)),
        "Json wire format must deliver as Text frame, got: {frame:?}"
    );
    if let Message::Text(payload) = &frame {
        let text = std::str::from_utf8(payload).expect("payload is not UTF-8");
        assert!(
            text.contains("pong"),
            "serialized payload should contain 'pong', got: {text}"
        );
    }
}

#[tokio::test]
async fn subscribe_getters_return_expected_values() {
    let ws_ref = create_websocket_ref().await;
    let filter = FilterNodeBuilder::eq("region", "us-east");
    let event_name_filter = vec!["message-created".to_string(), "message-updated".to_string()];

    ws_ref
        .subscribe_to_channel_with_filters(
            "presence-room".to_string(),
            Some(filter.clone()),
            Some(event_name_filter.clone()),
            None,
            true,
        )
        .await;

    assert_eq!(
        ws_ref.get_channel_filter("presence-room").await.as_deref(),
        Some(&filter)
    );
    assert_eq!(
        ws_ref.get_channel_filter_sync("presence-room").as_deref(),
        Some(&filter)
    );
    assert_eq!(
        ws_ref.get_event_name_filter_sync("presence-room"),
        Some(event_name_filter)
    );
    assert!(ws_ref.allows_annotation_events_sync("presence-room"));
    assert!(!ws_ref.allows_annotation_events_sync("unknown-room"));
}

#[tokio::test]
async fn resubscribe_preserves_rewind_gate_and_clears_attach_serial() {
    let ws_ref = create_websocket_ref().await;

    ws_ref.start_rewind_gate("presence-room".to_string());
    assert!(
        buffer_test_rewind_message(
            &ws_ref,
            "presence-room",
            create_test_pong_message("presence-room", 1, "msg-1"),
        )
        .await
        .expect("gate admission should succeed")
    );
    ws_ref.set_attach_serial("presence-room".to_string(), 41);

    ws_ref
        .subscribe_to_channel_with_filters(
            "presence-room".to_string(),
            Some(FilterNodeBuilder::eq("region", "us-east")),
            Some(vec!["message-created".to_string()]),
            None,
            false,
        )
        .await;

    assert_eq!(ws_ref.attach_serial("presence-room"), None);
    assert!(
        buffer_test_rewind_message(
            &ws_ref,
            "presence-room",
            create_test_pong_message("presence-room", 2, "msg-2"),
        )
        .await
        .expect("gate admission should succeed")
    );

    let drained = ws_ref
        .finish_rewind_gate("presence-room")
        .await
        .expect("gate should drain");
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0].serial, Some(1));
    assert_eq!(drained[1].serial, Some(2));
}

#[tokio::test]
async fn unsubscribe_removes_all_per_channel_state() {
    let ws_ref = create_websocket_ref().await;

    ws_ref
        .subscribe_to_channel_with_filters(
            "presence-room".to_string(),
            Some(FilterNodeBuilder::eq("region", "us-east")),
            Some(vec!["message-created".to_string()]),
            None,
            true,
        )
        .await;
    ws_ref.set_attach_serial("presence-room".to_string(), 99);
    ws_ref.start_rewind_gate("presence-room".to_string());
    assert!(
        buffer_test_rewind_message(
            &ws_ref,
            "presence-room",
            create_test_pong_message("presence-room", 1, "msg-1"),
        )
        .await
        .expect("gate admission should succeed")
    );

    assert!(ws_ref.unsubscribe_from_channel("presence-room").await);
    assert_eq!(ws_ref.get_channel_filter_sync("presence-room"), None);
    assert_eq!(ws_ref.get_event_name_filter_sync("presence-room"), None);
    assert!(!ws_ref.allows_annotation_events_sync("presence-room"));
    assert_eq!(ws_ref.attach_serial("presence-room"), None);
    assert!(
        !buffer_test_rewind_message(
            &ws_ref,
            "presence-room",
            create_test_pong_message("presence-room", 2, "msg-2"),
        )
        .await
        .expect("inactive gate should not fail")
    );
    assert!(
        ws_ref
            .finish_rewind_gate("presence-room")
            .await
            .expect("inactive gate should drain")
            .is_empty()
    );
}

#[tokio::test]
async fn finish_rewind_gate_drains_buffered_messages() {
    let ws_ref = create_websocket_ref_with_buffer_config(WebSocketBufferConfig::with_both_limits(
        2, 1024, true,
    ))
    .await;

    ws_ref.start_rewind_gate("presence-room".to_string());
    assert!(
        buffer_test_rewind_message(
            &ws_ref,
            "presence-room",
            create_test_pong_message("presence-room", 1, "msg-1"),
        )
        .await
        .expect("gate admission should succeed")
    );
    assert!(
        buffer_test_rewind_message(
            &ws_ref,
            "presence-room",
            create_test_pong_message("presence-room", 2, "msg-2"),
        )
        .await
        .expect("gate admission should succeed")
    );

    let drained = ws_ref
        .finish_rewind_gate("presence-room")
        .await
        .expect("gate should drain");
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0].message_id.as_deref(), Some("msg-1"));
    assert_eq!(drained[1].message_id.as_deref(), Some("msg-2"));
    assert!(!ws_ref.cancellation_token().is_cancelled());
    assert!(
        ws_ref
            .finish_rewind_gate("presence-room")
            .await
            .expect("closed gate should drain")
            .is_empty()
    );
    assert!(
        !buffer_test_rewind_message(
            &ws_ref,
            "presence-room",
            create_test_pong_message("presence-room", 3, "msg-3"),
        )
        .await
        .expect("closed gate should use live delivery")
    );
}

#[tokio::test]
async fn rewind_gate_message_overflow_shuts_down_connection() {
    let ws_ref = create_websocket_ref_with_buffer_config(
        WebSocketBufferConfig::with_message_limit(2, false),
    )
    .await;
    ws_ref.start_rewind_gate("presence-room".to_string());

    for serial in 1..=2 {
        assert!(
            buffer_test_rewind_message(
                &ws_ref,
                "presence-room",
                create_test_pong_message("presence-room", serial, &format!("msg-{serial}")),
            )
            .await
            .expect("messages within the configured limit should be buffered")
        );
    }
    assert!(!ws_ref.cancellation_token().is_cancelled());

    let error = buffer_test_rewind_message(
        &ws_ref,
        "presence-room",
        create_test_pong_message("presence-room", 3, "msg-3"),
    )
    .await
    .expect_err("overflow must fail closed");
    assert!(error.to_string().contains("attach gate exceeded"));
    assert!(ws_ref.cancellation_token().is_cancelled());
    assert!(ws_ref.finish_rewind_gate("presence-room").await.is_err());
}

#[tokio::test]
async fn rewind_gate_byte_overflow_shuts_down_connection() {
    let first = create_test_pong_message("presence-room", 1, "msg-1");
    let first_size =
        sockudo_protocol::wire::serialize_message(&first, sockudo_protocol::WireFormat::Json)
            .unwrap()
            .len();
    let ws_ref = create_websocket_ref_with_buffer_config(WebSocketBufferConfig::with_byte_limit(
        first_size, false,
    ))
    .await;
    ws_ref.start_rewind_gate("presence-room".to_string());

    assert!(
        buffer_test_rewind_message(&ws_ref, "presence-room", first)
            .await
            .expect("first message should fit exactly")
    );
    assert!(!ws_ref.cancellation_token().is_cancelled());

    let error = buffer_test_rewind_message(
        &ws_ref,
        "presence-room",
        create_test_pong_message("presence-room", 2, "msg-2"),
    )
    .await
    .expect_err("byte overflow must fail closed");
    assert!(error.to_string().contains("attach gate exceeded"));
    assert!(ws_ref.cancellation_token().is_cancelled());
    assert!(ws_ref.finish_rewind_gate("presence-room").await.is_err());
}

#[tokio::test]
async fn test_send_broadcast_delivers_without_lock() {
    use sockudo_ws::Message;

    let socket_id = SocketId::new();
    let (writer, mut client) = create_server_writer_with_client().await;
    let ws = WebSocket::new(socket_id, writer);
    let ws_ref = WebSocketRef::new(ws);

    let payload = Bytes::from_static(b"{\"event\":\"broadcast-test\"}");
    let result = ws_ref.send_broadcast(payload);
    assert!(
        result.is_ok(),
        "send_broadcast should succeed without acquiring the inner lock: {result:?}"
    );

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let frame = tokio::time::timeout(std::time::Duration::from_secs(1), client.next())
        .await
        .expect("timed out waiting for broadcast frame")
        .expect("client stream ended unexpectedly")
        .expect("frame read error");

    assert!(
        matches!(frame, Message::Text(_)),
        "broadcast must arrive at client as a Text frame, got: {frame:?}"
    );
}

#[tokio::test]
async fn test_send_after_close_returns_error() {
    use crate::error::Error;

    let socket_id = SocketId::new();
    let (writer, _client) = create_server_writer_with_client().await;
    let ws = WebSocket::new(socket_id, writer);
    let ws_ref = WebSocketRef::new(ws);

    let close_result = ws_ref.close(1000, "normal closure".to_string()).await;
    assert!(
        close_result.is_ok(),
        "close() should succeed: {close_result:?}"
    );

    let msg = PusherMessage::pong();
    let send_result = ws_ref.send_message(&msg);

    assert!(
        send_result.is_err(),
        "send_message after close must return an error, not succeed"
    );
    assert!(
        matches!(send_result.unwrap_err(), Error::ConnectionClosed(_)),
        "error variant must be Error::ConnectionClosed"
    );
}

#[tokio::test]
async fn test_send_message_respects_wire_format() {
    use sockudo_protocol::WireFormat;
    use sockudo_ws::Message;

    {
        let socket_id = SocketId::new();
        let (writer, mut client) = create_server_writer_with_client().await;
        let ws = WebSocket::new(socket_id, writer);
        let ws_ref = WebSocketRef::new(ws);

        ws_ref.send_message(&PusherMessage::pong()).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let frame = tokio::time::timeout(std::time::Duration::from_secs(1), client.next())
            .await
            .expect("timed out (json)")
            .expect("stream ended (json)")
            .expect("read error (json)");
        assert!(
            matches!(frame, Message::Text(_)),
            "WireFormat::Json must produce a Text frame, got: {frame:?}"
        );
    }

    {
        let socket_id = SocketId::new();
        let (writer, mut client) = create_server_writer_with_client().await;
        let mut ws = WebSocket::new(socket_id, writer);
        ws.state.wire_format = WireFormat::MessagePack;
        let ws_ref = WebSocketRef::new(ws);

        ws_ref.send_message(&PusherMessage::pong()).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let frame = tokio::time::timeout(std::time::Duration::from_secs(1), client.next())
            .await
            .expect("timed out (msgpack)")
            .expect("stream ended (msgpack)")
            .expect("read error (msgpack)");
        assert!(
            matches!(frame, Message::Binary(_)),
            "WireFormat::MessagePack must produce a Binary frame, got: {frame:?}"
        );
    }
}

#[tokio::test]
async fn test_concurrent_sends_preserve_ordering() {
    let socket_id = SocketId::new();
    let (writer, mut client) = create_server_writer_with_client().await;
    let buffer_config = WebSocketBufferConfig::new(2000, true);
    let ws = WebSocket::with_buffer_config(socket_id, writer, buffer_config);
    let ws_ref = WebSocketRef::new(ws);

    let mut handles = Vec::new();
    for _ in 0..10 {
        let clone = ws_ref.clone();
        handles.push(tokio::spawn(async move {
            let mut ok_count = 0usize;
            for _ in 0..100 {
                if clone.send_message(&PusherMessage::pong()).is_ok() {
                    ok_count += 1;
                }
            }
            ok_count
        }));
    }

    let mut total_sent = 0usize;
    for h in handles {
        total_sent += h.await.expect("sender task panicked");
    }
    assert_eq!(total_sent, 1000, "all 1000 sends must succeed (no drops)");

    let mut received = 0usize;
    while received < total_sent {
        match tokio::time::timeout(std::time::Duration::from_secs(5), client.next()).await {
            Ok(Some(Ok(_))) => received += 1,
            Ok(Some(Err(e))) => panic!("client read error after {received} messages: {e}"),
            Ok(None) => break,
            Err(_) => panic!("timed out after {received}/{total_sent} messages"),
        }
    }
    assert_eq!(
        received, total_sent,
        "every sent message must arrive at client (no drops)"
    );
}

#[tokio::test]
async fn test_shutdown_token_cancels_receiver() {
    let socket_id = SocketId::new();
    let (writer, _client) = create_server_writer_with_client().await;
    let ws = WebSocket::new(socket_id, writer);
    let ws_ref = WebSocketRef::new(ws);

    let token = ws_ref.cancellation_token();
    assert!(
        !token.is_cancelled(),
        "token must not be cancelled before shutdown()"
    );

    ws_ref.shutdown();

    assert!(
        token.is_cancelled(),
        "shutdown() must cancel the CancellationToken so the receiver task exits"
    );
}

#[tokio::test]
async fn close_frame_delivered_before_cancellation() {
    use sockudo_ws::Message;

    let socket_id = SocketId::new();
    let (writer, mut client) = create_server_writer_with_client().await;
    let ws = WebSocket::new(socket_id, writer);
    let ws_ref = WebSocketRef::new(ws);
    let token = ws_ref.cancellation_token();

    assert!(
        !token.is_cancelled(),
        "precondition: shutdown token must not be pre-cancelled"
    );

    let close_result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        ws_ref.close(4009, "user connection terminated".to_string()),
    )
    .await
    .expect("close() must complete within 2 s");

    assert!(
        close_result.is_ok(),
        "close() returned Err — the close frame was not enqueued before cancellation \
         (channel was already disconnected, meaning token fired before enqueue): {close_result:?}"
    );

    assert!(
        token.is_cancelled(),
        "shutdown token must be cancelled by the time close() returns"
    );

    let close_frame_received = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match client.next().await {
                Some(Ok(Message::Close(Some(frame)))) => {
                    assert_eq!(frame.code, 4009);
                    assert_eq!(frame.reason.as_str(), "user connection terminated");
                    return true;
                }
                Some(Ok(Message::Close(None))) => {
                    panic!("close frame must include the requested code and reason")
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => panic!("client read error: {e}"),
                None => return false,
            }
        }
    })
    .await
    .expect("timed out waiting for close frame at client");

    assert!(
        close_frame_received,
        "client stream ended without delivering a close frame"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn close_wait_returns_error_when_writer_is_cancelled_before_flush() {
    let socket_id = SocketId::new();
    let (writer, _client) = create_server_writer_with_client().await;
    let ws = WebSocket::new(socket_id, writer);
    let ws_ref = WebSocketRef::new(ws);

    ws_ref.shutdown();

    let close_result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        ws_ref
            .inner
            .lock()
            .await
            .message_sender
            .send_close(4009, "user connection terminated")
            .await
    })
    .await
    .expect("send_close() must not wait forever after writer cancellation");

    assert!(
        matches!(close_result, Err(crate::error::Error::ConnectionClosed(_))),
        "cancelled writer must report that the close frame was not flushed: {close_result:?}"
    );
}

#[tokio::test]
async fn shutdown_cancels_task_while_handle_retained() {
    use sockudo_ws::Message;

    let socket_id = SocketId::new();
    let (writer, client) = create_server_writer_with_client().await;
    let ws = WebSocket::new(socket_id, writer);
    let ws_ref = WebSocketRef::new(ws);

    let _retained_sender = ws_ref.message_sender.clone();

    ws_ref.shutdown();

    assert!(
        ws_ref.cancellation_token().is_cancelled(),
        "shutdown() must cancel the token synchronously"
    );

    let (task_done_tx, task_done_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let mut client = client;
        loop {
            match client.next().await {
                Some(Ok(Message::Close(_))) => {
                    let _ = task_done_tx.send(());
                    break;
                }
                Some(Ok(_)) => continue,
                _ => break,
            }
        }
    });

    tokio::time::timeout(std::time::Duration::from_secs(2), task_done_rx)
        .await
        .expect(
            "writer task did not terminate within 2 s — \
             cancellation must drive task exit even with retained sender handle",
        )
        .expect("completion oneshot closed without firing");

    drop(_retained_sender);
}
