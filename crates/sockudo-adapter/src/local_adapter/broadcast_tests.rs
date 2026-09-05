use super::*;
use futures_util::StreamExt;
use sockudo_core::websocket::{PerChannelState, WebSocket, WebSocketBufferConfig};
use sockudo_delta::{DeltaAlgorithm, DeltaCompressionConfig, DeltaCompressionManager};
use sockudo_protocol::wire::{WireFormat, deserialize_message};
use sockudo_ws::axum_integration::WebSocketWriter;
use std::time::Duration;
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
        let (mut reader, writer) = ws.split();
        tokio::spawn(async move {
            while let Some(result) = reader.next().await {
                if result.is_err() {
                    break;
                }
            }
        });
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

async fn socket(
    format: WireFormat,
    version: sockudo_protocol::ProtocolVersion,
    limit: usize,
) -> (WebSocketRef, ClientWs) {
    let (writer, client) = create_server_writer_with_client().await;
    let mut ws = WebSocket::with_buffer_config(
        SocketId::new(),
        writer,
        WebSocketBufferConfig::with_both_limits(1024, limit, false),
    );
    ws.state.wire_format = format;
    ws.state.protocol_version = version;
    (WebSocketRef::new(ws), client)
}
async fn read(client: &mut ClientWs, format: WireFormat) -> PusherMessage {
    use futures_util::SinkExt;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let frame = tokio::time::timeout_at(deadline, client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let payload = match frame {
            sockudo_ws::Message::Text(bytes) if format == WireFormat::Json => bytes,
            sockudo_ws::Message::Binary(bytes) if format.is_binary() => bytes,
            sockudo_ws::Message::Ping(bytes) => {
                client.send(sockudo_ws::Message::Pong(bytes)).await.unwrap();
                continue;
            }
            sockudo_ws::Message::Pong(_) => continue,
            other => panic!("incorrect frame type: {other:?}"),
        };
        return deserialize_message(&payload, format).unwrap();
    }
}

#[tokio::test]
async fn fanout_shares_bases_and_preserves_codecs_sequences_algorithms_and_failed_admission() {
    let adapter = LocalAdapter::new();
    let manager = Arc::new(DeltaCompressionManager::new(DeltaCompressionConfig {
        min_message_size: 1,
        ..Default::default()
    }));
    let mut recipients = Vec::new();
    for format in [
        WireFormat::Json,
        WireFormat::MessagePack,
        WireFormat::Protobuf,
    ] {
        for algorithm in [DeltaAlgorithm::Fossil, DeltaAlgorithm::Xdelta3] {
            let (socket, client) =
                socket(format, sockudo_protocol::ProtocolVersion::V2, 1024 * 1024).await;
            manager.enable_for_socket(socket.get_socket_id_sync());
            manager.set_channel_delta_settings(
                socket.get_socket_id_sync(),
                "orders",
                Some(true),
                Some(algorithm),
            );
            recipients.push((socket, client, format, algorithm));
        }
    }
    let (rejected, _client) =
        socket(WireFormat::Json, sockudo_protocol::ProtocolVersion::V2, 1).await;
    manager.enable_for_socket(rejected.get_socket_id_sync());
    for serial in 0..4 {
        let message = PusherMessage::channel_event(
            "update",
            "orders",
            sonic_rs::json!({"padding":"x".repeat(4096),"serial":serial}),
        );
        let payload = sonic_rs::to_vec(&message).unwrap();
        let mut sockets: Vec<_> = recipients
            .iter()
            .map(|(socket, ..)| socket.clone())
            .collect();
        sockets.push(rejected.clone());
        let results = adapter
            .send_messages_with_compression(
                sockets,
                message,
                payload.clone(),
                "orders",
                "update",
                crate::connection_manager::CompressionParams {
                    delta_compression: manager.clone(),
                    channel_settings: None,
                    envelope: None,
                },
            )
            .await;
        assert!(results.into_iter().all(|result| result.is_ok()));
        let mut shared = None;
        for (socket, client, format, algorithm) in &mut recipients {
            let message = read(client, *format).await;
            if serial < 2 {
                assert_eq!(message.delta_sequence, Some(serial));
            } else {
                assert_eq!(message.event.as_deref(), Some("sockudo:delta"));
                let data: sonic_rs::Value = match message.data.unwrap() {
                    sockudo_protocol::messages::MessageData::Json(value) => value,
                    sockudo_protocol::messages::MessageData::String(value) => {
                        sonic_rs::from_str(&value).unwrap()
                    }
                    other => panic!("unexpected delta data {other:?}"),
                };
                use sonic_rs::JsonValueTrait;
                assert_eq!(data["seq"].as_u64(), Some(serial));
                assert_eq!(data["base_index"].as_u64(), Some(serial - 1));
                assert_eq!(
                    data["algorithm"].as_str(),
                    Some(if *algorithm == DeltaAlgorithm::Fossil {
                        "fossil"
                    } else {
                        "xdelta3"
                    })
                );
            }
            let (base, stored_serial) = manager
                .get_last_message_with_sequence(socket.get_socket_id_sync(), "orders", "update")
                .await
                .unwrap();
            assert_eq!(base.as_ref(), &payload);
            assert_eq!(stored_serial, serial as u32);
            if let Some(prior) = shared.as_ref() {
                assert!(Arc::ptr_eq(prior, &base));
            } else {
                shared = Some(base);
            }
        }
        assert!(
            manager
                .get_last_message_with_sequence(rejected.get_socket_id_sync(), "orders", "update")
                .await
                .is_none()
        );
        assert_eq!(
            manager.get_next_sequence(rejected.get_socket_id_sync(), "orders", "update"),
            0
        );
    }
}

#[tokio::test]
async fn predicate_routing_preserves_exact_wildcard_or_and_annotation_permissions() {
    use sockudo_filter::{MessagePredicate, SubscriptionView};
    let (socket, _client) = socket(
        WireFormat::Json,
        sockudo_protocol::ProtocolVersion::V2,
        1024,
    )
    .await;
    let state = |event: &str, annotation_subscribe| PerChannelState {
        predicate: Some(Arc::new(
            MessagePredicate::compile(SubscriptionView {
                events: vec![event.into()],
                ..Default::default()
            })
            .unwrap(),
        )),
        annotation_subscribe,
        ..Default::default()
    };
    socket
        .channel_state
        .insert("orders.eu".into(), state("denied", true));
    socket
        .channel_state
        .insert("orders.*".into(), state("updated", false));
    let message = PusherMessage::channel_event("updated", "orders.eu", sonic_rs::json!({"x": 1}));
    let mut sockets = vec![socket.clone()];
    crate::v2_broadcast::apply_subscription_predicates_in_place(
        true,
        "orders.eu",
        &message,
        &mut sockets,
    );
    assert_eq!(sockets.len(), 1);
    socket.channel_state.insert(
        "orders.*".into(),
        state(sockudo_protocol::messages::ANNOTATION_EVENT_NAME, false),
    );
    let annotation = PusherMessage::channel_event(
        sockudo_protocol::messages::ANNOTATION_EVENT_NAME,
        "orders.eu",
        sonic_rs::json!({}),
    );
    crate::v2_broadcast::apply_subscription_predicates_in_place(
        true,
        "orders.eu",
        &annotation,
        &mut sockets,
    );
    assert!(sockets.is_empty());
}

#[tokio::test]
async fn divergent_delta_bases_preserve_frames_across_preparation_count_and_byte_limits() {
    use sonic_rs::JsonValueTrait;
    for (count, padding) in [(320, 1024), (32, 1024 * 1024)] {
        let adapter = LocalAdapter::new();
        let manager = Arc::new(DeltaCompressionManager::new(DeltaCompressionConfig {
            min_message_size: 1,
            ..Default::default()
        }));
        let message = PusherMessage::channel_event(
            "update",
            "churn",
            sonic_rs::json!({"padding":"x".repeat(padding),"serial":999}),
        );
        let next = sonic_rs::to_vec(&message).unwrap();
        let mut recipients = Vec::new();
        for index in 0..count {
            let format = [
                WireFormat::Json,
                WireFormat::MessagePack,
                WireFormat::Protobuf,
            ][index % 3];
            let (socket, client) = socket(
                format,
                sockudo_protocol::ProtocolVersion::V2,
                4 * 1024 * 1024,
            )
            .await;
            manager.enable_for_socket(socket.get_socket_id_sync());
            let base_message = PusherMessage::channel_event(
                "update",
                "churn",
                sonic_rs::json!({"padding":"x".repeat(padding),"serial":index}),
            );
            let base = Arc::new(sonic_rs::to_vec(&base_message).unwrap());
            // Two acknowledged full frames establish a distinct predecessor and seq=2.
            for _ in 0..2 {
                manager
                    .store_shared_sent_message(
                        socket.get_socket_id_sync(),
                        "churn",
                        "update",
                        base.clone(),
                        true,
                        None,
                    )
                    .await
                    .unwrap();
            }
            let expected = manager.compute_delta_for_broadcast(&base, &next).unwrap();
            recipients.push((socket, client, format, expected));
        }
        let outcomes = adapter
            .send_messages_with_compression(
                recipients.iter().map(|entry| entry.0.clone()).collect(),
                message,
                next.clone(),
                "churn",
                "update",
                crate::connection_manager::CompressionParams {
                    delta_compression: manager.clone(),
                    channel_settings: None,
                    envelope: None,
                },
            )
            .await;
        assert!(outcomes.into_iter().all(|outcome| outcome.is_ok()));
        for (socket, client, format, expected) in &mut recipients {
            let delivered = read(client, *format).await;
            assert_eq!(delivered.event.as_deref(), Some("sockudo:delta"));
            let data = match delivered.data.unwrap() {
                sockudo_protocol::messages::MessageData::Json(value) => value,
                sockudo_protocol::messages::MessageData::String(value) => {
                    sonic_rs::from_str(&value).unwrap()
                }
                other => panic!("invalid delta {other:?}"),
            };
            assert_eq!(data["seq"].as_u64(), Some(2));
            assert_eq!(data["base_index"].as_u64(), Some(1));
            let actual = base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                data["delta"].as_str().unwrap(),
            )
            .unwrap();
            assert_eq!(&actual, expected);
            let (stored, serial) = manager
                .get_last_message_with_sequence(socket.get_socket_id_sync(), "churn", "update")
                .await
                .unwrap();
            assert_eq!(stored.as_ref(), &next);
            assert_eq!(serial, 2);
        }
    }
}

#[tokio::test]
#[ignore = "repeated release fanout measurement using actual localhost WebSocket frames"]
async fn benchmark_shared_fanout_preparation() {
    use sonic_rs::JsonValueTrait;
    for count in [8, 128, 512] {
        for payload_size in [1024, 16384, 65536] {
            for mixed in [false, true] {
                let adapter = LocalAdapter::new();
                let manager = Arc::new(DeltaCompressionManager::new(DeltaCompressionConfig {
                    min_message_size: 1,
                    conflation_key_path: Some("asset".into()),
                    ..Default::default()
                }));
                let mut recipients = Vec::new();
                for index in 0..count {
                    let enabled = !mixed || index % 4 != 0;
                    let (socket, client) = socket(
                        WireFormat::Json,
                        if enabled {
                            sockudo_protocol::ProtocolVersion::V2
                        } else {
                            sockudo_protocol::ProtocolVersion::V1
                        },
                        4 * 1024 * 1024,
                    )
                    .await;
                    if enabled {
                        manager.enable_for_socket(socket.get_socket_id_sync());
                    }
                    recipients.push((socket, client, enabled, Vec::<u8>::new(), 0u32));
                }
                for serial in 0..12u64 {
                    let message = PusherMessage::channel_event(
                        "update",
                        "orders",
                        sonic_rs::json!({"asset":"same","padding":"x".repeat(payload_size),"serial":serial}),
                    );
                    let payload = sonic_rs::to_vec(&message).unwrap();
                    // Give half the sockets a distinct predecessor during warmup.
                    let include = |index: usize| !(mixed && serial == 1 && index.is_multiple_of(2));
                    let sockets = recipients
                        .iter()
                        .enumerate()
                        .filter(|(index, _)| include(*index))
                        .map(|(_, entry)| entry.0.clone())
                        .collect();
                    let start = std::time::Instant::now();
                    let outcomes = adapter
                        .send_messages_with_compression(
                            sockets,
                            message.clone(),
                            payload.clone(),
                            "orders",
                            "update",
                            crate::connection_manager::CompressionParams {
                                delta_compression: manager.clone(),
                                channel_settings: None,
                                envelope: None,
                            },
                        )
                        .await;
                    let prepared_ns = start.elapsed().as_nanos();
                    assert!(outcomes.into_iter().all(|outcome| outcome.is_ok()));
                    let mut delivered = 0;
                    for (index, (socket, client, enabled, previous, next_sequence)) in
                        recipients.iter_mut().enumerate()
                    {
                        if !include(index) {
                            continue;
                        }
                        let delivered_message = read(client, WireFormat::Json).await;
                        if delivered_message
                            .event
                            .as_deref()
                            .is_some_and(|event| event.ends_with(":delta"))
                        {
                            let data = match delivered_message.data.unwrap() {
                                sockudo_protocol::messages::MessageData::Json(value) => value,
                                sockudo_protocol::messages::MessageData::String(value) => {
                                    sonic_rs::from_str(&value).unwrap()
                                }
                                other => panic!("invalid delta {other:?}"),
                            };
                            assert_eq!(data["seq"].as_u64(), Some((*next_sequence).into()));
                            assert_eq!(
                                data["base_index"].as_u64(),
                                Some(next_sequence.saturating_sub(1).into())
                            );
                            let expected = manager
                                .compute_delta_for_broadcast(previous, &payload)
                                .unwrap();
                            let actual = base64::Engine::decode(
                                &base64::engine::general_purpose::STANDARD,
                                data["delta"].as_str().unwrap(),
                            )
                            .unwrap();
                            assert_eq!(
                                actual, expected,
                                "wire delta must reconstruct this exact predecessor/next pair"
                            );
                        } else {
                            assert_eq!(
                                sonic_rs::to_value(&delivered_message.data).unwrap(),
                                sonic_rs::to_value(&message.data).unwrap()
                            );
                            if *enabled {
                                assert_eq!(
                                    delivered_message.delta_sequence,
                                    Some((*next_sequence).into())
                                );
                            }
                        }
                        if *enabled {
                            let key = "update:same";
                            let (stored, sequence) = manager
                                .get_last_message_with_sequence(
                                    socket.get_socket_id_sync(),
                                    "orders",
                                    key,
                                )
                                .await
                                .unwrap();
                            assert_eq!(stored.as_ref(), &payload);
                            assert_eq!(sequence, *next_sequence);
                            *next_sequence += 1;
                        }
                        *previous = payload.clone();
                        delivered += 1;
                    }
                    let verified_ns = start.elapsed().as_nanos();
                    if serial >= 3 {
                        println!(
                            "F1,count={count},payload={payload_size},mixed={mixed},sample={},prepare_ns={prepared_ns},verified_ns={verified_ns},delivered={delivered}",
                            serial - 3
                        );
                    }
                }
            }
        }
    }
}

#[tokio::test]
#[ignore = "repeated release predicate routing measurement over real socket subscriptions"]
async fn benchmark_equivalent_predicate_routing() {
    use sockudo_filter::{MessagePredicate, SubscriptionView};
    for count in [8, 128, 512] {
        let mut keep_alive = Vec::new();
        let mut sockets = Vec::new();
        for _ in 0..count {
            let (socket, client) = socket(
                WireFormat::Json,
                sockudo_protocol::ProtocolVersion::V2,
                1024 * 1024,
            )
            .await;
            keep_alive.push(client);
            sockets.push(socket);
        }
        for payload_size in [1024, 16384, 65536] {
            let message = PusherMessage::channel_event(
                "updated",
                "orders",
                sonic_rs::json!({"padding":"x".repeat(payload_size),"price":101}),
            );
            for unique in [1, 16] {
                let predicates: Vec<_> = (0..unique)
                    .map(|index| {
                        Arc::new(
                            MessagePredicate::compile(SubscriptionView {
                                events: vec![if index == 0 {
                                    "updated".into()
                                } else {
                                    format!("other-{index}")
                                }],
                                ..Default::default()
                            })
                            .unwrap(),
                        )
                    })
                    .collect();
                for (index, socket) in sockets.iter().enumerate() {
                    socket.channel_state.insert(
                        "orders".into(),
                        PerChannelState {
                            predicate: Some(predicates[index % unique].clone()),
                            ..Default::default()
                        },
                    );
                }
                let expected = sockets
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| index % unique == 0)
                    .map(|(_, socket)| *socket.get_socket_id_sync())
                    .collect::<std::collections::HashSet<_>>();
                for sample in 0..9 {
                    let mut candidates = sockets.clone();
                    let start = std::time::Instant::now();
                    crate::v2_broadcast::apply_subscription_predicates_in_place(
                        true,
                        "orders",
                        &message,
                        &mut candidates,
                    );
                    let nanos = start.elapsed().as_nanos();
                    assert_eq!(
                        candidates
                            .iter()
                            .map(|socket| *socket.get_socket_id_sync())
                            .collect::<std::collections::HashSet<_>>(),
                        expected
                    );
                    println!(
                        "F4,count={count},payload={payload_size},unique={unique},sample={sample},ns={nanos},accepted={}",
                        candidates.len()
                    );
                }
            }
        }
    }
}
