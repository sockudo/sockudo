// Delivery accounting for `ws_messages_sent_total`, `ws_control_frames_total` and
// `ws_message_bytes`: every node counts only its own local recipients, control
// frames stay out of deliveries, and message size is observed once per message on
// the origin node.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::horizontal_adapter_helpers::{MockConfig, MockTransport};
use async_trait::async_trait;
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::handler::ConnectionHandler;
use sockudo_adapter::horizontal_adapter::BroadcastMessage;
use sockudo_adapter::horizontal_adapter_base::HorizontalAdapterBase;
use sockudo_adapter::local_adapter::LocalAdapter;
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_core::app::{App, AppManager, AppPolicy};
use sockudo_core::cache::CacheManager;
use sockudo_core::error::Result;
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::ServerOptions;
use sockudo_core::presence_registry::PresenceReplication;
use sockudo_core::websocket::{SocketId, WebSocketBufferConfig};
use sockudo_protocol::messages::PusherMessage;
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::{WebSocket, WebSocketWriter};
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, Stream as WsStream, WebSocketStream};
use sonic_rs::Value;
use tokio::net::{TcpListener, TcpStream};

const APP_ID: &str = "delivery-metrics-app";
const CHANNEL: &str = "orders";

type Client = WebSocketStream<WsStream<Http1>>;

#[derive(Default)]
struct RecordingMetrics {
    deliveries: Mutex<usize>,
    delivered_bytes: Mutex<usize>,
    batch_sizes: Mutex<Vec<usize>>,
    control_frames: Mutex<Vec<String>>,
    size_observations: Mutex<Vec<usize>>,
}

impl RecordingMetrics {
    fn deliveries(&self) -> usize {
        *self.deliveries.lock().unwrap()
    }
    fn delivered_bytes(&self) -> usize {
        *self.delivered_bytes.lock().unwrap()
    }
    fn batch_sizes(&self) -> Vec<usize> {
        self.batch_sizes.lock().unwrap().clone()
    }
    fn control_frames(&self) -> Vec<String> {
        self.control_frames.lock().unwrap().clone()
    }
    fn size_observations(&self) -> Vec<usize> {
        self.size_observations.lock().unwrap().clone()
    }
}

#[async_trait]
impl MetricsInterface for RecordingMetrics {
    async fn init(&self) -> Result<()> {
        Ok(())
    }
    fn mark_new_connection(&self, _: &str, _: &SocketId) {}
    fn mark_disconnection(&self, _: &str, _: &SocketId) {}
    fn mark_connection_error(&self, _: &str, _: &str) {}
    fn mark_rate_limit_check(&self, _: &str, _: &str) {}
    fn mark_rate_limit_check_with_context(&self, _: &str, _: &str, _: &str) {}
    fn mark_rate_limit_triggered(&self, _: &str, _: &str) {}
    fn mark_rate_limit_triggered_with_context(&self, _: &str, _: &str, _: &str) {}
    fn mark_channel_subscription(&self, _: &str, _: &str) {}
    fn mark_channel_unsubscription(&self, _: &str, _: &str) {}
    fn mark_api_message(&self, _: &str, _: usize, _: usize) {}
    fn mark_ws_message_sent(&self, _: &str, size: usize) {
        *self.deliveries.lock().unwrap() += 1;
        *self.delivered_bytes.lock().unwrap() += size;
    }
    fn mark_ws_messages_sent_batch(&self, _: &str, size: usize, count: usize) {
        *self.deliveries.lock().unwrap() += count;
        *self.delivered_bytes.lock().unwrap() += size * count;
        self.batch_sizes.lock().unwrap().push(size);
    }
    fn mark_ws_control_frame(&self, _: &str, frame_type: &str) {
        self.control_frames
            .lock()
            .unwrap()
            .push(frame_type.to_string());
    }
    fn observe_ws_message_size(&self, _: &str, size: usize) {
        self.size_observations.lock().unwrap().push(size);
    }
    fn mark_ws_message_received(&self, _: &str, _: usize) {}
    fn track_horizontal_adapter_resolve_time(&self, _: &str, _: f64) {}
    fn track_horizontal_adapter_resolved_promises(&self, _: &str, _: bool, _: &str) {}
    fn mark_horizontal_adapter_request_sent(&self, _: &str) {}
    fn mark_horizontal_adapter_request_received(&self, _: &str) {}
    fn mark_horizontal_adapter_response_received(&self, _: &str) {}
    fn track_broadcast_latency(&self, _: &str, _: &str, _: usize, _: f64) {}
    fn track_horizontal_delta_compression(&self, _: &str, _: &str, _: bool) {}
    fn track_delta_compression_bandwidth(&self, _: &str, _: &str, _: usize, _: usize) {}
    fn track_delta_compression_full_message(&self, _: &str, _: &str) {}
    fn track_delta_compression_delta_message(&self, _: &str, _: &str) {}
    async fn get_metrics_as_plaintext(&self) -> String {
        String::new()
    }
    async fn get_metrics_as_json(&self) -> Value {
        sonic_rs::json!({})
    }
    async fn clear(&self) {}
}

struct NullCacheManager;

#[async_trait]
impl CacheManager for NullCacheManager {
    async fn has(&self, _: &str) -> Result<bool> {
        Ok(false)
    }
    async fn get(&self, _: &str) -> Result<Option<String>> {
        Ok(None)
    }
    async fn set(&self, _: &str, _: &str, _: u64) -> Result<()> {
        Ok(())
    }
    async fn remove(&self, _: &str) -> Result<()> {
        Ok(())
    }
    async fn disconnect(&self) -> Result<()> {
        Ok(())
    }
    async fn ttl(&self, _: &str) -> Result<Option<Duration>> {
        Ok(None)
    }
    async fn check_health(&self) -> Result<()> {
        Ok(())
    }
}

fn make_app() -> App {
    App::from_policy(
        APP_ID.to_string(),
        "delivery-metrics-key".to_string(),
        "secret".to_string(),
        true,
        AppPolicy::default(),
    )
}

async fn make_ws_pair() -> (WebSocketWriter, Client) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        sockudo_ws::handshake::server_handshake(&mut stream)
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

    let client_stream = TcpStream::connect(addr).await.unwrap();
    let client = WebSocketClient::<Http1>::new(WsConfig::default());
    let (client_ws, _): (Client, _) = client
        .connect(client_stream, &addr.to_string(), "/", None)
        .await
        .unwrap();

    (server_task.await.unwrap(), client_ws)
}

/// Register `count` sockets on `local` and subscribe them to [`CHANNEL`].
async fn add_subscribers(
    local: &LocalAdapter,
    app_manager: &Arc<MemoryAppManager>,
    count: usize,
) -> Vec<(SocketId, Client)> {
    let mut sockets = Vec::with_capacity(count);
    for _ in 0..count {
        let socket_id = SocketId::new();
        let (writer, client) = make_ws_pair().await;
        local
            .add_socket(
                socket_id,
                writer,
                APP_ID,
                app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
                WebSocketBufferConfig::default(),
                ProtocolVersion::V1,
                WireFormat::Json,
                true,
                sockudo_protocol::AppendMode::Delta,
            )
            .await
            .unwrap();
        local
            .add_to_channel(APP_ID, CHANNEL, &socket_id)
            .await
            .unwrap();
        sockets.push((socket_id, client));
    }
    sockets
}

async fn make_app_manager() -> Arc<MemoryAppManager> {
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(make_app()).await.unwrap();
    app_manager
}

async fn build_local_handler(
    metrics: Arc<RecordingMetrics>,
) -> (ConnectionHandler, Arc<LocalAdapter>, Arc<MemoryAppManager>) {
    let app_manager = make_app_manager().await;
    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;

    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        ServerOptions::default(),
    )
    .local_adapter(adapter.clone())
    .metrics(metrics)
    .build();

    (handler, adapter, app_manager)
}

fn data_event() -> PusherMessage {
    PusherMessage::channel_event("order-created", CHANNEL, sonic_rs::json!({"id": 42}))
}

/// A horizontal node with listeners started, `subscribers` local sockets on
/// [`CHANNEL`], and `metrics` attached.
async fn build_receiving_node(
    metrics: Arc<RecordingMetrics>,
    subscribers: usize,
) -> (
    HorizontalAdapterBase<MockTransport>,
    Vec<(SocketId, Client)>,
) {
    let app_manager = make_app_manager().await;
    let node = HorizontalAdapterBase::<MockTransport>::new(MockConfig::default())
        .await
        .unwrap();
    node.init().await;
    node.start_listeners().await.unwrap();
    node.set_metrics(metrics).await.unwrap();
    let sockets = add_subscribers(&node.horizontal.local_adapter, &app_manager, subscribers).await;
    (node, sockets)
}

fn remote_broadcast(node_id: &str) -> BroadcastMessage {
    BroadcastMessage {
        node_id: node_id.to_string(),
        app_id: APP_ID.to_string(),
        channel: CHANNEL.to_string(),
        message: sonic_rs::to_string(&data_event()).unwrap(),
        presence_replication: None,
        envelope: None,
        except_socket_id: None,
        timestamp_ms: None,
        compression_metadata: None,
        idempotency_key: None,
        ephemeral: false,
        trace_context: Default::default(),
    }
}

#[tokio::test]
async fn pong_is_a_control_frame_not_a_delivery() {
    let metrics = Arc::new(RecordingMetrics::default());
    let (handler, adapter, app_manager) = build_local_handler(metrics.clone()).await;
    let sockets = add_subscribers(&adapter, &app_manager, 1).await;

    handler
        .send_message_to_socket(APP_ID, &sockets[0].0, PusherMessage::pong())
        .await
        .unwrap();

    assert_eq!(metrics.control_frames(), vec!["pong".to_string()]);
    assert_eq!(metrics.deliveries(), 0);
    assert!(metrics.size_observations().is_empty());
}

#[tokio::test]
async fn single_socket_data_send_counts_one_delivery_and_one_size_observation() {
    let metrics = Arc::new(RecordingMetrics::default());
    let (handler, adapter, app_manager) = build_local_handler(metrics.clone()).await;
    let sockets = add_subscribers(&adapter, &app_manager, 1).await;
    let message = data_event();
    let expected_size = sonic_rs::to_string(&message).unwrap().len();

    handler
        .send_message_to_socket(APP_ID, &sockets[0].0, message)
        .await
        .unwrap();

    assert_eq!(metrics.deliveries(), 1);
    assert_eq!(metrics.size_observations(), vec![expected_size]);
    assert!(metrics.control_frames().is_empty());
}

#[tokio::test]
async fn local_fanout_counts_each_recipient_once_and_observes_size_once() {
    let metrics = Arc::new(RecordingMetrics::default());
    let (handler, adapter, app_manager) = build_local_handler(metrics.clone()).await;
    let _sockets = add_subscribers(&adapter, &app_manager, 3).await;

    handler
        .broadcast_to_channel(&make_app(), CHANNEL, data_event(), None)
        .await
        .unwrap();

    assert_eq!(metrics.deliveries(), 3);
    let batch_sizes = metrics.batch_sizes();
    assert_eq!(batch_sizes.len(), 1);
    assert_eq!(
        metrics.size_observations(),
        batch_sizes,
        "one size observation per message, equal to the counted payload size"
    );
}

#[tokio::test]
async fn local_fanout_excludes_the_sender_socket() {
    let metrics = Arc::new(RecordingMetrics::default());
    let (handler, adapter, app_manager) = build_local_handler(metrics.clone()).await;
    let sockets = add_subscribers(&adapter, &app_manager, 3).await;

    handler
        .broadcast_to_channel(&make_app(), CHANNEL, data_event(), Some(&sockets[0].0))
        .await
        .unwrap();

    assert_eq!(metrics.deliveries(), 2);
}

#[tokio::test]
async fn remote_fanout_is_counted_by_the_receiving_node() {
    let metrics = Arc::new(RecordingMetrics::default());
    let (node, _sockets) = build_receiving_node(metrics.clone(), 4).await;
    let broadcast = remote_broadcast("remote-node");
    let payload_size = broadcast.message.len();

    node.transport.deliver_broadcast(broadcast).await.unwrap();

    assert_eq!(metrics.deliveries(), 4);
    assert_eq!(metrics.delivered_bytes(), payload_size * 4);
    assert!(
        metrics.size_observations().is_empty(),
        "receivers must not observe message size"
    );
}

#[tokio::test]
async fn remote_fanout_excludes_the_sender_socket() {
    let metrics = Arc::new(RecordingMetrics::default());
    let (node, sockets) = build_receiving_node(metrics.clone(), 3).await;
    let mut broadcast = remote_broadcast("remote-node");
    broadcast.except_socket_id = Some(sockets[0].0.to_string());

    node.transport.deliver_broadcast(broadcast).await.unwrap();

    assert_eq!(metrics.deliveries(), 2);
}

#[tokio::test]
async fn own_broadcast_looped_back_is_not_counted() {
    let metrics = Arc::new(RecordingMetrics::default());
    let (node, _sockets) = build_receiving_node(metrics.clone(), 2).await;
    let own = remote_broadcast(&node.node_id);

    node.transport.deliver_broadcast(own).await.unwrap();

    assert_eq!(metrics.deliveries(), 0);
}

#[tokio::test]
async fn presence_replication_broadcast_is_not_counted() {
    let metrics = Arc::new(RecordingMetrics::default());
    let (node, _sockets) = build_receiving_node(metrics.clone(), 2).await;
    let mut replication = remote_broadcast("remote-node");
    replication.presence_replication = Some(PresenceReplication::default());

    node.transport.deliver_broadcast(replication).await.unwrap();

    assert_eq!(metrics.deliveries(), 0);
}

// The publisher counts only its local sockets. Without a local adapter it cannot
// separate local from cluster-wide counts, and remote nodes count their own
// deliveries, so it must count nothing rather than the cluster total.
#[tokio::test]
async fn publisher_without_local_adapter_does_not_count_cluster_wide() {
    let metrics = Arc::new(RecordingMetrics::default());
    let app_manager = make_app_manager().await;
    let node = Arc::new(MockConfig::create_multi_node_adapter().await.unwrap());
    node.init().await;
    node.start_listeners().await.unwrap();
    let _sockets = add_subscribers(&node.horizontal.local_adapter, &app_manager, 2).await;

    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        node.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        ServerOptions::default(),
    )
    .metrics(metrics.clone())
    .build();

    handler
        .broadcast_to_channel(&make_app(), CHANNEL, data_event(), None)
        .await
        .unwrap();

    assert_eq!(metrics.deliveries(), 0);
    assert_eq!(
        metrics.size_observations().len(),
        1,
        "the origin still observes the message size once"
    );
}
