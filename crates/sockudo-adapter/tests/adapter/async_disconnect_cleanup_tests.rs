use async_trait::async_trait;
use crossfire::mpsc;
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::cleanup::{CleanupSender, DisconnectTask};
use sockudo_adapter::handler::ConnectionHandler;
use sockudo_adapter::local_adapter::LocalAdapter;
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_core::app::{App, AppLimitsPolicy, AppManager, AppPolicy};
use sockudo_core::cache::CacheManager;
use sockudo_core::error::Result;
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::ServerOptions;
use sockudo_core::websocket::{SocketId, WebSocketBufferConfig};
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::{WebSocket, WebSocketWriter};
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, Stream as WsStream, WebSocketStream};
use sonic_rs::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};

struct CountingMetrics {
    disconnections: AtomicUsize,
}

impl CountingMetrics {
    fn new() -> Self {
        Self {
            disconnections: AtomicUsize::new(0),
        }
    }

    fn disconnections(&self) -> usize {
        self.disconnections.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl MetricsInterface for CountingMetrics {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    fn mark_new_connection(&self, _app_id: &str, _socket_id: &SocketId) {}

    fn mark_disconnection(&self, _app_id: &str, _socket_id: &SocketId) {
        self.disconnections.fetch_add(1, Ordering::SeqCst);
    }

    fn mark_connection_error(&self, _: &str, _: &str) {}
    fn mark_rate_limit_check(&self, _: &str, _: &str) {}
    fn mark_rate_limit_check_with_context(&self, _: &str, _: &str, _: &str) {}
    fn mark_rate_limit_triggered(&self, _: &str, _: &str) {}
    fn mark_rate_limit_triggered_with_context(&self, _: &str, _: &str, _: &str) {}
    fn mark_channel_subscription(&self, _: &str, _: &str) {}
    fn mark_channel_unsubscription(&self, _: &str, _: &str) {}
    fn mark_api_message(&self, _: &str, _: usize, _: usize) {}
    fn mark_ws_message_sent(&self, _: &str, _: usize) {}
    fn mark_ws_messages_sent_batch(&self, _: &str, _: usize, _: usize) {}
    fn mark_ws_message_received(&self, _: &str, _: usize) {}
    fn track_horizontal_adapter_resolve_time(&self, _: &str, _: f64) {}
    fn track_horizontal_adapter_resolved_promises(&self, _: &str, _: bool) {}
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

    fn mark_channel_activated(&self, _: &str, _: &str) {}
    fn mark_channel_deactivated(&self, _: &str, _: &str) {}
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

const APP_ID: &str = "async-disconnect-test-app";
const APP_KEY: &str = "async-disconnect-test-key";

fn make_app() -> App {
    App::from_policy(
        APP_ID.to_string(),
        APP_KEY.to_string(),
        "secret".to_string(),
        true,
        AppPolicy {
            limits: AppLimitsPolicy {
                max_connections: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
}

async fn make_ws_pair() -> (WebSocketWriter, WebSocketStream<WsStream<Http1>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        sockudo_ws::handshake::server_handshake(&mut stream)
            .await
            .unwrap();
        let ws = WebSocket::from_tcp(stream, WsConfig::default());
        let (_reader, writer) = ws.split();
        writer
    });

    let client_stream = TcpStream::connect(addr).await.unwrap();
    let client = WebSocketClient::<Http1>::new(WsConfig::default());
    let (client_ws, _): (WebSocketStream<WsStream<Http1>>, _) = client
        .connect(client_stream, &addr.to_string(), "/", None)
        .await
        .unwrap();

    let server_writer = server_task.await.unwrap();
    (server_writer, client_ws)
}

fn dummy_disconnect_task() -> DisconnectTask {
    DisconnectTask {
        socket_id: SocketId::new(),
        app_id: APP_ID.to_string(),
        subscribed_channels: vec![],
        user_id: None,
        timestamp: Instant::now(),
        connection_info: None,
        presence_ungraceful_timeout_seconds: 0,
    }
}

#[tokio::test]
async fn delayed_worker_cannot_delay_cancellation() {
    let (tx, _rx) = mpsc::bounded_async::<DisconnectTask>(10);
    let cleanup_sender = CleanupSender::Direct(tx);

    let app = make_app();
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(app).await.unwrap();

    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;

    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        ServerOptions::default(),
    )
    .local_adapter(adapter.clone())
    .cleanup_queue(cleanup_sender)
    .build();

    let socket_id = SocketId::new();
    let (writer, _client) = make_ws_pair().await;
    adapter
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

    let token = adapter
        .get_connection(&socket_id, APP_ID)
        .await
        .unwrap()
        .cancellation_token();

    assert!(
        !token.is_cancelled(),
        "token must be live before disconnect"
    );

    handler.handle_disconnect(APP_ID, &socket_id).await.unwrap();

    assert!(
        token.is_cancelled(),
        "token must be cancelled immediately regardless of queue consumption"
    );
}

#[tokio::test]
async fn queue_fallback_remains_idempotent() {
    let (tx, _rx) = mpsc::bounded_async::<DisconnectTask>(1);

    tx.try_send(dummy_disconnect_task()).unwrap();

    let cleanup_sender = CleanupSender::Direct(tx);

    let metrics = Arc::new(CountingMetrics::new());
    let app = make_app();
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(app).await.unwrap();

    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;

    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        ServerOptions::default(),
    )
    .local_adapter(adapter.clone())
    .cleanup_queue(cleanup_sender)
    .metrics(metrics.clone() as Arc<dyn MetricsInterface + Send + Sync>)
    .build();

    let socket_id = SocketId::new();
    let (writer, _client) = make_ws_pair().await;
    adapter
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

    let token = adapter
        .get_connection(&socket_id, APP_ID)
        .await
        .unwrap()
        .cancellation_token();

    assert!(
        !token.is_cancelled(),
        "token must be live before disconnect"
    );

    handler.handle_disconnect(APP_ID, &socket_id).await.unwrap();

    assert!(
        token.is_cancelled(),
        "token must be cancelled even when queue is full (shutdown precedes try_send)"
    );

    assert_eq!(
        metrics.disconnections(),
        1,
        "lifecycle metric must change exactly once regardless of fallback path"
    );
}
