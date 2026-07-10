use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::handler::ConnectionHandler;
use sockudo_adapter::local_adapter::LocalAdapter;
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_core::app::{App, AppLimitsPolicy, AppManager, AppPolicy};
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::ServerOptions;
use sockudo_core::websocket::{SocketId, WebSocketBufferConfig};
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::{WebSocket, WebSocketWriter};
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, Stream as WsStream, WebSocketStream};
use sonic_rs::Value;
use tokio::net::{TcpListener, TcpStream};

#[allow(dead_code)]
struct CountingMetrics {
    new_connections: AtomicUsize,
    disconnections: AtomicUsize,
}

impl CountingMetrics {
    fn new() -> Self {
        Self {
            new_connections: AtomicUsize::new(0),
            disconnections: AtomicUsize::new(0),
        }
    }

    fn new_connections(&self) -> usize {
        self.new_connections.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl MetricsInterface for CountingMetrics {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    fn mark_new_connection(&self, _app_id: &str, _socket_id: &SocketId) {
        self.new_connections.fetch_add(1, Ordering::SeqCst);
    }

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

const APP_ID: &str = "capacity-test-app";
const APP_KEY: &str = "capacity-test-key";

fn make_app(max_connections: u32) -> App {
    App::from_policy(
        APP_ID.to_string(),
        APP_KEY.to_string(),
        "secret".to_string(),
        true,
        AppPolicy {
            limits: AppLimitsPolicy {
                max_connections,
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

async fn make_full_ws_pair() -> (WebSocket, WebSocketStream<WsStream<Http1>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        sockudo_ws::handshake::server_handshake(&mut stream)
            .await
            .unwrap();
        WebSocket::from_tcp(stream, WsConfig::default())
    });

    let client_stream = TcpStream::connect(addr).await.unwrap();
    let client = WebSocketClient::<Http1>::new(WsConfig::default());
    let (client_ws, _): (WebSocketStream<WsStream<Http1>>, _) = client
        .connect(client_stream, &addr.to_string(), "/", None)
        .await
        .unwrap();

    let server_ws = server_task.await.unwrap();
    (server_ws, client_ws)
}

#[tokio::test]
async fn server_max_connections_rejects_over_capacity() {
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(make_app(0)).await.unwrap();

    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;

    let metrics = Arc::new(CountingMetrics::new());

    let opts = ServerOptions {
        max_connections: 2,
        ..Default::default()
    };

    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        opts,
    )
    .local_adapter(adapter.clone())
    .metrics(metrics.clone())
    .build();

    let socket_id_1 = SocketId::new();
    let (writer1, _client1) = make_ws_pair().await;
    adapter
        .add_socket(
            socket_id_1,
            writer1,
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

    let socket_id_2 = SocketId::new();
    let (writer2, _client2) = make_ws_pair().await;
    adapter
        .add_socket(
            socket_id_2,
            writer2,
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

    assert_eq!(
        adapter.get_total_sockets_count(),
        2,
        "adapter should have 2 sockets"
    );

    let (server_ws, _client) = make_full_ws_pair().await;

    let result = handler
        .handle_socket(
            server_ws,
            APP_KEY.to_string(),
            None,
            ProtocolVersion::V1,
            WireFormat::Json,
            true,
            sockudo_protocol::AppendMode::Delta,
            None,
        )
        .await;

    assert!(
        matches!(result, Err(Error::OverCapacity)),
        "expected OverCapacity, got {result:?}"
    );
    assert_eq!(
        metrics.new_connections(),
        0,
        "mark_new_connection must NOT be called on capacity rejection"
    );
}

#[tokio::test]
async fn server_max_connections_zero_disables_limit() {
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(make_app(0)).await.unwrap();

    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;

    let metrics = Arc::new(CountingMetrics::new());

    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        ServerOptions::default(),
    )
    .local_adapter(adapter.clone())
    .metrics(metrics.clone())
    .build();

    let socket_id_1 = SocketId::new();
    let (writer1, _client1) = make_ws_pair().await;
    adapter
        .add_socket(
            socket_id_1,
            writer1,
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

    let socket_id_2 = SocketId::new();
    let (writer2, _client2) = make_ws_pair().await;
    adapter
        .add_socket(
            socket_id_2,
            writer2,
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

    assert_eq!(
        adapter.get_total_sockets_count(),
        2,
        "adapter should have 2 sockets"
    );

    let (server_ws, _client) = make_full_ws_pair().await;

    let result = tokio::time::timeout(
        Duration::from_secs(3),
        handler.handle_socket(
            server_ws,
            APP_KEY.to_string(),
            None,
            ProtocolVersion::V1,
            WireFormat::Json,
            true,
            sockudo_protocol::AppendMode::Delta,
            None,
        ),
    )
    .await;

    match result {
        Err(_elapsed) => {} // timed out inside the message loop — limit was not hit
        Ok(inner) => assert!(
            !matches!(inner, Err(Error::OverCapacity)),
            "max_connections = 0 should disable limit, got {inner:?}"
        ),
    }
}
