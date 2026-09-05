// Regression tests for the `sockudo_connected` gauge: every mark_new_connection
// must be balanced by a mark_disconnection, and a draining node must stop
// accepting connections. handle_socket and initialize_socket_with_quota_check
// are private/need a live socket, so most tests drive the same public calls the
// fixed paths use and assert on CountingMetrics.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::delegate_connection_manager;
use sockudo_adapter::handler::ConnectionHandler;
use sockudo_adapter::local_adapter::LocalAdapter;
use sockudo_adapter::test_support::NoopConnectionManager;
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
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};

// Counts only the two calls that move the `sockudo_connected` gauge.
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

    fn disconnections(&self) -> usize {
        self.disconnections.load(Ordering::SeqCst)
    }

    fn net(&self) -> i64 {
        self.new_connections.load(Ordering::SeqCst) as i64
            - self.disconnections.load(Ordering::SeqCst) as i64
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

// Reports a fixed socket count so the quota check rejects before mark_new_connection.
#[derive(Clone)]
struct TestAdapter {
    reported_count: usize,
}

impl TestAdapter {
    fn with_count(n: usize) -> Self {
        Self { reported_count: n }
    }
}

#[async_trait]
impl NoopConnectionManager for TestAdapter {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    async fn get_sockets_count(&self, _app_id: &str) -> Result<usize> {
        Ok(self.reported_count)
    }
}
delegate_connection_manager!(TestAdapter);

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

const APP_ID: &str = "gauge-test-app";
const APP_KEY: &str = "gauge-test-key";

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

async fn build_handler_with_local(
    app: App,
    metrics: Arc<CountingMetrics>,
) -> (ConnectionHandler, Arc<LocalAdapter>, Arc<MemoryAppManager>) {
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
    .metrics(metrics)
    .build();

    (handler, adapter, app_manager)
}

// Once registered, cleanup_socket (-> handle_disconnect) must decrement,
// even when a post-registration setup step fails.
#[tokio::test]
async fn connected_gauge_setup_failure_decrements_on_cleanup() {
    let metrics = Arc::new(CountingMetrics::new());
    let (handler, adapter, app_manager) =
        build_handler_with_local(make_app(0), metrics.clone()).await;

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

    metrics.mark_new_connection(APP_ID, &socket_id);
    assert_eq!(
        metrics.new_connections(),
        1,
        "pre-condition: one socket registered"
    );

    handler.handle_disconnect(APP_ID, &socket_id).await.unwrap();

    assert_eq!(
        metrics.new_connections(),
        1,
        "mark_new_connection was called exactly once"
    );
    assert_eq!(
        metrics.disconnections(),
        1,
        "mark_disconnection was called exactly once by handle_disconnect"
    );
    assert_eq!(metrics.net(), 0, "gauge must be balanced after cleanup");
}

// Two handle_disconnect calls (Close frame, then cleanup_socket) must decrement
// once: the `disconnecting` flag guards the second.
#[tokio::test]
async fn connected_gauge_no_double_decrement_on_close_and_cleanup() {
    let metrics = Arc::new(CountingMetrics::new());
    let (handler, adapter, app_manager) =
        build_handler_with_local(make_app(0), metrics.clone()).await;

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

    metrics.mark_new_connection(APP_ID, &socket_id);

    handler.handle_disconnect(APP_ID, &socket_id).await.unwrap();
    handler.handle_disconnect(APP_ID, &socket_id).await.unwrap();

    assert_eq!(
        metrics.disconnections(),
        1,
        "disconnecting flag must prevent the second handle_disconnect from decrementing"
    );
    assert_eq!(metrics.net(), 0, "gauge must remain balanced");
}

// Replacing a socket id decrements the evicted socket before the new one is
// counted, so the gauge nets one live connection.
#[tokio::test]
async fn connected_gauge_socket_replacement_nets_zero() {
    let metrics = Arc::new(CountingMetrics::new());
    let (_, adapter, app_manager) = build_handler_with_local(make_app(0), metrics.clone()).await;

    let socket_id = SocketId::new();
    let (writer1, _client1) = make_ws_pair().await;
    adapter
        .add_socket(
            socket_id,
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

    metrics.mark_new_connection(APP_ID, &socket_id);
    assert_eq!(metrics.new_connections(), 1, "one connection registered");

    let old_conn = adapter
        .get_connection(&socket_id, APP_ID)
        .await
        .expect("old socket must be present before replacement");
    adapter.cleanup_connection(APP_ID, old_conn).await;
    metrics.mark_disconnection(APP_ID, &socket_id);

    assert_eq!(metrics.disconnections(), 1, "old socket decremented");

    let (writer2, _client2) = make_ws_pair().await;
    adapter
        .add_socket(
            socket_id,
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
    metrics.mark_new_connection(APP_ID, &socket_id);

    assert_eq!(metrics.new_connections(), 2, "total increments: old + new");
    assert_eq!(
        metrics.disconnections(),
        1,
        "only the evicted socket was decremented"
    );
    assert_eq!(
        metrics.net(),
        1,
        "exactly one live connection remains after replacement"
    );
}

// A quota rejection happens before registration, so neither counter moves.
#[tokio::test]
async fn connected_gauge_over_quota_never_increments() {
    let app = make_app(1);

    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(app).await.unwrap();

    let metrics = Arc::new(CountingMetrics::new());

    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        Arc::new(TestAdapter::with_count(1)) as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        ServerOptions::default(),
    )
    .metrics(metrics.clone())
    .build();

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
        matches!(result, Err(Error::OverConnectionQuota)),
        "expected OverConnectionQuota, got {result:?}"
    );
    assert_eq!(
        metrics.new_connections(),
        0,
        "mark_new_connection must NOT be called on quota rejection"
    );
    assert_eq!(
        metrics.disconnections(),
        0,
        "mark_disconnection must NOT be called on quota rejection"
    );
    assert_eq!(metrics.net(), 0, "gauge must remain untouched");
}

// While draining (running=false), is_accepting() is false, which is what /up and
// the WS upgrade gate on to return 503 and stop the gauge climbing on a dying pod.
#[test]
fn drain_guard_is_accepting_false_when_running_false() {
    let handler = ConnectionHandler::builder(
        Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>,
        Arc::new(TestAdapter::with_count(0)) as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        ServerOptions::default(),
    )
    .running(Arc::new(AtomicBool::new(false)))
    .build();

    assert!(
        !handler.is_accepting(),
        "is_accepting() must be false while draining so the /up and WS guards return 503"
    );
}

// Without an injected running flag the handler must still accept connections,
// otherwise a builder missing .running() would silently reject all traffic.
#[test]
fn is_accepting_defaults_to_true() {
    let app_manager = Arc::new(MemoryAppManager::new());
    let handler = ConnectionHandler::builder(
        app_manager as Arc<dyn AppManager + Send + Sync>,
        Arc::new(TestAdapter::with_count(0)) as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        ServerOptions::default(),
    )
    .build();

    assert!(
        handler.is_accepting(),
        "default handler must accept connections"
    );
}
