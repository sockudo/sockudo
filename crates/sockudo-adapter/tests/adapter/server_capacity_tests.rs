use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::connection_manager::HorizontalAdapterInterface;
use sockudo_adapter::delegate_connection_manager;
use sockudo_adapter::handler::ConnectionHandler;
use sockudo_adapter::local_adapter::LocalAdapter;
use sockudo_adapter::test_support::NoopConnectionManager;
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_core::app::{App, AppLimitsPolicy, AppManager, AppPolicy};
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::metrics::{MetricsInterface, WsAdmissionRejection};
use sockudo_core::options::ServerOptions;
use sockudo_core::websocket::{DisconnectCause, SocketId, WebSocketBufferConfig};
use sockudo_protocol::messages::{MessageData, PusherMessage};
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::Message;
use sockudo_ws::axum_integration::{WebSocket, WebSocketWriter};
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, Stream as WsStream, WebSocketStream};
use sonic_rs::{JsonValueTrait, Value};
use std::any::Any;
use tokio::net::{TcpListener, TcpStream};

#[allow(dead_code)]
struct CountingMetrics {
    new_connections: AtomicUsize,
    disconnections: AtomicUsize,
    memory_pressure_rejections: AtomicUsize,
    connection_errors: AtomicUsize,
    admission_rejections: Mutex<Vec<WsAdmissionRejection>>,
    session_ends: Mutex<Vec<DisconnectCause>>,
}

impl CountingMetrics {
    fn new() -> Self {
        Self {
            new_connections: AtomicUsize::new(0),
            disconnections: AtomicUsize::new(0),
            memory_pressure_rejections: AtomicUsize::new(0),
            connection_errors: AtomicUsize::new(0),
            admission_rejections: Mutex::new(Vec::new()),
            session_ends: Mutex::new(Vec::new()),
        }
    }

    fn new_connections(&self) -> usize {
        self.new_connections.load(Ordering::SeqCst)
    }

    #[cfg(target_os = "linux")]
    fn memory_pressure_rejections(&self) -> usize {
        self.memory_pressure_rejections.load(Ordering::SeqCst)
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

    fn mark_connection_error(&self, _: &str, _: &str) {
        self.connection_errors.fetch_add(1, Ordering::SeqCst);
    }
    fn mark_ws_admission_rejection(&self, reason: WsAdmissionRejection) {
        self.admission_rejections.lock().unwrap().push(reason);
    }
    fn mark_ws_session_end(&self, cause: DisconnectCause) {
        self.session_ends.lock().unwrap().push(cause);
    }
    fn mark_memory_pressure_rejection(&self) {
        self.memory_pressure_rejections
            .fetch_add(1, Ordering::SeqCst);
    }
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

async fn wait_for(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("condition was not reached");
}

fn spawn_socket(
    handler: &ConnectionHandler,
    socket: WebSocket,
) -> tokio::task::JoinHandle<Result<()>> {
    let handler = handler.clone();
    tokio::spawn(async move {
        handler
            .handle_socket(
                socket,
                APP_KEY.to_string(),
                None,
                ProtocolVersion::V1,
                WireFormat::Json,
                true,
                sockudo_protocol::AppendMode::Delta,
                None,
            )
            .await
    })
}

async fn capped_handler(
    max_connections: u32,
    app_limit: u32,
) -> (ConnectionHandler, Arc<CountingMetrics>) {
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(make_app(app_limit)).await.unwrap();
    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;
    let metrics = Arc::new(CountingMetrics::new());
    let handler = ConnectionHandler::builder(
        app_manager as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        ServerOptions {
            max_connections,
            ..Default::default()
        },
    )
    .local_adapter(adapter)
    .metrics(metrics.clone())
    .build();
    (handler, metrics)
}

async fn next_frame(client: &mut WebSocketStream<WsStream<Http1>>) -> Message {
    tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .expect("frame timed out")
        .expect("socket closed before the rejection frame")
        .expect("frame read failed")
}

fn decode_error(frame: Message, wire_format: WireFormat) -> (bool, PusherMessage) {
    let (binary, bytes) = match frame {
        Message::Text(bytes) => (false, bytes.to_vec()),
        Message::Binary(bytes) => (true, bytes.to_vec()),
        other => panic!("expected a data frame, got {other:?}"),
    };
    let message = sockudo_protocol::wire::deserialize_message(&bytes, wire_format)
        .expect("rejection frame must decode in the negotiated format");
    (binary, message)
}

fn error_code(message: &PusherMessage) -> Option<u64> {
    match message.data.as_ref()? {
        MessageData::Json(value) => value.get("code").and_then(|code| code.as_u64()),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

    let mut _clients = Vec::new();
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let (server_ws, client) = make_full_ws_pair().await;
        _clients.push(client);
        tasks.push(spawn_socket(&handler, server_ws));
    }
    wait_for(|| metrics.new_connections() == 2).await;
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
        2,
        "mark_new_connection must NOT be called on capacity rejection"
    );
    assert_eq!(
        adapter.get_total_sockets_count(),
        2,
        "capacity rejection must not register a socket"
    );

    for task in tasks {
        task.abort();
    }
}

/// Registers sockets only after the test opens the gate, so competing upgrades
/// reach the admission decision together instead of one at a time.
struct GatedAdapter {
    gate: Arc<tokio::sync::Semaphore>,
    entered: AtomicUsize,
    registered: AtomicUsize,
}

#[async_trait]
impl NoopConnectionManager for GatedAdapter {
    async fn add_socket(
        &self,
        _socket_id: SocketId,
        _socket: WebSocketWriter,
        _app_id: &str,
        _app_manager: Arc<dyn AppManager + Send + Sync>,
        _buffer_config: WebSocketBufferConfig,
        _protocol_version: ProtocolVersion,
        _wire_format: WireFormat,
        _echo_messages: bool,
        _append_mode: sockudo_protocol::AppendMode,
    ) -> Result<()> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.gate
            .acquire()
            .await
            .map_err(|_| Error::ConnectionClosed("admission gate closed".to_string()))?
            .forget();
        self.registered.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

delegate_connection_manager!(GatedAdapter);

/// Reports peer socket counts like a clustered adapter, so the app quota can be
/// exercised without a real cluster.
struct ClusterCountAdapter {
    peer_sockets: usize,
}

#[async_trait]
impl NoopConnectionManager for ClusterCountAdapter {
    async fn get_sockets_count(&self, _app_id: &str) -> Result<usize> {
        Ok(self.peer_sockets)
    }

    fn as_horizontal_adapter(&self) -> Option<&dyn HorizontalAdapterInterface> {
        Some(self)
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[async_trait]
impl HorizontalAdapterInterface for ClusterCountAdapter {
    async fn broadcast_presence_join(
        &self,
        _app_id: &str,
        _channel: &str,
        _user_id: &str,
        _socket_id: &str,
        _user_info: Option<Value>,
    ) -> Result<()> {
        Ok(())
    }

    async fn broadcast_presence_leave(
        &self,
        _app_id: &str,
        _channel: &str,
        _user_id: &str,
        _socket_id: &str,
    ) -> Result<()> {
        Ok(())
    }

    async fn broadcast_presence_update(
        &self,
        _app_id: &str,
        _channel: &str,
        _user_id: &str,
        _socket_id: &str,
        _user_info: Value,
    ) -> Result<()> {
        Ok(())
    }
}

delegate_connection_manager!(ClusterCountAdapter);

async fn cluster_handler(
    peer_sockets: usize,
    app_limit: u32,
) -> (ConnectionHandler, Arc<CountingMetrics>) {
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(make_app(app_limit)).await.unwrap();
    let metrics = Arc::new(CountingMetrics::new());
    let handler = ConnectionHandler::builder(
        app_manager as Arc<dyn AppManager + Send + Sync>,
        Arc::new(ClusterCountAdapter { peer_sockets }) as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        ServerOptions::default(),
    )
    .metrics(metrics.clone())
    .build();
    (handler, metrics)
}

/// Runs `ATTEMPTS` simultaneous upgrades against `node_limit`/`app_limit` and
/// returns `(admitted, registered)`.
async fn gated_admissions(node_limit: u32, app_limit: u32) -> (usize, usize) {
    const ATTEMPTS: usize = 8;

    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(make_app(app_limit)).await.unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let adapter = Arc::new(GatedAdapter {
        gate: gate.clone(),
        entered: AtomicUsize::new(0),
        registered: AtomicUsize::new(0),
    });
    let metrics = Arc::new(CountingMetrics::new());
    let handler = ConnectionHandler::builder(
        app_manager as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        ServerOptions {
            max_connections: node_limit,
            ..Default::default()
        },
    )
    .metrics(metrics.clone())
    .build();

    let mut _clients = Vec::new();
    let mut tasks = Vec::new();
    for _ in 0..ATTEMPTS {
        let (server_ws, client) = make_full_ws_pair().await;
        _clients.push(client);
        tasks.push(spawn_socket(&handler, server_ws));
    }

    // Every attempt is now either parked in `add_socket` or already rejected.
    tokio::time::timeout(Duration::from_secs(5), async {
        while adapter.entered.load(Ordering::SeqCst)
            + tasks.iter().filter(|task| task.is_finished()).count()
            < ATTEMPTS
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("attempts never reached admission");

    // Release every parked registration at once: without a reservation all of
    // them would register, with one only the holders of a slot can.
    let parked = adapter.entered.load(Ordering::SeqCst);
    gate.add_permits(ATTEMPTS);
    tokio::time::timeout(Duration::from_secs(5), async {
        while adapter.registered.load(Ordering::SeqCst) < parked
            || metrics.new_connections() < parked
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("admissions never settled");

    let admitted = metrics.new_connections();
    let registered = adapter.registered.load(Ordering::SeqCst);
    for task in tasks {
        task.abort();
    }
    (admitted, registered)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_admissions_never_exceed_node_capacity() {
    let (admitted, registered) = gated_admissions(4, 0).await;

    assert_eq!(
        admitted, 4,
        "concurrent upgrades must not overshoot max_connections"
    );
    assert_eq!(registered, 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_admissions_never_exceed_app_quota() {
    let (admitted, registered) = gated_admissions(0, 4).await;

    assert_eq!(
        admitted, 4,
        "concurrent upgrades must not overshoot the per-app quota"
    );
    assert_eq!(registered, 4);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn memory_pressure_rejects_new_connection_without_marking_it_connected() {
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(make_app(0)).await.unwrap();

    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;
    let metrics = Arc::new(CountingMetrics::new());
    let mut options = ServerOptions::default();
    options.http_api.accept_traffic.enabled = true;
    options.http_api.accept_traffic.memory_limit_bytes = Some(1);

    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        options,
    )
    .local_adapter(adapter.clone())
    .metrics(metrics.clone())
    .build();

    assert!(handler.is_memory_pressure_shedding());
    let established_socket_id = SocketId::new();
    let (established_writer, _established_client) = make_ws_pair().await;
    adapter
        .add_socket(
            established_socket_id,
            established_writer,
            APP_ID,
            app_manager as Arc<dyn AppManager + Send + Sync>,
            WebSocketBufferConfig::default(),
            ProtocolVersion::V1,
            WireFormat::Json,
            true,
            sockudo_protocol::AppendMode::Delta,
        )
        .await
        .unwrap();

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

    assert!(matches!(result, Err(Error::OverCapacity)));
    assert_eq!(metrics.memory_pressure_rejections(), 1);
    assert_eq!(metrics.new_connections(), 0);
    assert_eq!(
        adapter.get_total_sockets_count(),
        1,
        "memory-pressure admission must leave established connections open"
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

#[tokio::test]
async fn capacity_readiness_recovers_before_disconnect_cleanup() {
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(make_app(0)).await.unwrap();
    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;
    let (tx, rx) = crossfire::mpsc::bounded_async(10);
    let handler = Arc::new(
        ConnectionHandler::builder(
            app_manager,
            adapter.clone(),
            Arc::new(NullCacheManager),
            ServerOptions {
                max_connections: 1,
                ..Default::default()
            },
        )
        .local_adapter(adapter.clone())
        .cleanup_queue(sockudo_adapter::cleanup::CleanupSender::Direct(tx))
        .build(),
    );
    let (server_ws, mut client) = make_full_ws_pair().await;
    let socket_handler = handler.clone();
    let socket_task = tokio::spawn(async move {
        socket_handler
            .handle_socket(
                server_ws,
                APP_KEY.into(),
                None,
                ProtocolVersion::V1,
                WireFormat::Json,
                true,
                sockudo_protocol::AppendMode::Delta,
                None,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!handler.is_capacity_ready());
    assert!(handler.is_accepting(), "capacity must not change /up");
    drop(client);
    tokio::time::timeout(Duration::from_secs(2), socket_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(handler.is_capacity_ready());
    assert_eq!(adapter.get_total_sockets_count(), 1, "cleanup has not run");
    assert!(
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .is_ok()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capacity_rejection_frames_use_the_negotiated_wire_format() {
    for (format, expect_binary) in [
        (WireFormat::Json, false),
        (WireFormat::MessagePack, true),
        (WireFormat::Protobuf, true),
    ] {
        let (handler, metrics) = capped_handler(1, 0).await;
        let (server_ws, _client) = make_full_ws_pair().await;
        let admitted = spawn_socket(&handler, server_ws);
        wait_for(|| metrics.new_connections() == 1).await;

        let (server_ws, mut client) = make_full_ws_pair().await;
        let result = handler
            .handle_socket(
                server_ws,
                APP_KEY.to_string(),
                None,
                ProtocolVersion::V2,
                format,
                true,
                sockudo_protocol::AppendMode::Delta,
                None,
            )
            .await;
        assert!(
            matches!(result, Err(Error::OverCapacity)),
            "{format:?}: expected OverCapacity, got {result:?}"
        );
        assert_eq!(
            *metrics.admission_rejections.lock().unwrap(),
            [WsAdmissionRejection::NodeCapacity]
        );

        let (binary, message) = decode_error(next_frame(&mut client).await, format);
        assert_eq!(binary, expect_binary, "{format:?} rejection frame type");
        assert_eq!(
            message.event.as_deref(),
            Some("pusher:error"),
            "{format:?} rejection event"
        );
        assert_eq!(
            error_code(&message),
            Some(4100),
            "{format:?} rejection code"
        );
        match next_frame(&mut client).await {
            Message::Close(Some(reason)) => {
                assert_eq!(reason.code, 4100, "{format:?} close code")
            }
            other => panic!("{format:?}: expected a close frame, got {other:?}"),
        }

        admitted.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_quota_rejection_sends_4004_frame() {
    let (handler, metrics) = capped_handler(0, 1).await;
    let (server_ws, _client) = make_full_ws_pair().await;
    let admitted = spawn_socket(&handler, server_ws);
    wait_for(|| metrics.new_connections() == 1).await;

    let (server_ws, mut client) = make_full_ws_pair().await;
    let result = handler
        .handle_socket(
            server_ws,
            APP_KEY.to_string(),
            None,
            ProtocolVersion::V2,
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
        *metrics.admission_rejections.lock().unwrap(),
        [WsAdmissionRejection::AppQuota]
    );

    let (_, message) = decode_error(next_frame(&mut client).await, WireFormat::Json);
    assert_eq!(error_code(&message), Some(4004));

    admitted.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_quota_counts_peer_nodes_on_a_clustered_adapter() {
    // A peer already holds the whole quota: this node must reject even though
    // its local counter is empty.
    let (handler, metrics) = cluster_handler(1, 1).await;
    let (server_ws, mut client) = make_full_ws_pair().await;
    let result = handler
        .handle_socket(
            server_ws,
            APP_KEY.to_string(),
            None,
            ProtocolVersion::V2,
            WireFormat::Json,
            true,
            sockudo_protocol::AppendMode::Delta,
            None,
        )
        .await;
    assert!(
        matches!(result, Err(Error::OverConnectionQuota)),
        "peer nodes count toward the app quota: {result:?}"
    );
    assert_eq!(
        *metrics.admission_rejections.lock().unwrap(),
        [WsAdmissionRejection::AppQuota]
    );
    assert_eq!(metrics.new_connections(), 0);
    let (_, message) = decode_error(next_frame(&mut client).await, WireFormat::Json);
    assert_eq!(error_code(&message), Some(4004));

    // No peer sockets: the same admission succeeds.
    let (handler, metrics) = cluster_handler(0, 1).await;
    let (server_ws, _client) = make_full_ws_pair().await;
    let admitted = spawn_socket(&handler, server_ws);
    wait_for(|| metrics.new_connections() == 1).await;
    admitted.abort();
}

struct AppRemovedDuringRegistration(LocalAdapter);

#[async_trait]
impl NoopConnectionManager for AppRemovedDuringRegistration {
    async fn add_socket(
        &self,
        socket_id: SocketId,
        socket: WebSocketWriter,
        app_id: &str,
        app_manager: Arc<dyn AppManager + Send + Sync>,
        buffer_config: WebSocketBufferConfig,
        protocol_version: ProtocolVersion,
        wire_format: WireFormat,
        echo_messages: bool,
        append_mode: sockudo_protocol::AppendMode,
    ) -> Result<()> {
        app_manager.delete_app(app_id).await?;
        self.0
            .add_socket(
                socket_id,
                socket,
                app_id,
                app_manager,
                buffer_config,
                protocol_version,
                wire_format,
                echo_messages,
                append_mode,
            )
            .await
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

delegate_connection_manager!(AppRemovedDuringRegistration);

#[tokio::test]
async fn app_not_found_is_counted_once_at_each_admission_stage() {
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(make_app(0)).await.unwrap();
    let metrics = Arc::new(CountingMetrics::new());
    let handler = ConnectionHandler::builder(
        app_manager,
        Arc::new(AppRemovedDuringRegistration(LocalAdapter::new())),
        Arc::new(NullCacheManager),
        ServerOptions::default(),
    )
    .metrics(metrics.clone())
    .build();

    // The initial lookup succeeds, then the adapter removes the app before registration.
    let (server_ws, _client) = make_full_ws_pair().await;
    let result = spawn_socket(&handler, server_ws).await.unwrap();
    assert!(matches!(result, Err(Error::ApplicationNotFound)));
    assert_eq!(
        *metrics.admission_rejections.lock().unwrap(),
        [WsAdmissionRejection::AppNotFound]
    );
    assert_eq!(metrics.connection_errors.load(Ordering::SeqCst), 0);

    // The next attempt fails at the initial lookup instead.
    let (server_ws, _client) = make_full_ws_pair().await;
    let result = spawn_socket(&handler, server_ws).await.unwrap();
    assert!(matches!(result, Err(Error::ApplicationNotFound)));
    assert_eq!(
        *metrics.admission_rejections.lock().unwrap(),
        [WsAdmissionRejection::AppNotFound; 2]
    );
    assert_eq!(metrics.connection_errors.load(Ordering::SeqCst), 1);
    assert_eq!(metrics.new_connections(), 0);
}

#[tokio::test]
async fn peer_close_is_counted_once_after_establishment() {
    let (handler, metrics) = capped_handler(1, 0).await;
    let (server_ws, mut client) = make_full_ws_pair().await;
    let session = spawn_socket(&handler, server_ws);
    assert!(matches!(next_frame(&mut client).await, Message::Text(_)));
    let socket = handler
        .connection_manager()
        .get_namespaces()
        .await
        .unwrap()
        .into_iter()
        .find(|(app_id, _)| app_id == APP_ID)
        .and_then(|(_, namespace)| namespace.sockets.iter().next().map(|entry| entry.clone()))
        .unwrap();
    let mut socket_guard = socket.inner.lock().await;
    client.close(1000, "done").await.unwrap();
    assert!(matches!(next_frame(&mut client).await, Message::Close(_)));
    wait_for(|| !metrics.session_ends.lock().unwrap().is_empty()).await;
    assert_eq!(
        *metrics.session_ends.lock().unwrap(),
        [DisconnectCause::ClientClose],
        "the session must be counted while disconnect cleanup is blocked"
    );
    // Cleanup cannot finish while the lock is held. An administrative close
    // recording its intent now must not replace the already observed peer Close.
    let result = socket_guard
        .close_with_outcome(
            4009,
            "app disabled".to_string(),
            DisconnectCause::AdministrativeClose,
        )
        .await;
    assert!(matches!(result, Err(Error::ConnectionClosed(_))));
    drop(socket_guard);
    tokio::time::timeout(Duration::from_secs(2), session)
        .await
        .expect("peer close must terminate the session")
        .unwrap()
        .unwrap();

    assert_eq!(
        *metrics.session_ends.lock().unwrap(),
        [DisconnectCause::ClientClose]
    );
}
