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
use sockudo_core::websocket::{DisconnectCause, SocketId, WebSocketBufferConfig};
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

fn dummy_disconnect_task() -> DisconnectTask {
    DisconnectTask {
        socket_id: SocketId::new(),
        app_id: APP_ID.to_string(),
        subscribed_channels: vec![],
        user_id: None,
        cause: DisconnectCause::Unknown,
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

async fn add_v1_socket(
    adapter: &Arc<LocalAdapter>,
    app_manager: &Arc<MemoryAppManager>,
) -> (SocketId, WebSocketStream<WsStream<Http1>>) {
    let socket_id = SocketId::new();
    let (writer, client) = make_ws_pair().await;
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
    (socket_id, client)
}

/// Regression: the activity-timeout task ran `handle_disconnect` inline. That marks the connection
/// `disconnecting` and then aborts the activity task itself (`clear_activity_timeout`); the next
/// contended lock made the aborted task drop the cleanup half-done (nothing queued, no
/// `mark_disconnection`), and the reader's later cleanup returned early on `disconnecting`.
/// The connection stayed in the adapter and its presence channels forever.
#[tokio::test(flavor = "current_thread")]
async fn activity_timeout_disconnect_survives_abort_of_its_own_task() {
    let (tx, rx) = mpsc::bounded_async::<DisconnectTask>(10);
    let metrics = Arc::new(CountingMetrics::new());
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(make_app()).await.unwrap();
    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;
    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        ServerOptions {
            activity_timeout: 1,
            ..ServerOptions::default()
        },
    )
    .local_adapter(adapter.clone())
    .cleanup_queue(CleanupSender::Direct(tx))
    .metrics(metrics.clone() as Arc<dyn MetricsInterface + Send + Sync>)
    .build();

    let (socket_id, _client) = add_v1_socket(&adapter, &app_manager).await;
    let conn = adapter.get_connection(&socket_id, APP_ID).await.unwrap();
    // The socket is already closed and idle, so the activity task takes its "closed connection" branch.
    {
        let mut ws = conn.inner.lock().await;
        ws.state.status = sockudo_core::websocket::ConnectionStatus::Closed;
        ws.state.last_ping = Instant::now() - Duration::from_secs(60);
    }
    handler
        .set_activity_timeout(APP_ID, &socket_id)
        .await
        .unwrap();

    // Another task keeps queueing for the connection lock (tokio's Mutex is FIFO), so every lock
    // the activity task requests after releasing one is pending.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let contender = {
        let conn = conn.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::SeqCst) {
                let guard = conn.inner.lock().await;
                tokio::task::yield_now().await;
                drop(guard);
            }
        })
    };
    // The activity task (first check after 1 s) must hand the disconnect to the cleanup queue
    // itself; the aborted inline version never got there.
    let task = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("activity timeout must queue the disconnect")
        .expect("cleanup queue must remain open");
    assert_eq!(task.socket_id, socket_id);
    stop.store(true, Ordering::SeqCst);
    contender.await.unwrap();

    // The reader's cleanup once the socket is gone (cleanup_socket) must not count it twice.
    handler
        .handle_ungraceful_disconnect(APP_ID, &socket_id)
        .await
        .unwrap();
    assert!(
        rx.try_recv().is_err(),
        "disconnect must be queued only once"
    );
    assert_eq!(
        metrics.disconnections(),
        1,
        "connection must be counted as gone once"
    );
    assert!(conn.cancellation_token().is_cancelled());
}

/// Regression: without the async queue, a disconnect that found the connection lock busy treated
/// it as "already disconnecting" and returned without any cleanup.
#[tokio::test(flavor = "current_thread")]
async fn sync_disconnect_waits_for_busy_connection_lock() {
    let metrics = Arc::new(CountingMetrics::new());
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(make_app()).await.unwrap();
    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;
    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        ServerOptions::default(),
    )
    .local_adapter(adapter.clone())
    .metrics(metrics.clone() as Arc<dyn MetricsInterface + Send + Sync>)
    .build();

    let (socket_id, _client) = add_v1_socket(&adapter, &app_manager).await;
    let conn = adapter.get_connection(&socket_id, APP_ID).await.unwrap();

    let guard = conn.inner.lock().await;
    let disconnect = handler.handle_disconnect(APP_ID, &socket_id);
    tokio::pin!(disconnect);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut disconnect)
            .await
            .is_err(),
        "disconnect must wait for the busy connection lock, not skip the cleanup"
    );
    drop(guard);
    disconnect.await.unwrap();

    assert!(
        adapter.get_connection(&socket_id, APP_ID).await.is_none(),
        "connection must be removed"
    );
    assert_eq!(metrics.disconnections(), 1);
}

/// Regression: a client that stopped reading blocks the writer in a socket write; a `close()`
/// queued behind it waits for its frame while holding the connection lock. When the socket then
/// died, the writer exited without telling anyone, so `close()` and everything queued on the lock
/// (the reader's cleanup, any disconnect) waited forever.
#[tokio::test]
async fn close_returns_when_writer_dies_before_flushing_it() {
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(make_app()).await.unwrap();
    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;
    let (socket_id, client) = add_v1_socket(&adapter, &app_manager).await;
    let conn = adapter.get_connection(&socket_id, APP_ID).await.unwrap();

    // The client never reads: fill the socket buffers until the writer is stuck in a write.
    {
        let ws = conn.inner.lock().await;
        for _ in 0..400 {
            ws.send_text("x".repeat(64 * 1024)).unwrap();
        }
    }
    let closing = {
        let conn = conn.clone();
        tokio::spawn(async move {
            let mut ws = conn.inner.lock().await;
            ws.close(4201, "Pong reply not received in time".to_string())
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !closing.is_finished(),
        "close() must be waiting behind the stuck writer"
    );

    drop(client);

    let closed = tokio::time::timeout(Duration::from_secs(5), closing)
        .await
        .expect("close() must return once the writer is gone")
        .unwrap();
    assert!(
        matches!(closed, Err(sockudo_core::error::Error::ConnectionClosed(_))),
        "close() must report the unflushed close frame, got {closed:?}"
    );
    let _guard = tokio::time::timeout(Duration::from_secs(1), conn.inner.lock())
        .await
        .expect("connection lock must be free again");
    assert!(conn.cancellation_token().is_cancelled());
}

/// Queues for the connection lock in a loop (tokio's Mutex is FIFO, so every lock another task
/// requests is pending) and records when the connection gets marked `disconnecting`.
fn contend_for_connection_lock(
    conn: sockudo_core::websocket::WebSocketRef,
) -> (
    Arc<std::sync::atomic::AtomicBool>,
    Arc<std::sync::atomic::AtomicBool>,
    tokio::task::JoinHandle<()>,
) {
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let marked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let contender = {
        let stop = stop.clone();
        let marked = marked.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::SeqCst) {
                let guard = conn.inner.lock().await;
                if guard.state.disconnecting {
                    marked.store(true, Ordering::SeqCst);
                }
                tokio::task::yield_now().await;
                drop(guard);
            }
        })
    };
    (stop, marked, contender)
}

/// Starts `handle_disconnect` in a task and aborts that task once the connection is marked
/// `disconnecting` and the cleanup is parked on the contended lock.
async fn cancel_disconnect_caller_mid_cleanup(
    handler: &ConnectionHandler,
    conn: &sockudo_core::websocket::WebSocketRef,
    socket_id: SocketId,
) {
    let (stop, marked, contender) = contend_for_connection_lock(conn.clone());
    let caller = {
        let handler = handler.clone();
        tokio::spawn(async move { handler.handle_disconnect(APP_ID, &socket_id).await })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while !marked.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("disconnect must mark the connection");
    caller.abort();
    let _ = caller.await;
    stop.store(true, Ordering::SeqCst);
    contender.await.unwrap();
}

/// Regression: cleanup marks the connection `disconnecting` before it awaits anything, so a caller
/// cancelled after that point (an aborted timeout task, a dropped future) left the connection in
/// the adapter for good: every later disconnect returned early on `disconnecting`.
#[tokio::test(flavor = "current_thread")]
async fn async_disconnect_completes_when_its_caller_is_cancelled() {
    let (tx, rx) = mpsc::bounded_async::<DisconnectTask>(10);
    let metrics = Arc::new(CountingMetrics::new());
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(make_app()).await.unwrap();
    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;
    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        ServerOptions::default(),
    )
    .local_adapter(adapter.clone())
    .cleanup_queue(CleanupSender::Direct(tx))
    .metrics(metrics.clone() as Arc<dyn MetricsInterface + Send + Sync>)
    .build();

    let (socket_id, _client) = add_v1_socket(&adapter, &app_manager).await;
    let conn = adapter.get_connection(&socket_id, APP_ID).await.unwrap();
    cancel_disconnect_caller_mid_cleanup(&handler, &conn, socket_id).await;

    let task = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("cleanup must finish after its caller is cancelled")
        .expect("cleanup queue must remain open");
    assert_eq!(task.socket_id, socket_id);
    assert_eq!(metrics.disconnections(), 1);
    assert!(conn.cancellation_token().is_cancelled());
}

/// Same as above for the synchronous cleanup path (no cleanup queue).
#[tokio::test(flavor = "current_thread")]
async fn sync_disconnect_completes_when_its_caller_is_cancelled() {
    let metrics = Arc::new(CountingMetrics::new());
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(make_app()).await.unwrap();
    let adapter = Arc::new(LocalAdapter::new());
    adapter.init().await;
    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(NullCacheManager),
        ServerOptions::default(),
    )
    .local_adapter(adapter.clone())
    .metrics(metrics.clone() as Arc<dyn MetricsInterface + Send + Sync>)
    .build();

    let (socket_id, _client) = add_v1_socket(&adapter, &app_manager).await;
    let conn = adapter.get_connection(&socket_id, APP_ID).await.unwrap();
    cancel_disconnect_caller_mid_cleanup(&handler, &conn, socket_id).await;

    tokio::time::timeout(Duration::from_secs(5), async {
        while adapter.get_connection(&socket_id, APP_ID).await.is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cleanup must remove the connection after its caller is cancelled");
    assert_eq!(metrics.disconnections(), 1);
}
