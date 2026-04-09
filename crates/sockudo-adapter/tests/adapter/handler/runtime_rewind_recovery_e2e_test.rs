use crate::mocks::connection_handler_mock::{MockCacheManager, MockMetricsInterface};
use async_trait::async_trait;
use bytes::Bytes;
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::handler::{
    ConnectionHandler,
    types::{SubscriptionRequest, SubscriptionRewind},
};
use sockudo_adapter::local_adapter::LocalAdapter;
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_core::app::{App, AppManager};
use sockudo_core::history::{
    HistoryAppendRecord, HistoryPage, HistoryReadRequest, HistoryRetentionPolicy,
    HistoryRuntimeStatus, HistoryStore, HistoryStreamRuntimeState, HistoryWriteReservation,
    MemoryHistoryStore, MemoryHistoryStoreConfig,
};
use sockudo_core::options::ServerOptions;
use sockudo_core::websocket::{SocketId, WebSocketBufferConfig};
use sockudo_protocol::messages::{MessageData, PusherMessage};
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::{WebSocket, WebSocketWriter};
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{
    Config as WsConfig, Http1, Message as WsMessage, SplitReader, Stream as WsStream,
    WebSocketStream,
};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, oneshot};
use tokio::time::{Duration, timeout};

type ClientReader = SplitReader<WsStream<Http1>>;

#[derive(Clone)]
struct GateHistoryStore {
    inner: Arc<MemoryHistoryStore>,
    gate: Arc<Mutex<Option<ReadGate>>>,
}

struct ReadGate {
    started_tx: Option<oneshot::Sender<()>>,
    continue_rx: oneshot::Receiver<()>,
}

impl GateHistoryStore {
    fn new(inner: Arc<MemoryHistoryStore>) -> Self {
        Self {
            inner,
            gate: Arc::new(Mutex::new(None)),
        }
    }

    async fn arm_gate(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (started_tx, started_rx) = oneshot::channel();
        let (continue_tx, continue_rx) = oneshot::channel();
        *self.gate.lock().await = Some(ReadGate {
            started_tx: Some(started_tx),
            continue_rx,
        });
        (started_rx, continue_tx)
    }
}

#[async_trait]
impl HistoryStore for GateHistoryStore {
    async fn reserve_publish_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> sockudo_core::error::Result<HistoryWriteReservation> {
        self.inner.reserve_publish_position(app_id, channel).await
    }

    async fn append(&self, record: HistoryAppendRecord) -> sockudo_core::error::Result<()> {
        self.inner.append(record).await
    }

    async fn read_page(
        &self,
        request: HistoryReadRequest,
    ) -> sockudo_core::error::Result<HistoryPage> {
        let page = self.inner.read_page(request).await?;
        if let Some(mut gate) = self.gate.lock().await.take() {
            if let Some(started_tx) = gate.started_tx.take() {
                let _ = started_tx.send(());
            }
            let _ = gate.continue_rx.await;
        }
        Ok(page)
    }

    async fn runtime_status(&self) -> sockudo_core::error::Result<HistoryRuntimeStatus> {
        self.inner.runtime_status().await
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> sockudo_core::error::Result<HistoryStreamRuntimeState> {
        self.inner.stream_runtime_state(app_id, channel).await
    }
}

#[derive(Clone)]
struct RejectingAppendHistoryStore {
    inner: Arc<MemoryHistoryStore>,
    error_message: &'static str,
}

#[async_trait]
impl HistoryStore for RejectingAppendHistoryStore {
    async fn reserve_publish_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> sockudo_core::error::Result<HistoryWriteReservation> {
        self.inner.reserve_publish_position(app_id, channel).await
    }

    async fn append(&self, _record: HistoryAppendRecord) -> sockudo_core::error::Result<()> {
        Err(sockudo_core::error::Error::Internal(
            self.error_message.to_string(),
        ))
    }

    async fn read_page(
        &self,
        request: HistoryReadRequest,
    ) -> sockudo_core::error::Result<HistoryPage> {
        self.inner.read_page(request).await
    }

    async fn runtime_status(&self) -> sockudo_core::error::Result<HistoryRuntimeStatus> {
        self.inner.runtime_status().await
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> sockudo_core::error::Result<HistoryStreamRuntimeState> {
        self.inner.stream_runtime_state(app_id, channel).await
    }
}

struct TestHarness {
    handler: ConnectionHandler,
    app: App,
    app_manager: Arc<MemoryAppManager>,
    adapter: Arc<LocalAdapter>,
    history_store: Arc<GateHistoryStore>,
}

async fn build_harness(options: ServerOptions) -> TestHarness {
    let app_manager = Arc::new(MemoryAppManager::new());
    let app = App::from_policy(
        "app".to_string(),
        "key".to_string(),
        "secret".to_string(),
        true,
        Default::default(),
    );
    app_manager.create_app(app.clone()).await.unwrap();

    let adapter = Arc::new(LocalAdapter::new());
    let history_inner = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
    let history_store = Arc::new(GateHistoryStore::new(history_inner));

    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(MockCacheManager::new()),
        options,
    )
    .local_adapter(adapter.clone())
    .history_store(history_store.clone() as Arc<dyn HistoryStore + Send + Sync>)
    .metrics(Arc::new(MockMetricsInterface::new()))
    .build();

    TestHarness {
        handler,
        app,
        app_manager,
        adapter,
        history_store,
    }
}

async fn build_harness_with_store(
    options: ServerOptions,
    history_store: Arc<dyn HistoryStore + Send + Sync>,
) -> (
    ConnectionHandler,
    App,
    Arc<MemoryAppManager>,
    Arc<LocalAdapter>,
) {
    let app_manager = Arc::new(MemoryAppManager::new());
    let app = App::from_policy(
        "app".to_string(),
        "key".to_string(),
        "secret".to_string(),
        true,
        Default::default(),
    );
    app_manager.create_app(app.clone()).await.unwrap();

    let adapter = Arc::new(LocalAdapter::new());
    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(MockCacheManager::new()),
        options,
    )
    .local_adapter(adapter.clone())
    .history_store(history_store)
    .metrics(Arc::new(MockMetricsInterface::new()))
    .build();

    (handler, app, app_manager, adapter)
}

async fn connect_v2_socket(harness: &TestHarness) -> (SocketId, ClientReader) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = sockudo_ws::handshake::server_handshake(&mut stream)
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
    let (reader, _writer) = client_ws.split();
    let server_writer: WebSocketWriter = server_task.await.unwrap();

    let socket_id = SocketId::new();
    harness
        .adapter
        .add_socket(
            socket_id,
            server_writer,
            &harness.app.id,
            harness.app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
            WebSocketBufferConfig::default(),
            ProtocolVersion::V2,
            WireFormat::Json,
            true,
        )
        .await
        .unwrap();

    (socket_id, reader)
}

fn retention() -> HistoryRetentionPolicy {
    HistoryRetentionPolicy {
        retention_window_seconds: 3600,
        max_messages_per_channel: None,
        max_bytes_per_channel: None,
    }
}

async fn append_history_message(
    store: &Arc<GateHistoryStore>,
    app_id: &str,
    channel: &str,
    stream_id: &str,
    serial: u64,
    message_id: &str,
    event: &str,
) {
    let payload = sonic_rs::to_vec(&PusherMessage {
        event: Some(event.to_string()),
        channel: Some(channel.to_string()),
        data: Some(MessageData::Json(sonic_rs::json!({ "event": event }))),
        name: None,
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: Some(message_id.to_string()),
        stream_id: Some(stream_id.to_string()),
        serial: Some(serial),
        idempotency_key: None,
        extras: None,
        delta_sequence: None,
        delta_conflation_key: None,
    })
    .unwrap();

    store
        .append(HistoryAppendRecord {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: stream_id.to_string(),
            serial,
            published_at_ms: sockudo_core::history::now_ms() + serial as i64,
            message_id: Some(message_id.to_string()),
            event_name: Some(event.to_string()),
            operation_kind: "append".to_string(),
            payload_bytes: Bytes::from(payload),
            retention: retention(),
        })
        .await
        .unwrap();
}

fn live_message(channel: &str, event: &str, message_id: &str) -> PusherMessage {
    PusherMessage {
        event: Some(event.to_string()),
        channel: Some(channel.to_string()),
        data: Some(MessageData::Json(sonic_rs::json!({ "event": event }))),
        name: None,
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: Some(message_id.to_string()),
        stream_id: None,
        serial: None,
        idempotency_key: None,
        extras: None,
        delta_sequence: None,
        delta_conflation_key: None,
    }
}

async fn recv_message(reader: &mut ClientReader) -> PusherMessage {
    let next = timeout(Duration::from_secs(2), reader.next())
        .await
        .expect("timed out waiting for websocket message")
        .expect("websocket stream ended")
        .expect("websocket receive failed");

    match next {
        WsMessage::Text(text) => {
            sonic_rs::from_str(&String::from_utf8_lossy(text.as_ref())).unwrap()
        }
        WsMessage::Binary(bytes) => sonic_rs::from_slice(&bytes).unwrap(),
        other => panic!("unexpected websocket message: {other:?}"),
    }
}

async fn recv_until_event(reader: &mut ClientReader, event_name: &str) -> Vec<PusherMessage> {
    let mut messages = Vec::new();
    loop {
        let message = recv_message(reader).await;
        let matches = message.event.as_deref() == Some(event_name);
        messages.push(message);
        if matches {
            return messages;
        }
    }
}

async fn expect_no_message(reader: &mut ClientReader, wait_for: Duration) {
    let result = timeout(wait_for, reader.next()).await;
    match result {
        Err(_) => {}
        Ok(None) => {}
        Ok(Some(Ok(frame))) => {
            panic!("unexpected websocket message after rejected publish: {frame:?}")
        }
        Ok(Some(Err(err))) => panic!("unexpected websocket read error: {err:?}"),
    }
}

#[tokio::test]
async fn rewind_handoff_has_no_gap_and_suppresses_duplicates_e2e() {
    let mut options = ServerOptions::default();
    options.history.enabled = true;
    options.history.max_page_size = 100;
    let harness = build_harness(options).await;

    append_history_message(
        &harness.history_store,
        &harness.app.id,
        "chat",
        "stream-1",
        1,
        "dup-msg",
        "history-1",
    )
    .await;
    append_history_message(
        &harness.history_store,
        &harness.app.id,
        "chat",
        "stream-1",
        2,
        "hist-2",
        "history-2",
    )
    .await;

    let (socket_id, mut reader) = connect_v2_socket(&harness).await;
    let request = SubscriptionRequest {
        channel: "chat".to_string(),
        auth: None,
        channel_data: None,
        #[cfg(feature = "tag-filtering")]
        tags_filter: None,
        #[cfg(feature = "delta")]
        delta: None,
        rewind: Some(SubscriptionRewind::Count(2)),
        event_name_filter: None,
    };

    let (started_rx, continue_tx) = harness.history_store.arm_gate().await;
    let handler = harness.handler.clone();
    let app = harness.app.clone();
    let subscribe_task = tokio::spawn(async move {
        handler
            .handle_subscribe_request(&socket_id, &app, request)
            .await
            .unwrap();
    });

    started_rx.await.unwrap();
    harness
        .handler
        .broadcast_to_channel(
            &harness.app,
            "chat",
            live_message("chat", "live-dup", "dup-msg"),
            None,
        )
        .await
        .unwrap();
    harness
        .handler
        .broadcast_to_channel(
            &harness.app,
            "chat",
            live_message("chat", "live-unique", "live-1"),
            None,
        )
        .await
        .unwrap();
    let _ = continue_tx.send(());
    subscribe_task.await.unwrap();

    let messages = recv_until_event(&mut reader, "sockudo:rewind_complete").await;
    let ordered_events: Vec<String> = messages
        .iter()
        .filter_map(|msg| msg.event.clone())
        .filter(|name| {
            name == "history-1"
                || name == "history-2"
                || name == "live-unique"
                || name == "sockudo:rewind_complete"
        })
        .collect();

    assert_eq!(
        ordered_events,
        vec![
            "history-1".to_string(),
            "history-2".to_string(),
            "live-unique".to_string(),
            "sockudo:rewind_complete".to_string()
        ]
    );
    assert!(
        !messages
            .iter()
            .any(|msg| msg.event.as_deref() == Some("live-dup"))
    );
}

#[tokio::test]
async fn hot_recovery_replays_real_deliveries_e2e() {
    let mut options = ServerOptions::default();
    options.history.enabled = true;
    options.connection_recovery.enabled = true;
    options.history.max_page_size = 100;
    let harness = build_harness(options).await;

    let (source_socket_id, mut source_reader) = connect_v2_socket(&harness).await;
    let subscribe = SubscriptionRequest {
        channel: "hot".to_string(),
        auth: None,
        channel_data: None,
        #[cfg(feature = "tag-filtering")]
        tags_filter: None,
        #[cfg(feature = "delta")]
        delta: None,
        rewind: None,
        event_name_filter: None,
    };
    harness
        .handler
        .handle_subscribe_request(&source_socket_id, &harness.app, subscribe)
        .await
        .unwrap();
    let _ = recv_message(&mut source_reader).await;

    for (event, message_id) in [
        ("hot-1", "hot-msg-1"),
        ("hot-2", "hot-msg-2"),
        ("hot-3", "hot-msg-3"),
    ] {
        harness
            .handler
            .broadcast_to_channel(
                &harness.app,
                "hot",
                live_message("hot", event, message_id),
                None,
            )
            .await
            .unwrap();
    }

    let first = recv_message(&mut source_reader).await;
    let _second = recv_message(&mut source_reader).await;
    let _third = recv_message(&mut source_reader).await;

    let position = sonic_rs::json!({
        "channel_positions": {
            "hot": {
                "stream_id": first.stream_id,
                "serial": first.serial,
                "last_message_id": first.message_id,
            }
        }
    });

    let (resume_socket_id, mut resume_reader) = connect_v2_socket(&harness).await;
    harness
        .handler
        .handle_resume(
            &resume_socket_id,
            &harness.app,
            &PusherMessage {
                event: Some("sockudo:resume".to_string()),
                channel: None,
                data: Some(MessageData::Json(position)),
                name: None,
                user_id: None,
                tags: None,
                sequence: None,
                conflation_key: None,
                message_id: None,
                stream_id: None,
                serial: None,
                idempotency_key: None,
                extras: None,
                delta_sequence: None,
                delta_conflation_key: None,
            },
        )
        .await
        .unwrap();

    let messages = recv_until_event(&mut resume_reader, "sockudo:resume_success").await;
    let replayed_ids: Vec<String> = messages
        .iter()
        .filter(|msg| matches!(msg.event.as_deref(), Some("hot-2" | "hot-3")))
        .filter_map(|msg| msg.message_id.clone())
        .collect();
    assert_eq!(
        replayed_ids,
        vec!["hot-msg-2".to_string(), "hot-msg-3".to_string()]
    );
}

#[tokio::test]
async fn cold_recovery_replays_real_deliveries_e2e() {
    let mut options_a = ServerOptions::default();
    options_a.history.enabled = true;
    options_a.connection_recovery.enabled = true;
    options_a.history.max_page_size = 100;
    let harness_a = build_harness(options_a.clone()).await;

    let (source_socket_id, mut source_reader) = connect_v2_socket(&harness_a).await;
    harness_a
        .handler
        .handle_subscribe_request(
            &source_socket_id,
            &harness_a.app,
            SubscriptionRequest {
                channel: "cold".to_string(),
                auth: None,
                channel_data: None,
                #[cfg(feature = "tag-filtering")]
                tags_filter: None,
                #[cfg(feature = "delta")]
                delta: None,
                rewind: None,
                event_name_filter: None,
            },
        )
        .await
        .unwrap();
    let _ = recv_message(&mut source_reader).await;

    for (event, message_id) in [
        ("cold-1", "cold-msg-1"),
        ("cold-2", "cold-msg-2"),
        ("cold-3", "cold-msg-3"),
    ] {
        harness_a
            .handler
            .broadcast_to_channel(
                &harness_a.app,
                "cold",
                live_message("cold", event, message_id),
                None,
            )
            .await
            .unwrap();
    }
    let first = recv_message(&mut source_reader).await;
    let _second = recv_message(&mut source_reader).await;
    let _third = recv_message(&mut source_reader).await;

    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(harness_a.app.clone()).await.unwrap();
    let adapter = Arc::new(LocalAdapter::new());
    let handler_b = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(MockCacheManager::new()),
        options_a,
    )
    .local_adapter(adapter.clone())
    .history_store(harness_a.history_store.clone() as Arc<dyn HistoryStore + Send + Sync>)
    .metrics(Arc::new(MockMetricsInterface::new()))
    .build();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = sockudo_ws::handshake::server_handshake(&mut stream)
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
    let (mut resume_reader, _writer) = client_ws.split();
    let resume_socket_id = SocketId::new();
    adapter
        .add_socket(
            resume_socket_id,
            server_task.await.unwrap(),
            &harness_a.app.id,
            app_manager as Arc<dyn AppManager + Send + Sync>,
            WebSocketBufferConfig::default(),
            ProtocolVersion::V2,
            WireFormat::Json,
            true,
        )
        .await
        .unwrap();

    let resume_payload = sonic_rs::json!({
        "channel_positions": {
            "cold": {
                "stream_id": first.stream_id,
                "serial": first.serial,
                "last_message_id": first.message_id,
            }
        }
    });
    handler_b
        .handle_resume(
            &resume_socket_id,
            &harness_a.app,
            &PusherMessage {
                event: Some("sockudo:resume".to_string()),
                channel: None,
                data: Some(MessageData::Json(resume_payload)),
                name: None,
                user_id: None,
                tags: None,
                sequence: None,
                conflation_key: None,
                message_id: None,
                stream_id: None,
                serial: None,
                idempotency_key: None,
                extras: None,
                delta_sequence: None,
                delta_conflation_key: None,
            },
        )
        .await
        .unwrap();

    let messages = recv_until_event(&mut resume_reader, "sockudo:resume_success").await;
    let replayed_ids: Vec<String> = messages
        .iter()
        .filter(|msg| matches!(msg.event.as_deref(), Some("cold-2" | "cold-3")))
        .filter_map(|msg| msg.message_id.clone())
        .collect();
    assert_eq!(
        replayed_ids,
        vec!["cold-msg-2".to_string(), "cold-msg-3".to_string()]
    );
}

#[tokio::test]
async fn partial_per_channel_recovery_results_surface_real_deliveries_e2e() {
    let mut options = ServerOptions::default();
    options.history.enabled = true;
    options.connection_recovery.enabled = true;
    options.history.max_page_size = 100;
    let harness = build_harness(options).await;

    append_history_message(
        &harness.history_store,
        &harness.app.id,
        "good",
        "good-stream",
        1,
        "good-1",
        "good-event-1",
    )
    .await;
    append_history_message(
        &harness.history_store,
        &harness.app.id,
        "good",
        "good-stream",
        2,
        "good-2",
        "good-event-2",
    )
    .await;

    let (socket_id, mut reader) = connect_v2_socket(&harness).await;
    let payload = sonic_rs::json!({
        "channel_positions": {
            "good": {
                "stream_id": "good-stream",
                "serial": 1,
                "last_message_id": "good-1",
            },
            "bad": {
                "stream_id": "wrong-stream",
                "serial": 1,
                "last_message_id": "bad-1",
            }
        }
    });
    harness
        .handler
        .handle_resume(
            &socket_id,
            &harness.app,
            &PusherMessage {
                event: Some("sockudo:resume".to_string()),
                channel: None,
                data: Some(MessageData::Json(payload)),
                name: None,
                user_id: None,
                tags: None,
                sequence: None,
                conflation_key: None,
                message_id: None,
                stream_id: None,
                serial: None,
                idempotency_key: None,
                extras: None,
                delta_sequence: None,
                delta_conflation_key: None,
            },
        )
        .await
        .unwrap();

    let messages = recv_until_event(&mut reader, "sockudo:resume_success").await;
    assert!(
        messages
            .iter()
            .any(|msg| msg.message_id.as_deref() == Some("good-2"))
    );
    let failure = messages
        .iter()
        .find(|msg| msg.event.as_deref() == Some("sockudo:resume_failed"))
        .expect("expected resume_failed event");
    let failure_data = match failure.data.as_ref().unwrap() {
        MessageData::String(data) => {
            sonic_rs::from_str::<BTreeMap<String, sonic_rs::Value>>(data).unwrap()
        }
        MessageData::Json(value) => {
            sonic_rs::from_value::<BTreeMap<String, sonic_rs::Value>>(value).unwrap()
        }
        other => panic!("unexpected failure payload: {other:?}"),
    };
    assert_eq!(failure.channel.as_deref(), Some("bad"));
    assert_eq!(
        failure_data.get("code").and_then(|v| v.as_str()).is_some(),
        true
    );

    let aggregate = messages
        .last()
        .expect("expected final resume_success aggregate");
    let aggregate_data = match aggregate.data.as_ref().unwrap() {
        MessageData::String(data) => {
            sonic_rs::from_str::<BTreeMap<String, sonic_rs::Value>>(data).unwrap()
        }
        MessageData::Json(value) => {
            sonic_rs::from_value::<BTreeMap<String, sonic_rs::Value>>(value).unwrap()
        }
        other => panic!("unexpected aggregate payload: {other:?}"),
    };
    assert_eq!(aggregate.event.as_deref(), Some("sockudo:resume_success"));
    assert_eq!(
        aggregate_data
            .get("recovered")
            .and_then(|v| v.as_array())
            .map(|v| v.len()),
        Some(1)
    );
    assert_eq!(
        aggregate_data
            .get("failed")
            .and_then(|v| v.as_array())
            .map(|v| v.len()),
        Some(1)
    );
}

#[tokio::test]
async fn writer_queue_full_fault_rejects_publish_without_live_delivery() {
    let mut options = ServerOptions::default();
    options.history.enabled = true;
    let rejecting_store = Arc::new(RejectingAppendHistoryStore {
        inner: Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default())),
        error_message: "History writer queue is full: simulated fault",
    });
    let (handler, app, app_manager, adapter) =
        build_harness_with_store(options, rejecting_store).await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = sockudo_ws::handshake::server_handshake(&mut stream)
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
    let (mut reader, _writer) = client_ws.split();
    let socket_id = SocketId::new();
    adapter
        .add_socket(
            socket_id,
            server_task.await.unwrap(),
            &app.id,
            app_manager as Arc<dyn AppManager + Send + Sync>,
            WebSocketBufferConfig::default(),
            ProtocolVersion::V2,
            WireFormat::Json,
            true,
        )
        .await
        .unwrap();

    handler
        .handle_subscribe_request(
            &socket_id,
            &app,
            SubscriptionRequest {
                channel: "chat".to_string(),
                auth: None,
                channel_data: None,
                #[cfg(feature = "tag-filtering")]
                tags_filter: None,
                #[cfg(feature = "delta")]
                delta: None,
                rewind: None,
                event_name_filter: None,
            },
        )
        .await
        .unwrap();
    let _ = recv_message(&mut reader).await;

    let publish_result = handler
        .broadcast_to_channel(
            &app,
            "chat",
            live_message("chat", "should-not-send", "queue-full-msg"),
            None,
        )
        .await;

    match publish_result {
        Ok(_) => panic!("expected queue-full fault to reject publish"),
        Err(sockudo_core::error::Error::Internal(message)) => {
            assert!(message.contains("History writer queue is full"));
        }
        Err(other) => panic!("unexpected publish error: {other:?}"),
    }

    expect_no_message(&mut reader, Duration::from_millis(200)).await;
}
