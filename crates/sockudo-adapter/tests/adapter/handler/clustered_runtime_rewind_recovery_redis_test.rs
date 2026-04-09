#![cfg(feature = "redis")]

use crate::mocks::connection_handler_mock::{MockCacheManager, MockMetricsInterface};
use async_trait::async_trait;
use bytes::Bytes;
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::handler::{
    ConnectionHandler,
    types::{SubscriptionRequest, SubscriptionRewind},
};
use sockudo_adapter::horizontal_transport::HorizontalTransport;
use sockudo_adapter::redis_adapter::{RedisAdapter, RedisAdapterOptions};
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_core::app::{App, AppManager};
use sockudo_core::history::{
    HistoryAppendRecord, HistoryDurableState, HistoryPage, HistoryReadRequest,
    HistoryRuntimeStatus, HistoryStore, HistoryStreamRuntimeState, HistoryWriteReservation,
    MemoryHistoryStore, MemoryHistoryStoreConfig,
};
use sockudo_core::options::{ClusterHealthConfig, ServerOptions};
use sockudo_core::websocket::{SocketId, WebSocketBufferConfig};
use sockudo_protocol::messages::{MessageData, PusherMessage};
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::{WebSocket, WebSocketWriter};
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{
    Config as WsConfig, Http1, Message as WsMessage, SplitReader, Stream as WsStream,
    WebSocketStream,
};
use sonic_rs::{JsonValueTrait, Value, json};
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock, oneshot};
use tokio::time::timeout;
use uuid::Uuid;

type ClientReader = SplitReader<WsStream<Http1>>;

struct ReadGate {
    started_tx: Option<oneshot::Sender<()>>,
    continue_rx: oneshot::Receiver<()>,
}

#[derive(Clone)]
struct ClusterHistoryStore {
    inner: Arc<MemoryHistoryStore>,
    gate: Arc<Mutex<Option<ReadGate>>>,
    states: Arc<RwLock<HashMap<String, HistoryStreamRuntimeState>>>,
}

impl ClusterHistoryStore {
    fn new(inner: Arc<MemoryHistoryStore>) -> Self {
        Self {
            inner,
            gate: Arc::new(Mutex::new(None)),
            states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn key(app_id: &str, channel: &str) -> String {
        format!("{app_id}\0{channel}")
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

    async fn set_state(&self, app_id: &str, channel: &str, state: HistoryStreamRuntimeState) {
        self.states
            .write()
            .await
            .insert(Self::key(app_id, channel), state);
    }
}

#[async_trait]
impl HistoryStore for ClusterHistoryStore {
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
        let state = self
            .stream_runtime_state(&request.app_id, &request.channel)
            .await?;
        if !state.recovery_allowed {
            return Err(sockudo_core::error::Error::Internal(
                "history_stream_state_blocks_reads".to_string(),
            ));
        }

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
        let states = self.states.read().await;
        let degraded_channels = states
            .values()
            .filter(|state| !state.recovery_allowed)
            .count();
        let reset_required_channels = states.values().filter(|state| state.reset_required).count();
        Ok(HistoryRuntimeStatus {
            enabled: true,
            backend: "memory".to_string(),
            state_authority: "shared_test_state".to_string(),
            degraded_channels,
            reset_required_channels,
            queue_depth: 0,
        })
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> sockudo_core::error::Result<HistoryStreamRuntimeState> {
        if let Some(state) = self
            .states
            .read()
            .await
            .get(&Self::key(app_id, channel))
            .cloned()
        {
            return Ok(state);
        }
        self.inner.stream_runtime_state(app_id, channel).await
    }
}

struct ClusterNode {
    handler: ConnectionHandler,
    adapter: Arc<RedisAdapter>,
    app_manager: Arc<MemoryAppManager>,
}

fn redis_url() -> String {
    env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:16379/".to_string())
}

async fn is_redis_available() -> bool {
    match redis::Client::open(redis_url()) {
        Ok(client) => match client.get_connection() {
            Ok(mut conn) => {
                use redis::Commands;
                let _: Result<String, _> = conn.ping();
                true
            }
            Err(_) => false,
        },
        Err(_) => false,
    }
}

fn test_app() -> App {
    App::from_policy(
        "app".to_string(),
        "key".to_string(),
        "secret".to_string(),
        true,
        Default::default(),
    )
}

async fn build_redis_node(
    prefix: &str,
    app: &App,
    history_store: Arc<dyn HistoryStore + Send + Sync>,
    options: ServerOptions,
) -> ClusterNode {
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(app.clone()).await.unwrap();

    let mut adapter = RedisAdapter::new(RedisAdapterOptions {
        url: redis_url(),
        prefix: prefix.to_string(),
        request_timeout_ms: 1000,
        cluster_mode: false,
    })
    .await
    .unwrap();
    adapter
        .set_cluster_health(&ClusterHealthConfig {
            enabled: false,
            heartbeat_interval_ms: 100,
            node_timeout_ms: 500,
            cleanup_interval_ms: 200,
        })
        .await
        .unwrap();
    adapter.local_adapter.init().await;
    adapter.start_listeners().await.unwrap();

    let adapter = Arc::new(adapter);
    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(MockCacheManager::new()),
        options,
    )
    .local_adapter(adapter.local_adapter.clone())
    .history_store(history_store)
    .metrics(Arc::new(MockMetricsInterface::new()))
    .build();

    ClusterNode {
        handler,
        adapter,
        app_manager,
    }
}

async fn wait_for_cluster_ready(nodes: &[&Arc<RedisAdapter>], expected_nodes: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let mut all_ready = true;
        for node in nodes {
            let count = node.transport.get_node_count().await.unwrap_or(1);
            if count < expected_nodes {
                all_ready = false;
                break;
            }
        }
        if all_ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for redis transport cluster readiness"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn connect_v2_socket(node: &ClusterNode, app: &App) -> (SocketId, ClientReader) {
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
    node.adapter
        .add_socket(
            socket_id,
            server_writer,
            &app.id,
            node.app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
            WebSocketBufferConfig::default(),
            ProtocolVersion::V2,
            WireFormat::Json,
            true,
        )
        .await
        .unwrap();

    (socket_id, reader)
}

fn live_message(channel: &str, event: &str, message_id: &str) -> PusherMessage {
    PusherMessage {
        event: Some(event.to_string()),
        channel: Some(channel.to_string()),
        data: Some(MessageData::Json(json!({ "event": event }))),
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

async fn append_history_message(
    store: &Arc<ClusterHistoryStore>,
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
        data: Some(MessageData::Json(json!({ "event": event }))),
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
            retention: sockudo_core::history::HistoryRetentionPolicy {
                retention_window_seconds: 3600,
                max_messages_per_channel: None,
                max_bytes_per_channel: None,
            },
        })
        .await
        .unwrap();
}

async fn recv_message(reader: &mut ClientReader) -> PusherMessage {
    let next = timeout(Duration::from_secs(4), reader.next())
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

async fn recv_additional_messages(
    reader: &mut ClientReader,
    idle_window: Duration,
) -> Vec<PusherMessage> {
    let mut messages = Vec::new();
    loop {
        let next = timeout(idle_window, reader.next()).await;
        let Ok(Some(Ok(frame))) = next else {
            return messages;
        };

        let message = match frame {
            WsMessage::Text(text) => {
                sonic_rs::from_str(&String::from_utf8_lossy(text.as_ref())).unwrap()
            }
            WsMessage::Binary(bytes) => sonic_rs::from_slice(&bytes).unwrap(),
            other => panic!("unexpected websocket message: {other:?}"),
        };
        messages.push(message);
    }
}

fn parse_message_data_map(message: &PusherMessage) -> BTreeMap<String, Value> {
    match message.data.as_ref().expect("expected message data") {
        MessageData::String(data) => sonic_rs::from_str(data).unwrap(),
        MessageData::Json(value) => sonic_rs::from_value(value).unwrap(),
        other => panic!("unexpected payload: {other:?}"),
    }
}

#[tokio::test]
async fn publish_on_node_a_reaches_subscriber_on_node_b_via_redis() {
    if !is_redis_available().await {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let app = test_app();
    let history_store = Arc::new(ClusterHistoryStore::new(Arc::new(MemoryHistoryStore::new(
        MemoryHistoryStoreConfig::default(),
    ))));
    let prefix = format!("history_cluster_live_{}", Uuid::new_v4().simple());
    let mut options = ServerOptions::default();
    options.history.enabled = true;

    let node_a = build_redis_node(&prefix, &app, history_store.clone(), options.clone()).await;
    let node_b = build_redis_node(&prefix, &app, history_store, options).await;
    wait_for_cluster_ready(&[&node_a.adapter, &node_b.adapter], 2).await;

    let (socket_id, mut reader) = connect_v2_socket(&node_b, &app).await;
    node_b
        .handler
        .handle_subscribe_request(
            &socket_id,
            &app,
            SubscriptionRequest {
                channel: "live".to_string(),
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

    node_a
        .handler
        .broadcast_to_channel(
            &app,
            "live",
            live_message("live", "cross-node-live", "live-1"),
            None,
        )
        .await
        .unwrap();

    let delivered = recv_message(&mut reader).await;
    assert_eq!(delivered.event.as_deref(), Some("cross-node-live"));
    assert_eq!(delivered.message_id.as_deref(), Some("live-1"));
    assert!(delivered.stream_id.is_some());
    assert!(delivered.serial.is_some());
}

#[tokio::test]
async fn fresh_node_with_empty_replay_buffer_uses_cold_recovery_after_cross_node_publish_via_redis()
{
    if !is_redis_available().await {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let app = test_app();
    let history_store = Arc::new(ClusterHistoryStore::new(Arc::new(MemoryHistoryStore::new(
        MemoryHistoryStoreConfig::default(),
    ))));
    let prefix = format!("history_cluster_cold_{}", Uuid::new_v4().simple());
    let mut options = ServerOptions::default();
    options.history.enabled = true;
    options.connection_recovery.enabled = true;
    options.history.max_page_size = 100;

    let node_a = build_redis_node(&prefix, &app, history_store.clone(), options.clone()).await;
    let node_b = build_redis_node(&prefix, &app, history_store.clone(), options.clone()).await;
    wait_for_cluster_ready(&[&node_a.adapter, &node_b.adapter], 2).await;

    let (source_socket_id, mut source_reader) = connect_v2_socket(&node_b, &app).await;
    node_b
        .handler
        .handle_subscribe_request(
            &source_socket_id,
            &app,
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
        node_a
            .handler
            .broadcast_to_channel(&app, "cold", live_message("cold", event, message_id), None)
            .await
            .unwrap();
    }

    let first = recv_message(&mut source_reader).await;
    let _ = recv_message(&mut source_reader).await;
    let _ = recv_message(&mut source_reader).await;

    let restarted_node_b =
        build_redis_node(&prefix, &app, history_store.clone(), options.clone()).await;
    wait_for_cluster_ready(&[&node_a.adapter, &restarted_node_b.adapter], 2).await;

    let (resume_socket_id, mut resume_reader) = connect_v2_socket(&restarted_node_b, &app).await;
    let resume_payload = json!({
        "channel_positions": {
            "cold": {
                "stream_id": first.stream_id,
                "serial": first.serial,
                "last_message_id": first.message_id,
            }
        }
    });
    restarted_node_b
        .handler
        .handle_resume(
            &resume_socket_id,
            &app,
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

    let aggregate = messages.last().unwrap();
    let aggregate_data = parse_message_data_map(aggregate);
    assert_eq!(aggregate.event.as_deref(), Some("sockudo:resume_success"));
    assert_eq!(
        aggregate_data["recovered"][0]["source"].as_str(),
        Some("cold")
    );
}

#[tokio::test]
async fn degraded_durable_state_causes_explicit_cross_node_recovery_failure_via_redis() {
    if !is_redis_available().await {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let app = test_app();
    let history_store = Arc::new(ClusterHistoryStore::new(Arc::new(MemoryHistoryStore::new(
        MemoryHistoryStoreConfig::default(),
    ))));
    let prefix = format!("history_cluster_degraded_{}", Uuid::new_v4().simple());
    let mut options = ServerOptions::default();
    options.history.enabled = true;
    options.connection_recovery.enabled = true;
    options.history.max_page_size = 100;

    let node_a = build_redis_node(&prefix, &app, history_store.clone(), options.clone()).await;
    let node_b = build_redis_node(&prefix, &app, history_store.clone(), options.clone()).await;
    wait_for_cluster_ready(&[&node_a.adapter, &node_b.adapter], 2).await;

    let (source_socket_id, mut source_reader) = connect_v2_socket(&node_b, &app).await;
    node_b
        .handler
        .handle_subscribe_request(
            &source_socket_id,
            &app,
            SubscriptionRequest {
                channel: "degraded".to_string(),
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
        ("deg-1", "deg-msg-1"),
        ("deg-2", "deg-msg-2"),
        ("deg-3", "deg-msg-3"),
    ] {
        node_a
            .handler
            .broadcast_to_channel(
                &app,
                "degraded",
                live_message("degraded", event, message_id),
                None,
            )
            .await
            .unwrap();
    }

    let first = recv_message(&mut source_reader).await;
    let _ = recv_message(&mut source_reader).await;
    let _ = recv_message(&mut source_reader).await;

    history_store
        .set_state(
            &app.id,
            "degraded",
            HistoryStreamRuntimeState {
                app_id: app.id.clone(),
                channel: "degraded".to_string(),
                stream_id: first.stream_id.clone(),
                durable_state: HistoryDurableState::Degraded,
                recovery_allowed: false,
                reset_required: false,
                reason: Some("durable_history_write_failed".to_string()),
                node_id: Some(node_a.adapter.get_node_id()),
                last_transition_at_ms: Some(sockudo_core::history::now_ms()),
                authoritative_source: "shared_test_state".to_string(),
                observed_source: "shared_test_state".to_string(),
            },
        )
        .await;

    let restarted_node_b =
        build_redis_node(&prefix, &app, history_store.clone(), options.clone()).await;
    wait_for_cluster_ready(&[&node_a.adapter, &restarted_node_b.adapter], 2).await;

    let (resume_socket_id, mut resume_reader) = connect_v2_socket(&restarted_node_b, &app).await;
    let resume_payload = json!({
        "channel_positions": {
            "degraded": {
                "stream_id": first.stream_id,
                "serial": first.serial,
                "last_message_id": first.message_id,
            }
        }
    });
    restarted_node_b
        .handler
        .handle_resume(
            &resume_socket_id,
            &app,
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
    let failure = messages
        .iter()
        .find(|msg| msg.event.as_deref() == Some("sockudo:resume_failed"))
        .expect("expected explicit resume failure");
    let failure_data = parse_message_data_map(failure);
    assert_eq!(failure.channel.as_deref(), Some("degraded"));
    assert_eq!(
        failure_data["code"].as_str(),
        Some("persistence_unavailable")
    );
    assert_eq!(
        failure_data["reason"].as_str(),
        Some("history_stream_degraded")
    );
}

#[tokio::test]
async fn rewind_live_handoff_across_nodes_has_no_gap_and_no_duplicates_via_redis() {
    if !is_redis_available().await {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let app = test_app();
    let history_store = Arc::new(ClusterHistoryStore::new(Arc::new(MemoryHistoryStore::new(
        MemoryHistoryStoreConfig::default(),
    ))));
    let prefix = format!("history_cluster_rewind_{}", Uuid::new_v4().simple());
    let mut options = ServerOptions::default();
    options.history.enabled = true;
    options.history.max_page_size = 100;

    let node_a = build_redis_node(&prefix, &app, history_store.clone(), options.clone()).await;
    let node_b = build_redis_node(&prefix, &app, history_store.clone(), options).await;
    wait_for_cluster_ready(&[&node_a.adapter, &node_b.adapter], 2).await;

    append_history_message(
        &history_store,
        &app.id,
        "chat",
        "stream-1",
        1,
        "dup-msg",
        "history-1",
    )
    .await;
    append_history_message(
        &history_store,
        &app.id,
        "chat",
        "stream-1",
        2,
        "hist-2",
        "history-2",
    )
    .await;

    let (socket_id, mut reader) = connect_v2_socket(&node_b, &app).await;
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

    let (started_rx, continue_tx) = history_store.arm_gate().await;
    let handler = node_b.handler.clone();
    let app_clone = app.clone();
    let subscribe_task = tokio::spawn(async move {
        handler
            .handle_subscribe_request(&socket_id, &app_clone, request)
            .await
            .unwrap();
    });

    started_rx.await.unwrap();
    node_a
        .handler
        .broadcast_to_channel(
            &app,
            "chat",
            live_message("chat", "live-dup", "dup-msg"),
            None,
        )
        .await
        .unwrap();
    node_a
        .handler
        .broadcast_to_channel(
            &app,
            "chat",
            live_message("chat", "live-unique", "live-1"),
            None,
        )
        .await
        .unwrap();
    let _ = continue_tx.send(());
    subscribe_task.await.unwrap();

    let mut messages = recv_until_event(&mut reader, "sockudo:rewind_complete").await;
    messages.extend(recv_additional_messages(&mut reader, Duration::from_millis(500)).await);
    let ordered_events: Vec<String> = messages
        .iter()
        .filter_map(|msg| msg.event.clone())
        .collect();

    assert!(
        ordered_events
            .windows(2)
            .any(|events| { events[0] == "history-1" && events[1] == "history-2" })
    );
    assert!(
        ordered_events
            .iter()
            .any(|event| event == "sockudo:rewind_complete")
    );
    assert_eq!(
        ordered_events
            .iter()
            .filter(|event| event.as_str() == "live-unique")
            .count(),
        1
    );
    assert!(
        !messages
            .iter()
            .any(|msg| msg.event.as_deref() == Some("live-dup"))
    );
}
