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
use sockudo_core::version_store::{MemoryVersionStore, StoredVersionRecord, VersionStore};
use sockudo_core::versioned_messages::{
    FieldPatch, MessageAction as CoreMessageAction, MessageFieldDelta, VersionMetadata,
    VersionSerial,
};
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
    build_redis_node_with_version_store(prefix, app, history_store, None, options).await
}

async fn build_redis_node_with_version_store(
    prefix: &str,
    app: &App,
    history_store: Arc<dyn HistoryStore + Send + Sync>,
    version_store: Option<Arc<dyn VersionStore + Send + Sync>>,
    options: ServerOptions,
) -> ClusterNode {
    let app_manager = Arc::new(MemoryAppManager::new());
    app_manager.create_app(app.clone()).await.unwrap();

    let mut adapter = RedisAdapter::new(RedisAdapterOptions {
        url: redis_url(),
        prefix: prefix.to_string(),
        request_timeout_ms: 1000,
        cluster_mode: false,
        sentinel: None,
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
    let builder = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(MockCacheManager::new()),
        options,
    )
    .local_adapter(adapter.local_adapter.clone())
    .history_store(history_store)
    .metrics(Arc::new(MockMetricsInterface::new()));
    let handler = if let Some(version_store) = version_store {
        builder.version_store(version_store).build()
    } else {
        builder.build()
    };

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
            sockudo_protocol::AppendMode::Delta,
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
                annotation_subscribe: false,
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
                annotation_subscribe: false,
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
async fn versioned_winner_selection_converges_under_reordered_arrival_via_redis() {
    if !is_redis_available().await {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let app = test_app();
    let history_store = Arc::new(ClusterHistoryStore::new(Arc::new(MemoryHistoryStore::new(
        MemoryHistoryStoreConfig::default(),
    ))));
    let version_store = Arc::new(MemoryVersionStore::new());
    let prefix = format!("versioned_cluster_winner_{}", Uuid::new_v4().simple());
    let mut options = ServerOptions::default();
    options.history.enabled = true;
    options.versioned_messages.enabled = true;

    let node_a = build_redis_node_with_version_store(
        &prefix,
        &app,
        history_store.clone(),
        Some(version_store.clone() as Arc<dyn VersionStore + Send + Sync>),
        options.clone(),
    )
    .await;
    let node_b = build_redis_node_with_version_store(
        &prefix,
        &app,
        history_store,
        Some(version_store.clone() as Arc<dyn VersionStore + Send + Sync>),
        options,
    )
    .await;
    wait_for_cluster_ready(&[&node_a.adapter, &node_b.adapter], 2).await;

    node_a
        .handler
        .broadcast_to_channel(
            &app,
            "versioned",
            live_message("versioned", "chat.message", "versioned-msg-1"),
            None,
        )
        .await
        .unwrap();

    let current = version_store
        .latest_by_history(&app.id, "versioned")
        .await
        .unwrap()
        .into_iter()
        .next()
        .expect("expected created version");

    let winner = StoredVersionRecord {
        app_id: current.app_id.clone(),
        channel: current.channel.clone(),
        original_client_id: current.original_client_id.clone(),
        message: current
            .message
            .apply_mutation(
                CoreMessageAction::Delete,
                VersionMetadata {
                    serial: VersionSerial::new(
                        "99999999999999999999:node-b:00000000000000000001".to_string(),
                    )
                    .unwrap(),
                    client_id: Some("user-2".to_string()),
                    timestamp_ms: 3,
                    description: Some("winner".to_string()),
                    metadata: None,
                },
                3,
                MessageFieldDelta {
                    data: FieldPatch::Clear,
                    ..Default::default()
                },
            )
            .unwrap(),
    };
    let loser = StoredVersionRecord {
        app_id: current.app_id.clone(),
        channel: current.channel.clone(),
        original_client_id: current.original_client_id.clone(),
        message: current
            .message
            .apply_mutation(
                CoreMessageAction::Update,
                VersionMetadata {
                    serial: VersionSerial::new(
                        "50000000000000000000:node-a:00000000000000000001".to_string(),
                    )
                    .unwrap(),
                    client_id: Some("user-1".to_string()),
                    timestamp_ms: 2,
                    description: Some("loser".to_string()),
                    metadata: None,
                },
                2,
                MessageFieldDelta {
                    data: FieldPatch::Replace(MessageData::String("older".to_string())),
                    ..Default::default()
                },
            )
            .unwrap(),
    };

    version_store.append_version(winner.clone()).await.unwrap();
    version_store.append_version(loser).await.unwrap();

    let latest_a = node_a
        .handler
        .version_store()
        .get_latest(&app.id, "versioned", current.message_serial())
        .await
        .unwrap()
        .unwrap();
    let latest_b = node_b
        .handler
        .version_store()
        .get_latest(&app.id, "versioned", current.message_serial())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        latest_a.version_serial().as_str(),
        "99999999999999999999:node-b:00000000000000000001"
    );
    assert_eq!(
        latest_b.version_serial().as_str(),
        latest_a.version_serial().as_str()
    );
    assert_eq!(latest_a.message.action, CoreMessageAction::Delete);
    assert_eq!(latest_b.message.action, CoreMessageAction::Delete);
}

#[tokio::test]
async fn versioned_cold_recovery_replays_mutations_across_nodes_via_redis() {
    if !is_redis_available().await {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let app = test_app();
    let history_store = Arc::new(ClusterHistoryStore::new(Arc::new(MemoryHistoryStore::new(
        MemoryHistoryStoreConfig::default(),
    ))));
    let version_store = Arc::new(MemoryVersionStore::new());
    let prefix = format!("versioned_cluster_cold_{}", Uuid::new_v4().simple());
    let mut options = ServerOptions::default();
    options.history.enabled = true;
    options.connection_recovery.enabled = true;
    options.versioned_messages.enabled = true;
    options.history.max_page_size = 100;

    let node_a = build_redis_node_with_version_store(
        &prefix,
        &app,
        history_store.clone(),
        Some(version_store.clone() as Arc<dyn VersionStore + Send + Sync>),
        options.clone(),
    )
    .await;
    let node_b = build_redis_node_with_version_store(
        &prefix,
        &app,
        history_store.clone(),
        Some(version_store.clone() as Arc<dyn VersionStore + Send + Sync>),
        options.clone(),
    )
    .await;
    wait_for_cluster_ready(&[&node_a.adapter, &node_b.adapter], 2).await;

    let (source_socket_id, mut source_reader) = connect_v2_socket(&node_b, &app).await;
    node_b
        .handler
        .handle_subscribe_request(
            &source_socket_id,
            &app,
            SubscriptionRequest {
                channel: "versioned".to_string(),
                auth: None,
                channel_data: None,
                #[cfg(feature = "tag-filtering")]
                tags_filter: None,
                #[cfg(feature = "delta")]
                delta: None,
                rewind: None,
                event_name_filter: None,
                annotation_subscribe: false,
            },
        )
        .await
        .unwrap();
    let _ = recv_message(&mut source_reader).await;

    node_a
        .handler
        .broadcast_to_channel(
            &app,
            "versioned",
            live_message("versioned", "chat.message", "versioned-create-1"),
            None,
        )
        .await
        .unwrap();
    let first = recv_message(&mut source_reader).await;

    let current = version_store
        .latest_by_history(&app.id, "versioned")
        .await
        .unwrap()
        .into_iter()
        .next()
        .expect("expected created version");

    let update_reservation = version_store
        .reserve_delivery_position(&app.id, "versioned")
        .await
        .unwrap();
    let updated = StoredVersionRecord {
        app_id: current.app_id.clone(),
        channel: current.channel.clone(),
        original_client_id: current.original_client_id.clone(),
        message: current
            .message
            .apply_mutation(
                CoreMessageAction::Update,
                VersionMetadata {
                    serial: VersionSerial::new(
                        "50000000000000000000:node-a:00000000000000000001".to_string(),
                    )
                    .unwrap(),
                    client_id: Some("user-1".to_string()),
                    timestamp_ms: 2,
                    description: Some("patched".to_string()),
                    metadata: None,
                },
                update_reservation.delivery_serial,
                MessageFieldDelta {
                    data: FieldPatch::Replace(MessageData::String("patched".to_string())),
                    ..Default::default()
                },
            )
            .unwrap(),
    };
    version_store.append_version(updated.clone()).await.unwrap();
    node_a
        .handler
        .broadcast_to_channel_force_full(
            &app,
            "versioned",
            node_a
                .handler
                .build_runtime_message_from_record(&updated, Some(update_reservation.stream_id)),
            None,
            None,
        )
        .await
        .unwrap();
    let _ = recv_message(&mut source_reader).await;

    let latest = version_store
        .get_latest(&app.id, "versioned", updated.message_serial())
        .await
        .unwrap()
        .unwrap();
    let append_reservation = version_store
        .reserve_delivery_position(&app.id, "versioned")
        .await
        .unwrap();
    let appended = StoredVersionRecord {
        app_id: latest.app_id.clone(),
        channel: latest.channel.clone(),
        original_client_id: latest.original_client_id.clone(),
        message: latest
            .message
            .apply_append(
                VersionMetadata {
                    serial: VersionSerial::new(
                        "70000000000000000000:node-a:00000000000000000001".to_string(),
                    )
                    .unwrap(),
                    client_id: Some("user-1".to_string()),
                    timestamp_ms: 3,
                    description: Some("append".to_string()),
                    metadata: None,
                },
                append_reservation.delivery_serial,
                sockudo_core::versioned_messages::MessageAppend {
                    data_fragment: " world".to_string(),
                    extras: None,
                },
            )
            .unwrap(),
    };
    version_store
        .append_version(appended.clone())
        .await
        .unwrap();
    node_a
        .handler
        .broadcast_to_channel_force_full(
            &app,
            "versioned",
            node_a
                .handler
                .build_runtime_message_from_record(&appended, Some(append_reservation.stream_id)),
            None,
            None,
        )
        .await
        .unwrap();
    let _ = recv_message(&mut source_reader).await;

    let restarted_node_b = build_redis_node_with_version_store(
        &prefix,
        &app,
        history_store,
        Some(version_store.clone() as Arc<dyn VersionStore + Send + Sync>),
        options,
    )
    .await;
    wait_for_cluster_ready(&[&node_a.adapter, &restarted_node_b.adapter], 2).await;

    let (resume_socket_id, mut resume_reader) = connect_v2_socket(&restarted_node_b, &app).await;
    let resume_payload = json!({
        "channel_positions": {
            "versioned": {
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
    let replayed_events: Vec<String> = messages
        .iter()
        .filter_map(|msg| msg.event.clone())
        .filter(|event| event == "sockudo:message.update" || event == "sockudo:message.append")
        .collect();
    assert_eq!(
        replayed_events,
        vec![
            "sockudo:message.update".to_string(),
            "sockudo:message.append".to_string()
        ]
    );

    let appended_message = messages
        .iter()
        .find(|msg| msg.event.as_deref() == Some("sockudo:message.append"))
        .expect("expected append replay");
    assert_eq!(
        appended_message.data,
        Some(MessageData::String("patched world".to_string()))
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
                annotation_subscribe: false,
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
        annotation_subscribe: false,
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
