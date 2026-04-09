#![cfg(all(feature = "redis", feature = "surrealdb", feature = "dynamodb"))]

#[path = "../src/history_dynamodb.rs"]
mod history_dynamodb;
#[path = "../src/history_surreal.rs"]
mod history_surreal;

use async_trait::async_trait;
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::handler::{ConnectionHandler, types::SubscriptionRequest};
use sockudo_adapter::horizontal_transport::HorizontalTransport;
use sockudo_adapter::redis_adapter::{RedisAdapter, RedisAdapterOptions};
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_cache::MemoryCacheManager;
use sockudo_core::app::{App, AppManager};
use sockudo_core::history::{
    HistoryAppendRecord, HistoryPage, HistoryReadRequest, HistoryRuntimeStatus, HistoryStore,
    HistoryStreamRuntimeState, HistoryWriteReservation,
};
use sockudo_core::options::{
    ClusterHealthConfig, DynamoDbSettings, MemoryCacheOptions, ServerOptions, SurrealDbSettings,
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
struct StateAwareHistoryStore {
    inner: Arc<dyn HistoryStore + Send + Sync>,
    gate: Arc<Mutex<Option<ReadGate>>>,
    states: Arc<RwLock<HashMap<String, HistoryStreamRuntimeState>>>,
}

impl StateAwareHistoryStore {
    fn new(inner: Arc<dyn HistoryStore + Send + Sync>) -> Self {
        Self {
            inner,
            gate: Arc::new(Mutex::new(None)),
            states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn key(app_id: &str, channel: &str) -> String {
        format!("{app_id}\0{channel}")
    }
}

#[async_trait]
impl HistoryStore for StateAwareHistoryStore {
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
            backend: "stateful_wrapper".to_string(),
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

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> sockudo_core::error::Result<sockudo_core::history::HistoryStreamInspection> {
        self.inner.stream_inspection(app_id, channel).await
    }

    async fn reset_stream(
        &self,
        app_id: &str,
        channel: &str,
        reason: &str,
        requested_by: Option<&str>,
    ) -> sockudo_core::error::Result<sockudo_core::history::HistoryResetResult> {
        self.inner
            .reset_stream(app_id, channel, reason, requested_by)
            .await
    }

    async fn purge_stream(
        &self,
        app_id: &str,
        channel: &str,
        request: sockudo_core::history::HistoryPurgeRequest,
    ) -> sockudo_core::error::Result<sockudo_core::history::HistoryPurgeResult> {
        self.inner.purge_stream(app_id, channel, request).await
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
    let cache = Arc::new(MemoryCacheManager::new(
        format!("cluster_test_cache_{}", Uuid::new_v4().simple()),
        MemoryCacheOptions::default(),
    ));
    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        cache,
        options,
    )
    .local_adapter(adapter.local_adapter.clone())
    .history_store(history_store)
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

fn parse_message_data_map(message: &PusherMessage) -> BTreeMap<String, Value> {
    match message.data.as_ref().expect("expected message data") {
        MessageData::String(data) => sonic_rs::from_str(data).unwrap(),
        MessageData::Json(value) => sonic_rs::from_value(value).unwrap(),
        other => panic!("unexpected payload: {other:?}"),
    }
}

async fn is_surreal_available() -> bool {
    for _ in 0..10 {
        let db = surrealdb::engine::any::connect("ws://127.0.0.1:18001").await;
        if let Ok(db) = db
            && db
                .signin(surrealdb::opt::auth::Root {
                    username: "root".to_string(),
                    password: "root".to_string(),
                })
                .await
                .is_ok()
            && db.health().await.is_ok()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    false
}

async fn is_dynamodb_available() -> bool {
    for _ in 0..10 {
        let settings = DynamoDbSettings {
            region: "us-east-1".to_string(),
            table_name: "sockudo_history_test_probe".to_string(),
            endpoint_url: Some("http://127.0.0.1:18000".to_string()),
            aws_access_key_id: Some("dummy".to_string()),
            aws_secret_access_key: Some("dummy".to_string()),
            aws_profile_name: None,
        };
        let mut aws_config_builder = aws_config::from_env()
            .region(aws_sdk_dynamodb::config::Region::new(
                settings.region.clone(),
            ))
            .endpoint_url(settings.endpoint_url.clone().unwrap());
        let credentials_provider =
            aws_sdk_dynamodb::config::Credentials::new("dummy", "dummy", None, None, "static");
        aws_config_builder = aws_config_builder.credentials_provider(credentials_provider);
        let client = aws_sdk_dynamodb::Client::new(&aws_config_builder.load().await);
        if client.list_tables().send().await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    false
}

async fn build_surreal_history_store() -> Arc<dyn HistoryStore + Send + Sync> {
    let settings = SurrealDbSettings {
        url: "ws://127.0.0.1:18001".to_string(),
        namespace: format!("sockudo_cluster_history_{}", Uuid::new_v4().simple()),
        database: "sockudo".to_string(),
        username: "root".to_string(),
        password: "root".to_string(),
        table_name: "applications".to_string(),
        cache_ttl: 300,
        cache_max_capacity: 100,
    };
    let mut config = sockudo_core::options::HistoryConfig::default();
    config.enabled = true;
    config.backend = sockudo_core::options::HistoryBackend::SurrealDb;
    config.surrealdb.table_prefix = format!("sockudo_history_{}", Uuid::new_v4().simple());
    history_surreal::create_surreal_history_store(&settings, config, None, None)
        .await
        .unwrap()
}

async fn build_dynamodb_history_store() -> Arc<dyn HistoryStore + Send + Sync> {
    let settings = DynamoDbSettings {
        region: "us-east-1".to_string(),
        table_name: format!("sockudo_history_{}", Uuid::new_v4().simple()),
        endpoint_url: Some("http://127.0.0.1:18000".to_string()),
        aws_access_key_id: Some("dummy".to_string()),
        aws_secret_access_key: Some("dummy".to_string()),
        aws_profile_name: None,
    };
    let mut config = sockudo_core::options::HistoryConfig::default();
    config.enabled = true;
    config.backend = sockudo_core::options::HistoryBackend::DynamoDb;
    history_dynamodb::create_dynamodb_history_store(&settings, config, None, None)
        .await
        .unwrap()
}

async fn run_cross_node_cold_recovery_test(history_store: Arc<dyn HistoryStore + Send + Sync>) {
    let app = test_app();
    let history_store = Arc::new(StateAwareHistoryStore::new(history_store));
    let prefix = format!("history_cluster_real_{}", Uuid::new_v4().simple());
    let mut options = ServerOptions::default();
    options.history.enabled = true;
    options.connection_recovery.enabled = true;
    options.history.max_page_size = 100;

    let node_a = build_redis_node(
        &prefix,
        &app,
        history_store.clone() as Arc<dyn HistoryStore + Send + Sync>,
        options.clone(),
    )
    .await;
    let node_b = build_redis_node(
        &prefix,
        &app,
        history_store.clone() as Arc<dyn HistoryStore + Send + Sync>,
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

    let restarted_node_b = build_redis_node(
        &prefix,
        &app,
        history_store.clone() as Arc<dyn HistoryStore + Send + Sync>,
        options.clone(),
    )
    .await;
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
async fn surreal_history_supports_cross_node_cold_recovery_over_redis_transport() {
    if !is_redis_available().await || !is_surreal_available().await {
        eprintln!("Skipping test: Redis or SurrealDB not available");
        return;
    }
    let history_store = build_surreal_history_store().await;
    run_cross_node_cold_recovery_test(history_store).await;
}

#[tokio::test]
async fn dynamodb_history_supports_cross_node_cold_recovery_over_redis_transport() {
    if !is_redis_available().await || !is_dynamodb_available().await {
        eprintln!("Skipping test: Redis or DynamoDB not available");
        return;
    }
    let history_store = build_dynamodb_history_store().await;
    run_cross_node_cold_recovery_test(history_store).await;
}
