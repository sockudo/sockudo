use crate::mocks::connection_handler_mock::{MockCacheManager, MockMetricsInterface};
use sockudo_adapter::ConnectionManager;
use sockudo_adapter::handler::ConnectionHandler;
use sockudo_adapter::handler::annotations::{
    DeleteAnnotationRuntimeRequest, PublishAnnotationRuntimeRequest,
};
use sockudo_adapter::handler::types::SubscriptionRequest;
use sockudo_adapter::local_adapter::LocalAdapter;
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_core::annotations::{
    Annotation, AnnotationAction, AnnotationEventLookupRequest, AnnotationEventsRequest,
    AnnotationId, AnnotationProjection, AnnotationProjectionOptions, AnnotationProjectionRequest,
    AnnotationProjectionsForChannelRequest, AnnotationSerial, AnnotationStore, AnnotationSummary,
    AnnotationType, IdentifiedAnnotationSummary, MemoryAnnotationStore, MultipleAnnotationSummary,
    RawAnnotationReplayRequest, StoredAnnotationEvent, StoredAnnotationProjection,
};
use sockudo_core::app::{App, AppChannelsPolicy, AppManager, AppPolicy};
use sockudo_core::history::{HistoryStore, MemoryHistoryStore, MemoryHistoryStoreConfig, now_ms};
use sockudo_core::options::ServerOptions;
use sockudo_core::version_store::{MemoryVersionStore, StoredVersionRecord, VersionStore};
use sockudo_core::versioned_messages::{
    MessageSerial, VersionMetadata, VersionSerial, VersionedMessage,
};
use sockudo_core::websocket::{ConnectionCapabilities, SocketId, UserInfo, WebSocketBufferConfig};
use sockudo_protocol::messages::{ANNOTATION_EVENT_NAME, MESSAGE_SUMMARY_EVENT_NAME, MessageData};
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::{WebSocket, WebSocketWriter};
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{
    Config as WsConfig, Http1, Message as WsMessage, SplitReader, Stream as WsStream,
    WebSocketStream,
};
use sonic_rs::{JsonValueTrait, Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, timeout};

type ClientReader = SplitReader<WsStream<Http1>>;

const APP_ID: &str = "app";
const CHANNEL: &str = "chat";
const MESSAGE_SERIAL: &str = "msg:1";

struct AnnotationHarness {
    handler: ConnectionHandler,
    app: App,
    app_manager: Arc<MemoryAppManager>,
    adapter: Arc<LocalAdapter>,
    metrics: Arc<MockMetricsInterface>,
}

struct RebuildReportingAnnotationStore {
    inner: Arc<MemoryAnnotationStore>,
    rebuild_count: usize,
}

#[async_trait::async_trait]
impl AnnotationStore for RebuildReportingAnnotationStore {
    async fn append_event(
        &self,
        event: StoredAnnotationEvent,
    ) -> sockudo_core::error::Result<StoredAnnotationProjection> {
        self.inner.append_event(event).await
    }

    async fn get_events(
        &self,
        request: AnnotationEventsRequest,
    ) -> sockudo_core::error::Result<Vec<StoredAnnotationEvent>> {
        self.inner.get_events(request).await
    }

    async fn replay_raw(
        &self,
        request: RawAnnotationReplayRequest,
    ) -> sockudo_core::error::Result<Vec<StoredAnnotationEvent>> {
        self.inner.replay_raw(request).await
    }

    async fn get_event_by_serial(
        &self,
        request: AnnotationEventLookupRequest,
    ) -> sockudo_core::error::Result<Option<StoredAnnotationEvent>> {
        self.inner.get_event_by_serial(request).await
    }

    async fn get_projection(
        &self,
        request: AnnotationProjectionRequest,
    ) -> sockudo_core::error::Result<Option<StoredAnnotationProjection>> {
        self.inner.get_projection(request).await
    }

    async fn list_projections_for_channel(
        &self,
        request: AnnotationProjectionsForChannelRequest,
    ) -> sockudo_core::error::Result<Vec<StoredAnnotationProjection>> {
        self.inner.list_projections_for_channel(request).await
    }

    async fn list_projections_for_channel_with_rebuild_count(
        &self,
        request: AnnotationProjectionsForChannelRequest,
    ) -> sockudo_core::error::Result<(Vec<StoredAnnotationProjection>, usize)> {
        let projections = self.inner.list_projections_for_channel(request).await?;
        Ok((projections, self.rebuild_count))
    }

    async fn rebuild_projection(
        &self,
        request: AnnotationProjectionRequest,
    ) -> sockudo_core::error::Result<StoredAnnotationProjection> {
        self.inner.rebuild_projection(request).await
    }

    async fn rebuild_projection_with_options(
        &self,
        request: AnnotationProjectionRequest,
        options: AnnotationProjectionOptions,
    ) -> sockudo_core::error::Result<StoredAnnotationProjection> {
        self.inner
            .rebuild_projection_with_options(request, options)
            .await
    }

    async fn purge_before(
        &self,
        before_ms: i64,
        batch_size: usize,
    ) -> sockudo_core::error::Result<(u64, bool)> {
        self.inner.purge_before(before_ms, batch_size).await
    }
}

fn annotation_type(value: &str) -> AnnotationType {
    AnnotationType::new(value).unwrap()
}

fn message_serial() -> MessageSerial {
    MessageSerial::new(MESSAGE_SERIAL).unwrap()
}

fn event(
    serial: &str,
    annotation_type: &str,
    action: AnnotationAction,
    name: Option<&str>,
    client_id: Option<&str>,
    count: Option<u64>,
) -> Annotation {
    Annotation {
        id: AnnotationId::new(format!("id:{serial}")).unwrap(),
        action,
        serial: AnnotationSerial::new(serial).unwrap(),
        message_serial: message_serial(),
        annotation_type: AnnotationType::new(annotation_type).unwrap(),
        name: name.map(str::to_string),
        client_id: client_id.map(str::to_string),
        count,
        data: None,
        encoding: None,
        timestamp: now_ms(),
    }
}

fn projection(annotation_type_value: &str, events: Vec<Annotation>) -> AnnotationSummary {
    AnnotationProjection::rebuild(
        CHANNEL,
        message_serial(),
        annotation_type(annotation_type_value),
        events,
    )
    .unwrap()
    .summary
}

fn projection_with_options(
    annotation_type_value: &str,
    events: Vec<Annotation>,
    options: AnnotationProjectionOptions,
) -> AnnotationSummary {
    AnnotationProjection::rebuild_with_options(
        CHANNEL,
        message_serial(),
        annotation_type(annotation_type_value),
        events,
        options,
    )
    .unwrap()
    .summary
}

fn summary_payload(message: &sockudo_protocol::messages::PusherMessage) -> Value {
    message_data(message)["annotations"]["summary"].clone()
}

fn message_data(message: &sockudo_protocol::messages::PusherMessage) -> Value {
    match message.data.as_ref().unwrap() {
        MessageData::Json(value) => value.clone(),
        MessageData::String(raw) => sonic_rs::from_str(raw).unwrap(),
        other => panic!("unexpected message data: {other:?}"),
    }
}

fn versioned_record() -> StoredVersionRecord {
    StoredVersionRecord {
        app_id: APP_ID.to_string(),
        channel: CHANNEL.to_string(),
        original_client_id: Some("author".to_string()),
        message: VersionedMessage::new_create(
            message_serial(),
            VersionMetadata {
                serial: VersionSerial::new("ver:1").unwrap(),
                client_id: Some("author".to_string()),
                timestamp_ms: now_ms(),
                description: None,
                metadata: None,
            },
            1,
            1,
            Some("chat.message".to_string()),
            Some(MessageData::Json(json!({"text": "hello"}))),
            None,
        ),
    }
}

async fn build_harness_with_shared_stores(
    version_store: Arc<MemoryVersionStore>,
    annotation_store: Arc<dyn AnnotationStore + Send + Sync>,
) -> AnnotationHarness {
    let app_manager = Arc::new(MemoryAppManager::new());
    let app = App::from_policy(
        APP_ID.to_string(),
        "key".to_string(),
        "secret".to_string(),
        true,
        AppPolicy {
            channels: AppChannelsPolicy {
                annotations_enabled: Some(true),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    app_manager.create_app(app.clone()).await.unwrap();

    let adapter = Arc::new(LocalAdapter::new());
    let mut options = ServerOptions::default();
    options.versioned_messages.enabled = true;
    options.history.enabled = true;
    options.annotations.enabled = true;

    let metrics = Arc::new(MockMetricsInterface::new());
    let handler = ConnectionHandler::builder(
        app_manager.clone() as Arc<dyn AppManager + Send + Sync>,
        adapter.clone() as Arc<dyn ConnectionManager + Send + Sync>,
        Arc::new(MockCacheManager::new()),
        options,
    )
    .local_adapter(adapter.clone())
    .version_store(version_store.clone() as Arc<dyn VersionStore + Send + Sync>)
    .annotation_store(annotation_store.clone())
    .history_store(
        Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()))
            as Arc<dyn HistoryStore + Send + Sync>,
    )
    .metrics(metrics.clone())
    .build();

    AnnotationHarness {
        handler,
        app,
        app_manager,
        adapter,
        metrics,
    }
}

async fn build_harness() -> AnnotationHarness {
    let version_store = Arc::new(MemoryVersionStore::new());
    version_store
        .append_version(versioned_record())
        .await
        .unwrap();
    build_harness_with_shared_stores(version_store, Arc::new(MemoryAnnotationStore::new())).await
}

async fn connect_v2_socket(harness: &AnnotationHarness) -> (SocketId, ClientReader) {
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

async fn sign_in_with_capabilities(
    harness: &AnnotationHarness,
    socket_id: &SocketId,
    user_id: &str,
    capabilities: ConnectionCapabilities,
) {
    harness
        .handler
        .update_connection_with_user_info(
            socket_id,
            &harness.app,
            &UserInfo {
                id: user_id.to_string(),
                watchlist: None,
                info: Some(json!({"id": user_id})),
                capabilities: Some(capabilities),
                meta: None,
            },
        )
        .await
        .unwrap();
}

fn subscribe_request(annotation_subscribe: bool) -> SubscriptionRequest {
    SubscriptionRequest {
        channel: CHANNEL.to_string(),
        auth: None,
        channel_data: None,
        #[cfg(feature = "tag-filtering")]
        tags_filter: None,
        #[cfg(feature = "delta")]
        delta: None,
        rewind: None,
        event_name_filter: None,
        annotation_subscribe,
    }
}

async fn subscribe(
    harness: &AnnotationHarness,
    socket_id: &SocketId,
    reader: &mut ClientReader,
    annotation_subscribe: bool,
) {
    harness
        .handler
        .handle_subscribe_request(
            socket_id,
            &harness.app,
            subscribe_request(annotation_subscribe),
        )
        .await
        .unwrap();
    let ack = recv_message(reader).await;
    assert_eq!(
        ack.event.as_deref(),
        Some("sockudo_internal:subscription_succeeded")
    );
}

async fn recv_message(reader: &mut ClientReader) -> sockudo_protocol::messages::PusherMessage {
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

async fn recv_event(
    reader: &mut ClientReader,
    event_name: &str,
) -> sockudo_protocol::messages::PusherMessage {
    loop {
        let message = recv_message(reader).await;
        if message.event.as_deref() == Some(event_name) {
            return message;
        }
    }
}

async fn expect_no_message(reader: &mut ClientReader) {
    match timeout(Duration::from_millis(150), reader.next()).await {
        Err(_) | Ok(None) => {}
        Ok(Some(Ok(frame))) => panic!("unexpected websocket frame: {frame:?}"),
        Ok(Some(Err(err))) => panic!("unexpected websocket read error: {err:?}"),
    }
}

async fn publish_annotation(
    harness: &AnnotationHarness,
    annotation_type_value: &str,
    name: Option<&str>,
    client_id: Option<&str>,
    count: Option<u64>,
) -> sockudo_adapter::handler::annotations::PublishAnnotationRuntimeResult {
    harness
        .handler
        .publish_annotation_runtime(PublishAnnotationRuntimeRequest {
            app: harness.app.clone(),
            channel: CHANNEL.to_string(),
            message_serial: message_serial(),
            annotation_type: annotation_type(annotation_type_value),
            name: name.map(str::to_string),
            client_id: client_id.map(str::to_string),
            count,
            data: None,
            encoding: None,
        })
        .await
        .unwrap()
}

async fn delete_annotation(harness: &AnnotationHarness, serial: AnnotationSerial) {
    harness
        .handler
        .delete_annotation_runtime(DeleteAnnotationRuntimeRequest {
            app: harness.app.clone(),
            channel: CHANNEL.to_string(),
            message_serial: message_serial(),
            target_serial: serial,
        })
        .await
        .unwrap();
}

#[test]
fn total_summary_create_delete_and_duplicate_client_counts() {
    let summary = projection(
        "reactions:total.v1",
        vec![
            event(
                "ann:1",
                "reactions:total.v1",
                AnnotationAction::Create,
                None,
                Some("client-1"),
                None,
            ),
            event(
                "ann:2",
                "reactions:total.v1",
                AnnotationAction::Create,
                None,
                Some("client-1"),
                None,
            ),
            event(
                "ann:3",
                "reactions:total.v1",
                AnnotationAction::Delete,
                None,
                Some("client-1"),
                None,
            ),
        ],
    );

    assert_eq!(
        summary,
        AnnotationSummary::Total(sockudo_core::annotations::TotalAnnotationSummary { total: 1 })
    );
}

#[test]
fn flag_summary_deduplicates_deletes_and_requires_client() {
    let missing_client = event(
        "ann:0",
        "reactions:flag.v1",
        AnnotationAction::Create,
        None,
        None,
        None,
    )
    .validate()
    .unwrap_err();
    assert!(missing_client.to_string().contains("client_id is required"));

    let summary = projection(
        "reactions:flag.v1",
        vec![
            event(
                "ann:1",
                "reactions:flag.v1",
                AnnotationAction::Create,
                None,
                Some("client-1"),
                None,
            ),
            event(
                "ann:2",
                "reactions:flag.v1",
                AnnotationAction::Create,
                None,
                Some("client-1"),
                None,
            ),
            event(
                "ann:3",
                "reactions:flag.v1",
                AnnotationAction::Create,
                None,
                Some("client-2"),
                None,
            ),
            event(
                "ann:4",
                "reactions:flag.v1",
                AnnotationAction::Delete,
                None,
                Some("client-1"),
                None,
            ),
        ],
    );

    assert_eq!(
        summary,
        AnnotationSummary::Flag(IdentifiedAnnotationSummary {
            total: 1,
            client_ids: vec!["client-2".to_string()],
            clipped: false,
        })
    );
}

#[test]
fn distinct_summary_tracks_per_name_buckets_and_named_deletes() {
    let summary = projection(
        "reactions:distinct.v1",
        vec![
            event(
                "ann:1",
                "reactions:distinct.v1",
                AnnotationAction::Create,
                Some("like"),
                Some("client-1"),
                None,
            ),
            event(
                "ann:2",
                "reactions:distinct.v1",
                AnnotationAction::Create,
                Some("laugh"),
                Some("client-1"),
                None,
            ),
            event(
                "ann:3",
                "reactions:distinct.v1",
                AnnotationAction::Create,
                Some("like"),
                Some("client-2"),
                None,
            ),
            event(
                "ann:4",
                "reactions:distinct.v1",
                AnnotationAction::Delete,
                Some("like"),
                Some("client-1"),
                None,
            ),
        ],
    );

    let AnnotationSummary::Distinct(names) = summary else {
        panic!("expected distinct summary");
    };
    assert_eq!(names["like"].client_ids, vec!["client-2".to_string()]);
    assert_eq!(names["laugh"].client_ids, vec!["client-1".to_string()]);
}

#[test]
fn unique_summary_moves_client_between_names() {
    let summary = projection(
        "reactions:unique.v1",
        vec![
            event(
                "ann:1",
                "reactions:unique.v1",
                AnnotationAction::Create,
                Some("like"),
                Some("client-1"),
                None,
            ),
            event(
                "ann:2",
                "reactions:unique.v1",
                AnnotationAction::Create,
                Some("laugh"),
                Some("client-1"),
                None,
            ),
        ],
    );

    let AnnotationSummary::Unique(names) = summary else {
        panic!("expected unique summary");
    };
    assert!(!names.contains_key("like"));
    assert_eq!(names["laugh"].client_ids, vec!["client-1".to_string()]);
}

#[test]
fn multiple_summary_tracks_counts_unidentified_and_delete_by_client() {
    let summary = projection(
        "moderation:multiple.v1",
        vec![
            event(
                "ann:1",
                "moderation:multiple.v1",
                AnnotationAction::Create,
                Some("spam"),
                Some("client-1"),
                Some(2),
            ),
            event(
                "ann:2",
                "moderation:multiple.v1",
                AnnotationAction::Create,
                Some("spam"),
                Some("client-2"),
                Some(3),
            ),
            event(
                "ann:3",
                "moderation:multiple.v1",
                AnnotationAction::Create,
                Some("spam"),
                None,
                Some(4),
            ),
            event(
                "ann:4",
                "moderation:multiple.v1",
                AnnotationAction::Delete,
                Some("spam"),
                Some("client-1"),
                None,
            ),
        ],
    );

    let AnnotationSummary::Multiple(names) = summary else {
        panic!("expected multiple summary");
    };
    assert_eq!(
        names["spam"],
        MultipleAnnotationSummary {
            total: 7,
            client_counts: BTreeMap::from([("client-2".to_string(), 3)]),
            total_unidentified: 4,
            clipped: false,
            total_client_ids: 1,
        }
    );
}

#[test]
fn identified_summary_clips_client_ids_at_configured_limit() {
    let events = (1..=5)
        .map(|idx| {
            event(
                &format!("ann:{idx}"),
                "reactions:flag.v1",
                AnnotationAction::Create,
                None,
                Some(&format!("client-{idx}")),
                None,
            )
        })
        .collect();

    let summary = projection_with_options(
        "reactions:flag.v1",
        events,
        AnnotationProjectionOptions {
            client_id_limit: Some(3),
        },
    );

    let AnnotationSummary::Flag(summary) = summary else {
        panic!("expected flag summary");
    };
    assert!(summary.clipped);
    assert_eq!(summary.total, 5);
    assert_eq!(summary.client_ids.len(), 3);
}

#[test]
fn multiple_summary_clips_client_counts_and_preserves_total_client_ids() {
    let events = (1..=5)
        .map(|idx| {
            event(
                &format!("ann:{idx}"),
                "moderation:multiple.v1",
                AnnotationAction::Create,
                Some("spam"),
                Some(&format!("client-{idx}")),
                Some(1),
            )
        })
        .collect();

    let summary = projection_with_options(
        "moderation:multiple.v1",
        events,
        AnnotationProjectionOptions {
            client_id_limit: Some(3),
        },
    );

    let AnnotationSummary::Multiple(names) = summary else {
        panic!("expected multiple summary");
    };
    assert!(names["spam"].clipped);
    assert_eq!(names["spam"].total, 5);
    assert_eq!(names["spam"].client_counts.len(), 3);
    assert_eq!(names["spam"].total_client_ids, 5);
}

#[tokio::test]
async fn publish_annotation_delivers_summary_and_delete_delivers_decrement() {
    let harness = build_harness().await;
    let (socket_id, mut reader) = connect_v2_socket(&harness).await;
    subscribe(&harness, &socket_id, &mut reader, false).await;

    let published = publish_annotation(&harness, "reactions:total.v1", None, None, None).await;
    let summary = recv_event(&mut reader, MESSAGE_SUMMARY_EVENT_NAME).await;
    assert_eq!(
        summary_payload(&summary)["reactions:total.v1"]["total"].as_u64(),
        Some(1)
    );

    delete_annotation(&harness, published.annotation_serial).await;
    let summary = recv_event(&mut reader, MESSAGE_SUMMARY_EVENT_NAME).await;
    assert_eq!(
        summary_payload(&summary)["reactions:total.v1"]["total"].as_u64(),
        Some(0)
    );
}

#[tokio::test]
async fn flag_publish_rejects_unidentified_client() {
    let harness = build_harness().await;

    let result = harness
        .handler
        .publish_annotation_runtime(PublishAnnotationRuntimeRequest {
            app: harness.app.clone(),
            channel: CHANNEL.to_string(),
            message_serial: message_serial(),
            annotation_type: annotation_type("reactions:flag.v1"),
            name: None,
            client_id: None,
            count: None,
            data: None,
            encoding: None,
        })
        .await;

    let err = match result {
        Err(err) => err,
        Ok(_) => panic!("expected unidentified flag publish to fail"),
    };
    assert!(err.to_string().contains("annotation client_id is required"));
}

#[tokio::test]
async fn annotation_subscribe_receives_raw_events_but_ordinary_subscribe_does_not() {
    let harness = build_harness().await;
    let (ordinary_socket, mut ordinary_reader) = connect_v2_socket(&harness).await;
    let (raw_socket, mut raw_reader) = connect_v2_socket(&harness).await;

    sign_in_with_capabilities(
        &harness,
        &raw_socket,
        "raw-subscriber",
        ConnectionCapabilities {
            subscribe: Some(vec![CHANNEL.to_string()]),
            annotation_subscribe: Some(vec![CHANNEL.to_string()]),
            ..Default::default()
        },
    )
    .await;

    subscribe(&harness, &ordinary_socket, &mut ordinary_reader, false).await;
    subscribe(&harness, &raw_socket, &mut raw_reader, true).await;

    publish_annotation(
        &harness,
        "reactions:distinct.v1",
        Some("like"),
        Some("client-1"),
        None,
    )
    .await;

    let ordinary_summary = recv_event(&mut ordinary_reader, MESSAGE_SUMMARY_EVENT_NAME).await;
    assert_eq!(
        summary_payload(&ordinary_summary)["reactions:distinct.v1"]["like"]["total"].as_u64(),
        Some(1)
    );
    expect_no_message(&mut ordinary_reader).await;

    let raw_summary = recv_event(&mut raw_reader, MESSAGE_SUMMARY_EVENT_NAME).await;
    assert_eq!(
        summary_payload(&raw_summary)["reactions:distinct.v1"]["like"]["total"].as_u64(),
        Some(1)
    );
    let raw = recv_event(&mut raw_reader, ANNOTATION_EVENT_NAME).await;
    let raw_payload = message_data(&raw);
    assert_eq!(raw_payload["action"].as_str(), Some("annotation.create"));
    assert_eq!(raw_payload["type"].as_str(), Some("reactions:distinct.v1"));
}

#[tokio::test]
async fn reconnect_subscription_receives_coherent_summary_snapshot() {
    let harness = build_harness().await;
    publish_annotation(
        &harness,
        "reactions:distinct.v1",
        Some("like"),
        Some("client-1"),
        None,
    )
    .await;
    publish_annotation(
        &harness,
        "reactions:distinct.v1",
        Some("like"),
        Some("client-2"),
        None,
    )
    .await;

    let (socket_id, mut reader) = connect_v2_socket(&harness).await;
    subscribe(&harness, &socket_id, &mut reader, false).await;

    let snapshot = recv_event(&mut reader, MESSAGE_SUMMARY_EVENT_NAME).await;
    assert_eq!(
        summary_payload(&snapshot)["reactions:distinct.v1"]["like"]["total"].as_u64(),
        Some(2)
    );
}

#[tokio::test]
async fn reconnect_subscription_marks_projection_rebuild_metrics_on_cache_miss() {
    let version_store = Arc::new(MemoryVersionStore::new());
    version_store
        .append_version(versioned_record())
        .await
        .unwrap();
    let inner_store = Arc::new(MemoryAnnotationStore::new());
    inner_store
        .append_event(StoredAnnotationEvent {
            app_id: APP_ID.to_string(),
            channel_id: CHANNEL.to_string(),
            stored_at_ms: now_ms(),
            annotation: event(
                "ann:1",
                "reactions:distinct.v1",
                AnnotationAction::Create,
                Some("like"),
                Some("client-1"),
                None,
            ),
        })
        .await
        .unwrap();
    let annotation_store = Arc::new(RebuildReportingAnnotationStore {
        inner: inner_store,
        rebuild_count: 1,
    });
    let harness = build_harness_with_shared_stores(version_store, annotation_store).await;

    let (socket_id, mut reader) = connect_v2_socket(&harness).await;
    subscribe(&harness, &socket_id, &mut reader, false).await;

    let snapshot = recv_event(&mut reader, MESSAGE_SUMMARY_EVENT_NAME).await;
    assert_eq!(
        summary_payload(&snapshot)["reactions:distinct.v1"]["like"]["total"].as_u64(),
        Some(1)
    );
    assert_eq!(harness.metrics.annotation_projection_rebuilds(), 1);
    assert_eq!(harness.metrics.annotation_projection_rebuild_durations(), 1);
}

#[tokio::test]
async fn shared_annotation_store_converges_under_two_node_concurrent_publishes() {
    let version_store = Arc::new(MemoryVersionStore::new());
    version_store
        .append_version(versioned_record())
        .await
        .unwrap();
    let annotation_store = Arc::new(MemoryAnnotationStore::new());
    let node_a =
        build_harness_with_shared_stores(version_store.clone(), annotation_store.clone()).await;
    let node_b = build_harness_with_shared_stores(version_store, annotation_store).await;

    let publish_a = publish_annotation(
        &node_a,
        "reactions:distinct.v1",
        Some("like"),
        Some("client-a"),
        None,
    );
    let publish_b = publish_annotation(
        &node_b,
        "reactions:distinct.v1",
        Some("like"),
        Some("client-b"),
        None,
    );

    let (result_a, result_b) = tokio::join!(publish_a, publish_b);
    assert_ne!(result_a.annotation_serial, result_b.annotation_serial);

    let request = AnnotationProjectionRequest {
        app_id: APP_ID.to_string(),
        channel_id: CHANNEL.to_string(),
        message_serial: message_serial(),
        annotation_type: annotation_type("reactions:distinct.v1"),
    };
    let projection_a = node_a
        .handler
        .annotation_store()
        .get_projection(request.clone())
        .await
        .unwrap()
        .unwrap();
    let projection_b = node_b
        .handler
        .annotation_store()
        .get_projection(request)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(projection_a.summary, projection_b.summary);
    let AnnotationSummary::Distinct(names) = projection_a.summary else {
        panic!("expected distinct summary");
    };
    assert_eq!(names["like"].total, 2);
    assert_eq!(
        names["like"].client_ids,
        vec!["client-a".to_string(), "client-b".to_string()]
    );
}

#[tokio::test]
async fn shared_annotation_store_converges_after_node_failover_delete_and_replay() {
    let version_store = Arc::new(MemoryVersionStore::new());
    version_store
        .append_version(versioned_record())
        .await
        .unwrap();
    let annotation_store = Arc::new(MemoryAnnotationStore::new());
    let node_a =
        build_harness_with_shared_stores(version_store.clone(), annotation_store.clone()).await;
    let node_b = build_harness_with_shared_stores(version_store, annotation_store.clone()).await;

    let result_a = publish_annotation(
        &node_a,
        "reactions:distinct.v1",
        Some("like"),
        Some("client-a"),
        None,
    )
    .await;
    publish_annotation(
        &node_b,
        "reactions:distinct.v1",
        Some("like"),
        Some("client-b"),
        None,
    )
    .await;

    let replayed = annotation_store
        .get_event_by_serial(AnnotationEventLookupRequest {
            app_id: APP_ID.to_string(),
            channel_id: CHANNEL.to_string(),
            annotation_serial: result_a.annotation_serial.clone(),
        })
        .await
        .unwrap()
        .unwrap();
    annotation_store.append_event(replayed).await.unwrap();

    delete_annotation(&node_b, result_a.annotation_serial).await;

    let request = AnnotationProjectionRequest {
        app_id: APP_ID.to_string(),
        channel_id: CHANNEL.to_string(),
        message_serial: message_serial(),
        annotation_type: annotation_type("reactions:distinct.v1"),
    };
    let projection_a = node_a
        .handler
        .annotation_store()
        .get_projection(request.clone())
        .await
        .unwrap()
        .unwrap();
    let projection_b = node_b
        .handler
        .annotation_store()
        .get_projection(request)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(projection_a.summary, projection_b.summary);
    let AnnotationSummary::Distinct(names) = projection_a.summary else {
        panic!("expected distinct summary");
    };
    assert_eq!(names["like"].total, 1);
    assert_eq!(names["like"].client_ids, vec!["client-b".to_string()]);
}
