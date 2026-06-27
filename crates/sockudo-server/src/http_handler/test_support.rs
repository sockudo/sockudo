use async_trait::async_trait;
use axum::{
    Json,
    extract::{Extension, Path, Query, RawQuery, State},
    http::{HeaderMap, HeaderValue, StatusCode, Uri, header},
    response::IntoResponse,
};
use sockudo_adapter::ConnectionHandler;
use sockudo_adapter::ConnectionHandlerBuilder;
use sockudo_adapter::local_adapter::LocalAdapter;
use sockudo_app::memory_app_manager::MemoryAppManager;
use sockudo_cache::memory_cache_manager::MemoryCacheManager;
#[cfg(feature = "push")]
use sockudo_core::app::ChannelNamespace;
use sockudo_core::app::{App, AppChannelsPolicy, AppManager, AppPolicy};
use sockudo_core::history::{HistoryStore, MemoryHistoryStore, MemoryHistoryStoreConfig};
use sockudo_core::options::MemoryCacheOptions;
use sockudo_core::presence_history::{
    MemoryPresenceHistoryStore, PresenceHistoryDirection, PresenceHistoryDurableState,
    PresenceHistoryEventCause, PresenceHistoryEventKind, PresenceHistoryQueryBounds,
    PresenceHistoryReadRequest, PresenceHistoryResetResult, PresenceHistoryRetentionPolicy,
    PresenceHistoryRuntimeStatus, PresenceHistoryStore, PresenceHistoryStreamInspection,
    PresenceHistoryStreamRuntimeState, PresenceHistoryTransitionRecord,
};
use sockudo_core::version_store::{StoredVersionRecord, VersionStore};
use sockudo_core::versioned_messages::{
    MessageSerial, VersionMetadata, VersionSerial, VersionedMessage,
};
use sockudo_core::websocket::{ConnectionCapabilities, SocketId, UserInfo, WebSocketBufferConfig};
use sockudo_protocol::messages::{
    AiExtras, ApiMessageData, MessageData, MessageExtras, PusherApiMessage, PusherMessage,
};
use sockudo_protocol::{ProtocolVersion, WireFormat};
#[cfg(feature = "push")]
use sockudo_push::{
    DeviceDetails, DevicePushDetails, DevicePushState, FormFactor, Platform, PushRecipient,
    SecretString, hash_device_identity_token,
};
use sockudo_ws::axum_integration::WebSocketWriter;
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, Stream as WsStream, WebSocketStream};
use sonic_rs::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

use super::{EventQuery, events};

#[cfg(feature = "push")]
pub(crate) fn test_push_store() -> Extension<sockudo_push::DynPushStore> {
    Extension(Arc::new(sockudo_push::MemoryPushStore::new()))
}

#[cfg(feature = "push")]
pub(crate) fn test_push_queue() -> Extension<sockudo_push::DynPushQueue> {
    Extension(Arc::new(sockudo_push::MemoryPushQueue::new()))
}

pub(crate) fn test_app() -> App {
    test_app_with_policy(Default::default())
}

pub(crate) fn test_annotation_app() -> App {
    test_app_with_policy(AppPolicy {
        channels: AppChannelsPolicy {
            annotations_enabled: Some(true),
            ..Default::default()
        },
        ..Default::default()
    })
}

#[cfg(feature = "push")]
pub(crate) fn test_notifications_app() -> App {
    test_app_with_policy(AppPolicy {
        channels: AppChannelsPolicy {
            channel_namespaces: Some(vec![ChannelNamespace {
                name: "notifications".to_string(),
                channel_name_pattern: None,
                max_channel_name_length: None,
                annotations_enabled: None,
                allow_user_limited_channels: None,
                allow_subscribe_for_client: None,
                allow_publish_for_client: None,
                allow_presence_for_client: None,
                history: None,
                presence_history: None,
            }]),
            ..Default::default()
        },
        ..Default::default()
    })
}

pub(crate) fn test_app_with_policy(policy: AppPolicy) -> App {
    App::from_policy(
        "app-1".to_string(),
        "key".to_string(),
        "secret".to_string(),
        true,
        policy,
    )
}

pub(crate) fn test_history_message(channel: &str, index: usize) -> PusherMessage {
    PusherMessage {
        event: Some(format!("message-created-{index}")),
        channel: Some(channel.to_string()),
        data: Some(MessageData::String(format!("{{\"text\":\"msg-{index}\"}}"))),
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
    }
}

pub(crate) fn test_history_handler(max_page_size: usize) -> Arc<ConnectionHandler> {
    test_history_handler_with_store(
        max_page_size,
        Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default())),
    )
}

pub(crate) fn test_history_handler_with_memory_store(
    max_page_size: usize,
) -> (Arc<ConnectionHandler>, Arc<MemoryHistoryStore>) {
    let store = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
    (
        test_history_handler_with_store(max_page_size, store.clone()),
        store,
    )
}

pub(crate) fn test_history_handler_with_store(
    max_page_size: usize,
    history_store: Arc<dyn HistoryStore + Send + Sync>,
) -> Arc<ConnectionHandler> {
    let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
    let adapter =
        Arc::new(LocalAdapter::new()) as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
    let cache = Arc::new(MemoryCacheManager::new(
        "test".to_string(),
        MemoryCacheOptions::default(),
    ));

    let mut options = sockudo_core::options::ServerOptions::default();
    options.history.enabled = true;
    options.history.max_page_size = max_page_size;

    Arc::new(
        ConnectionHandlerBuilder::new(app_manager, adapter, cache, options)
            .history_store(history_store)
            .build(),
    )
}

pub(crate) fn test_presence_history_handler(max_page_size: usize) -> Arc<ConnectionHandler> {
    test_presence_history_handler_with_store(
        max_page_size,
        Arc::new(MemoryPresenceHistoryStore::new(Default::default())),
    )
}

pub(crate) fn test_presence_history_handler_with_memory_store(
    max_page_size: usize,
) -> (Arc<ConnectionHandler>, Arc<MemoryPresenceHistoryStore>) {
    let store = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
    (
        test_presence_history_handler_with_store(max_page_size, store.clone()),
        store,
    )
}

pub(crate) fn test_presence_history_handler_with_store(
    max_page_size: usize,
    presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
) -> Arc<ConnectionHandler> {
    let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
    let adapter =
        Arc::new(LocalAdapter::new()) as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
    let cache = Arc::new(MemoryCacheManager::new(
        "test".to_string(),
        MemoryCacheOptions::default(),
    ));

    let mut options = sockudo_core::options::ServerOptions::default();
    options.presence_history.enabled = true;
    options.presence_history.max_page_size = max_page_size;

    Arc::new(
        ConnectionHandlerBuilder::new(app_manager, adapter, cache, options)
            .presence_history_store(presence_history_store)
            .build(),
    )
}

pub(crate) fn test_versioned_handler_harness(
    max_page_size: usize,
    version_store: Arc<dyn VersionStore + Send + Sync>,
) -> (
    Arc<ConnectionHandler>,
    Arc<LocalAdapter>,
    Arc<MemoryAppManager>,
) {
    test_versioned_handler_harness_with_annotations(max_page_size, version_store, true)
}

pub(crate) fn test_versioned_handler_harness_with_annotations(
    max_page_size: usize,
    version_store: Arc<dyn VersionStore + Send + Sync>,
    annotations_enabled: bool,
) -> (
    Arc<ConnectionHandler>,
    Arc<LocalAdapter>,
    Arc<MemoryAppManager>,
) {
    let app_manager = Arc::new(MemoryAppManager::new());
    let adapter = Arc::new(LocalAdapter::new());
    let app_manager_dyn = app_manager.clone() as Arc<dyn AppManager + Send + Sync>;
    let adapter_dyn = adapter.clone() as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
    let cache = Arc::new(MemoryCacheManager::new(
        "test".to_string(),
        MemoryCacheOptions::default(),
    ));

    let mut options = sockudo_core::options::ServerOptions::default();
    options.versioned_messages.enabled = true;
    options.versioned_messages.max_page_size = max_page_size;
    options.history.enabled = true;
    options.annotations.enabled = annotations_enabled;

    let handler = Arc::new(
        ConnectionHandlerBuilder::new(app_manager_dyn, adapter_dyn, cache, options)
            .history_store(Arc::new(MemoryHistoryStore::new(
                MemoryHistoryStoreConfig::default(),
            )))
            .version_store(version_store)
            .build(),
    );

    (handler, adapter, app_manager)
}

pub(crate) fn test_versioned_handler_with_store(
    max_page_size: usize,
    version_store: Arc<dyn VersionStore + Send + Sync>,
) -> Arc<ConnectionHandler> {
    let (handler, _, _) = test_versioned_handler_harness(max_page_size, version_store);
    handler
}

pub(crate) fn test_ai_versioned_handler_with_store(
    max_page_size: usize,
    version_store: Arc<dyn VersionStore + Send + Sync>,
    max_accumulated_message_bytes: usize,
    max_appends_per_message: usize,
    max_open_streaming_messages_per_channel: usize,
) -> Arc<ConnectionHandler> {
    let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
    let adapter =
        Arc::new(LocalAdapter::new()) as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
    let cache = Arc::new(MemoryCacheManager::new(
        "test".to_string(),
        MemoryCacheOptions::default(),
    ));

    let mut options = sockudo_core::options::ServerOptions::default();
    options.versioned_messages.enabled = true;
    options.versioned_messages.max_page_size = max_page_size;
    options.history.enabled = true;
    options.ai_transport.enabled = true;
    options.ai_transport.channels = vec![sockudo_core::options::AiTransportChannelConfig {
        prefix: "versioned-".to_string(),
    }];
    options.ai_transport.max_accumulated_message_bytes = max_accumulated_message_bytes;
    options.ai_transport.max_appends_per_message = max_appends_per_message;
    options.ai_transport.max_open_streaming_messages_per_channel =
        max_open_streaming_messages_per_channel;

    Arc::new(
        ConnectionHandlerBuilder::new(app_manager, adapter, cache, options)
            .history_store(Arc::new(MemoryHistoryStore::new(
                MemoryHistoryStoreConfig::default(),
            )))
            .version_store(version_store)
            .build(),
    )
}

#[cfg(feature = "push")]
pub(crate) fn test_handler_with_push_rules(
    rules: Vec<sockudo_core::options::PushRuleConfig>,
) -> Arc<ConnectionHandler> {
    let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
    let adapter =
        Arc::new(LocalAdapter::new()) as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
    let cache = Arc::new(MemoryCacheManager::new(
        "test".to_string(),
        MemoryCacheOptions::default(),
    ));
    let options = sockudo_core::options::ServerOptions {
        push_rules: rules,
        ..Default::default()
    };

    Arc::new(ConnectionHandlerBuilder::new(app_manager, adapter, cache, options).build())
}

#[cfg(feature = "push")]
pub(crate) fn test_push_device(device_id: &str) -> DeviceDetails {
    DeviceDetails {
        app_id: "app-1".to_string(),
        id: device_id.to_string(),
        client_id: Some("user-1".to_string()),
        form_factor: FormFactor::Phone,
        platform: Platform::Android,
        metadata: sonic_rs::json!({}),
        device_secret: hash_device_identity_token(&SecretString::new("device-token").unwrap()),
        timezone: "UTC".to_string(),
        locale: "en".to_string(),
        last_active_at_ms: 1,
        push: DevicePushDetails {
            recipient: PushRecipient::Fcm {
                registration_token: SecretString::new(format!("token-{device_id}")).unwrap(),
            },
            state: DevicePushState::Active,
            failure_count: 0,
            error_reason: None,
        },
        push_rate_policy: None,
    }
}

pub(crate) fn test_versioned_record(
    message_serial: &str,
    version_serial: &str,
    history_serial: u64,
    delivery_serial: u64,
    text: &str,
) -> StoredVersionRecord {
    StoredVersionRecord {
        app_id: "app-1".to_string(),
        channel: "versioned-room".to_string(),
        original_client_id: Some("user-1".to_string()),
        message: VersionedMessage::new_create(
            MessageSerial::new(message_serial.to_string()).unwrap(),
            VersionMetadata {
                serial: VersionSerial::new(version_serial.to_string()).unwrap(),
                client_id: Some("user-1".to_string()),
                timestamp_ms: 1,
                description: None,
                metadata: None,
            },
            history_serial,
            delivery_serial,
            Some("chat.message".to_string()),
            Some(MessageData::String(format!("{{\"text\":\"{text}\"}}"))),
            None,
        ),
    }
}

pub(crate) fn ai_extras(status: &str) -> MessageExtras {
    MessageExtras {
        ai: Some(AiExtras {
            transport: Some(HashMap::from([("status".to_string(), status.to_string())])),
            codec: Some(HashMap::from([(
                "content-type".to_string(),
                "text/plain".to_string(),
            )])),
        }),
        ..Default::default()
    }
}

pub(crate) async fn publish_ai_http_event(
    handler: Arc<ConnectionHandler>,
    app: App,
    event_name: &str,
    message_id: Option<&str>,
    idempotency_header: Option<&str>,
    status: &str,
    data: &str,
) -> Value {
    let mut headers = HeaderMap::new();
    if let Some(key) = idempotency_header {
        headers.insert(
            header::HeaderName::from_static("x-idempotency-key"),
            HeaderValue::from_str(key).unwrap(),
        );
    }

    let response = events(
        Path("app-1".to_string()),
        Query(empty_event_query()),
        Extension(app),
        #[cfg(feature = "push")]
        test_push_store(),
        #[cfg(feature = "push")]
        test_push_queue(),
        State(handler),
        headers,
        Uri::from_static("/apps/app-1/events"),
        RawQuery(None),
        Json(PusherApiMessage {
            name: Some(event_name.to_string()),
            data: Some(ApiMessageData::String(data.to_string())),
            channel: Some("versioned-room".to_string()),
            channels: None,
            socket_id: None,
            info: None,
            tags: None,
            delta: None,
            idempotency_key: None,
            message_id: message_id.map(str::to_string),
            extras: Some(ai_extras(status)),
        }),
    )
    .await
    .unwrap()
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    sonic_rs::from_slice(&body).unwrap()
}

pub(crate) async fn test_websocket_writer() -> WebSocketWriter {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = sockudo_ws::handshake::server_handshake(&mut stream)
            .await
            .unwrap();
        let ws = sockudo_ws::axum_integration::WebSocket::from_tcp(stream, WsConfig::default());
        let (_reader, writer) = ws.split();
        writer
    });

    let client_stream = TcpStream::connect(addr).await.unwrap();
    let client = WebSocketClient::<Http1>::new(WsConfig::default());
    let (client_ws, _): (WebSocketStream<WsStream<Http1>>, _) = client
        .connect(client_stream, &addr.to_string(), "/", None)
        .await
        .unwrap();
    let (_reader, _writer) = client_ws.split();

    server_task.await.unwrap()
}

pub(crate) fn empty_event_query() -> EventQuery {
    EventQuery {
        auth_key: String::new(),
        auth_timestamp: String::new(),
        auth_version: String::new(),
        body_md5: String::new(),
        auth_signature: String::new(),
    }
}

pub(crate) fn test_realtime_handler_harness() -> (Arc<ConnectionHandler>, Arc<MemoryAppManager>) {
    let app_manager = Arc::new(MemoryAppManager::new());
    let adapter = Arc::new(LocalAdapter::new());
    let app_manager_dyn = app_manager.clone() as Arc<dyn AppManager + Send + Sync>;
    let adapter_dyn = adapter.clone() as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
    let cache = Arc::new(MemoryCacheManager::new(
        "test".to_string(),
        MemoryCacheOptions::default(),
    ));
    let handler = Arc::new(
        ConnectionHandlerBuilder::new(
            app_manager_dyn,
            adapter_dyn,
            cache,
            sockudo_core::options::ServerOptions::default(),
        )
        .build(),
    );

    (handler, app_manager)
}

pub(crate) async fn attach_signed_in_mutation_actor(
    handler: &Arc<ConnectionHandler>,
    app_manager: Arc<MemoryAppManager>,
    app: &App,
    socket_id: SocketId,
    user_id: &str,
    capabilities: ConnectionCapabilities,
) {
    app_manager.create_app(app.clone()).await.unwrap();

    handler
        .connection_manager()
        .add_socket(
            socket_id,
            test_websocket_writer().await,
            &app.id,
            app_manager as Arc<dyn AppManager + Send + Sync>,
            WebSocketBufferConfig::default(),
            ProtocolVersion::V2,
            WireFormat::Json,
            true,
            sockudo_protocol::AppendMode::Delta,
        )
        .await
        .unwrap();

    handler
        .update_connection_with_user_info(
            &socket_id,
            app,
            &UserInfo {
                id: user_id.to_string(),
                watchlist: None,
                info: None,
                capabilities: Some(capabilities),
                meta: None,
            },
        )
        .await
        .unwrap();
}

pub(crate) async fn seed_presence_history(
    store: &Arc<MemoryPresenceHistoryStore>,
    app_id: &str,
    channel: &str,
    count: usize,
    base_ts: i64,
) {
    for index in 1..=count {
        store
            .record_transition(PresenceHistoryTransitionRecord {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                event_kind: if index % 2 == 0 {
                    PresenceHistoryEventKind::MemberRemoved
                } else {
                    PresenceHistoryEventKind::MemberAdded
                },
                cause: if index % 2 == 0 {
                    PresenceHistoryEventCause::Disconnect
                } else {
                    PresenceHistoryEventCause::Join
                },
                user_id: format!("user-{index}"),
                connection_id: Some(format!("socket-{index}")),
                user_info: Some(sonic_rs::json!({ "n": index })),
                dead_node_id: None,
                dedupe_key: format!("transition-{index}"),
                published_at_ms: base_ts + index as i64,
                retention: PresenceHistoryRetentionPolicy {
                    retention_window_seconds: 3600,
                    max_events_per_channel: None,
                    max_bytes_per_channel: None,
                },
            })
            .await
            .unwrap();
    }
}

#[derive(Clone)]
pub(crate) struct InspectablePresenceHistoryStore {
    pub(crate) inner: Arc<MemoryPresenceHistoryStore>,
    pub(crate) state: PresenceHistoryStreamRuntimeState,
}

#[async_trait]
impl PresenceHistoryStore for InspectablePresenceHistoryStore {
    async fn record_transition(
        &self,
        record: PresenceHistoryTransitionRecord,
    ) -> sockudo_core::error::Result<()> {
        self.inner.record_transition(record).await
    }

    async fn read_page(
        &self,
        request: PresenceHistoryReadRequest,
    ) -> sockudo_core::error::Result<sockudo_core::presence_history::PresenceHistoryPage> {
        let mut page = self.inner.read_page(request).await?;
        if !self.state.continuity_proven {
            page.complete = false;
            page.degraded = true;
        }
        Ok(page)
    }

    async fn runtime_status(&self) -> sockudo_core::error::Result<PresenceHistoryRuntimeStatus> {
        Ok(PresenceHistoryRuntimeStatus {
            enabled: true,
            backend: "memory".to_string(),
            state_authority: "test_state".to_string(),
            degraded_channels: usize::from(
                self.state.durable_state != PresenceHistoryDurableState::Healthy,
            ),
            reset_required_channels: usize::from(self.state.reset_required),
            queue_depth: 0,
        })
    }

    async fn stream_runtime_state(
        &self,
        _app_id: &str,
        _channel: &str,
    ) -> sockudo_core::error::Result<PresenceHistoryStreamRuntimeState> {
        Ok(self.state.clone())
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> sockudo_core::error::Result<PresenceHistoryStreamInspection> {
        Ok(PresenceHistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: self.state.stream_id.clone(),
            next_serial: Some(10),
            retained: self
                .inner
                .read_page(PresenceHistoryReadRequest {
                    app_id: app_id.to_string(),
                    channel: channel.to_string(),
                    direction: PresenceHistoryDirection::NewestFirst,
                    limit: 1,
                    cursor: None,
                    bounds: PresenceHistoryQueryBounds::default(),
                })
                .await
                .map(|page| page.retained)
                .unwrap_or_default(),
            state: self.state.clone(),
        })
    }

    async fn reset_stream(
        &self,
        app_id: &str,
        channel: &str,
        reason: &str,
        requested_by: Option<&str>,
    ) -> sockudo_core::error::Result<PresenceHistoryResetResult> {
        self.inner
            .reset_stream(app_id, channel, reason, requested_by)
            .await
    }
}
