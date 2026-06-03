// src/adapter/handler/mod.rs
#[cfg(feature = "ai-transport")]
pub mod ai_observability;
#[cfg(feature = "ai-transport")]
pub mod ai_orphans;
pub mod annotations;
pub mod auth_tokens;
pub mod authentication;
pub mod connection_management;
mod core;
mod history_frames;
pub mod message_handlers;
pub mod origin_validation;
pub mod presence_update;
pub mod rate_limiting;
#[cfg(feature = "recovery")]
pub mod recovery;
pub mod signin_management;
pub mod subscription_management;
pub mod timeout_management;
pub mod types;
pub mod validation;
pub mod webhook_management;

use crate::ConnectionManager;
use crate::presence::PresenceManager;
use crate::watchlist::WatchlistManager;
#[cfg(feature = "ai-transport")]
use sockudo_ai_transport::{RollupConfig, RollupEngine, observability::AiObservabilityTracker};
use sockudo_core::annotations::{AnnotationStore, MemoryAnnotationStore};
use sockudo_core::app::App;
use sockudo_core::app::AppManager;
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::{HistoryStore, NoopHistoryStore};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::ServerOptions;
use sockudo_core::presence_history::{NoopPresenceHistoryStore, PresenceHistoryStore};
use sockudo_core::rate_limiter::RateLimiter;
use sockudo_core::version_store::{NoopVersionStore, VersionStore};
use sockudo_core::websocket::SocketId;
use sockudo_protocol::constants::CLIENT_EVENT_PREFIX;
use sockudo_protocol::messages::{MessageData, PusherMessage, is_ai_event};
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_webhook::WebhookIntegration;

use crate::handler::types::{ClientEventRequest, SignInRequest, SubscriptionRequest};
use dashmap::DashMap;
use sockudo_ws::Message;
use sockudo_ws::axum_integration::{WebSocket, WebSocketReader, WebSocketWriter};
use sonic_rs::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use tokio::sync::Semaphore;
use tracing::{debug, error, warn};

#[derive(Clone)]
pub struct ConnectionHandler {
    pub(crate) app_manager: Arc<dyn AppManager + Send + Sync>,
    pub(crate) connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
    pub(crate) local_adapter: Option<Arc<crate::local_adapter::LocalAdapter>>,
    pub(crate) cache_manager: Arc<dyn CacheManager + Send + Sync>,
    pub(crate) metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    pub(crate) history_store: Arc<dyn HistoryStore + Send + Sync>,
    pub(crate) annotation_store: Arc<dyn AnnotationStore + Send + Sync>,
    pub(crate) version_store: Arc<dyn VersionStore + Send + Sync>,
    #[cfg(feature = "ai-transport")]
    pub(crate) ai_rollup_engine: Option<Arc<RollupEngine>>,
    #[cfg(feature = "ai-transport")]
    pub(crate) ai_observability_tracker: Option<Arc<AiObservabilityTracker>>,
    pub(crate) presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
    webhook_integration: Option<Arc<WebhookIntegration>>,
    client_event_limiters: Arc<DashMap<SocketId, Arc<dyn RateLimiter + Send + Sync>>>,
    message_limiters: Arc<DashMap<SocketId, Arc<dyn RateLimiter + Send + Sync>>>,
    presence_update_limiters: Arc<DashMap<SocketId, Arc<dyn RateLimiter + Send + Sync>>>,
    history_request_limits: Arc<DashMap<SocketId, Arc<Semaphore>>>,
    watchlist_manager: Arc<WatchlistManager>,
    server_options: Arc<ServerOptions>,
    cleanup_queue: Option<crate::cleanup::CleanupSender>,
    cleanup_consecutive_failures: Arc<AtomicUsize>,
    cleanup_circuit_breaker_opened_at: Arc<AtomicU64>,
    #[cfg(feature = "delta")]
    delta_compression: Arc<sockudo_delta::DeltaCompressionManager>,
    /// Presence manager for race-safe presence channel operations
    pub(crate) presence_manager: Arc<PresenceManager>,
    #[cfg(feature = "recovery")]
    /// Replay buffer for connection recovery (enabled via config)
    pub(crate) replay_buffer: Option<Arc<crate::replay_buffer::ReplayBuffer>>,
}

/// Builder for constructing a `ConnectionHandler` without feature-gated function parameters.
pub struct ConnectionHandlerBuilder {
    app_manager: Arc<dyn AppManager + Send + Sync>,
    connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
    local_adapter: Option<Arc<crate::local_adapter::LocalAdapter>>,
    cache_manager: Arc<dyn CacheManager + Send + Sync>,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    history_store: Option<Arc<dyn HistoryStore + Send + Sync>>,
    annotation_store: Option<Arc<dyn AnnotationStore + Send + Sync>>,
    version_store: Option<Arc<dyn VersionStore + Send + Sync>>,
    presence_history_store: Option<Arc<dyn PresenceHistoryStore + Send + Sync>>,
    webhook_integration: Option<Arc<WebhookIntegration>>,
    server_options: ServerOptions,
    cleanup_queue: Option<crate::cleanup::CleanupSender>,
    #[cfg(feature = "delta")]
    delta_compression: Option<Arc<sockudo_delta::DeltaCompressionManager>>,
}

impl ConnectionHandlerBuilder {
    pub fn new(
        app_manager: Arc<dyn AppManager + Send + Sync>,
        connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
        cache_manager: Arc<dyn CacheManager + Send + Sync>,
        server_options: ServerOptions,
    ) -> Self {
        Self {
            app_manager,
            connection_manager,
            local_adapter: None,
            cache_manager,
            metrics: None,
            history_store: None,
            annotation_store: None,
            version_store: None,
            presence_history_store: None,
            webhook_integration: None,
            server_options,
            cleanup_queue: None,
            #[cfg(feature = "delta")]
            delta_compression: None,
        }
    }

    pub fn local_adapter(mut self, adapter: Arc<crate::local_adapter::LocalAdapter>) -> Self {
        self.local_adapter = Some(adapter);
        self
    }

    pub fn metrics(mut self, metrics: Arc<dyn MetricsInterface + Send + Sync>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn history_store(mut self, history_store: Arc<dyn HistoryStore + Send + Sync>) -> Self {
        self.history_store = Some(history_store);
        self
    }

    pub fn annotation_store(
        mut self,
        annotation_store: Arc<dyn AnnotationStore + Send + Sync>,
    ) -> Self {
        self.annotation_store = Some(annotation_store);
        self
    }

    pub fn version_store(mut self, version_store: Arc<dyn VersionStore + Send + Sync>) -> Self {
        self.version_store = Some(version_store);
        self
    }

    pub fn presence_history_store(
        mut self,
        presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
    ) -> Self {
        self.presence_history_store = Some(presence_history_store);
        self
    }

    pub fn webhook_integration(mut self, wh: Arc<WebhookIntegration>) -> Self {
        self.webhook_integration = Some(wh);
        self
    }

    pub fn cleanup_queue(mut self, queue: crate::cleanup::CleanupSender) -> Self {
        self.cleanup_queue = Some(queue);
        self
    }

    #[cfg(feature = "delta")]
    pub fn delta_compression(mut self, dc: Arc<sockudo_delta::DeltaCompressionManager>) -> Self {
        self.delta_compression = Some(dc);
        self
    }

    pub fn build(self) -> ConnectionHandler {
        #[cfg(feature = "recovery")]
        let replay_buffer = if self.server_options.connection_recovery.enabled {
            Some(Arc::new(crate::replay_buffer::ReplayBuffer::new(
                self.server_options.connection_recovery.max_buffer_size,
                std::time::Duration::from_secs(
                    self.server_options.connection_recovery.buffer_ttl_seconds,
                ),
            )))
        } else {
            None
        };

        #[cfg(feature = "delta")]
        let delta_compression = self.delta_compression.unwrap_or_else(|| {
            Arc::new(sockudo_delta::DeltaCompressionManager::new(
                sockudo_delta::DeltaCompressionConfig::default(),
            ))
        });

        #[cfg(feature = "ai-transport")]
        let ai_rollup_engine = if self.server_options.ai_transport.enabled
            && self.server_options.ai_transport.rollup.enabled
        {
            let rollup = &self.server_options.ai_transport.rollup;
            Some(Arc::new(RollupEngine::new(RollupConfig {
                enabled: rollup.enabled,
                window_ms: rollup.default_window_ms,
                orphan_ttl_ms: rollup.orphan_ttl_ms,
                shards: rollup.shards,
                max_active_streams_per_channel: self
                    .server_options
                    .ai_transport
                    .max_open_streaming_messages_per_channel,
            })))
        } else {
            None
        };

        #[cfg(feature = "ai-transport")]
        if let Some(engine) = ai_rollup_engine.as_ref() {
            start_ai_rollup_worker(
                Arc::clone(engine),
                Arc::clone(&self.connection_manager),
                self.metrics.clone(),
                self.server_options.ai_transport.rollup.wheel_tick_ms,
            );
        }

        let handler = ConnectionHandler {
            app_manager: self.app_manager,
            connection_manager: self.connection_manager,
            local_adapter: self.local_adapter,
            cache_manager: self.cache_manager,
            metrics: self.metrics,
            history_store: self
                .history_store
                .unwrap_or_else(|| Arc::new(NoopHistoryStore)),
            annotation_store: self
                .annotation_store
                .unwrap_or_else(|| Arc::new(MemoryAnnotationStore::new())),
            version_store: self
                .version_store
                .unwrap_or_else(|| Arc::new(NoopVersionStore)),
            #[cfg(feature = "ai-transport")]
            ai_rollup_engine,
            #[cfg(feature = "ai-transport")]
            ai_observability_tracker: self
                .server_options
                .ai_transport
                .enabled
                .then(|| Arc::new(AiObservabilityTracker::default())),
            presence_history_store: self
                .presence_history_store
                .unwrap_or_else(|| Arc::new(NoopPresenceHistoryStore)),
            webhook_integration: self.webhook_integration,
            client_event_limiters: Arc::new(DashMap::new()),
            message_limiters: Arc::new(DashMap::new()),
            presence_update_limiters: Arc::new(DashMap::new()),
            history_request_limits: Arc::new(DashMap::new()),
            watchlist_manager: Arc::new(WatchlistManager::new()),
            server_options: Arc::new(self.server_options),
            cleanup_queue: self.cleanup_queue,
            cleanup_consecutive_failures: Arc::new(AtomicUsize::new(0)),
            cleanup_circuit_breaker_opened_at: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "delta")]
            delta_compression,
            presence_manager: Arc::new(PresenceManager::new()),
            #[cfg(feature = "recovery")]
            replay_buffer,
        };

        #[cfg(feature = "ai-transport")]
        if handler.server_options.ai_transport.enabled {
            start_ai_stream_orphan_worker(
                handler.clone(),
                handler
                    .server_options
                    .ai_transport
                    .rollup
                    .wheel_tick_ms
                    .max(1_000),
            );
        }

        handler
    }
}

#[cfg(feature = "ai-transport")]
fn start_ai_rollup_worker(
    engine: Arc<RollupEngine>,
    connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    wheel_tick_ms: u64,
) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let tick_ms = wheel_tick_ms.max(1);
    handle.spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(tick_ms));
        loop {
            interval.tick().await;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
                .unwrap_or_default();
            let mut deliveries = engine.flush_due(now_ms);
            deliveries.extend(engine.sweep_orphans(now_ms));
            for delivery in deliveries {
                if let Some(metrics) = metrics.as_ref() {
                    metrics.mark_ai_rollup_append_delivered(&delivery.app_id);
                    metrics.observe_ai_rollup_ratio(&delivery.app_id, delivery.coalesced as f64);
                    metrics.observe_ai_rollup_flush_latency(
                        &delivery.app_id,
                        delivery.latency_ms as f64,
                    );
                    metrics.update_ai_rollup_active_streams(
                        &delivery.app_id,
                        engine.active_streams() as u64,
                    );
                }
                if let Err(error) = connection_manager
                    .send(
                        &delivery.channel,
                        delivery.message,
                        None,
                        &delivery.app_id,
                        None,
                    )
                    .await
                {
                    warn!(
                        app_id = %delivery.app_id,
                        channel = %delivery.channel,
                        error = %error,
                        "failed to flush AI append rollup delivery"
                    );
                }
            }
        }
    });
}

#[cfg(feature = "ai-transport")]
fn start_ai_stream_orphan_worker(handler: ConnectionHandler, wheel_tick_ms: u64) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    handle.spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(wheel_tick_ms));
        loop {
            interval.tick().await;
            let now_ms = sockudo_core::history::now_ms();
            if let Err(error) = handler.sweep_ai_stream_orphans_once(now_ms).await {
                warn!(error = %error, "failed to sweep AI stream orphan registry");
            }
        }
    });
}

impl ConnectionHandler {
    /// Create a new `ConnectionHandlerBuilder`.
    pub fn builder(
        app_manager: Arc<dyn AppManager + Send + Sync>,
        connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
        cache_manager: Arc<dyn CacheManager + Send + Sync>,
        server_options: ServerOptions,
    ) -> ConnectionHandlerBuilder {
        ConnectionHandlerBuilder::new(
            app_manager,
            connection_manager,
            cache_manager,
            server_options,
        )
    }

    /// Get a reference to the presence manager
    pub fn presence_manager(&self) -> &Arc<PresenceManager> {
        &self.presence_manager
    }

    pub fn app_manager(&self) -> &Arc<dyn AppManager + Send + Sync> {
        &self.app_manager
    }

    pub fn server_options(&self) -> &ServerOptions {
        &self.server_options
    }

    pub fn webhook_integration(&self) -> &Option<Arc<WebhookIntegration>> {
        &self.webhook_integration
    }

    pub fn metrics(&self) -> &Option<Arc<dyn MetricsInterface + Send + Sync>> {
        &self.metrics
    }

    pub fn connection_manager(&self) -> &Arc<dyn ConnectionManager + Send + Sync> {
        &self.connection_manager
    }

    pub fn cache_manager(&self) -> &Arc<dyn CacheManager + Send + Sync> {
        &self.cache_manager
    }

    pub fn history_store(&self) -> &Arc<dyn HistoryStore + Send + Sync> {
        &self.history_store
    }

    pub fn annotation_store(&self) -> &Arc<dyn AnnotationStore + Send + Sync> {
        &self.annotation_store
    }

    pub fn version_store(&self) -> &Arc<dyn VersionStore + Send + Sync> {
        &self.version_store
    }

    pub fn presence_history_store(&self) -> &Arc<dyn PresenceHistoryStore + Send + Sync> {
        &self.presence_history_store
    }

    #[cfg(feature = "recovery")]
    pub fn replay_buffer(&self) -> Option<&Arc<crate::replay_buffer::ReplayBuffer>> {
        self.replay_buffer.as_ref()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn handle_socket(
        &self,
        socket: WebSocket,
        app_key: String,
        origin: Option<String>,
        protocol_version: ProtocolVersion,
        wire_format: WireFormat,
        echo_messages: bool,
        initial_token: Option<String>,
    ) -> Result<()> {
        // Early validation and setup
        let app_config = match self.validate_and_get_app(&app_key).await {
            Ok(app) => app,
            Err(e) => {
                // Track application validation errors
                if let Some(ref metrics) = self.metrics {
                    let error_type = match &e {
                        Error::ApplicationNotFound => "app_not_found",
                        Error::ApplicationDisabled => "app_disabled",
                        _ => "app_validation_failed",
                    };
                    metrics.mark_connection_error(&app_key, error_type);
                }
                return Err(e);
            }
        };

        // Origin validation will happen after WebSocket upgrade to allow error message sending
        let (socket_rx, mut socket_tx) = socket.split();

        // Validate origin AFTER WebSocket establishment to allow error message sending
        if let Some(allowed_origins) = app_config.allowed_origins_ref()
            && !allowed_origins.is_empty()
        {
            use crate::handler::origin_validation::OriginValidator;

            // If no origin header is provided, reject the connection
            let origin_str = origin.as_deref().unwrap_or("");

            if !OriginValidator::validate_origin(origin_str, allowed_origins) {
                // Track origin validation errors
                if let Some(ref metrics) = self.metrics {
                    metrics.mark_connection_error(&app_config.id, "origin_not_allowed");
                }

                // Send error message directly through the raw WebSocket before closing
                // Create and send the error message
                let error_message = PusherMessage::error(
                    u32::from(Error::OriginNotAllowed.close_code()),
                    Error::OriginNotAllowed.to_string(),
                    None,
                );

                if let Ok(payload) =
                    sockudo_protocol::wire::serialize_message(&error_message, wire_format)
                {
                    let send_result = if wire_format.is_binary() {
                        socket_tx.send(Message::Binary(payload.into())).await
                    } else {
                        let payload_str = String::from_utf8(payload).map_err(|e| {
                            sockudo_ws::Error::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("invalid utf-8 payload: {e}"),
                            ))
                        });
                        match payload_str {
                            Ok(payload_str) => socket_tx.send(Message::text(payload_str)).await,
                            Err(e) => Err(e),
                        }
                    };
                    if let Err(e) = send_result {
                        warn!("Failed to send origin rejection message: {}", e);
                    }
                } else {
                    warn!("Failed to serialize origin rejection message");
                }

                // Send close frame
                if let Err(e) = socket_tx
                    .close(
                        Error::OriginNotAllowed.close_code(),
                        &Error::OriginNotAllowed.to_string(),
                    )
                    .await
                {
                    warn!("Failed to send origin rejection close frame: {}", e);
                }

                // Ensure frames are flushed
                if let Err(e) = socket_tx.flush().await {
                    warn!(
                        "Failed to flush WebSocket frames during origin rejection: {}",
                        e
                    );
                }

                return Err(Error::OriginNotAllowed);
            }
        }

        // Initialize socket with atomic quota check
        let socket_id = SocketId::new();
        self.initialize_socket_with_quota_check(
            socket_id,
            socket_tx,
            &app_config,
            protocol_version,
            wire_format,
            echo_messages,
        )
        .await?;

        // Setup rate limiting if needed
        self.setup_rate_limiting(&socket_id, &app_config).await?;
        self.setup_message_rate_limiting(&socket_id, &app_config)
            .await?;

        if let Some(token) = initial_token.as_deref() {
            let auth_result = async {
                if protocol_version != ProtocolVersion::V2 {
                    return Err(Error::Auth(
                        "capability tokens require protocol V2".to_string(),
                    ));
                }
                let context = self.validate_connection_token(&app_config, token).await?;
                self.apply_connection_token(&socket_id, &app_config, context, false)
                    .await
            }
            .await;

            if let Err(error) = auth_result {
                let _ = self
                    .send_error(&app_config.id, &socket_id, &error, None)
                    .await;
                let _ = self
                    .close_connection(
                        &socket_id,
                        &app_config,
                        error.close_code(),
                        &error.to_string(),
                    )
                    .await;
                self.cleanup_socket(&socket_id, &app_config).await;
                return Err(error);
            }
        }

        // Get the cancellation token so the reader loop can be cancelled
        // when the connection is cleaned up (e.g., ghost connection timeout)
        let shutdown_token = self
            .connection_manager
            .get_connection(&socket_id, &app_config.id)
            .await
            .map(|conn| conn.cancellation_token());

        // Send connection established
        self.send_connection_established(&app_config.id, &socket_id)
            .await?;

        // Setup timeouts
        self.setup_initial_timeouts(&socket_id, &app_config).await?;

        // Main message loop
        let result = self
            .run_message_loop(socket_rx, &socket_id, &app_config, shutdown_token)
            .await;

        // Cleanup
        self.cleanup_socket(&socket_id, &app_config).await;

        result
    }

    async fn initialize_socket_with_quota_check(
        &self,
        socket_id: SocketId,
        socket_tx: WebSocketWriter,
        app_config: &App,
        protocol_version: ProtocolVersion,
        wire_format: WireFormat,
        echo_messages: bool,
    ) -> Result<()> {
        // True atomic operation: quota check and socket addition under single lock
        // This is the only way to prevent race conditions
        {
            // Check quota first - this must be atomic with add_socket
            if app_config.max_connections_limit() > 0 {
                let current_count = self
                    .connection_manager
                    .get_sockets_count(&app_config.id)
                    .await
                    .map_err(|e| {
                        error!(
                            "Error getting sockets count for app {}: {}",
                            app_config.id, e
                        );
                        Error::Internal("Failed to check connection quota".to_string())
                    })?;

                if current_count >= app_config.max_connections_limit() as usize {
                    return Err(Error::OverConnectionQuota);
                }
            }

            // Remove any existing connection with the same socket_id (should be rare)
            if let Some(conn) = self
                .connection_manager
                .get_connection(&socket_id, &app_config.id)
                .await
            {
                self.connection_manager
                    .cleanup_connection(&app_config.id, conn)
                    .await;
            }

            // Add the new socket - this must be in the same critical section as quota check
            // Create buffer config from server options
            let buffer_config = self.server_options.websocket.to_buffer_config();

            self.connection_manager
                .add_socket(
                    socket_id,
                    socket_tx,
                    &app_config.id,
                    Arc::clone(&self.app_manager),
                    buffer_config,
                    protocol_version,
                    wire_format,
                    echo_messages,
                )
                .await?;
        } // Lock released - atomic operation complete

        // Update metrics after lock is released to prevent deadlock
        if let Some(ref metrics) = self.metrics {
            metrics.mark_new_connection(&app_config.id, &socket_id);
        }

        Ok(())
    }

    async fn validate_and_get_app(&self, app_key: &str) -> Result<App> {
        match self.app_manager.find_by_key(app_key).await {
            Ok(Some(app)) if app.enabled => Ok(app),
            Ok(Some(_)) => Err(Error::ApplicationDisabled),
            Ok(None) => Err(Error::ApplicationNotFound),
            Err(e) => {
                error!(
                    "Database error during app lookup for key {}: {}",
                    app_key, e
                );
                Err(Error::Internal("App lookup failed".to_string()))
            }
        }
    }

    async fn run_message_loop(
        &self,
        mut reader: WebSocketReader,
        socket_id: &SocketId,
        app_config: &App,
        shutdown_token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<()> {
        loop {
            let next = if let Some(ref token) = shutdown_token {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => {
                        debug!("Reader loop for socket {} cancelled via shutdown token", socket_id);
                        break;
                    }
                    msg = reader.next() => msg,
                }
            } else {
                reader.next().await
            };

            let next = match next {
                Some(n) => n,
                None => break,
            };

            let message = match next {
                Ok(m) => m,
                Err(e) => return Err(Error::WebSocket(e)),
            };
            match message {
                Message::Close(_) => {
                    debug!("Received Close frame from socket {}", socket_id);
                    self.handle_disconnect(&app_config.id, socket_id).await?;
                    break;
                }
                Message::Text(_) | Message::Binary(_) => {
                    if let Err(e) = self
                        .handle_message(message, socket_id, app_config.clone())
                        .await
                    {
                        if e.is_fatal() {
                            error!("Message handling error for socket {}: {}", socket_id, e);
                            self.handle_fatal_error(socket_id, app_config, &e).await?;
                            break;
                        } else if matches!(&e, Error::ConnectionClosed(_)) {
                            debug!("Message handling error for socket {}: {}", socket_id, e);
                            break;
                        } else {
                            error!("Message handling error for socket {}: {}", socket_id, e);
                            // Send pusher:error for non-fatal errors
                            if let Err(send_err) =
                                self.send_error(&app_config.id, socket_id, &e, None).await
                            {
                                error!(
                                    "Failed to send error to socket {}: {}",
                                    socket_id, send_err
                                );
                            }
                        }
                    }
                }
                _ => {
                    warn!("Unsupported message type from {}", socket_id);
                }
            }
        }

        Ok(())
    }

    async fn handle_message(
        &self,
        message: Message,
        socket_id: &SocketId,
        app_config: App,
    ) -> Result<()> {
        let t0 = std::time::Instant::now();

        // Update activity timeout
        self.update_activity_timeout(&app_config.id, socket_id)
            .await?;

        // Parse message
        let wire_format = self
            .connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
            .map(|conn| conn.wire_format)
            .unwrap_or(WireFormat::Json);

        let parsed = match self.parse_message(&message, wire_format) {
            Ok(msg) => msg,
            Err(e) => {
                // Track message parsing errors
                if let Some(ref metrics) = self.metrics {
                    let error_type = match &e {
                        Error::InvalidMessageFormat(_) => "invalid_message_format",
                        _ => "message_parse_error",
                    };
                    metrics.mark_connection_error(&app_config.id, error_type);
                }
                return Err(e);
            }
        };

        let event_name = match parsed.event.as_deref() {
            Some(name) => name,
            None => {
                // Track missing event name errors
                if let Some(ref metrics) = self.metrics {
                    metrics.mark_connection_error(&app_config.id, "missing_event_name");
                }
                return Err(Error::InvalidEventName("Event name is required".into()));
            }
        };

        // Track WebSocket message received metrics
        if let Some(ref metrics) = self.metrics {
            let payload_size = match &message {
                Message::Text(b) | Message::Binary(b) => b.len(),
                _ => 0,
            };
            metrics.mark_ws_message_received(&app_config.id, payload_size);
        }

        debug!(
            %socket_id,
            event = %event_name,
            payload_bytes = match &message {
                Message::Text(bytes) | Message::Binary(bytes) => bytes.len(),
                _ => 0,
            },
            "Received WebSocket message"
        );

        // Check all-message rate limit first
        self.check_message_rate_limit(socket_id, &app_config)
            .await?;

        // Handle rate limiting for client events
        if event_name.starts_with(CLIENT_EVENT_PREFIX) {
            self.check_client_event_rate_limit(socket_id, &app_config, event_name)
                .await?;
        }

        // Route message to appropriate handler using canonical event names.
        // This accepts both pusher: and sockudo: prefixes transparently.
        use sockudo_protocol::protocol_version::*;
        let canonical = ProtocolVersion::parse_any_protocol_event(event_name);
        match canonical {
            Some((CANONICAL_PING, _)) => self.handle_ping(&app_config.id, socket_id).await,
            Some((CANONICAL_SUBSCRIBE, _)) => {
                let t1 = t0.elapsed().as_micros();
                let request = SubscriptionRequest::from_message(
                    &parsed,
                    &self.server_options.event_name_filtering,
                )?;
                let result = self
                    .handle_subscribe_request(socket_id, &app_config, request)
                    .await;
                let total = t0.elapsed().as_micros();
                debug!(
                    "PERF: subscription total={}μs parse_to_handler={}μs handler={}μs",
                    total,
                    t1,
                    total - t1
                );
                result
            }
            Some((CANONICAL_UNSUBSCRIBE, _)) => {
                self.handle_unsubscribe(socket_id, &parsed, &app_config)
                    .await
            }
            Some((CANONICAL_SIGNIN, _)) => {
                let request = SignInRequest::from_message(&parsed)?;
                self.handle_signin_request(socket_id, &app_config, request)
                    .await
            }
            Some((CANONICAL_AUTH, _)) => {
                self.handle_auth_token_refresh(socket_id, &app_config, &parsed)
                    .await
            }
            Some((CANONICAL_PONG, _)) => self.handle_pong(&app_config.id, socket_id).await,
            #[cfg(feature = "delta")]
            Some((CANONICAL_ENABLE_DELTA_COMPRESSION, _)) => {
                self.handle_enable_delta_compression(socket_id).await
            }
            #[cfg(feature = "delta")]
            Some((CANONICAL_DELTA_SYNC_ERROR, _)) => {
                self.handle_delta_sync_error(socket_id, &parsed).await
            }
            #[cfg(feature = "recovery")]
            Some((CANONICAL_RESUME, _)) => {
                self.handle_resume(socket_id, &app_config, &parsed).await
            }
            Some((CANONICAL_CHANNEL_HISTORY, _)) => {
                self.handle_channel_history_request(socket_id, &app_config, &parsed)
                    .await
            }
            Some((CANONICAL_PRESENCE_UPDATE, false)) => {
                self.handle_presence_update(socket_id, &app_config, &parsed)
                    .await
            }
            None if event_name == "channel_history" => {
                self.handle_channel_history_request(socket_id, &app_config, &parsed)
                    .await
            }
            _ if event_name.starts_with(CLIENT_EVENT_PREFIX) => {
                let request = self.parse_client_event(&parsed)?;
                self.handle_client_event_request(socket_id, &app_config, request)
                    .await
            }
            _ if is_ai_event(event_name) => {
                self.handle_ai_event_request(socket_id, &app_config, parsed)
                    .await
            }
            _ => {
                warn!("Unknown event '{}' from socket {}", event_name, socket_id);
                Ok(()) // Ignore unknown events per protocol spec
            }
        }
    }

    fn parse_message(&self, message: &Message, wire_format: WireFormat) -> Result<PusherMessage> {
        let payload = match message {
            Message::Text(bytes) | Message::Binary(bytes) => bytes,
            _ => {
                return Err(Error::InvalidMessageFormat(
                    "Unsupported WebSocket message type".to_string(),
                ));
            }
        };

        sockudo_protocol::wire::deserialize_message(payload, wire_format)
            .map_err(|e| Error::InvalidMessageFormat(format!("Decode failed: {e}")))
    }

    fn parse_client_event(&self, message: &PusherMessage) -> Result<ClientEventRequest> {
        let event = message
            .event
            .as_ref()
            .ok_or_else(|| Error::InvalidEventName("Event name required".into()))?
            .clone();

        let channel = message
            .channel
            .as_ref()
            .ok_or_else(|| Error::ClientEvent("Channel required for client event".into()))?
            .clone();

        let data = match &message.data {
            Some(MessageData::Json(data)) => data.clone(),
            Some(MessageData::String(s)) => {
                sonic_rs::from_str(s).unwrap_or_else(|_| Value::from(s.as_str()))
            }
            Some(MessageData::Structured { extra, .. }) => {
                // For client events, the data is in the extra fields
                // Always convert the extra HashMap to a JSON object to preserve structure
                if !extra.is_empty() {
                    sonic_rs::to_value(extra).unwrap_or_else(|_| Value::new_object())
                } else {
                    Value::new_null()
                }
            }
            None => Value::new_null(),
        };

        Ok(ClientEventRequest {
            event,
            channel,
            data,
        })
    }

    async fn handle_fatal_error(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        error: &Error,
    ) -> Result<()> {
        // Send error message
        self.send_error(&app_config.id, socket_id, error, None)
            .await
            .unwrap_or_else(|e| {
                error!("Failed to send error to socket {}: {}", socket_id, e);
            });

        // Close connection
        self.close_connection(
            socket_id,
            app_config,
            error.close_code(),
            &error.to_string(),
        )
        .await?;

        // Handle disconnect cleanup
        self.handle_disconnect(&app_config.id, socket_id).await?;

        Ok(())
    }

    async fn cleanup_socket(&self, socket_id: &SocketId, app_config: &App) {
        // Remove rate limiters
        self.client_event_limiters.remove(socket_id);
        self.message_limiters.remove(socket_id);
        self.history_request_limits.remove(socket_id);

        // MEMORY LEAK FIX: Clean up delta compression state for this socket
        #[cfg(feature = "delta")]
        self.delta_compression.remove_socket(socket_id);

        // Clear timeouts
        if let Err(e) = self.clear_activity_timeout(&app_config.id, socket_id).await {
            warn!("Failed to clear activity timeout for {}: {}", socket_id, e);
        }

        if let Err(e) = self
            .clear_user_authentication_timeout(&app_config.id, socket_id)
            .await
        {
            warn!("Failed to clear auth timeout for {}: {}", socket_id, e);
        }

        // Ensure disconnect cleanup is called to properly decrement connection count
        // This handles cases where the message loop exits without receiving a clean Close frame
        if let Err(e) = self
            .handle_ungraceful_disconnect(&app_config.id, socket_id)
            .await
        {
            warn!(
                "Failed to handle disconnect during cleanup for {}: {}",
                socket_id, e
            );
        }

        debug!("Socket {} cleanup completed", socket_id);
    }
}
