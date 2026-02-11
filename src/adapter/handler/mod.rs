// src/adapter/handler/mod.rs
pub mod authentication;
pub mod connection_management;
mod core;
pub mod message_handlers;
pub mod origin_validation;
pub mod rate_limiting;
pub mod signin_management;
pub mod subscription_management;
pub mod timeout_management;
pub mod types;
pub mod validation;
pub mod webhook_management;

use crate::adapter::ConnectionManager;
use crate::app::config::App;
use crate::app::manager::AppManager;
use crate::cache::manager::CacheManager;
use crate::error::{Error, Result};
use crate::metrics::MetricsInterface;
use crate::options::ServerOptions;
use crate::presence::PresenceManager;
use crate::protocol::constants::CLIENT_EVENT_PREFIX;
use crate::protocol::messages::{MessageData, PusherMessage};
use crate::rate_limiter::RateLimiter;
use crate::watchlist::WatchlistManager;
use crate::webhook::integration::WebhookIntegration;
use crate::websocket::SocketId;

use crate::adapter::handler::types::{ClientEventRequest, SignInRequest, SubscriptionRequest};
use dashmap::DashMap;
use serde_json::Value;
use sockudo_ws::Message;
use sockudo_ws::axum_integration::{WebSocket, WebSocketReader, WebSocketWriter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use tokio::sync::Mutex;
use tracing::{debug, error, warn};

#[derive(Clone)]
pub struct ConnectionHandler {
    pub(crate) app_manager: Arc<dyn AppManager + Send + Sync>,
    pub(crate) connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
    pub(crate) local_adapter: Option<Arc<crate::adapter::local_adapter::LocalAdapter>>,
    pub(crate) cache_manager: Arc<Mutex<dyn CacheManager + Send + Sync>>,
    pub(crate) metrics: Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>>,
    webhook_integration: Option<Arc<WebhookIntegration>>,
    client_event_limiters: Arc<DashMap<SocketId, Arc<dyn RateLimiter + Send + Sync>>>,
    watchlist_manager: Arc<WatchlistManager>,
    server_options: Arc<ServerOptions>,
    cleanup_queue: Option<crate::cleanup::CleanupSender>,
    cleanup_consecutive_failures: Arc<AtomicUsize>,
    cleanup_circuit_breaker_opened_at: Arc<AtomicU64>,
    delta_compression: Arc<crate::delta_compression::DeltaCompressionManager>,
    /// Presence manager for race-safe presence channel operations
    pub(crate) presence_manager: Arc<PresenceManager>,
}

impl ConnectionHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        app_manager: Arc<dyn AppManager + Send + Sync>,
        connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
        local_adapter: Option<Arc<crate::adapter::local_adapter::LocalAdapter>>,
        cache_manager: Arc<Mutex<dyn CacheManager + Send + Sync>>,
        metrics: Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>>,
        webhook_integration: Option<Arc<WebhookIntegration>>,
        server_options: ServerOptions,
        cleanup_queue: Option<crate::cleanup::CleanupSender>,
        delta_compression: Arc<crate::delta_compression::DeltaCompressionManager>,
    ) -> Self {
        Self {
            app_manager,
            connection_manager,
            local_adapter,
            cache_manager,
            metrics,
            webhook_integration,
            client_event_limiters: Arc::new(DashMap::new()),
            watchlist_manager: Arc::new(WatchlistManager::new()),
            server_options: Arc::new(server_options),
            cleanup_queue,
            cleanup_consecutive_failures: Arc::new(AtomicUsize::new(0)),
            cleanup_circuit_breaker_opened_at: Arc::new(AtomicU64::new(0)),
            delta_compression,
            presence_manager: Arc::new(PresenceManager::new()),
        }
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

    pub async fn handle_socket(
        &self,
        socket: WebSocket,
        app_key: String,
        origin: Option<String>,
    ) -> Result<()> {
        // Early validation and setup
        let app_config = match self.validate_and_get_app(&app_key).await {
            Ok(app) => app,
            Err(e) => {
                // Track application validation errors
                if let Some(ref metrics) = self.metrics {
                    let metrics_locked = metrics.lock().await;
                    let error_type = match &e {
                        Error::ApplicationNotFound => "app_not_found",
                        Error::ApplicationDisabled => "app_disabled",
                        _ => "app_validation_failed",
                    };
                    metrics_locked.mark_connection_error(&app_key, error_type);
                }
                return Err(e);
            }
        };

        // Origin validation will happen after WebSocket upgrade to allow error message sending
        let (socket_rx, mut socket_tx) = socket.split();

        // Validate origin AFTER WebSocket establishment to allow error message sending
        if let Some(ref allowed_origins) = app_config.allowed_origins
            && !allowed_origins.is_empty()
        {
            use crate::adapter::handler::origin_validation::OriginValidator;

            // If no origin header is provided, reject the connection
            let origin_str = origin.as_deref().unwrap_or("");

            if !OriginValidator::validate_origin(origin_str, allowed_origins) {
                // Track origin validation errors
                if let Some(ref metrics) = self.metrics {
                    let metrics_locked = metrics.lock().await;
                    metrics_locked.mark_connection_error(&app_config.id, "origin_not_allowed");
                }

                // Send error message directly through the raw WebSocket before closing
                // Create and send the error message
                let error_message = PusherMessage::error(
                    Error::OriginNotAllowed.close_code(),
                    Error::OriginNotAllowed.to_string(),
                    None,
                );

                if let Ok(payload_str) = serde_json::to_string(&error_message) {
                    if let Err(e) = socket_tx.send(Message::text(payload_str)).await {
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
        self.initialize_socket_with_quota_check(socket_id.clone(), socket_tx, &app_config)
            .await?;

        // Setup rate limiting if needed
        self.setup_rate_limiting(&socket_id, &app_config).await?;

        // Send connection established
        self.send_connection_established(&app_config.id, &socket_id)
            .await?;

        // Setup timeouts
        self.setup_initial_timeouts(&socket_id, &app_config).await?;

        // Main message loop
        let result = self
            .run_message_loop(socket_rx, &socket_id, &app_config)
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
    ) -> Result<()> {
        // True atomic operation: quota check and socket addition under single lock
        // This is the only way to prevent race conditions
        {
            // Check quota first - this must be atomic with add_socket
            if app_config.max_connections > 0 {
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

                if current_count >= app_config.max_connections as usize {
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
                    socket_id.clone(),
                    socket_tx,
                    &app_config.id,
                    Arc::clone(&self.app_manager),
                    buffer_config,
                )
                .await?;
        } // Lock released - atomic operation complete

        // Update metrics after lock is released to prevent deadlock
        if let Some(ref metrics) = self.metrics {
            let metrics_locked = metrics.lock().await;
            metrics_locked.mark_new_connection(&app_config.id, &socket_id);
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
    ) -> Result<()> {
        while let Some(next) = reader.next().await {
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
                        error!("Message handling error for socket {}: {}", socket_id, e);
                        if e.is_fatal() {
                            self.handle_fatal_error(socket_id, app_config, &e).await?;
                            break;
                        } else {
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
        let parsed = match self.parse_message(&message) {
            Ok(msg) => msg,
            Err(e) => {
                // Track message parsing errors
                if let Some(ref metrics) = self.metrics {
                    let metrics_locked = metrics.lock().await;
                    let error_type = match &e {
                        Error::InvalidMessageFormat(_) => "invalid_message_format",
                        _ => "message_parse_error",
                    };
                    metrics_locked.mark_connection_error(&app_config.id, error_type);
                }
                return Err(e);
            }
        };

        let event_name = match parsed.event.as_deref() {
            Some(name) => name,
            None => {
                // Track missing event name errors
                if let Some(ref metrics) = self.metrics {
                    let metrics_locked = metrics.lock().await;
                    metrics_locked.mark_connection_error(&app_config.id, "missing_event_name");
                }
                return Err(Error::InvalidEventName("Event name is required".into()));
            }
        };

        // Track WebSocket message received metrics
        if let Some(ref metrics) = self.metrics {
            let metrics_locked = metrics.lock().await;
            let payload_size = match &message {
                Message::Text(b) | Message::Binary(b) => b.len(),
                _ => 0,
            };
            metrics_locked.mark_ws_message_received(&app_config.id, payload_size);
        }

        debug!(
            "Received message from {socket_id}: event '{event_name}', full message: {:?}",
            message
        );

        // Handle rate limiting for client events
        if event_name.starts_with(CLIENT_EVENT_PREFIX) {
            self.check_client_event_rate_limit(socket_id, &app_config, event_name)
                .await?;
        }

        // Route message to appropriate handler
        match event_name {
            "pusher:ping" => self.handle_ping(&app_config.id, socket_id).await,
            "pusher:subscribe" => {
                let t1 = t0.elapsed().as_micros();
                let request = SubscriptionRequest::from_message(&parsed)?;
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
            "pusher:unsubscribe" => {
                self.handle_unsubscribe(socket_id, &parsed, &app_config)
                    .await
            }
            "pusher:signin" => {
                let request = SignInRequest::from_message(&parsed)?;
                self.handle_signin_request(socket_id, &app_config, request)
                    .await
            }
            "pusher:pong" => self.handle_pong(&app_config.id, socket_id).await,
            crate::protocol::constants::EVENT_ENABLE_DELTA_COMPRESSION => {
                self.handle_enable_delta_compression(socket_id).await
            }
            "pusher:delta_sync_error" => self.handle_delta_sync_error(socket_id, &parsed).await,
            _ if event_name.starts_with(CLIENT_EVENT_PREFIX) => {
                let request = self.parse_client_event(&parsed)?;
                self.handle_client_event_request(socket_id, &app_config, request)
                    .await
            }
            _ => {
                warn!("Unknown event '{}' from socket {}", event_name, socket_id);
                Ok(()) // Ignore unknown events per Pusher spec
            }
        }
    }

    fn parse_message(&self, message: &Message) -> Result<PusherMessage> {
        let payload = match message {
            Message::Text(bytes) | Message::Binary(bytes) => std::str::from_utf8(bytes)
                .map_err(|e| Error::InvalidMessageFormat(format!("Invalid UTF-8: {e}")))?
                .to_string(),
            _ => {
                return Err(Error::InvalidMessageFormat(
                    "Unsupported WebSocket message type".to_string(),
                ));
            }
        };

        serde_json::from_str(&payload)
            .map_err(|e| Error::InvalidMessageFormat(format!("Invalid JSON: {e}")))
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
                serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.clone()))
            }
            Some(MessageData::Structured { extra, .. }) => {
                // For client events, the data is in the extra fields
                // Always convert the extra HashMap to a JSON object to preserve structure
                if !extra.is_empty() {
                    Value::Object(
                        extra
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect::<serde_json::Map<String, Value>>(),
                    )
                } else {
                    Value::Null
                }
            }
            None => Value::Null,
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
        // Remove rate limiter
        self.client_event_limiters.remove(socket_id);

        // MEMORY LEAK FIX: Clean up delta compression state for this socket
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
        if let Err(e) = self.handle_disconnect(&app_config.id, socket_id).await {
            warn!(
                "Failed to handle disconnect during cleanup for {}: {}",
                socket_id, e
            );
        }

        debug!("Socket {} cleanup completed", socket_id);
    }

    /// Increment the active channel count for a specific channel type
    async fn increment_active_channel_count(
        &self,
        app_id: &str,
        channel_type: &str,
        metrics: Arc<Mutex<dyn MetricsInterface + Send + Sync>>,
    ) {
        // Get current count for this channel type
        // IMPORTANT: We must get the count BEFORE acquiring the metrics lock to avoid deadlock
        let current_count = self
            .get_active_channel_count_for_type(app_id, channel_type)
            .await;

        // Now acquire the metrics lock and update
        let metrics_locked = metrics.lock().await;
        metrics_locked.update_active_channels(app_id, channel_type, current_count + 1);

        debug!(
            "Incremented active {} channels for app {} to {}",
            channel_type,
            app_id,
            current_count + 1
        );
    }

    /// Decrement the active channel count for a specific channel type
    async fn decrement_active_channel_count(
        &self,
        app_id: &str,
        channel_type: &str,
        metrics: Arc<Mutex<dyn MetricsInterface + Send + Sync>>,
    ) {
        // Get current count for this channel type
        // IMPORTANT: We must get the count BEFORE acquiring the metrics lock to avoid deadlock
        let current_count = self
            .get_active_channel_count_for_type(app_id, channel_type)
            .await;

        // Decrement by 1, but never go below 0
        let new_count = std::cmp::max(0, current_count - 1);

        // Now acquire the metrics lock and update
        let metrics_locked = metrics.lock().await;
        metrics_locked.update_active_channels(app_id, channel_type, new_count);

        debug!(
            "Decremented active {} channels for app {} to {}",
            channel_type, app_id, new_count
        );
    }

    /// Get the current count of active channels for a specific type
    async fn get_active_channel_count_for_type(&self, app_id: &str, channel_type: &str) -> i64 {
        // Get all channels with their socket counts
        let channels_map = {
            match self
                .connection_manager
                .get_channels_with_socket_count(app_id)
                .await
            {
                Ok(map) => map,
                Err(e) => {
                    error!("Failed to get channels for metrics update: {}", e);
                    return 0;
                }
            }
        };

        // Count active channels of the specified type
        // This processing happens AFTER the connection_manager lock is released
        let mut count = 0i64;
        for (channel_name, socket_count) in &channels_map {
            // Only count channels that have active connections
            if *socket_count > 0 {
                let ch_type = crate::channel::ChannelType::from_name(channel_name);
                let ch_type_str = ch_type.as_str();

                if ch_type_str == channel_type {
                    count += 1;
                }
            }
        }

        count
    }
}
