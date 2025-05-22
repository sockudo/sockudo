// src/adapter/handler.rs
use crate::adapter::adapter::Adapter;
use crate::app::auth::AuthValidator;
use crate::app::config::App;
use crate::app::manager::AppManager;
use crate::cache::manager::CacheManager;
use crate::channel::{ChannelManager, ChannelType, PresenceMemberInfo};
use crate::metrics::MetricsInterface;
use crate::protocol::constants::{
    CHANNEL_NAME_MAX_LENGTH as DEFAULT_CHANNEL_NAME_MAX_LENGTH, CLIENT_EVENT_PREFIX,
    EVENT_NAME_MAX_LENGTH as DEFAULT_EVENT_NAME_MAX_LENGTH,
};
use crate::protocol::messages::{ErrorData, MessageData, PusherApiMessage, PusherMessage};
use crate::rate_limiter::{RateLimiter, memory_limiter::MemoryRateLimiter};
use crate::utils::{is_cache_channel, validate_channel_name};
use crate::webhook::integration::WebhookIntegration;
use crate::websocket::{SocketId, WebSocketRef};
use crate::{
    error::{Error, Result}, // Ensure this is crate::error::Result
    utils,
};
use dashmap::DashMap;
use fastwebsockets::{
    FragmentCollectorRead, Frame, OpCode, Payload, WebSocketError, WebSocketWrite, upgrade,
};
use hyper::upgrade::Upgraded; // Required for UpgradeFut
use hyper_util::rt::TokioIo; // Required for UpgradeFut
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::WriteHalf; // Required for WebSocketWrite
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

pub struct ConnectionHandler {
    pub(crate) app_manager: Arc<dyn AppManager + Send + Sync>,
    pub(crate) channel_manager: Arc<RwLock<ChannelManager>>,
    pub(crate) connection_manager: Arc<Mutex<Box<dyn Adapter + Send + Sync>>>,
    pub(crate) cache_manager: Arc<Mutex<dyn CacheManager + Send + Sync>>,
    pub(crate) metrics: Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>>,
    pub(crate) webhook_integration: Option<Arc<WebhookIntegration>>,
    pub(crate) http_rate_limiter: Option<Arc<dyn RateLimiter + Send + Sync>>,
    pub(crate) client_event_limiters: Arc<DashMap<SocketId, Arc<dyn RateLimiter + Send + Sync>>>,
}

impl ConnectionHandler {
    pub fn new(
        app_manager: Arc<dyn AppManager + Send + Sync>,
        channel_manager: Arc<RwLock<ChannelManager>>,
        connection_manager: Arc<Mutex<Box<dyn Adapter + Send + Sync>>>,
        cache_manager: Arc<Mutex<dyn CacheManager + Send + Sync>>,
        metrics: Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>>,
        webhook_integration: Option<Arc<WebhookIntegration>>,
        http_rate_limiter: Option<Arc<dyn RateLimiter + Send + Sync>>,
    ) -> Self {
        Self {
            app_manager,
            channel_manager,
            connection_manager,
            cache_manager,
            metrics,
            webhook_integration,
            http_rate_limiter,
            client_event_limiters: Arc::new(DashMap::new()),
        }
    }

    /// Helper to check if a user has any other connections to a specific presence channel.
    async fn user_has_other_connections_in_presence_channel(
        &self,
        app_id: &str,
        channel_name: &str,
        user_id: &str,
    ) -> Result<bool> {
        let mut connection_manager = self.connection_manager.lock().await;
        let user_sockets = connection_manager.get_user_sockets(user_id, app_id).await?;

        for ws_ref in user_sockets.iter() {
            let socket_state_guard = ws_ref.0.lock().await;
            if socket_state_guard.state.is_subscribed(channel_name) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[allow(dead_code)]
    async fn send_webhook_event<F, Fut>(&self, app: &App, webhook_fn: F) -> Result<()>
    where
        F: FnOnce(&WebhookIntegration, &App) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        if let Some(webhook_integration_instance) = &self.webhook_integration {
            if webhook_integration_instance.is_enabled() {
                match webhook_fn(webhook_integration_instance, app).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        warn!("Webhook event failed to send: {}", e);
                        Ok(())
                    }
                }
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    pub async fn send_missed_cache_if_exists(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        channel: &str,
    ) -> Result<()> {
        let mut cache_manager = self.cache_manager.lock().await;
        let key = format!("app:{}:channel:{}:cache_miss", app_id, channel);
        let cache_result = cache_manager.get(key.as_str()).await;

        match cache_result {
            Ok(Some(cache_content)) => {
                let cache_message: PusherMessage = serde_json::from_str(&cache_content)?;
                self.connection_manager
                    .lock()
                    .await
                    .send_message(app_id, socket_id, cache_message)
                    .await?;
            }
            Ok(None) => {
                let message = PusherMessage {
                    channel: Some(channel.to_string()),
                    name: None,
                    event: Some("pusher:cache_miss".to_string()),
                    data: None,
                };
                self.connection_manager
                    .lock()
                    .await
                    .send_message(app_id, socket_id, message)
                    .await?;

                if let Some(app_config) = self.app_manager.find_by_id(app_id).await? {
                    if let Some(webhook_integration_instance) = &self.webhook_integration {
                        webhook_integration_instance
                            .send_cache_missed(&app_config, channel)
                            .await?;
                    }
                }
                info!("No missed cache for channel: {}", channel);
            }
            Err(e) => {
                error!("Failed to get cache for channel {}: {}", channel, e);
                return Err(e);
            }
        }
        Ok(())
    }

    /// Centralized function to send Pusher error and WebSocket close frame.
    /// This is typically used for errors encountered *before* the main message loop starts,
    /// or when the WebSocket write half is directly available.
    async fn send_error_and_close_ws(
        ws_tx: &mut WebSocketWrite<WriteHalf<TokioIo<Upgraded>>>,
        error: &Error,
    ) {
        let error_data = ErrorData {
            message: error.to_string(),
            code: Some(error.close_code()),
        };
        let error_json_message = PusherMessage::error(
            error_data.code.unwrap_or(4000),
            error_data.message.clone(),
            None,
        );

        if let Ok(payload_str) = serde_json::to_string(&error_json_message) {
            if let Err(e) = ws_tx
                .write_frame(Frame::text(Payload::from(payload_str.into_bytes())))
                .await
            {
                warn!("Failed to send pusher:error message before close: {}", e);
            }
        } else {
            warn!("Failed to serialize pusher:error message.");
        }

        if let Err(e) = ws_tx
            .write_frame(Frame::close(
                error.close_code(),
                error.to_string().as_bytes(),
            ))
            .await
        {
            warn!("Failed to send WebSocket close frame: {}", e);
        }
    }

    pub async fn handle_socket(&self, fut: upgrade::UpgradeFut, app_key: String) -> Result<()> {
        let app_config_option = self.app_manager.find_by_key(&app_key).await;

        // Perform upgrade and handle early errors by sending Pusher error and closing.
        let (mut socket_rx_frag, mut socket_tx_direct) = match fut.await {
            Ok(ws) => ws.split(tokio::io::split),
            Err(e) => {
                // WebSocket upgrade itself failed. Not much we can send back.
                error!("WebSocket upgrade failed for app_key {}: {}", app_key, e);
                return Err(Error::WebSocketError(e)); // Propagate the upgrade error
            }
        };

        let app_config = match app_config_option {
            Ok(Some(app)) => app,
            Ok(None) => {
                ConnectionHandler::send_error_and_close_ws(
                    &mut socket_tx_direct,
                    &Error::ApplicationNotFound,
                )
                .await;
                return Ok(());
            }
            Err(db_err) => {
                error!(
                    "Database error during app lookup for key {}: {}",
                    app_key, db_err
                );
                let internal_err = Error::InternalError("App lookup failed".to_string());
                ConnectionHandler::send_error_and_close_ws(&mut socket_tx_direct, &internal_err)
                    .await;
                return Ok(());
            }
        };

        if !app_config.enabled {
            ConnectionHandler::send_error_and_close_ws(
                &mut socket_tx_direct,
                &Error::ApplicationDisabled,
            )
            .await;
            return Ok(());
        }

        let max_connections = app_config.max_connections;
        if max_connections > 0 {
            let current_connections_result = self
                .connection_manager
                .lock()
                .await
                .get_sockets_count(&app_config.id)
                .await;

            match current_connections_result {
                Ok(count) if count >= max_connections as usize => {
                    ConnectionHandler::send_error_and_close_ws(
                        &mut socket_tx_direct,
                        &Error::OverConnectionQuota,
                    )
                    .await;
                    return Ok(());
                }
                Err(e) => {
                    error!(
                        "Error getting sockets count for app {}: {}",
                        app_config.id, e
                    );
                    let internal_err =
                        Error::InternalError("Failed to check connection quota".to_string());
                    ConnectionHandler::send_error_and_close_ws(
                        &mut socket_tx_direct,
                        &internal_err,
                    )
                    .await;
                    return Ok(());
                }
                _ => {} // Quota check passed
            }
        }

        // All initial checks passed. Proceed to add socket to manager.
        let socket_id = SocketId::new();
        info!("New socket: {} for app: {}", socket_id, app_config.id);

        {
            let mut connection_manager_locked = self.connection_manager.lock().await;
            // It's unlikely a duplicate socket_id exists here, but good practice.
            if let Some(conn) = connection_manager_locked
                .get_connection(&socket_id, &app_config.id)
                .await
            {
                // This cleanup might send messages; ensure it's safe if the socket isn't fully "active" yet.
                // Consider if cleanup should only do resource release here.
                connection_manager_locked
                    .cleanup_connection(&app_config.id, WebSocketRef(conn))
                    .await;
            }
            // Add the socket (tx part) to the connection manager.
            // The `socket_tx_direct` is consumed here.
            if let Err(e) = connection_manager_locked
                .add_socket(
                    socket_id.clone(),
                    socket_tx_direct, // Pass the direct write half
                    &app_config.id,
                    &self.app_manager,
                )
                .await
            {
                // If add_socket fails, we can't use the manager's send_error.
                // The socket_tx_direct was consumed. This scenario is tricky.
                // The error from add_socket should ideally be a WebSocketError or similar
                // that can be returned to ws_handler to signal failure.
                error!(
                    "Fatal error: Failed to add socket {} to connection manager: {}. Connection cannot proceed.",
                    socket_id, e
                );
                // The connection is effectively dead from the server's perspective.
                // The client will eventually time out or detect a broken pipe.
                return Err(e); // Propagate the error.
            }

            if let Some(ref metrics) = self.metrics {
                let metrics_locked = metrics.lock().await;
                metrics_locked.mark_new_connection(&app_config.id, &socket_id)
            }
        }

        if app_config.max_client_events_per_second > 0 {
            let limiter = Arc::new(MemoryRateLimiter::new(
                app_config.max_client_events_per_second,
                1, // Per second
            ));
            self.client_event_limiters
                .insert(socket_id.clone(), limiter);
            info!(
                "Initialized client event rate limiter for socket {}: {} events/sec",
                socket_id, app_config.max_client_events_per_second
            );
        }

        // Send pusher:connection_established
        if let Err(e) = self
            .send_connection_established(&app_config.id, &socket_id)
            .await
        {
            // Failed to send connection_established. This is a server-side issue or socket closed prematurely.
            // Send pusher:error JSON message first via the manager's send_error.
            self.send_error(&app_config.id, &socket_id, &e, None)
                .await
                .unwrap_or_else(|err_send| {
                    error!(
                        "Failed to send pusher:error after send_connection_established failed: {}",
                        err_send
                    );
                });

            // Then, explicitly close the WebSocket via the manager.
            let mut connection_manager_locked = self.connection_manager.lock().await;
            if let Some(conn_arc) = connection_manager_locked
                .get_connection(&socket_id, &app_config.id)
                .await
            {
                let mut conn_locked = conn_arc.lock().await;
                if let Err(close_err) = conn_locked.close(e.close_code(), e.to_string()).await {
                    warn!(
                        "Failed to send WebSocket close frame to socket {} after send_connection_established failed: {}",
                        socket_id, close_err
                    );
                }
                // Adapter's remove_connection will be called by handle_disconnect later if needed.
            }
            drop(connection_manager_locked);

            // Perform full disconnect cleanup
            if let Err(disconnect_err) = self.handle_disconnect(&app_config.id, &socket_id).await {
                error!(
                    "Error during handle_disconnect after send_connection_established failed for {}: {}",
                    socket_id, disconnect_err
                );
            }
            self.client_event_limiters.remove(&socket_id); // Ensure limiter is cleaned up
            return Ok(()); // Error handled by closing the connection.
        }

        // Main message loop using the read half
        let mut fragment_collector = FragmentCollectorRead::new(socket_rx_frag);

        while let Ok(frame) = fragment_collector
            .read_frame(&mut move |_| async { Ok::<_, WebSocketError>(()) })
            .await
        {
            match frame.opcode {
                OpCode::Close => {
                    info!("Received Close frame from socket {}", socket_id);
                    // Client initiated close.
                    if let Some(ref metrics) = self.metrics {
                        let metrics_locked = metrics.lock().await;
                        metrics_locked.mark_disconnection(&app_config.id, &socket_id);
                    }
                    if let Err(e) = self.handle_disconnect(&app_config.id, &socket_id).await {
                        error!(
                            "Error during client-initiated disconnect for socket {}: {}",
                            socket_id, e
                        );
                    }
                    break; // Exit message loop
                }
                OpCode::Text | OpCode::Binary => {
                    if let Err(e) = self
                        .handle_message(frame, &socket_id, app_config.clone())
                        .await
                    {
                        // handle_message now takes care of sending pusher:error and closing WS for fatal errors.
                        // If an error is returned here, it means it was fatal and the connection should be considered closed.
                        error!(
                            "Message handling for socket {} resulted in error: {}. Connection loop will terminate.",
                            socket_id, e
                        );
                        // No need to call handle_disconnect here if handle_message did it for fatal errors.
                        break; // Exit message loop
                    }
                }
                OpCode::Ping => {
                    // Respond with Pong or update last ping time
                    let mut connection_manager_locked = self.connection_manager.lock().await;
                    if let Some(conn_arc) = connection_manager_locked
                        .get_connection(&socket_id, &app_config.id)
                        .await
                    {
                        let mut conn_locked = conn_arc.lock().await;
                        conn_locked.state.update_ping();
                        // Optionally send an explicit Pong frame if required by protocol version / client
                        // For now, assume updating last_ping is sufficient.
                    }
                }
                _ => {
                    warn!(
                        "Unsupported opcode received from {}: {:?}",
                        socket_id, frame.opcode
                    );
                }
            }
        }

        // TODO: Add a timeout mechanism here for inactivity if needed,
        // which would then send Error::InactivityTimeout (4202)
        // _ = tokio::time::sleep(Duration::from_secs(app_config.activity_timeout_seconds.unwrap_or(120))) => {
        //     info!("Socket {} inactive, sending ping or closing.", socket_id);
        //     // Implement ping logic or close with InactivityTimeout
        // }

        // Loop exited, ensure rate limiter is cleaned up if it was added
        self.client_event_limiters.remove(&socket_id);
        info!("Message loop terminated for socket {}", socket_id);
        Ok(())
    }

    pub async fn handle_message(
        &self,
        frame: Frame<'static>,
        socket_id: &SocketId,
        app_config: App,
    ) -> Result<()> {
        let msg_payload = String::from_utf8(frame.payload.to_vec())
            .map_err(|e| Error::InvalidMessageFormat(format!("Invalid UTF-8: {}", e)))?;

        let message: PusherMessage = serde_json::from_str(&msg_payload)
            .map_err(|e| Error::InvalidMessageFormat(format!("Invalid JSON: {}", e)))?;

        info!("Received message from {}: {:?}", socket_id, message);

        let event_name_str = message
            .event
            .as_deref()
            .ok_or_else(|| Error::InvalidEventName("Event name is required".into()))?;
        let channel_name_option = message.channel.clone();

        // Client Event Rate Limiting
        if event_name_str.starts_with(CLIENT_EVENT_PREFIX) {
            if let Some(limiter_arc) = self.client_event_limiters.get(socket_id) {
                let limiter = limiter_arc.value(); // Get a reference to the Arc<dyn RateLimiter>
                let limit_result = limiter.increment(socket_id.as_ref()).await?;
                if !limit_result.allowed {
                    warn!(
                        "Client event rate limit exceeded for socket {}: event '{}'",
                        socket_id, event_name_str
                    );
                    // Send pusher:error JSON message (non-fatal for client event rate limit)
                    self.send_error(
                        &app_config.id,
                        socket_id,
                        &Error::ClientEventRateLimit, // This error (4301) is typically not fatal
                        channel_name_option.clone(),
                    )
                    .await?;
                    return Err(Error::ClientEventRateLimit); // Return error, but loop might continue
                }
            } else if app_config.max_client_events_per_second > 0 {
                // This case indicates a server logic error if a limiter was expected but not found.
                warn!(
                    "Client event rate limiter not found for socket {} though app config expects one. App: {}, Event: {}",
                    socket_id, app_config.id, event_name_str
                );
                let err = Error::InternalError("Rate limiter misconfiguration".to_string());
                // Send pusher:error (this internal error might be considered fatal by the server)
                self.send_error(&app_config.id, socket_id, &err, channel_name_option.clone())
                    .await?;
                // If this internal error is fatal, we need to close the WS.
                // For now, returning it. The calling loop in `handle_socket` will break.
                return Err(err);
            }
        }

        // Process the message based on event type
        let processing_result = match event_name_str {
            "pusher:ping" => self.handle_ping(&app_config.id, socket_id).await,
            "pusher:subscribe" => {
                self.handle_subscribe(socket_id, &app_config, &message)
                    .await
            }
            "pusher:unsubscribe" => {
                self.handle_unsubscribe(socket_id, &message, &app_config)
                    .await
            }
            "pusher:signin" => {
                self.handle_signin(socket_id, message.clone(), &app_config) // Clone message if needed by signin
                    .await
            }
            _ if event_name_str.starts_with(CLIENT_EVENT_PREFIX) => {
                self.handle_client_event(
                    &app_config,
                    socket_id,
                    event_name_str,
                    message.channel.as_deref(),
                    message
                        .data
                        .and_then(|d| serde_json::to_value(d).ok())
                        .unwrap_or_default(),
                )
                .await
            }
            _ => {
                warn!(
                    "Received unknown Pusher event '{}' from socket {}",
                    event_name_str, socket_id
                );
                // According to Pusher, unknown events should be ignored.
                Ok(())
            }
        };

        // Handle errors from processing
        if let Err(e) = processing_result {
            // Send pusher:error JSON message, unless it was already sent (e.g., for ClientEventRateLimit)
            if !matches!(e, Error::ClientEventRateLimit) {
                self.send_error(&app_config.id, socket_id, &e, channel_name_option)
                    .await
                    .unwrap_or_else(|send_err| {
                        error!("Failed to send error to socket {}: {}", socket_id, send_err);
                    });
            }

            if e.is_fatal() {
                info!(
                    "Fatal error encountered for socket {}: {}. Closing connection.",
                    socket_id, e
                );
                let mut connection_manager_locked = self.connection_manager.lock().await;
                if let Some(conn_arc) = connection_manager_locked
                    .get_connection(socket_id, &app_config.id)
                    .await
                {
                    let mut conn_locked = conn_arc.lock().await;
                    if let Err(close_err) = conn_locked.close(e.close_code(), e.to_string()).await {
                        warn!(
                            "Attempted to send WebSocket close frame to socket {} due to fatal error, but failed: {}. The connection might already be closing or closed.",
                            socket_id, close_err
                        );
                    }
                } else {
                    warn!(
                        "Fatal error for socket {}: connection not found in manager for explicit close.",
                        socket_id
                    );
                }
                drop(connection_manager_locked);

                // Perform full server-side cleanup for the disconnected socket.
                if let Err(disconnect_err) = self.handle_disconnect(&app_config.id, socket_id).await
                {
                    error!(
                        "Error during handle_disconnect after fatal error processing message for socket {}: {}",
                        socket_id, disconnect_err
                    );
                }
                // No need to remove client_event_limiter here, handle_socket's main loop exit will do it.
            }
            return Err(e); // Propagate the error to the caller (handle_socket's message loop)
        }

        // Record metrics for successfully processed messages
        if let Some(ref metrics) = self.metrics {
            let metrics_locked = metrics.lock().await;
            let message_size = msg_payload.len();
            metrics_locked.mark_ws_message_received(&app_config.id, message_size);
        }

        Ok(())
    }

    pub async fn handle_ping(&self, app_id: &str, socket_id: &SocketId) -> Result<()> {
        self.connection_manager
            .lock()
            .await
            .send_message(app_id, socket_id, PusherMessage::pong())
            .await
    }

    fn extract_signature(&self, message: &PusherMessage) -> Result<String> {
        match &message.data {
            Some(MessageData::String(_)) => {
                // This case is unlikely for subscribe messages which usually have structured data.
                // If it happens, it implies a malformed subscribe request.
                Err(Error::InvalidMessageFormat(
                    "Subscribe message data should be structured, not a plain string for auth."
                        .into(),
                ))
            }
            Some(MessageData::Json(data_val)) => Ok(data_val
                .get("auth")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()),
            Some(MessageData::Structured { extra, .. }) => Ok(extra
                .get("auth")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()),
            None => {
                // No data field, so no auth possible.
                // This is an error if the channel requires auth.
                Err(Error::InvalidMessageFormat(
                    "Missing data field in message requiring authentication.".into(),
                ))
            }
        }
    }

    pub async fn handle_subscribe(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        message: &PusherMessage,
    ) -> Result<()> {
        let channel_str = match &message.data {
            Some(MessageData::Structured { channel, .. }) => {
                channel.as_ref().map(|s| s.as_str()).ok_or_else(|| {
                    Error::ChannelError("Missing channel field in structured data".into())
                })?
            }
            Some(MessageData::Json(data_val)) => data_val
                .get("channel")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    Error::ChannelError("Missing 'channel' field in JSON data".into())
                })?,
            _ => {
                return Err(Error::InvalidMessageFormat(
                    "Subscribe message data malformed or missing channel".into(),
                ));
            }
        };

        if !app_config.enabled {
            // Check if app itself is enabled
            return Err(Error::ApplicationDisabled);
        }

        validate_channel_name(app_config, channel_str).await?;

        let is_authenticated = {
            let channel_manager_locked = self.channel_manager.read().await;
            // extract_signature now returns Result, handle it.
            let signature = match self.extract_signature(message) {
                Ok(s) => s,
                Err(_)
                    if !(channel_str.starts_with("presence-")
                        || channel_str.starts_with("private-")) =>
                {
                    // For public channels, missing auth is fine.
                    String::new()
                }
                Err(e) => return Err(e), // Malformed auth data for private/presence
            };

            if (channel_str.starts_with("presence-") || channel_str.starts_with("private-"))
                && signature.is_empty()
            {
                return Err(Error::AuthError(
                    "Authentication signature required for this channel".into(),
                ));
            }

            if signature.is_empty()
                && !(channel_str.starts_with("presence-") || channel_str.starts_with("private-"))
            {
                true // Public channel, no signature needed
            } else {
                channel_manager_locked.signature_is_valid(
                    app_config.clone(),
                    socket_id,
                    &signature,
                    message.clone(),
                )
            }
        };

        if (channel_str.starts_with("presence-") || channel_str.starts_with("private-"))
            && !is_authenticated
        {
            return Err(Error::AuthError("Invalid authentication signature".into()));
        }

        // Presence channel specific validations (member size, channel capacity)
        if channel_str.starts_with("presence-") {
            let user_info_from_data = match &message.data {
                Some(MessageData::Structured { channel_data, .. }) => {
                    Some(channel_data.as_ref().unwrap().as_str())
                }
                Some(MessageData::Json(json_data)) => Some(
                    json_data
                        .get("channel_data")
                        .and_then(Value::as_str)
                        .unwrap(),
                ),
                _ => None,
            };

            if let Some(cd_str) = user_info_from_data {
                let user_info_payload: Value = serde_json::from_str(cd_str).map_err(|_| {
                    Error::InvalidMessageFormat("Invalid channel_data JSON for presence".into())
                })?;
                let user_info_for_size_calc = user_info_payload
                    .get("user_info")
                    .cloned()
                    .unwrap_or_default();
                let user_info_size_kb =
                    utils::data_to_bytes_flexible(vec![user_info_for_size_calc]) / 1024;

                if let Some(max_size) = app_config.max_presence_member_size_in_kb {
                    if user_info_size_kb > max_size as usize {
                        return Err(Error::ChannelError(format!(
                            // This error should map to a 43xx code
                            "Presence member data size ({}KB) exceeds limit ({}KB)",
                            user_info_size_kb, max_size
                        )));
                    }
                }
            } else {
                // If channel_data is missing for presence, it's an issue.
                return Err(Error::InvalidMessageFormat(
                    "Missing 'channel_data' for presence channel subscription.".into(),
                ));
            }

            if let Some(max_members) = app_config.max_presence_members_per_channel {
                let current_members = self
                    .connection_manager
                    .lock()
                    .await
                    .get_channel_members(&app_config.id, channel_str) // Assuming this gets count across nodes if applicable
                    .await?
                    .len();
                if current_members >= max_members as usize {
                    return Err(Error::OverCapacity); // Pusher code 4100
                }
            }
        }

        let subscription_result = {
            let mut channel_manager_locked = self.channel_manager.write().await;
            channel_manager_locked
                .subscribe(
                    socket_id.0.as_str(),
                    message,
                    channel_str,
                    is_authenticated, // This is now correctly determined
                    &app_config.id,
                )
                .await? // Propagate errors from subscribe
        };

        // If channel_manager.subscribe itself determined an auth failure not caught earlier
        if !subscription_result.success {
            return Err(Error::AuthError(
                subscription_result.auth_error.unwrap_or_else(|| {
                    "Subscription failed due to an authentication issue within channel manager"
                        .to_string()
                }),
            ));
        }

        if subscription_result.channel_connections == Some(1) {
            if let Some(webhook_integration_instance) = &self.webhook_integration {
                webhook_integration_instance
                    .send_channel_occupied(app_config, channel_str)
                    .await
                    .ok();
            }
        }

        if !channel_str.starts_with("presence-") {
            if let Some(webhook_integration_instance) = &self.webhook_integration {
                let current_count = self
                    .connection_manager
                    .lock()
                    .await
                    .get_channel_socket_count(&app_config.id, channel_str)
                    .await;
                info!(
                    "Sending subscription_count webhook for channel {} (count: {}) after subscribe",
                    channel_str, current_count
                );
                webhook_integration_instance
                    .send_subscription_count_changed(app_config, channel_str, current_count)
                    .await
                    .ok();
            }
        }

        let channel_type = ChannelType::from_name(channel_str);
        let presence_data_tuple = if channel_type == ChannelType::Presence {
            subscription_result.member.as_ref().map(|presence_member| {
                (
                    presence_member.user_id.as_str(),
                    PresenceMemberInfo {
                        user_id: presence_member.user_id.clone(),
                        user_info: Some(presence_member.user_info.clone()),
                    },
                )
            })
        } else {
            None
        };

        {
            let mut connection_manager_locked = self.connection_manager.lock().await;
            if let Some(conn_arc) = connection_manager_locked
                .get_connection(socket_id, &app_config.id)
                .await
            {
                let mut conn_locked = conn_arc.lock().await;
                conn_locked
                    .state
                    .subscribed_channels
                    .insert(channel_str.to_string());

                if let Some((user_id_str, presence_info_val)) = presence_data_tuple {
                    conn_locked.state.user_id = Some(user_id_str.to_string());
                    if let Some(ref mut presence_map_val) = conn_locked.state.presence {
                        presence_map_val.insert(channel_str.to_string(), presence_info_val);
                    } else {
                        let mut new_presence_map = HashMap::new();
                        new_presence_map.insert(channel_str.to_string(), presence_info_val);
                        conn_locked.state.presence = Some(new_presence_map);
                    }
                }
            }
        }

        if channel_type == ChannelType::Presence {
            if let Some(presence_member) = subscription_result.member {
                let user_id_str = &presence_member.user_id;
                let presence_info_val = PresenceMemberInfo {
                    user_id: user_id_str.clone(),
                    user_info: Some(presence_member.user_info.clone()),
                };

                if let Some(webhook_integration_instance) = &self.webhook_integration {
                    webhook_integration_instance
                        .send_member_added(app_config, channel_str, user_id_str)
                        .await
                        .ok();
                }

                let members_map = {
                    let mut connection_manager_locked = self.connection_manager.lock().await;
                    let current_members = connection_manager_locked
                        .get_channel_members(&app_config.id, channel_str)
                        .await?;
                    let member_added_msg = PusherMessage::member_added(
                        channel_str.to_string(),
                        user_id_str.clone(),
                        presence_info_val.user_info.clone(),
                    );
                    connection_manager_locked
                        .send(
                            channel_str,
                            member_added_msg,
                            Some(socket_id),
                            &app_config.id,
                        )
                        .await?;
                    current_members
                };

                let presence_message_val = json!({
                    "presence": {
                        "ids": members_map.keys().collect::<Vec<&String>>(),
                        "hash": members_map.iter()
                            .map(|(k, v)| (k.as_str(), v.user_info.clone()))
                            .collect::<HashMap<&str, Option<Value>>>(),
                        "count": members_map.len()
                    }
                });
                let subscription_succeeded_msg = PusherMessage::subscription_succeeded(
                    channel_str.to_string(),
                    Some(presence_message_val),
                );
                self.connection_manager
                    .lock()
                    .await
                    .send_message(&app_config.id, socket_id, subscription_succeeded_msg)
                    .await?;
            }
        } else {
            let response_msg = PusherMessage::subscription_succeeded(channel_str.to_string(), None);
            self.connection_manager
                .lock()
                .await
                .send_message(&app_config.id, socket_id, response_msg)
                .await?;
        }
        if is_cache_channel(channel_str) {
            self.send_missed_cache_if_exists(&app_config.id, socket_id, channel_str)
                .await?;
        }
        Ok(())
    }

    pub async fn handle_unsubscribe(
        &self,
        socket_id: &SocketId,
        message: &PusherMessage,
        app_config: &App,
    ) -> Result<()> {
        let message_data_ref = message.data.as_ref().ok_or_else(|| {
            Error::InvalidMessageFormat("Missing data in unsubscribe message".into())
        })?;
        let channel_name_str = match message_data_ref {
            MessageData::String(channel_str_val) => channel_str_val.as_str(),
            MessageData::Json(data_val) => data_val
                .get("channel")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    Error::InvalidMessageFormat("Missing channel in unsubscribe message".into())
                })?,
            MessageData::Structured { channel, .. } => {
                channel.as_ref().map(|s| s.as_str()).ok_or_else(|| {
                    Error::InvalidMessageFormat("Missing channel in unsubscribe message".into())
                })?
            }
        };

        let user_id_of_socket: Option<String> = {
            let mut conn_manager = self.connection_manager.lock().await;
            if let Some(conn) = conn_manager.get_connection(socket_id, &app_config.id).await {
                conn.lock().await.state.user_id.clone()
            } else {
                None
            }
        };

        let _leave_response = {
            let mut channel_manager_locked = self.channel_manager.write().await;
            channel_manager_locked
                .unsubscribe(
                    socket_id.0.as_str(),
                    channel_name_str,
                    &app_config.id,
                    user_id_of_socket.as_deref(),
                )
                .await? // Propagate errors
        };

        {
            let mut conn_manager = self.connection_manager.lock().await;
            if let Some(conn_arc) = conn_manager.get_connection(socket_id, &app_config.id).await {
                let mut conn_state_guard = conn_arc.lock().await;
                conn_state_guard
                    .state
                    .subscribed_channels
                    .remove(channel_name_str);
                if channel_name_str.starts_with("presence-") {
                    if let Some(presence_map) = conn_state_guard.state.presence.as_mut() {
                        presence_map.remove(channel_name_str);
                    }
                }
            }
        }

        let current_sub_count = self
            .connection_manager
            .lock()
            .await
            .get_channel_socket_count(&app_config.id, channel_name_str)
            .await;

        if channel_name_str.starts_with("presence-") {
            if let Some(user_id_that_left) = user_id_of_socket {
                let has_other_connections = self
                    .user_has_other_connections_in_presence_channel(
                        &app_config.id,
                        channel_name_str,
                        &user_id_that_left,
                    )
                    .await?;

                if !has_other_connections {
                    if let Some(webhook_integration_instance) = &self.webhook_integration {
                        info!(
                            "Sending member_removed webhook for user {} from channel {}",
                            user_id_that_left, channel_name_str
                        );
                        webhook_integration_instance
                            .send_member_removed(app_config, channel_name_str, &user_id_that_left)
                            .await
                            .ok();
                    }
                    let member_removed_msg = PusherMessage::member_removed(
                        channel_name_str.to_string(),
                        user_id_that_left.clone(),
                    );
                    self.connection_manager
                        .lock()
                        .await
                        .send(
                            channel_name_str,
                            member_removed_msg,
                            Some(socket_id),
                            &app_config.id,
                        )
                        .await?;
                }
            }
        } else if let Some(webhook_integration_instance) = &self.webhook_integration {
            info!(
                "Sending subscription_count webhook for channel {} (count: {}) after unsubscribe",
                channel_name_str, current_sub_count
            );
            webhook_integration_instance
                .send_subscription_count_changed(app_config, channel_name_str, current_sub_count)
                .await
                .ok();
        }

        if current_sub_count == 0 {
            if let Some(webhook_integration_instance) = &self.webhook_integration {
                info!(
                    "Sending channel_vacated webhook for channel {}",
                    channel_name_str
                );
                webhook_integration_instance
                    .send_channel_vacated(app_config, channel_name_str)
                    .await
                    .ok();
            }
        }
        Ok(())
    }

    pub async fn handle_signin(
        &self,
        socket_id: &SocketId,
        data: PusherMessage,
        app_config: &App,
    ) -> Result<()> {
        if !app_config.enable_user_authentication.unwrap_or(false) {
            // Default to false if None
            return Err(Error::AuthError(
                "User authentication is disabled for this app".into(),
            ));
        }
        let message_data_val = data
            .data
            .ok_or_else(|| Error::AuthError("Missing data in signin message".into()))?;

        let (user_data_str, auth_str) = {
            let extract_field = |field: &str| -> Result<&str> {
                match &message_data_val {
                    MessageData::Json(json_val) => {
                        json_val.get(field).and_then(|v| v.as_str()).ok_or_else(|| {
                            Error::AuthError(format!(
                                "Missing '{}' field in signin JSON data",
                                field
                            ))
                        })
                    }
                    MessageData::Structured { extra, .. } => {
                        extra.get(field).and_then(|v| v.as_str()).ok_or_else(|| {
                            Error::AuthError(format!(
                                "Missing '{}' field in signin structured data",
                                field
                            ))
                        })
                    }
                    MessageData::String(_) => Err(Error::InvalidMessageFormat(
                        "Signin data should be structured, not a plain string.".into(),
                    )),
                }
            };
            (extract_field("user_data")?, extract_field("auth")?)
        };

        let user_info_val: Value = serde_json::from_str(user_data_str)
            .map_err(|e| Error::AuthError(format!("Invalid user_data JSON: {}", e)))?;

        let auth_validator = AuthValidator::new(self.app_manager.clone());
        let is_valid_auth = auth_validator
            .validate_channel_auth(socket_id.clone(), &app_config.key, user_data_str, auth_str)
            .await?;

        if !is_valid_auth {
            return Err(Error::AuthError(
                "Connection not authorized for signin.".into(),
            ));
        }

        let mut connection_manager_locked = self.connection_manager.lock().await;

        let connection_arc = connection_manager_locked
            .get_connection(socket_id, &app_config.id)
            .await
            .ok_or_else(|| Error::ConnectionNotFound)?;

        // Temporarily take the WebSocketWrite half to re-add the socket
        // This is a bit complex; ideally, the adapter would handle user association without needing to re-add.
        let temp_socket_tx = {
            let mut conn_locked = connection_arc.lock().await;
            conn_locked.state.user = Some(user_info_val.clone());
            conn_locked.socket.take() // Take ownership of the Option<WebSocketWrite>
        };

        if let Some(socket_tx_val) = temp_socket_tx {
            // Re-add the socket to the manager, which might re-initialize its send loop.
            // This ensures the user state is associated correctly if the adapter's add_socket logic handles it.
            connection_manager_locked
                .add_socket(
                    socket_id.clone(),
                    socket_tx_val,
                    app_config.id.as_str(),
                    &self.app_manager,
                )
                .await?; // Propagate error if re-adding fails
        } else {
            // This should not happen if the connection is live.
            error!(
                "Socket write half was None during signin for socket {}",
                socket_id
            );
            return Err(Error::InternalError(
                "Socket state inconsistent during signin".into(),
            ));
        }

        // Add user to adapter's user tracking
        connection_manager_locked
            .add_user(connection_arc.clone())
            .await?;

        let success_message_val = PusherMessage {
            channel: None,
            name: None,
            event: Some("pusher:signin_success".into()),
            data: Some(MessageData::Json(user_info_val)),
        };

        connection_manager_locked
            .send_message(&app_config.id, socket_id, success_message_val)
            .await?;

        Ok(())
    }

    async fn handle_client_event(
        &self,
        app_config: &App,
        socket_id: &SocketId,
        event: &str,
        channel: Option<&str>,
        data: Value,
    ) -> Result<()> {
        let channel_name = channel.ok_or_else(|| {
            Error::ClientEventError("Channel name is required for client event".into())
        })?;

        // Validate event name length
        let max_event_name_len = app_config
            .max_event_name_length
            .unwrap_or(DEFAULT_EVENT_NAME_MAX_LENGTH as u32);
        if event.len() > max_event_name_len as usize {
            return Err(Error::InvalidEventName(format!(
                "Client event name '{}' exceeds maximum length of {}",
                event, max_event_name_len
            )));
        }

        // Validate payload size
        if let Some(max_payload_kb) = app_config.max_event_payload_in_kb {
            let payload_size_bytes = utils::data_to_bytes_flexible(vec![data.clone()]);
            if payload_size_bytes > (max_payload_kb as usize * 1024) {
                return Err(Error::ClientEventError(format!(
                    "Client event payload size ({} bytes) for event '{}' exceeds limit ({}KB)",
                    payload_size_bytes, event, max_payload_kb
                )));
            }
        }

        // Validate event prefix
        if !event.starts_with(CLIENT_EVENT_PREFIX) {
            return Err(Error::InvalidEventName(
                "Client events must start with 'client-'".into(),
            ));
        }

        // Validate channel name length
        let max_channel_len = app_config
            .max_channel_name_length
            .unwrap_or(DEFAULT_CHANNEL_NAME_MAX_LENGTH as u32);
        if channel_name.len() > max_channel_len as usize {
            return Err(Error::InvalidChannelName(format!(
                "Channel name '{}' for client event exceeds maximum length of {}",
                channel_name, max_channel_len
            )));
        }

        // Validate channel type (must be private or presence)
        let channel_type = ChannelType::from_name(channel_name);
        if !matches!(channel_type, ChannelType::Private | ChannelType::Presence) {
            return Err(Error::ClientEventError(
                "Client events can only be sent to private or presence channels".into(),
            ));
        }

        // Check if app allows client messages
        if !app_config.enable_client_messages {
            return Err(Error::ClientEventError(
                "Client events are not enabled for this app".into(),
            ));
        }

        // Verify socket is subscribed to the channel
        let (is_subscribed_globally, user_id_for_webhook) = {
            let mut conn_manager_locked = self.connection_manager.lock().await;
            let subscribed = conn_manager_locked
                .is_in_channel(&app_config.id, channel_name, socket_id)
                .await?;

            let user_id = if let Some(conn_arc) = conn_manager_locked
                .get_connection(socket_id, &app_config.id)
                .await
            {
                conn_arc
                    .lock()
                    .await
                    .state
                    .presence
                    .as_ref()
                    .and_then(|p_map| p_map.get(channel_name))
                    .map(|pi| pi.user_id.clone())
            } else {
                None
            };
            (subscribed, user_id)
        };

        if !is_subscribed_globally {
            return Err(Error::ClientEventError(format!(
                "Client {} is not subscribed to channel {} (or subscription check failed)",
                socket_id, channel_name
            )));
        }

        // Construct the message to send
        let message_to_send = PusherMessage {
            channel: Some(channel_name.to_string()),
            name: None, // Pusher messages use 'event', not 'name' for client-side
            event: Some(event.to_string()),
            data: Some(MessageData::Json(data.clone())),
        };

        // Send the message via the adapter (broadcasts to channel, excluding sender)
        {
            let mut conn_manager_locked = self.connection_manager.lock().await;
            conn_manager_locked
                .send(
                    channel_name,
                    message_to_send.clone(),
                    Some(socket_id), // Exclude the sender
                    &app_config.id,
                )
                .await?;
        }

        // Send webhook if configured
        if let Some(webhook_integration_val) = &self.webhook_integration {
            // For client events, Pusher includes user_id if it's a presence channel
            let final_user_id_for_webhook = if channel_name.starts_with("presence-") {
                user_id_for_webhook.as_deref()
            } else {
                None
            };

            webhook_integration_val
                .send_client_event(
                    app_config,
                    channel_name,
                    event,
                    data, // Send the original data
                    Some(socket_id.as_ref()),
                    final_user_id_for_webhook,
                )
                .await
                .unwrap_or_else(|e| {
                    warn!(
                        "Failed to send client_event webhook for {}: {}",
                        channel_name, e
                    );
                });
        }

        Ok(())
    }

    pub async fn send_error(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        error: &Error,
        channel: Option<String>,
    ) -> Result<()> {
        let error_data = ErrorData {
            message: error.to_string(),
            code: Some(error.close_code()),
        };
        let error_message =
            PusherMessage::error(error_data.code.unwrap_or(4000), error_data.message, channel);
        self.connection_manager
            .lock()
            .await
            .send_message(app_id, socket_id, error_message) // This uses the adapter's send_message
            .await
    }

    pub async fn send_connection_established(
        &self,
        app_id: &str,
        socket_id: &SocketId,
    ) -> Result<()> {
        let connection_message = PusherMessage::connection_established(socket_id.0.clone());
        self.connection_manager
            .lock()
            .await
            .send_message(app_id, socket_id, connection_message)
            .await
    }

    pub async fn handle_disconnect(&self, app_id: &str, socket_id: &SocketId) -> Result<()> {
        info!("Handling disconnect for socket: {}", socket_id);
        if self.client_event_limiters.remove(socket_id).is_some() {
            info!(
                "Removed client event rate limiter for socket: {}",
                socket_id
            );
        }

        let app_config = match self.app_manager.find_by_id(app_id).await? {
            Some(app) => app,
            None => {
                error!("App not found during disconnect: {}", app_id);
                let mut conn_manager = self.connection_manager.lock().await;
                // Attempt cleanup even if app is gone, as adapter might hold stale socket info.
                if let Some(conn_to_cleanup) = conn_manager.get_connection(socket_id, app_id).await
                {
                    conn_manager
                        .cleanup_connection(app_id, WebSocketRef(conn_to_cleanup))
                        .await;
                }
                conn_manager.remove_connection(socket_id, app_id).await.ok();
                return Err(Error::ApplicationNotFound); // Use a more specific error
            }
        };

        let (subscribed_channels_set, user_id_of_disconnected_socket) = {
            let mut connection_manager_locked = self.connection_manager.lock().await;
            match connection_manager_locked
                .get_connection(socket_id, app_id)
                .await
            {
                Some(conn_val_arc) => {
                    let conn_locked = conn_val_arc.lock().await;
                    (
                        conn_locked.state.subscribed_channels.clone(),
                        conn_locked.state.user_id.clone(),
                    )
                }
                None => {
                    warn!(
                        "No connection found for socket during disconnect: {}. Already cleaned up?",
                        socket_id
                    );
                    // If no connection state, still attempt adapter-level removal.
                    connection_manager_locked
                        .remove_connection(socket_id, app_id)
                        .await
                        .ok();
                    return Ok(());
                }
            }
        };

        if !subscribed_channels_set.is_empty() {
            info!(
                "Processing {} channels for disconnecting socket: {}",
                subscribed_channels_set.len(),
                socket_id
            );

            let channel_manager_locked = self.channel_manager.write().await;

            for channel_str in &subscribed_channels_set {
                info!(
                    "Processing channel {} for disconnect of socket {}",
                    channel_str, socket_id
                );

                match channel_manager_locked
                    .unsubscribe(
                        socket_id.0.as_str(),
                        channel_str,
                        app_id,
                        user_id_of_disconnected_socket.as_deref(),
                    )
                    .await
                {
                    Ok(_leave_response) => {
                        let current_sub_count_after_cm_unsubscribe = self
                            .connection_manager
                            .lock()
                            .await
                            .get_channel_socket_count(app_id, channel_str)
                            .await;

                        if channel_str.starts_with("presence-") {
                            if let Some(ref disconnected_user_id) = user_id_of_disconnected_socket {
                                let has_other_connections = self
                                    .user_has_other_connections_in_presence_channel(
                                        app_id,
                                        channel_str,
                                        disconnected_user_id,
                                    )
                                    .await?;

                                if !has_other_connections {
                                    if let Some(webhook_integration_instance) =
                                        &self.webhook_integration
                                    {
                                        info!(
                                            "Sending member_removed webhook for user {} from channel {}",
                                            disconnected_user_id, channel_str
                                        );
                                        webhook_integration_instance
                                            .send_member_removed(
                                                &app_config,
                                                channel_str,
                                                disconnected_user_id,
                                            )
                                            .await
                                            .ok();
                                    }
                                    let member_removed_msg = PusherMessage::member_removed(
                                        channel_str.to_string(),
                                        disconnected_user_id.clone(),
                                    );
                                    self.connection_manager
                                        .lock()
                                        .await
                                        .send(
                                            channel_str,
                                            member_removed_msg,
                                            Some(socket_id),
                                            app_id,
                                        )
                                        .await
                                        .ok();
                                }
                            }
                        } else if let Some(webhook_integration_instance) = &self.webhook_integration
                        {
                            info!(
                                "Sending subscription_count webhook for channel {} (count: {}) after disconnect processing",
                                channel_str, current_sub_count_after_cm_unsubscribe
                            );
                            webhook_integration_instance
                                .send_subscription_count_changed(
                                    &app_config,
                                    channel_str,
                                    current_sub_count_after_cm_unsubscribe,
                                )
                                .await
                                .ok();
                        }

                        if current_sub_count_after_cm_unsubscribe == 0 {
                            if let Some(webhook_integration_instance) = &self.webhook_integration {
                                info!(
                                    "Sending channel_vacated webhook for channel {}",
                                    channel_str
                                );
                                webhook_integration_instance
                                    .send_channel_vacated(&app_config, channel_str)
                                    .await
                                    .ok();
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Error unsubscribing socket {} from channel {} during disconnect: {}",
                            socket_id, channel_str, e
                        );
                    }
                }
            }
        }

        // Final removal from the adapter's main connection tracking.
        // This also handles the case where the socket might not have been subscribed to any channels.
        {
            let mut connection_manager_locked = self.connection_manager.lock().await;
            // cleanup_connection should be called to ensure all adapter-specific resources are freed.
            // It might be redundant if the connection was already removed due to a fatal error,
            // but it should be safe to call.
            if let Some(conn_to_cleanup) = connection_manager_locked
                .get_connection(socket_id, app_id)
                .await
            {
                connection_manager_locked
                    .cleanup_connection(app_id, WebSocketRef(conn_to_cleanup))
                    .await;
            }
            // Ensure it's removed from the primary map if cleanup_connection doesn't do that.
            connection_manager_locked
                .remove_connection(socket_id, app_id)
                .await
                .ok();
        }

        info!(
            "Successfully processed full disconnect for socket: {}",
            socket_id
        );
        Ok(())
    }

    // --- HTTP API related methods (not directly part of WebSocket connection handling loop) ---

    pub async fn channel(&self, app_id: &str, channel_name: &str) -> Value {
        let socket_count_val = self
            .connection_manager
            .lock()
            .await
            .get_channel_socket_count(app_id, channel_name)
            .await;
        json!({
            "occupied": socket_count_val > 0,
            "subscription_count": socket_count_val,
        })
    }

    pub async fn channels(&self, app_id: &str) -> Value {
        let mut connection_manager_locked = self.connection_manager.lock().await;
        let channels_map_result = connection_manager_locked
            .get_channels_with_socket_count(app_id)
            .await;
        let mut response_val = json!({});

        if let Ok(channels_map) = channels_map_result {
            for channel_entry in channels_map.iter() {
                let channel_name_str = channel_entry.key().clone();
                let socket_count_val = *channel_entry.value();
                response_val[channel_name_str] = json!({
                    "occupied": socket_count_val > 0,
                    "subscription_count": socket_count_val,
                });
            }
        } else if let Err(e) = channels_map_result {
            error!(
                "Failed to get channels with socket count for app {}: {}",
                app_id, e
            );
        }
        response_val
    }

    pub async fn channel_users(
        &self,
        app_id: &str,
        channel_name: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        let channel_type_val = ChannelType::from_name(channel_name);
        if channel_type_val != ChannelType::Presence {
            return Err(Error::ChannelError(
                "Channel is not a presence channel".into(),
            ));
        }
        let channel_manager_locked = self.channel_manager.read().await;
        channel_manager_locked
            .get_channel_members(app_id, channel_name)
            .await
    }

    pub async fn send_message(
        &self,
        app_id: &str,
        socket_id: Option<&SocketId>,
        message: PusherApiMessage,
        channel: &str,
    ) {
        let pusher_message_val = PusherMessage {
            event: message.name,
            data: message.data.map(|api_data| match api_data {
                crate::protocol::messages::ApiMessageData::String(s) => MessageData::String(s),
                crate::protocol::messages::ApiMessageData::Json(j) => MessageData::Json(j),
            }),
            channel: Some(channel.to_string()),
            name: None,
        };

        if let Some(ref metrics) = self.metrics {
            let metrics_locked = metrics.lock().await;
            let message_size_val =
                serde_json::to_string(&pusher_message_val).map_or(0, |s| s.len());
            metrics_locked.mark_ws_message_sent(app_id, message_size_val);
        }

        if let Err(e) = self
            .connection_manager
            .lock()
            .await
            .send(channel, pusher_message_val, socket_id, app_id)
            .await
        {
            error!("Failed to send message to channel {}: {:?}", channel, e);
        } else {
            info!(
                "Message sent to channel {} successfully (via HTTP API path)",
                channel
            );
        }
    }
}
