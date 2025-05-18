use crate::adapter::adapter::Adapter;
use crate::app::auth::AuthValidator;
use crate::app::config::App;
use crate::app::manager::AppManager;
use crate::cache::manager::CacheManager;
use crate::channel::{ChannelManager, ChannelType, PresenceMemberInfo}; // Added ChannelManager import

use crate::metrics::MetricsInterface;
use crate::protocol::constants::{
    CHANNEL_NAME_MAX_LENGTH as DEFAULT_CHANNEL_NAME_MAX_LENGTH, CLIENT_EVENT_PREFIX,
    EVENT_NAME_MAX_LENGTH as DEFAULT_EVENT_NAME_MAX_LENGTH,
};
use crate::protocol::messages::{ErrorData, MessageData, PusherApiMessage, PusherMessage};
use crate::rate_limiter::{memory_limiter::MemoryRateLimiter, RateLimiter};
use crate::utils::validate_channel_name;
use crate::webhook::integration::WebhookIntegration;
use crate::websocket::{SocketId, WebSocketRef};
use crate::{
    error::{Error, Result},
    utils,
};
use dashmap::DashMap;
use fastwebsockets::{upgrade, FragmentCollectorRead, Frame, OpCode, WebSocketError};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
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
    /// This is called *after* the current socket is assumed to be removed from the channel's direct accounting.
    async fn user_has_other_connections_in_presence_channel(
        &self,
        app_id: &str,
        channel_name: &str,
        user_id: &str,
    ) -> Result<bool> {
        let mut connection_manager = self.connection_manager.lock().await;
        // Get all sockets associated with this user_id for the given app_id
        let user_sockets = connection_manager.get_user_sockets(user_id, app_id).await?;

        for ws_ref in user_sockets.iter() {
            let socket_state_guard = ws_ref.0.lock().await; // WebSocketRef(Arc<Mutex<WebSocket>>)
                                                            // Check if this specific socket (belonging to the user) is subscribed to the channel in question.
            if socket_state_guard.state.is_subscribed(channel_name) {
                return Ok(true); // Found another active socket for this user in this channel
            }
        }
        Ok(false) // No other active sockets for this user found in this channel
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
                        warn!("{}", format!("Webhook event failed to send: {}", e));
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
                info!("{}", format!("No missed cache for channel: {}", channel));
            }
            Err(e) => {
                error!(
                    "{}",
                    format!("Failed to get cache for channel {}: {}", channel, e)
                );
                return Err(e);
            }
        }
        Ok(())
    }

    pub async fn handle_socket(&self, fut: upgrade::UpgradeFut, app_key: String) -> Result<()> {
        let app_config = self
            .app_manager
            .find_by_key(&app_key)
            .await?
            .ok_or(Error::InvalidAppKey)?;

        info!("{}", format!("Placeholder: App {} has {} max connections. Current connection count check would go here.", app_config.id, app_config.max_connections));
        let max_connections = app_config.max_connections;
        if max_connections > 0 {
            let mut connection_manager_locked = self.connection_manager.lock().await;
            let current_connections = connection_manager_locked
                .get_sockets_count(&app_config.id)
                .await;
            if current_connections? >= max_connections as usize {
                return Err(Error::OverConnectionQuota);
            }
        }
        let socket = fut.await?;
        let (socket_rx, socket_tx) = socket.split(tokio::io::split);
        let socket_id = SocketId::new();
        info!(
            "{}",
            format!("New socket: {} for app: {}", socket_id, app_config.id)
        );

        {
            let mut connection_manager_locked = self.connection_manager.lock().await;
            if let Some(conn) = connection_manager_locked
                .get_connection(&socket_id, &app_config.id)
                .await
            {
                connection_manager_locked
                    .cleanup_connection(&app_config.id, WebSocketRef(conn))
                    .await;
            }
            connection_manager_locked
                .add_socket(
                    socket_id.clone(),
                    socket_tx,
                    &app_config.id,
                    &self.app_manager,
                )
                .await
                .map_err(|e| {
                    error!("{}", format!("Failed to add socket: {}", e));
                    WebSocketError::ConnectionClosed
                })?;
            if let Some(ref metrics) = self.metrics {
                let metrics_locked = metrics.lock().await;
                metrics_locked.mark_new_connection(&app_config.id, &socket_id)
            }
        }

        if app_config.max_client_events_per_second > 0 {
            let limiter = Arc::new(MemoryRateLimiter::new(
                app_config.max_client_events_per_second,
                1,
            ));
            self.client_event_limiters
                .insert(socket_id.clone(), limiter);
            info!(
                "{}",
                format!(
                    "Initialized client event rate limiter for socket {}: {} events/sec",
                    socket_id, app_config.max_client_events_per_second
                )
            );
        }

        if let Err(e) = self
            .send_connection_established(&app_config.id, &socket_id)
            .await
        {
            self.send_error(&app_config.id, &socket_id, &e, None)
                .await
                .map_err(|err_send| {
                    error!(
                        "{}",
                        format!("Failed to send connection established: {}", err_send)
                    );
                    WebSocketError::ConnectionClosed
                })?;
            self.client_event_limiters.remove(&socket_id);
            return Ok(());
        }

        let mut socket_rx_collected = FragmentCollectorRead::new(socket_rx);

        while let Ok(frame) = socket_rx_collected
            .read_frame(&mut move |_| async { Ok::<_, WebSocketError>(()) })
            .await
        {
            match frame.opcode {
                OpCode::Close => {
                    if let Some(ref metrics) = self.metrics {
                        let metrics_locked = metrics.lock().await;
                        metrics_locked.mark_disconnection(&app_config.id, &socket_id);
                    }
                    if let Err(e) = self.handle_disconnect(&app_config.id, &socket_id).await {
                        error!(
                            "{}",
                            format!("Disconnect error for socket {}: {}", socket_id, e)
                        );
                    }
                    break;
                }
                OpCode::Text | OpCode::Binary => {
                    if let Err(e) = self
                        .handle_message(frame, &socket_id, app_config.clone())
                        .await
                    {
                        error!(
                            "{}",
                            format!("Message handling error for socket {}: {}", socket_id, e)
                        );
                        self.send_error(&app_config.id, &socket_id, &e, None)
                            .await
                            .ok();
                    }
                }
                OpCode::Ping => {
                    let mut connection_manager_locked = self.connection_manager.lock().await;
                    if let Some(conn_arc) = connection_manager_locked
                        .get_connection(&socket_id, &app_config.id)
                        .await
                    {
                        let mut conn_locked = conn_arc.lock().await;
                        conn_locked.state.update_ping();
                    }
                }
                _ => {
                    warn!("{}", format!("Unsupported opcode: {:?}", frame.opcode));
                }
            }
        }
        self.client_event_limiters.remove(&socket_id);
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

        info!(
            "{}",
            format!("Received message from {}: {:?}", socket_id, message)
        );

        let event_name_str = message
            .event
            .as_deref()
            .ok_or_else(|| Error::InvalidEventName("Event name is required".into()))?;
        let channel_name_option = message.channel.clone();

        if event_name_str.starts_with(CLIENT_EVENT_PREFIX) {
            if let Some(limiter) = self.client_event_limiters.get(socket_id) {
                let limit_result = limiter.increment(socket_id.as_ref()).await?;
                if !limit_result.allowed {
                    warn!(
                        "{}",
                        format!(
                            "Client event rate limit exceeded for socket {}: event '{}'",
                            socket_id, event_name_str
                        )
                    );
                    self.send_error(
                        &app_config.id,
                        socket_id,
                        &Error::ClientEventRateLimit,
                        channel_name_option.clone(),
                    )
                    .await?;
                    return Err(Error::ClientEventRateLimit);
                }
            } else if app_config.max_client_events_per_second > 0 {
                warn!("{}", format!(
                    "Client event rate limiter not found for socket {} though app config expects one. App: {}, Event: {}",
                    socket_id, app_config.id, event_name_str
                ));
                self.send_error(
                    &app_config.id,
                    socket_id,
                    &Error::InternalError("Rate limiter misconfiguration".to_string()),
                    channel_name_option.clone(),
                )
                .await?;
                return Err(Error::InternalError(
                    "Client event rate limiter missing".to_string(),
                ));
            }
        }

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
                self.handle_signin(socket_id, message.clone(), &app_config)
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
            _ => Ok(()),
        };

        if let Err(e) = processing_result {
            if !matches!(e, Error::ClientEventRateLimit) {
                self.send_error(&app_config.id, socket_id, &e, channel_name_option)
                    .await?;
            }

            if e.is_fatal() {
                let mut connection_manager_locked = self.connection_manager.lock().await;
                if let Some(conn_arc) = connection_manager_locked
                    .get_connection(socket_id, &app_config.id)
                    .await
                {
                    connection_manager_locked
                        .cleanup_connection(&app_config.id, WebSocketRef(conn_arc))
                        .await;
                }
                self.client_event_limiters.remove(socket_id);
            }
            return Err(e);
        }

        if let Some(ref metrics) = self.metrics {
            let metrics_locked = metrics.lock().await;
            let message_size = frame.payload.len();
            metrics_locked.mark_ws_message_received(&app_config.id, message_size);
        }

        Ok(())
    }

    pub async fn handle_ping(&self, app_id: &str, socket_id: &SocketId) -> Result<()> {
        self.connection_manager
            .lock()
            .await
            .send_message(
                app_id,
                socket_id,
                PusherMessage {
                    channel: None,
                    name: None,
                    event: Some("pusher:pong".to_string()),
                    data: None,
                },
            )
            .await
    }

    fn extract_signature(&self, message: &PusherMessage) -> Result<String> {
        match &message.data {
            Some(MessageData::String(sig)) => Ok(sig.to_string()),
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
            None => Ok(String::new()),
        }
    }

    pub async fn handle_subscribe(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        message: &PusherMessage,
    ) -> Result<()> {
        let channel_str =
            match &message.data {
                Some(MessageData::String(data_str)) => data_str.as_str(),
                Some(MessageData::Structured { channel, .. }) => channel
                    .as_ref()
                    .map(|s| s.as_str())
                    .ok_or_else(|| Error::ChannelError("Missing channel".into()))?,
                Some(MessageData::Json(data_val)) => data_val
                    .get("channel")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::ChannelError("Missing channel".into()))?,
                None => return Err(Error::ChannelError("Missing channel data".into())),
            };
        if !app_config.enable_client_messages {
            return Err(Error::ChannelError(
                "Client messages are disabled for this app".into(),
            ));
        }

        validate_channel_name(&app_config, channel_str).await?;

        let is_authenticated = {
            let channel_manager_locked = self.channel_manager.read().await;
            let signature = self.extract_signature(message)?;

            if (channel_str.starts_with("presence-") || channel_str.starts_with("private-"))
                && signature.is_empty()
            {
                return Err(Error::AuthError("Authentication required".into()));
            }

            channel_manager_locked.signature_is_valid(
                app_config.clone(),
                socket_id,
                &signature,
                message.clone(),
            )
        };

        if channel_str.starts_with("presence-") {
            if let Some(MessageData::Structured { channel_data, .. }) = &message.data {
                if let Some(cd_str) = channel_data {
                    let user_info_payload: Value = serde_json::from_str(cd_str).map_err(|_| {
                        Error::InvalidMessageFormat("Invalid channel_data JSON for presence".into())
                    })?;
                    let user_info_size_kb = utils::data_to_bytes_flexible(vec![user_info_payload
                        .get("user_info")
                        .cloned()
                        .unwrap_or_default()])
                        / 1024;
                    if let Some(max_size) = app_config.max_presence_member_size_in_kb {
                        if user_info_size_kb > max_size as usize {
                            return Err(Error::ChannelError(format!(
                                "Presence member data size ({}KB) exceeds limit ({}KB)",
                                user_info_size_kb, max_size
                            )));
                        }
                    }
                }
            } else if let Some(MessageData::Json(json_data)) = &message.data {
                if let Some(cd_str) = json_data.get("channel_data").and_then(Value::as_str) {
                    let user_info_payload: Value = serde_json::from_str(cd_str).map_err(|_| {
                        Error::InvalidMessageFormat("Invalid channel_data JSON for presence".into())
                    })?;
                    let user_info_size_kb = utils::data_to_bytes_flexible(vec![user_info_payload
                        .get("user_info")
                        .cloned()
                        .unwrap_or_default()])
                        / 1024;
                    if let Some(max_size) = app_config.max_presence_member_size_in_kb {
                        if user_info_size_kb > max_size as usize {
                            return Err(Error::ChannelError(format!(
                                "Presence member data size ({}KB) exceeds limit ({}KB)",
                                user_info_size_kb, max_size
                            )));
                        }
                    }
                }
            }

            if let Some(max_members) = app_config.max_presence_members_per_channel {
                let current_members = self
                    .connection_manager
                    .lock()
                    .await
                    .get_channel_members(&app_config.id, channel_str)
                    .await?
                    .len();
                if current_members >= max_members as usize {
                    return Err(Error::ChannelError(format!(
                        "Presence channel {} at capacity ({})",
                        channel_str, max_members
                    )));
                }
            }
        }

        let subscription_result = {
            let channel_manager_locked = self.channel_manager.write().await;
            channel_manager_locked
                .subscribe(
                    socket_id.0.as_str(),
                    message,
                    channel_str,
                    is_authenticated,
                    &app_config.id,
                )
                .await
                .map_err(|e| {
                    error!("{}", format!("Error subscribing to channel: {:?}", e));
                    Error::ChannelError("Failed to subscribe".into())
                })?
        };

        if !subscription_result.success {
            return self
                .send_error(
                    &app_config.id,
                    socket_id,
                    &Error::AuthError("Invalid authentication signature".into()),
                    Some(channel_str.to_string()),
                )
                .await;
        }

        if subscription_result.channel_connections == Some(1) {
            if let Some(webhook_integration_instance) = &self.webhook_integration {
                webhook_integration_instance
                    .send_channel_occupied(app_config, channel_str)
                    .await
                    .ok();
            }
        }

        // Send subscription_count webhook (except for presence channels)
        if !channel_str.starts_with("presence-") {
            if let Some(webhook_integration_instance) = &self.webhook_integration {
                // Get the count *after* the subscription has been processed by channel_manager and adapter
                let current_count = self
                    .connection_manager
                    .lock()
                    .await
                    .get_channel_socket_count(&app_config.id, channel_str)
                    .await;
                info!(
                    "{}",
                    format!(
                    "Sending subscription_count webhook for channel {} (count: {}) after subscribe",
                    channel_str, current_count
                )
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
                    .await
                    .map_err(|e| {
                        error!("{}", format!("Failed to send presence message: {:?}", e));
                        e
                    })?;
            }
        } else {
            let response_msg = PusherMessage::subscription_succeeded(channel_str.to_string(), None);
            self.connection_manager
                .lock()
                .await
                .send_message(&app_config.id, socket_id, response_msg)
                .await
                .map_err(|e| {
                    error!(
                        "{}",
                        format!("Failed to send subscription response: {:?}", e)
                    );
                    e
                })?;
        }

        self.send_missed_cache_if_exists(&app_config.id, socket_id, channel_str)
            .await?;

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

        // Unsubscribe from channel manager first to update its internal state
        let _leave_response = {
            let channel_manager_locked = self.channel_manager.write().await;
            channel_manager_locked
                .unsubscribe(
                    socket_id.0.as_str(),
                    channel_name_str,
                    &app_config.id,
                    user_id_of_socket.as_deref(),
                )
                .await
                .map_err(|e| {
                    error!(
                        "{}",
                        format!("Error unsubscribing from channel manager: {:?}", e)
                    );
                    e
                })?
        };

        // Update this socket's local state in the connection manager
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

        // Get current subscription count *after* all state updates for this socket
        let current_sub_count = self
            .connection_manager
            .lock()
            .await
            .get_channel_socket_count(&app_config.id, channel_name_str)
            .await;

        if channel_name_str.starts_with("presence-") {
            if let Some(user_id_that_left) = user_id_of_socket {
                // Check if this user has any *other* connections to this presence channel AFTER this socket's state is updated
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
                            "{}",
                            format!(
                                "Sending member_removed webhook for user {} from channel {}",
                                user_id_that_left, channel_name_str
                            )
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
            info!("{}", format!("Sending subscription_count webhook for channel {} (count: {}) after unsubscribe", channel_name_str, current_sub_count));
            webhook_integration_instance
                .send_subscription_count_changed(app_config, channel_name_str, current_sub_count)
                .await
                .ok();
        }

        if current_sub_count == 0 {
            if let Some(webhook_integration_instance) = &self.webhook_integration {
                info!(
                    "{}",
                    format!(
                        "Sending channel_vacated webhook for channel {}",
                        channel_name_str
                    )
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
        if !app_config.enable_user_authentication.unwrap() {
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
                    MessageData::String(s_val) => Ok(s_val.as_str()),
                    MessageData::Json(json_val) => json_val
                        .get(field)
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| Error::AuthError(format!("Missing {} field", field))),
                    MessageData::Structured { extra, .. } => extra
                        .get(field)
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| Error::AuthError(format!("Missing {} field", field))),
                }
            };
            (extract_field("user_data")?, extract_field("auth")?)
        };

        let user_info_val: Value = serde_json::from_str(user_data_str)
            .map_err(|e| Error::AuthError(format!("Invalid user data: {}", e)))?;

        let auth_validator = AuthValidator::new(self.app_manager.clone());
        let is_valid_auth = auth_validator
            .validate_channel_auth(socket_id.clone(), &app_config.key, user_data_str, auth_str)
            .await?;

        if !is_valid_auth {
            return Err(Error::AuthError("Connection not authorized.".into()));
        }

        let mut connection_manager_locked = self.connection_manager.lock().await;

        let connection_arc = connection_manager_locked
            .get_connection(socket_id, &app_config.id)
            .await
            .ok_or_else(|| Error::ConnectionNotFound)?;

        {
            let mut conn_locked = connection_arc.lock().await;
            conn_locked.state.user = Some(user_info_val.clone());

            let socket_tx_val = conn_locked
                .socket
                .take()
                .ok_or_else(|| Error::ConnectionError("Socket not found".into()))?;

            drop(conn_locked);

            connection_manager_locked
                .add_socket(
                    socket_id.clone(),
                    socket_tx_val,
                    app_config.id.as_str(),
                    &self.app_manager,
                )
                .await
                .map_err(|e| {
                    error!("{}", format!("Failed to add socket after signin: {}", e));
                    Error::ConnectionError("Failed to add socket".into())
                })?;

            if let Err(e) = connection_manager_locked
                .add_user(connection_arc.clone())
                .await
            {
                error!("{}", format!("Failed to add user: {}", e));
            }
        }

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
        let channel_name =
            channel.ok_or_else(|| Error::ClientEventError("Channel name is required".into()))?;

        let max_event_name_len = app_config
            .max_event_name_length
            .unwrap_or(DEFAULT_EVENT_NAME_MAX_LENGTH as u32);
        if event.len() > max_event_name_len as usize {
            return Err(Error::InvalidEventName(format!(
                "Event name exceeds maximum length of {}",
                max_event_name_len
            )));
        }

        if let Some(max_payload_kb) = app_config.max_event_payload_in_kb {
            let payload_size_bytes = utils::data_to_bytes_flexible(vec![data.clone()]);
            if payload_size_bytes > (max_payload_kb as usize * 1024) {
                return Err(Error::ClientEventError(format!(
                    "Event payload size ({} bytes) exceeds limit ({}KB)",
                    payload_size_bytes, max_payload_kb
                )));
            }
        }

        if !event.starts_with(CLIENT_EVENT_PREFIX) {
            return Err(Error::InvalidEventName(
                "Client events must start with 'client-'".into(),
            ));
        }

        let max_channel_len = app_config
            .max_channel_name_length
            .unwrap_or(DEFAULT_CHANNEL_NAME_MAX_LENGTH as u32);
        if channel_name.len() > max_channel_len as usize {
            return Err(Error::InvalidChannelName(format!(
                "Channel name for client event exceeds maximum length of {}",
                max_channel_len
            )));
        }

        let channel_type = ChannelType::from_name(channel_name);
        if !matches!(channel_type, ChannelType::Private | ChannelType::Presence) {
            return Err(Error::ClientEventError(
                "Client events can only be sent to private or presence channels".into(),
            ));
        }

        let (app_key_str, subscribed_channels_set, user_id_for_webhook) = {
            let mut conn_manager_locked = self.connection_manager.lock().await;
            let connection_arc = conn_manager_locked
                .get_connection(socket_id, &app_config.id)
                .await
                .ok_or_else(|| Error::ConnectionNotFound)?;

            let conn_locked = connection_arc.lock().await;
            let key = conn_locked.state.get_app_key();
            let channels = conn_locked.state.subscribed_channels.clone();
            let user_id = conn_locked
                .state
                .presence
                .as_ref()
                .and_then(|p_map| p_map.get(channel_name))
                .map(|pi| pi.user_id.clone());
            (key, channels, user_id)
        };

        let app = self
            .app_manager
            .find_by_key(&app_key_str)
            .await?
            .ok_or_else(|| Error::InvalidAppKey)?;

        if !app.enable_client_messages {
            return Err(Error::ClientEventError(
                "Client events are not enabled for this app".into(),
            ));
        }

        info!(
            "{}",
            format!(
                "Socket {} subscribed channels (from state): {:?}",
                socket_id, subscribed_channels_set
            )
        );
        info!(
            "{}",
            format!(
                "Checking if socket {} is in channel {} (adapter check)",
                socket_id, channel_name
            )
        );

        let is_subscribed_globally;
        {
            let mut conn_manager_locked = self.connection_manager.lock().await;
            is_subscribed_globally = conn_manager_locked
                .is_in_channel(&app_config.id, channel_name, socket_id)
                .await?;
        }

        if !is_subscribed_globally {
            if !subscribed_channels_set.contains(channel_name) {
                for subscribed_channel_name in &subscribed_channels_set {
                    if subscribed_channel_name.to_lowercase() == channel_name.to_lowercase() {
                        warn!(
                            "{}",
                            format!(
                            "Case mismatch between subscribed channel {} and requested channel {}",
                            subscribed_channel_name, channel_name
                        )
                        );
                    }
                }
                warn!(
                    "{}",
                    format!(
                    "Socket {} not subscribed to {} in connection state, and adapter check failed.",
                    socket_id, channel_name
                )
                );
            }
            return Err(Error::ClientEventError(format!(
                "Client {} is not subscribed to channel {}",
                socket_id, channel_name
            )));
        }

        let message_to_send = PusherMessage {
            channel: Some(channel_name.to_string()),
            name: None,
            event: Some(event.to_string()),
            data: Some(MessageData::Json(data.clone())),
        };

        {
            let mut conn_manager_locked = self.connection_manager.lock().await;
            conn_manager_locked
                .send(
                    channel_name,
                    message_to_send.clone(),
                    Some(socket_id),
                    &app_config.id,
                )
                .await?;
        }

        if let Some(webhook_integration_val) = &self.webhook_integration {
            if let Some(uid_str_val) = user_id_for_webhook {
                webhook_integration_val
                    .send_client_event(
                        app_config,
                        channel_name,
                        event,
                        data,
                        Some(socket_id.as_ref()),
                        Some(&uid_str_val),
                    )
                    .await
                    .ok();
            } else {
                warn!(
                    "{}",
                    format!(
                        "Could not determine user_id for client event webhook on channel: {}",
                        channel_name
                    )
                );
            }
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
            .send_message(app_id, socket_id, error_message)
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
        info!(
            "{}",
            format!("Handling disconnect for socket: {}", socket_id)
        );
        if self.client_event_limiters.remove(socket_id).is_some() {
            info!(
                "{}",
                format!(
                    "Removed client event rate limiter for socket: {}",
                    socket_id
                )
            );
        }

        let app_config = match self.app_manager.find_by_id(app_id).await? {
            Some(app) => app,
            None => {
                error!("{}", format!("App not found during disconnect: {}", app_id));
                let mut conn_manager = self.connection_manager.lock().await;
                if let Some(conn_to_cleanup) = conn_manager.get_connection(socket_id, app_id).await
                {
                    conn_manager
                        .cleanup_connection(app_id, WebSocketRef(conn_to_cleanup))
                        .await;
                }
                conn_manager.remove_connection(socket_id, app_id).await.ok();
                return Err(Error::InvalidAppKey);
            }
        };

        let (subscribed_channels_set, user_id_of_disconnected_socket) = {
            let mut connection_manager_locked = self.connection_manager.lock().await;
            let connection_arc = match connection_manager_locked
                .get_connection(socket_id, app_id)
                .await
            {
                Some(conn_val) => conn_val,
                None => {
                    warn!(
                        "{}",
                        format!(
                        "No connection found for socket during disconnect: {}. Already cleaned up?",
                        socket_id
                    )
                    );
                    return Ok(());
                }
            };
            let conn_locked = connection_arc.lock().await;
            (
                conn_locked.state.subscribed_channels.clone(),
                conn_locked.state.user_id.clone(),
            )
        };

        if !subscribed_channels_set.is_empty() {
            info!(
                "{}",
                format!(
                    "Processing {} channels for disconnecting socket: {}",
                    subscribed_channels_set.len(),
                    socket_id
                )
            );

            let channel_manager_locked = self.channel_manager.write().await;

            for channel_str in &subscribed_channels_set {
                info!(
                    "{}",
                    format!(
                        "Processing channel {} for disconnect of socket {}",
                        channel_str, socket_id
                    )
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
                                        info!("{}", format!("Sending member_removed webhook for user {} from channel {}", disconnected_user_id, channel_str));
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
                            info!("{}", format!("Sending subscription_count webhook for channel {} (count: {}) after disconnect processing", channel_str, current_sub_count_after_cm_unsubscribe));
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
                                    "{}",
                                    format!(
                                        "Sending channel_vacated webhook for channel {}",
                                        channel_str
                                    )
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
                            "{}",
                            format!(
                            "Error unsubscribing socket {} from channel {} during disconnect: {}",
                            socket_id, channel_str, e
                        )
                        );
                    }
                }
            }
        }

        {
            let mut connection_manager_locked = self.connection_manager.lock().await;
            if let Some(conn_to_cleanup) = connection_manager_locked
                .get_connection(socket_id, app_id)
                .await
            {
                connection_manager_locked
                    .remove_connection(socket_id, app_id)
                    .await
                    .ok();
            }

            info!(
                "{}",
                format!(
                    "Successfully processed full disconnect for socket: {}",
                    socket_id
                )
            );
        }

        Ok(())
    }

    pub async fn channel(&self, app_id: &str, channel_name: &str) -> Value {
        let socket_count_val = self
            .connection_manager
            .lock()
            .await
            .get_channel_socket_count(app_id, channel_name)
            .await;
        let response_val = json!({
            "occupied": socket_count_val > 0,
            "subscription_count": socket_count_val,
        });

        response_val
    }

    pub async fn channels(&self, app_id: &str) -> Value {
        let mut connection_manager_locked = self.connection_manager.lock().await;
        let channels_map_result = connection_manager_locked
            .get_channels_with_socket_count(app_id)
            .await;
        let mut response_val = json!({});

        if let Ok(channels_map) = channels_map_result {
            channels_map.iter_mut().for_each(|channel_entry| {
                let channel_name_str = channel_entry.key().clone();
                let socket_count_val = channel_entry.value();
                response_val[channel_name_str] = json!({
                    "occupied": socket_count_val > &0,
                    "subscription_count": socket_count_val,
                });
            });
        } else if let Err(e) = channels_map_result {
            error!(
                "{}",
                format!(
                    "Failed to get channels with socket count for app {}: {}",
                    app_id, e
                )
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
        let members_map = channel_manager_locked
            .get_channel_members(app_id, channel_name)
            .await?;
        Ok(members_map)
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
            data: Option::from(MessageData::Json(
                serde_json::to_value(message.data).unwrap_or(Value::Null),
            )),
            channel: Some(channel.to_string()),
            name: None,
        };

        if let Some(ref metrics) = self.metrics {
            let metrics_locked = metrics.lock().await;
            let message_size_val = match serde_json::to_string(&pusher_message_val) {
                Ok(msg_str) => msg_str.len(),
                Err(_) => 0,
            };
            metrics_locked.mark_ws_message_sent(app_id, message_size_val);
        }

        match self
            .connection_manager
            .lock()
            .await
            .send(channel, pusher_message_val, socket_id, app_id)
            .await
        {
            Ok(_) => {
                info!(
                    "{}",
                    format!("Message sent to channel {} successfully", channel)
                );
            }
            Err(e) => {
                error!(
                    "{}",
                    format!("Failed to send message to channel {}: {:?}", channel, e)
                );
            }
        }
    }
}
