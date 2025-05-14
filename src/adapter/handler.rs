use crate::adapter::adapter::Adapter;
use crate::app::auth::AuthValidator;
use crate::app::config::App;
use crate::app::manager::AppManager;
use crate::cache::manager::CacheManager;
use crate::channel::{ChannelType, PresenceMemberInfo};
use crate::log::Log;
use crate::metrics::MetricsInterface;
use crate::protocol::messages::{ErrorData, MessageData, PusherApiMessage, PusherMessage};
use crate::webhook::integration::WebhookIntegration;
use crate::websocket::{SocketId, WebSocketRef};
use crate::{
    channel::ChannelManager,
    error::{Error, Result},
};
use fastwebsockets::{upgrade, FragmentCollectorRead, Frame, OpCode, WebSocketError};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use crate::rate_limiter::RateLimiter;

pub struct ConnectionHandler {
    pub(crate) app_manager: Arc<dyn AppManager + Send + Sync>,
    pub(crate) channel_manager: Arc<RwLock<ChannelManager>>,
    pub(crate) connection_manager: Arc<Mutex<Box<dyn Adapter + Send + Sync>>>,
    pub(crate) cache_manager: Arc<Mutex<dyn CacheManager + Send + Sync>>,
    pub(crate) metrics: Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>>,
    pub(crate) webhook_integration: Option<Arc<WebhookIntegration>>,
    pub(crate) rate_limiter: Option<Arc<dyn RateLimiter>>, // Added rate_limiter field
}

impl ConnectionHandler {
    pub fn new(
        app_manager: Arc<dyn AppManager + Send + Sync>,
        channel_manager: Arc<RwLock<ChannelManager>>,
        connection_manager: Arc<Mutex<Box<dyn Adapter + Send + Sync>>>,
        cache_manager: Arc<Mutex<dyn CacheManager + Send + Sync>>,
        metrics: Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>>,
        webhook_integration: Option<Arc<WebhookIntegration>>,
        rate_limiter: Option<Arc<dyn RateLimiter>>, // Added rate_limiter parameter
    ) -> Self {
        Self {
            app_manager,
            channel_manager,
            connection_manager,
            cache_manager,
            metrics,
            webhook_integration,
            rate_limiter, // Store rate_limiter
        }
    }

    // ... (other methods remain unchanged, only handle_client_event is modified below)

    #[allow(dead_code)] // Keep the function signature even if some parts are simplified for now
    async fn send_webhook<F, Fut>(&self, app: &App, webhook_fn: F) -> Result<()>
    where
        F: FnOnce(&WebhookIntegration, &App) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        if let Some(webhook) = &self.webhook_integration {
            if webhook.is_enabled() {
                match webhook_fn(webhook, app).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        // Log the error but don't fail the operation
                        Log::warning(format!("Webhook event failed: {}", e));
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
        let cache_result = cache_manager.get(key.as_str()).await; // Renamed to avoid conflict

        match cache_result {
            Ok(Some(cache_content)) => { // Matched Ok(Some(cache_content))
                let cache_message: PusherMessage = serde_json::from_str(&cache_content)?;
                // Send message with a single lock
                self.connection_manager
                    .lock()
                    .await
                    .send_message(app_id, socket_id, cache_message)
                    .await?;
            }
            Ok(None) => { // Matched Ok(None)
                let message = PusherMessage {
                    channel: Some(channel.to_string()),
                    name: None,
                    event: Some("pusher:cache_miss".to_string()), // Used .to_string()
                    data: None,
                };
                // Send message with a single lock
                self.connection_manager
                    .lock()
                    .await
                    .send_message(app_id, socket_id, message)
                    .await?;

                if let Some(app) = self.app_manager.get_app(app_id).await? {
                    if let Some(webhook_integration) = &self.webhook_integration {
                        webhook_integration.send_cache_missed(&app, channel).await?;
                    }
                }
                Log::info(format!("No missed cache for channel: {}", channel));
            }
            Err(e) => { // Matched Err(e)
                Log::error(format!("Failed to get cache for channel {}: {}", channel, e));
                // Decide if you want to return the error or just log it
                // For now, let's propagate it:
                return Err(e);
            }
        }
        Ok(())
    }


    pub async fn handle_socket(&self, fut: upgrade::UpgradeFut, app_key: String) -> Result<()> {
        // Get app by key - this needs to handle both sync and potentially async implementations
        let app = self.app_manager.get_app_by_key(&app_key).await?;
        if app.is_none() {
            return Err(Error::InvalidAppKey);
        }
        let app = app.unwrap();

        let socket = fut.await?;
        let (socket_rx, socket_tx) = socket.split(tokio::io::split);
        let socket_id = SocketId::new();
        Log::info(format!("New socket: {}", socket_id));

        // Handle adapter setup in a single lock scope
        {
            let mut connection_manager_locked = self.connection_manager.lock().await; // Renamed lock guard
            if let Some(conn) = connection_manager_locked.get_connection(&socket_id, &app.id).await {
                connection_manager_locked
                    .cleanup_connection(&app.id, WebSocketRef(conn))
                    .await;
            }
            connection_manager_locked
                .add_socket(socket_id.clone(), socket_tx, &app.id, &self.app_manager)
                .await
                .map_err(|e| {
                    Log::error(format!("Failed to add socket: {}", e));
                    WebSocketError::ConnectionClosed // Or a more appropriate Error variant
                })?;
            if let Some(ref metrics) = self.metrics {
                let metrics_locked = metrics.lock().await; // Renamed lock guard
                metrics_locked.mark_new_connection(&app.id, &socket_id)
            }
        } // Lock on connection_manager_locked released here

        if let Err(e) = self.send_connection_established(&app.id, &socket_id).await {
            self.send_error(&app.id, &socket_id, &e, None)
                .await
                .map_err(|err_send| { // Renamed error variable
                    Log::error(format!("Failed to send connection established: {}", err_send));
                    WebSocketError::ConnectionClosed // Or a more appropriate Error variant
                })?;
            return Ok(());
        }

        let mut socket_rx_collected = FragmentCollectorRead::new(socket_rx); // Renamed variable

        while let Ok(frame) = socket_rx_collected // Use renamed variable
            .read_frame(&mut move |_| async { Ok::<_, WebSocketError>(()) })
            .await
        {
            match frame.opcode {
                OpCode::Close => {
                    if let Some(ref metrics) = self.metrics {
                        let metrics_locked = metrics.lock().await; // Renamed lock guard
                        metrics_locked.mark_disconnection(&app.id, &socket_id);
                    }
                    if let Err(e) = self.handle_disconnect(&app.id, &socket_id).await {
                        Log::error(format!("Disconnect error for socket {}: {}", socket_id, e));
                    }
                    break;
                }
                OpCode::Text | OpCode::Binary => {
                    if let Err(e) = self.handle_message(frame, &socket_id, app.clone()).await {
                        Log::error(format!(
                            "Message handling error for socket {}: {}",
                            socket_id, e
                        ));
                        // Optionally, send an error to the client and/or close the connection
                        self.send_error(&app.id, &socket_id, &e, None).await.ok(); // Best effort send
                        // Consider breaking the loop or closing the connection if errors are frequent/fatal
                    }
                }
                OpCode::Ping => {
                    let mut connection_manager_locked = self.connection_manager.lock().await; // Renamed lock guard
                    if let Some(conn_arc) = connection_manager_locked.get_connection(&socket_id, &app.id).await // Renamed variable
                    {
                        let mut conn_locked = conn_arc.lock().await; // Renamed lock guard
                        conn_locked.state.update_ping();
                    }
                }
                _ => {
                    Log::warning(format!("Unsupported opcode: {:?}", frame.opcode));
                }
            }
        }

        Ok(())
    }

    pub async fn handle_message(
        &self,
        frame: Frame<'static>,
        socket_id: &SocketId,
        app: App, // Pass App by value as it's cloned anyway
    ) -> Result<()> {
        let msg_payload = String::from_utf8(frame.payload.to_vec()) // Renamed variable
            .map_err(|e| Error::InvalidMessageFormat(format!("Invalid UTF-8: {}", e)))?;

        let message: PusherMessage = serde_json::from_str(&msg_payload) // Use renamed variable
            .map_err(|e| Error::InvalidMessageFormat(format!("Invalid JSON: {}", e)))?;

        Log::info(format!("Received message: {:?}", message));

        // Extract values we need after the match before moving message
        let event_name = message // Renamed variable
            .event
            .as_deref()
            .ok_or_else(|| Error::InvalidEventName("Event name is required".into()))?;
        let channel_name = message.channel.clone(); // Clone channel before potential move, renamed variable

        let processing_result = match event_name { // Renamed variable
            "pusher:ping" => self.handle_ping(&app.id, socket_id).await,
            "pusher:subscribe" => self.handle_subscribe(socket_id, &app.id, &message).await, // Pass original message by ref
            "pusher:unsubscribe" => {
                // No need to clone message here as it's not mutated before this call
                self.handle_unsubscribe(socket_id, &message, &app.id).await
            }
            "pusher:signin" => {
                // No need to clone message here
                self.handle_signin(socket_id, message.clone(), &app).await // Clone message if handle_signin needs ownership
            }
            _ if event_name.starts_with("client-") => {
                self.handle_client_event(
                    &app.id,
                    socket_id,
                    event_name,
                    message.channel.as_deref(), // Use original message
                    message // Use original message
                        .data
                        .and_then(|d| serde_json::to_value(d).ok())
                        .unwrap_or_default(),
                )
                    .await
            }
            _ => Ok(()),
        };

        if let Err(e) = processing_result {
            self.send_error(&app.id, socket_id, &e, channel_name).await?; // Use cloned channel_name

            // Lock connection_manager only once for cleanup
            let mut connection_manager_locked = self.connection_manager.lock().await; // Renamed lock guard
            if let Some(conn_arc) = connection_manager_locked.get_connection(socket_id, &app.id).await { // Renamed variable
                connection_manager_locked
                    .cleanup_connection(&app.id, WebSocketRef(conn_arc))
                    .await;
            }

            return Err(Error::ClientEventError(format!(
                "Failed to handle event: {}, error: {}",
                event_name, e
            )));
        }

        if let Some(ref metrics) = self.metrics {
            let metrics_locked = metrics.lock().await; // Renamed lock guard
            let message_size = frame.payload.len();
            metrics_locked.mark_ws_message_received(&app.id, message_size);
        }

        Ok(())
    }


    pub async fn handle_ping(&self, app_id: &str, socket_id: &SocketId) -> Result<()> {
        // Send message with a single lock
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
            Some(MessageData::Json(data_val)) => Ok(data_val // Renamed variable
                .get("auth")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()),
            Some(MessageData::Structured { extra, .. }) => Ok(extra
                .get("auth")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()),
            None => Ok(String::new()), // Return empty string if no data or auth field
        }
    }


    pub async fn handle_subscribe(
        &self,
        socket_id: &SocketId,
        app_id: &str,
        message: &PusherMessage, // Pass message by reference
    ) -> Result<()> {
        // Extract channel without cloning
        let channel_str = match &message.data { // Renamed variable
            Some(MessageData::String(data_str)) => data_str.as_str(), // Renamed variable, use as_str()
            Some(MessageData::Structured { channel, .. }) => channel
                .as_ref()
                .map(|s| s.as_str()) // Convert Option<String> to Option<&str>
                .ok_or_else(|| Error::ChannelError("Missing channel".into()))?,
            Some(MessageData::Json(data_val)) => data_val // Renamed variable
                .get("channel")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::ChannelError("Missing channel".into()))?,
            None => return Err(Error::ChannelError("Missing channel data".into())),
        };


        let app_option = self.app_manager.get_app(app_id).await?; // Renamed variable
        let app_config = app_option.ok_or(Error::InvalidAppKey)?; // Use app_config, renamed variable

        // Validate authentication in a single read lock scope
        let is_authenticated = {
            let channel_manager_locked = self.channel_manager.read().await; // Renamed lock guard
            let signature = self.extract_signature(message)?;

            if (channel_str.starts_with("presence-") || channel_str.starts_with("private-")) // Use channel_str
                && signature.is_empty()
            {
                return Err(Error::AuthError("Authentication required".into()));
            }

            channel_manager_locked.signature_is_valid( // Use channel_manager_locked
                                                       app_config.clone(), // Clone app_config for this specific use
                                                       socket_id,
                                                       &signature,
                                                       message.clone(), // Clone message if signature_is_valid needs ownership
            )
        };

        // Subscribe to channel with write lock
        let subscription_result = {
            let channel_manager_locked = self.channel_manager.write().await; // Renamed lock guard
            channel_manager_locked // Use channel_manager_locked
                .subscribe(
                    socket_id.0.as_str(),
                    message, // Pass original message by ref
                    channel_str, // Use channel_str
                    is_authenticated,
                    app_id,
                )
                .await
                .map_err(|e| {
                    Log::error(format!("Error subscribing to channel: {:?}", e));
                    Error::ChannelError("Failed to subscribe".into()) // Or propagate original error
                })?
        };

        if !subscription_result.success {
            return self
                .send_error(
                    app_id,
                    socket_id,
                    &Error::AuthError("Invalid authentication signature".into()),
                    Some(channel_str.to_string()), // Use channel_str
                )
                .await;
        }

        if subscription_result.channel_connections == Some(1) { // Compare with Some(1)
            if let Some(webhook_integration) = &self.webhook_integration {
                webhook_integration.send_channel_occupied(&app_config, channel_str).await?; // Use app_config and channel_str
            }
        }


        let channel_type = ChannelType::from_name(channel_str); // Use channel_str
        let presence_data_tuple = if channel_type == ChannelType::Presence { // Renamed variable
            subscription_result.member.as_ref().map(|presence_member| { // Renamed variable
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

        // Update adapter state in a single lock scope
        {
            let mut connection_manager_locked = self.connection_manager.lock().await; // Renamed lock guard
            if let Some(conn_arc) = connection_manager_locked.get_connection(socket_id, app_id).await { // Renamed variable
                let mut conn_locked = conn_arc.lock().await; // Renamed lock guard
                conn_locked
                    .state
                    .subscribed_channels
                    .insert(channel_str.to_string()); // Use channel_str

                if let Some((user_id_str, presence_info_val)) = presence_data_tuple { // Renamed variables
                    conn_locked.state.user_id = Some(user_id_str.to_string());

                    if let Some(ref mut presence_map_val) = conn_locked.state.presence { // Renamed variable
                        presence_map_val.insert(channel_str.to_string(), presence_info_val); // Use channel_str
                    } else {
                        let mut new_presence_map = HashMap::new();
                        new_presence_map.insert(channel_str.to_string(), presence_info_val); // Use channel_str
                        conn_locked.state.presence = Some(new_presence_map);
                    }
                }
            }
        } // Lock on connection_manager_locked released here


        if channel_type == ChannelType::Presence {
            if let Some(presence_member) = subscription_result.member { // Renamed variable
                let user_id_str = &presence_member.user_id; // Renamed variable
                let presence_info_val = PresenceMemberInfo { // Renamed variable
                    user_id: user_id_str.clone(),
                    user_info: Some(presence_member.user_info.clone()),
                };

                // Handle presence data and sending in a single lock scope
                let members_map = { // Renamed variable
                    let mut connection_manager_locked = self.connection_manager.lock().await; // Renamed lock guard
                    let current_members = connection_manager_locked // Renamed variable
                        .get_channel_members(app_id, channel_str) // Use channel_str
                        .await?;

                    if let Some(webhook_integration) = self.webhook_integration.clone() {
                        let app_clone = app_config.clone(); // Use app_config
                        let channel_owned = channel_str.to_string(); // Create owned String
                        let user_id_owned = user_id_str.to_string(); // Create owned String

                        tokio::spawn(async move {
                            if let Err(e) = webhook_integration
                                .send_member_added(&app_clone, &channel_owned, &user_id_owned)
                                .await
                            {
                                Log::error(format!("Error sending presence webhook: {:?}", e));
                            }
                        });
                    }

                    Log::webhook_sender(format!("webhook: {:?}", current_members));
                    let member_added_msg = PusherMessage::member_added( // Renamed variable
                                                                        channel_str.to_string(), // Use channel_str
                                                                        user_id_str.clone(),
                                                                        presence_info_val.user_info.clone(),
                    );

                    connection_manager_locked // Use connection_manager_locked
                        .send(channel_str, member_added_msg, Some(socket_id), app_id) // Use channel_str
                        .await?;

                    current_members // Return current_members
                }; // Lock on connection_manager_locked released here

                let presence_message_val = json!({ // Renamed variable
                    "presence": {
                        "ids": members_map.keys().collect::<Vec<&String>>(),
                        "hash": members_map.iter()
                            .map(|(k, v)| (k.as_str(), v.user_info.clone()))
                            .collect::<HashMap<&str, Option<Value>>>(),
                        "count": members_map.len()
                    }
                });

                let subscription_succeeded_msg = PusherMessage::subscription_succeeded( // Renamed variable
                                                                                        channel_str.to_string(), // Use channel_str
                                                                                        Some(presence_message_val),
                );

                // Send message with a single lock
                self.connection_manager
                    .lock()
                    .await
                    .send_message(app_id, socket_id, subscription_succeeded_msg)
                    .await
                    .map_err(|e| {
                        Log::error(format!("Failed to send presence message: {:?}", e));
                        e
                    })?;
            }
        } else {
            // Regular channel subscription response
            let response_msg = PusherMessage::subscription_succeeded(channel_str.to_string(), None); // Renamed variable, use channel_str
            // Send message with a single lock
            self.connection_manager
                .lock()
                .await
                .send_message(app_id, socket_id, response_msg)
                .await
                .map_err(|e| {
                    Log::error(format!("Failed to send subscription response: {:?}", e));
                    e
                })?;
        }

        // If we have a missed cache for this channel, send it
        self.send_missed_cache_if_exists(app_id, socket_id, channel_str) // Use channel_str
            .await?;

        Ok(())
    }


    pub async fn handle_unsubscribe(
        &self,
        socket_id: &SocketId,
        message: &PusherMessage, // Pass message by reference
        app_id: &str,
    ) -> Result<()> {
        println!("handle_unsubscribe: {:?}", message);
        let message_data_ref = message.data.as_ref().ok_or_else(|| { // Renamed variable
            Error::InvalidMessageFormat("Missing data in unsubscribe message".into())
        })?;
        let channel_name_str = match message_data_ref { // Renamed variable
            MessageData::String(channel_str_val) => channel_str_val.as_str(), // Renamed variable, use as_str()
            MessageData::Json(data_val) => { // Renamed variable
                data_val.get("channel").and_then(Value::as_str).ok_or_else(|| {
                    Error::InvalidMessageFormat("Missing channel in unsubscribe message".into())
                })?
            }
            MessageData::Structured { channel, .. } => channel.as_ref().map(|s| s.as_str()).ok_or_else(|| { // Convert Option<String> to Option<&str>
                Error::InvalidMessageFormat("Missing channel in unsubscribe message".into())
            })?,
        };

        let channel_type_val = ChannelType::from_name(channel_name_str); // Renamed variable

        match channel_type_val {
            ChannelType::Presence => {
                // Get presence member first to minimize lock time
                let presence_member_option = { // Renamed variable
                    let mut conn_manager_locked = self.connection_manager.lock().await; // Renamed lock guard
                    conn_manager_locked // Use conn_manager_locked
                        .get_presence_member(app_id, channel_name_str, socket_id)
                        .await
                }; // Lock on conn_manager_locked released here

                if let Some(presence_member_val) = presence_member_option { // Renamed variable
                    // Handle unsubscribe
                    {
                        let channel_manager_locked = self.channel_manager.write().await; // Renamed lock guard
                        channel_manager_locked // Use channel_manager_locked
                            .unsubscribe(
                                socket_id.0.as_str(),
                                channel_name_str,
                                app_id,
                                Some(&presence_member_val.user_id),
                            )
                            .await
                            .map_err(|e| {
                                Log::error(format!("Error unsubscribing: {:?}", e));
                                e
                            })?;
                    } // Lock on channel_manager_locked released here

                    // Update adapter state
                    {
                        let mut conn_manager_locked = self.connection_manager.lock().await; // Renamed lock guard
                        if let Some(conn_arc) = conn_manager_locked.get_connection(socket_id, app_id).await { // Renamed variable
                            let mut conn_locked = conn_arc.lock().await; // Renamed lock guard
                            if let Some(presence_map) = conn_locked.state.presence.as_mut() { // Renamed variable
                                presence_map.remove(channel_name_str);
                            }
                            conn_locked.state.subscribed_channels.remove(channel_name_str);
                        }

                        // send member removal within the same lock
                        let member_removed_msg = PusherMessage::member_removed( // Renamed variable
                                                                                channel_name_str.to_string(),
                                                                                presence_member_val.user_id.clone(),
                        );

                        if let Some(app_config) = self.app_manager.get_app(app_id).await? { // Renamed variable
                            if let Some(webhook_integration) = &self.webhook_integration {
                                webhook_integration.send_member_removed(
                                    &app_config,
                                    channel_name_str,
                                    presence_member_val.user_id.as_str(),
                                ).await?;
                            }
                        }


                        conn_manager_locked // Use conn_manager_locked
                            .send(channel_name_str, member_removed_msg, Some(socket_id), app_id)
                            .await
                            .map_err(|e| {
                                Log::error(format!("Error sending member_removed: {:?}", e));
                                e
                            })?;
                    } // Lock on conn_manager_locked released here
                }
            }
            _ => {
                // Simple unsubscribe for non-presence channels
                let response_val = { // Renamed variable
                    let channel_manager_locked = self.channel_manager.write().await; // Renamed lock guard
                    channel_manager_locked // Use channel_manager_locked
                        .unsubscribe(socket_id.0.as_str(), channel_name_str, app_id, None)
                        .await
                        .map_err(|e| {
                            Log::error(format!("Error unsubscribing: {:?}", e));
                            e
                        })?
                }; // Lock on channel_manager_locked released here

                if response_val.remaining_connections == Some(0) {
                    if let Some(app_config) = self.app_manager.get_app(app_id).await? { // Renamed variable
                        if let Some(webhook_integration) = &self.webhook_integration {
                            webhook_integration.send_channel_vacated(&app_config, channel_name_str).await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }


    pub async fn handle_signin(
        &self,
        socket_id: &SocketId,
        data: PusherMessage, // Pass data by value
        app: &App, // Pass app by reference
    ) -> Result<()> {
        // Extract and validate message data
        let message_data_val = data // Renamed variable
            .data
            .ok_or_else(|| Error::AuthError("Missing data in signin message".into()))?;

        // Extract fields efficiently
        let (user_data_str, auth_str) = { // Renamed variables
            let extract_field = |field: &str| -> Result<&str> {
                match &message_data_val {
                    MessageData::String(s_val) => Ok(s_val.as_str()), // Renamed variable
                    MessageData::Json(json_val) => json_val // Renamed variable
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

        // Parse user data
        let user_info_val: Value = serde_json::from_str(user_data_str) // Renamed variable
            .map_err(|e| Error::AuthError(format!("Invalid user data: {}", e)))?;

        // Validate auth
        let auth_validator = AuthValidator::new(self.app_manager.clone());
        let is_valid_auth = auth_validator // Renamed variable
            .validate_channel_auth(socket_id.clone(), &app.key, user_data_str, auth_str)
            .await?;

        if !is_valid_auth {
            return Err(Error::AuthError("Connection not authorized.".into()));
        }

        // Lock connection manager once for all operations
        let mut connection_manager_locked = self.connection_manager.lock().await; // Renamed lock guard

        // Get existing connection
        let connection_arc = connection_manager_locked // Renamed variable
            .get_connection(socket_id, &app.id)
            .await
            .ok_or_else(|| Error::ConnectionNotFound)?;

        {
            // Update user info in connection state and get socket
            let mut conn_locked = connection_arc.lock().await; // Renamed lock guard
            conn_locked.state.user = Some(user_info_val.clone());

            // Take the socket safely using Option::take
            let socket_tx_val = conn_locked // Renamed variable
                .socket
                .take()
                .ok_or_else(|| Error::ConnectionError("Socket not found".into()))?;

            drop(conn_locked); // Release lock on conn_locked

            // Add socket
            connection_manager_locked // Use connection_manager_locked
                .add_socket(
                    socket_id.clone(),
                    socket_tx_val,
                    app.id.as_str(),
                    &self.app_manager,
                )
                .await
                .map_err(|e| {
                    Log::error(format!("Failed to add socket after signin: {}", e));
                    Error::ConnectionError("Failed to add socket".into())
                })?;

            // Add user
            if let Err(e) = connection_manager_locked.add_user(connection_arc.clone()).await { // Use connection_manager_locked
                Log::error(format!("Failed to add user: {}", e));
            }
        } // Lock on connection_arc (implicitly through conn_locked) released here

        // Send success message
        let success_message_val = PusherMessage { // Renamed variable
            channel: None,
            name: None,
            event: Some("pusher:signin_success".into()),
            data: Some(MessageData::Json(user_info_val)),
        };

        connection_manager_locked // Use connection_manager_locked
            .send_message(&app.id, socket_id, success_message_val)
            .await?;

        Ok(())
    } // Lock on connection_manager_locked released here


    async fn handle_client_event(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        event: &str,
        channel: Option<&str>,
        data: Value,
    ) -> Result<()> {
        let channel_name =
            channel.ok_or_else(|| Error::ClientEventError("Channel name is required".into()))?;

        if !event.starts_with("client-") {
            return Err(Error::InvalidEventName(
                "Client events must start with 'client-'".into(),
            ));
        }

        let channel_type = ChannelType::from_name(channel_name);
        if !matches!(channel_type, ChannelType::Private | ChannelType::Presence) {
            return Err(Error::ClientEventError(
                "Client events can only be sent to private or presence channels".into(),
            ));
        }

        // --- Optimized Lock Handling ---
        // 1. Get app_key and subscribed_channels with one lock acquisition if possible,
        //    or ensure minimal lock duration for each.
        let (app_key_str, subscribed_channels_set, user_id_for_webhook) = {
            let mut conn_manager_locked = self.connection_manager.lock().await;
            let connection_arc = conn_manager_locked
                .get_connection(socket_id, app_id)
                .await
                .ok_or_else(|| Error::ConnectionNotFound)?; // Return early if connection not found

            let conn_locked = connection_arc.lock().await;
            let key = conn_locked.state.get_app_key();
            let channels = conn_locked.state.subscribed_channels.clone();
            let user_id = conn_locked.state.presence.as_ref()
                .and_then(|p_map| p_map.get(channel_name))
                .map(|pi| pi.user_id.clone());
            (key, channels, user_id)
        }; // All locks on connection_manager, connection_arc, conn_locked released

        // 2. Verify client events are enabled (no lock needed here for app_manager)
        if !self.app_manager.can_handle_client_events(&app_key_str).await? {
            return Err(Error::ClientEventError(
                "Client events are not enabled for this app".into(),
            ));
        }

        // Log state for debugging
        Log::info(format!(
            "Socket {} subscribed channels (from state): {:?}",
            socket_id, subscribed_channels_set
        ));
        Log::info(format!(
            "Checking if socket {} is in channel {} (adapter check)",
            socket_id, channel_name
        ));


        // 3. Verify channel subscription (acquires lock on connection_manager again)
        let is_subscribed_globally;
        {
            let mut conn_manager_locked = self.connection_manager.lock().await;
            is_subscribed_globally = conn_manager_locked
                .is_in_channel(app_id, channel_name, socket_id)
                .await?;
        } // Lock on conn_manager_locked released


        if !is_subscribed_globally {
            // Check for case mismatch if not found by adapter, using local state
            if !subscribed_channels_set.contains(channel_name) {
                for subscribed_channel_name in &subscribed_channels_set {
                    if subscribed_channel_name.to_lowercase() == channel_name.to_lowercase() {
                        Log::warning(format!(
                            "Case mismatch between subscribed channel {} and requested channel {}",
                            subscribed_channel_name, channel_name
                        ));
                        // Potentially allow if case-insensitive matching is desired,
                        // but current logic requires exact match.
                    }
                }
                Log::warning(format!(
                    "Socket {} not subscribed to {} in connection state, and adapter check failed.",
                    socket_id, channel_name
                ));
            }
            return Err(Error::ClientEventError(format!(
                "Client {} is not subscribed to channel {}",
                socket_id, channel_name
            )));
        }


        // 4. Prepare and send message (acquires lock on connection_manager again)
        let message_to_send = PusherMessage {
            channel: Some(channel_name.to_string()),
            name: None, // Name is not typically part of client events in this way
            event: Some(event.to_string()),
            data: Some(MessageData::Json(data.clone())), // Clone data for the message
        };

        {
            let mut conn_manager_locked = self.connection_manager.lock().await;
            conn_manager_locked
                .send(channel_name, message_to_send.clone(), Some(socket_id), app_id)
                .await?;
        } // Lock on conn_manager_locked released


        // 5. Send webhook (no lock on connection_manager needed here)
        if let Some(app_config) = self.app_manager.get_app(app_id).await? {
            if let Some(webhook_integration) = &self.webhook_integration {
                if let Some(uid_str) = user_id_for_webhook { // Use the user_id captured earlier
                    webhook_integration.send_client_event(
                        &app_config,
                        channel_name,
                        event, // event is &str, already available
                        data, // data is Value, already available
                        Some(socket_id.as_ref()),
                        Some(&uid_str),
                    ).await?;
                } else {
                    Log::warning(format!("Could not determine user_id for client event webhook on channel: {}", channel_name));
                }
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
        let error_data = ErrorData { // Renamed variable
            message: error.to_string(),
            code: Some(error.close_code()),
        };
        let error_message = PusherMessage::error(error_data.code.unwrap_or(4000), error_data.message, channel); // Renamed variable
        // Send message with a single lock
        self.connection_manager
            .lock()
            .await
            .send_message(app_id, socket_id, error_message)
            .await
    }

    pub async fn send_connection_established(&self, app_id: &str, socket_id: &SocketId) -> Result<()> {
        let connection_message = PusherMessage::connection_established(socket_id.0.clone()); // Renamed variable
        // Send message with a single lock
        self.connection_manager
            .lock()
            .await
            .send_message(app_id, socket_id, connection_message)
            .await
    }


    pub async fn handle_disconnect(&self, app_id: &str, socket_id: &SocketId) -> Result<()> {
        // First, get all the data we need with minimal locking
        let (subscribed_channels_set, user_id_option) = { // Renamed variables
            let mut connection_manager_locked = self.connection_manager.lock().await; // Renamed lock guard
            let connection_arc = match connection_manager_locked.get_connection(socket_id, app_id).await { // Renamed variable
                Some(conn_val) => conn_val, // Renamed variable
                None => {
                    Log::warning(format!("No connection found for socket: {}", socket_id));
                    return Ok(());
                }
            };

            let conn_locked = connection_arc.lock().await; // Renamed lock guard
            (
                conn_locked.state.subscribed_channels.clone(),
                conn_locked.state.user_id.clone(),
            )
        }; // Locks on connection_manager_locked and conn_locked released here

        // Process channel unsubscriptions first
        if !subscribed_channels_set.is_empty() {
            Log::info(format!(
                "Processing {} channels for disconnecting socket: {}",
                subscribed_channels_set.len(),
                socket_id
            ));

            let channel_manager_locked = self.channel_manager.write().await; // Renamed lock guard

            for channel_str in subscribed_channels_set { // Renamed variable
                Log::info(format!("Unsubscribing from channel: {}", channel_str));

                if let Err(e) = channel_manager_locked // Use channel_manager_locked
                    .unsubscribe(socket_id.0.as_str(), &channel_str, app_id, user_id_option.as_deref())
                    .await
                {
                    Log::error(format!(
                        "Error unsubscribing from channel {}: {}",
                        channel_str, e
                    ));
                    continue; // Continue with other channels
                }

                // Handle presence channel logic
                if channel_str.starts_with("presence-") && user_id_option.is_some() {
                    let should_broadcast_removal; // Renamed variable
                    {
                        let mut connection_manager_locked_inner = self.connection_manager.lock().await; // Renamed lock guard
                        let members_map = connection_manager_locked_inner // Renamed variable
                            .get_channel_members(app_id, &channel_str)
                            .await?;
                        should_broadcast_removal = !members_map.contains_key(user_id_option.as_ref().unwrap());
                    } // Lock on connection_manager_locked_inner released here

                    if should_broadcast_removal {
                        let removal_message = PusherMessage::member_removed( // Renamed variable
                                                                             channel_str.clone(),
                                                                             user_id_option.clone().unwrap(),
                        );

                        // Send message with a single lock
                        self.connection_manager
                            .lock()
                            .await
                            .send(&channel_str, removal_message, Some(socket_id), app_id)
                            .await?;
                    }
                }
            }
        } // Lock on channel_manager_locked released here

        // Only remove the connection after all channel processing is complete
        {
            let mut connection_manager_locked = self.connection_manager.lock().await; // Renamed lock guard
            let _ = connection_manager_locked // Use connection_manager_locked
                .remove_connection(socket_id, app_id)
                .await;
            Log::info(format!(
                "Successfully removed connection for socket: {}",
                socket_id
            ));
        } // Lock on connection_manager_locked released here

        Ok(())
    }


    pub async fn channel(&self, app_id: &str, channel_name: &str) -> Value {
        let socket_count_val = self // Renamed variable
            .connection_manager
            .lock()
            .await
            .get_channel_socket_count(app_id, channel_name)
            .await;
        let response_val = json!({ // Renamed variable
            "occupied": socket_count_val > 0,
            "subscription_count": socket_count_val,
        });

        response_val
    }

    pub async fn channels(&self, app_id: &str) -> Value {
        let mut connection_manager_locked = self.connection_manager.lock().await; // Renamed lock guard
        let channels_map_result = connection_manager_locked // Renamed variable
            .get_channels_with_socket_count(app_id)
            .await;
        let mut response_val = json!({}); // Renamed variable

        if let Ok(mut channels_map) = channels_map_result { // Check Ok result
            channels_map.iter_mut().for_each(|channel_entry| { // Renamed variable
                let channel_name_str = channel_entry.key().clone(); // Renamed variable
                let socket_count_val = channel_entry.value(); // Renamed variable
                response_val[channel_name_str] = json!({
                    "occupied": socket_count_val > &0,
                    "subscription_count": socket_count_val,
                });
            });
        } else if let Err(e) = channels_map_result {
            Log::error(format!("Failed to get channels with socket count for app {}: {}", app_id, e));
            // Return empty JSON or an error indicator
        }
        response_val
    }


    pub async fn channel_users(
        &self,
        app_id: &str,
        channel_name: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        // see if current channel is presence
        let channel_type_val = ChannelType::from_name(channel_name); // Renamed variable
        if channel_type_val != ChannelType::Presence {
            return Err(Error::ChannelError(
                "Channel is not a presence channel".into(),
            ));
        }
        let channel_manager_locked = self.channel_manager.read().await; // Renamed lock guard
        let members_map = channel_manager_locked // Renamed variable
            .get_channel_members(app_id, channel_name)
            .await?;
        Ok(members_map)
    }


    pub async fn send_message(
        &self,
        app_id: &str,
        socket_id: Option<&SocketId>,
        message: PusherApiMessage, // Pass message by value
        channel: &str,
    ) {
        // Create PusherMessage
        let pusher_message_val = PusherMessage { // Renamed variable
            event: message.name,
            data: Option::from(MessageData::Json(
                serde_json::to_value(message.data).unwrap_or(Value::Null),
            )),
            channel: Some(channel.to_string()),
            name: None, // Name is usually for API messages, not client-facing Pusher messages
        };

        // Track message metrics before sending
        if let Some(ref metrics) = self.metrics {
            let mut metrics_locked = metrics.lock().await; // Renamed lock guard
            let message_size_val = match serde_json::to_string(&pusher_message_val) { // Renamed variable
                Ok(msg_str) => msg_str.len(),
                Err(_) => 0,
            };
            metrics_locked.mark_ws_message_sent(app_id, message_size_val);
        }

        // Send the message
        match self
            .connection_manager
            .lock()
            .await
            .send(channel, pusher_message_val, socket_id, app_id) // Use pusher_message_val
            .await
        {
            Ok(_) => {
                Log::info(format!("Message sent to channel {} successfully", channel));
            }
            Err(e) => {
                Log::error(format!(
                    "Failed to send message to channel {}: {:?}",
                    channel, e
                ));
            }
        }
    }

}
