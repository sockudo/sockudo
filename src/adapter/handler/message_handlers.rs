// src/adapter/handler/message_handlers.rs
use super::ConnectionHandler;
use super::types::*;
use crate::app::config::App;
use crate::error::{Error, Result};
use crate::protocol::messages::PusherMessage;
use crate::websocket::SocketId;
use sonic_rs::prelude::*;

impl ConnectionHandler {
    pub async fn handle_ping(&self, app_id: &str, socket_id: &SocketId) -> Result<()> {
        // Reset connection status to Active when we receive a ping from client
        {
            let connection_manager = &self.connection_manager;
            if let Some(connection) = connection_manager.get_connection(socket_id, app_id).await {
                let mut conn_locked = connection.inner.lock().await;
                conn_locked.state.status = crate::websocket::ConnectionStatus::Active;
            } else {
                tracing::warn!("Ping received for unknown socket: {}", socket_id);
            }
        }

        let pong_message = PusherMessage::pong();
        self.send_message_to_socket(app_id, socket_id, pong_message)
            .await
    }

    pub async fn handle_pong(&self, app_id: &str, socket_id: &SocketId) -> Result<()> {
        tracing::debug!("Received pong from socket: {}", socket_id);
        let connection_manager = &self.connection_manager;
        if let Some(connection) = connection_manager.get_connection(socket_id, app_id).await {
            let mut conn_locked = connection.inner.lock().await;
            // Note: activity timestamp is already updated by handle_message() for ALL messages
            // We just need to reset connection status to Active when we receive a pong
            conn_locked.state.status = crate::websocket::ConnectionStatus::Active;
        } else {
            tracing::warn!("Pong received for unknown socket: {}", socket_id);
        }
        Ok(())
    }

    pub async fn handle_enable_delta_compression(&self, socket_id: &SocketId) -> Result<()> {
        tracing::info!("Enabling delta compression for socket: {}", socket_id);

        // Enable delta compression in the manager
        self.delta_compression.enable_for_socket(socket_id);

        // Update connection state to track that delta compression is enabled
        let connection_manager = &self.connection_manager;

        // Find the app_id for this socket by iterating through all apps
        // This is a bit inefficient but handle_enable_delta_compression is called rarely
        let apps = self.app_manager.get_apps().await?;

        for app in apps {
            if let Some(connection) = connection_manager.get_connection(socket_id, &app.id).await {
                let mut conn_locked = connection.inner.lock().await;
                conn_locked.state.delta_compression_enabled = true;

                let algorithm = self.delta_compression.get_algorithm();
                let algorithm_str = match algorithm {
                    crate::delta_compression::DeltaAlgorithm::Fossil => "fossil",
                    crate::delta_compression::DeltaAlgorithm::Xdelta3 => "xdelta3",
                };

                // Send confirmation message back to client
                let confirmation = PusherMessage {
                    event: Some("pusher:delta_compression_enabled".to_string()),
                    data: Some(crate::protocol::messages::MessageData::Json(
                        sonic_rs::json!({
                            "enabled": true,
                            "algorithm": algorithm_str
                        }),
                    )),
                    channel: None,
                    name: None,
                    user_id: None,
                    tags: None,
                    sequence: None,
                    conflation_key: None,
                };

                drop(conn_locked);

                self.send_message_to_socket(&app.id, socket_id, confirmation)
                    .await?;

                tracing::info!(
                    "Delta compression enabled successfully for socket: {}",
                    socket_id
                );
                return Ok(());
            }
        }

        tracing::warn!(
            "Could not find connection for socket {} to enable delta compression",
            socket_id
        );
        Ok(())
    }

    /// Handle delta sync error from client - reset delta compression state for the channel
    pub async fn handle_delta_sync_error(
        &self,
        socket_id: &SocketId,
        message: &PusherMessage,
    ) -> Result<()> {
        // Extract channel from the message data
        let channel: Option<String> = match &message.data {
            Some(crate::protocol::messages::MessageData::Structured { channel, .. }) => {
                channel.clone()
            }
            Some(crate::protocol::messages::MessageData::Json(json)) => json
                .get("channel")
                .and_then(|v| v.as_str())
                .map(String::from),
            Some(crate::protocol::messages::MessageData::String(s)) => {
                sonic_rs::from_str::<sonic_rs::Value>(s)
                    .ok()
                    .and_then(|v| v.get("channel").and_then(|c| c.as_str()).map(String::from))
            }
            None => None,
        };

        if let Some(channel_name) = channel {
            tracing::info!(
                "Delta sync error from socket {}, resetting state for channel {}",
                socket_id,
                channel_name
            );

            // Clear all delta compression state for this socket/channel combination
            self.delta_compression
                .clear_channel_state(socket_id, &channel_name);

            tracing::debug!(
                "Delta compression state cleared for socket {} channel {}",
                socket_id,
                channel_name
            );
        } else {
            tracing::warn!(
                "Delta sync error from socket {} but no channel specified in message: {:?}",
                socket_id,
                message
            );
        }

        Ok(())
    }

    pub async fn handle_subscribe_request(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        request: SubscriptionRequest,
    ) -> Result<()> {
        let t_start = std::time::Instant::now();

        // Validate the request
        let t_before_validate = t_start.elapsed().as_micros();
        self.validate_subscription_request(app_config, &request)
            .await?;
        let t_after_validate = t_start.elapsed().as_micros();

        // Check authentication if required
        let t_before_auth = t_start.elapsed().as_micros();
        let is_authenticated = match self
            .verify_channel_authentication(app_config, socket_id, &request)
            .await
        {
            Ok(authenticated) => authenticated,
            Err(e) => {
                // Track authentication failures
                if let Some(ref metrics) = self.metrics {
                    let metrics_locked = metrics.lock().await;
                    let error_type = match &e {
                        Error::Auth(_) => "authentication_failed",
                        _ => "authentication_error",
                    };
                    metrics_locked.mark_connection_error(&app_config.id, error_type);
                }
                return Err(e);
            }
        };
        let t_after_auth = t_start.elapsed().as_micros();

        // Validate presence channel specifics
        let t_before_presence_validate = t_start.elapsed().as_micros();
        if request.channel.starts_with("presence-") {
            self.validate_presence_subscription(app_config, &request)
                .await?;
        }
        let t_after_presence_validate = t_start.elapsed().as_micros();

        // Perform the subscription
        let t_before_execute = t_start.elapsed().as_micros();
        let subscription_result = self
            .execute_subscription(socket_id, app_config, &request, is_authenticated)
            .await?;
        let t_after_execute = t_start.elapsed().as_micros();

        // Handle post-subscription (sends client response first, then does background work)
        let t_before_post = t_start.elapsed().as_micros();
        self.handle_post_subscription(socket_id, app_config, &request, &subscription_result)
            .await?;
        let t_after_post = t_start.elapsed().as_micros();

        let total = t_start.elapsed().as_micros();
        tracing::debug!(
            "PERF[SUB_TOTAL] socket_id={} channel={} total={}μs validate={}μs auth={}μs presence_validate={}μs execute={}μs post={}μs",
            socket_id,
            request.channel,
            total,
            t_after_validate - t_before_validate,
            t_after_auth - t_before_auth,
            t_after_presence_validate - t_before_presence_validate,
            t_after_execute - t_before_execute,
            t_after_post - t_before_post
        );

        Ok(())
    }

    pub async fn handle_signin_request(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        request: SignInRequest,
    ) -> Result<()> {
        // Validate signin is enabled
        if !app_config.enable_user_authentication.unwrap_or(false) {
            return Err(Error::Auth(
                "User authentication is disabled for this app".into(),
            ));
        }

        // Parse and validate user data
        let user_info = self.parse_and_validate_user_data(&request.user_data)?;

        // Verify authentication
        self.verify_signin_authentication(socket_id, app_config, &request)
            .await?;

        // Update connection state
        self.update_connection_with_user_info(socket_id, app_config, &user_info)
            .await?;

        // Handle watchlist functionality
        self.handle_signin_watchlist(socket_id, app_config, &user_info)
            .await?;

        // Send success response
        self.send_signin_success(socket_id, app_config, &request)
            .await?;

        Ok(())
    }

    pub async fn handle_client_event_request(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        request: ClientEventRequest,
    ) -> Result<()> {
        // Validate the client event
        self.validate_client_event(app_config, &request).await?;

        // Check if socket is subscribed to the channel
        self.verify_channel_subscription(socket_id, app_config, &request.channel)
            .await?;

        // Rate limit check is handled in the main message handler

        // Send the event
        let message = PusherMessage {
            channel: Some(request.channel.clone()),
            event: Some(request.event.clone()),
            data: Some(crate::protocol::messages::MessageData::Json(
                request.data.clone(),
            )),
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
        };

        self.broadcast_to_channel(app_config, &request.channel, message, Some(socket_id))
            .await?;

        // Send webhook asynchronously (non-blocking for client)
        if let Some(webhook_integration) = self.webhook_integration.clone() {
            let socket_id = *socket_id;
            let app_config = app_config.clone();
            let request = request.clone();
            tokio::spawn(async move {
                if let Err(e) = webhook_integration
                    .send_client_event(
                        &app_config,
                        &request.channel,
                        &request.event,
                        request.data.clone(),
                        Some(&socket_id.to_string()),
                        None, // Skip user_id for async webhooks to avoid lock dependencies
                    )
                    .await
                {
                    tracing::error!("Failed to send client event webhook: {}", e);
                }
            });
        }

        Ok(())
    }
}
