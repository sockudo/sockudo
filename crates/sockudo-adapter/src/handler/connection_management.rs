#![allow(dead_code)]

// src/adapter/handler/connection_management.rs
use super::ConnectionHandler;
use sockudo_core::app::App;
use sockudo_core::error::{Error, Result};
#[cfg(feature = "recovery")]
use sockudo_core::options::ConnectionRecoveryConfig;
use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::PusherMessage;
#[cfg(feature = "recovery")]
use sockudo_protocol::messages::generate_message_id;
use sockudo_ws::Message;
use sockudo_ws::axum_integration::WebSocketWriter;
#[cfg(feature = "delta")]
use std::sync::Arc;
use tracing::warn;

/// Merge per-app connection recovery overrides with the global config.
/// Only fields explicitly set at the app level take precedence.
#[cfg(feature = "recovery")]
fn resolve_app_connection_recovery(
    global: &ConnectionRecoveryConfig,
    app: &App,
) -> ConnectionRecoveryConfig {
    match &app.connection_recovery {
        Some(app_config) => ConnectionRecoveryConfig {
            enabled: app_config.enabled.unwrap_or(global.enabled),
            buffer_ttl_seconds: app_config
                .buffer_ttl_seconds
                .unwrap_or(global.buffer_ttl_seconds),
            max_buffer_size: app_config
                .max_buffer_size
                .unwrap_or(global.max_buffer_size),
        },
        None => global.clone(),
    }
}

impl ConnectionHandler {
    pub async fn send_message_to_socket(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        message: PusherMessage,
    ) -> Result<()> {
        // Calculate message size for metrics
        let message_size = sonic_rs::to_string(&message).unwrap_or_default().len();

        // Send the message (lock-free - all ConnectionManager methods are &self)
        let result = self
            .connection_manager
            .send_message(app_id, socket_id, message)
            .await;

        // Track metrics if message was sent successfully
        if result.is_ok()
            && let Some(ref metrics) = self.metrics
        {
            metrics.mark_ws_message_sent(app_id, message_size);
        }

        result
    }

    /// Broadcast to channel (backward compatible version)
    pub async fn broadcast_to_channel(
        &self,
        app_config: &App,
        channel: &str,
        message: PusherMessage,
        exclude_socket: Option<&SocketId>,
    ) -> Result<()> {
        self.broadcast_to_channel_with_timing(app_config, channel, message, exclude_socket, None)
            .await
    }

    /// Broadcast to channel with optional timing for latency tracking
    pub async fn broadcast_to_channel_with_timing(
        &self,
        app_config: &App,
        channel: &str,
        message: PusherMessage,
        exclude_socket: Option<&SocketId>,
        start_time_ms: Option<f64>,
    ) -> Result<()> {
        self.broadcast_to_channel_internal(
            app_config,
            channel,
            message,
            exclude_socket,
            start_time_ms,
            false, // allow delta compression
        )
        .await
    }

    /// Broadcast to channel forcing full messages (skip delta compression)
    ///
    /// This is used when the publisher explicitly requests `delta: false` in the
    /// publish API. All recipients will receive the full message regardless of
    /// their delta compression subscription settings.
    pub async fn broadcast_to_channel_force_full(
        &self,
        app_config: &App,
        channel: &str,
        message: PusherMessage,
        exclude_socket: Option<&SocketId>,
        start_time_ms: Option<f64>,
    ) -> Result<()> {
        self.broadcast_to_channel_internal(
            app_config,
            channel,
            message,
            exclude_socket,
            start_time_ms,
            true, // force full messages, skip delta compression
        )
        .await
    }

    /// Internal broadcast implementation with delta compression control
    #[allow(unused_variables, unused_mut)]
    async fn broadcast_to_channel_internal(
        &self,
        app_config: &App,
        channel: &str,
        mut message: PusherMessage,
        exclude_socket: Option<&SocketId>,
        start_time_ms: Option<f64>,
        force_full_message: bool,
    ) -> Result<()> {
        // Connection recovery bundles serial + message_id + replay buffer (V2 feature, gated by config)
        #[cfg(feature = "recovery")]
        {
            let options = self.server_options();
            let recovery_config =
                resolve_app_connection_recovery(&options.connection_recovery, app_config);
            if recovery_config.enabled {
                if message.message_id.is_none() {
                    message.message_id = Some(generate_message_id());
                }
                if let Some(ref replay_buffer) = self.replay_buffer {
                    let serial = replay_buffer.next_serial(&app_config.id, channel);
                    message.serial = Some(serial);
                }

                if let Some(ref replay_buffer) = self.replay_buffer
                    && let Ok(bytes) = sonic_rs::to_vec(&message) {
                        replay_buffer.store(&app_config.id, channel, message.serial.unwrap_or(0), bytes);
                    }
            }
        }

        // Calculate message size for metrics
        let message_size = sonic_rs::to_string(&message).unwrap_or_default().len();

        // Get the number of sockets in the channel before sending and send the message
        let (result, target_socket_count) = {
            let socket_count = self
                .connection_manager
                .get_channel_socket_count(&app_config.id, channel)
                .await;

            // Adjust for excluded socket
            let target_socket_count = if exclude_socket.is_some() && socket_count > 0 {
                socket_count - 1
            } else {
                socket_count
            };

            #[cfg(feature = "delta")]
            let result = {
                // Extract channel-specific delta compression settings
                // If force_full_message is true, we pass None to disable delta compression
                let channel_settings = if force_full_message {
                    None
                } else {
                    Self::get_channel_delta_settings(app_config, channel)
                };

                if force_full_message {
                    // Send without compression - bypass delta compression entirely
                    self.connection_manager
                        .send(
                            channel,
                            message,
                            exclude_socket,
                            &app_config.id,
                            start_time_ms,
                        )
                        .await
                } else {
                    // Normal path with delta compression
                    self.connection_manager
                        .send_with_compression(
                            channel,
                            message,
                            exclude_socket,
                            &app_config.id,
                            start_time_ms,
                            crate::connection_manager::CompressionParams {
                                delta_compression: Arc::clone(&self.delta_compression),
                                channel_settings: channel_settings.as_ref(),
                            },
                        )
                        .await
                }
            };

            #[cfg(not(feature = "delta"))]
            let result = self
                .connection_manager
                .send(
                    channel,
                    message,
                    exclude_socket,
                    &app_config.id,
                    start_time_ms,
                )
                .await;

            (result, target_socket_count)
        };

        // Track metrics if message was sent successfully
        if result.is_ok()
            && target_socket_count > 0
            && let Some(ref metrics) = self.metrics
        {
            // Batch metrics update instead of loop for performance
            metrics.mark_ws_messages_sent_batch(&app_config.id, message_size, target_socket_count);

            // Track broadcast latency if we have a start time
            if let Some(start_ms) = start_time_ms {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as f64
                    / 1_000_000.0; // Convert to milliseconds
                let latency_ms = (now_ms - start_ms).max(0.0); // Already in milliseconds with microsecond precision

                metrics.track_broadcast_latency(
                    &app_config.id,
                    channel,
                    target_socket_count,
                    latency_ms,
                );
            }
        }

        result
    }

    pub async fn close_connection(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        code: u16,
        reason: &str,
    ) -> Result<()> {
        if let Some(conn) = self
            .connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
        {
            let mut conn_locked = conn.inner.lock().await;
            conn_locked
                .close(code, reason.to_string())
                .await
                .map_err(|e| Error::Internal(format!("Failed to close connection: {e}")))
        } else {
            warn!("Connection not found for close: {}", socket_id);
            Ok(())
        }
    }

    pub async fn get_channel_member_count(&self, app_config: &App, channel: &str) -> Result<usize> {
        self.connection_manager
            .get_channel_members(&app_config.id, channel)
            .await
            .map(|members| members.len())
    }

    pub async fn verify_channel_subscription(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        channel: &str,
    ) -> Result<()> {
        let is_subscribed = self
            .connection_manager
            .is_in_channel(&app_config.id, channel, socket_id)
            .await?;

        if !is_subscribed {
            return Err(Error::ClientEvent(format!(
                "Socket {socket_id} is not subscribed to channel {channel}"
            )));
        }

        Ok(())
    }

    async fn send_error_frame(ws_tx: &mut WebSocketWriter, error: &Error) {
        let error_message = PusherMessage::error(error.close_code(), error.to_string(), None);

        if let Ok(payload) = sonic_rs::to_string(&error_message)
            && let Err(e) = ws_tx.send(Message::text(payload)).await
        {
            warn!("Failed to send error frame: {e}");
        }

        if let Err(e) = ws_tx.close(error.close_code(), &error.to_string()).await {
            warn!("Failed to send close frame: {}", e);
        }
    }

    #[cfg(feature = "delta")]
    /// Get channel-specific delta compression settings with pattern matching support
    ///
    /// Supports:
    /// - Exact channel name match (e.g., "market-data")
    /// - Wildcard patterns (e.g., "market-*" matches "market-btc", "market-eth")
    /// - Prefix patterns (e.g., "private-*")
    fn get_channel_delta_settings(
        app_config: &App,
        channel: &str,
    ) -> Option<sockudo_delta::ChannelDeltaSettings> {
        let channel_delta_map = app_config.channel_delta_compression.as_ref()?;

        // First try exact match
        if let Some(config) = channel_delta_map.get(channel) {
            return Self::convert_channel_config_to_settings(config);
        }

        // Try pattern matching for wildcard patterns
        for (pattern, config) in channel_delta_map.iter() {
            if Self::matches_pattern(channel, pattern) {
                return Self::convert_channel_config_to_settings(config);
            }
        }

        None
    }

    #[cfg(feature = "delta")]
    /// Convert ChannelDeltaConfig enum to ChannelDeltaSettings struct
    fn convert_channel_config_to_settings(
        config: &sockudo_delta::ChannelDeltaConfig,
    ) -> Option<sockudo_delta::ChannelDeltaSettings> {
        use sockudo_delta::{ChannelDeltaConfig, ChannelDeltaSettings, DeltaAlgorithm};

        match config {
            ChannelDeltaConfig::Full(settings) => Some(settings.clone()),
            ChannelDeltaConfig::Simple(simple) => {
                use sockudo_delta::ChannelDeltaSimple;
                match simple {
                    ChannelDeltaSimple::Disabled => None,
                    ChannelDeltaSimple::Inherit => None, // Inherit from global settings
                    ChannelDeltaSimple::Fossil => Some(ChannelDeltaSettings {
                        enabled: true,
                        algorithm: DeltaAlgorithm::Fossil,
                        conflation_key: None,
                        max_messages_per_key: 10,
                        max_conflation_keys: 100,
                        enable_tags: true,
                    }),
                    ChannelDeltaSimple::Xdelta3 => Some(ChannelDeltaSettings {
                        enabled: true,
                        algorithm: DeltaAlgorithm::Xdelta3,
                        conflation_key: None,
                        max_messages_per_key: 10,
                        max_conflation_keys: 100,
                        enable_tags: true,
                    }),
                }
            }
        }
    }

    #[cfg(feature = "delta")]
    /// Check if a channel name matches a pattern
    /// Supports:
    /// - Exact match: "market-data" matches "market-data"
    /// - Wildcard suffix: "market-*" matches "market-btc", "market-eth", etc.
    /// - Wildcard prefix: "*-data" matches "market-data", "price-data", etc.
    fn matches_pattern(channel: &str, pattern: &str) -> bool {
        // Exact match
        if channel == pattern {
            return true;
        }

        // Wildcard pattern matching
        if pattern.contains('*') {
            if let Some(prefix) = pattern.strip_suffix('*') {
                // Prefix match: "market-*" matches "market-btc"
                return channel.starts_with(prefix);
            } else if let Some(suffix) = pattern.strip_prefix('*') {
                // Suffix match: "*-data" matches "market-data"
                return channel.ends_with(suffix);
            } else {
                // Middle wildcard: "market-*-data" matches "market-btc-data"
                let parts: Vec<&str> = pattern.split('*').collect();
                if parts.len() == 2 {
                    return channel.starts_with(parts[0]) && channel.ends_with(parts[1]);
                }
            }
        }

        false
    }
}
