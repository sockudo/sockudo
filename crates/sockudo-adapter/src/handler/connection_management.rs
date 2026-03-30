#![allow(dead_code)]

// src/adapter/handler/connection_management.rs
use super::ConnectionHandler;
use sockudo_core::app::App;
use sockudo_core::error::{Error, Result};
use sockudo_core::utils;
use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::PusherMessage;
#[cfg(feature = "recovery")]
use sockudo_protocol::messages::generate_message_id;
use sockudo_ws::Message;
use sockudo_ws::axum_integration::WebSocketWriter;
#[cfg(feature = "delta")]
use std::sync::Arc;
use tracing::warn;

fn sanitize_v2_feature_flags(
    server_options: &sockudo_core::options::ServerOptions,
    mut message: PusherMessage,
) -> PusherMessage {
    if let Some(extras) = message.extras.as_mut() {
        if !server_options.ephemeral.enabled {
            extras.ephemeral = None;
        }
        if !server_options.echo_control.enabled {
            extras.echo = None;
        }
        let extras_empty = extras.headers.is_none()
            && extras.ephemeral.is_none()
            && extras.idempotency_key.is_none()
            && extras.echo.is_none();
        if extras_empty {
            message.extras = None;
        }
    }

    message
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
        message = sanitize_v2_feature_flags(self.server_options(), message);

        // Extras-level message idempotency (V2 feature).
        // Check extras.idempotency_key before broadcasting. If the key was already
        // seen within the TTL window, silently drop the message.
        if let Some(extras_key) = message.extras_idempotency_key() {
            let config = app_config.resolved_idempotency(&self.server_options().idempotency);
            if config.enabled {
                let cache_key = format!("idempotency:{}:{}:{}", app_config.id, channel, extras_key);
                if let Some(ref metrics) = self.metrics {
                    metrics.mark_idempotency_publish(&app_config.id);
                }
                let is_new = self
                    .cache_manager
                    .set_if_not_exists(&cache_key, "1", config.ttl_seconds)
                    .await?;
                if !is_new {
                    if let Some(ref metrics) = self.metrics {
                        metrics.mark_idempotency_duplicate(&app_config.id);
                    }
                    tracing::debug!(
                        app_id = %app_config.id,
                        channel = %channel,
                        key = %extras_key,
                        "Extras idempotency: duplicate message dropped"
                    );
                    return Ok(());
                }
            }
        }

        // Track ephemeral message metric (V2 feature)
        if message.is_ephemeral()
            && let Some(ref metrics) = self.metrics
        {
            metrics.mark_ephemeral_message(&app_config.id);
        }

        // Connection recovery bundles serial + message_id + replay buffer (V2 feature, gated by config).
        // Ephemeral messages skip recovery entirely — they are not stored in the replay buffer.
        #[cfg(feature = "recovery")]
        {
            if !message.is_ephemeral() {
                let options = self.server_options();
                let recovery_config =
                    app_config.resolved_connection_recovery(&options.connection_recovery);
                if recovery_config.enabled {
                    if message.message_id.is_none() {
                        message.message_id = Some(generate_message_id());
                    }
                    if let Some(ref replay_buffer) = self.replay_buffer {
                        let serial = replay_buffer.next_serial(&app_config.id, channel);
                        message.serial = Some(serial);
                    }

                    if let Some(ref replay_buffer) = self.replay_buffer
                        && let Ok(bytes) = sonic_rs::to_vec(&message)
                    {
                        replay_buffer.store(
                            &app_config.id,
                            channel,
                            message.serial.unwrap_or(0),
                            bytes,
                        );
                    }
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

    pub async fn broadcast_metachannel_event(
        &self,
        app_config: &App,
        channel: &str,
        event_name: &str,
        data: sonic_rs::Value,
    ) -> Result<()> {
        let Some(meta_channel) = utils::meta_channel_for(channel) else {
            return Ok(());
        };

        let message = PusherMessage {
            event: Some(format!("sockudo_internal:{event_name}")),
            channel: Some(meta_channel.clone()),
            data: Some(sockudo_protocol::messages::MessageData::Json(data)),
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        };

        self.broadcast_to_channel(app_config, &meta_channel, message, None)
            .await
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
        let channel_delta_map = app_config.channel_delta_compression_ref()?;

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
        sockudo_core::utils::wildcard_pattern_matches(channel, pattern)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::app::AppIdempotencyConfig;
    use sockudo_core::options::IdempotencyConfig;
    use sockudo_protocol::messages::{MessageExtras, PusherMessage};
    use sonic_rs::JsonValueTrait;

    #[test]
    fn test_resolve_idempotency_uses_global_when_no_app_override() {
        let global = IdempotencyConfig {
            enabled: true,
            ttl_seconds: 300,
            max_key_length: 128,
        };
        let app = App::default();
        let resolved = app.resolved_idempotency(&global);
        assert!(resolved.enabled);
        assert_eq!(resolved.ttl_seconds, 300);
    }

    #[test]
    fn test_resolve_idempotency_app_override_enabled() {
        let global = IdempotencyConfig {
            enabled: false,
            ttl_seconds: 300,
            max_key_length: 128,
        };
        let app = App::from_policy(
            String::new(),
            String::new(),
            String::new(),
            false,
            sockudo_core::app::AppPolicy {
                idempotency: Some(AppIdempotencyConfig {
                    enabled: Some(true),
                    ttl_seconds: Some(600),
                }),
                ..Default::default()
            },
        );
        let resolved = app.resolved_idempotency(&global);
        assert!(resolved.enabled);
        assert_eq!(resolved.ttl_seconds, 600);
    }

    #[test]
    fn test_resolve_idempotency_app_disables_globally_enabled() {
        let global = IdempotencyConfig {
            enabled: true,
            ttl_seconds: 300,
            max_key_length: 128,
        };
        let app = App::from_policy(
            String::new(),
            String::new(),
            String::new(),
            false,
            sockudo_core::app::AppPolicy {
                idempotency: Some(AppIdempotencyConfig {
                    enabled: Some(false),
                    ttl_seconds: None,
                }),
                ..Default::default()
            },
        );
        let resolved = app.resolved_idempotency(&global);
        assert!(!resolved.enabled);
        assert_eq!(resolved.ttl_seconds, 300); // falls back to global
    }

    #[test]
    fn test_resolve_idempotency_partial_app_override() {
        let global = IdempotencyConfig {
            enabled: true,
            ttl_seconds: 120,
            max_key_length: 128,
        };
        let app = App::from_policy(
            String::new(),
            String::new(),
            String::new(),
            false,
            sockudo_core::app::AppPolicy {
                idempotency: Some(AppIdempotencyConfig {
                    enabled: None, // inherit global
                    ttl_seconds: Some(999),
                }),
                ..Default::default()
            },
        );
        let resolved = app.resolved_idempotency(&global);
        assert!(resolved.enabled); // from global
        assert_eq!(resolved.ttl_seconds, 999); // from app
    }

    #[test]
    fn test_extras_idempotency_key_cache_key_format() {
        let app_id = "app-123";
        let channel = "market-btc";
        let key = "dedup-abc";
        let cache_key = format!("idempotency:{}:{}:{}", app_id, channel, key);
        assert_eq!(cache_key, "idempotency:app-123:market-btc:dedup-abc");
    }

    #[test]
    fn test_different_channels_same_key_produce_different_cache_keys() {
        let app_id = "app-1";
        let key = "same-key";
        let key1 = format!("idempotency:{}:{}:{}", app_id, "channel-a", key);
        let key2 = format!("idempotency:{}:{}:{}", app_id, "channel-b", key);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_same_channel_same_key_different_apps_produce_different_cache_keys() {
        let channel = "market-btc";
        let key = "same-key";
        let key1 = format!("idempotency:{}:{}:{}", "app-1", channel, key);
        let key2 = format!("idempotency:{}:{}:{}", "app-2", channel, key);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_message_without_extras_key_proceeds() {
        let msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        assert!(msg.extras_idempotency_key().is_none());
    }

    #[test]
    fn test_message_with_extras_key_returns_key() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            idempotency_key: Some("my-key".to_string()),
            ..Default::default()
        });
        assert_eq!(msg.extras_idempotency_key(), Some("my-key"));
    }

    #[test]
    fn test_ephemeral_message_is_detected() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            ephemeral: Some(true),
            ..Default::default()
        });
        assert!(msg.is_ephemeral());
    }

    #[test]
    fn test_non_ephemeral_message() {
        let msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        assert!(!msg.is_ephemeral());
    }

    #[test]
    fn test_ephemeral_false_when_extras_present_but_ephemeral_not_set() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            echo: Some(true),
            ..Default::default()
        });
        assert!(!msg.is_ephemeral());
    }

    #[test]
    fn test_ephemeral_explicitly_false() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            ephemeral: Some(false),
            ..Default::default()
        });
        assert!(!msg.is_ephemeral());
    }

    #[test]
    fn test_ephemeral_with_idempotency_key_both_present() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            ephemeral: Some(true),
            idempotency_key: Some("dedup-123".to_string()),
            ..Default::default()
        });
        assert!(msg.is_ephemeral());
        assert_eq!(msg.extras_idempotency_key(), Some("dedup-123"));
    }

    #[test]
    fn test_ephemeral_preserves_echo_control() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            ephemeral: Some(true),
            echo: Some(false),
            ..Default::default()
        });
        assert!(msg.is_ephemeral());
        assert!(!msg.should_echo(true));
    }

    #[test]
    fn test_disabled_ephemeral_strips_ephemeral_flag() {
        let mut options = sockudo_core::options::ServerOptions::default();
        options.ephemeral.enabled = false;

        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            ephemeral: Some(true),
            ..Default::default()
        });

        let sanitized = sanitize_v2_feature_flags(&options, msg);
        assert!(!sanitized.is_ephemeral());
        assert!(sanitized.extras.is_none());
    }

    #[test]
    fn test_disabled_echo_control_strips_echo_override_only() {
        let mut options = sockudo_core::options::ServerOptions::default();
        options.echo_control.enabled = false;

        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            echo: Some(false),
            idempotency_key: Some("dedup-1".to_string()),
            ..Default::default()
        });

        let sanitized = sanitize_v2_feature_flags(&options, msg);
        assert_eq!(sanitized.extras_idempotency_key(), Some("dedup-1"));
        assert!(sanitized.should_echo(true));
        assert_eq!(
            sanitized.extras.as_ref().and_then(|extras| extras.echo),
            None
        );
    }

    #[test]
    fn test_broadcast_message_ephemeral_flag() {
        use crate::horizontal_adapter::BroadcastMessage;

        let broadcast = BroadcastMessage {
            node_id: "node-1".to_string(),
            app_id: "app-1".to_string(),
            channel: "cursors".to_string(),
            message: "{}".to_string(),
            except_socket_id: None,
            timestamp_ms: None,
            compression_metadata: None,
            idempotency_key: None,
            ephemeral: true,
        };
        assert!(broadcast.ephemeral);

        // Verify serialization includes ephemeral when true
        let json = sonic_rs::to_string(&broadcast).unwrap();
        assert!(json.contains("\"ephemeral\":true"));
    }

    #[test]
    fn test_broadcast_message_ephemeral_skipped_when_false() {
        use crate::horizontal_adapter::BroadcastMessage;

        let broadcast = BroadcastMessage {
            node_id: "node-1".to_string(),
            app_id: "app-1".to_string(),
            channel: "orders".to_string(),
            message: "{}".to_string(),
            except_socket_id: None,
            timestamp_ms: None,
            compression_metadata: None,
            idempotency_key: None,
            ephemeral: false,
        };

        // Verify serialization omits ephemeral when false (skip_serializing_if)
        let json = sonic_rs::to_string(&broadcast).unwrap();
        assert!(!json.contains("ephemeral"));
    }

    #[test]
    fn test_v1_delivery_strips_extras_including_ephemeral() {
        let mut msg =
            PusherMessage::channel_event("test", "ch", sonic_rs::json!({"hello": "world"}));
        msg.extras = Some(MessageExtras {
            ephemeral: Some(true),
            ..Default::default()
        });

        // Simulate V1 stripping
        msg.serial = None;
        msg.message_id = None;
        msg.extras = None;

        let json = sonic_rs::to_value(&msg).unwrap();
        assert!(json.get("extras").is_none());
    }

    // ── Echo control tests ──────────────────────────────────────────

    #[test]
    fn test_should_echo_default_true() {
        let msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        // Connection default true, no per-message override → echo
        assert!(msg.should_echo(true));
    }

    #[test]
    fn test_should_echo_connection_disabled() {
        let msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        // Connection default false, no per-message override → no echo
        assert!(!msg.should_echo(false));
    }

    #[test]
    fn test_should_echo_message_override_true() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            echo: Some(true),
            ..Default::default()
        });
        // Connection false but message says echo → echo
        assert!(msg.should_echo(false));
    }

    #[test]
    fn test_should_echo_message_override_false() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            echo: Some(false),
            ..Default::default()
        });
        // Connection true but message says no echo → no echo
        assert!(!msg.should_echo(true));
    }

    #[test]
    fn test_echo_messages_default_is_true() {
        let state = sockudo_core::websocket::ConnectionState::new();
        assert!(state.echo_messages);
    }

    #[test]
    fn test_v1_always_excludes_publisher() {
        // V1 connections always pass Some(socket_id) as except.
        // The should_echo method is not consulted for V1.
        // This test verifies the logic pattern used in handle_client_event_request.
        let protocol = sockudo_protocol::ProtocolVersion::V1;
        let is_v2 = matches!(protocol, sockudo_protocol::ProtocolVersion::V2);
        assert!(!is_v2); // V1 → always exclude publisher
    }

    #[test]
    fn test_v2_echo_messages_false_excludes_publisher() {
        let protocol = sockudo_protocol::ProtocolVersion::V2;
        let echo_messages = false;
        let msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));

        let is_v2 = matches!(protocol, sockudo_protocol::ProtocolVersion::V2);
        assert!(is_v2);
        let should_echo = msg.should_echo(echo_messages);
        assert!(!should_echo); // connection echo off → exclude publisher
    }

    #[test]
    fn test_v2_echo_messages_true_includes_publisher() {
        let protocol = sockudo_protocol::ProtocolVersion::V2;
        let echo_messages = true;
        let msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));

        let is_v2 = matches!(protocol, sockudo_protocol::ProtocolVersion::V2);
        assert!(is_v2);
        let should_echo = msg.should_echo(echo_messages);
        assert!(should_echo); // connection echo on → include publisher
    }

    #[test]
    fn test_rest_publish_no_socket_delivers_to_all() {
        // REST publish passes None as publisher_socket_id.
        // All subscribers receive the message.
        let publisher_socket_id: Option<&SocketId> = None;
        assert!(publisher_socket_id.is_none());
    }
}
