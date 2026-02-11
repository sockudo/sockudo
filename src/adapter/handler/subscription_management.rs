// src/adapter/handler/subscription_management.rs
use super::ConnectionHandler;
use super::types::*;
use crate::app::config::App;
use crate::channel::ChannelManager;
use crate::channel::manager::JoinResponse;
use crate::channel::{ChannelType, PresenceMemberInfo};
use crate::delta_compression::DeltaCompressionManager;
use crate::error::Result;

use crate::protocol::messages::{MessageData, PresenceData, PusherMessage};
use crate::utils::is_cache_channel;
use crate::websocket::SocketId;
use ahash::AHashMap;
use sonic_rs::Value;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SubscriptionResult {
    pub success: bool,
    pub auth_error: Option<String>,
    pub member: Option<PresenceMember>,
    pub channel_connections: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct PresenceMember {
    pub user_id: String,
    pub user_info: Value,
}

impl ConnectionHandler {
    pub async fn execute_subscription(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        request: &SubscriptionRequest,
        is_authenticated: bool,
    ) -> Result<SubscriptionResult> {
        let t_start = std::time::Instant::now();

        let t_before_msg_create = t_start.elapsed().as_micros();
        let temp_message = PusherMessage {
            channel: Some(request.channel.clone()),
            event: Some("pusher:subscribe".to_string()),
            data: Some(MessageData::Json(sonic_rs::json!({
                "channel": request.channel,
                "auth": request.auth,
                "channel_data": request.channel_data
            }))),
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
        };
        let t_after_msg_create = t_start.elapsed().as_micros();

        // Fast-path for LocalAdapter: lock-free subscription
        let subscription_result = if let Some(ref local_adapter) = self.local_adapter {
            // Only use fast-path for non-presence channels and when authenticated (or public channel)
            let t_before_channel_type = t_start.elapsed().as_micros();
            let channel_type = ChannelType::from_name(&request.channel);
            let t_after_channel_type = t_start.elapsed().as_micros();

            let can_use_fast_path = channel_type != ChannelType::Presence
                && (!channel_type.requires_authentication() || is_authenticated);

            if can_use_fast_path {
                let t_before_fast = t_start.elapsed().as_micros();

                // Try fast path with multiple retries for race conditions
                // In burst scenarios, sockets may not be immediately visible in the namespace
                let mut fast_result =
                    local_adapter.join_channel_fast(&app_config.id, &request.channel, socket_id);

                // Retry up to 3 times with brief yields if socket not found
                let mut retry_count = 0;
                while fast_result.is_none() && retry_count < 3 {
                    tokio::task::yield_now().await;
                    fast_result = local_adapter.join_channel_fast(
                        &app_config.id,
                        &request.channel,
                        socket_id,
                    );
                    retry_count += 1;
                }

                match fast_result {
                    Some(channel_connections) => {
                        let t_after_fast = t_start.elapsed().as_micros();
                        tracing::debug!(
                            "PERF[FAST_PATH] socket_id={} channel={} total={}μs channel_type={}μs",
                            socket_id,
                            request.channel,
                            t_after_fast - t_before_fast,
                            t_after_channel_type - t_before_channel_type
                        );
                        JoinResponse {
                            success: true,
                            channel_connections: Some(channel_connections),
                            member: None,
                            auth_error: None,
                            error_message: None,
                            error_code: None,
                            _type: None,
                        }
                    }
                    None => {
                        // Fast-path failed (socket not found), fall back to normal path
                        let t_before_fallback = t_start.elapsed().as_micros();
                        tracing::debug!(
                            "PERF[FAST_PATH_FALLBACK] socket_id={} channel={} fallback_at={}μs",
                            socket_id,
                            request.channel,
                            t_before_fallback
                        );
                        let result = ChannelManager::subscribe(
                            &self.connection_manager,
                            &socket_id.to_string(),
                            &temp_message,
                            &request.channel,
                            is_authenticated,
                            &app_config.id,
                        )
                        .await?;
                        let t_after_fallback = t_start.elapsed().as_micros();
                        tracing::debug!(
                            "PERF[FALLBACK_DONE] socket_id={} channel={} fallback_time={}μs",
                            socket_id,
                            request.channel,
                            t_after_fallback - t_before_fallback
                        );
                        result
                    }
                }
            } else {
                // Can't use fast-path for presence channels, use normal path
                let t_before_normal = t_start.elapsed().as_micros();
                let result = ChannelManager::subscribe(
                    &self.connection_manager,
                    &socket_id.to_string(),
                    &temp_message,
                    &request.channel,
                    is_authenticated,
                    &app_config.id,
                )
                .await?;
                let t_after_normal = t_start.elapsed().as_micros();
                tracing::debug!(
                    "PERF[NORMAL_PATH] socket_id={} channel={} reason=presence total={}μs",
                    socket_id,
                    request.channel,
                    t_after_normal - t_before_normal
                );
                result
            }
        } else {
            // No LocalAdapter, use normal path (Redis, NATS, etc.)
            let t_before_redis = t_start.elapsed().as_micros();
            let result = ChannelManager::subscribe(
                &self.connection_manager,
                &socket_id.to_string(),
                &temp_message,
                &request.channel,
                is_authenticated,
                &app_config.id,
            )
            .await?;
            let t_after_redis = t_start.elapsed().as_micros();
            tracing::debug!(
                "PERF[REDIS_PATH] socket_id={} channel={} total={}μs",
                socket_id,
                request.channel,
                t_after_redis - t_before_redis
            );
            result
        };

        let t_after_channel_mgr = t_start.elapsed().as_micros();

        // Note: Filter storage is handled in update_connection_subscription_state() below
        // to avoid duplicate storage and expensive clones

        // Track subscription metrics if successful
        let t_before_metrics = t_start.elapsed().as_micros();
        if subscription_result.success
            && let Some(ref metrics) = self.metrics
        {
            let channel_type = ChannelType::from_name(&request.channel);
            let channel_type_str = channel_type.as_str();

            // Mark subscription metric
            {
                let metrics_locked = metrics.lock().await;
                metrics_locked.mark_channel_subscription(&app_config.id, channel_type_str);
            }

            // Update active channel count if this is the first connection to the channel
            if subscription_result.channel_connections == Some(1) {
                // Channel became active - increment the count for this channel type
                // Pass the Arc directly to avoid holding any locks
                self.increment_active_channel_count(
                    &app_config.id,
                    channel_type_str,
                    metrics.clone(),
                )
                .await;
            }
        }
        let t_after_metrics = t_start.elapsed().as_micros();

        let total = t_start.elapsed().as_micros();
        tracing::debug!(
            "PERF[EXECUTE_SUB] socket_id={} channel={} total={}μs msg_create={}μs channel_mgr={}μs metrics={}μs",
            socket_id,
            request.channel,
            total,
            t_after_msg_create - t_before_msg_create,
            t_after_channel_mgr - t_after_msg_create,
            t_after_metrics - t_before_metrics
        );

        // Convert the channel manager result to our result type
        Ok(SubscriptionResult {
            success: subscription_result.success,
            auth_error: subscription_result.auth_error,
            member: subscription_result.member.map(|m| PresenceMember {
                user_id: m.user_id.to_string(),
                user_info: m.user_info,
            }),
            channel_connections: subscription_result.channel_connections,
        })
    }

    pub async fn handle_post_subscription(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        request: &SubscriptionRequest,
        subscription_result: &SubscriptionResult,
    ) -> Result<()> {
        // CRITICAL: Send subscription success to client FIRST (before any other work)
        // This ensures the client gets immediate feedback
        let channel_type = ChannelType::from_name(&request.channel);
        match channel_type {
            ChannelType::Presence => {
                // For presence, we need member list first, so can't optimize this path easily
                self.handle_presence_subscription_success(
                    socket_id,
                    app_config,
                    request,
                    subscription_result,
                )
                .await?;
            }
            _ => {
                // For non-presence channels, send success immediately
                self.send_subscription_succeeded(socket_id, app_config, &request.channel, None)
                    .await?;
            }
        }

        // Update connection state AFTER sending response (non-critical for client acknowledgment)
        self.update_connection_subscription_state(
            socket_id,
            app_config,
            request,
            subscription_result,
        )
        .await?;

        // Apply per-subscription delta settings if provided
        // This allows clients to negotiate delta compression on a per-channel basis
        self.apply_subscription_delta_settings(socket_id, &request.channel, &request.delta)
            .await;

        // Send delta compression cache sync if enabled for this socket and channel
        tracing::debug!(
            "About to call send_delta_cache_sync_if_needed for socket {} channel {}",
            socket_id,
            request.channel
        );
        self.send_delta_cache_sync_if_needed(socket_id, app_config, &request.channel)
            .await?;
        tracing::debug!(
            "send_delta_cache_sync_if_needed completed successfully for socket {} channel {}",
            socket_id,
            request.channel
        );

        // Send webhooks after subscription success response (non-blocking for client)
        if subscription_result.channel_connections == Some(1)
            && let Some(webhook_integration) = self.webhook_integration.clone()
        {
            let app_config = app_config.clone();
            let channel = request.channel.clone();
            tokio::spawn(async move {
                if let Err(e) = webhook_integration
                    .send_channel_occupied(&app_config, &channel)
                    .await
                {
                    tracing::warn!(
                        "Failed to send channel_occupied webhook for {}: {}",
                        channel,
                        e
                    );
                }
            });
        }

        // Send subscription count webhook for non-presence channels
        if !request.channel.starts_with("presence-")
            && let Some(webhook_integration) = &self.webhook_integration
        {
            let current_count = self
                .connection_manager
                .get_channel_socket_count(&app_config.id, &request.channel)
                .await;

            webhook_integration
                .send_subscription_count_changed(app_config, &request.channel, current_count)
                .await
                .ok();
        }

        // Handle cache channels
        if is_cache_channel(&request.channel) {
            self.send_missed_cache_if_exists(&app_config.id, socket_id, &request.channel)
                .await?;
        }

        Ok(())
    }

    async fn update_connection_subscription_state(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        request: &SubscriptionRequest,
        subscription_result: &SubscriptionResult,
    ) -> Result<()> {
        let connection_manager = &self.connection_manager;
        if let Some(conn_arc) = connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
        {
            // Use WebSocketRef's lock-free filter update
            conn_arc
                .subscribe_to_channel_with_filter(
                    request.channel.clone(),
                    request.tags_filter.clone(),
                )
                .await;

            // Register with filter index for O(1) message routing (if local adapter is available)
            if let Some(ref local_adapter) = self.local_adapter {
                let filter_index = local_adapter.get_filter_index();
                // Get the filter we just stored on the socket
                let filter_node = conn_arc.get_channel_filter_sync(&request.channel);
                filter_index.add_socket_filter(
                    &request.channel,
                    *socket_id,
                    filter_node.as_deref(),
                );
            }

            let mut conn_locked = conn_arc.inner.lock().await;

            // Handle presence data
            if let Some(ref member) = subscription_result.member {
                conn_locked.state.user_id = Some(member.user_id.clone());

                let presence_info = PresenceMemberInfo {
                    user_id: member.user_id.clone(),
                    user_info: Some(member.user_info.clone()),
                };

                conn_locked.add_presence_info(request.channel.clone(), presence_info);

                // Release the connection lock before calling add_user
                drop(conn_locked);

                // Add user to the user-socket mapping so get_user_sockets() can find it
                self.connection_manager.add_user(conn_arc.clone()).await?;
            } else {
                // Release locks when not needed
                drop(conn_locked);
            }
        }

        Ok(())
    }

    async fn handle_presence_subscription_success(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        request: &SubscriptionRequest,
        subscription_result: &SubscriptionResult,
    ) -> Result<()> {
        if let Some(ref presence_member) = subscription_result.member {
            // Use centralized presence member addition logic (instance method for race safety)
            self.presence_manager
                .handle_member_added(
                    Arc::clone(&self.connection_manager),
                    self.webhook_integration.as_ref(),
                    app_config,
                    &request.channel,
                    &presence_member.user_id,
                    Some(&presence_member.user_info),
                    Some(socket_id),
                )
                .await?;

            // Get current members and send presence data to new member
            let members_map = self
                .connection_manager
                .get_channel_members(&app_config.id, &request.channel)
                .await?;

            let presence_data = PresenceData {
                ids: members_map.keys().cloned().collect::<Vec<String>>(),
                hash: members_map
                    .iter()
                    .map(|(k, v)| (k.clone(), v.user_info.clone()))
                    .collect::<AHashMap<String, Option<Value>>>(),
                count: members_map.len(),
            };

            self.send_subscription_succeeded(
                socket_id,
                app_config,
                &request.channel,
                Some(presence_data),
            )
            .await?;
        }

        Ok(())
    }

    async fn send_subscription_succeeded(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        channel: &str,
        data: Option<PresenceData>,
    ) -> Result<()> {
        let response_msg = PusherMessage::subscription_succeeded(channel.to_string(), data);

        // Fast path: if we have LocalAdapter, use it directly (lock-free)
        if let Some(ref local_adapter) = self.local_adapter {
            // Cast to ConnectionManager trait to call send_message
            let adapter_ref: &dyn crate::adapter::connection_manager::ConnectionManager =
                local_adapter.as_ref();
            return adapter_ref
                .send_message(&app_config.id, socket_id, response_msg)
                .await;
        }

        // Slow path: use connection_manager (lock-free now for all adapters)
        self.connection_manager
            .send_message(&app_config.id, socket_id, response_msg)
            .await
    }

    /// Apply per-subscription delta settings from the subscription request
    ///
    /// This implements per-subscription delta negotiation, allowing clients to:
    /// - Enable/disable delta compression for specific channels
    /// - Specify a preferred algorithm per channel
    ///
    /// Formats supported in subscription:
    /// - `"delta": "fossil"` - Enable with Fossil algorithm
    /// - `"delta": "xdelta3"` - Enable with Xdelta3 algorithm
    /// - `"delta": true` - Enable with server default algorithm
    /// - `"delta": false` - Disable delta compression for this channel
    /// - `"delta": { "enabled": true, "algorithm": "fossil" }` - Full object format
    async fn apply_subscription_delta_settings(
        &self,
        socket_id: &SocketId,
        channel: &str,
        delta_settings: &Option<SubscriptionDeltaSettings>,
    ) {
        // Skip if no delta settings provided (use global/default behavior)
        let Some(settings) = delta_settings else {
            return;
        };

        // Skip encrypted channels - delta compression is useless for them
        if DeltaCompressionManager::is_encrypted_channel(channel) {
            tracing::debug!(
                "Ignoring per-subscription delta settings for encrypted channel '{}' - delta compression has no benefit",
                channel
            );
            return;
        }

        // Apply the per-channel settings
        self.delta_compression.set_channel_delta_settings(
            socket_id,
            channel,
            settings.should_enable(),
            settings.preferred_algorithm(),
        );

        tracing::info!(
            "Applied per-subscription delta settings for socket {} channel {}: enabled={:?}, algorithm={:?}",
            socket_id,
            channel,
            settings.should_enable(),
            settings.preferred_algorithm()
        );

        // If enabling delta compression for this channel and socket doesn't have global delta enabled,
        // send a confirmation message so the client knows delta is active
        if settings.should_enable() == Some(true)
            && !self.delta_compression.is_enabled_for_socket(socket_id)
        {
            let algorithm = settings
                .preferred_algorithm()
                .unwrap_or(self.delta_compression.get_algorithm());
            let algorithm_str = match algorithm {
                crate::delta_compression::DeltaAlgorithm::Fossil => "fossil",
                crate::delta_compression::DeltaAlgorithm::Xdelta3 => "xdelta3",
            };

            // Send confirmation that delta compression is enabled for this channel
            let confirmation = PusherMessage {
                event: Some("pusher:delta_compression_enabled".to_string()),
                channel: Some(channel.to_string()),
                data: Some(crate::protocol::messages::MessageData::Json(
                    sonic_rs::json!({
                        "enabled": true,
                        "algorithm": algorithm_str,
                        "channel": channel
                    }),
                )),
                name: None,
                user_id: None,
                tags: None,
                sequence: None,
                conflation_key: None,
            };

            // Get app_id from connection manager
            if let Ok(apps) = self.app_manager.get_apps().await {
                for app in apps {
                    if self
                        .connection_manager
                        .get_connection(socket_id, &app.id)
                        .await
                        .is_some()
                    {
                        if let Err(e) = self
                            .send_message_to_socket(&app.id, socket_id, confirmation.clone())
                            .await
                        {
                            tracing::warn!(
                                "Failed to send per-channel delta confirmation for socket {} channel {}: {}",
                                socket_id,
                                channel,
                                e
                            );
                        }
                        break;
                    }
                }
            }
        }
    }

    async fn send_delta_cache_sync_if_needed(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        channel: &str,
    ) -> Result<()> {
        tracing::debug!(
            "send_delta_cache_sync_if_needed: START for socket {} channel {}",
            socket_id,
            channel
        );

        // Check if delta compression is enabled for this socket
        let is_enabled = self.delta_compression.is_enabled_for_socket(socket_id);
        tracing::debug!(
            "send_delta_cache_sync_if_needed: delta compression enabled for socket {}: {}",
            socket_id,
            is_enabled
        );

        if !is_enabled {
            tracing::debug!("send_delta_cache_sync_if_needed: returning early - not enabled");
            return Ok(());
        }

        // Get channel-specific delta compression settings from app config
        let channel_settings = app_config
            .channel_delta_compression
            .as_ref()
            .and_then(|map| map.get(channel))
            .and_then(|config| {
                // Convert ChannelDeltaConfig to ChannelDeltaSettings
                use crate::delta_compression::ChannelDeltaConfig;
                match config {
                    ChannelDeltaConfig::Full(settings) => Some(settings.clone()),
                    _ => None,
                }
            });

        // Only send cache sync if channel has conflation key configured
        let conflation_key = channel_settings
            .as_ref()
            .and_then(|s| s.conflation_key.as_deref());

        tracing::debug!(
            "send_delta_cache_sync_if_needed: conflation_key = {:?}",
            conflation_key
        );

        if conflation_key.is_none() {
            // No conflation key configured, no cache sync needed
            tracing::debug!("send_delta_cache_sync_if_needed: returning early - no conflation key");
            return Ok(());
        }

        // IMPORTANT: Do NOT send cache sync after resubscribe when channel state was cleared.
        // After unsubscribe, clear_channel_state() removes all channel state for that socket.
        // If we send cache sync with messages from OTHER sockets (global cache), the sequence
        // numbers won't match what THIS socket expects, causing delta decode failures.
        //
        // The socket needs to build its cache from scratch starting at seq 0 with full messages.
        // Therefore, we skip cache sync entirely on resubscribe. Cache sync was designed for
        // initial subscription, but after testing it causes more problems than it solves due to
        // sequence number mismatches. Each socket must receive its own full messages to establish
        // proper base messages for delta compression.
        tracing::debug!(
            "send_delta_cache_sync_if_needed: skipping cache sync (causes sequence mismatch on resubscribe)"
        );
        Ok(())
    }
}
