// src/adapter/handler/core_methods.rs
use super::ConnectionHandler;
use crate::channel_manager::ChannelManager;
use crate::cleanup::{AuthInfo, ConnectionCleanupInfo, DisconnectTask};
use crate::horizontal_adapter::DeadNodeEvent;
use sockudo_core::app::App;
use sockudo_core::error::{Error, Result};

use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::{ErrorData, MessageData, PusherMessage};
use sonic_rs::Value;
use sonic_rs::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

impl ConnectionHandler {
    pub async fn send_connection_established(
        &self,
        app_id: &str,
        socket_id: &SocketId,
    ) -> Result<()> {
        let connection_message = PusherMessage::connection_established(
            socket_id.to_string(),
            self.server_options.activity_timeout,
        );
        self.send_message_to_socket(app_id, socket_id, connection_message)
            .await
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
            code: Some(u32::from(error.close_code())),
        };
        let error_message =
            PusherMessage::error(error_data.code.unwrap_or(4000), error_data.message, channel);
        self.send_message_to_socket(app_id, socket_id, error_message)
            .await
    }

    pub async fn handle_unsubscribe(
        &self,
        socket_id: &SocketId,
        message: &PusherMessage,
        app_config: &App,
    ) -> Result<()> {
        // Extract channel name from message
        let channel_name = self.extract_channel_from_unsubscribe_message(message)?;

        // Get user ID before unsubscribing (for presence channels)
        let user_id = self.get_user_id_for_socket(socket_id, app_config).await?;

        // CRITICAL: Clear delta compression state BEFORE unsubscribing from channel.
        // This prevents race condition where a message broadcast arrives between unsubscribe
        // and clear_channel_state, which would send a delta based on stale state.
        // By clearing first, any in-flight messages will be sent as FULL (no base state).
        #[cfg(feature = "delta")]
        self.delta_compression
            .clear_channel_state(socket_id, &channel_name);

        // Perform the local unsubscription first. Cluster-wide count updates are
        // emitted after the hot path so client room-switch churn does not block
        // on horizontal request/reply fanout.
        let leave_response = ChannelManager::unsubscribe_local(
            &self.connection_manager,
            &socket_id.to_string(),
            &channel_name,
            &app_config.id,
            user_id.as_deref(),
        )
        .await?;

        // Update connection state
        self.update_connection_unsubscribe_state(socket_id, app_config, &channel_name)
            .await?;

        // Track unsubscription metrics
        if let Some(ref metrics) = self.metrics {
            let channel_type = sockudo_core::channel::ChannelType::from_name(&channel_name);
            let channel_type_str = channel_type.as_str();

            // Mark unsubscription metric
            {
                metrics.mark_channel_unsubscription(&app_config.id, channel_type_str);
            }

            // Per-pod gauge: deactivate on the transition from unsubscribe_local,
            // not a separate count read.
            if leave_response.vacated_locally {
                metrics.mark_channel_deactivated(&app_config.id, channel_type_str);
            }
        }

        // Handle presence channel member removal
        if channel_name.starts_with("presence-")
            && let Some(user_id_str) = user_id
        {
            let presence_history_policy = app_config
                .resolved_presence_history(&channel_name, &self.server_options().presence_history);
            // Use centralized presence member removal logic (instance method for race safety)
            self.presence_manager
                .handle_member_removed(
                    &self.connection_manager,
                    Arc::clone(self.presence_history_store()),
                    presence_history_policy.enabled,
                    self.webhook_integration.as_ref(),
                    self.metrics.as_ref(),
                    app_config,
                    &channel_name,
                    &user_id_str,
                    Some(socket_id),
                    sockudo_core::presence_history::PresenceHistoryEventCause::Disconnect,
                    None,
                    0,
                    Some(presence_history_policy.retention()),
                )
                .await?;
        }

        if let Err(e) = self
            .send_unsubscribe_count_notifications(app_config, &channel_name)
            .await
        {
            warn!(
                "Failed to emit unsubscribe count notifications for {}: {}",
                channel_name, e
            );
        }

        Ok(())
    }

    async fn send_unsubscribe_count_notifications(
        &self,
        app_config: &App,
        channel_name: &str,
    ) -> Result<()> {
        if sockudo_core::utils::is_meta_channel(channel_name) {
            return Ok(());
        }

        // Skip count reads entirely when this leave cannot produce an observable
        // count-derived event.
        let wants_channel_vacated_webhook =
            self.subscription_count_webhook_configured(app_config, "channel_vacated");
        let wants_subscription_count_webhook =
            self.subscription_count_webhook_configured(app_config, "subscription_count");
        let wants_meta_channel = self
            .subscription_count_meta_channel_has_local_subscriber(app_config, channel_name)
            .await;

        if !wants_channel_vacated_webhook
            && !wants_subscription_count_webhook
            && !wants_meta_channel
        {
            return Ok(());
        }

        let current_sub_count = self
            .connection_manager
            .get_channel_socket_count(&app_config.id, channel_name)
            .await;

        if wants_subscription_count_webhook
            && !channel_name.starts_with("presence-")
            && let Some(webhook_integration) = &self.webhook_integration
        {
            webhook_integration
                .send_subscription_count_changed(app_config, channel_name, current_sub_count)
                .await
                .ok();
        }

        if wants_meta_channel {
            let event_name = if current_sub_count == 0 {
                "channel_vacated"
            } else {
                "subscription_count"
            };
            self.broadcast_metachannel_event(
                app_config,
                channel_name,
                event_name,
                sonic_rs::json!({
                    "channel": channel_name,
                    "subscription_count": current_sub_count,
                }),
            )
            .await
            .ok();
        }

        // Send channel_vacated webhook if no subscribers left (with 3-second delay).
        // Per Pusher spec: delay prevents spurious webhooks from momentary disconnects.
        if current_sub_count == 0
            && wants_channel_vacated_webhook
            && let Some(webhook_integration) = &self.webhook_integration
        {
            let wi = Arc::clone(webhook_integration);
            let cm = Arc::clone(&self.connection_manager);
            let app = app_config.clone();
            let channel = channel_name.to_string();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                let count = cm.get_channel_socket_count(&app.id, &channel).await;
                if count == 0 {
                    wi.send_channel_vacated(&app, &channel).await.ok();
                }
            });
        }

        Ok(())
    }

    pub(crate) fn subscription_count_webhook_configured(
        &self,
        app_config: &App,
        event_type: &str,
    ) -> bool {
        self.webhook_integration
            .as_ref()
            .is_some_and(|integration| integration.webhook_configured(app_config, event_type))
    }

    /// Returns true when this node has local subscribers for the channel's
    /// meta-channel. Count webhooks are checked separately so webhook-only apps
    /// don't also broadcast internal meta-channel events across the cluster.
    pub(crate) async fn subscription_count_meta_channel_has_local_subscriber(
        &self,
        app_config: &App,
        channel: &str,
    ) -> bool {
        match sockudo_core::utils::meta_channel_for(channel) {
            Some(meta_channel) => {
                self.connection_manager
                    .get_local_channel_socket_count(&app_config.id, &meta_channel)
                    .await
                    > 0
            }
            None => false,
        }
    }

    async fn should_use_async_cleanup(&self) -> bool {
        const MAX_CONSECUTIVE_FAILURES: usize = 10;
        const CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS: u64 = 30;

        if let Some(ref cleanup_queue) = self.cleanup_queue {
            // FIX: Use Acquire ordering for reads and Release for writes to ensure
            // proper memory ordering across threads
            let failures = self.cleanup_consecutive_failures.load(Ordering::Acquire);

            if failures > MAX_CONSECUTIVE_FAILURES {
                // Circuit breaker is open - check if we should try recovery
                let opened_at = self
                    .cleanup_circuit_breaker_opened_at
                    .load(Ordering::Acquire);
                let current_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                if opened_at == 0 {
                    // FIX: Use compare_exchange to atomically set the opened_at time
                    // Only one thread should "open" the circuit breaker
                    match self.cleanup_circuit_breaker_opened_at.compare_exchange(
                        0,
                        current_time,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            // We successfully opened the circuit breaker
                            warn!(
                                "Circuit breaker opened: too many cleanup failures ({}), disabling async cleanup for {} seconds",
                                failures, CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS
                            );
                        }
                        Err(_) => {
                            // Another thread already opened it, that's fine
                            debug!("Circuit breaker already opened by another thread");
                        }
                    }
                    return false;
                } else if current_time >= opened_at + CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS {
                    // Time to try recovery - enter half-open state
                    debug!(
                        "Circuit breaker entering half-open state after {} seconds, attempting recovery",
                        current_time - opened_at
                    );
                    return !cleanup_queue.is_closed();
                } else {
                    // Still in timeout period
                    debug!(
                        "Circuit breaker still open, {} seconds remaining until recovery attempt",
                        (opened_at + CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS) - current_time
                    );
                    return false;
                }
            }

            // Normal operation or successful recovery
            !cleanup_queue.is_closed()
        } else {
            false
        }
    }

    pub async fn handle_disconnect(&self, app_id: &str, socket_id: &SocketId) -> Result<()> {
        self.handle_disconnect_with_presence_timeout(app_id, socket_id, 0)
            .await
    }

    pub async fn handle_ungraceful_disconnect(
        &self,
        app_id: &str,
        socket_id: &SocketId,
    ) -> Result<()> {
        self.handle_disconnect_with_presence_timeout(
            app_id,
            socket_id,
            self.server_options().presence.ungraceful_timeout_seconds,
        )
        .await
    }

    async fn handle_disconnect_with_presence_timeout(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        presence_ungraceful_timeout_seconds: u64,
    ) -> Result<()> {
        debug!("Handling disconnect for socket: {}", socket_id);

        // Try async cleanup first if queue is available and circuit breaker allows
        if self.should_use_async_cleanup().await {
            // should_use_async_cleanup() already verified cleanup_queue exists
            let cleanup_queue = self.cleanup_queue.as_ref().unwrap();
            match self
                .handle_disconnect_async(
                    app_id,
                    socket_id,
                    cleanup_queue,
                    presence_ungraceful_timeout_seconds,
                )
                .await
            {
                Ok(()) => {
                    // Success - reset both failure counter and circuit breaker state
                    // FIX: Use Release ordering to ensure the reset is visible to other threads
                    let previous_failures =
                        self.cleanup_consecutive_failures.swap(0, Ordering::Release);
                    let was_circuit_breaker_open = self
                        .cleanup_circuit_breaker_opened_at
                        .swap(0, Ordering::Release);

                    if was_circuit_breaker_open > 0 {
                        info!(
                            "Circuit breaker recovered: async cleanup successful after {} failures",
                            previous_failures
                        );
                    }

                    return Ok(());
                }
                Err(e) => {
                    // Failure - increment counter (circuit breaker logic handles the rest)
                    // FIX: Use AcqRel ordering to ensure proper synchronization with
                    // should_use_async_cleanup's reads
                    let new_failure_count = self
                        .cleanup_consecutive_failures
                        .fetch_add(1, Ordering::AcqRel)
                        + 1;
                    warn!(
                        "Async cleanup failed for socket {} (failure #{}: {}), falling back to sync",
                        socket_id, new_failure_count, e
                    );
                }
            }
        }

        // Fall back to original synchronous cleanup
        self.handle_disconnect_sync(app_id, socket_id, presence_ungraceful_timeout_seconds)
            .await
    }

    async fn handle_disconnect_async(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        cleanup_queue: &crate::cleanup::CleanupSender,
        presence_ungraceful_timeout_seconds: u64,
    ) -> Result<()> {
        use std::time::Instant;

        debug!("Using async cleanup for socket: {}", socket_id);

        // Step 1: Quick connection state capture (< 1ms).
        // Retain the WebSocketRef outside the lock scope so we can call shutdown()
        // before queuing the cleanup task — cleanup queue latency must not extend
        // the reader/writer task lifetime.
        let (disconnect_task, connection) = {
            let connection_manager = &self.connection_manager;
            let conn_ref =
                if let Some(c) = connection_manager.get_connection(socket_id, app_id).await {
                    c
                } else {
                    // Connection doesn't exist - might have been cleaned up already
                    debug!("Connection {} not found during disconnect", socket_id);
                    return Ok(());
                };

            // Atomic check-and-set for disconnecting flag to ensure idempotency
            let mut conn_locked = conn_ref.inner.lock().await;

            if conn_locked.state.disconnecting {
                debug!("Connection {} already disconnecting, skipping", socket_id);
                return Ok(());
            }

            // Set disconnecting flag atomically
            conn_locked.state.disconnecting = true;

            let channels: Vec<String> = conn_locked.state.get_subscribed_channels_list().to_vec();
            let user_id = conn_locked.state.user_id.clone();

            // Extract presence channel info for webhook processing
            let presence_channels: Vec<String> = channels
                .iter()
                .filter(|ch| ch.starts_with("presence-"))
                .cloned()
                .collect();

            let task = DisconnectTask {
                socket_id: *socket_id,
                app_id: app_id.to_string(),
                subscribed_channels: channels,
                user_id: user_id.clone(),
                timestamp: Instant::now(),
                connection_info: if !presence_channels.is_empty() {
                    Some(ConnectionCleanupInfo {
                        presence_channels,
                        auth_info: user_id.map(|uid| AuthInfo {
                            user_id: uid,
                            user_info: None,
                        }),
                    })
                } else {
                    None
                },
                presence_ungraceful_timeout_seconds,
            };

            drop(conn_locked);
            (task, conn_ref)
        };

        // Step 2: Clear immediate timeouts (these should be fast)
        self.clear_activity_timeout(app_id, socket_id).await.ok();
        self.clear_user_authentication_timeout(app_id, socket_id)
            .await
            .ok();

        // Step 3: Clean up client event rate limiter (lock-free)
        if self.client_event_limiters.remove(socket_id).is_some() {
            debug!(
                "Removed client event rate limiter for socket: {}",
                socket_id
            );
        }

        // Step 3.5: MEMORY LEAK FIX - Clean up delta compression state for this socket
        #[cfg(feature = "delta")]
        self.delta_compression.remove_socket(socket_id);

        // Cancel reader/writer tasks immediately, before cleanup queue latency.
        // shutdown() is idempotent (CancellationToken::cancel) — safe to call
        // even if the queue-full fallback path calls it again via sync cleanup.
        connection.shutdown();

        // Step 4: Queue cleanup work (non-blocking)
        if let Err(_send_error) = cleanup_queue.try_send(disconnect_task) {
            // Queue is full or closed - don't return error, fall back to sync cleanup
            warn!(
                "Failed to queue async cleanup for socket {} (queue full/closed), falling back to sync cleanup",
                socket_id
            );

            // FIX: Reset the disconnecting flag using proper lock (not try_lock) to ensure
            // the flag is always reset before falling back to sync cleanup.
            // Using try_lock() could fail and leave the flag stuck at true forever,
            // preventing future cleanup attempts for this connection.
            {
                let connection_manager = &self.connection_manager;
                if let Some(conn_ref) = connection_manager.get_connection(socket_id, app_id).await {
                    // Use .lock().await instead of try_lock() to guarantee we reset the flag
                    let mut conn_locked = conn_ref.inner.lock().await;
                    conn_locked.state.disconnecting = false;
                }
            }
            // Fall back to synchronous cleanup immediately
            // We already have the disconnect task info, so use sync cleanup
            return self
                .handle_disconnect_sync(app_id, socket_id, presence_ungraceful_timeout_seconds)
                .await;
        }
        debug!("Queued async cleanup for socket: {}", socket_id);

        // Step 5: Update metrics immediately (outside connection lock to minimize contention)
        if let Some(ref metrics) = self.metrics {
            // Use regular lock instead of try_lock to ensure metrics are always updated
            metrics.mark_disconnection(app_id, socket_id);
        }

        debug!(
            "Fast disconnect processing completed for socket: {}",
            socket_id
        );
        Ok(())
    }

    async fn handle_disconnect_sync(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        presence_ungraceful_timeout_seconds: u64,
    ) -> Result<()> {
        debug!("Using synchronous cleanup for socket: {}", socket_id);

        // This is the original synchronous implementation
        // Check if already disconnecting and set flag atomically
        let conn = {
            let connection_manager = &self.connection_manager;
            connection_manager.get_connection(socket_id, app_id).await
        };

        let already_disconnecting = if let Some(conn) = conn {
            if let Ok(mut conn_locked) = conn.inner.try_lock() {
                let was_disconnecting = conn_locked.state.disconnecting;
                conn_locked.state.disconnecting = true;
                was_disconnecting
            } else {
                debug!(
                    "Connection {} is busy, assuming disconnect already in progress",
                    socket_id
                );
                true
            }
        } else {
            true
        };

        if already_disconnecting {
            debug!(
                "Connection {} already disconnecting or doesn't exist, skipping",
                socket_id
            );
            return Ok(());
        }

        // Clear timeouts
        self.clear_activity_timeout(app_id, socket_id).await?;
        self.clear_user_authentication_timeout(app_id, socket_id)
            .await?;

        // Clean up client event rate limiter
        if self.client_event_limiters.remove(socket_id).is_some() {
            debug!(
                "Removed client event rate limiter for socket: {}",
                socket_id
            );
        }

        // MEMORY LEAK FIX: Clean up delta compression state for this socket
        #[cfg(feature = "delta")]
        self.delta_compression.remove_socket(socket_id);

        // Get app configuration
        let app_config = match self.app_manager.find_by_id(app_id).await? {
            Some(app) => app,
            None => {
                error!("App not found during disconnect: {}", app_id);
                self.cleanup_connection_from_manager(socket_id, app_id)
                    .await;
                return Err(sockudo_core::error::Error::ApplicationNotFound);
            }
        };

        // Extract connection state before cleanup
        let (subscribed_channels, user_id, user_watchlist) = self
            .extract_connection_state_for_disconnect(socket_id, &app_config)
            .await?;

        // Handle watchlist offline events
        if let Some(ref user_id_str) = user_id {
            self.handle_disconnect_watchlist_events(
                &app_config,
                user_id_str,
                socket_id,
                user_watchlist,
            )
            .await?;
        }

        // Remove channel memberships while the snapshot still corresponds to
        // live namespace entries. Presence counting explicitly excludes this
        // socket, so member_removed decisions remain correct without deleting
        // the authoritative connection first.
        let unsubscribe_result = if subscribed_channels.is_empty() {
            Ok(())
        } else {
            self.process_channel_unsubscriptions_on_disconnect(
                socket_id,
                &app_config,
                &subscribed_channels,
                &user_id,
                presence_ungraceful_timeout_seconds,
            )
            .await
        };

        // Always finish comprehensive resource cleanup, even when channel or
        // presence processing reports an error.
        self.cleanup_connection_from_manager(socket_id, app_id)
            .await;
        unsubscribe_result?;

        // Update metrics
        if let Some(ref metrics) = self.metrics {
            metrics.mark_disconnection(app_id, socket_id);
        }

        debug!(
            "Successfully processed synchronous disconnect for socket: {}",
            socket_id
        );
        Ok(())
    }

    // Helper methods for the main disconnect handler
    async fn extract_connection_state_for_disconnect(
        &self,
        socket_id: &SocketId,
        app_config: &App,
    ) -> Result<(HashSet<String>, Option<String>, Option<Vec<String>>)> {
        let connection_manager = &self.connection_manager;
        match connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
        {
            Some(conn_arc) => {
                let mut conn_locked = conn_arc.inner.lock().await;

                // Cancel any active timeouts
                conn_locked.state.timeouts.clear_all();

                let watchlist = conn_locked
                    .state
                    .user_info
                    .as_ref()
                    .and_then(|ui| ui.watchlist.clone());

                Ok((
                    conn_locked
                        .state
                        .get_subscribed_channels_list()
                        .into_iter()
                        .collect(),
                    conn_locked.state.user_id.clone(),
                    watchlist,
                ))
            }
            None => {
                warn!(
                    "No connection found for socket during disconnect: {}",
                    socket_id
                );
                Ok((HashSet::new(), None, None))
            }
        }
    }

    async fn process_channel_unsubscriptions_on_disconnect(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        subscribed_channels: &HashSet<String>,
        user_id: &Option<String>,
        presence_ungraceful_timeout_seconds: u64,
    ) -> Result<()> {
        if subscribed_channels.is_empty() {
            return Ok(());
        }

        debug!(
            "Processing batch unsubscribe for socket {} from {} channels",
            socket_id,
            subscribed_channels.len()
        );

        // Prepare batch operations for all channels
        let operations: Vec<(String, String, String)> = subscribed_channels
            .iter()
            .map(|channel| {
                (
                    socket_id.to_string(),
                    channel.clone(),
                    app_config.id.clone(),
                )
            })
            .collect();

        match ChannelManager::batch_unsubscribe(&self.connection_manager, operations).await {
            Ok(results) => {
                // Process webhook events for each successful unsubscribe
                for (channel_name, result) in &results {
                    match result {
                        Ok((was_removed, remaining_connections, local_vacated)) => {
                            // Per-pod gauge: decrement when the channel empties on this node.
                            if *local_vacated && let Some(ref metrics) = self.metrics {
                                let channel_type =
                                    sockudo_core::channel::ChannelType::from_name(channel_name)
                                        .as_str();
                                metrics.mark_channel_deactivated(&app_config.id, channel_type);
                            }
                            if *was_removed {
                                self.handle_post_unsubscribe_webhooks(
                                    app_config,
                                    channel_name,
                                    user_id,
                                    *remaining_connections,
                                    socket_id,
                                    presence_ungraceful_timeout_seconds,
                                )
                                .await?;
                            }
                        }
                        Err(e) => {
                            error!(
                                "Error unsubscribing socket {} from channel {} during disconnect: {}",
                                socket_id, channel_name, e
                            );
                        }
                    }
                }
            }
            Err(e) => {
                error!(
                    "Batch unsubscribe failed for socket {} during disconnect: {}",
                    socket_id, e
                );
            }
        }

        Ok(())
    }

    async fn handle_post_unsubscribe_webhooks(
        &self,
        app_config: &App,
        channel_str: &str,
        user_id: &Option<String>,
        current_sub_count: usize,
        socket_id: &SocketId,
        presence_ungraceful_timeout_seconds: u64,
    ) -> Result<()> {
        if channel_str.starts_with("presence-") {
            if let Some(disconnected_user_id) = user_id {
                let presence_history_policy = app_config.resolved_presence_history(
                    channel_str,
                    &self.server_options().presence_history,
                );
                // Use centralized presence member removal logic (instance method for race safety)
                self.presence_manager
                    .handle_member_removed(
                        &self.connection_manager,
                        Arc::clone(self.presence_history_store()),
                        presence_history_policy.enabled,
                        self.webhook_integration.as_ref(),
                        self.metrics.as_ref(),
                        app_config,
                        channel_str,
                        disconnected_user_id,
                        Some(socket_id),
                        sockudo_core::presence_history::PresenceHistoryEventCause::Disconnect,
                        None,
                        presence_ungraceful_timeout_seconds,
                        Some(presence_history_policy.retention()),
                    )
                    .await
                    .ok();
            }
        } else {
            // Send subscription count webhook for non-presence channels
            if let Some(webhook_integration) = &self.webhook_integration {
                webhook_integration
                    .send_subscription_count_changed(app_config, channel_str, current_sub_count)
                    .await
                    .ok();
            }
        }

        // Send channel_vacated webhook if no subscribers left (with 3-second delay)
        // Per Pusher spec: delay prevents spurious webhooks from momentary disconnects
        if current_sub_count == 0
            && let Some(webhook_integration) = &self.webhook_integration
            && webhook_integration.webhook_configured(app_config, "channel_vacated")
        {
            let wi = Arc::clone(webhook_integration);
            let cm = Arc::clone(&self.connection_manager);
            let app = app_config.clone();
            let channel = channel_str.to_string();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                let count = cm.get_channel_socket_count(&app.id, &channel).await;
                if count == 0 {
                    wi.send_channel_vacated(&app, &channel).await.ok();
                }
            });
        }

        Ok(())
    }

    async fn handle_disconnect_watchlist_events(
        &self,
        app_config: &App,
        user_id_str: &str,
        socket_id: &SocketId,
        user_watchlist: Option<Vec<String>>,
    ) -> Result<()> {
        if app_config.watchlist_events_enabled() && user_watchlist.is_some() {
            info!(
                "Processing watchlist disconnect for user {} on socket {}",
                user_id_str, socket_id
            );

            // Remove user connection from watchlist manager
            let offline_events = self
                .watchlist_manager
                .remove_user_connection(&app_config.id, user_id_str, socket_id)
                .await?;

            // Send offline events to watchers if user went offline
            if !offline_events.is_empty() {
                let watchers_to_notify = self
                    .get_watchers_for_user(&app_config.id, user_id_str)
                    .await?;

                for event in offline_events {
                    for watcher_socket_id in &watchers_to_notify {
                        if let Err(e) = self
                            .send_message_to_socket(
                                &app_config.id,
                                watcher_socket_id,
                                event.clone(),
                            )
                            .await
                        {
                            warn!(
                                "Failed to send offline notification to watcher {}: {}",
                                watcher_socket_id, e
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn cleanup_connection_from_manager(&self, socket_id: &SocketId, app_id: &str) {
        let connection_manager = &self.connection_manager;

        // Cleanup connection resources
        if let Some(conn_to_cleanup) = connection_manager.get_connection(socket_id, app_id).await {
            connection_manager
                .cleanup_connection(app_id, conn_to_cleanup)
                .await;
        }

        // Remove connection from primary tracking
        connection_manager
            .remove_connection(socket_id, app_id)
            .await
            .ok();
    }

    // Helper methods for extracting data from messages
    fn extract_channel_from_unsubscribe_message(&self, message: &PusherMessage) -> Result<String> {
        let message_data = message.data.as_ref().ok_or_else(|| {
            Error::InvalidMessageFormat("Missing data in unsubscribe message".into())
        })?;

        match message_data {
            MessageData::String(channel_str) => Ok(channel_str.clone()),
            MessageData::Json(data) => data
                .get("channel")
                .and_then(Value::as_str)
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    Error::InvalidMessageFormat("Missing channel in unsubscribe message".into())
                }),
            MessageData::Structured { channel, .. } => {
                channel.as_ref().map(|s| s.to_string()).ok_or_else(|| {
                    Error::InvalidMessageFormat("Missing channel in unsubscribe message".into())
                })
            }
        }
    }

    async fn get_user_id_for_socket(
        &self,
        socket_id: &SocketId,
        app_config: &App,
    ) -> Result<Option<String>> {
        let connection_manager = &self.connection_manager;
        if let Some(conn) = connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
        {
            let conn_locked = conn.inner.lock().await;
            Ok(conn_locked.state.user_id.clone())
        } else {
            Ok(None)
        }
    }

    // Presence data is mirrored in Namespace.presence_data for lock-free reads.
    // These helpers are the only writers of the mirror: every add_presence_info or
    // remove_presence_info on connection state must be paired with the matching
    // call here or reads and connection state drift apart.
    pub(crate) fn set_namespace_presence(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        channel: &str,
        info: sockudo_core::channel::PresenceMemberInfo,
    ) {
        let Some(ref local_adapter) = self.local_adapter else {
            warn!(
                "local_adapter unavailable, namespace presence mirror not updated for socket {socket_id} in channel {channel}"
            );
            return;
        };
        let Some(namespace) = local_adapter.namespaces.get(app_id) else {
            warn!("namespace {app_id} missing, presence mirror not updated for socket {socket_id}");
            return;
        };
        namespace
            .presence_data
            .entry(*socket_id)
            .or_default()
            .insert(channel.to_string(), info);
    }

    pub(crate) fn clear_namespace_presence(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        channel: &str,
    ) {
        let Some(ref local_adapter) = self.local_adapter else {
            warn!(
                "local_adapter unavailable, namespace presence mirror not cleared for socket {socket_id} in channel {channel}"
            );
            return;
        };
        let Some(namespace) = local_adapter.namespaces.get(app_id) else {
            return;
        };
        if let dashmap::mapref::entry::Entry::Occupied(mut per_socket) =
            namespace.presence_data.entry(*socket_id)
        {
            per_socket.get_mut().remove(channel);
            if per_socket.get().is_empty() {
                per_socket.remove();
            }
        }
    }

    async fn update_connection_unsubscribe_state(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        channel_name: &str,
    ) -> Result<()> {
        let connection_manager = &self.connection_manager;
        if let Some(conn_arc) = connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
        {
            // Remove from filter index for O(1) message routing (if local adapter is available)
            #[cfg(feature = "tag-filtering")]
            if let Some(ref local_adapter) = self.local_adapter {
                let filter_index = local_adapter.get_filter_index();
                // Get the filter before removing it
                let filter_node = conn_arc.get_channel_filter_sync(channel_name);
                filter_index.remove_socket_filter(channel_name, *socket_id, filter_node.as_deref());
            }

            // Remove WebSocketRef's per-channel state (lock-free)
            conn_arc.channel_state.remove(channel_name);

            let mut conn_locked = conn_arc.inner.lock().await;
            conn_locked.unsubscribe_from_channel(channel_name);

            // Remove presence info if it's a presence channel
            if channel_name.starts_with("presence-") {
                conn_locked.remove_presence_info(channel_name);
                drop(conn_locked);

                self.clear_namespace_presence(&app_config.id, socket_id, channel_name);
            }
        }
        Ok(())
    }

    /// Helper to check if a user has any other connections to a specific presence channel.
    #[allow(dead_code)]
    async fn user_has_other_connections_in_presence_channel(
        &self,
        app_id: &str,
        channel_name: &str,
        user_id: &str,
    ) -> Result<bool> {
        let connection_manager = &self.connection_manager;
        let user_sockets = connection_manager.get_user_sockets(user_id, app_id).await?;

        for ws_ref in user_sockets.iter() {
            let socket_state_guard = ws_ref.inner.lock().await;
            if socket_state_guard.state.is_subscribed(channel_name) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn send_missed_cache_if_exists(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        channel: &str,
    ) -> Result<()> {
        let cache_key = format!("app:{app_id}:channel:{channel}:cache_miss");

        match self.cache_manager.get(&cache_key).await {
            Ok(Some(cache_content)) => {
                // Found cached content, send it to the socket
                let cache_message: PusherMessage =
                    sonic_rs::from_str(&cache_content).map_err(|e| {
                        Error::InvalidMessageFormat(format!("Invalid cached message format: {e}"))
                    })?;

                self.send_message_to_socket(app_id, socket_id, cache_message)
                    .await?;
                info!(
                    "Sent cached content to socket {} for channel {}",
                    socket_id, channel
                );
            }
            Ok(None) => {
                // No cache found, send cache miss event
                let cache_miss_message = PusherMessage::cache_miss_event(channel.to_string());

                self.send_message_to_socket(app_id, socket_id, cache_miss_message)
                    .await?;

                // Send cache miss webhook if configured
                if let Some(app_config) = self.app_manager.find_by_id(app_id).await?
                    && let Some(webhook_integration) = &self.webhook_integration
                    && let Err(e) = webhook_integration
                        .send_cache_missed(&app_config, channel)
                        .await
                {
                    warn!(
                        "Failed to send cache_missed webhook for channel {}: {}",
                        channel, e
                    );
                }

                info!(
                    "No cached content found for channel: {}, sent cache_miss event",
                    channel
                );
            }
            Err(e) => {
                error!("Failed to get cache for channel {}: {}", channel, e);

                // Send cache miss event as fallback
                let cache_miss_message = PusherMessage::cache_miss_event(channel.to_string());

                self.send_message_to_socket(app_id, socket_id, cache_miss_message)
                    .await?;

                return Err(Error::Internal(format!(
                    "Cache retrieval failed for channel {channel}: {e}"
                )));
            }
        }

        Ok(())
    }

    /// Store a message in cache for a channel
    pub async fn store_cache_for_channel(
        &self,
        app_id: &str,
        channel: &str,
        message: &PusherMessage,
        ttl_seconds: Option<u64>,
    ) -> Result<()> {
        let cache_key = format!("app:{app_id}:channel:{channel}:cache_miss");

        let message_json = sonic_rs::to_string(message).map_err(|e| {
            Error::InvalidMessageFormat(format!("Failed to serialize message for cache: {e}"))
        })?;

        match ttl_seconds {
            Some(ttl) => {
                self.cache_manager
                    .set(&cache_key, &message_json, ttl)
                    .await
                    .map_err(|e| Error::Internal(format!("Failed to store cache with TTL: {e}")))?;
            }
            None => {
                self.cache_manager
                    .set(&cache_key, &message_json, 0)
                    .await
                    .map_err(|e| Error::Internal(format!("Failed to store cache: {e}")))?;
            }
        }

        debug!("Stored cache for channel {} in app {}", channel, app_id);
        Ok(())
    }

    /// Clear cache for a specific channel
    pub async fn clear_cache_for_channel(&self, app_id: &str, channel: &str) -> Result<()> {
        let cache_key = format!("app:{app_id}:channel:{channel}:cache_miss");

        self.cache_manager.remove(&cache_key).await.map_err(|e| {
            Error::Internal(format!("Failed to clear cache for channel {channel}: {e}"))
        })?;

        debug!("Cleared cache for channel {} in app {}", channel, app_id);
        Ok(())
    }

    /// Check if a channel has cached content
    pub async fn has_cache_for_channel(&self, app_id: &str, channel: &str) -> Result<bool> {
        let cache_key = format!("app:{app_id}:channel:{channel}:cache_miss");

        match self.cache_manager.get(&cache_key).await {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => {
                warn!("Error checking cache for channel {}: {}", channel, e);
                Ok(false) // Assume no cache on error
            }
        }
    }

    /// Handle dead node cleanup event by processing each orphaned member
    pub async fn handle_dead_node_cleanup(&self, event: DeadNodeEvent) -> Result<()> {
        let orphaned_members_count = event.orphaned_members.len();
        debug!(
            "Processing dead node cleanup for node {}, cleaning up {} orphaned members",
            event.dead_node_id, orphaned_members_count
        );

        // Group orphaned members by app_id to batch app config lookups
        let mut members_by_app: HashMap<String, Vec<_>> = HashMap::new();
        for member in event.orphaned_members {
            members_by_app
                .entry(member.app_id.clone())
                .or_default()
                .push(member);
        }

        debug!(
            "Batched {} orphaned members across {} apps for efficient processing",
            orphaned_members_count,
            members_by_app.len()
        );

        // Process each app once
        for (app_id, members) in members_by_app {
            let app_config = match self.app_manager.find_by_id(&app_id).await {
                Ok(Some(app)) => app,
                Ok(None) => {
                    warn!(
                        "App {} not found during dead node cleanup, skipping {} members",
                        app_id,
                        members.len()
                    );
                    continue;
                }
                Err(e) => {
                    error!(
                        "Error fetching app {} during dead node cleanup: {}, skipping {} members",
                        app_id,
                        e,
                        members.len()
                    );
                    continue;
                }
            };

            debug!(
                "Processing {} orphaned members for app {}",
                members.len(),
                app_config.id
            );

            // Process all members for this app
            for orphaned_member in members {
                let presence_history_policy = app_config.resolved_presence_history(
                    &orphaned_member.channel,
                    &self.server_options().presence_history,
                );
                // Use PresenceManager to handle member removal (instance method for race safety)
                if let Err(e) = self
                    .presence_manager
                    .handle_member_removed(
                        &self.connection_manager,
                        Arc::clone(self.presence_history_store()),
                        presence_history_policy.enabled,
                        self.webhook_integration.as_ref(),
                        self.metrics.as_ref(),
                        &app_config,
                        &orphaned_member.channel,
                        &orphaned_member.user_id,
                        None, // No excluding socket for dead node cleanup
                        sockudo_core::presence_history::PresenceHistoryEventCause::OrphanCleanup,
                        Some(&event.dead_node_id),
                        0,
                        Some(presence_history_policy.retention()),
                    )
                    .await
                {
                    error!(
                        "Failed to handle member removal for user {} in channel {} (app: {}) during dead node cleanup: {}",
                        orphaned_member.user_id, orphaned_member.channel, orphaned_member.app_id, e
                    );
                } else {
                    debug!(
                        "Successfully cleaned up orphaned member {} from channel {} (app: {})",
                        orphaned_member.user_id, orphaned_member.channel, orphaned_member.app_id
                    );
                }
            }
        }

        info!(
            "Completed dead node cleanup for node {}, processed {} orphaned members",
            event.dead_node_id, orphaned_members_count
        );

        Ok(())
    }
}
