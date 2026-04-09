use crate::connection_manager::ConnectionManager;
use dashmap::DashMap;
use sockudo_core::app::App;
use sockudo_core::error::Result;
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::presence_history::{
    PresenceHistoryEventCause, PresenceHistoryEventKind, PresenceHistoryStore,
    PresenceHistoryTransitionRecord,
};
use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::PusherMessage;
use sockudo_webhook::WebhookIntegration;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, warn};

/// Lock key for presence operations to prevent TOCTOU races
/// Format: "app_id:channel:user_id"
fn presence_lock_key(app_id: &str, channel: &str, user_id: &str) -> String {
    format!("{}:{}:{}", app_id, channel, user_id)
}

/// Centralized presence channel management functionality
/// This module handles presence member removal logic that needs to be
/// consistent across different disconnect paths (sync, async, direct unsubscribe)
pub struct PresenceManager {
    /// Per-user locks to prevent TOCTOU races during presence operations
    /// Maps "app_id:channel:user_id" -> async mutex for true per-key exclusivity
    presence_locks: DashMap<String, Arc<Mutex<()>>>,
}

impl Default for PresenceManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PresenceManager {
    pub fn new() -> Self {
        Self {
            presence_locks: DashMap::new(),
        }
    }

    fn get_presence_lock(&self, lock_key: &str) -> Arc<Mutex<()>> {
        self.presence_locks
            .entry(lock_key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Handles presence member addition including both webhook and broadcast
    /// Only sends events if this is the user's FIRST connection to the presence channel
    ///
    /// FIX: Uses atomic check-and-act pattern to prevent TOCTOU race conditions
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_member_added(
        &self,
        connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
        presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
        presence_history_enabled: bool,
        webhook_integration: Option<&Arc<WebhookIntegration>>,
        metrics: Option<&Arc<dyn MetricsInterface + Send + Sync>>,
        app_config: &App,
        channel: &str,
        user_id: &str,
        user_info: Option<&sonic_rs::Value>,
        excluding_socket: Option<&SocketId>,
        retention: Option<sockudo_core::presence_history::PresenceHistoryRetentionPolicy>,
    ) -> Result<()> {
        debug!(
            "Processing presence member addition for user {} in channel {} (app: {})",
            user_id, channel, app_config.id
        );

        let lock_key = presence_lock_key(&app_config.id, channel, user_id);

        // FIX: Acquire per-user lock to prevent TOCTOU race between checking connection count
        // and sending member_added events. Without this lock:
        // - Thread A: checks count=0, about to send member_added
        // - Thread B: checks count=0, sends member_added (duplicate!)
        // - Thread A: sends member_added (duplicate!)
        //
        // With lock:
        // - Thread A: acquires lock, checks count=0, sends member_added, releases lock
        // - Thread B: acquires lock, checks count=1, skips member_added, releases lock
        let presence_lock = self.get_presence_lock(&lock_key);
        let _lock_guard = presence_lock.lock().await;

        // Check if user already had connections in this presence channel (excluding current socket)
        // This check is now protected by the lock
        let had_other_connections = Self::user_has_other_connections_in_presence_channel(
            Arc::clone(&connection_manager),
            &app_config.id,
            channel,
            user_id,
            excluding_socket,
        )
        .await?;

        if !had_other_connections {
            debug!(
                "User {} is joining channel {} for the first time, sending member_added events",
                user_id, channel
            );

            // Send member_added webhook
            if let Some(webhook_integration) = webhook_integration
                && let Err(e) = webhook_integration
                    .send_member_added(app_config, channel, user_id)
                    .await
            {
                warn!(
                    "Failed to send member_added webhook for user {} in channel {}: {}",
                    user_id, channel, e
                );
            }

            // Broadcast member_added event to existing clients in the channel
            let member_added_msg = sockudo_protocol::messages::PusherMessage::member_added(
                channel.to_string(),
                user_id.to_string(),
                user_info.cloned(),
            );
            Self::broadcast_to_channel(
                Arc::clone(&connection_manager),
                &app_config.id,
                channel,
                member_added_msg,
                excluding_socket,
            )
            .await?;

            let _ = Self::broadcast_to_channel(
                Arc::clone(&connection_manager),
                &app_config.id,
                &format!("[meta]{channel}"),
                sockudo_protocol::messages::PusherMessage {
                    event: Some("sockudo_internal:member_added".to_string()),
                    channel: Some(format!("[meta]{channel}")),
                    data: Some(sockudo_protocol::messages::MessageData::Json(
                        sonic_rs::json!({
                            "channel": channel,
                            "user_id": user_id,
                        }),
                    )),
                    name: None,
                    user_id: None,
                    tags: None,
                    sequence: None,
                    conflation_key: None,
                    message_id: None,
                    stream_id: None,
                    serial: None,
                    idempotency_key: None,
                    extras: None,
                    delta_sequence: None,
                    delta_conflation_key: None,
                },
                None,
            )
            .await;

            debug!(
                "Successfully processed member_added for user {} in channel {}",
                user_id, channel
            );

            if presence_history_enabled {
                self.record_presence_transition(
                    presence_history_store,
                    metrics,
                    PresenceHistoryTransitionRecord {
                        app_id: app_config.id.clone(),
                        channel: channel.to_string(),
                        event_kind: PresenceHistoryEventKind::MemberAdded,
                        cause: PresenceHistoryEventCause::Join,
                        user_id: user_id.to_string(),
                        connection_id: excluding_socket.map(ToString::to_string),
                        user_info: user_info.cloned(),
                        dead_node_id: None,
                        dedupe_key: format!(
                            "member_added:{}:{}:{}:{}",
                            app_config.id,
                            channel,
                            user_id,
                            excluding_socket
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "_".to_string())
                        ),
                        published_at_ms: sockudo_core::history::now_ms(),
                        retention: retention.clone().unwrap_or(
                            sockudo_core::presence_history::PresenceHistoryRetentionPolicy {
                                retention_window_seconds: 3600,
                                max_events_per_channel: None,
                                max_bytes_per_channel: None,
                            },
                        ),
                    },
                )
                .await;
            }
        } else {
            debug!(
                "User {} already has {} connections in channel {}, skipping member_added events",
                user_id,
                if had_other_connections { "other" } else { "no" },
                channel
            );
        }

        // Lock is released here when _lock_guard goes out of scope

        // Always broadcast presence join to all nodes for cluster replication (for every connection)
        // This is done OUTSIDE the lock to avoid holding it during network I/O
        if let Some(excluding_socket) = excluding_socket
            && let Some(horizontal_adapter) = connection_manager.as_horizontal_adapter()
        {
            horizontal_adapter
                .broadcast_presence_join(
                    &app_config.id,
                    channel,
                    user_id,
                    &excluding_socket.to_string(),
                    user_info.cloned(),
                )
                .await
                .map_err(|e| {
                    error!("Failed to broadcast presence join: {}", e);
                    e
                })
                .ok(); // Log but don't fail the operation
        }

        // Opportunistically cap lock-map growth on high-cardinality presence churn.
        self.cleanup_stale_locks();

        Ok(())
    }

    /// Handles presence member removal including both webhook and broadcast
    /// This centralizes the logic that was duplicated across sync cleanup,
    /// async cleanup, and direct unsubscribe paths
    ///
    /// FIX: Uses atomic check-and-act pattern to prevent TOCTOU race conditions
    pub async fn handle_member_removed(
        &self,
        connection_manager: &Arc<dyn ConnectionManager + Send + Sync>,
        presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
        presence_history_enabled: bool,
        webhook_integration: Option<&Arc<WebhookIntegration>>,
        metrics: Option<&Arc<dyn MetricsInterface + Send + Sync>>,
        app_config: &App,
        channel: &str,
        user_id: &str,
        excluding_socket: Option<&SocketId>,
        cause: PresenceHistoryEventCause,
        dead_node_id: Option<&str>,
        retention: Option<sockudo_core::presence_history::PresenceHistoryRetentionPolicy>,
    ) -> Result<()> {
        debug!(
            "Processing presence member removal for user {} in channel {} (app: {})",
            user_id, channel, app_config.id
        );

        let lock_key = presence_lock_key(&app_config.id, channel, user_id);

        // FIX: Acquire per-user lock to prevent TOCTOU race between checking connection count
        // and sending member_removed events. Without this lock:
        // - Thread A (disconnect socket 1): checks count=1, about to send member_removed
        // - Thread B (disconnect socket 2): checks count=1, sends member_removed
        // - Thread A: sends member_removed (duplicate! user still has socket 2... wait, no they don't)
        //
        // The real race is:
        // - Thread A (disconnect): checks count=1 (only this socket), prepares member_removed
        // - Thread B (connect): checks count=1 (sees socket being disconnected), skips member_added
        // - Thread A: sends member_removed
        // Result: User is connected but appears removed!
        //
        // With lock:
        // - Operations are serialized per user+channel, ensuring consistent state
        let presence_lock = self.get_presence_lock(&lock_key);
        let _lock_guard = presence_lock.lock().await;

        // Check if user has other connections in this presence channel
        // This check is now protected by the lock
        let has_other_connections = Self::user_has_other_connections_in_presence_channel(
            Arc::clone(connection_manager),
            &app_config.id,
            channel,
            user_id,
            excluding_socket,
        )
        .await?;

        if !has_other_connections {
            debug!(
                "User {} has no other connections in channel {}, sending member_removed events",
                user_id, channel
            );

            // Send member_removed webhook with 3-second delay
            // Per Pusher spec: delay prevents spurious webhooks from momentary disconnects.
            // If the user reconnects within the delay, no webhook is sent.
            if let Some(webhook_integration) = webhook_integration {
                let wi = Arc::clone(webhook_integration);
                let cm = Arc::clone(connection_manager);
                let app = app_config.clone();
                let ch = channel.to_string();
                let uid = user_id.to_string();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    let still_gone = match Self::user_has_other_connections_in_presence_channel(
                        cm, &app.id, &ch, &uid, None,
                    )
                    .await
                    {
                        Ok(has_connections) => !has_connections,
                        Err(_) => true,
                    };
                    if still_gone && let Err(e) = wi.send_member_removed(&app, &ch, &uid).await {
                        tracing::warn!(
                            "Failed to send member_removed webhook for user {} in channel {}: {}",
                            uid,
                            ch,
                            e
                        );
                    }
                });
            }

            // Broadcast member_removed event to remaining clients in the channel
            let member_removed_msg =
                PusherMessage::member_removed(channel.to_string(), user_id.to_string());
            Self::broadcast_to_channel(
                Arc::clone(connection_manager),
                &app_config.id,
                channel,
                member_removed_msg,
                excluding_socket,
            )
            .await?;

            let _ = Self::broadcast_to_channel(
                Arc::clone(connection_manager),
                &app_config.id,
                &format!("[meta]{channel}"),
                PusherMessage {
                    event: Some("sockudo_internal:member_removed".to_string()),
                    channel: Some(format!("[meta]{channel}")),
                    data: Some(sockudo_protocol::messages::MessageData::Json(
                        sonic_rs::json!({
                            "channel": channel,
                            "user_id": user_id,
                        }),
                    )),
                    name: None,
                    user_id: None,
                    tags: None,
                    sequence: None,
                    conflation_key: None,
                    message_id: None,
                    stream_id: None,
                    serial: None,
                    idempotency_key: None,
                    extras: None,
                    delta_sequence: None,
                    delta_conflation_key: None,
                },
                None,
            )
            .await;

            debug!(
                "Successfully processed member_removed for user {} in channel {}",
                user_id, channel
            );

            if presence_history_enabled {
                self.record_presence_transition(
                    presence_history_store,
                    metrics,
                    PresenceHistoryTransitionRecord {
                        app_id: app_config.id.clone(),
                        channel: channel.to_string(),
                        event_kind: PresenceHistoryEventKind::MemberRemoved,
                        cause,
                        user_id: user_id.to_string(),
                        connection_id: excluding_socket.map(ToString::to_string),
                        user_info: None,
                        dead_node_id: dead_node_id.map(ToString::to_string),
                        dedupe_key: format!(
                            "member_removed:{}:{}:{}:{}:{}",
                            app_config.id,
                            channel,
                            user_id,
                            excluding_socket
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "_".to_string()),
                            dead_node_id.unwrap_or("_")
                        ),
                        published_at_ms: sockudo_core::history::now_ms(),
                        retention: retention.clone().unwrap_or(
                            sockudo_core::presence_history::PresenceHistoryRetentionPolicy {
                                retention_window_seconds: 3600,
                                max_events_per_channel: None,
                                max_bytes_per_channel: None,
                            },
                        ),
                    },
                )
                .await;
            }
        } else {
            debug!(
                "User {} has other connections in channel {}, skipping member_removed events",
                user_id, channel
            );
        }

        // Lock is released here when _lock_guard goes out of scope

        // Always broadcast presence leave to all nodes for cluster replication (for every disconnection)
        // This is done OUTSIDE the lock to avoid holding it during network I/O
        if let Some(excluding_socket) = excluding_socket
            && let Some(horizontal_adapter) = connection_manager.as_horizontal_adapter()
        {
            horizontal_adapter
                .broadcast_presence_leave(
                    &app_config.id,
                    channel,
                    user_id,
                    &excluding_socket.to_string(),
                )
                .await
                .map_err(|e| {
                    error!("Failed to broadcast presence leave: {}", e);
                    e
                })
                .ok(); // Log but don't fail the operation
        }

        // Opportunistically cap lock-map growth on high-cardinality presence churn.
        self.cleanup_stale_locks();

        Ok(())
    }

    /// Cleanup stale lock entries periodically
    /// Call this from a background task to prevent unbounded growth of the locks map
    pub fn cleanup_stale_locks(&self) {
        if self.presence_locks.len() > 100_000 {
            let stale_keys: Vec<String> = self
                .presence_locks
                .iter()
                .filter_map(|entry| {
                    let lock = entry.value();
                    let in_use = Arc::strong_count(lock) > 1 || lock.try_lock().is_err();
                    if in_use {
                        None
                    } else {
                        Some(entry.key().clone())
                    }
                })
                .collect();

            warn!(
                "Presence locks map has {} entries, removing {} stale entries",
                self.presence_locks.len(),
                stale_keys.len()
            );

            for key in stale_keys {
                self.presence_locks.remove(&key);
            }
        }
    }

    /// Check if a user has other connections in a presence channel
    /// Uses the same logic as the original working implementation:
    /// Get all user's sockets and check if any are still subscribed to this channel (excluding specified socket)
    async fn user_has_other_connections_in_presence_channel(
        connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
        app_id: &str,
        channel: &str,
        user_id: &str,
        excluding_socket: Option<&SocketId>,
    ) -> Result<bool> {
        // Use cluster-wide connection check for multi-node support
        let subscribed_count = connection_manager
            .count_user_connections_in_channel(user_id, app_id, channel, excluding_socket)
            .await?;

        let has_other_connections = subscribed_count > 0;

        Ok(has_other_connections)
    }

    /// Broadcast a message to all clients in a channel, optionally excluding one socket
    async fn broadcast_to_channel(
        connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
        app_id: &str,
        channel: &str,
        message: PusherMessage,
        excluding_socket: Option<&SocketId>,
    ) -> Result<()> {
        connection_manager
            .send(channel, message, excluding_socket, app_id, None)
            .await
    }

    async fn record_presence_transition(
        &self,
        presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
        metrics: Option<&Arc<dyn MetricsInterface + Send + Sync>>,
        record: PresenceHistoryTransitionRecord,
    ) {
        if let Err(error) = presence_history_store
            .record_transition(record.clone())
            .await
        {
            warn!(
                "Failed to record presence history transition for user {} in channel {}: {}",
                record.user_id, record.channel, error
            );
            if let Some(metrics) = metrics {
                metrics.mark_presence_history_write_failure(&record.app_id);
            }
        }
    }
}

/// Static helper functions for backward compatibility with code that doesn't have
/// access to a PresenceManager instance. These use a global lock map.
///
/// NOTE: Prefer using the instance methods when possible for better testability
/// and to allow cleanup of stale locks.
mod static_helpers {
    use super::*;
    use std::sync::LazyLock;

    /// Global presence manager for static helper functions
    static GLOBAL_PRESENCE_MANAGER: LazyLock<PresenceManager> = LazyLock::new(PresenceManager::new);

    /// Get the global presence manager instance
    pub fn global() -> &'static PresenceManager {
        &GLOBAL_PRESENCE_MANAGER
    }
}

/// Re-export static helper access for backward compatibility
pub fn global_presence_manager() -> &'static PresenceManager {
    static_helpers::global()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::AHashMap;
    use async_trait::async_trait;
    use sockudo_core::app::{App, AppPolicy};
    use sockudo_core::channel::PresenceMemberInfo;
    use sockudo_core::namespace::Namespace;
    use sockudo_core::presence_history::{
        MemoryPresenceHistoryStore, PresenceHistoryDirection, PresenceHistoryEventCause,
        PresenceHistoryEventKind, PresenceHistoryReadRequest, PresenceHistoryRetentionPolicy,
    };
    use sockudo_protocol::WireFormat;
    use sockudo_ws::axum_integration::WebSocketWriter;
    use std::any::Any;
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;

    #[test]
    fn test_presence_lock_key_format() {
        let key = presence_lock_key("app123", "presence-room", "user456");
        assert_eq!(key, "app123:presence-room:user456");
    }

    #[test]
    fn test_presence_manager_creation() {
        let manager = PresenceManager::new();
        assert_eq!(manager.presence_locks.len(), 0);
    }

    #[test]
    fn test_cleanup_stale_locks_threshold() {
        let manager = PresenceManager::new();

        // Add entries below threshold
        for i in 0..100 {
            manager
                .presence_locks
                .insert(format!("key_{}", i), Arc::new(Mutex::new(())));
        }

        manager.cleanup_stale_locks();
        // Should not clear because below 100_000 threshold
        assert_eq!(manager.presence_locks.len(), 100);
    }

    struct ScriptedConnectionManager {
        counts: StdMutex<VecDeque<usize>>,
    }

    impl ScriptedConnectionManager {
        fn new(counts: Vec<usize>) -> Self {
            Self {
                counts: StdMutex::new(counts.into()),
            }
        }
    }

    #[async_trait]
    impl ConnectionManager for ScriptedConnectionManager {
        async fn init(&self) {}
        async fn get_namespace(&self, _app_id: &str) -> Option<Arc<Namespace>> {
            None
        }
        async fn add_socket(
            &self,
            _socket_id: SocketId,
            _socket: WebSocketWriter,
            _app_id: &str,
            _app_manager: Arc<dyn sockudo_core::app::AppManager + Send + Sync>,
            _buffer_config: sockudo_core::websocket::WebSocketBufferConfig,
            _protocol_version: sockudo_protocol::ProtocolVersion,
            _wire_format: WireFormat,
            _echo_messages: bool,
        ) -> Result<()> {
            Ok(())
        }
        async fn get_connection(
            &self,
            _socket_id: &SocketId,
            _app_id: &str,
        ) -> Option<sockudo_core::websocket::WebSocketRef> {
            None
        }
        async fn remove_connection(&self, _socket_id: &SocketId, _app_id: &str) -> Result<()> {
            Ok(())
        }
        async fn send_message(
            &self,
            _app_id: &str,
            _socket_id: &SocketId,
            _message: PusherMessage,
        ) -> Result<()> {
            Ok(())
        }
        async fn send(
            &self,
            _channel: &str,
            _message: PusherMessage,
            _except: Option<&SocketId>,
            _app_id: &str,
            _start_time_ms: Option<f64>,
        ) -> Result<()> {
            Ok(())
        }
        async fn get_channel_members(
            &self,
            _app_id: &str,
            _channel: &str,
        ) -> Result<AHashMap<String, PresenceMemberInfo>> {
            Ok(AHashMap::new())
        }
        async fn get_channel_sockets(
            &self,
            _app_id: &str,
            _channel: &str,
        ) -> Result<Vec<SocketId>> {
            Ok(Vec::new())
        }
        async fn remove_channel(&self, _app_id: &str, _channel: &str) {}
        async fn is_in_channel(
            &self,
            _app_id: &str,
            _channel: &str,
            _socket_id: &SocketId,
        ) -> Result<bool> {
            Ok(false)
        }
        async fn get_user_sockets(
            &self,
            _user_id: &str,
            _app_id: &str,
        ) -> Result<Vec<sockudo_core::websocket::WebSocketRef>> {
            Ok(Vec::new())
        }
        async fn cleanup_connection(
            &self,
            _app_id: &str,
            _ws: sockudo_core::websocket::WebSocketRef,
        ) {
        }
        async fn terminate_connection(&self, _app_id: &str, _user_id: &str) -> Result<()> {
            Ok(())
        }
        async fn add_channel_to_sockets(
            &self,
            _app_id: &str,
            _channel: &str,
            _socket_id: &SocketId,
        ) {
        }
        async fn get_channel_socket_count_info(
            &self,
            _app_id: &str,
            _channel: &str,
        ) -> crate::connection_manager::ChannelSocketCount {
            crate::connection_manager::ChannelSocketCount {
                count: 0,
                complete: true,
            }
        }
        async fn get_channel_socket_count(&self, _app_id: &str, _channel: &str) -> usize {
            0
        }
        async fn add_to_channel(
            &self,
            _app_id: &str,
            _channel: &str,
            _socket_id: &SocketId,
        ) -> Result<bool> {
            Ok(false)
        }
        async fn remove_from_channel(
            &self,
            _app_id: &str,
            _channel: &str,
            _socket_id: &SocketId,
        ) -> Result<bool> {
            Ok(false)
        }
        async fn get_presence_member(
            &self,
            _app_id: &str,
            _channel: &str,
            _socket_id: &SocketId,
        ) -> Option<PresenceMemberInfo> {
            None
        }
        async fn terminate_user_connections(&self, _app_id: &str, _user_id: &str) -> Result<()> {
            Ok(())
        }
        async fn add_user(&self, _ws: sockudo_core::websocket::WebSocketRef) -> Result<()> {
            Ok(())
        }
        async fn remove_user(&self, _ws: sockudo_core::websocket::WebSocketRef) -> Result<()> {
            Ok(())
        }
        async fn remove_user_socket(
            &self,
            _user_id: &str,
            _socket_id: &SocketId,
            _app_id: &str,
        ) -> Result<()> {
            Ok(())
        }
        async fn count_user_connections_in_channel(
            &self,
            _user_id: &str,
            _app_id: &str,
            _channel: &str,
            _excluding_socket: Option<&SocketId>,
        ) -> Result<usize> {
            Ok(self.counts.lock().unwrap().pop_front().unwrap_or_default())
        }
        async fn get_channels_with_socket_count(
            &self,
            _app_id: &str,
        ) -> Result<AHashMap<String, usize>> {
            Ok(AHashMap::new())
        }
        async fn get_sockets_count(&self, _app_id: &str) -> Result<usize> {
            Ok(0)
        }
        async fn get_namespaces(&self) -> Result<Vec<(String, Arc<Namespace>)>> {
            Ok(Vec::new())
        }
        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
        async fn check_health(&self) -> Result<()> {
            Ok(())
        }
        fn get_node_id(&self) -> String {
            "test-node".to_string()
        }
        fn as_horizontal_adapter(
            &self,
        ) -> Option<&dyn crate::connection_manager::HorizontalAdapterInterface> {
            None
        }
    }

    fn test_app() -> App {
        App::from_policy(
            "app-1".to_string(),
            "key".to_string(),
            "secret".to_string(),
            true,
            AppPolicy::default(),
        )
    }

    fn default_retention() -> PresenceHistoryRetentionPolicy {
        PresenceHistoryRetentionPolicy {
            retention_window_seconds: 3600,
            max_events_per_channel: None,
            max_bytes_per_channel: None,
        }
    }

    #[tokio::test]
    async fn presence_manager_records_authoritative_join_and_leave_history() {
        let manager = PresenceManager::new();
        let connection_manager: Arc<dyn ConnectionManager + Send + Sync> =
            Arc::new(ScriptedConnectionManager::new(vec![0, 0]));
        let store = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        let app = test_app();
        let socket = SocketId::new();

        manager
            .handle_member_added(
                Arc::clone(&connection_manager),
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                None,
                Some(&socket),
                Some(default_retention()),
            )
            .await
            .unwrap();

        manager
            .handle_member_removed(
                &connection_manager,
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                Some(&socket),
                PresenceHistoryEventCause::Disconnect,
                None,
                Some(default_retention()),
            )
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: app.id.clone(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: Default::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
        assert_eq!(page.items[1].event, PresenceHistoryEventKind::MemberRemoved);
        assert_eq!(page.items[1].cause, PresenceHistoryEventCause::Disconnect);
    }

    #[tokio::test]
    async fn presence_manager_skips_non_authoritative_socket_churn_history() {
        let manager = PresenceManager::new();
        let connection_manager: Arc<dyn ConnectionManager + Send + Sync> =
            Arc::new(ScriptedConnectionManager::new(vec![0, 1, 1, 0]));
        let store = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        let app = test_app();
        let socket_a = SocketId::new();
        let socket_b = SocketId::new();

        manager
            .handle_member_added(
                Arc::clone(&connection_manager),
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                None,
                Some(&socket_a),
                Some(default_retention()),
            )
            .await
            .unwrap();

        manager
            .handle_member_added(
                Arc::clone(&connection_manager),
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                None,
                Some(&socket_b),
                Some(default_retention()),
            )
            .await
            .unwrap();

        manager
            .handle_member_removed(
                &connection_manager,
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                Some(&socket_a),
                PresenceHistoryEventCause::Disconnect,
                None,
                Some(default_retention()),
            )
            .await
            .unwrap();

        manager
            .handle_member_removed(
                &connection_manager,
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                Some(&socket_b),
                PresenceHistoryEventCause::Disconnect,
                None,
                Some(default_retention()),
            )
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: app.id.clone(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: Default::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
        assert_eq!(page.items[1].event, PresenceHistoryEventKind::MemberRemoved);
    }

    #[tokio::test]
    async fn presence_manager_suppresses_duplicate_authoritative_join_reports() {
        let manager = PresenceManager::new();
        let connection_manager: Arc<dyn ConnectionManager + Send + Sync> =
            Arc::new(ScriptedConnectionManager::new(vec![0, 0]));
        let store = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        let app = test_app();
        let socket_a = SocketId::new();
        let socket_b = SocketId::new();

        manager
            .handle_member_added(
                Arc::clone(&connection_manager),
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                None,
                Some(&socket_a),
                Some(default_retention()),
            )
            .await
            .unwrap();

        manager
            .handle_member_added(
                Arc::clone(&connection_manager),
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                None,
                Some(&socket_b),
                Some(default_retention()),
            )
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: app.id.clone(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: Default::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
    }

    #[tokio::test]
    async fn presence_manager_suppresses_duplicate_authoritative_remove_reports() {
        let manager = PresenceManager::new();
        let connection_manager: Arc<dyn ConnectionManager + Send + Sync> =
            Arc::new(ScriptedConnectionManager::new(vec![0, 0, 0]));
        let store = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        let app = test_app();
        let socket = SocketId::new();

        manager
            .handle_member_added(
                Arc::clone(&connection_manager),
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                None,
                Some(&socket),
                Some(default_retention()),
            )
            .await
            .unwrap();

        manager
            .handle_member_removed(
                &connection_manager,
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                Some(&socket),
                PresenceHistoryEventCause::Disconnect,
                None,
                Some(default_retention()),
            )
            .await
            .unwrap();

        manager
            .handle_member_removed(
                &connection_manager,
                store.clone(),
                true,
                None,
                None,
                &app,
                "presence-room",
                "user-1",
                None,
                PresenceHistoryEventCause::OrphanCleanup,
                Some("dead-node"),
                Some(default_retention()),
            )
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: app.id.clone(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: Default::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
        assert_eq!(page.items[1].event, PresenceHistoryEventKind::MemberRemoved);
        assert_eq!(page.items[1].cause, PresenceHistoryEventCause::Disconnect);
    }
}
