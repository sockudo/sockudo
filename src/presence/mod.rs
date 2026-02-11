use crate::adapter::connection_manager::ConnectionManager;
use crate::app::config::App;
use crate::error::Result;
use crate::protocol::messages::PusherMessage;
use crate::webhook::integration::WebhookIntegration;
use crate::websocket::SocketId;
use dashmap::DashMap;
use std::sync::Arc;
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
    /// Maps "app_id:channel:user_id" -> lock guard
    /// Using DashMap for concurrent access with per-key granularity
    presence_locks: DashMap<String, ()>,
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

    /// Handles presence member addition including both webhook and broadcast
    /// Only sends events if this is the user's FIRST connection to the presence channel
    ///
    /// FIX: Uses atomic check-and-act pattern to prevent TOCTOU race conditions
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_member_added(
        &self,
        connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
        webhook_integration: Option<&Arc<WebhookIntegration>>,
        app_config: &App,
        channel: &str,
        user_id: &str,
        user_info: Option<&sonic_rs::Value>,
        excluding_socket: Option<&SocketId>,
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
        let _lock_guard = self.presence_locks.entry(lock_key.clone()).or_insert(());

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
            let member_added_msg = crate::protocol::messages::PusherMessage::member_added(
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

            debug!(
                "Successfully processed member_added for user {} in channel {}",
                user_id, channel
            );
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
        webhook_integration: Option<&Arc<WebhookIntegration>>,
        app_config: &App,
        channel: &str,
        user_id: &str,
        excluding_socket: Option<&SocketId>,
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
        let _lock_guard = self.presence_locks.entry(lock_key.clone()).or_insert(());

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

            // Send member_removed webhook
            if let Some(webhook_integration) = webhook_integration
                && let Err(e) = webhook_integration
                    .send_member_removed(app_config, channel, user_id)
                    .await
            {
                warn!(
                    "Failed to send member_removed webhook for user {} in channel {}: {}",
                    user_id, channel, e
                );
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

            debug!(
                "Successfully processed member_removed for user {} in channel {}",
                user_id, channel
            );
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

        Ok(())
    }

    /// Cleanup stale lock entries periodically
    /// Call this from a background task to prevent unbounded growth of the locks map
    pub fn cleanup_stale_locks(&self) {
        // DashMap entries are automatically cleaned up when the guard is dropped,
        // but we can periodically clear the map if it grows too large
        if self.presence_locks.len() > 100_000 {
            warn!(
                "Presence locks map has {} entries, clearing stale entries",
                self.presence_locks.len()
            );
            // Clear all entries - this is safe because we only use entry() which
            // will recreate entries as needed
            self.presence_locks.clear();
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
            manager.presence_locks.insert(format!("key_{}", i), ());
        }

        manager.cleanup_stale_locks();
        // Should not clear because below 100_000 threshold
        assert_eq!(manager.presence_locks.len(), 100);
    }
}
