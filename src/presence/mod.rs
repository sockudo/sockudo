use crate::adapter::connection_manager::ConnectionManager;
use crate::app::config::App;
use crate::error::Result;
use crate::protocol::messages::PusherMessage;
use crate::webhook::integration::WebhookIntegration;
use crate::websocket::SocketId;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::debug;

/// Centralized presence channel management functionality
/// This module handles presence member removal logic that needs to be
/// consistent across different disconnect paths (sync, async, direct unsubscribe)
pub struct PresenceManager;

impl PresenceManager {
    /// Handles presence member removal including both webhook and broadcast
    /// This centralizes the logic that was duplicated across sync cleanup,
    /// async cleanup, and direct unsubscribe paths
    pub async fn handle_member_removed(
        connection_manager: &Arc<Mutex<dyn ConnectionManager + Send + Sync>>,
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

        // Check if user has other connections in this presence channel
        let has_other_connections = Self::user_has_other_connections_in_presence_channel(
            connection_manager,
            &app_config.id,
            channel,
            user_id,
        )
        .await?;

        if !has_other_connections {
            debug!(
                "User {} has no other connections in channel {}, sending member_removed events",
                user_id, channel
            );

            // Send member_removed webhook
            if let Some(webhook_integration) = webhook_integration {
                webhook_integration
                    .send_member_removed(app_config, channel, user_id)
                    .await
                    .ok(); // Don't fail the entire operation if webhook fails
            }

            // Broadcast member_removed event to remaining clients in the channel
            let member_removed_msg = PusherMessage::member_removed(channel.to_string(), user_id.to_string());
            Self::broadcast_to_channel(
                connection_manager,
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

        Ok(())
    }

    /// Check if a user has other connections in a presence channel
    /// Uses the same logic as the original working implementation:
    /// Get all user's sockets and check if any are still subscribed to this channel
    async fn user_has_other_connections_in_presence_channel(
        connection_manager: &Arc<Mutex<dyn ConnectionManager + Send + Sync>>,
        app_id: &str,
        channel: &str,
        user_id: &str,
    ) -> Result<bool> {
        let mut connection_manager = connection_manager.lock().await;
        
        // Get all sockets for this user across the app
        let user_sockets = connection_manager.get_user_sockets(user_id, app_id).await?;
        
        // Check if any of the user's sockets are still subscribed to this channel
        for ws_ref in user_sockets.iter() {
            let socket_state_guard = ws_ref.0.lock().await;
            if socket_state_guard.state.is_subscribed(channel) {
                return Ok(true); // User has other connections to this channel
            }
        }
        
        Ok(false) // User has no other connections to this channel
    }

    /// Broadcast a message to all clients in a channel, optionally excluding one socket
    async fn broadcast_to_channel(
        connection_manager: &Arc<Mutex<dyn ConnectionManager + Send + Sync>>,
        app_id: &str,
        channel: &str,
        message: PusherMessage,
        excluding_socket: Option<&SocketId>,
    ) -> Result<()> {
        let mut connection_manager = connection_manager.lock().await;
        connection_manager
            .send(channel, message, excluding_socket, app_id, None)
            .await
    }
}