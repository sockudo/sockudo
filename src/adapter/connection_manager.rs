use crate::app::manager::AppManager;
use crate::channel::PresenceMemberInfo;
use crate::error::Result;
use crate::namespace::Namespace;
use crate::protocol::messages::PusherMessage;
use crate::websocket::{SocketId, WebSocketBufferConfig, WebSocketRef};
use ahash::AHashMap as HashMap;
use async_trait::async_trait;
use sockudo_ws::axum_integration::WebSocketWriter;
use std::any::Any;
use std::sync::Arc;

/// Parameters for delta compression when sending messages
pub struct CompressionParams<'a> {
    pub delta_compression: Arc<crate::delta_compression::DeltaCompressionManager>,
    pub channel_settings: Option<&'a crate::delta_compression::ChannelDeltaSettings>,
}

/// Interface for horizontal adapters that support cluster presence replication
#[async_trait]
pub trait HorizontalAdapterInterface: Send + Sync {
    /// Broadcast presence member joined to all nodes
    async fn broadcast_presence_join(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
        socket_id: &str,
        user_info: Option<sonic_rs::Value>,
    ) -> Result<()>;

    /// Broadcast presence member left to all nodes
    async fn broadcast_presence_leave(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
        socket_id: &str,
    ) -> Result<()>;
}

#[async_trait]
pub trait ConnectionManager: Send + Sync {
    async fn init(&self);
    // Namespace management
    async fn get_namespace(&self, app_id: &str) -> Option<Arc<Namespace>>;

    // WebSocket management
    async fn add_socket(
        &self,
        socket_id: SocketId,
        socket: WebSocketWriter,
        app_id: &str,
        app_manager: Arc<dyn AppManager + Send + Sync>,
        buffer_config: WebSocketBufferConfig,
    ) -> Result<()>;

    async fn get_connection(&self, socket_id: &SocketId, app_id: &str) -> Option<WebSocketRef>;

    async fn remove_connection(&self, socket_id: &SocketId, app_id: &str) -> Result<()>;

    // Message handling
    async fn send_message(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        message: PusherMessage,
    ) -> Result<()>;

    async fn send(
        &self,
        channel: &str,
        message: PusherMessage,
        except: Option<&SocketId>,
        app_id: &str,
        start_time_ms: Option<f64>,
    ) -> Result<()>;

    async fn send_with_compression(
        &self,
        channel: &str,
        message: PusherMessage,
        except: Option<&SocketId>,
        app_id: &str,
        start_time_ms: Option<f64>,
        compression: CompressionParams<'_>,
    ) -> Result<()> {
        // Default implementation: call send without compression
        // Suppress unused variable warnings for default trait implementation
        let _ = compression;
        self.send(channel, message, except, app_id, start_time_ms)
            .await
    }
    async fn get_channel_members(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>>;
    async fn get_channel_sockets(&self, app_id: &str, channel: &str) -> Result<Vec<SocketId>>;
    async fn remove_channel(&self, app_id: &str, channel: &str);
    async fn is_in_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool>;
    async fn get_user_sockets(&self, user_id: &str, app_id: &str) -> Result<Vec<WebSocketRef>>;
    async fn cleanup_connection(&self, app_id: &str, ws: WebSocketRef);
    async fn terminate_connection(&self, app_id: &str, user_id: &str) -> Result<()>;
    async fn add_channel_to_sockets(&self, app_id: &str, channel: &str, socket_id: &SocketId);
    async fn get_channel_socket_count(&self, app_id: &str, channel: &str) -> usize;
    async fn add_to_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool>;
    async fn remove_from_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool>;
    async fn get_presence_member(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<PresenceMemberInfo>;
    async fn terminate_user_connections(&self, app_id: &str, user_id: &str) -> Result<()>;
    async fn add_user(&self, ws: WebSocketRef) -> Result<()>;
    async fn remove_user(&self, ws: WebSocketRef) -> Result<()>;
    async fn remove_user_socket(
        &self,
        user_id: &str,
        socket_id: &SocketId,
        app_id: &str,
    ) -> Result<()>;
    async fn count_user_connections_in_channel(
        &self,
        user_id: &str,
        app_id: &str,
        channel: &str,
        excluding_socket: Option<&SocketId>,
    ) -> Result<usize>;
    async fn get_channels_with_socket_count(&self, app_id: &str) -> Result<HashMap<String, usize>>;

    async fn get_sockets_count(&self, app_id: &str) -> Result<usize>;
    async fn get_namespaces(&self) -> Result<Vec<(String, Arc<Namespace>)>>;
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Check the health of the connection manager and its underlying adapter
    /// Returns Ok(()) if healthy, Err(error_message) if unhealthy with specific reason
    async fn check_health(&self) -> Result<()>;

    /// Get the unique node ID for this instance
    fn get_node_id(&self) -> String;

    /// Get reference to horizontal adapter if available (for cluster presence replication)
    fn as_horizontal_adapter(&self) -> Option<&dyn HorizontalAdapterInterface>;

    /// Configure dead node event bus if this adapter supports clustering
    /// Returns Some(receiver) if configured, None if not supported
    fn configure_dead_node_events(
        &self,
    ) -> Option<
        tokio::sync::mpsc::UnboundedReceiver<crate::adapter::horizontal_adapter::DeadNodeEvent>,
    > {
        None // Default: no clustering support
    }
}
