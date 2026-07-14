use ahash::AHashMap as HashMap;
use async_trait::async_trait;
use crossfire::mpsc;
use sockudo_core::app::AppManager;
use sockudo_core::channel::PresenceMemberInfo;
use sockudo_core::error::Result;
use sockudo_core::message_envelope::MessageEnvelope;
use sockudo_core::namespace::Namespace;
use sockudo_core::presence_registry::PresenceReplication;
use sockudo_core::websocket::{SocketId, WebSocketBufferConfig, WebSocketRef};
use sockudo_protocol::messages::PusherMessage;
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::WebSocketWriter;
use std::any::Any;
use std::sync::Arc;

pub type DeadNodeEventBusFlavor = mpsc::List<crate::horizontal_adapter::DeadNodeEvent>;
pub type DeadNodeEventBusSender = crossfire::MTx<DeadNodeEventBusFlavor>;
pub type DeadNodeEventBusReceiver = crossfire::AsyncRx<DeadNodeEventBusFlavor>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelSocketCount {
    pub count: usize,
    pub complete: bool,
}

#[cfg(feature = "delta")]
/// Parameters for delta compression when sending messages
pub struct CompressionParams<'a> {
    pub delta_compression: Arc<sockudo_delta::DeltaCompressionManager>,
    pub channel_settings: Option<&'a sockudo_delta::ChannelDeltaSettings>,
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

    /// Broadcast presence member data update to all nodes.
    async fn broadcast_presence_update(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
        socket_id: &str,
        user_info: sonic_rs::Value,
    ) -> Result<()>;
}

#[async_trait]
pub trait ConnectionManager: Send + Sync {
    async fn init(&self);
    // Namespace management
    async fn get_namespace(&self, app_id: &str) -> Option<Arc<Namespace>>;

    // WebSocket management
    #[allow(clippy::too_many_arguments)]
    async fn add_socket(
        &self,
        socket_id: SocketId,
        socket: WebSocketWriter,
        app_id: &str,
        app_manager: Arc<dyn AppManager + Send + Sync>,
        buffer_config: WebSocketBufferConfig,
        protocol_version: ProtocolVersion,
        wire_format: WireFormat,
        echo_messages: bool,
        append_mode: sockudo_protocol::AppendMode,
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

    /// Send a message while retaining protocol-neutral commit facts for
    /// compatibility projections on remote nodes.
    async fn send_with_envelope(
        &self,
        channel: &str,
        message: PusherMessage,
        except: Option<&SocketId>,
        app_id: &str,
        start_time_ms: Option<f64>,
        envelope: Option<MessageEnvelope>,
    ) -> Result<()> {
        let _ = envelope;
        self.send(channel, message, except, app_id, start_time_ms)
            .await
    }

    /// Replicate protocol-neutral presence transitions to remote nodes without
    /// exposing an internal event to Protocol V1 subscribers.
    async fn send_presence_replication(
        &self,
        _app_id: &str,
        _channel: &str,
        _replication: PresenceReplication,
    ) -> Result<()> {
        Ok(())
    }

    /// Return protocol-neutral presence records retained by the horizontal
    /// presence registry, including state synced to a newly joined node.
    async fn replicated_presence_records(
        &self,
        _app_id: &str,
        _channel: &str,
    ) -> Result<Vec<sockudo_core::presence_registry::PresenceRecord>> {
        Ok(Vec::new())
    }

    /// Install the optional compatibility delivery observer. Horizontal
    /// adapters retain it for remote broadcast delivery; local adapters ignore it.
    fn set_realtime_egress_tap(&self, _tap: Arc<dyn crate::handler::RealtimeEgressTap>) {}

    #[cfg(feature = "delta")]
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

    async fn get_local_channel_members(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        self.get_channel_members(app_id, channel).await
    }

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
    async fn get_channel_socket_count_info(
        &self,
        app_id: &str,
        channel: &str,
    ) -> ChannelSocketCount;
    async fn get_channel_socket_count(&self, app_id: &str, channel: &str) -> usize;
    async fn add_to_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<(bool, bool)>;
    async fn add_to_channel_and_count_local(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<(bool, bool, usize)> {
        let (newly_inserted, activated) = self.add_to_channel(app_id, channel, socket_id).await?;
        let count = self.get_local_channel_socket_count(app_id, channel).await;
        Ok((newly_inserted, activated, count))
    }
    async fn remove_from_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<(bool, bool)>;
    async fn remove_from_channel_and_count_local(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<(bool, bool, usize)> {
        let (was_removed, vacated) = self.remove_from_channel(app_id, channel, socket_id).await?;
        let count = self.get_local_channel_socket_count(app_id, channel).await;
        Ok((was_removed, vacated, count))
    }
    async fn get_presence_member(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<PresenceMemberInfo>;
    async fn update_presence_member(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
        user_info: sonic_rs::Value,
    ) -> Result<Option<PresenceMemberInfo>> {
        let _ = (app_id, channel, socket_id, user_info);
        Ok(None)
    }
    async fn mark_presence_member_pending(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
        socket_id: &str,
        user_info: Option<sonic_rs::Value>,
        generation: u64,
    ) -> Result<()> {
        let _ = (app_id, channel, user_id, socket_id, user_info, generation);
        Ok(())
    }
    async fn cancel_pending_presence_member(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
    ) -> Result<Option<String>> {
        let _ = (app_id, channel, user_id);
        Ok(None)
    }
    async fn remove_pending_presence_member(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
        generation: u64,
    ) -> Result<Option<PresenceMemberInfo>> {
        let _ = (app_id, channel, user_id, generation);
        Ok(None)
    }
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

    /// Returns true if the user has any connections in the channel (excluding one socket).
    async fn user_has_connections_in_channel(
        &self,
        user_id: &str,
        app_id: &str,
        channel: &str,
        excluding_socket: Option<&SocketId>,
    ) -> Result<bool> {
        Ok(self
            .count_user_connections_in_channel(user_id, app_id, channel, excluding_socket)
            .await?
            > 0)
    }

    async fn get_channels_with_socket_count(&self, app_id: &str) -> Result<HashMap<String, usize>>;

    async fn get_sockets_count(&self, app_id: &str) -> Result<usize>;
    async fn get_all_connections(&self, app_id: &str) -> Result<Vec<SocketId>> {
        let _ = app_id;
        Ok(Vec::new())
    }
    async fn get_namespaces(&self) -> Result<Vec<(String, Arc<Namespace>)>>;
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Local-only socket count (no cross-node query)
    async fn get_local_channel_socket_count(&self, app_id: &str, channel: &str) -> usize {
        self.get_channel_socket_count(app_id, channel).await
    }

    /// Batch cross-node channel socket counts in one round-trip
    async fn get_batch_channel_socket_counts(
        &self,
        app_id: &str,
        channels: &[&str],
    ) -> Result<HashMap<String, usize>> {
        let mut result = HashMap::new();
        for channel in channels {
            let count = self.get_channel_socket_count(app_id, channel).await;
            if count > 0 {
                result.insert(channel.to_string(), count);
            }
        }
        Ok(result)
    }

    /// Check the health of the connection manager and its underlying adapter
    /// Returns Ok(()) if healthy, Err(error_message) if unhealthy with specific reason
    async fn check_health(&self) -> Result<()>;

    /// Get the unique node ID for this instance
    fn get_node_id(&self) -> String;

    /// Get reference to horizontal adapter if available (for cluster presence replication)
    fn as_horizontal_adapter(&self) -> Option<&dyn HorizontalAdapterInterface>;

    /// Configure dead node event bus if this adapter supports clustering
    /// Returns Some(receiver) if configured, None if not supported
    fn configure_dead_node_events(&self) -> Option<DeadNodeEventBusReceiver> {
        None // Default: no clustering support
    }

    /// Tell cluster peers this node is leaving so they prune
    /// heartbeat tracking without waiting for the timeout window
    async fn announce_node_departure(&self) -> Result<()> {
        Ok(())
    }
}
