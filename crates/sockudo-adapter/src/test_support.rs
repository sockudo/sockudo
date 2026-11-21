//! Shared test infrastructure for `ConnectionManager` mocks.
//!
//! Every test adapter in this crate overrides 1-2 methods and no-ops the rest.
//! [`NoopConnectionManager`](crate::test_support::NoopConnectionManager) provides default no-op
//! implementations for all required `ConnectionManager` methods, and
//! [`delegate_connection_manager!`] generates the `ConnectionManager` impl by delegating to the
//! trait.
//!
//! When `ConnectionManager` gains a new method:
//! 1. Add a default no-op to `NoopConnectionManager`.
//! 2. Add the delegation line in `delegate_connection_manager!`.
//! All existing test adapters compile unchanged.

use ahash::AHashMap as HashMap;
use async_trait::async_trait;
use sockudo_core::app::AppManager;
use sockudo_core::channel::PresenceMemberInfo;
use sockudo_core::error::Result;
use sockudo_core::namespace::Namespace;
use sockudo_core::websocket::{SocketId, WebSocketBufferConfig, WebSocketRef};
use sockudo_protocol::messages::PusherMessage;
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::axum_integration::WebSocketWriter;
use std::any::Any;
use std::sync::Arc;

use crate::connection_manager::{ChannelSocketCount, HorizontalAdapterInterface};

/// Trait mirroring every required `ConnectionManager` method with no-op defaults.
/// Test structs implement this and override only what they need.
#[async_trait]
pub trait NoopConnectionManager: Send + Sync {
    async fn init(&self) {}
    async fn get_namespace(&self, _app_id: &str) -> Option<Arc<Namespace>> {
        None
    }
    #[allow(clippy::too_many_arguments)]
    async fn add_socket(
        &self,
        _socket_id: SocketId,
        _socket: WebSocketWriter,
        _app_id: &str,
        _app_manager: Arc<dyn AppManager + Send + Sync>,
        _buffer_config: WebSocketBufferConfig,
        _protocol_version: ProtocolVersion,
        _wire_format: WireFormat,
        _echo_messages: bool,
        _append_mode: sockudo_protocol::AppendMode,
    ) -> Result<()> {
        Ok(())
    }
    async fn get_connection(&self, _socket_id: &SocketId, _app_id: &str) -> Option<WebSocketRef> {
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
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        Ok(HashMap::new())
    }
    async fn get_channel_sockets(&self, _app_id: &str, _channel: &str) -> Result<Vec<SocketId>> {
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
    async fn get_user_sockets(&self, _user_id: &str, _app_id: &str) -> Result<Vec<WebSocketRef>> {
        Ok(Vec::new())
    }
    async fn cleanup_connection(&self, _app_id: &str, _ws: WebSocketRef) {}
    async fn terminate_connection(&self, _app_id: &str, _user_id: &str) -> Result<()> {
        Ok(())
    }
    async fn add_channel_to_sockets(&self, _app_id: &str, _channel: &str, _socket_id: &SocketId) {}
    async fn get_channel_socket_count_info(
        &self,
        _app_id: &str,
        _channel: &str,
    ) -> ChannelSocketCount {
        ChannelSocketCount {
            count: 0,
            complete: true,
        }
    }
    async fn get_channel_socket_count(&self, _app_id: &str, _channel: &str) -> usize {
        0
    }
    async fn get_local_channel_socket_count(&self, _app_id: &str, _channel: &str) -> usize {
        0
    }
    async fn add_to_channel(
        &self,
        _app_id: &str,
        _channel: &str,
        _socket_id: &SocketId,
    ) -> Result<(bool, bool)> {
        Ok((false, false))
    }
    async fn remove_from_channel(
        &self,
        _app_id: &str,
        _channel: &str,
        _socket_id: &SocketId,
    ) -> Result<(bool, bool)> {
        Ok((false, false))
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
    async fn force_reconnect_user(&self, _app_id: &str, _user_id: &str) -> Result<()> {
        Ok(())
    }
    async fn add_user(&self, _ws: WebSocketRef) -> Result<()> {
        Ok(())
    }
    async fn remove_user(&self, _ws: WebSocketRef) -> Result<()> {
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
        Ok(0)
    }
    async fn get_channels_with_socket_count(
        &self,
        _app_id: &str,
    ) -> Result<HashMap<String, usize>> {
        Ok(HashMap::new())
    }
    async fn get_sockets_count(&self, _app_id: &str) -> Result<usize> {
        Ok(0)
    }
    async fn get_namespaces(&self) -> Result<Vec<(String, Arc<Namespace>)>> {
        Ok(Vec::new())
    }
    fn as_any_mut(&mut self) -> &mut dyn Any;
    async fn check_health(&self) -> Result<()> {
        Ok(())
    }
    fn get_node_id(&self) -> String {
        "test-node".to_string()
    }
    fn as_horizontal_adapter(&self) -> Option<&dyn HorizontalAdapterInterface> {
        None
    }
}

/// Generates `impl ConnectionManager for $T` by delegating every method to
/// the struct's [`NoopConnectionManager`] impl. Test structs override only
/// the methods they need on `NoopConnectionManager`; everything else gets
/// the no-op default.
#[macro_export]
macro_rules! delegate_connection_manager {
    ($T:ty) => {
        #[async_trait::async_trait]
        impl $crate::ConnectionManager for $T {
            async fn init(&self) {
                $crate::test_support::NoopConnectionManager::init(self).await
            }
            async fn get_namespace(
                &self,
                app_id: &str,
            ) -> Option<std::sync::Arc<sockudo_core::namespace::Namespace>> {
                $crate::test_support::NoopConnectionManager::get_namespace(self, app_id).await
            }
            async fn add_socket(
                &self,
                socket_id: sockudo_core::websocket::SocketId,
                socket: sockudo_ws::axum_integration::WebSocketWriter,
                app_id: &str,
                app_manager: std::sync::Arc<dyn sockudo_core::app::AppManager + Send + Sync>,
                buffer_config: sockudo_core::websocket::WebSocketBufferConfig,
                protocol_version: sockudo_protocol::ProtocolVersion,
                wire_format: sockudo_protocol::WireFormat,
                echo_messages: bool,
                append_mode: sockudo_protocol::AppendMode,
            ) -> sockudo_core::error::Result<()> {
                $crate::test_support::NoopConnectionManager::add_socket(
                    self,
                    socket_id,
                    socket,
                    app_id,
                    app_manager,
                    buffer_config,
                    protocol_version,
                    wire_format,
                    echo_messages,
                    append_mode,
                )
                .await
            }
            async fn get_connection(
                &self,
                socket_id: &sockudo_core::websocket::SocketId,
                app_id: &str,
            ) -> Option<sockudo_core::websocket::WebSocketRef> {
                $crate::test_support::NoopConnectionManager::get_connection(self, socket_id, app_id)
                    .await
            }
            async fn remove_connection(
                &self,
                socket_id: &sockudo_core::websocket::SocketId,
                app_id: &str,
            ) -> sockudo_core::error::Result<()> {
                $crate::test_support::NoopConnectionManager::remove_connection(
                    self, socket_id, app_id,
                )
                .await
            }
            async fn send_message(
                &self,
                app_id: &str,
                socket_id: &sockudo_core::websocket::SocketId,
                message: sockudo_protocol::messages::PusherMessage,
            ) -> sockudo_core::error::Result<()> {
                $crate::test_support::NoopConnectionManager::send_message(
                    self, app_id, socket_id, message,
                )
                .await
            }
            async fn send(
                &self,
                channel: &str,
                message: sockudo_protocol::messages::PusherMessage,
                except: Option<&sockudo_core::websocket::SocketId>,
                app_id: &str,
                start_time_ms: Option<f64>,
            ) -> sockudo_core::error::Result<()> {
                $crate::test_support::NoopConnectionManager::send(
                    self,
                    channel,
                    message,
                    except,
                    app_id,
                    start_time_ms,
                )
                .await
            }
            async fn get_channel_members(
                &self,
                app_id: &str,
                channel: &str,
            ) -> sockudo_core::error::Result<
                ahash::AHashMap<String, sockudo_core::channel::PresenceMemberInfo>,
            > {
                $crate::test_support::NoopConnectionManager::get_channel_members(
                    self, app_id, channel,
                )
                .await
            }
            async fn get_channel_sockets(
                &self,
                app_id: &str,
                channel: &str,
            ) -> sockudo_core::error::Result<Vec<sockudo_core::websocket::SocketId>> {
                $crate::test_support::NoopConnectionManager::get_channel_sockets(
                    self, app_id, channel,
                )
                .await
            }
            async fn remove_channel(&self, app_id: &str, channel: &str) {
                $crate::test_support::NoopConnectionManager::remove_channel(self, app_id, channel)
                    .await
            }
            async fn is_in_channel(
                &self,
                app_id: &str,
                channel: &str,
                socket_id: &sockudo_core::websocket::SocketId,
            ) -> sockudo_core::error::Result<bool> {
                $crate::test_support::NoopConnectionManager::is_in_channel(
                    self, app_id, channel, socket_id,
                )
                .await
            }
            async fn get_user_sockets(
                &self,
                user_id: &str,
                app_id: &str,
            ) -> sockudo_core::error::Result<Vec<sockudo_core::websocket::WebSocketRef>> {
                $crate::test_support::NoopConnectionManager::get_user_sockets(self, user_id, app_id)
                    .await
            }
            async fn cleanup_connection(
                &self,
                app_id: &str,
                ws: sockudo_core::websocket::WebSocketRef,
            ) {
                $crate::test_support::NoopConnectionManager::cleanup_connection(self, app_id, ws)
                    .await
            }
            async fn terminate_connection(
                &self,
                app_id: &str,
                user_id: &str,
            ) -> sockudo_core::error::Result<()> {
                $crate::test_support::NoopConnectionManager::terminate_connection(
                    self, app_id, user_id,
                )
                .await
            }
            async fn add_channel_to_sockets(
                &self,
                app_id: &str,
                channel: &str,
                socket_id: &sockudo_core::websocket::SocketId,
            ) {
                $crate::test_support::NoopConnectionManager::add_channel_to_sockets(
                    self, app_id, channel, socket_id,
                )
                .await
            }
            async fn get_channel_socket_count_info(
                &self,
                app_id: &str,
                channel: &str,
            ) -> $crate::connection_manager::ChannelSocketCount {
                $crate::test_support::NoopConnectionManager::get_channel_socket_count_info(
                    self, app_id, channel,
                )
                .await
            }
            async fn get_channel_socket_count(&self, app_id: &str, channel: &str) -> usize {
                $crate::test_support::NoopConnectionManager::get_channel_socket_count(
                    self, app_id, channel,
                )
                .await
            }
            async fn get_local_channel_socket_count(&self, app_id: &str, channel: &str) -> usize {
                $crate::test_support::NoopConnectionManager::get_local_channel_socket_count(
                    self, app_id, channel,
                )
                .await
            }
            async fn add_to_channel(
                &self,
                app_id: &str,
                channel: &str,
                socket_id: &sockudo_core::websocket::SocketId,
            ) -> sockudo_core::error::Result<(bool, bool)> {
                $crate::test_support::NoopConnectionManager::add_to_channel(
                    self, app_id, channel, socket_id,
                )
                .await
            }
            async fn remove_from_channel(
                &self,
                app_id: &str,
                channel: &str,
                socket_id: &sockudo_core::websocket::SocketId,
            ) -> sockudo_core::error::Result<(bool, bool)> {
                $crate::test_support::NoopConnectionManager::remove_from_channel(
                    self, app_id, channel, socket_id,
                )
                .await
            }
            async fn get_presence_member(
                &self,
                app_id: &str,
                channel: &str,
                socket_id: &sockudo_core::websocket::SocketId,
            ) -> Option<sockudo_core::channel::PresenceMemberInfo> {
                $crate::test_support::NoopConnectionManager::get_presence_member(
                    self, app_id, channel, socket_id,
                )
                .await
            }
            async fn terminate_user_connections(
                &self,
                app_id: &str,
                user_id: &str,
            ) -> sockudo_core::error::Result<()> {
                $crate::test_support::NoopConnectionManager::terminate_user_connections(
                    self, app_id, user_id,
                )
                .await
            }
            async fn force_reconnect_user(
                &self,
                app_id: &str,
                user_id: &str,
            ) -> sockudo_core::error::Result<()> {
                $crate::test_support::NoopConnectionManager::force_reconnect_user(
                    self, app_id, user_id,
                )
                .await
            }
            async fn add_user(
                &self,
                ws: sockudo_core::websocket::WebSocketRef,
            ) -> sockudo_core::error::Result<()> {
                $crate::test_support::NoopConnectionManager::add_user(self, ws).await
            }
            async fn remove_user(
                &self,
                ws: sockudo_core::websocket::WebSocketRef,
            ) -> sockudo_core::error::Result<()> {
                $crate::test_support::NoopConnectionManager::remove_user(self, ws).await
            }
            async fn remove_user_socket(
                &self,
                user_id: &str,
                socket_id: &sockudo_core::websocket::SocketId,
                app_id: &str,
            ) -> sockudo_core::error::Result<()> {
                $crate::test_support::NoopConnectionManager::remove_user_socket(
                    self, user_id, socket_id, app_id,
                )
                .await
            }
            async fn count_user_connections_in_channel(
                &self,
                user_id: &str,
                app_id: &str,
                channel: &str,
                excluding_socket: Option<&sockudo_core::websocket::SocketId>,
            ) -> sockudo_core::error::Result<usize> {
                $crate::test_support::NoopConnectionManager::count_user_connections_in_channel(
                    self,
                    user_id,
                    app_id,
                    channel,
                    excluding_socket,
                )
                .await
            }
            async fn get_channels_with_socket_count(
                &self,
                app_id: &str,
            ) -> sockudo_core::error::Result<ahash::AHashMap<String, usize>> {
                $crate::test_support::NoopConnectionManager::get_channels_with_socket_count(
                    self, app_id,
                )
                .await
            }
            async fn get_sockets_count(&self, app_id: &str) -> sockudo_core::error::Result<usize> {
                $crate::test_support::NoopConnectionManager::get_sockets_count(self, app_id).await
            }
            async fn get_namespaces(
                &self,
            ) -> sockudo_core::error::Result<
                Vec<(String, std::sync::Arc<sockudo_core::namespace::Namespace>)>,
            > {
                $crate::test_support::NoopConnectionManager::get_namespaces(self).await
            }
            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                $crate::test_support::NoopConnectionManager::as_any_mut(self)
            }
            async fn check_health(&self) -> sockudo_core::error::Result<()> {
                $crate::test_support::NoopConnectionManager::check_health(self).await
            }
            fn get_node_id(&self) -> String {
                $crate::test_support::NoopConnectionManager::get_node_id(self)
            }
            fn as_horizontal_adapter(
                &self,
            ) -> Option<&dyn $crate::connection_manager::HorizontalAdapterInterface> {
                $crate::test_support::NoopConnectionManager::as_horizontal_adapter(self)
            }
        }
    };
}
