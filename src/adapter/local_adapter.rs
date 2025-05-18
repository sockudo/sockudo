use crate::adapter::adapter::Adapter;
use crate::app::manager::AppManager;
use crate::channel::PresenceMemberInfo;
use crate::error::{Error, Result};

use crate::namespace::Namespace;
use crate::protocol::messages::PusherMessage;
use crate::websocket::{SocketId, WebSocket, WebSocketRef};
use dashmap::{DashMap, DashSet};
use fastwebsockets::{Frame, Payload, WebSocketWrite};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::WriteHalf;
use tokio::sync::Mutex;
use tracing::{error, info};

#[derive(Clone)]
pub struct LocalAdapter {
    pub namespaces: DashMap<String, Arc<Namespace>>,
}

impl Default for LocalAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalAdapter {
    pub fn new() -> Self {
        Self {
            namespaces: DashMap::new(),
        }
    }

    // Helper function to get or create namespace
    async fn get_or_create_namespace(&mut self, app_id: &str) -> Arc<Namespace> {
        if !self.namespaces.contains_key(app_id) {
            let namespace = Arc::new(Namespace::new(app_id.to_string()));
            self.namespaces.insert(app_id.to_string(), namespace);
        }
        self.namespaces.get(app_id).unwrap().clone()
    }
    pub async fn get_all_connections(
        &mut self,
        app_id: &str,
    ) -> DashMap<SocketId, Arc<Mutex<WebSocket>>> {
        // First, get or create the namespace for this app
        let namespace = self.get_or_create_namespace(app_id).await;

        // Return a clone of the sockets DashMap from the namespace
        namespace.sockets.clone()
    }
}

#[async_trait::async_trait]
impl Adapter for LocalAdapter {
    async fn init(&mut self) {
        info!("{}", "Initializing local adapter");
    }

    async fn get_namespace(&mut self, app_id: &str) -> Option<Arc<Namespace>> {
        Some(self.get_or_create_namespace(app_id).await)
    }

    async fn add_socket(
        &mut self,
        socket_id: SocketId,
        socket: WebSocketWrite<WriteHalf<TokioIo<Upgraded>>>,
        app_id: &str,
        app_manager: &Arc<dyn AppManager + Send + Sync>,
    ) -> Result<()> {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.add_socket(socket_id, socket, app_manager).await?;

        Ok(())
    }

    async fn get_connection(
        &mut self,
        socket_id: &SocketId,
        app_id: &str,
    ) -> Option<Arc<Mutex<WebSocket>>> {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.get_connection(socket_id)
    }

    async fn remove_connection(&mut self, socket_id: &SocketId, app_id: &str) -> Result<()> {
        if let Some(namespace) = self.namespaces.get(app_id) {
            namespace.remove_connection(socket_id);
            Ok(())
        } else {
            Err(Error::ConnectionError("Namespace not found".to_string()))
        }
    }

    async fn send_message(
        &mut self,
        app_id: &str,
        socket_id: &SocketId,
        message: PusherMessage,
    ) -> Result<()> {
        let connection = self
            .get_connection(socket_id, app_id)
            .await
            .ok_or_else(|| Error::ConnectionError("Connection not found".to_string()))?;

        let message = serde_json::to_string(&message)
            .map_err(|e| Error::ConnectionError(format!("Failed to serialize message: {}", e)))?;

        let frame = Frame::text(Payload::from(message.into_bytes()));

        // Get the sender without locking the entire adapter
        let sender = {
            let conn = connection.lock().await;
            conn.message_sender.clone()
        };

        if let Err(e) = sender.send(frame) {
            error!("{}", format!("Failed to send message: {}", e));
        }

        Ok(())
    }

    async fn send(
        &mut self,
        channel: &str,
        message: PusherMessage,
        except: Option<&SocketId>,
        app_id: &str,
    ) -> Result<()> {
        info!("{}", format!("Sending message to channel: {}", channel));
        info!("{}", format!("Message: {:?}", message));
        if channel.starts_with("#server-to-user-") {
            let user_id = channel.trim_start_matches("#server-to-user-");
            let namespace = self.get_namespace(app_id).await.unwrap();
            let socket_refs = namespace.get_user_sockets(user_id).await?;

            for socket_ref in socket_refs {
                // Get socket_id before sending
                let socket_id = {
                    let ws = socket_ref.0.lock().await;
                    ws.state.socket_id.clone()
                };

                if except != Some(&socket_id) {
                    self.send_message(app_id, &socket_id, message.clone())
                        .await?;
                }
            }
        } else {
            let namespace = self.get_namespace(app_id).await.unwrap();
            let sockets = namespace.get_channel_sockets(channel);

            // Iterate through our DashMap of sockets
            for socket_id in sockets.iter().map(|entry| entry.key().clone()) {
                if except != Some(&socket_id) {
                    self.send_message(app_id, &socket_id, message.clone())
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn get_channel_members(
        &mut self,
        app_id: &str,
        channel: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.get_channel_members(channel).await
    }

    async fn get_channel_sockets(
        &mut self,
        app_id: &str,
        channel: &str,
    ) -> Result<DashMap<SocketId, Arc<Mutex<WebSocket>>>> {
        let namespace = self.get_or_create_namespace(app_id).await;
        Ok(namespace.get_channel_sockets(channel))
    }

    async fn get_channel(&mut self, app_id: &str, channel: &str) -> Result<DashSet<SocketId>> {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.get_channel(channel)
    }

    async fn remove_channel(&mut self, app_id: &str, channel: &str) {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.remove_channel(channel);
    }

    async fn is_in_channel(
        &mut self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        let namespace = self.get_or_create_namespace(app_id).await;
        Ok(namespace.is_in_channel(channel, socket_id))
    }

    async fn get_user_sockets(
        &mut self,
        user_id: &str,
        app_id: &str,
    ) -> Result<DashSet<WebSocketRef>> {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.get_user_sockets(user_id).await
    }

    async fn cleanup_connection(&mut self, app_id: &str, ws: WebSocketRef) {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.cleanup_connection(ws).await;
    }

    async fn terminate_connection(&mut self, app_id: &str, user_id: &str) -> Result<()> {
        let namespace = self.get_or_create_namespace(app_id).await;
        if let Err(e) = namespace.terminate_user_connections(user_id).await {
            error!("{}", format!("Failed to terminate adapter: {}", e));
        }
        Ok(())
    }

    async fn add_channel_to_sockets(&mut self, app_id: &str, channel: &str, socket_id: &SocketId) {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.add_channel_to_socket(channel, socket_id);
    }

    async fn get_channel_socket_count(&mut self, app_id: &str, channel: &str) -> usize {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.get_channel_sockets(channel).len()
    }

    async fn add_to_channel(
        &mut self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        let namespace = self.get_or_create_namespace(app_id).await;
        Ok(namespace.add_channel_to_socket(channel, socket_id))
    }

    async fn remove_from_channel(
        &mut self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        let namespace = self.get_or_create_namespace(app_id).await;
        Ok(namespace.remove_channel_from_socket(channel, socket_id))
    }

    async fn get_presence_member(
        &mut self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<PresenceMemberInfo> {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.get_presence_member(channel, socket_id).await
    }

    async fn terminate_user_connections(&mut self, app_id: &str, user_id: &str) -> Result<()> {
        let namespace = self.get_or_create_namespace(app_id).await;
        if let Err(e) = namespace.terminate_user_connections(user_id).await {
            error!("{}", format!("Failed to terminate user connections: {}", e));
        }
        Ok(())
    }

    async fn add_user(&mut self, ws: Arc<Mutex<WebSocket>>) -> Result<()> {
        let app_id = ws.lock().await.state.get_app_key();
        let namespace = self.get_namespace(&app_id).await.unwrap();
        namespace.add_user(ws).await
    }

    async fn remove_user(&mut self, ws: Arc<Mutex<WebSocket>>) -> Result<()> {
        let app_id = ws.lock().await.state.get_app_key();
        let namespace = self.get_namespace(&app_id).await.unwrap();
        namespace.remove_user(ws).await
    }

    async fn get_channels_with_socket_count(
        &mut self,
        app_id: &str,
    ) -> Result<DashMap<String, usize>> {
        let namespace = self.get_or_create_namespace(app_id).await;
        let channels = namespace.get_channels_with_socket_count().await;
        Ok(channels?)
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
