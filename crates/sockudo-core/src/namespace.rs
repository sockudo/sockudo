use crate::app::AppManager;
use crate::channel::PresenceMemberInfo;
use crate::error::{Error, Result};
use crate::websocket::{SocketId, WebSocket, WebSocketBufferConfig, WebSocketRef};
use ahash::AHashMap as HashMap;
use dashmap::{DashMap, DashSet};
use futures_util::future::join_all;
use sockudo_ws::axum_integration::WebSocketWriter;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

// Represents a namespace, typically tied to a specific application ID.
pub struct Namespace {
    pub app_id: String,
    pub sockets: DashMap<SocketId, WebSocketRef>,
    pub channels: DashMap<String, DashSet<SocketId>>,
    pub users: DashMap<String, DashSet<WebSocketRef>>,
}

impl Namespace {
    pub fn new(app_id: String) -> Self {
        Self {
            app_id,
            sockets: DashMap::new(),
            channels: DashMap::new(),
            users: DashMap::new(),
        }
    }

    pub async fn add_socket(
        &self,
        socket_id: SocketId,
        socket_writer: WebSocketWriter,
        app_manager: Arc<dyn AppManager + Send + Sync>,
        buffer_config: WebSocketBufferConfig,
        protocol_version: sockudo_protocol::ProtocolVersion,
    ) -> Result<WebSocketRef> {
        let app_config = match app_manager.find_by_id(&self.app_id).await {
            Ok(Some(app)) => app,
            Ok(None) => {
                error!(
                    "App not found for app_id: {}. Cannot initialize socket: {}",
                    self.app_id, socket_id
                );
                return Err(Error::ApplicationNotFound);
            }
            Err(e) => {
                error!(
                    "Failed to get app {} for socket {}: {}",
                    self.app_id, socket_id, e
                );
                return Err(Error::Internal(format!(
                    "Failed to retrieve app config: {e}"
                )));
            }
        };

        let mut websocket = WebSocket::with_buffer_config(socket_id, socket_writer, buffer_config);
        websocket.state.app = Some(app_config);
        websocket.state.protocol_version = protocol_version;

        let websocket_ref = WebSocketRef::new(websocket);
        self.sockets.insert(socket_id, websocket_ref.clone());

        debug!(socket_id = %socket_id, "WebSocket connection added successfully");

        Ok(websocket_ref)
    }

    pub fn get_connection(&self, socket_id: &SocketId) -> Option<WebSocketRef> {
        self.sockets
            .get(socket_id)
            .map(|conn_ref| conn_ref.value().clone())
    }

    pub async fn get_channel_members(
        &self,
        channel: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        let mut presence_members = HashMap::new();

        if let Some(socket_ids_ref) = self.channels.get(channel) {
            let socket_ids_snapshot = socket_ids_ref.clone();
            drop(socket_ids_ref);

            for socket_id_entry in socket_ids_snapshot.iter() {
                let socket_id = socket_id_entry.key();
                if let Some(connection) = self.get_connection(socket_id) {
                    let presence_data = {
                        let conn_guard = connection.inner.lock().await;
                        conn_guard
                            .state
                            .presence
                            .as_ref()
                            .and_then(|p_map| p_map.get(channel).cloned())
                    };
                    if let Some(presence_info) = presence_data {
                        presence_members.insert(presence_info.user_id.clone(), presence_info);
                    }
                }
            }
        } else {
            info!(
                "get_channel_members called on non-existent channel: {}",
                channel
            );
        }
        Ok(presence_members)
    }

    pub fn get_channel_socket_count(&self, channel: &str) -> usize {
        self.channels
            .get(channel)
            .map(|set_ref| set_ref.len())
            .unwrap_or(0)
    }

    #[inline]
    pub fn get_channel_sockets(&self, channel: &str) -> Vec<SocketId> {
        if let Some(channel_sockets_ref) = self.channels.get(channel) {
            channel_sockets_ref
                .iter()
                .map(|entry| *entry.key())
                .collect()
        } else {
            debug!(
                "get_channel_sockets called on non-existent channel: {}",
                channel
            );
            Vec::new()
        }
    }

    pub fn get_channel_socket_refs_except(
        &self,
        channel: &str,
        except: Option<&SocketId>,
    ) -> Vec<WebSocketRef> {
        let mut socket_refs = Vec::new();

        if let Some(channel_sockets_ref) = self.channels.get(channel) {
            let socket_ids: Vec<SocketId> = channel_sockets_ref
                .value()
                .iter()
                .filter_map(|entry| {
                    let socket_id = entry.key();
                    if except == Some(socket_id) {
                        None
                    } else {
                        Some(*socket_id)
                    }
                })
                .collect();

            drop(channel_sockets_ref);

            for socket_id in socket_ids {
                if let Some(socket_ref) = self.get_connection(&socket_id) {
                    socket_refs.push(socket_ref);
                }
            }
        } else {
            debug!(
                "get_channel_socket_refs_except called on non-existent channel: {}",
                channel
            );
        }

        socket_refs
    }

    #[inline]
    pub async fn get_user_sockets(&self, user_id: &str) -> Result<Vec<WebSocketRef>> {
        match self.users.get(user_id) {
            Some(user_sockets_ref) => Ok(user_sockets_ref.iter().map(|r| r.clone()).collect()),
            None => Ok(Vec::new()),
        }
    }

    pub async fn cleanup_connection(&self, ws_ref: WebSocketRef) {
        let socket_id = ws_ref.get_socket_id().await;

        // Signal reader and writer tasks to stop
        ws_ref.shutdown();

        let channels_to_check: Vec<String> = self
            .channels
            .iter()
            .filter(|entry| entry.value().contains(&socket_id))
            .map(|entry| entry.key().clone())
            .collect();

        for channel_name in channels_to_check {
            self.channels.remove_if(&channel_name, |_, set| {
                set.remove(&socket_id);
                set.is_empty()
            });
        }

        let user_id_option = ws_ref.get_user_id().await;

        if let Some(user_id) = user_id_option
            && let Some(user_sockets_ref) = self.users.get_mut(&user_id)
        {
            user_sockets_ref.remove(&ws_ref);
            let is_empty = user_sockets_ref.is_empty();
            drop(user_sockets_ref);
            if is_empty {
                self.users.remove(&user_id);
                debug!("Removed empty user entry for: {}", user_id);
            }
        }

        if self.sockets.remove(&socket_id).is_some() {
            debug!("Removed socket {} from main map.", socket_id);
        } else {
            warn!(
                "Socket {} already removed from main map during cleanup.",
                socket_id
            );
        }
    }

    pub async fn terminate_user_connections(&self, user_id: &str) -> Result<()> {
        if let Some(user_sockets_ref) = self.users.get(user_id) {
            let user_sockets_snapshot = user_sockets_ref.clone();
            drop(user_sockets_ref);

            let cleanup_tasks: Vec<_> = user_sockets_snapshot
                .iter()
                .map(|ws_ref| async move {
                    if let Err(e) = ws_ref
                        .close(4009, "You got disconnected by the app.".to_string())
                        .await
                    {
                        warn!("Failed to close connection: {}", e);
                    }
                })
                .collect();

            join_all(cleanup_tasks).await;
        }
        Ok(())
    }

    pub fn add_channel_to_socket(&self, channel: &str, socket_id: &SocketId) -> bool {
        let t_start = std::time::Instant::now();

        let t_before_entry = t_start.elapsed().as_nanos();
        let result = self
            .channels
            .entry(channel.to_string())
            .or_default()
            .insert(*socket_id);
        let t_after_entry = t_start.elapsed().as_nanos();

        tracing::debug!(
            "PERF[NS_ADD_CHAN] channel={} socket={} entry_op={}ns inserted={}",
            channel,
            socket_id,
            t_after_entry - t_before_entry,
            result
        );

        result
    }

    pub fn remove_channel_from_socket(&self, channel: &str, socket_id: &SocketId) -> bool {
        if let Some(channel_sockets_ref) = self.channels.get(channel) {
            let removed = channel_sockets_ref.remove(socket_id);
            drop(channel_sockets_ref);

            if self
                .channels
                .remove_if(channel, |_, set| set.is_empty())
                .is_some()
            {
                debug!("Removed empty channel entry: {}", channel);
            }
            return removed.is_some();
        }
        false
    }

    pub fn remove_connection(&self, socket_id: &SocketId) {
        if self.sockets.remove(socket_id).is_some() {
            debug!("Explicitly removed socket: {}", socket_id);
        }
    }

    pub fn get_channel(&self, channel: &str) -> Result<DashSet<SocketId>> {
        let channel_data = self.channels.entry(channel.to_string()).or_default();
        Ok(channel_data.value().clone())
    }

    pub fn remove_channel(&self, channel: &str) {
        self.channels.remove(channel);
        debug!("Removed channel entry: {}", channel);
    }

    pub fn is_in_channel(&self, channel: &str, socket_id: &SocketId) -> bool {
        self.channels
            .get(channel)
            .is_some_and(|channel_sockets| channel_sockets.contains(socket_id))
    }

    pub async fn get_presence_member(
        &self,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<PresenceMemberInfo> {
        if let Some(connection) = self.get_connection(socket_id) {
            let conn_guard = connection.inner.lock().await;
            conn_guard
                .state
                .presence
                .as_ref()
                .and_then(|presence_map| presence_map.get(channel))
                .cloned()
        } else {
            None
        }
    }

    pub async fn add_user(&self, ws_ref: WebSocketRef) -> Result<()> {
        let user_id_option = ws_ref.get_user_id().await;

        if let Some(user_id) = user_id_option {
            let user_sockets_ref = self.users.entry(user_id.clone()).or_default();
            user_sockets_ref.insert(ws_ref.clone());
            let socket_id = ws_ref.get_socket_id().await;
            debug!("Added socket {} to user {}", socket_id, user_id);
        } else {
            let socket_id = ws_ref.get_socket_id().await;
            warn!(
                "User data found for socket {} but missing user ID.",
                socket_id
            );
        }
        Ok(())
    }

    pub async fn remove_user(&self, ws_ref: WebSocketRef) -> Result<()> {
        let user_id_option = ws_ref.get_user_id().await;
        let socket_id = ws_ref.get_socket_id().await;

        if let Some(user_id) = user_id_option {
            if let Some(user_sockets_ref) = self.users.get_mut(&user_id) {
                let removed = user_sockets_ref.remove(&ws_ref);
                let is_empty = user_sockets_ref.is_empty();
                drop(user_sockets_ref);
                if removed.is_some() {
                    debug!("Removed socket {} from user {}", socket_id, user_id);
                }
                if is_empty {
                    self.users.remove(&user_id);
                    debug!("Removed empty user entry for: {}", user_id);
                }
            }
        } else {
            warn!(
                "User data found for socket {} during removal but missing user ID.",
                socket_id
            );
        }
        Ok(())
    }

    pub async fn remove_user_socket(&self, user_id: &str, socket_id: &SocketId) -> Result<()> {
        if let Some(user_sockets_ref) = self.users.get_mut(user_id) {
            let mut found_socket = None;

            for ws_ref in user_sockets_ref.iter() {
                let ws_socket_id = ws_ref.get_socket_id().await;
                if ws_socket_id == *socket_id {
                    found_socket = Some(ws_ref.clone());
                    break;
                }
            }

            if let Some(ws_ref) = found_socket {
                user_sockets_ref.remove(&ws_ref);
            }

            let is_empty = user_sockets_ref.is_empty();
            drop(user_sockets_ref);

            if is_empty {
                self.users.remove(user_id);
                debug!("Removed empty user entry for: {}", user_id);
            }

            debug!("Removed socket {} from user {}", socket_id, user_id);
        } else {
            debug!(
                "User {} not found when removing socket {}",
                user_id, socket_id
            );
        }
        Ok(())
    }

    pub async fn count_user_connections_in_channel(
        &self,
        user_id: &str,
        channel: &str,
        excluding_socket: Option<&SocketId>,
    ) -> Result<usize> {
        let mut count = 0;

        if let Some(user_sockets_ref) = self.users.get(user_id) {
            for ws_ref in user_sockets_ref.iter() {
                let socket_state_guard = ws_ref.inner.lock().await;
                let socket_id = &socket_state_guard.state.socket_id;
                let is_subscribed = socket_state_guard.state.is_subscribed(channel);

                let should_exclude = excluding_socket == Some(socket_id);

                if is_subscribed && !should_exclude {
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    pub async fn get_channels_with_socket_count(&self) -> Result<HashMap<String, usize>> {
        let mut channels_with_count: HashMap<String, usize> = HashMap::new();
        for channel_ref in self.channels.iter() {
            let channel_name = channel_ref.key().clone();
            let socket_count = channel_ref.value().len();
            channels_with_count.insert(channel_name, socket_count);
        }
        Ok(channels_with_count)
    }

    #[inline]
    pub async fn get_sockets(&self) -> Result<Vec<(SocketId, WebSocketRef)>> {
        Ok(self
            .sockets
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect())
    }
}
