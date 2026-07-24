use crate::app::AppManager;
use crate::channel::PresenceMemberInfo;
use crate::error::{Error, Result};
use crate::utils::wildcard_pattern_matches;
use crate::websocket::{SocketId, WebSocket, WebSocketBufferConfig, WebSocketRef};
use ahash::AHashMap as HashMap;
use ahash::AHashSet;
use dashmap::mapref::entry::Entry;
use dashmap::{DashMap, DashSet};
use futures_util::future::join_all;
use sockudo_ws::axum_integration::WebSocketWriter;
use std::sync::Arc;
use tracing::{debug, error, warn};

// Represents a namespace, typically tied to a specific application ID.
pub struct Namespace {
    pub app_id: String,
    pub sockets: DashMap<SocketId, WebSocketRef>,
    pub channels: DashMap<String, DashSet<SocketId>>,
    wildcard_channels: DashSet<String>,
    pub users: DashMap<String, DashSet<SocketId>>,
    pub presence_data: DashMap<SocketId, HashMap<String, PresenceMemberInfo>>,
    // Reverse index of channels joined per socket, for O(channels-per-socket) disconnect cleanup.
    socket_channels: DashMap<SocketId, DashSet<String>>,
    // Reverse index: socket → set of user IDs associated with that socket.
    socket_users: DashMap<SocketId, DashSet<String>>,
}

pub struct SocketInitOptions {
    pub buffer_config: WebSocketBufferConfig,
    pub protocol_version: sockudo_protocol::ProtocolVersion,
    pub wire_format: sockudo_protocol::WireFormat,
    pub echo_messages: bool,
    pub append_mode: sockudo_protocol::AppendMode,
}

impl Namespace {
    pub fn new(app_id: String) -> Self {
        Self {
            app_id,
            sockets: DashMap::new(),
            channels: DashMap::new(),
            wildcard_channels: DashSet::new(),
            users: DashMap::new(),
            presence_data: DashMap::new(),
            socket_channels: DashMap::new(),
            socket_users: DashMap::new(),
        }
    }

    pub async fn add_socket(
        &self,
        socket_id: SocketId,
        socket_writer: WebSocketWriter,
        app_manager: Arc<dyn AppManager + Send + Sync>,
        init: SocketInitOptions,
    ) -> Result<WebSocketRef> {
        let app_config = match app_manager.find_by_id(&self.app_id).await {
            Ok(Some(app)) => app,
            Ok(None) => {
                error!(
                    app_id = %self.app_id,
                    socket_id = %socket_id,
                    "app not found during socket initialization"
                );
                return Err(Error::ApplicationNotFound);
            }
            Err(e) => {
                error!(
                    app_id = %self.app_id,
                    socket_id = %socket_id,
                    error = %e,
                    "app lookup failed during socket initialization"
                );
                return Err(Error::Internal(format!(
                    "Failed to retrieve app config: {e}"
                )));
            }
        };

        let mut websocket =
            WebSocket::with_buffer_config(socket_id, socket_writer, init.buffer_config);
        websocket.state.app = Some(app_config);
        websocket.state.protocol_version = init.protocol_version;
        websocket.state.wire_format = init.wire_format;
        websocket.state.echo_messages = init.echo_messages;
        websocket.state.append_mode = init.append_mode;

        let websocket_ref = WebSocketRef::new(websocket);
        self.sockets.insert(socket_id, websocket_ref.clone());

        debug!(app_id = %self.app_id, socket_id = %socket_id, "websocket connection added");

        Ok(websocket_ref)
    }

    pub fn get_connection(&self, socket_id: &SocketId) -> Option<WebSocketRef> {
        self.sockets
            .get(socket_id)
            .map(|conn_ref| conn_ref.value().clone())
    }

    pub fn get_channel_members(
        &self,
        channel: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        let mut presence_members = HashMap::new();

        if let Some(socket_ids_ref) = self.channels.get(channel) {
            for socket_id_entry in socket_ids_ref.iter() {
                let socket_id = socket_id_entry.key();
                if let Some(per_socket) = self.presence_data.get(socket_id)
                    && let Some(info) = per_socket.get(channel)
                {
                    presence_members
                        .entry(info.user_id.clone())
                        .or_insert_with(|| info.clone());
                }
            }
        } else {
            debug!(channel = %channel, "channel members requested for missing channel");
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
            debug!(channel = %channel, "channel sockets requested for missing channel");
            Vec::new()
        }
    }

    pub fn get_channel_socket_refs_except(
        &self,
        channel: &str,
        except: Option<&SocketId>,
    ) -> Vec<WebSocketRef> {
        let Some(channel_sockets_ref) = self.channels.get(channel) else {
            debug!(channel = %channel, "channel socket references requested for missing channel");
            return Vec::new();
        };

        let mut socket_refs = Vec::with_capacity(channel_sockets_ref.len());

        for socket_id_entry in channel_sockets_ref.iter() {
            let socket_id = socket_id_entry.key();
            if except == Some(socket_id) {
                continue;
            }

            if let Some(socket_ref) = self.get_connection(socket_id) {
                socket_refs.push(socket_ref);
            }
        }

        socket_refs
    }

    pub fn get_matching_channel_socket_refs_except(
        &self,
        channel: &str,
        except: Option<&SocketId>,
    ) -> Vec<WebSocketRef> {
        if channel.contains('*') {
            return self
                .get_matching_channel_socket_ids_except(channel, except)
                .into_iter()
                .filter_map(|socket_id| self.get_connection(&socket_id))
                .collect();
        }

        let mut socket_refs = self.get_channel_socket_refs_except(channel, except);
        if self.wildcard_channels.is_empty() {
            return socket_refs;
        }

        let mut seen_socket_ids = None;
        self.collect_matching_socket_refs(channel, except, &mut seen_socket_ids, &mut socket_refs);
        socket_refs
    }

    pub fn get_matching_channel_socket_refs_partitioned_except(
        &self,
        channel: &str,
        except: Option<&SocketId>,
    ) -> (Vec<WebSocketRef>, Vec<WebSocketRef>) {
        if channel.contains('*') {
            let mut v1_refs = Vec::new();
            let mut v2_refs = Vec::new();

            for socket_id in self.get_matching_channel_socket_ids_except(channel, except) {
                if let Some(socket_ref) = self.get_connection(&socket_id) {
                    Self::push_socket_ref_by_protocol(socket_ref, &mut v1_refs, &mut v2_refs);
                }
            }

            return (v1_refs, v2_refs);
        }

        let mut v1_refs = Vec::new();
        let mut v2_refs = Vec::new();

        if let Some(channel_sockets_ref) = self.channels.get(channel) {
            for socket_id_entry in channel_sockets_ref.iter() {
                let socket_id = socket_id_entry.key();
                if except == Some(socket_id) {
                    continue;
                }

                if let Some(socket_ref) = self.get_connection(socket_id) {
                    Self::push_socket_ref_by_protocol(socket_ref, &mut v1_refs, &mut v2_refs);
                }
            }
        }

        if self.wildcard_channels.is_empty() {
            return (v1_refs, v2_refs);
        }

        let mut seen_socket_ids = None;
        self.collect_matching_socket_refs_partitioned(
            channel,
            except,
            &mut seen_socket_ids,
            &mut v1_refs,
            &mut v2_refs,
        );

        (v1_refs, v2_refs)
    }

    pub fn get_matching_channel_socket_ids_except(
        &self,
        channel: &str,
        except: Option<&SocketId>,
    ) -> AHashSet<SocketId> {
        let mut socket_ids = AHashSet::new();

        if channel.contains('*') {
            self.collect_matching_socket_ids(channel, except, &mut socket_ids, true);
            return socket_ids;
        }

        self.collect_exact_channel_socket_ids(channel, except, &mut socket_ids);
        self.collect_matching_socket_ids(channel, except, &mut socket_ids, false);
        socket_ids
    }

    fn collect_exact_channel_socket_ids(
        &self,
        channel: &str,
        except: Option<&SocketId>,
        socket_ids: &mut AHashSet<SocketId>,
    ) {
        if let Some(channel_sockets_ref) = self.channels.get(channel) {
            for socket_id_entry in channel_sockets_ref.iter() {
                let socket_id = socket_id_entry.key();
                if except != Some(socket_id) {
                    socket_ids.insert(*socket_id);
                }
            }
        }
    }

    fn collect_matching_socket_ids(
        &self,
        channel: &str,
        except: Option<&SocketId>,
        socket_ids: &mut AHashSet<SocketId>,
        include_exact_channels: bool,
    ) {
        for wildcard_channel in self.wildcard_channels.iter() {
            let subscribed_channel = wildcard_channel.key();
            if !wildcard_pattern_matches(channel, subscribed_channel) {
                continue;
            }

            if let Some(channel_sockets_ref) = self.channels.get(subscribed_channel) {
                for socket_id_entry in channel_sockets_ref.iter() {
                    let socket_id = socket_id_entry.key();
                    if except != Some(socket_id) {
                        socket_ids.insert(*socket_id);
                    }
                }
            }
        }

        if include_exact_channels {
            self.collect_exact_channel_socket_ids(channel, except, socket_ids);
        }
    }

    fn collect_matching_socket_refs(
        &self,
        channel: &str,
        except: Option<&SocketId>,
        seen_socket_ids: &mut Option<AHashSet<SocketId>>,
        socket_refs: &mut Vec<WebSocketRef>,
    ) {
        for wildcard_channel in self.wildcard_channels.iter() {
            let subscribed_channel = wildcard_channel.key();
            if !wildcard_pattern_matches(channel, subscribed_channel) {
                continue;
            }

            let seen_socket_ids = seen_socket_ids.get_or_insert_with(|| {
                let mut seen = AHashSet::with_capacity(socket_refs.len().saturating_mul(2));
                for socket_ref in socket_refs.iter() {
                    seen.insert(*socket_ref.get_socket_id_sync());
                }
                seen
            });

            if let Some(channel_sockets_ref) = self.channels.get(subscribed_channel) {
                for socket_id_entry in channel_sockets_ref.iter() {
                    let socket_id = socket_id_entry.key();
                    if except == Some(socket_id) || !seen_socket_ids.insert(*socket_id) {
                        continue;
                    }

                    if let Some(socket_ref) = self.get_connection(socket_id) {
                        socket_refs.push(socket_ref);
                    }
                }
            }
        }
    }

    fn collect_matching_socket_refs_partitioned(
        &self,
        channel: &str,
        except: Option<&SocketId>,
        seen_socket_ids: &mut Option<AHashSet<SocketId>>,
        v1_refs: &mut Vec<WebSocketRef>,
        v2_refs: &mut Vec<WebSocketRef>,
    ) {
        for wildcard_channel in self.wildcard_channels.iter() {
            let subscribed_channel = wildcard_channel.key();
            if !wildcard_pattern_matches(channel, subscribed_channel) {
                continue;
            }

            let seen_socket_ids = seen_socket_ids.get_or_insert_with(|| {
                let mut seen =
                    AHashSet::with_capacity((v1_refs.len() + v2_refs.len()).saturating_mul(2));
                for socket_ref in v1_refs.iter().chain(v2_refs.iter()) {
                    seen.insert(*socket_ref.get_socket_id_sync());
                }
                seen
            });

            if let Some(channel_sockets_ref) = self.channels.get(subscribed_channel) {
                for socket_id_entry in channel_sockets_ref.iter() {
                    let socket_id = socket_id_entry.key();
                    if except == Some(socket_id) || !seen_socket_ids.insert(*socket_id) {
                        continue;
                    }

                    if let Some(socket_ref) = self.get_connection(socket_id) {
                        Self::push_socket_ref_by_protocol(socket_ref, v1_refs, v2_refs);
                    }
                }
            }
        }
    }

    fn push_socket_ref_by_protocol(
        socket_ref: WebSocketRef,
        v1_refs: &mut Vec<WebSocketRef>,
        v2_refs: &mut Vec<WebSocketRef>,
    ) {
        if socket_ref.protocol_version == sockudo_protocol::ProtocolVersion::V1 {
            v1_refs.push(socket_ref);
        } else {
            v2_refs.push(socket_ref);
        }
    }

    #[inline]
    pub fn get_user_sockets(&self, user_id: &str) -> Result<Vec<WebSocketRef>> {
        let socket_ids: Vec<SocketId> = match self.users.get(user_id) {
            Some(user_sockets_ref) => user_sockets_ref.iter().map(|r| *r.key()).collect(),
            None => return Ok(Vec::new()),
        };

        Ok(socket_ids
            .into_iter()
            .filter_map(|sid| self.get_connection(&sid))
            .collect())
    }

    pub async fn cleanup_connection(&self, ws_ref: WebSocketRef) {
        let socket_id = ws_ref.get_socket_id().await;
        self.remove_connection(&socket_id);
    }

    pub async fn terminate_user_connections(&self, user_id: &str) -> Result<()> {
        let socket_ids: Vec<SocketId> = match self.users.get(user_id) {
            Some(user_sockets_ref) => user_sockets_ref.iter().map(|r| *r.key()).collect(),
            None => return Ok(()),
        };

        let cleanup_tasks: Vec<_> = socket_ids
            .into_iter()
            .filter_map(|sid| self.get_connection(&sid))
            .map(|ws_ref| async move {
                if let Err(e) = ws_ref
                    .close(4009, "You got disconnected by the app.".to_string())
                    .await
                {
                    warn!(error = %e, "failed to close connection");
                }
            })
            .collect();

        join_all(cleanup_tasks).await;
        Ok(())
    }

    pub async fn force_reconnect_user_connections(&self, user_id: &str) -> Result<()> {
        let socket_ids: Vec<SocketId> = match self.users.get(user_id) {
            Some(user_sockets_ref) => user_sockets_ref.iter().map(|r| *r.key()).collect(),
            None => return Ok(()),
        };

        let cleanup_tasks: Vec<_> = socket_ids
            .into_iter()
            .filter_map(|sid| self.get_connection(&sid))
            .map(|ws_ref| async move {
                if let Err(e) = ws_ref
                    .close(4200, "Force reconnect requested by the app.".to_string())
                    .await
                {
                    warn!(error = %e, "failed to close connection");
                }
            })
            .collect();

        join_all(cleanup_tasks).await;
        Ok(())
    }

    /// `activated` reports the first socket joining the channel, computed under
    /// the shard write lock so it is safe against concurrent add/remove.
    pub fn add_channel_to_socket(&self, channel: &str, socket_id: &SocketId) -> (bool, bool) {
        let t_start = std::time::Instant::now();

        let t_before_entry = t_start.elapsed().as_nanos();
        let (newly_inserted, activated) = match self.channels.entry(channel.to_string()) {
            Entry::Occupied(entry) => (entry.get().insert(*socket_id), false),
            Entry::Vacant(entry) => {
                let set_ref = entry.insert(DashSet::new());
                set_ref.insert(*socket_id);
                (true, true)
            }
        };
        let t_after_entry = t_start.elapsed().as_nanos();

        self.socket_channels
            .entry(*socket_id)
            .or_default()
            .insert(channel.to_string());

        if channel.contains('*') {
            self.wildcard_channels.insert(channel.to_string());
        }

        tracing::debug!(
            channel = %channel,
            socket_id = %socket_id,
            entry_operation_ns = t_after_entry - t_before_entry,
            inserted = newly_inserted,
            "namespace channel add timing"
        );

        (newly_inserted, activated)
    }

    /// `vacated` reports the last socket leaving: `remove_if` saw an empty set and
    /// deleted the entry under the shard write lock. Exactly one concurrent remover
    /// wins, so it pairs with `add`'s `activated`.
    pub fn remove_channel_from_socket(&self, channel: &str, socket_id: &SocketId) -> (bool, bool) {
        if let Some(socket_channels_ref) = self.socket_channels.get(socket_id) {
            socket_channels_ref.remove(channel);
        }

        if let Some(channel_sockets_ref) = self.channels.get(channel) {
            let removed = channel_sockets_ref.remove(socket_id);
            drop(channel_sockets_ref);

            let vacated = self
                .channels
                .remove_if(channel, |_, set| set.is_empty())
                .is_some();
            if vacated {
                if channel.contains('*') {
                    self.wildcard_channels.remove(channel);
                }
                debug!(channel = %channel, "empty channel entry removed");
            }
            return (removed.is_some(), vacated);
        }
        (false, false)
    }

    pub fn remove_connection(&self, socket_id: &SocketId) {
        // 1. Linearization point: remove from authoritative map first.
        let removed = self.sockets.remove(socket_id).map(|(_, ws_ref)| ws_ref);

        // 2. Cancel the connection's reader/writer tasks.
        if let Some(ws_ref) = removed.as_ref() {
            ws_ref.shutdown();
            debug!(socket_id = %socket_id, "removed socket from main map");
        }

        // 3. Exhaustive user cleanup via reverse index.
        self.remove_socket_from_all_users(socket_id);

        // 4. Exhaustive channel cleanup via reverse index.
        if let Some((_, channels)) = self.socket_channels.remove(socket_id) {
            for ch in channels.iter() {
                let channel = ch.key();
                self.channels.remove_if(channel, |_, set| {
                    set.remove(socket_id);
                    set.is_empty()
                });
                if channel.contains('*') && !self.channels.contains_key(channel) {
                    self.wildcard_channels.remove(channel);
                }
            }
        }
        // 5. Presence data cleanup.
        self.presence_data.remove(socket_id);
    }

    pub fn get_channel(&self, channel: &str) -> Result<DashSet<SocketId>> {
        Ok(self
            .channels
            .get(channel)
            .map(|channel_data| channel_data.value().clone())
            .unwrap_or_default())
    }

    pub fn remove_channel(&self, channel: &str) {
        self.channels.remove(channel);
        if channel.contains('*') {
            self.wildcard_channels.remove(channel);
        }
        debug!(channel = %channel, "channel entry removed");
    }

    pub fn is_in_channel(&self, channel: &str, socket_id: &SocketId) -> bool {
        self.channels
            .get(channel)
            .is_some_and(|channel_sockets| channel_sockets.contains(socket_id))
    }

    pub fn get_presence_member(
        &self,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<PresenceMemberInfo> {
        self.presence_data
            .get(socket_id)
            .and_then(|per_socket| per_socket.get(channel).cloned())
    }

    pub async fn update_presence_member(
        &self,
        channel: &str,
        socket_id: &SocketId,
        user_info: sonic_rs::Value,
    ) -> Option<PresenceMemberInfo> {
        let connection = self.get_connection(socket_id)?;
        let mut conn_guard = connection.inner.lock().await;
        conn_guard.update_presence_info(channel, user_info)
    }

    pub async fn add_user(&self, ws_ref: WebSocketRef) -> Result<()> {
        let socket_id = *ws_ref.get_socket_id_sync();

        let user_id = {
            let connection = ws_ref.inner.lock().await;
            if connection.state.disconnecting {
                return Ok(());
            }
            connection.state.user_id.clone()
        };

        let Some(user_id) = user_id else {
            warn!(socket_id = %socket_id, "socket has no user id");
            return Ok(());
        };

        if !self.sockets.contains_key(&socket_id) {
            return Ok(());
        }

        self.socket_users
            .entry(socket_id)
            .or_default()
            .insert(user_id.clone());
        self.users
            .entry(user_id.clone())
            .or_default()
            .insert(socket_id);

        let still_valid = self.sockets.contains_key(&socket_id) && {
            let connection = ws_ref.inner.lock().await;
            !connection.state.disconnecting
        };

        if !still_valid {
            self.remove_user_socket_association(&user_id, &socket_id);
        } else {
            debug!(socket_id = %socket_id, user_id = %user_id, "added socket to user");
        }

        Ok(())
    }

    pub async fn remove_user(&self, ws_ref: WebSocketRef) -> Result<()> {
        let socket_id = *ws_ref.get_socket_id_sync();
        let user_id_option = ws_ref.get_user_id().await;

        if let Some(user_id) = user_id_option {
            self.remove_user_socket_association(&user_id, &socket_id);
            debug!(socket_id = %socket_id, user_id = %user_id, "removed socket from user");
        } else {
            warn!(socket_id = %socket_id, "user data missing user id during removal");
        }
        Ok(())
    }

    pub fn remove_user_socket(&self, user_id: &str, socket_id: &SocketId) -> Result<()> {
        self.remove_user_socket_association(user_id, socket_id);
        debug!(socket_id = %socket_id, user_id = %user_id, "removed socket from user");
        Ok(())
    }

    pub fn count_user_connections_in_channel(
        &self,
        user_id: &str,
        channel: &str,
        excluding_socket: Option<&SocketId>,
    ) -> Result<usize> {
        let mut count = 0;

        if let Some(user_sockets_ref) = self.users.get(user_id) {
            for entry in user_sockets_ref.iter() {
                let socket_id = entry.key();
                if excluding_socket == Some(socket_id) {
                    continue;
                }
                if self
                    .presence_data
                    .get(socket_id)
                    .map(|m| m.contains_key(channel))
                    .unwrap_or(false)
                {
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

    fn remove_user_socket_association(&self, user_id: &str, socket_id: &SocketId) {
        self.users.remove_if(user_id, |_, socket_ids| {
            socket_ids.remove(socket_id);
            socket_ids.is_empty()
        });
        self.socket_users.remove_if(socket_id, |_, user_ids| {
            user_ids.remove(user_id);
            user_ids.is_empty()
        });
    }

    fn remove_socket_from_all_users(&self, socket_id: &SocketId) {
        if let Some((_, user_ids)) = self.socket_users.remove(socket_id) {
            for user_id_entry in user_ids.iter() {
                let user_id = user_id_entry.key();
                self.users.remove_if(user_id.as_str(), |_, socket_ids| {
                    socket_ids.remove(socket_id);
                    socket_ids.is_empty()
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Namespace;
    use crate::app::{App, AppManager, AppPolicy};
    use crate::namespace::SocketInitOptions;
    use crate::websocket::SocketId;
    use async_trait::async_trait;
    use sockudo_protocol::{ProtocolVersion, WireFormat};
    use sockudo_ws::Config as WsConfig;
    use sockudo_ws::Http1;
    use sockudo_ws::WebSocketStream;
    use sockudo_ws::axum_integration::{WebSocket, WebSocketWriter};
    use sockudo_ws::client::WebSocketClient;
    use std::sync::Arc;
    use tokio::net::{TcpListener, TcpStream};

    #[derive(Clone)]
    struct TestAppManager {
        app: App,
    }

    #[async_trait]
    impl AppManager for TestAppManager {
        async fn init(&self) -> crate::error::Result<()> {
            Ok(())
        }

        async fn create_app(&self, _config: App) -> crate::error::Result<()> {
            Ok(())
        }

        async fn update_app(&self, _config: App) -> crate::error::Result<()> {
            Ok(())
        }

        async fn delete_app(&self, _app_id: &str) -> crate::error::Result<()> {
            Ok(())
        }

        async fn get_apps(&self) -> crate::error::Result<Vec<App>> {
            Ok(vec![self.app.clone()])
        }

        async fn find_by_id(&self, app_id: &str) -> crate::error::Result<Option<App>> {
            Ok((app_id == self.app.id).then(|| self.app.clone()))
        }

        async fn find_by_key(&self, key: &str) -> crate::error::Result<Option<App>> {
            Ok((key == self.app.key).then(|| self.app.clone()))
        }

        async fn check_health(&self) -> crate::error::Result<()> {
            Ok(())
        }
    }

    async fn create_server_writer() -> WebSocketWriter {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = sockudo_ws::handshake::server_handshake(&mut stream)
                .await
                .unwrap();
            let ws = WebSocket::from_tcp(stream, WsConfig::default());
            let (_reader, writer) = ws.split();
            writer
        });

        let client_stream = TcpStream::connect(local_addr).await.unwrap();
        let client = WebSocketClient::<Http1>::new(WsConfig::default());
        let (_client_ws, _): (WebSocketStream<sockudo_ws::Stream<Http1>>, _) = client
            .connect(client_stream, &local_addr.to_string(), "/", None)
            .await
            .unwrap();

        server_task.await.unwrap()
    }

    #[test]
    fn exact_channel_matching_uses_exact_and_wildcard_memberships_only() {
        let namespace = Namespace::new("app".to_string());
        let exact_socket = SocketId::new();
        let wildcard_socket = SocketId::new();
        let unrelated_socket = SocketId::new();

        namespace.add_channel_to_socket("room-42", &exact_socket);
        namespace.add_channel_to_socket("room-*", &wildcard_socket);
        namespace.add_channel_to_socket("other-room", &unrelated_socket);

        let matches = namespace.get_matching_channel_socket_ids_except("room-42", None);

        assert!(matches.contains(&exact_socket));
        assert!(matches.contains(&wildcard_socket));
        assert!(!matches.contains(&unrelated_socket));
    }

    #[test]
    fn wildcard_index_is_removed_when_last_subscription_leaves() {
        let namespace = Namespace::new("app".to_string());
        let socket_id = SocketId::new();

        namespace.add_channel_to_socket("room-*", &socket_id);
        assert!(namespace.wildcard_channels.contains("room-*"));

        namespace.remove_channel_from_socket("room-*", &socket_id);

        assert!(!namespace.wildcard_channels.contains("room-*"));
    }

    #[tokio::test]
    async fn wildcard_only_subscription_matches_concrete_channel_in_partitioned_lookup() {
        let namespace = Namespace::new("app".to_string());
        let wildcard_socket = SocketId::new();
        let writer = create_server_writer().await;
        let app_manager: Arc<dyn AppManager + Send + Sync> = Arc::new(TestAppManager {
            app: App::from_policy(
                "app".to_string(),
                "app-key".to_string(),
                "app-secret".to_string(),
                true,
                AppPolicy::default(),
            ),
        });

        namespace
            .add_socket(
                wildcard_socket,
                writer,
                app_manager,
                SocketInitOptions {
                    buffer_config: crate::websocket::WebSocketBufferConfig::default(),
                    protocol_version: ProtocolVersion::V2,
                    wire_format: WireFormat::Json,
                    append_mode: sockudo_protocol::AppendMode::Delta,
                    echo_messages: true,
                },
            )
            .await
            .unwrap();

        namespace.add_channel_to_socket("room-*", &wildcard_socket);

        let (v1, v2) =
            namespace.get_matching_channel_socket_refs_partitioned_except("room-42", None);

        assert!(v1.is_empty());
        assert_eq!(v2.len(), 1);
        assert_eq!(*v2[0].get_socket_id_sync(), wildcard_socket);
    }

    #[test]
    fn remove_connection_clears_all_channel_membership() {
        let namespace = Namespace::new("app".to_string());
        let socket_id = SocketId::new();

        namespace.add_channel_to_socket("room-1", &socket_id);
        namespace.add_channel_to_socket("room-2", &socket_id);
        namespace.add_channel_to_socket("presence-x", &socket_id);

        namespace.remove_connection(&socket_id);

        assert!(!namespace.is_in_channel("room-1", &socket_id));
        assert!(!namespace.is_in_channel("room-2", &socket_id));
        assert!(!namespace.is_in_channel("presence-x", &socket_id));
    }

    #[test]
    fn add_reports_activation_only_on_first_socket() {
        let namespace = Namespace::new("app".to_string());
        let s1 = SocketId::new();
        let s2 = SocketId::new();

        let (newly1, activated1) = namespace.add_channel_to_socket("presence-room", &s1);
        assert!(newly1);
        assert!(activated1, "first socket must activate the channel");

        let (newly2, activated2) = namespace.add_channel_to_socket("presence-room", &s2);
        assert!(newly2);
        assert!(!activated2, "second socket must not re-activate");

        let (newly1_again, activated1_again) =
            namespace.add_channel_to_socket("presence-room", &s1);
        assert!(!newly1_again, "re-subscribe is not a new insertion");
        assert!(!activated1_again, "re-subscribe must not activate");
    }

    #[test]
    fn remove_reports_vacation_only_on_last_socket() {
        let namespace = Namespace::new("app".to_string());
        let s1 = SocketId::new();
        let s2 = SocketId::new();
        namespace.add_channel_to_socket("presence-room", &s1);
        namespace.add_channel_to_socket("presence-room", &s2);

        let (removed1, vacated1) = namespace.remove_channel_from_socket("presence-room", &s1);
        assert!(removed1);
        assert!(!vacated1, "channel still has a socket, must not vacate");

        let (removed2, vacated2) = namespace.remove_channel_from_socket("presence-room", &s2);
        assert!(removed2);
        assert!(vacated2, "last socket leaving must vacate the channel");

        let (removed_absent, vacated_absent) =
            namespace.remove_channel_from_socket("presence-room", &s1);
        assert!(!removed_absent, "socket already gone");
        assert!(!vacated_absent, "absent channel must not report vacation");
    }

    // Under concurrent churn on a shared channel, activations and vacations must
    // stay balanced: each entry is created once and deleted once, so the gauge
    // never goes negative.
    #[test]
    fn concurrent_churn_keeps_activation_and_vacation_balanced() {
        use std::sync::atomic::{AtomicI64, Ordering};

        let namespace = Arc::new(Namespace::new("app".to_string()));
        let channel = "presence-shared";
        let activations = Arc::new(AtomicI64::new(0));
        let vacations = Arc::new(AtomicI64::new(0));

        let threads = 16;
        let iterations = 2_000;

        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let namespace = Arc::clone(&namespace);
                let activations = Arc::clone(&activations);
                let vacations = Arc::clone(&vacations);
                std::thread::spawn(move || {
                    for _ in 0..iterations {
                        let socket_id = SocketId::new();
                        let (_, activated) = namespace.add_channel_to_socket(channel, &socket_id);
                        if activated {
                            activations.fetch_add(1, Ordering::Relaxed);
                        }
                        let (_, vacated) =
                            namespace.remove_channel_from_socket(channel, &socket_id);
                        if vacated {
                            vacations.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(
            activations.load(Ordering::Relaxed),
            vacations.load(Ordering::Relaxed),
            "every activation must pair with exactly one vacation (gauge returns to zero)"
        );
        assert_eq!(
            namespace.get_channel_socket_count(channel),
            0,
            "channel must be empty after all sockets leave"
        );
    }

    fn assert_user_index_bidirectional(namespace: &Namespace) {
        for entry in namespace.socket_users.iter() {
            let socket_id = entry.key();
            for user_id_entry in entry.value().iter() {
                let user_id = user_id_entry.key();
                assert!(
                    namespace
                        .users
                        .get(user_id.as_str())
                        .is_some_and(|s| s.contains(socket_id)),
                    "socket_users[{socket_id}] contains {user_id} but users[{user_id}] does not contain {socket_id}"
                );
            }
        }
        for entry in namespace.users.iter() {
            let user_id = entry.key();
            for socket_id_entry in entry.value().iter() {
                let socket_id = socket_id_entry.key();
                assert!(
                    namespace
                        .socket_users
                        .get(socket_id)
                        .is_some_and(|u| u.contains(user_id.as_str())),
                    "users[{user_id}] contains {socket_id} but socket_users[{socket_id}] does not contain {user_id}"
                );
            }
        }
    }

    #[tokio::test]
    async fn live_user_association_resolves_through_sockets() {
        let namespace = Namespace::new("app".to_string());
        let socket_id = SocketId::new();
        let writer = create_server_writer().await;
        let app_manager: Arc<dyn AppManager + Send + Sync> = Arc::new(TestAppManager {
            app: App::from_policy(
                "app".to_string(),
                "app-key".to_string(),
                "app-secret".to_string(),
                true,
                AppPolicy::default(),
            ),
        });

        let ws_ref = namespace
            .add_socket(
                socket_id,
                writer,
                app_manager,
                SocketInitOptions {
                    buffer_config: crate::websocket::WebSocketBufferConfig::default(),
                    protocol_version: ProtocolVersion::V2,
                    wire_format: WireFormat::Json,
                    append_mode: sockudo_protocol::AppendMode::Delta,
                    echo_messages: true,
                },
            )
            .await
            .unwrap();

        {
            let mut conn = ws_ref.inner.lock().await;
            conn.state.user_id = Some("user-1".to_string());
        }

        namespace.add_user(ws_ref.clone()).await.unwrap();

        assert!(namespace.users.get("user-1").is_some());
        assert!(namespace.users.get("user-1").unwrap().contains(&socket_id));
        assert!(namespace.socket_users.get(&socket_id).is_some());
        assert!(
            namespace
                .socket_users
                .get(&socket_id)
                .unwrap()
                .contains("user-1")
        );

        let resolved = namespace.get_user_sockets("user-1").unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(*resolved[0].get_socket_id_sync(), socket_id);

        assert_user_index_bidirectional(&namespace);
    }

    #[tokio::test]
    async fn late_user_association_rolls_back_when_socket_gone() {
        let namespace = Namespace::new("app".to_string());
        let socket_id = SocketId::new();
        let writer = create_server_writer().await;
        let app_manager: Arc<dyn AppManager + Send + Sync> = Arc::new(TestAppManager {
            app: App::from_policy(
                "app".to_string(),
                "app-key".to_string(),
                "app-secret".to_string(),
                true,
                AppPolicy::default(),
            ),
        });

        let ws_ref = namespace
            .add_socket(
                socket_id,
                writer,
                app_manager,
                SocketInitOptions {
                    buffer_config: crate::websocket::WebSocketBufferConfig::default(),
                    protocol_version: ProtocolVersion::V2,
                    wire_format: WireFormat::Json,
                    append_mode: sockudo_protocol::AppendMode::Delta,
                    echo_messages: true,
                },
            )
            .await
            .unwrap();

        {
            let mut conn = ws_ref.inner.lock().await;
            conn.state.user_id = Some("user-late".to_string());
        }

        namespace.sockets.remove(&socket_id);

        namespace.add_user(ws_ref.clone()).await.unwrap();

        assert!(
            namespace.users.get("user-late").is_none(),
            "user entry should have been rolled back"
        );
        assert!(
            namespace.socket_users.get(&socket_id).is_none(),
            "socket_users entry should have been rolled back"
        );

        let resolved = namespace.get_user_sockets("user-late").unwrap();
        assert!(resolved.is_empty());

        assert_user_index_bidirectional(&namespace);
    }

    #[tokio::test]
    async fn remove_user_socket_association_cleans_both_indexes() {
        let namespace = Namespace::new("app".to_string());
        let s1 = SocketId::new();
        let s2 = SocketId::new();
        let writer1 = create_server_writer().await;
        let writer2 = create_server_writer().await;
        let app_manager: Arc<dyn AppManager + Send + Sync> = Arc::new(TestAppManager {
            app: App::from_policy(
                "app".to_string(),
                "app-key".to_string(),
                "app-secret".to_string(),
                true,
                AppPolicy::default(),
            ),
        });

        let ws1 = namespace
            .add_socket(
                s1,
                writer1,
                app_manager.clone(),
                SocketInitOptions {
                    buffer_config: crate::websocket::WebSocketBufferConfig::default(),
                    protocol_version: ProtocolVersion::V2,
                    wire_format: WireFormat::Json,
                    append_mode: sockudo_protocol::AppendMode::Delta,
                    echo_messages: true,
                },
            )
            .await
            .unwrap();

        let ws2 = namespace
            .add_socket(
                s2,
                writer2,
                app_manager,
                SocketInitOptions {
                    buffer_config: crate::websocket::WebSocketBufferConfig::default(),
                    protocol_version: ProtocolVersion::V2,
                    wire_format: WireFormat::Json,
                    append_mode: sockudo_protocol::AppendMode::Delta,
                    echo_messages: true,
                },
            )
            .await
            .unwrap();

        {
            let mut conn = ws1.inner.lock().await;
            conn.state.user_id = Some("shared-user".to_string());
        }
        {
            let mut conn = ws2.inner.lock().await;
            conn.state.user_id = Some("shared-user".to_string());
        }

        namespace.add_user(ws1.clone()).await.unwrap();
        namespace.add_user(ws2.clone()).await.unwrap();
        assert_user_index_bidirectional(&namespace);

        assert_eq!(namespace.users.get("shared-user").unwrap().len(), 2);

        namespace.remove_user_socket_association("shared-user", &s1);

        assert_eq!(namespace.users.get("shared-user").unwrap().len(), 1);
        assert!(namespace.users.get("shared-user").unwrap().contains(&s2));
        assert!(namespace.socket_users.get(&s1).is_none());
        assert_user_index_bidirectional(&namespace);

        namespace.remove_user_socket_association("shared-user", &s2);

        assert!(namespace.users.get("shared-user").is_none());
        assert!(namespace.socket_users.get(&s2).is_none());
        assert_user_index_bidirectional(&namespace);
    }

    #[tokio::test]
    async fn remove_socket_from_all_users_cleans_all_associations() {
        let namespace = Namespace::new("app".to_string());
        let socket_id = SocketId::new();
        let writer = create_server_writer().await;
        let app_manager: Arc<dyn AppManager + Send + Sync> = Arc::new(TestAppManager {
            app: App::from_policy(
                "app".to_string(),
                "app-key".to_string(),
                "app-secret".to_string(),
                true,
                AppPolicy::default(),
            ),
        });

        let ws_ref = namespace
            .add_socket(
                socket_id,
                writer,
                app_manager,
                SocketInitOptions {
                    buffer_config: crate::websocket::WebSocketBufferConfig::default(),
                    protocol_version: ProtocolVersion::V2,
                    wire_format: WireFormat::Json,
                    append_mode: sockudo_protocol::AppendMode::Delta,
                    echo_messages: true,
                },
            )
            .await
            .unwrap();

        {
            let mut conn = ws_ref.inner.lock().await;
            conn.state.user_id = Some("user-a".to_string());
        }
        namespace.add_user(ws_ref.clone()).await.unwrap();

        namespace
            .socket_users
            .entry(socket_id)
            .or_default()
            .insert("user-b".to_string());
        namespace
            .users
            .entry("user-b".to_string())
            .or_default()
            .insert(socket_id);

        assert_user_index_bidirectional(&namespace);

        namespace.remove_socket_from_all_users(&socket_id);

        assert!(namespace.socket_users.get(&socket_id).is_none());
        assert!(namespace.users.get("user-a").is_none());
        assert!(namespace.users.get("user-b").is_none());
        assert_user_index_bidirectional(&namespace);
    }

    #[tokio::test]
    async fn exhaustive_teardown_removes_all_state() {
        use crate::channel::PresenceMemberInfo;

        let namespace = Namespace::new("app".to_string());
        let socket_id = SocketId::new();
        let writer = create_server_writer().await;
        let app_manager: Arc<dyn AppManager + Send + Sync> = Arc::new(TestAppManager {
            app: App::from_policy(
                "app".to_string(),
                "app-key".to_string(),
                "app-secret".to_string(),
                true,
                AppPolicy::default(),
            ),
        });

        let ws_ref = namespace
            .add_socket(
                socket_id,
                writer,
                app_manager,
                SocketInitOptions {
                    buffer_config: crate::websocket::WebSocketBufferConfig::default(),
                    protocol_version: ProtocolVersion::V2,
                    wire_format: WireFormat::Json,
                    append_mode: sockudo_protocol::AppendMode::Delta,
                    echo_messages: true,
                },
            )
            .await
            .unwrap();

        // Two user IDs
        {
            let mut conn = ws_ref.inner.lock().await;
            conn.state.user_id = Some("user-alpha".to_string());
        }
        namespace.add_user(ws_ref.clone()).await.unwrap();
        namespace
            .socket_users
            .entry(socket_id)
            .or_default()
            .insert("user-beta".to_string());
        namespace
            .users
            .entry("user-beta".to_string())
            .or_default()
            .insert(socket_id);

        // Two channels (one wildcard)
        namespace.add_channel_to_socket("presence-room", &socket_id);
        namespace.add_channel_to_socket("events-*", &socket_id);

        // Presence data
        let mut per_socket = ahash::AHashMap::new();
        per_socket.insert(
            "presence-room".to_string(),
            PresenceMemberInfo {
                user_id: "user-alpha".to_string(),
                user_info: None,
            },
        );
        namespace.presence_data.insert(socket_id, per_socket);

        let token = ws_ref.shutdown_token.clone();

        // Act
        namespace.remove_connection(&socket_id);

        // Assert: authoritative map
        assert!(namespace.sockets.get(&socket_id).is_none());

        // Assert: token cancelled
        assert!(token.is_cancelled());

        // Assert: user indexes
        assert!(namespace.users.get("user-alpha").is_none());
        assert!(namespace.users.get("user-beta").is_none());
        assert!(namespace.socket_users.get(&socket_id).is_none());

        // Assert: channel indexes
        assert!(!namespace.is_in_channel("presence-room", &socket_id));
        assert!(!namespace.is_in_channel("events-*", &socket_id));
        assert!(namespace.socket_channels.get(&socket_id).is_none());
        assert!(!namespace.wildcard_channels.contains("events-*"));

        // Assert: presence data
        assert!(namespace.presence_data.get(&socket_id).is_none());

        assert_user_index_bidirectional(&namespace);
    }

    #[tokio::test]
    async fn duplicate_removal_is_harmless() {
        let namespace = Namespace::new("app".to_string());
        let socket_id = SocketId::new();
        let writer = create_server_writer().await;
        let app_manager: Arc<dyn AppManager + Send + Sync> = Arc::new(TestAppManager {
            app: App::from_policy(
                "app".to_string(),
                "app-key".to_string(),
                "app-secret".to_string(),
                true,
                AppPolicy::default(),
            ),
        });

        let ws_ref = namespace
            .add_socket(
                socket_id,
                writer,
                app_manager,
                SocketInitOptions {
                    buffer_config: crate::websocket::WebSocketBufferConfig::default(),
                    protocol_version: ProtocolVersion::V2,
                    wire_format: WireFormat::Json,
                    append_mode: sockudo_protocol::AppendMode::Delta,
                    echo_messages: true,
                },
            )
            .await
            .unwrap();

        {
            let mut conn = ws_ref.inner.lock().await;
            conn.state.user_id = Some("user-dup".to_string());
        }
        namespace.add_user(ws_ref.clone()).await.unwrap();
        namespace.add_channel_to_socket("ch-1", &socket_id);

        // First removal: comprehensive teardown
        namespace.remove_connection(&socket_id);

        // Second removal: must be a no-op, no panic
        namespace.remove_connection(&socket_id);

        assert!(namespace.sockets.get(&socket_id).is_none());
        assert!(namespace.users.get("user-dup").is_none());
        assert!(namespace.socket_users.get(&socket_id).is_none());
        assert!(namespace.socket_channels.get(&socket_id).is_none());
        assert!(namespace.presence_data.get(&socket_id).is_none());
        assert!(!namespace.is_in_channel("ch-1", &socket_id));
    }

    /// Covers the ordering where association completes before authoritative
    /// removal. Teardown must drain both user indexes without retaining the
    /// connection.
    #[tokio::test]
    async fn add_user_before_remove_connection_drains_indexes() {
        let namespace = Namespace::new("app".to_string());
        let socket_id = SocketId::new();
        let writer = create_server_writer().await;
        let app_manager: Arc<dyn AppManager + Send + Sync> = Arc::new(TestAppManager {
            app: App::from_policy(
                "app".to_string(),
                "app-key".to_string(),
                "app-secret".to_string(),
                true,
                AppPolicy::default(),
            ),
        });

        let ws_ref = namespace
            .add_socket(
                socket_id,
                writer,
                app_manager,
                SocketInitOptions {
                    buffer_config: crate::websocket::WebSocketBufferConfig::default(),
                    protocol_version: ProtocolVersion::V2,
                    wire_format: WireFormat::Json,
                    append_mode: sockudo_protocol::AppendMode::Delta,
                    echo_messages: true,
                },
            )
            .await
            .unwrap();

        {
            let mut conn = ws_ref.inner.lock().await;
            conn.state.user_id = Some("alice".to_string());
        }
        let token = ws_ref.shutdown_token.clone();

        namespace.add_user(ws_ref.clone()).await.unwrap();
        assert!(namespace.users.get("alice").unwrap().contains(&socket_id));
        assert!(
            namespace
                .socket_users
                .get(&socket_id)
                .unwrap()
                .contains("alice")
        );

        namespace.remove_connection(&socket_id);

        assert!(
            namespace.users.is_empty(),
            "users must be empty after removal"
        );
        assert!(
            namespace.socket_users.is_empty(),
            "socket_users must be empty after removal"
        );
        assert!(
            namespace.sockets.is_empty(),
            "sockets must be empty after removal"
        );
        assert!(token.is_cancelled(), "shutdown token must be cancelled");
        assert_user_index_bidirectional(&namespace);
    }

    /// Race scenario 2: `remove_connection` completes first (removes socket from
    /// the authoritative map), and only then `add_user` runs. `add_user` sees
    /// `sockets.contains_key` = false at its pre-check and returns early without
    /// inserting anything.
    ///
    /// Uses a oneshot channel to enforce strict ordering: `remove_connection`
    /// signals completion, then `add_user` proceeds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remove_connection_before_add_user_prevents_insertion() {
        use std::time::Duration;

        let namespace = Arc::new(Namespace::new("app".to_string()));
        let socket_id = SocketId::new();
        let writer = create_server_writer().await;
        let app_manager: Arc<dyn AppManager + Send + Sync> = Arc::new(TestAppManager {
            app: App::from_policy(
                "app".to_string(),
                "app-key".to_string(),
                "app-secret".to_string(),
                true,
                AppPolicy::default(),
            ),
        });

        let ws_ref = namespace
            .add_socket(
                socket_id,
                writer,
                app_manager,
                SocketInitOptions {
                    buffer_config: crate::websocket::WebSocketBufferConfig::default(),
                    protocol_version: ProtocolVersion::V2,
                    wire_format: WireFormat::Json,
                    append_mode: sockudo_protocol::AppendMode::Delta,
                    echo_messages: true,
                },
            )
            .await
            .unwrap();

        {
            let mut conn = ws_ref.inner.lock().await;
            conn.state.user_id = Some("alice".to_string());
        }

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let ns_rm = Arc::clone(&namespace);
        let remove_task = tokio::spawn(async move {
            ns_rm.remove_connection(&socket_id);
            tx.send(()).expect("signal send failed");
        });

        let ns_add = Arc::clone(&namespace);
        let ws_add = ws_ref.clone();
        let add_task = tokio::spawn(async move {
            rx.await.expect("signal recv failed");
            ns_add.add_user(ws_add).await.unwrap();
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            let (rr, ra) = tokio::join!(remove_task, add_task);
            rr.expect("remove_connection task panicked");
            ra.expect("add_user task panicked");
        })
        .await
        .expect("test timed out after 5s");

        assert!(
            namespace.users.is_empty(),
            "users must be empty when remove_connection runs first"
        );
        assert!(
            namespace.socket_users.is_empty(),
            "socket_users must be empty when remove_connection runs first"
        );
        assert!(
            namespace.sockets.is_empty(),
            "sockets must be empty after remove_connection"
        );
        assert_user_index_bidirectional(&namespace);
    }

    /// Churn test: N sockets all associated with user "alice", each concurrently
    /// added via `add_user` then removed via `remove_connection`. Final state must
    /// be completely clean: no user entries, no socket_users entries, no sockets,
    /// and all shutdown tokens cancelled.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_add_remove_churn_leaves_empty_indexes() {
        use std::time::Duration;
        use tokio::task::JoinSet;

        let n: usize = 16;
        let namespace = Arc::new(Namespace::new("app".to_string()));
        let app_manager: Arc<dyn AppManager + Send + Sync> = Arc::new(TestAppManager {
            app: App::from_policy(
                "app".to_string(),
                "app-key".to_string(),
                "app-secret".to_string(),
                true,
                AppPolicy::default(),
            ),
        });

        let mut socket_pairs = Vec::with_capacity(n);
        for _ in 0..n {
            let socket_id = SocketId::new();
            let writer = create_server_writer().await;
            let ws_ref = namespace
                .add_socket(
                    socket_id,
                    writer,
                    app_manager.clone(),
                    SocketInitOptions {
                        buffer_config: crate::websocket::WebSocketBufferConfig::default(),
                        protocol_version: ProtocolVersion::V2,
                        wire_format: WireFormat::Json,
                        append_mode: sockudo_protocol::AppendMode::Delta,
                        echo_messages: true,
                    },
                )
                .await
                .unwrap();
            {
                let mut conn = ws_ref.inner.lock().await;
                conn.state.user_id = Some("alice".to_string());
            }
            socket_pairs.push((socket_id, ws_ref));
        }

        let tokens: Vec<_> = socket_pairs
            .iter()
            .map(|(_, ws)| ws.shutdown_token.clone())
            .collect();

        let barrier = Arc::new(tokio::sync::Barrier::new(n));
        let mut join_set = JoinSet::new();

        for (socket_id, ws_ref) in socket_pairs {
            let ns = Arc::clone(&namespace);
            let bar = Arc::clone(&barrier);
            join_set.spawn(async move {
                bar.wait().await;
                ns.add_user(ws_ref).await.unwrap();
                ns.remove_connection(&socket_id);
            });
        }

        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(res) = join_set.join_next().await {
                res.expect("churn task panicked");
            }
        })
        .await
        .expect("churn test timed out after 5s");

        assert!(
            namespace.users.is_empty(),
            "users must be empty after churn"
        );
        assert!(
            namespace.socket_users.is_empty(),
            "socket_users must be empty after churn"
        );
        assert!(
            namespace.sockets.is_empty(),
            "sockets must be empty after churn"
        );
        for (i, token) in tokens.iter().enumerate() {
            assert!(token.is_cancelled(), "shutdown token {i} must be cancelled");
        }
        assert_user_index_bidirectional(&namespace);
    }
}
