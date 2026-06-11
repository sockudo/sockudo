use super::LocalAdapter;
use super::helpers::*;
use crate::ConnectionManager;
use ahash::AHashMap as HashMap;
use async_trait::async_trait;
use dashmap::DashMap;
use sockudo_core::app::AppManager;
use sockudo_core::channel::PresenceMemberInfo;
use sockudo_core::error::{Error, Result};
use sockudo_core::namespace::{Namespace, SocketInitOptions};
use sockudo_core::websocket::{SocketId, WebSocketRef};
use sockudo_protocol::messages::{PusherMessage, generate_message_id};
use sockudo_ws::axum_integration::WebSocketWriter;
use std::any::Any;
use std::sync::Arc;
use tracing::{debug, error, info};

#[async_trait]
impl ConnectionManager for LocalAdapter {
    async fn init(&self) {
        info!("Initializing local adapter");
    }

    async fn get_namespace(&self, app_id: &str) -> Option<Arc<Namespace>> {
        self.existing_namespace(app_id)
    }

    async fn add_socket(
        &self,
        socket_id: SocketId,
        socket: WebSocketWriter,
        app_id: &str,
        app_manager: Arc<dyn AppManager + Send + Sync>,
        buffer_config: sockudo_core::websocket::WebSocketBufferConfig,
        protocol_version: sockudo_protocol::ProtocolVersion,
        wire_format: sockudo_protocol::WireFormat,
        echo_messages: bool,
    ) -> Result<()> {
        debug!(
            "LocalAdapter::add_socket: adding socket {} for app {}",
            &socket_id, app_id
        );
        let namespace = self.get_or_create_namespace(app_id).await;
        let socket_id_clone = socket_id;
        namespace
            .add_socket(
                socket_id,
                socket,
                app_manager,
                SocketInitOptions {
                    buffer_config,
                    protocol_version,
                    wire_format,
                    echo_messages,
                },
            )
            .await?;
        debug!(
            "LocalAdapter::add_socket: successfully added socket {} for app {}",
            socket_id_clone, app_id
        );
        Ok(())
    }

    // Updated to return WebSocketRef instead of Arc<Mutex<WebSocket>>
    async fn get_connection(&self, socket_id: &SocketId, app_id: &str) -> Option<WebSocketRef> {
        debug!(
            "LocalAdapter::get_connection: looking for socket {} in app {}",
            socket_id, app_id
        );
        let result = self
            .existing_namespace(app_id)
            .and_then(|namespace| namespace.get_connection(socket_id));
        debug!(
            "LocalAdapter::get_connection: socket {} in app {} found: {}",
            socket_id,
            app_id,
            result.is_some()
        );
        result
    }

    async fn remove_connection(&self, socket_id: &SocketId, app_id: &str) -> Result<()> {
        if let Some(namespace) = self.namespaces.get(app_id) {
            namespace.remove_connection(socket_id);
            Ok(())
        } else {
            Err(Error::Connection("Namespace not found".to_string()))
        }
    }

    async fn send_message(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        message: PusherMessage,
    ) -> Result<()> {
        let connection = self
            .get_connection(socket_id, app_id)
            .await
            .ok_or_else(|| Error::Connection("Connection not found".to_string()))?;

        match connection.protocol_version {
            sockudo_protocol::ProtocolVersion::V2 => {
                let mut rewritten = message;
                if Self::should_assign_v2_message_id(&rewritten) {
                    rewritten.message_id = Some(generate_message_id());
                }
                rewritten.rewrite_prefix(sockudo_protocol::ProtocolVersion::V2);
                connection.send_message(&rewritten).await
            }
            sockudo_protocol::ProtocolVersion::V1 => match Self::v1_compatible_message(&message) {
                Some(v1_msg) => connection.send_message(&v1_msg).await,
                None => Ok(()),
            },
        }
    }

    async fn send(
        &self,
        channel: &str,
        message: PusherMessage,
        except: Option<&SocketId>,
        app_id: &str,
        _start_time_ms: Option<f64>,
    ) -> Result<()> {
        debug!(
            channel = %channel,
            event = message.event.as_deref(),
            has_data = message.data.is_some(),
            has_extras = message.extras.is_some(),
            has_idempotency_key = message.idempotency_key.is_some(),
            "Sending message to channel"
        );

        // Check if delta compression is available (lock-free read via OnceLock)
        #[cfg(feature = "delta")]
        {
            let delta_compression = self.delta_compression.get();
            let app_manager = self.app_manager.get();

            if let (Some(delta_compression), Some(app_manager)) = (delta_compression, app_manager) {
                // Get app config to check for channel-specific delta settings
                if let Ok(Some(app)) = app_manager.find_by_id(app_id).await {
                    // Get channel-specific delta compression settings
                    let channel_settings = app
                        .channel_delta_compression_ref()
                        .and_then(|map| map.get(channel))
                        .and_then(|config| {
                            use sockudo_delta::ChannelDeltaConfig;
                            match config {
                                ChannelDeltaConfig::Full(settings) => Some(settings.clone()),
                                _ => None,
                            }
                        });

                    // Use compression-aware sending if we have settings with conflation key
                    if channel_settings
                        .as_ref()
                        .and_then(|s| s.conflation_key.as_ref())
                        .is_some()
                    {
                        return self
                            .send_with_compression(
                                channel,
                                message,
                                except,
                                app_id,
                                _start_time_ms,
                                crate::connection_manager::CompressionParams {
                                    delta_compression: Arc::clone(delta_compression),
                                    channel_settings: channel_settings.as_ref(),
                                },
                            )
                            .await;
                    }
                }
            }
        }

        // Fall back to regular sending without delta compression
        let Some(namespace) = self.get_namespace(app_id).await else {
            return Ok(());
        };

        // Get target socket references based on channel type
        let (v1_all_sockets, v2_target_sockets) = if channel.starts_with("#server-to-user-") {
            let user_id = channel.trim_start_matches("#server-to-user-");
            let socket_refs = namespace.get_user_sockets(user_id).await?;

            let mut target_refs = Vec::new();
            for socket_ref in socket_refs.iter() {
                let socket_id = socket_ref.get_socket_id_sync();
                if except != Some(socket_id) {
                    target_refs.push(socket_ref.clone());
                }
            }
            target_refs
                .into_iter()
                .partition(|s| s.protocol_version == sockudo_protocol::ProtocolVersion::V1)
        } else {
            namespace.get_matching_channel_socket_refs_partitioned_except(channel, except)
        };

        if !is_v2_only_protocol_event(&message) {
            self.send_to_v1_sockets(v1_all_sockets, &message).await?;
        }

        let mut filtered_socket_refs = v2_target_sockets;
        self.filter_v2_sockets_in_place(
            channel,
            &message,
            &mut filtered_socket_refs,
            except,
            &namespace,
        );
        filter_annotation_subscribers_in_place(channel, &message, &mut filtered_socket_refs);
        crate::v2_broadcast::apply_event_name_filter_in_place(
            channel,
            &message,
            &mut filtered_socket_refs,
        );
        self.split_rewind_gated_sockets_in_place(channel, &message, &mut filtered_socket_refs)
            .await;

        // Send to filtered V2 sockets (Sockudo-native: sockudo: prefix, serial + message_id)
        if !filtered_socket_refs.is_empty() {
            let (v2_message, _v2_bytes) = crate::v2_broadcast::prepare_v2_message(message)?;
            Self::log_send_errors(
                self.send_protocol_messages_concurrent(filtered_socket_refs, v2_message)
                    .await,
            );
        }

        Ok(())
    }

    #[cfg(feature = "delta")]
    async fn send_with_compression(
        &self,
        channel: &str,
        message: PusherMessage,
        except: Option<&SocketId>,
        app_id: &str,
        _start_time_ms: Option<f64>,
        compression: crate::connection_manager::CompressionParams<'_>,
    ) -> Result<()> {
        let delta_compression = compression.delta_compression;
        let channel_settings = compression.channel_settings;
        debug!(
            channel = %channel,
            event = message.event.as_deref(),
            has_data = message.data.is_some(),
            has_extras = message.extras.is_some(),
            has_idempotency_key = message.idempotency_key.is_some(),
            "Sending message to channel with compression support"
        );

        let Some(namespace) = self.get_namespace(app_id).await else {
            return Ok(());
        };

        // Get target socket references based on channel type
        let (v1_all_sockets, v2_target_sockets) = if channel.starts_with("#server-to-user-") {
            let user_id = channel.trim_start_matches("#server-to-user-");
            let socket_refs = namespace.get_user_sockets(user_id).await?;

            let mut target_refs = Vec::new();
            for socket_ref in socket_refs.iter() {
                let socket_id = socket_ref.get_socket_id_sync();
                if except != Some(socket_id) {
                    target_refs.push(socket_ref.clone());
                }
            }
            target_refs
                .into_iter()
                .partition(|s| s.protocol_version == sockudo_protocol::ProtocolVersion::V1)
        } else {
            namespace.get_matching_channel_socket_refs_partitioned_except(channel, except)
        };

        if !is_v2_annotation_protocol_event(&message) {
            self.send_to_v1_sockets(v1_all_sockets, &message).await?;
        }

        let mut filtered_socket_refs = v2_target_sockets;
        self.filter_v2_sockets_strict_in_place(
            channel,
            &message,
            &mut filtered_socket_refs,
            except,
            &namespace,
        );
        filter_annotation_subscribers_in_place(channel, &message, &mut filtered_socket_refs);
        crate::v2_broadcast::apply_event_name_filter_in_place(
            channel,
            &message,
            &mut filtered_socket_refs,
        );
        self.split_rewind_gated_sockets_in_place(channel, &message, &mut filtered_socket_refs)
            .await;

        let message = self.maybe_strip_tags(message, channel_settings);

        // V2 sockets get delta compression
        if !filtered_socket_refs.is_empty() {
            let mut v2_message = message;
            v2_message.rewrite_prefix(sockudo_protocol::ProtocolVersion::V2);
            v2_message.idempotency_key = None;
            let v2_event_name = v2_message.event.as_deref().unwrap_or("").to_string();
            let v2_bytes = sonic_rs::to_vec(&v2_message)
                .map_err(|e| Error::InvalidMessageFormat(format!("Serialization failed: {e}")))?;
            Self::log_send_errors(
                self.send_messages_with_compression(
                    filtered_socket_refs,
                    v2_message,
                    v2_bytes,
                    channel,
                    &v2_event_name,
                    crate::connection_manager::CompressionParams {
                        delta_compression,
                        channel_settings,
                    },
                )
                .await,
            );
        }

        Ok(())
    }

    async fn get_channel_members(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        let mut members = match self.existing_namespace(app_id) {
            Some(namespace) => namespace.get_channel_members(channel).await,
            None => Ok(HashMap::new()),
        }?;

        let channel_key = pending_presence_channel_key(app_id, channel);
        if let Some(index) = self.pending_presence_by_channel.get(&channel_key) {
            for pending_key in index.iter() {
                let Some(pending) = self.pending_presence_members.get(pending_key.key()) else {
                    continue;
                };
                members
                    .entry(pending.member.user_id.clone())
                    .or_insert_with(|| pending.member.clone());
            }
        }

        Ok(members)
    }

    async fn get_channel_sockets(&self, app_id: &str, channel: &str) -> Result<Vec<SocketId>> {
        Ok(self
            .existing_namespace(app_id)
            .map(|namespace| namespace.get_channel_sockets(channel))
            .unwrap_or_default())
    }

    async fn remove_channel(&self, app_id: &str, channel: &str) {
        // MEMORY LEAK FIX: Clear filter index entries for this channel
        // This must happen before removing the channel from namespace
        #[cfg(feature = "tag-filtering")]
        self.filter_index.clear_channel(channel);

        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.remove_channel(channel);

        if namespace.sockets.is_empty()
            && namespace.channels.is_empty()
            && namespace.users.is_empty()
        {
            self.namespaces.remove(app_id);
            debug!(
                "Removed empty namespace for app_id: {} after channel removal",
                app_id
            );
        }
    }

    async fn is_in_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        Ok(self
            .existing_namespace(app_id)
            .is_some_and(|namespace| namespace.is_in_channel(channel, socket_id)))
    }

    async fn get_user_sockets(&self, user_id: &str, app_id: &str) -> Result<Vec<WebSocketRef>> {
        match self.existing_namespace(app_id) {
            Some(namespace) => namespace.get_user_sockets(user_id).await,
            None => Ok(Vec::new()),
        }
    }

    async fn cleanup_connection(&self, app_id: &str, ws: WebSocketRef) {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.cleanup_connection(ws).await;

        if namespace.sockets.is_empty()
            && namespace.channels.is_empty()
            && namespace.users.is_empty()
        {
            self.namespaces.remove(app_id);
            debug!("Removed empty namespace for app_id: {}", app_id);
        }
    }

    async fn terminate_connection(&self, app_id: &str, user_id: &str) -> Result<()> {
        let namespace = self.get_or_create_namespace(app_id).await;
        if let Err(e) = namespace.terminate_user_connections(user_id).await {
            error!("Failed to terminate adapter: {}", e);
        }
        Ok(())
    }

    async fn add_channel_to_sockets(&self, app_id: &str, channel: &str, socket_id: &SocketId) {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.add_channel_to_socket(channel, socket_id);
    }

    async fn get_channel_socket_count_info(
        &self,
        app_id: &str,
        channel: &str,
    ) -> crate::connection_manager::ChannelSocketCount {
        crate::connection_manager::ChannelSocketCount {
            count: self.get_channel_socket_count(app_id, channel).await,
            complete: true,
        }
    }

    async fn get_channel_socket_count(&self, app_id: &str, channel: &str) -> usize {
        self.existing_namespace(app_id)
            .map(|namespace| namespace.get_channel_socket_count(channel))
            .unwrap_or(0)
    }

    async fn add_to_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        let t_start = std::time::Instant::now();
        let t_before_ns = t_start.elapsed().as_micros();
        let namespace = self.get_or_create_namespace(app_id).await;
        let t_after_ns = t_start.elapsed().as_micros();

        let t_before_add = t_start.elapsed().as_micros();
        let result = namespace.add_channel_to_socket(channel, socket_id);
        let t_after_add = t_start.elapsed().as_micros();

        debug!(
            "PERF[LOCAL_ADD_CHAN] channel={} socket={} total={}μs get_ns={}μs add={}μs",
            channel,
            socket_id,
            t_after_add,
            t_after_ns - t_before_ns,
            t_after_add - t_before_add
        );

        Ok(result)
    }

    async fn remove_from_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        let namespace = self.get_or_create_namespace(app_id).await;

        // MEMORY LEAK FIX: Clean up filter index BEFORE removing from channel
        // Get the socket's filter for this channel so we can remove it from the index
        #[cfg(feature = "tag-filtering")]
        if let Some(socket_ref) = namespace.sockets.get(socket_id) {
            let filter_node = socket_ref.get_channel_filter_sync(channel);
            self.filter_index
                .remove_socket_filter(channel, *socket_id, filter_node.as_deref());
            // Also remove from the socket's channel_filters map
            socket_ref.channel_filters.remove(channel);
        }

        Ok(namespace.remove_channel_from_socket(channel, socket_id))
    }

    async fn get_presence_member(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<PresenceMemberInfo> {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.get_presence_member(channel, socket_id).await
    }

    async fn update_presence_member(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
        user_info: sonic_rs::Value,
    ) -> Result<Option<PresenceMemberInfo>> {
        let namespace = self.get_or_create_namespace(app_id).await;
        Ok(namespace
            .update_presence_member(channel, socket_id, user_info)
            .await)
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
        let pending_key = pending_presence_key(app_id, channel, user_id);
        self.pending_presence_members.insert(
            pending_key.clone(),
            PendingPresenceMember {
                member: PresenceMemberInfo {
                    user_id: user_id.to_string(),
                    user_info,
                },
                socket_id: socket_id.to_string(),
                generation,
            },
        );
        self.pending_presence_by_channel
            .entry(pending_presence_channel_key(app_id, channel))
            .or_insert_with(|| Arc::new(DashMap::new()))
            .insert(pending_key, ());
        Ok(())
    }

    async fn cancel_pending_presence_member(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
    ) -> Result<Option<String>> {
        let pending_key = pending_presence_key(app_id, channel, user_id);
        let removed = self
            .pending_presence_members
            .remove(&pending_key)
            .map(|(_, pending)| pending.socket_id);
        self.remove_pending_presence_index_entry(app_id, channel, &pending_key);
        Ok(removed)
    }

    async fn remove_pending_presence_member(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
        generation: u64,
    ) -> Result<Option<PresenceMemberInfo>> {
        let key = pending_presence_key(app_id, channel, user_id);
        let removed = self
            .pending_presence_members
            .remove_if(&key, |_, pending| pending.generation == generation)
            .map(|(_, pending)| pending.member);
        if removed.is_some() {
            self.remove_pending_presence_index_entry(app_id, channel, &key);
        }
        Ok(removed)
    }

    async fn terminate_user_connections(&self, app_id: &str, user_id: &str) -> Result<()> {
        let namespace = self.get_or_create_namespace(app_id).await;
        if let Err(e) = namespace.terminate_user_connections(user_id).await {
            error!("Failed to terminate user connections: {}", e);
        }
        Ok(())
    }

    // Updated to use WebSocketRef
    async fn add_user(&self, ws_ref: WebSocketRef) -> Result<()> {
        // Get app_id using WebSocketRef async method
        let app_id = {
            let ws_guard = ws_ref.inner.lock().await;
            ws_guard.state.get_app_id()
        };
        let namespace = self.get_or_create_namespace(&app_id).await;
        namespace.add_user(ws_ref).await
    }

    // Updated to use WebSocketRef
    async fn remove_user(&self, ws_ref: WebSocketRef) -> Result<()> {
        // Get app_id using WebSocketRef async method
        let app_id = {
            let ws_guard = ws_ref.inner.lock().await;
            ws_guard.state.get_app_id()
        };
        match self.existing_namespace(&app_id) {
            Some(namespace) => namespace.remove_user(ws_ref).await,
            None => Ok(()),
        }
    }

    async fn remove_user_socket(
        &self,
        user_id: &str,
        socket_id: &SocketId,
        app_id: &str,
    ) -> Result<()> {
        match self.existing_namespace(app_id) {
            Some(namespace) => namespace.remove_user_socket(user_id, socket_id).await,
            None => Ok(()),
        }
    }

    async fn count_user_connections_in_channel(
        &self,
        user_id: &str,
        app_id: &str,
        channel: &str,
        excluding_socket: Option<&SocketId>,
    ) -> Result<usize> {
        match self.existing_namespace(app_id) {
            Some(namespace) => {
                namespace
                    .count_user_connections_in_channel(user_id, channel, excluding_socket)
                    .await
            }
            None => Ok(0),
        }
    }

    async fn get_channels_with_socket_count(&self, app_id: &str) -> Result<HashMap<String, usize>> {
        match self.existing_namespace(app_id) {
            Some(namespace) => namespace.get_channels_with_socket_count().await,
            None => Ok(HashMap::new()),
        }
    }

    async fn get_sockets_count(&self, app_id: &str) -> Result<usize> {
        if let Some(namespace) = self.namespaces.get(app_id) {
            let count = namespace.sockets.len();
            Ok(count)
        } else {
            Ok(0) // No namespace means no sockets
        }
    }

    async fn get_all_connections(&self, app_id: &str) -> Result<Vec<SocketId>> {
        Ok(LocalAdapter::get_all_connections(self, app_id).await)
    }

    async fn get_namespaces(&self) -> Result<Vec<(String, Arc<Namespace>)>> {
        Ok(self
            .namespaces
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect())
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    async fn check_health(&self) -> Result<()> {
        // Local adapter is always healthy since it's in-memory
        Ok(())
    }

    fn get_node_id(&self) -> String {
        // Local adapter doesn't have a node ID concept (single node)
        "local".to_string()
    }

    fn as_horizontal_adapter(
        &self,
    ) -> Option<&dyn crate::connection_manager::HorizontalAdapterInterface> {
        // Local adapter doesn't support horizontal scaling
        None
    }
}
