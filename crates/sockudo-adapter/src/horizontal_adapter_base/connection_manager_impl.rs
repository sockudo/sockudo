use super::*;

#[async_trait]
impl<T: HorizontalTransport + 'static> ConnectionManager for HorizontalAdapterBase<T>
where
    T::Config: TransportConfig,
{
    async fn init(&self) {
        self.local_adapter.init().await;

        if let Err(e) = self.start_listeners().await {
            error!(error = %e, "failed to start transport listeners");
        }
    }

    async fn get_namespace(&self, app_id: &str) -> Option<Arc<Namespace>> {
        self.local_adapter.get_namespace(app_id).await
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
        append_mode: sockudo_protocol::AppendMode,
    ) -> Result<()> {
        self.local_adapter
            .add_socket(
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

    async fn get_connection(&self, socket_id: &SocketId, app_id: &str) -> Option<WebSocketRef> {
        self.local_adapter.get_connection(socket_id, app_id).await
    }

    async fn remove_connection(&self, socket_id: &SocketId, app_id: &str) -> Result<()> {
        self.local_adapter
            .remove_connection(socket_id, app_id)
            .await
    }

    async fn send_message(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        message: PusherMessage,
    ) -> Result<()> {
        self.local_adapter
            .send_message(app_id, socket_id, message)
            .await
    }

    async fn send(
        &self,
        channel: &str,
        message: PusherMessage,
        except: Option<&SocketId>,
        app_id: &str,
        start_time_ms: Option<f64>,
    ) -> Result<()> {
        // Check if delta compression is available and configured for this channel
        #[cfg(feature = "delta")]
        if let (Some(delta_compression), Some(app_manager)) =
            (&self.delta_compression, &self.app_manager)
        {
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
                            start_time_ms,
                            crate::connection_manager::CompressionParams {
                                delta_compression: Arc::clone(delta_compression),
                                channel_settings: channel_settings.as_ref(),
                            },
                        )
                        .await;
                }
            }
        }

        // Fall back to regular sending without delta compression
        // Send locally first (tracked in connection manager for metrics)
        let node_id = self.horizontal.node_id.clone();

        let local_result = self
            .local_adapter
            .send(channel, message.clone(), except, app_id, start_time_ms)
            .await;

        if let Err(e) = local_result {
            warn!(channel = %channel, error = %e, "local send failed");
        }

        // Broadcast to other nodes
        let message_json = sonic_rs::to_string(&message)?;
        let broadcast = BroadcastMessage {
            node_id,
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            message: message_json,
            except_socket_id: except.map(|id| id.to_string()),
            timestamp_ms: start_time_ms.or_else(|| {
                Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as f64
                        / 1_000_000.0, // Convert to milliseconds with microsecond precision
                )
            }),
            compression_metadata: None,
            idempotency_key: message.idempotency_key.clone(),
            ephemeral: message.is_ephemeral(),
        };

        // Skip broadcasting to other nodes if we're in single-node mode
        if !self.should_skip_horizontal_communication().await {
            self.transport.publish_broadcast(&broadcast).await?;
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
        start_time_ms: Option<f64>,
        compression: crate::connection_manager::CompressionParams<'_>,
    ) -> Result<()> {
        // Send locally first with delta compression support
        let (node_id, local_result) = {
            let result = self
                .horizontal
                .local_adapter
                .send_with_compression(
                    channel,
                    message.clone(),
                    except,
                    app_id,
                    start_time_ms,
                    crate::connection_manager::CompressionParams {
                        delta_compression: compression.delta_compression.clone(),
                        channel_settings: compression.channel_settings,
                    },
                )
                .await;
            (self.horizontal.node_id.clone(), result)
        };

        if let Err(e) = local_result {
            warn!(channel = %channel, error = %e, "local send with compression failed");
        }

        // Broadcast to other nodes with compression metadata
        // Other nodes will apply their own delta compression using this metadata
        let message_json = sonic_rs::to_string(&message)?;

        // Extract conflation key from channel settings
        let conflation_key = compression
            .channel_settings
            .and_then(|s| s.conflation_key.clone());

        // Extract event name from message for tracking
        let event_name = message.event.as_deref().map(|s| s.to_string());

        // Check cluster coordination for synchronized full message intervals
        let (cluster_should_send_full, cluster_delta_count) = if compression
            .delta_compression
            .has_cluster_coordination()
        {
            if let Some(ck) = conflation_key.as_ref() {
                // Use cluster coordination to determine if we should send full message
                match compression
                    .delta_compression
                    .check_cluster_interval(app_id, channel, ck)
                    .await
                {
                    Ok((should_send_full, count)) => {
                        debug!(
                            should_send_full = should_send_full,
                            count = count,
                            app_id = %app_id,
                            channel = %channel,
                            conflation_key = %ck,
                            "cluster coordination check"
                        );
                        (Some(should_send_full), Some(count))
                    }
                    Err(e) => {
                        warn!(error = %e, "cluster coordination failed, falling back to node-local");
                        (None, None)
                    }
                }
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        // For horizontal broadcasts, we send full messages and let each node
        // decide whether to apply delta compression based on its local state.
        // If cluster coordination is enabled, we use the cluster-wide decision.
        let is_full_message = cluster_should_send_full.unwrap_or(true);

        let broadcast = BroadcastMessage {
            node_id,
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            message: message_json,
            except_socket_id: except.map(|id| id.to_string()),
            timestamp_ms: start_time_ms.or_else(|| {
                Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as f64
                        / 1_000_000.0,
                )
            }),
            compression_metadata: Some(crate::horizontal_adapter::CompressionMetadata {
                conflation_key,
                enabled: true,
                sequence: cluster_delta_count, // Cluster-wide sequence if coordination enabled
                is_full_message, // Determined by cluster coordination or defaults to true
                event_name,
            }),
            idempotency_key: message.idempotency_key.clone(),
            ephemeral: message.is_ephemeral(),
        };

        // Skip broadcasting to other nodes if we're in single-node mode
        if !self.should_skip_horizontal_communication().await {
            self.transport.publish_broadcast(&broadcast).await?;
        }

        Ok(())
    }

    async fn get_channel_members(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        // Get local members
        let mut members = self
            .local_adapter
            .get_channel_members(app_id, channel)
            .await?;

        // Get distributed members
        let response = self
            .send_request(
                app_id,
                RequestType::ChannelMembers,
                Some(channel),
                None,
                None,
            )
            .await?;

        members.extend(response.members);
        Ok(members)
    }

    async fn get_local_channel_members(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        let mut members = self
            .local_adapter
            .get_channel_members(app_id, channel)
            .await?;

        {
            let registry = self.horizontal.cluster_presence_registry.read().await;
            for (node_id, node_data) in registry.iter() {
                if node_id == &self.node_id {
                    continue;
                }
                if let Some(channel_sockets) = node_data.get(channel) {
                    for entry in channel_sockets.values() {
                        if entry.app_id != app_id {
                            continue;
                        }
                        members.entry(entry.user_id.clone()).or_insert_with(|| {
                            PresenceMemberInfo {
                                user_id: entry.user_id.clone(),
                                user_info: entry
                                    .user_info
                                    .as_ref()
                                    .map(|info| info.as_ref().clone()),
                            }
                        });
                    }
                }
            }
        }

        Ok(members)
    }

    async fn get_channel_sockets(&self, app_id: &str, channel: &str) -> Result<Vec<SocketId>> {
        // Get local sockets
        let mut all_socket_ids = self
            .local_adapter
            .get_channel_sockets(app_id, channel)
            .await?;

        // Get remote sockets
        let response = self
            .send_request(
                app_id,
                RequestType::ChannelSockets,
                Some(channel),
                None,
                None,
            )
            .await?;

        for socket_id in response.socket_ids {
            if let Ok(sid) = SocketId::from_string(&socket_id) {
                all_socket_ids.push(sid);
            }
        }

        Ok(all_socket_ids)
    }

    async fn remove_channel(&self, app_id: &str, channel: &str) {
        self.local_adapter.remove_channel(app_id, channel).await;
    }

    async fn is_in_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        // Check locally first
        let local_result = self
            .local_adapter
            .is_in_channel(app_id, channel, socket_id)
            .await?;

        if local_result {
            return Ok(true);
        }

        // Check other nodes
        let response = self
            .send_request(
                app_id,
                RequestType::SocketExistsInChannel,
                Some(channel),
                Some(&socket_id.to_string()),
                None,
            )
            .await?;

        Ok(response.exists)
    }

    async fn get_user_sockets(&self, user_id: &str, app_id: &str) -> Result<Vec<WebSocketRef>> {
        self.local_adapter.get_user_sockets(user_id, app_id).await
    }

    async fn cleanup_connection(&self, app_id: &str, ws: WebSocketRef) {
        self.local_adapter.cleanup_connection(app_id, ws).await;
    }

    async fn terminate_connection(&self, app_id: &str, user_id: &str) -> Result<()> {
        // Terminate locally
        self.local_adapter
            .terminate_user_connections(app_id, user_id)
            .await?;

        // Broadcast termination to other nodes
        let _response = self
            .send_request(
                app_id,
                RequestType::TerminateUserConnections,
                None,
                None,
                Some(user_id),
            )
            .await?;

        Ok(())
    }

    async fn add_channel_to_sockets(&self, app_id: &str, channel: &str, socket_id: &SocketId) {
        self.local_adapter
            .add_channel_to_sockets(app_id, channel, socket_id)
            .await;
    }

    async fn get_channel_socket_count_info(
        &self,
        app_id: &str,
        channel: &str,
    ) -> crate::connection_manager::ChannelSocketCount {
        // Get local count
        let local_count = self
            .local_adapter
            .get_channel_socket_count(app_id, channel)
            .await;

        // Tier 1A: read peer contributions from the gossiped registry instead of
        // cross-node request/reply.
        if self.aggregate_counts {
            return crate::connection_manager::ChannelSocketCount {
                count: local_count + self.horizontal.remote_channel_count(app_id, channel),
                complete: true,
            };
        }

        // Get distributed count
        match self
            .send_request(
                app_id,
                RequestType::ChannelSocketsCount,
                Some(channel),
                None,
                None,
            )
            .await
        {
            Ok(response) => crate::connection_manager::ChannelSocketCount {
                count: local_count + response.sockets_count,
                complete: response.complete,
            },
            Err(e) => {
                error!(error = %e, "failed to get remote channel socket count");
                crate::connection_manager::ChannelSocketCount {
                    count: local_count,
                    complete: false,
                }
            }
        }
    }

    async fn get_channel_socket_count(&self, app_id: &str, channel: &str) -> usize {
        self.get_channel_socket_count_info(app_id, channel)
            .await
            .count
    }

    async fn get_local_channel_socket_count(&self, app_id: &str, channel: &str) -> usize {
        self.local_adapter
            .get_channel_socket_count(app_id, channel)
            .await
    }

    async fn get_batch_channel_socket_counts(
        &self,
        app_id: &str,
        channels: &[&str],
    ) -> Result<HashMap<String, usize>> {
        // Get local counts (no cross-node communication)
        let mut counts: HashMap<String, usize> = HashMap::new();
        for ch in channels {
            let c = self
                .local_adapter
                .get_channel_socket_count(app_id, ch)
                .await;
            if c > 0 {
                counts.insert(ch.to_string(), c);
            }
        }

        // Tier 1A: read peer contributions from the gossiped registry instead of
        // publishing a request/reply batch during unsubscribe cleanup.
        if self.aggregate_counts {
            for ch in channels {
                let remote_count = self.horizontal.remote_channel_count(app_id, ch);
                if remote_count > 0 {
                    *counts.entry((*ch).to_string()).or_insert(0) += remote_count;
                }
            }
            return Ok(counts);
        }

        if self.should_skip_horizontal_communication().await {
            return Ok(counts);
        }

        // Single batched request for all channels
        let request = RequestBody {
            request_id: Uuid::new_v4().to_string(),
            node_id: self.horizontal.node_id.clone(),
            app_id: app_id.to_string(),
            request_type: RequestType::BatchChannelSocketsCount,
            channel: None,
            socket_id: None,
            user_id: None,
            user_info: None,
            timestamp: None,
            dead_node_id: None,
            target_node_id: None,
            reply_to: None,
            channels: Some(channels.iter().map(|c| c.to_string()).collect()),
        };

        let response = self.send_request_with_body(request).await?;

        // Merge remote counts into local
        for (ch, count) in response.channels_with_sockets_count {
            *counts.entry(ch).or_insert(0) += count;
        }

        Ok(counts)
    }

    async fn add_to_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<(bool, bool)> {
        // Fast path: direct local adapter access without locking horizontal
        self.local_adapter
            .add_to_channel(app_id, channel, socket_id)
            .await
    }

    async fn remove_from_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<(bool, bool)> {
        // Fast path: direct local adapter access without locking horizontal
        self.local_adapter
            .remove_from_channel(app_id, channel, socket_id)
            .await
    }

    async fn get_presence_member(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<PresenceMemberInfo> {
        self.local_adapter
            .get_presence_member(app_id, channel, socket_id)
            .await
    }

    async fn update_presence_member(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
        user_info: sonic_rs::Value,
    ) -> Result<Option<PresenceMemberInfo>> {
        self.local_adapter
            .update_presence_member(app_id, channel, socket_id, user_info)
            .await
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
        self.local_adapter
            .mark_presence_member_pending(
                app_id, channel, user_id, socket_id, user_info, generation,
            )
            .await
    }

    async fn cancel_pending_presence_member(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
    ) -> Result<Option<String>> {
        self.local_adapter
            .cancel_pending_presence_member(app_id, channel, user_id)
            .await
    }

    async fn remove_pending_presence_member(
        &self,
        app_id: &str,
        channel: &str,
        user_id: &str,
        generation: u64,
    ) -> Result<Option<PresenceMemberInfo>> {
        self.local_adapter
            .remove_pending_presence_member(app_id, channel, user_id, generation)
            .await
    }

    async fn terminate_user_connections(&self, app_id: &str, user_id: &str) -> Result<()> {
        self.terminate_connection(app_id, user_id).await
    }

    async fn force_reconnect_user(&self, app_id: &str, user_id: &str) -> Result<()> {
        self.local_adapter
            .force_reconnect_user(app_id, user_id)
            .await?;

        let _response = self
            .send_request(
                app_id,
                RequestType::ForceReconnect,
                None,
                None,
                Some(user_id),
            )
            .await?;

        Ok(())
    }

    async fn add_user(&self, ws_ref: WebSocketRef) -> Result<()> {
        self.local_adapter.add_user(ws_ref).await
    }

    async fn remove_user(&self, ws_ref: WebSocketRef) -> Result<()> {
        self.local_adapter.remove_user(ws_ref).await
    }

    async fn remove_user_socket(
        &self,
        user_id: &str,
        socket_id: &SocketId,
        app_id: &str,
    ) -> Result<()> {
        self.local_adapter
            .remove_user_socket(user_id, socket_id, app_id)
            .await
    }

    async fn count_user_connections_in_channel(
        &self,
        user_id: &str,
        app_id: &str,
        channel: &str,
        excluding_socket: Option<&SocketId>,
    ) -> Result<usize> {
        // Get local count (with excluding_socket filter)
        let local_count = self
            .local_adapter
            .count_user_connections_in_channel(user_id, app_id, channel, excluding_socket)
            .await?;

        if self.fast_presence_transitions && channel.starts_with("presence-") {
            return Ok(local_count
                + self
                    .horizontal
                    .remote_presence_user_connection_count(app_id, channel, user_id)
                    .await);
        }

        // Get remote count (no excluding_socket since it's local-only)
        match self
            .send_request(
                app_id,
                RequestType::CountUserConnectionsInChannel,
                Some(channel),
                None,
                Some(user_id),
            )
            .await
        {
            Ok(response) => Ok(local_count + response.sockets_count),
            Err(e) => {
                error!(error = %e, "failed to get remote user connections count");
                Ok(local_count)
            }
        }
    }

    async fn user_has_connections_in_channel(
        &self,
        user_id: &str,
        app_id: &str,
        channel: &str,
        excluding_socket: Option<&SocketId>,
    ) -> Result<bool> {
        let local_count = self
            .local_adapter
            .count_user_connections_in_channel(user_id, app_id, channel, excluding_socket)
            .await?;

        if local_count > 0 {
            return Ok(true);
        }

        if self.fast_presence_transitions && channel.starts_with("presence-") {
            return Ok(self
                .horizontal
                .remote_presence_user_has_connections(app_id, channel, user_id)
                .await);
        }

        match self
            .send_request(
                app_id,
                RequestType::CountUserConnectionsInChannel,
                Some(channel),
                None,
                Some(user_id),
            )
            .await
        {
            Ok(response) => Ok(response.sockets_count > 0),
            Err(e) => {
                error!(error = %e, "failed to get remote user connections count");
                Ok(false)
            }
        }
    }

    async fn get_channels_with_socket_count(&self, app_id: &str) -> Result<HashMap<String, usize>> {
        // Get local channels
        let mut channels = self
            .local_adapter
            .get_channels_with_socket_count(app_id)
            .await?;

        // Tier 1A: merge peer counts from the gossiped registry, no fan-out.
        if self.aggregate_counts {
            for (channel, count) in self.horizontal.remote_channels_with_counts(app_id) {
                *channels.entry(channel).or_insert(0) += count;
            }
            return Ok(channels);
        }

        // Get distributed channels
        match self
            .send_request(
                app_id,
                RequestType::ChannelsWithSocketsCount,
                None,
                None,
                None,
            )
            .await
        {
            Ok(response) => {
                for (channel, count) in response.channels_with_sockets_count {
                    *channels.entry(channel).or_insert(0) += count;
                }
            }
            Err(e) => {
                error!(error = %e, "failed to get remote channels with socket count");
            }
        }

        Ok(channels)
    }

    async fn get_sockets_count(&self, app_id: &str) -> Result<usize> {
        // Always keep the local count available for per-node quota checks.
        let local_count = self.local_adapter.get_sockets_count(app_id).await?;

        // Disabling socket counting only skips the cross-node request/reply path.
        if !self.enable_socket_counting {
            return Ok(local_count);
        }

        // Get distributed count
        match self
            .send_request(app_id, RequestType::SocketsCount, None, None, None)
            .await
        {
            Ok(response) => Ok(local_count + response.sockets_count),
            Err(e) => {
                error!(error = %e, "failed to get remote socket count");
                Ok(local_count)
            }
        }
    }

    async fn get_all_connections(&self, app_id: &str) -> Result<Vec<SocketId>> {
        Ok(self.local_adapter.get_all_connections(app_id).await)
    }

    async fn get_namespaces(&self) -> Result<Vec<(String, Arc<Namespace>)>> {
        self.local_adapter.get_namespaces().await
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    async fn check_health(&self) -> Result<()> {
        self.transport.check_health().await
    }

    async fn announce_node_departure(&self) -> Result<()> {
        let request = RequestBody {
            request_id: generate_request_id(),
            node_id: self.node_id.clone(),
            app_id: "cluster".to_string(),
            request_type: RequestType::NodeDead,
            channel: None,
            socket_id: None,
            user_id: None,
            user_info: None,
            timestamp: Some(current_timestamp()),
            dead_node_id: Some(self.node_id.clone()),
            target_node_id: None,
            reply_to: None,
            channels: None,
        };

        match tokio::time::timeout(
            Duration::from_secs(2),
            self.transport.publish_request(&request),
        )
        .await
        {
            Ok(Ok(())) => {
                info!("Announced node departure to cluster peers");
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(())
            }
            Ok(Err(e)) => {
                warn!(error = %e, "failed to announce node departure");
                Ok(())
            }
            Err(_) => {
                warn!("Node departure announcement timed out");
                Ok(())
            }
        }
    }

    fn get_node_id(&self) -> String {
        self.node_id.clone()
    }

    fn as_horizontal_adapter(&self) -> Option<&dyn HorizontalAdapterInterface> {
        Some(self)
    }

    fn configure_dead_node_events(&self) -> Option<DeadNodeEventBusReceiver> {
        let (event_sender, event_receiver) = mpsc::unbounded_async();
        self.set_event_bus(event_sender);
        Some(event_receiver)
    }
}
