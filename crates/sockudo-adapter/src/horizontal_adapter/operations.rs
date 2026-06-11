use super::*;
use crate::ConnectionManager;
use sockudo_core::error::{Error, Result};
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};

impl HorizontalAdapter {
    /// Create a new horizontal adapter
    pub fn new() -> Self {
        Self {
            node_id: Uuid::new_v4().to_string(),
            local_adapter: Arc::new(LocalAdapter::new()),
            pending_requests: Arc::new(DashMap::new()),
            requests_timeout: AtomicU64::new(5000),
            metrics: OnceLock::new(),
            cluster_presence_registry: Arc::new(RwLock::new(AHashMap::new())),
            node_heartbeats: Arc::new(RwLock::new(AHashMap::new())),
            sequence_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn set_metrics(&self, metrics: Arc<dyn MetricsInterface + Send + Sync>) {
        let _ = self.metrics.set(metrics);
    }

    /// Start the request cleanup task
    pub fn start_request_cleanup(&self) {
        let timeout = self.requests_timeout.load(Ordering::Relaxed);
        let pending_requests = Arc::clone(&self.pending_requests);

        // Spawn a background task to clean up stale requests
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(1000)).await;

                // Find and process expired requests
                let now = Instant::now();
                let mut expired_requests = Vec::new();

                // We can't modify pending_requests while iterating
                for entry in pending_requests.iter() {
                    let request_id = entry.key();
                    let request = entry.value();
                    if now.duration_since(request.start_time).as_millis() > timeout as u128 {
                        expired_requests.push(request_id.clone());
                    }
                }

                // Process expired requests
                for request_id in expired_requests {
                    warn!("{}", format!("Request {} expired", request_id));
                    if let Some((_, request)) = pending_requests.remove(&request_id) {
                        request.notify.notify_waiters();
                    }
                }
            }
        });
    }

    /// Process a received request from another node
    pub async fn process_request(&self, request: RequestBody) -> Result<ResponseBody> {
        debug!(
            "{}",
            format!(
                "Processing request from node {}: {:?}",
                request.node_id, request.request_type
            )
        );

        // Skip processing our own requests
        if request.node_id == self.node_id {
            return Err(Error::OwnRequestIgnored);
        }

        // Track metrics for received request
        if let Some(metrics) = self.metrics.get() {
            metrics.mark_horizontal_adapter_request_received(&request.app_id);
        }

        // Initialize empty response
        let mut response = ResponseBody {
            request_id: request.request_id.clone(),
            node_id: self.node_id.clone(),
            app_id: request.app_id.clone(),
            members: AHashMap::new(),
            socket_ids: Vec::new(),
            sockets_count: 0,
            channels_with_sockets_count: AHashMap::new(),
            exists: false,
            channels: HashSet::new(),
            members_count: 0,
            responses_received: 0,
            expected_responses: 0,
            complete: true,
        };

        // Process based on request type
        match request.request_type {
            RequestType::ChannelMembers => {
                if let Some(channel) = &request.channel {
                    // Get channel members from local adapter
                    let members = self
                        .local_adapter
                        .get_channel_members(&request.app_id, channel)
                        .await?;
                    response.members = members;
                }
            }
            RequestType::ChannelSockets => {
                if let Some(channel) = &request.channel {
                    // Get channel sockets from local adapter (returns Vec now)
                    let channel_sockets = self
                        .local_adapter
                        .get_channel_sockets(&request.app_id, channel)
                        .await?;
                    response.socket_ids = channel_sockets
                        .into_iter()
                        .map(|socket_id| socket_id.to_string())
                        .collect();
                }
            }
            RequestType::ChannelSocketsCount => {
                if let Some(channel) = &request.channel {
                    // Get channel socket count from local adapter
                    response.sockets_count = self
                        .local_adapter
                        .get_channel_socket_count(&request.app_id, channel)
                        .await;
                }
            }
            RequestType::SocketExistsInChannel => {
                if let (Some(channel), Some(socket_id)) = (&request.channel, &request.socket_id) {
                    // Check if socket exists in channel
                    let socket_id =
                        SocketId::from_string(socket_id).map_err(Error::InvalidMessageFormat)?;
                    response.exists = self
                        .local_adapter
                        .is_in_channel(&request.app_id, channel, &socket_id)
                        .await?;
                }
            }
            RequestType::TerminateUserConnections => {
                if let Some(user_id) = &request.user_id {
                    // Terminate user connections locally
                    self.local_adapter
                        .terminate_user_connections(&request.app_id, user_id)
                        .await?;
                    response.exists = true;
                }
            }
            RequestType::ChannelsWithSocketsCount => {
                // Get channels with socket count from local adapter (returns HashMap now)
                let channels = self
                    .local_adapter
                    .get_channels_with_socket_count(&request.app_id)
                    .await?;
                response.channels_with_sockets_count = channels.into_iter().collect();
            }
            // New request types
            RequestType::Sockets => {
                // Get all connections for the app
                let connections = self
                    .local_adapter
                    .get_all_connections(&request.app_id)
                    .await;
                response.socket_ids = connections.iter().map(ToString::to_string).collect();
                response.sockets_count = connections.len();
            }
            RequestType::Channels => {
                // Get all channels for the app (returns HashMap now)
                let channels = self
                    .local_adapter
                    .get_channels_with_socket_count(&request.app_id)
                    .await?;
                response.channels = channels.into_keys().collect();
            }
            RequestType::SocketsCount => {
                // Get count of all sockets
                let connections = self
                    .local_adapter
                    .get_all_connections(&request.app_id)
                    .await;
                response.sockets_count = connections.len();
            }
            RequestType::ChannelMembersCount => {
                if let Some(channel) = &request.channel {
                    // Get count of members in a channel
                    let members = self
                        .local_adapter
                        .get_channel_members(&request.app_id, channel)
                        .await?;
                    response.members_count = members.len();
                }
            }
            RequestType::CountUserConnectionsInChannel => {
                if let (Some(user_id), Some(channel)) = (&request.user_id, &request.channel) {
                    // Count user's connections in the specific channel on this node
                    response.sockets_count = self
                        .local_adapter
                        .count_user_connections_in_channel(user_id, &request.app_id, channel, None)
                        .await?;
                }
            }
            // Presence replication handlers
            RequestType::PresenceMemberJoined => {
                if let (Some(channel), Some(user_id), Some(socket_id)) =
                    (&request.channel, &request.user_id, &request.socket_id)
                {
                    // Use the helper method to update cluster presence registry
                    self.add_presence_entry(
                        &request.node_id,
                        channel,
                        socket_id,
                        user_id,
                        &request.app_id,
                        request.user_info.clone(),
                    )
                    .await;
                }
            }
            RequestType::PresenceMemberLeft => {
                if let (Some(channel), Some(_user_id), Some(socket_id)) =
                    (&request.channel, &request.user_id, &request.socket_id)
                {
                    // Use the helper method to update cluster presence registry
                    self.remove_presence_entry(&request.node_id, channel, socket_id)
                        .await;
                }
            }
            RequestType::PresenceMemberUpdated => {
                if let (Some(channel), Some(user_id), Some(socket_id), Some(user_info)) = (
                    &request.channel,
                    &request.user_id,
                    &request.socket_id,
                    &request.user_info,
                ) {
                    self.update_presence_entry(
                        &request.node_id,
                        channel,
                        socket_id,
                        user_id,
                        &request.app_id,
                        user_info.clone(),
                    )
                    .await;
                }
            }
            RequestType::Heartbeat => {
                // Update heartbeat tracking (don't track our own heartbeat)
                if request.node_id != self.node_id {
                    let mut heartbeats = self.node_heartbeats.write().await;

                    // Atomically check and insert to avoid race conditions
                    let receive_time = Instant::now();
                    let is_new_node = match heartbeats.entry(request.node_id.clone()) {
                        Entry::Vacant(e) => {
                            e.insert(receive_time);
                            true
                        }
                        Entry::Occupied(mut e) => {
                            e.insert(receive_time);
                            false
                        }
                    };

                    if is_new_node {
                        info!("New node detected: {}", request.node_id);
                        // Note: State sync will be handled by the caller
                        // Return a special response that indicates new node detected
                        response.exists = true; // Use this field to signal new node
                    }

                    debug!(
                        "Received heartbeat from node: {} at local time: {:?} (new: {})",
                        request.node_id, receive_time, is_new_node
                    );
                }
            }
            RequestType::NodeDead => {
                if let Some(dead_node_id) = &request.dead_node_id {
                    debug!("Received dead node notification for: {}", dead_node_id);

                    // This message is only for followers to clean registry
                    // Leader already did full cleanup before sending this message

                    // Remove from heartbeat tracking
                    {
                        let mut heartbeats = self.node_heartbeats.write().await;
                        heartbeats.remove(dead_node_id);
                    }

                    // Clean up local presence registry only
                    self.cleanup_local_presence_registry(dead_node_id).await;
                }
            }
            RequestType::BatchChannelSocketsCount => {
                if let Some(channels) = &request.channels {
                    for channel in channels {
                        let count = self
                            .local_adapter
                            .get_channel_socket_count(&request.app_id, channel)
                            .await;
                        if count > 0 {
                            response
                                .channels_with_sockets_count
                                .insert(channel.clone(), count);
                        }
                    }
                }
            }
            RequestType::PresenceStateSync => {
                if let Some(presence_data) = request.user_info {
                    let deser_result = sonic_rs::to_vec(&presence_data).and_then(|bytes| {
                        sonic_rs::from_slice::<AHashMap<String, AHashMap<String, PresenceEntry>>>(
                            &bytes,
                        )
                    });
                    match deser_result {
                        Ok(incoming_node_data) => {
                            let mut registry = self.cluster_presence_registry.write().await;

                            // Get or create the sending node's entry
                            // Each node sends its own data, so different nodes update different keys
                            let node_registry = registry
                                .entry(request.node_id.clone())
                                .or_insert_with(AHashMap::new);

                            // Merge channel data
                            for (channel, incoming_sockets) in incoming_node_data {
                                let channel_registry = node_registry
                                    .entry(channel.clone())
                                    .or_insert_with(AHashMap::new);

                                // Merge socket entries, preferring newer data
                                for (socket_id, incoming_entry) in incoming_sockets {
                                    match channel_registry.get(&socket_id) {
                                        Some(existing)
                                            if existing.sequence_number
                                                > incoming_entry.sequence_number =>
                                        {
                                            // Keep existing (it's newer)
                                            debug!(
                                                "Keeping existing entry for socket {} (newer)",
                                                socket_id
                                            );
                                        }
                                        _ => {
                                            // Insert new or replace older
                                            channel_registry.insert(socket_id, incoming_entry);
                                        }
                                    }
                                }
                            }

                            info!(
                                "Merged presence state from node: {} ({} channels)",
                                request.node_id,
                                registry.get(&request.node_id).map(|d| d.len()).unwrap_or(0)
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to deserialize presence state from node {}: {}",
                                request.node_id, e
                            );
                        }
                    }
                }
            }
        }

        // Return the response
        Ok(response)
    }

    /// Process a response received from another node
    pub async fn process_response(&self, response: ResponseBody) -> Result<()> {
        // Track received response
        if let Some(metrics) = self.metrics.get() {
            metrics.mark_horizontal_adapter_response_received(&response.app_id);
        }

        // Get the pending request and notify waiters
        if let Some(mut request) = self.pending_requests.get_mut(&response.request_id) {
            // Add response to the list
            request.responses.push(response);

            // Notify any waiting send_request calls that a new response has arrived
            request.notify.notify_one();
        }

        Ok(())
    }

    /// Returns the responses and whether the wait timed out.
    pub(super) async fn wait_for_request_responses(
        &self,
        request_id: &str,
        start: Instant,
        timeout_duration: Duration,
        max_expected_responses: usize,
    ) -> (Vec<ResponseBody>, bool) {
        let deadline = start + timeout_duration;

        loop {
            let Some(notify) = self
                .pending_requests
                .get(request_id)
                .map(|request| Arc::clone(&request.notify))
            else {
                return (Vec::new(), false);
            };
            let notified = notify.notified();
            tokio::pin!(notified);

            let response_count = self
                .pending_requests
                .get(request_id)
                .map(|pending_request| pending_request.responses.len());

            match response_count {
                Some(count) if count >= max_expected_responses => {
                    debug!(
                        "Request {} completed successfully with {}/{} responses in {}ms",
                        request_id,
                        count,
                        max_expected_responses,
                        start.elapsed().as_millis()
                    );
                    let responses = self
                        .pending_requests
                        .remove(request_id)
                        .map(|(_, req)| req.responses)
                        .unwrap_or_default();
                    return (responses, false);
                }
                Some(_) => {}
                None => return (Vec::new(), false),
            }

            let now = Instant::now();
            if now >= deadline {
                let current_responses = self
                    .pending_requests
                    .get(request_id)
                    .map(|r| r.responses.len())
                    .unwrap_or(0);
                warn!(
                    "Request {} timed out after {}ms, got {} responses out of {} expected",
                    request_id,
                    start.elapsed().as_millis(),
                    current_responses,
                    max_expected_responses
                );
                let responses = self
                    .pending_requests
                    .remove(request_id)
                    .map(|(_, req)| req.responses)
                    .unwrap_or_default();
                return (responses, true);
            }

            let remaining = deadline.saturating_duration_since(now);
            if tokio::time::timeout(remaining, &mut notified)
                .await
                .is_err()
            {
                let current_responses = self
                    .pending_requests
                    .get(request_id)
                    .map(|r| r.responses.len())
                    .unwrap_or(0);
                warn!(
                    "Request {} timed out after {}ms, got {} responses out of {} expected",
                    request_id,
                    start.elapsed().as_millis(),
                    current_responses,
                    max_expected_responses
                );
                let responses = self
                    .pending_requests
                    .remove(request_id)
                    .map(|(_, req)| req.responses)
                    .unwrap_or_default();
                return (responses, true);
            }
        }
    }

    /// Send a request to other nodes and wait for responses
    pub async fn send_request(
        &mut self,
        app_id: &str,
        request_type: RequestType,
        channel: Option<&str>,
        socket_id: Option<&str>,
        user_id: Option<&str>,
        expected_node_count: usize,
    ) -> Result<ResponseBody> {
        let request_id = Uuid::new_v4().to_string();
        let start = Instant::now();

        // Create the request
        let _request = RequestBody {
            request_id: request_id.clone(),
            node_id: self.node_id.clone(),
            app_id: app_id.to_string(),
            request_type: request_type.clone(),
            channel: channel.map(String::from),
            socket_id: socket_id.map(String::from),
            user_id: user_id.map(String::from),
            // Cluster presence fields (not used for regular requests)
            user_info: None,
            timestamp: None,
            dead_node_id: None,
            target_node_id: None,
            reply_to: None,
            channels: None,
        };

        // Add to pending requests with proper initialization
        self.pending_requests.insert(
            request_id.clone(),
            PendingRequest {
                start_time: start,
                app_id: app_id.to_string(),
                responses: Vec::with_capacity(expected_node_count.saturating_sub(1)),
                notify: Arc::new(Notify::new()),
            },
        );

        // Track sent request in metrics
        if let Some(metrics) = self.metrics.get() {
            metrics.mark_horizontal_adapter_request_sent(app_id);
        }

        // ✅ IMPORTANT: Broadcasting is handled by the adapter implementation
        // For Redis: RedisAdapter publishes to request_channel in its listeners
        // For NATS: NatsAdapter would publish to NATS subjects
        // For HTTP: HttpAdapter would POST to other nodes
        debug!(
            "Request {} created for type {:?} on app {} - broadcasting handled by adapter",
            request_id, request_type, app_id
        );

        // Wait for responses with proper timeout handling
        let timeout_duration = Duration::from_millis(self.requests_timeout.load(Ordering::Relaxed));
        let max_expected_responses = expected_node_count.saturating_sub(1);

        // If we don't expect any responses (single node), return immediately
        if max_expected_responses == 0 {
            debug!(
                "Single node deployment, no responses expected for request {}",
                request_id
            );
            self.pending_requests.remove(&request_id);
            return Ok(ResponseBody {
                request_id: request_id.clone(),
                node_id: self.node_id.clone(),
                app_id: app_id.to_string(),
                members: AHashMap::new(),
                socket_ids: Vec::new(),
                sockets_count: 0,
                channels_with_sockets_count: AHashMap::new(),
                exists: false,
                channels: HashSet::new(),
                members_count: 0,
                responses_received: 0,
                expected_responses: 0,
                complete: true,
            });
        }

        let (responses, timed_out) = self
            .wait_for_request_responses(
                &request_id,
                start,
                timeout_duration,
                max_expected_responses,
            )
            .await;

        // Use the aggregation method
        let responses_received = responses.len();
        let complete = responses_received >= max_expected_responses;
        let combined_response = self.aggregate_responses(
            request_id.clone(),
            self.node_id.clone(),
            app_id.to_string(),
            &request_type,
            responses,
            AggregationStats {
                responses_received,
                expected_responses: max_expected_responses,
                complete,
            },
        );

        // Validate the aggregated response
        if let Err(e) = self.validate_aggregated_response(&combined_response, &request_type) {
            warn!(
                "Response validation failed for request {}: {}",
                request_id, e
            );
        }

        // Track metrics
        if let Some(metrics) = self.metrics.get() {
            let duration_ms = start.elapsed().as_micros() as f64 / 1000.0; // Convert to milliseconds with 3 decimal places

            metrics.track_horizontal_adapter_resolve_time(app_id, duration_ms);

            // Empty results are still resolved — only a timeout is uncomplete
            metrics.track_horizontal_adapter_resolved_promises(app_id, !timed_out);
        }

        Ok(combined_response)
    }

    pub fn aggregate_responses(
        &self,
        request_id: String,
        node_id: String,
        app_id: String,
        request_type: &RequestType,
        responses: Vec<ResponseBody>,
        stats: AggregationStats,
    ) -> ResponseBody {
        let mut combined_response = ResponseBody {
            request_id,
            node_id,
            app_id,
            members: AHashMap::new(),
            socket_ids: Vec::new(),
            sockets_count: 0,
            channels_with_sockets_count: AHashMap::new(),
            exists: false,
            channels: HashSet::new(),
            members_count: 0,
            responses_received: stats.responses_received,
            expected_responses: stats.expected_responses,
            complete: stats.complete,
        };

        if responses.is_empty() {
            return combined_response;
        }

        // Track unique socket IDs to avoid duplicates when aggregating
        let mut unique_socket_ids = HashSet::new();

        for response in responses {
            match request_type {
                RequestType::ChannelMembers => {
                    // Merge members - later responses can overwrite earlier ones with same user_id
                    // This handles the case where a user might be connected to multiple nodes
                    combined_response.members.extend(response.members);
                }

                RequestType::ChannelSockets => {
                    // Collect unique socket IDs across all nodes
                    for socket_id in response.socket_ids {
                        unique_socket_ids.insert(socket_id);
                    }
                }

                RequestType::ChannelSocketsCount => {
                    // Sum socket counts from all nodes
                    combined_response.sockets_count += response.sockets_count;
                }

                RequestType::SocketExistsInChannel => {
                    // If any node reports the socket exists, it exists
                    combined_response.exists = combined_response.exists || response.exists;
                }

                RequestType::TerminateUserConnections => {
                    // If any node successfully terminated connections, mark as success
                    combined_response.exists = combined_response.exists || response.exists;
                }

                RequestType::ChannelsWithSocketsCount => {
                    // FIXED: Actually add the socket counts instead of just inserting 0
                    for (channel, socket_count) in response.channels_with_sockets_count {
                        *combined_response
                            .channels_with_sockets_count
                            .entry(channel)
                            .or_insert(0) += socket_count;
                    }
                }

                RequestType::Sockets => {
                    // Collect unique socket IDs and sum total count
                    for socket_id in response.socket_ids {
                        unique_socket_ids.insert(socket_id);
                    }
                    combined_response.sockets_count += response.sockets_count;
                }

                RequestType::Channels => {
                    // Union of all channels across nodes
                    combined_response.channels.extend(response.channels);
                }

                RequestType::SocketsCount => {
                    // Sum socket counts from all nodes
                    combined_response.sockets_count += response.sockets_count;
                }

                RequestType::ChannelMembersCount => {
                    // FIXED: Actually sum the members count
                    combined_response.members_count += response.members_count;
                }

                RequestType::CountUserConnectionsInChannel => {
                    // Sum connection counts from all nodes
                    combined_response.sockets_count += response.sockets_count;
                }
                RequestType::BatchChannelSocketsCount => {
                    for (channel, count) in response.channels_with_sockets_count {
                        *combined_response
                            .channels_with_sockets_count
                            .entry(channel)
                            .or_insert(0) += count;
                    }
                }

                // Presence replication - no response aggregation needed (broadcast-only)
                RequestType::PresenceMemberJoined => {
                    // These are broadcast-only requests, no response aggregation needed
                }
                RequestType::PresenceMemberLeft => {
                    // These are broadcast-only requests, no response aggregation needed
                }
                RequestType::PresenceMemberUpdated => {
                    // These are broadcast-only requests, no response aggregation needed
                }
                RequestType::Heartbeat => {
                    // These are broadcast-only requests, no response aggregation needed
                }
                RequestType::NodeDead => {
                    // These are broadcast-only requests, no response aggregation needed
                }
                RequestType::PresenceStateSync => {
                    // These are broadcast-only requests, no response aggregation needed
                }
            }
        }

        // Convert unique socket IDs back to Vec for responses that need it
        if matches!(
            request_type,
            RequestType::ChannelSockets | RequestType::Sockets
        ) {
            combined_response.socket_ids = unique_socket_ids.into_iter().collect();
            // Update sockets_count to reflect unique count only for ChannelSockets
            // RequestType::Sockets keeps the raw sum to distinguish from unique socket IDs
            if matches!(request_type, RequestType::ChannelSockets) {
                combined_response.sockets_count = combined_response.socket_ids.len();
            }
        }

        // Update members_count to reflect actual merged member count
        if matches!(request_type, RequestType::ChannelMembers) {
            combined_response.members_count = combined_response.members.len();
        }

        combined_response
    }

    pub fn validate_aggregated_response(
        &self,
        response: &ResponseBody,
        request_type: &RequestType,
    ) -> Result<()> {
        match request_type {
            RequestType::ChannelSocketsCount | RequestType::SocketsCount
                if response.sockets_count == 0 && !response.socket_ids.is_empty() =>
            {
                warn!("Inconsistent response: sockets_count is 0 but socket_ids is not empty");
            }
            RequestType::ChannelMembersCount
                if response.members_count == 0 && !response.members.is_empty() =>
            {
                warn!("Inconsistent response: members_count is 0 but members map is not empty");
            }
            RequestType::ChannelsWithSocketsCount => {
                let total_from_channels: usize =
                    response.channels_with_sockets_count.values().sum();
                if total_from_channels == 0 && response.sockets_count > 0 {
                    warn!("Inconsistent response: channels show 0 sockets but sockets_count > 0");
                }
            }
            _ => {} // No specific validation needed for other types
        }

        Ok(())
    }

    /// Helper function to track broadcast latency metrics
    pub async fn track_broadcast_latency_if_successful(
        send_result: &Result<()>,
        timestamp_ms: Option<f64>,
        recipient_count: Option<usize>,
        app_id: &str,
        channel: &str,
        metrics_ref: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    ) {
        if send_result.is_ok()
            && let Some(timestamp_ms) = timestamp_ms
        {
            // Only proceed if we have metrics
            if let Some(metrics) = metrics_ref {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as f64
                    / 1_000_000.0;
                let latency_ms = (now_ms - timestamp_ms).max(0.0); // Already in milliseconds with microsecond precision

                // Only track metrics if we have a valid recipient count
                if let Some(recipient_count) = recipient_count {
                    metrics.track_broadcast_latency(app_id, channel, recipient_count, latency_ms);
                }
            }
        }
    }

    /// Helper function to calculate local recipient count for broadcasting
    pub async fn get_local_recipient_count(
        &self,
        app_id: &str,
        channel: &str,
        except: Option<&SocketId>,
    ) -> usize {
        // Get recipient count before sending
        let recipient_count = self
            .local_adapter
            .get_channel_socket_count(app_id, channel)
            .await;

        // Adjust for excluded socket
        if except.is_some() && recipient_count > 0 {
            recipient_count - 1
        } else {
            recipient_count
        }
    }

    /// Get dead nodes by checking heartbeat timeouts
    pub async fn get_dead_nodes(&self, dead_node_timeout_ms: u64) -> Vec<String> {
        let now = Instant::now();
        let timeout_duration = Duration::from_millis(dead_node_timeout_ms);

        let heartbeats_guard = self.node_heartbeats.read().await;
        heartbeats_guard
            .iter()
            .filter_map(|(node, last_heartbeat_time)| {
                if now.duration_since(*last_heartbeat_time) > timeout_duration {
                    Some(node.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Remove a dead node from heartbeat tracking
    pub async fn remove_dead_node(&self, dead_node_id: &str) {
        let mut heartbeats = self.node_heartbeats.write().await;
        heartbeats.remove(dead_node_id);
    }

    /// Clean up local presence registry for a dead node (followers only)
    pub async fn cleanup_local_presence_registry(&self, dead_node_id: &str) {
        let mut registry = self.cluster_presence_registry.write().await;

        // O(1) removal of entire node's data
        if let Some(dead_node_data) = registry.remove(dead_node_id) {
            let total_members: usize = dead_node_data
                .values()
                .map(|channel_members| channel_members.len())
                .sum();

            debug!(
                "Removed {} orphaned presence members from dead node: {}",
                total_members, dead_node_id
            );
        }
    }

    /// Determine if this node should be the cleanup leader
    /// Excludes dead nodes from the election pool
    pub async fn is_cleanup_leader(&self, dead_nodes: &[String]) -> bool {
        let heartbeats = self.node_heartbeats.read().await;
        let mut alive_nodes: Vec<String> = heartbeats.keys().cloned().collect();
        alive_nodes.push(self.node_id.clone()); // Include ourselves

        // Remove dead nodes from election pool
        alive_nodes.retain(|node| !dead_nodes.contains(node));

        alive_nodes.sort();
        alive_nodes.first() == Some(&self.node_id)
    }

    /// Handle cleanup for a dead node (called only by elected leader)
    /// Returns the list of orphaned presence members that need broadcast cleanup
    /// Format: Vec<(app_id, channel, user_id, user_info)>
    pub async fn handle_dead_node_cleanup(
        &self,
        dead_node_id: &str,
    ) -> Result<Vec<(String, String, String, Option<sonic_rs::Value>)>> {
        let mut registry = self.cluster_presence_registry.write().await;

        // O(1) lookup and removal of entire node's data
        if let Some(dead_node_data) = registry.remove(dead_node_id) {
            // Group sockets by (app_id, channel, user_id) to avoid duplicate cleanup calls for same user
            let mut unique_members =
                ahash::AHashMap::<(String, String, String), Option<sonic_rs::Value>>::new();

            for (channel, sockets) in dead_node_data {
                for (_socket_id, entry) in sockets {
                    let key = (entry.app_id.clone(), channel.clone(), entry.user_id.clone());
                    // Keep user_info from first socket (they should all be the same for same user)
                    unique_members.entry(key).or_insert(entry.user_info);
                }
            }

            // Convert to list format
            let cleanup_tasks: Vec<(String, String, String, Option<sonic_rs::Value>)> =
                unique_members
                    .into_iter()
                    .map(|((app_id, channel, user_id), user_info)| {
                        (app_id, channel, user_id, user_info)
                    })
                    .collect();

            debug!(
                "Found {} unique presence members to clean up from dead node {}",
                cleanup_tasks.len(),
                dead_node_id
            );

            // Return the list of members that need broadcast cleanup
            Ok(cleanup_tasks)
        } else {
            debug!("No presence members found for dead node {}", dead_node_id);
            Ok(Vec::new())
        }
    }
    /// Add a presence entry to the cluster registry
    pub async fn add_presence_entry(
        &self,
        node_id: &str,
        channel: &str,
        socket_id: &str,
        user_id: &str,
        app_id: &str,
        user_info: Option<sonic_rs::Value>,
    ) {
        let mut registry = self.cluster_presence_registry.write().await;
        registry
            .entry(node_id.to_string())
            .or_insert_with(AHashMap::new)
            .entry(channel.to_string())
            .or_insert_with(AHashMap::new)
            .insert(
                socket_id.to_string(),
                PresenceEntry {
                    user_info,
                    node_id: node_id.to_string(),
                    app_id: app_id.to_string(),
                    user_id: user_id.to_string(),
                    socket_id: socket_id.to_string(),
                    sequence_number: self.sequence_counter.fetch_add(1, Ordering::SeqCst),
                },
            );

        debug!(
            "Added presence entry: user {} (socket {}) in channel {} for node {}",
            user_id, socket_id, channel, node_id
        );
    }

    /// Remove a presence entry from the cluster registry
    pub async fn remove_presence_entry(&self, node_id: &str, channel: &str, socket_id: &str) {
        let mut registry = self.cluster_presence_registry.write().await;
        if let Some(node_data) = registry.get_mut(node_id) {
            if let Some(channel_sockets) = node_data.get_mut(channel) {
                channel_sockets.remove(socket_id);
                if channel_sockets.is_empty() {
                    node_data.remove(channel);
                }
            }
            if node_data.is_empty() {
                registry.remove(node_id);
            }
        }

        debug!(
            "Removed presence entry: socket {} in channel {} for node {}",
            socket_id, channel, node_id
        );
    }

    /// Update presence member data in the cluster registry.
    pub async fn update_presence_entry(
        &self,
        node_id: &str,
        channel: &str,
        socket_id: &str,
        user_id: &str,
        app_id: &str,
        user_info: sonic_rs::Value,
    ) {
        let mut registry = self.cluster_presence_registry.write().await;
        let channel_entries = registry
            .entry(node_id.to_string())
            .or_insert_with(AHashMap::new)
            .entry(channel.to_string())
            .or_insert_with(AHashMap::new);

        match channel_entries.get_mut(socket_id) {
            Some(entry) => {
                entry.user_info = Some(user_info);
                entry.sequence_number = self.sequence_counter.fetch_add(1, Ordering::SeqCst);
            }
            None => {
                channel_entries.insert(
                    socket_id.to_string(),
                    PresenceEntry {
                        user_info: Some(user_info),
                        node_id: node_id.to_string(),
                        app_id: app_id.to_string(),
                        user_id: user_id.to_string(),
                        socket_id: socket_id.to_string(),
                        sequence_number: self.sequence_counter.fetch_add(1, Ordering::SeqCst),
                    },
                );
            }
        }

        debug!(
            "Updated presence entry: user {} (socket {}) in channel {} for node {}",
            user_id, socket_id, channel, node_id
        );
    }

    /// Get count of active nodes (including ourselves)
    pub async fn get_effective_node_count(&self) -> usize {
        let heartbeats = self.node_heartbeats.read().await;
        heartbeats.len() + 1 // +1 for ourselves
    }

    /// Add a discovered node for testing purposes
    /// This simulates that node discovery has already happened
    pub async fn add_discovered_node_for_test(&self, node_id: String) {
        use std::time::Instant;
        let mut heartbeats = self.node_heartbeats.write().await;
        heartbeats.insert(node_id, Instant::now());
    }
}
