#![allow(dead_code)]

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::adapter::horizontal_transport::{HorizontalTransport, TransportConfig, TransportHandlers};
use crate::adapter::local_adapter::LocalAdapter;
use crate::adapter::ConnectionManager;
use crate::app::manager::AppManager;
use crate::channel::PresenceMemberInfo;
use crate::error::{Error, Result};
use crate::metrics::MetricsInterface;
use crate::namespace::Namespace;
use crate::protocol::messages::PusherMessage;
use crate::websocket::{SocketId, WebSocketRef};
use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use fastwebsockets::WebSocketWrite;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::io::WriteHalf;
use tokio::sync::{Mutex, Notify};
use tokio::time::sleep;
use tracing::{debug, error, warn};
use uuid::Uuid;

/// Request types for horizontal communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RequestType {
    // Original request types
    ChannelMembers,           // Get members in a channel
    ChannelSockets,           // Get sockets in a channel
    ChannelSocketsCount,      // Get count of sockets in a channel
    SocketExistsInChannel,    // Check if socket exists in a channel
    TerminateUserConnections, // Terminate user connections
    ChannelsWithSocketsCount, // Get channels with socket counts

    // New request types
    Sockets,                       // Get all sockets
    Channels,                      // Get all channels
    SocketsCount,                  // Get count of all sockets
    ChannelMembersCount,           // Get count of members in a channel
    CountUserConnectionsInChannel, // Count user's connections in a specific channel
}

/// Request body for horizontal communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestBody {
    pub request_id: String,
    pub node_id: String,
    pub app_id: String,
    pub request_type: RequestType,
    pub channel: Option<String>,
    pub socket_id: Option<String>,
    pub user_id: Option<String>,
}

/// Response body for horizontal requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseBody {
    pub request_id: String,
    pub node_id: String,
    pub app_id: String,
    pub members: HashMap<String, PresenceMemberInfo>,
    pub channels_with_sockets_count: HashMap<String, usize>,
    pub socket_ids: Vec<String>,
    pub sockets_count: usize,
    pub exists: bool,
    pub channels: HashSet<String>,
    pub members_count: usize, // New field for ChannelMembersCount
}

/// Message for broadcasting events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastMessage {
    pub node_id: String,
    pub app_id: String,
    pub channel: String,
    pub message: String,
    pub except_socket_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<f64>, // Timestamp when broadcast was initiated (milliseconds since epoch with microsecond precision)
}

/// Request tracking struct
#[derive(Clone)]
pub struct PendingRequest {
    pub(crate) start_time: Instant,
    pub(crate) app_id: String,
    pub(crate) responses: Vec<ResponseBody>,
    pub(crate) notify: Arc<Notify>,
}

/// Unified horizontal adapter with transport abstraction
pub struct HorizontalAdapter<T: HorizontalTransport> {
    /// Unique node ID
    pub node_id: String,

    /// Local adapter for handling local connections
    pub local_adapter: LocalAdapter,

    /// Pending requests map - Use DashMap for thread-safe access
    pub pending_requests: DashMap<String, PendingRequest>,

    /// Timeout for requests in milliseconds
    pub requests_timeout: u64,

    /// Metrics interface
    pub metrics: Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>>,

    /// Transport implementation for messaging
    pub transport: T,

    /// Transport configuration
    pub config: T::Config,
}

impl<T: HorizontalTransport> HorizontalAdapter<T>
where
    T::Config: TransportConfig,
{
    /// Create a new horizontal adapter with transport
    pub async fn new(config: T::Config) -> Result<Self> {
        let transport = T::new(config.clone()).await?;
        let requests_timeout = config.request_timeout_ms();

        Ok(Self {
            node_id: Uuid::new_v4().to_string(),
            local_adapter: LocalAdapter::new(),
            pending_requests: DashMap::new(),
            requests_timeout,
            metrics: None,
            transport,
            config,
        })
    }

    pub async fn set_metrics(
        &mut self,
        metrics: Arc<Mutex<dyn MetricsInterface + Send + Sync>>,
    ) -> Result<()> {
        self.metrics = Some(metrics);
        Ok(())
    }

    /// Start the request cleanup task
    pub fn start_request_cleanup(&mut self) {
        let timeout = self.requests_timeout;
        let pending_requests_clone = self.pending_requests.clone();

        // Spawn a background task to clean up stale requests
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(1000)).await;

                // Find and process expired requests
                let now = Instant::now();
                let mut expired_requests = Vec::new();

                // We can't modify pending_requests while iterating
                for entry in &pending_requests_clone {
                    let request_id = entry.key();
                    let request = entry.value();
                    if now.duration_since(request.start_time).as_millis() > timeout as u128 {
                        expired_requests.push(request_id.clone());
                    }
                }

                // Process expired requests
                for request_id in expired_requests {
                    warn!("{}", format!("Request {} expired", request_id));
                    pending_requests_clone.remove(&request_id);
                }
            }
        });
    }

    /// Process a received request from another node
    pub async fn process_request(&mut self, request: RequestBody) -> Result<ResponseBody> {
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
        if let Some(ref metrics) = self.metrics {
            let metrics = metrics.lock().await;
            metrics.mark_horizontal_adapter_request_received(&request.app_id);
        }

        // Initialize empty response
        let mut response = ResponseBody {
            request_id: request.request_id.clone(),
            node_id: self.node_id.clone(),
            app_id: request.app_id.clone(),
            members: HashMap::new(),
            socket_ids: Vec::new(),
            sockets_count: 0,
            channels_with_sockets_count: HashMap::new(),
            exists: false,
            channels: HashSet::new(),
            members_count: 0,
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
                    // Get channel sockets from local adapter
                    let channel_set = self
                        .local_adapter
                        .get_channel_sockets(&request.app_id, channel)
                        .await?;
                    response.socket_ids = channel_set
                        .iter()
                        .map(|socket_id| socket_id.0.clone())
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
                    let socket_id = SocketId(socket_id.clone());
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
                // Get channels with socket count from local adapter
                let channels = self
                    .local_adapter
                    .get_channels_with_socket_count(&request.app_id)
                    .await?;
                response.channels_with_sockets_count = channels
                    .iter()
                    .map(|entry| (entry.key().clone(), *entry.value()))
                    .collect();
            }
            // New request types
            RequestType::Sockets => {
                // Get all connections for the app
                let connections = self
                    .local_adapter
                    .get_all_connections(&request.app_id)
                    .await;
                response.socket_ids = connections
                    .iter()
                    .map(|entry| entry.key().0.clone())
                    .collect();
                response.sockets_count = connections.len();
            }
            RequestType::Channels => {
                // Get all channels for the app
                let channels = self
                    .local_adapter
                    .get_channels_with_socket_count(&request.app_id)
                    .await?;
                response.channels = channels.iter().map(|entry| entry.key().clone()).collect();
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
        }

        // Return the response
        Ok(response)
    }

    /// Process a response received from another node
    pub async fn process_response(&self, response: ResponseBody) -> Result<()> {
        // Track received response
        if let Some(metrics_ref) = &self.metrics {
            let metrics = metrics_ref.lock().await;
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

    pub fn aggregate_responses(
        &self,
        request_id: String,
        node_id: String,
        app_id: String,
        request_type: &RequestType,
        responses: Vec<ResponseBody>,
    ) -> ResponseBody {
        let mut combined_response = ResponseBody {
            request_id,
            node_id,
            app_id,
            members: HashMap::new(),
            socket_ids: Vec::new(),
            sockets_count: 0,
            channels_with_sockets_count: HashMap::new(),
            exists: false,
            channels: HashSet::new(),
            members_count: 0,
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
            }
        }

        // Convert unique socket IDs back to Vec for responses that need it
        if matches!(
            request_type,
            RequestType::ChannelSockets | RequestType::Sockets
        ) {
            combined_response.socket_ids = unique_socket_ids.into_iter().collect();
            // Update sockets_count to reflect unique count for these request types
            if matches!(request_type, RequestType::ChannelSockets) {
                combined_response.sockets_count = combined_response.socket_ids.len();
            }
        }

        combined_response
    }

    pub fn validate_aggregated_response(
        &self,
        response: &ResponseBody,
        request_type: &RequestType,
    ) -> Result<()> {
        match request_type {
            RequestType::ChannelSocketsCount | RequestType::SocketsCount => {
                if response.sockets_count == 0 && !response.socket_ids.is_empty() {
                    warn!("Inconsistent response: sockets_count is 0 but socket_ids is not empty");
                }
            }
            RequestType::ChannelMembersCount => {
                if response.members_count == 0 && !response.members.is_empty() {
                    warn!("Inconsistent response: members_count is 0 but members map is not empty");
                }
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
        metrics_ref: Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>>,
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
                    let metrics_locked = metrics.lock().await;
                    metrics_locked.track_broadcast_latency(
                        app_id,
                        channel,
                        recipient_count,
                        latency_ms,
                    );
                }
            }
        }
    }

    /// Helper function to calculate local recipient count for broadcasting
    pub async fn get_local_recipient_count(
        &mut self,
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

    /// Send a request to other nodes and wait for responses
    pub async fn send_request(
        &self,
        app_id: &str,
        request_type: RequestType,
        channel: Option<&str>,
        socket_id: Option<&str>,
        user_id: Option<&str>,
    ) -> Result<ResponseBody> {
        let node_count = self.transport.get_node_count().await?;

        // Create the request
        let request_id = Uuid::new_v4().to_string();
        let node_id = self.node_id.clone();

        let request = RequestBody {
            request_id: request_id.clone(),
            node_id,
            app_id: app_id.to_string(),
            request_type: request_type.clone(),
            channel: channel.map(String::from),
            socket_id: socket_id.map(String::from),
            user_id: user_id.map(String::from),
        };

        // Add to pending requests
        self.pending_requests.insert(
            request_id.clone(),
            PendingRequest {
                start_time: Instant::now(),
                app_id: app_id.to_string(),
                responses: Vec::with_capacity(node_count.saturating_sub(1)),
                notify: Arc::new(Notify::new()),
            },
        );

        if let Some(metrics_ref) = &self.metrics {
            let metrics = metrics_ref.lock().await;
            metrics.mark_horizontal_adapter_request_sent(app_id);
        }

        // Broadcast the request via transport
        self.transport.publish_request(&request).await?;

        // Wait for responses
        let timeout_duration = Duration::from_millis(self.config.request_timeout_ms());
        let max_expected_responses = node_count.saturating_sub(1);

        if max_expected_responses == 0 {
            self.pending_requests.remove(&request_id);
            return Ok(ResponseBody {
                request_id,
                node_id: request.node_id,
                app_id: app_id.to_string(),
                members: HashMap::new(),
                socket_ids: Vec::new(),
                sockets_count: 0,
                channels_with_sockets_count: HashMap::new(),
                exists: false,
                channels: HashSet::new(),
                members_count: 0,
            });
        }

        // Wait for responses using event-driven approach
        let start = Instant::now();
        let notify = self
            .pending_requests
            .get(&request_id)
            .map(|req| req.notify.clone())
            .ok_or_else(|| {
                Error::Other(format!(
                    "Request {request_id} not found in pending requests"
                ))
            })?;

        let responses = loop {
            // Wait for notification or timeout
            let result = tokio::select! {
                _ = notify.notified() => {
                    // Check if we have enough responses
                    if let Some(pending_request) = self.pending_requests.get(&request_id) {
                        if pending_request.responses.len() >= max_expected_responses {
                            debug!(
                                "Request {} completed with {}/{} responses in {}ms",
                                request_id,
                                pending_request.responses.len(),
                                max_expected_responses,
                                start.elapsed().as_millis()
                            );
                            // Extract responses without removing the entry yet to avoid race condition
                            let responses = pending_request.responses.clone();
                            Some(responses)
                        } else {
                            None // Continue waiting
                        }
                    } else {
                        return Err(Error::Other(format!(
                            "Request {request_id} was removed unexpectedly"
                        )));
                    }
                }
                _ = tokio::time::sleep(timeout_duration) => {
                    // Timeout occurred
                    warn!(
                        "Request {} timed out after {}ms",
                        request_id,
                        start.elapsed().as_millis()
                    );
                    let responses = if let Some(pending_request) = self.pending_requests.get(&request_id) {
                        pending_request.responses.clone()
                    } else {
                        Vec::new()
                    };
                    Some(responses)
                }
            };

            if let Some(responses) = result {
                break responses;
            }
            // If result is None, continue waiting (notification came but not enough responses yet)
        };

        // Clean up the pending request now that we're done
        self.pending_requests.remove(&request_id);

        // Aggregate responses
        let combined_response = self.aggregate_responses(
            request_id.clone(),
            request.node_id,
            app_id.to_string(),
            &request_type,
            responses,
        );

        // Validate aggregated response
        if let Err(e) = self.validate_aggregated_response(&combined_response, &request_type) {
            warn!("Response validation failed for request {}: {}", request_id, e);
        }

        // Track metrics
        if let Some(metrics_ref) = &self.metrics {
            let metrics = metrics_ref.lock().await;
            let duration_ms = start.elapsed().as_micros() as f64 / 1000.0; // Convert to milliseconds with 3 decimal places
            metrics.track_horizontal_adapter_resolve_time(app_id, duration_ms);

            let resolved = combined_response.sockets_count > 0
                || !combined_response.members.is_empty()
                || combined_response.exists
                || !combined_response.channels.is_empty()
                || combined_response.members_count > 0
                || !combined_response.channels_with_sockets_count.is_empty();

            metrics.track_horizontal_adapter_resolved_promises(app_id, resolved);
        }

        Ok(combined_response)
    }

    pub async fn start_listeners(&mut self) -> Result<()> {
        self.start_request_cleanup();

        // Create shared state for handlers
        let node_id = self.node_id.clone();
        let local_adapter = Arc::new(Mutex::new(self.local_adapter.clone()));
        let pending_requests = self.pending_requests.clone();
        let metrics = self.metrics.clone();

        // Clone for each handler
        let broadcast_node_id = node_id.clone();
        let broadcast_local_adapter = local_adapter.clone();
        let broadcast_metrics = metrics.clone();

        let request_node_id = node_id.clone();
        let request_local_adapter = local_adapter.clone();
        let request_metrics = metrics.clone();

        let response_node_id = node_id.clone();
        let response_pending_requests = pending_requests.clone();
        let response_metrics = metrics.clone();

        let handlers = TransportHandlers {
            on_broadcast: Arc::new(move |broadcast| {
                let node_id = broadcast_node_id.clone();
                let local_adapter = broadcast_local_adapter.clone();
                let metrics = broadcast_metrics.clone();
                
                Box::pin(async move {
                    if broadcast.node_id == node_id {
                        return;
                    }

                    if let Ok(message) = serde_json::from_str(&broadcast.message) {
                        let except_id = broadcast
                            .except_socket_id
                            .as_ref()
                            .map(|id| SocketId(id.clone()));

                        // Send the message locally
                        let mut local_adapter_lock = local_adapter.lock().await;
                        
                        // Count local recipients for this node (adjusts for excluded socket)
                        let local_recipient_count = local_adapter_lock
                            .get_channel_socket_count(&broadcast.app_id, &broadcast.channel)
                            .await;
                        
                        let adjusted_count = if except_id.is_some() && local_recipient_count > 0 {
                            local_recipient_count - 1
                        } else {
                            local_recipient_count
                        };

                        // Use the timestamp from the broadcast message for end-to-end tracking
                        let send_result = local_adapter_lock
                            .send(
                                &broadcast.channel,
                                message,
                                except_id.as_ref(),
                                &broadcast.app_id,
                                broadcast.timestamp_ms,
                            )
                            .await;

                        drop(local_adapter_lock); // Release lock before metrics

                        // Track broadcast latency metrics
                        Self::track_broadcast_latency_if_successful(
                            &send_result,
                            broadcast.timestamp_ms,
                            Some(adjusted_count),
                            &broadcast.app_id,
                            &broadcast.channel,
                            metrics,
                        )
                        .await;
                    }
                })
            }),
            on_request: Arc::new(move |request| {
                let node_id = request_node_id.clone();
                let local_adapter = request_local_adapter.clone();
                let metrics = request_metrics.clone();
                
                Box::pin(async move {
                    if request.node_id == node_id {
                        return Err(Error::Other("Skipping own request".to_string()));
                    }

                    // Track metrics for received request
                    if let Some(ref metrics) = metrics {
                        let metrics = metrics.lock().await;
                        metrics.mark_horizontal_adapter_request_received(&request.app_id);
                    }

                    // Process the request using local adapter
                    let mut local_adapter_lock = local_adapter.lock().await;
                    let mut response = ResponseBody {
                        request_id: request.request_id.clone(),
                        node_id: node_id.clone(),
                        app_id: request.app_id.clone(),
                        members: HashMap::new(),
                        socket_ids: Vec::new(),
                        sockets_count: 0,
                        channels_with_sockets_count: HashMap::new(),
                        exists: false,
                        channels: HashSet::new(),
                        members_count: 0,
                    };

                    // Process ALL request types (complete implementation)
                    match request.request_type {
                        RequestType::ChannelMembers => {
                            if let Some(channel) = &request.channel {
                                let members = local_adapter_lock
                                    .get_channel_members(&request.app_id, channel)
                                    .await?;
                                response.members = members;
                            }
                        }
                        RequestType::ChannelSockets => {
                            if let Some(channel) = &request.channel {
                                let channel_set = local_adapter_lock
                                    .get_channel_sockets(&request.app_id, channel)
                                    .await?;
                                response.socket_ids = channel_set
                                    .iter()
                                    .map(|socket_id| socket_id.0.clone())
                                    .collect();
                            }
                        }
                        RequestType::ChannelSocketsCount => {
                            if let Some(channel) = &request.channel {
                                response.sockets_count = local_adapter_lock
                                    .get_channel_socket_count(&request.app_id, channel)
                                    .await;
                            }
                        }
                        RequestType::SocketExistsInChannel => {
                            if let (Some(channel), Some(socket_id)) = (&request.channel, &request.socket_id) {
                                let socket_id = SocketId(socket_id.clone());
                                response.exists = local_adapter_lock
                                    .is_in_channel(&request.app_id, channel, &socket_id)
                                    .await?;
                            }
                        }
                        RequestType::TerminateUserConnections => {
                            if let Some(user_id) = &request.user_id {
                                local_adapter_lock
                                    .terminate_user_connections(&request.app_id, user_id)
                                    .await?;
                                response.exists = true;
                            }
                        }
                        RequestType::ChannelsWithSocketsCount => {
                            let channels = local_adapter_lock
                                .get_channels_with_socket_count(&request.app_id)
                                .await?;
                            response.channels_with_sockets_count = channels
                                .iter()
                                .map(|entry| (entry.key().clone(), *entry.value()))
                                .collect();
                        }
                        RequestType::Sockets => {
                            let connections = local_adapter_lock
                                .get_all_connections(&request.app_id)
                                .await;
                            response.socket_ids = connections
                                .iter()
                                .map(|entry| entry.key().0.clone())
                                .collect();
                            response.sockets_count = connections.len();
                        }
                        RequestType::Channels => {
                            let channels = local_adapter_lock
                                .get_channels_with_socket_count(&request.app_id)
                                .await?;
                            response.channels = channels.iter().map(|entry| entry.key().clone()).collect();
                        }
                        RequestType::SocketsCount => {
                            let connections = local_adapter_lock
                                .get_all_connections(&request.app_id)
                                .await;
                            response.sockets_count = connections.len();
                        }
                        RequestType::ChannelMembersCount => {
                            if let Some(channel) = &request.channel {
                                let members = local_adapter_lock
                                    .get_channel_members(&request.app_id, channel)
                                    .await?;
                                response.members_count = members.len();
                            }
                        }
                        RequestType::CountUserConnectionsInChannel => {
                            if let (Some(user_id), Some(channel)) = (&request.user_id, &request.channel) {
                                response.sockets_count = local_adapter_lock
                                    .count_user_connections_in_channel(user_id, &request.app_id, channel, None)
                                    .await?;
                            }
                        }
                    }

                    Ok(response)
                })
            }),
            on_response: Arc::new(move |response| {
                let node_id = response_node_id.clone();
                let pending_requests = response_pending_requests.clone();
                let metrics = response_metrics.clone();
                
                Box::pin(async move {
                    if response.node_id == node_id {
                        return;
                    }

                    // Track received response
                    if let Some(metrics_ref) = &metrics {
                        let metrics = metrics_ref.lock().await;
                        metrics.mark_horizontal_adapter_response_received(&response.app_id);
                    }

                    // Get the pending request and notify waiters
                    if let Some(mut request) = pending_requests.get_mut(&response.request_id) {
                        // Add response to the list
                        request.responses.push(response);

                        // Notify any waiting send_request calls that a new response has arrived
                        request.notify.notify_one();
                    }
                })
            }),
        };

        self.transport.start_listeners(handlers).await?;
        Ok(())
    }
}

#[async_trait]
impl<T: HorizontalTransport + 'static> ConnectionManager for HorizontalAdapter<T>
where
    T::Config: TransportConfig,
{
    async fn init(&mut self) {
        self.local_adapter.init().await;

        if let Err(e) = self.start_listeners().await {
            error!("Failed to start transport listeners: {}", e);
        }
    }

    async fn get_namespace(&mut self, app_id: &str) -> Option<Arc<Namespace>> {
        self.local_adapter.get_namespace(app_id).await
    }

    async fn add_socket(
        &mut self,
        socket_id: SocketId,
        socket: WebSocketWrite<WriteHalf<TokioIo<Upgraded>>>,
        app_id: &str,
        app_manager: &Arc<dyn AppManager + Send + Sync>,
    ) -> Result<()> {
        self.local_adapter
            .add_socket(socket_id, socket, app_id, app_manager)
            .await
    }

    async fn get_connection(&mut self, socket_id: &SocketId, app_id: &str) -> Option<WebSocketRef> {
        self.local_adapter.get_connection(socket_id, app_id).await
    }

    async fn remove_connection(&mut self, socket_id: &SocketId, app_id: &str) -> Result<()> {
        self.local_adapter.remove_connection(socket_id, app_id).await
    }

    async fn send_message(
        &mut self,
        app_id: &str,
        socket_id: &SocketId,
        message: PusherMessage,
    ) -> Result<()> {
        self.local_adapter
            .send_message(app_id, socket_id, message)
            .await
    }

    async fn send(
        &mut self,
        channel: &str,
        message: PusherMessage,
        except: Option<&SocketId>,
        app_id: &str,
        start_time_ms: Option<f64>,
    ) -> Result<()> {
        // Send locally first (tracked in connection manager for metrics)
        let local_result = self.local_adapter
            .send(channel, message.clone(), except, app_id, start_time_ms)
            .await;

        if let Err(e) = local_result {
            warn!("Local send failed for channel {}: {}", channel, e);
        }

        // Broadcast to other nodes
        let message_json = serde_json::to_string(&message)?;
        let broadcast = BroadcastMessage {
            node_id: self.node_id.clone(),
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            message: message_json,
            except_socket_id: except.map(|id| id.0.clone()),
            timestamp_ms: start_time_ms.or_else(|| {
                Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as f64
                        / 1_000_000.0, // Convert to milliseconds with microsecond precision
                )
            }),
        };

        self.transport.publish_broadcast(&broadcast).await?;

        Ok(())
    }

    async fn get_channel_members(
        &mut self,
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

    async fn get_channel_sockets(
        &mut self,
        app_id: &str,
        channel: &str,
    ) -> Result<DashSet<SocketId>> {
        let all_socket_ids = DashSet::new();

        // Get local sockets
        let sockets = self
            .local_adapter
            .get_channel_sockets(app_id, channel)
            .await?;

        for entry in sockets.iter() {
            all_socket_ids.insert(entry.key().clone());
        }

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
            all_socket_ids.insert(SocketId(socket_id));
        }

        Ok(all_socket_ids)
    }

    async fn remove_channel(&mut self, app_id: &str, channel: &str) {
        self.local_adapter.remove_channel(app_id, channel).await
    }

    async fn is_in_channel(
        &mut self,
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
                Some(&socket_id.0),
                None,
            )
            .await?;

        Ok(response.exists)
    }

    async fn get_user_sockets(
        &mut self,
        user_id: &str,
        app_id: &str,
    ) -> Result<DashSet<WebSocketRef>> {
        self.local_adapter.get_user_sockets(user_id, app_id).await
    }

    async fn cleanup_connection(&mut self, app_id: &str, ws: WebSocketRef) {
        self.local_adapter.cleanup_connection(app_id, ws).await
    }

    async fn terminate_connection(&mut self, app_id: &str, user_id: &str) -> Result<()> {
        // Terminate locally
        self.local_adapter
            .terminate_connection(app_id, user_id)
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

    async fn add_channel_to_sockets(&mut self, app_id: &str, channel: &str, socket_id: &SocketId) {
        self.local_adapter
            .add_channel_to_sockets(app_id, channel, socket_id)
            .await
    }

    async fn get_channel_socket_count(&mut self, app_id: &str, channel: &str) -> usize {
        // Get local count
        let local_count = self
            .local_adapter
            .get_channel_socket_count(app_id, channel)
            .await;

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
            Ok(response) => local_count + response.sockets_count,
            Err(e) => {
                error!("Failed to get remote channel socket count: {}", e);
                local_count
            }
        }
    }

    async fn add_to_channel(
        &mut self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        self.local_adapter
            .add_to_channel(app_id, channel, socket_id)
            .await
    }

    async fn remove_from_channel(
        &mut self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        self.local_adapter
            .remove_from_channel(app_id, channel, socket_id)
            .await
    }

    async fn get_presence_member(
        &mut self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<PresenceMemberInfo> {
        self.local_adapter
            .get_presence_member(app_id, channel, socket_id)
            .await
    }

    async fn terminate_user_connections(&mut self, app_id: &str, user_id: &str) -> Result<()> {
        self.terminate_connection(app_id, user_id).await
    }

    async fn add_user(&mut self, ws: WebSocketRef) -> Result<()> {
        self.local_adapter.add_user(ws).await
    }

    async fn remove_user(&mut self, ws: WebSocketRef) -> Result<()> {
        self.local_adapter.remove_user(ws).await
    }

    async fn remove_user_socket(
        &mut self,
        user_id: &str,
        socket_id: &SocketId,
        app_id: &str,
    ) -> Result<()> {
        self.local_adapter
            .remove_user_socket(user_id, socket_id, app_id)
            .await
    }

    async fn count_user_connections_in_channel(
        &mut self,
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
                error!("Failed to get remote user connections count: {}", e);
                Ok(local_count)
            }
        }
    }

    async fn get_channels_with_socket_count(
        &mut self,
        app_id: &str,
    ) -> Result<DashMap<String, usize>> {
        // Get local channels
        let channels = self
            .local_adapter
            .get_channels_with_socket_count(app_id)
            .await?;

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
                error!("Failed to get remote channels with socket count: {}", e);
            }
        }

        Ok(channels)
    }

    async fn get_sockets_count(&self, app_id: &str) -> Result<usize> {
        // Get local count
        let local_count = self.local_adapter.get_sockets_count(app_id).await?;

        // Get distributed count
        match self
            .send_request(app_id, RequestType::SocketsCount, None, None, None)
            .await
        {
            Ok(response) => Ok(local_count + response.sockets_count),
            Err(e) => {
                error!("Failed to get remote socket count: {}", e);
                Ok(local_count)
            }
        }
    }

    async fn get_namespaces(&mut self) -> Result<DashMap<String, Arc<Namespace>>> {
        self.local_adapter.get_namespaces().await
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    async fn check_health(&self) -> Result<()> {
        self.transport.check_health().await
    }
}