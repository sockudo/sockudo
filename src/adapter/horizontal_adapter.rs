use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::adapter::Adapter;
use crate::adapter::local_adapter::LocalAdapter;
use crate::channel::PresenceMemberInfo;
use crate::error::{Error, Result};

use crate::metrics::MetricsInterface;
use crate::websocket::SocketId;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{info, warn};
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
    Sockets,             // Get all sockets
    Channels,            // Get all channels
    SocketsCount,        // Get count of all sockets
    ChannelMembersCount, // Get count of members in a channel
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
}

/// Request tracking struct
#[derive(Clone)]
pub struct PendingRequest {
    start_time: Instant,
    app_id: String,
    responses: Vec<ResponseBody>,
}

/// Base horizontal adapter
pub struct HorizontalAdapter {
    /// Unique node ID
    pub node_id: String,

    /// Local adapter for handling local connections
    pub local_adapter: LocalAdapter,

    /// Pending requests map - Use DashMap for thread-safe access
    pub pending_requests: DashMap<String, PendingRequest>,

    /// Timeout for requests in milliseconds
    pub requests_timeout: u64,

    pub metrics: Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>>,
}

impl HorizontalAdapter {
    /// Create a new horizontal adapter
    pub fn new() -> Self {
        Self {
            node_id: Uuid::new_v4().to_string(),
            local_adapter: LocalAdapter::new(),
            pending_requests: DashMap::new(),
            requests_timeout: 5000, // Default 5 seconds
            metrics: None,
        }
    }

    /// Start the request cleanup task
    pub fn start_request_cleanup(&mut self) {
        // Clone data needed for the task
        let node_id = self.node_id.clone();
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
        info!(
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
                        .get_channel(&request.app_id, channel)
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

        // Get the pending request
        if let Some(mut request) = self.pending_requests.get_mut(&response.request_id) {
            // Add response to the list
            request.responses.push(response);
        }

        Ok(())
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
        // Generate a new request ID
        let request_id = Uuid::new_v4().to_string();

        // Create the request
        let request = RequestBody {
            request_id: request_id.clone(),
            node_id: self.node_id.clone(),
            app_id: app_id.to_string(),
            request_type,
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
                responses: Vec::new(),
            },
        );

        // Serialize the request
        let request_json = serde_json::to_string(&request)?;
        if let Some(metrics_ref) = &self.metrics {
            let metrics = metrics_ref.lock().await;
            metrics.mark_horizontal_adapter_request_sent(app_id);
        }

        // This would be implemented by the specific adapter (Redis, etc.)
        // self.broadcast_request(request_json).await?;

        // Wait for responses
        let timeout = self.requests_timeout;
        let start = Instant::now();

        // Maximum nodes to wait for (don't wait for more than expected_node_count)
        let max_nodes = if expected_node_count > 1 {
            expected_node_count - 1
        } else {
            0
        };

        // Combine the results
        let mut combined_response = ResponseBody {
            request_id: request_id.clone(),
            node_id: self.node_id.clone(),
            app_id: app_id.to_string(),
            members: HashMap::new(),
            socket_ids: Vec::new(),
            sockets_count: 0,
            channels_with_sockets_count: HashMap::new(),
            exists: false,
            channels: Default::default(),
            members_count: 0,
        };

        // Wait for responses until timeout or we have enough responses
        while start.elapsed().as_millis() < timeout as u128 {
            // Check if we have the request
            if let Some(request) = self.pending_requests.get(&request_id) {
                // Check if we have enough responses
                if request.responses.len() >= max_nodes {
                    break;
                }
            } else {
                // Request was removed, something went wrong
                return Err(Error::Other("Request was removed".into()));
            }

            // Sleep a bit to avoid busy waiting
            sleep(Duration::from_millis(10)).await;
        }

        // Get all responses
        if let Some((_, request)) = self.pending_requests.remove(&request_id) {
            // Combine the results
            for response in request.responses {
                // Also note it's "responses" not "response"
                // Add members
                combined_response.members.extend(response.members);

                // Add socket IDs
                combined_response.socket_ids.extend(response.socket_ids);

                // Add socket count
                combined_response.sockets_count += response.sockets_count;

                // If any node says socket exists, it exists
                combined_response.exists = combined_response.exists || response.exists;

                // Combine channels with sockets count
                for (channel, sockets) in response.channels_with_sockets_count {
                    combined_response
                        .channels_with_sockets_count
                        .entry(channel)
                        .or_insert_with(|| 0);
                }
            }
        }
        if let Some(metrics_ref) = &self.metrics {
            let duration_ms = start.elapsed().as_millis() as f64;
            let metrics = metrics_ref.lock().await;

            // Track resolution time
            metrics.track_horizontal_adapter_resolve_time(app_id, duration_ms);

            // Track if the request was successfully resolved
            metrics.track_horizontal_adapter_resolved_promises(app_id, true);
        }

        // Return the combined response
        Ok(combined_response)
    }
}
