use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::jetstream::AckKind::Nak;
use async_nats::{
    Client as NatsClient, ConnectOptions as NatsOptions, Message as NatsMessage, Subject,
};
use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use fastwebsockets::WebSocketWrite;
use futures::StreamExt;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::io::WriteHalf;
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

use crate::adapter::adapter::Adapter;
use crate::adapter::horizontal_adapter::{
    BroadcastMessage, HorizontalAdapter, RequestBody, RequestType, ResponseBody,
};
use crate::app::manager::AppManager;
use crate::channel::PresenceMemberInfo;
use crate::error::{Error, Result};
use crate::log::Log;
use crate::metrics::MetricsInterface;
use crate::namespace::Namespace;
use crate::protocol::messages::PusherMessage;
use crate::websocket::{SocketId, WebSocket, WebSocketRef};

/// NATS channels/subjects
pub const DEFAULT_PREFIX: &str = "sockudo";
const BROADCAST_SUFFIX: &str = ".broadcast";
const REQUESTS_SUFFIX: &str = ".requests";
const RESPONSES_SUFFIX: &str = ".responses";

/// NATS adapter configuration
#[derive(Debug, Clone)]
pub struct NatsAdapterConfig {
    /// NATS server URLs
    pub servers: Vec<String>,
    /// Channel prefix
    pub prefix: String,
    /// Request timeout in milliseconds
    pub request_timeout_ms: u64,
    /// Username for NATS authentication
    pub username: Option<String>,
    /// Password for NATS authentication
    pub password: Option<String>,
    /// Token for NATS authentication
    pub token: Option<String>,
    /// Connection timeout in milliseconds
    pub connection_timeout_ms: u64,
    /// Optional nodes number for metrics
    pub nodes_number: Option<u32>,
}

impl Default for NatsAdapterConfig {
    fn default() -> Self {
        Self {
            servers: vec!["nats://localhost:4222".to_string()],
            prefix: DEFAULT_PREFIX.to_string(),
            request_timeout_ms: 5000,
            username: None,
            password: None,
            token: None,
            connection_timeout_ms: 5000,
            nodes_number: None,
        }
    }
}

/// NATS adapter for horizontal scaling
pub struct NatsAdapter {
    /// Base horizontal adapter (protected by a Mutex)
    pub horizontal: Arc<Mutex<HorizontalAdapter>>,

    /// NATS client
    pub client: NatsClient,

    /// Channel names
    pub prefix: String,
    pub broadcast_subject: String,
    pub request_subject: String,
    pub response_subject: String,

    /// Configuration
    pub config: NatsAdapterConfig,
}

impl NatsAdapter {
    /// Create a new NATS adapter
    pub async fn new(config: NatsAdapterConfig) -> Result<Self> {
        // Create the base horizontal adapter
        let mut horizontal = HorizontalAdapter::new();
        Log::info(format!("NATS adapter config: {:?}", config)); // Borrows config temporarily

        // Set timeout
        horizontal.requests_timeout = config.request_timeout_ms; // Accesses field (likely Copy)

        // --- Build NATS Options ---
        let mut nats_options = NatsOptions::new();

        // Set credentials conditionally: Prefer user/password if both are provided
        // Use as_deref() assuming the methods take &str.
        // If they require owned String, use .cloned() instead.
        if let (Some(username), Some(password)) =
            (config.username.as_deref(), config.password.as_deref())
        {
            nats_options =
                nats_options.user_and_password(username.to_string(), password.parse().unwrap());
        } else if let Some(token) = config.token.as_deref() {
            nats_options = nats_options.token(token.parse().unwrap());
        };
        // Note: If both user/pass and token are None, no credentials are set.

        // Set connection timeout
        nats_options =
            nats_options.connection_timeout(Duration::from_millis(config.connection_timeout_ms));

        // --- Connect to NATS ---
        // Assuming connect takes a reference to the servers string/list (e.g., &str or &[String])
        let client = nats_options.connect(&config.servers).await.map_err(|e| {
            // Borrow config.servers
            Error::InternalError(format!("Failed to connect to NATS: {}", e))
        })?;

        // Build subject names
        // Clone the prefix string as it's used multiple times and stored
        let broadcast_subject = format!("{}{}", config.prefix, BROADCAST_SUFFIX);
        let request_subject = format!("{}{}", config.prefix, REQUESTS_SUFFIX);
        let response_subject = format!("{}{}", config.prefix, RESPONSES_SUFFIX);

        // Create the adapter instance
        let adapter = Self {
            horizontal: Arc::new(Mutex::new(horizontal)),
            client,
            // Clone prefix again for storing in the struct
            prefix: config.prefix.clone(),
            broadcast_subject,
            request_subject,
            response_subject,
            // Clone the entire config *once* here to store it
            config: config.clone(),
        };

        Ok(adapter)
    }

    /// Create a new NATS adapter with simple configuration
    pub async fn with_servers(servers: Vec<String>) -> Result<Self> {
        let config = NatsAdapterConfig {
            servers,
            ..Default::default()
        };
        Self::new(config).await
    }

    /// Start listening for NATS messages
    pub async fn start_listeners(&self) -> Result<()> {
        // Lock needed only for starting cleanup task
        {
            let mut horizontal = self.horizontal.lock().await;
            // Start cleanup task
            horizontal.start_request_cleanup();
        } // Lock released here

        // Start NATS listeners
        self.start_subject_listeners().await?;

        Ok(())
    }

    /// Start subject listeners for NATS
    async fn start_subject_listeners(&self) -> Result<()> {
        // Clone needed values for the async task
        let horizontal_arc = self.horizontal.clone();
        let nats_client = self.client.clone();
        let broadcast_subject = self.broadcast_subject.clone();
        let request_subject = self.request_subject.clone();
        let response_subject = self.response_subject.clone();

        // Get node_id without holding the lock for the whole setup
        let node_id = {
            let horizontal_lock = horizontal_arc.lock().await;
            horizontal_lock.node_id.clone()
        }; // Lock released

        // Subscribe to broadcast channel
        let mut broadcast_subscription = nats_client
            .subscribe(Subject::from(broadcast_subject.clone()))
            .await
            .map_err(|e| {
                Error::InternalError(format!("Failed to subscribe to broadcast subject: {}", e))
            })?;

        // Subscribe to requests channel
        let mut request_subscription = nats_client
            .subscribe(Subject::from(request_subject.clone()))
            .await
            .map_err(|e| {
                Error::InternalError(format!("Failed to subscribe to request subject: {}", e))
            })?;

        // Subscribe to responses channel
        let mut response_subscription = nats_client
            .subscribe(Subject::from(response_subject.clone()))
            .await
            .map_err(|e| {
                Error::InternalError(format!("Failed to subscribe to response subject: {}", e))
            })?;

        Log::info(format!(
            "NATS adapter listening on subjects: {}, {}, {}",
            broadcast_subject, request_subject, response_subject
        ));

        // Spawn a task to handle broadcast messages
        let broadcast_horizontal = horizontal_arc.clone();
        let broadcast_node_id = node_id.clone();
        tokio::spawn(async move {
            while let Some(msg) = broadcast_subscription.next().await {
                match serde_json::from_slice::<BroadcastMessage>(&msg.payload) {
                    Ok(broadcast) => {
                        // Skip our own messages
                        if broadcast.node_id == broadcast_node_id {
                            continue;
                        }
                        // Process the broadcast
                        match serde_json::from_str(&broadcast.message) {
                            Ok(message) => {
                                let except_id = broadcast
                                    .except_socket_id
                                    .as_ref()
                                    .map(|id| SocketId(id.clone()));
                                // Lock only when interacting with local adapter
                                let mut horizontal_lock = broadcast_horizontal.lock().await;
                                let _ = horizontal_lock
                                    .local_adapter
                                    .send(
                                        &broadcast.channel,
                                        message,
                                        except_id.as_ref(),
                                        &broadcast.app_id,
                                    )
                                    .await;
                                // Lock released automatically when horizontal_lock goes out of scope
                            }
                            Err(e) => {
                                Log::warning(format!(
                                    "Failed to deserialize broadcast inner message: {}, Payload: {}",
                                    e, broadcast.message
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        Log::warning(format!(
                            "Failed to deserialize broadcast message: {}, Payload: {:?}",
                            e, msg.payload
                        ));
                    }
                }
            }
            Log::info("NATS broadcast listener stream ended.");
        });

        // Spawn a task to handle request messages
        let request_horizontal = horizontal_arc.clone();
        let request_node_id = node_id.clone();
        let request_client = nats_client.clone();
        let request_response_subject = response_subject.clone();
        tokio::spawn(async move {
            while let Some(msg) = request_subscription.next().await {
                match serde_json::from_slice::<RequestBody>(&msg.payload) {
                    Ok(request) => {
                        // Skip our own requests
                        if request.node_id == request_node_id {
                            continue;
                        }
                        // Process the request (already designed to be async)
                        // Lock only when processing
                        let response = {
                            // Scope for the lock
                            let mut horizontal_lock = request_horizontal.lock().await;
                            horizontal_lock.process_request(request).await
                        }; // Lock released
                        if let Ok(response) = response {
                            // Send response
                            match serde_json::to_vec(&response) {
                                Ok(response_data) => {
                                    if let Err(e) = request_client
                                        .publish(
                                            Subject::from(request_response_subject.clone()),
                                            response_data.into(),
                                        )
                                        .await
                                    {
                                        Log::error(format!("Failed to publish response: {}", e));
                                    }
                                }
                                Err(e) => {
                                    Log::error(format!("Failed to serialize response: {}", e));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        Log::warning(format!(
                            "Failed to deserialize request message: {}, Payload: {:?}",
                            e, msg.payload
                        ));
                    }
                }
            }
            Log::info("NATS request listener stream ended.");
        });

        // Spawn a task to handle response messages
        let response_horizontal = horizontal_arc.clone();
        let response_node_id = node_id.clone();
        tokio::spawn(async move {
            while let Some(msg) = response_subscription.next().await {
                match serde_json::from_slice::<ResponseBody>(&msg.payload) {
                    Ok(response) => {
                        // Skip our own responses
                        if response.node_id == response_node_id {
                            continue;
                        }
                        // Process the response (already designed to be async)
                        // Lock only when processing
                        let mut horizontal_lock = response_horizontal.lock().await;
                        let _ = horizontal_lock.process_response(response).await;
                        // Lock released automatically
                    }
                    Err(e) => {
                        Log::warning(format!(
                            "Failed to deserialize response message: {}, Payload: {:?}",
                            e, msg.payload
                        ));
                    }
                }
            }
            Log::info("NATS response listener stream ended.");
        });

        Ok(())
    }

    /// Get the number of nodes in the cluster
    pub async fn get_node_count(&self) -> Result<usize> {
        // If nodes_number is explicitly set, use that value
        if let Some(nodes) = self.config.nodes_number {
            return Ok(nodes as usize);
        }

        // Otherwise estimate based on NATS server info
        // NATS doesn't provide an easy way to count subscriptions like Redis PUBSUB NUMSUB
        // So we'll either need to:
        // 1. Maintain our own heartbeat system
        // 2. Just return a fixed/configured value
        // For now, we'll assume at least 1 node (ourselves)
        Ok(1)
    }

    /// Set the metrics instance
    pub async fn set_metrics(
        &mut self,
        metrics: Arc<Mutex<dyn MetricsInterface + Send + Sync>>,
    ) -> Result<()> {
        // Get lock on the horizontal adapter
        let mut horizontal = self.horizontal.lock().await;

        // Set the metrics in horizontal adapter
        horizontal.metrics = Some(metrics);

        Ok(())
    }

    /// Initialize with metrics
    pub async fn init_with_metrics(
        &mut self,
        metrics: Option<Arc<Mutex<dyn MetricsInterface + Send + Sync>>>,
    ) -> Result<()> {
        // First initialize the adapter
        self.init().await;

        // If metrics are provided, set them in the horizontal adapter
        if let Some(metrics_instance) = metrics {
            self.set_metrics(metrics_instance).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Adapter for NatsAdapter {
    async fn init(&mut self) {
        // Lock scope minimized
        {
            let mut horizontal = self.horizontal.lock().await;
            horizontal.local_adapter.init().await;
        } // Lock released

        // Start NATS listeners (already optimized)
        if let Err(e) = self.start_listeners().await {
            Log::error(format!("Failed to start NATS listeners: {}", e));
            // Consider returning the error or handling it more gracefully
        }
    }

    async fn get_namespace(&mut self, app_id: &str) -> Option<Arc<Namespace>> {
        let mut horizontal = self.horizontal.lock().await;
        horizontal.local_adapter.get_namespace(app_id).await
    }

    async fn add_socket(
        &mut self,
        socket_id: SocketId,
        socket: WebSocketWrite<WriteHalf<TokioIo<Upgraded>>>,
        app_id: &str,
        app_manager: &Arc<dyn AppManager + Send + Sync>,
    ) -> Result<()> {
        let mut horizontal = self.horizontal.lock().await;
        horizontal
            .local_adapter
            .add_socket(socket_id, socket, app_id, app_manager)
            .await
    }

    async fn get_connection(
        &mut self,
        socket_id: &SocketId,
        app_id: &str,
    ) -> Option<Arc<Mutex<WebSocket>>> {
        let mut horizontal = self.horizontal.lock().await;
        horizontal
            .local_adapter
            .get_connection(socket_id, app_id)
            .await
    }

    async fn remove_connection(&mut self, socket_id: &SocketId, app_id: &str) -> Result<()> {
        let mut horizontal = self.horizontal.lock().await;
        horizontal
            .local_adapter
            .remove_connection(socket_id, app_id)
            .await
    }

    async fn send_message(
        &mut self,
        app_id: &str,
        socket_id: &SocketId,
        message: PusherMessage,
    ) -> Result<()> {
        // This likely sends directly to a specific socket, lock scope depends on local_adapter.
        let mut horizontal = self.horizontal.lock().await;
        horizontal
            .local_adapter
            .send_message(app_id, socket_id, message)
            .await
    }

    /// Send to a channel (Optimized Lock Scope)
    async fn send(
        &mut self,
        channel: &str,
        message: PusherMessage,
        except: Option<&SocketId>,
        app_id: &str,
    ) -> Result<()> {
        // --- Optimization: Minimize lock duration ---
        let (node_id, broadcast_data) = {
            // 1. Lock and perform local send
            let mut horizontal_lock = self.horizontal.lock().await;
            let local_send_result = horizontal_lock
                .local_adapter
                .send(channel, message.clone(), except, app_id)
                .await;

            // Log local send errors if necessary, but continue to broadcast
            if let Err(e) = local_send_result {
                Log::warning_title(format!(
                    "Local send failed during broadcast for channel {}: {}",
                    channel, e
                ));
            }

            // 2. Prepare data needed for broadcast *outside* the lock
            (
                horizontal_lock.node_id.clone(),
                (
                    app_id.to_string(),
                    channel.to_string(),
                    except.map(|id| id.0.clone()),
                ),
            )
            // 3. Lock released here
        };

        // 4. Serialize the original message (outside the lock)
        let message_json = serde_json::to_string(&message)?;

        // 5. Create broadcast message (outside the lock)
        let broadcast = BroadcastMessage {
            node_id, // Cloned node_id
            app_id: broadcast_data.0,
            channel: broadcast_data.1,
            message: message_json, // Serialized message
            except_socket_id: broadcast_data.2,
        };

        Log::info(format!("Broadcasting message: {:?}", broadcast));

        // 6. Serialize broadcast message (outside the lock)
        let broadcast_data = serde_json::to_vec(&broadcast)?;

        // 7. Publish to NATS (outside the lock)
        self.client
            .publish(
                Subject::from(self.broadcast_subject.clone()),
                broadcast_data.into(),
            )
            .await
            .map_err(|e| Error::InternalError(format!("Failed to publish broadcast: {}", e)))?;

        Ok(())
    }

    async fn get_channel_members(
        &mut self,
        app_id: &str,
        channel: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        let node_count = self.get_node_count().await?; // Fetch node count first
        let mut horizontal = self.horizontal.lock().await; // Lock for local + remote request

        // Get local members first
        let mut members = horizontal
            .local_adapter
            .get_channel_members(app_id, channel)
            .await?;

        // Get distributed members if needed
        if node_count > 1 {
            // send_request handles its own locking/timing
            let response_data = horizontal
                .send_request(
                    app_id,
                    RequestType::ChannelMembers,
                    Some(channel),
                    None,
                    None,
                    node_count,
                )
                .await?;
            members.extend(response_data.members);
        }

        Ok(members)
    }

    // Returns only local sockets - inherent limitation
    async fn get_channel_sockets(
        &mut self,
        app_id: &str,
        channel: &str,
    ) -> Result<DashMap<SocketId, Arc<Mutex<WebSocket>>>> {
        let mut horizontal = self.horizontal.lock().await;
        horizontal
            .local_adapter
            .get_channel_sockets(app_id, channel)
            .await
    }

    async fn get_channel(&mut self, app_id: &str, channel: &str) -> Result<DashSet<SocketId>> {
        let node_count = self.get_node_count().await?;

        // Get local channel data with minimal lock duration
        let mut result = {
            let mut horizontal = self.horizontal.lock().await;
            horizontal
                .local_adapter
                .get_channel(app_id, channel)
                .await?
        };

        // Get distributed channels with a separate lock acquisition
        if node_count > 1 {
            let response_data = {
                let mut horizontal = self.horizontal.lock().await;
                horizontal
                    .send_request(
                        app_id,
                        RequestType::ChannelSockets,
                        Some(channel),
                        None,
                        None,
                        node_count,
                    )
                    .await?
            };

            // Add remote sockets to the result (outside of lock)
            for socket_id in response_data.socket_ids {
                result.insert(SocketId(socket_id));
            }
        }

        Ok(result)
    }

    async fn remove_channel(&mut self, app_id: &str, channel: &str) {
        // This seems purely local, lock scope depends on local_adapter
        let mut horizontal = self.horizontal.lock().await;
        horizontal
            .local_adapter
            .remove_channel(app_id, channel)
            .await
    }

    async fn is_in_channel(
        &mut self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        let node_count = self.get_node_count().await?; // Get count first
        let mut horizontal = self.horizontal.lock().await; // Lock for local + potential remote

        Log::warning(format!(
            "Checking if socket {} is in channel {} locally",
            socket_id, channel
        ));
        let local_result = horizontal
            .local_adapter
            .is_in_channel(app_id, channel, socket_id)
            .await?;

        if local_result {
            Log::warning_title(format!(
                "Socket {} found in channel {} locally",
                socket_id, channel
            ));
            return Ok(true);
        }

        // If not found locally, check other nodes if they exist
        if node_count > 1 {
            Log::warning_title(format!(
                "Checking remote nodes for socket {} in channel {}",
                socket_id, channel
            ));
            // send_request handles its own locking/timing
            let response_data = horizontal
                .send_request(
                    app_id,
                    RequestType::SocketExistsInChannel,
                    Some(channel),
                    Some(&socket_id.0),
                    None,
                    node_count,
                )
                .await?;
            Log::warning_title(format!(
                "Remote check result for socket {} in channel {}: {}",
                socket_id, channel, response_data.exists
            ));
            return Ok(response_data.exists);
        }

        Log::warning_title(format!(
            "Socket {} NOT found in channel {} (only local node checked or remote check negative)",
            socket_id, channel
        ));
        Ok(false) // Not found locally, and no other nodes or not found remotely
    }

    // Returns only local sockets - inherent limitation
    async fn get_user_sockets(
        &mut self,
        user_id: &str,
        app_id: &str,
    ) -> Result<DashSet<WebSocketRef>> {
        let mut horizontal = self.horizontal.lock().await;
        horizontal
            .local_adapter
            .get_user_sockets(user_id, app_id)
            .await
    }

    async fn cleanup_connection(&mut self, app_id: &str, ws: WebSocketRef) {
        let mut horizontal = self.horizontal.lock().await;
        horizontal
            .local_adapter
            .cleanup_connection(app_id, ws)
            .await
    }

    async fn add_channel_to_sockets(&mut self, app_id: &str, channel: &str, socket_id: &SocketId) {
        // Seems purely local
        let mut horizontal = self.horizontal.lock().await;
        horizontal
            .local_adapter
            .add_channel_to_sockets(app_id, channel, socket_id)
            .await
    }

    async fn get_channel_socket_count(&mut self, app_id: &str, channel: &str) -> usize {
        let node_count = self.get_node_count().await.unwrap_or(1); // Get count first
        let mut horizontal = self.horizontal.lock().await; // Lock for local + potential remote

        // Get local count
        let local_count = horizontal
            .local_adapter
            .get_channel_socket_count(app_id, channel)
            .await;

        // Get distributed count if needed
        if node_count > 1 {
            // send_request handles its own locking/timing
            match horizontal
                .send_request(
                    app_id,
                    RequestType::ChannelSocketsCount,
                    Some(channel),
                    None,
                    None,
                    node_count,
                )
                .await
            {
                Ok(response_data) => local_count + response_data.sockets_count,
                Err(e) => {
                    Log::error(format!(
                        "Failed to get remote socket count for channel {}: {}",
                        channel, e
                    ));
                    local_count // Return local count on error
                }
            }
        } else {
            local_count
        }
    }

    async fn add_to_channel(
        &mut self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        // Seems purely local
        let mut horizontal = self.horizontal.lock().await;
        Log::warning_title(format!(
            "Adding socket {} to channel {}",
            socket_id, channel
        ));
        horizontal
            .local_adapter
            .add_to_channel(app_id, channel, socket_id)
            .await
    }

    async fn remove_from_channel(
        &mut self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        // Seems purely local
        let mut horizontal = self.horizontal.lock().await;
        horizontal
            .local_adapter
            .remove_from_channel(app_id, channel, socket_id)
            .await
    }

    async fn get_presence_member(
        &mut self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<PresenceMemberInfo> {
        // Seems purely local
        let mut horizontal = self.horizontal.lock().await;
        horizontal
            .local_adapter
            .get_presence_member(app_id, channel, socket_id)
            .await
    }

    // Public method using the optimized internal call
    async fn terminate_user_connections(&mut self, app_id: &str, user_id: &str) -> Result<()> {
        self.terminate_connection(app_id, user_id).await
    }

    async fn add_user(&mut self, ws: Arc<Mutex<WebSocket>>) -> Result<()> {
        // Seems purely local
        let mut horizontal = self.horizontal.lock().await;
        horizontal.local_adapter.add_user(ws).await
    }

    async fn remove_user(&mut self, ws: Arc<Mutex<WebSocket>>) -> Result<()> {
        // Seems purely local
        let mut horizontal = self.horizontal.lock().await;
        horizontal.local_adapter.remove_user(ws).await
    }

    async fn terminate_connection(&mut self, app_id: &str, user_id: &str) -> Result<()> {
        let node_count = self.get_node_count().await?; // Get count first
        let mut horizontal = self.horizontal.lock().await; // Lock for local + potential remote

        // First terminate locally
        horizontal
            .local_adapter
            .terminate_connection(app_id, user_id)
            .await?; // Propagate local errors

        // Then broadcast to other nodes if needed
        if node_count > 1 {
            // send_request handles its own locking/timing
            // We ignore the result here as it's a "fire and forget" termination broadcast
            let _ = horizontal
                .send_request(
                    app_id,
                    RequestType::TerminateUserConnections,
                    None,
                    None,
                    Some(user_id),
                    node_count,
                )
                .await;
        }

        Ok(())
    }

    async fn get_channels_with_socket_count(
        &mut self,
        app_id: &str,
    ) -> Result<DashMap<String, usize>> {
        let node_count = self.get_node_count().await?; // Get count first
        let mut horizontal = self.horizontal.lock().await; // Lock for local + potential remote

        // Get local channels
        let local_channels = horizontal
            .local_adapter
            .get_channels_with_socket_count(app_id)
            .await?;

        // Get distributed channels if needed
        if node_count > 1 {
            // send_request handles its own locking/timing
            let response_data = horizontal
                .send_request(
                    app_id,
                    RequestType::ChannelsWithSocketsCount,
                    None,
                    None,
                    None,
                    node_count,
                )
                .await?;
            for (channel, count) in response_data.channels_with_sockets_count {
                local_channels.insert(channel, count);
            }
        }

        Ok(local_channels)
    }
}
