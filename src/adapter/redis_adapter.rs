use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use fastwebsockets::WebSocketWrite;
use futures::StreamExt;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use redis::{AsyncCommands, AsyncConnectionConfig};
use serde::{Deserialize, Serialize};
use serde_json::json;
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

/// Redis channels
pub const DEFAULT_PREFIX: &str = "sockudo";
const BROADCAST_SUFFIX: &str = "#broadcast";
const REQUESTS_SUFFIX: &str = "#requests";
const RESPONSES_SUFFIX: &str = "#responses";

/// Redis adapter configuration
#[derive(Debug, Clone)]
pub struct RedisAdapterConfig {
    /// Redis URL
    pub url: String,
    /// Channel prefix
    pub prefix: String,
    /// Request timeout in milliseconds
    pub request_timeout_ms: u64,
    /// Use connection manager for auto-reconnection
    pub use_connection_manager: bool,
    /// Cluster mode (for Redis Cluster)
    pub cluster_mode: bool,
}

impl Default for RedisAdapterConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379/".to_string(),
            prefix: DEFAULT_PREFIX.to_string(),
            request_timeout_ms: 5000,
            use_connection_manager: true,
            cluster_mode: false,
        }
    }
}

/// Redis adapter for horizontal scaling (Optimized Version)
pub struct RedisAdapter {
    /// Base horizontal adapter (protected by a Mutex)
    /// Optimization Note: While the Mutex remains, methods now try to minimize lock duration.
    /// Further optimization would require refactoring HorizontalAdapter internals.
    pub horizontal: Arc<Mutex<HorizontalAdapter>>,

    /// Redis client
    pub client: redis::Client,

    /// Redis connection for publishing (Multiplexed for efficiency)
    pub connection: redis::aio::MultiplexedConnection,

    /// Channel names
    pub prefix: String,
    pub broadcast_channel: String,
    pub request_channel: String,
    pub response_channel: String,

    /// Configuration
    pub config: RedisAdapterConfig,
}

impl RedisAdapter {
    /// Create a new Redis adapter
    pub async fn new(config: RedisAdapterConfig) -> Result<Self> {
        // Create the base horizontal adapter
        let mut horizontal = HorizontalAdapter::new();
        Log::info(format!("Redis adapter config: {:?}", config));

        // Set timeout
        horizontal.requests_timeout = config.request_timeout_ms;

        // Create Redis client
        let client = redis::Client::open(&*config.url)
            .map_err(|e| Error::RedisError(format!("Failed to create Redis client: {}", e)))?;

        // Get connection based on configuration
        let connection = if config.use_connection_manager {
            client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| {
                    Error::RedisError(format!("Failed to create connection manager: {}", e))
                })?
        } else {
            client
                .get_multiplexed_tokio_connection()
                .await
                .map_err(|e| Error::RedisError(format!("Failed to connect to Redis: {}", e)))?
        };

        // Build channel names
        let broadcast_channel = format!("{}:{}", config.prefix, BROADCAST_SUFFIX);
        let request_channel = format!("{}:{}", config.prefix, REQUESTS_SUFFIX);
        let response_channel = format!("{}:{}", config.prefix, RESPONSES_SUFFIX);

        // Create the adapter
        let adapter = Self {
            horizontal: Arc::new(Mutex::new(horizontal)),
            client,
            connection,
            prefix: config.prefix.clone(),
            broadcast_channel,
            request_channel,
            response_channel,
            config,
        };

        Ok(adapter)
    }

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

    // Method to initialize metrics during adapter startup
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

    /// Create a new Redis adapter with simple configuration
    pub async fn with_url(redis_url: &str) -> Result<Self> {
        let config = RedisAdapterConfig {
            url: redis_url.to_string(),
            ..Default::default()
        };
        Self::new(config).await
    }

    /// Start listening for Redis messages
    pub async fn start_listeners(&self) -> Result<()> {
        // Lock needed only for starting cleanup task
        {
            let mut horizontal = self.horizontal.lock().await;
            // Start cleanup task
            horizontal.start_request_cleanup();
        } // Lock released here

        // Start PubSub listeners
        self.start_listeners_pubsub().await?;

        Ok(())
    }

    /// Start traditional PubSub listeners (Optimized with task spawning)
    async fn start_listeners_pubsub(&self) -> Result<()> {
        // Create a subscription connection (separate from the multiplexed one)
        let sub_client = self.client.clone();

        // Clone needed values for the async task
        // Clone Arc for cheap sharing across tasks
        let horizontal_arc = self.horizontal.clone();
        let pub_connection = self.connection.clone();
        let broadcast_channel = self.broadcast_channel.clone();
        let request_channel = self.request_channel.clone();
        let response_channel = self.response_channel.clone();

        // Get node_id without holding the lock for the whole setup
        let node_id = {
            let horizontal_lock = horizontal_arc.lock().await;
            horizontal_lock.node_id.clone()
        };

        // Spawn the main listener task
        tokio::spawn(async move {
            // Create a pubsub connection
            let mut pubsub = match sub_client.get_async_pubsub().await {
                Ok(pubsub) => pubsub,
                Err(e) => {
                    Log::error(format!("Failed to get pubsub connection: {}", e));
                    // Consider adding retry logic or more robust error handling here
                    return;
                }
            };

            // Subscribe to all channels
            // Using psubscribe for potential pattern matching flexibility if needed later,
            // but currently checking exact channel names.
            if let Err(e) = pubsub
                .subscribe(&[&broadcast_channel, &request_channel, &response_channel])
                .await
            {
                Log::error(format!("Failed to subscribe to channels: {}", e));
                return;
            }

            Log::info(format!(
                "Redis adapter listening on channels: {}, {}, {}",
                broadcast_channel, request_channel, response_channel
            ));

            // Listen for messages
            let mut message_stream = pubsub.on_message();

            while let Some(msg) = message_stream.next().await {
                let channel: String = msg.get_channel_name().to_string();
                let payload_result: redis::RedisResult<String> = msg.get_payload();

                if let Ok(payload) = payload_result {
                    // --- Optimization: Process each message type in its own task ---
                    let horizontal_clone = horizontal_arc.clone();
                    let node_id_clone = node_id.clone();
                    let pub_connection_clone = pub_connection.clone();
                    let broadcast_channel_clone = broadcast_channel.clone();
                    let request_channel_clone = request_channel.clone();
                    let response_channel_clone = response_channel.clone();

                    tokio::spawn(async move {
                        // Process based on channel name
                        if channel == broadcast_channel_clone {
                            // Handle broadcast message
                            match serde_json::from_str::<BroadcastMessage>(&payload) {
                                Ok(broadcast) => {
                                    // Skip our own messages
                                    if broadcast.node_id == node_id_clone {
                                        return;
                                    }
                                    // Process the broadcast
                                    match serde_json::from_str(&broadcast.message) {
                                        Ok(message) => {
                                            let except_id = broadcast
                                                .except_socket_id
                                                .as_ref()
                                                .map(|id| SocketId(id.clone()));
                                            // Lock only when interacting with local adapter
                                            let mut horizontal_lock = horizontal_clone.lock().await;
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
                                        "Failed to deserialize broadcast message: {}, Payload: {}",
                                        e, payload
                                    ));
                                }
                            }
                        } else if channel == request_channel_clone {
                            // Handle request message
                            match serde_json::from_str::<RequestBody>(&payload) {
                                Ok(request) => {
                                    // Skip our own requests
                                    if request.node_id == node_id_clone {
                                        return;
                                    }
                                    // Process the request (already designed to be async)
                                    // Lock only when processing
                                    let response = {
                                        // Scope for the lock
                                        let mut horizontal_lock = horizontal_clone.lock().await;
                                        horizontal_lock.process_request(request).await
                                    }; // Lock released
                                    if let Ok(response) = response {
                                        // Send response
                                        match serde_json::to_string(&response) {
                                            Ok(response_json) => {
                                                let mut conn = pub_connection_clone.clone();
                                                if let Err(e) = conn
                                                    .publish::<_, _, ()>(
                                                        &response_channel_clone,
                                                        response_json,
                                                    )
                                                    .await
                                                {
                                                    Log::error(format!(
                                                        "Failed to publish response: {}",
                                                        e
                                                    ));
                                                }
                                            }
                                            Err(e) => {
                                                Log::error(format!(
                                                    "Failed to serialize response: {}",
                                                    e
                                                ));
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    Log::warning(format!(
                                        "Failed to deserialize request message: {}, Payload: {}",
                                        e, payload
                                    ));
                                }
                            }
                        } else if channel == response_channel_clone {
                            // Handle response message
                            match serde_json::from_str::<ResponseBody>(&payload) {
                                Ok(response) => {
                                    // Skip our own responses
                                    if response.node_id == node_id_clone {
                                        return;
                                    }
                                    // Process the response (already designed to be async)
                                    // Lock only when processing
                                    let mut horizontal_lock = horizontal_clone.lock().await;
                                    let _ = horizontal_lock.process_response(response).await;
                                    // Lock released automatically
                                }
                                Err(e) => {
                                    Log::warning(format!(
                                        "Failed to deserialize response message: {}, Payload: {}",
                                        e, payload
                                    ));
                                }
                            }
                        }
                    }); // End of spawned task for message processing
                } else if let Err(e) = payload_result {
                    Log::error(format!("Failed to get payload from Redis message: {}", e));
                }
            }
            Log::info("Redis Pub/Sub listener stream ended.");
        });

        Ok(())
    }

    /// Get the number of nodes in the cluster (Optimized parsing)
    pub async fn get_node_count(&self) -> Result<usize> {
        if self.config.cluster_mode {
            // TODO: Implement actual Redis Cluster node counting logic
            // This requires querying CLUSTER NODES and potentially aggregating
            // PUBSUB NUMSUB results from multiple nodes. It's complex.
            Log::warning(
                "Cluster mode node count is not fully implemented, returning placeholder.",
            );
            Ok(5) // Placeholder
        } else {
            // Use a cloned connection for the command
            let mut conn = self.connection.clone();

            // Use the PUBSUB NUMSUB command directly
            let result: redis::RedisResult<Vec<redis::Value>> = redis::cmd("PUBSUB")
                .arg("NUMSUB")
                .arg(&self.request_channel)
                .query_async(&mut conn)
                .await;

            match result {
                Ok(values) => {
                    // PUBSUB NUMSUB returns [channel, count] format for a single channel
                    // Or [] if channel doesn't exist or has no subscribers (unlikely for request channel)
                    if values.len() >= 2 {
                        if let redis::Value::Int(count) = values[1] {
                            // Ensure at least 1 node (ourselves)
                            Ok((count as usize).max(1))
                        } else {
                            Log::warning(format!(
                                "Failed to parse PUBSUB NUMSUB count (not an Int): {:?}",
                                values
                            ));
                            Ok(1) // Default to 1 on unexpected format
                        }
                    } else {
                        Log::warning(format!(
                            "PUBSUB NUMSUB returned unexpected result format: {:?}",
                            values
                        ));
                        Ok(1) // Default to 1 if format is wrong (e.g., channel not found)
                    }
                }
                Err(e) => {
                    // Log the error but default to 1 node to avoid breaking logic that relies on node count
                    Log::error(format!("Failed to execute PUBSUB NUMSUB: {}", e));
                    Ok(1) // Default to 1 node on error
                }
            }
        }
    }

    // --- Adapter Trait Implementation ---
    // Most methods primarily delegate to horizontal adapter.
    // Optimization focuses on minimizing lock duration where possible,
    // especially in methods that perform both local actions and remote communication.
}

#[async_trait]
impl Adapter for RedisAdapter {
    async fn init(&mut self) {
        // Lock scope minimized
        {
            let mut horizontal = self.horizontal.lock().await;
            horizontal.local_adapter.init().await;
        } // Lock released

        // Start Redis listeners (already optimized)
        if let Err(e) = self.start_listeners().await {
            Log::error(format!("Failed to start Redis listeners: {}", e));
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
        let broadcast_json = serde_json::to_string(&broadcast)?;

        // 7. Publish to Redis (outside the lock)
        let mut conn = self.connection.clone();
        conn.publish::<_, _, ()>(&self.broadcast_channel, broadcast_json)
            .await
            .map_err(|e| Error::RedisError(format!("Failed to publish broadcast: {}", e)))?;

        Ok(())
    }

    // Methods involving requests to other nodes: Lock scope depends on send_request.
    // `send_request` itself handles locking internally.
    async fn get_channel_members(
        &mut self,
        app_id: &str,
        channel: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        let node_count = self.get_node_count().await?; // Fetch node count first

        // Get local members with minimal lock duration
        let mut members = {
            let mut horizontal = self.horizontal.lock().await;
            horizontal
                .local_adapter
                .get_channel_members(app_id, channel)
                .await?
        };

        // Get distributed members if needed, with a separate lock acquisition
        if node_count > 1 {
            let response_data = {
                let mut horizontal = self.horizontal.lock().await;
                horizontal
                    .send_request(
                        app_id,
                        RequestType::ChannelMembers,
                        Some(channel),
                        None,
                        None,
                        node_count,
                    )
                    .await?
            };
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
        let mut horizontal = self.horizontal.lock().await; // Lock for local + remote request

        // Start with local channel data
        let mut result = horizontal
            .local_adapter
            .get_channel(app_id, channel)
            .await?;

        // Get distributed channels if needed
        if node_count > 1 {
            // send_request handles its own locking/timing
            let response_data = horizontal
                .send_request(
                    app_id,
                    RequestType::ChannelSockets,
                    Some(channel),
                    None,
                    None,
                    node_count,
                )
                .await?;

            // Add remote sockets to the result
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

        // Then broadcast to other nodes if needed
        if node_count > 1 {
            // send_request handles its own locking/timing
            // We ignore the result here as it's a "fire and forget" termination broadcast
            match horizontal
                .send_request(
                    app_id,
                    RequestType::ChannelsWithSocketsCount,
                    None,
                    None,
                    None,
                    node_count,
                )
                .await
            {
                Ok(response_data) => {
                    // Merge the local and remote data
                    let mut channels = horizontal
                        .local_adapter
                        .get_channels_with_socket_count(app_id)
                        .await?;
                    for channel in response_data.channels_with_sockets_count {
                        channels.insert(channel.0, channel.1);
                    }
                    return Ok(channels);
                }
                Err(e) => {
                    Log::error(format!(
                        "Failed to get remote channels with socket count: {}",
                        e
                    ));
                }
            }
        }

        Ok(horizontal
            .local_adapter
            .get_channels_with_socket_count(app_id)
            .await?)
    }
}
