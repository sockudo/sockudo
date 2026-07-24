use crate::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::horizontal_transport::{HorizontalTransport, TransportConfig, TransportHandlers};
use async_trait::async_trait;
use redis::cluster_read_routing::RandomReplicaStrategy;
use sockudo_core::error::{Error, Result};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::RedisClusterAdapterConfig;

use redis::AsyncCommands;
use redis::cluster::{ClusterClient, ClusterClientBuilder};
use redis::cluster_async::ClusterConnection;

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

/// Helper function to borrow a string view from redis::Value when possible.
fn value_to_str(v: &redis::Value) -> Option<&str> {
    match v {
        redis::Value::BulkString(bytes) => std::str::from_utf8(bytes).ok(),
        redis::Value::SimpleString(s) => Some(s.as_str()),
        redis::Value::VerbatimString { format: _, text } => Some(text.as_str()),
        _ => None,
    }
}

fn value_to_bytes(v: &redis::Value) -> Option<&[u8]> {
    match v {
        redis::Value::BulkString(bytes) => Some(bytes),
        redis::Value::SimpleString(s) => Some(s.as_bytes()),
        redis::Value::VerbatimString { format: _, text } => Some(text.as_bytes()),
        _ => None,
    }
}

impl TransportConfig for RedisClusterAdapterConfig {
    fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }

    fn prefix(&self) -> &str {
        &self.prefix
    }
}

/// Redis Cluster transport implementation
///
/// Note: ClusterConnection is cheap to clone and thread-safe (like MultiplexedConnection).
/// Per redis-rs docs: "Async cluster connections also don't require pooling and are thread-safe and reusable."
/// We store a single connection and clone it for each operation, avoiding the overhead of
/// creating new connections (which was causing "connection storms").
pub struct RedisClusterTransport {
    /// Client for creating dedicated connections (e.g., health checks)
    /// Kept for potential future reconnection scenarios
    #[allow(dead_code)]
    client: ClusterClient,
    /// Persistent connection for publishing - cloned for each operation (cheap, thread-safe)
    publish_connection: ClusterConnection,
    /// Dedicated connection for health checks to avoid contention with publish operations
    health_check_connection: ClusterConnection,
    broadcast_channel: String,
    request_channel: String,
    response_channel: String,
    config: RedisClusterAdapterConfig,
    use_sharded_pubsub: bool,
    prefix: String,
    reply_channel: String,
    metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
    shutdown: Arc<Notify>,
    is_running: Arc<AtomicBool>,
    owner_count: Arc<AtomicUsize>,
}

#[async_trait]
impl HorizontalTransport for RedisClusterTransport {
    type Config = RedisClusterAdapterConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        let client = ClusterClientBuilder::new(config.nodes.clone())
            .retries(3)
            .read_routing_strategy(RandomReplicaStrategy)
            .build()
            .map_err(|e| Error::Redis(format!("Failed to create Redis Cluster client: {e}")))?;

        // Create a persistent connection for publishing
        // This connection will be reused for all publish operations to avoid connection storms
        let publish_connection = client.get_async_connection().await.map_err(|e| {
            Error::Redis(format!(
                "Failed to create Redis Cluster publish connection: {e}"
            ))
        })?;

        // Create a dedicated connection for health checks
        // This prevents health check timeouts under high publish load (10K+ msg/s)
        let health_check_connection = client.get_async_connection().await.map_err(|e| {
            Error::Redis(format!(
                "Failed to create Redis Cluster health check connection: {e}"
            ))
        })?;

        let broadcast_channel = format!("{}:#broadcast", config.prefix);
        let request_channel = format!("{}:#requests", config.prefix);
        let response_channel = format!("{}:#responses", config.prefix);
        let prefix = config.prefix.clone();
        let uuid = Uuid::new_v4();
        let reply_channel = format!("{}:#reply:{}", prefix, uuid.as_simple());

        let use_sharded_pubsub = config.use_sharded_pubsub;

        if use_sharded_pubsub {
            info!(
                adapter = "redis_cluster",
                "using sharded pub/sub (SSUBSCRIBE/SPUBLISH)"
            );
        } else {
            debug!(
                adapter = "redis_cluster",
                "using standard pub/sub (SUBSCRIBE/PUBLISH)"
            );
        }

        info!(
            adapter = "redis_cluster",
            "transport initialized with dedicated publish and health check connections"
        );

        Ok(Self {
            client,
            publish_connection,
            health_check_connection,
            broadcast_channel,
            request_channel,
            response_channel,
            config,
            use_sharded_pubsub,
            prefix,
            reply_channel,
            metrics: Arc::new(OnceLock::new()),
            shutdown: Arc::new(Notify::new()),
            is_running: Arc::new(AtomicBool::new(true)),
            owner_count: Arc::new(AtomicUsize::new(1)),
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        let broadcast_json = sonic_rs::to_string(message)?;

        // Retry logic with exponential backoff using persistent connection
        let mut retry_delay = 100u64; // Start with 100ms
        const MAX_RETRIES: u32 = 3;
        const MAX_RETRY_DELAY: u64 = 1000; // Max 1 second

        for attempt in 0..=MAX_RETRIES {
            // Clone the connection (cheap, thread-safe per redis-rs docs)
            let mut conn = self.publish_connection.clone();

            // Use SPUBLISH for sharded pub/sub if enabled, otherwise standard PUBLISH
            let publish_result: redis::RedisResult<()> = if self.use_sharded_pubsub {
                conn.spublish(&self.broadcast_channel, &broadcast_json)
                    .await
            } else {
                conn.publish(&self.broadcast_channel, &broadcast_json).await
            };

            match publish_result {
                Ok(_) => {
                    if attempt > 0 {
                        debug!(
                            adapter = "redis_cluster",
                            attempt, "broadcast publish succeeded on retry"
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    if attempt == MAX_RETRIES {
                        return Err(Error::Redis(format!(
                            "Failed to publish broadcast after {} retries: {e}",
                            MAX_RETRIES
                        )));
                    }
                    warn!(adapter = "redis_cluster", attempt = attempt + 1, error = %e, retryable = true, "broadcast publish failed");
                    tokio::time::sleep(Duration::from_millis(retry_delay)).await;
                    retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                }
            }
        }

        unreachable!("Retry loop should have returned");
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        let request_json = sonic_rs::to_string(request)
            .map_err(|e| Error::Other(format!("Failed to serialize request: {e}")))?;

        // Retry logic with exponential backoff using persistent connection
        let mut retry_delay = 100u64;
        const MAX_RETRIES: u32 = 3;
        const MAX_RETRY_DELAY: u64 = 1000;

        for attempt in 0..=MAX_RETRIES {
            // Clone the connection (cheap, thread-safe per redis-rs docs)
            let mut conn = self.publish_connection.clone();

            // Use SPUBLISH for sharded pub/sub if enabled, otherwise standard PUBLISH
            let publish_result: redis::RedisResult<i32> = if self.use_sharded_pubsub {
                conn.spublish(&self.request_channel, &request_json).await
            } else {
                conn.publish(&self.request_channel, &request_json).await
            };

            match publish_result {
                Ok(subscriber_count) => {
                    if attempt > 0 {
                        debug!(
                            adapter = "redis_cluster",
                            attempt, "request publish succeeded on retry"
                        );
                    }
                    trace!(
                        adapter = "redis_cluster",
                        request_id = %request.request_id,
                        subscriber_count,
                        "request published to transport"
                    );
                    return Ok(());
                }
                Err(e) => {
                    if attempt == MAX_RETRIES {
                        return Err(Error::Redis(format!(
                            "Failed to publish request after {} retries: {e}",
                            MAX_RETRIES
                        )));
                    }
                    warn!(adapter = "redis_cluster", attempt = attempt + 1, error = %e, retryable = true, "request publish failed");
                    tokio::time::sleep(Duration::from_millis(retry_delay)).await;
                    retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                }
            }
        }

        unreachable!("Retry loop should have returned");
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        let response_json = sonic_rs::to_string(response)
            .map_err(|e| Error::Other(format!("Failed to serialize response: {e}")))?;

        // Retry logic with exponential backoff using persistent connection
        let mut retry_delay = 100u64;
        const MAX_RETRIES: u32 = 3;
        const MAX_RETRY_DELAY: u64 = 1000;

        for attempt in 0..=MAX_RETRIES {
            // Clone the connection (cheap, thread-safe per redis-rs docs)
            let mut conn = self.publish_connection.clone();

            // Use SPUBLISH for sharded pub/sub if enabled, otherwise standard PUBLISH
            let publish_result: redis::RedisResult<()> = if self.use_sharded_pubsub {
                conn.spublish(&self.response_channel, &response_json).await
            } else {
                conn.publish(&self.response_channel, &response_json).await
            };

            match publish_result {
                Ok(_) => {
                    if attempt > 0 {
                        debug!(
                            adapter = "redis_cluster",
                            attempt, "response publish succeeded on retry"
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    if attempt == MAX_RETRIES {
                        return Err(Error::Redis(format!(
                            "Failed to publish response after {} retries: {e}",
                            MAX_RETRIES
                        )));
                    }
                    warn!(adapter = "redis_cluster", attempt = attempt + 1, error = %e, retryable = true, "response publish failed");
                    tokio::time::sleep(Duration::from_millis(retry_delay)).await;
                    retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                }
            }
        }

        unreachable!("Retry loop should have returned");
    }

    fn new_inbox(&self) -> Option<String> {
        Some(self.reply_channel.clone())
    }

    async fn publish_request_with_reply(
        &self,
        request: &RequestBody,
        reply_to: &str,
    ) -> Result<()> {
        let mut request = request.clone();
        request.reply_to = Some(reply_to.to_string());
        self.publish_request(&request).await
    }

    async fn publish_request_to_node(
        &self,
        request: &RequestBody,
        target_node_id: &str,
    ) -> Result<()> {
        let target_channel = format!("{}:#node:{}", self.prefix, target_node_id);
        let request_json = sonic_rs::to_string(request)
            .map_err(|e| Error::Other(format!("Failed to serialize request: {e}")))?;
        let mut conn = self.publish_connection.clone();

        let publish_result: redis::RedisResult<()> = if self.use_sharded_pubsub {
            conn.spublish(&target_channel, &request_json).await
        } else {
            conn.publish(&target_channel, &request_json).await
        };

        publish_result.map_err(|e| {
            Error::Redis(format!("Failed to publish to node {target_node_id}: {e}"))
        })?;
        trace!(adapter = "redis_cluster", request_id = %request.request_id, target_node_id = %target_node_id, "request published to node");
        Ok(())
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        // Clone needed values for the async task
        let broadcast_channel = self.broadcast_channel.clone();
        let request_channel = self.request_channel.clone();
        let response_channel = self.response_channel.clone();
        let nodes = self.config.nodes.clone();
        let use_sharded_pubsub = self.use_sharded_pubsub;
        // Clone the publish connection for use in handlers (cheap, thread-safe)
        let publish_connection = self.publish_connection.clone();
        let metrics = self.metrics.clone();
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();
        let node_channel = format!("{}:#node:{}", self.prefix, handlers.node_id);
        let reply_channel = self.reply_channel.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        // Spawn the main listener task with reconnection logic
        tokio::spawn(async move {
            let mut retry_delay = 500u64; // Start with 500ms delay
            const MAX_RETRY_DELAY: u64 = 10_000; // Max 10 seconds
            let mut reconnection_count = 0u64;
            let mut ready_tx = Some(ready_tx);

            loop {
                if !is_running.load(Ordering::Relaxed) {
                    break;
                }
                let channels_owned = vec![
                    broadcast_channel.clone(),
                    request_channel.clone(),
                    response_channel.clone(),
                    node_channel.clone(),
                    reply_channel.clone(),
                ];

                let subscriber = if use_sharded_pubsub {
                    use crate::transports::redis_cluster_sharded_pubsub::ShardedSubscriber;

                    ShardedSubscriber::new(
                        channels_owned,
                        nodes.clone(),
                        metrics.clone(),
                        is_running.clone(),
                        shutdown.clone(),
                    )
                } else {
                    use crate::transports::redis_cluster_sharded_pubsub::ShardedSubscriber;

                    ShardedSubscriber::new_standard(
                        channels_owned,
                        nodes.clone(),
                        metrics.clone(),
                        is_running.clone(),
                        shutdown.clone(),
                    )
                };

                let rx = match subscriber.start().await {
                    Ok(rx) => {
                        retry_delay = 500;
                        if reconnection_count > 0 {
                            debug!(
                                adapter = "redis_cluster",
                                reconnection_count, "pubsub reconnected successfully"
                            );
                        }
                        if let Some(ready_tx) = ready_tx.take() {
                            let _ = ready_tx.send(());
                        }
                        rx
                    }
                    Err(e) => {
                        reconnection_count += 1;
                        if let Some(metrics) = metrics.get() {
                            metrics.mark_horizontal_transport_reconnection("redis_cluster");
                        }
                        error!(adapter = "redis_cluster", retry_delay_ms = retry_delay, error = %e, "pubsub subscriber start failed");
                        tokio::select! {
                            _ = shutdown.notified() => break,
                            _ = tokio::time::sleep(Duration::from_millis(retry_delay)) => {}
                        }
                        retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                        continue;
                    }
                };

                debug!(
                    adapter = "redis_cluster",
                    broadcast_channel = %broadcast_channel,
                    request_channel = %request_channel,
                    response_channel = %response_channel,
                    node_channel = %node_channel,
                    reply_channel = %reply_channel,
                    "transport subscriptions established"
                );

                // Reset reconnection count on successful subscription
                reconnection_count = 0;

                // Process messages from the channel - PushInfo is the message type for RESP3
                loop {
                    if !is_running.load(Ordering::Relaxed) {
                        break;
                    }
                    let recv_result = tokio::select! {
                        _ = shutdown.notified() => break,
                        result = tokio::time::timeout(Duration::from_millis(100), rx.recv()) => result,
                    };
                    let Ok(Ok(push_info)) = recv_result else {
                        if matches!(recv_result, Ok(Err(_))) {
                            break;
                        }
                        continue;
                    };
                    // Extract channel and payload from PushInfo
                    // Handle both standard (Message) and sharded (SMessage) pub/sub messages
                    let is_message = matches!(
                        push_info.kind,
                        redis::PushKind::Message | redis::PushKind::SMessage
                    );
                    if !is_message {
                        continue; // Skip non-message push notifications
                    }

                    // PushInfo.data for messages should be [channel, payload]
                    if push_info.data.len() < 2 {
                        if let Some(metrics) = metrics.get() {
                            metrics.mark_horizontal_transport_message_dropped("redis_cluster");
                        }
                        error!(adapter = "redis_cluster", "invalid push message format");
                        continue;
                    }

                    let channel = match value_to_str(&push_info.data[0]) {
                        Some(s) => s,
                        None => {
                            if let Some(metrics) = metrics.get() {
                                metrics.mark_horizontal_transport_message_dropped("redis_cluster");
                            }
                            error!(
                                adapter = "redis_cluster",
                                "push message channel name parse failed"
                            );
                            continue;
                        }
                    };

                    enum ChannelKind {
                        Broadcast,
                        Request,
                        Response,
                        NodeRequest,
                        Reply,
                    }

                    let channel_kind = if channel == broadcast_channel.as_str() {
                        ChannelKind::Broadcast
                    } else if channel == request_channel.as_str() {
                        ChannelKind::Request
                    } else if channel == response_channel.as_str() {
                        ChannelKind::Response
                    } else if channel == node_channel.as_str() {
                        ChannelKind::NodeRequest
                    } else if channel == reply_channel.as_str() {
                        ChannelKind::Reply
                    } else {
                        continue;
                    };

                    let payload = match value_to_bytes(&push_info.data[1]).map(<[u8]>::to_vec) {
                        Some(bytes) => bytes,
                        None => {
                            if let Some(metrics) = metrics.get() {
                                metrics.mark_horizontal_transport_message_dropped("redis_cluster");
                            }
                            error!(
                                adapter = "redis_cluster",
                                "push message payload parse failed"
                            );
                            continue;
                        }
                    };

                    match channel_kind {
                        ChannelKind::Broadcast => {
                            let broadcast_handler = handlers.on_broadcast.clone();
                            let metrics_clone = metrics.clone();

                            tokio::spawn(async move {
                                if let Ok(broadcast) =
                                    sonic_rs::from_slice::<BroadcastMessage>(&payload)
                                {
                                    broadcast_handler(broadcast).await;
                                } else if let Some(metrics) = metrics_clone.get() {
                                    metrics
                                        .mark_horizontal_transport_message_dropped("redis_cluster");
                                }
                            });
                        }
                        ChannelKind::Request | ChannelKind::NodeRequest => {
                            let request_handler = handlers.on_request.clone();
                            let publish_conn = publish_connection.clone();
                            let response_channel_clone = response_channel.clone();
                            let metrics_clone = metrics.clone();
                            let sharded = use_sharded_pubsub;

                            tokio::spawn(async move {
                                if let Ok(request) = sonic_rs::from_slice::<RequestBody>(&payload) {
                                    let reply_to = request.reply_to.clone();
                                    let response_result = request_handler(request).await;

                                    if let Ok(response) = response_result
                                        && let Ok(response_json) = sonic_rs::to_string(&response)
                                    {
                                        let target = reply_to.unwrap_or(response_channel_clone);
                                        // Clone connection (cheap, thread-safe per redis-rs docs)
                                        let mut conn = publish_conn.clone();

                                        let result: redis::RedisResult<()> = if sharded {
                                            conn.spublish(&target, &response_json).await
                                        } else {
                                            conn.publish(&target, response_json).await
                                        };
                                        if let Err(e) = result {
                                            warn!(adapter = "redis_cluster", error = %e, "response publish failed");
                                        }
                                    }
                                } else if let Some(metrics) = metrics_clone.get() {
                                    metrics
                                        .mark_horizontal_transport_message_dropped("redis_cluster");
                                }
                            });
                        }
                        ChannelKind::Response => {
                            let response_handler = handlers.on_response.clone();
                            let metrics_clone = metrics.clone();

                            tokio::spawn(async move {
                                if let Ok(response) = sonic_rs::from_slice::<ResponseBody>(&payload)
                                {
                                    response_handler(response).await;
                                } else if let Some(metrics) = metrics_clone.get() {
                                    metrics
                                        .mark_horizontal_transport_message_dropped("redis_cluster");
                                }
                            });
                        }
                        ChannelKind::Reply => {
                            let response_handler = handlers.on_response.clone();
                            let metrics_clone = metrics.clone();

                            tokio::spawn(async move {
                                if let Ok(response) = sonic_rs::from_slice::<ResponseBody>(&payload)
                                {
                                    response_handler(response).await;
                                } else if let Some(metrics) = metrics_clone.get() {
                                    metrics
                                        .mark_horizontal_transport_message_dropped("redis_cluster");
                                }
                            });
                        }
                    }
                }

                // Connection ended, reconnect with exponential backoff
                if let Some(metrics) = metrics.get() {
                    metrics.mark_horizontal_transport_reconnection("redis_cluster");
                }
                warn!(
                    adapter = "redis_cluster",
                    retryable = true,
                    "pubsub connection ended"
                );
                tokio::select! {
                    _ = shutdown.notified() => break,
                    _ = tokio::time::sleep(Duration::from_millis(retry_delay)) => {}
                }
                retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
            }
        });

        tokio::time::timeout(Duration::from_secs(5), ready_rx)
            .await
            .map_err(|_| {
                Error::Redis("Redis Cluster PubSub initial subscription timed out".to_string())
            })?
            .map_err(|_| {
                Error::Redis("Redis Cluster PubSub listener stopped before subscribing".to_string())
            })?;

        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        if !self.use_sharded_pubsub {
            return Ok(1);
        }

        use redis::cluster_routing::{Route, RoutingInfo, SingleNodeRoutingInfo, Slot, SlotAddr};

        // PUBSUB SHARDNUMSUB must target the shard master that owns the
        // request channel's slot. redis-rs routes this command to AllNodes
        // by default, so we force SpecificNode routing via route_command.
        let slot = Slot::for_key(&self.request_channel);
        let routing = RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(
            Route::with_slot(slot, SlotAddr::Master),
        ));

        let mut cmd = redis::cmd("PUBSUB");
        cmd.arg("SHARDNUMSUB").arg(&self.request_channel);

        // Clone connection (cheap, thread-safe per redis-rs docs)
        let mut conn = self.publish_connection.clone();
        let result = conn.route_command(cmd, routing).await;

        match result {
            Ok(redis::Value::Array(values)) if values.len() >= 2 => {
                if let redis::Value::Int(count) = values[1] {
                    Ok((count as usize).max(1))
                } else {
                    Ok(1)
                }
            }
            Ok(_) => Ok(1),
            Err(e) => {
                error!(adapter = "redis_cluster", error = %e, "SHARDNUMSUB on shard master failed");
                Ok(1)
            }
        }
    }

    fn node_count_is_real_time(&self) -> bool {
        self.use_sharded_pubsub
    }

    async fn check_health(&self) -> Result<()> {
        // Use dedicated health check connection to avoid contention with publish operations
        // Under high load (10K+ msg/s), sharing the publish connection causes timeouts
        let mut conn = self.health_check_connection.clone();

        let response = redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| Error::Redis(format!("Cluster health check PING failed: {e}")))?;

        if response == "PONG" {
            Ok(())
        } else {
            Err(Error::Redis(format!(
                "Cluster PING returned unexpected response: {response}"
            )))
        }
    }

    fn set_metrics(&self, metrics: Arc<dyn MetricsInterface + Send + Sync>) {
        let _ = self.metrics.set(metrics);
    }
}

impl Drop for RedisClusterTransport {
    fn drop(&mut self) {
        if self.owner_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.is_running.store(false, Ordering::Relaxed);
            self.shutdown.notify_waiters();
        }
    }
}

impl Clone for RedisClusterTransport {
    fn clone(&self) -> Self {
        self.owner_count.fetch_add(1, Ordering::Relaxed);
        Self {
            client: self.client.clone(),
            publish_connection: self.publish_connection.clone(),
            health_check_connection: self.health_check_connection.clone(),
            broadcast_channel: self.broadcast_channel.clone(),
            request_channel: self.request_channel.clone(),
            response_channel: self.response_channel.clone(),
            config: self.config.clone(),
            use_sharded_pubsub: self.use_sharded_pubsub,
            prefix: self.prefix.clone(),
            reply_channel: self.reply_channel.clone(),
            metrics: self.metrics.clone(),
            shutdown: self.shutdown.clone(),
            is_running: self.is_running.clone(),
            owner_count: self.owner_count.clone(),
        }
    }
}
