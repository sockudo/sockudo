use crate::adapter::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::adapter::horizontal_transport::{
    HorizontalTransport, TransportConfig, TransportHandlers,
};
use crate::error::{Error, Result};
use crate::options::RedisClusterAdapterConfig;
use async_trait::async_trait;

use redis::AsyncCommands;
use redis::cluster::{ClusterClient, ClusterClientBuilder};
use redis::cluster_async::ClusterConnection;

use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Helper function to convert redis::Value to String
fn value_to_string(v: &redis::Value) -> Option<String> {
    match v {
        redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        redis::Value::SimpleString(s) => Some(s.clone()),
        redis::Value::VerbatimString { format: _, text } => Some(text.clone()),
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
#[derive(Clone)]
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
}

#[async_trait]
impl HorizontalTransport for RedisClusterTransport {
    type Config = RedisClusterAdapterConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        let client = ClusterClientBuilder::new(config.nodes.clone())
            .retries(3)
            .read_from_replicas()
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

        let use_sharded_pubsub = config.use_sharded_pubsub;

        if use_sharded_pubsub {
            info!(
                "Redis Cluster using sharded pub/sub (SSUBSCRIBE/SPUBLISH) for optimal performance"
            );
        } else {
            debug!("Redis Cluster using standard pub/sub (SUBSCRIBE/PUBLISH)");
        }

        info!(
            "Redis Cluster transport initialized with dedicated publish and health check connections"
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
                redis::cmd("SPUBLISH")
                    .arg(&self.broadcast_channel)
                    .arg(&broadcast_json)
                    .query_async(&mut conn)
                    .await
            } else {
                conn.publish(&self.broadcast_channel, &broadcast_json).await
            };

            match publish_result {
                Ok(_) => {
                    if attempt > 0 {
                        debug!("Broadcast succeeded on retry attempt {}", attempt);
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
                    warn!(
                        "Failed to publish broadcast (attempt {}): {}",
                        attempt + 1,
                        e
                    );
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
                redis::cmd("SPUBLISH")
                    .arg(&self.request_channel)
                    .arg(&request_json)
                    .query_async(&mut conn)
                    .await
            } else {
                conn.publish(&self.request_channel, &request_json).await
            };

            match publish_result {
                Ok(subscriber_count) => {
                    if attempt > 0 {
                        debug!("Request publish succeeded on retry attempt {}", attempt);
                    }
                    debug!(
                        "Broadcasted request {} to {} subscribers",
                        request.request_id, subscriber_count
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
                    warn!("Failed to publish request (attempt {}): {}", attempt + 1, e);
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
                redis::cmd("SPUBLISH")
                    .arg(&self.response_channel)
                    .arg(&response_json)
                    .query_async(&mut conn)
                    .await
            } else {
                conn.publish(&self.response_channel, &response_json).await
            };

            match publish_result {
                Ok(_) => {
                    if attempt > 0 {
                        debug!("Response publish succeeded on retry attempt {}", attempt);
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
                    warn!(
                        "Failed to publish response (attempt {}): {}",
                        attempt + 1,
                        e
                    );
                    tokio::time::sleep(Duration::from_millis(retry_delay)).await;
                    retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                }
            }
        }

        unreachable!("Retry loop should have returned");
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

        // Spawn the main listener task with reconnection logic
        tokio::spawn(async move {
            let mut retry_delay = 500u64; // Start with 500ms delay
            const MAX_RETRY_DELAY: u64 = 10_000; // Max 10 seconds
            let mut reconnection_count = 0u64;

            loop {
                // Create a new channel and client for each connection attempt
                // This is necessary because the push_sender moves tx into the client,
                // and when the channel closes, we need a fresh one for reconnection
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

                let sub_client = match ClusterClientBuilder::new(nodes.clone())
                    .use_protocol(redis::ProtocolVersion::RESP3)
                    .push_sender(tx)
                    .build()
                {
                    Ok(client) => client,
                    Err(e) => {
                        error!(
                            "Failed to create PubSub client: {}, retrying in {}ms",
                            e, retry_delay
                        );
                        tokio::time::sleep(Duration::from_millis(retry_delay)).await;
                        retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                        continue;
                    }
                };

                // Create a connection for PubSub with retry logic
                let mut pubsub = match sub_client.get_async_connection().await {
                    Ok(conn) => {
                        retry_delay = 500; // Reset retry delay on success
                        if reconnection_count > 0 {
                            debug!(
                                "Redis Cluster PubSub reconnected successfully after {} attempts",
                                reconnection_count
                            );
                            // Note: Reconnection metrics are tracked in src/metrics/prometheus.rs
                            // but not accessible here without passing metrics driver through
                        }
                        conn
                    }
                    Err(e) => {
                        reconnection_count += 1;
                        error!(
                            "Failed to get pubsub connection: {}, retrying in {}ms (attempt {})",
                            e, retry_delay, reconnection_count
                        );
                        tokio::time::sleep(Duration::from_millis(retry_delay)).await;
                        retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                        continue;
                    }
                };

                // Subscribe to all channels with retry logic
                // Use SSUBSCRIBE for sharded pub/sub if enabled, otherwise standard SUBSCRIBE
                let subscribe_result: redis::RedisResult<()> = if use_sharded_pubsub {
                    redis::cmd("SSUBSCRIBE")
                        .arg(&broadcast_channel)
                        .arg(&request_channel)
                        .arg(&response_channel)
                        .query_async(&mut pubsub)
                        .await
                } else {
                    pubsub
                        .subscribe(&[&broadcast_channel, &request_channel, &response_channel])
                        .await
                };

                if let Err(e) = subscribe_result {
                    error!(
                        "Failed to subscribe to channels: {}, retrying in {}ms",
                        e, retry_delay
                    );
                    tokio::time::sleep(Duration::from_millis(retry_delay)).await;
                    retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                    continue;
                }

                debug!(
                    "Redis Cluster transport listening on channels: {}, {}, {}",
                    broadcast_channel, request_channel, response_channel
                );

                // Reset reconnection count on successful subscription
                reconnection_count = 0;

                // Process messages from the channel - PushInfo is the message type for RESP3
                while let Some(push_info) = rx.recv().await {
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
                        error!("Invalid push message format: {:?}", push_info);
                        continue;
                    }

                    let channel = match value_to_string(&push_info.data[0]) {
                        Some(s) => s,
                        None => {
                            error!("Failed to parse channel name: {:?}", push_info.data[0]);
                            continue;
                        }
                    };

                    let payload = match value_to_string(&push_info.data[1]) {
                        Some(s) => s,
                        None => {
                            error!("Failed to parse payload: {:?}", push_info.data[1]);
                            continue;
                        }
                    };

                    // Process the message in a separate task
                    let broadcast_handler = handlers.on_broadcast.clone();
                    let request_handler = handlers.on_request.clone();
                    let response_handler = handlers.on_response.clone();
                    let publish_conn = publish_connection.clone(); // Cheap clone per redis-rs docs
                    let broadcast_channel_clone = broadcast_channel.clone();
                    let request_channel_clone = request_channel.clone();
                    let response_channel_clone = response_channel.clone();

                    tokio::spawn(async move {
                        if channel == broadcast_channel_clone {
                            // Handle broadcast message
                            if let Ok(broadcast) =
                                sonic_rs::from_str::<BroadcastMessage>(&payload)
                            {
                                broadcast_handler(broadcast).await;
                            }
                        } else if channel == request_channel_clone {
                            // Handle request message
                            if let Ok(request) = sonic_rs::from_str::<RequestBody>(&payload) {
                                let response_result = request_handler(request).await;

                                if let Ok(response) = response_result
                                    && let Ok(response_json) = sonic_rs::to_string(&response)
                                {
                                    // Clone connection (cheap) instead of creating new one
                                    let mut conn = publish_conn.clone();
                                    let _ = conn
                                        .publish::<_, _, ()>(&response_channel_clone, response_json)
                                        .await;
                                }
                            }
                        } else if channel == response_channel_clone {
                            // Handle response message
                            if let Ok(response) = sonic_rs::from_str::<ResponseBody>(&payload) {
                                response_handler(response).await;
                            }
                        }
                    });
                }

                // Connection ended, reconnect with exponential backoff
                warn!("Redis Cluster PubSub connection ended, reconnecting...");
                tokio::time::sleep(Duration::from_millis(retry_delay)).await;
                retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
            }
        });

        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        // Clone connection (cheap, thread-safe per redis-rs docs)
        let mut conn = self.publish_connection.clone();

        let result: redis::RedisResult<Vec<redis::Value>> = redis::cmd("PUBSUB")
            .arg("NUMSUB")
            .arg(&self.request_channel)
            .query_async(&mut conn)
            .await;

        match result {
            Ok(values) => {
                if values.len() >= 2 {
                    if let redis::Value::Int(count) = values[1] {
                        Ok((count as usize).max(1))
                    } else {
                        Ok(1)
                    }
                } else {
                    Ok(1)
                }
            }
            Err(e) => {
                error!("Failed to execute PUBSUB NUMSUB: {}", e);
                Ok(1)
            }
        }
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
}
