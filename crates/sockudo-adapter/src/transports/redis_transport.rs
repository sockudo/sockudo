use crate::serialization::{self, Serialization};
use crate::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::horizontal_transport::{HorizontalTransport, TransportConfig, TransportHandlers};
use crate::transports::redis_client::RedisClient;
use async_trait::async_trait;
use futures::StreamExt;
use redis::AsyncCommands;
use sockudo_core::error::{Error, Result};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::SentinelSpec;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;
use tracing::{debug, error, warn};
use uuid::Uuid;

/// Redis adapter configuration
#[derive(Debug, Clone)]
pub struct RedisAdapterConfig {
    pub url: String,
    pub prefix: String,
    pub request_timeout_ms: u64,
    pub cluster_mode: bool,
    /// When set, connect via Redis Sentinel (with optional TLS / client certs)
    /// instead of opening `url` directly. `url` is then used only for diagnostics.
    pub sentinel: Option<SentinelSpec>,
    pub serialization: Serialization,
}

impl Default for RedisAdapterConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379/".to_string(),
            prefix: "sockudo".to_string(),
            request_timeout_ms: 5000,
            cluster_mode: false,
            sentinel: None,
            serialization: Serialization::default(),
        }
    }
}

impl TransportConfig for RedisAdapterConfig {
    fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }

    fn prefix(&self) -> &str {
        &self.prefix
    }
}

/// Redis transport implementation
pub struct RedisTransport {
    client: RedisClient,
    broadcast_channel: String,
    request_channel: String,
    response_channel: String,
    prefix: String,
    reply_channel: String,
    serialization: Serialization,
    metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
    shutdown: Arc<Notify>,
    is_running: Arc<AtomicBool>,
    owner_count: Arc<AtomicUsize>,
}

#[async_trait]
impl HorizontalTransport for RedisTransport {
    type Config = RedisAdapterConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        // Standalone opens `url`; Sentinel builds a native Sentinel client (with TLS
        // and optional client certs) and resolves the current master.
        let client = RedisClient::connect(&config.url, config.sentinel.clone()).await?;

        let broadcast_channel = format!("{}:#broadcast", config.prefix);
        let request_channel = format!("{}:#requests", config.prefix);
        let response_channel = format!("{}:#responses", config.prefix);
        let reply_channel = format!("{}:#reply:{}", config.prefix, Uuid::new_v4().as_simple());

        Ok(Self {
            client,
            broadcast_channel,
            request_channel,
            response_channel,
            prefix: config.prefix.clone(),
            reply_channel,
            serialization: config.serialization,
            metrics: Arc::new(OnceLock::new()),
            shutdown: Arc::new(Notify::new()),
            is_running: Arc::new(AtomicBool::new(true)),
            owner_count: Arc::new(AtomicUsize::new(1)),
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        let payload = serialization::serialize(message, self.serialization)?;

        // Retry broadcast with exponential backoff to handle connection recovery
        let mut retry_delay = 100u64; // Start with 100ms
        const MAX_RETRIES: u32 = 3;
        const MAX_RETRY_DELAY: u64 = 1000; // Max 1 second

        for attempt in 0..=MAX_RETRIES {
            let mut conn = match self.client.events_connection().await {
                Ok(conn) => conn,
                Err(e) => {
                    self.client.invalidate();
                    if attempt == MAX_RETRIES {
                        return Err(Error::Redis(format!(
                            "Failed to acquire broadcast connection after {} attempts: {}",
                            MAX_RETRIES + 1,
                            e
                        )));
                    }
                    warn!(
                        "Broadcast connection attempt {} failed: {}, retrying in {}ms",
                        attempt + 1,
                        e,
                        retry_delay
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(retry_delay)).await;
                    retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                    continue;
                }
            };
            match conn
                .publish::<_, _, i32>(&self.broadcast_channel, &payload)
                .await
            {
                Ok(_subscriber_count) => {
                    if attempt > 0 {
                        debug!("Broadcast succeeded on retry attempt {}", attempt);
                    }
                    return Ok(());
                }
                Err(e) => {
                    // Drop cached connections so Sentinel re-resolves the master on retry.
                    self.client.invalidate();
                    if attempt == MAX_RETRIES {
                        return Err(Error::Redis(format!(
                            "Failed to publish broadcast after {} attempts: {}",
                            MAX_RETRIES + 1,
                            e
                        )));
                    }

                    warn!(
                        "Broadcast attempt {} failed: {}, retrying in {}ms",
                        attempt + 1,
                        e,
                        retry_delay
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(retry_delay)).await;
                    retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                }
            }
        }

        // This should never be reached due to the loop logic, but return error for safety
        Err(Error::Redis(
            "All retry attempts failed unexpectedly".to_string(),
        ))
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        let payload = serialization::serialize(request, self.serialization)?;

        let mut conn = self.client.command_connection().await?;
        let subscriber_count: i32 = match conn.publish(&self.request_channel, &payload).await {
            Ok(count) => count,
            Err(e) => {
                self.client.invalidate();
                return Err(Error::Redis(format!("Failed to publish request: {e}")));
            }
        };

        debug!(
            "Broadcasted request {} to {} subscribers",
            request.request_id, subscriber_count
        );
        Ok(())
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        let payload = serialization::serialize(response, self.serialization)?;

        let mut conn = self.client.command_connection().await?;
        if let Err(e) = conn
            .publish::<_, _, ()>(&self.response_channel, &payload)
            .await
        {
            self.client.invalidate();
            return Err(Error::Redis(format!("Failed to publish response: {e}")));
        }

        Ok(())
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
        let payload = serialization::serialize(request, self.serialization)?;
        let mut conn = self.client.command_connection().await?;
        let _: i32 = match conn.publish(&target_channel, &payload).await {
            Ok(count) => count,
            Err(e) => {
                self.client.invalidate();
                return Err(Error::Redis(format!(
                    "Failed to publish to node {target_node_id}: {e}"
                )));
            }
        };
        debug!(
            "Published request {} to node {} via Redis",
            request.request_id, target_node_id
        );
        Ok(())
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        let sub_client = self.client.clone();
        let pub_client = self.client.clone();
        let broadcast_channel = self.broadcast_channel.clone();
        let request_channel = self.request_channel.clone();
        let response_channel = self.response_channel.clone();
        let node_channel = format!("{}:#node:{}", self.prefix, handlers.node_id);
        let reply_channel = self.reply_channel.clone();
        let serialization = self.serialization;
        let metrics = self.metrics.clone();
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            let mut retry_delay = 500u64; // Start with 500ms delay
            const MAX_RETRY_DELAY: u64 = 10_000; // Max 10 seconds

            loop {
                if !is_running.load(Ordering::Relaxed) {
                    break;
                }
                debug!("Attempting to establish pub/sub connection...");

                // For Sentinel, this re-resolves the current master each reconnect,
                // which is how the listener follows master failover.
                let mut pubsub = match sub_client.pubsub().await {
                    Ok(pubsub) => {
                        retry_delay = 500; // Reset retry delay on success
                        debug!("Pub/sub connection established successfully");
                        pubsub
                    }
                    Err(e) => {
                        sub_client.invalidate();
                        error!(
                            "Failed to get pubsub connection: {}, retrying in {}ms",
                            e, retry_delay
                        );
                        tokio::select! {
                            _ = shutdown.notified() => break,
                            _ = tokio::time::sleep(tokio::time::Duration::from_millis(retry_delay)) => {}
                        }
                        retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                        continue;
                    }
                };

                if let Err(e) = pubsub
                    .subscribe(&[
                        &broadcast_channel,
                        &request_channel,
                        &response_channel,
                        &node_channel,
                        &reply_channel,
                    ])
                    .await
                {
                    error!(
                        "Failed to subscribe to channels: {}, retrying in {}ms",
                        e, retry_delay
                    );
                    tokio::select! {
                        _ = shutdown.notified() => break,
                        _ = tokio::time::sleep(tokio::time::Duration::from_millis(retry_delay)) => {}
                    }
                    retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                    continue;
                }

                debug!(
                    "Redis transport listening on channels: {}, {}, {}, {}, {}",
                    broadcast_channel,
                    request_channel,
                    response_channel,
                    node_channel,
                    reply_channel
                );

                let mut message_stream = pubsub.on_message();
                let mut connection_broken = false;

                loop {
                    if !is_running.load(Ordering::Relaxed) {
                        break;
                    }
                    let next_msg = tokio::select! {
                        _ = shutdown.notified() => break,
                        msg = message_stream.next() => msg,
                    };
                    let Some(msg) = next_msg else {
                        break;
                    };
                    let channel = msg.get_channel_name();
                    let payload_result: redis::RedisResult<Vec<u8>> = msg.get_payload();

                    if let Ok(payload) = payload_result {
                        if channel == broadcast_channel.as_str() {
                            let broadcast_handler = handlers.on_broadcast.clone();
                            let metrics_clone = metrics.clone();

                            tokio::spawn(async move {
                                if let Ok(broadcast) =
                                    serialization::deserialize::<BroadcastMessage>(&payload)
                                {
                                    broadcast_handler(broadcast).await;
                                } else if let Some(metrics) = metrics_clone.get() {
                                    metrics.mark_horizontal_transport_message_dropped("redis");
                                }
                            });
                        } else if channel == request_channel.as_str()
                            || channel == node_channel.as_str()
                        {
                            let request_handler = handlers.on_request.clone();
                            let pub_client_clone = pub_client.clone();
                            let response_channel_clone = response_channel.clone();
                            let metrics_clone = metrics.clone();
                            let serialization = serialization;

                            tokio::spawn(async move {
                                if let Ok(request) =
                                    serialization::deserialize::<RequestBody>(&payload)
                                {
                                    let reply_to = request.reply_to.clone();
                                    let response_result = request_handler(request).await;

                                    if let Ok(response) = response_result
                                        && let Ok(response_data) = serialization::serialize(
                                            &response,
                                            serialization,
                                        )
                                    {
                                        let target =
                                            reply_to.unwrap_or(response_channel_clone.clone());
                                        match pub_client_clone.command_connection().await {
                                            Ok(mut conn) => {
                                                if conn
                                                    .publish::<_, _, ()>(&target, &response_data)
                                                    .await
                                                    .is_err()
                                                {
                                                    pub_client_clone.invalidate();
                                                }
                                            }
                                            Err(e) => {
                                                pub_client_clone.invalidate();
                                                warn!(
                                                    "Failed to acquire connection to publish response: {}",
                                                    e
                                                );
                                            }
                                        }
                                    }
                                } else if let Some(metrics) = metrics_clone.get() {
                                    metrics.mark_horizontal_transport_message_dropped("redis");
                                }
                            });
                        } else if channel == response_channel.as_str()
                            || channel == reply_channel.as_str()
                        {
                            let response_handler = handlers.on_response.clone();
                            let metrics_clone = metrics.clone();

                            tokio::spawn(async move {
                                if let Ok(response) =
                                    serialization::deserialize::<ResponseBody>(&payload)
                                {
                                    response_handler(response).await;
                                } else {
                                    if let Some(metrics) = metrics_clone.get() {
                                        metrics.mark_horizontal_transport_message_dropped("redis");
                                    }
                                    warn!(
                                        "Failed to parse response message: {}",
                                        String::from_utf8_lossy(&payload)
                                    );
                                }
                            });
                        }
                    } else {
                        // Error getting payload - connection might be broken
                        warn!("Error getting message payload: {:?}", payload_result);
                        connection_broken = true;
                        break;
                    }
                }

                if connection_broken {
                    if let Some(metrics) = metrics.get() {
                        metrics.mark_horizontal_transport_reconnection("redis");
                    }
                    warn!(
                        "Pub/sub connection broken, reconnecting in {}ms...",
                        retry_delay
                    );
                    tokio::select! {
                        _ = shutdown.notified() => break,
                        _ = tokio::time::sleep(tokio::time::Duration::from_millis(retry_delay)) => {}
                    }
                    retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                } else {
                    if let Some(metrics) = metrics.get() {
                        metrics.mark_horizontal_transport_reconnection("redis");
                    }
                    warn!("Pub/sub message stream ended unexpectedly, reconnecting...");
                }

                // Connection ended, will retry in outer loop
            }
        });

        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        let mut conn = match self.client.command_connection().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!("Failed to acquire connection for node count: {}", e);
                return Ok(1);
            }
        };
        let result: redis::RedisResult<Vec<redis::Value>> = redis::cmd("PUBSUB")
            .arg("NUMSUB")
            .arg(&self.request_channel)
            .query_async(&mut conn)
            .await;

        match result {
            Ok(values) => {
                if values.len() >= 2 {
                    if let redis::Value::Int(count) = values[1] {
                        let node_count = (count as usize).max(1);
                        debug!(
                            "Detected {} application instances via PUBSUB NUMSUB",
                            node_count
                        );
                        Ok(node_count)
                    } else {
                        warn!("PUBSUB NUMSUB returned non-integer count: {:?}", values[1]);
                        Ok(1)
                    }
                } else {
                    warn!("PUBSUB NUMSUB returned unexpected format: {:?}", values);
                    Ok(1)
                }
            }
            Err(e) => {
                error!("Failed to execute PUBSUB NUMSUB: {}", e);
                Ok(1)
            }
        }
    }

    fn node_count_is_real_time(&self) -> bool {
        true
    }

    async fn check_health(&self) -> Result<()> {
        // Use a dedicated connection for health check to avoid impacting main operations
        let mut conn = self.client.multiplexed().await?;

        let response = redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| Error::Redis(format!("Health check PING failed: {e}")))?;

        if response == "PONG" {
            Ok(())
        } else {
            Err(Error::Redis(format!(
                "PING returned unexpected response: {response}"
            )))
        }
    }

    fn set_metrics(&self, metrics: Arc<dyn MetricsInterface + Send + Sync>) {
        let _ = self.metrics.set(metrics);
    }
}

impl Drop for RedisTransport {
    fn drop(&mut self) {
        if self.owner_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.is_running.store(false, Ordering::Relaxed);
            self.shutdown.notify_waiters();
        }
    }
}

impl Clone for RedisTransport {
    fn clone(&self) -> Self {
        self.owner_count.fetch_add(1, Ordering::Relaxed);
        Self {
            client: self.client.clone(),
            broadcast_channel: self.broadcast_channel.clone(),
            request_channel: self.request_channel.clone(),
            response_channel: self.response_channel.clone(),
            prefix: self.prefix.clone(),
            reply_channel: self.reply_channel.clone(),
            serialization: self.serialization,
            metrics: self.metrics.clone(),
            shutdown: self.shutdown.clone(),
            is_running: self.is_running.clone(),
            owner_count: self.owner_count.clone(),
        }
    }
}
