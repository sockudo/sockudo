use crate::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::horizontal_transport::{HorizontalTransport, TransportConfig, TransportHandlers};
use crate::transports::redis_client::RedisClient;
use crate::transports::redis_pubsub_probe::{PubSubProbeFailure, pubsub_connection_lost};
use async_trait::async_trait;
use futures::StreamExt;
use redis::AsyncCommands;
use sockudo_core::error::{Error, Result};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::{DEFAULT_REDIS_PUBSUB_PING_INTERVAL_MS, RedisTlsOptions, SentinelSpec};
use sockudo_core::redis_client::RedisClientOptions;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{debug, error, info, trace, warn};
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
    /// TLS settings for a direct Redis data connection.
    pub tls: RedisTlsOptions,
    /// Interval between PINGs on the pub/sub connection; `0` disables the liveness probe.
    pub pubsub_ping_interval_ms: u64,
}

impl Default for RedisAdapterConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379/".to_string(),
            prefix: "sockudo".to_string(),
            request_timeout_ms: 5000,
            cluster_mode: false,
            sentinel: None,
            tls: RedisTlsOptions::default(),
            pubsub_ping_interval_ms: DEFAULT_REDIS_PUBSUB_PING_INTERVAL_MS,
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
    pubsub_ping_interval: Duration,
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
        let client = RedisClient::connect_with_options(
            &config.url,
            RedisClientOptions {
                sentinel: config.sentinel.clone(),
                tls: config.tls.clone(),
                response_timeout: None,
            },
        )
        .await?;

        // Verify and retain an independent probe path before accepting traffic.
        let _ = client.health_connection().await?;

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
            pubsub_ping_interval: Duration::from_millis(config.pubsub_ping_interval_ms),
            metrics: Arc::new(OnceLock::new()),
            shutdown: Arc::new(Notify::new()),
            is_running: Arc::new(AtomicBool::new(true)),
            owner_count: Arc::new(AtomicUsize::new(1)),
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        let broadcast_json = sonic_rs::to_string(message)?;

        // Retry broadcast with exponential backoff to handle connection recovery
        let mut retry_delay = 100u64; // Start with 100ms
        const MAX_RETRIES: u32 = 3;
        const MAX_RETRY_DELAY: u64 = 1000; // Max 1 second

        for attempt in 0..=MAX_RETRIES {
            let mut conn = match self.client.events_connection().await {
                Ok(conn) => conn,
                Err(_e) => {
                    self.client.invalidate();
                    if attempt == MAX_RETRIES {
                        return Err(Error::Redis(format!(
                            "Failed to acquire broadcast connection after {} attempts",
                            MAX_RETRIES + 1,
                        )));
                    }
                    warn!(
                        adapter = "redis",
                        attempt = attempt + 1,
                        retry_delay_ms = retry_delay,
                        retryable = true,
                        "broadcast connection attempt failed"
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(retry_delay)).await;
                    retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                    continue;
                }
            };
            match conn
                .publish::<_, _, i32>(&self.broadcast_channel, &broadcast_json)
                .await
            {
                Ok(_subscriber_count) => {
                    if attempt > 0 {
                        debug!(
                            adapter = "redis",
                            attempt, "broadcast publish succeeded on retry"
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    self.client.invalidate();
                    if attempt == MAX_RETRIES {
                        return Err(Error::Redis(format!(
                            "Failed to publish broadcast after {} attempts: {e}",
                            MAX_RETRIES + 1,
                        )));
                    }

                    warn!(adapter = "redis", attempt = attempt + 1, retry_delay_ms = retry_delay, error = %e, retryable = true, "broadcast publish attempt failed");
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
        let request_json = sonic_rs::to_string(request)
            .map_err(|e| Error::Other(format!("Failed to serialize request: {e}")))?;

        let mut conn = self.client.command_connection().await?;
        let subscriber_count: i32 = match conn.publish(&self.request_channel, &request_json).await {
            Ok(count) => count,
            Err(e) => {
                self.client.invalidate();
                return Err(Error::Redis(format!("Failed to publish request: {e}")));
            }
        };

        trace!(adapter = "redis", request_id = %request.request_id, subscriber_count, "request published to transport");
        Ok(())
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        let response_json = sonic_rs::to_string(response)
            .map_err(|e| Error::Other(format!("Failed to serialize response: {e}")))?;

        let mut conn = self.client.command_connection().await?;
        if let Err(e) = conn
            .publish::<_, _, ()>(&self.response_channel, response_json)
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
        let request_json = sonic_rs::to_string(request)
            .map_err(|e| Error::Other(format!("Failed to serialize request: {e}")))?;
        let mut conn = self.client.command_connection().await?;
        let _: i32 = match conn.publish(&target_channel, &request_json).await {
            Ok(count) => count,
            Err(e) => {
                self.client.invalidate();
                return Err(Error::Redis(format!(
                    "Failed to publish to node {target_node_id}: {e}"
                )));
            }
        };
        trace!(adapter = "redis", request_id = %request.request_id, target_node_id = %target_node_id, "request published to node");
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
        let pubsub_ping_interval = self.pubsub_ping_interval;
        let metrics = self.metrics.clone();
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            let mut retry_delay = 500u64; // Start with 500ms delay
            const MAX_RETRY_DELAY: u64 = 10_000; // Max 10 seconds
            // Fires when the Sentinel primary moves. A subscription on the old
            // primary can stay silently open (its host may vanish without a TCP
            // reset), so the listener resubscribes instead of waiting on it.
            let mut master_changes = sub_client.master_changes();

            loop {
                if !is_running.load(Ordering::Relaxed) {
                    break;
                }
                debug!(
                    adapter = "redis",
                    "attempting to establish pub/sub connection"
                );

                // For Sentinel, this re-resolves the current master each reconnect,
                // which is how the listener follows master failover.
                master_changes.mark_seen();
                let mut pubsub = match sub_client.pubsub().await {
                    Ok(pubsub) => {
                        retry_delay = 500;
                        debug!(adapter = "redis", "pub/sub connection established");
                        pubsub
                    }
                    Err(_e) => {
                        sub_client.invalidate();
                        error!(
                            adapter = "redis",
                            retry_delay_ms = retry_delay,
                            retryable = true,
                            "pub/sub connection failed"
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
                    error!(adapter = "redis", error = %e, retry_delay_ms = retry_delay, retryable = true, "pub/sub channel subscription failed");
                    tokio::select! {
                        _ = shutdown.notified() => break,
                        _ = tokio::time::sleep(tokio::time::Duration::from_millis(retry_delay)) => {}
                    }
                    retry_delay = std::cmp::min(retry_delay * 2, MAX_RETRY_DELAY);
                    continue;
                }

                debug!(
                    adapter = "redis",
                    broadcast_channel = %broadcast_channel,
                    request_channel = %request_channel,
                    response_channel = %response_channel,
                    node_channel = %node_channel,
                    reply_channel = %reply_channel,
                    "transport subscriptions established"
                );

                let (ping_sink, mut message_stream) = pubsub.split();
                let liveness = pubsub_connection_lost(pubsub_ping_interval, move || {
                    let mut sink = ping_sink.clone();
                    async move { sink.ping::<()>().await }
                });
                tokio::pin!(liveness);
                let mut connection_broken = false;
                let mut primary_moved = false;
                let mut probe_failure = None;

                loop {
                    if !is_running.load(Ordering::Relaxed) {
                        break;
                    }
                    let next_msg = tokio::select! {
                        // A reset that fails the probe also ends the message stream; poll the
                        // probe first so the reconnect is attributed to it.
                        biased;
                        _ = shutdown.notified() => break,
                        _ = master_changes.changed() => {
                            primary_moved = true;
                            break;
                        }
                        failure = &mut liveness => {
                            probe_failure = Some(failure);
                            break;
                        }
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
                                    sonic_rs::from_slice::<BroadcastMessage>(&payload)
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

                            tokio::spawn(async move {
                                if let Ok(request) = sonic_rs::from_slice::<RequestBody>(&payload) {
                                    let reply_to = request.reply_to.clone();
                                    let response_result = request_handler(request).await;

                                    if let Ok(response) = response_result
                                        && let Ok(response_json) = sonic_rs::to_string(&response)
                                    {
                                        let target =
                                            reply_to.unwrap_or(response_channel_clone.clone());
                                        match pub_client_clone.command_connection().await {
                                            Ok(mut conn) => {
                                                if let Err(e) = conn
                                                    .publish::<_, _, ()>(&target, response_json)
                                                    .await
                                                {
                                                    pub_client_clone.invalidate();
                                                    warn!(adapter = "redis", error = %e, "response publish failed");
                                                }
                                            }
                                            Err(_e) => {
                                                pub_client_clone.invalidate();
                                                warn!(
                                                    adapter = "redis",
                                                    "response connection acquisition failed"
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
                                match sonic_rs::from_slice::<ResponseBody>(&payload) {
                                    Ok(response) => response_handler(response).await,
                                    Err(e) => {
                                        if let Some(metrics) = metrics_clone.get() {
                                            metrics
                                                .mark_horizontal_transport_message_dropped("redis");
                                        }
                                        warn!(adapter = "redis", error = %e, "response message parse failed");
                                    }
                                }
                            });
                        }
                    } else {
                        // Error getting payload - connection might be broken
                        warn!(adapter = "redis", "message payload read failed");
                        connection_broken = true;
                        break;
                    }
                }

                if primary_moved {
                    if let Some(metrics) = metrics.get() {
                        metrics.mark_horizontal_transport_reconnection("redis");
                    }
                    info!(
                        adapter = "redis",
                        "redis sentinel primary changed, resubscribing"
                    );
                } else if let Some(failure) = probe_failure {
                    if let Some(metrics) = metrics.get() {
                        metrics.mark_horizontal_transport_reconnection("redis");
                    }
                    match failure {
                        PubSubProbeFailure::Failed(e) => warn!(
                            adapter = "redis",
                            error = %e,
                            retryable = true,
                            "pub/sub liveness ping failed, resubscribing"
                        ),
                        PubSubProbeFailure::TimedOut => warn!(
                            adapter = "redis",
                            timeout_ms = pubsub_ping_interval.as_millis() as u64,
                            retryable = true,
                            "pub/sub liveness ping timed out, resubscribing"
                        ),
                    }
                } else if connection_broken {
                    if let Some(metrics) = metrics.get() {
                        metrics.mark_horizontal_transport_reconnection("redis");
                    }
                    warn!(
                        adapter = "redis",
                        retry_delay_ms = retry_delay,
                        retryable = true,
                        "pub/sub connection broken"
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
                    warn!(
                        adapter = "redis",
                        retryable = true,
                        "pub/sub message stream ended unexpectedly"
                    );
                }
            }
        });

        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        let mut conn = match self.client.command_connection().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!(adapter = "redis", error = %e, "connection acquisition for node count failed");
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
                        trace!(
                            adapter = "redis",
                            node_count, "application instances detected via PUBSUB NUMSUB"
                        );
                        Ok(node_count)
                    } else {
                        warn!(
                            adapter = "redis",
                            "PUBSUB NUMSUB returned non-integer count"
                        );
                        Ok(1)
                    }
                } else {
                    warn!(
                        adapter = "redis",
                        "PUBSUB NUMSUB returned unexpected format"
                    );
                    Ok(1)
                }
            }
            Err(e) => {
                error!(adapter = "redis", error = %e, "PUBSUB NUMSUB failed");
                Ok(1)
            }
        }
    }

    fn node_count_is_real_time(&self) -> bool {
        true
    }

    async fn check_health(&self) -> Result<()> {
        // Reuse a dedicated reconnecting connection: periodic maintenance must
        // not force every probe through a new TCP/TLS handshake.
        let mut conn = self.client.health_connection().await?;

        let response = redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| {
                self.client.invalidate();
                Error::Redis(format!("Health check PING failed: {e}"))
            })?;

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
            pubsub_ping_interval: self.pubsub_ping_interval,
            metrics: self.metrics.clone(),
            shutdown: self.shutdown.clone(),
            is_running: self.is_running.clone(),
            owner_count: self.owner_count.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;
    use tokio::task::JoinSet;

    #[tokio::test]
    async fn health_reuses_independent_connection_and_propagates_ping_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let reject_ping = Arc::new(AtomicBool::new(false));
        let server_count = accepted.clone();
        let server_reject = reject_ping.clone();
        let server = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                server_count.fetch_add(1, Ordering::SeqCst);
                let reject = server_reject.clone();
                connections.spawn(async move {
                    let mut socket = BufReader::new(socket);
                    loop {
                        let mut line = String::new();
                        if socket.read_line(&mut line).await.unwrap() == 0 {
                            break;
                        }
                        let count: usize = line.trim().strip_prefix('*').unwrap().parse().unwrap();
                        let mut args = Vec::with_capacity(count);
                        for _ in 0..count {
                            line.clear();
                            socket.read_line(&mut line).await.unwrap();
                            let len: usize =
                                line.trim().strip_prefix('$').unwrap().parse().unwrap();
                            let mut arg = vec![0; len + 2];
                            socket.read_exact(&mut arg).await.unwrap();
                            arg.truncate(len);
                            args.push(arg);
                        }
                        let response: &[u8] = if args[0] == b"PING" {
                            if reject.load(Ordering::SeqCst) {
                                b"-ERR probe failed\r\n"
                            } else {
                                b"+PONG\r\n"
                            }
                        } else {
                            b"+OK\r\n"
                        };
                        socket.get_mut().write_all(response).await.unwrap();
                    }
                });
            }
        });
        let transport = RedisTransport::new(RedisAdapterConfig {
            url: format!("redis://{address}"),
            ..Default::default()
        })
        .await
        .unwrap();
        // Command, event, and health each have an independent connection.
        assert_eq!(accepted.load(Ordering::SeqCst), 3);
        for _ in 0..10 {
            transport.clone().check_health().await.unwrap();
        }
        assert_eq!(accepted.load(Ordering::SeqCst), 3);
        reject_ping.store(true, Ordering::SeqCst);
        assert!(transport.check_health().await.is_err());
        reject_ping.store(false, Ordering::SeqCst);
        transport.check_health().await.unwrap();
        assert_eq!(accepted.load(Ordering::SeqCst), 3);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }

    #[derive(Default)]
    struct FakeRedisCounters {
        subscribes: AtomicUsize,
        pubsub_pings: AtomicUsize,
    }

    /// Minimal RESP2 server. The first connection that subscribes stops answering PING, like a
    /// subscriber socket whose Redis vanished without a FIN or RST; later ones answer normally.
    async fn spawn_fake_redis(
        first_subscriber_goes_silent: bool,
    ) -> (String, Arc<FakeRedisCounters>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let counters = Arc::new(FakeRedisCounters::default());
        let server_counters = counters.clone();
        let server = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let counters = server_counters.clone();
                connections.spawn(async move {
                    let mut socket = BufReader::new(socket);
                    let mut silent = false;
                    let mut subscribed = false;
                    loop {
                        let mut line = String::new();
                        if socket.read_line(&mut line).await.unwrap_or(0) == 0 {
                            break;
                        }
                        let count: usize = line.trim().strip_prefix('*').unwrap().parse().unwrap();
                        let mut args = Vec::with_capacity(count);
                        for _ in 0..count {
                            line.clear();
                            socket.read_line(&mut line).await.unwrap();
                            let len: usize =
                                line.trim().strip_prefix('$').unwrap().parse().unwrap();
                            let mut arg = vec![0; len + 2];
                            socket.read_exact(&mut arg).await.unwrap();
                            arg.truncate(len);
                            args.push(arg);
                        }
                        let mut response = Vec::new();
                        match args[0].as_slice() {
                            b"SUBSCRIBE" => {
                                let previous = counters.subscribes.fetch_add(1, Ordering::SeqCst);
                                silent = first_subscriber_goes_silent && previous == 0;
                                subscribed = true;
                                for (index, channel) in args[1..].iter().enumerate() {
                                    response.extend_from_slice(b"*3\r\n$9\r\nsubscribe\r\n");
                                    response.extend_from_slice(
                                        format!("${}\r\n", channel.len()).as_bytes(),
                                    );
                                    response.extend_from_slice(channel);
                                    response.extend_from_slice(
                                        format!("\r\n:{}\r\n", index + 1).as_bytes(),
                                    );
                                }
                            }
                            b"PING" if subscribed => {
                                counters.pubsub_pings.fetch_add(1, Ordering::SeqCst);
                                if !silent {
                                    response.extend_from_slice(b"*2\r\n$4\r\npong\r\n$0\r\n\r\n");
                                }
                            }
                            b"PING" => response.extend_from_slice(b"+PONG\r\n"),
                            _ => response.extend_from_slice(b"+OK\r\n"),
                        }
                        if socket.get_mut().write_all(&response).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        (format!("redis://{address}"), counters, server)
    }

    fn noop_handlers() -> TransportHandlers {
        TransportHandlers {
            node_id: "probe-test".to_string(),
            on_broadcast: Arc::new(|_| Box::pin(async {})),
            on_request: Arc::new(|_| Box::pin(async { Err(Error::Other("not used".to_string())) })),
            on_response: Arc::new(|_| Box::pin(async {})),
        }
    }

    #[tokio::test]
    async fn silent_subscriber_connection_is_replaced_and_resubscribed() {
        let (url, counters, server) = spawn_fake_redis(true).await;
        let transport = RedisTransport::new(RedisAdapterConfig {
            url,
            pubsub_ping_interval_ms: 50,
            ..Default::default()
        })
        .await
        .unwrap();
        transport.start_listeners(noop_handlers()).await.unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while counters.subscribes.load(Ordering::SeqCst) < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "listener never resubscribed after its PING went unanswered"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(transport);
        server.abort();
    }

    #[tokio::test]
    async fn answered_pings_keep_the_subscription() {
        let (url, counters, server) = spawn_fake_redis(false).await;
        let transport = RedisTransport::new(RedisAdapterConfig {
            url,
            pubsub_ping_interval_ms: 50,
            ..Default::default()
        })
        .await
        .unwrap();
        transport.start_listeners(noop_handlers()).await.unwrap();

        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(counters.pubsub_pings.load(Ordering::SeqCst) >= 3);
        assert_eq!(counters.subscribes.load(Ordering::SeqCst), 1);
        drop(transport);
        server.abort();
    }
}
