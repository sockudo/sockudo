use crate::horizontal_adapter::{
    BroadcastMessage, PresenceEntry, RequestBody, RequestType, ResponseBody, generate_request_id,
};
use crate::horizontal_transport::{HorizontalTransport, TransportConfig, TransportHandlers};
use ahash::AHashMap;
use async_nats::{Client as NatsClient, ConnectOptions as NatsOptions, Subject};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use sockudo_core::error::{Error, Result};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::NatsAdapterConfig;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const NATS_SYS_SERVER_PING_SUBJECT: &str = "$SYS.REQ.SERVER.PING";

#[doc(hidden)]
pub fn build_presence_state_chunks(
    our_data: &AHashMap<String, AHashMap<String, PresenceEntry>>,
    chunk_size: usize,
) -> sockudo_core::error::Result<Vec<sonic_rs::Value>> {
    let cap = chunk_size.max(1);
    let pairs: Vec<(&String, &AHashMap<String, PresenceEntry>)> = our_data.iter().collect();
    pairs
        .chunks(cap)
        .map(|c| {
            let chunk_map: AHashMap<&String, &AHashMap<String, PresenceEntry>> =
                c.iter().copied().collect();
            sonic_rs::to_value(&chunk_map).map_err(Into::into)
        })
        .collect()
}

/// Metrics for tracking message processing and drops
#[derive(Debug, Default)]
pub struct TransportMetrics {
    pub messages_received: AtomicU64,
    pub messages_processed: AtomicU64,
    pub messages_dropped_parse_error: AtomicU64,
}

impl TransportMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn record_received(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_processed(&self) {
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_parse_error(&self) {
        self.messages_dropped_parse_error
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.messages_received.load(Ordering::Relaxed),
            self.messages_processed.load(Ordering::Relaxed),
            self.messages_dropped_parse_error.load(Ordering::Relaxed),
        )
    }
}

/// NATS transport implementation
pub struct NatsTransport {
    client: NatsClient,
    broadcast_subject: String,
    request_subject: String,
    response_subject: String,
    /// Shared inbox prefix for direct reply routing
    inbox_prefix: String,
    config: NatsAdapterConfig,
    /// Metrics for tracking message processing
    metrics: Arc<TransportMetrics>,
    metrics_driver: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
    shutdown: Arc<Notify>,
    is_running: Arc<AtomicBool>,
    owner_count: Arc<AtomicUsize>,
}

impl NatsTransport {
    /// Get transport metrics for monitoring
    pub fn get_metrics(&self) -> Arc<TransportMetrics> {
        self.metrics.clone()
    }

    pub fn client(&self) -> &NatsClient {
        &self.client
    }

    async fn discover_node_count_via_system_ping(&self) -> Result<Option<usize>> {
        let reply_subject = self.client.new_inbox();
        let mut responses = self
            .client
            .subscribe(reply_subject.clone())
            .await
            .map_err(|e| Error::Internal(format!("Failed to subscribe for NATS discovery: {e}")))?;

        self.client
            .publish_with_reply(
                Subject::from(NATS_SYS_SERVER_PING_SUBJECT),
                reply_subject,
                Bytes::new(),
            )
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to publish NATS discovery request: {e}"))
            })?;

        let max_wait_ms = self.config.request_timeout_ms.clamp(
            self.config.discovery_idle_wait_ms,
            self.config.discovery_max_wait_ms,
        );
        let max_wait = Duration::from_millis(max_wait_ms);
        let idle_wait = Duration::from_millis(self.config.discovery_idle_wait_ms);
        let start = tokio::time::Instant::now();
        let mut count = 0usize;

        loop {
            let elapsed = start.elapsed();
            if elapsed >= max_wait {
                break;
            }

            let remaining = max_wait.saturating_sub(elapsed);
            let wait_for = if count == 0 {
                remaining
            } else {
                remaining.min(idle_wait)
            };

            match tokio::time::timeout(wait_for, responses.next()).await {
                Ok(Some(_message)) => count += 1,
                Ok(None) | Err(_) => break,
            }
        }

        if count > 0 {
            debug!("Detected {} NATS server(s) via system ping", count);
            Ok(Some(count))
        } else {
            debug!(
                "NATS system discovery returned no responses; falling back to configured/default node count"
            );
            Ok(None)
        }
    }
}

impl TransportConfig for NatsAdapterConfig {
    fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }

    fn prefix(&self) -> &str {
        &self.prefix
    }
}

#[async_trait]
impl HorizontalTransport for NatsTransport {
    type Config = NatsAdapterConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        info!(
            "NATS transport config: servers={:?}, prefix={}, request_timeout={}ms, connection_timeout={}ms",
            config.servers, config.prefix, config.request_timeout_ms, config.connection_timeout_ms
        );
        debug!(
            username_configured = config.username.is_some(),
            password_configured = config.password.is_some(),
            token_configured = config.token.is_some(),
            "NATS transport credentials configured"
        );

        // Build NATS Options
        let mut nats_options = NatsOptions::new()
            .retry_on_initial_connect()
            .event_callback(|event| async move {
                match event {
                    async_nats::Event::Connected => {
                        info!("NATS connection established");
                    }
                    async_nats::Event::Disconnected => {
                        warn!("NATS connection lost");
                    }
                    async_nats::Event::SlowConsumer(sid) => {
                        error!(subscription_id = sid, "NATS slow consumer detected");
                    }
                    async_nats::Event::ServerError(err) => {
                        error!("NATS server error: {}", err);
                    }
                    async_nats::Event::ClientError(ref err) => match err {
                        async_nats::ClientError::MaxReconnects => {
                            error!("NATS max reconnects exhausted");
                        }
                        async_nats::ClientError::Other(msg) => {
                            error!("NATS client error: {}", msg);
                        }
                    },
                    async_nats::Event::LameDuckMode => {
                        warn!("NATS server entering lame duck mode");
                    }
                    async_nats::Event::Draining => {
                        info!("NATS client draining");
                    }
                    async_nats::Event::Closed => {
                        warn!("NATS connection closed");
                    }
                }
            });

        if let Some(cap) = config.subscription_capacity {
            nats_options = nats_options.subscription_capacity(cap);
        }
        if let Some(cap) = config.client_capacity {
            nats_options = nats_options.client_capacity(cap);
        }
        if let Some(max) = config.max_reconnects {
            nats_options = nats_options.max_reconnects(max);
        }

        // Set credentials conditionally
        if let (Some(username), Some(password)) =
            (config.username.as_deref(), config.password.as_deref())
        {
            nats_options =
                nats_options.user_and_password(username.to_string(), password.to_string());
        } else if let Some(token) = config.token.as_deref() {
            nats_options = nats_options.token(token.to_string());
        }

        // Set connection timeout
        nats_options =
            nats_options.connection_timeout(Duration::from_millis(config.connection_timeout_ms));

        // Connect to NATS
        let client = nats_options
            .connect(&config.servers)
            .await
            .map_err(|e| Error::Internal(format!("Failed to connect to NATS: {e}")))?;

        // Build subject names
        let broadcast_subject = format!("{}.broadcast", config.prefix);
        let request_subject = format!("{}.requests", config.prefix);
        let response_subject = format!("{}.responses", config.prefix);

        // Build inbox prefix
        let inbox_prefix = format!("_INBOX.{}.", Uuid::new_v4().as_simple());

        Ok(Self {
            client,
            broadcast_subject,
            request_subject,
            response_subject,
            inbox_prefix,
            config,
            metrics: Arc::new(TransportMetrics::new()),
            metrics_driver: Arc::new(OnceLock::new()),
            shutdown: Arc::new(Notify::new()),
            is_running: Arc::new(AtomicBool::new(true)),
            owner_count: Arc::new(AtomicUsize::new(1)),
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        let message_data = sonic_rs::to_vec(message)
            .map_err(|e| Error::Other(format!("Failed to serialize broadcast message: {e}")))?;

        self.client
            .publish(
                Subject::from(self.broadcast_subject.clone()),
                message_data.into(),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to publish broadcast: {e}")))?;

        debug!("Published broadcast message via NATS");
        Ok(())
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        let request_data = sonic_rs::to_vec(request)
            .map_err(|e| Error::Other(format!("Failed to serialize request: {e}")))?;

        self.client
            .publish(
                Subject::from(self.request_subject.clone()),
                request_data.into(),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to publish request: {e}")))?;

        debug!("Broadcasted request {} via NATS", request.request_id);
        Ok(())
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        let response_data = sonic_rs::to_vec(response)
            .map_err(|e| Error::Other(format!("Failed to serialize response: {e}")))?;

        self.client
            .publish(
                Subject::from(self.response_subject.clone()),
                response_data.into(),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to publish response: {e}")))?;

        debug!("Published response via NATS");
        Ok(())
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        let client = self.client.clone();
        let broadcast_subject = self.broadcast_subject.clone();
        let request_subject = self.request_subject.clone();
        let response_subject = self.response_subject.clone();
        let response_client = self.client.clone();
        let metrics_broadcast = self.metrics.clone();
        let metrics_request = self.metrics.clone();
        let metrics_response = self.metrics.clone();
        let metrics_inbox = self.metrics.clone();
        let metrics_driver_broadcast = self.metrics_driver.clone();
        let metrics_driver_request = self.metrics_driver.clone();
        let metrics_driver_response = self.metrics_driver.clone();
        let metrics_driver_inbox = self.metrics_driver.clone();
        let shutdown_broadcast = self.shutdown.clone();
        let shutdown_request = self.shutdown.clone();
        let shutdown_response = self.shutdown.clone();
        let shutdown_inbox = self.shutdown.clone();
        let running_broadcast = self.is_running.clone();
        let running_request = self.is_running.clone();
        let running_response = self.is_running.clone();
        let running_inbox = self.is_running.clone();

        // Subscribe to broadcast channel
        let mut broadcast_subscription = client
            .subscribe(Subject::from(broadcast_subject.clone()))
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to subscribe to broadcast subject: {e}"))
            })?;

        // Subscribe to requests channel
        let mut request_subscription = client
            .subscribe(Subject::from(request_subject.clone()))
            .await
            .map_err(|e| Error::Internal(format!("Failed to subscribe to request subject: {e}")))?;

        // Subscribe to responses channel
        let mut response_subscription = client
            .subscribe(Subject::from(response_subject.clone()))
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to subscribe to response subject: {e}"))
            })?;

        // Subscribe to shared inbox wildcard
        let inbox_subject = format!("{}*", self.inbox_prefix);
        let mut inbox_subscription = client
            .subscribe(Subject::from(inbox_subject.clone()))
            .await
            .map_err(|e| Error::Internal(format!("Failed to subscribe to inbox subject: {e}")))?;

        // Subscribe to per-node channel
        let node_subject = format!("{}.node.{}", self.config.prefix, handlers.node_id);
        let mut node_subscription = client
            .subscribe(Subject::from(node_subject.clone()))
            .await
            .map_err(|e| Error::Internal(format!("Failed to subscribe to node subject: {e}")))?;

        info!(
            "NATS transport listening on subjects: {}, {}, {}, {}, {}",
            broadcast_subject, request_subject, response_subject, inbox_subject, node_subject
        );

        // Spawn a task to handle broadcast messages
        let broadcast_handler = handlers.on_broadcast.clone();
        tokio::spawn(async move {
            loop {
                if !running_broadcast.load(Ordering::Relaxed) {
                    break;
                }
                let msg = tokio::select! {
                    _ = shutdown_broadcast.notified() => break,
                    msg = broadcast_subscription.next() => msg,
                };
                let Some(msg) = msg else {
                    break;
                };
                metrics_broadcast.record_received();
                let handler = broadcast_handler.clone();
                let metrics = metrics_broadcast.clone();
                let metrics_driver = metrics_driver_broadcast.clone();
                tokio::spawn(async move {
                    match sonic_rs::from_slice::<BroadcastMessage>(&msg.payload) {
                        Ok(broadcast) => {
                            handler(broadcast).await;
                            metrics.record_processed();
                        }
                        Err(e) => {
                            metrics.record_parse_error();
                            if let Some(m) = metrics_driver.get() {
                                m.mark_horizontal_transport_message_dropped("nats");
                            }
                            let payload_preview =
                                String::from_utf8_lossy(&msg.payload[..msg.payload.len().min(200)]);
                            error!(
                                "Failed to parse broadcast message: {} - payload preview: {}",
                                e, payload_preview
                            );
                        }
                    }
                });
            }
            warn!("Broadcast subscription ended unexpectedly");
        });

        // Spawn a task to handle request messages
        let request_handler = handlers.on_request.clone();
        tokio::spawn(async move {
            loop {
                if !running_request.load(Ordering::Relaxed) {
                    break;
                }
                let msg = tokio::select! {
                    _ = shutdown_request.notified() => break,
                    msg = request_subscription.next() => msg,
                };
                let Some(msg) = msg else {
                    break;
                };
                metrics_request.record_received();
                let handler = request_handler.clone();
                let metrics = metrics_request.clone();
                let metrics_driver = metrics_driver_request.clone();
                let client = response_client.clone();
                tokio::spawn(async move {
                    match sonic_rs::from_slice::<RequestBody>(&msg.payload) {
                        Ok(request) => {
                            let response_result = handler(request).await;

                            if let Ok(response) = response_result
                                && let Ok(response_data) = sonic_rs::to_vec(&response)
                            {
                                // Some requests (heartbeats, presence sync) have no reply address
                                // and expect no response. Only reply when explicitly asked.
                                if let Some(reply_to) = msg.reply
                                    && let Err(e) =
                                        client.publish(reply_to, response_data.into()).await
                                {
                                    warn!("Failed to publish response: {}", e);
                                }
                            }
                            metrics.record_processed();
                        }
                        Err(e) => {
                            metrics.record_parse_error();
                            if let Some(m) = metrics_driver.get() {
                                m.mark_horizontal_transport_message_dropped("nats");
                            }
                            let payload_preview =
                                String::from_utf8_lossy(&msg.payload[..msg.payload.len().min(200)]);
                            error!(
                                "Failed to parse request message: {} - payload preview: {}",
                                e, payload_preview
                            );
                        }
                    }
                });
            }
            warn!("Request subscription ended unexpectedly");
        });

        // Spawn a task to handle response messages
        let response_handler = handlers.on_response.clone();
        tokio::spawn(async move {
            loop {
                if !running_response.load(Ordering::Relaxed) {
                    break;
                }
                let msg = tokio::select! {
                    _ = shutdown_response.notified() => break,
                    msg = response_subscription.next() => msg,
                };
                let Some(msg) = msg else {
                    break;
                };
                metrics_response.record_received();
                match sonic_rs::from_slice::<ResponseBody>(&msg.payload) {
                    Ok(response) => {
                        response_handler(response).await;
                        metrics_response.record_processed();
                    }
                    Err(e) => {
                        metrics_response.record_parse_error();
                        if let Some(m) = metrics_driver_response.get() {
                            m.mark_horizontal_transport_message_dropped("nats");
                        }
                        let payload_preview =
                            String::from_utf8_lossy(&msg.payload[..msg.payload.len().min(200)]);
                        error!(
                            "Failed to parse response message: {} - payload preview: {}",
                            e, payload_preview
                        );
                    }
                }
            }
            warn!("Response subscription ended unexpectedly");
        });

        // Spawn a task to handle inbox responses
        let inbox_handler = handlers.on_response.clone();
        tokio::spawn(async move {
            loop {
                if !running_inbox.load(Ordering::Relaxed) {
                    break;
                }
                let msg = tokio::select! {
                    _ = shutdown_inbox.notified() => break,
                    msg = inbox_subscription.next() => msg,
                };
                let Some(msg) = msg else {
                    break;
                };
                metrics_inbox.record_received();
                match sonic_rs::from_slice::<ResponseBody>(&msg.payload) {
                    Ok(response) => {
                        inbox_handler(response).await;
                        metrics_inbox.record_processed();
                    }
                    Err(e) => {
                        metrics_inbox.record_parse_error();
                        if let Some(m) = metrics_driver_inbox.get() {
                            m.mark_horizontal_transport_message_dropped("nats");
                        }
                        let payload_preview =
                            String::from_utf8_lossy(&msg.payload[..msg.payload.len().min(200)]);
                        error!(
                            "Failed to parse inbox response: {} - payload preview: {}",
                            e, payload_preview
                        );
                    }
                }
            }
            warn!("Inbox subscription ended unexpectedly");
        });

        // Spawn a task to handle per-node targeted requests
        let node_request_handler = handlers.on_request.clone();
        let metrics_node = self.metrics.clone();
        let metrics_driver_node = self.metrics_driver.clone();
        let shutdown_node = self.shutdown.clone();
        let running_node = self.is_running.clone();

        tokio::spawn(async move {
            loop {
                if !running_node.load(Ordering::Relaxed) {
                    break;
                }
                let msg = tokio::select! {
                    _ = shutdown_node.notified() => break,
                    msg = node_subscription.next() => msg,
                };
                let Some(msg) = msg else {
                    break;
                };
                metrics_node.record_received();
                match sonic_rs::from_slice::<RequestBody>(&msg.payload) {
                    Ok(request) => {
                        let _ = node_request_handler(request).await;
                        metrics_node.record_processed();
                    }
                    Err(e) => {
                        metrics_node.record_parse_error();
                        if let Some(m) = metrics_driver_node.get() {
                            m.mark_horizontal_transport_message_dropped("nats");
                        }
                        let preview =
                            String::from_utf8_lossy(&msg.payload[..msg.payload.len().min(200)]);
                        error!(
                            "Failed to parse node-targeted request: {} - preview: {}",
                            e, preview
                        );
                    }
                }
            }
            warn!("Node subscription ended unexpectedly");
        });

        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        // If nodes_number is explicitly set, use that value
        if let Some(nodes) = self.config.nodes_number {
            return Ok(nodes as usize);
        }

        match self.discover_node_count_via_system_ping().await {
            Ok(Some(nodes)) => Ok(nodes.max(1)),
            Ok(None) => Ok(1),
            Err(error) => {
                warn!(
                    "NATS node discovery via system ping failed: {}. Falling back to 1 node",
                    error
                );
                Ok(1)
            }
        }
    }

    async fn check_health(&self) -> Result<()> {
        // Check if the NATS client connection is still active
        // NATS client maintains internal health state
        let state = self.client.connection_state();
        match state {
            async_nats::connection::State::Connected => Ok(()),
            async_nats::connection::State::Disconnected => Err(
                sockudo_core::error::Error::Connection("NATS client is disconnected".to_string()),
            ),
            other_state => Err(sockudo_core::error::Error::Connection(format!(
                "NATS client in transitional state: {other_state:?}"
            ))),
        }
    }

    fn set_metrics(&self, metrics: Arc<dyn MetricsInterface + Send + Sync>) {
        let _ = self.metrics_driver.set(metrics);
    }

    fn new_inbox(&self) -> Option<String> {
        Some(format!(
            "{}{}",
            self.inbox_prefix,
            Uuid::new_v4().as_simple()
        ))
    }

    async fn publish_request_with_reply(
        &self,
        request: &RequestBody,
        reply_to: &str,
    ) -> Result<()> {
        let request_data = sonic_rs::to_vec(request)
            .map_err(|e| Error::Other(format!("Failed to serialize request: {e}")))?;

        self.client
            .publish_with_reply(
                Subject::from(self.request_subject.clone()),
                Subject::from(reply_to.to_string()),
                request_data.into(),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to publish request: {e}")))?;

        debug!(
            "Published request {} with reply_to via NATS",
            request.request_id
        );
        Ok(())
    }

    async fn publish_request_to_node(
        &self,
        request: &RequestBody,
        target_node_id: &str,
    ) -> Result<()> {
        let target_subject = format!("{}.node.{}", self.config.prefix, target_node_id);
        let request_data = sonic_rs::to_vec(request)
            .map_err(|e| Error::Other(format!("Failed to serialize request: {e}")))?;
        self.client
            .publish(Subject::from(target_subject), request_data.into())
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to publish to node {target_node_id}: {e}"))
            })?;
        debug!(
            "Published request {} to node {} via NATS",
            request.request_id, target_node_id
        );
        Ok(())
    }

    async fn sync_presence_state_to_node(
        &self,
        horizontal: &Arc<crate::horizontal_adapter::HorizontalAdapter>,
        target_node_id: &str,
    ) -> Result<()> {
        use crate::horizontal_transport::send_presence_state_to_node;

        let Some(chunk_size) = self.config.presence_sync_chunk_size else {
            return send_presence_state_to_node(self, horizontal, target_node_id).await;
        };

        let (our_node_id, serialized_chunks, total_channels) = {
            let registry = horizontal.cluster_presence_registry.read().await;
            let Some(our_data) = registry.get(&horizontal.node_id) else {
                debug!("No presence data to send to new node: {}", target_node_id);
                return Ok(());
            };
            if our_data.is_empty() {
                debug!("Empty presence data for new node: {}", target_node_id);
                return Ok(());
            }
            let total = our_data.len();
            let chunks = build_presence_state_chunks(our_data, chunk_size)?;
            (horizontal.node_id.clone(), chunks, total)
        };

        let total_chunks = serialized_chunks.len();
        debug!(
            "Sending presence state to node {} in {} chunk(s) ({} channels)",
            target_node_id, total_chunks, total_channels
        );

        for (i, chunk_value) in serialized_chunks.into_iter().enumerate() {
            let sync_request = RequestBody {
                request_id: generate_request_id(),
                node_id: our_node_id.clone(),
                app_id: "cluster".to_string(),
                request_type: RequestType::PresenceStateSync,
                target_node_id: Some(target_node_id.to_string()),
                user_info: Some(chunk_value),
                channel: None,
                socket_id: None,
                user_id: None,
                timestamp: None,
                dead_node_id: None,
                reply_to: None,
                channels: None,
            };

            if let Err(e) = self
                .publish_request_to_node(&sync_request, target_node_id)
                .await
            {
                warn!(
                    "Failed to publish presence chunk {}/{} to node {}: {}",
                    i + 1,
                    total_chunks,
                    target_node_id,
                    e
                );
            }
        }

        if let Err(e) = self.client.flush().await {
            warn!("Failed to flush NATS client after chunked sync: {}", e);
        }

        info!(
            "Sent presence state to new node: {} ({} channels in {} chunk(s))",
            target_node_id, total_channels, total_chunks
        );
        Ok(())
    }
}

impl Drop for NatsTransport {
    fn drop(&mut self) {
        if self.owner_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.is_running.store(false, Ordering::Relaxed);
            self.shutdown.notify_waiters();

            // Drain flushes pending messages and sends UNSUB before closing.
            // try_current() avoids panicking if the runtime is already gone.
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let client = self.client.clone();
                handle.spawn(async move {
                    match tokio::time::timeout(Duration::from_secs(5), client.drain()).await {
                        Ok(Ok(())) => debug!("NATS client drained successfully"),
                        Ok(Err(e)) => warn!("NATS client drain failed: {}", e),
                        Err(_) => warn!("NATS client drain timed out"),
                    }
                });
            }
        }
    }
}

impl Clone for NatsTransport {
    fn clone(&self) -> Self {
        self.owner_count.fetch_add(1, Ordering::Relaxed);
        Self {
            client: self.client.clone(),
            broadcast_subject: self.broadcast_subject.clone(),
            request_subject: self.request_subject.clone(),
            response_subject: self.response_subject.clone(),
            inbox_prefix: self.inbox_prefix.clone(),
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            metrics_driver: self.metrics_driver.clone(),
            shutdown: self.shutdown.clone(),
            is_running: self.is_running.clone(),
            owner_count: self.owner_count.clone(),
        }
    }
}
