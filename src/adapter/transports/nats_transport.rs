use crate::adapter::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::adapter::horizontal_transport::{
    HorizontalTransport, TransportConfig, TransportHandlers,
};
use crate::error::{Error, Result};
use crate::options::NatsAdapterConfig;
use async_nats::{Client as NatsClient, ConnectOptions as NatsOptions, Subject};
use async_trait::async_trait;
use futures::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, error, info, warn};

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
#[derive(Clone)]
pub struct NatsTransport {
    client: NatsClient,
    broadcast_subject: String,
    request_subject: String,
    response_subject: String,
    config: NatsAdapterConfig,
    /// Metrics for tracking message processing
    metrics: Arc<TransportMetrics>,
}

impl NatsTransport {
    /// Get transport metrics for monitoring
    pub fn get_metrics(&self) -> Arc<TransportMetrics> {
        self.metrics.clone()
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
            "NATS transport credentials: username={:?}, password={:?}, token={:?}",
            config.username, config.password, config.token
        );

        // Build NATS Options
        let mut nats_options = NatsOptions::new();

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

        Ok(Self {
            client,
            broadcast_subject,
            request_subject,
            response_subject,
            config,
            metrics: Arc::new(TransportMetrics::new()),
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        let message_data = serde_json::to_vec(message)
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
        let request_data = serde_json::to_vec(request)
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
        let response_data = serde_json::to_vec(response)
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

        info!(
            "NATS transport listening on subjects: {}, {}, {}",
            broadcast_subject, request_subject, response_subject
        );

        // Spawn a task to handle broadcast messages
        let broadcast_handler = handlers.on_broadcast.clone();
        tokio::spawn(async move {
            while let Some(msg) = broadcast_subscription.next().await {
                metrics_broadcast.record_received();
                match serde_json::from_slice::<BroadcastMessage>(&msg.payload) {
                    Ok(broadcast) => {
                        broadcast_handler(broadcast).await;
                        metrics_broadcast.record_processed();
                    }
                    Err(e) => {
                        metrics_broadcast.record_parse_error();
                        let payload_preview =
                            String::from_utf8_lossy(&msg.payload[..msg.payload.len().min(200)]);
                        error!(
                            "Failed to parse broadcast message: {} - payload preview: {}",
                            e, payload_preview
                        );
                    }
                }
            }
            warn!("Broadcast subscription ended unexpectedly");
        });

        // Spawn a task to handle request messages
        let request_handler = handlers.on_request.clone();
        tokio::spawn(async move {
            while let Some(msg) = request_subscription.next().await {
                metrics_request.record_received();
                match serde_json::from_slice::<RequestBody>(&msg.payload) {
                    Ok(request) => {
                        let response_result = request_handler(request).await;

                        if let Ok(response) = response_result
                            && let Ok(response_data) = serde_json::to_vec(&response)
                                && let Err(e) = response_client
                                    .publish(
                                        Subject::from(response_subject.clone()),
                                        response_data.into(),
                                    )
                                    .await
                                {
                                    warn!("Failed to publish response: {}", e);
                                }
                        metrics_request.record_processed();
                    }
                    Err(e) => {
                        metrics_request.record_parse_error();
                        let payload_preview =
                            String::from_utf8_lossy(&msg.payload[..msg.payload.len().min(200)]);
                        error!(
                            "Failed to parse request message: {} - payload preview: {}",
                            e, payload_preview
                        );
                    }
                }
            }
            warn!("Request subscription ended unexpectedly");
        });

        // Spawn a task to handle response messages
        let response_handler = handlers.on_response.clone();
        tokio::spawn(async move {
            while let Some(msg) = response_subscription.next().await {
                metrics_response.record_received();
                match serde_json::from_slice::<ResponseBody>(&msg.payload) {
                    Ok(response) => {
                        response_handler(response).await;
                        metrics_response.record_processed();
                    }
                    Err(e) => {
                        metrics_response.record_parse_error();
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

        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        // If nodes_number is explicitly set, use that value
        if let Some(nodes) = self.config.nodes_number {
            return Ok(nodes as usize);
        }

        // NATS doesn't provide an easy way to count subscriptions like Redis
        // For now, we'll assume at least 1 node (ourselves)
        Ok(1)
    }

    async fn check_health(&self) -> Result<()> {
        // Check if the NATS client connection is still active
        // NATS client maintains internal health state
        let state = self.client.connection_state();
        match state {
            async_nats::connection::State::Connected => Ok(()),
            async_nats::connection::State::Disconnected => Err(crate::error::Error::Connection(
                "NATS client is disconnected".to_string(),
            )),
            other_state => Err(crate::error::Error::Connection(format!(
                "NATS client in transitional state: {other_state:?}"
            ))),
        }
    }
}
