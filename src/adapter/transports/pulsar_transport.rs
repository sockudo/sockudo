#[cfg(feature = "with-pulsar")]
use crate::adapter::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
#[cfg(feature = "with-pulsar")]
use crate::adapter::horizontal_transport::{
    HorizontalTransport, TransportConfig, TransportHandlers,
};
#[cfg(feature = "with-pulsar")]
use crate::error::{Error, Result};
#[cfg(feature = "with-pulsar")]
use crate::options::PulsarAdapterConfig;
#[cfg(feature = "with-pulsar")]
use async_trait::async_trait;
#[cfg(feature = "with-pulsar")]
use futures_util::TryStreamExt;
#[cfg(feature = "with-pulsar")]
use pulsar::{
    Authentication, Consumer, ConsumerOptions, ProducerOptions, Pulsar, SubType, TokioExecutor,
};
#[cfg(feature = "with-pulsar")]
use serde_json;
#[cfg(feature = "with-pulsar")]
use std::time::Duration;
#[cfg(feature = "with-pulsar")]
use tokio::task;
#[cfg(feature = "with-pulsar")]
use tracing::{debug, info, warn};

/// Pulsar transport implementation
#[cfg(feature = "with-pulsar")]
#[derive(Clone)]
pub struct PulsarTransport {
    client: Pulsar<TokioExecutor>,
    broadcast_topic: String,
    request_topic: String,
    response_topic: String,
    config: PulsarAdapterConfig,
    // Channels for sending messages to dedicated producer tasks
    broadcast_sender: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    request_sender: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    response_sender: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
}

#[cfg(feature = "with-pulsar")]
impl TransportConfig for PulsarAdapterConfig {
    fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }

    fn prefix(&self) -> &str {
        &self.prefix
    }
}

#[cfg(feature = "with-pulsar")]
#[async_trait]
impl HorizontalTransport for PulsarTransport {
    type Config = PulsarAdapterConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        info!(
            "Pulsar transport config: service_url={}, prefix={}, request_timeout={}ms, connection_timeout={}ms",
            config.service_url,
            config.prefix,
            config.request_timeout_ms,
            config.connection_timeout_ms
        );

        // Build Pulsar client
        let mut builder = Pulsar::builder(&config.service_url, TokioExecutor);

        // Set up authentication if provided
        if let (Some(oauth2_issuer_url), Some(oauth2_client_id), Some(oauth2_client_secret)) = (
            config.oauth2_issuer_url.as_ref(),
            config.oauth2_client_id.as_ref(),
            config.oauth2_client_secret.as_ref(),
        ) {
            let auth = Authentication {
                name: "oauth2".to_string(),
                data: format!(
                    "{{\"issuerUrl\":\"{}\",\"clientId\":\"{}\",\"clientSecret\":\"{}\"{}}}",
                    oauth2_issuer_url,
                    oauth2_client_id,
                    oauth2_client_secret,
                    config
                        .oauth2_audience
                        .as_ref()
                        .map(|aud| format!(",\"audience\":\"{}\"", aud))
                        .unwrap_or_default()
                )
                .into_bytes(),
            };
            builder = builder.with_auth(auth);
        } else if let (Some(auth_name), Some(auth_params)) =
            (config.auth_name.as_ref(), config.auth_params.as_ref())
        {
            let auth = Authentication {
                name: auth_name.clone(),
                data: auth_params.as_bytes().to_vec(),
            };
            builder = builder.with_auth(auth);
        }

        // Set up TLS if cert path provided
        if let Some(cert_path) = config.tls_trust_certs_file_path.as_ref() {
            builder = builder
                .with_certificate_chain_file(cert_path)
                .map_err(|e| Error::Internal(format!("Failed to load TLS certificate: {e}")))?;
        }

        let client = builder
            .build()
            .await
            .map_err(|e| Error::Internal(format!("Failed to connect to Pulsar: {e}")))?;

        // Build topic names in Pulsar format: tenant/namespace/topic
        let broadcast_topic = format!(
            "{}/{}/{}-broadcast",
            config.tenant, config.namespace, config.prefix
        );
        let request_topic = format!(
            "{}/{}/{}-requests",
            config.tenant, config.namespace, config.prefix
        );
        let response_topic = format!(
            "{}/{}/{}-responses",
            config.tenant, config.namespace, config.prefix
        );

        // Create channels for dedicated producer tasks
        let (broadcast_sender, broadcast_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (request_sender, request_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (response_sender, response_receiver) = tokio::sync::mpsc::unbounded_channel();

        // Spawn dedicated producer tasks that will live for the transport's lifetime
        info!("Spawning producer tasks for Pulsar topics");
        Self::spawn_producer_task(client.clone(), broadcast_topic.clone(), broadcast_receiver);
        Self::spawn_producer_task(client.clone(), request_topic.clone(), request_receiver);
        Self::spawn_producer_task(client.clone(), response_topic.clone(), response_receiver);
        info!("All producer tasks spawned successfully");

        Ok(Self {
            client,
            broadcast_topic,
            request_topic,
            response_topic,
            config,
            broadcast_sender,
            request_sender,
            response_sender,
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        let message_data = serde_json::to_vec(message)
            .map_err(|e| Error::Other(format!("Failed to serialize broadcast message: {e}")))?;

        // Send to dedicated producer task via channel
        info!(
            "Attempting to send broadcast message of {} bytes",
            message_data.len()
        );
        self.broadcast_sender.send(message_data).map_err(|e| {
            warn!("Failed to send to broadcast producer task: {}", e);
            Error::Internal("Broadcast producer task is down".to_string())
        })?;

        info!("Successfully sent broadcast message to Pulsar producer task");
        Ok(())
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        let request_data = serde_json::to_vec(request)
            .map_err(|e| Error::Other(format!("Failed to serialize request: {e}")))?;

        // Send to dedicated producer task via channel
        info!(
            "Attempting to send request {} of {} bytes",
            request.request_id,
            request_data.len()
        );
        self.request_sender.send(request_data).map_err(|e| {
            warn!("Failed to send request to producer task: {}", e);
            Error::Internal("Request producer task is down".to_string())
        })?;

        info!(
            "Successfully sent request {} to Pulsar producer task",
            request.request_id
        );
        Ok(())
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        let response_data = serde_json::to_vec(response)
            .map_err(|e| Error::Other(format!("Failed to serialize response: {e}")))?;

        // Send to dedicated producer task via channel
        info!(
            "Attempting to send response of {} bytes",
            response_data.len()
        );
        self.response_sender.send(response_data).map_err(|e| {
            warn!("Failed to send response to producer task: {}", e);
            Error::Internal("Response producer task is down".to_string())
        })?;

        info!("Successfully sent response to Pulsar producer task");
        Ok(())
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        let client = self.client.clone();
        let broadcast_topic = self.broadcast_topic.clone();
        let request_topic = self.request_topic.clone();
        let response_topic = self.response_topic.clone();

        // Subscribe to broadcast topic
        let mut broadcast_consumer: Consumer<Vec<u8>, _> = client
            .consumer()
            .with_topic(&broadcast_topic)
            .with_consumer_name("sockudo-broadcast-consumer")
            .with_subscription_type(SubType::Shared)
            .with_subscription("sockudo-broadcast")
            .with_options(ConsumerOptions::default())
            .build()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create broadcast consumer: {e}")))?;

        // Subscribe to request topic
        let mut request_consumer: Consumer<Vec<u8>, _> = client
            .consumer()
            .with_topic(&request_topic)
            .with_consumer_name("sockudo-request-consumer")
            .with_subscription_type(SubType::Shared)
            .with_subscription("sockudo-requests")
            .with_options(ConsumerOptions::default())
            .build()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create request consumer: {e}")))?;

        // Subscribe to response topic
        let mut response_consumer: Consumer<Vec<u8>, _> = client
            .consumer()
            .with_topic(&response_topic)
            .with_consumer_name("sockudo-response-consumer")
            .with_subscription_type(SubType::Shared)
            .with_subscription("sockudo-responses")
            .with_options(ConsumerOptions::default())
            .build()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create response consumer: {e}")))?;

        info!(
            "Pulsar transport listening on topics: {}, {}, {}",
            broadcast_topic, request_topic, response_topic
        );

        // Spawn task to handle broadcast messages
        let broadcast_handler = handlers.on_broadcast.clone();
        task::spawn(async move {
            loop {
                match broadcast_consumer.try_next().await {
                    Ok(Some(msg)) => {
                        if let Ok(broadcast) =
                            serde_json::from_slice::<BroadcastMessage>(&msg.payload.data)
                        {
                            broadcast_handler(broadcast).await;
                        }
                        // Acknowledge the message
                        let _ = broadcast_consumer.ack(&msg).await;
                    }
                    Ok(None) => {
                        // No message available, continue
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        warn!("Error receiving broadcast message: {}", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });

        // Spawn task to handle request messages
        let request_handler = handlers.on_request.clone();
        let transport_for_responses = self.clone();
        task::spawn(async move {
            loop {
                match request_consumer.try_next().await {
                    Ok(Some(msg)) => {
                        if let Ok(request) =
                            serde_json::from_slice::<RequestBody>(&msg.payload.data)
                        {
                            let response_result = request_handler(request).await;

                            if let Ok(response) = response_result {
                                // Use the cached response producer instead of creating a new one
                                let _ = transport_for_responses.publish_response(&response).await;
                            }
                        }
                        // Acknowledge the message
                        let _ = request_consumer.ack(&msg).await;
                    }
                    Ok(None) => {
                        // No message available, continue
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        warn!("Error receiving request message: {}", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });

        // Spawn task to handle response messages
        let response_handler = handlers.on_response.clone();
        task::spawn(async move {
            loop {
                match response_consumer.try_next().await {
                    Ok(Some(msg)) => {
                        if let Ok(response) =
                            serde_json::from_slice::<ResponseBody>(&msg.payload.data)
                        {
                            response_handler(response).await;
                        }
                        // Acknowledge the message
                        let _ = response_consumer.ack(&msg).await;
                    }
                    Ok(None) => {
                        // No message available, continue
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        warn!("Error receiving response message: {}", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });

        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        // If nodes_number is explicitly set, use that value
        if let Some(nodes) = self.config.nodes_number {
            return Ok(nodes as usize);
        }

        // Pulsar doesn't provide an easy way to count active consumers
        // For now, we'll assume at least 1 node (ourselves)
        Ok(1)
    }

    async fn check_health(&self) -> Result<()> {
        // Simple health check by trying to get topic metadata
        self.client
            .lookup_topic(&self.broadcast_topic)
            .await
            .map_err(|e| Error::Connection(format!("Pulsar health check failed: {e}")))?;

        Ok(())
    }
}

#[cfg(feature = "with-pulsar")]
impl PulsarTransport {
    /// Spawn a dedicated producer task for a topic that reuses the same producer
    fn spawn_producer_task(
        client: Pulsar<TokioExecutor>,
        topic: String,
        mut receiver: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    ) {
        tokio::spawn(async move {
            // Create a single persistent producer for this topic
            let mut producer = match client
                .producer()
                .with_topic(&topic)
                .with_options(ProducerOptions {
                    batch_size: Some(100),
                    ..Default::default()
                })
                .build()
                .await
            {
                Ok(producer) => producer,
                Err(e) => {
                    warn!("Failed to create producer for topic {}: {}", topic, e);
                    return;
                }
            };

            info!("Started persistent producer for topic: {}", topic);

            // Process messages as they arrive
            while let Some(message_data) = receiver.recv().await {
                info!(
                    "Producer task for {} received message of {} bytes",
                    topic,
                    message_data.len()
                );
                match producer.send_non_blocking(message_data).await {
                    Ok(_) => {
                        info!("Successfully sent message to Pulsar topic {}", topic);
                    }
                    Err(e) => {
                        warn!("Failed to send message to {}: {}", topic, e);
                    }
                }
            }

            // Gracefully shutdown when channel is closed
            info!("Producer task for topic {} shutting down", topic);
            // Note: Producer will be dropped here, which handles cleanup
        });
    }
}
