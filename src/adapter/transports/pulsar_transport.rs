use crate::adapter::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::adapter::horizontal_transport::{
    HorizontalTransport, TransportConfig, TransportHandlers,
};
use crate::error::{Error, Result};
use crate::options::PulsarAdapterConfig;
use async_trait::async_trait;
use futures_util::TryStreamExt;
use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use pulsar::message::proto::command_subscribe::SubType;
use pulsar::{Authentication, ConnectionRetryOptions, Consumer, Pulsar, TokioExecutor};
use std::time::Duration;
use tracing::{debug, error, info};

/// Pulsar transport implementation
#[derive(Clone)]
pub struct PulsarTransport {
    pulsar: Pulsar<TokioExecutor>,
    broadcast_topic: String,
    request_topic: String,
    response_topic: String,
    config: PulsarAdapterConfig,
}

impl TransportConfig for PulsarAdapterConfig {
    fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }

    fn prefix(&self) -> &str {
        &self.prefix
    }
}

#[async_trait]
impl HorizontalTransport for PulsarTransport {
    type Config = PulsarAdapterConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        info!(
            "Pulsar transport config: url={}, namespace={}, prefix={}, request_timeout={}ms",
            config.url, config.namespace, config.prefix, config.request_timeout_ms
        );

        // Build Pulsar client
        let mut pulsar_builder = Pulsar::builder(&config.url, TokioExecutor);

        // Set authentication if provided
        if let Some(auth_config) = &config.auth {
            match auth_config.method.as_str() {
                "oauth2" => {
                    let oauth2_params = OAuth2Params {
                        issuer_url: auth_config
                            .issuer_url
                            .as_ref()
                            .ok_or_else(|| {
                                Error::Configuration("OAuth2 issuer_url is required".to_string())
                            })?
                            .clone(),
                        credentials_url: auth_config
                            .credentials_url
                            .as_ref()
                            .ok_or_else(|| {
                                Error::Configuration(
                                    "OAuth2 credentials_url is required".to_string(),
                                )
                            })?
                            .clone(),
                        audience: Option::from(
                            auth_config
                                .audience
                                .as_ref()
                                .ok_or_else(|| {
                                    Error::Configuration("OAuth2 audience is required".to_string())
                                })?
                                .clone(),
                        ),
                        scope: auth_config.scope.clone(),
                    };
                    let auth = OAuth2Authentication::client_credentials(oauth2_params);
                    pulsar_builder = pulsar_builder.with_auth_provider(auth);
                }
                "token" => {
                    let token = auth_config.token.as_ref().ok_or_else(|| {
                        Error::Configuration(
                            "Token is required for token authentication".to_string(),
                        )
                    })?;
                    let auth = Authentication {
                        name: "token".to_string(),
                        data: token.clone().into_bytes(),
                    };
                    pulsar_builder = pulsar_builder.with_auth(auth);
                }
                method => {
                    return Err(Error::Configuration(format!(
                        "Unsupported authentication method: {}",
                        method
                    )));
                }
            }
        }
        if config.connection_timeout_ms == 0 {
            return Err(Error::Configuration(
                "Pulsar connection_timeout_ms must be a positive, non-zero value.".to_string(),
            ));
        }

        // Set connection timeout
        let connection_retry_options = ConnectionRetryOptions {
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            max_retries: 10,
            connection_timeout: Duration::from_millis(config.connection_timeout_ms),
            keep_alive: Duration::from_secs(30),
        };
        pulsar_builder = pulsar_builder.with_connection_retry_options(connection_retry_options);

        // Build the Pulsar client
        let pulsar = pulsar_builder
            .build()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create Pulsar client: {e}")))?;

        // Build topic names with namespace
        let namespace_prefix = if config.namespace.is_empty() {
            config.prefix.clone()
        } else {
            format!("{}/{}", config.namespace, config.prefix)
        };

        let broadcast_topic = format!("persistent://public/default/{}.broadcast", namespace_prefix);
        let request_topic = format!("persistent://public/default/{}.requests", namespace_prefix);
        let response_topic = format!("persistent://public/default/{}.responses", namespace_prefix);

        info!(
            "Pulsar transport topics: broadcast={}, request={}, response={}",
            broadcast_topic, request_topic, response_topic
        );

        Ok(Self {
            pulsar,
            broadcast_topic,
            request_topic,
            response_topic,
            config,
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        let message_data = serde_json::to_vec(message)
            .map_err(|e| Error::Other(format!("Failed to serialize broadcast message: {e}")))?;

        let mut producer = self
            .pulsar
            .producer()
            .with_topic(&self.broadcast_topic)
            .with_name("broadcast-producer")
            .build()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create broadcast producer: {e}")))?;

        producer
            .send_non_blocking(message_data)
            .await
            .map_err(|e| Error::Internal(format!("Failed to publish broadcast: {e}")))?;

        debug!("Published broadcast message via Pulsar");
        Ok(())
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        let request_data = serde_json::to_vec(request)
            .map_err(|e| Error::Other(format!("Failed to serialize request: {e}")))?;

        let mut producer = self
            .pulsar
            .producer()
            .with_topic(&self.request_topic)
            .with_name("request-producer")
            .build()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create request producer: {e}")))?;

        producer
            .send_non_blocking(request_data)
            .await
            .map_err(|e| Error::Internal(format!("Failed to publish request: {e}")))?;

        debug!("Broadcasted request {} via Pulsar", request.request_id);
        Ok(())
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        let response_data = serde_json::to_vec(response)
            .map_err(|e| Error::Other(format!("Failed to serialize response: {e}")))?;

        let mut producer = self
            .pulsar
            .producer()
            .with_topic(&self.response_topic)
            .with_name("response-producer")
            .build()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create response producer: {e}")))?;

        producer
            .send_non_blocking(response_data)
            .await
            .map_err(|e| Error::Internal(format!("Failed to publish response: {e}")))?;

        debug!("Published response via Pulsar");
        Ok(())
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        let pulsar = self.pulsar.clone();
        let broadcast_topic = self.broadcast_topic.clone();
        let request_topic = self.request_topic.clone();
        let response_topic = self.response_topic.clone();
        let response_pulsar = self.pulsar.clone();

        // Create subscription name based on node ID or instance
        let subscription_name = format!(
            "sockudo-{}",
            std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string())
        );
        info!("Using subscription name: {}", subscription_name);

        // Subscribe to broadcast topic
        let mut broadcast_consumer: Consumer<Vec<u8>, TokioExecutor> = pulsar
            .consumer()
            .with_topic(&broadcast_topic)
            .with_subscription(&subscription_name)
            .with_consumer_name("broadcast-consumer")
            .with_subscription_type(SubType::Shared)
            .build()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create broadcast consumer: {e}")))?;

        // Subscribe to request topic
        let mut request_consumer: Consumer<Vec<u8>, TokioExecutor> = pulsar
            .consumer()
            .with_topic(&request_topic)
            .with_subscription(&subscription_name)
            .with_consumer_name("request-consumer")
            .with_subscription_type(SubType::Shared)
            .build()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create request consumer: {e}")))?;

        // Subscribe to response topic
        let mut response_consumer: Consumer<Vec<u8>, TokioExecutor> = pulsar
            .consumer()
            .with_topic(&response_topic)
            .with_subscription(&subscription_name)
            .with_consumer_name("response-consumer")
            .with_subscription_type(SubType::Shared)
            .build()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create response consumer: {e}")))?;

        info!(
            "Pulsar transport listening on topics: {}, {}, {}",
            broadcast_topic, request_topic, response_topic
        );

        // Spawn a task to handle broadcast messages
        let broadcast_handler = handlers.on_broadcast.clone();
        tokio::spawn(async move {
            while let Some(msg) = broadcast_consumer.try_next().await.unwrap_or(None) {
                if let Ok(broadcast) = serde_json::from_slice::<BroadcastMessage>(&msg.payload.data)
                {
                    broadcast_handler(broadcast).await;
                }
                if let Err(e) = broadcast_consumer.ack(&msg).await {
                    error!("Failed to ack broadcast message: {}", e);
                }
            }
        });

        // Spawn a task to handle request messages
        let request_handler = handlers.on_request.clone();
        tokio::spawn(async move {
            while let Some(msg) = request_consumer.try_next().await.unwrap_or(None) {
                if let Ok(request) = serde_json::from_slice::<RequestBody>(&msg.payload.data) {
                    let response_result = request_handler(request).await;

                    if let Ok(response) = response_result {
                        if let Ok(response_data) = serde_json::to_vec(&response) {
                            if let Ok(mut producer) = response_pulsar
                                .producer()
                                .with_topic(&response_topic)
                                .with_name("response-producer")
                                .build()
                                .await
                            {
                                let _ = producer.send_non_blocking(response_data).await;
                            }
                        }
                    }
                }
                if let Err(e) = request_consumer.ack(&msg).await {
                    error!("Failed to ack request message: {}", e);
                }
            }
        });

        // Spawn a task to handle response messages
        let response_handler = handlers.on_response.clone();
        tokio::spawn(async move {
            while let Some(msg) = response_consumer.try_next().await.unwrap_or(None) {
                if let Ok(response) = serde_json::from_slice::<ResponseBody>(&msg.payload.data) {
                    response_handler(response).await;
                }
                if let Err(e) = response_consumer.ack(&msg).await {
                    error!("Failed to ack response message: {}", e);
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
        // Try to create a simple producer to check connectivity
        match self
            .pulsar
            .producer()
            .with_topic("persistent://public/default/health-check")
            .with_name("health-check-producer")
            .build()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Connection(format!(
                "Pulsar health check failed: {}",
                e
            ))),
        }
    }
}
