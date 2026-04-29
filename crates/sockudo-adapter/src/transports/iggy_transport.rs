use crate::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::horizontal_transport::{HorizontalTransport, TransportConfig, TransportHandlers};
use async_trait::async_trait;
use bytes::Bytes;
use iggy::prelude::{
    Client, CompressionAlgorithm, Consumer, IggyClient, IggyError, IggyExpiry, IggyMessage,
    MaxTopicSize, MessageClient, Partitioning, PollingStrategy, StreamClient, SystemClient,
    TopicClient, UserClient,
};
use sockudo_core::error::{Error, Result};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::IggyConfig;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};

pub struct IggyTransport {
    client: Arc<IggyClient>,
    stream: String,
    broadcast_topic: String,
    request_topic: String,
    response_topic: String,
    listener_id: String,
    config: IggyConfig,
    metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
    shutdown: Arc<Notify>,
    is_running: Arc<AtomicBool>,
    owner_count: Arc<AtomicUsize>,
}

impl TransportConfig for IggyConfig {
    fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }

    fn prefix(&self) -> &str {
        &self.topic_prefix
    }
}

#[async_trait]
impl HorizontalTransport for IggyTransport {
    type Config = IggyConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        validate_config(&config)?;
        let client = Arc::new(connect_client(&config).await?);
        let stream = normalize_name(&config.stream, "sockudo");
        let prefix = normalize_name(&config.topic_prefix, "sockudo-adapter");
        let broadcast_topic = format!("{prefix}-broadcast");
        let request_topic = format!("{prefix}-requests");
        let response_topic = format!("{prefix}-responses");

        ensure_topic(&client, &config, &stream, &broadcast_topic).await?;
        ensure_topic(&client, &config, &stream, &request_topic).await?;
        ensure_topic(&client, &config, &stream, &response_topic).await?;

        info!(
            "Apache Iggy transport initialized with stream '{}' and topics: {}, {}, {}",
            stream, broadcast_topic, request_topic, response_topic
        );

        Ok(Self {
            client,
            stream,
            broadcast_topic,
            request_topic,
            response_topic,
            listener_id: uuid::Uuid::new_v4().simple().to_string(),
            config,
            metrics: Arc::new(OnceLock::new()),
            shutdown: Arc::new(Notify::new()),
            is_running: Arc::new(AtomicBool::new(true)),
            owner_count: Arc::new(AtomicUsize::new(1)),
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        publish_message(
            &self.client,
            &self.config,
            &self.stream,
            &self.broadcast_topic,
            message,
        )
        .await
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        publish_message(
            &self.client,
            &self.config,
            &self.stream,
            &self.request_topic,
            request,
        )
        .await
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        publish_message(
            &self.client,
            &self.config,
            &self.stream,
            &self.response_topic,
            response,
        )
        .await
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        self.spawn_consumer(
            self.broadcast_topic.clone(),
            format!("{}-broadcast", self.listener_id),
            "broadcast",
            handlers.on_broadcast.clone(),
        )?;
        self.spawn_request_consumer(
            self.request_topic.clone(),
            format!("{}-request", self.listener_id),
            handlers.on_request.clone(),
        )?;
        self.spawn_consumer(
            self.response_topic.clone(),
            format!("{}-response", self.listener_id),
            "response",
            handlers.on_response.clone(),
        )?;
        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        Ok(self.config.nodes_number.unwrap_or(1) as usize)
    }

    async fn check_health(&self) -> Result<()> {
        with_timeout(&self.config, self.client.ping())
            .await
            .map_err(|e| Error::Internal(format!("Apache Iggy health check failed: {e}")))
    }

    fn set_metrics(&self, metrics: Arc<dyn MetricsInterface + Send + Sync>) {
        let _ = self.metrics.set(metrics);
    }
}

impl IggyTransport {
    fn spawn_consumer<T>(
        &self,
        topic: String,
        consumer_name: String,
        kind: &'static str,
        handler: Arc<
            dyn Fn(T) -> crate::horizontal_transport::BoxFuture<'static, ()> + Send + Sync,
        >,
    ) -> Result<()>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let config = self.config.clone();
        let stream = self.stream.clone();
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let client = match connect_client(&config).await {
                Ok(client) => Arc::new(client),
                Err(error) => {
                    error!("Failed to start Apache Iggy {kind} listener: {}", error);
                    return;
                }
            };

            let consumer = match consumer_name.as_str().try_into() {
                Ok(id) => Consumer::new(id),
                Err(error) => {
                    error!("Invalid Apache Iggy {kind} consumer name: {}", error);
                    return;
                }
            };
            let mut strategy = if config.start_from_latest {
                PollingStrategy::last()
            } else {
                PollingStrategy::next()
            };

            loop {
                if !is_running.load(Ordering::Relaxed) {
                    break;
                }
                tokio::select! {
                    _ = shutdown.notified() => break,
                    _ = sleep(Duration::from_millis(config.poll_interval_ms)) => {}
                }

                match poll_messages(&client, &config, &stream, &topic, &consumer, &strategy).await {
                    Ok(messages) => {
                        strategy = PollingStrategy::next();
                        for message in messages {
                            match sonic_rs::from_slice::<T>(&message.payload) {
                                Ok(payload) => handler(payload).await,
                                Err(error) => {
                                    if let Some(metrics) = metrics.get() {
                                        metrics.mark_horizontal_transport_message_dropped("iggy");
                                    }
                                    warn!("Failed to parse Apache Iggy {kind} payload: {}", error);
                                }
                            }
                        }
                    }
                    Err(error) => warn!("Apache Iggy {kind} poll failed: {}", error),
                }
            }
            warn!("Apache Iggy {kind} consumer loop ended");
        });

        Ok(())
    }

    fn spawn_request_consumer(
        &self,
        topic: String,
        consumer_name: String,
        handler: Arc<
            dyn Fn(
                    RequestBody,
                )
                    -> crate::horizontal_transport::BoxFuture<'static, Result<ResponseBody>>
                + Send
                + Sync,
        >,
    ) -> Result<()> {
        let config = self.config.clone();
        let stream = self.stream.clone();
        let response_topic = self.response_topic.clone();
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let client = match connect_client(&config).await {
                Ok(client) => Arc::new(client),
                Err(error) => {
                    error!("Failed to start Apache Iggy request listener: {}", error);
                    return;
                }
            };

            let consumer = match consumer_name.as_str().try_into() {
                Ok(id) => Consumer::new(id),
                Err(error) => {
                    error!("Invalid Apache Iggy request consumer name: {}", error);
                    return;
                }
            };
            let mut strategy = if config.start_from_latest {
                PollingStrategy::last()
            } else {
                PollingStrategy::next()
            };

            loop {
                if !is_running.load(Ordering::Relaxed) {
                    break;
                }
                tokio::select! {
                    _ = shutdown.notified() => break,
                    _ = sleep(Duration::from_millis(config.poll_interval_ms)) => {}
                }

                match poll_messages(&client, &config, &stream, &topic, &consumer, &strategy).await {
                    Ok(messages) => {
                        strategy = PollingStrategy::next();
                        for message in messages {
                            match sonic_rs::from_slice::<RequestBody>(&message.payload) {
                                Ok(request) => match handler(request).await {
                                    Ok(response) => {
                                        if let Err(error) = publish_message(
                                            &client,
                                            &config,
                                            &stream,
                                            &response_topic,
                                            &response,
                                        )
                                        .await
                                        {
                                            warn!(
                                                "Failed to publish Apache Iggy response: {}",
                                                error
                                            );
                                        }
                                    }
                                    Err(error) => {
                                        warn!("Apache Iggy request handler failed: {}", error)
                                    }
                                },
                                Err(error) => {
                                    if let Some(metrics) = metrics.get() {
                                        metrics.mark_horizontal_transport_message_dropped("iggy");
                                    }
                                    warn!("Failed to parse Apache Iggy request payload: {}", error);
                                }
                            }
                        }
                    }
                    Err(error) => warn!("Apache Iggy request poll failed: {}", error),
                }
            }
            warn!("Apache Iggy request consumer loop ended");
        });

        Ok(())
    }
}

impl Clone for IggyTransport {
    fn clone(&self) -> Self {
        self.owner_count.fetch_add(1, Ordering::Relaxed);
        Self {
            client: self.client.clone(),
            stream: self.stream.clone(),
            broadcast_topic: self.broadcast_topic.clone(),
            request_topic: self.request_topic.clone(),
            response_topic: self.response_topic.clone(),
            listener_id: self.listener_id.clone(),
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            shutdown: self.shutdown.clone(),
            is_running: self.is_running.clone(),
            owner_count: self.owner_count.clone(),
        }
    }
}

impl Drop for IggyTransport {
    fn drop(&mut self) {
        if self.owner_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.is_running.store(false, Ordering::Relaxed);
            self.shutdown.notify_waiters();
        }
    }
}

async fn connect_client(config: &IggyConfig) -> Result<IggyClient> {
    let client = IggyClient::from_connection_string(&config.connection_string)
        .map_err(|e| Error::Internal(format!("Invalid Apache Iggy connection string: {e}")))?;
    client
        .connect()
        .await
        .map_err(|e| Error::Internal(format!("Failed to connect to Apache Iggy: {e}")))?;

    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        client
            .login_user(username, password)
            .await
            .map_err(|e| Error::Internal(format!("Failed to authenticate to Apache Iggy: {e}")))?;
    }

    Ok(client)
}

async fn ensure_topic(
    client: &IggyClient,
    config: &IggyConfig,
    stream: &str,
    topic: &str,
) -> Result<()> {
    let stream_id = stream_identifier(stream)?;
    if client
        .get_stream(&stream_id)
        .await
        .map_err(to_iggy_error)?
        .is_none()
    {
        if !config.auto_create {
            return Err(Error::Config(format!(
                "Apache Iggy stream '{stream}' does not exist and auto_create is false"
            )));
        }
        match client.create_stream(stream).await {
            Ok(_) | Err(IggyError::StreamNameAlreadyExists(_)) => {}
            Err(error) => return Err(to_iggy_error(error)),
        }
    }

    let topic_id = stream_identifier(topic)?;
    if client
        .get_topic(&stream_id, &topic_id)
        .await
        .map_err(to_iggy_error)?
        .is_none()
    {
        if !config.auto_create {
            return Err(Error::Config(format!(
                "Apache Iggy topic '{topic}' does not exist and auto_create is false"
            )));
        }
        match client
            .create_topic(
                &stream_id,
                topic,
                config.partitions_count,
                CompressionAlgorithm::default(),
                None,
                IggyExpiry::ServerDefault,
                MaxTopicSize::ServerDefault,
            )
            .await
        {
            Ok(_) | Err(IggyError::TopicNameAlreadyExists(_, _)) => {}
            Err(error) => return Err(to_iggy_error(error)),
        }
    }

    Ok(())
}

async fn publish_message<T: serde::Serialize>(
    client: &IggyClient,
    config: &IggyConfig,
    stream: &str,
    topic: &str,
    message: &T,
) -> Result<()> {
    let payload = sonic_rs::to_vec(message)
        .map_err(|e| Error::Other(format!("Failed to serialize Apache Iggy message: {e}")))?;
    let mut messages = [IggyMessage::builder()
        .payload(Bytes::from(payload))
        .build()
        .map_err(to_iggy_error)?];
    let partitioning = Partitioning::partition_id(config.partition_id);
    with_timeout(
        config,
        client.send_messages(
            &stream_identifier(stream)?,
            &stream_identifier(topic)?,
            &partitioning,
            &mut messages,
        ),
    )
    .await
    .map_err(|e| Error::Internal(format!("Failed to publish Apache Iggy message: {e}")))?;

    debug!("Published Apache Iggy message to {stream}/{topic}");
    Ok(())
}

async fn poll_messages(
    client: &IggyClient,
    config: &IggyConfig,
    stream: &str,
    topic: &str,
    consumer: &Consumer,
    strategy: &PollingStrategy,
) -> Result<Vec<IggyMessage>> {
    let result = with_timeout(
        config,
        client.poll_messages(
            &stream_identifier(stream)?,
            &stream_identifier(topic)?,
            Some(config.partition_id),
            consumer,
            strategy,
            config.poll_batch_size,
            true,
        ),
    )
    .await
    .map_err(|e| Error::Internal(format!("Failed to poll Apache Iggy messages: {e}")))?;

    Ok(result.messages)
}

async fn with_timeout<F, T>(config: &IggyConfig, future: F) -> std::result::Result<T, IggyError>
where
    F: std::future::Future<Output = std::result::Result<T, IggyError>>,
{
    timeout(Duration::from_millis(config.request_timeout_ms), future)
        .await
        .map_err(|_| IggyError::CannotEstablishConnection)?
}

fn stream_identifier(value: &str) -> Result<iggy::prelude::Identifier> {
    value
        .try_into()
        .map_err(|e| Error::Config(format!("Invalid Apache Iggy identifier '{value}': {e}")))
}

fn validate_config(config: &IggyConfig) -> Result<()> {
    if config.connection_string.trim().is_empty() {
        return Err(Error::Config(
            "Apache Iggy connection_string must not be empty".to_string(),
        ));
    }
    if config.partitions_count == 0 {
        return Err(Error::Config(
            "Apache Iggy partitions_count must be greater than 0".to_string(),
        ));
    }
    if config.partition_id == 0 || config.partition_id > config.partitions_count {
        return Err(Error::Config(format!(
            "Apache Iggy partition_id must be between 1 and partitions_count ({})",
            config.partitions_count
        )));
    }
    if config.poll_batch_size == 0 {
        return Err(Error::Config(
            "Apache Iggy poll_batch_size must be greater than 0".to_string(),
        ));
    }
    Ok(())
}

fn normalize_name(value: &str, fallback: &str) -> String {
    let normalized = value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch.to_ascii_lowercase(),
            _ => '-',
        })
        .collect::<String>();
    let trimmed = normalized.trim_matches(['-', '_']);
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed.to_string()
    }
}

fn to_iggy_error(error: IggyError) -> Error {
    Error::Internal(format!("Apache Iggy error: {error}"))
}
