use super::dispatch::{OrderedDispatcher, validate_frame_size};
use crate::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::horizontal_transport::{HorizontalTransport, TransportConfig, TransportHandlers};
use async_trait::async_trait;
use futures_util::StreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::Message as KafkaMessage;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use sockudo_core::error::{Error, Result};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::KafkaAdapterConfig;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{error, info, trace, warn};

pub struct KafkaTransport {
    producer: FutureProducer,
    client_config: ClientConfig,
    broadcast_topic: String,
    request_topic: String,
    response_topic: String,
    broadcast_group_id: String,
    request_group_id: String,
    response_group_id: String,
    config: KafkaAdapterConfig,
    metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
    shutdown: Arc<Notify>,
    is_running: Arc<AtomicBool>,
    owner_count: Arc<AtomicUsize>,
    health_admission: Arc<tokio::sync::Semaphore>,
}

impl TransportConfig for KafkaAdapterConfig {
    fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }

    fn prefix(&self) -> &str {
        &self.prefix
    }
}

#[async_trait]
impl HorizontalTransport for KafkaTransport {
    type Config = KafkaAdapterConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        if config.brokers.is_empty() {
            return Err(Error::Internal(
                "Kafka brokers must not be empty".to_string(),
            ));
        }

        let prefix = topic_generation(&config)?;
        let broadcast_topic = format!("{prefix}.broadcast");
        let request_topic = format!("{prefix}.requests");
        let response_topic = format!("{prefix}.responses");
        let listener_group_prefix = format!("{prefix}-{}", uuid::Uuid::new_v4().simple());
        let broadcast_group_id = format!("{listener_group_prefix}-broadcast");
        let request_group_id = format!("{listener_group_prefix}-request");
        let response_group_id = format!("{listener_group_prefix}-response");
        let client_config = kafka_config(&config);

        let admin: AdminClient<DefaultClientContext> = client_config
            .clone()
            .create()
            .map_err(|e| Error::Internal(format!("Failed to create Kafka admin client: {e}")))?;
        ensure_topics(
            &admin,
            [
                broadcast_topic.as_str(),
                request_topic.as_str(),
                response_topic.as_str(),
            ],
            config.partitions,
        )
        .await?;

        let producer: FutureProducer = client_config
            .clone()
            .create()
            .map_err(|e| Error::Internal(format!("Failed to create Kafka producer: {e}")))?;

        if config.topic_epoch.is_some() {
            let check_producer = producer.clone();
            let expected_partitions = config.partitions as usize;
            let topics = [
                broadcast_topic.clone(),
                request_topic.clone(),
                response_topic.clone(),
            ];
            tokio::task::spawn_blocking(move || -> Result<()> {
                let metadata = check_producer.client().fetch_metadata(None, Timeout::After(Duration::from_secs(5)))
                    .map_err(|error| Error::Internal(format!("Kafka topology validation failed: {error}")))?;
                for topic in topics {
                    let actual = metadata.topics().iter().find(|entry| entry.name() == topic)
                        .map(|entry| entry.partitions().len());
                    if actual != Some(expected_partitions) {
                        return Err(Error::Internal("Kafka topic generation partition count differs; use a fresh drained epoch".into()));
                    }
                }
                Ok(())
            }).await.map_err(|error| Error::Internal(format!("Kafka topology worker failed: {error}")))??;
        }

        info!(
            adapter = "kafka",
            broadcast_topic = %broadcast_topic,
            request_topic = %request_topic,
            response_topic = %response_topic,
            "transport initialized"
        );

        Ok(Self {
            producer,
            client_config,
            broadcast_topic,
            request_topic,
            response_topic,
            broadcast_group_id,
            request_group_id,
            response_group_id,
            config,
            metrics: Arc::new(OnceLock::new()),
            shutdown: Arc::new(Notify::new()),
            is_running: Arc::new(AtomicBool::new(true)),
            owner_count: Arc::new(AtomicUsize::new(1)),
            health_admission: Arc::new(tokio::sync::Semaphore::new(1)),
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        publish_message(
            &self.producer,
            &self.broadcast_topic,
            message,
            partition_key(&self.config, &message.app_id, Some(&message.channel)).as_str(),
        )
        .await
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        publish_message(
            &self.producer,
            &self.request_topic,
            request,
            partition_key(&self.config, &request.app_id, request.channel.as_deref()).as_str(),
        )
        .await
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        publish_message(
            &self.producer,
            &self.response_topic,
            response,
            if self.config.topic_epoch.is_some() {
                &response.request_id
            } else {
                "sockudo"
            },
        )
        .await
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        self.spawn_consumer(
            &self.broadcast_topic,
            &self.broadcast_group_id,
            "broadcast",
            handlers.on_broadcast.clone(),
        )?;

        self.spawn_request_consumer(
            &self.request_topic,
            &self.request_group_id,
            handlers.on_request.clone(),
        )?;

        self.spawn_consumer(
            &self.response_topic,
            &self.response_group_id,
            "response",
            handlers.on_response.clone(),
        )?;

        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        Ok(self.config.nodes_number.unwrap_or(1) as usize)
    }

    async fn check_health(&self) -> Result<()> {
        let permit = Arc::clone(&self.health_admission)
            .try_acquire_owned()
            .map_err(|_| Error::Internal("Kafka health check already in progress".into()))?;
        let producer = self.producer.clone();
        let timeout = Duration::from_millis(self.config.request_timeout_ms);
        tokio::task::spawn_blocking(move || {
            // Keep admission inside the blocking closure: cancelling the caller
            // must not permit another metadata task while this one is running.
            let _permit = permit;
            producer
                .client()
                .fetch_metadata(None, Timeout::After(timeout))
                .map(|_| ())
                .map_err(|error| Error::Internal(format!("Kafka health check failed: {error}")))
        })
        .await
        .map_err(|error| Error::Internal(format!("Kafka health worker failed: {error}")))?
    }

    fn set_metrics(&self, metrics: Arc<dyn MetricsInterface + Send + Sync>) {
        let _ = self.metrics.set(metrics);
    }
}

impl KafkaTransport {
    fn spawn_consumer<T>(
        &self,
        topic: &str,
        group_id: &str,
        kind: &'static str,
        handler: Arc<
            dyn Fn(T) -> crate::horizontal_transport::BoxFuture<'static, ()> + Send + Sync,
        >,
    ) -> Result<()>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let consumer = create_consumer(&self.client_config, group_id, &[topic])?;
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();
        let metrics = self.metrics.clone();

        let dispatcher = OrderedDispatcher::new(16);
        tokio::spawn(async move {
            let mut stream = consumer.stream();
            loop {
                if !is_running.load(Ordering::Relaxed) {
                    break;
                }
                let message = tokio::select! {
                    _ = shutdown.notified() => break,
                    message = stream.next() => message,
                };
                let Some(message) = message else {
                    break;
                };
                match message {
                    Ok(message) => {
                        let Some(payload) = message.payload() else {
                            continue;
                        };

                        match sonic_rs::from_slice::<T>(payload) {
                            Ok(decoded) => {
                                let handler = Arc::clone(&handler);
                                if let Err(error) = dispatcher
                                    .dispatch(
                                        message.partition() as u64,
                                        payload.len(),
                                        Box::pin(async move {
                                            handler(decoded).await;
                                        }),
                                    )
                                    .await
                                {
                                    error!(adapter = "kafka", error = %error, "consumer admission failed");
                                    break;
                                }
                            }
                            Err(error) => {
                                if let Some(metrics) = metrics.get() {
                                    metrics.mark_horizontal_transport_message_dropped("kafka");
                                }
                                warn!(adapter = "kafka", kind = kind, error = %error, "transport message parse failed")
                            }
                        }
                    }
                    Err(error) => {
                        error!(adapter = "kafka", kind = kind, error = %error, "consumer loop error");
                    }
                }
            }
            dispatcher.drain().await;
            warn!(adapter = "kafka", kind = kind, "consumer loop ended");
        });

        Ok(())
    }

    fn spawn_request_consumer(
        &self,
        topic: &str,
        group_id: &str,
        handler: Arc<
            dyn Fn(
                    RequestBody,
                )
                    -> crate::horizontal_transport::BoxFuture<'static, Result<ResponseBody>>
                + Send
                + Sync,
        >,
    ) -> Result<()> {
        let consumer = create_consumer(&self.client_config, group_id, &[topic])?;
        let producer = self.producer.clone();
        let response_topic = self.response_topic.clone();
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();
        let metrics = self.metrics.clone();

        let dispatcher = OrderedDispatcher::new(16);
        tokio::spawn(async move {
            let mut stream = consumer.stream();
            loop {
                if !is_running.load(Ordering::Relaxed) {
                    break;
                }
                let message = tokio::select! {
                    _ = shutdown.notified() => break,
                    message = stream.next() => message,
                };
                let Some(message) = message else {
                    break;
                };
                match message {
                    Ok(message) => {
                        let Some(payload) = message.payload() else {
                            continue;
                        };

                        match sonic_rs::from_slice::<RequestBody>(payload) {
                            Ok(request) => {
                                let handler = Arc::clone(&handler);
                                let producer = producer.clone();
                                let response_topic = response_topic.clone();
                                if let Err(error) = dispatcher.dispatch(message.partition() as u64, payload.len(), Box::pin(async move {
                                    match handler(request).await {
                                Ok(response) => {
                                    if let Err(error) =
                                        publish_message(&producer, &response_topic, &response, &response.request_id).await
                                    {
                                        warn!(adapter = "kafka", error = %error, "response publish failed");
                                    }
                                }
                                Err(
                                    Error::OwnRequestIgnored
                                    | Error::RequestNotForThisNode
                                    | Error::NoResponseNeeded,
                                ) => {}
                                Err(error) => {
                                    warn!(adapter = "kafka", error = %error, "request handler failed");
                                }
                            }
                                })).await {
                                    error!(adapter = "kafka", error = %error, "request ingress admission failed"); break;
                                }
                            }
                            Err(error) => {
                                if let Some(metrics) = metrics.get() {
                                    metrics.mark_horizontal_transport_message_dropped("kafka");
                                }
                                warn!(adapter = "kafka", error = %error, "request payload parse failed")
                            }
                        }
                    }
                    Err(error) => {
                        error!(adapter = "kafka", error = %error, "request consumer loop error");
                    }
                }
            }
            dispatcher.drain().await;
            warn!(adapter = "kafka", "request consumer loop ended");
        });

        Ok(())
    }
}

impl Clone for KafkaTransport {
    fn clone(&self) -> Self {
        self.owner_count.fetch_add(1, Ordering::Relaxed);
        Self {
            producer: self.producer.clone(),
            client_config: self.client_config.clone(),
            broadcast_topic: self.broadcast_topic.clone(),
            request_topic: self.request_topic.clone(),
            response_topic: self.response_topic.clone(),
            broadcast_group_id: self.broadcast_group_id.clone(),
            request_group_id: self.request_group_id.clone(),
            response_group_id: self.response_group_id.clone(),
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            shutdown: self.shutdown.clone(),
            is_running: self.is_running.clone(),
            owner_count: self.owner_count.clone(),
            health_admission: self.health_admission.clone(),
        }
    }
}

impl Drop for KafkaTransport {
    fn drop(&mut self) {
        if self.owner_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.is_running.store(false, Ordering::Relaxed);
            self.shutdown.notify_waiters();
        }
    }
}

async fn publish_message<T: serde::Serialize>(
    producer: &FutureProducer,
    topic: &str,
    message: &T,
    key: &str,
) -> Result<()> {
    let payload = sonic_rs::to_vec(message)
        .map_err(|e| Error::Other(format!("Failed to serialize Kafka message: {e}")))?;

    validate_frame_size(payload.len())?;
    producer
        .send(
            FutureRecord::to(topic).key(key).payload(payload.as_slice()),
            Timeout::After(Duration::from_millis(5_000)),
        )
        .await
        .map_err(|(e, _)| Error::Internal(format!("Failed to publish Kafka message: {e}")))?;

    trace!(adapter = "kafka", topic = %topic, "message published to transport");
    Ok(())
}

async fn ensure_topics(
    admin: &AdminClient<DefaultClientContext>,
    topics: [&str; 3],
    partitions: i32,
) -> Result<()> {
    let new_topics = [
        NewTopic::new(topics[0], partitions, TopicReplication::Fixed(1)),
        NewTopic::new(topics[1], partitions, TopicReplication::Fixed(1)),
        NewTopic::new(topics[2], partitions, TopicReplication::Fixed(1)),
    ];
    let topic_refs = [&new_topics[0], &new_topics[1], &new_topics[2]];

    let results = admin
        .create_topics(topic_refs, &AdminOptions::new())
        .await
        .map_err(|e| Error::Internal(format!("Failed to create Kafka topics: {e}")))?;

    for result in results {
        match result {
            Ok(_) => {}
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((topic, code)) => {
                return Err(Error::Internal(format!(
                    "Failed to ensure Kafka topic '{topic}': {code:?}"
                )));
            }
        }
    }

    Ok(())
}

fn create_consumer(
    client_config: &ClientConfig,
    group_id: &str,
    topics: &[&str],
) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = client_config
        .clone()
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "latest")
        .create()
        .map_err(|e| Error::Internal(format!("Failed to create Kafka consumer: {e}")))?;

    consumer
        .subscribe(topics)
        .map_err(|e| Error::Internal(format!("Failed to subscribe Kafka consumer: {e}")))?;

    Ok(consumer)
}

fn kafka_config(config: &KafkaAdapterConfig) -> ClientConfig {
    let mut client_config = ClientConfig::new();
    client_config
        .set("queued.max.messages.kbytes", "65536")
        .set("queued.min.messages", "64")
        .set("fetch.message.max.bytes", "16777216")
        .set("receive.message.max.bytes", "67108864");
    client_config
        .set("bootstrap.servers", config.brokers.join(","))
        .set("message.timeout.ms", config.request_timeout_ms.to_string());

    if let Some(security_protocol) = config.security_protocol.as_deref() {
        client_config.set("security.protocol", security_protocol);
    }
    if let Some(sasl_mechanism) = config.sasl_mechanism.as_deref() {
        client_config.set("sasl.mechanism", sasl_mechanism);
    }
    if let Some(username) = config.sasl_username.as_deref() {
        client_config.set("sasl.username", username);
    }
    if let Some(password) = config.sasl_password.as_deref() {
        client_config.set("sasl.password", password);
    }

    client_config
}

fn normalize_topic_prefix(value: &str) -> String {
    let normalized: String = value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => ch,
            _ => '-',
        })
        .collect();

    let trimmed = normalized.trim_matches(['.', '-', '_']);
    if trimmed.is_empty() {
        "sockudo".to_string()
    } else {
        trimmed.to_string()
    }
}

fn topic_generation(config: &KafkaAdapterConfig) -> Result<String> {
    if !(1..=256).contains(&config.partitions) {
        return Err(Error::InvalidMessageFormat(
            "Kafka partitions must be between 1 and 256".into(),
        ));
    }
    let prefix = normalize_topic_prefix(&config.prefix);
    match config.topic_epoch.as_deref() {
        Some(epoch)
            if !epoch.is_empty()
                && epoch.len() <= 64
                && epoch
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') =>
        {
            Ok(format!("{prefix}.epoch.{epoch}"))
        }
        Some(_) => Err(Error::InvalidMessageFormat(
            "Kafka topic_epoch must be 1-64 ASCII letters, digits, hyphens or underscores".into(),
        )),
        None if config.partitions == 1 => Ok(prefix),
        None => Err(Error::InvalidMessageFormat(
            "Kafka repartitioning requires a fresh topic_epoch and drained migration".into(),
        )),
    }
}

fn partition_key(config: &KafkaAdapterConfig, app_id: &str, channel: Option<&str>) -> String {
    if config.topic_epoch.is_none() {
        return "sockudo".into();
    }
    format!(
        "{}:{app_id}{}:{}",
        app_id.len(),
        channel.unwrap_or("").len(),
        channel.unwrap_or("")
    )
}

#[cfg(test)]
mod migration_tests {
    use super::*;
    #[test]
    fn legacy_topology_is_unchanged_and_repartitioning_requires_generation() {
        let mut config = KafkaAdapterConfig::default();
        assert_eq!(topic_generation(&config).unwrap(), config.prefix);
        assert_eq!(partition_key(&config, "app", Some("channel")), "sockudo");
        config.partitions = 16;
        assert!(topic_generation(&config).is_err());
        config.topic_epoch = Some("v2".into());
        assert!(topic_generation(&config).unwrap().ends_with(".epoch.v2"));
        assert_ne!(
            partition_key(&config, "ab", Some("c")),
            partition_key(&config, "a", Some("bc"))
        );
    }
}
