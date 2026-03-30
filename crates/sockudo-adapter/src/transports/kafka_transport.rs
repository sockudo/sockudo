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
use sockudo_core::options::KafkaAdapterConfig;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

pub struct KafkaTransport {
    producer: FutureProducer,
    broadcast_topic: String,
    request_topic: String,
    response_topic: String,
    listener_group_id: String,
    config: KafkaAdapterConfig,
    shutdown: Arc<Notify>,
    is_running: Arc<AtomicBool>,
    owner_count: Arc<AtomicUsize>,
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

        let prefix = normalize_topic_prefix(&config.prefix);
        let broadcast_topic = format!("{prefix}.broadcast");
        let request_topic = format!("{prefix}.requests");
        let response_topic = format!("{prefix}.responses");
        let listener_group_id = format!("{prefix}-{}", uuid::Uuid::new_v4().simple());

        let admin: AdminClient<DefaultClientContext> = kafka_config(&config)
            .create()
            .map_err(|e| Error::Internal(format!("Failed to create Kafka admin client: {e}")))?;
        ensure_topics(&admin, [&broadcast_topic, &request_topic, &response_topic]).await?;

        let producer: FutureProducer = kafka_config(&config)
            .create()
            .map_err(|e| Error::Internal(format!("Failed to create Kafka producer: {e}")))?;

        info!(
            "Kafka transport initialized with topics: {}, {}, {}",
            broadcast_topic, request_topic, response_topic
        );

        Ok(Self {
            producer,
            broadcast_topic,
            request_topic,
            response_topic,
            listener_group_id,
            config,
            shutdown: Arc::new(Notify::new()),
            is_running: Arc::new(AtomicBool::new(true)),
            owner_count: Arc::new(AtomicUsize::new(1)),
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        publish_message(&self.producer, &self.broadcast_topic, message).await
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        publish_message(&self.producer, &self.request_topic, request).await
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        publish_message(&self.producer, &self.response_topic, response).await
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        self.spawn_consumer(
            self.broadcast_topic.clone(),
            "broadcast",
            handlers.on_broadcast.clone(),
        )?;

        self.spawn_request_consumer(self.request_topic.clone(), handlers.on_request.clone())?;

        self.spawn_consumer(
            self.response_topic.clone(),
            "response",
            handlers.on_response.clone(),
        )?;

        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        Ok(self.config.nodes_number.unwrap_or(2) as usize)
    }

    async fn check_health(&self) -> Result<()> {
        self.producer
            .client()
            .fetch_metadata(
                None,
                Timeout::After(Duration::from_millis(self.config.request_timeout_ms)),
            )
            .map(|_| ())
            .map_err(|e| Error::Internal(format!("Kafka health check failed: {e}")))
    }
}

impl KafkaTransport {
    fn spawn_consumer<T>(
        &self,
        topic: String,
        kind: &'static str,
        handler: Arc<
            dyn Fn(T) -> crate::horizontal_transport::BoxFuture<'static, ()> + Send + Sync,
        >,
    ) -> Result<()>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let consumer = create_consumer(
            &self.config,
            &format!("{}-{kind}", self.listener_group_id),
            &[topic.as_str()],
        )?;
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();

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
                            Ok(payload) => handler(payload).await,
                            Err(error) => warn!("Failed to parse Kafka {kind} payload: {}", error),
                        }
                    }
                    Err(error) => {
                        error!("Kafka {kind} consumer error: {}", error);
                    }
                }
            }
            warn!("Kafka {kind} consumer loop ended");
        });

        Ok(())
    }

    fn spawn_request_consumer(
        &self,
        topic: String,
        handler: Arc<
            dyn Fn(
                    RequestBody,
                )
                    -> crate::horizontal_transport::BoxFuture<'static, Result<ResponseBody>>
                + Send
                + Sync,
        >,
    ) -> Result<()> {
        let consumer = create_consumer(
            &self.config,
            &format!("{}-request", self.listener_group_id),
            &[topic.as_str()],
        )?;
        let producer = self.producer.clone();
        let response_topic = self.response_topic.clone();
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();

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
                            Ok(request) => match handler(request).await {
                                Ok(response) => {
                                    if let Err(error) =
                                        publish_message(&producer, &response_topic, &response).await
                                    {
                                        warn!("Failed to publish Kafka response: {}", error);
                                    }
                                }
                                Err(error) => {
                                    warn!("Kafka request handler failed: {}", error);
                                }
                            },
                            Err(error) => warn!("Failed to parse Kafka request payload: {}", error),
                        }
                    }
                    Err(error) => {
                        error!("Kafka request consumer error: {}", error);
                    }
                }
            }
            warn!("Kafka request consumer loop ended");
        });

        Ok(())
    }
}

impl Clone for KafkaTransport {
    fn clone(&self) -> Self {
        self.owner_count.fetch_add(1, Ordering::Relaxed);
        Self {
            producer: self.producer.clone(),
            broadcast_topic: self.broadcast_topic.clone(),
            request_topic: self.request_topic.clone(),
            response_topic: self.response_topic.clone(),
            listener_group_id: self.listener_group_id.clone(),
            config: self.config.clone(),
            shutdown: self.shutdown.clone(),
            is_running: self.is_running.clone(),
            owner_count: self.owner_count.clone(),
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
) -> Result<()> {
    let payload = sonic_rs::to_vec(message)
        .map_err(|e| Error::Other(format!("Failed to serialize Kafka message: {e}")))?;

    producer
        .send(
            FutureRecord::to(topic)
                .key("sockudo")
                .payload(payload.as_slice()),
            Timeout::After(Duration::from_millis(5_000)),
        )
        .await
        .map_err(|(e, _)| Error::Internal(format!("Failed to publish Kafka message: {e}")))?;

    debug!("Published Kafka message to topic {}", topic);
    Ok(())
}

async fn ensure_topics(
    admin: &AdminClient<DefaultClientContext>,
    topics: [&String; 3],
) -> Result<()> {
    let new_topics: Vec<_> = topics
        .iter()
        .map(|topic| NewTopic::new(topic.as_str(), 1, TopicReplication::Fixed(1)))
        .collect();
    let topic_refs: Vec<_> = new_topics.iter().collect();

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
    config: &KafkaAdapterConfig,
    group_id: &str,
    topics: &[&str],
) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = kafka_config(config)
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
