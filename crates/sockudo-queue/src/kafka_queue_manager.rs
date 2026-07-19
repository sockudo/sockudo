use crate::ArcJobProcessorFn;
use crate::broker_batch::prepare_default_batch;
use crate::worker_registry::WorkerRegistry;
use async_trait::async_trait;
use dashmap::DashSet;
use futures_util::{StreamExt, TryStreamExt, stream};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::Message as KafkaMessage;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{KafkaAdapterConfig, QueueReliabilityConfig};
use sockudo_core::queue::{
    QueueBackendKind, QueueCapabilities, QueueInterface, QueueJobId, QueueJobRequest,
};
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{error, info, warn};

pub struct KafkaQueueManager {
    producer: FutureProducer,
    admin: AdminClient<DefaultClientContext>,
    config: KafkaAdapterConfig,
    prefix: String,
    shutdown: Arc<Notify>,
    running: Arc<AtomicBool>,
    workers: WorkerRegistry,
    provisioned_topics: DashSet<String>,
    reliability: QueueReliabilityConfig,
}

impl KafkaQueueManager {
    pub async fn new(config: KafkaAdapterConfig) -> Result<Self> {
        Self::new_with_reliability(config, QueueReliabilityConfig::default()).await
    }

    pub async fn new_with_reliability(
        config: KafkaAdapterConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Self> {
        reliability.validate().map_err(Error::Config)?;
        let producer: FutureProducer = kafka_config(&config)
            .create()
            .map_err(|e| Error::Queue(format!("Failed to create Kafka queue producer: {e}")))?;
        let admin = kafka_config(&config)
            .create()
            .map_err(|e| Error::Queue(format!("Failed to create Kafka admin client: {e}")))?;
        let prefix = normalize_topic_prefix(&config.prefix);
        Ok(Self {
            producer,
            admin,
            config,
            prefix,
            shutdown: Arc::new(Notify::new()),
            running: Arc::new(AtomicBool::new(true)),
            workers: WorkerRegistry::default(),
            provisioned_topics: DashSet::new(),
            reliability,
        })
    }

    fn topic_name(&self, queue_name: &str) -> String {
        format!(
            "{}.queue.{}",
            self.prefix,
            normalize_topic_prefix(queue_name)
        )
    }

    fn group_id(&self, queue_name: &str) -> String {
        format!(
            "{}.queue-workers.{}",
            self.prefix,
            normalize_topic_prefix(queue_name)
        )
    }

    async fn ensure_topic(&self, topic: &str) -> Result<()> {
        if self.provisioned_topics.contains(topic) {
            return Ok(());
        }
        let topics = [NewTopic::new(topic, 1, TopicReplication::Fixed(1))];
        let results = self
            .admin
            .create_topics(&topics, &AdminOptions::new())
            .await
            .map_err(|e| Error::Queue(format!("Failed to create Kafka queue topic: {e}")))?;
        for result in results {
            match result {
                Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
                Err((name, code)) => {
                    return Err(Error::Queue(format!(
                        "Failed to ensure Kafka queue topic '{name}': {code:?}"
                    )));
                }
            }
        }
        self.provisioned_topics.insert(topic.to_string());
        Ok(())
    }

    async fn publish_batch(&self, queue_name: &str, jobs: Vec<JobData>) -> Result<()> {
        if jobs.is_empty() {
            return Ok(());
        }
        let topic = self.topic_name(queue_name);
        self.ensure_topic(&topic).await?;
        let timeout = Duration::from_millis(self.config.request_timeout_ms);

        for chunk in jobs.chunks(self.reliability.max_batch_size) {
            let payloads = chunk
                .iter()
                .map(|data| {
                    sonic_rs::to_vec(data).map_err(|e| {
                        Error::Queue(format!("Failed to serialize Kafka queue job: {e}"))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let concurrency = payloads.len().min(self.reliability.worker_prefetch).max(1);
            stream::iter(payloads)
                .map(|payload| {
                    let producer = self.producer.clone();
                    let topic = topic.clone();
                    async move {
                        producer
                            .send(
                                FutureRecord::to(&topic).key("").payload(&payload),
                                Timeout::After(timeout),
                            )
                            .await
                            .map(|_| ())
                            .map_err(|(e, _)| {
                                Error::Queue(format!("Failed to publish Kafka queue job: {e}"))
                            })
                    }
                })
                .buffer_unordered(concurrency)
                .try_collect::<Vec<_>>()
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl QueueInterface for KafkaQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> Result<()> {
        self.publish_batch(queue_name, vec![data]).await
    }

    async fn add_batch_to_queue(&self, queue_name: &str, data: Vec<JobData>) -> Result<()> {
        self.publish_batch(queue_name, data).await
    }

    async fn enqueue_batch(
        &self,
        queue_name: &str,
        jobs: Vec<QueueJobRequest>,
    ) -> Result<Vec<QueueJobId>> {
        let prepared = prepare_default_batch(self.backend(), jobs)?;
        self.publish_batch(queue_name, prepared.data).await?;
        Ok(prepared.ids)
    }

    async fn process_queue(&self, queue_name: &str, callback: JobProcessorFnAsync) -> Result<()> {
        let topic = self.topic_name(queue_name);
        self.ensure_topic(&topic).await?;

        let consumer: StreamConsumer = kafka_config(&self.config)
            .set("group.id", self.group_id(queue_name))
            .set("enable.auto.commit", "false")
            .create()
            .map_err(|e| Error::Queue(format!("Failed to create Kafka queue consumer: {e}")))?;
        consumer
            .subscribe(&[topic.as_str()])
            .map_err(|e| Error::Queue(format!("Failed to subscribe Kafka queue consumer: {e}")))?;

        let callback: ArcJobProcessorFn = Arc::from(callback);
        let shutdown = self.shutdown.clone();
        let running = self.running.clone();
        let producer = self.producer.clone();
        let dead_letter_topic = format!("{topic}.dlq");
        self.ensure_topic(&dead_letter_topic).await?;
        let request_timeout = self.config.request_timeout_ms;
        let max_attempts = self.reliability.max_attempts;
        let retry_base_delay_ms = self.reliability.retry_base_delay_ms;
        let retry_max_delay_ms = self.reliability.retry_max_delay_ms;

        self.workers.spawn(async move {
            let mut stream = consumer.stream();
            loop {
                if !running.load(Ordering::Relaxed) {
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
                        if let Some(payload) = message.payload() {
                            match sonic_rs::from_slice::<JobData>(payload) {
                                Ok(job) => {
                                    let mut succeeded = false;
                                    for attempt in 1_u32..=max_attempts {
                                        if callback(job.clone()).await.is_ok() {
                                            succeeded = true;
                                            break;
                                        }
                                        if attempt < max_attempts {
                                            tokio::time::sleep(Duration::from_millis(
                                                retry_base_delay_ms
                                                    .saturating_mul(1_u64 << attempt.saturating_sub(1).min(63))
                                                    .min(retry_max_delay_ms),
                                            ))
                                            .await;
                                        }
                                    }
                                    if !succeeded {
                                        let dlq_result = producer
                                            .send(
                                                FutureRecord::to(&dead_letter_topic)
                                                    .key("")
                                                    .payload(payload),
                                                Timeout::After(Duration::from_millis(
                                                    request_timeout,
                                                )),
                                            )
                                            .await;
                                        if let Err((e, _)) = dlq_result {
                                            error!("Failed to publish Kafka dead-letter job: {}", e);
                                            continue;
                                        }
                                        warn!("Kafka queue job moved to dead-letter topic after {max_attempts} attempts");
                                    }
                                    if let Err(e) =
                                        consumer.commit_message(&message, CommitMode::Sync)
                                    {
                                        error!("Failed to commit Kafka queue message: {}", e);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to deserialize Kafka queue job: {}", e);
                                    let dlq_result = producer
                                        .send(
                                            FutureRecord::to(&dead_letter_topic)
                                                .key("")
                                                .payload(payload),
                                            Timeout::After(Duration::from_millis(request_timeout)),
                                        )
                                        .await;
                                    if dlq_result.is_ok() {
                                        let _ = consumer
                                            .commit_message(&message, CommitMode::Sync);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Kafka queue consumer error: {}", e);
                        break;
                    }
                }
            }
            info!("Kafka queue consumer stopped");
        });

        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        self.shutdown.notify_waiters();
        self.workers
            .shutdown(Duration::from_millis(self.reliability.shutdown_timeout_ms))
            .await;
        Ok(())
    }

    async fn check_health(&self) -> Result<()> {
        self.producer
            .client()
            .fetch_metadata(
                None,
                Timeout::After(Duration::from_millis(self.config.request_timeout_ms)),
            )
            .map(|_| ())
            .map_err(|e| Error::Queue(format!("Kafka queue health check failed: {e}")))
    }

    fn backend(&self) -> QueueBackendKind {
        QueueBackendKind::Kafka
    }

    fn capabilities(&self) -> QueueCapabilities {
        QueueCapabilities {
            consume: true,
            acknowledgements: true,
            delayed_delivery: false,
            retries: true,
            dead_letter: true,
            deduplication: false,
            leasing: false,
            durable: true,
            batch_enqueue: true,
            observable_lag: false,
        }
    }
}

fn kafka_config(config: &KafkaAdapterConfig) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", config.brokers.join(","))
        .set("socket.timeout.ms", config.request_timeout_ms.to_string())
        .set("message.timeout.ms", config.request_timeout_ms.to_string())
        .set("auto.offset.reset", "earliest")
        .set("enable.idempotence", "true")
        .set("acks", "all");

    if let Some(protocol) = &config.security_protocol {
        cfg.set("security.protocol", protocol);
    }
    if let Some(mechanism) = &config.sasl_mechanism {
        cfg.set("sasl.mechanisms", mechanism);
    }
    if let Some(username) = &config.sasl_username {
        cfg.set("sasl.username", username);
    }
    if let Some(password) = &config.sasl_password {
        cfg.set("sasl.password", password);
    }

    cfg
}

fn normalize_topic_prefix(value: &str) -> String {
    let normalized = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    normalized
        .trim_matches('-')
        .trim_matches('.')
        .to_string()
        .chars()
        .take(200)
        .collect()
}
