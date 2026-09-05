use crate::ArcJobProcessorFn;
use crate::broker_batch::prepare_default_batch;
use crate::worker_registry::WorkerRegistry;
use async_trait::async_trait;
use dashmap::DashSet;
use futures_util::{StreamExt, TryStreamExt, stream};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer};
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

struct QueueConsumerContext;
impl ClientContext for QueueConsumerContext {}
impl ConsumerContext for QueueConsumerContext {
    fn commit_callback(
        &self,
        result: rdkafka::error::KafkaResult<()>,
        _: &rdkafka::TopicPartitionList,
    ) {
        if let Err(error) = result {
            warn!(error = %error, "kafka queue offset commit failed");
        }
    }
}

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
        if !(1..=256).contains(&config.partitions)
            || (config.partitions > 1 && config.topic_epoch.as_deref().is_none_or(str::is_empty))
        {
            return Err(Error::Config("partitioned Kafka queues require positive partitions and a fresh topic_epoch; drain the old generation before cutover".into()));
        }
        if config.topic_epoch.as_deref().is_some_and(|epoch| {
            epoch.is_empty()
                || epoch.len() > 64
                || !epoch
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        }) {
            return Err(Error::Config(
                "Kafka topic_epoch must be 1-64 ASCII letters, digits, hyphens or underscores"
                    .into(),
            ));
        }
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
        let topic = format!(
            "{}.queue.{}",
            self.prefix,
            normalize_topic_prefix(queue_name)
        );
        match self.config.topic_epoch.as_deref() {
            Some(epoch) => format!("{topic}.epoch.{}", normalize_topic_prefix(epoch)),
            None => topic,
        }
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
        let topics = [NewTopic::new(
            topic,
            self.config.partitions,
            TopicReplication::Fixed(1),
        )];
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
                    sonic_rs::to_vec(data)
                        .map(|payload| {
                            (
                                if self.config.topic_epoch.is_some() {
                                    data.app_id.clone()
                                } else {
                                    String::new()
                                },
                                payload,
                            )
                        })
                        .map_err(|e| {
                            Error::Queue(format!("Failed to serialize Kafka queue job: {e}"))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            let concurrency = payloads.len().min(self.reliability.worker_prefetch).max(1);
            stream::iter(payloads)
                .map(|(key, payload)| {
                    let producer = self.producer.clone();
                    let topic = topic.clone();
                    async move {
                        producer
                            .send(
                                FutureRecord::to(&topic).key(&key).payload(&payload),
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

        let callback: ArcJobProcessorFn = Arc::from(callback);
        for _ in 0..64_usize.min(self.config.partitions as usize).max(1) {
            let consumer: StreamConsumer<QueueConsumerContext> = kafka_config(&self.config)
                .set("group.id", self.group_id(queue_name))
                .set("enable.auto.commit", "false")
                .set("enable.auto.offset.store", "false")
                .set(
                    "queued.min.messages",
                    self.reliability.worker_prefetch.to_string(),
                )
                .set("queued.max.messages.kbytes", "16384")
                .create_with_context(QueueConsumerContext)
                .map_err(|e| Error::Queue(format!("Failed to create Kafka queue consumer: {e}")))?;
            consumer.subscribe(&[topic.as_str()]).map_err(|e| {
                Error::Queue(format!("Failed to subscribe Kafka queue consumer: {e}"))
            })?;

            let callback = Arc::clone(&callback);
            let shutdown = self.shutdown.clone();
            let running = self.running.clone();
            let producer = self.producer.clone();
            let dead_letter_topic = format!("{topic}.dlq");
            self.ensure_topic(&dead_letter_topic).await?;
            let request_timeout = self.config.request_timeout_ms;
            let max_attempts = self.reliability.max_attempts;
            let reliability = self.reliability.clone();

            self.workers.spawn(async move {
            let mut stream = consumer.stream();
            loop {
                if !running.load(Ordering::Relaxed) { break; }
                let message = tokio::select! { _ = shutdown.notified() => break, message = stream.next() => message };
                let Some(message) = message else { break; };
                let message = match message {
                    Ok(message) => message,
                    Err(error) => { error!(error = %error, "kafka queue consumer error"); break; }
                };
                let payload = message.payload().unwrap_or_default();
                let mut succeeded = false;
                match sonic_rs::from_slice::<JobData>(payload) {
                    Ok(job) => {
                        for attempt in 1..=max_attempts {
                            match callback(job.clone()).await {
                                Ok(()) => { succeeded = true; break; }
                                Err(error) => warn!(error = %error, attempt, max_attempts, "kafka queue processor failed"),
                            }
                            if attempt < max_attempts { tokio::time::sleep(crate::broker_retry_delay(&reliability, attempt)).await; }
                        }
                    }
                    Err(error) => error!(error = %error, "failed to deserialize kafka queue job"),
                }
                if !succeeded {
                    // Do not consume/commit a later offset until the failed source has
                    // a confirmed durable successor. Retry transfer stays in this
                    // partition-owning worker and is cancelled without committing.
                    loop {
                        if !running.load(Ordering::Relaxed) { return; }
                        let result = tokio::select! {
                            _ = shutdown.notified() => return,
                            result = producer.send(FutureRecord::to(&dead_letter_topic).key("").payload(payload), Timeout::After(Duration::from_millis(request_timeout))) => result,
                        };
                        match result {
                            Ok(_) => break,
                            Err((error, _)) => {
                                error!(error = %error, "failed to publish kafka dead-letter job");
                                tokio::select! { _ = shutdown.notified() => return, _ = tokio::time::sleep(crate::broker_retry_delay(&reliability, 1)) => {} }
                            }
                        }
                    }
                }
                if let Err(error) = consumer.commit_message(&message, CommitMode::Async) {
                    error!(error = %error, "failed to enqueue kafka queue offset commit");
                }
            }
            info!("kafka queue consumer stopped");
        });
        }

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
