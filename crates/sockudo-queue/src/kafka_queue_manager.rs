use crate::ArcJobProcessorFn;
use async_trait::async_trait;
use futures_util::StreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::Message as KafkaMessage;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::KafkaAdapterConfig;
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{error, info, warn};

pub struct KafkaQueueManager {
    producer: FutureProducer,
    config: KafkaAdapterConfig,
    prefix: String,
    shutdown: Arc<Notify>,
    running: Arc<AtomicBool>,
}

impl KafkaQueueManager {
    pub async fn new(config: KafkaAdapterConfig) -> Result<Self> {
        let producer: FutureProducer = kafka_config(&config)
            .create()
            .map_err(|e| Error::Queue(format!("Failed to create Kafka queue producer: {e}")))?;
        let prefix = normalize_topic_prefix(&config.prefix);
        Ok(Self {
            producer,
            config,
            prefix,
            shutdown: Arc::new(Notify::new()),
            running: Arc::new(AtomicBool::new(true)),
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
        let admin: AdminClient<DefaultClientContext> = kafka_config(&self.config)
            .create()
            .map_err(|e| Error::Queue(format!("Failed to create Kafka admin client: {e}")))?;
        let topics = [NewTopic::new(topic, 1, TopicReplication::Fixed(1))];
        let results = admin
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
        Ok(())
    }
}

#[async_trait]
impl QueueInterface for KafkaQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> Result<()> {
        let topic = self.topic_name(queue_name);
        self.ensure_topic(&topic).await?;
        let payload = sonic_rs::to_vec(&data)
            .map_err(|e| Error::Queue(format!("Failed to serialize Kafka queue job: {e}")))?;

        self.producer
            .send(
                FutureRecord::to(&topic).key("").payload(&payload),
                Timeout::After(Duration::from_millis(self.config.request_timeout_ms)),
            )
            .await
            .map_err(|(e, _)| Error::Queue(format!("Failed to publish Kafka queue job: {e}")))?;

        Ok(())
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

        tokio::spawn(async move {
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
                                    if callback(job).await.is_ok() {
                                        if let Err(e) =
                                            consumer.commit_message(&message, CommitMode::Async)
                                        {
                                            error!("Failed to commit Kafka queue message: {}", e);
                                        }
                                    } else {
                                        warn!(
                                            "Kafka queue job failed; offset not committed for retry"
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to deserialize Kafka queue job: {}", e);
                                    let _ = consumer.commit_message(&message, CommitMode::Async);
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
}

fn kafka_config(config: &KafkaAdapterConfig) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", config.brokers.join(","))
        .set("socket.timeout.ms", config.request_timeout_ms.to_string())
        .set("message.timeout.ms", config.request_timeout_ms.to_string())
        .set("auto.offset.reset", "earliest");

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
