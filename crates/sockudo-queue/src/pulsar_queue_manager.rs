use crate::ArcJobProcessorFn;
use async_trait::async_trait;
use futures_util::StreamExt;
use pulsar::{Authentication, Consumer, Producer, Pulsar, SubType, TokioExecutor};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::PulsarAdapterConfig;
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, Notify};
use tracing::{error, info, warn};

pub struct PulsarQueueManager {
    client: Pulsar<TokioExecutor>,
    prefix: String,
    producer_cache: Arc<Mutex<std::collections::HashMap<String, Producer<TokioExecutor>>>>,
    shutdown: Arc<Notify>,
    running: Arc<AtomicBool>,
}

impl PulsarQueueManager {
    pub async fn new(config: PulsarAdapterConfig) -> Result<Self> {
        let mut builder = Pulsar::builder(config.url.clone(), TokioExecutor);
        if let Some(token) = config.token.as_ref() {
            builder = builder.with_auth(Authentication {
                name: "token".to_string(),
                data: token.clone().into_bytes(),
            });
        }
        let client = builder
            .build()
            .await
            .map_err(|e| Error::Queue(format!("Failed to connect to Pulsar queue broker: {e}")))?;

        Ok(Self {
            client,
            prefix: normalize_topic_prefix(&config.prefix),
            producer_cache: Arc::new(Mutex::new(std::collections::HashMap::new())),
            shutdown: Arc::new(Notify::new()),
            running: Arc::new(AtomicBool::new(true)),
        })
    }

    fn topic_name(&self, queue_name: &str) -> String {
        format!(
            "{}-queue-{}",
            self.prefix,
            normalize_topic_prefix(queue_name)
        )
    }

    fn subscription_name(&self, queue_name: &str) -> String {
        format!(
            "{}-queue-workers-{}",
            self.prefix,
            normalize_topic_prefix(queue_name)
        )
    }

    async fn producer_for(&self, topic: &str) -> Result<Producer<TokioExecutor>> {
        let mut cache = self.producer_cache.lock().await;
        if let Some(producer) = cache.remove(topic) {
            return Ok(producer);
        }
        self.client
            .producer()
            .with_topic(topic)
            .with_name(format!("sockudo-queue-{}", uuid::Uuid::new_v4().simple()))
            .build()
            .await
            .map_err(|e| Error::Queue(format!("Failed to create Pulsar queue producer: {e}")))
    }

    async fn build_consumer(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Result<Consumer<Vec<u8>, TokioExecutor>> {
        self.client
            .consumer()
            .with_topic(topic)
            .with_subscription(subscription)
            .with_subscription_type(SubType::Shared)
            .with_consumer_name(format!("sockudo-queue-{}", uuid::Uuid::new_v4().simple()))
            .build()
            .await
            .map_err(|e| Error::Queue(format!("Failed to create Pulsar queue consumer: {e}")))
    }
}

#[async_trait]
impl QueueInterface for PulsarQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> Result<()> {
        let topic = self.topic_name(queue_name);
        let payload = sonic_rs::to_vec(&data)
            .map_err(|e| Error::Queue(format!("Failed to serialize Pulsar queue job: {e}")))?;
        let mut producer = self.producer_for(&topic).await?;
        producer
            .send_non_blocking(payload)
            .await
            .map_err(|e| Error::Queue(format!("Failed to enqueue Pulsar queue job: {e}")))?;
        producer
            .send_batch()
            .await
            .map_err(|e| Error::Queue(format!("Failed to publish Pulsar queue job: {e}")))?;
        self.producer_cache.lock().await.insert(topic, producer);
        Ok(())
    }

    async fn process_queue(&self, queue_name: &str, callback: JobProcessorFnAsync) -> Result<()> {
        let topic = self.topic_name(queue_name);
        let subscription = self.subscription_name(queue_name);
        let mut consumer = self.build_consumer(&topic, &subscription).await?;
        let callback: ArcJobProcessorFn = Arc::from(callback);
        let shutdown = self.shutdown.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }
                let message = tokio::select! {
                    _ = shutdown.notified() => break,
                    message = consumer.next() => message,
                };
                let Some(message) = message else {
                    break;
                };
                match message {
                    Ok(message) => match sonic_rs::from_slice::<JobData>(&message.payload.data) {
                        Ok(job) => {
                            if callback(job).await.is_ok() {
                                if let Err(e) = consumer.ack(&message).await {
                                    error!("Failed to ack Pulsar queue job: {}", e);
                                }
                            } else {
                                warn!("Pulsar queue job failed; leaving message unacked for retry");
                            }
                        }
                        Err(e) => {
                            error!("Failed to deserialize Pulsar queue job: {}", e);
                            let _ = consumer.ack(&message).await;
                        }
                    },
                    Err(e) => {
                        error!("Pulsar queue consumer error: {}", e);
                        break;
                    }
                }
            }
            info!("Pulsar queue consumer stopped");
        });

        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        self.shutdown.notify_waiters();
        Ok(())
    }

    async fn check_health(&self) -> Result<()> {
        let topic = self.topic_name("health");
        let producer = self.producer_for(&topic).await?;
        producer
            .check_connection()
            .await
            .map_err(|e| Error::Queue(format!("Pulsar queue health check failed: {e}")))
    }
}

fn normalize_topic_prefix(prefix: &str) -> String {
    prefix
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
}
