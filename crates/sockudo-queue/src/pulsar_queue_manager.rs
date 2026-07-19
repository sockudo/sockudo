use crate::ArcJobProcessorFn;
use crate::broker_batch::prepare_default_batch;
use crate::worker_registry::WorkerRegistry;
use async_trait::async_trait;
use futures_util::{StreamExt, future::try_join_all};
use pulsar::consumer::DeadLetterPolicy;
use pulsar::{Authentication, Consumer, Producer, Pulsar, SubType, TokioExecutor};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{PulsarAdapterConfig, QueueReliabilityConfig};
use sockudo_core::queue::{
    QueueBackendKind, QueueCapabilities, QueueInterface, QueueJobId, QueueJobRequest,
};
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, Notify};
use tracing::{error, info};

pub struct PulsarQueueManager {
    client: Pulsar<TokioExecutor>,
    prefix: String,
    producer_cache: Mutex<std::collections::HashMap<String, Arc<Mutex<Producer<TokioExecutor>>>>>,
    shutdown: Arc<Notify>,
    running: Arc<AtomicBool>,
    workers: WorkerRegistry,
    reliability: QueueReliabilityConfig,
}

impl PulsarQueueManager {
    pub async fn new(config: PulsarAdapterConfig) -> Result<Self> {
        Self::new_with_reliability(config, QueueReliabilityConfig::default()).await
    }

    pub async fn new_with_reliability(
        config: PulsarAdapterConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Self> {
        reliability.validate().map_err(Error::Config)?;
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
            producer_cache: Mutex::new(std::collections::HashMap::new()),
            shutdown: Arc::new(Notify::new()),
            running: Arc::new(AtomicBool::new(true)),
            workers: WorkerRegistry::default(),
            reliability,
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

    async fn producer_for(&self, topic: &str) -> Result<Arc<Mutex<Producer<TokioExecutor>>>> {
        if let Some(producer) = self.producer_cache.lock().await.get(topic).cloned() {
            return Ok(producer);
        }
        let producer = self
            .client
            .producer()
            .with_topic(topic)
            .with_name(format!("sockudo-queue-{}", uuid::Uuid::new_v4().simple()))
            .build()
            .await
            .map_err(|e| Error::Queue(format!("Failed to create Pulsar queue producer: {e}")))?;
        let producer = Arc::new(Mutex::new(producer));
        Ok(self
            .producer_cache
            .lock()
            .await
            .entry(topic.to_string())
            .or_insert_with(|| producer.clone())
            .clone())
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
            .with_dead_letter_policy(DeadLetterPolicy {
                max_redeliver_count: self.reliability.max_attempts as usize,
                dead_letter_topic: format!("{topic}-dlq"),
            })
            .with_unacked_message_resend_delay(Some(std::time::Duration::from_millis(
                self.reliability.lease_duration_ms,
            )))
            .with_consumer_name(format!("sockudo-queue-{}", uuid::Uuid::new_v4().simple()))
            .build()
            .await
            .map_err(|e| Error::Queue(format!("Failed to create Pulsar queue consumer: {e}")))
    }

    async fn publish_batch(&self, queue_name: &str, jobs: Vec<JobData>) -> Result<()> {
        if jobs.is_empty() {
            return Ok(());
        }
        let topic = self.topic_name(queue_name);
        let producer = self.producer_for(&topic).await?;

        for chunk in jobs.chunks(self.reliability.max_batch_size) {
            let payloads = chunk
                .iter()
                .map(|data| {
                    sonic_rs::to_vec(data).map_err(|e| {
                        Error::Queue(format!("Failed to serialize Pulsar queue job: {e}"))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let receipts = {
                // Pulsar's producer is mutable; this per-topic lock serializes only
                // construction of its native batch and is released before receipts wait.
                let mut producer = producer.lock().await;
                let receipts = producer.send_all(payloads).await.map_err(|e| {
                    Error::Queue(format!("Failed to enqueue Pulsar queue batch: {e}"))
                })?;
                producer.send_batch().await.map_err(|e| {
                    Error::Queue(format!("Failed to flush Pulsar queue batch: {e}"))
                })?;
                receipts
            };
            try_join_all(receipts)
                .await
                .map_err(|e| Error::Queue(format!("Failed to publish Pulsar queue batch: {e}")))?;
        }
        Ok(())
    }
}

#[async_trait]
impl QueueInterface for PulsarQueueManager {
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
        let subscription = self.subscription_name(queue_name);
        let mut consumer = self.build_consumer(&topic, &subscription).await?;
        let callback: ArcJobProcessorFn = Arc::from(callback);
        let shutdown = self.shutdown.clone();
        let running = self.running.clone();
        let prefetch = self.reliability.worker_prefetch;

        self.workers.spawn(async move {
            let mut in_flight = tokio::task::JoinSet::new();
            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }
                if in_flight.len() >= prefetch {
                    if let Some(completed) = in_flight.join_next().await {
                        complete_pulsar_delivery(&mut consumer, completed).await;
                    }
                    continue;
                }
                let message = tokio::select! {
                    _ = shutdown.notified() => break,
                    completed = in_flight.join_next(), if !in_flight.is_empty() => {
                        if let Some(completed) = completed {
                            complete_pulsar_delivery(&mut consumer, completed).await;
                        }
                        continue;
                    }
                    message = consumer.next() => message,
                };
                let Some(message) = message else {
                    break;
                };
                match message {
                    Ok(message) => match sonic_rs::from_slice::<JobData>(&message.payload.data) {
                        Ok(job) => {
                            let callback = callback.clone();
                            in_flight.spawn(async move {
                                let succeeded = callback(job).await.is_ok();
                                (message, succeeded)
                            });
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
            while let Some(completed) = in_flight.join_next().await {
                complete_pulsar_delivery(&mut consumer, completed).await;
            }
            info!("Pulsar queue consumer stopped");
        });

        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        self.shutdown.notify_waiters();
        self.workers
            .shutdown(std::time::Duration::from_millis(
                self.reliability.shutdown_timeout_ms,
            ))
            .await;
        Ok(())
    }

    async fn check_health(&self) -> Result<()> {
        let topic = self.topic_name("health");
        let producer = self.producer_for(&topic).await?;
        producer
            .lock()
            .await
            .check_connection()
            .await
            .map_err(|e| Error::Queue(format!("Pulsar queue health check failed: {e}")))
    }

    fn backend(&self) -> QueueBackendKind {
        QueueBackendKind::Pulsar
    }

    fn capabilities(&self) -> QueueCapabilities {
        QueueCapabilities {
            consume: true,
            acknowledgements: true,
            delayed_delivery: false,
            retries: true,
            dead_letter: true,
            deduplication: false,
            leasing: true,
            durable: true,
            batch_enqueue: true,
            observable_lag: false,
        }
    }
}

async fn complete_pulsar_delivery(
    consumer: &mut Consumer<Vec<u8>, TokioExecutor>,
    completed: std::result::Result<
        (pulsar::consumer::Message<Vec<u8>>, bool),
        tokio::task::JoinError,
    >,
) {
    let (message, succeeded) = match completed {
        Ok(result) => result,
        Err(error) => {
            error!("Pulsar queue callback task failed: {error}");
            return;
        }
    };
    let result = if succeeded {
        consumer.ack(&message).await
    } else {
        consumer.nack(&message).await
    };
    if let Err(error) = result {
        error!("Failed to settle Pulsar queue job: {error}");
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
