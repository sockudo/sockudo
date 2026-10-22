use crate::ArcJobProcessorFn;
use crate::broker_batch::prepare_delayed_batch;
use crate::worker_registry::WorkerRegistry;
use async_trait::async_trait;
use futures_util::{StreamExt, future::try_join_all};
use pulsar::consumer::DeadLetterPolicy;
use pulsar::{Authentication, Consumer, Producer, Pulsar, SubType, TokioExecutor};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{PulsarAdapterConfig, QueueReliabilityConfig};
use sockudo_core::queue::{
    QueueBackendKind, QueueCapabilities, QueueInterface, QueueJobId, QueueJobOptions,
    QueueJobRequest,
};
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, Notify};
use tracing::{error, info, warn};

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

    /// Publishes `(job, delay_ms)` pairs. A non-zero delay sets Pulsar's `deliver_at_time`,
    /// which the broker honours for the Shared subscription `process_queue` uses.
    async fn publish_batch(&self, queue_name: &str, jobs: Vec<(JobData, u64)>) -> Result<()> {
        if jobs.is_empty() {
            return Ok(());
        }
        let topic = self.topic_name(queue_name);
        let producer = self.producer_for(&topic).await?;

        for chunk in jobs.chunks(self.reliability.max_batch_size) {
            let payloads = chunk
                .iter()
                .map(|(data, delay_ms)| {
                    sonic_rs::to_vec(data)
                        .map(|payload| (payload, *delay_ms))
                        .map_err(|e| {
                            Error::Queue(format!("Failed to serialize Pulsar queue job: {e}"))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            let receipts = {
                // Pulsar's producer is mutable; this per-topic lock serializes only
                // the sends and is released before receipts wait. The producer does not
                // batch (batched messages drop `deliver_at_time`), so each send goes out
                // on its own and there is no batch to flush.
                let mut producer = producer.lock().await;
                let mut receipts = Vec::with_capacity(payloads.len());
                for (payload, delay_ms) in payloads {
                    let mut message = producer.create_message().with_content(payload);
                    if delay_ms > 0 {
                        message = message
                            .delay(std::time::Duration::from_millis(delay_ms))
                            .map_err(|e| {
                                Error::Queue(format!("Invalid Pulsar queue job delay: {e}"))
                            })?;
                    }
                    receipts.push(message.send_non_blocking().await.map_err(|e| {
                        Error::Queue(format!("Failed to enqueue Pulsar queue job: {e}"))
                    })?);
                }
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
        self.publish_batch(queue_name, vec![(data, 0)]).await
    }

    async fn add_batch_to_queue(&self, queue_name: &str, data: Vec<JobData>) -> Result<()> {
        self.publish_batch(queue_name, data.into_iter().map(|job| (job, 0)).collect())
            .await
    }

    async fn enqueue(
        &self,
        queue_name: &str,
        data: JobData,
        options: QueueJobOptions,
    ) -> Result<QueueJobId> {
        self.enqueue_batch(queue_name, vec![QueueJobRequest { data, options }])
            .await?
            .pop()
            .ok_or_else(|| Error::Queue("Pulsar enqueue returned no job id".to_string()))
    }

    async fn enqueue_batch(
        &self,
        queue_name: &str,
        jobs: Vec<QueueJobRequest>,
    ) -> Result<Vec<QueueJobId>> {
        let prepared = prepare_delayed_batch(self.backend(), jobs)?;
        self.publish_batch(
            queue_name,
            prepared.data.into_iter().zip(prepared.delays_ms).collect(),
        )
        .await?;
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
                                let succeeded = match callback(job).await {
                                    Ok(()) => true,
                                    Err(_) => {
                                        warn!("pulsar queue processor failed");
                                        false
                                    }
                                };
                                (message, succeeded)
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "failed to deserialize pulsar queue job");
                            if let Err(error) = consumer.ack(&message).await {
                                error!(error = %error, "failed to ack malformed pulsar queue job");
                            }
                        }
                    },
                    Err(e) => {
                        error!(error = %e, "pulsar queue consumer error");
                        break;
                    }
                }
            }
            while let Some(completed) = in_flight.join_next().await {
                complete_pulsar_delivery(&mut consumer, completed).await;
            }
            info!("pulsar queue consumer stopped");
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
            // `deliver_at_time`, honoured for the Shared subscription used by `process_queue`.
            delayed_delivery: true,
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
            error!(error = %error, "pulsar queue callback task failed");
            return;
        }
    };
    let result = if succeeded {
        consumer.ack(&message).await
    } else {
        consumer.nack(&message).await
    };
    if let Err(error) = result {
        error!(error = %error, "failed to settle pulsar queue job");
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[tokio::test]
    #[ignore = "requires SOCKUDO_PULSAR_QUEUE_TEST_URL"]
    async fn immediate_and_delayed_jobs_are_delivered_on_time() {
        let url = std::env::var("SOCKUDO_PULSAR_QUEUE_TEST_URL")
            .expect("SOCKUDO_PULSAR_QUEUE_TEST_URL is required");
        let manager = PulsarQueueManager::new(PulsarAdapterConfig {
            url,
            prefix: format!("sockudo-test-{}", uuid::Uuid::new_v4().simple()),
            ..PulsarAdapterConfig::default()
        })
        .await
        .expect("Pulsar queue manager should connect");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        manager
            .process_queue(
                "jobs",
                Box::new(move |job: JobData| {
                    let tx = tx.clone();
                    Box::pin(async move {
                        let _ = tx.send((job.job_id, Instant::now()));
                        Ok(())
                    })
                }),
            )
            .await
            .expect("consumer should start");
        // The Shared subscription exists once the consumer is built; jobs published
        // before it would not be delivered to it.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let started = Instant::now();
        let delayed = manager
            .enqueue(
                "jobs",
                JobData::default(),
                QueueJobOptions {
                    job_id: Some("delayed".to_string()),
                    delay_ms: 3_000,
                    ..QueueJobOptions::default()
                },
            )
            .await
            .expect("a delayed enqueue should succeed");
        assert_eq!(delayed.0, "delayed");
        manager
            .enqueue(
                "jobs",
                JobData::default(),
                QueueJobOptions {
                    job_id: Some("immediate".to_string()),
                    ..QueueJobOptions::default()
                },
            )
            .await
            .expect("an immediate enqueue should succeed");
        manager
            .add_to_queue(
                "jobs",
                JobData {
                    job_id: Some("legacy".to_string()),
                    ..JobData::default()
                },
            )
            .await
            .expect("add_to_queue should succeed");

        let mut arrivals = std::collections::HashMap::new();
        while arrivals.len() < 3 {
            let (job_id, at) = tokio::time::timeout(Duration::from_secs(15), rx.recv())
                .await
                .expect("all jobs should arrive")
                .expect("consumer should stay open");
            arrivals.insert(job_id.unwrap_or_default(), at.duration_since(started));
        }
        manager.disconnect().await.unwrap();

        assert!(
            arrivals["immediate"] < Duration::from_secs(2),
            "{arrivals:?}"
        );
        assert!(arrivals["legacy"] < Duration::from_secs(2), "{arrivals:?}");
        assert!(
            arrivals["delayed"] >= Duration::from_millis(2_900),
            "{arrivals:?}"
        );
        assert!(arrivals["delayed"] < Duration::from_secs(8), "{arrivals:?}");
        eprintln!("pulsar arrivals: {arrivals:?}");
    }
}
