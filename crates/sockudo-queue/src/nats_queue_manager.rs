use crate::ArcJobProcessorFn;
use crate::broker_batch::prepare_default_batch;
use crate::worker_registry::WorkerRegistry;
use async_nats::jetstream;
use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::stream;
use async_trait::async_trait;
use dashmap::DashSet;
use futures_util::{TryStreamExt, future::try_join_all};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{NatsAdapterConfig, QueueReliabilityConfig};
use sockudo_core::queue::{
    QueueBackendKind, QueueCapabilities, QueueInterface, QueueJobId, QueueJobRequest,
};
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;
use tracing::{error, info};

pub struct NatsJetStreamQueueManager {
    client: async_nats::Client,
    jetstream: jetstream::Context,
    prefix: String,
    shutdown: Arc<Notify>,
    running: Arc<AtomicBool>,
    workers: WorkerRegistry,
    provisioned_streams: DashSet<String>,
    reliability: QueueReliabilityConfig,
}

impl NatsJetStreamQueueManager {
    pub async fn new(config: NatsAdapterConfig) -> Result<Self> {
        Self::new_with_reliability(config, QueueReliabilityConfig::default()).await
    }

    pub async fn new_with_reliability(
        config: NatsAdapterConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Self> {
        reliability.validate().map_err(Error::Config)?;
        let mut options = async_nats::ConnectOptions::new();
        if let (Some(username), Some(password)) =
            (config.username.as_deref(), config.password.as_deref())
        {
            options = options.user_and_password(username.to_string(), password.to_string());
        } else if let Some(token) = config.token.as_deref() {
            options = options.token(token.to_string());
        }
        options = options.connection_timeout(std::time::Duration::from_millis(
            config.connection_timeout_ms,
        ));
        let client = options
            .connect(&config.servers)
            .await
            .map_err(|e| Error::Queue(format!("Failed to connect to NATS JetStream: {e}")))?;
        let jetstream = jetstream::new(client.clone());

        Ok(Self {
            client,
            jetstream,
            prefix: config.prefix,
            shutdown: Arc::new(Notify::new()),
            running: Arc::new(AtomicBool::new(true)),
            workers: WorkerRegistry::default(),
            provisioned_streams: DashSet::new(),
            reliability,
        })
    }

    fn normalize(value: &str) -> String {
        value
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect()
    }

    fn stream_name(&self, queue_name: &str) -> String {
        format!(
            "{}_q_{}",
            Self::normalize(&self.prefix),
            Self::normalize(queue_name)
        )
    }

    fn subject_name(&self, queue_name: &str) -> String {
        format!("{}.queue.{}", self.prefix, queue_name)
    }

    async fn ensure_stream(&self, queue_name: &str) -> Result<stream::Stream> {
        let stream_name = self.stream_name(queue_name);
        let subject = self.subject_name(queue_name);
        let stream = self
            .jetstream
            .get_or_create_stream(stream::Config {
                name: stream_name,
                subjects: vec![subject],
                max_messages: 100_000,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                Error::Queue(format!("Failed to ensure NATS JetStream queue stream: {e}"))
            })?;
        self.provisioned_streams.insert(queue_name.to_string());
        Ok(stream)
    }

    async fn ensure_stream_once(&self, queue_name: &str) -> Result<()> {
        if !self.provisioned_streams.contains(queue_name) {
            self.ensure_stream(queue_name).await?;
        }
        Ok(())
    }

    async fn publish_batch(&self, queue_name: &str, jobs: Vec<JobData>) -> Result<()> {
        if jobs.is_empty() {
            return Ok(());
        }
        self.ensure_stream_once(queue_name).await?;
        let subject = self.subject_name(queue_name);

        for chunk in jobs.chunks(self.reliability.max_batch_size) {
            let mut acknowledgements = Vec::with_capacity(chunk.len());
            for data in chunk {
                let payload = sonic_rs::to_vec(data).map_err(|e| {
                    Error::Queue(format!("Failed to serialize NATS queue job: {e}"))
                })?;
                acknowledgements.push(
                    self.jetstream
                        .publish(subject.clone(), payload.into())
                        .await
                        .map_err(|e| {
                            Error::Queue(format!("Failed to publish NATS queue job: {e}"))
                        })?,
                );
            }
            try_join_all(
                acknowledgements
                    .into_iter()
                    .map(|acknowledgement| async move { acknowledgement.await }),
            )
            .await
            .map_err(|e| Error::Queue(format!("Failed to await NATS publish ack: {e}")))?;
        }
        Ok(())
    }
}

#[async_trait]
impl QueueInterface for NatsJetStreamQueueManager {
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
        let stream = self.ensure_stream(queue_name).await?;
        let consumer_name = format!("{}_consumer", self.stream_name(queue_name));
        let consumer = stream
            .get_or_create_consumer(
                consumer_name.as_str(),
                pull::Config {
                    durable_name: Some(consumer_name.clone()),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: std::time::Duration::from_millis(self.reliability.lease_duration_ms),
                    max_deliver: self.reliability.max_attempts as i64,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| Error::Queue(format!("Failed to create NATS queue consumer: {e}")))?;

        let callback: ArcJobProcessorFn = Arc::from(callback);
        let shutdown = self.shutdown.clone();
        let running = self.running.clone();
        let retry_delay = std::time::Duration::from_millis(self.reliability.retry_base_delay_ms);
        let prefetch = self.reliability.worker_prefetch;

        self.workers.spawn(async move {
            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }
                let messages = match consumer.messages().await {
                    Ok(messages) => messages,
                    Err(e) => {
                        error!("Failed to open NATS JetStream message stream: {}", e);
                        break;
                    }
                };
                tokio::pin!(messages);
                let mut in_flight = tokio::task::JoinSet::new();
                loop {
                    if !running.load(Ordering::Relaxed) {
                        break;
                    }
                    if in_flight.len() >= prefetch {
                        if let Some(Err(error)) = in_flight.join_next().await {
                            error!("NATS queue callback task failed: {error}");
                        }
                        continue;
                    }
                    let message = tokio::select! {
                        _ = shutdown.notified() => break,
                        completed = in_flight.join_next(), if !in_flight.is_empty() => {
                            if let Some(Err(error)) = completed {
                                error!("NATS queue callback task failed: {error}");
                            }
                            continue;
                        }
                        message = messages.try_next() => message,
                    };
                    match message {
                        Ok(Some(message)) => {
                            let callback = callback.clone();
                            in_flight.spawn(async move {
                                process_nats_message(message, callback, retry_delay).await;
                            });
                        }
                        Ok(None) => break,
                        Err(e) => {
                            error!("NATS queue consumer error: {}", e);
                            break;
                        }
                    }
                }
                while let Some(result) = in_flight.join_next().await {
                    if let Err(error) = result {
                        error!("NATS queue callback task failed while draining: {error}");
                    }
                }
            }
            info!("NATS JetStream queue consumer stopped");
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
        self.client
            .drain()
            .await
            .map_err(|e| Error::Queue(format!("Failed to drain NATS queue client: {e}")))?;
        Ok(())
    }

    async fn check_health(&self) -> Result<()> {
        self.jetstream
            .query_account()
            .await
            .map(|_| ())
            .map_err(|e| Error::Queue(format!("NATS JetStream queue health check failed: {e}")))
    }

    fn backend(&self) -> QueueBackendKind {
        QueueBackendKind::Nats
    }

    fn capabilities(&self) -> QueueCapabilities {
        QueueCapabilities {
            consume: true,
            acknowledgements: true,
            delayed_delivery: false,
            retries: true,
            dead_letter: false,
            deduplication: false,
            leasing: true,
            durable: true,
            batch_enqueue: true,
            observable_lag: false,
        }
    }
}

async fn process_nats_message(
    message: async_nats::jetstream::Message,
    callback: ArcJobProcessorFn,
    retry_delay: std::time::Duration,
) {
    match sonic_rs::from_slice::<JobData>(&message.payload) {
        Ok(job) => {
            if callback(job).await.is_ok() {
                if let Err(error) = message.ack().await {
                    error!("Failed to ack NATS queue job: {error}");
                }
            } else if let Err(error) = message
                .ack_with(jetstream::AckKind::Nak(Some(retry_delay)))
                .await
            {
                error!("Failed to nack NATS queue job: {error}");
            }
        }
        Err(error) => {
            error!("Failed to deserialize NATS queue job: {error}");
            let _ = message.ack_with(jetstream::AckKind::Term).await;
        }
    }
}
