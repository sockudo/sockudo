use crate::ArcJobProcessorFn;
use crate::broker_batch::prepare_default_batch;
use crate::worker_registry::WorkerRegistry;
use async_trait::async_trait;
use dashmap::DashSet;
use futures_util::{StreamExt, future::try_join_all};
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{QueueReliabilityConfig, RabbitMqAdapterConfig};
use sockudo_core::queue::{
    QueueBackendKind, QueueCapabilities, QueueInterface, QueueJobId, QueueJobRequest,
};
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;
use tracing::{error, info};

const DELIVERY_ATTEMPT_HEADER: &str = "sockudo-delivery-attempt";

pub struct RabbitMqQueueManager {
    connection: Arc<Connection>,
    publish_channel: Channel,
    prefix: String,
    shutdown: Arc<Notify>,
    running: Arc<AtomicBool>,
    workers: WorkerRegistry,
    declared_queues: DashSet<String>,
    reliability: QueueReliabilityConfig,
}

impl RabbitMqQueueManager {
    pub async fn new(config: RabbitMqAdapterConfig) -> Result<Self> {
        Self::new_with_reliability(config, QueueReliabilityConfig::default()).await
    }

    pub async fn new_with_reliability(
        config: RabbitMqAdapterConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Self> {
        reliability.validate().map_err(Error::Config)?;
        let connection = Connection::connect(&config.url, ConnectionProperties::default())
            .await
            .map_err(|e| Error::Queue(format!("Failed to connect to RabbitMQ: {e}")))?;
        let connection = Arc::new(connection);
        let publish_channel = connection
            .create_channel()
            .await
            .map_err(|e| Error::Queue(format!("Failed to create RabbitMQ publish channel: {e}")))?;
        publish_channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|e| Error::Queue(format!("Failed to enable RabbitMQ confirms: {e}")))?;

        Ok(Self {
            connection,
            publish_channel,
            prefix: config.prefix,
            shutdown: Arc::new(Notify::new()),
            running: Arc::new(AtomicBool::new(true)),
            workers: WorkerRegistry::default(),
            declared_queues: DashSet::new(),
            reliability,
        })
    }

    fn queue_name(&self, queue_name: &str) -> String {
        format!("{}.queue.{}", self.prefix, queue_name)
    }

    async fn ensure_queue(&self, channel: &Channel, queue_name: &str) -> Result<()> {
        if self.declared_queues.contains(queue_name) {
            return Ok(());
        }
        channel
            .queue_declare(
                queue_name.into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| {
                Error::Queue(format!(
                    "Failed to declare RabbitMQ queue {queue_name}: {e}"
                ))
            })?;
        self.declared_queues.insert(queue_name.to_string());
        Ok(())
    }

    async fn publish_batch(&self, queue_name: &str, jobs: Vec<JobData>) -> Result<()> {
        if jobs.is_empty() {
            return Ok(());
        }
        let queue_name = self.queue_name(queue_name);
        self.ensure_queue(&self.publish_channel, &queue_name)
            .await?;

        for chunk in jobs.chunks(self.reliability.max_batch_size) {
            let mut confirmations = Vec::with_capacity(chunk.len());
            for data in chunk {
                let payload = sonic_rs::to_vec(data)
                    .map_err(|e| Error::Queue(format!("Failed to serialize RabbitMQ job: {e}")))?;
                confirmations.push(
                    self.publish_channel
                        .basic_publish(
                            "".into(),
                            queue_name.as_str().into(),
                            BasicPublishOptions::default(),
                            &payload,
                            properties_for_attempt(1),
                        )
                        .await
                        .map_err(|e| {
                            Error::Queue(format!("Failed to publish RabbitMQ job: {e}"))
                        })?,
                );
            }
            try_join_all(confirmations)
                .await
                .map_err(|e| Error::Queue(format!("RabbitMQ publish confirmation failed: {e}")))?;
        }
        Ok(())
    }
}

#[async_trait]
impl QueueInterface for RabbitMqQueueManager {
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
        let queue_name = self.queue_name(queue_name);
        let dead_letter_queue = format!("{queue_name}.dlq");
        let consumer_channel = self.connection.create_channel().await.map_err(|e| {
            Error::Queue(format!("Failed to create RabbitMQ consumer channel: {e}"))
        })?;
        consumer_channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|e| Error::Queue(format!("Failed to enable RabbitMQ confirms: {e}")))?;
        self.ensure_queue(&consumer_channel, &queue_name).await?;
        self.ensure_queue(&consumer_channel, &dead_letter_queue)
            .await?;
        consumer_channel
            .basic_qos(
                self.reliability.worker_prefetch.min(u16::MAX as usize) as u16,
                BasicQosOptions::default(),
            )
            .await
            .map_err(|e| Error::Queue(format!("Failed to configure RabbitMQ prefetch: {e}")))?;

        let mut consumer = consumer_channel
            .basic_consume(
                queue_name.as_str().into(),
                format!("sockudo-queue-{}", uuid::Uuid::new_v4()).into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Queue(format!("Failed to start RabbitMQ consumer: {e}")))?;

        let callback: ArcJobProcessorFn = Arc::from(callback);
        let shutdown = self.shutdown.clone();
        let running = self.running.clone();
        let prefetch = self.reliability.worker_prefetch;
        let max_attempts = self.reliability.max_attempts;

        self.workers.spawn(async move {
            let mut in_flight = tokio::task::JoinSet::new();
            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }
                if in_flight.len() >= prefetch {
                    if let Some(Err(error)) = in_flight.join_next().await {
                        error!("RabbitMQ queue callback task failed: {error}");
                    }
                    continue;
                }
                let delivery = tokio::select! {
                    _ = shutdown.notified() => break,
                    completed = in_flight.join_next(), if !in_flight.is_empty() => {
                        if let Some(Err(error)) = completed {
                            error!("RabbitMQ queue callback task failed: {error}");
                        }
                        continue;
                    }
                    delivery = consumer.next() => delivery,
                };
                let Some(delivery) = delivery else {
                    break;
                };
                match delivery {
                    Ok(delivery) => {
                        let callback = callback.clone();
                        let channel = consumer_channel.clone();
                        let queue_name = queue_name.clone();
                        let dead_letter_queue = dead_letter_queue.clone();
                        in_flight.spawn(async move {
                            process_rabbitmq_delivery(
                                delivery,
                                callback,
                                channel,
                                queue_name,
                                dead_letter_queue,
                                max_attempts,
                            )
                            .await;
                        });
                    }
                    Err(e) => {
                        error!("RabbitMQ queue consumer error: {}", e);
                        break;
                    }
                }
            }
            while let Some(result) = in_flight.join_next().await {
                if let Err(error) = result {
                    error!("RabbitMQ queue callback task failed while draining: {error}");
                }
            }
            info!("RabbitMQ queue consumer stopped");
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
        if self.connection.status().connected() {
            Ok(())
        } else {
            Err(Error::Queue(
                "RabbitMQ queue connection is not connected".to_string(),
            ))
        }
    }

    fn backend(&self) -> QueueBackendKind {
        QueueBackendKind::RabbitMq
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

async fn process_rabbitmq_delivery(
    delivery: lapin::message::Delivery,
    callback: ArcJobProcessorFn,
    channel: Channel,
    queue_name: String,
    dead_letter_queue: String,
    max_attempts: u32,
) {
    match sonic_rs::from_slice::<JobData>(&delivery.data) {
        Ok(job) => {
            if callback(job).await.is_ok() {
                if let Err(error) = delivery.ack(BasicAckOptions::default()).await {
                    error!("Failed to ack RabbitMQ queue delivery: {error}");
                }
            } else {
                let attempt = delivery_attempt(&delivery);
                let (target, next_attempt) = if attempt >= max_attempts {
                    (dead_letter_queue.as_str(), attempt)
                } else {
                    (queue_name.as_str(), attempt.saturating_add(1))
                };
                republish_rabbitmq_delivery(&delivery, &channel, target, next_attempt).await;
            }
        }
        Err(error) => {
            error!("Failed to deserialize RabbitMQ queue job: {error}");
            republish_rabbitmq_delivery(&delivery, &channel, &dead_letter_queue, max_attempts)
                .await;
        }
    }
}

async fn republish_rabbitmq_delivery(
    delivery: &lapin::message::Delivery,
    channel: &Channel,
    target: &str,
    attempt: u32,
) {
    let published = async {
        channel
            .basic_publish(
                "".into(),
                target.into(),
                BasicPublishOptions::default(),
                &delivery.data,
                properties_for_attempt(attempt),
            )
            .await?
            .await
    }
    .await;
    match published {
        Ok(_) => {
            if let Err(error) = delivery.ack(BasicAckOptions::default()).await {
                error!("Failed to ack republished RabbitMQ queue delivery: {error}");
            }
        }
        Err(error) => {
            error!("Failed to republish RabbitMQ queue delivery to {target}: {error}");
            if let Err(nack_error) = delivery
                .nack(BasicNackOptions {
                    multiple: false,
                    requeue: true,
                })
                .await
            {
                error!("Failed to release RabbitMQ queue delivery: {nack_error}");
            }
        }
    }
}

fn delivery_attempt(delivery: &lapin::message::Delivery) -> u32 {
    delivery
        .properties
        .headers()
        .as_ref()
        .and_then(|headers| headers.inner().get(DELIVERY_ATTEMPT_HEADER))
        .and_then(|value| match value {
            AMQPValue::LongUInt(attempt) => Some(*attempt),
            _ => None,
        })
        .unwrap_or(1)
}

fn properties_for_attempt(attempt: u32) -> BasicProperties {
    let mut headers = FieldTable::default();
    headers.insert(
        ShortString::from(DELIVERY_ATTEMPT_HEADER.to_string()),
        AMQPValue::LongUInt(attempt),
    );
    BasicProperties::default()
        .with_delivery_mode(2)
        .with_headers(headers)
}
