use crate::ArcJobProcessorFn;
use async_trait::async_trait;
use futures_util::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::RabbitMqAdapterConfig;
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;
use tracing::{error, info, warn};

pub struct RabbitMqQueueManager {
    connection: Arc<Connection>,
    publish_channel: Channel,
    prefix: String,
    shutdown: Arc<Notify>,
    running: Arc<AtomicBool>,
}

impl RabbitMqQueueManager {
    pub async fn new(config: RabbitMqAdapterConfig) -> Result<Self> {
        let connection = Connection::connect(&config.url, ConnectionProperties::default())
            .await
            .map_err(|e| Error::Queue(format!("Failed to connect to RabbitMQ: {e}")))?;
        let connection = Arc::new(connection);
        let publish_channel = connection
            .create_channel()
            .await
            .map_err(|e| Error::Queue(format!("Failed to create RabbitMQ publish channel: {e}")))?;

        Ok(Self {
            connection,
            publish_channel,
            prefix: config.prefix,
            shutdown: Arc::new(Notify::new()),
            running: Arc::new(AtomicBool::new(true)),
        })
    }

    fn queue_name(&self, queue_name: &str) -> String {
        format!("{}.queue.{}", self.prefix, queue_name)
    }

    async fn ensure_queue(&self, channel: &Channel, queue_name: &str) -> Result<()> {
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
        Ok(())
    }
}

#[async_trait]
impl QueueInterface for RabbitMqQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> Result<()> {
        let queue_name = self.queue_name(queue_name);
        self.ensure_queue(&self.publish_channel, &queue_name)
            .await?;
        let payload = sonic_rs::to_vec(&data)
            .map_err(|e| Error::Queue(format!("Failed to serialize RabbitMQ job: {e}")))?;

        self.publish_channel
            .basic_publish(
                "".into(),
                queue_name.as_str().into(),
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .map_err(|e| Error::Queue(format!("Failed to publish RabbitMQ job: {e}")))?
            .await
            .map_err(|e| Error::Queue(format!("RabbitMQ publish confirmation failed: {e}")))?;

        Ok(())
    }

    async fn process_queue(&self, queue_name: &str, callback: JobProcessorFnAsync) -> Result<()> {
        let queue_name = self.queue_name(queue_name);
        let consumer_channel = self.connection.create_channel().await.map_err(|e| {
            Error::Queue(format!("Failed to create RabbitMQ consumer channel: {e}"))
        })?;
        self.ensure_queue(&consumer_channel, &queue_name).await?;

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

        tokio::spawn(async move {
            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }
                let delivery = tokio::select! {
                    _ = shutdown.notified() => break,
                    delivery = consumer.next() => delivery,
                };
                let Some(delivery) = delivery else {
                    break;
                };
                match delivery {
                    Ok(delivery) => match sonic_rs::from_slice::<JobData>(&delivery.data) {
                        Ok(job) => {
                            if callback(job).await.is_ok() {
                                if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                                    error!("Failed to ack RabbitMQ queue delivery: {}", e);
                                }
                            } else {
                                warn!(
                                    "RabbitMQ queue job failed; leaving delivery unacked for broker retry"
                                );
                            }
                        }
                        Err(e) => {
                            error!("Failed to deserialize RabbitMQ queue job: {}", e);
                            let _ = delivery.ack(BasicAckOptions::default()).await;
                        }
                    },
                    Err(e) => {
                        error!("RabbitMQ queue consumer error: {}", e);
                        break;
                    }
                }
            }
            info!("RabbitMQ queue consumer stopped");
        });

        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        self.shutdown.notify_waiters();
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
}
