use crate::ArcJobProcessorFn;
use async_trait::async_trait;
use bytes::Bytes;
use iggy::prelude::{
    Client, CompressionAlgorithm, Consumer, ConsumerGroupClient, ConsumerOffsetClient, IggyClient,
    IggyError, IggyExpiry, IggyMessage, MaxTopicSize, MessageClient, Partitioning, PollingStrategy,
    StreamClient, SystemClient, TopicClient, UserClient,
};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::IggyConfig;
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};
use tracing::{error, info, warn};

pub struct IggyQueueManager {
    client: Arc<IggyClient>,
    config: IggyConfig,
    stream: String,
    shutdown: Arc<Notify>,
    running: Arc<AtomicBool>,
}

impl IggyQueueManager {
    pub async fn new(config: IggyConfig) -> Result<Self> {
        validate_config(&config)?;
        let client = Arc::new(connect_client(&config).await?);
        let stream = normalize_name(&config.stream, "sockudo");
        ensure_stream(&client, &config, &stream).await?;
        Ok(Self {
            client,
            config,
            stream,
            shutdown: Arc::new(Notify::new()),
            running: Arc::new(AtomicBool::new(true)),
        })
    }

    fn topic_name(&self, queue_name: &str) -> String {
        format!(
            "{}-{}",
            normalize_name(&self.config.queue_topic_prefix, "sockudo-queue"),
            normalize_name(queue_name, "default")
        )
    }

    fn group_name(&self, queue_name: &str) -> String {
        format!(
            "{}-{}",
            normalize_name(&self.config.consumer_group_prefix, "sockudo-workers"),
            normalize_name(queue_name, "default")
        )
    }
}

#[async_trait]
impl QueueInterface for IggyQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> Result<()> {
        let topic = self.topic_name(queue_name);
        ensure_topic(&self.client, &self.config, &self.stream, &topic).await?;

        let payload = sonic_rs::to_vec(&data)
            .map_err(|e| Error::Queue(format!("Failed to serialize Apache Iggy queue job: {e}")))?;
        let mut messages = [IggyMessage::builder()
            .payload(Bytes::from(payload))
            .build()
            .map_err(to_queue_error)?];
        let partitioning = Partitioning::messages_key(queue_name.as_bytes())
            .unwrap_or_else(|_| Partitioning::partition_id(self.config.partition_id));

        with_timeout(
            &self.config,
            self.client.send_messages(
                &identifier(&self.stream)?,
                &identifier(&topic)?,
                &partitioning,
                &mut messages,
            ),
        )
        .await
        .map_err(|e| Error::Queue(format!("Failed to publish Apache Iggy queue job: {e}")))?;

        Ok(())
    }

    async fn process_queue(&self, queue_name: &str, callback: JobProcessorFnAsync) -> Result<()> {
        let topic = self.topic_name(queue_name);
        let group = self.group_name(queue_name);
        ensure_topic(&self.client, &self.config, &self.stream, &topic).await?;
        ensure_consumer_group(&self.client, &self.config, &self.stream, &topic, &group).await?;

        let callback: ArcJobProcessorFn = Arc::from(callback);
        let config = self.config.clone();
        let stream = self.stream.clone();
        let shutdown = self.shutdown.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            let client = match connect_client(&config).await {
                Ok(client) => Arc::new(client),
                Err(error) => {
                    error!("Failed to start Apache Iggy queue worker: {}", error);
                    return;
                }
            };

            let group_id = match identifier(&group) {
                Ok(id) => id,
                Err(error) => {
                    error!("Invalid Apache Iggy consumer group name: {}", error);
                    return;
                }
            };
            let stream_id = match identifier(&stream) {
                Ok(id) => id,
                Err(error) => {
                    error!("Invalid Apache Iggy stream name: {}", error);
                    return;
                }
            };
            let topic_id = match identifier(&topic) {
                Ok(id) => id,
                Err(error) => {
                    error!("Invalid Apache Iggy queue topic name: {}", error);
                    return;
                }
            };

            if let Err(error) = client
                .join_consumer_group(&stream_id, &topic_id, &group_id)
                .await
            {
                warn!("Failed to join Apache Iggy consumer group '{group}': {error}");
            }

            let consumer = Consumer::group(group_id);
            let strategy = PollingStrategy::next();

            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }
                tokio::select! {
                    _ = shutdown.notified() => break,
                    _ = sleep(Duration::from_millis(config.poll_interval_ms)) => {}
                }

                let polled = with_timeout(
                    &config,
                    client.poll_messages(
                        &stream_id,
                        &topic_id,
                        None,
                        &consumer,
                        &strategy,
                        config.poll_batch_size,
                        false,
                    ),
                )
                .await;

                match polled {
                    Ok(messages) => {
                        let partition_id =
                            (messages.partition_id != 0).then_some(messages.partition_id);
                        for message in messages.messages {
                            match sonic_rs::from_slice::<JobData>(&message.payload) {
                                Ok(job) => {
                                    if callback(job).await.is_ok() {
                                        if let Err(error) = client
                                            .store_consumer_offset(
                                                &consumer,
                                                &stream_id,
                                                &topic_id,
                                                partition_id,
                                                message.header.offset,
                                            )
                                            .await
                                        {
                                            error!(
                                                "Failed to commit Apache Iggy queue offset: {error}"
                                            );
                                        }
                                    } else {
                                        warn!(
                                            "Apache Iggy queue job failed; offset not committed for retry"
                                        );
                                        break;
                                    }
                                }
                                Err(error) => {
                                    error!("Failed to deserialize Apache Iggy queue job: {error}");
                                    if let Err(error) = client
                                        .store_consumer_offset(
                                            &consumer,
                                            &stream_id,
                                            &topic_id,
                                            partition_id,
                                            message.header.offset,
                                        )
                                        .await
                                    {
                                        error!(
                                            "Failed to commit malformed Apache Iggy queue job: {error}"
                                        );
                                    }
                                }
                            }
                        }
                    }
                    Err(error) => warn!("Apache Iggy queue poll failed: {error}"),
                }
            }
            info!("Apache Iggy queue worker stopped");
        });

        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        self.shutdown.notify_waiters();
        Ok(())
    }

    async fn check_health(&self) -> Result<()> {
        with_timeout(&self.config, self.client.ping())
            .await
            .map_err(|e| Error::Queue(format!("Apache Iggy queue health check failed: {e}")))
    }
}

async fn connect_client(config: &IggyConfig) -> Result<IggyClient> {
    let client = IggyClient::from_connection_string(&config.connection_string)
        .map_err(|e| Error::Queue(format!("Invalid Apache Iggy connection string: {e}")))?;
    client
        .connect()
        .await
        .map_err(|e| Error::Queue(format!("Failed to connect to Apache Iggy: {e}")))?;

    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        client
            .login_user(username, password)
            .await
            .map_err(|e| Error::Queue(format!("Failed to authenticate to Apache Iggy: {e}")))?;
    }

    Ok(client)
}

async fn ensure_stream(client: &IggyClient, config: &IggyConfig, stream: &str) -> Result<()> {
    let stream_id = identifier(stream)?;
    if client
        .get_stream(&stream_id)
        .await
        .map_err(to_queue_error)?
        .is_none()
    {
        if !config.auto_create {
            return Err(Error::Queue(format!(
                "Apache Iggy stream '{stream}' does not exist and auto_create is false"
            )));
        }
        match client.create_stream(stream).await {
            Ok(_) | Err(IggyError::StreamNameAlreadyExists(_)) => {}
            Err(error) => return Err(to_queue_error(error)),
        }
    }
    Ok(())
}

async fn ensure_topic(
    client: &IggyClient,
    config: &IggyConfig,
    stream: &str,
    topic: &str,
) -> Result<()> {
    ensure_stream(client, config, stream).await?;
    let stream_id = identifier(stream)?;
    let topic_id = identifier(topic)?;
    if client
        .get_topic(&stream_id, &topic_id)
        .await
        .map_err(to_queue_error)?
        .is_none()
    {
        if !config.auto_create {
            return Err(Error::Queue(format!(
                "Apache Iggy topic '{topic}' does not exist and auto_create is false"
            )));
        }
        match client
            .create_topic(
                &stream_id,
                topic,
                config.partitions_count,
                CompressionAlgorithm::default(),
                None,
                IggyExpiry::ServerDefault,
                MaxTopicSize::ServerDefault,
            )
            .await
        {
            Ok(_) | Err(IggyError::TopicNameAlreadyExists(_, _)) => {}
            Err(error) => return Err(to_queue_error(error)),
        }
    }
    Ok(())
}

async fn ensure_consumer_group(
    client: &IggyClient,
    config: &IggyConfig,
    stream: &str,
    topic: &str,
    group: &str,
) -> Result<()> {
    let stream_id = identifier(stream)?;
    let topic_id = identifier(topic)?;
    let group_id = identifier(group)?;
    if client
        .get_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .map_err(to_queue_error)?
        .is_none()
    {
        if !config.auto_create {
            return Err(Error::Queue(format!(
                "Apache Iggy consumer group '{group}' does not exist and auto_create is false"
            )));
        }
        match client
            .create_consumer_group(&stream_id, &topic_id, group)
            .await
        {
            Ok(_) | Err(IggyError::ConsumerGroupNameAlreadyExists(_, _)) => {}
            Err(error) => return Err(to_queue_error(error)),
        }
    }
    Ok(())
}

async fn with_timeout<F, T>(config: &IggyConfig, future: F) -> std::result::Result<T, IggyError>
where
    F: std::future::Future<Output = std::result::Result<T, IggyError>>,
{
    timeout(Duration::from_millis(config.request_timeout_ms), future)
        .await
        .map_err(|_| IggyError::CannotEstablishConnection)?
}

fn identifier(value: &str) -> Result<iggy::prelude::Identifier> {
    value
        .try_into()
        .map_err(|e| Error::Queue(format!("Invalid Apache Iggy identifier '{value}': {e}")))
}

fn validate_config(config: &IggyConfig) -> Result<()> {
    if config.connection_string.trim().is_empty() {
        return Err(Error::Queue(
            "Apache Iggy connection_string must not be empty".to_string(),
        ));
    }
    if config.partitions_count == 0 {
        return Err(Error::Queue(
            "Apache Iggy partitions_count must be greater than 0".to_string(),
        ));
    }
    if config.partition_id == 0 || config.partition_id > config.partitions_count {
        return Err(Error::Queue(format!(
            "Apache Iggy partition_id must be between 1 and partitions_count ({})",
            config.partitions_count
        )));
    }
    if config.poll_batch_size == 0 {
        return Err(Error::Queue(
            "Apache Iggy poll_batch_size must be greater than 0".to_string(),
        ));
    }
    Ok(())
}

fn normalize_name(value: &str, fallback: &str) -> String {
    let normalized = value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch.to_ascii_lowercase(),
            _ => '-',
        })
        .collect::<String>();
    let trimmed = normalized.trim_matches(['-', '_']);
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed.to_string()
    }
}

fn to_queue_error(error: IggyError) -> Error {
    Error::Queue(format!("Apache Iggy error: {error}"))
}
