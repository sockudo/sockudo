use crate::ArcJobProcessorFn;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use iggy::prelude::{
    AutoCommit, Client, CompressionAlgorithm, ConsumerGroupClient, HeaderKey, HeaderValue,
    IggyClient, IggyDuration, IggyError, IggyExpiry, IggyMessage, IggyProducer, MaxTopicSize,
    Partitioning, StreamClient, SystemClient, TopicClient, UserClient,
};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::IggyConfig;
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};

const IGGY_QUEUE_ATTEMPT_HEADER: &str = "sockudo-delivery-attempt";
const IGGY_QUEUE_MAX_DELIVERY_ATTEMPTS: u32 = 5;

struct QueuePublisher<'a> {
    producers: &'a Arc<Mutex<HashMap<String, Arc<IggyProducer>>>>,
    client: &'a IggyClient,
    config: &'a IggyConfig,
    stream: &'a str,
}

pub struct IggyQueueManager {
    client: Arc<IggyClient>,
    config: IggyConfig,
    stream: String,
    producers: Arc<Mutex<HashMap<String, Arc<IggyProducer>>>>,
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
            producers: Arc::new(Mutex::new(HashMap::new())),
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
        publish_queue_payload(
            &self.producers,
            &self.client,
            &self.config,
            &self.stream,
            &topic,
            Bytes::from(payload),
            1,
        )
        .await?;

        Ok(())
    }

    async fn process_queue(&self, queue_name: &str, callback: JobProcessorFnAsync) -> Result<()> {
        let topic = self.topic_name(queue_name);
        let dlq_topic = format!("{topic}-dlq");
        let group = self.group_name(queue_name);
        ensure_topic(&self.client, &self.config, &self.stream, &topic).await?;
        ensure_topic(&self.client, &self.config, &self.stream, &dlq_topic).await?;
        ensure_consumer_group(&self.client, &self.config, &self.stream, &topic, &group).await?;

        let callback: ArcJobProcessorFn = Arc::from(callback);
        let config = self.config.clone();
        let stream = self.stream.clone();
        let producers = self.producers.clone();
        let shutdown = self.shutdown.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            let mut retry_attempt = 0;
            while running.load(Ordering::Relaxed) {
                let client = match connect_client(&config).await {
                    Ok(client) => Arc::new(client),
                    Err(error) => {
                        warn!(error = %error, "failed to connect apache iggy queue worker, retrying");
                        retry_attempt += 1;
                        wait_before_retry(&config, retry_attempt, &shutdown, &running).await;
                        continue;
                    }
                };

                let mut consumer = match build_queue_consumer(
                    &client, &config, &stream, &topic, &group,
                )
                .await
                {
                    Ok(consumer) => {
                        retry_attempt = 0;
                        consumer
                    }
                    Err(error) => {
                        warn!(group = %group, error = %error, "failed to initialize apache iggy queue consumer group, retrying");
                        if let Err(error) = client.shutdown().await {
                            warn!(error = %error, "failed to shutdown apache iggy queue worker client");
                        }
                        retry_attempt += 1;
                        wait_before_retry(&config, retry_attempt, &shutdown, &running).await;
                        continue;
                    }
                };

                loop {
                    if !running.load(Ordering::Relaxed) {
                        break;
                    }
                    tokio::select! {
                        _ = shutdown.notified() => break,
                        received = consumer.next() => {
                            match received {
                                Some(Ok(received)) => {
                                    let partition_id = received.partition_id;
                                    let message = received.message;
                                    match sonic_rs::from_slice::<JobData>(&message.payload) {
                                        Ok(job) => {
                                            if callback(job).await.is_ok() {
                                                if let Err(error) = consumer
                                                    .store_offset(message.header.offset, Some(partition_id))
                                                    .await
                                                {
                                                    error!(error = %error, "failed to commit apache iggy queue offset");
                                                }
                                            } else if let Err(error) = handle_failed_job(
                                                &QueuePublisher {
                                                    producers: &producers,
                                                    client: &client,
                                                    config: &config,
                                                    stream: &stream,
                                                },
                                                &topic,
                                                &message,
                                                &mut consumer,
                                                partition_id,
                                            )
                                            .await
                                            {
                                                warn!(error = %error, "apache iggy queue job failed and could not be moved for retry or dlq");
                                            }
                                        }
                                        Err(error) => {
                                            error!(error = %error, "failed to deserialize apache iggy queue job");
                                            if let Err(error) = consumer
                                                .store_offset(message.header.offset, Some(partition_id))
                                                .await
                                            {
                                                error!(error = %error, "failed to commit malformed apache iggy queue job");
                                            }
                                        }
                                    }
                                }
                                Some(Err(error)) => warn!(error = %error, "apache iggy queue consumer failed"),
                                None => break,
                            }
                        }
                    }
                }
                if let Err(error) = consumer.shutdown().await {
                    warn!(error = %error, "failed to shutdown apache iggy queue consumer");
                }
                if let Err(error) = client.shutdown().await {
                    warn!(error = %error, "failed to shutdown apache iggy queue worker client");
                }
                if running.load(Ordering::Relaxed) {
                    retry_attempt += 1;
                    warn!("Apache Iggy queue worker ended unexpectedly; retrying");
                    wait_before_retry(&config, retry_attempt, &shutdown, &running).await;
                }
            }
            info!("Apache Iggy queue worker stopped");
        });

        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        self.shutdown.notify_waiters();
        self.client
            .shutdown()
            .await
            .map_err(|e| Error::Queue(format!("Failed to shutdown Apache Iggy client: {e}")))?;
        Ok(())
    }

    async fn check_health(&self) -> Result<()> {
        with_timeout(&self.config, self.client.ping())
            .await
            .map_err(|e| Error::Queue(format!("Apache Iggy queue health check failed: {e}")))
    }
}

async fn connect_client(config: &IggyConfig) -> Result<IggyClient> {
    if config.username.is_some()
        && config.password.is_some()
        && connection_string_has_credentials(&config.connection_string)
    {
        return Err(Error::Queue(
            "Apache Iggy credentials must come from either connection_string or username/password, not both"
                .to_string(),
        ));
    }

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

async fn build_queue_consumer(
    client: &IggyClient,
    config: &IggyConfig,
    stream: &str,
    topic: &str,
    group: &str,
) -> Result<iggy::prelude::IggyConsumer> {
    let mut consumer = client
        .consumer_group(group, stream, topic)
        .map_err(to_queue_error)?
        .poll_interval(IggyDuration::from(Duration::from_millis(
            config.poll_interval_ms,
        )))
        .batch_length(config.poll_batch_size)
        .auto_commit(AutoCommit::Disabled)
        .build();
    with_timeout(config, consumer.init()).await?;
    Ok(consumer)
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
                IggyExpiry::NeverExpire,
                MaxTopicSize::Unlimited,
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

async fn publish_queue_payload(
    producers: &Arc<Mutex<HashMap<String, Arc<IggyProducer>>>>,
    client: &IggyClient,
    config: &IggyConfig,
    stream: &str,
    topic: &str,
    payload: Bytes,
    attempt: u32,
) -> Result<()> {
    let mut headers = std::collections::BTreeMap::new();
    headers.insert(
        HeaderKey::try_from(IGGY_QUEUE_ATTEMPT_HEADER).map_err(to_queue_error)?,
        HeaderValue::from(attempt),
    );
    let message = IggyMessage::builder()
        .payload(payload)
        .user_headers(headers)
        .build()
        .map_err(to_queue_error)?;

    let producer = cached_queue_producer(producers, client, config, stream, topic).await?;
    with_timeout(config, producer.send_one(message))
        .await
        .map_err(|e| Error::Queue(format!("Failed to publish Apache Iggy queue job: {e}")))?;

    Ok(())
}

async fn cached_queue_producer(
    producers: &Arc<Mutex<HashMap<String, Arc<IggyProducer>>>>,
    client: &IggyClient,
    config: &IggyConfig,
    stream: &str,
    topic: &str,
) -> Result<Arc<IggyProducer>> {
    let mut producers = producers.lock().await;
    if let Some(producer) = producers.get(topic) {
        return Ok(producer.clone());
    }

    let producer = Arc::new(build_queue_producer(client, config, stream, topic).await?);
    producers.insert(topic.to_string(), producer.clone());
    Ok(producer)
}

async fn build_queue_producer(
    client: &IggyClient,
    config: &IggyConfig,
    stream: &str,
    topic: &str,
) -> Result<iggy::prelude::IggyProducer> {
    let mut builder = client
        .producer(stream, topic)
        .map_err(to_queue_error)?
        .partitioning(Partitioning::balanced());

    builder = if config.auto_create {
        builder.create_topic_if_not_exists(
            config.partitions_count,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::Unlimited,
        )
    } else {
        builder
            .do_not_create_stream_if_not_exists()
            .do_not_create_topic_if_not_exists()
    };

    let producer = builder.build();
    with_timeout(config, producer.init()).await?;
    Ok(producer)
}

async fn handle_failed_job(
    publisher: &QueuePublisher<'_>,
    topic: &str,
    message: &IggyMessage,
    consumer: &mut iggy::prelude::IggyConsumer,
    partition_id: u32,
) -> Result<()> {
    let current_attempt = delivery_attempt(message);
    let should_dlq = current_attempt >= IGGY_QUEUE_MAX_DELIVERY_ATTEMPTS;
    let next_attempt = if should_dlq {
        current_attempt
    } else {
        current_attempt.saturating_add(1)
    };
    let retry_topic = if should_dlq {
        format!("{topic}-dlq")
    } else {
        topic.to_string()
    };

    publish_queue_payload(
        publisher.producers,
        publisher.client,
        publisher.config,
        publisher.stream,
        &retry_topic,
        message.payload.clone(),
        next_attempt,
    )
    .await?;
    consumer
        .store_offset(message.header.offset, Some(partition_id))
        .await
        .map_err(to_queue_error)?;

    if retry_topic.ends_with("-dlq") {
        warn!(
            max_attempts = IGGY_QUEUE_MAX_DELIVERY_ATTEMPTS,
            dlq_topic = %retry_topic,
            "apache iggy queue job exceeded max delivery attempts, moved to dlq"
        );
    } else {
        warn!(
            attempt = next_attempt,
            "apache iggy queue job failed, republished for retry"
        );
    }
    Ok(())
}

fn delivery_attempt(message: &IggyMessage) -> u32 {
    let key = match HeaderKey::try_from(IGGY_QUEUE_ATTEMPT_HEADER) {
        Ok(key) => key,
        Err(e) => {
            debug!(error = %e, "failed to parse delivery attempt header key, using default");
            return 1;
        }
    };
    message
        .user_headers_map()
        .ok()
        .flatten()
        .and_then(|headers| {
            headers
                .get(&key)
                .and_then(|value| u32::try_from(value).ok())
        })
        .unwrap_or(1)
}

async fn with_timeout<F, T>(config: &IggyConfig, future: F) -> Result<T>
where
    F: std::future::Future<Output = std::result::Result<T, IggyError>>,
{
    timeout(Duration::from_millis(config.request_timeout_ms), future)
        .await
        .map_err(|_| {
            Error::Queue(format!(
                "Apache Iggy request timed out after {} ms",
                config.request_timeout_ms
            ))
        })?
        .map_err(to_queue_error)
}

async fn wait_before_retry(
    config: &IggyConfig,
    attempt: u32,
    shutdown: &Notify,
    running: &AtomicBool,
) {
    if !running.load(Ordering::Relaxed) {
        return;
    }
    let multiplier = 1_u64 << attempt.min(5);
    let delay_ms = config
        .poll_interval_ms
        .max(100)
        .saturating_mul(multiplier)
        .min(30_000);
    tokio::select! {
        _ = shutdown.notified() => {}
        _ = sleep(Duration::from_millis(delay_ms)) => {}
    }
}

impl Drop for IggyQueueManager {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        self.shutdown.notify_waiters();
    }
}

fn identifier(value: &str) -> Result<iggy::prelude::Identifier> {
    iggy::prelude::Identifier::named(value)
        .map_err(|e| Error::Queue(format!("Invalid Apache Iggy identifier '{value}': {e}")))
}

fn validate_config(config: &IggyConfig) -> Result<()> {
    if config.connection_string.trim().is_empty() {
        return Err(Error::Queue(
            "Apache Iggy connection_string must not be empty".to_string(),
        ));
    }
    if config.username.is_some() != config.password.is_some() {
        return Err(Error::Queue(
            "Apache Iggy username and password must be configured together".to_string(),
        ));
    }
    if config.partitions_count == 0 {
        return Err(Error::Queue(
            "Apache Iggy partitions_count must be greater than 0".to_string(),
        ));
    }
    if config.partition_id >= config.partitions_count {
        return Err(Error::Queue(format!(
            "Apache Iggy partition_id must be between 0 and partitions_count - 1 ({})",
            config.partitions_count - 1
        )));
    }
    if config.poll_batch_size == 0 {
        return Err(Error::Queue(
            "Apache Iggy poll_batch_size must be greater than 0".to_string(),
        ));
    }
    for (field, value) in [
        ("stream", config.stream.as_str()),
        ("queue_topic_prefix", config.queue_topic_prefix.as_str()),
        (
            "consumer_group_prefix",
            config.consumer_group_prefix.as_str(),
        ),
    ] {
        let normalized = normalize_name(value, "");
        if !normalized.is_empty() && normalized.chars().all(|ch| ch.is_ascii_digit()) {
            return Err(Error::Queue(format!(
                "Apache Iggy {field} must not normalize to an all-digit name"
            )));
        }
    }
    Ok(())
}

fn connection_string_has_credentials(connection_string: &str) -> bool {
    let Some(after_scheme) = connection_string.split_once("://").map(|(_, rest)| rest) else {
        return false;
    };
    after_scheme
        .split(['/', '?', '#'])
        .next()
        .is_some_and(|authority| authority.contains('@'))
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
