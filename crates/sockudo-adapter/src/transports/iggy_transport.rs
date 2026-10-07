use crate::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::horizontal_transport::{HorizontalTransport, TransportConfig, TransportHandlers};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{FutureExt, StreamExt};
use iggy::prelude::{
    AutoCommit, Client, ClusterClient, ClusterNodeRole, ClusterNodeStatus, CompressionAlgorithm,
    Consumer, ConsumerOffsetClient, Durability, IggyClient, IggyDuration, IggyError, IggyExpiry,
    IggyMessage, IggyProducer, MaxTopicSize, Partitioning, PollingStrategy, StreamClient,
    SystemClient, TopicClient, TopicCreateOptions, UserClient,
};
use sockudo_core::error::{Error, Result};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::{IggyConfig, IggyConnectionCandidate, IggyDurability};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};
use tracing::{debug, info, trace, warn};

pub struct IggyTransport {
    client: Arc<IggyClient>,
    stream: String,
    broadcast_topic: String,
    request_topic: String,
    response_topic: String,
    broadcast_producer: Arc<IggyProducer>,
    request_producer: Arc<IggyProducer>,
    response_producer: Arc<IggyProducer>,
    listener_id: String,
    config: IggyConfig,
    metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
    shutdown: Arc<Notify>,
    is_running: Arc<AtomicBool>,
    owner_count: Arc<AtomicUsize>,
}

impl TransportConfig for IggyConfig {
    fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }

    fn prefix(&self) -> &str {
        &self.topic_prefix
    }
}

#[async_trait]
impl HorizontalTransport for IggyTransport {
    type Config = IggyConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        validate_config(&config)?;
        let client = Arc::new(connect_client(&config).await?);
        let stream = normalize_name(&config.stream, "sockudo");
        let prefix = normalize_name(&config.topic_prefix, "sockudo-adapter");
        let broadcast_topic = format!("{prefix}-broadcast");
        let request_topic = format!("{prefix}-requests");
        let response_topic = format!("{prefix}-responses");

        log_cluster_topology(&client, &config).await;
        let broadcast_producer =
            Arc::new(build_producer(&client, &config, &stream, &broadcast_topic).await?);
        let request_producer =
            Arc::new(build_producer(&client, &config, &stream, &request_topic).await?);
        let response_producer =
            Arc::new(build_producer(&client, &config, &stream, &response_topic).await?);
        let listener_id = stable_consumer_name(&config);

        info!(
            adapter = "iggy",
            stream = %stream,
            broadcast_topic = %broadcast_topic,
            request_topic = %request_topic,
            response_topic = %response_topic,
            "transport initialized"
        );

        Ok(Self {
            client,
            stream,
            broadcast_topic,
            request_topic,
            response_topic,
            broadcast_producer,
            request_producer,
            response_producer,
            listener_id,
            config,
            metrics: Arc::new(OnceLock::new()),
            shutdown: Arc::new(Notify::new()),
            is_running: Arc::new(AtomicBool::new(true)),
            owner_count: Arc::new(AtomicUsize::new(1)),
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        publish_message(&self.config, &self.broadcast_producer, message).await
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        publish_message(&self.config, &self.request_producer, request).await
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        publish_message(&self.config, &self.response_producer, response).await
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        self.spawn_consumer(
            self.broadcast_topic.clone(),
            format!("{}-broadcast", self.listener_id),
            "broadcast",
            handlers.on_broadcast.clone(),
        )?;
        self.spawn_request_consumer(
            self.request_topic.clone(),
            format!("{}-request", self.listener_id),
            handlers.on_request.clone(),
        )?;
        self.spawn_consumer(
            self.response_topic.clone(),
            format!("{}-response", self.listener_id),
            "response",
            handlers.on_response.clone(),
        )?;
        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        Ok(self.config.nodes_number.unwrap_or(1) as usize)
    }

    async fn check_health(&self) -> Result<()> {
        with_timeout(&self.config, self.client.ping())
            .await
            .map_err(|e| Error::Internal(format!("Apache Iggy health check failed: {e}")))
    }

    fn set_metrics(&self, metrics: Arc<dyn MetricsInterface + Send + Sync>) {
        let _ = self.metrics.set(metrics);
    }
}

impl IggyTransport {
    fn spawn_consumer<T>(
        &self,
        topic: String,
        consumer_name: String,
        kind: &'static str,
        handler: Arc<
            dyn Fn(T) -> crate::horizontal_transport::BoxFuture<'static, ()> + Send + Sync,
        >,
    ) -> Result<()>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let config = self.config.clone();
        let stream = self.stream.clone();
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let mut retry_attempt = 0;
            while is_running.load(Ordering::Relaxed) {
                let client = match connect_client(&config).await {
                    Ok(client) => Arc::new(client),
                    Err(error) => {
                        warn!(adapter = "iggy", kind = kind, error = %error, retryable = true, "listener connection failed");
                        retry_attempt += 1;
                        wait_before_retry(&config, retry_attempt, &shutdown, &is_running).await;
                        continue;
                    }
                };

                let mut consumer = match build_consumer(
                    &client,
                    &config,
                    &stream,
                    &topic,
                    &consumer_name,
                )
                .await
                {
                    Ok(consumer) => {
                        retry_attempt = 0;
                        consumer
                    }
                    Err(error) => {
                        warn!(adapter = "iggy", kind = kind, error = %error, retryable = true, "consumer initialization failed");
                        if let Err(error) = client.shutdown().await {
                            warn!(adapter = "iggy", kind = kind, error = %error, "listener client shutdown failed");
                        }
                        retry_attempt += 1;
                        wait_before_retry(&config, retry_attempt, &shutdown, &is_running).await;
                        continue;
                    }
                };

                loop {
                    if !is_running.load(Ordering::Relaxed) {
                        break;
                    }
                    tokio::select! {
                        _ = shutdown.notified() => break,
                        received = consumer.next() => {
                            match received {
                                Some(Ok(received)) => {
                                    let mut pending_offsets = PendingOffsets::default();
                                    let completed = process_consumer_message(
                                        &metrics,
                                        kind,
                                        &handler,
                                        received,
                                    )
                                    .await;
                                    pending_offsets.record(completed);

                                    let mut consumer_exhausted = false;
                                    while is_running.load(Ordering::Relaxed) {
                                        match consumer.next().now_or_never() {
                                            Some(Some(Ok(received))) => {
                                                let completed = process_consumer_message(
                                                    &metrics,
                                                    kind,
                                                    &handler,
                                                    received,
                                                )
                                                .await;
                                                pending_offsets.record(completed);
                                            }
                                            Some(Some(Err(error))) => {
                                                warn!(adapter = "iggy", kind = kind, error = %error, "consumer message failed");
                                            }
                                            Some(None) => {
                                                consumer_exhausted = true;
                                                break;
                                            }
                                            None => break,
                                        }
                                    }
                                    commit_offsets(&consumer, &pending_offsets, kind).await;
                                    if consumer_exhausted {
                                        break;
                                    }
                                }
                                Some(Err(error)) => warn!(adapter = "iggy", kind = kind, error = %error, "consumer message failed"),
                                None => break,
                            }
                        }
                    }
                }
                if let Err(error) = consumer.shutdown().await {
                    warn!(adapter = "iggy", kind = kind, error = %error, "consumer shutdown failed");
                }
                if let Err(error) = client.shutdown().await {
                    warn!(adapter = "iggy", kind = kind, error = %error, "listener client shutdown failed");
                }
                if is_running.load(Ordering::Relaxed) {
                    retry_attempt += 1;
                    warn!(
                        adapter = "iggy",
                        kind = kind,
                        retryable = true,
                        "consumer loop ended unexpectedly"
                    );
                    wait_before_retry(&config, retry_attempt, &shutdown, &is_running).await;
                }
            }
            warn!(adapter = "iggy", kind = kind, "consumer loop ended");
        });

        Ok(())
    }

    fn spawn_request_consumer(
        &self,
        topic: String,
        consumer_name: String,
        handler: Arc<
            dyn Fn(
                    RequestBody,
                )
                    -> crate::horizontal_transport::BoxFuture<'static, Result<ResponseBody>>
                + Send
                + Sync,
        >,
    ) -> Result<()> {
        let config = self.config.clone();
        let stream = self.stream.clone();
        let response_topic = self.response_topic.clone();
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let mut retry_attempt = 0;
            while is_running.load(Ordering::Relaxed) {
                let client = match connect_client(&config).await {
                    Ok(client) => Arc::new(client),
                    Err(error) => {
                        warn!(adapter = "iggy", error = %error, retryable = true, "request listener connection failed");
                        retry_attempt += 1;
                        wait_before_retry(&config, retry_attempt, &shutdown, &is_running).await;
                        continue;
                    }
                };

                let mut consumer = match build_consumer(
                    &client,
                    &config,
                    &stream,
                    &topic,
                    &consumer_name,
                )
                .await
                {
                    Ok(consumer) => consumer,
                    Err(error) => {
                        warn!(adapter = "iggy", error = %error, retryable = true, "request consumer initialization failed");
                        if let Err(error) = client.shutdown().await {
                            warn!(adapter = "iggy", error = %error, "request listener client shutdown failed");
                        }
                        retry_attempt += 1;
                        wait_before_retry(&config, retry_attempt, &shutdown, &is_running).await;
                        continue;
                    }
                };
                let response_producer = match build_producer(
                    &client,
                    &config,
                    &stream,
                    &response_topic,
                )
                .await
                {
                    Ok(producer) => {
                        retry_attempt = 0;
                        producer
                    }
                    Err(error) => {
                        warn!(adapter = "iggy", error = %error, retryable = true, "response producer initialization failed");
                        if let Err(error) = consumer.shutdown().await {
                            warn!(adapter = "iggy", error = %error, "request consumer shutdown failed");
                        }
                        if let Err(error) = client.shutdown().await {
                            warn!(adapter = "iggy", error = %error, "request listener client shutdown failed");
                        }
                        retry_attempt += 1;
                        wait_before_retry(&config, retry_attempt, &shutdown, &is_running).await;
                        continue;
                    }
                };

                loop {
                    if !is_running.load(Ordering::Relaxed) {
                        break;
                    }
                    tokio::select! {
                        _ = shutdown.notified() => break,
                        received = consumer.next() => {
                            match received {
                                Some(Ok(received)) => {
                                    let mut pending_offsets = PendingOffsets::default();
                                    if let Some(completed) = process_request_message(
                                        &config,
                                        &response_producer,
                                        &metrics,
                                        &handler,
                                        received,
                                    )
                                    .await
                                    {
                                        pending_offsets.record(completed);
                                    }

                                    let mut consumer_exhausted = false;
                                    while is_running.load(Ordering::Relaxed) {
                                        match consumer.next().now_or_never() {
                                            Some(Some(Ok(received))) => {
                                                if let Some(completed) = process_request_message(
                                                    &config,
                                                    &response_producer,
                                                    &metrics,
                                                    &handler,
                                                    received,
                                                )
                                                .await
                                                {
                                                    pending_offsets.record(completed);
                                                }
                                            }
                                            Some(Some(Err(error))) => {
                                                warn!(adapter = "iggy", error = %error, "request consumer message failed");
                                            }
                                            Some(None) => {
                                                consumer_exhausted = true;
                                                break;
                                            }
                                            None => break,
                                        }
                                    }
                                    commit_offsets(&consumer, &pending_offsets, "request").await;
                                    if consumer_exhausted {
                                        break;
                                    }
                                }
                                Some(Err(error)) => warn!(adapter = "iggy", error = %error, "request consumer message failed"),
                                None => break,
                            }
                        }
                    }
                }
                if let Err(error) = consumer.shutdown().await {
                    warn!(adapter = "iggy", error = %error, "request consumer shutdown failed");
                }
                response_producer.shutdown().await;
                if let Err(error) = client.shutdown().await {
                    warn!(adapter = "iggy", error = %error, "request listener client shutdown failed");
                }
                if is_running.load(Ordering::Relaxed) {
                    retry_attempt += 1;
                    warn!(
                        adapter = "iggy",
                        retryable = true,
                        "request consumer loop ended unexpectedly"
                    );
                    wait_before_retry(&config, retry_attempt, &shutdown, &is_running).await;
                }
            }
            warn!(adapter = "iggy", "request consumer loop ended");
        });

        Ok(())
    }
}

async fn process_consumer_message<T>(
    metrics: &OnceLock<Arc<dyn MetricsInterface + Send + Sync>>,
    kind: &'static str,
    handler: &Arc<dyn Fn(T) -> crate::horizontal_transport::BoxFuture<'static, ()> + Send + Sync>,
    received: iggy::prelude::ReceivedMessage,
) -> (u32, u64)
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    let partition_id = received.partition_id;
    let offset = received.message.header.offset;
    match sonic_rs::from_slice::<T>(&received.message.payload) {
        Ok(payload) => handler(payload).await,
        Err(error) => {
            if let Some(metrics) = metrics.get() {
                metrics.mark_horizontal_transport_message_dropped("iggy");
            }
            warn!(adapter = "iggy", kind = kind, error = %error, "transport message parse failed");
        }
    }
    (partition_id, offset)
}

async fn process_request_message(
    config: &IggyConfig,
    response_producer: &IggyProducer,
    metrics: &OnceLock<Arc<dyn MetricsInterface + Send + Sync>>,
    handler: &Arc<
        dyn Fn(RequestBody) -> crate::horizontal_transport::BoxFuture<'static, Result<ResponseBody>>
            + Send
            + Sync,
    >,
    received: iggy::prelude::ReceivedMessage,
) -> Option<(u32, u64)> {
    let partition_id = received.partition_id;
    let offset = received.message.header.offset;
    let mut should_commit = false;
    match sonic_rs::from_slice::<RequestBody>(&received.message.payload) {
        Ok(request) => match handler(request).await {
            Ok(response) => match publish_message(config, response_producer, &response).await {
                Ok(()) => should_commit = true,
                Err(error) => warn!(adapter = "iggy", error = %error, "response publish failed"),
            },
            Err(
                Error::OwnRequestIgnored | Error::RequestNotForThisNode | Error::NoResponseNeeded,
            ) => {
                should_commit = true;
            }
            Err(error) => warn!(adapter = "iggy", error = %error, "request handler failed"),
        },
        Err(error) => {
            if let Some(metrics) = metrics.get() {
                metrics.mark_horizontal_transport_message_dropped("iggy");
            }
            warn!(adapter = "iggy", error = %error, "request payload parse failed");
            should_commit = true;
        }
    }
    should_commit.then_some((partition_id, offset))
}

#[derive(Default)]
struct PendingOffsets {
    offsets: Vec<(u32, u64)>,
}

impl PendingOffsets {
    fn record(&mut self, (partition_id, offset): (u32, u64)) {
        if let Some((_, pending_offset)) = self
            .offsets
            .iter_mut()
            .find(|(pending_partition, _)| *pending_partition == partition_id)
        {
            *pending_offset = (*pending_offset).max(offset);
        } else {
            self.offsets.push((partition_id, offset));
        }
    }
}

async fn commit_offsets(
    consumer: &iggy::prelude::IggyConsumer,
    pending_offsets: &PendingOffsets,
    kind: &'static str,
) {
    // Commit only after every completed handler in this poll batch. A crash can replay the
    // uncommitted batch, preserving the adapter's at-least-once delivery behavior.
    for &(partition_id, offset) in &pending_offsets.offsets {
        if let Err(error) = consumer.store_offset(offset, Some(partition_id)).await {
            warn!(adapter = "iggy", kind = kind, error = %error, "offset commit failed");
        }
    }
}

impl Clone for IggyTransport {
    fn clone(&self) -> Self {
        self.owner_count.fetch_add(1, Ordering::Relaxed);
        Self {
            client: self.client.clone(),
            stream: self.stream.clone(),
            broadcast_topic: self.broadcast_topic.clone(),
            request_topic: self.request_topic.clone(),
            response_topic: self.response_topic.clone(),
            broadcast_producer: self.broadcast_producer.clone(),
            request_producer: self.request_producer.clone(),
            response_producer: self.response_producer.clone(),
            listener_id: self.listener_id.clone(),
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            shutdown: self.shutdown.clone(),
            is_running: self.is_running.clone(),
            owner_count: self.owner_count.clone(),
        }
    }
}

impl Drop for IggyTransport {
    fn drop(&mut self) {
        if self.owner_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.is_running.store(false, Ordering::Relaxed);
            self.shutdown.notify_waiters();
            let client = self.client.clone();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    if let Err(error) = client.shutdown().await {
                        warn!(adapter = "iggy", error = %error, "transport client shutdown failed");
                    }
                });
            } else {
                warn!(
                    adapter = "iggy",
                    "no tokio runtime for transport client shutdown"
                );
            }
        }
    }
}

async fn connect_client(config: &IggyConfig) -> Result<IggyClient> {
    if config.username.is_some()
        && config.password.is_some()
        && connection_string_has_credentials(&config.connection_string)
    {
        return Err(Error::Config(
            "Apache Iggy credentials must come from either connection_string or username/password, not both"
                .to_string(),
        ));
    }

    let candidates = config.connection_candidates().map_err(Error::Config)?;
    // Seeds only bootstrap the connection; once connected, the SDK learns the cluster roster,
    // follows the leader, and fails over on its own. With one candidate, the SDK's
    // reconnection policy governs the dial as it always has.
    let bounded = candidates.len() > 1;
    let mut last_error = None;
    for candidate in &candidates {
        match connect_candidate(config, candidate, bounded).await {
            Ok(client) => return Ok(client),
            Err(error) => {
                if bounded {
                    warn!(adapter = "iggy", node = %candidate.address, error = %error, "cluster seed unreachable");
                }
                last_error = Some(error);
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        Error::Internal("No Apache Iggy cluster node could be reached".to_string())
    }))
}

async fn connect_candidate(
    config: &IggyConfig,
    candidate: &IggyConnectionCandidate,
    bounded: bool,
) -> Result<IggyClient> {
    let connect_timeout = Duration::from_millis(config.connect_timeout_ms);
    if bounded && candidate.is_tcp() {
        // The SDK redials a refused node until its retry policy gives up, which would spend
        // the whole connect timeout on a seed that is plainly down. A bare TCP dial fails
        // that seed in milliseconds and moves on.
        match timeout(connect_timeout, TcpStream::connect(&candidate.address)).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                return Err(Error::Internal(format!(
                    "Apache Iggy node {} is unreachable: {error}",
                    candidate.address
                )));
            }
            Err(_) => {
                return Err(Error::Internal(format!(
                    "Apache Iggy node {} did not accept a connection within {} ms",
                    candidate.address, config.connect_timeout_ms
                )));
            }
        }
    }

    let client = IggyClient::from_connection_string(&candidate.connection_string)
        .map_err(|e| Error::Internal(format!("Invalid Apache Iggy connection string: {e}")))?;
    let connected = if bounded {
        timeout(connect_timeout, client.connect())
            .await
            .unwrap_or(Err(IggyError::CannotEstablishConnection))
    } else {
        client.connect().await
    };
    if let Err(error) = connected {
        let _ = client.shutdown().await;
        return Err(Error::Internal(format!(
            "Failed to connect to Apache Iggy at {}: {error}",
            candidate.address
        )));
    }

    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        client
            .login_user(username, password)
            .await
            .map_err(|e| Error::Internal(format!("Failed to authenticate to Apache Iggy: {e}")))?;
    }

    Ok(client)
}

/// Logs the Apache Iggy cluster roster this client joined. Informational only: the SDK
/// handles leader redirection and failover, and a single-node server reports one node.
async fn log_cluster_topology(client: &IggyClient, config: &IggyConfig) {
    match with_timeout(config, client.get_cluster_metadata()).await {
        Ok(metadata) => {
            let leader = metadata.nodes.iter().find(|node| {
                node.role == ClusterNodeRole::Leader && node.status == ClusterNodeStatus::Healthy
            });
            info!(
                adapter = "iggy",
                cluster = %metadata.name,
                nodes = metadata.nodes.len(),
                leader = leader.map_or("none", |node| node.name.as_str()),
                durability = %config.durability,
                consumer_offset_durability = %config.consumer_offset_durability,
                "cluster topology discovered"
            );
        }
        Err(error) => {
            debug!(adapter = "iggy", error = %error, "cluster metadata unavailable");
        }
    }
}

fn to_iggy_durability(durability: IggyDurability) -> Durability {
    match durability {
        IggyDurability::Replicated => Durability::Replicated,
        IggyDurability::Persisted => Durability::Persisted,
    }
}

async fn build_producer(
    client: &IggyClient,
    config: &IggyConfig,
    stream: &str,
    topic: &str,
) -> Result<IggyProducer> {
    // Topics are created only by `ensure_topic`, so the configured durability always applies.
    ensure_topic(client, config, stream, topic).await?;
    let producer = client
        .producer(stream, topic)
        .map_err(to_iggy_error)?
        .partitioning(Partitioning::balanced())
        .do_not_create_stream_if_not_exists()
        .do_not_create_topic_if_not_exists()
        .build();
    with_timeout(config, producer.init()).await?;
    Ok(producer)
}

async fn build_consumer(
    client: &IggyClient,
    config: &IggyConfig,
    stream: &str,
    topic: &str,
    consumer_name: &str,
) -> Result<iggy::prelude::IggyConsumer> {
    let consumer = Consumer::new(
        consumer_name
            .try_into()
            .map_err(|e| Error::Config(format!("Invalid Apache Iggy consumer name: {e}")))?,
    );
    if config.start_from_latest {
        seed_latest_offsets(client, config, stream, topic, &consumer).await?;
    }

    let mut consumer = client
        .consumer(consumer_name, stream, topic, 0)
        .map_err(to_iggy_error)?
        .partition(None)
        .poll_interval(IggyDuration::from(Duration::from_millis(
            config.poll_interval_ms,
        )))
        .batch_length(config.poll_batch_size)
        .auto_commit(AutoCommit::Disabled)
        .polling_strategy(PollingStrategy::next())
        .build();
    with_timeout(config, consumer.init()).await?;
    Ok(consumer)
}

async fn seed_latest_offsets(
    client: &IggyClient,
    config: &IggyConfig,
    stream: &str,
    topic: &str,
    consumer: &Consumer,
) -> Result<()> {
    let stream_id = stream_identifier(stream)?;
    let topic_id = stream_identifier(topic)?;
    let Some(topic_details) = with_timeout(config, client.get_topic(&stream_id, &topic_id)).await?
    else {
        return Ok(());
    };

    for partition in topic_details.partitions {
        let existing_offset = with_timeout(
            config,
            client.get_consumer_offset(consumer, &stream_id, &topic_id, Some(partition.id)),
        )
        .await?;
        if existing_offset.is_some() {
            continue;
        }
        with_timeout(
            config,
            client.store_consumer_offset(
                consumer,
                &stream_id,
                &topic_id,
                Some(partition.id),
                partition.current_offset,
            ),
        )
        .await?;
    }
    Ok(())
}

async fn ensure_topic(
    client: &IggyClient,
    config: &IggyConfig,
    stream: &str,
    topic: &str,
) -> Result<()> {
    let stream_id = stream_identifier(stream)?;
    if client
        .get_stream(&stream_id)
        .await
        .map_err(to_iggy_error)?
        .is_none()
    {
        if !config.auto_create {
            return Err(Error::Config(format!(
                "Apache Iggy stream '{stream}' does not exist and auto_create is false"
            )));
        }
        match client.create_stream(stream).await {
            Ok(_) | Err(IggyError::StreamNameAlreadyExists(_)) => {}
            Err(error) => return Err(to_iggy_error(error)),
        }
    }

    let topic_id = stream_identifier(topic)?;
    if client
        .get_topic(&stream_id, &topic_id)
        .await
        .map_err(to_iggy_error)?
        .is_none()
    {
        if !config.auto_create {
            return Err(Error::Config(format!(
                "Apache Iggy topic '{topic}' does not exist and auto_create is false"
            )));
        }
        let options = TopicCreateOptions {
            partitions_count: Some(config.partitions_count),
            compression_algorithm: Some(CompressionAlgorithm::default()),
            message_expiry: Some(IggyExpiry::ServerDefault),
            max_topic_size: Some(MaxTopicSize::ServerDefault),
            durability: to_iggy_durability(config.durability),
            consumer_offset_durability: to_iggy_durability(config.consumer_offset_durability),
            ..Default::default()
        };
        match client.create_topic(&stream_id, topic, &options).await {
            Ok(_) | Err(IggyError::TopicNameAlreadyExists(_, _)) => {}
            Err(error) => return Err(to_iggy_error(error)),
        }
    }

    Ok(())
}

async fn publish_message<T: serde::Serialize>(
    config: &IggyConfig,
    producer: &IggyProducer,
    message: &T,
) -> Result<()> {
    let payload = sonic_rs::to_vec(message)
        .map_err(|e| Error::Other(format!("Failed to serialize Apache Iggy message: {e}")))?;
    let message = IggyMessage::builder()
        .payload(Bytes::from(payload))
        .build()
        .map_err(to_iggy_error)?;
    with_timeout(config, producer.send_one(message))
        .await
        .map_err(|e| Error::Internal(format!("Failed to publish Apache Iggy message: {e}")))?;

    trace!(adapter = "iggy", "message published to transport");
    Ok(())
}

async fn with_timeout<F, T>(config: &IggyConfig, future: F) -> Result<T>
where
    F: std::future::Future<Output = std::result::Result<T, IggyError>>,
{
    timeout(Duration::from_millis(config.request_timeout_ms), future)
        .await
        .map_err(|_| {
            Error::Internal(format!(
                "Apache Iggy request timed out after {} ms",
                config.request_timeout_ms
            ))
        })?
        .map_err(to_iggy_error)
}

fn stream_identifier(value: &str) -> Result<iggy::prelude::Identifier> {
    iggy::prelude::Identifier::named(value)
        .map_err(|e| Error::Config(format!("Invalid Apache Iggy identifier '{value}': {e}")))
}

fn validate_config(config: &IggyConfig) -> Result<()> {
    if config.connection_string.trim().is_empty() {
        return Err(Error::Config(
            "Apache Iggy connection_string must not be empty".to_string(),
        ));
    }
    if config.username.is_some() != config.password.is_some() {
        return Err(Error::Config(
            "Apache Iggy username and password must be configured together".to_string(),
        ));
    }
    config.connection_candidates().map_err(Error::Config)?;
    if !config.cluster_seeds.is_empty() && config.connect_timeout_ms == 0 {
        return Err(Error::Config(
            "Apache Iggy connect_timeout_ms must be greater than 0 when cluster_seeds is set"
                .to_string(),
        ));
    }
    if config.partitions_count == 0 {
        return Err(Error::Config(
            "Apache Iggy partitions_count must be greater than 0".to_string(),
        ));
    }
    if config.poll_batch_size == 0 {
        return Err(Error::Config(
            "Apache Iggy poll_batch_size must be greater than 0".to_string(),
        ));
    }
    for (field, value) in [
        ("stream", config.stream.as_str()),
        ("topic_prefix", config.topic_prefix.as_str()),
    ] {
        let normalized = normalize_name(value, "");
        if !normalized.is_empty() && normalized.chars().all(|ch| ch.is_ascii_digit()) {
            return Err(Error::Config(format!(
                "Apache Iggy {field} must not normalize to an all-digit name"
            )));
        }
    }
    Ok(())
}

async fn wait_before_retry(
    config: &IggyConfig,
    attempt: u32,
    shutdown: &Notify,
    is_running: &AtomicBool,
) {
    if !is_running.load(Ordering::Relaxed) {
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

fn stable_consumer_name(config: &IggyConfig) -> String {
    let raw = config
        .consumer_name
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| std::env::var("INSTANCE_PROCESS_ID").ok())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "sockudo".to_string());
    normalize_name(&raw, "sockudo")
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

fn to_iggy_error(error: IggyError) -> Error {
    Error::Internal(format!("Apache Iggy error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::PendingOffsets;

    #[test]
    fn pending_offsets_keep_only_highest_offset_per_partition() {
        let mut pending = PendingOffsets::default();

        pending.record((2, 10));
        pending.record((1, 20));
        pending.record((2, 15));
        pending.record((1, 18));

        assert_eq!(pending.offsets, vec![(2, 15), (1, 20)]);
    }
}
