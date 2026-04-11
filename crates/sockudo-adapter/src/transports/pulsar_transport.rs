use crate::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::horizontal_transport::{HorizontalTransport, TransportConfig, TransportHandlers};
use async_trait::async_trait;
use futures_util::StreamExt;
use pulsar::{Authentication, Consumer, Producer, Pulsar, SubType, TokioExecutor};
use sockudo_core::error::{Error, Result};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::PulsarAdapterConfig;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Mutex, Notify};
use tracing::{debug, error, info, warn};

pub struct PulsarTransport {
    client: Pulsar<TokioExecutor>,
    broadcast_producer: Arc<Mutex<Producer<TokioExecutor>>>,
    request_producer: Arc<Mutex<Producer<TokioExecutor>>>,
    response_producer: Arc<Mutex<Producer<TokioExecutor>>>,
    broadcast_topic: String,
    request_topic: String,
    response_topic: String,
    broadcast_subscription: String,
    request_subscription: String,
    response_subscription: String,
    config: PulsarAdapterConfig,
    metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
    shutdown: Arc<Notify>,
    is_running: Arc<AtomicBool>,
    owner_count: Arc<AtomicUsize>,
}

impl TransportConfig for PulsarAdapterConfig {
    fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }

    fn prefix(&self) -> &str {
        &self.prefix
    }
}

#[async_trait]
impl HorizontalTransport for PulsarTransport {
    type Config = PulsarAdapterConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        let mut builder = Pulsar::builder(config.url.clone(), TokioExecutor);

        if let Some(token) = config.token.as_ref() {
            builder = builder.with_auth(Authentication {
                name: "token".to_string(),
                data: token.clone().into_bytes(),
            });
        }

        let client: Pulsar<_> = builder
            .build()
            .await
            .map_err(|e| Error::Internal(format!("Failed to connect to Pulsar: {e}")))?;

        let topic_prefix = normalize_topic_prefix(&config.prefix);
        let subscription_prefix = normalize_subscription_prefix(&config.prefix);
        let listener_id = uuid::Uuid::new_v4().simple().to_string();

        let broadcast_topic = format!("{topic_prefix}-broadcast");
        let request_topic = format!("{topic_prefix}-requests");
        let response_topic = format!("{topic_prefix}-responses");
        let broadcast_subscription = format!("{subscription_prefix}-broadcast-{listener_id}");
        let request_subscription = format!("{subscription_prefix}-requests-{listener_id}");
        let response_subscription = format!("{subscription_prefix}-responses-{listener_id}");

        let broadcast_producer = client
            .producer()
            .with_topic(&broadcast_topic)
            .with_name(format!("sockudo-broadcast-{listener_id}"))
            .build()
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to create Pulsar broadcast producer: {e}"))
            })?;
        let request_producer = client
            .producer()
            .with_topic(&request_topic)
            .with_name(format!("sockudo-request-{listener_id}"))
            .build()
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to create Pulsar request producer: {e}"))
            })?;
        let response_producer = client
            .producer()
            .with_topic(&response_topic)
            .with_name(format!("sockudo-response-{listener_id}"))
            .build()
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to create Pulsar response producer: {e}"))
            })?;

        info!(
            "Pulsar transport initialized with topics: {}, {}, {}",
            broadcast_topic, request_topic, response_topic
        );

        Ok(Self {
            client,
            broadcast_producer: Arc::new(Mutex::new(broadcast_producer)),
            request_producer: Arc::new(Mutex::new(request_producer)),
            response_producer: Arc::new(Mutex::new(response_producer)),
            broadcast_topic,
            request_topic,
            response_topic,
            broadcast_subscription,
            request_subscription,
            response_subscription,
            config,
            metrics: Arc::new(OnceLock::new()),
            shutdown: Arc::new(Notify::new()),
            is_running: Arc::new(AtomicBool::new(true)),
            owner_count: Arc::new(AtomicUsize::new(1)),
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        publish_message(&self.broadcast_producer, message).await
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        publish_message(&self.request_producer, request).await
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        publish_message(&self.response_producer, response).await
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        let broadcast_consumer = self
            .build_consumer(&self.broadcast_topic, &self.broadcast_subscription)
            .await?;
        let request_consumer = self
            .build_consumer(&self.request_topic, &self.request_subscription)
            .await?;
        let response_consumer = self
            .build_consumer(&self.response_topic, &self.response_subscription)
            .await?;

        self.spawn_consumer(
            "broadcast",
            broadcast_consumer,
            handlers.on_broadcast.clone(),
        );
        self.spawn_request_consumer(request_consumer, handlers.on_request.clone());
        self.spawn_consumer("response", response_consumer, handlers.on_response.clone());

        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        Ok(self.config.nodes_number.unwrap_or(1) as usize)
    }

    async fn check_health(&self) -> Result<()> {
        let producer = self.broadcast_producer.lock().await;
        producer
            .check_connection()
            .await
            .map_err(|e| Error::Internal(format!("Pulsar health check failed: {e}")))
    }

    fn set_metrics(&self, metrics: Arc<dyn MetricsInterface + Send + Sync>) {
        let _ = self.metrics.set(metrics);
    }
}

impl PulsarTransport {
    async fn build_consumer(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Result<Consumer<Vec<u8>, TokioExecutor>> {
        self.client
            .consumer()
            .with_topic(topic)
            .with_consumer_name(subscription)
            .with_subscription_type(SubType::Exclusive)
            .with_subscription(subscription)
            .build()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create Pulsar consumer: {e}")))
    }

    fn spawn_consumer<T>(
        &self,
        kind: &'static str,
        mut consumer: Consumer<Vec<u8>, TokioExecutor>,
        handler: Arc<
            dyn Fn(T) -> crate::horizontal_transport::BoxFuture<'static, ()> + Send + Sync,
        >,
    ) where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            loop {
                if !is_running.load(Ordering::Relaxed) {
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
                    Ok(message) => {
                        match sonic_rs::from_slice::<T>(&message.payload.data) {
                            Ok(payload) => handler(payload).await,
                            Err(error) => {
                                if let Some(metrics) = metrics.get() {
                                    metrics.mark_horizontal_transport_message_dropped("pulsar");
                                }
                                warn!("Failed to parse Pulsar {kind} payload: {}", error);
                            }
                        }

                        if let Err(error) = consumer.ack(&message).await {
                            warn!("Failed to ack Pulsar {kind} message: {}", error);
                        }
                    }
                    Err(error) => {
                        error!("Pulsar {kind} consumer error: {}", error);
                        break;
                    }
                }
            }

            let _ = consumer.unsubscribe().await;
            let _ = consumer.close().await;
            warn!("Pulsar {kind} consumer loop ended");
        });
    }

    fn spawn_request_consumer(
        &self,
        mut consumer: Consumer<Vec<u8>, TokioExecutor>,
        handler: Arc<
            dyn Fn(
                    RequestBody,
                )
                    -> crate::horizontal_transport::BoxFuture<'static, Result<ResponseBody>>
                + Send
                + Sync,
        >,
    ) {
        let response_producer = self.response_producer.clone();
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            loop {
                if !is_running.load(Ordering::Relaxed) {
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
                    Ok(message) => {
                        match sonic_rs::from_slice::<RequestBody>(&message.payload.data) {
                            Ok(request) => match handler(request).await {
                                Ok(response) => {
                                    if let Err(error) =
                                        publish_message(&response_producer, &response).await
                                    {
                                        warn!("Failed to publish Pulsar response: {}", error);
                                    }
                                }
                                Err(error) => {
                                    warn!("Pulsar request handler failed: {}", error);
                                }
                            },
                            Err(error) => {
                                if let Some(metrics) = metrics.get() {
                                    metrics.mark_horizontal_transport_message_dropped("pulsar");
                                }
                                warn!("Failed to parse Pulsar request payload: {}", error);
                            }
                        }

                        if let Err(error) = consumer.ack(&message).await {
                            warn!("Failed to ack Pulsar request message: {}", error);
                        }
                    }
                    Err(error) => {
                        error!("Pulsar request consumer error: {}", error);
                        break;
                    }
                }
            }

            let _ = consumer.unsubscribe().await;
            let _ = consumer.close().await;
            warn!("Pulsar request consumer loop ended");
        });
    }
}

async fn publish_message<T: serde::Serialize>(
    producer: &Arc<Mutex<Producer<TokioExecutor>>>,
    message: &T,
) -> Result<()> {
    let payload = sonic_rs::to_vec(message)
        .map_err(|e| Error::Other(format!("Failed to serialize Pulsar message: {e}")))?;

    let mut producer = producer.lock().await;
    let receipt = producer
        .send_non_blocking(payload)
        .await
        .map_err(|e| Error::Internal(format!("Failed to enqueue Pulsar message: {e}")))?;
    receipt
        .await
        .map_err(|e| Error::Internal(format!("Failed to publish Pulsar message: {e}")))?;

    debug!("Published Pulsar message");
    Ok(())
}

fn normalize_topic_prefix(prefix: &str) -> String {
    if prefix.contains("://") {
        return prefix.trim_end_matches(['-', '/']).to_string();
    }

    let sanitized = prefix
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();

    format!("persistent://public/default/{sanitized}")
}

fn normalize_subscription_prefix(prefix: &str) -> String {
    prefix
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

impl Drop for PulsarTransport {
    fn drop(&mut self) {
        if self.owner_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.is_running.store(false, Ordering::Relaxed);
            self.shutdown.notify_waiters();
        }
    }
}

impl Clone for PulsarTransport {
    fn clone(&self) -> Self {
        self.owner_count.fetch_add(1, Ordering::Relaxed);
        Self {
            client: self.client.clone(),
            broadcast_producer: self.broadcast_producer.clone(),
            request_producer: self.request_producer.clone(),
            response_producer: self.response_producer.clone(),
            broadcast_topic: self.broadcast_topic.clone(),
            request_topic: self.request_topic.clone(),
            response_topic: self.response_topic.clone(),
            broadcast_subscription: self.broadcast_subscription.clone(),
            request_subscription: self.request_subscription.clone(),
            response_subscription: self.response_subscription.clone(),
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            shutdown: self.shutdown.clone(),
            is_running: self.is_running.clone(),
            owner_count: self.owner_count.clone(),
        }
    }
}
