use crate::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::horizontal_transport::{HorizontalTransport, TransportConfig, TransportHandlers};
use async_trait::async_trait;
use futures_util::StreamExt;
use lapin::message::Delivery;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions,
    QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::RabbitMqAdapterConfig;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

pub struct RabbitMqTransport {
    connection: Arc<Connection>,
    publish_channel: Channel,
    broadcast_exchange: String,
    request_exchange: String,
    response_exchange: String,
    config: RabbitMqAdapterConfig,
    shutdown: Arc<Notify>,
    is_running: Arc<AtomicBool>,
    owner_count: Arc<AtomicUsize>,
}

impl TransportConfig for RabbitMqAdapterConfig {
    fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }

    fn prefix(&self) -> &str {
        &self.prefix
    }
}

#[async_trait]
impl HorizontalTransport for RabbitMqTransport {
    type Config = RabbitMqAdapterConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        let connection = Connection::connect(&config.url, ConnectionProperties::default())
            .await
            .map_err(|e| Error::Internal(format!("Failed to connect to RabbitMQ: {e}")))?;
        let connection = Arc::new(connection);

        let publish_channel = connection
            .create_channel()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create RabbitMQ channel: {e}")))?;

        let broadcast_exchange = format!("{}.broadcast", config.prefix);
        let request_exchange = format!("{}.requests", config.prefix);
        let response_exchange = format!("{}.responses", config.prefix);

        for exchange in [&broadcast_exchange, &request_exchange, &response_exchange] {
            publish_channel
                .exchange_declare(
                    exchange.as_str().into(),
                    ExchangeKind::Fanout,
                    ExchangeDeclareOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to declare RabbitMQ exchange '{exchange}': {e}"
                    ))
                })?;
        }

        info!(
            "RabbitMQ transport initialized with exchanges: {}, {}, {}",
            broadcast_exchange, request_exchange, response_exchange
        );

        Ok(Self {
            connection,
            publish_channel,
            broadcast_exchange,
            request_exchange,
            response_exchange,
            config,
            shutdown: Arc::new(Notify::new()),
            is_running: Arc::new(AtomicBool::new(true)),
            owner_count: Arc::new(AtomicUsize::new(1)),
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        self.publish_to_exchange(&self.broadcast_exchange, message)
            .await
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        self.publish_to_exchange(&self.request_exchange, request)
            .await
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        self.publish_to_exchange(&self.response_exchange, response)
            .await
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        self.spawn_consumer(
            self.broadcast_exchange.clone(),
            "broadcast",
            handlers.on_broadcast.clone(),
        )
        .await?;

        self.spawn_request_consumer(
            self.request_exchange.clone(),
            self.response_exchange.clone(),
            handlers.on_request.clone(),
        )
        .await?;

        self.spawn_consumer(
            self.response_exchange.clone(),
            "response",
            handlers.on_response.clone(),
        )
        .await?;

        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        Ok(self.config.nodes_number.unwrap_or(2) as usize)
    }

    async fn check_health(&self) -> Result<()> {
        if self.connection.status().connected() {
            Ok(())
        } else {
            Err(Error::Internal(
                "RabbitMQ connection is not currently connected".to_string(),
            ))
        }
    }
}

impl RabbitMqTransport {
    async fn publish_to_exchange<T: serde::Serialize>(
        &self,
        exchange: &str,
        message: &T,
    ) -> Result<()> {
        let payload = sonic_rs::to_vec(message)
            .map_err(|e| Error::Other(format!("Failed to serialize RabbitMQ message: {e}")))?;

        self.publish_channel
            .basic_publish(
                exchange.into(),
                "".into(),
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default(),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to publish to RabbitMQ: {e}")))?
            .await
            .map_err(|e| Error::Internal(format!("RabbitMQ publish confirmation failed: {e}")))?;

        Ok(())
    }

    async fn spawn_consumer<T>(
        &self,
        exchange: String,
        kind: &'static str,
        handler: Arc<
            dyn Fn(T) -> crate::horizontal_transport::BoxFuture<'static, ()> + Send + Sync,
        >,
    ) -> Result<()>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let channel = self
            .connection
            .create_channel()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create RabbitMQ channel: {e}")))?;

        let queue = Self::declare_bound_queue(&channel, &exchange, kind).await?;
        let consumer_tag = format!("sockudo-{kind}-{}", uuid::Uuid::new_v4());
        let mut consumer = channel
            .basic_consume(
                queue.as_str().into(),
                consumer_tag.as_str().into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to start RabbitMQ consumer: {e}")))?;
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();

        info!("RabbitMQ transport consuming {kind} from queue {}", queue);

        tokio::spawn(async move {
            loop {
                if !is_running.load(Ordering::Relaxed) {
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
                    Ok(delivery) => {
                        Self::handle_delivery(delivery, &handler).await;
                    }
                    Err(e) => {
                        error!("RabbitMQ {kind} consumer error: {}", e);
                        break;
                    }
                }
            }

            warn!("RabbitMQ {kind} consumer loop ended");
        });

        Ok(())
    }

    async fn spawn_request_consumer(
        &self,
        exchange: String,
        response_exchange: String,
        handler: Arc<
            dyn Fn(
                    RequestBody,
                )
                    -> crate::horizontal_transport::BoxFuture<'static, Result<ResponseBody>>
                + Send
                + Sync,
        >,
    ) -> Result<()> {
        let channel = self
            .connection
            .create_channel()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create RabbitMQ channel: {e}")))?;

        let queue = Self::declare_bound_queue(&channel, &exchange, "request").await?;
        let consumer_tag = format!("sockudo-request-{}", uuid::Uuid::new_v4());
        let mut consumer = channel
            .basic_consume(
                queue.as_str().into(),
                consumer_tag.as_str().into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to start RabbitMQ consumer: {e}")))?;
        let shutdown = self.shutdown.clone();
        let is_running = self.is_running.clone();

        info!("RabbitMQ transport consuming requests from queue {}", queue);

        tokio::spawn(async move {
            loop {
                if !is_running.load(Ordering::Relaxed) {
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
                    Ok(delivery) => {
                        let response_channel = channel.clone();
                        let response_exchange = response_exchange.clone();

                        match sonic_rs::from_slice::<RequestBody>(&delivery.data) {
                            Ok(request) => match handler(request).await {
                                Ok(response) => {
                                    if let Ok(payload) = sonic_rs::to_vec(&response) {
                                        if let Err(e) = response_channel
                                            .basic_publish(
                                                response_exchange.as_str().into(),
                                                "".into(),
                                                BasicPublishOptions::default(),
                                                &payload,
                                                BasicProperties::default(),
                                            )
                                            .await
                                        {
                                            warn!("Failed to publish RabbitMQ response: {}", e);
                                        } else {
                                            debug!("Published RabbitMQ response");
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("RabbitMQ request handler failed: {}", e);
                                }
                            },
                            Err(e) => {
                                warn!("Failed to parse RabbitMQ request payload: {}", e);
                            }
                        }

                        if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                            warn!("Failed to ack RabbitMQ request delivery: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("RabbitMQ request consumer error: {}", e);
                        break;
                    }
                }
            }

            warn!("RabbitMQ request consumer loop ended");
        });

        Ok(())
    }

    async fn declare_bound_queue(channel: &Channel, exchange: &str, kind: &str) -> Result<String> {
        channel
            .exchange_declare(
                exchange.into(),
                ExchangeKind::Fanout,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to declare RabbitMQ exchange: {e}")))?;

        let queue = channel
            .queue_declare(
                "".into(),
                QueueDeclareOptions {
                    durable: false,
                    exclusive: true,
                    auto_delete: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to declare RabbitMQ queue: {e}")))?;

        channel
            .queue_bind(
                queue.name().as_str().into(),
                exchange.into(),
                "".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to bind RabbitMQ queue: {e}")))?;

        debug!(
            "RabbitMQ transport bound {} queue {} to exchange {}",
            kind,
            queue.name().as_str(),
            exchange
        );

        Ok(queue.name().as_str().to_string())
    }

    async fn handle_delivery<T>(
        delivery: Delivery,
        handler: &Arc<
            dyn Fn(T) -> crate::horizontal_transport::BoxFuture<'static, ()> + Send + Sync,
        >,
    ) where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        match sonic_rs::from_slice::<T>(&delivery.data) {
            Ok(message) => handler(message).await,
            Err(e) => warn!("Failed to parse RabbitMQ payload: {}", e),
        }

        if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
            warn!("Failed to ack RabbitMQ delivery: {}", e);
        }
    }
}

impl Clone for RabbitMqTransport {
    fn clone(&self) -> Self {
        self.owner_count.fetch_add(1, Ordering::Relaxed);
        Self {
            connection: self.connection.clone(),
            publish_channel: self.publish_channel.clone(),
            broadcast_exchange: self.broadcast_exchange.clone(),
            request_exchange: self.request_exchange.clone(),
            response_exchange: self.response_exchange.clone(),
            config: self.config.clone(),
            shutdown: self.shutdown.clone(),
            is_running: self.is_running.clone(),
            owner_count: self.owner_count.clone(),
        }
    }
}

impl Drop for RabbitMqTransport {
    fn drop(&mut self) {
        if self.owner_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.is_running.store(false, Ordering::Relaxed);
            self.shutdown.notify_waiters();
        }
    }
}
