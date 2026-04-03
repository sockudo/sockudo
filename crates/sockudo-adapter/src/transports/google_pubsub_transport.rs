use crate::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::horizontal_transport::{HorizontalTransport, TransportConfig, TransportHandlers};
use async_trait::async_trait;
use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
use google_cloud_pubsub::client::{Publisher, Subscriber, SubscriptionAdmin, TopicAdmin};
use google_cloud_pubsub::model::Message;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::GooglePubSubAdapterConfig;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{debug, error, info, warn};

pub struct GooglePubSubTransport {
    broadcast_publisher: Publisher,
    request_publisher: Publisher,
    response_publisher: Publisher,
    subscriber: Subscriber,
    subscription_admin: SubscriptionAdmin,
    broadcast_subscription: String,
    request_subscription: String,
    response_subscription: String,
    config: GooglePubSubAdapterConfig,
    owner_count: Arc<AtomicUsize>,
}

impl TransportConfig for GooglePubSubAdapterConfig {
    fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }

    fn prefix(&self) -> &str {
        &self.prefix
    }
}

#[async_trait]
impl HorizontalTransport for GooglePubSubTransport {
    type Config = GooglePubSubAdapterConfig;

    async fn new(config: Self::Config) -> Result<Self> {
        if config.project_id.trim().is_empty() {
            return Err(Error::Internal(
                "Google Pub/Sub project_id must not be empty".to_string(),
            ));
        }

        let prefix = normalize_resource_id(&config.prefix);
        let listener_id = uuid::Uuid::new_v4().simple().to_string();

        let broadcast_topic = topic_name(&config.project_id, &prefix, "broadcast");
        let request_topic = topic_name(&config.project_id, &prefix, "requests");
        let response_topic = topic_name(&config.project_id, &prefix, "responses");
        let broadcast_subscription =
            subscription_name(&config.project_id, &prefix, "broadcast", &listener_id);
        let request_subscription =
            subscription_name(&config.project_id, &prefix, "requests", &listener_id);
        let response_subscription =
            subscription_name(&config.project_id, &prefix, "responses", &listener_id);

        let topic_admin = build_topic_admin(&config).await?;
        let subscription_admin = build_subscription_admin(&config).await?;
        let subscriber = build_subscriber(&config).await?;

        for topic in [&broadcast_topic, &request_topic, &response_topic] {
            ensure_topic(&topic_admin, topic).await?;
        }

        for (subscription, topic) in [
            (&broadcast_subscription, &broadcast_topic),
            (&request_subscription, &request_topic),
            (&response_subscription, &response_topic),
        ] {
            ensure_subscription(&subscription_admin, subscription, topic).await?;
        }

        let broadcast_publisher = build_publisher(&config, &broadcast_topic).await?;
        let request_publisher = build_publisher(&config, &request_topic).await?;
        let response_publisher = build_publisher(&config, &response_topic).await?;

        info!(
            "Google Pub/Sub transport initialized with topics: {}, {}, {}",
            broadcast_topic, request_topic, response_topic
        );

        Ok(Self {
            broadcast_publisher,
            request_publisher,
            response_publisher,
            subscriber,
            subscription_admin,
            broadcast_subscription,
            request_subscription,
            response_subscription,
            config,
            owner_count: Arc::new(AtomicUsize::new(1)),
        })
    }

    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()> {
        publish_message(&self.broadcast_publisher, message).await
    }

    async fn publish_request(&self, request: &RequestBody) -> Result<()> {
        publish_message(&self.request_publisher, request).await
    }

    async fn publish_response(&self, response: &ResponseBody) -> Result<()> {
        publish_message(&self.response_publisher, response).await
    }

    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()> {
        self.spawn_consumer(
            self.broadcast_subscription.clone(),
            "broadcast",
            handlers.on_broadcast.clone(),
        );

        self.spawn_request_consumer(
            self.request_subscription.clone(),
            handlers.on_request.clone(),
        );

        self.spawn_consumer(
            self.response_subscription.clone(),
            "response",
            handlers.on_response.clone(),
        );

        Ok(())
    }

    async fn get_node_count(&self) -> Result<usize> {
        Ok(self.config.nodes_number.unwrap_or(1) as usize)
    }

    async fn check_health(&self) -> Result<()> {
        if self.config.project_id.trim().is_empty() {
            return Err(Error::Internal(
                "Google Pub/Sub project_id must not be empty".to_string(),
            ));
        }

        Ok(())
    }
}

impl GooglePubSubTransport {
    fn spawn_consumer<T>(
        &self,
        subscription: String,
        kind: &'static str,
        handler: Arc<
            dyn Fn(T) -> crate::horizontal_transport::BoxFuture<'static, ()> + Send + Sync,
        >,
    ) where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let subscriber = self.subscriber.clone();
        tokio::spawn(async move {
            let mut stream = subscriber.subscribe(subscription).build();

            while let Some(message) = stream.next().await {
                match message {
                    Ok((message, ack_handler)) => match sonic_rs::from_slice::<T>(&message.data) {
                        Ok(payload) => {
                            handler(payload).await;
                            ack_handler.ack();
                        }
                        Err(error) => {
                            warn!("Failed to parse Google Pub/Sub {kind} payload: {}", error);
                            ack_handler.ack();
                        }
                    },
                    Err(error) => {
                        error!("Google Pub/Sub {kind} consumer error: {}", error);
                        break;
                    }
                }
            }

            warn!("Google Pub/Sub {kind} consumer loop ended");
        });
    }

    fn spawn_request_consumer(
        &self,
        subscription: String,
        handler: Arc<
            dyn Fn(
                    RequestBody,
                )
                    -> crate::horizontal_transport::BoxFuture<'static, Result<ResponseBody>>
                + Send
                + Sync,
        >,
    ) {
        let subscriber = self.subscriber.clone();
        let response_publisher = self.response_publisher.clone();

        tokio::spawn(async move {
            let mut stream = subscriber.subscribe(subscription).build();

            while let Some(message) = stream.next().await {
                match message {
                    Ok((message, ack_handler)) => {
                        match sonic_rs::from_slice::<RequestBody>(&message.data) {
                            Ok(request) => match handler(request).await {
                                Ok(response) => {
                                    if let Err(error) =
                                        publish_message(&response_publisher, &response).await
                                    {
                                        warn!(
                                            "Failed to publish Google Pub/Sub response: {}",
                                            error
                                        );
                                    }
                                    ack_handler.ack();
                                }
                                Err(error) => {
                                    warn!("Google Pub/Sub request handler failed: {}", error);
                                    ack_handler.ack();
                                }
                            },
                            Err(error) => {
                                warn!("Failed to parse Google Pub/Sub request payload: {}", error);
                                ack_handler.ack();
                            }
                        }
                    }
                    Err(error) => {
                        error!("Google Pub/Sub request consumer error: {}", error);
                        break;
                    }
                }
            }

            warn!("Google Pub/Sub request consumer loop ended");
        });
    }
}

impl Drop for GooglePubSubTransport {
    fn drop(&mut self) {
        if self.owner_count.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }

        let subscription_admin = self.subscription_admin.clone();
        let subscriptions = vec![
            self.broadcast_subscription.clone(),
            self.request_subscription.clone(),
            self.response_subscription.clone(),
        ];

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                for subscription in subscriptions {
                    if let Err(error) = subscription_admin
                        .delete_subscription()
                        .set_subscription(&subscription)
                        .send()
                        .await
                    {
                        warn!(
                            "Failed to delete Google Pub/Sub subscription '{}': {}",
                            subscription, error
                        );
                    }
                }
            });
        } else {
            warn!("No Tokio runtime available to clean up Google Pub/Sub subscriptions");
        }
    }
}

impl Clone for GooglePubSubTransport {
    fn clone(&self) -> Self {
        self.owner_count.fetch_add(1, Ordering::Relaxed);
        Self {
            broadcast_publisher: self.broadcast_publisher.clone(),
            request_publisher: self.request_publisher.clone(),
            response_publisher: self.response_publisher.clone(),
            subscriber: self.subscriber.clone(),
            subscription_admin: self.subscription_admin.clone(),
            broadcast_subscription: self.broadcast_subscription.clone(),
            request_subscription: self.request_subscription.clone(),
            response_subscription: self.response_subscription.clone(),
            config: self.config.clone(),
            owner_count: self.owner_count.clone(),
        }
    }
}

async fn publish_message<T: serde::Serialize>(publisher: &Publisher, message: &T) -> Result<()> {
    let payload = sonic_rs::to_vec(message)
        .map_err(|e| Error::Other(format!("Failed to serialize Google Pub/Sub message: {e}")))?;

    publisher
        .publish(Message::new().set_data(payload))
        .await
        .map_err(|e| Error::Internal(format!("Failed to publish Google Pub/Sub message: {e}")))?;

    debug!("Published Google Pub/Sub message");
    Ok(())
}

async fn ensure_topic(topic_admin: &TopicAdmin, topic: &str) -> Result<()> {
    match topic_admin.create_topic().set_name(topic).send().await {
        Ok(_) => Ok(()),
        Err(create_error) => topic_admin
            .get_topic()
            .set_topic(topic)
            .send()
            .await
            .map(|_| ())
            .map_err(|_| {
                Error::Internal(format!(
                    "Failed to ensure Google Pub/Sub topic '{topic}': {create_error}"
                ))
            }),
    }
}

async fn ensure_subscription(
    subscription_admin: &SubscriptionAdmin,
    subscription: &str,
    topic: &str,
) -> Result<()> {
    match subscription_admin
        .create_subscription()
        .set_name(subscription)
        .set_topic(topic)
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(create_error) => subscription_admin
            .get_subscription()
            .set_subscription(subscription)
            .send()
            .await
            .map(|_| ())
            .map_err(|_| {
                Error::Internal(format!(
                    "Failed to ensure Google Pub/Sub subscription '{subscription}': {create_error}"
                ))
            }),
    }
}

async fn build_publisher(config: &GooglePubSubAdapterConfig, topic: &str) -> Result<Publisher> {
    let mut builder = Publisher::builder(topic.to_string());

    if let Some(endpoint) = emulator_endpoint(config) {
        builder = builder
            .with_endpoint(endpoint)
            .with_credentials(AnonymousCredentialsBuilder::new().build());
    }

    builder
        .build()
        .await
        .map_err(|e| Error::Internal(format!("Failed to create Google Pub/Sub publisher: {e}")))
}

async fn build_subscriber(config: &GooglePubSubAdapterConfig) -> Result<Subscriber> {
    let mut builder = Subscriber::builder();

    if let Some(endpoint) = emulator_endpoint(config) {
        builder = builder
            .with_endpoint(endpoint)
            .with_credentials(AnonymousCredentialsBuilder::new().build());
    }

    builder
        .build()
        .await
        .map_err(|e| Error::Internal(format!("Failed to create Google Pub/Sub subscriber: {e}")))
}

async fn build_topic_admin(config: &GooglePubSubAdapterConfig) -> Result<TopicAdmin> {
    let mut builder = TopicAdmin::builder();

    if let Some(endpoint) = emulator_endpoint(config) {
        builder = builder
            .with_endpoint(endpoint)
            .with_credentials(AnonymousCredentialsBuilder::new().build());
    }

    builder
        .build()
        .await
        .map_err(|e| Error::Internal(format!("Failed to create Google Pub/Sub topic admin: {e}")))
}

async fn build_subscription_admin(config: &GooglePubSubAdapterConfig) -> Result<SubscriptionAdmin> {
    let mut builder = SubscriptionAdmin::builder();

    if let Some(endpoint) = emulator_endpoint(config) {
        builder = builder
            .with_endpoint(endpoint)
            .with_credentials(AnonymousCredentialsBuilder::new().build());
    }

    builder.build().await.map_err(|e| {
        Error::Internal(format!(
            "Failed to create Google Pub/Sub subscription admin: {e}"
        ))
    })
}

fn emulator_endpoint(config: &GooglePubSubAdapterConfig) -> Option<String> {
    config.emulator_host.as_ref().map(|host| {
        if host.starts_with("http://") || host.starts_with("https://") {
            host.clone()
        } else {
            format!("http://{host}")
        }
    })
}

fn topic_name(project_id: &str, prefix: &str, suffix: &str) -> String {
    format!("projects/{project_id}/topics/{prefix}-{suffix}")
}

fn subscription_name(project_id: &str, prefix: &str, suffix: &str, listener_id: &str) -> String {
    format!("projects/{project_id}/subscriptions/{prefix}-{suffix}-{listener_id}")
}

fn normalize_resource_id(value: &str) -> String {
    let normalized: String = value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | '0'..='9' | '-' => ch,
            'A'..='Z' => ch.to_ascii_lowercase(),
            _ => '-',
        })
        .collect();

    let trimmed = normalized.trim_matches('-');
    if trimmed.is_empty() {
        "sockudo".to_string()
    } else {
        trimmed.to_string()
    }
}
