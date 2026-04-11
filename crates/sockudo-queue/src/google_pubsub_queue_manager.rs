use crate::ArcJobProcessorFn;
use async_trait::async_trait;
use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
use google_cloud_pubsub::client::{Publisher, Subscriber, SubscriptionAdmin, TopicAdmin};
use google_cloud_pubsub::model::Message;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::GooglePubSubAdapterConfig;
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;
use tracing::{error, info, warn};

pub struct GooglePubSubQueueManager {
    config: GooglePubSubAdapterConfig,
    topic_admin: TopicAdmin,
    subscription_admin: SubscriptionAdmin,
    subscriber: Subscriber,
    shutdown: Arc<Notify>,
    running: Arc<AtomicBool>,
}

impl GooglePubSubQueueManager {
    pub async fn new(config: GooglePubSubAdapterConfig) -> Result<Self> {
        if config.project_id.trim().is_empty() {
            return Err(Error::Queue(
                "Google Pub/Sub queue project_id must not be empty".to_string(),
            ));
        }

        Ok(Self {
            topic_admin: build_topic_admin(&config).await?,
            subscription_admin: build_subscription_admin(&config).await?,
            subscriber: build_subscriber(&config).await?,
            config,
            shutdown: Arc::new(Notify::new()),
            running: Arc::new(AtomicBool::new(true)),
        })
    }

    fn prefix(&self) -> String {
        normalize_resource_id(&self.config.prefix)
    }

    fn topic_name(&self, queue_name: &str) -> String {
        format!(
            "projects/{}/topics/{}-queue-{}",
            self.config.project_id,
            self.prefix(),
            normalize_resource_id(queue_name)
        )
    }

    fn subscription_name(&self, queue_name: &str) -> String {
        format!(
            "projects/{}/subscriptions/{}-queue-workers-{}",
            self.config.project_id,
            self.prefix(),
            normalize_resource_id(queue_name)
        )
    }

    async fn ensure_topic(&self, topic: &str) -> Result<()> {
        match self.topic_admin.create_topic().set_name(topic).send().await {
            Ok(_) => Ok(()),
            Err(create_error) => self
                .topic_admin
                .get_topic()
                .set_topic(topic)
                .send()
                .await
                .map(|_| ())
                .map_err(|_| {
                    Error::Queue(format!(
                        "Failed to ensure Google Pub/Sub topic {topic}: {create_error}"
                    ))
                }),
        }
    }

    async fn ensure_subscription(&self, subscription: &str, topic: &str) -> Result<()> {
        match self
            .subscription_admin
            .create_subscription()
            .set_name(subscription)
            .set_topic(topic)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(create_error) => self
                .subscription_admin
                .get_subscription()
                .set_subscription(subscription)
                .send()
                .await
                .map(|_| ())
                .map_err(|_| Error::Queue(format!(
                    "Failed to ensure Google Pub/Sub subscription {subscription}: {create_error}"
                ))),
        }
    }
}

#[async_trait]
impl QueueInterface for GooglePubSubQueueManager {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> Result<()> {
        let topic = self.topic_name(queue_name);
        self.ensure_topic(&topic).await?;
        let publisher = build_publisher(&self.config, &topic).await?;
        let payload = sonic_rs::to_vec(&data).map_err(|e| {
            Error::Queue(format!("Failed to serialize Google Pub/Sub queue job: {e}"))
        })?;
        let _message_id = publisher
            .publish(Message::new().set_data(payload))
            .await
            .map_err(|e| {
                Error::Queue(format!("Failed to publish Google Pub/Sub queue job: {e}"))
            })?;
        Ok(())
    }

    async fn process_queue(&self, queue_name: &str, callback: JobProcessorFnAsync) -> Result<()> {
        let topic = self.topic_name(queue_name);
        let subscription = self.subscription_name(queue_name);
        self.ensure_topic(&topic).await?;
        self.ensure_subscription(&subscription, &topic).await?;
        let subscriber = self.subscriber.clone();
        let callback: ArcJobProcessorFn = Arc::from(callback);
        let shutdown = self.shutdown.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            let mut stream = subscriber.subscribe(subscription).build();
            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }
                let message = tokio::select! {
                    _ = shutdown.notified() => break,
                    message = stream.next() => message,
                };
                let Some(message) = message else {
                    break;
                };
                match message {
                    Ok((message, ack_handler)) => {
                        match sonic_rs::from_slice::<JobData>(&message.data) {
                            Ok(job) => {
                                if callback(job).await.is_ok() {
                                    ack_handler.ack();
                                } else {
                                    warn!(
                                        "Google Pub/Sub queue job failed; leaving message unacked for retry"
                                    );
                                }
                            }
                            Err(e) => {
                                error!("Failed to deserialize Google Pub/Sub queue job: {}", e);
                                ack_handler.ack();
                            }
                        }
                    }
                    Err(e) => {
                        error!("Google Pub/Sub queue consumer error: {}", e);
                        break;
                    }
                }
            }
            info!("Google Pub/Sub queue consumer stopped");
        });

        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        self.shutdown.notify_waiters();
        Ok(())
    }

    async fn check_health(&self) -> Result<()> {
        if self.config.project_id.trim().is_empty() {
            return Err(Error::Queue(
                "Google Pub/Sub queue project_id must not be empty".to_string(),
            ));
        }
        Ok(())
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
        .map_err(|e| Error::Queue(format!("Failed to build Google Pub/Sub publisher: {e}")))
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
        .map_err(|e| Error::Queue(format!("Failed to build Google Pub/Sub subscriber: {e}")))
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
        .map_err(|e| Error::Queue(format!("Failed to build Google Pub/Sub topic admin: {e}")))
}

async fn build_subscription_admin(config: &GooglePubSubAdapterConfig) -> Result<SubscriptionAdmin> {
    let mut builder = SubscriptionAdmin::builder();
    if let Some(endpoint) = emulator_endpoint(config) {
        builder = builder
            .with_endpoint(endpoint)
            .with_credentials(AnonymousCredentialsBuilder::new().build());
    }
    builder.build().await.map_err(|e| {
        Error::Queue(format!(
            "Failed to build Google Pub/Sub subscription admin: {e}"
        ))
    })
}

fn emulator_endpoint(config: &GooglePubSubAdapterConfig) -> Option<String> {
    config
        .emulator_host
        .as_ref()
        .map(|host| format!("http://{host}"))
}

fn normalize_resource_id(value: &str) -> String {
    let normalized = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    normalized.trim_matches('-').chars().take(255).collect()
}
