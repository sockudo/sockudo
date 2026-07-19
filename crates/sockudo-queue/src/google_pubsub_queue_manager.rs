use crate::ArcJobProcessorFn;
use crate::broker_batch::prepare_default_batch;
use crate::worker_registry::WorkerRegistry;
use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use futures_util::future::try_join_all;
use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentialsBuilder;
use google_cloud_pubsub::client::{Publisher, Subscriber, SubscriptionAdmin, TopicAdmin};
use google_cloud_pubsub::model::Message;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{GooglePubSubAdapterConfig, QueueReliabilityConfig};
use sockudo_core::queue::{
    QueueBackendKind, QueueCapabilities, QueueInterface, QueueJobId, QueueJobRequest,
};
use sockudo_core::webhook_types::{JobData, JobProcessorFnAsync};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;
use tracing::{error, info};

pub struct GooglePubSubQueueManager {
    config: GooglePubSubAdapterConfig,
    topic_admin: TopicAdmin,
    subscription_admin: SubscriptionAdmin,
    subscriber: Subscriber,
    shutdown: Arc<Notify>,
    running: Arc<AtomicBool>,
    workers: WorkerRegistry,
    publishers: DashMap<String, Publisher>,
    provisioned_topics: DashSet<String>,
    provisioned_subscriptions: DashSet<String>,
    reliability: QueueReliabilityConfig,
}

impl GooglePubSubQueueManager {
    pub async fn new(config: GooglePubSubAdapterConfig) -> Result<Self> {
        Self::new_with_reliability(config, QueueReliabilityConfig::default()).await
    }

    pub async fn new_with_reliability(
        config: GooglePubSubAdapterConfig,
        reliability: QueueReliabilityConfig,
    ) -> Result<Self> {
        reliability.validate().map_err(Error::Config)?;
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
            workers: WorkerRegistry::default(),
            publishers: DashMap::new(),
            provisioned_topics: DashSet::new(),
            provisioned_subscriptions: DashSet::new(),
            reliability,
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
        if self.provisioned_topics.contains(topic) {
            return Ok(());
        }
        match self.topic_admin.create_topic().set_name(topic).send().await {
            Ok(_) => {
                self.provisioned_topics.insert(topic.to_string());
                Ok(())
            }
            Err(create_error) => self
                .topic_admin
                .get_topic()
                .set_topic(topic)
                .send()
                .await
                .map(|_| {
                    self.provisioned_topics.insert(topic.to_string());
                })
                .map_err(|_| {
                    Error::Queue(format!(
                        "Failed to ensure Google Pub/Sub topic {topic}: {create_error}"
                    ))
                }),
        }
    }

    async fn ensure_subscription(&self, subscription: &str, topic: &str) -> Result<()> {
        if self.provisioned_subscriptions.contains(subscription) {
            return Ok(());
        }
        match self
            .subscription_admin
            .create_subscription()
            .set_name(subscription)
            .set_topic(topic)
            .send()
            .await
        {
            Ok(_) => {
                self.provisioned_subscriptions
                    .insert(subscription.to_string());
                Ok(())
            }
            Err(create_error) => self
                .subscription_admin
                .get_subscription()
                .set_subscription(subscription)
                .send()
                .await
                .map(|_| {
                    self.provisioned_subscriptions
                        .insert(subscription.to_string());
                })
                .map_err(|_| Error::Queue(format!(
                    "Failed to ensure Google Pub/Sub subscription {subscription}: {create_error}"
                ))),
        }
    }

    async fn publisher_for(&self, topic: &str) -> Result<Publisher> {
        if let Some(publisher) = self.publishers.get(topic) {
            return Ok(publisher.clone());
        }
        let publisher = build_publisher(&self.config, topic).await?;
        Ok(self
            .publishers
            .entry(topic.to_string())
            .or_insert_with(|| publisher.clone())
            .clone())
    }

    async fn publish_batch(&self, queue_name: &str, jobs: Vec<JobData>) -> Result<()> {
        if jobs.is_empty() {
            return Ok(());
        }
        let topic = self.topic_name(queue_name);
        self.ensure_topic(&topic).await?;
        let publisher = self.publisher_for(&topic).await?;

        for chunk in jobs.chunks(self.reliability.max_batch_size) {
            let mut publishes = Vec::with_capacity(chunk.len());
            for data in chunk {
                let payload = sonic_rs::to_vec(data).map_err(|e| {
                    Error::Queue(format!("Failed to serialize Google Pub/Sub queue job: {e}"))
                })?;
                publishes.push(publisher.publish(Message::new().set_data(payload)));
            }
            try_join_all(publishes).await.map_err(|e| {
                Error::Queue(format!("Failed to publish Google Pub/Sub queue batch: {e}"))
            })?;
        }
        Ok(())
    }
}

#[async_trait]
impl QueueInterface for GooglePubSubQueueManager {
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
        let topic = self.topic_name(queue_name);
        let subscription = self.subscription_name(queue_name);
        self.ensure_topic(&topic).await?;
        self.ensure_subscription(&subscription, &topic).await?;
        let subscriber = self.subscriber.clone();
        let callback: ArcJobProcessorFn = Arc::from(callback);
        let shutdown = self.shutdown.clone();
        let running = self.running.clone();
        let prefetch = self.reliability.worker_prefetch;

        self.workers.spawn(async move {
            let mut stream = subscriber.subscribe(subscription).build();
            let mut in_flight = tokio::task::JoinSet::new();
            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }
                if in_flight.len() >= prefetch {
                    if let Some(Err(error)) = in_flight.join_next().await {
                        error!("Google Pub/Sub queue callback task failed: {error}");
                    }
                    continue;
                }
                let message = tokio::select! {
                    _ = shutdown.notified() => break,
                    completed = in_flight.join_next(), if !in_flight.is_empty() => {
                        if let Some(Err(error)) = completed {
                            error!("Google Pub/Sub queue callback task failed: {error}");
                        }
                        continue;
                    }
                    message = stream.next() => message,
                };
                let Some(message) = message else {
                    break;
                };
                match message {
                    Ok((message, ack_handler)) => {
                        let callback = callback.clone();
                        in_flight.spawn(async move {
                            match sonic_rs::from_slice::<JobData>(&message.data) {
                                Ok(job) => {
                                    if callback(job).await.is_ok() {
                                        ack_handler.ack();
                                    } else {
                                        ack_handler.nack();
                                    }
                                }
                                Err(error) => {
                                    error!(
                                        "Failed to deserialize Google Pub/Sub queue job: {error}"
                                    );
                                    ack_handler.ack();
                                }
                            }
                        });
                    }
                    Err(e) => {
                        error!("Google Pub/Sub queue consumer error: {}", e);
                        break;
                    }
                }
            }
            while let Some(result) = in_flight.join_next().await {
                if let Err(error) = result {
                    error!("Google Pub/Sub queue callback task failed while draining: {error}");
                }
            }
            info!("Google Pub/Sub queue consumer stopped");
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
        self.topic_admin
            .list_topics()
            .set_project(format!("projects/{}", self.config.project_id))
            .set_page_size(1)
            .send()
            .await
            .map(|_| ())
            .map_err(|e| Error::Queue(format!("Google Pub/Sub queue health check failed: {e}")))
    }

    fn backend(&self) -> QueueBackendKind {
        QueueBackendKind::GooglePubSub
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
