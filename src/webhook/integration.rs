// src/webhook/integration.rs
use crate::app::config::App;
use crate::app::manager::AppManager;
use crate::error::{Error, Result};
use crate::log::Log;
use crate::queue::manager::{QueueManager, QueueManagerFactory};
use crate::webhook::sender::WebhookSender;
use crate::webhook::types::{JobData, JobPayload, Webhook};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::interval;

/// Configuration for the webhook integration
#[derive(Debug, Clone)]
pub struct WebhookConfig {
    pub enabled: bool,
    pub batching: BatchingConfig,
    pub queue_driver: String,
    pub redis_url: Option<String>,
    pub redis_prefix: Option<String>,
    pub redis_concurrency: Option<usize>,
    pub process_id: String,
    pub debug: bool,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            batching: BatchingConfig::default(),
            queue_driver: "redis".to_string(),
            redis_url: None,
            redis_prefix: None,
            redis_concurrency: Some(5),
            process_id: uuid::Uuid::new_v4().to_string(),
            debug: false,
        }
    }
}

/// Configuration for webhook batching
#[derive(Debug, Clone)]
pub struct BatchingConfig {
    pub enabled: bool,
    pub duration: u64,
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            duration: 50,
        }
    }
}

// Type for callback function that processes jobs
pub type JobProcessorFnAsync = Box<
    dyn Fn(JobData) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static,
>;

/// Webhook integration for processing events
pub struct WebhookIntegration {
    config: WebhookConfig,
    batched_webhooks: Arc<Mutex<HashMap<String, Vec<JobData>>>>,
    queue_manager: Option<Arc<Mutex<QueueManager>>>,
    app_manager: Arc<dyn AppManager + Send + Sync>,
}

impl WebhookIntegration {
    /// Create a new webhook integration
    pub async fn new(
        config: WebhookConfig,
        app_manager: Arc<dyn AppManager + Send + Sync>,
    ) -> Result<Self> {
        // Store app_manager reference
        let mut integration = Self {
            config,
            batched_webhooks: Arc::new(Mutex::new(HashMap::new())),
            queue_manager: None,
            app_manager,
        };

        // Initialize queue manager if webhooks are enabled
        if integration.config.enabled {
            integration.init_queue_manager().await?;
        }

        Ok(integration)
    }

    /// Initialize the queue manager
    async fn init_queue_manager(&mut self) -> Result<()> {
        if self.config.enabled {
            // Create queue manager
            let driver = QueueManagerFactory::create(
                &self.config.queue_driver,
                self.config.redis_url.as_deref(),
                self.config.redis_prefix.as_deref(),
                self.config.redis_concurrency,
            )
            .await?;

            // Create queue manager wrapper
            let queue_manager = Arc::new(Mutex::new(QueueManager::new(driver)));

            // Create webhook sender with app_manager
            let webhook_sender = Arc::new(WebhookSender::new(self.app_manager.clone()));

            // Process queue
            let queue_name = "webhooks".to_string();
            let sender = webhook_sender.clone();

            let processor: JobProcessorFnAsync = Box::new(move |job_data| {
                let sender_clone = sender.clone();
                Box::pin(async move {
                    Log::webhook_sender(format!("Processing webhook job: {:?}", job_data));
                    sender_clone.process_webhook_job(job_data).await
                })
            });

            // Register processor for queue
            {
                let mut manager = queue_manager.lock().await;
                manager.process_queue(&queue_name, processor).await?;
            }

            // Store queue manager
            self.queue_manager = Some(queue_manager);

            // If batching is enabled, start background task
            if self.config.batching.enabled {
                self.start_batching_task();
            }
        }

        Ok(())
    }

    /// Start task to process batched webhooks
    fn start_batching_task(&self) {
        if !self.config.batching.enabled {
            return;
        }

        // Clone data for task
        let queue_manager = self.queue_manager.clone();
        let batched_webhooks = self.batched_webhooks.clone();
        let duration = self.config.batching.duration;

        // Start background task
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(duration));

            loop {
                interval.tick().await;

                // Get all batched webhooks
                let webhooks_to_process: HashMap<String, Vec<JobData>> = {
                    let mut batched = batched_webhooks.lock().await;
                    std::mem::take(&mut *batched)
                };

                // Skip if no webhooks to process
                if webhooks_to_process.is_empty() {
                    continue;
                }

                Log::webhook_sender(format!(
                    "Processing {} batched webhook queues",
                    webhooks_to_process.len()
                ));

                // Process each group
                if let Some(manager) = &queue_manager {
                    for (queue_name, jobs) in webhooks_to_process {
                        let mut manager = manager.lock().await;
                        for job in jobs {
                            if let Err(e) = manager.add_to_queue(&queue_name, job).await {
                                Log::error(format!("Failed to add job to queue: {}", e));
                            }
                        }
                    }
                }
            }
        });
    }

    /// Check if webhooks are enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Add webhook to queue or batch
    async fn add_webhook(&self, queue_name: &str, job_data: JobData) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        if self.config.batching.enabled {
            // Add to batch
            let mut batched_webhooks = self.batched_webhooks.lock().await;
            let entry = batched_webhooks
                .entry(queue_name.to_string())
                .or_insert_with(Vec::new);
            entry.push(job_data);
            Ok(())
        } else if let Some(queue_manager) = &self.queue_manager {
            // Add directly to queue
            let mut manager = queue_manager.lock().await;
            manager.add_to_queue(queue_name, job_data).await
        } else {
            Err(Error::InternalError(
                "Queue manager not initialized".to_string(),
            ))
        }
    }

    /// Get app configuration for an app
    async fn get_app(&self, app_id: &str) -> Result<App> {
        // Use app_manager to get app info
        match self.app_manager.get_app(app_id).await? {
            Some(app) => Ok(app),
            None => {
                // Log that app was not found
                Log::error(format!("App not found with ID: {}", app_id));
                // Log all available apps for debugging
                let apps = self.app_manager.get_apps().await?;
                Log::webhook_sender(format!("Available apps: {:?}", apps));
                Err(Error::InvalidAppKey)
            }
        }
    }

    /// Check if any app has webhooks configured for an event type
    async fn should_send_webhook(&self, app: &App, event_type: &str) -> bool {
        if !self.config.enabled {
            return false;
        }

        // Check if the app has webhooks configured
        if let Some(webhooks) = &app.webhooks {
            // Check if any webhook is configured for this event type
            for webhook in webhooks {
                if webhook.event_types.contains(&event_type.to_string()) {
                    return true;
                }
            }
        }

        false
    }

    /// Create a webhook job data object
    fn create_job_data(&self, app: &App, event: &str, data: Value, signature: &str) -> JobData {
        // Create job payload
        let payload = JobPayload {
            time_ms: chrono::Utc::now().timestamp_millis(),
            events: vec![event.to_string()],
            data,
        };

        // Create job data
        JobData {
            app_key: app.key.clone(),
            app_id: app.id.clone(),
            payload,
            original_signature: signature.to_string(),
        }
    }

    /// Send channel occupied event
    pub async fn send_channel_occupied(&self, app: &App, channel: &str) -> Result<()> {
        // Debug log to trace app details
        Log::webhook_sender(format!("send_channel_occupied for app: {:?}", app));

        if !self.should_send_webhook(app, "channel_occupied").await {
            return Ok(());
        }

        let data = json!({
            "channel": channel
        });

        let signature = format!("{}:{}:channel_occupied", app.id, channel);
        let job_data = self.create_job_data(app, "channel_occupied", data, &signature);

        self.add_webhook("webhooks", job_data).await
    }

    /// Send channel vacated event
    pub async fn send_channel_vacated(&self, app: &App, channel: &str) -> Result<()> {
        // Debug log to trace app details
        Log::webhook_sender(format!("send_channel_vacated for app: {:?}", app));

        if !self.should_send_webhook(app, "channel_vacated").await {
            return Ok(());
        }

        let data = json!({
            "channel": channel
        });

        let signature = format!("{}:{}:channel_vacated", app.id, channel);
        let job_data = self.create_job_data(app, "channel_vacated", data, &signature);

        self.add_webhook("webhooks", job_data).await
    }

    /// Send member added event
    pub async fn send_member_added(&self, app: &App, channel: &str, user_id: &str) -> Result<()> {
        // Debug log to trace app details
        Log::webhook_sender(format!("send_member_added for app: {:?}", app));

        if !self.should_send_webhook(app, "member_added").await {
            return Ok(());
        }

        let data = json!({
            "channel": channel,
            "user_id": user_id
        });

        let signature = format!("{}:{}:{}:member_added", app.id, channel, user_id);
        let job_data = self.create_job_data(app, "member_added", data, &signature);

        self.add_webhook("webhooks", job_data).await
    }

    /// Send member removed event
    pub async fn send_member_removed(&self, app: &App, channel: &str, user_id: &str) -> Result<()> {
        // Debug log to trace app details
        Log::webhook_sender(format!("send_member_removed for app: {:?}", app));

        if !self.should_send_webhook(app, "member_removed").await {
            return Ok(());
        }

        let data = json!({
            "channel": channel,
            "user_id": user_id
        });

        let signature = format!("{}:{}:{}:member_removed", app.id, channel, user_id);
        let job_data = self.create_job_data(app, "member_removed", data, &signature);

        self.add_webhook("webhooks", job_data).await
    }

    /// Send client event
    pub async fn send_client_event(
        &self,
        app: &App,
        channel: &str,
        event: &str,
        data: Value,
        socket_id: Option<&str>,
        user_id: Option<&str>,
    ) -> Result<()> {
        // Debug log to trace app details
        Log::webhook_sender(format!("send_client_event for app: {:?}", app));

        if !self.should_send_webhook(app, "client_event").await {
            return Ok(());
        }

        let event_data = json!({
            "channel": channel,
            "event": event,
            "data": data,
            "socket_id": socket_id,
            "user_id": user_id
        });

        let socket_part = socket_id.unwrap_or("no-socket");
        let signature = format!("{}:{}:{}:client_event", app.id, channel, socket_part);
        let job_data = self.create_job_data(app, "client_event", event_data, &signature);

        self.add_webhook("webhooks", job_data).await
    }

    /// Send cache missed event
    pub async fn send_cache_missed(&self, app: &App, channel: &str) -> Result<()> {
        // Debug log to trace app details
        Log::webhook_sender(format!("send_cache_missed for app: {:?}", app));

        if !self.should_send_webhook(app, "cache_miss").await {
            return Ok(());
        }

        let data = json!({
            "channel": channel
        });

        let signature = format!("{}:{}:cache_miss", app.id, channel);
        let job_data = self.create_job_data(app, "cache_miss", data, &signature);

        self.add_webhook("webhooks", job_data).await
    }
}
