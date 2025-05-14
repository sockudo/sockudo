// Updated src/webhook/sender.rs
use crate::app::config::App;
use crate::app::manager::AppManager;
use crate::error::{Error, Result};
use crate::log::Log;
use crate::webhook::lambda_sender::LambdaWebhookSender;
use crate::webhook::types::{JobData, Webhook}; // Removed JobPayload as it's part of JobData
use reqwest::{header, Client};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore; // Added for bounded concurrency

// Type for callback function that processes jobs
pub type JobProcessorFnAsync = Box<
    dyn Fn(JobData) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static,
>;

const MAX_CONCURRENT_WEBHOOKS: usize = 20; // Configurable: Max number of concurrent webhook sends

/// WebhookSender is responsible for sending webhook events to configured endpoints
pub struct WebhookSender {
    client: Client,
    app_manager: Arc<dyn AppManager + Send + Sync>,
    lambda_sender: LambdaWebhookSender,
    webhook_semaphore: Arc<Semaphore>, // Semaphore for limiting concurrent webhooks
}

impl WebhookSender {
    /// Create a new webhook sender
    pub fn new(app_manager: Arc<dyn AppManager + Send + Sync>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Self {
            client,
            app_manager,
            lambda_sender: LambdaWebhookSender::new(),
            webhook_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_WEBHOOKS)),
        }
    }

    /// Process a webhook job
    pub async fn process_webhook_job(&self, job: JobData) -> Result<()> {
        Log::webhook_sender(format!("Processing webhook job: {:?}", job));

        // 1. Get app information
        let app = match self.app_manager.get_app(&job.app_id).await? {
            Some(app) => app,
            None => {
                Log::error(format!("Failed to find app with ID: {}", job.app_id));
                return Err(Error::InvalidAppKey);
            }
        };

        // 2. Get webhooks configured for this app
        let webhooks = match &app.webhooks {
            Some(webhooks) => webhooks,
            None => {
                Log::info(format!("No webhooks configured for app: {}", app.id));
                return Ok(());
            }
        };

        // 3. Check if any events in the job match webhook events
        if job.payload.events.is_empty() {
            Log::warning("Job has no events specified");
            return Ok(());
        }

        let event = &job.payload.events[0]; // Use the first event

        // 4. Find matching webhooks for this event
        let matching_webhooks: Vec<&Webhook> = webhooks
            .iter()
            .filter(|webhook| webhook.event_types.contains(event))
            .collect();

        if matching_webhooks.is_empty() {
            Log::info(format!("No webhooks configured for event: {}", event));
            return Ok(());
        }

        log_webhook_processing(&job);

        // 5. Send webhooks to all matching endpoints
        let mut tasks = Vec::new();

        for webhook_config in matching_webhooks {
            // Prepare payload for sending
            let payload_to_send = json!({
                "time_ms": job.payload.time_ms,
                "events": job.payload.events, // Send all events in the job
                "app_id": job.app_id,
                "app_key": job.app_key,
                "data": job.payload.data // Send the original data field
            });

            // Acquire a permit from the semaphore before spawning the task
            // Clone semaphore Arc for the task
            let permit_semaphore = self.webhook_semaphore.clone();
            let permit = permit_semaphore.acquire_owned().await.map_err(|e| Error::Other(format!("Failed to acquire webhook semaphore permit: {}", e)))?;


            if let Some(url) = &webhook_config.url {
                let client = self.client.clone();
                let url_str = url.to_string();
                let event_clone = event.clone(); // Clone event for the task
                let app_id_clone = app.id.clone(); // Clone app_id for the task
                let payload_clone = payload_to_send.clone(); // Clone payload for the task
                let headers = webhook_config
                    .headers
                    .as_ref()
                    .map(|h| h.headers.clone())
                    .unwrap_or_default();

                let task = tokio::spawn(async move {
                    // The permit is moved into the task and will be released when the task finishes
                    let _permit = permit;
                    if let Err(e) = send_webhook(&client, &url_str, &event_clone, &app_id_clone, payload_clone, headers).await {
                        Log::error(format!("Webhook send error to URL {}: {}", url_str, e));
                    } else {
                        Log::webhook_sender(format!(
                            "Successfully sent webhook to URL: {}",
                            url_str
                        ));
                    }
                });
                tasks.push(task);
            } else if webhook_config.lambda.is_some() || webhook_config.lambda_function.is_some() {
                let lambda_sender = self.lambda_sender.clone();
                let webhook_clone = webhook_config.clone(); // Clone the specific webhook_config
                let event_clone = event.clone();
                let app_id_clone = app.id.clone();
                let payload_clone = payload_to_send.clone();

                let task = tokio::spawn(async move {
                    // The permit is moved into the task and will be released when the task finishes
                    let _permit = permit;
                    if let Err(e) = lambda_sender.invoke_lambda(
                        &webhook_clone,
                        &event_clone,
                        &app_id_clone,
                        payload_clone
                    ).await {
                        Log::error(format!("Lambda webhook error for app {}: {}", app_id_clone, e));
                    }
                });
                tasks.push(task);
            } else {
                Log::warning("Webhook has neither URL nor Lambda configuration, skipping");
                // Manually drop the permit if the task is not spawned
                drop(permit);
            }
        }

        // 6. Wait for all webhook sends to complete
        for task in tasks {
            if let Err(e) = task.await {
                // This catches errors from tokio::spawn (e.g., if the task panicked)
                Log::error(format!("Webhook task failed: {}", e));
            }
        }

        Ok(())
    }
}

impl Clone for WebhookSender {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            app_manager: self.app_manager.clone(),
            lambda_sender: self.lambda_sender.clone(),
            webhook_semaphore: self.webhook_semaphore.clone(), // Clone the semaphore Arc
        }
    }
}

/// Helper function to send a webhook
async fn send_webhook(
    client: &Client,
    url: &str,
    event: &str,
    app_id: &str,
    payload: Value,
    custom_headers: HashMap<String, String>,
) -> Result<()> {
    Log::webhook_sender(format!(
        "Sending webhook for event '{}' to URL: {}",
        event, url
    ));

    // Build the request with custom headers
    let mut request_builder = client
        .post(url)
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Sockudo-Event", event)
        .header("X-Sockudo-App-ID", app_id);

    // Add any custom headers
    for (key, value) in custom_headers {
        request_builder = request_builder.header(key, value);
    }

    // Add the payload and send
    match request_builder.json(&payload).send().await {
        Ok(response) => {
            let status = response.status();
            if status.is_success() {
                Log::webhook_sender(format!(
                    "Successfully sent webhook for event '{}' (status: {})",
                    event, status
                ));
                Ok(())
            } else {
                let error_text = response.text().await.unwrap_or_default();
                Log::error(format!(
                    "Webhook failed with status {}: {}",
                    status, error_text
                ));
                Err(Error::Other(format!(
                    "Webhook failed with status {}",
                    status
                )))
            }
        }
        Err(e) => {
            Log::error(format!("Failed to send webhook: {}", e));
            Err(Error::Other(format!("Failed to send webhook: {}", e)))
        }
    }
}

// Helper function to log webhook processing details
fn log_webhook_processing(job: &JobData) {
    Log::webhook_sender(format!("Webhook for app ID: {}", job.app_id));
    Log::webhook_sender(format!("Events: {:?}", job.payload.events));

    if let Some(first_event) = job.payload.events.first() {
        match first_event.as_str() {
            "channel_occupied" => {
                if let Some(channel) = job.payload.data.get("channel") {
                    Log::webhook_sender(format!("Channel occupied: {}", channel));
                }
            }
            "channel_vacated" => {
                if let Some(channel) = job.payload.data.get("channel") {
                    Log::webhook_sender(format!("Channel vacated: {}", channel));
                }
            }
            "member_added" => {
                if let (Some(channel), Some(user_id)) = (
                    job.payload.data.get("channel"),
                    job.payload.data.get("user_id"),
                ) {
                    Log::webhook_sender(format!(
                        "Member added: {} to channel {}",
                        user_id, channel
                    ));
                }
            }
            "member_removed" => {
                if let (Some(channel), Some(user_id)) = (
                    job.payload.data.get("channel"),
                    job.payload.data.get("user_id"),
                ) {
                    Log::webhook_sender(format!(
                        "Member removed: {} from channel {}",
                        user_id, channel
                    ));
                }
            }
            "client_event" => {
                if let (Some(channel), Some(event)) = (
                    job.payload.data.get("channel"),
                    job.payload.data.get("event"),
                ) {
                    Log::webhook_sender(format!("Client event: {} on channel {}", event, channel));
                }
            }
            "cache_miss" => {
                if let Some(channel) = job.payload.data.get("channel") {
                    Log::webhook_sender(format!("Cache miss for channel: {}", channel));
                }
            }
            _ => {
                Log::webhook_sender(format!("Unknown event type: {}", first_event));
            }
        }
    }
}
