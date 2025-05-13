// src/webhook/sender.rs
use crate::app::config::App;
use crate::app::manager::AppManager;
use crate::error::{Error, Result};
use crate::log::Log;
use crate::webhook::types::{JobData, JobPayload, Webhook};
use reqwest::{header, Client};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

// Type for callback function that processes jobs
pub type JobProcessorFnAsync = Box<
    dyn Fn(JobData) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static,
>;

/// WebhookSender is responsible for sending webhook events to configured endpoints
pub struct WebhookSender {
    client: Client,
    app_manager: Arc<dyn AppManager + Send + Sync>,
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

        for webhook in matching_webhooks {
            // Skip if no URL is specified
            let url = match &webhook.url {
                Some(url_str) => url_str,
                None => continue,
            };

            // Create the payload to send
            let payload = json!({
                "time_ms": job.payload.time_ms,
                "events": job.payload.events,
                "app_id": job.app_id,
                "app_key": job.app_key,
                "data": job.payload.data
            });

            // Clone what we need for the async task
            let client = self.client.clone();
            let url = url.clone();
            let event = event.clone();
            let app_id = app.id.clone();

            // Build custom headers if specified
            let headers = webhook
                .headers
                .as_ref()
                .map(|h| h.headers.clone())
                .unwrap_or_default();

            // Spawn a task to send the webhook
            let task = tokio::spawn(async move {
                send_webhook(&client, url.as_ref(), &event, &app_id, payload, headers).await
            });

            tasks.push(task);
        }

        // 6. Wait for all webhook sends to complete
        for task in tasks {
            if let Err(e) = task.await {
                Log::error(format!("Webhook task failed: {}", e));
            }
        }

        Ok(())
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
