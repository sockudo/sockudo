// src/webhook/sender.rs
use crate::app::config::App; // Keep for App struct
use crate::app::manager::AppManager; // Keep for AppManager trait
use crate::error::{Error, Result};

use crate::webhook::lambda_sender::LambdaWebhookSender;
// JobData now contains app_secret and its payload.events is Vec<Value>
// PusherWebhookPayload is the structure for the final POST body
use crate::token::Token; // For HMAC SHA256 signing
use crate::webhook::types::{JobData, LambdaConfig, PusherWebhookPayload, Webhook};
use reqwest::{header, Client};
use serde_json::{json, Value}; // Keep json! and Value
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

pub type JobProcessorFnAsync = Box<
    dyn Fn(JobData) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static,
>;

const MAX_CONCURRENT_WEBHOOKS: usize = 20;

pub struct WebhookSender {
    client: Client,
    app_manager: Arc<dyn AppManager + Send + Sync>, // Still needed to fetch App if JobData doesn't have full App
    lambda_sender: LambdaWebhookSender,
    webhook_semaphore: Arc<Semaphore>,
}

impl WebhookSender {
    pub fn new(app_manager: Arc<dyn AppManager + Send + Sync>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10)) // Timeout for HTTP requests
            .build()
            .unwrap_or_default();
        Self {
            client,
            app_manager,
            lambda_sender: LambdaWebhookSender::new(),
            webhook_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_WEBHOOKS)),
        }
    }

    pub async fn process_webhook_job(&self, job: JobData) -> Result<()> {
        let app_key = job.app_key.clone();
        let app_id = job.app_id.clone();
        info!(
            "{}",
            format!("Processing webhook job for app_id: {}", app_id.clone())
        );

        // The App struct (or at least its webhooks configuration) is needed.
        // If JobData doesn't contain the full App.webhooks, we fetch it.
        // For simplicity, let's assume JobData might not have the Webhook configs, so we fetch the app.
        let app_config = match self.app_manager.find_by_id(&app_id.clone()).await? {
            Some(app) => app,
            None => {
                error!(
                    "{}",
                    format!("Webhook: Failed to find app with ID: {}", app_id.clone())
                );
                return Err(Error::InvalidAppKey); // Or handle as non-retryable error
            }
        };

        let webhook_configurations = match &app_config.webhooks {
            Some(hooks) => hooks,
            None => {
                info!(
                    "{}",
                    format!("No webhooks configured for app: {}", app_id.clone())
                );
                return Ok(());
            }
        };

        if job.payload.events.is_empty() {
            warn!(
                "{}",
                format!("Webhook job for app {} has no events.", app_id.clone())
            );
            return Ok(());
        }

        // Construct the Pusher-compatible payload body
        // The `job.payload.events` is already a Vec<Value> where each Value is a Pusher event object.
        let pusher_payload_body = PusherWebhookPayload {
            time_ms: job.payload.time_ms,
            events: job.payload.events.clone(), // Clone the events for the payload
        };

        // Serialize the payload body to JSON string for signing and sending
        let body_json_string = serde_json::to_string(&pusher_payload_body).map_err(|e| {
            Error::SerializationError(format!("Failed to serialize webhook body: {}", e))
        })?;

        // Create the HMAC SHA256 signature
        // The app_secret is now in JobData
        let signature =
            Token::new(job.app_key.clone(), job.app_secret.clone()).sign(&body_json_string);

        let mut tasks = Vec::new();

        // Iterate through events in the job to find matching webhook configurations
        // Pusher webhooks are configured per event type. A single job might trigger webhooks
        // if any of its contained events match a webhook configuration.
        let mut relevant_webhook_configs: HashMap<String, &Webhook> = HashMap::new();

        for event_value in &job.payload.events {
            if let Some(event_name) = event_value.get("name").and_then(Value::as_str) {
                for wh_config in webhook_configurations {
                    if wh_config.event_types.contains(&event_name.to_string()) {
                        // Use webhook URL or function name as key to avoid duplicate tasks for the same endpoint
                        let key = wh_config
                            .url
                            .as_ref()
                            .map(|u| u.to_string())
                            .or_else(|| wh_config.lambda_function.clone())
                            .or_else(|| wh_config.lambda.as_ref().map(|l| l.function_name.clone()))
                            .unwrap_or_else(String::new); // Should have one

                        if !key.is_empty() {
                            relevant_webhook_configs.entry(key).or_insert(wh_config);
                        }
                    }
                }
            }
        }

        if relevant_webhook_configs.is_empty() {
            info!(
                "{}",
                format!(
                    "No matching webhook configurations for events in job for app {}",
                    app_id.clone()
                )
            );
            return Ok(());
        }

        log_webhook_processing_pusher_format(&app_id, &pusher_payload_body);

        for (_endpoint_key, webhook_config) in relevant_webhook_configs {
            let permit_semaphore = self.webhook_semaphore.clone();
            let permit = permit_semaphore.acquire_owned().await.map_err(|e| {
                Error::Other(format!("Failed to acquire webhook semaphore permit: {}", e))
            })?;
            let app_id = app_id.clone(); // Clone app_id for the async task
            let current_app_key = app_key.clone();
            let current_signature = signature.clone();
            let body_to_send = body_json_string.clone(); // Clone the already serialized body

            if let Some(url) = &webhook_config.url {
                let client = self.client.clone();
                let url_str = url.to_string();
                let custom_headers_config = webhook_config
                    .headers
                    .as_ref()
                    .map(|h| h.headers.clone())
                    .unwrap_or_default();

                let task = tokio::spawn(async move {
                    let _permit = permit; // Permit dropped when task finishes
                    if let Err(e) = send_pusher_webhook(
                        &client,
                        &url_str,
                        &current_app_key,
                        &current_signature,
                        body_to_send, // Send the pre-serialized JSON string
                        custom_headers_config,
                    )
                    .await
                    {
                        error!(
                            "{}",
                            format!("Webhook send error to URL {}: {}", url_str, e)
                        );
                    } else {
                        info!(
                            "{}",
                            format!("Successfully sent Pusher webhook to URL: {}", url_str)
                        );
                    }
                });
                tasks.push(task);
            } else if webhook_config.lambda.is_some() || webhook_config.lambda_function.is_some() {
                // Lambda sending needs to be adapted if it's to receive the Pusher format.
                // For now, we assume Lambda might expect a different format or handle the Pusher format.
                // The body_to_send is the Pusher JSON string.
                let lambda_sender = self.lambda_sender.clone();
                let webhook_clone = webhook_config.clone();
                // For Lambda, we might need to pass the raw Pusher payload or a structured version of it.
                // The current lambda_sender.invoke_lambda expects a Value.
                // Let's parse the body_to_send back to Value for the lambda_sender.
                let payload_for_lambda: Value =
                    serde_json::from_str(&body_to_send).unwrap_or(json!({}));

                let task = tokio::spawn(async move {
                    let _permit = permit;
                    // The lambda_sender.invoke_lambda might need adjustment if it's also to send
                    // X-Pusher-Key and X-Pusher-Signature as part of its invocation,
                    // or if the Lambda function itself expects to verify these from an API Gateway.
                    // This example passes the Pusher payload body to Lambda.
                    if let Err(e) = lambda_sender
                        .invoke_lambda(
                            &webhook_clone,
                            "batch_events",  // Generic event type for batched lambda
                            &app_id.clone(), // Pass app_id for context
                            payload_for_lambda, // Pass the parsed Value
                        )
                        .await
                    {
                        error!(
                            "{}",
                            format!("Lambda webhook error for app {}: {}", app_id.clone(), e)
                        );
                    } else {
                        info!(
                            "{}",
                            format!("Successfully invoked Lambda for app: {}", app_id.clone())
                        );
                    }
                });
                tasks.push(task);
            } else {
                warn!(
                    "{}",
                    format!(
                        "Webhook for app {} has neither URL nor Lambda config.",
                        app_id.clone()
                    )
                );
                drop(permit);
            }
        }

        for task_handle in tasks {
            // Renamed variable
            if let Err(e) = task_handle.await {
                error!("{}", format!("Webhook task execution failed: {}", e));
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
            webhook_semaphore: self.webhook_semaphore.clone(),
        }
    }
}

/// Helper function to send a Pusher-formatted webhook
async fn send_pusher_webhook(
    client: &Client,
    url: &str,
    app_key: &str,
    signature: &str,
    json_body: String, // Expects already serialized JSON string
    custom_headers_config: HashMap<String, String>,
) -> Result<()> {
    info!("{}", format!("Sending Pusher webhook to URL: {}", url));

    let mut request_builder = client
        .post(url)
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Pusher-Key", app_key)
        .header("X-Pusher-Signature", signature);

    for (key, value) in custom_headers_config {
        request_builder = request_builder.header(key, value);
    }

    match request_builder.body(json_body).send().await {
        Ok(response) => {
            let status = response.status();
            if status.is_success() {
                // 2XX status codes
                info!(
                    "{}",
                    format!(
                        "Successfully sent Pusher webhook to {} (status: {})",
                        url, status
                    )
                );
                Ok(())
            } else {
                let error_text = response.text().await.unwrap_or_default();
                error!(
                    "{}",
                    format!(
                        "Pusher webhook to {} failed with status {}: {}",
                        url, status, error_text
                    )
                );
                Err(Error::Other(format!(
                    "Webhook to {} failed with status {}",
                    url, status
                )))
            }
        }
        Err(e) => {
            error!(
                "{}",
                format!("Failed to send Pusher webhook to {}: {}", url, e)
            );
            Err(Error::Other(format!(
                "HTTP request failed for webhook to {}: {}",
                url, e
            )))
        }
    }
}

// Helper function to log webhook processing details (Pusher format)
fn log_webhook_processing_pusher_format(app_id: &str, payload: &PusherWebhookPayload) {
    info!("{}", format!("Pusher Webhook for app ID: {}", app_id));
    info!("{}", format!("Time (ms): {}", payload.time_ms));
    for event in &payload.events {
        info!("{}", format!("  Event: {:?}", event));
    }
}
