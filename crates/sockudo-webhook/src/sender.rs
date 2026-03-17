use sockudo_core::app::App;
use sockudo_core::app::AppManager;
use sockudo_core::error::{Error, Result};

#[cfg(feature = "lambda")]
use crate::lambda_sender::LambdaWebhookSender;
use ahash::AHashMap;
use reqwest::{Client, header};
use sockudo_core::token::Token;
use sockudo_core::webhook_types::{
    JobData, JobPayload, PusherWebhookPayload, Webhook, WebhookFilter,
};
use sonic_rs::Value;
#[cfg(feature = "lambda")]
use sonic_rs::json;
use sonic_rs::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

const MAX_CONCURRENT_WEBHOOKS: usize = 20;

/// Parameters for creating an HTTP webhook task
struct HttpWebhookTaskParams {
    url: url::Url,
    webhook_config: Webhook,
    permit: tokio::sync::OwnedSemaphorePermit,
    app_key: String,
    signature: String,
    body_to_send: String,
}

pub struct WebhookSender {
    client: Client,
    app_manager: Arc<dyn AppManager + Send + Sync>,
    #[cfg(feature = "lambda")]
    lambda_sender: LambdaWebhookSender,
    webhook_semaphore: Arc<Semaphore>,
}

impl WebhookSender {
    pub fn new(app_manager: Arc<dyn AppManager + Send + Sync>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        Self {
            client,
            app_manager,
            #[cfg(feature = "lambda")]
            lambda_sender: LambdaWebhookSender::new(),
            webhook_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_WEBHOOKS)),
        }
    }

    async fn get_app_config(&self, app_id: &str) -> Result<App> {
        match self.app_manager.find_by_id(app_id).await? {
            Some(app) => Ok(app),
            None => {
                error!("Webhook: Failed to find app with ID: {}", app_id);
                Err(Error::InvalidAppKey)
            }
        }
    }

    async fn validate_webhook_job(&self, app_id: &str, events: &[Value]) -> Result<()> {
        if events.is_empty() {
            warn!("Webhook job for app {} has no events.", app_id);
            return Ok(());
        }
        Ok(())
    }

    fn create_pusher_payload(&self, job: &JobData) -> Result<(PusherWebhookPayload, String)> {
        let pusher_payload = PusherWebhookPayload {
            time_ms: job.payload.time_ms,
            events: job.payload.events.clone(),
        };

        let body_json_string = sonic_rs::to_string(&pusher_payload)
            .map_err(|e| Error::Serialization(format!("Failed to serialize webhook body: {e}")))?;

        let _signature =
            Token::new(job.app_key.clone(), job.app_secret.clone()).sign(&body_json_string);
        Ok((pusher_payload, body_json_string))
    }

    fn event_matches_webhook_filter(&self, event: &Value, filter: Option<&WebhookFilter>) -> bool {
        let Some(filter) = filter else {
            return true;
        };

        let channel = event
            .get("channel")
            .and_then(Value::as_str)
            .unwrap_or_default();

        if let Some(prefix) = &filter.channel_prefix
            && !channel.starts_with(prefix)
        {
            return false;
        }

        if let Some(suffix) = &filter.channel_suffix
            && !channel.ends_with(suffix)
        {
            return false;
        }

        if let Some(pattern) = &filter.channel_pattern {
            let Ok(regex) = regex::Regex::new(pattern) else {
                warn!(
                    "Ignoring invalid webhook channel_pattern regex: {}",
                    pattern
                );
                return false;
            };

            if !regex.is_match(channel) {
                return false;
            }
        }

        true
    }

    fn filter_events_for_webhook(&self, events: &[Value], webhook_config: &Webhook) -> Vec<Value> {
        events
            .iter()
            .filter(|event| {
                event
                    .get("name")
                    .and_then(Value::as_str)
                    .is_some_and(|event_name| {
                        webhook_config.event_types.contains(&event_name.to_string())
                            && self
                                .event_matches_webhook_filter(event, webhook_config.filter.as_ref())
                    })
            })
            .cloned()
            .collect()
    }

    fn find_relevant_webhooks<'a>(
        &self,
        events: &[Value],
        webhook_configs: &'a [Webhook],
    ) -> AHashMap<String, (&'a Webhook, Vec<Value>)> {
        let mut relevant_configs = AHashMap::new();

        for wh_config in webhook_configs {
            let filtered_events = self.filter_events_for_webhook(events, wh_config);
            if filtered_events.is_empty() {
                continue;
            }

            let key = wh_config
                .url
                .as_ref()
                .map(|u| u.to_string())
                .or_else(|| wh_config.lambda_function.clone())
                .or_else(|| wh_config.lambda.as_ref().map(|l| l.function_name.clone()))
                .unwrap_or_else(String::new);

            if !key.is_empty() {
                relevant_configs.insert(key, (wh_config, filtered_events));
            }
        }
        relevant_configs
    }

    pub async fn process_webhook_job(&self, job: JobData) -> Result<()> {
        let app_id = job.app_id.clone();
        let app_key = job.app_key.clone();
        debug!("Processing webhook job for app_id: {}", app_id);

        let app_config = self.get_app_config(&app_id).await?;

        let webhook_configs = match &app_config.webhooks {
            Some(hooks) => hooks,
            None => {
                debug!("No webhooks configured for app: {}", app_id);
                return Ok(());
            }
        };

        self.validate_webhook_job(&app_id, &job.payload.events)
            .await?;

        let (pusher_payload, _body_json_string) = self.create_pusher_payload(&job)?;

        let relevant_webhooks = self.find_relevant_webhooks(&job.payload.events, webhook_configs);
        if relevant_webhooks.is_empty() {
            debug!(
                "No matching webhook configurations for events in job for app {}",
                app_id
            );
            return Ok(());
        }

        log_webhook_processing_pusher_format(&app_id, &pusher_payload);

        let mut tasks = Vec::new();
        for (_endpoint_key, (webhook_config, filtered_events)) in relevant_webhooks {
            let permit = self
                .webhook_semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| {
                    Error::Other(format!("Failed to acquire webhook semaphore permit: {e}"))
                })?;

            let filtered_job = JobData {
                payload: JobPayload {
                    time_ms: job.payload.time_ms,
                    events: filtered_events,
                },
                ..job.clone()
            };
            let (_, filtered_body_json_string) = self.create_pusher_payload(&filtered_job)?;
            let filtered_signature = Token::new(job.app_key.clone(), job.app_secret.clone())
                .sign(&filtered_body_json_string);

            let task = self.create_webhook_task(
                webhook_config,
                permit,
                app_id.clone(),
                app_key.clone(),
                filtered_signature,
                filtered_body_json_string,
            );
            tasks.push(task);
        }

        for task_handle in tasks {
            if let Err(e) = task_handle.await {
                error!("Webhook task execution failed: {}", e);
            }
        }

        Ok(())
    }

    fn create_webhook_task(
        &self,
        webhook_config: &Webhook,
        permit: tokio::sync::OwnedSemaphorePermit,
        app_id: String,
        app_key: String,
        signature: String,
        body_to_send: String,
    ) -> tokio::task::JoinHandle<()> {
        if let Some(url) = &webhook_config.url {
            let params = HttpWebhookTaskParams {
                url: url.clone(),
                webhook_config: webhook_config.clone(),
                permit,
                app_key,
                signature,
                body_to_send,
            };

            self.create_http_webhook_task(params)
        } else if webhook_config.lambda.is_some() || webhook_config.lambda_function.is_some() {
            #[cfg(feature = "lambda")]
            {
                self.create_lambda_webhook_task(webhook_config, permit, app_id, body_to_send)
            }
            #[cfg(not(feature = "lambda"))]
            {
                warn!(
                    "Lambda webhook configured for app {} but Lambda support not compiled in.",
                    app_id
                );
                drop(permit);
                tokio::spawn(async {})
            }
        } else {
            warn!(
                "Webhook for app {} has neither URL nor Lambda config.",
                app_id
            );
            drop(permit);
            tokio::spawn(async {})
        }
    }

    fn create_http_webhook_task(
        &self,
        params: HttpWebhookTaskParams,
    ) -> tokio::task::JoinHandle<()> {
        let client = self.client.clone();
        let url_str = params.url.to_string();
        let custom_headers = params
            .webhook_config
            .headers
            .as_ref()
            .map(|h| h.headers.clone())
            .unwrap_or_default();

        tokio::spawn(async move {
            let _permit = params.permit;
            if let Err(e) = send_pusher_webhook(
                &client,
                &url_str,
                &params.app_key,
                &params.signature,
                params.body_to_send,
                custom_headers,
            )
            .await
            {
                error!("Webhook send error to URL {}: {}", url_str, e);
            } else {
                debug!("Successfully sent Pusher webhook to URL: {}", url_str);
            }
        })
    }

    #[cfg(feature = "lambda")]
    fn create_lambda_webhook_task(
        &self,
        webhook_config: &Webhook,
        permit: tokio::sync::OwnedSemaphorePermit,
        app_id: String,
        body_to_send: String,
    ) -> tokio::task::JoinHandle<()> {
        let lambda_sender = self.lambda_sender.clone();
        let webhook_clone = webhook_config.clone();
        let payload_for_lambda: Value = sonic_rs::from_str(&body_to_send).unwrap_or(json!({}));

        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = lambda_sender
                .invoke_lambda(&webhook_clone, "batch_events", &app_id, payload_for_lambda)
                .await
            {
                error!("Lambda webhook error for app {}: {}", app_id, e);
            } else {
                debug!("Successfully invoked Lambda for app: {}", app_id);
            }
        })
    }
}

impl Clone for WebhookSender {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            app_manager: self.app_manager.clone(),
            #[cfg(feature = "lambda")]
            lambda_sender: self.lambda_sender.clone(),
            webhook_semaphore: self.webhook_semaphore.clone(),
        }
    }
}

/// Maximum total retry duration (5 minutes) per Pusher spec.
const MAX_RETRY_DURATION: Duration = Duration::from_secs(300);

/// Initial retry delay.
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Helper function to send a Pusher-formatted webhook with retry and exponential backoff.
///
/// On non-2XX responses or network errors, retries with exponential backoff
/// for up to 5 minutes (per Pusher protocol spec).
async fn send_pusher_webhook(
    client: &Client,
    url: &str,
    app_key: &str,
    signature: &str,
    json_body: String,
    custom_headers_config: AHashMap<String, String>,
) -> Result<()> {
    debug!("Sending Pusher webhook to URL: {}", url);

    let start = tokio::time::Instant::now();
    let mut delay = INITIAL_RETRY_DELAY;
    let mut attempt = 0u32;

    loop {
        attempt += 1;
        let result = send_pusher_webhook_once(
            client,
            url,
            app_key,
            signature,
            &json_body,
            &custom_headers_config,
        )
        .await;

        match result {
            Ok(()) => return Ok(()),
            Err(e) => {
                let elapsed = start.elapsed();
                if elapsed + delay > MAX_RETRY_DURATION {
                    error!(
                        "Webhook to {} failed after {} attempts over {:.1}s, giving up: {}",
                        url,
                        attempt,
                        elapsed.as_secs_f64(),
                        e
                    );
                    return Err(e);
                }

                warn!(
                    "Webhook to {} failed (attempt {}), retrying in {:.1}s: {}",
                    url,
                    attempt,
                    delay.as_secs_f64(),
                    e
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(60));
            }
        }
    }
}

/// Single attempt to send a Pusher webhook.
async fn send_pusher_webhook_once(
    client: &Client,
    url: &str,
    app_key: &str,
    signature: &str,
    json_body: &str,
    custom_headers_config: &AHashMap<String, String>,
) -> Result<()> {
    let mut request_builder = client
        .post(url)
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Pusher-Key", app_key)
        .header("X-Pusher-Signature", signature);

    for (key, value) in custom_headers_config {
        request_builder = request_builder.header(key, value);
    }

    match request_builder.body(json_body.to_string()).send().await {
        Ok(response) => {
            let status = response.status();
            if status.is_success() {
                info!(
                    "Successfully sent Pusher webhook to {} (status: {})",
                    url, status
                );
                Ok(())
            } else {
                let error_text = response.text().await.unwrap_or_default();
                error!(
                    "Pusher webhook to {} failed with status {}: {}",
                    url, status, error_text
                );
                Err(Error::Other(format!(
                    "Webhook to {url} failed with status {status}"
                )))
            }
        }
        Err(e) => {
            error!("Failed to send Pusher webhook to {}: {}", url, e);
            Err(Error::Other(format!(
                "HTTP request failed for webhook to {url}: {e}"
            )))
        }
    }
}

fn log_webhook_processing_pusher_format(app_id: &str, payload: &PusherWebhookPayload) {
    debug!("Pusher Webhook for app ID: {}", app_id);
    for event in &payload.events {
        debug!("  Event: {:?}", event);
    }
}

#[cfg(test)]
mod tests {
    use sockudo_app::memory_app_manager::MemoryAppManager;
    use sockudo_core::app::{App, AppManager};
    use sockudo_core::webhook_types::JobPayload;

    use super::*;

    #[tokio::test]
    async fn test_creating_webhook_sender() {
        let webhook_sender = WebhookSender::new(Arc::new(MemoryAppManager::new()));
        assert!(webhook_sender.webhook_semaphore.available_permits() > 0);
        assert!(webhook_sender.app_manager.get_apps().await.is_ok());
    }

    #[tokio::test]
    async fn test_process_webhook_job_no_events() {
        let app_manager = Arc::new(MemoryAppManager::new());
        let app = App {
            id: "test_app".to_string(),
            key: "test_key".to_string(),
            secret: "test_secret".to_string(),
            max_connections: 100,
            enable_client_messages: true,
            enabled: true,
            max_client_events_per_second: 100,
            ..Default::default()
        };
        app_manager.create_app(app).await.unwrap();
        let webhook_sender = WebhookSender::new(app_manager.clone());

        let job = JobData {
            app_id: "test_app".to_string(),
            app_key: "test_key".to_string(),
            app_secret: "test_secret".to_string(),
            payload: JobPayload {
                time_ms: 1234567890,
                events: vec![],
            },
            original_signature: "test_signature".to_string(),
        };

        let result = webhook_sender.process_webhook_job(job).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_webhook_job_with_events() {
        let app_manager = Arc::new(MemoryAppManager::new());
        let app = App {
            id: "test_app".to_string(),
            key: "test_key".to_string(),
            secret: "test_secret".to_string(),
            max_connections: 100,
            enable_client_messages: true,
            enabled: true,
            max_client_events_per_second: 100,
            ..Default::default()
        };
        app_manager.create_app(app).await.unwrap();
        let webhook_sender = WebhookSender::new(app_manager.clone());

        let job = JobData {
            app_id: "test_app".to_string(),
            app_key: "test_key".to_string(),
            app_secret: "test_secret".to_string(),
            payload: JobPayload {
                time_ms: 1234567890,
                events: vec![sonic_rs::json!({
                    "name": "channel_occupied",
                    "channel": "test-channel"
                })],
            },
            original_signature: "test_signature".to_string(),
        };

        let result = webhook_sender.process_webhook_job(job).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_webhook_job_invalid_app() {
        let app_manager = Arc::new(MemoryAppManager::new());
        let webhook_sender = WebhookSender::new(app_manager.clone());

        let job = JobData {
            app_id: "non_existent_app".to_string(),
            app_key: "test_key".to_string(),
            app_secret: "test_secret".to_string(),
            payload: JobPayload {
                time_ms: 1234567890,
                events: vec![],
            },
            original_signature: "test_signature".to_string(),
        };

        let result = webhook_sender.process_webhook_job(job).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_process_webhook_job_concurrent_requests() {
        let app_manager = Arc::new(MemoryAppManager::new());
        let app = App {
            id: "test_app".to_string(),
            key: "test_key".to_string(),
            secret: "test_secret".to_string(),
            max_connections: 100,
            enable_client_messages: true,
            enabled: true,
            max_client_events_per_second: 100,
            ..Default::default()
        };
        app_manager.create_app(app).await.unwrap();
        let webhook_sender = Arc::new(WebhookSender::new(app_manager.clone()));

        let mut handles = vec![];
        for i in 0..10 {
            let sender_clone = webhook_sender.clone();
            let job = JobData {
                app_id: "test_app".to_string(),
                app_key: "test_key".to_string(),
                app_secret: "test_secret".to_string(),
                payload: JobPayload {
                    time_ms: 1234567890 + i,
                    events: vec![sonic_rs::json!({
                        "name": "channel_occupied",
                        "channel": format!("test-channel-{}", i)
                    })],
                },
                original_signature: format!("test_signature_{i}"),
            };

            handles.push(tokio::spawn(async move {
                sender_clone.process_webhook_job(job).await
            }));
        }

        let results = futures::future::join_all(handles).await;
        for result in results {
            assert!(result.unwrap().is_ok());
        }
    }

    #[test]
    fn test_filter_events_for_webhook_respects_channel_prefix() {
        let webhook_sender = WebhookSender::new(Arc::new(MemoryAppManager::new()));
        let webhook = Webhook {
            url: Some(url::Url::parse("http://localhost/webhook").unwrap()),
            lambda_function: None,
            lambda: None,
            event_types: vec!["channel_occupied".to_string()],
            filter: Some(WebhookFilter {
                channel_prefix: Some("#server-to-user".to_string()),
                channel_suffix: None,
                channel_pattern: None,
            }),
            headers: None,
        };

        let filtered = webhook_sender.filter_events_for_webhook(
            &[
                sonic_rs::json!({
                    "name": "channel_occupied",
                    "channel": "#server-to-user-123"
                }),
                sonic_rs::json!({
                    "name": "channel_occupied",
                    "channel": "private-conversation.123"
                }),
            ],
            &webhook,
        );

        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered[0].get("channel").and_then(Value::as_str),
            Some("#server-to-user-123")
        );
    }

    #[test]
    fn test_find_relevant_webhooks_splits_events_per_endpoint() {
        let webhook_sender = WebhookSender::new(Arc::new(MemoryAppManager::new()));
        let prefixed = Webhook {
            url: Some(url::Url::parse("http://localhost/prefix").unwrap()),
            lambda_function: None,
            lambda: None,
            event_types: vec!["channel_occupied".to_string()],
            filter: Some(WebhookFilter {
                channel_prefix: Some("#server-to-user".to_string()),
                channel_suffix: None,
                channel_pattern: None,
            }),
            headers: None,
        };
        let catch_all = Webhook {
            url: Some(url::Url::parse("http://localhost/all").unwrap()),
            lambda_function: None,
            lambda: None,
            event_types: vec!["channel_occupied".to_string()],
            filter: None,
            headers: None,
        };

        let webhooks = [prefixed.clone(), catch_all.clone()];
        let relevant = webhook_sender.find_relevant_webhooks(
            &[
                sonic_rs::json!({
                    "name": "channel_occupied",
                    "channel": "#server-to-user-1"
                }),
                sonic_rs::json!({
                    "name": "channel_occupied",
                    "channel": "private-conversation.1"
                }),
            ],
            &webhooks,
        );

        assert_eq!(relevant.len(), 2);
        assert_eq!(relevant.get("http://localhost/prefix").unwrap().1.len(), 1);
        assert_eq!(relevant.get("http://localhost/all").unwrap().1.len(), 2);
    }
}
