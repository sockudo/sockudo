use sockudo_core::app::App;
use sockudo_core::app::AppManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::WebhookRetryConfig;

#[cfg(feature = "lambda")]
use crate::lambda_sender::LambdaWebhookSender;
use ahash::AHashMap;
use reqwest::{Client, header};
use serde::Serialize;
use sockudo_core::token::Token;
use sockudo_core::utils::channel_namespace_name;
use sockudo_core::webhook_types::{JobData, Webhook, WebhookFilter, WebhookRetryPolicy};
use sonic_rs::Value;
#[cfg(feature = "lambda")]
use sonic_rs::json;
use sonic_rs::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{debug, error, warn};

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

#[derive(Serialize)]
struct BorrowedPusherWebhookPayload<'a> {
    time_ms: i64,
    events: &'a [Value],
}

pub struct WebhookSender {
    client: Client,
    app_manager: Arc<dyn AppManager + Send + Sync>,
    retry_config: WebhookRetryConfig,
    request_timeout_ms: u64,
    #[cfg(feature = "lambda")]
    lambda_sender: LambdaWebhookSender,
    webhook_semaphore: Arc<Semaphore>,
}

impl WebhookSender {
    pub fn new(
        app_manager: Arc<dyn AppManager + Send + Sync>,
        retry_config: WebhookRetryConfig,
        request_timeout_ms: u64,
    ) -> Self {
        let client = Client::builder().build().unwrap_or_default();
        Self {
            client,
            app_manager,
            retry_config,
            request_timeout_ms,
            #[cfg(feature = "lambda")]
            lambda_sender: LambdaWebhookSender::new(),
            webhook_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_WEBHOOKS)),
        }
    }

    async fn get_app_config(&self, app_id: &str) -> Result<App> {
        match self.app_manager.find_by_id(app_id).await? {
            Some(app) => Ok(app),
            None => {
                error!(app_id = %app_id, "webhook app not found");
                Err(Error::InvalidAppKey)
            }
        }
    }

    async fn validate_webhook_job(&self, app_id: &str, events: &[Value]) -> Result<()> {
        if events.is_empty() {
            warn!(app_id = %app_id, "webhook job has no events");
            return Ok(());
        }
        Ok(())
    }

    fn create_pusher_body(&self, time_ms: i64, events: &[Value]) -> Result<String> {
        let pusher_payload = BorrowedPusherWebhookPayload { time_ms, events };
        sonic_rs::to_string(&pusher_payload)
            .map_err(|e| Error::Serialization(format!("Failed to serialize webhook body: {e}")))
    }

    fn event_matches_webhook_filter(
        &self,
        event: &Value,
        filter: Option<&WebhookFilter>,
        channel_pattern: Option<&regex::Regex>,
    ) -> bool {
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

        if let Some(regex) = channel_pattern
            && !regex.is_match(channel)
        {
            return false;
        }

        let namespace = channel_namespace_name(channel);

        if let Some(expected_namespace) = &filter.channel_namespace
            && namespace != Some(expected_namespace.as_str())
        {
            return false;
        }

        if let Some(expected_namespaces) = &filter.channel_namespaces
            && !expected_namespaces
                .iter()
                .any(|candidate| namespace == Some(candidate.as_str()))
        {
            return false;
        }

        true
    }

    fn filter_events_for_webhook(&self, events: &[Value], webhook_config: &Webhook) -> Vec<Value> {
        let filter = webhook_config.filter.as_ref();
        let channel_pattern = match filter.and_then(|filter| filter.channel_pattern.as_deref()) {
            Some(pattern) => match regex::Regex::new(pattern) {
                Ok(regex) => Some(regex),
                Err(_) => {
                    warn!("invalid webhook channel_pattern regex");
                    return Vec::new();
                }
            },
            None => None,
        };

        let mut filtered_events = Vec::new();
        for event in events {
            let Some(event_name) = event.get("name").and_then(Value::as_str) else {
                continue;
            };

            if webhook_config
                .event_types
                .iter()
                .any(|configured| configured.as_str() == event_name)
                && self.event_matches_webhook_filter(event, filter, channel_pattern.as_ref())
            {
                filtered_events.push(event.clone());
            }
        }

        filtered_events
    }

    fn find_relevant_webhooks<'a>(
        &self,
        events: &[Value],
        webhook_configs: &'a [Webhook],
    ) -> AHashMap<String, (&'a Webhook, Vec<Value>)> {
        let mut relevant_configs = AHashMap::with_capacity(webhook_configs.len());

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
        debug!(app_id = %app_id, "processing webhook job");

        let app_config = self.get_app_config(&app_id).await?;

        let webhook_configs = match app_config.webhooks_ref() {
            Some(hooks) => hooks,
            None => {
                debug!(app_id = %app_id, "no webhooks configured for app");
                return Ok(());
            }
        };

        self.validate_webhook_job(&app_id, &job.payload.events)
            .await?;

        let relevant_webhooks = self.find_relevant_webhooks(&job.payload.events, webhook_configs);
        if relevant_webhooks.is_empty() {
            debug!(app_id = %app_id, "no matching webhook configurations for events");
            return Ok(());
        }

        log_webhook_processing_pusher_format(&app_id, job.payload.events.len());

        let signer = Token::new(job.app_key.clone(), job.app_secret.clone());
        let mut tasks = Vec::with_capacity(relevant_webhooks.len());
        for (_endpoint_key, (webhook_config, filtered_events)) in relevant_webhooks {
            let filtered_body_json_string =
                self.create_pusher_body(job.payload.time_ms, &filtered_events)?;
            let filtered_signature = signer.sign(&filtered_body_json_string);

            let permit = self
                .webhook_semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| {
                    Error::Other(format!("Failed to acquire webhook semaphore permit: {e}"))
                })?;

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
                error!(error = %e, "webhook task execution failed");
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
                    app_id = %app_id,
                    "lambda webhook configured but lambda feature not compiled in"
                );
                drop(permit);
                tokio::spawn(async {})
            }
        } else {
            warn!(app_id = %app_id, "webhook has neither url nor lambda config");
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
        let retry_policy =
            resolve_retry_policy(&self.retry_config, params.webhook_config.retry.as_ref());
        let request_timeout_ms = params
            .webhook_config
            .request_timeout_ms
            .unwrap_or(self.request_timeout_ms);
        let custom_headers = params
            .webhook_config
            .headers
            .as_ref()
            .map(|h| h.headers.clone())
            .unwrap_or_default();

        tokio::spawn(async move {
            let _permit = params.permit;
            let _ = send_pusher_webhook(
                &client,
                PusherWebhookRequest {
                    url: url_str,
                    app_key: params.app_key,
                    signature: params.signature,
                    json_body: params.body_to_send,
                    custom_headers,
                    request_timeout_ms,
                    retry_config: retry_policy,
                },
            )
            .await;
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
                error!(
                    delivery_method = "lambda",
                    app_id = %app_id,
                    error = %e,
                    "lambda webhook invocation failed"
                );
            } else {
                debug!(
                    delivery_method = "lambda",
                    app_id = %app_id,
                    "lambda webhook invoked successfully"
                );
            }
        })
    }
}

impl Clone for WebhookSender {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            app_manager: self.app_manager.clone(),
            retry_config: self.retry_config.clone(),
            request_timeout_ms: self.request_timeout_ms,
            #[cfg(feature = "lambda")]
            lambda_sender: self.lambda_sender.clone(),
            webhook_semaphore: self.webhook_semaphore.clone(),
        }
    }
}

fn resolve_retry_policy(
    global: &WebhookRetryConfig,
    override_policy: Option<&WebhookRetryPolicy>,
) -> WebhookRetryConfig {
    let Some(policy) = override_policy else {
        return global.clone();
    };

    WebhookRetryConfig {
        enabled: policy.enabled.unwrap_or(global.enabled),
        max_attempts: policy.max_attempts.or(global.max_attempts),
        max_elapsed_time_ms: policy
            .max_elapsed_time_ms
            .unwrap_or(global.max_elapsed_time_ms),
        initial_backoff_ms: policy
            .initial_backoff_ms
            .unwrap_or(global.initial_backoff_ms),
        max_backoff_ms: policy.max_backoff_ms.unwrap_or(global.max_backoff_ms),
    }
}

fn should_retry_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || matches!(status.as_u16(), 408 | 409 | 425 | 429)
}

struct PusherWebhookRequest {
    url: String,
    app_key: String,
    signature: String,
    json_body: String,
    custom_headers: AHashMap<String, String>,
    request_timeout_ms: u64,
    retry_config: WebhookRetryConfig,
}

async fn send_pusher_webhook(client: &Client, request: PusherWebhookRequest) -> Result<()> {
    let start = tokio::time::Instant::now();
    let request_timeout = Duration::from_millis(request.request_timeout_ms.max(1));
    let max_retry_duration = Duration::from_millis(request.retry_config.max_elapsed_time_ms.max(1));
    let mut delay = Duration::from_millis(request.retry_config.initial_backoff_ms.max(1));
    let mut attempt = 0u32;

    loop {
        attempt += 1;
        let result = send_pusher_webhook_once(
            client,
            &request.url,
            &request.app_key,
            &request.signature,
            &request.json_body,
            &request.custom_headers,
            request_timeout,
        )
        .await;

        match result {
            Ok(()) => return Ok(()),
            Err(e) => {
                if matches!(e, Error::Protocol(_)) {
                    return Err(e);
                }

                if !request.retry_config.enabled {
                    warn!(
                        delivery_method = "http",
                        attempt,
                        error = %e,
                        "webhook delivery failed, retries disabled"
                    );
                    return Err(e);
                }

                if let Some(max_attempts) = request.retry_config.max_attempts
                    && attempt >= max_attempts
                {
                    warn!(
                        delivery_method = "http",
                        attempt,
                        error = %e,
                        "webhook delivery failed, max attempts reached"
                    );
                    return Err(e);
                }

                let elapsed = start.elapsed();
                if elapsed + delay > max_retry_duration {
                    warn!(
                        delivery_method = "http",
                        attempt,
                        elapsed_ms = elapsed.as_millis() as u64,
                        error = %e,
                        "webhook delivery budget exhausted"
                    );
                    return Err(e);
                }

                warn!(
                    delivery_method = "http",
                    attempt,
                    retry_delay_ms = delay.as_millis() as u64,
                    error = %e,
                    "webhook delivery failed, retrying"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_millis(
                    request.retry_config.max_backoff_ms.max(1),
                ));
            }
        }
    }
}

async fn send_pusher_webhook_once(
    client: &Client,
    url: &str,
    app_key: &str,
    signature: &str,
    json_body: &str,
    custom_headers_config: &AHashMap<String, String>,
    request_timeout: Duration,
) -> Result<()> {
    let mut request_builder = client
        .post(url)
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Pusher-Key", app_key)
        .header("X-Pusher-Signature", signature)
        .timeout(request_timeout);

    for (key, value) in custom_headers_config {
        request_builder = request_builder.header(key, value);
    }

    match request_builder.body(json_body.to_string()).send().await {
        Ok(response) => {
            let status = response.status();
            if status.is_success() {
                debug!(
                    delivery_method = "http",
                    status = status.as_u16(),
                    "webhook delivered"
                );
                Ok(())
            } else {
                let _ = response.text().await;
                if should_retry_status(status) {
                    debug!(
                        delivery_method = "http",
                        status = status.as_u16(),
                        "webhook returned retryable status"
                    );
                    Err(Error::Other(format!(
                        "webhook returned retryable status {status}"
                    )))
                } else {
                    error!(
                        delivery_method = "http",
                        status = status.as_u16(),
                        "webhook permanent delivery failure"
                    );
                    Err(Error::Protocol(format!(
                        "webhook failed with non-retryable status {status}"
                    )))
                }
            }
        }
        Err(e) => {
            let ec = request_error_class(&e);
            debug!(
                delivery_method = "http",
                error_class = ec,
                error = %e,
                "webhook http request attempt failed"
            );
            Err(Error::Other(format!("webhook http request failed: {e}")))
        }
    }
}

fn log_webhook_processing_pusher_format(app_id: &str, event_count: usize) {
    debug!(
        app_id = %app_id,
        event_count,
        "processing pusher webhook payload"
    );
}

#[allow(dead_code)]
fn webhook_error_class(e: &reqwest::Error) -> &'static str {
    if e.is_timeout() {
        "timeout"
    } else if e.is_connect() {
        "connect"
    } else {
        "request"
    }
}

fn request_error_class(e: &reqwest::Error) -> &'static str {
    if e.is_timeout() {
        "timeout"
    } else if e.is_connect() {
        "connect"
    } else {
        "request"
    }
}

#[cfg(test)]
mod tests {
    use sockudo_app::memory_app_manager::MemoryAppManager;
    use sockudo_core::app::{App, AppFeaturesPolicy, AppLimitsPolicy, AppManager, AppPolicy};
    use sockudo_core::webhook_types::JobPayload;

    use super::*;

    fn test_app() -> App {
        App::from_policy(
            "test_app".to_string(),
            "test_key".to_string(),
            "test_secret".to_string(),
            true,
            AppPolicy {
                limits: AppLimitsPolicy {
                    max_connections: 100,
                    max_client_events_per_second: 100,
                    ..Default::default()
                },
                features: AppFeaturesPolicy {
                    enable_client_messages: true,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn test_creating_webhook_sender() {
        let webhook_sender = WebhookSender::new(
            Arc::new(MemoryAppManager::new()),
            WebhookRetryConfig::default(),
            10_000,
        );
        assert!(webhook_sender.webhook_semaphore.available_permits() > 0);
        assert!(webhook_sender.app_manager.get_apps().await.is_ok());
    }

    #[tokio::test]
    async fn test_process_webhook_job_no_events() {
        let app_manager = Arc::new(MemoryAppManager::new());
        let app = test_app();
        app_manager.create_app(app).await.unwrap();
        let webhook_sender =
            WebhookSender::new(app_manager.clone(), WebhookRetryConfig::default(), 10_000);

        let job = JobData {
            job_id: None,
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
        let app = test_app();
        app_manager.create_app(app).await.unwrap();
        let webhook_sender =
            WebhookSender::new(app_manager.clone(), WebhookRetryConfig::default(), 10_000);

        let job = JobData {
            job_id: None,
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
        let webhook_sender =
            WebhookSender::new(app_manager.clone(), WebhookRetryConfig::default(), 10_000);

        let job = JobData {
            job_id: None,
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
        let app = test_app();
        app_manager.create_app(app).await.unwrap();
        let webhook_sender = Arc::new(WebhookSender::new(
            app_manager.clone(),
            WebhookRetryConfig::default(),
            10_000,
        ));

        let mut handles = vec![];
        for i in 0..10 {
            let sender_clone = webhook_sender.clone();
            let job = JobData {
                job_id: None,
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
        let webhook_sender = WebhookSender::new(
            Arc::new(MemoryAppManager::new()),
            WebhookRetryConfig::default(),
            10_000,
        );
        let webhook = Webhook {
            url: Some(url::Url::parse("http://localhost/webhook").unwrap()),
            lambda_function: None,
            lambda: None,
            event_types: vec!["channel_occupied".to_string()],
            filter: Some(WebhookFilter {
                channel_prefix: Some("#server-to-user".to_string()),
                channel_suffix: None,
                channel_pattern: None,
                channel_namespace: None,
                channel_namespaces: None,
            }),
            headers: None,
            retry: None,
            request_timeout_ms: None,
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
        let webhook_sender = WebhookSender::new(
            Arc::new(MemoryAppManager::new()),
            WebhookRetryConfig::default(),
            10_000,
        );
        let prefixed = Webhook {
            url: Some(url::Url::parse("http://localhost/prefix").unwrap()),
            lambda_function: None,
            lambda: None,
            event_types: vec!["channel_occupied".to_string()],
            filter: Some(WebhookFilter {
                channel_prefix: Some("#server-to-user".to_string()),
                channel_suffix: None,
                channel_pattern: None,
                channel_namespace: None,
                channel_namespaces: None,
            }),
            headers: None,
            retry: None,
            request_timeout_ms: None,
        };
        let catch_all = Webhook {
            url: Some(url::Url::parse("http://localhost/all").unwrap()),
            lambda_function: None,
            lambda: None,
            event_types: vec!["channel_occupied".to_string()],
            filter: None,
            headers: None,
            retry: None,
            request_timeout_ms: None,
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
