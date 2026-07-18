use sockudo_core::app::App;
use sockudo_core::app::AppManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::queue::QueueInterface;
use sockudo_core::webhook_types::{JobData, JobPayload, JobProcessorFnAsync};

use crate::sender::WebhookSender;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sonic_rs::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{debug, error, warn};

const WEBHOOK_QUEUE_NAME: &str = "webhooks";

/// Configuration for the webhook integration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub enabled: bool,
    pub batching: BatchingConfig,
    pub retry: sockudo_core::options::WebhookRetryConfig,
    pub request_timeout_ms: u64,
    pub process_id: String,
    pub debug: bool,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            batching: BatchingConfig::default(),
            retry: sockudo_core::options::WebhookRetryConfig::default(),
            request_timeout_ms: 10_000,
            process_id: uuid::Uuid::new_v4().to_string(),
            debug: false,
        }
    }
}

/// Configuration for webhook batching (Sockudo's internal batching)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchingConfig {
    pub enabled: bool,
    pub duration: u64, // in milliseconds
    pub size: usize,
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            duration: 50,
            size: 100,
        }
    }
}

/// Thin wrapper around a queue driver, mirroring the main crate's QueueManager.
/// This avoids a circular dependency while keeping the same API surface.
pub struct QueueManager {
    driver: Box<dyn QueueInterface>,
}

impl QueueManager {
    pub fn new(driver: Box<dyn QueueInterface>) -> Self {
        Self { driver }
    }

    pub async fn add_to_queue(&self, queue_name: &str, data: JobData) -> Result<()> {
        self.driver.add_to_queue(queue_name, data).await
    }

    pub async fn add_batch_to_queue(&self, queue_name: &str, data: Vec<JobData>) -> Result<()> {
        self.driver.add_batch_to_queue(queue_name, data).await
    }

    pub async fn process_queue(
        &self,
        queue_name: &str,
        callback: JobProcessorFnAsync,
    ) -> Result<()> {
        self.driver.process_queue(queue_name, callback).await
    }

    pub async fn disconnect(&self) -> Result<()> {
        self.driver.disconnect().await
    }

    pub async fn check_health(&self) -> Result<()> {
        self.driver.check_health().await
    }
}

/// Webhook integration for processing events
pub struct WebhookIntegration {
    config: WebhookConfig,
    batched_webhooks: Arc<Mutex<Vec<JobData>>>,
    queue_manager: Option<Arc<QueueManager>>,
    app_manager: Arc<dyn AppManager + Send + Sync>,
}

impl WebhookIntegration {
    pub async fn new(
        config: WebhookConfig,
        app_manager: Arc<dyn AppManager + Send + Sync>,
        queue_manager: Option<Arc<QueueManager>>,
    ) -> Result<Self> {
        let mut integration = Self {
            config,
            batched_webhooks: Arc::new(Mutex::new(Vec::new())),
            queue_manager: None,
            app_manager,
        };

        if integration.config.enabled {
            if let Some(qm) = queue_manager {
                integration.setup_webhook_processor(qm).await?;
            } else {
                warn!("webhooks enabled but no queue manager provided, disabling webhooks");
                integration.config.enabled = false;
            }
        }

        if integration.config.enabled && integration.config.batching.enabled {
            integration.start_batching_task();
        }

        Ok(integration)
    }

    async fn setup_webhook_processor(&mut self, queue_manager: Arc<QueueManager>) -> Result<()> {
        let webhook_sender = Arc::new(WebhookSender::new(
            self.app_manager.clone(),
            self.config.retry.clone(),
            self.config.request_timeout_ms,
        ));
        let sender_clone = webhook_sender.clone();

        let processor: JobProcessorFnAsync = Box::new(move |job_data| {
            let sender_for_task = sender_clone.clone();
            Box::pin(async move {
                debug!(
                    app_id = %job_data.app_id,
                    webhook_job_id = job_data.job_id.as_deref().unwrap_or("legacy"),
                    event_count = job_data.payload.events.len(),
                    "webhook job processing started"
                );
                sender_for_task.process_webhook_job(job_data).await
            })
        });

        queue_manager
            .process_queue(WEBHOOK_QUEUE_NAME, processor)
            .await?;
        self.queue_manager = Some(queue_manager);
        Ok(())
    }

    fn start_batching_task(&self) {
        if !self.config.batching.enabled {
            return;
        }
        let queue_manager_clone = self.queue_manager.clone();
        let batched_webhooks_clone = self.batched_webhooks.clone();
        let batch_duration = self.config.batching.duration;
        let batch_size = self.config.batching.size.max(1);

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(batch_duration));
            loop {
                interval.tick().await;
                let jobs_to_process = {
                    let mut batched = batched_webhooks_clone.lock();
                    std::mem::take(&mut *batched)
                };

                if jobs_to_process.is_empty() {
                    continue;
                }
                debug!(
                    job_count = jobs_to_process.len(),
                    "processing batched webhook jobs"
                );

                if let Some(qm) = &queue_manager_clone {
                    let batches = Self::merge_jobs_for_queue(jobs_to_process, batch_size);
                    if let Err(e) = qm.add_batch_to_queue(WEBHOOK_QUEUE_NAME, batches).await {
                        error!(
                            error = %e,
                            "failed to enqueue batched webhook jobs"
                        );
                    }
                }
            }
        });
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn add_webhook(&self, mut job_data: JobData) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }
        if self.config.batching.enabled {
            self.batched_webhooks.lock().push(job_data);
        } else if let Some(qm) = &self.queue_manager {
            job_data.job_id = Some(uuid::Uuid::new_v4().to_string());
            qm.add_to_queue(WEBHOOK_QUEUE_NAME, job_data).await?;
        } else {
            return Err(Error::Internal(
                "Queue manager not initialized for webhooks".to_string(),
            ));
        }
        Ok(())
    }

    fn merge_jobs_for_queue(jobs: Vec<JobData>, batch_size: usize) -> Vec<JobData> {
        let mut merged = Vec::with_capacity(jobs.len());
        let mut current: Option<JobData> = None;
        let batch_size = batch_size.max(1);

        for job in jobs {
            for chunk in Self::split_job_by_size(job, batch_size) {
                match current.as_mut() {
                    Some(existing)
                        if existing.app_id == chunk.app_id
                            && existing.app_key == chunk.app_key
                            && existing.app_secret == chunk.app_secret
                            && existing.payload.events.len() + chunk.payload.events.len()
                                <= batch_size =>
                    {
                        existing.payload.time_ms =
                            existing.payload.time_ms.min(chunk.payload.time_ms);
                        existing.payload.events.extend(chunk.payload.events);
                    }
                    Some(_) => {
                        if let Some(finished) = current.take() {
                            merged.push(finished);
                        }
                        current = Some(chunk);
                    }
                    None => current = Some(chunk),
                }
            }
        }

        if let Some(finished) = current {
            merged.push(finished);
        }

        for job in &mut merged {
            job.job_id = Some(uuid::Uuid::new_v4().to_string());
        }

        merged
    }

    fn split_job_by_size(job: JobData, batch_size: usize) -> Vec<JobData> {
        let batch_size = batch_size.max(1);
        if job.payload.events.len() <= batch_size {
            return vec![job];
        }

        let JobData {
            job_id: _,
            app_key,
            app_id,
            app_secret,
            payload,
            original_signature,
        } = job;

        let JobPayload { time_ms, events } = payload;
        let chunk_count = events.len().div_ceil(batch_size);
        let mut chunks = Vec::with_capacity(chunk_count);

        let mut events = events.into_iter();
        for _ in 0..chunk_count {
            chunks.push(JobData {
                job_id: None,
                app_key: app_key.clone(),
                app_id: app_id.clone(),
                app_secret: app_secret.clone(),
                payload: JobPayload {
                    time_ms,
                    events: events.by_ref().take(batch_size).collect(),
                },
                original_signature: original_signature.clone(),
            });
        }

        chunks
    }

    fn create_job_data(
        &self,
        app: &App,
        events_payload: Vec<Value>,
        original_signature_for_queue: &str,
    ) -> JobData {
        let job_payload = JobPayload {
            time_ms: chrono::Utc::now().timestamp_millis(),
            events: events_payload,
        };
        JobData {
            job_id: None,
            app_key: app.key.clone(),
            app_id: app.id.clone(),
            app_secret: app.secret.clone(),
            payload: job_payload,
            original_signature: original_signature_for_queue.to_string(),
        }
    }

    fn should_send_webhook(&self, app: &App, event_type_name: &str) -> bool {
        self.webhook_configured(app, event_type_name)
    }

    /// Cheap synchronous check: is a webhook configured for `event_type` on this app?
    /// Unlike [`Self::should_send_webhook`] this does not allocate, so it is safe to
    /// call on the subscribe/unsubscribe hot path.
    pub fn webhook_configured(&self, app: &App, event_type: &str) -> bool {
        self.is_enabled()
            && app.webhooks_ref().is_some_and(|webhooks| {
                webhooks
                    .iter()
                    .any(|wh| wh.event_types.iter().any(|e| e.as_str() == event_type))
            })
    }

    /// Whether any subscription-count-derived webhook (channel_occupied,
    /// channel_vacated, subscription_count) is configured for this app. When this is
    /// false — and no client subscribes to the channel's meta-channel — the
    /// subscribe/unsubscribe hot path can skip the cluster-wide count fanout.
    pub fn wants_subscription_count(&self, app: &App) -> bool {
        self.webhook_configured(app, "channel_occupied")
            || self.webhook_configured(app, "channel_vacated")
            || self.webhook_configured(app, "subscription_count")
    }

    pub async fn send_channel_occupied(&self, app: &App, channel: &str) -> Result<()> {
        if !self.should_send_webhook(app, "channel_occupied") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "channel_occupied",
            "channel": channel
        });
        let signature = format!("{}:{}:channel_occupied", app.id, channel);
        let job_data = self.create_job_data(app, vec![event_obj], &signature);

        self.add_webhook(job_data).await
    }

    pub async fn send_channel_vacated(&self, app: &App, channel: &str) -> Result<()> {
        if !self.should_send_webhook(app, "channel_vacated") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "channel_vacated",
            "channel": channel
        });
        let signature = format!("{}:{}:channel_vacated", app.id, channel);
        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    pub async fn send_member_added(&self, app: &App, channel: &str, user_id: &str) -> Result<()> {
        if !self.should_send_webhook(app, "member_added") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "member_added",
            "channel": channel,
            "user_id": user_id
        });
        let signature = format!("{}:{}:{}:member_added", app.id, channel, user_id);
        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    pub async fn send_member_removed(&self, app: &App, channel: &str, user_id: &str) -> Result<()> {
        if !self.should_send_webhook(app, "member_removed") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "member_removed",
            "channel": channel,
            "user_id": user_id
        });
        let signature = format!("{}:{}:{}:member_removed", app.id, channel, user_id);
        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    pub async fn send_member_updated(
        &self,
        app: &App,
        channel: &str,
        user_id: &str,
        user_info: Value,
    ) -> Result<()> {
        if !self.should_send_webhook(app, "member_updated") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "member_updated",
            "channel": channel,
            "user_id": user_id,
            "user_info": user_info
        });
        let signature = format!("{}:{}:{}:member_updated", app.id, channel, user_id);
        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    pub async fn send_client_event(
        &self,
        app: &App,
        channel: &str,
        event_name: &str,
        event_data: Value,
        socket_id: Option<&str>,
        user_id: Option<&str>,
    ) -> Result<()> {
        if !self.should_send_webhook(app, "client_event") {
            return Ok(());
        }

        let mut client_event_pusher_payload = json!({
            "name": "client_event",
            "channel": channel,
            "event": event_name,
            "data": event_data,
            "socket_id": socket_id,
        });

        if channel.starts_with("presence-")
            && let Some(uid) = user_id
        {
            client_event_pusher_payload["user_id"] = json!(uid);
        }

        let signature = format!(
            "{}:{}:{}:client_event",
            app.id,
            channel,
            socket_id.unwrap_or("unknown")
        );
        let job_data = self.create_job_data(app, vec![client_event_pusher_payload], &signature);
        self.add_webhook(job_data).await
    }

    pub async fn send_cache_missed(&self, app: &App, channel: &str) -> Result<()> {
        if !self.should_send_webhook(app, "cache_miss") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "cache_miss",
            "channel": channel,
            "data" : "{}"
        });
        let signature = format!("{}:{}:cache_miss", app.id, channel);
        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    pub async fn send_ai_stream_cancelled(
        &self,
        app: &App,
        channel: &str,
        message_serial: &str,
        reason: &str,
    ) -> Result<()> {
        if !self.should_send_webhook(app, "ai_stream_cancelled") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "ai_stream_cancelled",
            "channel": channel,
            "message_serial": message_serial,
            "reason": reason,
        });
        let signature = format!(
            "{}:{}:{}:ai_stream_cancelled",
            app.id, channel, message_serial
        );
        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    pub async fn send_ai_run_started(
        &self,
        app: &App,
        channel: &str,
        run_id: Option<&str>,
        client_id: Option<&str>,
    ) -> Result<()> {
        if !self.should_send_webhook(app, "ai_run_started") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "ai_run_started",
            "channel": channel,
            "run_id": run_id,
            "client_id": client_id,
        });
        let signature = format!(
            "{}:{}:{}:ai_run_started",
            app.id,
            channel,
            run_id.unwrap_or("unknown")
        );
        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    pub async fn send_ai_turn_started(
        &self,
        app: &App,
        channel: &str,
        turn_id: Option<&str>,
        client_id: Option<&str>,
    ) -> Result<()> {
        if !self.should_send_webhook(app, "ai_turn_started") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "ai_turn_started",
            "channel": channel,
            "turn_id": turn_id,
            "client_id": client_id,
        });
        let signature = format!(
            "{}:{}:{}:ai_turn_started",
            app.id,
            channel,
            turn_id.unwrap_or("unknown")
        );
        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    pub async fn send_ai_run_ended(
        &self,
        app: &App,
        channel: &str,
        run_id: Option<&str>,
        reason: &str,
        error_code: Option<&str>,
    ) -> Result<()> {
        if !self.should_send_webhook(app, "ai_run_ended") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "ai_run_ended",
            "channel": channel,
            "run_id": run_id,
            "reason": reason,
            "error_code": error_code,
        });
        let signature = format!(
            "{}:{}:{}:{}:ai_run_ended",
            app.id,
            channel,
            run_id.unwrap_or("unknown"),
            reason
        );
        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    pub async fn send_ai_turn_ended(
        &self,
        app: &App,
        channel: &str,
        turn_id: Option<&str>,
        reason: &str,
        error_code: Option<&str>,
    ) -> Result<()> {
        if !self.should_send_webhook(app, "ai_turn_ended") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "ai_turn_ended",
            "channel": channel,
            "turn_id": turn_id,
            "reason": reason,
            "error_code": error_code,
        });
        let signature = format!(
            "{}:{}:{}:{}:ai_turn_ended",
            app.id,
            channel,
            turn_id.unwrap_or("unknown"),
            reason
        );
        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    pub async fn send_ai_cancel_requested(
        &self,
        app: &App,
        channel: &str,
        turn_id: Option<&str>,
        client_id: Option<&str>,
    ) -> Result<()> {
        if !self.should_send_webhook(app, "ai_cancel_requested") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "ai_cancel_requested",
            "channel": channel,
            "run_id": turn_id,
            "turn_id": turn_id,
            "client_id": client_id,
        });
        let signature = format!(
            "{}:{}:{}:ai_cancel_requested",
            app.id,
            channel,
            turn_id.unwrap_or("unknown")
        );
        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    pub async fn send_ai_stream_orphaned(
        &self,
        app: &App,
        channel: &str,
        message_serial: &str,
        reason: &str,
    ) -> Result<()> {
        if !self.should_send_webhook(app, "ai_stream_orphaned") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "ai_stream_orphaned",
            "channel": channel,
            "message_serial": message_serial,
            "reason": reason,
        });
        let signature = format!(
            "{}:{}:{}:ai_stream_orphaned",
            app.id, channel, message_serial
        );
        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    pub async fn send_message_version_created(
        &self,
        app: &App,
        channel: &str,
        message_serial: &str,
        version_serial: &str,
        action: &str,
    ) -> Result<()> {
        if !self.should_send_webhook(app, "message_version_created") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "message_version_created",
            "channel": channel,
            "message_serial": message_serial,
            "version_serial": version_serial,
            "action": action,
        });
        let signature = format!(
            "{}:{}:{}:{}:message_version_created",
            app.id, channel, message_serial, version_serial
        );
        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    pub async fn send_annotation_created(
        &self,
        app: &App,
        channel: &str,
        message_serial: &str,
        annotation_serial: &str,
        annotation_type: &str,
    ) -> Result<()> {
        if !self.should_send_webhook(app, "annotation_created") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "annotation_created",
            "channel": channel,
            "message_serial": message_serial,
            "annotation_serial": annotation_serial,
            "annotation_type": annotation_type,
        });
        let signature = format!(
            "{}:{}:{}:{}:annotation_created",
            app.id, channel, message_serial, annotation_serial
        );
        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    pub async fn send_annotation_deleted(
        &self,
        app: &App,
        channel: &str,
        message_serial: &str,
        annotation_serial: &str,
        deleted_annotation_serial: &str,
        annotation_type: &str,
    ) -> Result<()> {
        if !self.should_send_webhook(app, "annotation_deleted") {
            return Ok(());
        }
        let event_obj = json!({
            "name": "annotation_deleted",
            "channel": channel,
            "message_serial": message_serial,
            "annotation_serial": annotation_serial,
            "deleted_annotation_serial": deleted_annotation_serial,
            "annotation_type": annotation_type,
        });
        let signature = format!(
            "{}:{}:{}:{}:annotation_deleted",
            app.id, channel, message_serial, annotation_serial
        );
        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    /// Sends a webhook when the subscription count for a channel changes.
    pub async fn send_subscription_count_changed(
        &self,
        app: &App,
        channel: &str,
        subscription_count: usize,
    ) -> Result<()> {
        if !self.should_send_webhook(app, "subscription_count") {
            return Ok(());
        }

        let event_obj = json!({
            "name": "subscription_count",
            "channel": channel,
            "subscription_count": subscription_count
        });

        let signature = format!(
            "{}:{}:subscription_count:{}",
            app.id, channel, subscription_count
        );

        let job_data = self.create_job_data(app, vec![event_obj], &signature);
        self.add_webhook(job_data).await
    }

    /// Check the health of the queue manager used by webhook integration
    pub async fn check_queue_health(&self) -> Result<()> {
        if let Some(qm) = &self.queue_manager {
            qm.check_health().await
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_app::memory_app_manager::MemoryAppManager;
    use sockudo_core::app::{AppFeaturesPolicy, AppLimitsPolicy, AppPolicy};
    use sockudo_core::webhook_types::{JobData, JobPayload, Webhook};
    use sockudo_queue::manager::QueueManagerFactory;

    async fn create_test_queue_manager() -> Arc<QueueManager> {
        let driver = QueueManagerFactory::create("memory", None, None, None, None)
            .await
            .expect("Failed to create test queue manager");
        Arc::new(QueueManager::new(driver))
    }

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
    async fn test_send_cache_missed() {
        let app = test_app();
        let app_manager = Arc::new(MemoryAppManager::new());
        let config = WebhookConfig {
            ..Default::default()
        };
        let queue_manager = create_test_queue_manager().await;
        let integration = WebhookIntegration::new(config, app_manager.clone(), Some(queue_manager))
            .await
            .unwrap();

        let result = integration.send_cache_missed(&app, "test_channel").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_send_ai_stream_cancelled() {
        let mut app = test_app();
        app.policy.webhooks = Some(vec![Webhook {
            event_types: vec!["ai_stream_cancelled".to_string()],
            ..Webhook::default()
        }]);
        let app_manager = Arc::new(MemoryAppManager::new());
        let config = WebhookConfig {
            ..Default::default()
        };
        let queue_manager = create_test_queue_manager().await;
        let integration = WebhookIntegration::new(config, app_manager, Some(queue_manager))
            .await
            .unwrap();

        let result = integration
            .send_ai_stream_cancelled(&app, "ai-chat", "msg-1", "orphan_timeout")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_send_ai_observability_and_version_webhooks() {
        let mut app = test_app();
        app.policy.webhooks = Some(vec![Webhook {
            event_types: vec![
                "ai_run_started".to_string(),
                "ai_run_ended".to_string(),
                "ai_turn_started".to_string(),
                "ai_turn_ended".to_string(),
                "ai_cancel_requested".to_string(),
                "ai_stream_orphaned".to_string(),
                "message_version_created".to_string(),
                "annotation_created".to_string(),
                "annotation_deleted".to_string(),
            ],
            ..Webhook::default()
        }]);
        let app_manager = Arc::new(MemoryAppManager::new());
        let queue_manager = create_test_queue_manager().await;
        let integration =
            WebhookIntegration::new(WebhookConfig::default(), app_manager, Some(queue_manager))
                .await
                .unwrap();

        assert!(
            integration
                .send_ai_run_started(&app, "ai-chat", Some("run-1"), Some("client-1"))
                .await
                .is_ok()
        );
        assert!(
            integration
                .send_ai_run_ended(&app, "ai-chat", Some("run-1"), "complete", None)
                .await
                .is_ok()
        );
        assert!(
            integration
                .send_ai_turn_started(&app, "ai-chat", Some("legacy-turn-1"), Some("client-1"))
                .await
                .is_ok()
        );
        assert!(
            integration
                .send_ai_turn_ended(&app, "ai-chat", Some("legacy-turn-1"), "complete", None)
                .await
                .is_ok()
        );
        assert!(
            integration
                .send_ai_cancel_requested(&app, "ai-chat", Some("run-1"), Some("client-1"))
                .await
                .is_ok()
        );
        assert!(
            integration
                .send_ai_stream_orphaned(&app, "ai-chat", "msg-1", "orphan_timeout")
                .await
                .is_ok()
        );
        assert!(
            integration
                .send_message_version_created(&app, "ai-chat", "msg-1", "ver-1", "message.append",)
                .await
                .is_ok()
        );
        assert!(
            integration
                .send_annotation_created(&app, "ai-chat", "msg-1", "ann-1", "reaction")
                .await
                .is_ok()
        );
        assert!(
            integration
                .send_annotation_deleted(
                    &app,
                    "ai-chat",
                    "msg-1",
                    "ann-del-1",
                    "ann-1",
                    "reaction",
                )
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_send_subscription_count_changed() {
        let app = test_app();
        let app_manager = Arc::new(MemoryAppManager::new());
        let config = WebhookConfig {
            ..Default::default()
        };
        let queue_manager = create_test_queue_manager().await;
        let integration = WebhookIntegration::new(config, app_manager.clone(), Some(queue_manager))
            .await
            .unwrap();

        let result = integration
            .send_subscription_count_changed(&app, "test_channel", 5)
            .await;
        assert!(result.is_ok());

        let config = WebhookConfig {
            enabled: true,
            ..Default::default()
        };
        let queue_manager = create_test_queue_manager().await;
        let integration = WebhookIntegration::new(config, app_manager, Some(queue_manager))
            .await
            .unwrap();

        let result = integration
            .send_subscription_count_changed(&app, "test_channel", 5)
            .await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_webhook_config_serialization() {
        let config = WebhookConfig {
            enabled: true,
            batching: BatchingConfig {
                enabled: true,
                duration: 1000,
                size: 50,
            },
            retry: sockudo_core::options::WebhookRetryConfig::default(),
            request_timeout_ms: 10_000,
            process_id: "test-process".to_string(),
            debug: false,
        };

        let serialized = sonic_rs::to_string(&config).unwrap();
        let deserialized: WebhookConfig = sonic_rs::from_str(&serialized).unwrap();

        assert_eq!(config.enabled, deserialized.enabled);
        assert_eq!(config.batching.enabled, deserialized.batching.enabled);
        assert_eq!(config.batching.duration, deserialized.batching.duration);
        assert_eq!(config.batching.size, deserialized.batching.size);
    }

    #[tokio::test]
    async fn test_webhook_integration_new() {
        let app_manager = Arc::new(MemoryAppManager::new());
        let config = WebhookConfig {
            ..Default::default()
        };

        let queue_manager = create_test_queue_manager().await;
        let integration = WebhookIntegration::new(config, app_manager, Some(queue_manager)).await;
        assert!(integration.is_ok());
    }

    #[tokio::test]
    async fn test_webhook_integration_send_event() {
        let app = test_app();
        let app_manager = Arc::new(MemoryAppManager::new());
        let config = WebhookConfig {
            ..Default::default()
        };
        let queue_manager = create_test_queue_manager().await;
        let integration = WebhookIntegration::new(config, app_manager.clone(), Some(queue_manager))
            .await
            .unwrap();

        let result = integration
            .send_client_event(
                &app,
                "test_channel",
                "test_event",
                json!("test_data"),
                None,
                None,
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_webhook_integration_send_client_event() {
        let app = test_app();
        let app_manager = Arc::new(MemoryAppManager::new());
        let config = WebhookConfig {
            ..Default::default()
        };
        let queue_manager = create_test_queue_manager().await;
        let integration = WebhookIntegration::new(config, app_manager.clone(), Some(queue_manager))
            .await
            .unwrap();

        let result = integration
            .send_client_event(
                &app,
                "test_channel",
                "test_event",
                json!("test_data"),
                None,
                None,
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_webhook_integration_send_member_added() {
        let app = test_app();
        let app_manager = Arc::new(MemoryAppManager::new());
        let config = WebhookConfig {
            ..Default::default()
        };
        let queue_manager = create_test_queue_manager().await;
        let integration = WebhookIntegration::new(config, app_manager.clone(), Some(queue_manager))
            .await
            .unwrap();

        let result = integration
            .send_member_added(&app, "test_channel", "test_user")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_webhook_integration_send_member_removed() {
        let app = test_app();
        let app_manager = Arc::new(MemoryAppManager::new());
        let config = WebhookConfig {
            ..Default::default()
        };
        let queue_manager = create_test_queue_manager().await;
        let integration = WebhookIntegration::new(config, app_manager.clone(), Some(queue_manager))
            .await
            .unwrap();

        let result = integration
            .send_member_removed(&app, "test_channel", "test_user")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_webhook_integration_send_channel_occupied() {
        let app = test_app();
        let app_manager = Arc::new(MemoryAppManager::new());
        let config = WebhookConfig {
            ..Default::default()
        };
        let queue_manager = create_test_queue_manager().await;
        let integration = WebhookIntegration::new(config, app_manager.clone(), Some(queue_manager))
            .await
            .unwrap();

        let result = integration
            .send_channel_occupied(&app, "test_channel")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_webhook_integration_send_channel_vacated() {
        let app = test_app();
        let app_manager = Arc::new(MemoryAppManager::new());
        let config = WebhookConfig {
            ..Default::default()
        };
        let queue_manager = create_test_queue_manager().await;
        let integration = WebhookIntegration::new(config, app_manager.clone(), Some(queue_manager))
            .await
            .unwrap();

        let result = integration.send_channel_vacated(&app, "test_channel").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_webhook_integration_send_subscription_count_changed() {
        let app = test_app();
        let app_manager = Arc::new(MemoryAppManager::new());
        let config = WebhookConfig {
            ..Default::default()
        };
        let queue_manager = create_test_queue_manager().await;
        let integration = WebhookIntegration::new(config, app_manager.clone(), Some(queue_manager))
            .await
            .unwrap();

        let result = integration
            .send_subscription_count_changed(&app, "test_channel", 5)
            .await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_merge_jobs_for_queue_batches_by_app_and_size() {
        let jobs = vec![
            JobData {
                job_id: None,
                app_key: "key-a".to_string(),
                app_id: "app-a".to_string(),
                app_secret: "secret-a".to_string(),
                payload: JobPayload {
                    time_ms: 10,
                    events: vec![json!({"name": "channel_occupied", "channel": "one"})],
                },
                original_signature: "sig-1".to_string(),
            },
            JobData {
                job_id: None,
                app_key: "key-a".to_string(),
                app_id: "app-a".to_string(),
                app_secret: "secret-a".to_string(),
                payload: JobPayload {
                    time_ms: 20,
                    events: vec![json!({"name": "channel_vacated", "channel": "two"})],
                },
                original_signature: "sig-2".to_string(),
            },
            JobData {
                job_id: None,
                app_key: "key-b".to_string(),
                app_id: "app-b".to_string(),
                app_secret: "secret-b".to_string(),
                payload: JobPayload {
                    time_ms: 30,
                    events: vec![json!({"name": "channel_occupied", "channel": "three"})],
                },
                original_signature: "sig-3".to_string(),
            },
        ];

        let merged = WebhookIntegration::merge_jobs_for_queue(jobs, 2);

        assert_eq!(merged.len(), 2);
        assert!(merged.iter().all(|job| job.job_id.is_some()));
        assert_ne!(merged[0].job_id, merged[1].job_id);
        assert_eq!(merged[0].app_id, "app-a");
        assert_eq!(merged[0].payload.events.len(), 2);
        assert_eq!(merged[1].app_id, "app-b");
        assert_eq!(merged[1].payload.events.len(), 1);
    }

    #[test]
    fn test_merge_jobs_for_queue_splits_oversized_jobs() {
        let job = JobData {
            job_id: None,
            app_key: "key-a".to_string(),
            app_id: "app-a".to_string(),
            app_secret: "secret-a".to_string(),
            payload: JobPayload {
                time_ms: 10,
                events: vec![
                    json!({"name": "channel_occupied", "channel": "one"}),
                    json!({"name": "channel_occupied", "channel": "two"}),
                    json!({"name": "channel_occupied", "channel": "three"}),
                ],
            },
            original_signature: "sig-1".to_string(),
        };

        let merged = WebhookIntegration::merge_jobs_for_queue(vec![job], 2);

        assert_eq!(merged.len(), 2);
        assert!(merged.iter().all(|job| job.job_id.is_some()));
        assert_ne!(merged[0].job_id, merged[1].job_id);
        assert_eq!(merged[0].payload.events.len(), 2);
        assert_eq!(merged[1].payload.events.len(), 1);
    }
}
