use futures_util::StreamExt;

use crate::domain::{
    DeadLetter, DeliveryEvent, DeliveryFeedback, DeliveryOutcome, DeliveryResult, DevicePushState,
    PublishLifecycleState,
};
use crate::meta::{PushMetaEvent, emit_push_meta_event};
use crate::metrics::{PushMetrics, provider_label};
use crate::pipeline::{PushPipelineResult, PushQueuePayload, PushQueueStage, QueueMessage, now_ms};
use crate::retry::RetryPolicy;
use crate::storage::{DynPushStore, IdempotencyRecord};

#[derive(Clone)]
pub struct PushFeedbackProcessor {
    store: DynPushStore,
    queue: crate::pipeline::DynPushQueue,
    failure_threshold: u32,
    retry_policy: RetryPolicy,
    metrics: PushMetrics,
}

impl PushFeedbackProcessor {
    const FEEDBACK_IDEMPOTENCY_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1000;

    pub fn new(store: DynPushStore, queue: crate::pipeline::DynPushQueue) -> Self {
        Self {
            store,
            queue,
            failure_threshold: 3,
            retry_policy: RetryPolicy::default(),
            metrics: PushMetrics::default(),
        }
    }

    pub fn with_metrics(mut self, metrics: PushMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_failure_threshold(mut self, failure_threshold: u32) -> Self {
        self.failure_threshold = failure_threshold.max(1);
        self
    }

    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy.bounded();
        self
    }

    pub async fn run_once(&self, consumer_group: &str) -> PushPipelineResult<usize> {
        let messages = self
            .queue
            .consume(PushQueueStage::DeliveryResults, consumer_group, 64, 30_000)
            .await?;
        let outcomes = futures_util::stream::iter(messages.into_iter().map(|message| {
            let processor = self.clone();
            async move {
                let ack = message.ack.clone();
                match processor.handle_message(message).await {
                    Ok(()) => Ok(1_usize),
                    Err(error) => {
                        processor.queue.dead_letter(ack, error.to_string()).await?;
                        Ok(0)
                    }
                }
            }
        }))
        .buffer_unordered(16)
        .collect::<Vec<PushPipelineResult<usize>>>()
        .await;

        outcomes.into_iter().try_fold(0_usize, |count, outcome| {
            outcome.map(|processed| count + processed)
        })
    }

    async fn handle_message(&self, message: QueueMessage) -> PushPipelineResult<()> {
        let feedback = match message.payload.clone() {
            PushQueuePayload::DeliveryResult(result) => DeliveryFeedback::from_result(*result),
            PushQueuePayload::DeliveryFeedback(feedback) => *feedback,
            _ => {
                self.queue
                    .dead_letter(message.ack, "unexpected payload for feedback".to_owned())
                    .await?;
                return Ok(());
            }
        };

        self.apply_feedback(feedback).await?;
        self.queue.ack(message.ack).await?;
        Ok(())
    }

    pub async fn apply_result(&self, result: DeliveryResult) -> PushPipelineResult<()> {
        self.apply_feedback(DeliveryFeedback::from_result(result))
            .await
    }

    pub async fn apply_feedback(&self, feedback: DeliveryFeedback) -> PushPipelineResult<()> {
        let result = &feedback.result;
        let event_id = result_event_id(&feedback);
        let dedupe_key = format!("delivery-result:{event_id}");
        let inserted = self
            .store
            .put_idempotency_record_if_absent(IdempotencyRecord {
                app_id: result.app_id.clone(),
                key: dedupe_key,
                publish_id: result.publish_id.clone(),
                expires_at_ms: now_ms().saturating_add(Self::FEEDBACK_IDEMPOTENCY_TTL_MS),
            })
            .await?;
        if !inserted {
            self.metrics.duplicate_suppressed();
            return Ok(());
        }

        let event = DeliveryEvent {
            app_id: result.app_id.clone(),
            publish_id: result.publish_id.clone(),
            event_id,
            occurred_at_ms: now_ms(),
            result: result.clone(),
        };
        self.store.append_delivery_event(event).await?;
        if !matches!(result.outcome, DeliveryOutcome::Accepted) {
            emit_push_meta_event(PushMetaEvent::provider_rejected(
                &result.app_id,
                &result.publish_id,
                result.provider,
                result.outcome,
                result.error.as_ref().map(|error| error.class.as_str()),
            ));
        }

        if let Some(device_id) = result.device_id.as_deref() {
            self.update_device_state(result, device_id).await?;
        }
        self.update_publish_status(result).await?;
        self.handle_retry_or_dlq(&feedback).await?;
        emit_feedback_meta_event(result);
        Ok(())
    }

    async fn update_device_state(
        &self,
        result: &DeliveryResult,
        device_id: &str,
    ) -> PushPipelineResult<()> {
        match result.outcome {
            DeliveryOutcome::Accepted => {
                if let Some(mut device) = self.store.get_device(&result.app_id, device_id).await? {
                    let previous = device.push.state;
                    device.record_delivery_success();
                    device.last_active_at_ms = now_ms();
                    if previous != device.push.state {
                        self.metrics.device_state_transition(
                            &result.app_id,
                            previous,
                            device.push.state,
                        );
                        emit_push_meta_event(PushMetaEvent::device_state_changed(
                            &result.app_id,
                            &result.publish_id,
                            previous,
                            device.push.state,
                        ));
                    }
                    self.store.upsert_device(device).await?;
                }
            }
            DeliveryOutcome::Rejected if is_invalid_token(result) => {
                self.store.delete_device(&result.app_id, device_id).await?;
                self.metrics
                    .token_invalidated(result.provider, &result.app_id);
                emit_push_meta_event(PushMetaEvent::token_invalidated(
                    &result.app_id,
                    &result.publish_id,
                    result.provider,
                ));
            }
            DeliveryOutcome::Rejected | DeliveryOutcome::Retryable => {
                if let Some(mut device) = self.store.get_device(&result.app_id, device_id).await? {
                    let previous = device.push.state;
                    if device.push.failure_count == 0 {
                        tracing::warn!(
                            app_id = %result.app_id,
                            publish_id = %result.publish_id,
                            device_id = %device_id,
                            provider = ?result.provider,
                            "first push delivery failure"
                        );
                    }
                    device.record_delivery_failure(
                        self.failure_threshold,
                        result
                            .error
                            .as_ref()
                            .and_then(|error| error.reason.clone())
                            .unwrap_or_else(|| "provider delivery failed".to_owned()),
                    );
                    if previous != device.push.state {
                        self.metrics.device_state_transition(
                            &result.app_id,
                            previous,
                            device.push.state,
                        );
                        emit_push_meta_event(PushMetaEvent::device_state_changed(
                            &result.app_id,
                            &result.publish_id,
                            previous,
                            device.push.state,
                        ));
                        if device.push.state == DevicePushState::Failed {
                            tracing::warn!(
                                app_id = %result.app_id,
                                publish_id = %result.publish_id,
                                device_id = %device_id,
                                provider = ?result.provider,
                                "push device transitioned to FAILED"
                            );
                        }
                    }
                    self.store.upsert_device(device).await?;
                }
            }
            DeliveryOutcome::Expired | DeliveryOutcome::Cancelled => {}
        }
        Ok(())
    }

    async fn update_publish_status(&self, result: &DeliveryResult) -> PushPipelineResult<()> {
        let Some(mut status) = self
            .store
            .get_publish_status(&result.app_id, &result.publish_id)
            .await?
        else {
            return Ok(());
        };

        match result.outcome {
            DeliveryOutcome::Accepted => {
                status.counters.dispatched = status.counters.dispatched.saturating_add(1);
                status.counters.succeeded = status.counters.succeeded.saturating_add(1);
            }
            DeliveryOutcome::Rejected => {
                status.counters.dispatched = status.counters.dispatched.saturating_add(1);
                status.counters.failed = status.counters.failed.saturating_add(1);
            }
            DeliveryOutcome::Expired => {
                status.counters.dispatched = status.counters.dispatched.saturating_add(1);
                status.counters.expired = status.counters.expired.saturating_add(1);
            }
            DeliveryOutcome::Retryable | DeliveryOutcome::Cancelled => {}
        }
        if let Some(retry_after_ms) = result.error.as_ref().and_then(|error| error.retry_after_ms) {
            status.retry_after_ms = Some(retry_after_ms);
        }
        status.state = terminal_state(
            status.counters.succeeded,
            status.counters.failed,
            status.counters.expired,
            status.counters.dead_lettered,
            status.counters.planned,
            status.state,
        );
        self.metrics
            .delivery_status(&result.app_id, outcome_label(result.outcome));
        if matches!(
            status.state,
            PublishLifecycleState::Succeeded
                | PublishLifecycleState::PartiallySucceeded
                | PublishLifecycleState::Failed
                | PublishLifecycleState::Expired
                | PublishLifecycleState::DeadLettered
        ) {
            emit_push_meta_event(PushMetaEvent::completed(
                &result.app_id,
                &result.publish_id,
                lifecycle_label(status.state),
            ));
            tracing::info!(
                app_id = %result.app_id,
                publish_id = %result.publish_id,
                state = ?status.state,
                "push publish completed"
            );
        }
        self.store.put_publish_status(status).await?;
        Ok(())
    }

    async fn handle_retry_or_dlq(&self, feedback: &DeliveryFeedback) -> PushPipelineResult<()> {
        let result = &feedback.result;
        if matches!(result.outcome, DeliveryOutcome::Retryable) {
            let Some(entry) = self.retry_policy.schedule_retry(feedback, now_ms()) else {
                let dead_letter = DeadLetter {
                    app_id: result.app_id.clone(),
                    publish_id: result.publish_id.clone(),
                    stage: "delivery_result".to_owned(),
                    key: result.batch_id.clone(),
                    reason: "retryable result missing retry context".to_owned(),
                    occurred_at_ms: now_ms(),
                };
                self.queue
                    .produce(
                        PushQueueStage::DeadLetters,
                        dead_letter.key.clone(),
                        PushQueuePayload::DeadLetter(Box::new(dead_letter)),
                    )
                    .await?;
                self.mark_retry_context_missing(result).await?;
                emit_push_meta_event(PushMetaEvent::dead_letter(
                    &result.app_id,
                    &result.publish_id,
                    "delivery_result",
                    "retryable result missing retry context",
                ));
                return Ok(());
            };
            let next_attempt_at_ms = entry.next_attempt_at_ms;
            self.queue
                .retry_at(
                    PushQueueStage::RetrySchedule,
                    entry.key.clone(),
                    PushQueuePayload::RetrySchedule(Box::new(entry)),
                    next_attempt_at_ms,
                )
                .await?;
            self.mark_retry_scheduled(result, next_attempt_at_ms)
                .await?;
            self.metrics
                .retry_scheduled(result.provider, &result.app_id);
            emit_push_meta_event(PushMetaEvent::scheduler_event(
                &result.app_id,
                &result.publish_id,
                "retry-scheduled",
            ));
        } else if matches!(result.outcome, DeliveryOutcome::Rejected) && !is_invalid_token(result) {
            let dead_letter = DeadLetter {
                app_id: result.app_id.clone(),
                publish_id: result.publish_id.clone(),
                stage: "delivery_result".to_owned(),
                key: result.batch_id.clone(),
                reason: result
                    .error
                    .as_ref()
                    .map(|error| error.class.clone())
                    .unwrap_or_else(|| "rejected".to_owned()),
                occurred_at_ms: now_ms(),
            };
            self.queue
                .produce(
                    PushQueueStage::DeadLetters,
                    dead_letter.key.clone(),
                    PushQueuePayload::DeadLetter(Box::new(dead_letter)),
                )
                .await?;
            emit_push_meta_event(PushMetaEvent::dead_letter(
                &result.app_id,
                &result.publish_id,
                "delivery_result",
                result
                    .error
                    .as_ref()
                    .map(|error| error.class.as_str())
                    .unwrap_or("rejected"),
            ));
        }
        Ok(())
    }

    async fn mark_retry_scheduled(
        &self,
        result: &DeliveryResult,
        next_attempt_at_ms: u64,
    ) -> PushPipelineResult<()> {
        if let Some(mut status) = self
            .store
            .get_publish_status(&result.app_id, &result.publish_id)
            .await?
        {
            status.counters.retry_scheduled = status.counters.retry_scheduled.saturating_add(1);
            status.retry_after_ms = Some(next_attempt_at_ms);
            self.store.put_publish_status(status).await?;
        }
        Ok(())
    }

    async fn mark_retry_context_missing(&self, result: &DeliveryResult) -> PushPipelineResult<()> {
        if let Some(mut status) = self
            .store
            .get_publish_status(&result.app_id, &result.publish_id)
            .await?
        {
            status.counters.dead_lettered = status.counters.dead_lettered.saturating_add(1);
            status.error_reason = Some("retryable result missing retry context".to_owned());
            status.state = terminal_state(
                status.counters.succeeded,
                status.counters.failed,
                status.counters.expired,
                status.counters.dead_lettered,
                status.counters.planned,
                status.state,
            );
            self.store.put_publish_status(status).await?;
        }
        Ok(())
    }
}

fn result_event_id(feedback: &DeliveryFeedback) -> String {
    let result = &feedback.result;
    format!(
        "result-{}-{}-{}-{}-{}-{}",
        provider_label(result.provider),
        result.publish_id,
        result.batch_id,
        feedback.delivery_key,
        result.attempt,
        outcome_label(result.outcome)
    )
}

fn outcome_label(outcome: DeliveryOutcome) -> &'static str {
    match outcome {
        DeliveryOutcome::Accepted => "accepted",
        DeliveryOutcome::Rejected => "rejected",
        DeliveryOutcome::Retryable => "retryable",
        DeliveryOutcome::Expired => "expired",
        DeliveryOutcome::Cancelled => "cancelled",
    }
}

fn lifecycle_label(state: PublishLifecycleState) -> &'static str {
    match state {
        PublishLifecycleState::Queued => "queued",
        PublishLifecycleState::Planning => "planning",
        PublishLifecycleState::Throttled => "throttled",
        PublishLifecycleState::QuotaExceeded => "quota_exceeded",
        PublishLifecycleState::Dispatching => "dispatching",
        PublishLifecycleState::Cancelled => "cancelled",
        PublishLifecycleState::Expired => "expired",
        PublishLifecycleState::Failed => "failed",
        PublishLifecycleState::DeadLettered => "dead_lettered",
        PublishLifecycleState::Succeeded => "succeeded",
        PublishLifecycleState::PartiallySucceeded => "partially_succeeded",
    }
}

fn terminal_state(
    succeeded: u64,
    failed: u64,
    expired: u64,
    dead_lettered: u64,
    planned: u64,
    current: PublishLifecycleState,
) -> PublishLifecycleState {
    let terminal = succeeded.saturating_add(failed).saturating_add(expired);
    let terminal = terminal.saturating_add(dead_lettered);
    if planned == 0 || terminal < planned {
        return current;
    }
    if failed == 0 && expired == 0 && dead_lettered == 0 {
        PublishLifecycleState::Succeeded
    } else if succeeded > 0 {
        PublishLifecycleState::PartiallySucceeded
    } else if expired > 0 {
        PublishLifecycleState::Expired
    } else if dead_lettered > 0 {
        PublishLifecycleState::DeadLettered
    } else {
        PublishLifecycleState::Failed
    }
}

fn is_invalid_token(result: &DeliveryResult) -> bool {
    result.error.as_ref().is_some_and(|error| {
        matches!(
            error.class.as_str(),
            "invalid_token" | "unregistered" | "not_registered" | "expired_channel"
        )
    })
}

pub fn device_is_terminally_failed(state: DevicePushState) -> bool {
    matches!(state, DevicePushState::Failed)
}

fn emit_feedback_meta_event(result: &DeliveryResult) {
    tracing::info!(
        target: "[meta]log:push",
        app_id = %result.app_id,
        publish_id = %result.publish_id,
        provider = ?result.provider,
        batch_id = %result.batch_id,
        device_id = result.device_id.as_deref().unwrap_or("[provider-target]"),
        outcome = outcome_label(result.outcome),
        invalid_token = is_invalid_token(result),
        "push provider feedback processed"
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sonic_rs::json;

    use crate::domain::{
        DeliveryFeedback, DeliveryJob, DeliveryOutcome, DeliveryResult, ProviderError,
        PublishCounters, PublishLifecycleState, PublishStatus, PushPayload, PushProviderKind,
        PushRecipient, SecretString,
    };
    use crate::memory::MemoryPushStore;
    use crate::metrics::PushMetrics;
    use crate::pipeline::{MemoryPushQueue, PushQueue};
    use crate::retry::RetryPolicy;
    use crate::storage::PushPublishStatusStore;

    use super::*;

    #[tokio::test]
    async fn feedback_duplicate_suppression_prevents_counter_drift() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store
            .put_publish_status(PublishStatus {
                app_id: "app-1".to_owned(),
                publish_id: "publish-1".to_owned(),
                state: PublishLifecycleState::Dispatching,
                counters: PublishCounters {
                    planned: 1,
                    dispatched: 0,
                    succeeded: 0,
                    failed: 0,
                    expired: 0,
                    retry_scheduled: 0,
                    retry_attempted: 0,
                    dead_lettered: 0,
                },
                fanout_regime: None,
                retry_after_ms: None,
                error_reason: None,
            })
            .await
            .unwrap();
        let metrics = PushMetrics::default();
        let processor =
            PushFeedbackProcessor::new(store.clone(), queue).with_metrics(metrics.clone());
        let result = DeliveryResult {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            provider: PushProviderKind::Fcm,
            batch_id: "batch-1".to_owned(),
            device_id: Some("device-1".to_owned()),
            outcome: DeliveryOutcome::Accepted,
            provider_message_id: Some("provider-1".to_owned()),
            error: None,
            attempt: 1,
        };

        processor.apply_result(result.clone()).await.unwrap();
        processor.apply_result(result).await.unwrap();

        let status = store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.counters.dispatched, 1);
        assert_eq!(status.counters.succeeded, 1);
        assert_eq!(status.state, PublishLifecycleState::Succeeded);
        assert_eq!(metrics.get("sockudo_push_duplicate_suppressed_total"), 1);
    }

    #[tokio::test]
    async fn feedback_dedupe_is_scoped_by_publish_id() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        for publish_id in ["publish-1", "publish-2"] {
            store
                .put_publish_status(PublishStatus {
                    app_id: "app-1".to_owned(),
                    publish_id: publish_id.to_owned(),
                    state: PublishLifecycleState::Dispatching,
                    counters: PublishCounters {
                        planned: 1,
                        dispatched: 0,
                        succeeded: 0,
                        failed: 0,
                        expired: 0,
                        retry_scheduled: 0,
                        retry_attempted: 0,
                        dead_lettered: 0,
                    },
                    fanout_regime: None,
                    retry_after_ms: None,
                    error_reason: None,
                })
                .await
                .unwrap();
        }
        let metrics = PushMetrics::default();
        let processor =
            PushFeedbackProcessor::new(store.clone(), queue).with_metrics(metrics.clone());

        for publish_id in ["publish-1", "publish-2"] {
            processor
                .apply_result(DeliveryResult {
                    app_id: "app-1".to_owned(),
                    publish_id: publish_id.to_owned(),
                    provider: PushProviderKind::Fcm,
                    batch_id: "batch-1".to_owned(),
                    device_id: Some("device-1".to_owned()),
                    outcome: DeliveryOutcome::Accepted,
                    provider_message_id: Some(format!("provider-{publish_id}")),
                    error: None,
                    attempt: 1,
                })
                .await
                .unwrap();
        }

        for publish_id in ["publish-1", "publish-2"] {
            let status = store
                .get_publish_status("app-1", publish_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(status.counters.dispatched, 1);
            assert_eq!(status.counters.succeeded, 1);
            assert_eq!(status.state, PublishLifecycleState::Succeeded);
        }
        assert_eq!(metrics.get("sockudo_push_duplicate_suppressed_total"), 0);
    }

    #[tokio::test]
    async fn retryable_feedback_enqueues_retry_with_original_job_context() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store.put_publish_status(status()).await.unwrap();
        let metrics = PushMetrics::default();
        let processor = PushFeedbackProcessor::new(store.clone(), queue.clone())
            .with_metrics(metrics.clone())
            .with_retry_policy(RetryPolicy {
                initial_backoff_ms: 1,
                max_backoff_ms: 1,
                jitter_ratio_percent: 0,
                ..RetryPolicy::default()
            });

        processor
            .apply_feedback(retryable_feedback())
            .await
            .unwrap();

        let mut messages = queue
            .consume(PushQueueStage::RetrySchedule, "retry-test", 1, 30_000)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        let PushQueuePayload::RetrySchedule(entry) = messages.pop().unwrap().payload else {
            panic!("expected retry schedule payload");
        };
        assert_eq!(entry.app_id, "app-1");
        assert_eq!(entry.publish_id, "publish-1");
        assert_eq!(entry.provider, Some(PushProviderKind::Fcm));
        assert_eq!(entry.attempt, 2);
        assert_eq!(entry.max_attempts, 5);
        let job = entry.job.unwrap();
        assert_eq!(job.device_id.as_deref(), Some("device-1"));
        assert_eq!(job.attempt, 2);
        assert_eq!(job.first_attempt_at_ms, Some(1_000));
        assert!(!entry.retry_idempotency_key.is_empty());

        let status = store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.counters.dispatched, 0);
        assert_eq!(status.counters.retry_scheduled, 1);
        assert_eq!(status.state, PublishLifecycleState::Dispatching);
        assert_eq!(metrics.get("sockudo_push_retry_scheduled_total"), 1);
    }

    #[tokio::test]
    async fn duplicate_retryable_feedback_is_suppressed() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store.put_publish_status(status()).await.unwrap();
        let metrics = PushMetrics::default();
        let processor =
            PushFeedbackProcessor::new(store.clone(), queue.clone()).with_metrics(metrics.clone());
        let feedback = retryable_feedback();

        processor.apply_feedback(feedback.clone()).await.unwrap();
        processor.apply_feedback(feedback).await.unwrap();

        let status = store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.counters.retry_scheduled, 1);
        assert_eq!(status.counters.dead_lettered, 0);
        assert_eq!(metrics.get("sockudo_push_duplicate_suppressed_total"), 1);
        let lag = queue.lag(PushQueueStage::RetrySchedule).await.unwrap();
        assert_eq!(lag.ready_depth + lag.delayed_depth, 1);
    }

    fn status() -> PublishStatus {
        PublishStatus {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            state: PublishLifecycleState::Dispatching,
            counters: PublishCounters {
                planned: 1,
                dispatched: 0,
                succeeded: 0,
                failed: 0,
                expired: 0,
                retry_scheduled: 0,
                retry_attempted: 0,
                dead_lettered: 0,
            },
            fanout_regime: None,
            retry_after_ms: None,
            error_reason: None,
        }
    }

    fn retryable_feedback() -> DeliveryFeedback {
        DeliveryFeedback {
            result: DeliveryResult {
                app_id: "app-1".to_owned(),
                publish_id: "publish-1".to_owned(),
                provider: PushProviderKind::Fcm,
                batch_id: "batch-1".to_owned(),
                device_id: Some("device-1".to_owned()),
                outcome: DeliveryOutcome::Retryable,
                provider_message_id: None,
                error: Some(ProviderError {
                    class: "unavailable".to_owned(),
                    reason: Some("provider unavailable".to_owned()),
                    retry_after_ms: Some(now_ms()),
                }),
                attempt: 1,
            },
            delivery_key: "delivery-key-1".to_owned(),
            retry_job: Some(Box::new(DeliveryJob {
                app_id: "app-1".to_owned(),
                publish_id: "publish-1".to_owned(),
                provider: PushProviderKind::Fcm,
                batch_id: "batch-1".to_owned(),
                device_id: Some("device-1".to_owned()),
                recipient: PushRecipient::Fcm {
                    registration_token: SecretString::new("token-1").unwrap(),
                },
                payload: Arc::new(PushPayload {
                    template_id: None,
                    template_data: json!({}),
                    title: Some("hello".to_owned()),
                    body: Some("body".to_owned()),
                    icon: None,
                    sound: None,
                    collapse_key: None,
                }),
                attempt: 1,
                first_attempt_at_ms: Some(1_000),
                not_before_ms: None,
                expires_at_ms: None,
            })),
            first_attempt_at_ms: Some(1_000),
            expires_at_ms: None,
        }
    }
}
