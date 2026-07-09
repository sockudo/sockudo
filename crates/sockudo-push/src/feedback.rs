use futures_util::StreamExt;

use crate::domain::{
    DeadLetter, DeliveryEvent, DeliveryFeedback, DeliveryOutcome, DeliveryResult, DevicePushState,
    ProviderError, ProviderFailureClass, PublishLifecycleState, PublishStatus,
};
use crate::meta::{PushMetaEvent, emit_push_meta_event};
use crate::metrics::{PushMetrics, provider_label};
use crate::pipeline::{
    PushPipelineResult, PushQueuePayload, PushQueueStage, QueueMessage, lock_publish_status, now_ms,
};
use crate::retry::RetryPolicy;
use crate::storage::{DynPushStore, IdempotencyRecord};

const INVALIDATION_GUARD_MIN_SAMPLES: u64 = 10;
const INVALIDATION_GUARD_RATIO_PERCENT: u64 = 20;

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
            let failure_class = result_failure_class(result);
            emit_push_meta_event(PushMetaEvent::provider_rejected(
                &result.app_id,
                &result.publish_id,
                result.provider,
                result.outcome,
                result.error.as_ref().map(|error| error.class.as_str()),
                Some(failure_class),
            ));
            self.metrics.provider_failure_class(
                result.provider,
                &result.app_id,
                failure_class.label(),
            );
        }

        if let Some(device_id) = result.device_id.as_deref() {
            self.update_device_state(result, device_id).await?;
        }
        let status = self.update_publish_status(result).await?;
        if is_device_terminal_failure(result)
            && let Some(status) = status.as_ref()
        {
            self.emit_invalidation_guard_if_needed(result, status);
        }
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
            DeliveryOutcome::Rejected if is_device_terminal_failure(result) => {
                self.store.delete_device(&result.app_id, device_id).await?;
                self.metrics
                    .token_invalidated(result.provider, &result.app_id);
                emit_push_meta_event(PushMetaEvent::token_invalidated(
                    &result.app_id,
                    &result.publish_id,
                    result.provider,
                ));
            }
            DeliveryOutcome::Rejected | DeliveryOutcome::Retryable
                if is_device_transient_failure(result) =>
            {
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
            DeliveryOutcome::Rejected | DeliveryOutcome::Retryable => {}
            DeliveryOutcome::Expired | DeliveryOutcome::Cancelled => {}
        }
        Ok(())
    }

    async fn update_publish_status(
        &self,
        result: &DeliveryResult,
    ) -> PushPipelineResult<Option<PublishStatus>> {
        let _status_guard = lock_publish_status(&result.app_id, &result.publish_id).await;
        let Some(current) = self
            .store
            .get_publish_status(&result.app_id, &result.publish_id)
            .await?
        else {
            return Ok(None);
        };
        let mut status = current.clone();

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
        let next = status.counters.resolve_lifecycle_state(current.state);
        // Audience counts for channel/client targets are estimates until fanout. A late valid
        // outcome may refine one terminal delivery summary into another, but never reactivates it.
        status.state = next;
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
        self.store.put_publish_status(status.clone()).await?;
        Ok(Some(status))
    }

    fn emit_invalidation_guard_if_needed(&self, result: &DeliveryResult, status: &PublishStatus) {
        if !invalidation_guard_threshold_crossed(status) {
            return;
        }
        self.metrics
            .token_invalidation_guard(result.provider, &result.app_id);
        emit_push_meta_event(PushMetaEvent::token_invalidation_guard(
            &result.app_id,
            &result.publish_id,
            result.provider,
            status.counters.planned,
            status.counters.failed,
            INVALIDATION_GUARD_RATIO_PERCENT,
        ));
        tracing::warn!(
            app_id = %result.app_id,
            publish_id = %result.publish_id,
            provider = ?result.provider,
            planned = status.counters.planned,
            failed = status.counters.failed,
            threshold_percent = INVALIDATION_GUARD_RATIO_PERCENT,
            "push token invalidation guard threshold crossed"
        );
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
        } else if matches!(result.outcome, DeliveryOutcome::Rejected)
            && !is_device_terminal_failure(result)
        {
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
        let _status_guard = lock_publish_status(&result.app_id, &result.publish_id).await;
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
        let _status_guard = lock_publish_status(&result.app_id, &result.publish_id).await;
        if let Some(current) = self
            .store
            .get_publish_status(&result.app_id, &result.publish_id)
            .await?
        {
            let mut status = current.clone();
            status.counters.dead_lettered = status.counters.dead_lettered.saturating_add(1);
            status.error_reason = Some("retryable result missing retry context".to_owned());
            let next = status.counters.resolve_lifecycle_state(current.state);
            status.state = next;
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

fn result_failure_class(result: &DeliveryResult) -> ProviderFailureClass {
    result
        .error
        .as_ref()
        .map(ProviderError::resolved_failure_class)
        .unwrap_or(ProviderFailureClass::Unknown)
}

fn is_device_terminal_failure(result: &DeliveryResult) -> bool {
    result_failure_class(result).is_device_terminal()
}

fn is_device_transient_failure(result: &DeliveryResult) -> bool {
    result_failure_class(result).is_device_transient()
}

fn invalidation_guard_threshold_crossed(status: &PublishStatus) -> bool {
    let planned = status.counters.planned;
    if planned == 0 || status.counters.failed < INVALIDATION_GUARD_MIN_SAMPLES {
        return false;
    }
    let threshold = planned
        .saturating_mul(INVALIDATION_GUARD_RATIO_PERCENT)
        .saturating_add(99)
        / 100;
    let threshold = threshold.max(INVALIDATION_GUARD_MIN_SAMPLES);
    let previous_failed = status.counters.failed.saturating_sub(1);
    status.counters.failed >= threshold && previous_failed < threshold
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
        failure_class = result_failure_class(result).label(),
        device_terminal = is_device_terminal_failure(result),
        "push provider feedback processed"
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sonic_rs::json;

    use crate::domain::{
        DeliveryFeedback, DeliveryJob, DeliveryOutcome, DeliveryResult, DeviceDetails,
        DevicePushDetails, FormFactor, Platform, ProviderError, ProviderFailureClass,
        PublishCounters, PublishLifecycleState, PublishStatus, PushPayload, PushProviderKind,
        PushRecipient, SecretString, generate_device_identity_token, hash_device_identity_token,
    };
    use crate::memory::MemoryPushStore;
    use crate::metrics::PushMetrics;
    use crate::pipeline::{MemoryPushQueue, PushQueue, PushQueuePayload, PushQueueStage};
    use crate::retry::RetryPolicy;
    use crate::storage::{PushDeviceStore, PushPublishStatusStore};

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
    async fn redelivered_feedback_message_does_not_double_count_status() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store.put_publish_status(status()).await.unwrap();
        let processor = PushFeedbackProcessor::new(store.clone(), queue.clone());
        let feedback = DeliveryFeedback::from_result(DeliveryResult {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            provider: PushProviderKind::Fcm,
            batch_id: "batch-1".to_owned(),
            device_id: Some("device-1".to_owned()),
            outcome: DeliveryOutcome::Accepted,
            provider_message_id: Some("provider-1".to_owned()),
            error: None,
            attempt: 1,
        });

        for key in ["result-before-ack", "result-redelivered"] {
            queue
                .produce(
                    PushQueueStage::DeliveryResults,
                    key.to_owned(),
                    PushQueuePayload::DeliveryFeedback(Box::new(feedback.clone())),
                )
                .await
                .unwrap();
        }

        assert_eq!(processor.run_once("feedback").await.unwrap(), 2);
        let status = store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.counters.dispatched, 1);
        assert_eq!(status.counters.succeeded, 1);
        assert_eq!(status.state, PublishLifecycleState::Succeeded);
    }

    #[tokio::test]
    async fn feedback_can_refine_a_terminal_summary_without_reactivating_it() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        let mut terminal = status_with_planned("publish-1", 2);
        terminal.state = PublishLifecycleState::Succeeded;
        terminal.counters.dispatched = 1;
        terminal.counters.succeeded = 1;
        store.put_publish_status(terminal).await.unwrap();
        let processor = PushFeedbackProcessor::new(store.clone(), queue);

        let observed = processor
            .update_publish_status(&rejected_result(
                "publish-1",
                "device-2",
                "rejected",
                ProviderFailureClass::CallerPayload,
            ))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(observed.state, PublishLifecycleState::PartiallySucceeded);
        assert_eq!(observed.counters.failed, 1);
        let persisted = store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted, observed);
    }

    #[tokio::test]
    async fn concurrent_feedback_updates_do_not_lose_publish_counters() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store
            .put_publish_status(status_with_planned("publish-1", 2))
            .await
            .unwrap();
        let processor = PushFeedbackProcessor::new(store.clone(), queue);
        let first = rejected_result(
            "publish-1",
            "device-1",
            "rejected-1",
            ProviderFailureClass::CallerPayload,
        );
        let second = rejected_result(
            "publish-1",
            "device-2",
            "rejected-2",
            ProviderFailureClass::CallerPayload,
        );

        let (first_result, second_result) = tokio::join!(
            processor.update_publish_status(&first),
            processor.update_publish_status(&second)
        );
        first_result.unwrap();
        second_result.unwrap();

        let status = store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.counters.dispatched, 2);
        assert_eq!(status.counters.failed, 2);
        assert_eq!(status.state, PublishLifecycleState::Failed);
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

    #[tokio::test]
    async fn feedback_provider_transient_does_not_increment_device_failure_count() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store.put_publish_status(status()).await.unwrap();
        store.upsert_device(device("device-1")).await.unwrap();
        let metrics = PushMetrics::default();
        let processor = PushFeedbackProcessor::new(store.clone(), queue)
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

        let device = store
            .get_device("app-1", "device-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(device.push.state, DevicePushState::Active);
        assert_eq!(device.push.failure_count, 0);
        assert_eq!(metrics.get("sockudo_push_provider_failures_total"), 1);
    }

    #[tokio::test]
    async fn feedback_device_transient_increments_only_that_device_failure_count() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store.put_publish_status(status()).await.unwrap();
        store.upsert_device(device("device-1")).await.unwrap();
        store.upsert_device(device("device-2")).await.unwrap();
        let processor = PushFeedbackProcessor::new(store.clone(), queue);

        processor
            .apply_result(rejected_result(
                "publish-1",
                "device-1",
                "device_transient",
                ProviderFailureClass::DeviceTransient,
            ))
            .await
            .unwrap();

        let device_1 = store
            .get_device("app-1", "device-1")
            .await
            .unwrap()
            .unwrap();
        let device_2 = store
            .get_device("app-1", "device-2")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(device_1.push.state, DevicePushState::Failing);
        assert_eq!(device_1.push.failure_count, 1);
        assert_eq!(device_2.push.state, DevicePushState::Active);
        assert_eq!(device_2.push.failure_count, 0);
    }

    #[tokio::test]
    async fn feedback_device_terminal_deletes_only_target_and_is_idempotent() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store.put_publish_status(status()).await.unwrap();
        store.upsert_device(device("device-1")).await.unwrap();
        store.upsert_device(device("device-2")).await.unwrap();
        let metrics = PushMetrics::default();
        let processor =
            PushFeedbackProcessor::new(store.clone(), queue).with_metrics(metrics.clone());
        let result = rejected_result(
            "publish-1",
            "device-1",
            "invalid_token",
            ProviderFailureClass::DeviceTerminal,
        );

        processor.apply_result(result.clone()).await.unwrap();
        processor.apply_result(result).await.unwrap();

        assert!(
            store
                .get_device("app-1", "device-1")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_device("app-1", "device-2")
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(metrics.get("sockudo_push_token_invalidations_total"), 1);
        assert_eq!(metrics.get("sockudo_push_duplicate_suppressed_total"), 1);
    }

    #[tokio::test]
    async fn feedback_mass_provider_auth_outage_preserves_device_registry() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store
            .put_publish_status(status_with_planned("publish-auth", 3))
            .await
            .unwrap();
        for device_id in ["device-1", "device-2", "device-3"] {
            store.upsert_device(device(device_id)).await.unwrap();
        }
        let metrics = PushMetrics::default();
        let processor =
            PushFeedbackProcessor::new(store.clone(), queue).with_metrics(metrics.clone());

        for device_id in ["device-1", "device-2", "device-3"] {
            processor
                .apply_result(rejected_result(
                    "publish-auth",
                    device_id,
                    "auth_failure",
                    ProviderFailureClass::CredentialAuth,
                ))
                .await
                .unwrap();
        }

        for device_id in ["device-1", "device-2", "device-3"] {
            let device = store.get_device("app-1", device_id).await.unwrap().unwrap();
            assert_eq!(device.push.state, DevicePushState::Active);
            assert_eq!(device.push.failure_count, 0);
        }
        let status = store
            .get_publish_status("app-1", "publish-auth")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.counters.failed, 3);
        assert_eq!(status.state, PublishLifecycleState::Failed);
        assert_eq!(metrics.get("sockudo_push_provider_failures_total"), 3);
        assert_eq!(metrics.get("sockudo_push_token_invalidations_total"), 0);
    }

    fn status() -> PublishStatus {
        status_with_planned("publish-1", 1)
    }

    fn status_with_planned(publish_id: &str, planned: u64) -> PublishStatus {
        PublishStatus {
            app_id: "app-1".to_owned(),
            publish_id: publish_id.to_owned(),
            state: PublishLifecycleState::Dispatching,
            counters: PublishCounters {
                planned,
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

    fn rejected_result(
        publish_id: &str,
        device_id: &str,
        class: &str,
        failure_class: ProviderFailureClass,
    ) -> DeliveryResult {
        DeliveryResult {
            app_id: "app-1".to_owned(),
            publish_id: publish_id.to_owned(),
            provider: PushProviderKind::Fcm,
            batch_id: format!("batch-{device_id}"),
            device_id: Some(device_id.to_owned()),
            outcome: DeliveryOutcome::Rejected,
            provider_message_id: None,
            error: Some(ProviderError {
                class: class.to_owned(),
                failure_class,
                reason: Some(class.to_owned()),
                retry_after_ms: None,
            }),
            attempt: 1,
        }
    }

    fn device(device_id: &str) -> DeviceDetails {
        let identity_token = generate_device_identity_token();
        DeviceDetails {
            app_id: "app-1".to_owned(),
            id: device_id.to_owned(),
            client_id: None,
            form_factor: FormFactor::Phone,
            platform: Platform::Android,
            metadata: json!({}),
            device_secret: hash_device_identity_token(&identity_token),
            timezone: "UTC".to_owned(),
            locale: "en-US".to_owned(),
            last_active_at_ms: 1,
            push: DevicePushDetails {
                recipient: PushRecipient::Fcm {
                    registration_token: SecretString::new(format!("token-{device_id}")).unwrap(),
                },
                state: DevicePushState::Active,
                failure_count: 0,
                error_reason: None,
            },
            push_rate_policy: None,
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
                    failure_class: ProviderFailureClass::ProviderTransient,
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
                rendered_payload: None,
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
