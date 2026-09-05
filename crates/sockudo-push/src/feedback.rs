use crate::domain::DevicePushState;
use futures_util::StreamExt;
use std::collections::BTreeSet;

use crate::domain::{
    DeadLetter, DeliveryEvent, DeliveryFeedback, DeliveryOutcome, DeliveryResult, ProviderError,
    ProviderFailureClass, PublishLifecycleState, PublishStatus,
};
use crate::meta::{PushMetaEvent, emit_push_meta_event};
use crate::metrics::{PushMetrics, provider_label};
use crate::pipeline::{PushPipelineResult, PushQueuePayload, PushQueueStage, QueueMessage, now_ms};
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
    effect_slots: std::sync::Arc<tokio::sync::Semaphore>,
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
            effect_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(16)),
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
        let mut groups = std::collections::BTreeMap::<(String, String), Vec<QueueMessage>>::new();
        for message in messages {
            let result = match &message.payload {
                PushQueuePayload::DeliveryResult(result) => result.as_ref(),
                PushQueuePayload::DeliveryFeedback(feedback) => &feedback.result,
                _ => {
                    self.queue
                        .dead_letter(message.ack, "unexpected payload for feedback".into())
                        .await?;
                    continue;
                }
            };
            groups
                .entry((result.app_id.clone(), result.publish_id.clone()))
                .or_default()
                .push(message);
        }
        let outcomes =
            futures_util::stream::iter(groups.into_values().map(|messages| async move {
                let mut acknowledgements = Vec::with_capacity(messages.len());
                let mut feedback = Vec::with_capacity(messages.len());
                for message in messages {
                    acknowledgements.push(message.ack);
                    feedback.push(match message.payload {
                        PushQueuePayload::DeliveryResult(result) => {
                            DeliveryFeedback::from_result(*result)
                        }
                        PushQueuePayload::DeliveryFeedback(feedback) => *feedback,
                        _ => unreachable!("validated feedback group"),
                    });
                }
                match self.apply_feedback_batch(feedback).await {
                    Ok(()) => {
                        let count = acknowledgements.len();
                        for ack in acknowledgements {
                            self.queue.ack(ack).await?;
                        }
                        Ok(count)
                    }
                    Err(error) => {
                        warn_feedback_retry(&error);
                        // A storage/queue outage must not turn a partially committed
                        // accepted batch into a terminal dead letter.
                        for ack in acknowledgements {
                            self.queue
                                .nack(ack, Some(now_ms().saturating_add(250)))
                                .await?;
                        }
                        Ok(0)
                    }
                }
            }))
            .buffer_unordered(16)
            .collect::<Vec<PushPipelineResult<usize>>>()
            .await;
        outcomes
            .into_iter()
            .try_fold(0, |total, outcome| outcome.map(|count| total + count))
    }

    pub async fn apply_result(&self, result: DeliveryResult) -> PushPipelineResult<()> {
        self.apply_feedback(DeliveryFeedback::from_result(result))
            .await
    }

    pub async fn apply_feedback(&self, feedback: DeliveryFeedback) -> PushPipelineResult<()> {
        self.apply_feedback_batch(vec![feedback]).await
    }

    async fn apply_feedback_batch(
        &self,
        feedback: Vec<DeliveryFeedback>,
    ) -> PushPipelineResult<()> {
        let Some(first) = feedback.first() else {
            return Ok(());
        };
        let app_id = first.result.app_id.clone();
        let publish_id = first.result.publish_id.clone();
        if feedback.len() > 64
            || feedback
                .iter()
                .any(|f| f.result.app_id != app_id || f.result.publish_id != publish_id)
        {
            return Err(crate::pipeline::PushPipelineError::InvalidPayload(
                "feedback batch must contain at most 64 results for one publish".into(),
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        let feedback = feedback.into_iter().filter(|f| {
            if seen.insert(feedback_receipt_id(f)) {
                true
            } else {
                self.metrics.duplicate_suppressed();
                false
            }
        });
        let results =
            futures_util::stream::iter(feedback.map(|feedback| self.prepare_feedback(feedback)))
                .buffer_unordered(16)
                .collect::<Vec<_>>()
                .await;
        let mut prepared = Vec::new();
        let mut first_error = None;
        for result in results {
            match result {
                Ok(Some(feedback)) => prepared.push(feedback),
                Ok(None) => {}
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(error) => warn_feedback_retry(&error),
            }
        }
        self.commit_feedback_status(&app_id, &publish_id, &prepared, &BTreeSet::new())
            .await?;
        let completed_ids: BTreeSet<String> = prepared.iter().map(|item| item.id.clone()).collect();
        let completions = futures_util::stream::iter(
            prepared
                .into_iter()
                .map(|prepared| self.complete_feedback(prepared)),
        )
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;
        for completed in completions {
            completed?;
        }
        // The canonical status receipt is removed only after its durable complete
        // outcome marker. Replays can therefore never apply a counter twice. The
        // markers this batch just wrote are known complete, so the release pass
        // does not read them back.
        self.commit_feedback_status(&app_id, &publish_id, &[], &completed_ids)
            .await?;
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    async fn complete_feedback(&self, prepared: PreparedFeedback) -> PushPipelineResult<()> {
        let result = &prepared.feedback.result;
        self.store
            .put_idempotency_record_if_absent(IdempotencyRecord {
                app_id: result.app_id.clone(),
                key: format!("delivery-result:{}", prepared.id),
                publish_id: result.publish_id.clone(),
                expires_at_ms: prepared
                    .receipt
                    .occurred_at_ms
                    .saturating_add(Self::FEEDBACK_IDEMPOTENCY_TTL_MS),
            })
            .await?;
        if let Some(device_id) = result.device_id.as_deref() {
            self.store
                .complete_device_feedback_receipt(&result.app_id, device_id, &prepared.id)
                .await?;
        }
        emit_feedback_meta_event(result);
        Ok(())
    }

    async fn prepare_feedback(
        &self,
        feedback: DeliveryFeedback,
    ) -> PushPipelineResult<Option<PreparedFeedback>> {
        let _permit = self.effect_slots.acquire().await.map_err(|_| {
            crate::pipeline::PushPipelineError::Backpressure("feedback admission closed".into())
        })?;
        let result = &feedback.result;
        let id = feedback_receipt_id(&feedback);
        let event_id = result_event_id(&feedback);
        // Honor old complete markers during rolling upgrades. New pending markers
        // are intentionally a different namespace and never suppress work. Both
        // reads are independent, so they run concurrently.
        let (current_marker, legacy_marker) = futures_util::future::try_join(
            self.store
                .get_idempotency_record(&result.app_id, &format!("delivery-result:{id}")),
            self.store
                .get_idempotency_record(&result.app_id, &format!("delivery-result:{event_id}")),
        )
        .await?;
        let complete = current_marker.is_some() || legacy_marker.is_some();
        if complete {
            if let Some(device_id) = result.device_id.as_deref() {
                self.store
                    .complete_device_feedback_receipt(&result.app_id, device_id, &id)
                    .await?;
            }
            self.metrics.duplicate_suppressed();
            return Ok(None);
        }
        let pending_key = format!("delivery-pending:{id}");
        let received_at_ms = now_ms();
        let inserted = self
            .store
            .put_idempotency_record_if_absent(IdempotencyRecord {
                app_id: result.app_id.clone(),
                key: pending_key.clone(),
                publish_id: result.publish_id.clone(),
                expires_at_ms: received_at_ms.saturating_add(Self::FEEDBACK_IDEMPOTENCY_TTL_MS),
            })
            .await?;
        let occurred_at_ms = if inserted {
            // A successful conditional insertion proves the canonical timestamp;
            // only a replay needs to read the earlier owner's persisted value.
            received_at_ms
        } else {
            self.store
                .get_idempotency_record(&result.app_id, &pending_key)
                .await?
                .ok_or_else(|| {
                    crate::storage::PushStorageError::Backend(
                        "feedback pending record disappeared".into(),
                    )
                })?
                .expires_at_ms
                .saturating_sub(Self::FEEDBACK_IDEMPOTENCY_TTL_MS)
        };
        let retry_at_ms = self
            .retry_policy
            .schedule_retry(&feedback, occurred_at_ms)
            .map(|entry| entry.next_attempt_at_ms);
        self.store
            .append_delivery_event(DeliveryEvent {
                app_id: result.app_id.clone(),
                publish_id: result.publish_id.clone(),
                event_id,
                occurred_at_ms,
                result: result.clone(),
            })
            .await?;
        self.update_device_state(result, &id, occurred_at_ms)
            .await?;
        self.handle_retry_or_dlq(&feedback, occurred_at_ms).await?;
        Ok(Some(PreparedFeedback {
            feedback,
            id,
            receipt: crate::storage::FeedbackReceipt {
                occurred_at_ms,
                retry_at_ms,
                status_applied: true,
            },
        }))
    }

    async fn update_device_state(
        &self,
        result: &DeliveryResult,
        receipt_id: &str,
        occurred_at_ms: u64,
    ) -> PushPipelineResult<()> {
        use crate::storage::{DeviceFeedbackEffect, DeviceFeedbackRequest};
        let Some(device_id) = result.device_id.as_deref() else {
            return Ok(());
        };
        let effect = match result.outcome {
            DeliveryOutcome::Accepted => DeviceFeedbackEffect::Success,
            DeliveryOutcome::Rejected if is_device_terminal_failure(result) => {
                DeviceFeedbackEffect::Delete
            }
            DeliveryOutcome::Rejected | DeliveryOutcome::Retryable
                if is_device_transient_failure(result) =>
            {
                DeviceFeedbackEffect::Failure {
                    threshold: self.failure_threshold,
                    reason: result
                        .error
                        .as_ref()
                        .and_then(|error| error.reason.clone())
                        .unwrap_or_else(|| "provider delivery failed".into()),
                }
            }
            _ => return Ok(()),
        };
        let applied = self
            .store
            .apply_device_feedback_once(DeviceFeedbackRequest {
                app_id: result.app_id.clone(),
                device_id: device_id.into(),
                publish_id: result.publish_id.clone(),
                receipt_id: receipt_id.into(),
                occurred_at_ms,
                expires_at_ms: occurred_at_ms.saturating_add(Self::FEEDBACK_IDEMPOTENCY_TTL_MS),
                effect,
            })
            .await?;
        if !applied.applied {
            return Ok(());
        }
        if let (Some(previous), Some(next)) = (applied.previous, applied.next) {
            if previous != next {
                self.metrics
                    .device_state_transition(&result.app_id, previous, next);
                emit_push_meta_event(PushMetaEvent::device_state_changed(
                    &result.app_id,
                    &result.publish_id,
                    previous,
                    next,
                ));
            }
        } else if applied.previous.is_some() {
            self.metrics
                .token_invalidated(result.provider, &result.app_id);
            emit_push_meta_event(PushMetaEvent::token_invalidated(
                &result.app_id,
                &result.publish_id,
                result.provider,
            ));
        }
        Ok(())
    }

    /// Apply prepared feedback to the publish status and release completed
    /// receipts in one compare-and-swap. `known_completed` lists receipt IDs whose
    /// durable complete markers this worker wrote itself; they are released
    /// without reading the marker back.
    async fn commit_feedback_status(
        &self,
        app_id: &str,
        publish_id: &str,
        prepared: &[PreparedFeedback],
        known_completed: &BTreeSet<String>,
    ) -> PushPipelineResult<()> {
        for attempt in 0..8 {
            let Some(expected) = self
                .store
                .get_versioned_publish_status(app_id, publish_id)
                .await?
            else {
                return Ok(());
            };
            let mut pending = expected.pending_feedback.clone();
            let mut status = expected.status.clone();
            let ids = expected
                .pending_feedback
                .keys()
                .cloned()
                .chain(prepared.iter().map(|item| item.id.clone()))
                .filter(|id| !known_completed.contains(id))
                .collect::<std::collections::BTreeSet<_>>();
            let completed = futures_util::stream::iter(ids.into_iter().map(|id| async move {
                let done = self
                    .store
                    .get_idempotency_record(app_id, &format!("delivery-result:{id}"))
                    .await?
                    .is_some();
                Ok::<_, crate::storage::PushStorageError>((id, done))
            }))
            .buffer_unordered(16)
            .collect::<Vec<_>>()
            .await;
            let mut completed_ids = std::collections::BTreeSet::new();
            for id in known_completed {
                if pending.remove(id).is_some() || prepared.iter().any(|item| &item.id == id) {
                    completed_ids.insert(id.clone());
                }
            }
            for result in completed {
                let (id, done) = result?;
                if done {
                    pending.remove(&id);
                    completed_ids.insert(id);
                }
            }
            let mut newly_applied = Vec::new();
            for item in prepared {
                // Read completion AFTER the status snapshot. If a concurrent
                // worker completes/removes this receipt, our CAS must conflict.
                if completed_ids.contains(&item.id) || pending.contains_key(&item.id) {
                    continue;
                }
                if pending.len() >= crate::storage::MAX_PENDING_FEEDBACK {
                    return Err(crate::pipeline::PushPipelineError::Backpressure(
                        "publish feedback receipt capacity reached".into(),
                    ));
                }
                apply_feedback_status_delta(
                    &mut status,
                    &item.feedback.result,
                    item.receipt.retry_at_ms,
                );
                pending.insert(item.id.clone(), item.receipt.clone());
                newly_applied.push(item);
            }
            if pending == expected.pending_feedback && status == expected.status {
                return Ok(());
            }
            match self
                .store
                .compare_and_swap_feedback_status(&expected, status.clone(), pending)
                .await?
            {
                crate::storage::PublishStatusCasOutcome::Updated { .. } => {
                    let mut projected = expected.status.clone();
                    for item in newly_applied {
                        apply_feedback_status_delta(
                            &mut projected,
                            &item.feedback.result,
                            item.receipt.retry_at_ms,
                        );
                        let result = &item.feedback.result;
                        self.metrics
                            .delivery_status(app_id, outcome_label(result.outcome));
                        if !matches!(result.outcome, DeliveryOutcome::Accepted) {
                            let failure = result_failure_class(result);
                            self.metrics.provider_failure_class(
                                result.provider,
                                app_id,
                                failure.label(),
                            );
                            emit_push_meta_event(PushMetaEvent::provider_rejected(
                                app_id,
                                publish_id,
                                result.provider,
                                result.outcome,
                                result.error.as_ref().map(|error| error.class.as_str()),
                                Some(failure),
                            ));
                        }
                        if is_device_terminal_failure(result) {
                            self.emit_invalidation_guard_if_needed(result, &projected);
                        }
                    }
                    if status.state != expected.status.state
                        && matches!(
                            status.state,
                            PublishLifecycleState::Succeeded
                                | PublishLifecycleState::PartiallySucceeded
                                | PublishLifecycleState::Failed
                                | PublishLifecycleState::Expired
                                | PublishLifecycleState::DeadLettered
                        )
                    {
                        emit_push_meta_event(PushMetaEvent::completed(
                            app_id,
                            publish_id,
                            lifecycle_label(status.state),
                        ));
                    }
                    return Ok(());
                }
                crate::storage::PublishStatusCasOutcome::Missing => return Ok(()),
                crate::storage::PublishStatusCasOutcome::Conflict => {
                    self.metrics.publish_status_cas_conflict("feedback");
                    tokio::time::sleep(std::time::Duration::from_millis(1 << attempt.min(5))).await;
                }
                outcome => {
                    return Err(
                        crate::pipeline::PushPipelineError::UnexpectedPublishStatusCasOutcome {
                            operation: "feedback batch",
                            outcome,
                        },
                    );
                }
            }
        }
        Err(
            crate::pipeline::PushPipelineError::PublishStatusCasExhausted {
                component: "feedback",
                app_id: app_id.into(),
                publish_id: publish_id.into(),
            },
        )
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

    async fn handle_retry_or_dlq(
        &self,
        feedback: &DeliveryFeedback,
        occurred_at_ms: u64,
    ) -> PushPipelineResult<()> {
        let result = &feedback.result;
        if matches!(result.outcome, DeliveryOutcome::Retryable) {
            let Some(entry) = self.retry_policy.schedule_retry(feedback, occurred_at_ms) else {
                let dead_letter = DeadLetter {
                    app_id: result.app_id.clone(),
                    publish_id: result.publish_id.clone(),
                    stage: "delivery_result".to_owned(),
                    key: result.batch_id.clone(),
                    reason: "retryable result missing retry context".to_owned(),
                    occurred_at_ms,
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
                occurred_at_ms,
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
}

fn warn_feedback_retry(error: &crate::pipeline::PushPipelineError) {
    // Backend error displays can contain stored JSON fragments or queue payloads.
    // Keep the operational source without copying content-bearing diagnostics.
    let failure_source = match error {
        crate::pipeline::PushPipelineError::Storage(_) => "storage",
        crate::pipeline::PushPipelineError::Queue(_) => "queue",
        crate::pipeline::PushPipelineError::Domain(_) => "validation",
        crate::pipeline::PushPipelineError::Backpressure(_) => "capacity",
        _ => "feedback-state",
    };
    tracing::warn!(failure_source, "push feedback work will retry");
}

struct PreparedFeedback {
    feedback: DeliveryFeedback,
    id: String,
    receipt: crate::storage::FeedbackReceipt,
}

fn feedback_receipt_id(feedback: &DeliveryFeedback) -> String {
    let digest = aws_lc_rs::digest::digest(
        &aws_lc_rs::digest::SHA256,
        result_event_id(feedback).as_bytes(),
    );
    hex::encode(digest.as_ref())
}

fn apply_feedback_status_delta(
    status: &mut PublishStatus,
    result: &DeliveryResult,
    retry_at_ms: Option<u64>,
) {
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
        DeliveryOutcome::Retryable if retry_at_ms.is_some() => {
            status.counters.retry_scheduled = status.counters.retry_scheduled.saturating_add(1);
        }
        DeliveryOutcome::Retryable => {
            status.counters.dead_lettered = status.counters.dead_lettered.saturating_add(1);
            status.error_reason = Some("retryable result missing retry context".into());
        }
        DeliveryOutcome::Cancelled => {}
    }
    if let Some(next) =
        retry_at_ms.or_else(|| result.error.as_ref().and_then(|error| error.retry_after_ms))
    {
        status.retry_after_ms = Some(next);
    }
    status.state = status.counters.resolve_lifecycle_state(status.state);
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
    use crate::domain::DevicePushState;
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

    #[test]
    fn feedback_worker_future_can_run_on_supervised_multithreaded_executor() {
        fn assert_send<T: Send>(_: T) {}
        let processor = PushFeedbackProcessor::new(
            Arc::new(MemoryPushStore::new()),
            Arc::new(MemoryPushQueue::new()),
        );
        assert_send(async move { processor.run_once("supervised-feedback").await });
    }

    #[tokio::test]
    async fn feedback_replays_every_committed_boundary_without_counter_or_device_drift() {
        use crate::storage::{PushDeliveryEventStore, PushIdempotencyStore};
        for boundary in 0..4 {
            let store = Arc::new(MemoryPushStore::new());
            let queue = Arc::new(MemoryPushQueue::new());
            store
                .put_publish_status(status_with_planned("publish-1", 1))
                .await
                .unwrap();
            store.upsert_device(device("device-1")).await.unwrap();
            let feedback = DeliveryFeedback::from_result(rejected_result(
                "publish-1",
                "device-1",
                "transient",
                ProviderFailureClass::DeviceTransient,
            ));
            let id = feedback_receipt_id(&feedback);
            let processor = PushFeedbackProcessor::new(store.clone(), queue.clone());
            let pending_time = now_ms();
            store
                .put_idempotency_record_if_absent(IdempotencyRecord {
                    app_id: "app-1".into(),
                    key: format!("delivery-pending:{id}"),
                    publish_id: "publish-1".into(),
                    expires_at_ms: pending_time
                        + PushFeedbackProcessor::FEEDBACK_IDEMPOTENCY_TTL_MS,
                })
                .await
                .unwrap();
            if boundary >= 1 {
                let prepared = processor
                    .prepare_feedback(feedback.clone())
                    .await
                    .unwrap()
                    .unwrap();
                if boundary >= 2 {
                    processor
                        .commit_feedback_status(
                            "app-1",
                            "publish-1",
                            std::slice::from_ref(&prepared),
                            &BTreeSet::new(),
                        )
                        .await
                        .unwrap();
                }
                if boundary >= 3 {
                    store
                        .put_idempotency_record_if_absent(IdempotencyRecord {
                            app_id: "app-1".into(),
                            key: format!("delivery-result:{id}"),
                            publish_id: "publish-1".into(),
                            expires_at_ms: pending_time
                                + PushFeedbackProcessor::FEEDBACK_IDEMPOTENCY_TTL_MS,
                        })
                        .await
                        .unwrap();
                }
            }
            drop(processor);
            let restarted = PushFeedbackProcessor::new(store.clone(), queue.clone());
            restarted.apply_feedback(feedback.clone()).await.unwrap();
            restarted.apply_feedback(feedback).await.unwrap();
            let final_status = store
                .get_versioned_publish_status("app-1", "publish-1")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                final_status.status.counters.failed, 1,
                "boundary {boundary}"
            );
            assert_eq!(final_status.status.counters.dispatched, 1);
            assert!(final_status.pending_feedback.is_empty());
            assert_eq!(
                store
                    .get_device("app-1", "device-1")
                    .await
                    .unwrap()
                    .unwrap()
                    .push
                    .failure_count,
                1
            );
            assert_eq!(
                store
                    .list_delivery_events("app-1", "publish-1", 10, None)
                    .await
                    .unwrap()
                    .items
                    .len(),
                1
            );
            assert_eq!(
                queue
                    .lag(PushQueueStage::DeadLetters)
                    .await
                    .unwrap()
                    .ready_depth,
                1
            );
        }
    }

    #[tokio::test]
    async fn feedback_batch_applies_sixty_four_outcomes_with_two_status_writes() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        store
            .put_publish_status(status_with_planned("publish-1", 64))
            .await
            .unwrap();
        for number in 0..64 {
            let result = rejected_result(
                "publish-1",
                &format!("device-{number}"),
                "bad-payload",
                ProviderFailureClass::CallerPayload,
            );
            queue
                .produce(
                    PushQueueStage::DeliveryResults,
                    format!("result-{number}"),
                    PushQueuePayload::DeliveryResult(Box::new(result)),
                )
                .await
                .unwrap();
        }
        let processor = PushFeedbackProcessor::new(store.clone(), queue.clone());
        assert_eq!(processor.run_once("feedback").await.unwrap(), 64);
        let status = store
            .get_versioned_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.status.counters.failed, 64);
        assert_eq!(status.status.counters.dispatched, 64);
        assert_eq!(status.revision, 3);
        assert!(status.pending_feedback.is_empty());
        assert_eq!(
            queue
                .lag(PushQueueStage::DeliveryResults)
                .await
                .unwrap()
                .inflight_depth,
            0
        );
    }

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

        processor
            .apply_result(rejected_result(
                "publish-1",
                "device-2",
                "rejected",
                ProviderFailureClass::CallerPayload,
            ))
            .await
            .unwrap();
        let observed = store
            .get_publish_status("app-1", "publish-1")
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
            processor.apply_result(first),
            processor.apply_result(second)
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

    pub(super) fn status_with_planned(publish_id: &str, planned: u64) -> PublishStatus {
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

    pub(super) fn rejected_result(
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

    pub(super) fn device(device_id: &str) -> DeviceDetails {
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

#[cfg(all(
    test,
    any(
        feature = "postgres",
        feature = "mysql",
        feature = "dynamodb",
        feature = "scylladb",
        feature = "surrealdb"
    )
))]
pub(crate) mod live_tests;

#[cfg(test)]
mod fault_tests;
