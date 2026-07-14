use crate::domain::{
    DEFAULT_PUSH_RETRY_INITIAL_BACKOFF_MS, DEFAULT_PUSH_RETRY_JITTER_RATIO_PERCENT,
    DEFAULT_PUSH_RETRY_MAX_AGE_MS, DEFAULT_PUSH_RETRY_MAX_ATTEMPTS,
    DEFAULT_PUSH_RETRY_MAX_BACKOFF_MS, DeliveryBatch, DeliveryFeedback, DeliveryJob,
    DeliveryOutcome, ProviderError, PublishLifecycleState, PushProviderKind, RetryScheduleEntry,
    provider_key, stable_hash,
};
use crate::meta::{PushMetaEvent, emit_push_meta_event};
use crate::metrics::PushMetrics;
use crate::pipeline::{
    PushPipelineResult, PushQueuePayload, PushQueueStage, QueueMessage,
    guard_publish_status_transition, mutate_publish_status_with_cas, now_ms,
};
use crate::storage::{DynPushStore, IdempotencyRecord, SchedulerLock};

const MAX_RETRY_ATTEMPTS: u32 = 100;
const MAX_RETRY_BACKOFF_MS: u64 = 24 * 60 * 60 * 1000;
const MAX_RETRY_AGE_MS: u64 = 30 * 24 * 60 * 60 * 1000;
const RETRY_IDEMPOTENCY_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub max_attempts: u32,
    pub max_retry_age_ms: u64,
    pub jitter_ratio_percent: u8,
    pub respect_retry_after: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_backoff_ms: DEFAULT_PUSH_RETRY_INITIAL_BACKOFF_MS,
            max_backoff_ms: DEFAULT_PUSH_RETRY_MAX_BACKOFF_MS,
            max_attempts: DEFAULT_PUSH_RETRY_MAX_ATTEMPTS,
            max_retry_age_ms: DEFAULT_PUSH_RETRY_MAX_AGE_MS,
            jitter_ratio_percent: DEFAULT_PUSH_RETRY_JITTER_RATIO_PERCENT,
            respect_retry_after: true,
        }
    }
}

impl RetryPolicy {
    pub fn bounded(mut self) -> Self {
        self.max_attempts = self.max_attempts.clamp(1, MAX_RETRY_ATTEMPTS);
        self.initial_backoff_ms = self.initial_backoff_ms.clamp(1, MAX_RETRY_BACKOFF_MS);
        self.max_backoff_ms = self
            .max_backoff_ms
            .clamp(self.initial_backoff_ms, MAX_RETRY_BACKOFF_MS);
        self.max_retry_age_ms = self.max_retry_age_ms.clamp(1, MAX_RETRY_AGE_MS);
        self.jitter_ratio_percent = self.jitter_ratio_percent.min(100);
        self
    }

    pub fn schedule_retry(
        &self,
        feedback: &DeliveryFeedback,
        now_ms: u64,
    ) -> Option<RetryScheduleEntry> {
        if !matches!(feedback.result.outcome, DeliveryOutcome::Retryable) {
            return None;
        }
        let policy = self.clone().bounded();
        let mut job = feedback.retry_job.as_deref()?.clone();
        let next_attempt = feedback.result.attempt.saturating_add(1);
        let first_attempt_at_ms = feedback
            .first_attempt_at_ms
            .or(job.first_attempt_at_ms)
            .unwrap_or(now_ms);
        let retry_age_deadline = first_attempt_at_ms.saturating_add(policy.max_retry_age_ms);
        let delivery_deadline = job.expires_at_ms.unwrap_or(u64::MAX);
        let expires_at_ms = retry_age_deadline.min(delivery_deadline);
        let retry_idempotency_key =
            retry_idempotency_key(&feedback.result, &feedback.delivery_key, next_attempt);
        let next_attempt_at_ms = if next_attempt > policy.max_attempts {
            now_ms
        } else {
            policy.next_attempt_at_ms(
                feedback.result.attempt,
                feedback
                    .result
                    .error
                    .as_ref()
                    .and_then(|error| error.retry_after_ms),
                now_ms,
                &retry_idempotency_key,
            )
        }
        .min(expires_at_ms);

        job.attempt = next_attempt;
        job.first_attempt_at_ms = Some(first_attempt_at_ms);
        job.not_before_ms = Some(next_attempt_at_ms);

        Some(RetryScheduleEntry {
            app_id: feedback.result.app_id.clone(),
            publish_id: feedback.result.publish_id.clone(),
            provider: Some(feedback.result.provider),
            job: Some(Box::new(job)),
            attempt: next_attempt,
            first_attempt_at_ms,
            next_attempt_at_ms,
            max_attempts: policy.max_attempts,
            expires_at_ms,
            last_error: feedback.result.error.clone(),
            retry_idempotency_key: retry_idempotency_key.clone(),
            stage: "delivery".to_owned(),
            key: retry_idempotency_key,
            not_before_ms: Some(next_attempt_at_ms),
            payload: None,
        })
    }

    pub fn next_attempt_at_ms(
        &self,
        current_attempt: u32,
        provider_retry_after_ms: Option<u64>,
        now_ms: u64,
        jitter_seed: &str,
    ) -> u64 {
        let policy = self.clone().bounded();
        if policy.respect_retry_after
            && let Some(retry_after_ms) = provider_retry_after_ms
        {
            return retry_after_ms.max(now_ms);
        }

        let exponent = current_attempt.saturating_sub(1).min(31);
        let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
        let delay = policy
            .initial_backoff_ms
            .saturating_mul(multiplier)
            .min(policy.max_backoff_ms);
        let jittered =
            apply_deterministic_jitter(delay, policy.jitter_ratio_percent, jitter_seed.as_bytes())
                .min(policy.max_backoff_ms);
        now_ms.saturating_add(jittered)
    }
}

#[derive(Clone)]
pub struct PushRetryScheduler {
    store: DynPushStore,
    queue: crate::pipeline::DynPushQueue,
    owner_id: String,
    lock_ttl_ms: u64,
    max_messages_per_tick: usize,
    metrics: PushMetrics,
}

impl PushRetryScheduler {
    pub fn new(
        store: DynPushStore,
        queue: crate::pipeline::DynPushQueue,
        owner_id: impl Into<String>,
    ) -> Self {
        Self {
            store,
            queue,
            owner_id: owner_id.into(),
            lock_ttl_ms: 30_000,
            max_messages_per_tick: 64,
            metrics: PushMetrics::default(),
        }
    }

    pub fn with_metrics(mut self, metrics: PushMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_lock_ttl_ms(mut self, lock_ttl_ms: u64) -> Self {
        self.lock_ttl_ms = lock_ttl_ms.max(1);
        self
    }

    pub fn with_max_messages_per_tick(mut self, max_messages_per_tick: usize) -> Self {
        self.max_messages_per_tick = max_messages_per_tick.max(1);
        self
    }

    pub async fn run_once(&self, consumer_group: &str) -> PushPipelineResult<usize> {
        let messages = self
            .queue
            .consume(
                PushQueueStage::RetrySchedule,
                consumer_group,
                self.max_messages_per_tick,
                30_000,
            )
            .await?;
        let mut processed = 0;
        for message in messages {
            if self.handle_message(message).await? {
                processed += 1;
            }
        }
        Ok(processed)
    }

    async fn handle_message(&self, message: QueueMessage) -> PushPipelineResult<bool> {
        let PushQueuePayload::RetrySchedule(entry) = message.payload.clone() else {
            self.queue
                .dead_letter(
                    message.ack,
                    "unexpected payload for retry scheduler".to_owned(),
                )
                .await?;
            self.metrics
                .retry_malformed("unknown", "unexpected_payload");
            return Ok(false);
        };
        let mut entry = *entry;
        normalize_retry_entry(&mut entry);
        let now = now_ms();
        let Some(provider) = entry.provider else {
            return self
                .dead_letter_malformed(message, &entry, "missing retry provider")
                .await
                .map(|_| false);
        };
        let Some(job) = entry.job.as_deref().cloned() else {
            return self
                .dead_letter_malformed(message, &entry, "missing retry job")
                .await
                .map(|_| false);
        };
        if entry.retry_idempotency_key.trim().is_empty() {
            return self
                .dead_letter_malformed(message, &entry, "missing retry idempotency key")
                .await
                .map(|_| false);
        }
        if entry.next_attempt_at_ms > now {
            self.metrics.retry_deferred(
                provider_for_metrics(entry.provider),
                &entry.app_id,
                entry.next_attempt_at_ms.saturating_sub(now),
            );
            self.queue
                .nack(message.ack, Some(entry.next_attempt_at_ms))
                .await?;
            return Ok(false);
        }

        if self.retry_already_completed(&entry).await? {
            self.queue.ack(message.ack).await?;
            return Ok(false);
        }

        let lock_key = retry_lock_key(&entry.retry_idempotency_key);
        let lock_acquired = self
            .store
            .acquire_scheduler_lock(
                SchedulerLock {
                    app_id: entry.app_id.clone(),
                    publish_id: lock_key.clone(),
                    owner_id: self.owner_id.clone(),
                    expires_at_ms: now.saturating_add(self.lock_ttl_ms),
                },
                now,
            )
            .await?;
        if !lock_acquired {
            self.queue
                .nack(message.ack, Some(now.saturating_add(1_000)))
                .await?;
            return Ok(false);
        }

        let app_id = entry.app_id.clone();
        let result = self
            .handle_locked_retry(message, entry, provider, job, now)
            .await;
        let release = self
            .store
            .release_scheduler_lock(&app_id, &lock_key, &self.owner_id)
            .await;
        result?;
        release?;
        Ok(true)
    }

    async fn handle_locked_retry(
        &self,
        message: QueueMessage,
        entry: RetryScheduleEntry,
        provider: PushProviderKind,
        job: DeliveryJob,
        now: u64,
    ) -> PushPipelineResult<()> {
        if job
            .expires_at_ms
            .is_some_and(|expires_at_ms| expires_at_ms <= now)
        {
            self.complete_terminal_retry(
                message,
                &entry,
                RetryTerminalKind::Expired,
                "delivery expired before retry",
            )
            .await?;
            return Ok(());
        }
        if entry.attempt > entry.max_attempts || entry.max_attempts == 0 {
            self.complete_terminal_retry(
                message,
                &entry,
                RetryTerminalKind::DeadLettered,
                "retry attempts exhausted",
            )
            .await?;
            return Ok(());
        }
        if entry.expires_at_ms <= now {
            self.complete_terminal_retry(
                message,
                &entry,
                RetryTerminalKind::DeadLettered,
                "retry age expired",
            )
            .await?;
            return Ok(());
        }

        let mut retry_job = job;
        retry_job.app_id = entry.app_id.clone();
        retry_job.publish_id = entry.publish_id.clone();
        retry_job.provider = provider;
        retry_job.attempt = entry.attempt;
        retry_job.first_attempt_at_ms = Some(entry.first_attempt_at_ms);
        retry_job.not_before_ms = Some(entry.next_attempt_at_ms);
        retry_job.batch_id = format!("{}-retry-{}", retry_job.batch_id, entry.attempt);
        let batch = DeliveryBatch {
            app_id: entry.app_id.clone(),
            publish_id: entry.publish_id.clone(),
            provider,
            batch_id: retry_job.batch_id.clone(),
            jobs: vec![retry_job],
        };
        let status = self
            .store
            .get_publish_status(&entry.app_id, &entry.publish_id)
            .await?;
        if let Some(current) = status.as_ref() {
            // A terminal summary can be based on a mutable audience estimate. An explicitly
            // scheduled-but-unattempted retry is still real work; dispatch it without regressing
            // the terminal lifecycle state. Otherwise a terminal retry entry is stale.
            let scheduled_retry_is_pending = current.state.is_terminal()
                && current.counters.retry_attempted < current.counters.retry_scheduled;
            if !scheduled_retry_is_pending
                && !guard_publish_status_transition(
                    &self.metrics,
                    "retry",
                    current,
                    PublishLifecycleState::Dispatching,
                )
            {
                self.put_retry_idempotency(&entry).await?;
                self.queue.ack(message.ack).await?;
                return Ok(());
            }
        }
        self.queue
            .produce(
                PushQueueStage::DeliveryJobs(provider),
                batch.queue_key(),
                PushQueuePayload::DeliveryBatch(Box::new(batch)),
            )
            .await?;
        // The successor job is already visible. Record its deterministic retry key before status
        // accounting so a status-store failure cannot make source-message redelivery enqueue the
        // same provider attempt again.
        self.put_retry_idempotency(&entry).await?;
        mutate_publish_status_with_cas(
            self.store.as_ref(),
            &self.metrics,
            "retry",
            &entry.app_id,
            &entry.publish_id,
            |current| {
                let scheduled_retry_is_pending = current.state.is_terminal()
                    && current.counters.retry_attempted < current.counters.retry_scheduled;
                if current.state.is_terminal() && !scheduled_retry_is_pending {
                    return Ok(None);
                }
                if !current.state.is_terminal()
                    && !guard_publish_status_transition(
                        &self.metrics,
                        "retry",
                        current,
                        PublishLifecycleState::Dispatching,
                    )
                {
                    return Ok(None);
                }

                let mut status = current.clone();
                status.counters.retry_attempted = status.counters.retry_attempted.saturating_add(1);
                if !status.state.is_terminal() {
                    status.state = PublishLifecycleState::Dispatching;
                }
                status.retry_after_ms = None;
                Ok(Some(status))
            },
        )
        .await?;
        self.metrics.retry_attempted(provider, &entry.app_id);
        emit_push_meta_event(PushMetaEvent::scheduler_event(
            &entry.app_id,
            &entry.publish_id,
            "retry-dispatched",
        ));
        self.queue.ack(message.ack).await?;
        Ok(())
    }

    async fn dead_letter_malformed(
        &self,
        message: QueueMessage,
        entry: &RetryScheduleEntry,
        reason: &'static str,
    ) -> PushPipelineResult<()> {
        self.metrics
            .retry_malformed(provider_for_metrics(entry.provider), reason);
        self.mark_terminal_status(entry, RetryTerminalKind::DeadLettered, reason)
            .await?;
        self.queue
            .dead_letter(message.ack, reason.to_owned())
            .await?;
        emit_push_meta_event(PushMetaEvent::dead_letter(
            &entry.app_id,
            &entry.publish_id,
            "retry_schedule",
            reason,
        ));
        Ok(())
    }

    async fn complete_terminal_retry(
        &self,
        message: QueueMessage,
        entry: &RetryScheduleEntry,
        kind: RetryTerminalKind,
        reason: &'static str,
    ) -> PushPipelineResult<()> {
        self.mark_terminal_status(entry, kind, reason).await?;
        self.put_retry_idempotency(entry).await?;
        match kind {
            RetryTerminalKind::Expired => {
                self.metrics
                    .retry_expired(provider_for_metrics(entry.provider), &entry.app_id);
            }
            RetryTerminalKind::DeadLettered => {
                self.metrics
                    .retry_dead_lettered(provider_for_metrics(entry.provider), &entry.app_id);
            }
        }
        emit_push_meta_event(PushMetaEvent::dead_letter(
            &entry.app_id,
            &entry.publish_id,
            "retry_schedule",
            reason,
        ));
        self.queue
            .dead_letter(message.ack, reason.to_owned())
            .await?;
        Ok(())
    }

    async fn mark_terminal_status(
        &self,
        entry: &RetryScheduleEntry,
        kind: RetryTerminalKind,
        reason: &str,
    ) -> PushPipelineResult<()> {
        mutate_publish_status_with_cas(
            self.store.as_ref(),
            &self.metrics,
            "retry",
            &entry.app_id,
            &entry.publish_id,
            |current| {
                let mut status = current.clone();
                match kind {
                    RetryTerminalKind::Expired => {
                        status.counters.expired = status.counters.expired.saturating_add(1);
                    }
                    RetryTerminalKind::DeadLettered => {
                        status.counters.dead_lettered =
                            status.counters.dead_lettered.saturating_add(1);
                    }
                }
                status.error_reason = Some(redact_retry_reason(reason, entry.last_error.as_ref()));
                let next = status.counters.resolve_lifecycle_state(status.state);
                // Like feedback, a terminal retry outcome may refine a summary after a mutable
                // audience exceeded its acceptance-time estimate. It never moves the publish back
                // to an active state.
                status.state = next;
                Ok(Some(status))
            },
        )
        .await?;
        Ok(())
    }

    async fn retry_already_completed(
        &self,
        entry: &RetryScheduleEntry,
    ) -> PushPipelineResult<bool> {
        Ok(self
            .store
            .get_idempotency_record(&entry.app_id, &entry.retry_idempotency_key)
            .await?
            .is_some())
    }

    async fn put_retry_idempotency(&self, entry: &RetryScheduleEntry) -> PushPipelineResult<()> {
        self.store
            .put_idempotency_record_if_absent(IdempotencyRecord {
                app_id: entry.app_id.clone(),
                key: entry.retry_idempotency_key.clone(),
                publish_id: entry.publish_id.clone(),
                expires_at_ms: now_ms().saturating_add(RETRY_IDEMPOTENCY_TTL_MS),
            })
            .await?;
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum RetryTerminalKind {
    Expired,
    DeadLettered,
}

fn normalize_retry_entry(entry: &mut RetryScheduleEntry) {
    if entry.next_attempt_at_ms == 0 {
        entry.next_attempt_at_ms = entry.not_before_ms.unwrap_or_else(now_ms);
    }
    if entry.retry_idempotency_key.trim().is_empty() && !entry.key.trim().is_empty() {
        entry.retry_idempotency_key = entry.key.clone();
    }
    if entry.expires_at_ms == 0 {
        entry.expires_at_ms = entry
            .job
            .as_ref()
            .and_then(|job| job.expires_at_ms)
            .unwrap_or(u64::MAX);
    }
    if entry.first_attempt_at_ms == 0 {
        entry.first_attempt_at_ms = entry
            .job
            .as_ref()
            .and_then(|job| job.first_attempt_at_ms)
            .unwrap_or_else(now_ms);
    }
    if entry.max_attempts == 0 {
        entry.max_attempts = DEFAULT_PUSH_RETRY_MAX_ATTEMPTS;
    }
}

fn retry_idempotency_key(
    result: &crate::domain::DeliveryResult,
    delivery_key: &str,
    next_attempt: u32,
) -> String {
    format!(
        "retry:{}",
        stable_hash(
            format!(
                "{}:{}:{}:{}:{}:{}",
                result.app_id,
                result.publish_id,
                provider_key(result.provider),
                result.batch_id,
                delivery_key,
                next_attempt
            )
            .as_bytes()
        )
    )
}

fn retry_lock_key(retry_idempotency_key: &str) -> String {
    format!("retry-lock:{retry_idempotency_key}")
}

fn apply_deterministic_jitter(delay_ms: u64, ratio_percent: u8, seed: &[u8]) -> u64 {
    if delay_ms == 0 || ratio_percent == 0 {
        return delay_ms;
    }
    let spread = delay_ms.saturating_mul(u64::from(ratio_percent)) / 100;
    if spread == 0 {
        return delay_ms;
    }
    let hash = stable_hash(seed);
    let prefix = hash.get(..16).unwrap_or(&hash);
    let raw = u64::from_str_radix(prefix, 16).unwrap_or(0);
    let range = spread.saturating_mul(2).saturating_add(1);
    delay_ms.saturating_sub(spread).saturating_add(raw % range)
}

fn provider_for_metrics(provider: Option<PushProviderKind>) -> &'static str {
    provider.map(provider_key).unwrap_or("unknown")
}

fn redact_retry_reason(reason: &str, last_error: Option<&ProviderError>) -> String {
    last_error
        .map(|error| format!("{reason}: {}", error.class))
        .unwrap_or_else(|| reason.to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use sonic_rs::json;

    use crate::domain::{
        DeliveryOutcome, DeliveryResult, ProviderFailureClass, PublishStatus, PushPayload,
        PushProviderKind, PushRecipient, SecretString,
    };
    use crate::memory::MemoryPushStore;
    use crate::metrics::PushMetrics;
    use crate::pipeline::{MemoryPushQueue, PushQueue, PushQueueError, QueueAckToken};
    use crate::storage::PushPublishStatusStore;

    use super::*;

    #[test]
    fn retry_after_wins_over_exponential_backoff() {
        let policy = RetryPolicy::default();
        assert_eq!(
            policy.next_attempt_at_ms(1, Some(10_000), 1_000, "seed"),
            10_000
        );
    }

    #[test]
    fn deterministic_jitter_stays_inside_ratio() {
        let policy = RetryPolicy {
            jitter_ratio_percent: 20,
            ..RetryPolicy::default()
        };
        let next = policy.next_attempt_at_ms(1, None, 1_000, "seed");
        assert!((1_800..=2_000).contains(&next));
    }

    #[test]
    fn max_backoff_clamps_exponential_growth() {
        let policy = RetryPolicy {
            initial_backoff_ms: 1_000,
            max_backoff_ms: 5_000,
            jitter_ratio_percent: 0,
            ..RetryPolicy::default()
        };
        assert_eq!(policy.next_attempt_at_ms(10, None, 1_000, "seed"), 6_000);
    }

    #[test]
    fn overflow_safe_timestamp_math_saturates() {
        let policy = RetryPolicy {
            initial_backoff_ms: u64::MAX / 2,
            max_backoff_ms: u64::MAX,
            jitter_ratio_percent: 0,
            ..RetryPolicy::default()
        };
        assert_eq!(
            policy.next_attempt_at_ms(32, None, u64::MAX - 10, "seed"),
            u64::MAX
        );
    }

    #[test]
    fn expiry_prevents_retry_schedule_from_passing_deadline() {
        let feedback = retryable_feedback(Some(1_500));
        let entry = RetryPolicy {
            initial_backoff_ms: 10_000,
            jitter_ratio_percent: 0,
            ..RetryPolicy::default()
        }
        .schedule_retry(&feedback, 1_000)
        .unwrap();

        assert_eq!(entry.next_attempt_at_ms, 1_500);
        assert_eq!(entry.expires_at_ms, 1_500);
    }

    #[tokio::test]
    async fn retry_scheduler_defers_future_entries() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        let mut entry = retry_entry(2, 60_000);
        entry.next_attempt_at_ms = now_ms().saturating_add(60_000);
        queue
            .retry_at(
                PushQueueStage::RetrySchedule,
                entry.key.clone(),
                PushQueuePayload::RetrySchedule(Box::new(entry)),
                now_ms(),
            )
            .await
            .unwrap();

        let scheduler = PushRetryScheduler::new(store, queue.clone(), "node-a");
        assert_eq!(scheduler.run_once("retry").await.unwrap(), 0);
        assert_eq!(
            queue
                .lag(PushQueueStage::RetrySchedule)
                .await
                .unwrap()
                .delayed_depth,
            1
        );
    }

    #[tokio::test]
    async fn retry_scheduler_emits_due_delivery_batch() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        let entry = retry_entry(2, now_ms());
        queue
            .produce(
                PushQueueStage::RetrySchedule,
                entry.key.clone(),
                PushQueuePayload::RetrySchedule(Box::new(entry)),
            )
            .await
            .unwrap();

        let scheduler = PushRetryScheduler::new(store, queue.clone(), "node-a");
        assert_eq!(scheduler.run_once("retry").await.unwrap(), 1);
        assert_eq!(
            queue
                .lag(PushQueueStage::DeliveryJobs(PushProviderKind::Fcm))
                .await
                .unwrap()
                .ready_depth,
            1
        );
    }

    #[tokio::test]
    async fn retry_status_write_does_not_regress_a_terminal_publish() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        let mut terminal = status();
        terminal.state = PublishLifecycleState::Succeeded;
        terminal.counters.succeeded = 1;
        store.put_publish_status(terminal).await.unwrap();
        let entry = retry_entry(2, now_ms());
        queue
            .produce(
                PushQueueStage::RetrySchedule,
                entry.key.clone(),
                PushQueuePayload::RetrySchedule(Box::new(entry)),
            )
            .await
            .unwrap();
        let metrics = PushMetrics::default();
        let scheduler = PushRetryScheduler::new(store.clone(), queue.clone(), "node-a")
            .with_metrics(metrics.clone());

        assert_eq!(scheduler.run_once("retry").await.unwrap(), 1);

        let status = store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.state, PublishLifecycleState::Succeeded);
        assert_eq!(status.counters.retry_attempted, 0);
        assert_eq!(metrics.get("sockudo_push_invariant_violations_total"), 1);
        assert_eq!(
            queue
                .lag(PushQueueStage::DeliveryJobs(PushProviderKind::Fcm))
                .await
                .unwrap()
                .ready_depth,
            0
        );
    }

    #[tokio::test]
    async fn scheduled_retry_after_estimated_terminal_state_dispatches_without_regression() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        let mut terminal = status();
        terminal.state = PublishLifecycleState::Succeeded;
        terminal.counters.succeeded = 1;
        terminal.counters.retry_scheduled = 1;
        store.put_publish_status(terminal).await.unwrap();
        let entry = retry_entry(2, now_ms());
        queue
            .produce(
                PushQueueStage::RetrySchedule,
                entry.key.clone(),
                PushQueuePayload::RetrySchedule(Box::new(entry)),
            )
            .await
            .unwrap();

        let scheduler = PushRetryScheduler::new(store.clone(), queue.clone(), "node-a");
        assert_eq!(scheduler.run_once("retry").await.unwrap(), 1);

        let status = store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.state, PublishLifecycleState::Succeeded);
        assert_eq!(status.counters.retry_attempted, 1);
        assert_eq!(
            queue
                .lag(PushQueueStage::DeliveryJobs(PushProviderKind::Fcm))
                .await
                .unwrap()
                .ready_depth,
            1
        );
    }

    #[tokio::test]
    async fn retry_terminal_outcome_can_refine_an_estimated_terminal_summary() {
        let store = Arc::new(MemoryPushStore::new());
        let mut terminal = status();
        terminal.state = PublishLifecycleState::Succeeded;
        terminal.counters.succeeded = 1;
        store.put_publish_status(terminal).await.unwrap();
        let queue = Arc::new(MemoryPushQueue::new());
        let mut entry = retry_entry(2, now_ms());
        if let Some(job) = entry.job.as_mut() {
            job.expires_at_ms = Some(now_ms().saturating_sub(1));
        }
        queue
            .produce(
                PushQueueStage::RetrySchedule,
                entry.key.clone(),
                PushQueuePayload::RetrySchedule(Box::new(entry)),
            )
            .await
            .unwrap();

        let scheduler = PushRetryScheduler::new(store.clone(), queue, "node-a");
        assert_eq!(scheduler.run_once("retry").await.unwrap(), 1);

        let status = store
            .get_publish_status("app-1", "publish-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.counters.expired, 1);
        assert_eq!(status.state, PublishLifecycleState::PartiallySucceeded);
    }

    #[tokio::test]
    async fn retry_scheduler_dead_letters_expired_entries() {
        let store = Arc::new(MemoryPushStore::new());
        store.put_publish_status(status()).await.unwrap();
        let queue = Arc::new(MemoryPushQueue::new());
        let mut entry = retry_entry(2, now_ms());
        entry.expires_at_ms = now_ms().saturating_sub(1);
        if let Some(job) = entry.job.as_mut() {
            job.expires_at_ms = Some(now_ms().saturating_sub(1));
        }
        queue
            .produce(
                PushQueueStage::RetrySchedule,
                entry.key.clone(),
                PushQueuePayload::RetrySchedule(Box::new(entry)),
            )
            .await
            .unwrap();

        let scheduler = PushRetryScheduler::new(store.clone(), queue.clone(), "node-a");
        assert_eq!(scheduler.run_once("retry").await.unwrap(), 1);
        assert_eq!(
            store
                .get_publish_status("app-1", "publish-1")
                .await
                .unwrap()
                .unwrap()
                .state,
            PublishLifecycleState::Expired
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

    #[tokio::test]
    async fn retry_scheduler_dead_letters_attempt_exhaustion() {
        let store = Arc::new(MemoryPushStore::new());
        store.put_publish_status(status()).await.unwrap();
        let queue = Arc::new(MemoryPushQueue::new());
        let mut entry = retry_entry(6, now_ms());
        entry.max_attempts = 5;
        queue
            .produce(
                PushQueueStage::RetrySchedule,
                entry.key.clone(),
                PushQueuePayload::RetrySchedule(Box::new(entry)),
            )
            .await
            .unwrap();

        let scheduler = PushRetryScheduler::new(store.clone(), queue.clone(), "node-a");
        assert_eq!(scheduler.run_once("retry").await.unwrap(), 1);
        assert_eq!(
            store
                .get_publish_status("app-1", "publish-1")
                .await
                .unwrap()
                .unwrap()
                .state,
            PublishLifecycleState::DeadLettered
        );
    }

    #[tokio::test]
    async fn retry_scheduler_dead_letters_malformed_payload() {
        let store = Arc::new(MemoryPushStore::new());
        store.put_publish_status(status()).await.unwrap();
        let queue = Arc::new(MemoryPushQueue::new());
        let mut entry = retry_entry(2, now_ms());
        entry.job = None;
        queue
            .produce(
                PushQueueStage::RetrySchedule,
                entry.key.clone(),
                PushQueuePayload::RetrySchedule(Box::new(entry)),
            )
            .await
            .unwrap();

        let scheduler = PushRetryScheduler::new(store, queue.clone(), "node-a");
        assert_eq!(scheduler.run_once("retry").await.unwrap(), 0);
        assert_eq!(
            queue
                .lag(PushQueueStage::DeadLetters)
                .await
                .unwrap()
                .ready_depth,
            1
        );
    }

    #[tokio::test]
    async fn retry_scheduler_does_not_ack_when_successor_produce_fails() {
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(FailingProduceQueue::default());
        let entry = retry_entry(2, now_ms());
        queue.seed_retry(entry).await;

        let scheduler = PushRetryScheduler::new(store, queue.clone(), "node-a");
        assert!(scheduler.run_once("retry").await.is_err());
        assert!(!queue.acked().await);
    }

    fn retryable_feedback(expires_at_ms: Option<u64>) -> DeliveryFeedback {
        let mut job = sample_job();
        job.expires_at_ms = expires_at_ms;
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
                    reason: None,
                    retry_after_ms: None,
                }),
                attempt: 1,
            },
            delivery_key: stable_hash(b"delivery-1"),
            retry_job: Some(Box::new(job)),
            first_attempt_at_ms: Some(1_000),
            expires_at_ms,
        }
    }

    fn retry_entry(attempt: u32, next_attempt_at_ms: u64) -> RetryScheduleEntry {
        let feedback = retryable_feedback(None);
        let mut entry = RetryPolicy {
            jitter_ratio_percent: 0,
            ..RetryPolicy::default()
        }
        .schedule_retry(&feedback, now_ms())
        .unwrap();
        entry.attempt = attempt;
        entry.next_attempt_at_ms = next_attempt_at_ms;
        entry.not_before_ms = Some(next_attempt_at_ms);
        entry.expires_at_ms = now_ms().saturating_add(60_000);
        entry
    }

    fn sample_job() -> DeliveryJob {
        DeliveryJob {
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
        }
    }

    fn status() -> PublishStatus {
        PublishStatus {
            app_id: "app-1".to_owned(),
            publish_id: "publish-1".to_owned(),
            state: PublishLifecycleState::Dispatching,
            counters: crate::domain::PublishCounters {
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

    #[derive(Default)]
    struct FailingProduceQueue {
        inner: tokio::sync::Mutex<FailingProduceState>,
    }

    #[derive(Default)]
    struct FailingProduceState {
        message: Option<QueueMessage>,
        acked: bool,
    }

    impl FailingProduceQueue {
        async fn seed_retry(&self, entry: RetryScheduleEntry) {
            let payload = PushQueuePayload::RetrySchedule(Box::new(entry.clone()));
            self.inner.lock().await.message = Some(QueueMessage {
                message_id: "retry-1".to_owned(),
                stage: PushQueueStage::RetrySchedule,
                key: entry.key,
                partition_key: "app-1".to_owned(),
                partition: 0,
                payload,
                attempt: 1,
                enqueued_at_ms: crate::pipeline::now_ms(),
                not_before_ms: None,
                lease_deadline_ms: u64::MAX,
                ack: QueueAckToken {
                    stage: PushQueueStage::RetrySchedule,
                    message_id: "retry-1".to_owned(),
                },
            });
        }

        async fn acked(&self) -> bool {
            self.inner.lock().await.acked
        }
    }

    #[async_trait]
    impl crate::pipeline::PushQueue for FailingProduceQueue {
        fn backend(&self) -> crate::pipeline::PushQueueBackendKind {
            crate::pipeline::PushQueueBackendKind::Memory
        }

        async fn produce(
            &self,
            stage: PushQueueStage,
            _key: String,
            _payload: PushQueuePayload,
        ) -> crate::pipeline::PushQueueResult<String> {
            if matches!(stage, PushQueueStage::DeliveryJobs(_)) {
                return Err(PushQueueError::Backend("produce failed".to_owned()));
            }
            Ok("ok".to_owned())
        }

        async fn retry_at(
            &self,
            stage: PushQueueStage,
            key: String,
            payload: PushQueuePayload,
            _not_before_ms: u64,
        ) -> crate::pipeline::PushQueueResult<String> {
            self.produce(stage, key, payload).await
        }

        async fn consume(
            &self,
            _stage: PushQueueStage,
            _consumer_group: &str,
            _max_messages: usize,
            _lease_timeout_ms: u64,
        ) -> crate::pipeline::PushQueueResult<Vec<QueueMessage>> {
            Ok(self.inner.lock().await.message.take().into_iter().collect())
        }

        async fn ack(&self, _token: QueueAckToken) -> crate::pipeline::PushQueueResult<()> {
            self.inner.lock().await.acked = true;
            Ok(())
        }

        async fn nack(
            &self,
            _token: QueueAckToken,
            _retry_at_ms: Option<u64>,
        ) -> crate::pipeline::PushQueueResult<()> {
            Ok(())
        }

        async fn dead_letter(
            &self,
            _token: QueueAckToken,
            _reason: String,
        ) -> crate::pipeline::PushQueueResult<()> {
            Ok(())
        }

        async fn health(&self) -> crate::pipeline::PushQueueResult<crate::pipeline::QueueHealth> {
            Ok(crate::pipeline::QueueHealth {
                backend: crate::pipeline::PushQueueBackendKind::Memory,
                healthy: true,
                details: "test".to_owned(),
            })
        }

        async fn lag(
            &self,
            _stage: PushQueueStage,
        ) -> crate::pipeline::PushQueueResult<crate::pipeline::QueueLagMetrics> {
            Ok(crate::pipeline::QueueLagMetrics::default())
        }
    }
}
