use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use super::PushDispatcher;
use crate::domain::{
    DeliveryBatch, DeliveryFeedback, DeliveryJob, DeliveryOutcome, DeliveryResult, ProviderError,
    ProviderFailureClass, PushProviderKind, provider_key, stable_hash,
};
use crate::meta::{PushMetaEvent, emit_push_meta_event};
use crate::metrics::PushMetrics;
use crate::pipeline::{PushPipelineResult, PushQueuePayload, PushQueueStage, QueueMessage, now_ms};

#[derive(Clone)]
pub struct ProviderDispatchWorker {
    provider: PushProviderKind,
    queue: crate::pipeline::DynPushQueue,
    dispatcher: Arc<dyn PushDispatcher + Send + Sync>,
    circuit_breaker: ProviderCircuitBreaker,
    rate_limiter: AdaptiveRateLimiter,
    metrics: PushMetrics,
    max_batches_per_tick: usize,
    over_quota_tenants: BTreeSet<String>,
    tenant_inflight_cap: usize,
}

impl ProviderDispatchWorker {
    pub fn new(
        provider: PushProviderKind,
        queue: crate::pipeline::DynPushQueue,
        dispatcher: Arc<dyn PushDispatcher + Send + Sync>,
    ) -> Self {
        Self {
            provider,
            queue,
            dispatcher,
            circuit_breaker: ProviderCircuitBreaker::default(),
            rate_limiter: AdaptiveRateLimiter::default(),
            metrics: PushMetrics::default(),
            max_batches_per_tick: 32,
            over_quota_tenants: BTreeSet::new(),
            tenant_inflight_cap: 8,
        }
    }

    pub fn with_circuit_breaker(mut self, circuit_breaker: ProviderCircuitBreaker) -> Self {
        self.circuit_breaker = circuit_breaker;
        self
    }

    pub fn with_metrics(mut self, metrics: PushMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_rate_limiter(mut self, rate_limiter: AdaptiveRateLimiter) -> Self {
        self.rate_limiter = rate_limiter;
        self
    }

    pub fn with_over_quota_tenants(mut self, tenants: impl IntoIterator<Item = String>) -> Self {
        self.over_quota_tenants = tenants.into_iter().collect();
        self
    }

    pub fn with_tenant_inflight_cap(mut self, cap: usize) -> Self {
        self.tenant_inflight_cap = cap.max(1);
        self
    }

    pub async fn run_once(&mut self, consumer_group: &str) -> PushPipelineResult<usize> {
        let messages = self
            .queue
            .consume(
                PushQueueStage::DeliveryJobs(self.provider),
                consumer_group,
                self.max_batches_per_tick,
                30_000,
            )
            .await?;
        let mut scheduler = WeightedFairScheduler::default()
            .with_over_quota_tenants(self.over_quota_tenants.clone())
            .with_tenant_inflight_cap(self.tenant_inflight_cap);
        for message in messages {
            scheduler.push(message);
        }

        let mut processed = 0;
        while let Some(message) = scheduler.pop_next() {
            if let PushQueuePayload::DeliveryBatch(batch) = &message.payload {
                self.metrics.wfq_dispatched(self.provider, &batch.app_id);
            }
            self.handle_message(message).await?;
            processed += 1;
        }
        for message in scheduler.drain_remaining() {
            self.queue
                .nack(message.ack, Some(now_ms().saturating_add(1_000)))
                .await?;
        }
        Ok(processed)
    }

    async fn handle_message(&mut self, message: QueueMessage) -> PushPipelineResult<()> {
        let PushQueuePayload::DeliveryBatch(batch) = message.payload.clone() else {
            self.queue
                .dead_letter(
                    message.ack,
                    "unexpected payload for provider worker".to_owned(),
                )
                .await?;
            return Ok(());
        };
        let batch = *batch;
        let Some(batch) = self.preflight_batch(message.ack.clone(), batch).await? else {
            return Ok(());
        };
        let app_id = batch.app_id.clone();
        let retry_jobs = batch.jobs.clone();
        self.metrics
            .worker_pool(self.provider, self.max_batches_per_tick, 1);

        if self.circuit_breaker.is_open(now_ms()) {
            self.metrics.counter(
                "sockudo_push_circuit_breaker_deferred_total",
                &[
                    ("provider", crate::metrics::provider_label(self.provider)),
                    ("app", &app_id),
                ],
                1,
            );
            self.metrics
                .circuit_breaker_state(self.provider, &app_id, true);
            self.queue
                .nack(
                    message.ack,
                    Some(self.circuit_breaker.retry_after_ms(now_ms())),
                )
                .await?;
            return Ok(());
        }

        if !self.rate_limiter.acquire(&app_id, self.provider) {
            self.metrics.rate_limiter_throttled(self.provider, &app_id);
            self.queue
                .nack(message.ack, Some(now_ms().saturating_add(1_000)))
                .await?;
            return Ok(());
        }

        let started = Instant::now();
        let started_ms = now_ms();
        self.metrics.dispatch_started(self.provider, &app_id);
        let results = self.dispatcher.dispatch(batch).await;
        let mut saw_retry_after = None;
        let mut failures = 0_u64;
        let mut retry_jobs = retry_jobs.into_iter();
        for result in results {
            let original_job = retry_jobs.next();
            if !matches!(result.outcome, DeliveryOutcome::Accepted) {
                failures += 1;
                tracing::warn!(
                    app_id = %result.app_id,
                    publish_id = %result.publish_id,
                    provider = ?result.provider,
                    batch_id = %result.batch_id,
                    outcome = ?result.outcome,
                    error_class = result.error.as_ref().map(|error| error.class.as_str()),
                    failure_class = result
                        .error
                        .as_ref()
                        .map(|error| error.resolved_failure_class().label()),
                    retry_after_ms = result.error.as_ref().and_then(|error| error.retry_after_ms),
                    "push dispatch failure"
                );
            } else {
                tracing::info!(
                    app_id = %result.app_id,
                    publish_id = %result.publish_id,
                    provider = ?result.provider,
                    batch_id = %result.batch_id,
                    "push dispatch success"
                );
            }
            self.metrics.dispatch_finished(
                result.provider,
                &result.app_id,
                result.outcome,
                started.elapsed(),
            );
            if let Some(retry_after_ms) =
                result.error.as_ref().and_then(|error| error.retry_after_ms)
            {
                saw_retry_after = Some(retry_after_ms);
            }
            let feedback = delivery_feedback(result, original_job, started_ms);
            self.queue
                .produce(
                    PushQueueStage::DeliveryResults,
                    feedback_key(&feedback),
                    PushQueuePayload::DeliveryFeedback(Box::new(feedback)),
                )
                .await?;
        }

        if let Some(retry_after_ms) = saw_retry_after {
            self.rate_limiter
                .record_throttle(&app_id, self.provider, now_ms());
            self.circuit_breaker.defer_until(retry_after_ms);
            self.metrics
                .circuit_breaker_state(self.provider, &app_id, true);
            self.emit_circuit_event("open_retry_after", retry_after_ms);
        } else if failures == 0 {
            self.rate_limiter
                .record_success_window(&app_id, self.provider, now_ms());
            self.circuit_breaker.record_success();
            self.metrics
                .circuit_breaker_state(self.provider, &app_id, false);
        } else {
            if self.circuit_breaker.record_failure(now_ms()) {
                self.metrics
                    .circuit_breaker_state(self.provider, &app_id, true);
                self.emit_circuit_event("open_failure_rate", self.circuit_breaker.open_until_ms);
            }
        }

        self.rate_limiter.release(&app_id, self.provider);
        self.metrics
            .worker_pool(self.provider, self.max_batches_per_tick, 0);
        self.queue.ack(message.ack).await?;
        Ok(())
    }

    async fn preflight_batch(
        &self,
        ack: crate::pipeline::QueueAckToken,
        batch: DeliveryBatch,
    ) -> PushPipelineResult<Option<DeliveryBatch>> {
        let now = now_ms();
        let DeliveryBatch {
            app_id,
            publish_id,
            provider,
            batch_id,
            jobs,
        } = batch;
        let mut ready = Vec::with_capacity(jobs.len());
        let mut future = Vec::new();
        let mut expired = Vec::new();

        for job in jobs {
            if is_expired_before_dispatch(&job, now) {
                expired.push(job);
            } else if job.not_before_ms.is_some_and(|not_before| not_before > now) {
                future.push(job);
            } else {
                ready.push(job);
            }
        }

        let emitted_expired = !expired.is_empty();
        for job in expired {
            self.emit_expired_feedback(job, now).await?;
        }

        let next_not_before_ms = future.iter().filter_map(|job| job.not_before_ms).min();
        if ready.is_empty() {
            if let Some(not_before_ms) = next_not_before_ms {
                if future.is_empty() {
                    self.queue.ack(ack).await?;
                } else if emitted_expired {
                    self.defer_future_jobs(
                        &app_id,
                        &publish_id,
                        provider,
                        &batch_id,
                        future,
                        not_before_ms,
                    )
                    .await?;
                    self.queue.ack(ack).await?;
                } else {
                    self.queue.nack(ack, Some(not_before_ms)).await?;
                }
            } else {
                self.queue.ack(ack).await?;
            }
            return Ok(None);
        }

        if let Some(not_before_ms) = next_not_before_ms {
            self.defer_future_jobs(
                &app_id,
                &publish_id,
                provider,
                &batch_id,
                future,
                not_before_ms,
            )
            .await?;
        }

        Ok(Some(DeliveryBatch {
            app_id,
            publish_id,
            provider,
            batch_id,
            jobs: ready,
        }))
    }

    async fn defer_future_jobs(
        &self,
        app_id: &str,
        publish_id: &str,
        provider: PushProviderKind,
        original_batch_id: &str,
        mut jobs: Vec<DeliveryJob>,
        not_before_ms: u64,
    ) -> PushPipelineResult<()> {
        if jobs.is_empty() {
            return Ok(());
        }
        let batch_id = format!("{original_batch_id}-deferred-{not_before_ms}");
        for job in &mut jobs {
            job.batch_id = batch_id.clone();
        }
        let batch = DeliveryBatch {
            app_id: app_id.to_owned(),
            publish_id: publish_id.to_owned(),
            provider,
            batch_id,
            jobs,
        };
        self.queue
            .retry_at(
                PushQueueStage::DeliveryJobs(provider),
                batch.queue_key(),
                PushQueuePayload::DeliveryBatch(Box::new(batch)),
                not_before_ms,
            )
            .await?;
        Ok(())
    }

    async fn emit_expired_feedback(
        &self,
        job: DeliveryJob,
        occurred_at_ms: u64,
    ) -> PushPipelineResult<()> {
        let result = DeliveryResult {
            app_id: job.app_id.clone(),
            publish_id: job.publish_id.clone(),
            provider: job.provider,
            batch_id: job.batch_id.clone(),
            device_id: job.device_id.clone(),
            outcome: DeliveryOutcome::Expired,
            provider_message_id: None,
            error: Some(ProviderError {
                class: "expired".to_owned(),
                failure_class: ProviderFailureClass::Unknown,
                reason: Some("delivery expired before provider dispatch".to_owned()),
                retry_after_ms: None,
            }),
            attempt: job.attempt,
        };
        let feedback = delivery_feedback(result, Some(job), occurred_at_ms);
        self.queue
            .produce(
                PushQueueStage::DeliveryResults,
                feedback_key(&feedback),
                PushQueuePayload::DeliveryFeedback(Box::new(feedback)),
            )
            .await?;
        Ok(())
    }

    fn emit_circuit_event(&self, action: &'static str, retry_at_ms: u64) {
        emit_push_meta_event(PushMetaEvent::circuit_breaker_event(
            "unknown",
            self.provider,
            action,
            retry_at_ms,
        ));
    }
}

#[derive(Clone, Debug)]
pub struct ProviderCircuitBreaker {
    state: CircuitState,
    failure_count: u32,
    open_until_ms: u64,
    failure_threshold: u32,
    cool_down_ms: u64,
}

impl Default for ProviderCircuitBreaker {
    fn default() -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            open_until_ms: 0,
            failure_threshold: 5,
            cool_down_ms: 30_000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl ProviderCircuitBreaker {
    pub fn is_open(&mut self, now_ms: u64) -> bool {
        match self.state {
            CircuitState::Open if now_ms >= self.open_until_ms => {
                self.state = CircuitState::HalfOpen;
                false
            }
            CircuitState::Open => true,
            CircuitState::Closed | CircuitState::HalfOpen => false,
        }
    }

    pub fn retry_after_ms(&self, now_ms: u64) -> u64 {
        self.open_until_ms.max(now_ms.saturating_add(1_000))
    }

    pub fn defer_until(&mut self, retry_after_ms: u64) {
        self.state = CircuitState::Open;
        self.open_until_ms = retry_after_ms;
    }

    pub fn record_success(&mut self) {
        self.state = CircuitState::Closed;
        self.failure_count = 0;
        self.open_until_ms = 0;
    }

    pub fn record_failure(&mut self, now_ms: u64) -> bool {
        self.failure_count = self.failure_count.saturating_add(1);
        if self.failure_count >= self.failure_threshold {
            self.state = CircuitState::Open;
            self.open_until_ms = now_ms.saturating_add(self.cool_down_ms);
            return true;
        }
        false
    }
}

#[derive(Clone, Debug)]
pub struct AdaptiveRateLimiter {
    lanes: BTreeMap<(String, PushProviderKind), AdaptiveLane>,
    default_limit: u32,
    min_limit: u32,
    max_limit: u32,
    grow_after_ms: u64,
}

#[derive(Clone, Debug)]
struct AdaptiveLane {
    limit: u32,
    inflight: u32,
    last_throttle_ms: u64,
    last_growth_ms: u64,
}

impl Default for AdaptiveRateLimiter {
    fn default() -> Self {
        Self {
            lanes: BTreeMap::new(),
            default_limit: 100,
            min_limit: 1,
            max_limit: 10_000,
            grow_after_ms: 60_000,
        }
    }
}

impl AdaptiveRateLimiter {
    pub fn acquire(&mut self, app_id: &str, provider: PushProviderKind) -> bool {
        let lane = self.lane(app_id, provider);
        if lane.inflight >= lane.limit {
            return false;
        }
        lane.inflight += 1;
        true
    }

    pub fn release(&mut self, app_id: &str, provider: PushProviderKind) {
        let lane = self.lane(app_id, provider);
        lane.inflight = lane.inflight.saturating_sub(1);
    }

    pub fn record_throttle(&mut self, app_id: &str, provider: PushProviderKind, now_ms: u64) {
        let min_limit = self.min_limit;
        let grow_after_ms = self.grow_after_ms;
        let lane = self.lane(app_id, provider);
        if lane.last_throttle_ms == 0
            || now_ms.saturating_sub(lane.last_throttle_ms) >= grow_after_ms / 2
        {
            lane.limit = (lane.limit / 2).max(min_limit);
        }
        lane.last_throttle_ms = now_ms;
        lane.last_growth_ms = now_ms;
    }

    pub fn record_success_window(&mut self, app_id: &str, provider: PushProviderKind, now_ms: u64) {
        let grow_after_ms = self.grow_after_ms;
        let max_limit = self.max_limit;
        let lane = self.lane(app_id, provider);
        if now_ms.saturating_sub(lane.last_throttle_ms) >= grow_after_ms
            && now_ms.saturating_sub(lane.last_growth_ms) >= grow_after_ms
        {
            lane.limit = lane.limit.saturating_add(1).min(max_limit);
            lane.last_growth_ms = now_ms.saturating_add(jitter_ms(grow_after_ms / 5));
        }
    }

    pub fn limit(&mut self, app_id: &str, provider: PushProviderKind) -> u32 {
        self.lane(app_id, provider).limit
    }

    fn lane(&mut self, app_id: &str, provider: PushProviderKind) -> &mut AdaptiveLane {
        self.lanes
            .entry((app_id.to_owned(), provider))
            .or_insert_with(|| AdaptiveLane {
                limit: self.default_limit,
                inflight: 0,
                last_throttle_ms: 0,
                last_growth_ms: 0,
            })
    }
}

fn jitter_ms(spread_ms: u64) -> u64 {
    if spread_ms == 0 {
        return 0;
    }
    u64::from(rand::random::<u32>()) % spread_ms.saturating_add(1)
}

pub struct WeightedFairScheduler {
    lanes: BTreeMap<String, TenantLane>,
    order: VecDeque<String>,
    over_quota_tenants: BTreeSet<String>,
    tenant_inflight_cap: usize,
}

impl Default for WeightedFairScheduler {
    fn default() -> Self {
        Self {
            lanes: BTreeMap::new(),
            order: VecDeque::new(),
            over_quota_tenants: BTreeSet::new(),
            tenant_inflight_cap: 8,
        }
    }
}

struct TenantLane {
    messages: VecDeque<QueueMessage>,
    deficit: u32,
    weight_units: u32,
    dispatched_this_tick: usize,
}

impl WeightedFairScheduler {
    const DEFAULT_WEIGHT_UNITS: u32 = 10;
    const OVER_QUOTA_WEIGHT_UNITS: u32 = 1;
    const MESSAGE_COST_UNITS: u32 = 10;

    pub fn with_over_quota_tenants(mut self, tenants: impl IntoIterator<Item = String>) -> Self {
        self.over_quota_tenants = tenants.into_iter().collect();
        self
    }

    pub fn with_tenant_inflight_cap(mut self, cap: usize) -> Self {
        self.tenant_inflight_cap = cap.max(1);
        self
    }

    pub fn push(&mut self, message: QueueMessage) {
        let app_id = match &message.payload {
            PushQueuePayload::DeliveryBatch(batch) => batch.app_id.clone(),
            _ => "[unknown]".to_owned(),
        };
        if !self.lanes.contains_key(&app_id) {
            self.order.push_back(app_id.clone());
        }
        let weight_units = if self.over_quota_tenants.contains(&app_id) {
            Self::OVER_QUOTA_WEIGHT_UNITS
        } else {
            Self::DEFAULT_WEIGHT_UNITS
        };
        self.lanes
            .entry(app_id)
            .or_insert_with(|| TenantLane {
                messages: VecDeque::new(),
                deficit: 0,
                weight_units,
                dispatched_this_tick: 0,
            })
            .messages
            .push_back(message);
    }

    pub fn pop_next(&mut self) -> Option<QueueMessage> {
        let mut scanned = 0_usize;
        let scan_limit = self.order.len().saturating_mul(12).max(1);
        while let Some(app_id) = self.order.pop_front() {
            scanned += 1;
            let Some(lane) = self.lanes.get_mut(&app_id) else {
                continue;
            };
            if lane.dispatched_this_tick >= self.tenant_inflight_cap {
                self.order.push_back(app_id);
                if scanned >= scan_limit {
                    return None;
                }
                continue;
            }
            lane.deficit = lane.deficit.saturating_add(lane.weight_units);
            if lane.deficit < Self::MESSAGE_COST_UNITS {
                self.order.push_back(app_id);
                if scanned >= scan_limit {
                    return None;
                }
                continue;
            }
            if let Some(message) = lane.messages.pop_front() {
                lane.deficit = lane.deficit.saturating_sub(Self::MESSAGE_COST_UNITS);
                lane.dispatched_this_tick = lane.dispatched_this_tick.saturating_add(1);
                if lane.messages.is_empty() {
                    self.lanes.remove(&app_id);
                } else {
                    self.order.push_back(app_id);
                }
                return Some(message);
            }
            self.lanes.remove(&app_id);
            if scanned >= scan_limit {
                return None;
            }
        }
        None
    }

    pub fn drain_remaining(self) -> Vec<QueueMessage> {
        self.lanes
            .into_values()
            .flat_map(|lane| lane.messages)
            .collect()
    }
}

fn delivery_feedback(
    result: DeliveryResult,
    original_job: Option<DeliveryJob>,
    started_ms: u64,
) -> DeliveryFeedback {
    let delivery_key = original_job
        .as_ref()
        .map(|job| stable_hash(job.idempotency_key().as_bytes()))
        .unwrap_or_else(|| {
            stable_hash(
                format!(
                    "{}:{}:{}:{}:{}",
                    result.app_id,
                    result.publish_id,
                    provider_key(result.provider),
                    result.batch_id,
                    result.device_id.as_deref().unwrap_or("[provider-target]")
                )
                .as_bytes(),
            )
        });
    let retry_job = if matches!(result.outcome, DeliveryOutcome::Retryable) {
        original_job.map(|mut job| {
            job.first_attempt_at_ms = Some(job.first_attempt_at_ms.unwrap_or(started_ms));
            Box::new(job)
        })
    } else {
        None
    };
    let first_attempt_at_ms = retry_job
        .as_ref()
        .and_then(|job| job.first_attempt_at_ms)
        .or(Some(started_ms));
    let expires_at_ms = retry_job.as_ref().and_then(|job| job.expires_at_ms);
    DeliveryFeedback {
        result,
        delivery_key,
        retry_job,
        first_attempt_at_ms,
        expires_at_ms,
    }
}

fn feedback_key(feedback: &DeliveryFeedback) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}",
        feedback.result.app_id,
        feedback.result.publish_id,
        provider_key(feedback.result.provider),
        feedback.result.batch_id,
        feedback.delivery_key,
        feedback.result.attempt
    )
}

fn is_expired_before_dispatch(job: &DeliveryJob, now_ms: u64) -> bool {
    job.expires_at_ms.is_some_and(|expires_at_ms| {
        expires_at_ms <= now_ms
            || job
                .not_before_ms
                .is_some_and(|not_before_ms| expires_at_ms <= not_before_ms)
    })
}
