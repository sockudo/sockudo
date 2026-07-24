use super::ConnectionHandler;
use futures_util::stream::{self, StreamExt};
use parking_lot::Mutex;
use sockudo_ai_transport::{
    ActiveStreamDelta, DueStreamToken, RollupDelivery, RollupDeliveryReason, RollupEngine,
};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

const MAX_DUE_PER_TICK: usize = 4_096;
const MAX_RETRY_QUEUE: usize = 4_096;
const MAX_DELIVERY_ATTEMPTS: u8 = 3;
const MAX_CONCURRENT_CHANNELS: usize = 16;

pub(super) struct AiWorkerLifecycle {
    cancellation: CancellationToken,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl AiWorkerLifecycle {
    pub(super) fn new() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            handles: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub(super) fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        self.handles.lock().push(runtime.spawn(future));
    }

    pub(super) async fn shutdown(&self) {
        self.cancellation.cancel();
        let handles = std::mem::take(&mut *self.handles.lock());
        for handle in handles {
            if let Err(error) = handle.await
                && !error.is_cancelled()
            {
                warn!(error = %error, "AI worker stopped unexpectedly during shutdown");
            }
        }
    }
}

impl Drop for AiWorkerLifecycle {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

#[derive(Debug)]
enum ScheduledPayload {
    Due(DueStreamToken),
    Delivery(Box<RollupDelivery>),
}

#[derive(Debug)]
struct ScheduledItem {
    payload: ScheduledPayload,
    attempts: u8,
}

impl ScheduledItem {
    fn due(token: DueStreamToken) -> Self {
        Self {
            payload: ScheduledPayload::Due(token),
            attempts: 0,
        }
    }

    fn delivery(delivery: RollupDelivery, attempts: u8) -> Self {
        Self {
            payload: ScheduledPayload::Delivery(Box::new(delivery)),
            attempts,
        }
    }

    fn app_id(&self) -> &str {
        match &self.payload {
            ScheduledPayload::Due(token) => token.app_id(),
            ScheduledPayload::Delivery(delivery) => &delivery.app_id,
        }
    }

    fn channel(&self) -> &str {
        match &self.payload {
            ScheduledPayload::Due(token) => token.channel(),
            ScheduledPayload::Delivery(delivery) => &delivery.channel,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailedDeliveryOutcome {
    Retry,
    ResetRequired,
}

fn failed_delivery_outcome(attempts: u8, retry_queue_len: usize) -> FailedDeliveryOutcome {
    if attempts < MAX_DELIVERY_ATTEMPTS && retry_queue_len < MAX_RETRY_QUEUE {
        FailedDeliveryOutcome::Retry
    } else {
        FailedDeliveryOutcome::ResetRequired
    }
}

pub(super) fn start_rollup_worker(
    handler: ConnectionHandler,
    lifecycle: Arc<AiWorkerLifecycle>,
    engine: Arc<RollupEngine>,
    wheel_tick_ms: u64,
) {
    let cancellation = lifecycle.token();
    lifecycle.spawn(async move {
        run_rollup_worker(handler, engine, cancellation, wheel_tick_ms.max(1)).await;
    });
}

async fn run_rollup_worker(
    handler: ConnectionHandler,
    engine: Arc<RollupEngine>,
    cancellation: CancellationToken,
    wheel_tick_ms: u64,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(wheel_tick_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut retries = VecDeque::with_capacity(MAX_RETRY_QUEUE);

    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                drain_for_shutdown(&handler, &engine, &mut retries).await;
                return;
            }
            _ = interval.tick() => {
                let available = MAX_DUE_PER_TICK.saturating_sub(retries.len());
                let poll = engine.poll_due_tokens(rollup_now_ms(), available);
                let mut work = retries.drain(..).collect::<Vec<_>>();
                work.extend(poll.tokens.into_iter().map(ScheduledItem::due));
                process_deliveries(&handler, &engine, work, &mut retries).await;
            }
        }
    }
}

async fn drain_for_shutdown(
    handler: &ConnectionHandler,
    engine: &RollupEngine,
    retries: &mut VecDeque<ScheduledItem>,
) {
    while !retries.is_empty() || engine.active_streams() > 0 {
        let available = MAX_DUE_PER_TICK.saturating_sub(retries.len());
        let drained = engine.drain_pending(available);
        apply_active_deltas(handler, drained.active_stream_deltas);
        let mut work = retries.drain(..).collect::<Vec<_>>();
        work.extend(
            drained
                .deliveries
                .into_iter()
                .map(|delivery| ScheduledItem::delivery(delivery, 0)),
        );
        if work.is_empty() {
            continue;
        }
        process_deliveries(handler, engine, work, retries).await;
    }
}

async fn process_deliveries(
    handler: &ConnectionHandler,
    engine: &RollupEngine,
    work: Vec<ScheduledItem>,
    retries: &mut VecDeque<ScheduledItem>,
) {
    if work.is_empty() {
        return;
    }

    let mut groups: Vec<Vec<ScheduledItem>> = Vec::new();
    let mut group_indexes = HashMap::new();
    for item in work {
        let key = format!("{}\0{}", item.app_id(), item.channel());
        let index = *group_indexes.entry(key).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[index].push(item);
    }

    let failures = stream::iter(groups)
        .map(|group| process_channel_group(handler, engine, group))
        .buffer_unordered(MAX_CONCURRENT_CHANNELS)
        .collect::<Vec<_>>()
        .await;

    for group in failures {
        let mut reset_channel = false;
        for item in group {
            let app_id = item.app_id().to_string();
            let channel = item.channel().to_string();
            let outcome = if reset_channel {
                FailedDeliveryOutcome::ResetRequired
            } else {
                failed_delivery_outcome(item.attempts, retries.len())
            };
            match outcome {
                FailedDeliveryOutcome::Retry => {
                    warn!(
                        app_id = %app_id,
                        channel = %channel,
                        attempts = item.attempts,
                        outcome = "retry",
                        "deferred AI rollup delivery failed"
                    );
                    retries.push_back(item);
                }
                FailedDeliveryOutcome::ResetRequired => {
                    reset_channel = true;
                    if let ScheduledPayload::Due(token) = item.payload {
                        let claimed = engine.claim_due(token);
                        apply_active_deltas(handler, claimed.active_stream_deltas);
                    }
                    error!(
                        app_id = %app_id,
                        channel = %channel,
                        attempts = item.attempts,
                        outcome = "reset_required",
                        "deferred AI rollup delivery exhausted bounded retries; clients must reset continuity"
                    );
                }
            }
        }
    }
}

async fn process_channel_group(
    handler: &ConnectionHandler,
    engine: &RollupEngine,
    mut group: Vec<ScheduledItem>,
) -> Vec<ScheduledItem> {
    let app_id = group[0].app_id().to_string();
    let channel = group[0].channel().to_string();
    let app = match handler.app_manager.find_by_id(&app_id).await {
        Ok(Some(app)) if app.enabled => app,
        Ok(_) => {
            warn!(app_id, channel, "deferred AI rollup app is unavailable");
            for item in &mut group {
                item.attempts = item.attempts.saturating_add(1);
            }
            return group;
        }
        Err(error) => {
            warn!(app_id, channel, error = %error, "failed to load app for deferred AI rollup");
            for item in &mut group {
                item.attempts = item.attempts.saturating_add(1);
            }
            return group;
        }
    };
    let _permit = match handler
        .acquire_channel_publish_permit(&app_id, &channel)
        .await
    {
        Ok(permit) => permit,
        Err(error) => {
            warn!(app_id, channel, error = %error, "failed to enter deferred AI rollup ordering gate");
            for item in &mut group {
                item.attempts = item.attempts.saturating_add(1);
            }
            return group;
        }
    };

    let mut iter = group.into_iter();
    while let Some(mut item) = iter.next() {
        let delivery = match item.payload {
            ScheduledPayload::Due(token) => {
                let mut claimed = engine.claim_due(token);
                apply_active_deltas(handler, claimed.active_stream_deltas);
                let Some(delivery) = claimed.deliveries.pop() else {
                    continue;
                };
                delivery
            }
            ScheduledPayload::Delivery(delivery) => *delivery,
        };
        loop {
            item.attempts = item.attempts.saturating_add(1);
            match handler
                .send_one_egress_message(
                    &app,
                    &delivery.channel,
                    delivery.message.clone(),
                    delivery.context.exclude_socket.as_ref(),
                    delivery.context.start_time_ms,
                    delivery.context.force_full_message,
                    delivery.context.envelope.clone(),
                )
                .await
            {
                Ok(()) => {
                    mark_delivered(handler, &delivery);
                    break;
                }
                Err(error) if item.attempts < MAX_DELIVERY_ATTEMPTS => {
                    warn!(
                        app_id = %delivery.app_id,
                        channel = %delivery.channel,
                        attempts = item.attempts,
                        error = %error,
                        outcome = "retry",
                        "deferred AI rollup fanout attempt failed"
                    );
                }
                Err(error) => {
                    error!(
                        app_id = %delivery.app_id,
                        channel = %delivery.channel,
                        attempts = item.attempts,
                        error = %error,
                        outcome = "reset_required",
                        "deferred AI rollup delivery failed while ordered; clients must reset continuity"
                    );
                    for remaining in iter {
                        if let ScheduledPayload::Due(token) = remaining.payload {
                            let claimed = engine.claim_due(token);
                            apply_active_deltas(handler, claimed.active_stream_deltas);
                        }
                    }
                    return Vec::new();
                }
            }
        }
    }
    Vec::new()
}

fn apply_active_deltas(handler: &ConnectionHandler, deltas: Vec<ActiveStreamDelta>) {
    let Some(metrics) = handler.metrics() else {
        return;
    };
    let mut by_app: HashMap<String, isize> = HashMap::new();
    for delta in deltas {
        *by_app.entry(delta.app_id).or_default() += delta.delta;
    }
    for (app_id, delta) in by_app {
        metrics.add_ai_rollup_active_streams(&app_id, delta as i64);
    }
}

fn mark_delivered(handler: &ConnectionHandler, delivery: &RollupDelivery) {
    if delivery.reason == RollupDeliveryReason::Bypass {
        return;
    }
    if let Some(metrics) = handler.metrics() {
        metrics.mark_ai_rollup_append_delivered(&delivery.app_id);
        metrics.observe_ai_rollup_ratio(&delivery.app_id, delivery.coalesced as f64);
        metrics.observe_ai_rollup_flush_latency(&delivery.app_id, delivery.latency_ms as f64);
    }
}

pub(super) fn rollup_now_ms() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn failed_delivery_retries_then_requires_reset() {
        assert_eq!(failed_delivery_outcome(1, 0), FailedDeliveryOutcome::Retry);
        assert_eq!(
            failed_delivery_outcome(MAX_DELIVERY_ATTEMPTS, 0),
            FailedDeliveryOutcome::ResetRequired
        );
    }

    #[test]
    fn full_retry_queue_requires_reset() {
        assert_eq!(
            failed_delivery_outcome(1, MAX_RETRY_QUEUE),
            FailedDeliveryOutcome::ResetRequired
        );
    }

    #[tokio::test]
    async fn lifecycle_shutdown_cancels_and_joins_workers() {
        let lifecycle = Arc::new(AiWorkerLifecycle::new());
        let stopped = Arc::new(AtomicBool::new(false));
        let token = lifecycle.token();
        let task_stopped = Arc::clone(&stopped);
        lifecycle.spawn(async move {
            token.cancelled().await;
            task_stopped.store(true, Ordering::Release);
        });

        lifecycle.shutdown().await;

        assert!(stopped.load(Ordering::Acquire));
    }
}
