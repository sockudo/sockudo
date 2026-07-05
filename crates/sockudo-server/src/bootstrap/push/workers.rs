use super::queue::push_queue_now_ms;
use futures_util::FutureExt;
use sockudo_core::options::ServerOptions;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::warn;

mod providers;

use providers::{
    start_apns_provider_workers, start_fcm_provider_workers, start_hms_provider_workers,
    start_webpush_provider_workers, start_wns_provider_workers,
};

#[cfg(all(feature = "push", feature = "monolith"))]
fn push_fanout_config(config: &ServerOptions) -> sockudo_push::FanoutConfig {
    sockudo_push::FanoutConfig {
        fast_threshold: config.push.fanout_fast_threshold,
        shard_size: config.push.fanout_shard_size,
        status_retention_days: config.push.publish_status_ttl_days,
        ..sockudo_push::FanoutConfig::default()
    }
}

#[cfg(all(feature = "push", feature = "monolith"))]
fn push_retry_policy(config: &ServerOptions) -> sockudo_push::RetryPolicy {
    sockudo_push::RetryPolicy {
        initial_backoff_ms: config.push.retry.initial_backoff_ms,
        max_backoff_ms: config.push.retry.max_backoff_ms,
        max_attempts: config.push.retry.max_attempts,
        max_retry_age_ms: config.push.retry.max_elapsed_secs.saturating_mul(1_000),
        jitter_ratio_percent: if config.push.retry.jitter {
            config.push.retry.jitter_ratio_percent
        } else {
            0
        },
        respect_retry_after: config.push.retry.respect_retry_after,
    }
    .bounded()
}

const PUSH_WORKER_RESTART_BACKOFF: Duration = Duration::from_secs(1);

pub(super) fn spawn_supervised_worker<F, Fut>(
    kind: &'static str,
    group: String,
    mut build: F,
) -> JoinHandle<()>
where
    F: FnMut(String) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let metrics = sockudo_push::PushMetrics::default();
        loop {
            let worker = group.clone();
            let reason = match AssertUnwindSafe(build(worker.clone())).catch_unwind().await {
                Ok(()) => "returned",
                Err(_) => "panic",
            };
            metrics.worker_exit(kind, &worker, reason);
            warn!(
                worker = %worker,
                kind,
                reason,
                "push worker exited; restarting after backoff"
            );
            tokio::time::sleep(PUSH_WORKER_RESTART_BACKOFF).await;
        }
    })
}

#[cfg(all(feature = "push", feature = "monolith"))]
pub(crate) fn start_push_monolith_workers(
    config: &ServerOptions,
    store: sockudo_push::DynPushStore,
    queue: sockudo_push::DynPushQueue,
) -> Vec<JoinHandle<()>> {
    let fanout_config = push_fanout_config(config);
    let retry_policy = push_retry_policy(config);
    let mut handles = Vec::new();

    for worker_index in 0..config.push.planner_worker_count {
        let planner =
            sockudo_push::PushPlanner::new(store.clone(), queue.clone(), fanout_config.clone());
        handles.push(spawn_supervised_worker(
            "planner",
            format!("sockudo-monolith-planner-{worker_index}"),
            move |group| {
                let planner = planner.clone();
                async move {
            warn!(worker = %group, "push planner worker started");
            loop {
                match planner.run_once(&group).await {
                    Ok(processed) if processed > 0 => {
                        warn!(worker = %group, processed, "push planner worker processed messages");
                    }
                    Ok(_) => {}
                    Err(error) => {
                        warn!(worker = %group, error = %error, "push planner worker tick failed");
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
                }
            },
        ));
    }

    for worker_index in 0..config.push.shard_worker_count {
        let worker =
            sockudo_push::PushShardWorker::new(store.clone(), queue.clone(), fanout_config.clone());
        handles.push(spawn_supervised_worker(
            "shard",
            format!("sockudo-monolith-shard-{worker_index}"),
            move |group| {
                let worker = worker.clone();
                async move {
            warn!(worker = %group, "push shard worker started");
            loop {
                match worker.run_once(&group).await {
                    Ok(processed) if processed > 0 => {
                        warn!(worker = %group, processed, "push shard worker processed messages");
                    }
                    Ok(_) => {}
                    Err(error) => {
                        warn!(worker = %group, error = %error, "push shard worker tick failed");
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
                }
            },
        ));
    }

    for worker_index in 0..config.push.feedback_worker_count {
        let processor = sockudo_push::PushFeedbackProcessor::new(store.clone(), queue.clone())
            .with_retry_policy(retry_policy.clone());
        handles.push(spawn_supervised_worker(
            "feedback",
            format!("sockudo-monolith-feedback-{worker_index}"),
            move |group| {
                let processor = processor.clone();
                async move {
            warn!(worker = %group, "push feedback worker started");
            loop {
                match processor.run_once(&group).await {
                    Ok(processed) if processed > 0 => {
                        warn!(worker = %group, processed, "push feedback worker processed messages");
                    }
                    Ok(_) => {}
                    Err(error) => {
                        warn!(worker = %group, error = %error, "push feedback worker tick failed");
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
                }
            },
        ));
    }

    for worker_index in 0..config.push.retry_worker_count {
        let scheduler = sockudo_push::PushRetryScheduler::new(
            store.clone(),
            queue.clone(),
            format!("sockudo-monolith-retry-{worker_index}"),
        );
        handles.push(spawn_supervised_worker(
            "retry",
            format!("sockudo-monolith-retry-{worker_index}"),
            move |group| {
                let scheduler = scheduler.clone();
                async move {
            warn!(worker = %group, "push retry scheduler worker started");
            loop {
                match scheduler.run_once(&group).await {
                    Ok(processed) if processed > 0 => {
                        warn!(worker = %group, processed, "push retry scheduler worker processed messages");
                    }
                    Ok(_) => {}
                    Err(error) => {
                        warn!(worker = %group, error = %error, "push retry scheduler worker tick failed");
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
                }
            },
        ));
    }

    {
        let scheduler = sockudo_push::PushScheduler::new(
            store.clone(),
            queue.clone(),
            "sockudo-monolith-scheduler",
            fanout_config.clone(),
        );
        let store = store.clone();
        handles.push(spawn_supervised_worker(
            "scheduler",
            "sockudo-monolith-scheduler".to_owned(),
            move |group| {
                let scheduler = scheduler.clone();
                let store = store.clone();
                async move {
            warn!(worker = %group, "push scheduler worker started");
            loop {
                let now = push_queue_now_ms();
                let due_minute_ms = now - (now % 60_000);
                match store.list_scheduled_apps().await {
                    Ok(app_ids) => {
                        for app_id in app_ids {
                            match scheduler.poll_due_bucket(&app_id, due_minute_ms, now).await {
                                Ok(processed) if processed > 0 => {
                                    warn!(worker = %group, app_id = %app_id, processed, "push scheduler worker emitted due jobs");
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    warn!(worker = %group, app_id = %app_id, error = %error, "push scheduler app tick failed");
                                }
                            }
                        }
                    }
                    Err(error) => {
                        warn!(worker = %group, error = %error, "push scheduler app discovery failed");
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
                }
            },
        ));
    }

    handles.extend(start_push_provider_workers(config, store, queue));
    handles
}

#[cfg(all(feature = "push", feature = "monolith"))]
fn start_push_provider_workers(
    config: &ServerOptions,
    store: sockudo_push::DynPushStore,
    queue: sockudo_push::DynPushQueue,
) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::new();
    if config.push.webpush_enabled {
        handles.extend(start_webpush_provider_workers(config, queue.clone()));
    }

    if config.push.apns_enabled {
        handles.extend(start_apns_provider_workers(
            config,
            store.clone(),
            queue.clone(),
        ));
    }

    if config.push.fcm_enabled {
        handles.extend(start_fcm_provider_workers(
            config,
            store.clone(),
            queue.clone(),
        ));
    }

    if config.push.hms_enabled {
        handles.extend(start_hms_provider_workers(config, queue.clone()));
    }

    if config.push.wns_enabled {
        handles.extend(start_wns_provider_workers(config, queue));
    }
    handles
}
