use super::queue::push_queue_now_ms;
use sockudo_core::options::ServerOptions;
use std::time::Duration;
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

#[cfg(all(feature = "push", feature = "monolith"))]
pub(crate) fn start_push_monolith_workers(
    config: &ServerOptions,
    store: sockudo_push::DynPushStore,
    queue: sockudo_push::DynPushQueue,
) {
    let fanout_config = push_fanout_config(config);
    let retry_policy = push_retry_policy(config);

    for worker_index in 0..config.push.planner_worker_count {
        let planner =
            sockudo_push::PushPlanner::new(store.clone(), queue.clone(), fanout_config.clone());
        tokio::spawn(async move {
            let group = format!("sockudo-monolith-planner-{worker_index}");
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
        });
    }

    for worker_index in 0..config.push.shard_worker_count {
        let worker =
            sockudo_push::PushShardWorker::new(store.clone(), queue.clone(), fanout_config.clone());
        tokio::spawn(async move {
            let group = format!("sockudo-monolith-shard-{worker_index}");
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
        });
    }

    for worker_index in 0..config.push.feedback_worker_count {
        let processor = sockudo_push::PushFeedbackProcessor::new(store.clone(), queue.clone())
            .with_retry_policy(retry_policy.clone());
        tokio::spawn(async move {
            let group = format!("sockudo-monolith-feedback-{worker_index}");
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
        });
    }

    for worker_index in 0..config.push.retry_worker_count {
        let scheduler = sockudo_push::PushRetryScheduler::new(
            store.clone(),
            queue.clone(),
            format!("sockudo-monolith-retry-{worker_index}"),
        );
        tokio::spawn(async move {
            let group = format!("sockudo-monolith-retry-{worker_index}");
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
        });
    }

    {
        let scheduler = sockudo_push::PushScheduler::new(
            store.clone(),
            queue.clone(),
            "sockudo-monolith-scheduler",
            fanout_config.clone(),
        );
        let store = store.clone();
        tokio::spawn(async move {
            let group = "sockudo-monolith-scheduler";
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
        });
    }

    start_push_provider_workers(config, store, queue);
}

#[cfg(all(feature = "push", feature = "monolith"))]
fn start_push_provider_workers(
    config: &ServerOptions,
    store: sockudo_push::DynPushStore,
    queue: sockudo_push::DynPushQueue,
) {
    if config.push.webpush_enabled {
        start_webpush_provider_workers(config, queue.clone());
    }

    if config.push.apns_enabled {
        start_apns_provider_workers(config, store.clone(), queue.clone());
    }

    if config.push.fcm_enabled {
        start_fcm_provider_workers(config, store.clone(), queue.clone());
    }

    if config.push.hms_enabled {
        start_hms_provider_workers(config, queue.clone());
    }

    if config.push.wns_enabled {
        start_wns_provider_workers(config, queue);
    }
}
