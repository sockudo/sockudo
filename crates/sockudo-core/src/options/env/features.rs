use super::*;

pub(super) fn apply(options: &mut ServerOptions) -> Result<(), Box<dyn std::error::Error>> {
    options.versioned_messages.enabled = parse_bool_env(
        "VERSIONED_MESSAGES_ENABLED",
        options.versioned_messages.enabled,
    );
    if let Ok(driver_str) = std::env::var("VERSIONED_MESSAGES_DRIVER") {
        options.versioned_messages.driver = parse_driver_enum(
            driver_str,
            options.versioned_messages.driver.clone(),
            "VersionedMessages Backend",
        );
    }
    if let Ok(driver_str) = std::env::var("PUSH_STORAGE_DRIVER") {
        options.push.storage_driver = parse_driver_enum(
            driver_str,
            options.push.storage_driver.clone(),
            "Push Storage Backend",
        );
    }
    if let Ok(driver_str) = std::env::var("PUSH_QUEUE_DRIVER") {
        options.push.queue_driver = parse_driver_enum(
            driver_str,
            options.push.queue_driver.clone(),
            "Push Queue Backend",
        );
    }
    options.push.allow_memory_drivers = parse_bool_env(
        "PUSH_ALLOW_MEMORY_DRIVERS",
        options.push.allow_memory_drivers,
    );
    options.push.fcm_enabled = parse_bool_env("PUSH_FCM_ENABLED", options.push.fcm_enabled);
    options.push.apns_enabled = parse_bool_env("PUSH_APNS_ENABLED", options.push.apns_enabled);
    options.push.webpush_enabled =
        parse_bool_env("PUSH_WEBPUSH_ENABLED", options.push.webpush_enabled);
    options.push.hms_enabled = parse_bool_env("PUSH_HMS_ENABLED", options.push.hms_enabled);
    options.push.wns_enabled = parse_bool_env("PUSH_WNS_ENABLED", options.push.wns_enabled);
    if let Some(key) = parse_env_optional::<String>("PUSH_CREDENTIAL_ENCRYPTION_KEY") {
        options.push.credential_encryption_key = Some(key);
    }
    options.push.fanout_fast_threshold = parse_env::<u64>(
        "PUSH_FANOUT_FAST_THRESHOLD",
        options.push.fanout_fast_threshold,
    );
    options.push.fanout_shard_size =
        parse_env::<u64>("PUSH_FANOUT_SHARD_SIZE", options.push.fanout_shard_size);
    options.push.fanout_sync_threshold = parse_env::<u64>(
        "PUSH_FANOUT_SYNC_THRESHOLD",
        options.push.fanout_sync_threshold,
    );
    options.push.backpressure_lag_threshold_secs = parse_env::<u64>(
        "PUSH_BACKPRESSURE_LAG_THRESHOLD_SECS",
        options.push.backpressure_lag_threshold_secs,
    );
    options.push.publish_status_ttl_days = parse_env::<u64>(
        "PUSH_PUBLISH_STATUS_TTL_DAYS",
        options.push.publish_status_ttl_days,
    );
    options.push.retry.max_attempts =
        parse_env::<u32>("PUSH_RETRY_MAX_ATTEMPTS", options.push.retry.max_attempts);
    options.push.retry.initial_backoff_ms = parse_env::<u64>(
        "PUSH_RETRY_INITIAL_BACKOFF_MS",
        options.push.retry.initial_backoff_ms,
    );
    options.push.retry.max_backoff_ms = parse_env::<u64>(
        "PUSH_RETRY_MAX_BACKOFF_MS",
        options.push.retry.max_backoff_ms,
    );
    options.push.retry.max_elapsed_secs = parse_env::<u64>(
        "PUSH_RETRY_MAX_ELAPSED_SECS",
        options.push.retry.max_elapsed_secs,
    );
    options.push.retry.jitter = parse_bool_env("PUSH_RETRY_JITTER", options.push.retry.jitter);
    options.push.retry.jitter_ratio_percent = parse_env::<u8>(
        "PUSH_RETRY_JITTER_RATIO_PERCENT",
        options.push.retry.jitter_ratio_percent,
    );
    options.push.retry.respect_retry_after = parse_bool_env(
        "PUSH_RETRY_RESPECT_RETRY_AFTER",
        options.push.retry.respect_retry_after,
    );
    options.push.retry_worker_count =
        parse_env::<u32>("PUSH_RETRY_WORKER_COUNT", options.push.retry_worker_count);
    options.push.dispatch_max_outbound_requests = parse_env::<usize>(
        "PUSH_DISPATCH_MAX_OUTBOUND_REQUESTS",
        options.push.dispatch_max_outbound_requests,
    );
    options.push.circuit_breaker.failure_threshold = parse_env::<u32>(
        "PUSH_FAILURE_THRESHOLD",
        options.push.circuit_breaker.failure_threshold,
    );
    options.push.scheduler_interval_secs = parse_env::<u64>(
        "PUSH_SCHEDULER_INTERVAL_SECS",
        options.push.scheduler_interval_secs,
    );
    options.push.repair_interval_secs = parse_env::<u64>(
        "PUSH_REPAIR_INTERVAL_SECS",
        options.push.repair_interval_secs,
    );
    options.push.repair_min_age_secs =
        parse_env::<u64>("PUSH_REPAIR_MIN_AGE_SECS", options.push.repair_min_age_secs);
    options.push.repair_batch_size =
        parse_env::<usize>("PUSH_REPAIR_BATCH_SIZE", options.push.repair_batch_size);
    options.push.cleanup_interval_secs = parse_env::<u64>(
        "PUSH_CLEANUP_INTERVAL_SECS",
        options.push.cleanup_interval_secs,
    );
    options.push.cleanup_batch_size =
        parse_env::<usize>("PUSH_CLEANUP_BATCH_SIZE", options.push.cleanup_batch_size);
    options.push.cleanup_max_deleted_per_tick = parse_env::<usize>(
        "PUSH_CLEANUP_MAX_DELETED_PER_TICK",
        options.push.cleanup_max_deleted_per_tick,
    );
    options.push.stale_device_max_age_days = parse_env::<u64>(
        "PUSH_STALE_DEVICE_MAX_AGE_DAYS",
        options.push.stale_device_max_age_days,
    );
    options.push.analytics_enabled =
        parse_bool_env("PUSH_ANALYTICS_ENABLED", options.push.analytics_enabled);
    options.push.default_quotas.acceptance_rps = parse_env::<u64>(
        "PUSH_DEFAULT_ACCEPTANCE_RPS",
        options.push.default_quotas.acceptance_rps,
    );
    options.push.default_quotas.delivery_quota_daily = parse_env::<u64>(
        "PUSH_DEFAULT_DELIVERY_QUOTA_DAILY",
        options.push.default_quotas.delivery_quota_daily,
    );
    options.push.default_quotas.fanout_max = parse_env::<u64>(
        "PUSH_DEFAULT_FANOUT_MAX",
        options.push.default_quotas.fanout_max,
    );
    options.push.default_quotas.inflight_max = parse_env::<u64>(
        "PUSH_DEFAULT_INFLIGHT_MAX",
        options.push.default_quotas.inflight_max,
    );
    options.versioned_messages.max_page_size = parse_env::<usize>(
        "VERSIONED_MESSAGES_MAX_PAGE_SIZE",
        options.versioned_messages.max_page_size,
    );
    options.versioned_messages.retention_window_seconds = parse_env::<u64>(
        "VERSIONED_MESSAGES_RETENTION_WINDOW_SECONDS",
        options.versioned_messages.retention_window_seconds,
    );
    options.versioned_messages.purge_interval_seconds = parse_env::<u64>(
        "VERSIONED_MESSAGES_PURGE_INTERVAL_SECONDS",
        options.versioned_messages.purge_interval_seconds,
    );
    options.versioned_messages.purge_batch_size = parse_env::<usize>(
        "VERSIONED_MESSAGES_PURGE_BATCH_SIZE",
        options.versioned_messages.purge_batch_size,
    );
    options.versioned_messages.max_purge_per_tick = parse_env::<usize>(
        "VERSIONED_MESSAGES_MAX_PURGE_PER_TICK",
        options.versioned_messages.max_purge_per_tick,
    );
    options.ai_transport.enabled =
        parse_bool_env("AI_TRANSPORT_ENABLED", options.ai_transport.enabled);
    options.ai_transport.max_accumulated_message_bytes = parse_env::<usize>(
        "AI_TRANSPORT_MAX_ACCUMULATED_MESSAGE_BYTES",
        options.ai_transport.max_accumulated_message_bytes,
    );
    options.ai_transport.max_appends_per_message = parse_env::<usize>(
        "AI_TRANSPORT_MAX_APPENDS_PER_MESSAGE",
        options.ai_transport.max_appends_per_message,
    );
    options.ai_transport.max_open_streaming_messages_per_channel = parse_env::<usize>(
        "AI_TRANSPORT_MAX_OPEN_STREAMING_MESSAGES_PER_CHANNEL",
        options.ai_transport.max_open_streaming_messages_per_channel,
    );
    options.ai_transport.rollup.enabled = parse_bool_env(
        "AI_TRANSPORT_ROLLUP_ENABLED",
        options.ai_transport.rollup.enabled,
    );
    options.ai_transport.rollup.default_window_ms = parse_env::<u64>(
        "AI_TRANSPORT_ROLLUP_DEFAULT_WINDOW_MS",
        options.ai_transport.rollup.default_window_ms,
    );
    options.ai_transport.rollup.min_window_ms = parse_env::<u64>(
        "AI_TRANSPORT_ROLLUP_MIN_WINDOW_MS",
        options.ai_transport.rollup.min_window_ms,
    );
    options.ai_transport.rollup.max_window_ms = parse_env::<u64>(
        "AI_TRANSPORT_ROLLUP_MAX_WINDOW_MS",
        options.ai_transport.rollup.max_window_ms,
    );
    options.ai_transport.rollup.orphan_ttl_ms = parse_env::<u64>(
        "AI_TRANSPORT_ROLLUP_ORPHAN_TTL_MS",
        options.ai_transport.rollup.orphan_ttl_ms,
    );
    options.ai_transport.rollup.wheel_tick_ms = parse_env::<u64>(
        "AI_TRANSPORT_ROLLUP_WHEEL_TICK_MS",
        options.ai_transport.rollup.wheel_tick_ms,
    );
    options.ai_transport.rollup.shards = parse_env::<usize>(
        "AI_TRANSPORT_ROLLUP_SHARDS",
        options.ai_transport.rollup.shards,
    );
    options.annotations.enabled =
        parse_bool_env("ANNOTATIONS_ENABLED", options.annotations.enabled);

    Ok(())
}
