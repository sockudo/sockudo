use super::*;

pub(super) fn apply(options: &mut ServerOptions) -> Result<(), Box<dyn std::error::Error>> {
    // --- Queue: Redis ---
    options.queue.redis.concurrency =
        parse_env::<u32>("QUEUE_REDIS_CONCURRENCY", options.queue.redis.concurrency);
    options.queue.redis.response_timeout_ms = parse_env::<u64>(
        "QUEUE_REDIS_RESPONSE_TIMEOUT_MS",
        options.queue.redis.response_timeout_ms,
    );
    if let Ok(prefix) = std::env::var("QUEUE_REDIS_PREFIX") {
        options.queue.redis.prefix = Some(prefix);
    }

    // --- Queue: reliability ---
    let reliability = &mut options.queue.reliability;
    reliability.max_attempts = parse_env::<u32>("QUEUE_MAX_ATTEMPTS", reliability.max_attempts);
    reliability.retry_base_delay_ms =
        parse_env::<u64>("QUEUE_RETRY_BASE_DELAY_MS", reliability.retry_base_delay_ms);
    reliability.retry_max_delay_ms =
        parse_env::<u64>("QUEUE_RETRY_MAX_DELAY_MS", reliability.retry_max_delay_ms);
    reliability.retry_jitter = parse_env::<f64>("QUEUE_RETRY_JITTER", reliability.retry_jitter);
    reliability.lease_duration_ms =
        parse_env::<u64>("QUEUE_LEASE_DURATION_MS", reliability.lease_duration_ms);
    reliability.lease_renew_interval_ms = parse_env::<u64>(
        "QUEUE_LEASE_RENEW_INTERVAL_MS",
        reliability.lease_renew_interval_ms,
    );
    reliability.stalled_batch_size =
        parse_env::<u32>("QUEUE_STALLED_BATCH_SIZE", reliability.stalled_batch_size);
    reliability.worker_poll_interval_ms = parse_env::<u64>(
        "QUEUE_WORKER_POLL_INTERVAL_MS",
        reliability.worker_poll_interval_ms,
    );
    reliability.worker_prefetch =
        parse_env::<usize>("QUEUE_WORKER_PREFETCH", reliability.worker_prefetch);
    reliability.shutdown_timeout_ms =
        parse_env::<u64>("QUEUE_SHUTDOWN_TIMEOUT_MS", reliability.shutdown_timeout_ms);
    reliability.completed_retention =
        parse_env::<u32>("QUEUE_COMPLETED_RETENTION", reliability.completed_retention);
    reliability.failed_retention =
        parse_env::<u32>("QUEUE_FAILED_RETENTION", reliability.failed_retention);
    reliability.event_retention =
        parse_env::<u32>("QUEUE_EVENT_RETENTION", reliability.event_retention);
    reliability.deduplication_ttl_ms = parse_env::<u64>(
        "QUEUE_DEDUPLICATION_TTL_MS",
        reliability.deduplication_ttl_ms,
    );
    reliability.memory_capacity =
        parse_env::<usize>("QUEUE_MEMORY_CAPACITY", reliability.memory_capacity);
    reliability.max_batch_size =
        parse_env::<usize>("QUEUE_MAX_BATCH_SIZE", reliability.max_batch_size);

    // --- Queue: SQS ---
    if let Ok(region) = std::env::var("QUEUE_SQS_REGION") {
        options.queue.sqs.region = region;
    }
    options.queue.sqs.visibility_timeout = parse_env::<i32>(
        "QUEUE_SQS_VISIBILITY_TIMEOUT",
        options.queue.sqs.visibility_timeout,
    );
    options.queue.sqs.max_messages =
        parse_env::<i32>("QUEUE_SQS_MAX_MESSAGES", options.queue.sqs.max_messages);
    options.queue.sqs.wait_time_seconds = parse_env::<i32>(
        "QUEUE_SQS_WAIT_TIME_SECONDS",
        options.queue.sqs.wait_time_seconds,
    );
    options.queue.sqs.concurrency =
        parse_env::<u32>("QUEUE_SQS_CONCURRENCY", options.queue.sqs.concurrency);
    options.queue.sqs.fifo = parse_bool_env("QUEUE_SQS_FIFO", options.queue.sqs.fifo);
    if let Ok(endpoint) = std::env::var("QUEUE_SQS_ENDPOINT_URL") {
        options.queue.sqs.endpoint_url = Some(endpoint);
    }

    // --- Queue: SNS ---
    if let Ok(region) = std::env::var("QUEUE_SNS_REGION") {
        options.queue.sns.region = region;
    }
    if let Ok(topic_arn) = std::env::var("QUEUE_SNS_TOPIC_ARN") {
        options.queue.sns.topic_arn = topic_arn;
    }
    if let Ok(endpoint) = std::env::var("QUEUE_SNS_ENDPOINT_URL") {
        options.queue.sns.endpoint_url = Some(endpoint);
    }

    // --- Webhooks ---
    options.webhooks.batching.enabled = parse_bool_env(
        "WEBHOOK_BATCHING_ENABLED",
        options.webhooks.batching.enabled,
    );
    options.webhooks.batching.duration = parse_env::<u64>(
        "WEBHOOK_BATCHING_DURATION",
        options.webhooks.batching.duration,
    );
    options.webhooks.batching.size =
        parse_env::<usize>("WEBHOOK_BATCHING_SIZE", options.webhooks.batching.size);

    Ok(())
}
