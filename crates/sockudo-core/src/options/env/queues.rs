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
