use sockudo_core::options::{
    HistoryBackend, PushQueueDriver, PushRuleConfig, PushStorageDriver, ServerOptions,
    VersionStoreDriver,
};
use std::fs;
use std::path::PathBuf;
use toml::Value;

#[test]
fn documented_ai_transport_related_defaults_match_code() {
    let docs = load_documented_defaults();
    let options = ServerOptions::default();

    assert_bool(
        &docs,
        "versioned_messages.enabled",
        options.versioned_messages.enabled,
    );
    assert_str(
        &docs,
        "versioned_messages.driver",
        version_store_driver_name(&options.versioned_messages.driver),
    );
    assert_usize(
        &docs,
        "versioned_messages.max_page_size",
        options.versioned_messages.max_page_size,
    );
    assert_u64(
        &docs,
        "versioned_messages.retention_window_seconds",
        options.versioned_messages.retention_window_seconds,
    );
    assert_u64(
        &docs,
        "versioned_messages.purge_interval_seconds",
        options.versioned_messages.purge_interval_seconds,
    );
    assert_usize(
        &docs,
        "versioned_messages.purge_batch_size",
        options.versioned_messages.purge_batch_size,
    );
    assert_usize(
        &docs,
        "versioned_messages.max_purge_per_tick",
        options.versioned_messages.max_purge_per_tick,
    );

    assert_bool(&docs, "history.enabled", options.history.enabled);
    assert_bool(
        &docs,
        "history.rewind_enabled",
        options.history.rewind_enabled,
    );
    assert_str(
        &docs,
        "history.backend",
        history_backend_name(&options.history.backend),
    );
    assert_u64(
        &docs,
        "history.retention_window_seconds",
        options.history.retention_window_seconds,
    );
    assert_usize(
        &docs,
        "history.max_page_size",
        options.history.max_page_size,
    );
    assert_usize(
        &docs,
        "history.writer_shards",
        options.history.writer_shards,
    );
    assert_usize(
        &docs,
        "history.writer_queue_capacity",
        options.history.writer_queue_capacity,
    );
    assert_u64(
        &docs,
        "history.purge_interval_seconds",
        options.history.purge_interval_seconds,
    );
    assert_usize(
        &docs,
        "history.purge_batch_size",
        options.history.purge_batch_size,
    );
    assert_usize(
        &docs,
        "history.max_purge_per_tick",
        options.history.max_purge_per_tick,
    );
    assert_str(
        &docs,
        "history.postgres.table_prefix",
        &options.history.postgres.table_prefix,
    );
    assert_u64(
        &docs,
        "history.postgres.write_timeout_ms",
        options.history.postgres.write_timeout_ms,
    );

    assert_bool(
        &docs,
        "presence_history.enabled",
        options.presence_history.enabled,
    );
    assert_u64(
        &docs,
        "presence_history.retention_window_seconds",
        options.presence_history.retention_window_seconds,
    );
    assert_usize(
        &docs,
        "presence_history.max_page_size",
        options.presence_history.max_page_size,
    );
    assert_bool(&docs, "annotations.enabled", options.annotations.enabled);

    assert_bool(&docs, "ai_transport.enabled", options.ai_transport.enabled);
    assert_usize(
        &docs,
        "ai_transport.max_accumulated_message_bytes",
        options.ai_transport.max_accumulated_message_bytes,
    );
    assert_usize(
        &docs,
        "ai_transport.max_appends_per_message",
        options.ai_transport.max_appends_per_message,
    );
    assert_usize(
        &docs,
        "ai_transport.max_open_streaming_messages_per_channel",
        options.ai_transport.max_open_streaming_messages_per_channel,
    );
    assert_bool(
        &docs,
        "ai_transport.rollup.enabled",
        options.ai_transport.rollup.enabled,
    );
    assert_u64(
        &docs,
        "ai_transport.rollup.default_window_ms",
        options.ai_transport.rollup.default_window_ms,
    );
    assert_u64(
        &docs,
        "ai_transport.rollup.min_window_ms",
        options.ai_transport.rollup.min_window_ms,
    );
    assert_u64(
        &docs,
        "ai_transport.rollup.max_window_ms",
        options.ai_transport.rollup.max_window_ms,
    );
    assert_u64(
        &docs,
        "ai_transport.rollup.orphan_ttl_ms",
        options.ai_transport.rollup.orphan_ttl_ms,
    );
    assert_u64(
        &docs,
        "ai_transport.rollup.wheel_tick_ms",
        options.ai_transport.rollup.wheel_tick_ms,
    );
    assert_usize(
        &docs,
        "ai_transport.rollup.shards",
        options.ai_transport.rollup.shards,
    );

    assert_str(
        &docs,
        "push.storage_driver",
        push_storage_driver_name(&options.push.storage_driver),
    );
    assert_str(
        &docs,
        "push.queue_driver",
        push_queue_driver_name(&options.push.queue_driver),
    );
    assert_bool(&docs, "push.fcm_enabled", options.push.fcm_enabled);
    assert_bool(&docs, "push.apns_enabled", options.push.apns_enabled);
    assert_bool(&docs, "push.webpush_enabled", options.push.webpush_enabled);
    assert_bool(&docs, "push.hms_enabled", options.push.hms_enabled);
    assert_bool(&docs, "push.wns_enabled", options.push.wns_enabled);
    assert_u64(
        &docs,
        "push.fanout_fast_threshold",
        options.push.fanout_fast_threshold,
    );
    assert_u64(
        &docs,
        "push.fanout_shard_size",
        options.push.fanout_shard_size,
    );
    assert_u64(
        &docs,
        "push.fanout_sync_threshold",
        options.push.fanout_sync_threshold,
    );
    assert_u64(
        &docs,
        "push.backpressure_lag_threshold_secs",
        options.push.backpressure_lag_threshold_secs,
    );
    assert_u64(
        &docs,
        "push.publish_status_ttl_days",
        options.push.publish_status_ttl_days,
    );
    assert_u64(
        &docs,
        "push.stale_device_max_age_days",
        options.push.stale_device_max_age_days,
    );
    assert_bool(&docs, "push.dry_run", options.push.dry_run);
    assert_bool(
        &docs,
        "push.analytics_enabled",
        options.push.analytics_enabled,
    );
    assert_u64(
        &docs,
        "push.analytics_retention_days",
        options.push.analytics_retention_days,
    );
    assert_u64(
        &docs,
        "push.scheduler_interval_secs",
        options.push.scheduler_interval_secs,
    );
    assert_u64(
        &docs,
        "push.retry.max_attempts",
        options.push.retry.max_attempts,
    );
    assert_u64(
        &docs,
        "push.retry.initial_backoff_ms",
        options.push.retry.initial_backoff_ms,
    );
    assert_u64(
        &docs,
        "push.retry.max_backoff_ms",
        options.push.retry.max_backoff_ms,
    );
    assert_u64(
        &docs,
        "push.retry.max_elapsed_secs",
        options.push.retry.max_elapsed_secs,
    );
    assert_bool(&docs, "push.retry.jitter", options.push.retry.jitter);
    assert_bool(
        &docs,
        "push.retry.respect_retry_after",
        options.push.retry.respect_retry_after,
    );
    assert_u64(
        &docs,
        "push.circuit_breaker.failure_threshold",
        options.push.circuit_breaker.failure_threshold,
    );
    assert_u64(
        &docs,
        "push.circuit_breaker.cooldown_secs",
        options.push.circuit_breaker.cooldown_secs,
    );
    assert_u64(
        &docs,
        "push.circuit_breaker.half_open_max_inflight",
        options.push.circuit_breaker.half_open_max_inflight,
    );
    assert_u64(
        &docs,
        "push.default_quotas.acceptance_rps",
        options.push.default_quotas.acceptance_rps,
    );
    assert_u64(
        &docs,
        "push.default_quotas.delivery_quota_daily",
        options.push.default_quotas.delivery_quota_daily,
    );
    assert_u64(
        &docs,
        "push.default_quotas.fanout_max",
        options.push.default_quotas.fanout_max,
    );
    assert_u64(
        &docs,
        "push.default_quotas.inflight_max",
        options.push.default_quotas.inflight_max,
    );
    assert_bool(
        &docs,
        "push.payload_redaction.redact_payload",
        options.push.payload_redaction.redact_payload,
    );
    assert_bool(
        &docs,
        "push.payload_redaction.redact_template_data",
        options.push.payload_redaction.redact_template_data,
    );
    assert_bool(
        &docs,
        "push.payload_redaction.redact_provider_overrides",
        options.push.payload_redaction.redact_provider_overrides,
    );
    assert_bool(
        &docs,
        "push.payload_redaction.allow_debug_payload_logging",
        options.push.payload_redaction.allow_debug_payload_logging,
    );

    let documented_rule = &docs["push_rules"]
        .as_array()
        .expect("push_rules must be a TOML array")[0];
    let rule = PushRuleConfig::default();
    assert_eq!(
        documented_rule["enabled"].as_bool(),
        Some(rule.enabled),
        "push_rules.enabled"
    );
    assert_eq!(
        documented_rule["channel_pattern"].as_str(),
        Some(rule.channel_pattern.as_str()),
        "push_rules.channel_pattern"
    );
    assert_eq!(
        documented_rule["event_filter"].as_array().map(Vec::len),
        Some(rule.event_filter.len()),
        "push_rules.event_filter"
    );
    assert_eq!(
        documented_rule["rate_limit_per_second"].as_integer(),
        Some(rule.rate_limit_per_second as i64),
        "push_rules.rate_limit_per_second"
    );
    assert_eq!(
        documented_rule["payload_mapping"]["title_field"].as_str(),
        Some(rule.payload_mapping.title_field.as_str()),
        "push_rules.payload_mapping.title_field"
    );
    assert_eq!(
        documented_rule["payload_mapping"]["body_field"].as_str(),
        Some(rule.payload_mapping.body_field.as_str()),
        "push_rules.payload_mapping.body_field"
    );
    assert_eq!(
        documented_rule["payload_mapping"]["template_data_field"].as_str(),
        Some(rule.payload_mapping.template_data_field.as_str()),
        "push_rules.payload_mapping.template_data_field"
    );
    assert_eq!(
        documented_rule["payload_mapping"]["include_remaining_fields"].as_bool(),
        Some(rule.payload_mapping.include_remaining_fields),
        "push_rules.payload_mapping.include_remaining_fields"
    );
    assert!(
        options.push_rules.is_empty(),
        "server default must not install push rules"
    );
}

fn load_documented_defaults() -> Value {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("../../docs/content/docs/reference/configuration.mdx");
    let text = fs::read_to_string(path).expect("configuration docs must be readable");
    let marker = "```toml doc-defaults";
    let start = text
        .find(marker)
        .expect("configuration docs must contain doc-defaults block")
        + marker.len();
    let rest = &text[start..];
    let end = rest
        .find("```")
        .expect("doc-defaults block must be terminated");
    toml::from_str(&rest[..end]).expect("doc-defaults block must be valid TOML")
}

fn assert_bool(root: &Value, path: &str, expected: bool) {
    assert_eq!(value(root, path).as_bool(), Some(expected), "{path}");
}

fn assert_str(root: &Value, path: &str, expected: &str) {
    assert_eq!(value(root, path).as_str(), Some(expected), "{path}");
}

fn assert_u64(root: &Value, path: &str, expected: impl Into<u64>) {
    assert_eq!(
        value(root, path).as_integer(),
        Some(expected.into() as i64),
        "{path}"
    );
}

fn assert_usize(root: &Value, path: &str, expected: usize) {
    assert_eq!(
        value(root, path).as_integer(),
        Some(expected as i64),
        "{path}"
    );
}

fn value<'a>(root: &'a Value, path: &str) -> &'a Value {
    path.split('.').fold(root, |value, segment| {
        value
            .get(segment)
            .unwrap_or_else(|| panic!("missing {path}"))
    })
}

fn version_store_driver_name(driver: &VersionStoreDriver) -> &'static str {
    match driver {
        VersionStoreDriver::Memory => "memory",
        VersionStoreDriver::Postgres => "postgres",
        VersionStoreDriver::Mysql => "mysql",
        VersionStoreDriver::DynamoDb => "dynamodb",
        VersionStoreDriver::ScyllaDb => "scylladb",
        VersionStoreDriver::SurrealDb => "surrealdb",
    }
}

fn history_backend_name(backend: &HistoryBackend) -> &'static str {
    match backend {
        HistoryBackend::Postgres => "postgres",
        HistoryBackend::Mysql => "mysql",
        HistoryBackend::DynamoDb => "dynamodb",
        HistoryBackend::SurrealDb => "surrealdb",
        HistoryBackend::ScyllaDb => "scylladb",
        HistoryBackend::Memory => "memory",
    }
}

fn push_storage_driver_name(driver: &PushStorageDriver) -> &'static str {
    match driver {
        PushStorageDriver::Memory => "memory",
        PushStorageDriver::Postgres => "postgres",
        PushStorageDriver::Mysql => "mysql",
        PushStorageDriver::DynamoDb => "dynamodb",
        PushStorageDriver::SurrealDb => "surrealdb",
        PushStorageDriver::ScyllaDb => "scylladb",
    }
}

fn push_queue_driver_name(driver: &PushQueueDriver) -> &'static str {
    match driver {
        PushQueueDriver::Memory => "memory",
        PushQueueDriver::Redis => "redis",
        PushQueueDriver::RedisCluster => "redis-cluster",
        PushQueueDriver::Nats => "nats",
        PushQueueDriver::Pulsar => "pulsar",
        PushQueueDriver::RabbitMq => "rabbitmq",
        PushQueueDriver::GooglePubsub => "google-pubsub",
        PushQueueDriver::Kafka => "kafka",
        PushQueueDriver::Iggy => "iggy",
        PushQueueDriver::Sqs => "sqs",
        PushQueueDriver::Sns => "sns",
    }
}
