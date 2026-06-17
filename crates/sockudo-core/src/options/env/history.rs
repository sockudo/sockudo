use super::*;

pub(super) fn apply(options: &mut ServerOptions) -> Result<(), Box<dyn std::error::Error>> {
    options.history.enabled = parse_bool_env("HISTORY_ENABLED", options.history.enabled);
    options.history.rewind_enabled =
        parse_bool_env("HISTORY_REWIND_ENABLED", options.history.rewind_enabled);
    options.history.retention_window_seconds = parse_env::<u64>(
        "HISTORY_RETENTION_WINDOW_SECONDS",
        options.history.retention_window_seconds,
    );
    options.history.max_page_size =
        parse_env::<usize>("HISTORY_MAX_PAGE_SIZE", options.history.max_page_size);
    options.history.writer_shards =
        parse_env::<usize>("HISTORY_WRITER_SHARDS", options.history.writer_shards);
    options.history.writer_queue_capacity = parse_env::<usize>(
        "HISTORY_WRITER_QUEUE_CAPACITY",
        options.history.writer_queue_capacity,
    );
    if let Ok(backend) = std::env::var("HISTORY_BACKEND") {
        options.history.backend = HistoryBackend::from_str(&backend)?;
    }
    if let Ok(max_messages) = std::env::var("HISTORY_MAX_MESSAGES_PER_CHANNEL") {
        options.history.max_messages_per_channel = Some(
            max_messages
                .parse::<usize>()
                .map_err(|e| format!("Invalid HISTORY_MAX_MESSAGES_PER_CHANNEL: {e}"))?,
        );
    }
    if let Ok(max_bytes) = std::env::var("HISTORY_MAX_BYTES_PER_CHANNEL") {
        options.history.max_bytes_per_channel = Some(
            max_bytes
                .parse::<u64>()
                .map_err(|e| format!("Invalid HISTORY_MAX_BYTES_PER_CHANNEL: {e}"))?,
        );
    }
    if let Ok(table_prefix) = std::env::var("HISTORY_POSTGRES_TABLE_PREFIX") {
        options.history.postgres.table_prefix = table_prefix;
    }
    options.history.postgres.write_timeout_ms = parse_env::<u64>(
        "HISTORY_POSTGRES_WRITE_TIMEOUT_MS",
        options.history.postgres.write_timeout_ms,
    );
    options.history.purge_interval_seconds = parse_env::<u64>(
        "HISTORY_PURGE_INTERVAL_SECONDS",
        options.history.purge_interval_seconds,
    );
    options.history.purge_batch_size =
        parse_env::<usize>("HISTORY_PURGE_BATCH_SIZE", options.history.purge_batch_size);
    options.history.max_purge_per_tick = parse_env::<usize>(
        "HISTORY_MAX_PURGE_PER_TICK",
        options.history.max_purge_per_tick,
    );

    options.presence_history.enabled =
        parse_bool_env("PRESENCE_HISTORY_ENABLED", options.presence_history.enabled);
    options.presence_history.retention_window_seconds = parse_env::<u64>(
        "PRESENCE_HISTORY_RETENTION_WINDOW_SECONDS",
        options.presence_history.retention_window_seconds,
    );
    options.presence_history.max_page_size = parse_env::<usize>(
        "PRESENCE_HISTORY_MAX_PAGE_SIZE",
        options.presence_history.max_page_size,
    );
    if let Ok(max_events) = std::env::var("PRESENCE_HISTORY_MAX_EVENTS_PER_CHANNEL") {
        options.presence_history.max_events_per_channel = Some(
            max_events
                .parse::<usize>()
                .map_err(|e| format!("Invalid PRESENCE_HISTORY_MAX_EVENTS_PER_CHANNEL: {e}"))?,
        );
    }
    if let Ok(max_bytes) = std::env::var("PRESENCE_HISTORY_MAX_BYTES_PER_CHANNEL") {
        options.presence_history.max_bytes_per_channel = Some(
            max_bytes
                .parse::<u64>()
                .map_err(|e| format!("Invalid PRESENCE_HISTORY_MAX_BYTES_PER_CHANNEL: {e}"))?,
        );
    }
    options.idempotency.enabled =
        parse_bool_env("IDEMPOTENCY_ENABLED", options.idempotency.enabled);
    options.idempotency.ttl_seconds =
        parse_env::<u64>("IDEMPOTENCY_TTL_SECONDS", options.idempotency.ttl_seconds);
    options.idempotency.max_key_length = parse_env::<usize>(
        "IDEMPOTENCY_MAX_KEY_LENGTH",
        options.idempotency.max_key_length,
    );

    options.ephemeral.enabled = parse_bool_env("EPHEMERAL_ENABLED", options.ephemeral.enabled);

    options.echo_control.enabled =
        parse_bool_env("ECHO_CONTROL_ENABLED", options.echo_control.enabled);
    options.echo_control.default_echo_messages = parse_bool_env(
        "ECHO_CONTROL_DEFAULT_ECHO_MESSAGES",
        options.echo_control.default_echo_messages,
    );

    options.event_name_filtering.enabled = parse_bool_env(
        "EVENT_NAME_FILTERING_ENABLED",
        options.event_name_filtering.enabled,
    );
    options.event_name_filtering.max_events_per_filter = parse_env::<usize>(
        "EVENT_NAME_FILTERING_MAX_EVENTS_PER_FILTER",
        options.event_name_filtering.max_events_per_filter,
    );
    options.event_name_filtering.max_event_name_length = parse_env::<usize>(
        "EVENT_NAME_FILTERING_MAX_EVENT_NAME_LENGTH",
        options.event_name_filtering.max_event_name_length,
    );

    Ok(())
}
