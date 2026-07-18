use super::*;

pub(super) fn apply(options: &mut ServerOptions) -> Result<(), Box<dyn std::error::Error>> {
    // --- Logging Configuration ---
    let has_colors_env = std::env::var("LOG_COLORS_ENABLED").is_ok();
    let has_target_env = std::env::var("LOG_INCLUDE_TARGET").is_ok();
    if has_colors_env || has_target_env {
        let logging_config = options.logging.get_or_insert_with(Default::default);
        if has_colors_env {
            logging_config.colors_enabled =
                parse_bool_env("LOG_COLORS_ENABLED", logging_config.colors_enabled);
        }
        if has_target_env {
            logging_config.include_target =
                parse_bool_env("LOG_INCLUDE_TARGET", logging_config.include_target);
        }
    }

    // --- Cleanup Configuration ---
    options.cleanup.async_enabled =
        parse_bool_env("CLEANUP_ASYNC_ENABLED", options.cleanup.async_enabled);
    options.cleanup.fallback_to_sync =
        parse_bool_env("CLEANUP_FALLBACK_TO_SYNC", options.cleanup.fallback_to_sync);
    options.cleanup.queue_buffer_size = parse_env::<usize>(
        "CLEANUP_QUEUE_BUFFER_SIZE",
        options.cleanup.queue_buffer_size,
    );
    options.cleanup.batch_size =
        parse_env::<usize>("CLEANUP_BATCH_SIZE", options.cleanup.batch_size);
    options.cleanup.batch_timeout_ms =
        parse_env::<u64>("CLEANUP_BATCH_TIMEOUT_MS", options.cleanup.batch_timeout_ms);
    if let Ok(worker_threads_str) = std::env::var("CLEANUP_WORKER_THREADS") {
        options.cleanup.worker_threads = if worker_threads_str.to_lowercase() == "auto" {
            WorkerThreadsConfig::Auto
        } else if let Ok(n) = worker_threads_str.parse::<usize>() {
            WorkerThreadsConfig::Fixed(n)
        } else {
            warn!(
                env_var = "CLEANUP_WORKER_THREADS",
                reason = "invalid_value",
                "env config parse failed, keeping current setting"
            );
            options.cleanup.worker_threads.clone()
        };
    }
    options.cleanup.max_retry_attempts = parse_env::<u32>(
        "CLEANUP_MAX_RETRY_ATTEMPTS",
        options.cleanup.max_retry_attempts,
    );

    // Cluster health configuration
    options.cluster_health.enabled =
        parse_bool_env("CLUSTER_HEALTH_ENABLED", options.cluster_health.enabled);
    options.cluster_health.heartbeat_interval_ms = parse_env::<u64>(
        "CLUSTER_HEALTH_HEARTBEAT_INTERVAL",
        options.cluster_health.heartbeat_interval_ms,
    );
    options.cluster_health.node_timeout_ms = parse_env::<u64>(
        "CLUSTER_HEALTH_NODE_TIMEOUT",
        options.cluster_health.node_timeout_ms,
    );
    options.cluster_health.cleanup_interval_ms = parse_env::<u64>(
        "CLUSTER_HEALTH_CLEANUP_INTERVAL",
        options.cluster_health.cleanup_interval_ms,
    );

    // Health check endpoint timeout
    options.health_check_timeout_ms =
        parse_env::<u64>("HEALTH_CHECK_TIMEOUT_MS", options.health_check_timeout_ms);

    // Tag filtering configuration
    options.tag_filtering.enabled =
        parse_bool_env("TAG_FILTERING_ENABLED", options.tag_filtering.enabled);

    // WebSocket buffer configuration
    if let Ok(val) = std::env::var("WEBSOCKET_MAX_MESSAGES") {
        if val.to_lowercase() == "none" || val == "0" {
            options.websocket.max_messages = None;
        } else if let Ok(n) = val.parse::<usize>() {
            options.websocket.max_messages = Some(n);
        }
    }
    if let Ok(val) = std::env::var("WEBSOCKET_MAX_BYTES") {
        if val.to_lowercase() == "none" || val == "0" {
            options.websocket.max_bytes = None;
        } else if let Ok(n) = val.parse::<usize>() {
            options.websocket.max_bytes = Some(n);
        }
    }
    options.websocket.disconnect_on_buffer_full = parse_bool_env(
        "WEBSOCKET_DISCONNECT_ON_BUFFER_FULL",
        options.websocket.disconnect_on_buffer_full,
    );
    options.websocket.max_message_size = parse_env::<usize>(
        "WEBSOCKET_MAX_MESSAGE_SIZE",
        options.websocket.max_message_size,
    );
    options.websocket.max_frame_size =
        parse_env::<usize>("WEBSOCKET_MAX_FRAME_SIZE", options.websocket.max_frame_size);
    options.websocket.write_buffer_size = parse_env::<usize>(
        "WEBSOCKET_WRITE_BUFFER_SIZE",
        options.websocket.write_buffer_size,
    );
    options.websocket.max_backpressure = parse_env::<usize>(
        "WEBSOCKET_MAX_BACKPRESSURE",
        options.websocket.max_backpressure,
    );
    options.websocket.auto_ping =
        parse_bool_env("WEBSOCKET_AUTO_PING", options.websocket.auto_ping);
    options.websocket.ping_interval =
        parse_env::<u32>("WEBSOCKET_PING_INTERVAL", options.websocket.ping_interval);
    options.websocket.idle_timeout =
        parse_env::<u32>("WEBSOCKET_IDLE_TIMEOUT", options.websocket.idle_timeout);
    if let Ok(mode) = std::env::var("WEBSOCKET_COMPRESSION") {
        options.websocket.compression = mode;
    }

    // Connection recovery (includes serial + message_id + replay buffer)
    options.connection_recovery.enabled = parse_bool_env(
        "CONNECTION_RECOVERY_ENABLED",
        options.connection_recovery.enabled,
    );
    options.connection_recovery.buffer_ttl_seconds = parse_env::<u64>(
        "CONNECTION_RECOVERY_BUFFER_TTL",
        options.connection_recovery.buffer_ttl_seconds,
    );
    options.connection_recovery.max_buffer_size = parse_env::<usize>(
        "CONNECTION_RECOVERY_MAX_BUFFER_SIZE",
        options.connection_recovery.max_buffer_size,
    );

    Ok(())
}
