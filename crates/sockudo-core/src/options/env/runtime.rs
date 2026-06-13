use super::*;

pub(super) fn apply(options: &mut ServerOptions) -> Result<(), Box<dyn std::error::Error>> {
    // --- SSL Configuration ---
    options.ssl.enabled = parse_bool_env("SSL_ENABLED", options.ssl.enabled);
    if let Ok(val) = std::env::var("SSL_CERT_PATH") {
        options.ssl.cert_path = val;
    }
    if let Ok(val) = std::env::var("SSL_KEY_PATH") {
        options.ssl.key_path = val;
    }
    options.ssl.redirect_http = parse_bool_env("SSL_REDIRECT_HTTP", options.ssl.redirect_http);
    if let Some(port) = parse_env_optional::<u16>("SSL_HTTP_PORT") {
        options.ssl.http_port = Some(port);
    }

    // --- Unix Socket Configuration ---
    options.unix_socket.enabled =
        parse_bool_env("UNIX_SOCKET_ENABLED", options.unix_socket.enabled);
    if let Ok(path) = std::env::var("UNIX_SOCKET_PATH") {
        options.unix_socket.path = path;
    }
    if let Ok(mode_str) = std::env::var("UNIX_SOCKET_PERMISSION_MODE") {
        if mode_str.chars().all(|c| c.is_digit(8)) {
            if let Ok(mode) = u32::from_str_radix(&mode_str, 8) {
                if mode <= 0o777 {
                    options.unix_socket.permission_mode = mode;
                } else {
                    warn!(
                        "UNIX_SOCKET_PERMISSION_MODE '{}' exceeds maximum value 777. Using default: {:o}",
                        mode_str, options.unix_socket.permission_mode
                    );
                }
            } else {
                warn!(
                    "Failed to parse UNIX_SOCKET_PERMISSION_MODE '{}' as octal. Using default: {:o}",
                    mode_str, options.unix_socket.permission_mode
                );
            }
        } else {
            warn!(
                "UNIX_SOCKET_PERMISSION_MODE '{}' must contain only octal digits (0-7). Using default: {:o}",
                mode_str, options.unix_socket.permission_mode
            );
        }
    }

    // --- Metrics ---
    if let Ok(driver_str) = std::env::var("METRICS_DRIVER") {
        options.metrics.driver =
            parse_driver_enum(driver_str, options.metrics.driver.clone(), "Metrics");
    }
    options.metrics.enabled = parse_bool_env("METRICS_ENABLED", options.metrics.enabled);
    if let Ok(val) = std::env::var("METRICS_HOST") {
        options.metrics.host = val;
    }
    options.metrics.port = parse_env::<u16>("METRICS_PORT", options.metrics.port);
    if let Ok(val) = std::env::var("METRICS_PROMETHEUS_PREFIX") {
        options.metrics.prometheus.prefix = val;
    }
    options.metrics.tcp_exporter.enabled = parse_bool_env(
        "METRICS_TCP_EXPORTER_ENABLED",
        options.metrics.tcp_exporter.enabled,
    );
    if let Ok(val) = std::env::var("METRICS_TCP_EXPORTER_HOST") {
        options.metrics.tcp_exporter.host = val;
    }
    options.metrics.tcp_exporter.port = parse_env::<u16>(
        "METRICS_TCP_EXPORTER_PORT",
        options.metrics.tcp_exporter.port,
    );
    if let Some(buffer_size) = parse_env_optional::<usize>("METRICS_TCP_EXPORTER_BUFFER_SIZE") {
        options.metrics.tcp_exporter.buffer_size = Some(buffer_size);
    }

    // --- HTTP API ---
    options.http_api.usage_enabled =
        parse_bool_env("HTTP_API_USAGE_ENABLED", options.http_api.usage_enabled);

    // --- Rate Limiter ---
    options.rate_limiter.enabled =
        parse_bool_env("RATE_LIMITER_ENABLED", options.rate_limiter.enabled);
    options.rate_limiter.api_rate_limit.max_requests = parse_env::<u32>(
        "RATE_LIMITER_API_MAX_REQUESTS",
        options.rate_limiter.api_rate_limit.max_requests,
    );
    options.rate_limiter.api_rate_limit.window_seconds = parse_env::<u64>(
        "RATE_LIMITER_API_WINDOW_SECONDS",
        options.rate_limiter.api_rate_limit.window_seconds,
    );
    if let Some(hops) = parse_env_optional::<u32>("RATE_LIMITER_API_TRUST_HOPS") {
        options.rate_limiter.api_rate_limit.trust_hops = Some(hops);
    }
    options.rate_limiter.websocket_rate_limit.max_requests = parse_env::<u32>(
        "RATE_LIMITER_WS_MAX_REQUESTS",
        options.rate_limiter.websocket_rate_limit.max_requests,
    );
    options.rate_limiter.websocket_rate_limit.window_seconds = parse_env::<u64>(
        "RATE_LIMITER_WS_WINDOW_SECONDS",
        options.rate_limiter.websocket_rate_limit.window_seconds,
    );
    if let Some(hops) = parse_env_optional::<u32>("RATE_LIMITER_WS_TRUST_HOPS") {
        options.rate_limiter.websocket_rate_limit.trust_hops = Some(hops);
    }
    options.presence.update_rate_limit_per_member_per_second = parse_env::<u32>(
        "PRESENCE_UPDATE_RATE_LIMIT_PER_MEMBER_PER_SECOND",
        options.presence.update_rate_limit_per_member_per_second,
    );
    options.presence.ungraceful_timeout_seconds = parse_env::<u64>(
        "PRESENCE_UNGRACEFUL_TIMEOUT_SECONDS",
        options.presence.ungraceful_timeout_seconds,
    );
    if let Ok(prefix) = std::env::var("RATE_LIMITER_REDIS_PREFIX") {
        options.rate_limiter.redis.prefix = Some(prefix);
    }

    Ok(())
}
