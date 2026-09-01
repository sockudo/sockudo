use super::*;

fn optional_string_env(name: &str) -> Option<Option<String>> {
    std::env::var(name).ok().map(|value| {
        if value.trim().is_empty() {
            None
        } else {
            Some(value)
        }
    })
}

fn apply_opentelemetry_resource_attributes(options: &mut ServerOptions) {
    let Ok(value) = std::env::var("SOCKUDO_OTEL_RESOURCE_ATTRIBUTES") else {
        return;
    };

    let mut attributes = std::collections::BTreeMap::new();
    let mut invalid_count = 0_u64;
    for entry in value.split(',') {
        let Some((key, value)) = entry.split_once('=') else {
            invalid_count += 1;
            continue;
        };
        let key = key.trim();
        if key.is_empty() {
            invalid_count += 1;
            continue;
        }
        attributes.insert(key.to_string(), value.trim().to_string());
    }

    if invalid_count > 0 {
        warn!(
            env_var = "SOCKUDO_OTEL_RESOURCE_ATTRIBUTES",
            invalid_count, "env config contains invalid resource attributes"
        );
    }
    options.opentelemetry.resource_attributes = attributes;
}

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
                        env_var = "UNIX_SOCKET_PERMISSION_MODE",
                        reason = "exceeds_max_value",
                        "env config parse failed, using default"
                    );
                }
            } else {
                warn!(
                    env_var = "UNIX_SOCKET_PERMISSION_MODE",
                    reason = "not_valid_octal",
                    "env config parse failed, using default"
                );
            }
        } else {
            warn!(
                env_var = "UNIX_SOCKET_PERMISSION_MODE",
                reason = "non_octal_digits",
                "env config parse failed, using default"
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

    // --- OpenTelemetry ---
    options.opentelemetry.enabled =
        parse_bool_env("SOCKUDO_OTEL_ENABLED", options.opentelemetry.enabled);
    options.opentelemetry.traces_enabled = parse_bool_env(
        "SOCKUDO_OTEL_TRACES_ENABLED",
        options.opentelemetry.traces_enabled,
    );
    options.opentelemetry.metrics_enabled = parse_bool_env(
        "SOCKUDO_OTEL_METRICS_ENABLED",
        options.opentelemetry.metrics_enabled,
    );
    options.opentelemetry.logs_enabled = parse_bool_env(
        "SOCKUDO_OTEL_LOGS_ENABLED",
        options.opentelemetry.logs_enabled,
    );
    if let Ok(value) = std::env::var("SOCKUDO_OTEL_SERVICE_NAME") {
        options.opentelemetry.service_name = value;
    }
    if let Some(value) = optional_string_env("SOCKUDO_OTEL_SERVICE_NAMESPACE") {
        options.opentelemetry.service_namespace = value;
    }
    if let Some(value) = optional_string_env("SOCKUDO_OTEL_DEPLOYMENT_ENVIRONMENT") {
        options.opentelemetry.deployment_environment = value;
    }
    apply_opentelemetry_resource_attributes(options);
    if let Some(value) = optional_string_env("SOCKUDO_OTEL_ENDPOINT") {
        options.opentelemetry.endpoint = value;
    }
    options.opentelemetry.export_timeout_ms = parse_env::<u64>(
        "SOCKUDO_OTEL_EXPORT_TIMEOUT_MS",
        options.opentelemetry.export_timeout_ms,
    );
    options.opentelemetry.batch_scheduled_delay_ms = parse_env::<u64>(
        "SOCKUDO_OTEL_BATCH_SCHEDULED_DELAY_MS",
        options.opentelemetry.batch_scheduled_delay_ms,
    );
    options.opentelemetry.batch_max_queue_size = parse_env::<usize>(
        "SOCKUDO_OTEL_BATCH_MAX_QUEUE_SIZE",
        options.opentelemetry.batch_max_queue_size,
    );
    options.opentelemetry.batch_max_export_batch_size = parse_env::<usize>(
        "SOCKUDO_OTEL_BATCH_MAX_EXPORT_BATCH_SIZE",
        options.opentelemetry.batch_max_export_batch_size,
    );
    options.opentelemetry.metric_export_interval_ms = parse_env::<u64>(
        "SOCKUDO_OTEL_METRIC_EXPORT_INTERVAL_MS",
        options.opentelemetry.metric_export_interval_ms,
    );
    options.opentelemetry.propagation_trace_context = parse_bool_env(
        "SOCKUDO_OTEL_PROPAGATION_TRACE_CONTEXT",
        options.opentelemetry.propagation_trace_context,
    );
    options.opentelemetry.propagation_baggage = parse_bool_env(
        "SOCKUDO_OTEL_PROPAGATION_BAGGAGE",
        options.opentelemetry.propagation_baggage,
    );

    // --- HTTP API ---
    options.http_api.usage_enabled =
        parse_bool_env("HTTP_API_USAGE_ENABLED", options.http_api.usage_enabled);
    options.http_api.accept_traffic.enabled = parse_bool_env(
        "HTTP_API_ACCEPT_TRAFFIC_ENABLED",
        options.http_api.accept_traffic.enabled,
    );
    options.http_api.accept_traffic.memory_threshold = parse_env::<f64>(
        "HTTP_API_ACCEPT_TRAFFIC_MEMORY_THRESHOLD",
        options.http_api.accept_traffic.memory_threshold,
    );
    if let Some(limit_bytes) =
        parse_env_optional::<u64>("HTTP_API_ACCEPT_TRAFFIC_MEMORY_LIMIT_BYTES")
    {
        options.http_api.accept_traffic.memory_limit_bytes = Some(limit_bytes);
    }
    options.http_api.accept_traffic.sample_interval_ms = parse_env::<u64>(
        "HTTP_API_ACCEPT_TRAFFIC_SAMPLE_INTERVAL_MS",
        options.http_api.accept_traffic.sample_interval_ms,
    );

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
    options.presence.v2_ungraceful_timeout_seconds = parse_env::<u64>(
        "PRESENCE_V2_UNGRACEFUL_TIMEOUT_SECONDS",
        options.presence.v2_ungraceful_timeout_seconds,
    );
    if let Ok(prefix) = std::env::var("RATE_LIMITER_REDIS_PREFIX") {
        options.rate_limiter.redis.prefix = Some(prefix);
    }

    Ok(())
}
