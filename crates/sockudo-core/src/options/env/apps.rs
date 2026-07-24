use super::*;

pub(super) fn apply(options: &mut ServerOptions) -> Result<(), Box<dyn std::error::Error>> {
    // --- CORS ---
    if let Ok(origins) = std::env::var("CORS_ORIGINS") {
        let parsed: Vec<String> = origins.split(',').map(|s| s.trim().to_string()).collect();
        if let Err(_e) = crate::origin_validation::OriginValidator::validate_patterns(&parsed) {
            warn!(
                env_var = "CORS_ORIGINS",
                reason = "invalid_patterns",
                "cors origins env config rejected, keeping previous value"
            );
        } else {
            options.cors.origin = parsed;
        }
    }
    if let Ok(methods) = std::env::var("CORS_METHODS") {
        options.cors.methods = methods.split(',').map(|s| s.trim().to_string()).collect();
    }
    if let Ok(headers) = std::env::var("CORS_HEADERS") {
        options.cors.allowed_headers = headers.split(',').map(|s| s.trim().to_string()).collect();
    }
    options.cors.credentials = parse_bool_env("CORS_CREDENTIALS", options.cors.credentials);

    // --- Performance Tuning ---
    options.database_pooling.enabled =
        parse_bool_env("DATABASE_POOLING_ENABLED", options.database_pooling.enabled);
    if let Some(min) = parse_env_optional::<u32>("DATABASE_POOL_MIN") {
        options.database_pooling.min = min;
    }
    if let Some(max) = parse_env_optional::<u32>("DATABASE_POOL_MAX") {
        options.database_pooling.max = max;
    }

    if let Some(pool_size) = parse_env_optional::<u32>("DATABASE_CONNECTION_POOL_SIZE") {
        options.database.mysql.connection_pool_size = pool_size;
        options.database.postgres.connection_pool_size = pool_size;
    }
    if let Some(cache_ttl) = parse_env_optional::<u64>("CACHE_TTL_SECONDS") {
        options.app_manager.cache.ttl = cache_ttl;
        options.channel_limits.cache_ttl = cache_ttl;
        options.database.mysql.cache_ttl = cache_ttl;
        options.database.postgres.cache_ttl = cache_ttl;
        options.database.surrealdb.cache_ttl = cache_ttl;
        options.cache.memory.ttl = cache_ttl;
    }
    if let Some(cleanup_interval) = parse_env_optional::<u64>("CACHE_CLEANUP_INTERVAL") {
        options.database.mysql.cache_cleanup_interval = cleanup_interval;
        options.database.postgres.cache_cleanup_interval = cleanup_interval;
        options.cache.memory.cleanup_interval = cleanup_interval;
    }
    if let Some(max_capacity) = parse_env_optional::<u64>("CACHE_MAX_CAPACITY") {
        options.database.mysql.cache_max_capacity = max_capacity;
        options.database.postgres.cache_max_capacity = max_capacity;
        options.database.surrealdb.cache_max_capacity = max_capacity;
        options.cache.memory.max_capacity = max_capacity;
    }

    let skip_inline_apps = parse_bool_env("SOCKUDO_SKIP_INLINE_APPS", false)
        || !parse_bool_env("APP_MANAGER_REGISTER_INLINE_APPS", true);
    if skip_inline_apps {
        let app_count = options.app_manager.array.apps.len();
        options.app_manager.array.apps.clear();
        if app_count > 0 {
            info!(
                app_count = app_count,
                source = "environment",
                "inline apps skipped due to environment override"
            );
        }
    }

    let default_app_id = std::env::var("SOCKUDO_DEFAULT_APP_ID").ok();
    let default_app_key = std::env::var("SOCKUDO_DEFAULT_APP_KEY").ok();
    let default_app_secret = std::env::var("SOCKUDO_DEFAULT_APP_SECRET").ok();
    let default_app_enabled_env = std::env::var("SOCKUDO_DEFAULT_APP_ENABLED").ok();
    let default_app_enabled = parse_bool_env("SOCKUDO_DEFAULT_APP_ENABLED", true);
    let default_app_credentials_configured =
        default_app_id.is_some() || default_app_key.is_some() || default_app_secret.is_some();
    let default_app_env_configured =
        default_app_credentials_configured || default_app_enabled_env.is_some();
    let default_app_should_override_inline = default_app_credentials_configured
        || default_app_enabled_env.is_some_and(|_| !default_app_enabled);

    if default_app_should_override_inline {
        let app_count = options.app_manager.array.apps.len();
        options.app_manager.array.apps.clear();
        if app_count > 0 {
            info!(
                app_count = app_count,
                source = "environment",
                "inline apps replaced by SOCKUDO_DEFAULT_APP_* settings"
            );
        }
    }

    if let (Some(app_id), Some(app_key), Some(app_secret)) =
        (default_app_id, default_app_key, default_app_secret)
        && default_app_enabled
    {
        let default_app = App::from_policy(
            app_id,
            app_key,
            app_secret,
            default_app_enabled,
            crate::app::AppPolicy {
                limits: crate::app::AppLimitsPolicy {
                    max_connections: parse_env::<u32>("SOCKUDO_DEFAULT_APP_MAX_CONNECTIONS", 100),
                    max_backend_events_per_second: Some(parse_env::<u32>(
                        "SOCKUDO_DEFAULT_APP_MAX_BACKEND_EVENTS_PER_SECOND",
                        100,
                    )),
                    max_client_events_per_second: parse_env::<u32>(
                        "SOCKUDO_DEFAULT_APP_MAX_CLIENT_EVENTS_PER_SECOND",
                        100,
                    ),
                    max_read_requests_per_second: Some(parse_env::<u32>(
                        "SOCKUDO_DEFAULT_APP_MAX_READ_REQUESTS_PER_SECOND",
                        100,
                    )),
                    max_presence_members_per_channel: Some(parse_env::<u32>(
                        "SOCKUDO_DEFAULT_APP_MAX_PRESENCE_MEMBERS_PER_CHANNEL",
                        100,
                    )),
                    max_presence_member_size_in_kb: Some(parse_env::<u32>(
                        "SOCKUDO_DEFAULT_APP_MAX_PRESENCE_MEMBER_SIZE_IN_KB",
                        100,
                    )),
                    max_channel_name_length: Some(parse_env::<u32>(
                        "SOCKUDO_DEFAULT_APP_MAX_CHANNEL_NAME_LENGTH",
                        100,
                    )),
                    max_event_channels_at_once: Some(parse_env::<u32>(
                        "SOCKUDO_DEFAULT_APP_MAX_EVENT_CHANNELS_AT_ONCE",
                        100,
                    )),
                    max_event_name_length: Some(parse_env::<u32>(
                        "SOCKUDO_DEFAULT_APP_MAX_EVENT_NAME_LENGTH",
                        100,
                    )),
                    max_event_payload_in_kb: Some(parse_env::<u32>(
                        "SOCKUDO_DEFAULT_APP_MAX_EVENT_PAYLOAD_IN_KB",
                        100,
                    )),
                    max_event_batch_size: Some(parse_env::<u32>(
                        "SOCKUDO_DEFAULT_APP_MAX_EVENT_BATCH_SIZE",
                        100,
                    )),
                    decay_seconds: None,
                    terminate_on_limit: false,
                    message_rate_limit: None,
                },
                features: crate::app::AppFeaturesPolicy {
                    enable_client_messages: std::env::var(
                        "SOCKUDO_DEFAULT_APP_ENABLE_CLIENT_MESSAGES",
                    )
                    .ok()
                    .map(|value| {
                        matches!(
                            value.trim().to_ascii_lowercase().as_str(),
                            "true" | "1" | "yes" | "on"
                        )
                    })
                    .unwrap_or_else(|| parse_bool_env("SOCKUDO_ENABLE_CLIENT_MESSAGES", false)),
                    enable_user_authentication: Some(parse_bool_env(
                        "SOCKUDO_DEFAULT_APP_ENABLE_USER_AUTHENTICATION",
                        false,
                    )),
                    enable_watchlist_events: Some(parse_bool_env(
                        "SOCKUDO_DEFAULT_APP_ENABLE_WATCHLIST_EVENTS",
                        false,
                    )),
                },
                channels: crate::app::AppChannelsPolicy {
                    allowed_origins: {
                        if let Ok(origins_str) =
                            std::env::var("SOCKUDO_DEFAULT_APP_ALLOWED_ORIGINS")
                        {
                            if !origins_str.is_empty() {
                                Some(
                                    origins_str
                                        .split(',')
                                        .map(|s| s.trim().to_string())
                                        .collect(),
                                )
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    },
                    annotations_enabled: Some(parse_bool_env(
                        "SOCKUDO_DEFAULT_APP_ANNOTATIONS_ENABLED",
                        false,
                    )),
                    channel_delta_compression: None,
                    channel_namespaces: None,
                },
                webhooks: None,
                idempotency: None,
                connection_recovery: None,
                history: None,
                presence_history: None,
            },
        );

        options.app_manager.array.apps.push(default_app);
        info!(source = "environment", "default app registered from env");
    } else if default_app_env_configured && !default_app_enabled {
        info!(
            source = "environment",
            "default app registration disabled by env"
        );
    } else if default_app_credentials_configured {
        warn!(
            source = "environment",
            reason = "incomplete_credentials",
            "default app env configured but id, key, and secret were not all provided"
        );
    }

    // Special handling for REDIS_URL
    if let Ok(redis_url_env) = std::env::var("REDIS_URL") {
        info!(source = "environment", "redis url override applied");

        let redis_url_json = sonic_rs::json!(redis_url_env);

        options
            .adapter
            .redis
            .redis_pub_options
            .insert("url".to_string(), redis_url_json.clone());
        options
            .adapter
            .redis
            .redis_sub_options
            .insert("url".to_string(), redis_url_json);

        options.cache.redis.url_override = Some(redis_url_env.clone());
        options.queue.redis.url_override = Some(redis_url_env.clone());
        options.rate_limiter.redis.url_override = Some(redis_url_env);
    }

    Ok(())
}
