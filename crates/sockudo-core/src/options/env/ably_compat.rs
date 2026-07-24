use super::*;

pub(super) fn apply(options: &mut ServerOptions) -> Result<(), Box<dyn std::error::Error>> {
    options.ably_compat.enabled =
        parse_bool_env("SOCKUDO_ABLY_COMPAT_ENABLED", options.ably_compat.enabled);
    options.ably_compat.realtime_admission = parse_env::<AblyRealtimeAdmission>(
        "SOCKUDO_ABLY_COMPAT_REALTIME_ADMISSION",
        options.ably_compat.realtime_admission,
    );
    options.ably_compat.attach_timeout_ms = parse_env::<u64>(
        "SOCKUDO_ABLY_COMPAT_ATTACH_TIMEOUT_MS",
        options.ably_compat.attach_timeout_ms,
    );
    options.ably_compat.max_token_ttl_ms = parse_env::<i64>(
        "SOCKUDO_ABLY_COMPAT_MAX_TOKEN_TTL_MS",
        options.ably_compat.max_token_ttl_ms,
    );
    options.ably_compat.token_request_timestamp_skew_ms = parse_env::<i64>(
        "SOCKUDO_ABLY_COMPAT_TOKEN_REQUEST_TIMESTAMP_SKEW_MS",
        options.ably_compat.token_request_timestamp_skew_ms,
    );
    options.ably_compat.nonce_ttl_seconds = parse_env::<u64>(
        "SOCKUDO_ABLY_COMPAT_NONCE_TTL_SECONDS",
        options.ably_compat.nonce_ttl_seconds,
    );
    options.ably_compat.stats_fixture_ingest_enabled = parse_bool_env(
        "SOCKUDO_ABLY_COMPAT_STATS_FIXTURE_INGEST_ENABLED",
        options.ably_compat.stats_fixture_ingest_enabled,
    );
    options.ably_compat.stats_queue_capacity = parse_env::<usize>(
        "SOCKUDO_ABLY_COMPAT_STATS_QUEUE_CAPACITY",
        options.ably_compat.stats_queue_capacity,
    );
    options.ably_compat.stats_flush_interval_ms = parse_env::<u64>(
        "SOCKUDO_ABLY_COMPAT_STATS_FLUSH_INTERVAL_MS",
        options.ably_compat.stats_flush_interval_ms,
    );
    options.ably_compat.stats_retention_seconds = parse_env::<u64>(
        "SOCKUDO_ABLY_COMPAT_STATS_RETENTION_SECONDS",
        options.ably_compat.stats_retention_seconds,
    );
    options.ably_compat.stats_max_scan_entries = parse_env::<usize>(
        "SOCKUDO_ABLY_COMPAT_STATS_MAX_SCAN_ENTRIES",
        options.ably_compat.stats_max_scan_entries,
    );
    options.ably_compat.stats_cas_retries = parse_env::<usize>(
        "SOCKUDO_ABLY_COMPAT_STATS_CAS_RETRIES",
        options.ably_compat.stats_cas_retries,
    );
    if let Ok(raw) = std::env::var("SOCKUDO_ABLY_COMPAT_KEYS_JSON") {
        options.ably_compat.keys = serde_json::from_str(&raw)?;
    }
    options.ably_compat.validate().map_err(Into::into)
}
