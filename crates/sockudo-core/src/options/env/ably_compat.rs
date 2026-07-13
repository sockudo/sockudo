use super::*;

pub(super) fn apply(options: &mut ServerOptions) -> Result<(), Box<dyn std::error::Error>> {
    options.ably_compat.enabled =
        parse_bool_env("SOCKUDO_ABLY_COMPAT_ENABLED", options.ably_compat.enabled);
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
    if let Ok(raw) = std::env::var("SOCKUDO_ABLY_COMPAT_KEYS_JSON") {
        options.ably_compat.keys = serde_json::from_str(&raw)?;
    }
    options.ably_compat.validate().map_err(Into::into)
}
