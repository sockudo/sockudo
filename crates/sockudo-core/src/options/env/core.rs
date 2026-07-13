use super::*;

pub(super) fn apply(options: &mut ServerOptions) -> Result<(), Box<dyn std::error::Error>> {
    // --- General Settings ---
    if let Ok(mode) = std::env::var("ENVIRONMENT") {
        options.mode = mode;
    }
    options.debug = parse_bool_env("DEBUG_MODE", options.debug);
    if parse_bool_env("DEBUG", false) {
        options.debug = true;
        info!("DEBUG environment variable forces debug mode ON");
    }

    options.activity_timeout = parse_env::<u64>("ACTIVITY_TIMEOUT", options.activity_timeout);

    if let Ok(host) = std::env::var("HOST") {
        options.host = host;
    }
    options.port = parse_env::<u16>("PORT", options.port);
    options.shutdown_grace_period =
        parse_env::<u64>("SHUTDOWN_GRACE_PERIOD", options.shutdown_grace_period);
    options.user_authentication_timeout = parse_env::<u64>(
        "USER_AUTHENTICATION_TIMEOUT",
        options.user_authentication_timeout,
    );
    options.websocket_max_payload_kb =
        parse_env::<u32>("WEBSOCKET_MAX_PAYLOAD_KB", options.websocket_max_payload_kb);
    options.max_connections = parse_env::<u32>("SOCKUDO_MAX_CONNECTIONS", options.max_connections);
    if let Ok(id) = std::env::var("INSTANCE_PROCESS_ID") {
        options.instance.process_id = id;
    }

    Ok(())
}
