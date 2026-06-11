use super::*;

pub(super) fn apply(options: &mut ServerOptions) -> Result<(), Box<dyn std::error::Error>> {
    // --- Driver Configuration ---
    if let Ok(driver_str) = std::env::var("ADAPTER_DRIVER") {
        options.adapter.driver =
            parse_driver_enum(driver_str, options.adapter.driver.clone(), "Adapter");
    }
    options.adapter.buffer_multiplier_per_cpu = parse_env::<usize>(
        "ADAPTER_BUFFER_MULTIPLIER_PER_CPU",
        options.adapter.buffer_multiplier_per_cpu,
    );
    options.adapter.enable_socket_counting = parse_env::<bool>(
        "ADAPTER_ENABLE_SOCKET_COUNTING",
        options.adapter.enable_socket_counting,
    );
    options.adapter.fallback_to_local = parse_env::<bool>(
        "ADAPTER_FALLBACK_TO_LOCAL",
        options.adapter.fallback_to_local,
    );
    if let Ok(driver_str) = std::env::var("CACHE_DRIVER") {
        options.cache.driver = parse_driver_enum(driver_str, options.cache.driver.clone(), "Cache");
    }
    if let Ok(driver_str) = std::env::var("QUEUE_DRIVER") {
        options.queue.driver = parse_driver_enum(driver_str, options.queue.driver.clone(), "Queue");
    }
    if let Ok(driver_str) = std::env::var("APP_MANAGER_DRIVER") {
        options.app_manager.driver =
            parse_driver_enum(driver_str, options.app_manager.driver.clone(), "AppManager");
    }
    if let Ok(driver_str) = std::env::var("RATE_LIMITER_DRIVER") {
        options.rate_limiter.driver = parse_driver_enum(
            driver_str,
            options.rate_limiter.driver.clone(),
            "RateLimiter Backend",
        );
    }

    Ok(())
}
