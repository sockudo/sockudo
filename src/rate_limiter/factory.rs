// src/rate_limiter/factory.rs
use crate::rate_limiter::RateLimiter;
use std::sync::Arc;
use tracing::{error, info, warn};
// Use the type-safe RedisConfig and CacheDriver from options.rs
use crate::error::Result;

use crate::options::{
    CacheDriver, RateLimiterConfig, RedisConfig as RateLimiterRedisBackendConfig, RedisConnection,
};
use crate::rate_limiter::memory_limiter::MemoryRateLimiter;
use crate::rate_limiter::redis_limiter::RedisRateLimiter;

pub struct RateLimiterFactory;

impl RateLimiterFactory {
    pub async fn create(
        config: &RateLimiterConfig,
        global_redis_conn_details: &RedisConnection, // For Redis URL/nodes if not in RateLimiterConfig.redis.url_override
        debug_enabled: bool,
    ) -> Result<Arc<dyn RateLimiter + Send + Sync>> {
        if !config.enabled {
            info!(
                "{}",
                "HTTP API Rate limiting is globally disabled. Returning a permissive limiter."
                    .to_string(),
            );
            return Ok(Arc::new(MemoryRateLimiter::new(u32::MAX, 1))); // Allows all
        }

        info!(
            "{}",
            format!(
                "Initializing HTTP API RateLimiter with driver: {:?}",
                config.driver
            )
        );
        match config.driver {
            CacheDriver::Redis => {
                // Assuming RateLimiter uses CacheDriver enum for its backend
                // Determine if this Redis instance should be cluster or standalone
                // based on the specific RateLimiterConfig.redis.cluster_mode
                if config.redis.cluster_mode {
                    info!(
                        "{}",
                        "RateLimiter: Using Redis Cluster backend.".to_string()
                    );
                    if global_redis_conn_details.cluster_nodes.is_empty() {
                        error!("{}", "RateLimiter: Redis cluster mode enabled, but no cluster_nodes configured.".to_string());
                        return Err(crate::error::Error::ConfigurationError(
                            "RateLimiter: Redis cluster nodes not configured.".to_string(),
                        ));
                    }
                    let nodes: Vec<String> = global_redis_conn_details
                        .cluster_nodes
                        .iter()
                        .map(|node| format!("redis://{}:{}", node.host, node.port))
                        .collect();

                    let prefix = config.redis.prefix.clone().unwrap_or_else(|| {
                        global_redis_conn_details.key_prefix.clone() + "rl_http:"
                    });

                    // Here you would instantiate your RedisClusterRateLimiter
                    // For now, let's assume it's not implemented and fall back or error
                    warn!("{}", "RedisClusterRateLimiter not yet implemented. Falling back to MemoryRateLimiter for HTTP API.".to_string());
                    let limiter = MemoryRateLimiter::new(
                        config.api_rate_limit.max_requests,
                        config.api_rate_limit.window_seconds,
                    );
                    Ok(Arc::new(limiter))
                    // Example if it were implemented:
                    // let limiter = RedisClusterRateLimiter::new(nodes, prefix, config.api_rate_limit.max_requests, config.api_rate_limit.window_seconds).await?;
                    // Ok(Arc::new(limiter))
                } else {
                    info!(
                        "{}",
                        "RateLimiter: Using standalone Redis backend.".to_string()
                    );
                    let redis_url = config.redis.url_override.clone().unwrap_or_else(|| {
                        format!(
                            "redis://{}:{}",
                            global_redis_conn_details.host, global_redis_conn_details.port
                        )
                    });

                    let prefix = config.redis.prefix.clone().unwrap_or_else(|| {
                        global_redis_conn_details.key_prefix.clone() + "rl_http:"
                    });

                    let client = redis::Client::open(redis_url.as_str()).map_err(|e| {
                        crate::error::Error::RedisError(format!(
                            "Failed to create Redis client for rate limiter: {}",
                            e
                        ))
                    })?;

                    let limiter = RedisRateLimiter::new(
                        client,
                        prefix,
                        config.api_rate_limit.max_requests,
                        config.api_rate_limit.window_seconds,
                    )
                    .await?;
                    Ok(Arc::new(limiter))
                }
            }
            CacheDriver::RedisCluster => {
                // Explicit RedisCluster driver for RateLimiter backend
                info!(
                    "{}",
                    "RateLimiter: Using Redis Cluster backend (explicitly selected).".to_string(),
                );
                if global_redis_conn_details.cluster_nodes.is_empty() {
                    error!("{}", "RateLimiter: Redis cluster driver selected, but no cluster_nodes configured.".to_string());
                    return Err(crate::error::Error::ConfigurationError("RateLimiter: Redis cluster nodes not configured for explicit cluster driver.".to_string()));
                }
                // As above, if RedisClusterRateLimiter is implemented:
                warn!("{}", "RedisClusterRateLimiter not yet implemented. Falling back to MemoryRateLimiter for HTTP API.".to_string());
                let limiter = MemoryRateLimiter::new(
                    config.api_rate_limit.max_requests,
                    config.api_rate_limit.window_seconds,
                );
                Ok(Arc::new(limiter))
            }
            CacheDriver::Memory | _ => {
                // Default to memory for rate limiter if driver is "memory" or unknown
                info!("{}", "Using memory rate limiter for HTTP API.".to_string());
                let limiter = MemoryRateLimiter::new(
                    config.api_rate_limit.max_requests,
                    config.api_rate_limit.window_seconds,
                );
                Ok(Arc::new(limiter))
            }
        }
    }
}
