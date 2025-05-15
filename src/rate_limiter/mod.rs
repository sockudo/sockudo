// src/rate_limiter/mod.rs
pub mod memory_limiter;
pub mod middleware;
pub mod redis_limiter;
pub mod redis_cluster_limiter;

use crate::error::Result;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use redis::cluster::ClusterClient;
use crate::rate_limiter::redis_cluster_limiter::RedisClusterRateLimiter;

/// Configuration for rate limiters
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum number of requests
    pub max_requests: u32,
    /// Time window in seconds
    pub window_secs: u64,
    /// Optional identifier for the limiter (e.g., "api_calls", "websocket_connects")
    pub identifier: Option<String>,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests: 60,
            window_secs: 60, // 60 requests per minute by default
            identifier: None,
        }
    }
}

/// Rate limit check result
#[derive(Debug, Clone)]
pub struct RateLimitResult {
    /// Whether the request should be allowed
    pub allowed: bool,
    /// Number of remaining requests in the current window
    pub remaining: u32,
    /// When the rate limit will reset (in seconds)
    pub reset_after: u64,
    /// Total limit for the window
    pub limit: u32,
}

/// Common trait for all rate limiters
#[async_trait]
pub trait RateLimiter: Send + Sync + 'static {
    /// Check if a request is allowed for a given key
    async fn check(&self, key: &str) -> Result<RateLimitResult>;
    /// Increment the counter for a key and check if the request is allowed
    /// Returns the same result as `check` but also increments the counter
    async fn increment(&self, key: &str) -> Result<RateLimitResult>;
    /// Reset the counter for a key
    async fn reset(&self, key: &str) -> Result<()>;
    /// Get the remaining requests for a key without incrementing
    async fn get_remaining(&self, key: &str) -> Result<u32>;
}

/// Factory method to create a rate limiter based on the configuration
pub async fn create_rate_limiter(
    config: &crate::options::RateLimiterConfig,
) -> Result<Arc<dyn RateLimiter + Send + Sync + 'static>> {
    match config.driver.as_str() {
        "redis" => {
            // Get Redis URL from config or use default
            let redis_url = match &config.redis.redis_options.get("url") {
                Some(url) => {
                    url.as_str().unwrap_or("redis://127.0.0.1:6379/")
                }
                None => "redis://127.0.0.1:6379/",
            };

            let redis_client = redis::Client::open(redis_url).map_err(|e| {
                crate::error::Error::CacheError(format!("Failed to create Redis client: {}", e))
            })?;

            // Get prefix from config or use default
            let prefix = match &config.redis.redis_options.get("prefix") {
                Some(prefix) => {
                    if let Some(prefix_str) = prefix.as_str() {
                        prefix_str.to_string()
                    } else {
                        "rate_limit".to_string()
                    }
                }
                None => "rate_limit".to_string(),
            };

            let limiter = redis_limiter::RedisRateLimiter::new(
                redis_client,
                prefix,
                config.default_limit_per_second,
                config.default_window_seconds,
            )
            .await?;

            Ok(Arc::new(limiter))
        }
        "redis_cluster" => {
            // Get Redis URL from config or use default
            let redis_nodes = match &config.redis.redis_options.get("nodes") {
                Some(nodes) => {
                    nodes.as_str().unwrap_or("redis://127.0.1:6379/")
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .collect::<Vec<String>>()
                }
                None => {
                    vec!["redis://127.0.1:6379/".to_string()]
                }
            };

            let redis_client = ClusterClient::new(redis_nodes).map_err(|e| {
                crate::error::Error::CacheError(format!("Failed to create Redis client: {}", e))
            })?;

            // Get prefix from config or use default
            let prefix = match &config.redis.redis_options.get("prefix") {
                Some(prefix) => {
                    if let Some(prefix_str) = prefix.as_str() {
                        prefix_str.to_string()
                    } else {
                        "rate_limit".to_string()
                    }
                }
                None => "rate_limit".to_string(),
            };

            let limiter = RedisClusterRateLimiter::new(
                redis_client,
                prefix,
                config.default_limit_per_second,
                config.default_window_seconds,
            )
                .await?;

            Ok(Arc::new(limiter))
        }

        "memory" | _ => {
            // Default to memory rate limiter
            let limiter = memory_limiter::MemoryRateLimiter::new(
                config.default_limit_per_second,
                config.default_window_seconds,
            );

            Ok(Arc::new(limiter))
        }
    }
}

// Convenience function to create a memory rate limiter with specific configuration
pub fn create_memory_rate_limiter(max_requests: u32, window_secs: u64) -> Arc<dyn RateLimiter> {
    Arc::new(memory_limiter::MemoryRateLimiter::new(
        max_requests,
        window_secs,
    ))
}

// Convenience function to create a redis rate limiter with specific configuration
pub async fn create_redis_rate_limiter(
    redis_url: &str,
    prefix: &str,
    max_requests: u32,
    window_secs: u64,
) -> Result<Arc<dyn RateLimiter>> {
    let redis_client = redis::Client::open(redis_url).map_err(|e| {
        crate::error::Error::CacheError(format!("Failed to create Redis client: {}", e))
    })?;

    let limiter = redis_limiter::RedisRateLimiter::new(
        redis_client,
        prefix.to_string(),
        max_requests,
        window_secs,
    )
    .await?;

    Ok(Arc::new(limiter))
}

// Then modify the middleware module to accept Arc<dyn RateLimiter> directly
pub mod middleware_utils {
    use super::*;
    use crate::rate_limiter::middleware::{IpKeyExtractor, RateLimitLayer, RateLimitOptions};

    // Helper function to create rate limit middleware with Arc<dyn RateLimiter>
    pub fn with_arc_ip_limiter(
        limiter: Arc<dyn RateLimiter>,
        options: RateLimitOptions,
    ) -> RateLimitLayer<IpKeyExtractor> {
        RateLimitLayer::with_options(limiter, IpKeyExtractor::new(1), options)
    }
}
