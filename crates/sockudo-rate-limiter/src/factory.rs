#![allow(dead_code)]
#![allow(unused_variables)]

use sockudo_core::error::Result;
use sockudo_core::options::RateLimit;
use sockudo_core::rate_limiter::RateLimiter;
use std::sync::Arc;
use tracing::{info, warn};

use crate::memory_limiter::MemoryRateLimiter;
#[cfg(feature = "redis-cluster")]
use crate::redis_cluster_limiter::RedisClusterRateLimiter;
#[cfg(feature = "redis")]
use crate::redis_limiter::RedisRateLimiter;
use sockudo_core::options::{CacheDriver, RateLimiterConfig, RedisConnection};

pub struct RateLimiterFactory;

impl RateLimiterFactory {
    pub async fn create_api(
        config: &RateLimiterConfig,
        global_redis_conn_details: &RedisConnection,
    ) -> Result<Arc<dyn RateLimiter + Send + Sync>> {
        Self::create_for_limit(
            config,
            &config.api_rate_limit,
            "HTTP API",
            "rl_http:",
            global_redis_conn_details,
        )
        .await
    }

    pub async fn create_websocket(
        config: &RateLimiterConfig,
        global_redis_conn_details: &RedisConnection,
    ) -> Result<Arc<dyn RateLimiter + Send + Sync>> {
        Self::create_for_limit(
            config,
            &config.websocket_rate_limit,
            "WebSocket",
            "rl_ws:",
            global_redis_conn_details,
        )
        .await
    }

    pub async fn create(
        config: &RateLimiterConfig,
        global_redis_conn_details: &RedisConnection,
    ) -> Result<Arc<dyn RateLimiter + Send + Sync>> {
        Self::create_api(config, global_redis_conn_details).await
    }

    async fn create_for_limit(
        config: &RateLimiterConfig,
        limit: &RateLimit,
        limiter_name: &str,
        default_prefix_suffix: &str,
        global_redis_conn_details: &RedisConnection,
    ) -> Result<Arc<dyn RateLimiter + Send + Sync>> {
        if !config.enabled {
            info!(
                "{} rate limiting is globally disabled. Returning a permissive limiter.",
                limiter_name
            );
            return Ok(Arc::new(MemoryRateLimiter::new(u32::MAX, 1))); // Allows all
        }

        info!(
            "Initializing {} RateLimiter with driver: {:?}",
            limiter_name, config.driver
        );

        match config.driver {
            #[cfg(feature = "redis")]
            CacheDriver::Redis => {
                Self::create_redis_limiter(
                    limit,
                    limiter_name,
                    default_prefix_suffix,
                    config,
                    global_redis_conn_details,
                )
                .await
            }
            #[cfg(feature = "redis-cluster")]
            CacheDriver::RedisCluster => {
                Self::create_redis_cluster_limiter(
                    limit,
                    limiter_name,
                    default_prefix_suffix,
                    config,
                    global_redis_conn_details,
                )
                .await
            }
            CacheDriver::Memory => Self::create_memory_limiter(limit, limiter_name),
            CacheDriver::None => {
                warn!("Rate limiter driver set to 'None'. Using memory limiter as fallback.");
                Ok(Arc::new(MemoryRateLimiter::new(
                    limit.max_requests,
                    limit.window_seconds,
                )))
            }
            #[cfg(not(feature = "redis"))]
            CacheDriver::Redis => {
                warn!(
                    "Redis rate limiter requested but not compiled in. Falling back to memory limiter."
                );
                Self::create_memory_limiter(limit, limiter_name)
            }
            #[cfg(not(feature = "redis-cluster"))]
            CacheDriver::RedisCluster => {
                warn!(
                    "Redis Cluster rate limiter requested but not compiled in. Falling back to memory limiter."
                );
                Self::create_memory_limiter(limit, limiter_name)
            }
        }
    }

    #[cfg(feature = "redis")]
    async fn create_redis_limiter(
        limit: &RateLimit,
        limiter_name: &str,
        default_prefix_suffix: &str,
        config: &RateLimiterConfig,
        global_redis_conn_details: &RedisConnection,
    ) -> Result<Arc<dyn RateLimiter + Send + Sync>> {
        info!(
            "RateLimiter: Using standalone Redis backend for {}.",
            limiter_name
        );

        let redis_url = config
            .redis
            .url_override
            .clone()
            .unwrap_or_else(|| global_redis_conn_details.to_url());

        let prefix = config.redis.prefix.clone().unwrap_or_else(|| {
            global_redis_conn_details.key_prefix.clone() + default_prefix_suffix
        });

        let client = redis::Client::open(redis_url.as_str()).map_err(|e| {
            sockudo_core::error::Error::Redis(format!(
                "Failed to create Redis client for rate limiter: {e}"
            ))
        })?;

        let limiter =
            RedisRateLimiter::new(client, prefix, limit.max_requests, limit.window_seconds).await?;

        Ok(Arc::new(limiter))
    }

    #[cfg(feature = "redis-cluster")]
    async fn create_redis_cluster_limiter(
        limit: &RateLimit,
        limiter_name: &str,
        default_prefix_suffix: &str,
        config: &RateLimiterConfig,
        global_redis_conn_details: &RedisConnection,
    ) -> Result<Arc<dyn RateLimiter + Send + Sync>> {
        info!(
            "RateLimiter: Using Redis Cluster backend for {}.",
            limiter_name
        );

        if global_redis_conn_details.cluster_nodes.is_empty() {
            tracing::error!(
                "RateLimiter: Redis cluster driver selected, but no cluster_nodes configured."
            );
            return Err(sockudo_core::error::Error::Configuration(
                "RateLimiter: Redis cluster nodes not configured.".to_string(),
            ));
        }

        let nodes: Vec<String> = global_redis_conn_details
            .cluster_nodes
            .iter()
            .map(|node| node.to_url())
            .collect();

        let prefix = config.redis.prefix.clone().unwrap_or_else(|| {
            global_redis_conn_details.key_prefix.clone() + default_prefix_suffix
        });

        let client = redis::cluster::ClusterClient::new(nodes).map_err(|e| {
            sockudo_core::error::Error::Redis(format!(
                "Failed to create Redis cluster client for rate limiter: {e}"
            ))
        })?;

        let limiter =
            RedisClusterRateLimiter::new(client, prefix, limit.max_requests, limit.window_seconds)
                .await?;

        Ok(Arc::new(limiter))
    }

    fn create_memory_limiter(
        limit: &RateLimit,
        limiter_name: &str,
    ) -> Result<Arc<dyn RateLimiter + Send + Sync>> {
        info!("Using memory rate limiter for {}.", limiter_name);
        let limiter = MemoryRateLimiter::new(limit.max_requests, limit.window_seconds);
        Ok(Arc::new(limiter))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::options::{CacheDriver, RateLimiterConfig, RedisConfig};

    fn test_config() -> RateLimiterConfig {
        RateLimiterConfig {
            enabled: true,
            driver: CacheDriver::Memory,
            api_rate_limit: RateLimit {
                max_requests: 2,
                window_seconds: 60,
                identifier: Some("api".to_string()),
                trust_hops: Some(0),
            },
            websocket_rate_limit: RateLimit {
                max_requests: 1,
                window_seconds: 60,
                identifier: Some("websocket_connect".to_string()),
                trust_hops: Some(0),
            },
            redis: RedisConfig {
                prefix: Some("sockudo_rate_limiter:".to_string()),
                url_override: None,
                cluster_mode: false,
            },
        }
    }

    fn test_redis_connection() -> RedisConnection {
        RedisConnection {
            host: "127.0.0.1".to_string(),
            port: 6379,
            db: 0,
            username: None,
            password: None,
            key_prefix: "sockudo:".to_string(),
            sentinels: vec![],
            sentinel_password: None,
            name: "mymaster".to_string(),
            cluster: Default::default(),
            cluster_nodes: vec![],
        }
    }

    #[tokio::test]
    async fn create_api_uses_api_rate_limit_settings() {
        let limiter = RateLimiterFactory::create_api(&test_config(), &test_redis_connection())
            .await
            .unwrap();

        assert!(limiter.increment("same-key").await.unwrap().allowed);
        assert!(limiter.increment("same-key").await.unwrap().allowed);
        assert!(!limiter.increment("same-key").await.unwrap().allowed);
    }

    #[tokio::test]
    async fn create_websocket_uses_websocket_rate_limit_settings() {
        let limiter =
            RateLimiterFactory::create_websocket(&test_config(), &test_redis_connection())
                .await
                .unwrap();

        assert!(limiter.increment("same-key").await.unwrap().allowed);
        assert!(!limiter.increment("same-key").await.unwrap().allowed);
    }
}
