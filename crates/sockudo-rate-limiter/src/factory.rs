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
#[cfg(feature = "redis-cluster")]
use sockudo_core::redis_client::configure_cluster_builder;
#[cfg(feature = "redis")]
use sockudo_core::redis_client::{RedisClient, RedisClientOptions};

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

    /// Backend for per-app HTTP API limits (`max_backend_events_per_second`,
    /// `max_read_requests_per_second`). Callers pass each app's limit with
    /// `increment_with_limit`; the configured limit here is never used.
    pub async fn create_app_api(
        config: &RateLimiterConfig,
        global_redis_conn_details: &RedisConnection,
    ) -> Result<Arc<dyn RateLimiter + Send + Sync>> {
        let limit = RateLimit {
            max_requests: u32::MAX,
            window_seconds: 1,
            identifier: Some("app_api".to_owned()),
            trust_hops: Some(0),
        };
        Self::create_for_limit(
            config,
            &limit,
            "Per-app HTTP API",
            "rl_app:",
            global_redis_conn_details,
        )
        .await
    }

    pub async fn create_push_acceptance(
        config: &RateLimiterConfig,
        max_requests: u32,
        window_seconds: u64,
        global_redis_conn_details: &RedisConnection,
    ) -> Result<Arc<dyn RateLimiter + Send + Sync>> {
        let limit = RateLimit {
            max_requests,
            window_seconds,
            identifier: Some("push_acceptance".to_owned()),
            trust_hops: Some(0),
        };
        Self::create_for_limit(
            config,
            &limit,
            "Push acceptance",
            "rl_push_accept:",
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
                limiter = limiter_name,
                "rate limiting globally disabled, using permissive limiter"
            );
            return Ok(Arc::new(MemoryRateLimiter::new(u32::MAX, 1))); // Allows all
        }

        info!(limiter = limiter_name, driver = ?config.driver, "initializing rate limiter");

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
                warn!(
                    limiter = limiter_name,
                    "rate limiter driver set to none, using memory fallback"
                );
                Ok(Arc::new(MemoryRateLimiter::new(
                    limit.max_requests,
                    limit.window_seconds,
                )))
            }
            #[cfg(not(feature = "redis"))]
            CacheDriver::Redis => {
                warn!(
                    limiter = limiter_name,
                    "redis rate limiter not compiled in, using memory fallback"
                );
                Self::create_memory_limiter(limit, limiter_name)
            }
            #[cfg(not(feature = "redis-cluster"))]
            CacheDriver::RedisCluster => {
                warn!(
                    limiter = limiter_name,
                    "redis cluster rate limiter not compiled in, using memory fallback"
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
            limiter = limiter_name,
            backend = "redis",
            "rate limiter using redis backend"
        );

        let redis_url = config
            .redis
            .url_override
            .clone()
            .unwrap_or_else(|| global_redis_conn_details.to_url());
        let sentinel = config
            .redis
            .url_override
            .is_none()
            .then(|| global_redis_conn_details.sentinel_spec())
            .flatten();

        let prefix = config.redis.prefix.clone().unwrap_or_else(|| {
            global_redis_conn_details.key_prefix.clone() + default_prefix_suffix
        });

        let client = RedisClient::connect_with_options(
            &redis_url,
            RedisClientOptions {
                sentinel,
                tls: global_redis_conn_details.master_tls.clone(),
                response_timeout: None,
            },
        )
        .await?;

        let limiter = RedisRateLimiter::with_redis_client(
            client,
            prefix,
            limit.max_requests,
            limit.window_seconds,
        )
        .await?;

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
            limiter = limiter_name,
            backend = "redis_cluster",
            "rate limiter using redis cluster backend"
        );

        if !global_redis_conn_details.has_cluster_nodes() {
            tracing::error!(
                limiter = limiter_name,
                "rate limiter: redis cluster nodes not configured"
            );
            return Err(sockudo_core::error::Error::Configuration(
                "RateLimiter: Redis cluster nodes not configured.".to_string(),
            ));
        }

        let nodes = global_redis_conn_details.cluster_node_urls();

        let prefix = config.redis.prefix.clone().unwrap_or_else(|| {
            global_redis_conn_details.key_prefix.clone() + default_prefix_suffix
        });

        let builder = redis::cluster::ClusterClientBuilder::new(nodes);
        let client =
            configure_cluster_builder(builder, &global_redis_conn_details.cluster_tls_options())
                .await?
                .build()
                .map_err(|e| {
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
        info!(
            limiter = limiter_name,
            backend = "memory",
            "rate limiter using memory backend"
        );
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
            ..Default::default()
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

    #[tokio::test]
    async fn create_push_acceptance_uses_existing_driver_limit() {
        let limiter = RateLimiterFactory::create_push_acceptance(
            &test_config(),
            2,
            60,
            &test_redis_connection(),
        )
        .await
        .unwrap();

        assert!(
            limiter
                .increment("push:acceptance:app-1")
                .await
                .unwrap()
                .allowed
        );
        assert!(
            limiter
                .increment("push:acceptance:app-1")
                .await
                .unwrap()
                .allowed
        );
        assert!(
            !limiter
                .increment("push:acceptance:app-1")
                .await
                .unwrap()
                .allowed
        );
    }
}
