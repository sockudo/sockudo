#![allow(unused_assignments)]
#![allow(unused_variables)]
#![allow(dead_code)]

use async_trait::async_trait;
use redis::AsyncCommands;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use sockudo_core::error::{Error, Result};
use sockudo_core::rate_limiter::{RateLimitConfig, RateLimitResult, RateLimiter};
use tokio::sync::RwLock;
use tracing::warn;

/// Redis Cluster-based rate limiter implementation
pub struct RedisClusterRateLimiter {
    /// Redis client
    client: ClusterClient,
    /// Redis connection
    connection: RwLock<ClusterConnection>,
    /// Prefix for Redis keys
    prefix: String,
    /// Configuration for rate limiting
    config: RateLimitConfig,
}

impl RedisClusterRateLimiter {
    /// Create a new Redis Cluster-based rate limiter
    pub async fn new(
        client: ClusterClient,
        prefix: String,
        max_requests: u32,
        window_secs: u64,
    ) -> Result<Self> {
        Self::with_config(
            client,
            prefix,
            RateLimitConfig {
                max_requests,
                window_secs,
                identifier: Some("redis".to_string()),
            },
        )
        .await
    }

    /// Create a new Redis Cluster-based rate limiter with a specific configuration
    pub async fn with_config(
        client: ClusterClient,
        prefix: String,
        config: RateLimitConfig,
    ) -> Result<Self> {
        let connection = sockudo_core::redis_client::cluster_connect_with_retry(&client).await?;

        Ok(Self {
            client,
            connection: RwLock::new(connection),
            prefix,
            config,
        })
    }

    /// Get a key formatted with the prefix
    fn get_key(&self, key: &str) -> String {
        format!("{}:rl:{}", self.prefix, key)
    }

    /// Run sliding window rate limiting using Redis
    async fn run_sliding_window_check(
        &self,
        key: &str,
        increment: bool,
    ) -> Result<RateLimitResult> {
        self.run_sliding_window(
            key,
            increment,
            1,
            self.config.max_requests,
            self.config.window_secs,
        )
        .await
    }

    async fn run_sliding_window(
        &self,
        key: &str,
        increment: bool,
        cost: u32,
        max_requests: u32,
        window_secs: u64,
    ) -> Result<RateLimitResult> {
        let redis_key = self.get_key(key);
        let now_ms = crate::redis_window::current_time_ms();
        let window_start_ms = crate::redis_window::window_start_ms(now_ms, window_secs);
        let member = if increment {
            crate::redis_window::entry_member(now_ms)
        } else {
            String::new()
        };
        let request = crate::redis_window::SlidingWindowRequest {
            key: &redis_key,
            now_ms,
            window_start_ms,
            window_secs,
            max_requests,
            cost,
            increment,
            member: &member,
        };

        let mut connection = { self.connection.read().await.clone() };
        let first_result = crate::redis_window::run_sliding_window(&mut connection, request).await;

        match first_result {
            Ok(result) => Ok(result),
            Err(error) if error.is_connection_dropped() => {
                warn!(
                    error = %error,
                    attempt_count = 1,
                    retryable = true,
                    "redis cluster rate limiter command retry scheduled"
                );
                let replacement =
                    self.client
                        .get_async_connection()
                        .await
                        .map_err(|retry_error| {
                            Error::Redis(format!(
                                "failed to reconnect Redis Cluster rate limiter: {retry_error}"
                            ))
                        })?;
                let mut retry_connection = replacement.clone();
                let result = crate::redis_window::run_sliding_window(
                    &mut retry_connection,
                    request,
                )
                .await
                .map_err(|retry_error| {
                    Error::Redis(format!(
                        "redis cluster sliding-window command failed after reconnect: {retry_error}"
                    ))
                })?;
                *self.connection.write().await = replacement;
                Ok(result)
            }
            Err(error) => Err(Error::Redis(format!(
                "redis cluster sliding-window command failed: {error}"
            ))),
        }
    }
}

#[async_trait]
impl RateLimiter for RedisClusterRateLimiter {
    async fn check(&self, key: &str) -> Result<RateLimitResult> {
        self.run_sliding_window_check(key, false).await
    }

    async fn increment(&self, key: &str) -> Result<RateLimitResult> {
        self.run_sliding_window_check(key, true).await
    }

    async fn increment_with_limit(
        &self,
        key: &str,
        cost: u32,
        max_requests: u32,
        window_secs: u64,
    ) -> Result<RateLimitResult> {
        self.run_sliding_window(key, true, cost, max_requests, window_secs)
            .await
    }

    async fn reset(&self, key: &str) -> Result<()> {
        let redis_key = self.get_key(key);
        let mut conn = { self.connection.read().await.clone() };

        let _: () = conn
            .del(&redis_key)
            .await
            .map_err(|e| Error::Redis(format!("Failed to delete Redis key: {e}")))?;

        Ok(())
    }

    async fn get_remaining(&self, key: &str) -> Result<u32> {
        let result = self.check(key).await?;
        Ok(result.remaining)
    }
}
