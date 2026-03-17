use async_trait::async_trait;
use redis::AsyncCommands;
use sockudo_core::delta_types::ClusterCoordinator;
use sockudo_core::error::{Error, Result};
use std::sync::Arc;
use tracing::debug;

/// Redis-based cluster coordinator for delta interval synchronization
pub struct RedisClusterCoordinator {
    connection: Arc<tokio::sync::Mutex<redis::aio::ConnectionManager>>,
    prefix: String,
    ttl_seconds: u64,
}

impl RedisClusterCoordinator {
    /// Create a new Redis cluster coordinator
    pub async fn new(redis_url: &str, prefix: Option<&str>) -> Result<Self> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| Error::Redis(format!("Failed to create Redis client: {}", e)))?;

        let connection_manager_config = redis::aio::ConnectionManagerConfig::new()
            .set_number_of_retries(5)
            .set_exponent_base(2.0)
            .set_max_delay(std::time::Duration::from_millis(5000));

        let connection = client
            .get_connection_manager_with_config(connection_manager_config)
            .await
            .map_err(|e| Error::Redis(format!("Failed to connect to Redis: {}", e)))?;

        Ok(Self {
            connection: Arc::new(tokio::sync::Mutex::new(connection)),
            prefix: prefix.unwrap_or("sockudo").to_string(),
            ttl_seconds: 300,
        })
    }

    fn get_key(&self, app_id: &str, channel: &str, conflation_key: &str) -> String {
        format!(
            "{}:delta_count:{}:{}:{}",
            self.prefix, app_id, channel, conflation_key
        )
    }
}

#[async_trait]
impl ClusterCoordinator for RedisClusterCoordinator {
    async fn increment_and_check(
        &self,
        app_id: &str,
        channel: &str,
        conflation_key: &str,
        interval: u32,
    ) -> Result<(bool, u32)> {
        let key = self.get_key(app_id, channel, conflation_key);
        let mut conn = self.connection.lock().await;

        let count: u32 = conn
            .incr(&key, 1)
            .await
            .map_err(|e| Error::Redis(format!("Failed to increment counter: {}", e)))?;

        // Set TTL on first increment
        if count == 1 {
            let _: () = conn
                .expire(&key, self.ttl_seconds as i64)
                .await
                .map_err(|e| Error::Redis(format!("Failed to set TTL: {}", e)))?;
        }

        let should_send_full = count >= interval;

        if should_send_full {
            debug!(
                "Cluster coordination: Full message triggered (count={}, interval={}) for app={}, channel={}, key={}",
                count, interval, app_id, channel, conflation_key
            );

            let _: () = conn
                .set(&key, 0)
                .await
                .map_err(|e| Error::Redis(format!("Failed to reset counter: {}", e)))?;

            let _: () = conn
                .expire(&key, self.ttl_seconds as i64)
                .await
                .map_err(|e| Error::Redis(format!("Failed to refresh TTL: {}", e)))?;

            Ok((true, interval))
        } else {
            debug!(
                "Cluster coordination: Delta message (count={}/{}) for app={}, channel={}, key={}",
                count, interval, app_id, channel, conflation_key
            );
            Ok((false, count))
        }
    }

    async fn reset_counter(
        &self,
        app_id: &str,
        channel: &str,
        conflation_key: &str,
    ) -> Result<()> {
        let key = self.get_key(app_id, channel, conflation_key);
        let mut conn = self.connection.lock().await;

        let _: () = conn
            .del(&key)
            .await
            .map_err(|e| Error::Redis(format!("Failed to delete counter: {}", e)))?;

        debug!(
            "Cluster coordination: Reset counter for app={}, channel={}, key={}",
            app_id, channel, conflation_key
        );
        Ok(())
    }

    async fn get_counter(
        &self,
        app_id: &str,
        channel: &str,
        conflation_key: &str,
    ) -> Result<u32> {
        let key = self.get_key(app_id, channel, conflation_key);
        let mut conn = self.connection.lock().await;

        let count: Option<u32> = conn
            .get(&key)
            .await
            .map_err(|e| Error::Redis(format!("Failed to get counter: {}", e)))?;

        Ok(count.unwrap_or(0))
    }
}

impl Clone for RedisClusterCoordinator {
    fn clone(&self) -> Self {
        Self {
            connection: Arc::clone(&self.connection),
            prefix: self.prefix.clone(),
            ttl_seconds: self.ttl_seconds,
        }
    }
}
