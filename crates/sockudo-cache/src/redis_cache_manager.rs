#![allow(dead_code)]

use async_trait::async_trait;
use redis::{AsyncCommands, Client, aio::ConnectionManager};
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use std::time::Duration;

/// Configuration for the Redis cache manager
#[derive(Clone, Debug)]
pub struct RedisCacheConfig {
    /// Redis URL
    pub url: String,
    /// Key prefix
    pub prefix: String,
    /// Response timeout
    pub response_timeout: Option<Duration>,
    /// Use RESP3 protocol
    pub use_resp3: bool,
}

impl Default for RedisCacheConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379/".to_string(),
            prefix: "cache".to_string(),
            response_timeout: Some(Duration::from_secs(5)),
            use_resp3: false,
        }
    }
}

/// A Redis-based implementation of the CacheManager trait
pub struct RedisCacheManager {
    /// Redis client
    client: Client,
    /// Connection manager with automatic reconnection. Clone is cheap (shared internal state).
    connection: ConnectionManager,
    /// Key prefix
    prefix: String,
}

impl RedisCacheManager {
    /// Creates a new Redis cache manager with configuration
    pub async fn new(config: RedisCacheConfig) -> Result<Self> {
        let redis_url = if config.use_resp3 && !config.url.contains("protocol=resp3") {
            if config.url.contains('?') {
                format!("{}&protocol=resp3", config.url)
            } else {
                format!("{}?protocol=resp3", config.url)
            }
        } else {
            config.url
        };

        let client = Client::open(redis_url)
            .map_err(|e| Error::Cache(format!("Failed to create Redis client: {e}")))?;

        let connection_manager_config = redis::aio::ConnectionManagerConfig::new()
            .set_number_of_retries(5)
            .set_exponent_base(2.0)
            .set_max_delay(std::time::Duration::from_millis(5000));

        let connection = client
            .get_connection_manager_with_config(connection_manager_config)
            .await
            .map_err(|e| Error::Cache(format!("Failed to connect to Redis: {e}")))?;

        Ok(Self {
            client,
            connection,
            prefix: config.prefix,
        })
    }

    /// Creates a new Redis cache manager with simple configuration
    pub async fn with_url(redis_url: &str, prefix: Option<&str>) -> Result<Self> {
        let config = RedisCacheConfig {
            url: redis_url.to_string(),
            prefix: prefix.unwrap_or("cache").to_string(),
            ..Default::default()
        };

        Self::new(config).await
    }

    /// Get the prefixed key
    fn prefixed_key(&self, key: &str) -> String {
        format!("{}:{}", self.prefix, key)
    }
}

#[async_trait]
impl CacheManager for RedisCacheManager {
    async fn has(&self, key: &str) -> Result<bool> {
        let mut connection = self.connection.clone();
        let exists: bool = connection
            .exists(self.prefixed_key(key))
            .await
            .map_err(|e| Error::Cache(format!("Redis exists error: {e}")))?;
        Ok(exists)
    }

    async fn get(&self, key: &str) -> Result<Option<String>> {
        let mut connection = self.connection.clone();
        let value: Option<String> = connection
            .get(self.prefixed_key(key))
            .await
            .map_err(|e| Error::Cache(format!("Redis get error: {e}")))?;
        Ok(value)
    }

    async fn set(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<()> {
        let prefixed_key = self.prefixed_key(key);
        let mut connection = self.connection.clone();

        if ttl_seconds > 0 {
            connection
                .set_ex::<_, _, ()>(prefixed_key, value, ttl_seconds)
                .await
                .map_err(|e| Error::Cache(format!("Redis set error: {e}")))?;
        } else {
            connection
                .set::<_, _, ()>(prefixed_key, value)
                .await
                .map_err(|e| Error::Cache(format!("Redis set error: {e}")))?;
        }

        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        let mut connection = self.connection.clone();
        let deleted: i32 = connection
            .del(self.prefixed_key(key))
            .await
            .map_err(|e| Error::Cache(format!("Redis delete error: {e}")))?;
        if deleted > 0 {
            Ok(())
        } else {
            Err(Error::Cache(format!("Key '{key}' not found")))
        }
    }

    async fn disconnect(&self) -> Result<()> {
        self.clear_prefix().await?;
        Ok(())
    }

    async fn check_health(&self) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::Cache(format!("Failed to acquire health check connection: {e}")))?;

        let response = redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| Error::Cache(format!("Cache health check PING failed: {e}")))?;

        if response == "PONG" {
            Ok(())
        } else {
            Err(Error::Cache(format!(
                "Cache Redis PING returned unexpected response: {response}"
            )))
        }
    }

    async fn ttl(&self, key: &str) -> Result<Option<Duration>> {
        let mut connection = self.connection.clone();
        let ttl: i64 = connection
            .ttl(self.prefixed_key(key))
            .await
            .map_err(|e| Error::Cache(format!("Redis TTL error: {e}")))?;
        if ttl > 0 {
            Ok(Some(Duration::from_secs(ttl as u64)))
        } else {
            Ok(None)
        }
    }

    async fn set_if_not_exists(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<bool> {
        let prefixed_key = self.prefixed_key(key);
        let mut connection = self.connection.clone();
        let result: Option<String> = redis::cmd("SET")
            .arg(&prefixed_key)
            .arg(value)
            .arg("NX")
            .arg("EX")
            .arg(ttl_seconds)
            .query_async(&mut connection)
            .await
            .map_err(|e| Error::Cache(format!("Redis SET NX error: {e}")))?;
        Ok(result.is_some())
    }
}

impl RedisCacheManager {
    pub async fn delete(&self, key: &str) -> Result<bool> {
        let mut connection = self.connection.clone();
        let deleted: i32 = connection
            .del(self.prefixed_key(key))
            .await
            .map_err(|e| Error::Cache(format!("Redis delete error: {e}")))?;
        Ok(deleted > 0)
    }

    pub async fn clear_prefix(&self) -> Result<usize> {
        let pattern = format!("{}:*", self.prefix);
        let mut connection = self.connection.clone();

        let keys = {
            let mut keys = Vec::new();
            let mut iter: redis::AsyncIter<String> = connection
                .scan_match(&pattern)
                .await
                .map_err(|e| Error::Cache(format!("Redis scan error: {e}")))?;

            while let Some(key) = iter.next_item().await {
                let key =
                    key.map_err(|e| Error::Cache(format!("Redis scan iteration error: {e}")))?;
                keys.push(key);
            }
            keys
        };

        if keys.is_empty() {
            return Ok(0);
        }

        let deleted: i32 = connection
            .del(keys)
            .await
            .map_err(|e| Error::Cache(format!("Redis delete error: {e}")))?;

        Ok(deleted as usize)
    }

    pub async fn set_many(&self, pairs: &[(&str, &str)], ttl_seconds: u64) -> Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }

        let prefixed_pairs: Vec<(String, &str)> = pairs
            .iter()
            .map(|(k, v)| (self.prefixed_key(k), *v))
            .collect();

        let mut pipe = redis::pipe();
        for (key, value) in &prefixed_pairs {
            if ttl_seconds > 0 {
                pipe.set_ex(key, *value, ttl_seconds);
            } else {
                pipe.set(key, *value);
            }
        }

        let mut connection = self.connection.clone();
        pipe.query_async::<()>(&mut connection)
            .await
            .map_err(|e| Error::Cache(format!("Redis pipeline error: {e}")))?;

        Ok(())
    }

    pub async fn increment(&self, key: &str, by: i64) -> Result<i64> {
        let mut connection = self.connection.clone();
        let value: i64 = connection
            .incr(self.prefixed_key(key), by)
            .await
            .map_err(|e| Error::Cache(format!("Redis increment error: {e}")))?;
        Ok(value)
    }

    pub async fn get_remaining_ttl() {
        todo!()
    }

    pub async fn get_many(&self, keys: &[&str]) -> Result<Vec<Option<String>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let prefixed_keys: Vec<String> = keys.iter().map(|k| self.prefixed_key(k)).collect();
        let mut connection = self.connection.clone();
        let values: Vec<Option<String>> = connection
            .mget(prefixed_keys)
            .await
            .map_err(|e| Error::Cache(format!("Redis mget error: {e}")))?;

        Ok(values)
    }

    pub async fn flush_db(&self) -> Result<()> {
        let mut connection = self.connection.clone();
        redis::cmd("FLUSHDB")
            .query_async::<()>(&mut connection)
            .await
            .map_err(|e| Error::Cache(format!("Redis flushdb error: {e}")))?;

        Ok(())
    }

    pub fn get_connection(&self) -> ConnectionManager {
        self.connection.clone()
    }
}

/// Factory for creating cache managers
pub struct CacheManagerFactory;

impl CacheManagerFactory {
    pub async fn create_redis(
        redis_url: &str,
        prefix: Option<&str>,
        response_timeout: Option<Duration>,
    ) -> Result<Box<dyn CacheManager + Send>> {
        let config = RedisCacheConfig {
            url: redis_url.to_string(),
            prefix: prefix.unwrap_or("cache").to_string(),
            response_timeout,
            use_resp3: false,
        };

        let cache_manager = RedisCacheManager::new(config).await?;
        Ok(Box::new(cache_manager))
    }

    pub async fn create_redis_resp3(
        redis_url: &str,
        prefix: Option<&str>,
        response_timeout: Option<Duration>,
    ) -> Result<Box<dyn CacheManager + Send>> {
        let config = RedisCacheConfig {
            url: redis_url.to_string(),
            prefix: prefix.unwrap_or("cache").to_string(),
            response_timeout,
            use_resp3: true,
        };

        let cache_manager = RedisCacheManager::new(config).await?;
        Ok(Box::new(cache_manager))
    }
}
