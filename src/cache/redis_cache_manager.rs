use crate::cache::manager::CacheManager;
use crate::error::{Error, Result};
use async_trait::async_trait;
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};
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
    /// Multiplexed connection for better performance
    connection: MultiplexedConnection,
    /// Key prefix
    prefix: String,
}

impl RedisCacheManager {
    /// Creates a new Redis cache manager with configuration
    pub async fn new(config: RedisCacheConfig) -> Result<Self> {
        // Build the Redis URL with RESP3 if enabled
        let redis_url = if config.use_resp3 && !config.url.contains("protocol=resp3") {
            if config.url.contains('?') {
                format!("{}&protocol=resp3", config.url)
            } else {
                format!("{}?protocol=resp3", config.url)
            }
        } else {
            config.url
        };

        // Create Redis client
        let client = Client::open(redis_url)
            .map_err(|e| Error::CacheError(format!("Failed to create Redis client: {}", e)))?;

        // Get multiplexed connection for better performance
        let mut connection = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|e| Error::CacheError(format!("Failed to connect to Redis: {}", e)))?;

        // Set response timeout if configured
        if let Some(timeout) = config.response_timeout {
            connection.set_response_timeout(timeout);
        }

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
    /// Check if the given key exists in cache
    async fn has(&mut self, key: &str) -> Result<bool> {
        let exists: bool = self
            .connection
            .exists(self.prefixed_key(key))
            .await
            .map_err(|e| Error::CacheError(format!("Redis exists error: {}", e)))?;
        Ok(exists)
    }

    /// Get a key from the cache
    /// Returns None if cache does not exist
    async fn get(&mut self, key: &str) -> Result<Option<String>> {
        let value: Option<String> = self
            .connection
            .get(self.prefixed_key(key))
            .await
            .map_err(|e| Error::CacheError(format!("Redis get error: {}", e)))?;
        Ok(value)
    }

    /// Set or overwrite the value in the cache
    async fn set(&mut self, key: &str, value: &str, ttl_seconds: u64) -> Result<()> {
        let prefixed_key = self.prefixed_key(key);

        if ttl_seconds > 0 {
            // Set with expiration
            self.connection
                .set_ex::<_, _, ()>(prefixed_key, value, ttl_seconds)
                .await
                .map_err(|e| Error::CacheError(format!("Redis set error: {}", e)))?;
        } else {
            // Set without expiration
            self.connection
                .set::<_, _, ()>(prefixed_key, value)
                .await
                .map_err(|e| Error::CacheError(format!("Redis set error: {}", e)))?;
        }

        Ok(())
    }

    async fn remove(&mut self, key: &str) -> Result<()> {
        let deleted: i32 = self
            .connection
            .del(self.prefixed_key(key))
            .await
            .map_err(|e| Error::CacheError(format!("Redis delete error: {}", e)))?;
        if deleted > 0 {
            Ok(())
        } else {
            Err(Error::CacheError(format!("Key '{}' not found", key)))
        }
    }

    /// Disconnect the manager's made connections
    async fn disconnect(&mut self) -> Result<()> {
        // delete all keys with the current prefix
        let pattern = format!("{}:*", self.prefix);
        let keys: Vec<String> = self
            .connection
            .keys(pattern)
            .await
            .map_err(|e| Error::CacheError(format!("Redis keys error: {}", e)))?;

        Ok(())
    }

    /// Check if the Redis connection is healthy
    async fn is_healthy(&self) -> Result<bool> {
        // Try a PING command to verify Redis is responsive
        let result: redis::RedisResult<String> = redis::cmd("PING")
            .query_async(&mut self.connection.clone())
            .await;

        match result {
            Ok(response) => Ok(response == "PONG"),
            Err(_) => Ok(false),
        }
    }
    async fn ttl(&mut self, key: &str) -> Result<Option<Duration>> {
        let ttl: i64 = self
            .connection
            .ttl(self.prefixed_key(key))
            .await
            .map_err(|e| Error::CacheError(format!("Redis TTL error: {}", e)))?;
        if ttl > 0 {
            Ok(Some(Duration::from_secs(ttl as u64)))
        } else {
            Ok(None)
        }
    }
}

// Additional utility methods for the cache manager
impl RedisCacheManager {
    /// Delete a key from the cache
    pub async fn delete(&mut self, key: &str) -> Result<bool> {
        let deleted: i32 = self
            .connection
            .del(self.prefixed_key(key))
            .await
            .map_err(|e| Error::CacheError(format!("Redis delete error: {}", e)))?;
        Ok(deleted > 0)
    }

    /// Clear all keys with the current prefix
    pub async fn clear_prefix(&mut self) -> Result<usize> {
        let pattern = format!("{}:*", self.prefix);

        // First get the keys
        let keys: Vec<String> = self
            .connection
            .keys(pattern)
            .await
            .map_err(|e| Error::CacheError(format!("Redis keys error: {}", e)))?;

        if keys.is_empty() {
            return Ok(0);
        }

        // Then delete them
        let deleted: i32 = self
            .connection
            .del(keys)
            .await
            .map_err(|e| Error::CacheError(format!("Redis delete error: {}", e)))?;

        Ok(deleted as usize)
    }

    /// Set multiple key-value pairs at once
    pub async fn set_many(&mut self, pairs: &[(&str, &str)], ttl_seconds: u64) -> Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }

        // Convert to prefixed keys
        let prefixed_pairs: Vec<(String, &str)> = pairs
            .iter()
            .map(|(k, v)| (self.prefixed_key(k), *v))
            .collect();

        // Use a pipeline for better performance
        let mut pipe = redis::pipe();

        for (key, value) in &prefixed_pairs {
            if ttl_seconds > 0 {
                pipe.set_ex(key, *value, ttl_seconds as usize as u64);
            } else {
                pipe.set(key, *value);
            }
        }

        // Execute pipeline
        pipe.query_async::<()>(&mut self.connection)
            .await
            .map_err(|e| Error::CacheError(format!("Redis pipeline error: {}", e)))?;

        Ok(())
    }

    /// Increment a counter in Redis
    pub async fn increment(&mut self, key: &str, by: i64) -> Result<i64> {
        let value: i64 = self
            .connection
            .incr(self.prefixed_key(key), by)
            .await
            .map_err(|e| Error::CacheError(format!("Redis increment error: {}", e)))?;
        Ok(value)
    }

    /// Get the remaining TTL for a key in seconds - todo
    pub async fn get_remaining_ttl() {
        todo!()
    }

    /// Get multiple keys at once
    pub async fn get_many(&mut self, keys: &[&str]) -> Result<Vec<Option<String>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        // Convert to prefixed keys
        let prefixed_keys: Vec<String> = keys.iter().map(|k| self.prefixed_key(k)).collect();

        // Use MGET for better performance
        let values: Vec<Option<String>> = self
            .connection
            .mget(prefixed_keys)
            .await
            .map_err(|e| Error::CacheError(format!("Redis mget error: {}", e)))?;

        Ok(values)
    }

    /// Flush all keys from the current database
    pub async fn flush_db(&mut self) -> Result<()> {
        // Use the cmd method to execute FLUSHDB command
        redis::cmd("FLUSHDB")
            .query_async::<()>(&mut self.connection)
            .await
            .map_err(|e| Error::CacheError(format!("Redis flushdb error: {}", e)))?;

        Ok(())
    }

    /// Return the raw multiplexed connection for advanced operations
    pub fn get_connection(&self) -> MultiplexedConnection {
        self.connection.clone()
    }
}

/// Factory for creating cache managers
pub struct CacheManagerFactory;

impl CacheManagerFactory {
    /// Create a new Redis cache manager
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

    /// Create a new Redis cache manager with RESP3 protocol
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
