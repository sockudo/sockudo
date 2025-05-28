use crate::error::Result;
use async_trait::async_trait;
use std::time::Duration;

// Cache Manager Interface trait
#[async_trait]
pub trait CacheManager: Send + Sync {
    /// Check if the given key exists in cache
    async fn has(&mut self, key: &str) -> Result<bool>;

    /// Get a key from the cache
    /// Returns None if cache does not exist
    async fn get(&mut self, key: &str) -> Result<Option<String>>;

    /// Set or overwrite the value in the cache
    async fn set(&mut self, key: &str, value: &str, ttl_seconds: u64) -> Result<()>;
    
    /// Remove a key from the cache
    async fn remove(&mut self, key: &str) -> Result<()>;

    /// Disconnect the manager's made connections
    async fn disconnect(&mut self) -> Result<()>;

    async fn is_healthy(&self) -> Result<bool> {
        Ok(true)
    }
    async fn ttl(&mut self, key: &str) -> Result<Option<Duration>>;
}
