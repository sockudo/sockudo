use crate::error::Result;
use async_trait::async_trait;
use std::time::Duration;

// Cache Manager Interface trait
#[async_trait]
pub trait CacheManager: Send + Sync {
    /// Check if the given key exists in cache
    async fn has(&self, key: &str) -> Result<bool>;

    /// Get a key from the cache
    /// Returns None if cache does not exist
    async fn get(&self, key: &str) -> Result<Option<String>>;

    /// Set or overwrite the value in the cache
    async fn set(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<()>;

    /// Remove a key from the cache
    async fn remove(&self, key: &str) -> Result<()>;

    /// Disconnect the manager's made connections
    async fn disconnect(&self) -> Result<()>;

    /// Health check for the cache manager
    async fn check_health(&self) -> Result<()> {
        // Default implementation - always healthy for memory/no-op caches
        Ok(())
    }

    async fn ttl(&self, key: &str) -> Result<Option<Duration>>;
}
