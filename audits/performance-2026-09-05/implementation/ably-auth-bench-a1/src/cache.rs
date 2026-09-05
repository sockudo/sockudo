use crate::error::Result;
use async_trait::async_trait;
use std::time::Duration;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CacheScanPage {
    pub entries: Vec<(String, String)>,
    pub next_cursor: Option<String>,
}

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

    /// Release client resources without deleting shared cache entries.
    ///
    /// Stored values are TTL-managed coordination state and can be shared by
    /// other nodes. Destructive test or administrative cleanup must use an
    /// explicit backend-specific operation instead.
    async fn disconnect(&self) -> Result<()>;

    /// Health check for the cache manager
    async fn check_health(&self) -> Result<()> {
        // Default implementation - always healthy for memory/no-op caches
        Ok(())
    }

    async fn ttl(&self, key: &str) -> Result<Option<Duration>>;

    /// Return up to `limit` unprefixed cache entries whose key starts with `prefix`.
    ///
    /// Implementations must bound the returned set by `limit`; callers use this
    /// for low-frequency janitor work, not publish fan-out.
    async fn scan_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, String)>> {
        let _ = (prefix, limit);
        Ok(Vec::new())
    }

    async fn scan_prefix_page(
        &self,
        prefix: &str,
        cursor: Option<String>,
        limit: usize,
    ) -> Result<CacheScanPage> {
        let _ = cursor;
        Ok(CacheScanPage {
            entries: self.scan_prefix(prefix, limit).await?,
            next_cursor: None,
        })
    }

    /// Atomically set a key only if it does not already exist. Returns `true`
    /// if the key was set (i.e., it did not exist), `false` otherwise.
    /// Default implementation falls back to non-atomic has+set.
    async fn set_if_not_exists(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<bool> {
        if self.has(key).await? {
            return Ok(false);
        }
        self.set(key, value, ttl_seconds).await?;
        Ok(true)
    }

    /// Atomically replace `expected` with `value` while retaining a bounded TTL.
    ///
    /// Distributed implementations must override this default with a server-side
    /// compare-and-swap operation. The fallback is intended only for simple test
    /// doubles which do not coordinate multiple processes.
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: &str,
        value: &str,
        ttl_seconds: u64,
    ) -> Result<bool> {
        if self.get(key).await?.as_deref() != Some(expected) {
            return Ok(false);
        }
        self.set(key, value, ttl_seconds).await?;
        Ok(true)
    }

    /// Atomically remove `key` only when its current value equals `expected`.
    async fn compare_and_remove(&self, key: &str, expected: &str) -> Result<bool> {
        if self.get(key).await?.as_deref() != Some(expected) {
            return Ok(false);
        }
        self.remove(key).await?;
        Ok(true)
    }

    async fn increment_by(&self, key: &str, delta: i64, ttl_seconds: u64) -> Result<i64> {
        let current = self
            .get(key)
            .await?
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let next = current.saturating_add(delta);
        self.set(key, &next.to_string(), ttl_seconds).await?;
        Ok(next)
    }
}
