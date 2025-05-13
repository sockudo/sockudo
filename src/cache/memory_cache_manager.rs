use crate::cache::manager::CacheManager;
use crate::error::{Error, Result}; // Assuming Error/Result are defined elsewhere
use async_trait::async_trait;
use moka::future::Cache;
use moka::Expiry; // Needed for ttl() method
use std::sync::{Arc, Mutex}; // Keep Mutex due to CacheFactory signature
use std::time::{Duration, Instant};

// --- Error/Result Placeholder (if not defined elsewhere) ---
// pub mod error {
//     #[derive(Debug)]
//     pub enum Error {
//         // Define potential errors if needed
//         Other(String),
//     }
//     pub type Result<T> = std::result::Result<T, Error>;
// }
// --- CacheManager Trait Placeholder ---
// pub mod cache {
//     pub mod manager {
//         use crate::error::Result;
//         use async_trait::async_trait;
//         use std::sync::{Arc, Mutex};
//         use std::time::Duration;

//         #[async_trait]
//         pub trait CacheManager {
//             async fn has(&mut self, key: &str) -> Result<bool>;
//             async fn get(&mut self, key: &str) -> Result<Option<String>>;
//             async fn set(&mut self, key: &str, value: &str, ttl_seconds: u64) -> Result<()>;
//             async fn disconnect(&self) -> Result<()>; // Changed to &self as moka clear doesn't need mut
//             async fn is_healthy(&self) -> Result<bool>;
//         }
//     }
//     // --- CacheFactory Placeholder ---
//     pub mod factory {
//         use crate::cache::manager::CacheManager;
//         use crate::error::Result;
//         use std::sync::{Arc, Mutex};
//         pub struct CacheFactory;
//         impl CacheFactory {
//              // Implementation will be added below
//         }
//     }
// }
// --- End Placeholders ---

/// Configuration for the Memory cache manager using Moka
#[derive(Clone, Debug)]
pub struct MemoryCacheConfig {
    /// Key prefix
    pub prefix: String,
    /// Response timeout (Not directly used by Moka cache itself, but kept for potential higher-level logic)
    pub response_timeout: Option<Duration>,
    /// Global TTL for entries if not specified during set operation.
    /// If None, entries live forever by default unless TTL is given in `set`.
    pub default_ttl: Option<Duration>,
    /// Maximum number of entries in the cache.
    pub max_capacity: u64,
    // Removed cleanup_interval_ms as Moka handles it
}

impl Default for MemoryCacheConfig {
    fn default() -> Self {
        Self {
            prefix: "memory_cache".to_string(),
            response_timeout: Some(Duration::from_secs(5)),
            default_ttl: Some(Duration::from_secs(3600)), // 1 hour default TTL
            max_capacity: 10_000,                         // Default max capacity
        }
    }
}

// CacheEntry struct is no longer needed as Moka manages values and expiry internally.

/// A Memory-based implementation of the CacheManager trait using Moka.
pub struct MemoryCacheManager {
    /// Moka async cache for storing entries. Key and Value are Strings.
    cache: Cache<String, String>,
    /// Configuration
    config: MemoryCacheConfig,
    // cleanup_task and shutdown_tx are no longer needed.
}

impl MemoryCacheManager {
    /// Creates a new Memory cache manager with Moka configuration.
    pub fn new(config: MemoryCacheConfig) -> Self {
        let cache_builder = Cache::builder().max_capacity(config.max_capacity);

        // Set default time_to_live if provided
        let cache = if let Some(ttl) = config.default_ttl {
            cache_builder.time_to_live(ttl).build()
        } else {
            // No default TTL, entries live forever unless specified in set()
            cache_builder.build()
        };

        Self { cache, config }
        // No need to manually start a cleanup task.
    }

    /// Creates a new Memory cache manager with simple configuration (prefix only).
    pub fn with_prefix(prefix: &str) -> Self {
        let config = MemoryCacheConfig {
            prefix: prefix.to_string(),
            ..Default::default()
        };
        Self::new(config)
    }

    /// Get the prefixed key.
    fn prefixed_key(&self, key: &str) -> String {
        format!("{}:{}", self.config.prefix, key)
    }

    // cleanup_expired_entries is no longer needed.
    // start_cleanup_task is no longer needed.
}

#[async_trait]
impl CacheManager for MemoryCacheManager {
    /// Check if the given key exists in the Moka cache and is not expired.
    /// Note: `contains_key` is synchronous, but usually very fast.
    /// For a fully async check, one might use `get` and check if Some.
    async fn has(&mut self, key: &str) -> Result<bool> {
        let prefixed_key = self.prefixed_key(key);
        // `contains_key` is sync, but checks existence quickly.
        // Moka's `get` implicitly checks expiry, so calling `get` might be
        // more idiomatic if you need the value soon anyway.
        // Let's use `get` for consistency with expiry checks.
        let exists = self.cache.get(&prefixed_key).await.is_some();
        Ok(exists)
    }

    /// Get a key from the Moka cache.
    /// Returns None if the key does not exist or is expired.
    async fn get(&mut self, key: &str) -> Result<Option<String>> {
        let prefixed_key = self.prefixed_key(key);
        // Moka's get automatically handles expiration.
        Ok(self.cache.get(&prefixed_key).await)
    }

    /// Set or overwrite the value in the Moka cache.
    async fn set(&mut self, key: &str, value: &str, ttl_seconds: u64) -> Result<()> {
        let prefixed_key = self.prefixed_key(key);
        let value_string = value.to_string(); // Moka needs owned values

        if ttl_seconds > 0 {
            // Use specific TTL for this entry
            let ttl = Duration::from_secs(ttl_seconds);
            self.cache.insert(prefixed_key, value_string).await;
        } else if let Some(default_ttl) = self.config.default_ttl {
            // Use the default TTL from config
            self.cache.insert(prefixed_key, value_string).await;
        } else {
            // No TTL specified and no default TTL -> insert without expiration
            self.cache.insert(prefixed_key, value_string).await;
        }

        Ok(())
    }

    /// Invalidate all entries in the cache. Moka doesn't have a persistent connection concept.
    /// Changed signature to `&self` as `invalidate_all` does not require `&mut self`.
    /// Kept original trait signature for compatibility if needed, but implementation uses `&self`.
    async fn disconnect(&self) -> Result<()> {
        // Invalidate all entries in this specific cache instance.
        self.cache.invalidate_all();
        // Moka caches are automatically managed in terms of resources,
        // explicit disconnect isn't usually needed beyond clearing.
        Ok(())
    }

    /// Check if the cache is healthy (always true for Moka in-memory cache).
    async fn is_healthy(&self) -> Result<bool> {
        // Moka cache is generally always "healthy" unless the process runs out of memory.
        Ok(true)
    }

    async fn ttl(&mut self, key: &str) -> Result<Option<Duration>> {
        let prefixed_key = self.prefixed_key(key);
        // Moka doesn't expose TTL directly, but we can check if the key exists
        if self.cache.contains_key(&prefixed_key) {
            Ok(Some(self.config.default_ttl.unwrap_or_default()))
        } else {
            Ok(None)
        }
    }
}

// Drop implementation is no longer needed for cleanup task management.
// Moka's cache handles its resources automatically.
// impl Drop for MemoryCacheManager {
//     fn drop(&mut self) {
//         // No explicit cleanup needed for Moka cache itself.
//     }
// }

/// Extension methods for MemoryCacheManager using Moka
impl MemoryCacheManager {
    /// Delete a key from the cache.
    pub async fn delete(&mut self, key: &str) -> Result<bool> {
        let prefixed_key = self.prefixed_key(key);
        // Moka's invalidate doesn't return if the key existed.
        // We can check first for similar behavior, though less efficient.
        let exists = self.cache.contains_key(&prefixed_key); // Sync check
        if exists {
            self.cache.invalidate(&prefixed_key).await;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get multiple keys at once.
    pub async fn get_many(&mut self, keys: &[&str]) -> Result<Vec<Option<String>>> {
        let mut results = Vec::with_capacity(keys.len());
        for &key in keys {
            // Reuse the existing get method which handles prefixing and expiry
            results.push(self.get(key).await?);
        }
        Ok(results)
    }

    /// Set multiple key-value pairs at once.
    pub async fn set_many(&mut self, pairs: &[(&str, &str)], ttl_seconds: u64) -> Result<()> {
        // Determine TTL outside the loop
        let ttl_duration = if ttl_seconds > 0 {
            Some(Duration::from_secs(ttl_seconds))
        } else {
            self.config.default_ttl // Use default TTL if specified, otherwise None
        };

        for (key, value) in pairs {
            let prefixed_key = self.prefixed_key(key);
            let value_string = value.to_string();
            self.cache.insert(prefixed_key, value_string).await;
        }
        Ok(())
    }
}

/// Update the cache manager factory to support Moka-based memory cache
impl crate::cache::factory::CacheFactory {
    /// Create a new memory cache manager using Moka
    pub fn create_memory(prefix: Option<&str>) -> Result<Arc<Mutex<Box<dyn CacheManager + Send>>>> {
        let config = MemoryCacheConfig {
            prefix: prefix.unwrap_or("memory_cache").to_string(),
            ..Default::default()
        };

        let cache_manager = MemoryCacheManager::new(config);
        // No need to call start_cleanup_task()

        // Wrap in Arc<Mutex<Box<...>>> to match the expected factory signature.
        // Even though Moka Cache is internally thread-safe (&self methods),
        // the CacheManager trait requires &mut self, hence the Mutex is needed here
        // to satisfy the trait bounds provided in the original code.
        Ok(Arc::new(Mutex::new(Box::new(cache_manager))))
    }
}
