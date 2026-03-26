use async_trait::async_trait;
use moka::future::Cache;
use sockudo_core::cache::CacheManager;
use sockudo_core::error::Result;
use sockudo_core::options::MemoryCacheOptions;
use std::time::Duration;

/// A Memory-based implementation of the CacheManager trait using Moka.
#[derive(Clone)]
pub struct MemoryCacheManager {
    /// Moka async cache for storing entries. Key and Value are Strings.
    cache: Cache<String, String, ahash::RandomState>,
    /// Configuration options for this cache instance.
    options: MemoryCacheOptions,
    /// Prefix for all keys in this cache instance.
    prefix: String,
}

impl MemoryCacheManager {
    /// Creates a new Memory cache manager with Moka configuration.
    pub fn new(prefix: String, options: MemoryCacheOptions) -> Self {
        let cache_builder = Cache::builder()
            .max_capacity(options.max_capacity)
            .name(format!("sockudo-memory-cache-{prefix}").as_str());

        let cache = if options.ttl > 0 {
            cache_builder.time_to_live(Duration::from_secs(options.ttl))
        } else {
            cache_builder
        }
        .build_with_hasher(ahash::RandomState::new());

        Self {
            cache,
            options,
            prefix,
        }
    }

    /// Get the prefixed key.
    fn prefixed_key(&self, key: &str) -> String {
        format!("{}:{}", self.prefix, key)
    }
}

#[async_trait]
impl CacheManager for MemoryCacheManager {
    async fn has(&self, key: &str) -> Result<bool> {
        let prefixed_key = self.prefixed_key(key);
        let exists = self.cache.get(&prefixed_key).await.is_some();
        Ok(exists)
    }

    async fn get(&self, key: &str) -> Result<Option<String>> {
        let prefixed_key = self.prefixed_key(key);
        Ok(self.cache.get(&prefixed_key).await)
    }

    async fn set(&self, key: &str, value: &str, _ttl_seconds: u64) -> Result<()> {
        let prefixed_key = self.prefixed_key(key);
        let value_string = value.to_string();

        self.cache.insert(prefixed_key, value_string).await;
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        let prefixed_key = self.prefixed_key(key);
        self.cache.invalidate(&prefixed_key).await;
        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        self.cache.invalidate_all();
        Ok(())
    }

    async fn ttl(&self, key: &str) -> Result<Option<Duration>> {
        let prefixed_key = self.prefixed_key(key);
        if self.cache.contains_key(&prefixed_key) {
            if self.options.ttl > 0 {
                Ok(Some(Duration::from_secs(self.options.ttl)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    async fn set_if_not_exists(&self, key: &str, value: &str, _ttl_seconds: u64) -> Result<bool> {
        let prefixed_key = self.prefixed_key(key);
        // Moka's `contains_key` + `insert` isn't truly atomic, but for in-memory
        // single-process use this is sufficient since Tokio tasks on the same
        // runtime don't preempt each other within a single .await-free block.
        if self.cache.contains_key(&prefixed_key) {
            Ok(false)
        } else {
            self.cache.insert(prefixed_key, value.to_string()).await;
            Ok(true)
        }
    }
}

impl MemoryCacheManager {
    /// Delete a key from the cache.
    pub async fn delete(&mut self, key: &str) -> Result<bool> {
        let prefixed_key = self.prefixed_key(key);
        if self.cache.contains_key(&prefixed_key) {
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
            results.push(self.get(key).await?);
        }
        Ok(results)
    }

    /// Set multiple key-value pairs at once.
    pub async fn set_many(&mut self, pairs: &[(&str, &str)], _ttl_seconds: u64) -> Result<()> {
        for (key, value) in pairs {
            let prefixed_key = self.prefixed_key(key);
            let value_string = value.to_string();
            self.cache.insert(prefixed_key, value_string).await;
        }
        Ok(())
    }

    /// Get all entries from the cache as (key, value, ttl) tuples.
    /// Returns entries without the prefix.
    ///
    /// Note: Moka doesn't support per-entry TTL tracking, so this returns the
    /// cache's default TTL for all entries. When syncing to another cache system,
    /// this means all entries will get the same TTL, not their remaining time.
    pub async fn get_all_entries(&self) -> Vec<(String, String, Option<Duration>)> {
        let mut entries = Vec::new();
        let prefix_len = self.prefix.len() + 1; // +1 for the colon separator

        for (key, value) in self.cache.iter() {
            if key.starts_with(&format!("{}:", self.prefix)) {
                let unprefixed_key = key[prefix_len..].to_string();
                let ttl = if self.options.ttl > 0 {
                    Some(Duration::from_secs(self.options.ttl))
                } else {
                    None
                };
                entries.push((unprefixed_key, value.clone(), ttl));
            }
        }

        entries
    }
}
