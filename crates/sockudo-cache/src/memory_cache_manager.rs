use async_trait::async_trait;
use moka::{
    future::Cache,
    ops::compute::{CompResult, Op},
};
use sockudo_core::cache::{CacheManager, CacheScanPage};
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

    async fn scan_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, String)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut entries = Vec::with_capacity(limit.min(64));
        let cache_prefix = format!("{}:", self.prefix);
        let prefix_len = cache_prefix.len();

        for (key, value) in self.cache.iter() {
            if entries.len() >= limit {
                break;
            }
            if !key.starts_with(&cache_prefix) {
                continue;
            }
            let unprefixed_key = &key[prefix_len..];
            if unprefixed_key.starts_with(prefix) {
                entries.push((unprefixed_key.to_string(), value.clone()));
            }
        }

        Ok(entries)
    }

    async fn scan_prefix_page(
        &self,
        prefix: &str,
        cursor: Option<String>,
        limit: usize,
    ) -> Result<CacheScanPage> {
        if limit == 0 {
            return Ok(CacheScanPage::default());
        }

        let cache_prefix = format!("{}:", self.prefix);
        let prefix_len = cache_prefix.len();
        let mut matching = self
            .cache
            .iter()
            .filter_map(|(key, value)| {
                if !key.starts_with(&cache_prefix) {
                    return None;
                }
                let unprefixed_key = key[prefix_len..].to_string();
                if unprefixed_key.starts_with(prefix) {
                    Some((unprefixed_key, value))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        matching.sort_by(|left, right| left.0.cmp(&right.0));

        let start = cursor
            .as_deref()
            .and_then(|cursor| matching.iter().position(|(key, _)| key.as_str() > cursor))
            .unwrap_or(0);
        let end = start.saturating_add(limit).min(matching.len());
        let entries = matching[start..end].to_vec();
        let next_cursor = if end < matching.len() {
            entries.last().map(|(key, _)| key.clone())
        } else {
            None
        };

        Ok(CacheScanPage {
            entries,
            next_cursor,
        })
    }

    async fn set_if_not_exists(&self, key: &str, value: &str, _ttl_seconds: u64) -> Result<bool> {
        let prefixed_key = self.prefixed_key(key);
        let value = value.to_string();
        let result = self
            .cache
            .entry(prefixed_key)
            .and_compute_with(|entry| {
                let operation = if entry.is_none() {
                    Op::Put(value)
                } else {
                    Op::Nop
                };
                std::future::ready(operation)
            })
            .await;
        Ok(matches!(result, CompResult::Inserted(_)))
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected: &str,
        value: &str,
        _ttl_seconds: u64,
    ) -> Result<bool> {
        let prefixed_key = self.prefixed_key(key);
        let expected = expected.to_string();
        let value = value.to_string();
        let result = self
            .cache
            .entry(prefixed_key)
            .and_compute_with(|entry| {
                let operation = match entry {
                    Some(entry) if entry.value() == &expected => Op::Put(value),
                    _ => Op::Nop,
                };
                std::future::ready(operation)
            })
            .await;
        Ok(matches!(result, CompResult::ReplacedWith(_)))
    }

    async fn compare_and_remove(&self, key: &str, expected: &str) -> Result<bool> {
        let prefixed_key = self.prefixed_key(key);
        let expected = expected.to_string();
        let result = self
            .cache
            .entry(prefixed_key)
            .and_compute_with(|entry| {
                let operation = match entry {
                    Some(entry) if entry.value() == &expected => Op::Remove,
                    _ => Op::Nop,
                };
                std::future::ready(operation)
            })
            .await;
        Ok(matches!(result, CompResult::Removed(_)))
    }

    async fn increment_by(&self, key: &str, delta: i64, _ttl_seconds: u64) -> Result<i64> {
        let prefixed_key = self.prefixed_key(key);
        let entry = self
            .cache
            .entry(prefixed_key)
            .and_upsert_with(|entry| {
                let next = entry
                    .and_then(|entry| entry.into_value().parse::<i64>().ok())
                    .unwrap_or(0)
                    .saturating_add(delta);
                std::future::ready(next.to_string())
            })
            .await;
        Ok(entry.into_value().parse::<i64>().unwrap_or(0))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn test_cache() -> MemoryCacheManager {
        MemoryCacheManager::new(
            "compare".to_string(),
            MemoryCacheOptions {
                ttl: 60,
                cleanup_interval: 60,
                max_capacity: 1_000,
            },
        )
    }

    #[tokio::test]
    async fn increment_by_serializes_concurrent_updates() {
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions {
                ttl: 60,
                cleanup_interval: 60,
                max_capacity: 1_000,
            },
        ));

        let handles = (0..128)
            .map(|_| {
                let cache = Arc::clone(&cache);
                tokio::spawn(async move { cache.increment_by("counter", 1, 60).await })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        assert_eq!(cache.get("counter").await.unwrap().as_deref(), Some("128"));
    }

    #[tokio::test]
    async fn set_if_not_exists_has_one_winner_under_concurrency() {
        let cache = Arc::new(MemoryCacheManager::new(
            "set-once".to_string(),
            MemoryCacheOptions {
                ttl: 60,
                cleanup_interval: 60,
                max_capacity: 1_000,
            },
        ));

        let handles = (0..128)
            .map(|index| {
                let cache = Arc::clone(&cache);
                tokio::spawn(async move {
                    cache
                        .set_if_not_exists("same-key", &index.to_string(), 60)
                        .await
                })
            })
            .collect::<Vec<_>>();
        let mut winners = 0;
        for handle in handles {
            winners += usize::from(handle.await.unwrap().unwrap());
        }

        assert_eq!(winners, 1);
        assert!(cache.get("same-key").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn set_if_not_exists_has_one_winner_in_repeated_synchronized_races() {
        const ROUNDS: usize = 64;
        const CONTENDERS: usize = 16;

        let cache = Arc::new(MemoryCacheManager::new(
            "set-once-synchronized".to_string(),
            MemoryCacheOptions {
                ttl: 60,
                cleanup_interval: 60,
                max_capacity: 10_000,
            },
        ));

        for round in 0..ROUNDS {
            let barrier = Arc::new(tokio::sync::Barrier::new(CONTENDERS));
            let handles = (0..CONTENDERS)
                .map(|contender| {
                    let cache = Arc::clone(&cache);
                    let barrier = Arc::clone(&barrier);
                    tokio::spawn(async move {
                        barrier.wait().await;
                        cache
                            .set_if_not_exists(&format!("race-{round}"), &contender.to_string(), 60)
                            .await
                    })
                })
                .collect::<Vec<_>>();

            let mut winners = 0;
            for handle in handles {
                winners += usize::from(handle.await.unwrap().unwrap());
            }
            assert_eq!(winners, 1, "round {round} must have exactly one winner");
        }
    }

    #[tokio::test]
    async fn compare_and_swap_requires_the_expected_value() {
        let cache = test_cache();
        cache.set("receipt", "pending-owner-a", 60).await.unwrap();

        assert!(
            !cache
                .compare_and_swap("receipt", "pending-owner-b", "committed-b", 60)
                .await
                .unwrap()
        );
        assert_eq!(
            cache.get("receipt").await.unwrap().as_deref(),
            Some("pending-owner-a")
        );

        assert!(
            cache
                .compare_and_swap("receipt", "pending-owner-a", "committed-a", 60)
                .await
                .unwrap()
        );
        assert_eq!(
            cache.get("receipt").await.unwrap().as_deref(),
            Some("committed-a")
        );
    }

    #[tokio::test]
    async fn compare_and_remove_cannot_release_another_owner_claim() {
        let cache = test_cache();
        cache.set("claim", "owner-b", 60).await.unwrap();

        assert!(!cache.compare_and_remove("claim", "owner-a").await.unwrap());
        assert_eq!(
            cache.get("claim").await.unwrap().as_deref(),
            Some("owner-b")
        );

        assert!(cache.compare_and_remove("claim", "owner-b").await.unwrap());
        assert_eq!(cache.get("claim").await.unwrap(), None);
    }

    #[tokio::test]
    async fn concurrent_idempotent_retries_observe_one_committed_receipt() {
        use sockudo_core::idempotency::{
            IdempotencyReceipt, IdempotencyStart, begin_publish, commit_publish,
        };

        let cache = Arc::new(test_cache());
        let receipt = IdempotencyReceipt {
            acknowledgement_id: "serial-1".to_string(),
            message_serial: Some("serial-1".to_string()),
            history_serial: Some(1),
            delivery_serial: Some(1),
            version_serial: Some("serial-1".to_string()),
        };
        let handles = (0..64)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let receipt = receipt.clone();
                tokio::spawn(async move {
                    match begin_publish(
                        cache.as_ref(),
                        "publish-key".to_string(),
                        "same-fingerprint".to_string(),
                        60,
                    )
                    .await
                    .unwrap()
                    {
                        IdempotencyStart::Acquired(claim) => {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            commit_publish(cache.as_ref(), &claim, &receipt)
                                .await
                                .unwrap();
                            receipt
                        }
                        IdempotencyStart::Replay(replayed) => replayed,
                    }
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            assert_eq!(handle.await.unwrap(), receipt);
        }
    }
}
