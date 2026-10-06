use async_trait::async_trait;
use moka::{
    Expiry,
    future::Cache,
    ops::compute::{CompResult, Op},
};
use sockudo_core::cache::{CacheManager, CacheScanPage};
use sockudo_core::error::Result;
use sockudo_core::options::MemoryCacheOptions;
use std::time::{Duration, Instant};

/// A cached value together with the instant it expires at (`None` never expires).
#[derive(Clone)]
struct CachedValue {
    value: String,
    expires_at: Option<Instant>,
}

impl CachedValue {
    fn remaining_ttl(&self) -> Option<Duration> {
        self.expires_at
            .map(|expires_at| expires_at.saturating_duration_since(Instant::now()))
            .filter(|remaining| !remaining.is_zero())
    }
}

/// Expires every entry at its own `expires_at`; writes reset the deadline.
struct PerEntryExpiry;

impl Expiry<String, CachedValue> for PerEntryExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &CachedValue,
        created_at: Instant,
    ) -> Option<Duration> {
        value
            .expires_at
            .map(|expires_at| expires_at.saturating_duration_since(created_at))
    }

    fn expire_after_update(
        &self,
        _key: &String,
        value: &CachedValue,
        updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        value
            .expires_at
            .map(|expires_at| expires_at.saturating_duration_since(updated_at))
    }
}

/// A Memory-based implementation of the CacheManager trait using Moka.
///
/// Entries expire after the `ttl_seconds` passed to each write, capped by the cache-wide
/// `options.ttl`. A `ttl_seconds` of 0 uses `options.ttl`, and when that is also 0 the entry
/// does not expire (like a Redis `SET` without `EX`).
#[derive(Clone)]
pub struct MemoryCacheManager {
    /// Moka async cache for storing entries, keyed by prefixed key.
    cache: Cache<String, CachedValue, ahash::RandomState>,
    /// Configuration options for this cache instance.
    options: MemoryCacheOptions,
    /// Prefix for all keys in this cache instance.
    prefix: String,
}

impl MemoryCacheManager {
    /// Creates a new Memory cache manager with Moka configuration.
    pub fn new(prefix: String, options: MemoryCacheOptions) -> Self {
        let cache = Cache::builder()
            .max_capacity(options.max_capacity)
            .name(format!("sockudo-memory-cache-{prefix}").as_str())
            .expire_after(PerEntryExpiry)
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

    /// Resolves a write's TTL against the cache-wide TTL, which is both the default (for 0)
    /// and the upper bound.
    fn effective_ttl(&self, ttl_seconds: u64) -> Option<Duration> {
        let seconds = match (ttl_seconds, self.options.ttl) {
            (0, 0) => return None,
            (0, global) => global,
            (requested, 0) => requested,
            (requested, global) => requested.min(global),
        };
        Some(Duration::from_secs(seconds))
    }

    fn cached_value(&self, value: String, ttl_seconds: u64) -> CachedValue {
        CachedValue {
            value,
            expires_at: self
                .effective_ttl(ttl_seconds)
                .map(|ttl| Instant::now() + ttl),
        }
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
        Ok(self
            .cache
            .get(&prefixed_key)
            .await
            .map(|cached| cached.value))
    }

    async fn set(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<()> {
        let prefixed_key = self.prefixed_key(key);
        let cached = self.cached_value(value.to_string(), ttl_seconds);

        self.cache.insert(prefixed_key, cached).await;
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
        Ok(self
            .cache
            .get(&prefixed_key)
            .await
            .and_then(|cached| cached.remaining_ttl()))
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
                entries.push((unprefixed_key.to_string(), value.value));
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
                    Some((unprefixed_key, value.value))
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

    async fn set_if_not_exists(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<bool> {
        let prefixed_key = self.prefixed_key(key);
        let value = self.cached_value(value.to_string(), ttl_seconds);
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
        ttl_seconds: u64,
    ) -> Result<bool> {
        let prefixed_key = self.prefixed_key(key);
        let expected = expected.to_string();
        let value = self.cached_value(value.to_string(), ttl_seconds);
        let result = self
            .cache
            .entry(prefixed_key)
            .and_compute_with(|entry| {
                let operation = match entry {
                    Some(entry) if entry.value().value == expected => Op::Put(value),
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
                    Some(entry) if entry.value().value == expected => Op::Remove,
                    _ => Op::Nop,
                };
                std::future::ready(operation)
            })
            .await;
        Ok(matches!(result, CompResult::Removed(_)))
    }

    async fn increment_by(&self, key: &str, delta: i64, ttl_seconds: u64) -> Result<i64> {
        let prefixed_key = self.prefixed_key(key);
        let entry = self
            .cache
            .entry(prefixed_key)
            .and_upsert_with(|entry| {
                let current = entry.map(|entry| entry.into_value());
                let next = current
                    .as_ref()
                    .and_then(|current| current.value.parse::<i64>().ok())
                    .unwrap_or(0)
                    .saturating_add(delta)
                    .to_string();
                // Like Redis INCRBY + EXPIRE: a TTL resets the deadline, 0 keeps the existing one.
                let next = match current {
                    Some(current) if ttl_seconds == 0 => CachedValue {
                        value: next,
                        expires_at: current.expires_at,
                    },
                    _ => self.cached_value(next, ttl_seconds),
                };
                std::future::ready(next)
            })
            .await;
        Ok(entry.into_value().value.parse::<i64>().unwrap_or(0))
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
    pub async fn set_many(&mut self, pairs: &[(&str, &str)], ttl_seconds: u64) -> Result<()> {
        for (key, value) in pairs {
            let prefixed_key = self.prefixed_key(key);
            let cached = self.cached_value(value.to_string(), ttl_seconds);
            self.cache.insert(prefixed_key, cached).await;
        }
        Ok(())
    }

    /// Get all entries from the cache as (key, value, remaining ttl) tuples.
    /// Returns entries without the prefix; the TTL is `None` for entries that never expire.
    pub async fn get_all_entries(&self) -> Vec<(String, String, Option<Duration>)> {
        let mut entries = Vec::new();
        let cache_prefix = format!("{}:", self.prefix);
        let prefix_len = cache_prefix.len();

        for (key, cached) in self.cache.iter() {
            if key.starts_with(&cache_prefix) {
                let unprefixed_key = key[prefix_len..].to_string();
                let ttl = cached.remaining_ttl();
                if cached.expires_at.is_some() && ttl.is_none() {
                    continue;
                }
                entries.push((unprefixed_key, cached.value, ttl));
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

    fn cache_with_global_ttl(ttl: u64) -> MemoryCacheManager {
        MemoryCacheManager::new(
            "ttl".to_string(),
            MemoryCacheOptions {
                ttl,
                cleanup_interval: 60,
                max_capacity: 1_000,
            },
        )
    }

    #[tokio::test]
    async fn per_key_ttl_expires_before_global_ttl() {
        let cache = cache_with_global_ttl(300);
        cache.set("short", "value", 1).await.unwrap();
        cache.set("long", "value", 60).await.unwrap();
        assert!(cache.has("short").await.unwrap());

        tokio::time::sleep(Duration::from_millis(1_100)).await;

        assert_eq!(cache.get("short").await.unwrap(), None);
        assert!(!cache.has("short").await.unwrap());
        assert_eq!(cache.ttl("short").await.unwrap(), None);
        assert_eq!(cache.scan_prefix("", 10).await.unwrap().len(), 1);
        assert_eq!(cache.get("long").await.unwrap().as_deref(), Some("value"));
    }

    #[tokio::test]
    async fn conditional_writes_honor_per_key_ttl() {
        let cache = cache_with_global_ttl(300);
        assert!(cache.set_if_not_exists("claim", "a", 1).await.unwrap());
        cache.set("swap", "old", 60).await.unwrap();
        assert!(
            cache
                .compare_and_swap("swap", "old", "new", 1)
                .await
                .unwrap()
        );
        cache.increment_by("counter", 1, 1).await.unwrap();

        tokio::time::sleep(Duration::from_millis(1_100)).await;

        assert_eq!(cache.get("claim").await.unwrap(), None);
        assert_eq!(cache.get("swap").await.unwrap(), None);
        assert_eq!(cache.get("counter").await.unwrap(), None);
        assert!(cache.set_if_not_exists("claim", "b", 1).await.unwrap());
    }

    #[tokio::test]
    async fn rewriting_a_key_resets_its_ttl() {
        let cache = cache_with_global_ttl(300);
        cache.set("key", "first", 1).await.unwrap();
        cache.set("key", "second", 60).await.unwrap();

        tokio::time::sleep(Duration::from_millis(1_100)).await;

        assert_eq!(cache.get("key").await.unwrap().as_deref(), Some("second"));
    }

    #[tokio::test]
    async fn ttl_reports_the_remaining_per_key_ttl() {
        let cache = cache_with_global_ttl(300);
        cache.set("key", "value", 10).await.unwrap();

        let ttl = cache.ttl("key").await.unwrap().expect("key has a ttl");
        assert!(ttl <= Duration::from_secs(10) && ttl > Duration::from_secs(9));
        assert_eq!(cache.ttl("missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn global_ttl_caps_longer_per_key_ttls() {
        let cache = cache_with_global_ttl(5);
        cache.set("key", "value", 3_600).await.unwrap();

        let ttl = cache.ttl("key").await.unwrap().expect("key has a ttl");
        assert!(ttl <= Duration::from_secs(5) && ttl > Duration::from_secs(4));
    }

    #[tokio::test]
    async fn zero_ttl_uses_the_global_ttl() {
        let cache = cache_with_global_ttl(30);
        cache.set("key", "value", 0).await.unwrap();

        let ttl = cache.ttl("key").await.unwrap().expect("key has a ttl");
        assert!(ttl <= Duration::from_secs(30) && ttl > Duration::from_secs(29));
    }

    #[tokio::test]
    async fn zero_ttl_without_global_ttl_never_expires() {
        let cache = cache_with_global_ttl(0);
        cache.set("forever", "value", 0).await.unwrap();
        cache.set("short", "value", 1).await.unwrap();

        assert_eq!(cache.ttl("forever").await.unwrap(), None);
        assert!(cache.ttl("short").await.unwrap().is_some());

        tokio::time::sleep(Duration::from_millis(1_100)).await;

        assert_eq!(
            cache.get("forever").await.unwrap().as_deref(),
            Some("value")
        );
        assert_eq!(cache.get("short").await.unwrap(), None);
    }

    #[tokio::test]
    async fn increment_by_with_zero_ttl_keeps_the_existing_deadline() {
        let cache = cache_with_global_ttl(300);
        cache.increment_by("counter", 1, 10).await.unwrap();
        cache.increment_by("counter", 1, 0).await.unwrap();

        let ttl = cache
            .ttl("counter")
            .await
            .unwrap()
            .expect("counter has a ttl");
        assert!(ttl <= Duration::from_secs(10));
        assert_eq!(cache.get("counter").await.unwrap().as_deref(), Some("2"));
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
