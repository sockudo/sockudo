use crate::cache::manager::CacheManager;
use crate::cache::memory_cache_manager::MemoryCacheManager;
use crate::error::Result;
use crate::options::MemoryCacheOptions;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, warn};

const RECOVERY_CHECK_INTERVAL_SECS: u64 = 30;

/// Cache manager that wraps a primary (Redis/Redis Cluster) cache and automatically
/// falls back to in-memory cache when the primary becomes unavailable.
///
/// When the primary cache recovers, data from the fallback cache is synchronized
/// back to the primary before switching over to prevent data loss.
pub struct FallbackCacheManager {
    primary: Mutex<Box<dyn CacheManager + Send + Sync>>,
    fallback: Mutex<MemoryCacheManager>,
    using_fallback: AtomicBool,
    last_failure_time: AtomicU64,
    start_time: Instant,
    /// Guards state transitions to prevent race conditions during recovery
    recovery_lock: RwLock<()>,
}

impl FallbackCacheManager {
    pub fn new(
        primary: Box<dyn CacheManager + Send + Sync>,
        fallback_options: MemoryCacheOptions,
    ) -> Self {
        let fallback = MemoryCacheManager::new("fallback_cache".to_string(), fallback_options);

        Self {
            primary: Mutex::new(primary),
            fallback: Mutex::new(fallback),
            using_fallback: AtomicBool::new(false),
            last_failure_time: AtomicU64::new(0),
            start_time: Instant::now(),
            recovery_lock: RwLock::new(()),
        }
    }

    fn is_using_fallback(&self) -> bool {
        self.using_fallback.load(Ordering::SeqCst)
    }

    fn switch_to_fallback(&self, error: &str) {
        if !self.using_fallback.swap(true, Ordering::SeqCst) {
            warn!(
                "Redis cache unavailable, switching to in-memory fallback. Error: {}",
                error
            );
            self.last_failure_time
                .store(self.start_time.elapsed().as_secs(), Ordering::SeqCst);
        }
    }

    fn should_attempt_recovery(&self) -> bool {
        if !self.is_using_fallback() {
            return false;
        }

        let last_failure = self.last_failure_time.load(Ordering::SeqCst);
        let current_time = self.start_time.elapsed().as_secs();

        current_time.saturating_sub(last_failure) >= RECOVERY_CHECK_INTERVAL_SECS
    }

    async fn try_recover(&self) -> bool {
        if !self.should_attempt_recovery() {
            return false;
        }

        // Acquire write lock to prevent concurrent recovery attempts and operations
        let _recovery_guard = self.recovery_lock.write().await;

        // Double-check after acquiring lock - another thread may have already recovered
        if !self.is_using_fallback() {
            return false;
        }

        debug!("Attempting to recover Redis cache connection...");

        let mut primary = self.primary.lock().await;
        match primary.check_health().await {
            Ok(()) => {
                info!("Redis cache connection recovered, synchronizing fallback data...");
                
                // Sync data from fallback to primary before switching
                if let Err(e) = self.sync_fallback_to_primary(&mut primary).await {
                    warn!("Failed to sync fallback data to primary during recovery: {}", e);
                    // Continue with recovery despite sync failure - primary is healthy
                }
                
                self.using_fallback.store(false, Ordering::SeqCst);
                info!("Successfully switched back to primary cache after recovery");
                true
            }
            Err(e) => {
                debug!("Redis cache still unavailable: {}", e);
                self.last_failure_time
                    .store(self.start_time.elapsed().as_secs(), Ordering::SeqCst);
                false
            }
        }
    }

    /// Synchronizes data from fallback cache to primary cache during recovery.
    /// This prevents data loss for entries written during the outage.
    async fn sync_fallback_to_primary(
        &self,
        primary: &mut Box<dyn CacheManager + Send + Sync>,
    ) -> Result<()> {
        let fallback = self.fallback.lock().await;
        
        // Get all entries from fallback cache
        let entries = fallback.get_all_entries().await;
        
        if entries.is_empty() {
            debug!("No entries in fallback cache to sync");
            return Ok(());
        }
        
        debug!("Syncing {} entries from fallback to primary cache", entries.len());
        
        let mut synced = 0;
        let mut failed = 0;
        
        for (key, value, ttl) in entries {
            let ttl_seconds = ttl.map(|d| d.as_secs()).unwrap_or(0);
            match primary.set(&key, &value, ttl_seconds).await {
                Ok(()) => synced += 1,
                Err(e) => {
                    warn!("Failed to sync key '{}' to primary cache: {}", key, e);
                    failed += 1;
                }
            }
        }
        
        if failed > 0 {
            warn!(
                "Synced {}/{} entries from fallback to primary ({} failed)",
                synced,
                synced + failed,
                failed
            );
        } else {
            info!("Successfully synced {} entries from fallback to primary cache", synced);
        }
        
        Ok(())
    }
}

#[async_trait]
impl CacheManager for FallbackCacheManager {
    async fn has(&mut self, key: &str) -> Result<bool> {
        self.try_recover().await;

        // Acquire read lock to prevent state changes during operation
        let _guard = self.recovery_lock.read().await;

        if self.is_using_fallback() {
            return self.fallback.lock().await.has(key).await;
        }

        let mut primary = self.primary.lock().await;
        match primary.has(key).await {
            Ok(result) => Ok(result),
            Err(e) => {
                // Release locks before switching to fallback
                drop(primary);
                drop(_guard);
                self.switch_to_fallback(&e.to_string());
                self.fallback.lock().await.has(key).await
            }
        }
    }

    async fn get(&mut self, key: &str) -> Result<Option<String>> {
        self.try_recover().await;

        // Acquire read lock to prevent state changes during operation
        let _guard = self.recovery_lock.read().await;

        if self.is_using_fallback() {
            return self.fallback.lock().await.get(key).await;
        }

        let mut primary = self.primary.lock().await;
        match primary.get(key).await {
            Ok(result) => Ok(result),
            Err(e) => {
                // Release locks before switching to fallback
                drop(primary);
                drop(_guard);
                self.switch_to_fallback(&e.to_string());
                self.fallback.lock().await.get(key).await
            }
        }
    }

    async fn set(&mut self, key: &str, value: &str, ttl_seconds: u64) -> Result<()> {
        self.try_recover().await;

        // Acquire read lock to prevent state changes during operation
        let _guard = self.recovery_lock.read().await;

        if self.is_using_fallback() {
            return self
                .fallback
                .lock()
                .await
                .set(key, value, ttl_seconds)
                .await;
        }

        let mut primary = self.primary.lock().await;
        match primary.set(key, value, ttl_seconds).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Release locks before switching to fallback
                drop(primary);
                drop(_guard);
                self.switch_to_fallback(&e.to_string());
                self.fallback
                    .lock()
                    .await
                    .set(key, value, ttl_seconds)
                    .await
            }
        }
    }

    async fn remove(&mut self, key: &str) -> Result<()> {
        self.try_recover().await;

        // Acquire read lock to prevent state changes during operation
        let _guard = self.recovery_lock.read().await;

        if self.is_using_fallback() {
            return self.fallback.lock().await.remove(key).await;
        }

        let mut primary = self.primary.lock().await;
        match primary.remove(key).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Release locks before switching to fallback
                drop(primary);
                drop(_guard);
                self.switch_to_fallback(&e.to_string());
                self.fallback.lock().await.remove(key).await
            }
        }
    }

    async fn disconnect(&mut self) -> Result<()> {
        let primary_result = self.primary.lock().await.disconnect().await;
        if let Err(ref e) = primary_result {
            warn!(error = ?e, "Failed to disconnect primary cache");
        }

        let fallback_result = self.fallback.lock().await.disconnect().await;
        if let Err(ref e) = fallback_result {
            warn!(error = ?e, "Failed to disconnect fallback cache");
        }

        match (primary_result, fallback_result) {
            (Ok(_), Ok(_)) => Ok(()),
            (Err(e), _) => Err(e),
            (_, Err(e)) => Err(e),
        }
    }

    async fn check_health(&self) -> Result<()> {
        if self.is_using_fallback() {
            return Ok(());
        }

        let primary = self.primary.lock().await;
        primary.check_health().await
    }

    async fn ttl(&mut self, key: &str) -> Result<Option<Duration>> {
        self.try_recover().await;

        // Acquire read lock to prevent state changes during operation
        let _guard = self.recovery_lock.read().await;

        if self.is_using_fallback() {
            return self.fallback.lock().await.ttl(key).await;
        }

        let mut primary = self.primary.lock().await;
        match primary.ttl(key).await {
            Ok(result) => Ok(result),
            Err(e) => {
                // Release locks before switching to fallback
                drop(primary);
                drop(_guard);
                self.switch_to_fallback(&e.to_string());
                self.fallback.lock().await.ttl(key).await
            }
        }
    }
}
