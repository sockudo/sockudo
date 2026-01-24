use crate::cache::manager::CacheManager;
use crate::cache::memory_cache_manager::MemoryCacheManager;
use crate::error::Result;
use crate::options::MemoryCacheOptions;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

const RECOVERY_CHECK_INTERVAL_SECS: u64 = 30;

/// Cache manager that wraps a primary (Redis/Redis Cluster) cache and automatically
/// falls back to in-memory cache when the primary becomes unavailable.
pub struct FallbackCacheManager {
    primary: Mutex<Box<dyn CacheManager + Send + Sync>>,
    fallback: Mutex<MemoryCacheManager>,
    using_fallback: AtomicBool,
    last_failure_time: AtomicU64,
    start_time: Instant,
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

        debug!("Attempting to recover Redis cache connection...");

        let primary = match self.primary.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                debug!("Primary cache is busy, skipping recovery attempt");
                return false;
            }
        };

        match primary.check_health().await {
            Ok(()) => {
                self.using_fallback.store(false, Ordering::SeqCst);
                info!("Redis cache connection recovered, switching back from fallback");
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
}

#[async_trait]
impl CacheManager for FallbackCacheManager {
    async fn has(&mut self, key: &str) -> Result<bool> {
        self.try_recover().await;

        if self.is_using_fallback() {
            return self.fallback.lock().await.has(key).await;
        }

        let mut primary = self.primary.lock().await;
        match primary.has(key).await {
            Ok(result) => Ok(result),
            Err(e) => {
                self.switch_to_fallback(&e.to_string());
                self.fallback.lock().await.has(key).await
            }
        }
    }

    async fn get(&mut self, key: &str) -> Result<Option<String>> {
        self.try_recover().await;

        if self.is_using_fallback() {
            return self.fallback.lock().await.get(key).await;
        }

        let mut primary = self.primary.lock().await;
        match primary.get(key).await {
            Ok(result) => Ok(result),
            Err(e) => {
                self.switch_to_fallback(&e.to_string());
                self.fallback.lock().await.get(key).await
            }
        }
    }

    async fn set(&mut self, key: &str, value: &str, ttl_seconds: u64) -> Result<()> {
        self.try_recover().await;

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

        if self.is_using_fallback() {
            return self.fallback.lock().await.remove(key).await;
        }

        let mut primary = self.primary.lock().await;
        match primary.remove(key).await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.switch_to_fallback(&e.to_string());
                self.fallback.lock().await.remove(key).await
            }
        }
    }

    async fn disconnect(&mut self) -> Result<()> {
        let _ = self.primary.lock().await.disconnect().await;
        let _ = self.fallback.lock().await.disconnect().await;
        Ok(())
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

        if self.is_using_fallback() {
            return self.fallback.lock().await.ttl(key).await;
        }

        let mut primary = self.primary.lock().await;
        match primary.ttl(key).await {
            Ok(result) => Ok(result),
            Err(e) => {
                self.switch_to_fallback(&e.to_string());
                self.fallback.lock().await.ttl(key).await
            }
        }
    }
}
