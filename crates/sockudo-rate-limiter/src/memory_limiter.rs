use async_trait::async_trait;
use dashmap::DashMap;
use sockudo_core::error::Result;
use sockudo_core::rate_limiter::{RateLimitConfig, RateLimitResult, RateLimiter};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

/// Entry in the rate limiter map
#[derive(Clone)]
struct RateLimitEntry {
    /// Current count of requests
    count: u32,
    /// When the window started
    window_start: Instant,
    /// When the window will reset
    expiry: Instant,
}

/// In-memory rate limiter implementation
pub struct MemoryRateLimiter {
    /// Storage for rate limit counters
    limits: Arc<RateLimitMap>,
    /// Configuration for rate limiting
    config: RateLimitConfig,
}

type RateLimitMap = DashMap<String, RateLimitEntry, ahash::RandomState>;

struct SharedLimiterCleanup {
    tracked_maps: Arc<Mutex<Vec<Weak<RateLimitMap>>>>,
}

impl SharedLimiterCleanup {
    fn global() -> &'static Self {
        static REGISTRY: OnceLock<SharedLimiterCleanup> = OnceLock::new();

        REGISTRY.get_or_init(|| {
            let cleanup = SharedLimiterCleanup {
                tracked_maps: Arc::new(Mutex::new(Vec::new())),
            };
            cleanup.spawn_worker();
            cleanup
        })
    }

    fn register(&self, limits: &Arc<RateLimitMap>) {
        let mut tracked_maps = self.tracked_maps.lock().unwrap();
        tracked_maps.push(Arc::downgrade(limits));
    }

    fn spawn_worker(&self) {
        let tracked_maps = Arc::clone(&self.tracked_maps);

        std::thread::Builder::new()
            .name("sockudo-rate-limit-sweeper".to_string())
            .spawn(move || {
                loop {
                    std::thread::sleep(shared_cleanup_interval());
                    let now = Instant::now();
                    let live_maps = {
                        let mut tracked = tracked_maps.lock().unwrap();
                        let mut live_maps = Vec::with_capacity(tracked.len());
                        tracked.retain(|weak_map| match weak_map.upgrade() {
                            Some(map) => {
                                live_maps.push(map);
                                true
                            }
                            None => false,
                        });
                        live_maps
                    };

                    for limits in live_maps {
                        sweep_expired_entries(&limits, now);
                    }
                }
            })
            .expect("failed to spawn shared rate limiter cleanup worker");
    }
}

fn sweep_expired_entries(limits: &RateLimitMap, now: Instant) {
    limits.retain(|_, value| value.expiry > now);
}

fn shared_cleanup_interval() -> Duration {
    #[cfg(test)]
    {
        Duration::from_millis(25)
    }

    #[cfg(not(test))]
    {
        Duration::from_secs(10)
    }
}

impl MemoryRateLimiter {
    /// Create a new memory-based rate limiter
    pub fn new(max_requests: u32, window_secs: u64) -> Self {
        Self::with_config(RateLimitConfig {
            max_requests,
            window_secs,
            identifier: Some("memory".to_string()),
        })
    }

    /// Create a new memory-based rate limiter with a specific configuration
    pub fn with_config(config: RateLimitConfig) -> Self {
        let limits = Arc::new(DashMap::with_hasher(ahash::RandomState::new()));
        SharedLimiterCleanup::global().register(&limits);

        Self { limits, config }
    }
}

#[async_trait]
impl RateLimiter for MemoryRateLimiter {
    async fn check(&self, key: &str) -> Result<RateLimitResult> {
        let now = Instant::now();

        if let Some(entry) = self.limits.get(key) {
            // Check if the window has expired
            if entry.expiry <= now {
                drop(entry);
                self.limits.remove(key);
                return Ok(RateLimitResult {
                    allowed: true,
                    remaining: self.config.max_requests,
                    reset_after: self.config.window_secs,
                    limit: self.config.max_requests,
                });
            }

            // Calculate remaining requests and time to reset
            let remaining = self.config.max_requests.saturating_sub(entry.count);
            let reset_after = entry.expiry.saturating_duration_since(now).as_secs();

            Ok(RateLimitResult {
                allowed: remaining > 0,
                remaining,
                reset_after,
                limit: self.config.max_requests,
            })
        } else {
            // No entry yet, so full allowance
            Ok(RateLimitResult {
                allowed: true,
                remaining: self.config.max_requests,
                reset_after: self.config.window_secs,
                limit: self.config.max_requests,
            })
        }
    }

    async fn increment(&self, key: &str) -> Result<RateLimitResult> {
        let now = Instant::now();
        let window_duration = Duration::from_secs(self.config.window_secs);
        let max_requests = self.config.max_requests;

        let mut entry = self
            .limits
            .entry(key.to_string())
            .or_insert_with(|| RateLimitEntry {
                count: 0,
                window_start: now,
                expiry: now + window_duration,
            });

        // Check if the window has expired and reset if needed
        if entry.expiry <= now {
            entry.count = 0;
            entry.window_start = now;
            entry.expiry = now + window_duration;
        }

        // Increment the counter atomically (we hold the entry lock)
        entry.count += 1;
        let new_count = entry.count;

        // Calculate result while still holding the lock
        let remaining = max_requests.saturating_sub(new_count);
        let reset_after = entry.expiry.saturating_duration_since(now).as_secs();

        Ok(RateLimitResult {
            allowed: new_count <= max_requests,
            remaining,
            reset_after,
            limit: max_requests,
        })
    }

    async fn reset(&self, key: &str) -> Result<()> {
        self.limits.remove(key);
        Ok(())
    }

    async fn get_remaining(&self, key: &str) -> Result<u32> {
        let now = Instant::now();

        if let Some(entry) = self.limits.get(key) {
            // Check if the window has expired
            if entry.expiry <= now {
                drop(entry);
                self.limits.remove(key);
                return Ok(self.config.max_requests);
            }

            let remaining = self.config.max_requests.saturating_sub(entry.count);
            Ok(remaining)
        } else {
            Ok(self.config.max_requests)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn test_rate_limiter_basic() {
        let limiter = MemoryRateLimiter::new(5, 60);

        let result = limiter.increment("test_key").await.unwrap();
        assert!(result.allowed);
        assert_eq!(result.remaining, 4);
        assert_eq!(result.limit, 5);
    }

    #[tokio::test]
    async fn test_rate_limiter_exceeds_limit() {
        let limiter = MemoryRateLimiter::new(3, 60);

        for i in 0..3 {
            let result = limiter.increment("test_key").await.unwrap();
            assert!(result.allowed, "Request {} should be allowed", i + 1);
        }

        let result = limiter.increment("test_key").await.unwrap();
        assert!(!result.allowed);
        assert_eq!(result.remaining, 0);
    }

    #[tokio::test]
    async fn test_rate_limiter_check_doesnt_increment() {
        let limiter = MemoryRateLimiter::new(5, 60);

        let result = limiter.check("test_key").await.unwrap();
        assert!(result.allowed);
        assert_eq!(result.remaining, 5);

        limiter.increment("test_key").await.unwrap();

        let result = limiter.check("test_key").await.unwrap();
        assert!(result.allowed);
        assert_eq!(result.remaining, 4);
    }

    #[tokio::test]
    async fn test_rate_limiter_reset() {
        let limiter = MemoryRateLimiter::new(5, 60);

        limiter.increment("test_key").await.unwrap();
        limiter.increment("test_key").await.unwrap();

        limiter.reset("test_key").await.unwrap();

        let result = limiter.check("test_key").await.unwrap();
        assert_eq!(result.remaining, 5);
    }

    #[tokio::test]
    async fn test_rate_limiter_concurrent_increments() {
        use std::sync::Arc;

        let limiter = Arc::new(MemoryRateLimiter::new(100, 60));
        let mut handles = vec![];

        for _ in 0..50 {
            let limiter_clone = Arc::clone(&limiter);
            let handle = tokio::spawn(async move {
                limiter_clone.increment("concurrent_key").await.unwrap();
                limiter_clone.increment("concurrent_key").await.unwrap();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let result = limiter.check("concurrent_key").await.unwrap();
        assert_eq!(result.remaining, 0, "Should have exactly 100 increments");
    }

    #[tokio::test]
    async fn test_shared_cleanup_reaps_idle_expired_entries() {
        let limiter = MemoryRateLimiter::new(1, 1);

        limiter.increment("test_key").await.unwrap();
        assert_eq!(limiter.limits.len(), 1);

        sleep(Duration::from_secs(2)).await;

        timeout(Duration::from_secs(1), async {
            loop {
                if limiter.limits.is_empty() {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("shared sweeper should reap expired idle limiter entries");
    }
}
