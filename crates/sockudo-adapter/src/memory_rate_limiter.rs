use async_trait::async_trait;
use dashmap::DashMap;
use sockudo_core::error::Result;
use sockudo_core::rate_limiter::{RateLimitConfig, RateLimitResult, RateLimiter};
use sockudo_core::websocket::PER_SOCKET_MAP_SHARDS;
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
            .name("sockudo-adapter-rate-limit-sweeper".to_string())
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
        // One limiter exists per socket and holds a handful of keys.
        let limits = Arc::new(DashMap::with_hasher_and_shard_amount(
            ahash::RandomState::new(),
            PER_SOCKET_MAP_SHARDS,
        ));
        SharedLimiterCleanup::global().register(&limits);

        Self { limits, config }
    }
}

#[async_trait]
impl RateLimiter for MemoryRateLimiter {
    async fn check(&self, key: &str) -> Result<RateLimitResult> {
        let now = Instant::now();

        if let Some(entry) = self.limits.get(key) {
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

            let remaining = self.config.max_requests.saturating_sub(entry.count);
            let reset_after = entry.expiry.saturating_duration_since(now).as_secs();

            Ok(RateLimitResult {
                allowed: remaining > 0,
                remaining,
                reset_after,
                limit: self.config.max_requests,
            })
        } else {
            Ok(RateLimitResult {
                allowed: true,
                remaining: self.config.max_requests,
                reset_after: self.config.window_secs,
                limit: self.config.max_requests,
            })
        }
    }

    async fn increment(&self, key: &str) -> Result<RateLimitResult> {
        self.increment_with_limit(key, 1, self.config.max_requests, self.config.window_secs)
            .await
    }

    async fn increment_with_limit(
        &self,
        key: &str,
        cost: u32,
        max_requests: u32,
        window_secs: u64,
    ) -> Result<RateLimitResult> {
        let now = Instant::now();
        let window_duration = Duration::from_secs(window_secs);

        let mut entry = self
            .limits
            .entry(key.to_string())
            .or_insert_with(|| RateLimitEntry {
                count: 0,
                window_start: now,
                expiry: now + window_duration,
            });

        if entry.expiry <= now {
            entry.count = 0;
            entry.window_start = now;
            entry.expiry = now + window_duration;
        }

        // All-or-nothing, like the Redis sliding-window script: a denied
        // request, however large, uses no budget.
        let allowed = entry.count.saturating_add(cost) <= max_requests;
        if allowed {
            entry.count += cost;
        }

        let remaining = max_requests.saturating_sub(entry.count);
        let reset_after = entry.expiry.saturating_duration_since(now).as_secs();

        Ok(RateLimitResult {
            allowed,
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
    use super::MemoryRateLimiter;
    use sockudo_core::rate_limiter::RateLimiter;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn expired_entries_reset_without_background_cleanup_tasks() {
        let limiter = MemoryRateLimiter::new(1, 1);

        let first = limiter.increment("socket-1").await.unwrap();
        assert!(first.allowed);

        let second = limiter.increment("socket-1").await.unwrap();
        assert!(!second.allowed);

        sleep(std::time::Duration::from_secs(2)).await;

        let reset = limiter.increment("socket-1").await.unwrap();
        assert!(reset.allowed);
    }

    #[tokio::test]
    async fn denied_batch_consumes_no_budget() {
        let limiter = MemoryRateLimiter::new(100, 60);

        let batch = limiter
            .increment_with_limit("app", 10_000, 100, 60)
            .await
            .unwrap();
        assert!(!batch.allowed);
        assert_eq!(batch.remaining, 100);

        let single = limiter
            .increment_with_limit("app", 1, 100, 60)
            .await
            .unwrap();
        assert!(single.allowed);
        assert_eq!(single.remaining, 99);
    }

    #[tokio::test]
    async fn shared_cleanup_reaps_idle_expired_entries() {
        let limiter = MemoryRateLimiter::new(1, 1);

        limiter.increment("socket-1").await.unwrap();
        assert_eq!(limiter.limits.len(), 1);

        sleep(std::time::Duration::from_secs(2)).await;

        timeout(std::time::Duration::from_secs(1), async {
            loop {
                if limiter.limits.is_empty() {
                    break;
                }
                sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("shared sweeper should reap expired idle limiter entries");
    }
}
