// src/rate_limiter/memory_limiter.rs
use super::{RateLimitConfig, RateLimitResult, RateLimiter};
use crate::error::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::interval;

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
    limits: Arc<DashMap<String, RateLimitEntry, ahash::RandomState>>,
    /// Configuration for rate limiting
    config: RateLimitConfig,
    /// Cleanup task handle
    #[allow(dead_code)]
    cleanup_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
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
        let limits: Arc<DashMap<String, RateLimitEntry, ahash::RandomState>> =
            Arc::new(DashMap::with_hasher(ahash::RandomState::new()));
        let limits_clone = Arc::clone(&limits);

        // Start cleanup task
        let cleanup_task = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(10)); // Check every 10 seconds
            loop {
                interval.tick().await;
                let now = Instant::now();
                // remove all expired limits
                limits_clone.retain(|_, value| value.expiry > now);
            }
        });

        Self {
            limits,
            config,
            cleanup_task: Arc::new(Mutex::new(Some(cleanup_task))),
        }
    }
}

#[async_trait]
impl RateLimiter for MemoryRateLimiter {
    async fn check(&self, key: &str) -> Result<RateLimitResult> {
        let now = Instant::now();

        if let Some(entry) = self.limits.get(key) {
            // Check if the window has expired
            if entry.expiry <= now {
                // Window expired, will be cleaned up later
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

        // FIX: Use entry API to atomically get-or-insert and modify in one operation
        // This eliminates the TOCTOU race condition where another thread could insert
        // between our get() and insert() calls
        let mut entry = self.limits.entry(key.to_string()).or_insert_with(|| {
            RateLimitEntry {
                count: 0, // Will be incremented below
                window_start: now,
                expiry: now + window_duration,
            }
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

        // Entry lock is released here when `entry` goes out of scope

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
                // Window expired, will be cleaned up later
                return Ok(self.config.max_requests);
            }

            // Calculate remaining requests
            let remaining = self.config.max_requests.saturating_sub(entry.count);
            Ok(remaining)
        } else {
            // No entry yet, so full allowance
            Ok(self.config.max_requests)
        }
    }
}

impl Drop for MemoryRateLimiter {
    fn drop(&mut self) {
        // Attempt to abort the cleanup task if it's still running
        if let Ok(mut task_guard) = self.cleanup_task.try_lock()
            && let Some(task) = task_guard.take()
        {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiter_basic() {
        let limiter = MemoryRateLimiter::new(5, 60);

        // First request should be allowed
        let result = limiter.increment("test_key").await.unwrap();
        assert!(result.allowed);
        assert_eq!(result.remaining, 4);
        assert_eq!(result.limit, 5);
    }

    #[tokio::test]
    async fn test_rate_limiter_exceeds_limit() {
        let limiter = MemoryRateLimiter::new(3, 60);

        // Use up all requests
        for i in 0..3 {
            let result = limiter.increment("test_key").await.unwrap();
            assert!(result.allowed, "Request {} should be allowed", i + 1);
        }

        // Next request should be denied
        let result = limiter.increment("test_key").await.unwrap();
        assert!(!result.allowed);
        assert_eq!(result.remaining, 0);
    }

    #[tokio::test]
    async fn test_rate_limiter_check_doesnt_increment() {
        let limiter = MemoryRateLimiter::new(5, 60);

        // Check should not increment
        let result = limiter.check("test_key").await.unwrap();
        assert!(result.allowed);
        assert_eq!(result.remaining, 5);

        // Increment once
        limiter.increment("test_key").await.unwrap();

        // Check should show updated count
        let result = limiter.check("test_key").await.unwrap();
        assert!(result.allowed);
        assert_eq!(result.remaining, 4);
    }

    #[tokio::test]
    async fn test_rate_limiter_reset() {
        let limiter = MemoryRateLimiter::new(5, 60);

        // Increment a few times
        limiter.increment("test_key").await.unwrap();
        limiter.increment("test_key").await.unwrap();

        // Reset
        limiter.reset("test_key").await.unwrap();

        // Should be back to full allowance
        let result = limiter.check("test_key").await.unwrap();
        assert_eq!(result.remaining, 5);
    }

    #[tokio::test]
    async fn test_rate_limiter_concurrent_increments() {
        use std::sync::Arc;

        let limiter = Arc::new(MemoryRateLimiter::new(100, 60));
        let mut handles = vec![];

        // Spawn 50 concurrent tasks, each incrementing twice
        for _ in 0..50 {
            let limiter_clone = Arc::clone(&limiter);
            let handle = tokio::spawn(async move {
                limiter_clone.increment("concurrent_key").await.unwrap();
                limiter_clone.increment("concurrent_key").await.unwrap();
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        // Should have exactly 100 increments (50 tasks * 2 increments each)
        let result = limiter.check("concurrent_key").await.unwrap();
        assert_eq!(result.remaining, 0, "Should have exactly 100 increments");
    }
}
