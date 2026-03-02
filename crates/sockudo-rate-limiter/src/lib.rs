pub mod factory;
pub mod memory_limiter;
pub mod middleware;
#[cfg(feature = "redis-cluster")]
pub mod redis_cluster_limiter;
#[cfg(feature = "redis")]
pub mod redis_limiter;

use async_trait::async_trait;
use sockudo_core::error::Result;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub max_requests: u32,
    pub window_secs: u64,
    pub identifier: Option<String>,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests: 60,
            window_secs: 60,
            identifier: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RateLimitResult {
    pub allowed: bool,
    pub remaining: u32,
    pub reset_after: u64,
    pub limit: u32,
}

#[async_trait]
pub trait RateLimiter: Send + Sync + 'static {
    async fn check(&self, key: &str) -> Result<RateLimitResult>;
    async fn increment(&self, key: &str) -> Result<RateLimitResult>;
    async fn reset(&self, key: &str) -> Result<()>;
    async fn get_remaining(&self, key: &str) -> Result<u32>;
}

pub mod middleware_utils {
    use super::*;
    use crate::middleware::{IpKeyExtractor, RateLimitLayer, RateLimitOptions};

    pub fn with_arc_ip_limiter(
        limiter: Arc<dyn RateLimiter>,
        options: RateLimitOptions,
    ) -> RateLimitLayer<IpKeyExtractor> {
        RateLimitLayer::with_options(limiter, IpKeyExtractor::new(1), options)
    }
}
