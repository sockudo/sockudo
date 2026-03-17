pub mod factory;
pub mod memory_limiter;
pub mod middleware;
#[cfg(feature = "redis-cluster")]
pub mod redis_cluster_limiter;
#[cfg(feature = "redis")]
pub mod redis_limiter;

pub use sockudo_core::rate_limiter::{RateLimitConfig, RateLimitResult, RateLimiter};

pub mod middleware_utils {
    use super::*;
    use crate::middleware::{IpKeyExtractor, RateLimitLayer, RateLimitOptions};
    use std::sync::Arc;

    pub fn with_arc_ip_limiter(
        limiter: Arc<dyn RateLimiter>,
        options: RateLimitOptions,
    ) -> RateLimitLayer<IpKeyExtractor> {
        RateLimitLayer::with_options(limiter, IpKeyExtractor::new(1), options)
    }
}
