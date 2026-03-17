pub mod factory;
pub mod fallback_cache_manager;
pub mod memory_cache_manager;
#[cfg(feature = "redis")]
pub mod redis_cache_manager;
#[cfg(feature = "redis-cluster")]
pub mod redis_cluster_cache_manager;

pub use factory::CacheManagerFactory;
pub use fallback_cache_manager::FallbackCacheManager;
pub use memory_cache_manager::MemoryCacheManager;
#[cfg(feature = "redis")]
pub use redis_cache_manager::{RedisCacheConfig, RedisCacheManager};
#[cfg(feature = "redis-cluster")]
pub use redis_cluster_cache_manager::{RedisClusterCacheConfig, RedisClusterCacheManager};
