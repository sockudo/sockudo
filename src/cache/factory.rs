// src/cache/factory.rs
use crate::cache::manager::CacheManager;
use crate::cache::memory_cache_manager::MemoryCacheManager; // Assuming MemoryCacheConfig is from options
use crate::cache::redis_cache_manager::{
    RedisCacheConfig as StandaloneRedisCacheConfig, RedisCacheManager,
};
use crate::cache::redis_cluster_cache_manager::{
    RedisClusterCacheConfig, RedisClusterCacheManager,
};
use crate::error::{Error, Result};
use crate::log::Log;
use crate::options::{
    CacheConfig, CacheDriver, MemoryCacheOptions, RedisConfig as OptionsRedisConfig,
    RedisConnection,
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct CacheManagerFactory;

impl CacheManagerFactory {
    pub async fn create(
        config: &CacheConfig,
        global_redis_conn_details: &RedisConnection,
        debug_enabled: bool,
    ) -> Result<Arc<Mutex<dyn CacheManager + Send + Sync>>> {
        // Corrected return type
        Log::info(format!(
            "Initializing CacheManager with driver: {:?}",
            config.driver
        ));

        match config.driver {
            CacheDriver::Redis => {
                if config.redis.cluster_mode {
                    Log::info("Cache: Using Redis Cluster driver.".to_string());
                    if global_redis_conn_details.cluster_nodes.is_empty() {
                        Log::error( "Cache: Redis cluster mode enabled, but no cluster_nodes configured in database.redis section.".to_string());
                        return Err(Error::CacheError(
                            "Cache: Redis cluster nodes not configured.".to_string(),
                        ));
                    }
                    let nodes: Vec<String> = global_redis_conn_details
                        .cluster_nodes
                        .iter()
                        .map(|node| format!("redis://{}:{}", node.host, node.port))
                        .collect();

                    let prefix =
                        config.redis.prefix.clone().unwrap_or_else(|| {
                            global_redis_conn_details.key_prefix.clone() + "cache:"
                        });

                    let cluster_cache_config = RedisClusterCacheConfig {
                        nodes,
                        prefix,
                        ..Default::default()
                    };
                    let manager = RedisClusterCacheManager::new(cluster_cache_config).await?;
                    // Directly create Arc<Mutex<ConcreteType>> which coerces to Arc<Mutex<dyn Trait>>
                    Ok(Arc::new(Mutex::new(manager)))
                } else {
                    Log::info("Cache: Using standalone Redis driver.".to_string());
                    let redis_url = config.redis.url_override.clone().unwrap_or_else(|| {
                        format!(
                            "redis://{}:{}",
                            global_redis_conn_details.host, global_redis_conn_details.port
                        )
                    });

                    let prefix =
                        config.redis.prefix.clone().unwrap_or_else(|| {
                            global_redis_conn_details.key_prefix.clone() + "cache:"
                        });

                    let standalone_redis_cache_config = StandaloneRedisCacheConfig {
                        url: redis_url,
                        prefix,
                        ..Default::default()
                    };
                    let manager = RedisCacheManager::new(standalone_redis_cache_config).await?;
                    Ok(Arc::new(Mutex::new(manager)))
                }
            }
            CacheDriver::RedisCluster => {
                Log::info("Cache: Using Redis Cluster driver (explicitly selected).".to_string());
                if global_redis_conn_details.cluster_nodes.is_empty() {
                    Log::error( "Cache: Redis cluster driver selected, but no cluster_nodes configured in database.redis section.".to_string());
                    return Err(Error::CacheError(
                        "Cache: Redis cluster nodes not configured for explicit cluster driver."
                            .to_string(),
                    ));
                }
                let nodes: Vec<String> = global_redis_conn_details
                    .cluster_nodes
                    .iter()
                    .map(|node| format!("redis://{}:{}", node.host, node.port))
                    .collect();

                let prefix = config
                    .redis
                    .prefix
                    .clone()
                    .unwrap_or_else(|| global_redis_conn_details.key_prefix.clone() + "cache:");

                let cluster_cache_config = RedisClusterCacheConfig {
                    nodes,
                    prefix,
                    ..Default::default()
                };
                let manager = RedisClusterCacheManager::new(cluster_cache_config).await?;
                Ok(Arc::new(Mutex::new(manager)))
            }
            CacheDriver::Memory => {
                Log::info("Using memory cache manager.".to_string());
                // Assuming MemoryCacheOptions is the correct config struct for MemoryCacheManager
                let mem_config = MemoryCacheOptions {
                    ttl: config.memory.ttl,
                    cleanup_interval: config.memory.cleanup_interval,
                    max_capacity: config.memory.max_capacity,
                };
                let manager =
                    MemoryCacheManager::new("default_mem_cache".to_string(), config.memory.clone()); // Pass prefix and MemoryCacheOptions
                Ok(Arc::new(Mutex::new(manager)))
            }
            CacheDriver::None => {
                Log::info("Cache driver is 'None'. Cache will be disabled.".to_string());
                Err(Error::CacheError(
                    "Cache driver explicitly set to 'None'.".to_string(),
                ))
            }
        }
    }
}
