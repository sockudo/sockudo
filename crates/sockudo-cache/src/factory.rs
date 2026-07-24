#![allow(dead_code)]

use crate::memory_cache_manager::MemoryCacheManager;
#[cfg(feature = "redis")]
use crate::redis_cache_manager::{
    RedisCacheConfig as StandaloneRedisCacheConfig, RedisCacheManager,
};
#[cfg(feature = "redis-cluster")]
use crate::redis_cluster_cache_manager::{RedisClusterCacheConfig, RedisClusterCacheManager};
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::options::{CacheConfig, CacheDriver, MemoryCacheOptions, RedisConnection};
use std::sync::Arc;
use tracing::info;

pub struct CacheManagerFactory;

impl CacheManagerFactory {
    #[allow(unused_variables)]
    pub async fn create(
        config: &CacheConfig,
        global_redis_conn_details: &RedisConnection,
    ) -> Result<Arc<dyn CacheManager + Send + Sync>> {
        info!(cache_driver = ?config.driver, "initializing cache manager");

        match config.driver {
            #[cfg(feature = "redis")]
            CacheDriver::Redis => {
                #[cfg(feature = "redis-cluster")]
                if config.redis.cluster_mode {
                    info!(cache_driver = "redis_cluster", "using cache driver");
                    if global_redis_conn_details.cluster_nodes.is_empty() {
                        tracing::error!(
                            cache_driver = "redis_cluster",
                            "redis cluster cache requires configured cluster nodes"
                        );
                        return Err(Error::Cache(
                            "Cache: Redis cluster nodes not configured.".to_string(),
                        ));
                    }
                    let nodes: Vec<String> = global_redis_conn_details
                        .cluster_nodes
                        .iter()
                        .map(|node| node.to_url())
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
                    return Ok(Arc::new(manager));
                }

                #[cfg(not(feature = "redis-cluster"))]
                if config.redis.cluster_mode {
                    tracing::warn!(
                        cache_driver = "redis",
                        requested_driver = "redis_cluster",
                        "redis cluster cache feature unavailable, using standalone redis"
                    );
                }

                info!(cache_driver = "redis", "using cache driver");
                let redis_url = config
                    .redis
                    .url_override
                    .clone()
                    .unwrap_or_else(|| global_redis_conn_details.to_url());

                let prefix = config
                    .redis
                    .prefix
                    .clone()
                    .unwrap_or_else(|| global_redis_conn_details.key_prefix.clone() + "cache:");

                let standalone_redis_cache_config = StandaloneRedisCacheConfig {
                    url: redis_url,
                    prefix,
                    ..Default::default()
                };
                let manager = RedisCacheManager::new(standalone_redis_cache_config).await?;
                // Redis is a coordination authority for nonces, idempotency,
                // revocations, recovery leases, and distributed stats. A
                // node-private memory fallback would acknowledge divergent
                // state during an outage, so backend errors must propagate.
                Ok(Arc::new(manager))
            }
            #[cfg(feature = "redis-cluster")]
            CacheDriver::RedisCluster => {
                info!(cache_driver = "redis_cluster", "using cache driver");
                if global_redis_conn_details.cluster_nodes.is_empty() {
                    tracing::error!(
                        cache_driver = "redis_cluster",
                        "redis cluster cache requires configured cluster nodes"
                    );
                    return Err(Error::Cache(
                        "Cache: Redis cluster nodes not configured for explicit cluster driver."
                            .to_string(),
                    ));
                }
                let nodes: Vec<String> = global_redis_conn_details
                    .cluster_nodes
                    .iter()
                    .map(|node| node.to_url())
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
                Ok(Arc::new(manager))
            }
            CacheDriver::Memory => {
                info!(cache_driver = "memory", "using cache driver");
                let _mem_config = MemoryCacheOptions {
                    ttl: config.memory.ttl,
                    cleanup_interval: config.memory.cleanup_interval,
                    max_capacity: config.memory.max_capacity,
                };
                let manager =
                    MemoryCacheManager::new("default_mem_cache".to_string(), config.memory.clone());
                Ok(Arc::new(manager))
            }
            CacheDriver::None => {
                info!(cache_driver = "none", "cache disabled");
                Err(Error::Cache(
                    "Cache driver explicitly set to 'None'.".to_string(),
                ))
            }
            #[cfg(not(feature = "redis"))]
            CacheDriver::Redis => {
                Err(Error::Cache(
                    "Redis cache manager requested but the redis feature is not enabled"
                        .to_string(),
                ))
            }
            #[cfg(not(feature = "redis-cluster"))]
            CacheDriver::RedisCluster => {
                Err(Error::Cache(
                    "Redis Cluster cache manager requested but the redis-cluster feature is not enabled"
                        .to_string(),
                ))
            }
        }
    }
}
