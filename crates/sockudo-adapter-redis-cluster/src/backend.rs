use std::sync::Arc;

pub use crate::adapter::redis_cluster_adapter::RedisClusterAdapter as RedisClusterAdapterBackend;
pub use sockudo_config::adapter::RedisClusterAdapterConfig;

pub async fn new_redis_cluster_adapter(
    config: RedisClusterAdapterConfig,
) -> crate::error::Result<RedisClusterAdapterBackend> {
    RedisClusterAdapterBackend::new(config).await
}

pub async fn redis_cluster_adapter_with_nodes(
    nodes: Vec<String>,
) -> crate::error::Result<RedisClusterAdapterBackend> {
    RedisClusterAdapterBackend::with_nodes(nodes).await
}

pub fn into_connection_manager(
    adapter: RedisClusterAdapterBackend,
) -> Arc<dyn crate::adapter::ConnectionManager + Send + Sync> {
    Arc::new(adapter)
}
