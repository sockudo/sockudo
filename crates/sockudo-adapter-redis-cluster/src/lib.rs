//! Redis Cluster adapter implementation crate.

pub use sockudo_adapter_local::{app, delta_compression, error, metrics, namespace, websocket};

pub mod adapter;
pub mod backend;

pub use adapter::connection_manager::{
    CompressionParams, ConnectionManager, HorizontalAdapterInterface,
};
pub use backend::{
    RedisClusterAdapterBackend, RedisClusterAdapterConfig, into_connection_manager,
    new_redis_cluster_adapter, redis_cluster_adapter_with_nodes,
};

pub fn crate_status() -> &'static str {
    "implemented"
}
