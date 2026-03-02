//! Redis adapter implementation crate.

pub(crate) use sockudo_adapter_local::{
    app, delta_compression, error, metrics, namespace, websocket,
};

pub mod adapter;
pub mod backend;

pub use adapter::connection_manager::{
    CompressionParams, ConnectionManager, HorizontalAdapterInterface,
};
pub use backend::{
    RedisAdapterBackend, RedisAdapterOptions, into_connection_manager, new_redis_adapter,
    redis_adapter_with_url,
};

pub fn crate_status() -> &'static str {
    "implemented"
}
