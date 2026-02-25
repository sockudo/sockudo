use std::sync::Arc;

pub use crate::adapter::redis_adapter::{RedisAdapter as RedisAdapterBackend, RedisAdapterOptions};

pub async fn new_redis_adapter(
    config: RedisAdapterOptions,
) -> crate::error::Result<RedisAdapterBackend> {
    RedisAdapterBackend::new(config).await
}

pub async fn redis_adapter_with_url(redis_url: &str) -> crate::error::Result<RedisAdapterBackend> {
    RedisAdapterBackend::with_url(redis_url).await
}

pub fn into_connection_manager(
    adapter: RedisAdapterBackend,
) -> Arc<dyn crate::adapter::ConnectionManager + Send + Sync> {
    Arc::new(adapter)
}
