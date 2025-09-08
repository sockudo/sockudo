use crate::adapter::horizontal_adapter::HorizontalAdapter;
use crate::adapter::transports::{RedisAdapterConfig, RedisTransport};
use crate::error::Result;

/// Redis adapter for horizontal scaling - now a type alias for the merged implementation
pub type RedisAdapter = HorizontalAdapter<RedisTransport>;

// Re-export config for backward compatibility
pub use crate::adapter::transports::RedisAdapterConfig as RedisAdapterOptions;

impl RedisAdapter {
    pub async fn with_url(redis_url: &str) -> Result<Self> {
        let config = RedisAdapterConfig {
            url: redis_url.to_string(),
            ..Default::default()
        };
        HorizontalAdapter::new(config).await
    }
}
