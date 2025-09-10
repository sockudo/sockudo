use crate::adapter::horizontal_adapter_base::HorizontalAdapterBase;
use crate::adapter::transports::{RedisAdapterConfig, RedisTransport};
use crate::cluster::{ClusterConfig, ClusterNodeTracking, ClusterService, RedisClusterService};
use crate::error::Result;
use async_trait::async_trait;
use std::sync::Arc;

/// Redis adapter for horizontal scaling - now a type alias for the base implementation
pub type RedisAdapter = HorizontalAdapterBase<RedisTransport>;

// Re-export config for backward compatibility
pub use crate::adapter::transports::RedisAdapterConfig as RedisAdapterOptions;

impl RedisAdapter {
    pub async fn with_url(redis_url: &str) -> Result<Self> {
        let config = RedisAdapterConfig {
            url: redis_url.to_string(),
            ..Default::default()
        };
        HorizontalAdapterBase::new(config).await
    }
}

#[async_trait]
impl ClusterNodeTracking for RedisAdapter {
    async fn create_cluster_service(
        &self,
        node_id: String,
        config: ClusterConfig,
    ) -> Result<Arc<dyn ClusterService>> {
        let redis_client = self.transport.get_redis_client();
        let cluster_service = RedisClusterService::new(node_id, config, redis_client)?;
        Ok(Arc::new(cluster_service))
    }
}
