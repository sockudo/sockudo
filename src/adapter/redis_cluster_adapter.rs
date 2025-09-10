use crate::adapter::horizontal_adapter_base::HorizontalAdapterBase;
use crate::adapter::transports::RedisClusterTransport;
use crate::cluster::{ClusterConfig, ClusterNodeTracking, ClusterService, RedisClusterService};
use crate::error::Result;
use async_trait::async_trait;
pub(crate) use crate::options::RedisClusterAdapterConfig;
use std::sync::Arc;

/// Redis Cluster channels
pub const DEFAULT_PREFIX: &str = "sockudo";

/// Redis Cluster adapter for horizontal scaling - now a type alias for the base implementation
pub type RedisClusterAdapter = HorizontalAdapterBase<RedisClusterTransport>;

impl RedisClusterAdapter {
    pub async fn with_nodes(nodes: Vec<String>) -> Result<Self> {
        let config = RedisClusterAdapterConfig {
            nodes,
            ..Default::default()
        };
        HorizontalAdapterBase::new(config).await
    }
}

#[async_trait]
impl ClusterNodeTracking for RedisClusterAdapter {
    async fn create_cluster_service(
        &self,
        node_id: String,
        config: ClusterConfig,
    ) -> Result<Arc<dyn ClusterService>> {
        let redis_client = self.transport.get_redis_client()?;
        let cluster_service = RedisClusterService::new(node_id, config, redis_client)?;
        Ok(Arc::new(cluster_service))
    }
}
