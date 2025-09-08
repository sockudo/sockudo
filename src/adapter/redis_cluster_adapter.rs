use crate::adapter::horizontal_adapter::HorizontalAdapter;
use crate::adapter::transports::RedisClusterTransport;
use crate::error::Result;
pub(crate) use crate::options::RedisClusterAdapterConfig;

/// Redis Cluster channels
pub const DEFAULT_PREFIX: &str = "sockudo";

/// Redis Cluster adapter for horizontal scaling - now a type alias for the merged implementation
pub type RedisClusterAdapter = HorizontalAdapter<RedisClusterTransport>;

impl RedisClusterAdapter {
    pub async fn with_nodes(nodes: Vec<String>) -> Result<Self> {
        let config = RedisClusterAdapterConfig {
            nodes,
            ..Default::default()
        };
        HorizontalAdapter::new(config).await
    }
}
