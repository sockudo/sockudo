#[cfg(feature = "redis-cluster")]
pub use inner::*;

#[cfg(feature = "redis-cluster")]
mod inner {
    use crate::horizontal_adapter_base::HorizontalAdapterBase;
    use crate::transports::RedisClusterTransport;
    use sockudo_core::error::Result;
    pub(crate) use sockudo_core::options::RedisClusterAdapterConfig;

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
}
