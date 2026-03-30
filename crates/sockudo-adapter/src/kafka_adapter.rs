#[cfg(feature = "kafka")]
pub use inner::*;

#[cfg(feature = "kafka")]
mod inner {
    use crate::horizontal_adapter_base::HorizontalAdapterBase;
    use crate::transports::KafkaTransport;
    use sockudo_core::error::Result;
    pub(crate) use sockudo_core::options::KafkaAdapterConfig;

    /// Kafka adapter for horizontal scaling.
    pub type KafkaAdapter = HorizontalAdapterBase<KafkaTransport>;

    impl KafkaAdapter {
        pub async fn with_brokers(brokers: Vec<String>) -> Result<Self> {
            let config = KafkaAdapterConfig {
                brokers,
                ..Default::default()
            };
            HorizontalAdapterBase::new(config).await
        }
    }
}
