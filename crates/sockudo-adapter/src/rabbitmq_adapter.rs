#[cfg(feature = "rabbitmq")]
pub use inner::*;

#[cfg(feature = "rabbitmq")]
mod inner {
    use crate::horizontal_adapter_base::HorizontalAdapterBase;
    use crate::transports::RabbitMqTransport;
    use sockudo_core::error::Result;
    pub(crate) use sockudo_core::options::RabbitMqAdapterConfig;

    /// RabbitMQ adapter for horizontal scaling.
    pub type RabbitMqAdapter = HorizontalAdapterBase<RabbitMqTransport>;

    impl RabbitMqAdapter {
        pub async fn with_url(url: impl Into<String>) -> Result<Self> {
            let config = RabbitMqAdapterConfig {
                url: url.into(),
                ..Default::default()
            };
            HorizontalAdapterBase::new(config).await
        }
    }
}
