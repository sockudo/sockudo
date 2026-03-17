#[cfg(feature = "nats")]
pub use inner::*;

#[cfg(feature = "nats")]
mod inner {
    use crate::horizontal_adapter_base::HorizontalAdapterBase;
    use crate::transports::NatsTransport;
    use sockudo_core::error::Result;
    pub(crate) use sockudo_core::options::NatsAdapterConfig;

    /// NATS channels/subjects
    pub const DEFAULT_PREFIX: &str = "sockudo";

    /// NATS adapter for horizontal scaling - now a type alias for the base implementation
    pub type NatsAdapter = HorizontalAdapterBase<NatsTransport>;

    impl NatsAdapter {
        pub async fn with_servers(servers: Vec<String>) -> Result<Self> {
            let config = NatsAdapterConfig {
                servers,
                ..Default::default()
            };
            HorizontalAdapterBase::new(config).await
        }
    }
}
