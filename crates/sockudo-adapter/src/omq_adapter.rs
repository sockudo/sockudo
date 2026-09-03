#[cfg(feature = "omq")]
pub use inner::*;

#[cfg(feature = "omq")]
mod inner {
    use crate::horizontal_adapter_base::HorizontalAdapterBase;
    use crate::transports::OmqTransport;
    use sockudo_core::error::Result;
    pub(crate) use sockudo_core::options::OmqAdapterConfig;

    /// OMQ adapter for brokerless horizontal scaling.
    pub type OmqAdapter = HorizontalAdapterBase<OmqTransport>;

    impl OmqAdapter {
        pub async fn with_bind_endpoint(bind_endpoint: String) -> Result<Self> {
            let config = OmqAdapterConfig {
                bind_endpoint,
                ..Default::default()
            };
            HorizontalAdapterBase::new(config).await
        }
    }
}
