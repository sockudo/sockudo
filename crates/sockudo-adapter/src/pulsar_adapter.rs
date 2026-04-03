#[cfg(feature = "pulsar")]
pub use inner::*;

#[cfg(feature = "pulsar")]
mod inner {
    use crate::horizontal_adapter_base::HorizontalAdapterBase;
    use crate::transports::PulsarTransport;
    use sockudo_core::error::Result;
    pub(crate) use sockudo_core::options::PulsarAdapterConfig;

    pub const DEFAULT_PREFIX: &str = "sockudo-adapter";

    pub type PulsarAdapter = HorizontalAdapterBase<PulsarTransport>;

    impl PulsarAdapter {
        pub async fn with_url(url: impl Into<String>) -> Result<Self> {
            let config = PulsarAdapterConfig {
                url: url.into(),
                ..Default::default()
            };
            HorizontalAdapterBase::new(config).await
        }
    }
}
