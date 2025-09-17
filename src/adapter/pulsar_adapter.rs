use crate::adapter::horizontal_adapter_base::HorizontalAdapterBase;
use crate::adapter::transports::PulsarTransport;
use crate::error::Result;
pub(crate) use crate::options::PulsarAdapterConfig;

/// Pulsar channels/topics prefix
pub const DEFAULT_PREFIX: &str = "sockudo";

/// Pulsar adapter for horizontal scaling - type alias for the base implementation
pub type PulsarAdapter = HorizontalAdapterBase<PulsarTransport>;

impl PulsarAdapter {
    pub async fn with_url(url: String) -> Result<Self> {
        let config = PulsarAdapterConfig {
            url,
            ..Default::default()
        };
        HorizontalAdapterBase::new(config).await
    }

    pub async fn with_config(config: PulsarAdapterConfig) -> Result<Self> {
        HorizontalAdapterBase::new(config).await
    }
}
