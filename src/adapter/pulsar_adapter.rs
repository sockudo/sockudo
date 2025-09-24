#[cfg(feature = "with-pulsar")]
use crate::adapter::horizontal_adapter_base::HorizontalAdapterBase;
#[cfg(feature = "with-pulsar")]
use crate::adapter::transports::PulsarTransport;
#[cfg(feature = "with-pulsar")]
use crate::error::Result;
/// Pulsar channels/topics
pub const DEFAULT_PREFIX: &str = "sockudo";

/// Pulsar adapter for horizontal scaling - type alias for the base implementation
#[cfg(feature = "with-pulsar")]
pub type PulsarAdapter = HorizontalAdapterBase<PulsarTransport>;

#[cfg(feature = "with-pulsar")]
impl PulsarAdapter {
    pub async fn with_service_url(service_url: String) -> Result<Self> {
        let config = PulsarAdapterConfig {
            service_url,
            ..Default::default()
        };
        HorizontalAdapterBase::new(config).await
    }

    pub async fn with_tenant_namespace(
        service_url: String,
        tenant: String,
        namespace: String,
    ) -> Result<Self> {
        let config = PulsarAdapterConfig {
            service_url,
            tenant,
            namespace,
            ..Default::default()
        };
        HorizontalAdapterBase::new(config).await
    }
}
