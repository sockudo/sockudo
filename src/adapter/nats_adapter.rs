use crate::adapter::horizontal_adapter::HorizontalAdapter;
use crate::adapter::transports::NatsTransport;
use crate::error::Result;
pub(crate) use crate::options::NatsAdapterConfig;

/// NATS channels/subjects
pub const DEFAULT_PREFIX: &str = "sockudo";

/// NATS adapter for horizontal scaling - now a type alias for the merged implementation
pub type NatsAdapter = HorizontalAdapter<NatsTransport>;

impl NatsAdapter {
    pub async fn with_servers(servers: Vec<String>) -> Result<Self> {
        let config = NatsAdapterConfig {
            servers,
            ..Default::default()
        };
        HorizontalAdapter::new(config).await
    }
}
