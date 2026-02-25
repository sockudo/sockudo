use std::sync::Arc;

pub use crate::adapter::nats_adapter::NatsAdapter as NatsAdapterBackend;
pub use sockudo_config::adapter::NatsAdapterConfig;

pub async fn new_nats_adapter(
    config: NatsAdapterConfig,
) -> crate::error::Result<NatsAdapterBackend> {
    NatsAdapterBackend::new(config).await
}

pub async fn nats_adapter_with_servers(
    servers: Vec<String>,
) -> crate::error::Result<NatsAdapterBackend> {
    NatsAdapterBackend::with_servers(servers).await
}

pub fn into_connection_manager(
    adapter: NatsAdapterBackend,
) -> Arc<dyn crate::adapter::ConnectionManager + Send + Sync> {
    Arc::new(adapter)
}
