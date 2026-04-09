use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::{PresenceHistoryBackend, PresenceHistoryConfig};
use sockudo_core::presence_history::{
    MemoryPresenceHistoryStore, MemoryPresenceHistoryStoreConfig, NoopPresenceHistoryStore,
    PresenceHistoryStore, TrackingPresenceHistoryStore,
};
use std::sync::Arc;
use std::time::Duration;

pub async fn create_presence_history_store(
    config: &PresenceHistoryConfig,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
) -> Arc<dyn PresenceHistoryStore + Send + Sync> {
    if !config.enabled {
        return Arc::new(NoopPresenceHistoryStore);
    }

    let inner: Arc<dyn PresenceHistoryStore + Send + Sync> = match config.backend {
        PresenceHistoryBackend::Memory => Arc::new(MemoryPresenceHistoryStore::new(
            MemoryPresenceHistoryStoreConfig {
                retention_window: Duration::from_secs(config.retention_window_seconds),
                max_events_per_channel: config.max_events_per_channel,
                max_bytes_per_channel: config.max_bytes_per_channel,
                metrics: metrics.clone(),
            },
        )),
    };

    Arc::new(TrackingPresenceHistoryStore::new(
        inner,
        metrics,
        "in_memory",
    ))
}
