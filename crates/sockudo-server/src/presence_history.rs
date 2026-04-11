use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::PresenceHistoryConfig;
use sockudo_core::presence_history::{
    DurablePresenceHistoryStore, MemoryPresenceHistoryStore, MemoryPresenceHistoryStoreConfig,
    NoopPresenceHistoryStore, PresenceHistoryStore, TrackingPresenceHistoryStore,
};
use sockudo_core::history::HistoryStore;
use std::sync::Arc;
use std::time::Duration;

pub async fn create_presence_history_store(
    config: &PresenceHistoryConfig,
    inherit_durable_history: bool,
    history_store: Option<Arc<dyn HistoryStore + Send + Sync>>,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
) -> Arc<dyn PresenceHistoryStore + Send + Sync> {
    if !config.enabled {
        return Arc::new(NoopPresenceHistoryStore);
    }

    if inherit_durable_history && let Some(history_store) = history_store {
        return Arc::new(DurablePresenceHistoryStore::new(history_store, metrics));
    }

    let inner: Arc<dyn PresenceHistoryStore + Send + Sync> = Arc::new(
        MemoryPresenceHistoryStore::new(MemoryPresenceHistoryStoreConfig {
            retention_window: Duration::from_secs(config.retention_window_seconds),
            max_events_per_channel: config.max_events_per_channel,
            max_bytes_per_channel: config.max_bytes_per_channel,
            metrics: metrics.clone(),
        }),
    );

    Arc::new(TrackingPresenceHistoryStore::new(
        inner,
        metrics,
        "in_memory",
    ))
}
