use aws_sdk_dynamodb::Client;
use dashmap::DashMap;
use sockudo_core::cache::CacheManager;
use sockudo_core::error::Result;
use sockudo_core::history::{
    HistoryDurableState, HistoryRetentionStats, HistoryStore, HistoryStreamInspection,
    HistoryStreamRuntimeState,
};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::{DynamoDbSettings, HistoryConfig};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

mod degraded;
mod items;
mod provisioning;
mod queries;
mod store_impl;
#[cfg(feature = "versioned-messages")]
mod version_store;
mod writes;

#[allow(unused_imports)]
#[cfg(feature = "versioned-messages")]
pub use version_store::{DynamoDbVersionStore, create_dynamodb_version_store};

#[derive(Clone)]
struct HistoryTables {
    streams: String,
    entries: String,
    entries_time_index: String,
    version_streams: String,
    version_messages: String,
    version_messages_history_index: String,
    version_entries: String,
    version_entries_delivery_index: String,
    version_entries_history_index: String,
    version_entries_message_index: String,
}

#[derive(Debug, Clone)]
struct HistoryDegradedState {
    app_id: String,
    channel: String,
    durable_state: HistoryDurableState,
    reason: String,
    node_id: Option<String>,
    last_transition_at_ms: i64,
    observed_source: &'static str,
}

#[derive(Debug, Clone)]
struct HistoryStreamRecord {
    stream_id: String,
    next_serial: u64,
    durable_state: HistoryDurableState,
    durable_state_reason: Option<String>,
    durable_state_node_id: Option<String>,
    durable_state_changed_at_ms: Option<i64>,
    retained: HistoryRetentionStats,
}

impl HistoryStreamRecord {
    fn runtime_state(
        &self,
        app_id: &str,
        channel: &str,
        observed_source: &str,
    ) -> HistoryStreamRuntimeState {
        HistoryStreamRuntimeState {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: Some(self.stream_id.clone()),
            durable_state: self.durable_state,
            recovery_allowed: self.durable_state.recovery_allowed(),
            reset_required: self.durable_state.reset_required(),
            reason: self.durable_state_reason.clone(),
            node_id: self.durable_state_node_id.clone(),
            last_transition_at_ms: self.durable_state_changed_at_ms,
            authoritative_source: "durable_store".to_string(),
            observed_source: observed_source.to_string(),
        }
    }

    fn inspection(
        &self,
        app_id: &str,
        channel: &str,
        observed_source: &str,
    ) -> HistoryStreamInspection {
        HistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: Some(self.stream_id.clone()),
            next_serial: Some(self.next_serial),
            retained: self.retained.clone(),
            state: self.runtime_state(app_id, channel, observed_source),
        }
    }
}

#[derive(Debug, Clone)]
struct StoredStreamRecord {
    app_id: String,
    channel: String,
    stream_id: String,
    next_serial: u64,
    retained_messages: u64,
    retained_bytes: u64,
    oldest_available_serial: Option<u64>,
    newest_available_serial: Option<u64>,
    oldest_available_published_at_ms: Option<i64>,
    newest_available_published_at_ms: Option<i64>,
    durable_state: HistoryDurableState,
    durable_state_reason: Option<String>,
    durable_state_node_id: Option<String>,
    durable_state_changed_at_ms: Option<i64>,
    updated_at_ms: i64,
}

#[derive(Debug, Clone)]
struct StoredEntryRecord {
    app_id: String,
    channel: String,
    stream_id: String,
    serial: u64,
    published_at_ms: i64,
    message_id: Option<String>,
    event_name: Option<String>,
    operation_kind: String,
    payload_bytes: Vec<u8>,
    payload_size_bytes: u64,
}

pub struct DynamoDbHistoryStore {
    client: Client,
    tables: HistoryTables,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    degraded_channels: Arc<DashMap<String, HistoryDegradedState>>,
    queue_depth_total: AtomicUsize,
}

pub async fn create_dynamodb_history_store(
    db_config: &DynamoDbSettings,
    config: HistoryConfig,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
) -> Result<Arc<dyn HistoryStore + Send + Sync>> {
    let store = DynamoDbHistoryStore::new(db_config, config, metrics, cache_manager).await?;
    Ok(Arc::new(store))
}
