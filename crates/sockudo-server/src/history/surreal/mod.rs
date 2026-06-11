use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDurableState, HistoryItem, HistoryPage,
    HistoryPurgeMode, HistoryPurgeRequest, HistoryPurgeResult, HistoryQueryBounds,
    HistoryReadRequest, HistoryResetResult, HistoryRetentionStats, HistoryRuntimeStatus,
    HistoryStore, HistoryStreamInspection, HistoryStreamRuntimeState, HistoryWriteReservation,
};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::{HistoryConfig, SurrealDbSettings};
use sockudo_core::version_store::{
    StoredVersionRecord, VersionReplayRequest, VersionStore, VersionStoreCursor,
    VersionStoreDirection, VersionStorePage, VersionStoreReadRequest, VersionStreamState,
    VersionWriteReservation, VersionWriteReservationBlock,
};
use sockudo_core::versioned_messages::MessageSerial;
use sonic_rs::JsonValueTrait;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::auth::Root;
use surrealdb_types::SurrealValue;
use tracing::{error, info};

#[derive(Clone)]
struct HistoryTables {
    streams: String,
    entries: String,
    entries_stream_serial_idx: String,
    entries_stream_time_idx: String,
    streams_app_idx: String,
    version_streams: String,
    version_messages: String,
    version_entries: String,
    version_streams_app_idx: String,
    version_messages_message_idx: String,
    version_messages_history_idx: String,
    version_entries_message_idx: String,
    version_entries_delivery_idx: String,
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

pub struct SurrealHistoryStore {
    db: Surreal<Any>,
    tables: HistoryTables,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    degraded_channels: Arc<DashMap<String, HistoryDegradedState>>,
    queue_depth_total: AtomicUsize,
}

pub async fn create_surreal_history_store(
    db_config: &SurrealDbSettings,
    config: HistoryConfig,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
) -> Result<Arc<dyn HistoryStore + Send + Sync>> {
    let store = SurrealHistoryStore::new(db_config, config, metrics, cache_manager).await?;
    Ok(Arc::new(store))
}

impl SurrealHistoryStore {
    async fn new(
        db_config: &SurrealDbSettings,
        config: HistoryConfig,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
        cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    ) -> Result<Self> {
        let streams = format!("{}_streams", config.surrealdb.table_prefix);
        let entries = format!("{}_entries", config.surrealdb.table_prefix);
        let version_streams = format!("{}_version_streams", config.surrealdb.table_prefix);
        let version_messages = format!("{}_version_messages", config.surrealdb.table_prefix);
        let version_entries = format!("{}_version_entries", config.surrealdb.table_prefix);
        validate_identifier(&streams, "streams table")?;
        validate_identifier(&entries, "entries table")?;
        validate_identifier(&version_streams, "version streams table")?;
        validate_identifier(&version_messages, "version messages table")?;
        validate_identifier(&version_entries, "version entries table")?;

        let db = connect(db_config.url.as_str())
            .await
            .map_err(|e| Error::Internal(format!("Failed to connect to SurrealDB: {e}")))?;
        db.signin(Root {
            username: db_config.username.clone(),
            password: db_config.password.clone(),
        })
        .await
        .map_err(|e| Error::Internal(format!("Failed to authenticate with SurrealDB: {e}")))?;
        db.use_ns(db_config.namespace.as_str())
            .use_db(db_config.database.as_str())
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to select SurrealDB namespace/database: {e}"
                ))
            })?;

        let store = Self {
            db,
            tables: HistoryTables {
                streams_app_idx: format!("{}_app_idx", streams),
                entries_stream_serial_idx: format!("{}_stream_serial_idx", entries),
                entries_stream_time_idx: format!("{}_stream_time_idx", entries),
                streams,
                entries,
                version_streams_app_idx: format!("{}_app_idx", version_streams),
                version_messages_message_idx: format!("{}_message_idx", version_messages),
                version_messages_history_idx: format!("{}_history_idx", version_messages),
                version_entries_message_idx: format!("{}_message_idx", version_entries),
                version_entries_delivery_idx: format!("{}_delivery_idx", version_entries),
                version_streams,
                version_messages,
                version_entries,
            },
            metrics,
            cache_manager,
            degraded_channels: Arc::new(DashMap::new()),
            queue_depth_total: AtomicUsize::new(0),
        };
        store.ensure_schema().await?;
        Ok(store)
    }

    async fn resolved_stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamRuntimeState> {
        let durable_record = self.load_stream_record(app_id, channel).await?;
        let durable_state = durable_record
            .as_ref()
            .map(|record| record.runtime_state(app_id, channel, "durable_store"))
            .unwrap_or_else(|| {
                HistoryStreamRuntimeState::healthy(app_id, channel, None, "durable_store")
            });
        let local_hint = self
            .degraded_channels
            .get(&degraded_channel_key(app_id, channel))
            .map(|entry| entry.value().clone());
        let cache_hint =
            get_cached_channel_degraded(self.cache_manager.as_ref(), app_id, channel).await?;
        Ok(resolve_runtime_state(durable_state, local_hint, cache_hint))
    }

    async fn resolved_stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamInspection> {
        let durable_record = self.load_stream_record(app_id, channel).await?;
        let runtime_state = self.resolved_stream_runtime_state(app_id, channel).await?;
        Ok(match durable_record {
            Some(record) => record.inspection(app_id, channel, &runtime_state.observed_source),
            None => HistoryStreamInspection {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                stream_id: None,
                next_serial: None,
                retained: HistoryRetentionStats::default(),
                state: runtime_state,
            },
        })
    }
}

mod entries;
mod records;
use records::*;
mod resources;
mod schema;
mod state;
use state::*;

mod store_impl;

mod version_store;
#[allow(unused_imports)]
pub(super) use version_store::create_surreal_version_store;

#[cfg(test)]
mod tests;
