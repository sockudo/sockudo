use dashmap::DashMap;
use futures_util::TryStreamExt;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::{SerialConsistency, Statement};
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDirection, HistoryDurableState, HistoryItem,
    HistoryPage, HistoryPurgeMode, HistoryPurgeRequest, HistoryPurgeResult, HistoryQueryBounds,
    HistoryReadRequest, HistoryResetResult, HistoryRetentionStats, HistoryRuntimeStatus,
    HistoryStore, HistoryStreamInspection, HistoryStreamRuntimeState, HistoryWriteReservation,
};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::{HistoryConfig, ScyllaDbSettings};
use sonic_rs::JsonValueTrait;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tracing::{error, info};

#[derive(Clone)]
struct HistoryTables {
    keyspace: String,
    streams: String,
    entries: String,
    version_streams: String,
    version_messages: String,
    version_entries_by_message: String,
    version_entries_by_delivery: String,
    version_commits: String,
}

impl HistoryTables {
    fn streams_fq(&self) -> String {
        format!("{}.{}", self.keyspace, self.streams)
    }

    fn entries_fq(&self) -> String {
        format!("{}.{}", self.keyspace, self.entries)
    }

    fn version_streams_fq(&self) -> String {
        format!("{}.{}", self.keyspace, self.version_streams)
    }

    fn version_messages_fq(&self) -> String {
        format!("{}.{}", self.keyspace, self.version_messages)
    }

    fn version_entries_by_message_fq(&self) -> String {
        format!("{}.{}", self.keyspace, self.version_entries_by_message)
    }

    fn version_entries_by_delivery_fq(&self) -> String {
        format!("{}.{}", self.keyspace, self.version_entries_by_delivery)
    }

    fn version_commits_fq(&self) -> String {
        format!("{}.{}", self.keyspace, self.version_commits)
    }
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

pub struct ScyllaHistoryStore {
    session: Arc<Session>,
    config: HistoryConfig,
    tables: HistoryTables,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    degraded_channels: Arc<DashMap<String, HistoryDegradedState>>,
    queue_depth_total: AtomicUsize,
}

struct StreamWriteParams<'a> {
    app_id: &'a str,
    channel: &'a str,
    stream_id: &'a str,
    next_serial: u64,
    durable_state: HistoryDurableState,
    durable_state_reason: Option<&'a str>,
    durable_state_node_id: Option<&'a str>,
    durable_state_changed_at_ms: Option<i64>,
    retained: &'a HistoryRetentionStats,
    updated_at_ms: i64,
}

struct StreamRetentionUpdateParams<'a> {
    app_id: &'a str,
    channel: &'a str,
    stream_id: &'a str,
    next_serial: u64,
    durable_state: HistoryDurableState,
    durable_state_reason: Option<&'a str>,
    durable_state_node_id: Option<&'a str>,
    durable_state_changed_at_ms: Option<i64>,
    updated_at_ms: i64,
}

pub async fn create_scylla_history_store(
    db_config: &ScyllaDbSettings,
    config: HistoryConfig,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
) -> Result<Arc<dyn HistoryStore + Send + Sync>> {
    let store = ScyllaHistoryStore::new(db_config, config, metrics, cache_manager).await?;
    Ok(Arc::new(store))
}

impl ScyllaHistoryStore {
    async fn new(
        db_config: &ScyllaDbSettings,
        config: HistoryConfig,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
        cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    ) -> Result<Self> {
        let mut builder = SessionBuilder::new().known_nodes(db_config.nodes.clone());
        if let (Some(username), Some(password)) = (&db_config.username, &db_config.password) {
            builder = builder.user(username, password);
        }

        let session = builder.build().await.map_err(|e| {
            Error::Internal(format!("Failed to connect history store to ScyllaDB: {e}"))
        })?;
        let session = Arc::new(session);
        let keyspace = if db_config.keyspace.trim().is_empty() {
            "sockudo".to_string()
        } else {
            db_config.keyspace.clone()
        };
        let tables = HistoryTables {
            keyspace,
            streams: format!("{}_streams", config.scylladb.table_prefix),
            entries: format!("{}_entries", config.scylladb.table_prefix),
            version_streams: format!("{}_version_streams", config.scylladb.table_prefix),
            version_messages: format!("{}_version_messages", config.scylladb.table_prefix),
            version_entries_by_message: format!(
                "{}_version_entries_by_message",
                config.scylladb.table_prefix
            ),
            version_entries_by_delivery: format!(
                "{}_version_entries_by_delivery",
                config.scylladb.table_prefix
            ),
            version_commits: format!("{}_version_commits", config.scylladb.table_prefix),
        };

        let store = Self {
            session,
            config,
            tables,
            metrics,
            cache_manager,
            degraded_channels: Arc::new(DashMap::new()),
            queue_depth_total: AtomicUsize::new(0),
        };
        store.ensure_schema().await?;
        Ok(store)
    }

    async fn load_stream_record(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Option<HistoryStreamRecord>> {
        let query = format!(
            "SELECT stream_id, next_serial, durable_state, durable_state_reason, durable_state_node_id, durable_state_changed_at_ms, retained_messages, retained_bytes, oldest_available_serial, newest_available_serial, oldest_available_published_at_ms, newest_available_published_at_ms FROM {} WHERE app_id = ? AND channel = ?",
            self.tables.streams_fq()
        );
        let rows = self
            .session
            .query_unpaged(query, (app_id, channel))
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to read ScyllaDB history stream row: {e}"))
            })?
            .into_rows_result()
            .map_err(|e| {
                Error::Internal(format!("Failed to decode ScyllaDB history stream row: {e}"))
            })?;
        let row = rows.maybe_first_row::<StreamRow>().map_err(|e| {
            Error::Internal(format!(
                "Failed to deserialize ScyllaDB history stream row: {e}"
            ))
        })?;
        Ok(row.map(HistoryStreamRecord::from_row))
    }

    async fn retained_stats(&self, app_id: &str, channel: &str) -> Result<HistoryRetentionStats> {
        Ok(self
            .load_stream_record(app_id, channel)
            .await?
            .map(|record| record.retention_stats())
            .unwrap_or_default())
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
mod rows;
use rows::*;
mod schema;
mod state;
use state::*;

mod store_impl;

mod version_store;
pub(super) use version_store::create_scylla_version_store;

#[cfg(test)]
mod tests;
