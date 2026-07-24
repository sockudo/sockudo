use dashmap::DashMap;
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDirection, HistoryDurableState, HistoryItem,
    HistoryPage, HistoryPurgeMode, HistoryPurgeRequest, HistoryPurgeResult, HistoryQueryBounds,
    HistoryReadRequest, HistoryResetResult, HistoryRetentionStats, HistoryRuntimeStatus,
    HistoryStore, HistoryStreamInspection, HistoryStreamRuntimeState, HistoryWriteReservation,
};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::{DatabaseConnection, DatabasePooling, HistoryConfig};
#[cfg(feature = "versioned-messages")]
use sockudo_core::versioned_messages::MAX_VERSIONED_SERIAL_LENGTH;
use sonic_rs::JsonValueTrait;
use sqlx::{MySqlPool, Row, mysql::MySqlPoolOptions};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info};

const MYSQL_ASCII_IDENTIFIER_CHARSET: &str = "CHARACTER SET ascii COLLATE ascii_general_ci";

#[derive(Clone)]
struct HistoryTables {
    streams: String,
    entries: String,
    version_streams: String,
    version_messages: String,
    version_entries: String,
    annotation_streams: String,
    annotation_events: String,
    annotation_projections: String,
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

pub struct MySqlHistoryStore {
    pool: MySqlPool,
    tables: HistoryTables,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    degraded_channels: Arc<DashMap<String, HistoryDegradedState>>,
}

pub async fn create_mysql_history_store(
    db_config: &DatabaseConnection,
    pooling: &DatabasePooling,
    config: HistoryConfig,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
) -> Result<Arc<dyn HistoryStore + Send + Sync>> {
    let store = MySqlHistoryStore::new(db_config, pooling, config, metrics, cache_manager).await?;
    Ok(Arc::new(store))
}

impl MySqlHistoryStore {
    async fn new(
        db_config: &DatabaseConnection,
        pooling: &DatabasePooling,
        config: HistoryConfig,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
        cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    ) -> Result<Self> {
        let password = urlencoding::encode(&db_config.password);
        let connection_string = format!(
            "mysql://{}:{}@{}:{}/{}",
            db_config.username, password, db_config.host, db_config.port, db_config.database
        );

        let mut opts = MySqlPoolOptions::new();
        opts = if pooling.enabled {
            let min = db_config.pool_min.unwrap_or(pooling.min);
            let max = db_config.pool_max.unwrap_or(pooling.max);
            opts.min_connections(min).max_connections(max)
        } else {
            opts.max_connections(db_config.connection_pool_size)
        };

        let pool = opts
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(180))
            .connect(&connection_string)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to connect history store to MySQL: {e}"))
            })?;

        let tables = HistoryTables {
            streams: format!("{}_streams", config.mysql.table_prefix),
            entries: format!("{}_entries", config.mysql.table_prefix),
            version_streams: format!("{}_version_streams", config.mysql.table_prefix),
            version_messages: format!("{}_version_messages", config.mysql.table_prefix),
            version_entries: format!("{}_version_entries", config.mysql.table_prefix),
            annotation_streams: format!("{}_annotation_streams", config.mysql.table_prefix),
            annotation_events: format!("{}_annotation_events", config.mysql.table_prefix),
            annotation_projections: format!("{}_annotation_projections", config.mysql.table_prefix),
        };

        let store = Self {
            pool,
            tables,
            metrics,
            cache_manager,
            degraded_channels: Arc::new(DashMap::new()),
        };

        store.ensure_tables().await?;
        Ok(store)
    }

    async fn load_stream_record(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Option<HistoryStreamRecord>> {
        let sql = format!(
            "SELECT stream_id, next_serial, retained_messages, retained_bytes, oldest_available_serial, newest_available_serial, oldest_available_published_at_ms, newest_available_published_at_ms, durable_state, durable_state_reason, durable_state_node_id, durable_state_changed_at_ms FROM {} WHERE app_id = ? AND channel = ?",
            self.tables.streams
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to read MySQL history retention stats: {e}"))
            })?;

        Ok(row.map(HistoryStreamRecord::from_row))
    }

    async fn retained_stats(&self, app_id: &str, channel: &str) -> Result<HistoryRetentionStats> {
        Ok(self
            .load_stream_record(app_id, channel)
            .await?
            .map(|r| r.retention_stats())
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

    async fn update_stream_retention_from_entries(
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        tables: &HistoryTables,
        app_id: &str,
        channel: &str,
        updated_at_ms: i64,
    ) -> Result<HistoryRetentionStats> {
        let aggregates_sql = format!(
            "SELECT COUNT(*) AS retained_messages, CAST(COALESCE(SUM(payload_size_bytes), 0) AS SIGNED) AS retained_bytes, MIN(serial) AS oldest_serial, MAX(serial) AS newest_serial, MIN(published_at_ms) AS oldest_published_at_ms, MAX(published_at_ms) AS newest_published_at_ms FROM {} WHERE app_id = ? AND channel = ?",
            tables.entries
        );
        let aggregates = sqlx::query(sqlx::AssertSqlSafe(aggregates_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to aggregate MySQL history rows: {e}")))?;

        let retained = HistoryRetentionStats {
            stream_id: None,
            retained_messages: aggregates.get::<i64, _>("retained_messages") as u64,
            retained_bytes: aggregates.get::<i64, _>("retained_bytes") as u64,
            oldest_serial: aggregates
                .try_get::<Option<i64>, _>("oldest_serial")
                .unwrap_or(None)
                .map(|v| v as u64),
            newest_serial: aggregates
                .try_get::<Option<i64>, _>("newest_serial")
                .unwrap_or(None)
                .map(|v| v as u64),
            oldest_published_at_ms: aggregates
                .try_get::<Option<i64>, _>("oldest_published_at_ms")
                .unwrap_or(None),
            newest_published_at_ms: aggregates
                .try_get::<Option<i64>, _>("newest_published_at_ms")
                .unwrap_or(None),
        };

        let update_sql = format!(
            "UPDATE {} SET retained_messages = ?, retained_bytes = ?, oldest_available_serial = ?, newest_available_serial = ?, oldest_available_published_at_ms = ?, newest_available_published_at_ms = ?, updated_at_ms = ? WHERE app_id = ? AND channel = ?",
            tables.streams
        );
        sqlx::query(sqlx::AssertSqlSafe(update_sql.as_str()))
            .bind(retained.retained_messages as i64)
            .bind(retained.retained_bytes as i64)
            .bind(retained.oldest_serial.map(|v| v as i64))
            .bind(retained.newest_serial.map(|v| v as i64))
            .bind(retained.oldest_published_at_ms)
            .bind(retained.newest_published_at_ms)
            .bind(updated_at_ms)
            .bind(app_id)
            .bind(channel)
            .execute(&mut **tx)
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to update MySQL history stream metadata: {e}"
                ))
            })?;

        Ok(retained)
    }
}

mod schema;
mod state;
use state::*;
mod stream_record;
use stream_record::HistoryStreamRecord;
mod writes;

mod store_impl;

#[cfg(feature = "versioned-messages")]
mod version_store;
#[cfg(feature = "versioned-messages")]
pub(super) use version_store::create_mysql_version_store;

#[cfg(feature = "versioned-messages")]
mod annotation_store;
#[cfg(feature = "versioned-messages")]
pub(super) use annotation_store::MysqlAnnotationStore;

#[cfg(test)]
mod tests;
