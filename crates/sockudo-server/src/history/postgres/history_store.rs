use dashmap::DashMap;
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::{
    HistoryRetentionStats, HistoryStreamInspection, HistoryStreamRuntimeState,
};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::DatabaseConnection;
use sockudo_core::options::{DatabasePooling, HistoryConfig};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use sqlx::{PgPool, Row, postgres::PgPoolOptions};

use super::HistoryTables;
use super::history_record::HistoryStreamRecord;
use super::stream_state::{
    HistoryDegradedState, degraded_channel_key, get_cached_channel_degraded, resolve_runtime_state,
};
use super::writers::WriterHandle;

pub struct PostgresHistoryStore {
    pub(super) pool: PgPool,
    pub(super) config: HistoryConfig,
    pub(super) tables: HistoryTables,
    pub(super) writers: Vec<WriterHandle>,
    pub(super) next_writer: AtomicUsize,
    pub(super) metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    pub(super) cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    pub(super) degraded_channels: Arc<DashMap<String, HistoryDegradedState>>,
    pub(super) queue_depth_total: Arc<AtomicUsize>,
    pub(super) queue_depth_by_app: Arc<DashMap<String, usize>>,
}

impl PostgresHistoryStore {
    pub(in crate::history) async fn new(
        db_config: &DatabaseConnection,
        pooling: &DatabasePooling,
        config: HistoryConfig,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
        cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    ) -> Result<Self> {
        let password = urlencoding::encode(&db_config.password);
        let connection_string = format!(
            "postgresql://{}:{}@{}:{}/{}",
            db_config.username, password, db_config.host, db_config.port, db_config.database
        );

        let mut opts = PgPoolOptions::new();
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
                Error::Internal(format!(
                    "Failed to connect history store to PostgreSQL: {e}"
                ))
            })?;

        let tables = HistoryTables {
            streams: format!("{}_streams", config.postgres.table_prefix),
            entries: format!("{}_entries", config.postgres.table_prefix),
            version_streams: format!("{}_version_streams", config.postgres.table_prefix),
            version_messages: format!("{}_version_messages", config.postgres.table_prefix),
            version_entries: format!("{}_version_entries", config.postgres.table_prefix),
            annotation_events: format!("{}_annotation_events", config.postgres.table_prefix),
            annotation_projections: format!(
                "{}_annotation_projections",
                config.postgres.table_prefix
            ),
        };

        let store = Self {
            pool,
            config,
            tables,
            writers: Vec::new(),
            next_writer: AtomicUsize::new(0),
            metrics,
            cache_manager,
            degraded_channels: Arc::new(DashMap::new()),
            queue_depth_total: Arc::new(AtomicUsize::new(0)),
            queue_depth_by_app: Arc::new(DashMap::new()),
        };

        store.ensure_tables().await?;
        let mut store = store;
        store.start_writers();
        Ok(store)
    }

    async fn load_stream_record(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Option<HistoryStreamRecord>> {
        let sql = format!(
            "SELECT stream_id, next_serial, retained_messages, retained_bytes, oldest_available_serial, newest_available_serial, oldest_available_published_at_ms, newest_available_published_at_ms, durable_state, durable_state_reason, durable_state_node_id, durable_state_changed_at_ms FROM {} WHERE app_id = $1 AND channel = $2",
            self.tables.streams
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to read history retention stats: {e}")))?;

        Ok(row.map(HistoryStreamRecord::from_row))
    }

    pub(super) async fn retained_stats(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryRetentionStats> {
        Ok(self
            .load_stream_record(app_id, channel)
            .await?
            .map(|record| record.retention_stats())
            .unwrap_or_default())
    }

    pub(super) async fn resolved_stream_runtime_state(
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

    pub(super) async fn resolved_stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamInspection> {
        let durable_record = self.load_stream_record(app_id, channel).await?;
        let runtime_state = self.resolved_stream_runtime_state(app_id, channel).await?;
        Ok(match durable_record {
            Some(record) => record.inspection_with_state(app_id, channel, runtime_state),
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

    pub(super) async fn update_stream_retention_from_entries(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        tables: &HistoryTables,
        app_id: &str,
        channel: &str,
        updated_at_ms: i64,
    ) -> Result<HistoryRetentionStats> {
        let aggregates_sql = format!(
            r#"
            SELECT
                COUNT(*) AS retained_messages,
                CAST(COALESCE(SUM(payload_size_bytes), 0) AS BIGINT) AS retained_bytes,
                MIN(serial) AS oldest_serial,
                MAX(serial) AS newest_serial,
                MIN(published_at_ms) AS oldest_published_at_ms,
                MAX(published_at_ms) AS newest_published_at_ms
            FROM {}
            WHERE app_id = $1 AND channel = $2
            "#,
            tables.entries
        );
        let aggregates = sqlx::query(sqlx::AssertSqlSafe(aggregates_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to aggregate history rows: {e}")))?;

        let retained = HistoryRetentionStats {
            stream_id: None,
            retained_messages: aggregates.get::<i64, _>("retained_messages") as u64,
            retained_bytes: aggregates.get::<i64, _>("retained_bytes") as u64,
            oldest_serial: aggregates
                .try_get::<Option<i64>, _>("oldest_serial")
                .unwrap_or(None)
                .map(|value| value as u64),
            newest_serial: aggregates
                .try_get::<Option<i64>, _>("newest_serial")
                .unwrap_or(None)
                .map(|value| value as u64),
            oldest_published_at_ms: aggregates
                .try_get::<Option<i64>, _>("oldest_published_at_ms")
                .unwrap_or(None),
            newest_published_at_ms: aggregates
                .try_get::<Option<i64>, _>("newest_published_at_ms")
                .unwrap_or(None),
        };

        let update_sql = format!(
            r#"
            UPDATE {}
            SET retained_messages = $3,
                retained_bytes = $4,
                oldest_available_serial = $5,
                newest_available_serial = $6,
                oldest_available_published_at_ms = $7,
                newest_available_published_at_ms = $8,
                updated_at_ms = $9
            WHERE app_id = $1 AND channel = $2
            "#,
            tables.streams
        );
        sqlx::query(sqlx::AssertSqlSafe(update_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .bind(retained.retained_messages as i64)
            .bind(retained.retained_bytes as i64)
            .bind(retained.oldest_serial.map(|value| value as i64))
            .bind(retained.newest_serial.map(|value| value as i64))
            .bind(retained.oldest_published_at_ms)
            .bind(retained.newest_published_at_ms)
            .bind(updated_at_ms)
            .execute(&mut **tx)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to update history stream metadata: {e}"))
            })?;

        Ok(retained)
    }
}
