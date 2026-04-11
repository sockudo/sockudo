#[cfg(feature = "postgres")]
use dashmap::DashMap;
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
#[cfg(feature = "postgres")]
use sockudo_core::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDirection, HistoryDurableState, HistoryItem,
    HistoryPage, HistoryPurgeMode, HistoryPurgeRequest, HistoryPurgeResult, HistoryQueryBounds,
    HistoryReadRequest, HistoryResetResult, HistoryRetentionStats, HistoryRuntimeStatus,
    HistoryStreamInspection, HistoryStreamRuntimeState, HistoryWriteReservation,
};
use sockudo_core::history::{HistoryStore, MemoryHistoryStore, MemoryHistoryStoreConfig};
use sockudo_core::metrics::MetricsInterface;
#[cfg(any(feature = "postgres", feature = "mysql"))]
use sockudo_core::options::DatabaseConnection;
use sockudo_core::options::{DatabaseConfig, DatabasePooling, HistoryBackend, HistoryConfig};
use std::sync::Arc;
#[cfg(feature = "postgres")]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(feature = "postgres"))]
use std::time::Duration;
#[cfg(feature = "postgres")]
use std::time::{Duration, Instant};
#[cfg(feature = "postgres")]
use tokio::sync::mpsc;
#[cfg(feature = "postgres")]
use tracing::{error, info};

#[cfg(feature = "postgres")]
use sonic_rs::JsonValueTrait;
#[cfg(feature = "postgres")]
use sonic_rs::json;
#[cfg(feature = "postgres")]
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
#[cfg(feature = "dynamodb")]
#[path = "history_dynamodb.rs"]
mod history_dynamodb;
#[cfg(feature = "mysql")]
#[path = "history_mysql.rs"]
mod history_mysql;
#[cfg(feature = "scylladb")]
#[path = "history_scylla.rs"]
mod history_scylla;
#[cfg(feature = "surrealdb")]
#[path = "history_surreal.rs"]
mod history_surreal;

pub async fn create_history_store(
    history_config: &HistoryConfig,
    db_config: &DatabaseConfig,
    pooling: &DatabasePooling,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
) -> Result<Arc<dyn HistoryStore + Send + Sync>> {
    if !history_config.enabled {
        return Ok(Arc::new(sockudo_core::history::NoopHistoryStore));
    }

    match history_config.backend {
        HistoryBackend::Memory => Ok(Arc::new(MemoryHistoryStore::new(
            MemoryHistoryStoreConfig {
                retention_window: Duration::from_secs(history_config.retention_window_seconds),
                max_messages_per_channel: history_config.max_messages_per_channel,
                max_bytes_per_channel: history_config.max_bytes_per_channel,
            },
        ))),
        HistoryBackend::Mysql => {
            #[cfg(feature = "mysql")]
            {
                history_mysql::create_mysql_history_store(
                    &db_config.mysql,
                    pooling,
                    history_config.clone(),
                    metrics,
                    cache_manager,
                )
                .await
            }
            #[cfg(not(feature = "mysql"))]
            {
                let _ = (db_config, pooling, metrics, cache_manager);
                Err(Error::Configuration(
                    "History backend 'mysql' requires the 'mysql' feature".to_string(),
                ))
            }
        }
        HistoryBackend::DynamoDb => {
            #[cfg(feature = "dynamodb")]
            {
                history_dynamodb::create_dynamodb_history_store(
                    &db_config.dynamodb,
                    history_config.clone(),
                    metrics,
                    cache_manager,
                )
                .await
            }
            #[cfg(not(feature = "dynamodb"))]
            {
                let _ = (db_config, pooling, metrics, cache_manager);
                Err(Error::Configuration(
                    "History backend 'dynamodb' requires the 'dynamodb' feature".to_string(),
                ))
            }
        }
        HistoryBackend::SurrealDb => {
            #[cfg(feature = "surrealdb")]
            {
                history_surreal::create_surreal_history_store(
                    &db_config.surrealdb,
                    history_config.clone(),
                    metrics,
                    cache_manager,
                )
                .await
            }
            #[cfg(not(feature = "surrealdb"))]
            {
                let _ = (db_config, pooling, metrics, cache_manager);
                Err(Error::Configuration(
                    "History backend 'surrealdb' requires the 'surrealdb' feature".to_string(),
                ))
            }
        }
        HistoryBackend::Postgres => {
            #[cfg(feature = "postgres")]
            {
                let store = PostgresHistoryStore::new(
                    &db_config.postgres,
                    pooling,
                    history_config.clone(),
                    metrics,
                    cache_manager,
                )
                .await?;
                Ok(Arc::new(store))
            }
            #[cfg(not(feature = "postgres"))]
            {
                let _ = (db_config, pooling, metrics, cache_manager);
                Err(Error::Configuration(
                    "History backend 'postgres' requires the 'postgres' feature".to_string(),
                ))
            }
        }
        HistoryBackend::ScyllaDb => {
            #[cfg(feature = "scylladb")]
            {
                history_scylla::create_scylla_history_store(
                    &db_config.scylladb,
                    history_config.clone(),
                    metrics,
                    cache_manager,
                )
                .await
            }
            #[cfg(not(feature = "scylladb"))]
            {
                let _ = (db_config, pooling, metrics, cache_manager);
                Err(Error::Configuration(
                    "History backend 'scylladb' requires the 'scylladb' feature".to_string(),
                ))
            }
        }
    }
}

#[cfg(feature = "postgres")]
#[derive(Clone)]
struct HistoryTables {
    streams: String,
    entries: String,
}

#[cfg(feature = "postgres")]
#[derive(Clone)]
struct WriterHandle {
    tx: mpsc::Sender<HistoryAppendRecord>,
}

#[cfg(feature = "postgres")]
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

#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
struct HistoryStreamRecord {
    stream_id: String,
    next_serial: u64,
    retained_messages: u64,
    retained_bytes: u64,
    oldest_serial: Option<u64>,
    newest_serial: Option<u64>,
    oldest_published_at_ms: Option<i64>,
    newest_published_at_ms: Option<i64>,
    durable_state: HistoryDurableState,
    durable_state_reason: Option<String>,
    durable_state_node_id: Option<String>,
    durable_state_changed_at_ms: Option<i64>,
}

#[cfg(feature = "postgres")]
impl HistoryStreamRecord {
    fn retention_stats(&self) -> HistoryRetentionStats {
        HistoryRetentionStats {
            stream_id: Some(self.stream_id.clone()),
            retained_messages: self.retained_messages,
            retained_bytes: self.retained_bytes,
            oldest_serial: self.oldest_serial,
            newest_serial: self.newest_serial,
            oldest_published_at_ms: self.oldest_published_at_ms,
            newest_published_at_ms: self.newest_published_at_ms,
        }
    }

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
            retained: self.retention_stats(),
            state: self.runtime_state(app_id, channel, observed_source),
        }
    }
}

#[cfg(feature = "postgres")]
pub struct PostgresHistoryStore {
    pool: PgPool,
    config: HistoryConfig,
    tables: HistoryTables,
    writers: Vec<WriterHandle>,
    next_writer: AtomicUsize,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    degraded_channels: Arc<DashMap<String, HistoryDegradedState>>,
    queue_depth_total: Arc<AtomicUsize>,
    queue_depth_by_app: Arc<DashMap<String, usize>>,
}

#[cfg(feature = "postgres")]
impl PostgresHistoryStore {
    async fn new(
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

    async fn ensure_tables(&self) -> Result<()> {
        let create_streams = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                next_serial BIGINT NOT NULL,
                durable_state TEXT NOT NULL DEFAULT 'healthy',
                durable_state_reason TEXT NULL,
                durable_state_node_id TEXT NULL,
                durable_state_changed_at_ms BIGINT NULL,
                retained_messages BIGINT NOT NULL DEFAULT 0,
                retained_bytes BIGINT NOT NULL DEFAULT 0,
                oldest_available_serial BIGINT NULL,
                newest_available_serial BIGINT NULL,
                oldest_available_published_at_ms BIGINT NULL,
                newest_available_published_at_ms BIGINT NULL,
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel)
            )
            "#,
            self.tables.streams
        );
        let create_entries = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                serial BIGINT NOT NULL,
                published_at_ms BIGINT NOT NULL,
                message_id TEXT NULL,
                event_name TEXT NULL,
                operation_kind TEXT NOT NULL,
                payload_bytes BYTEA NOT NULL,
                payload_size_bytes BIGINT NOT NULL,
                metadata JSONB NULL,
                tombstone BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (app_id, channel, stream_id, serial)
            )
            "#,
            self.tables.entries
        );
        let index_serial = format!(
            "CREATE INDEX IF NOT EXISTS {0}_app_channel_serial_idx ON {0} (app_id, channel, serial DESC)",
            self.tables.entries
        );
        let index_time = format!(
            "CREATE INDEX IF NOT EXISTS {0}_app_channel_time_idx ON {0} (app_id, channel, published_at_ms DESC, serial DESC)",
            self.tables.entries
        );
        let add_oldest_time = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS oldest_available_published_at_ms BIGINT NULL",
            self.tables.streams
        );
        let add_newest_time = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS newest_available_published_at_ms BIGINT NULL",
            self.tables.streams
        );
        let add_durable_state = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS durable_state TEXT NOT NULL DEFAULT 'healthy'",
            self.tables.streams
        );
        let add_durable_reason = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS durable_state_reason TEXT NULL",
            self.tables.streams
        );
        let add_durable_node_id = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS durable_state_node_id TEXT NULL",
            self.tables.streams
        );
        let add_durable_changed_at = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS durable_state_changed_at_ms BIGINT NULL",
            self.tables.streams
        );

        for sql in [
            create_streams,
            create_entries,
            index_serial,
            index_time,
            add_oldest_time,
            add_newest_time,
            add_durable_state,
            add_durable_reason,
            add_durable_node_id,
            add_durable_changed_at,
        ] {
            sqlx::query(&sql).execute(&self.pool).await.map_err(|e| {
                Error::Internal(format!("Failed to initialize history tables: {e}"))
            })?;
        }

        Ok(())
    }

    fn start_writers(&mut self) {
        for shard in 0..self.config.writer_shards {
            let (tx, mut rx) =
                mpsc::channel::<HistoryAppendRecord>(self.config.writer_queue_capacity);
            let pool = self.pool.clone();
            let tables = self.tables.clone();
            let metrics = self.metrics.clone();
            let cache_manager = self.cache_manager.clone();
            let degraded_channels = self.degraded_channels.clone();
            let queue_depth_total = self.queue_depth_total.clone();
            let queue_depth_by_app = self.queue_depth_by_app.clone();
            tokio::spawn(async move {
                while let Some(record) = rx.recv().await {
                    queue_depth_total.fetch_sub(1, Ordering::Relaxed);
                    decrement_app_queue_depth(
                        &queue_depth_by_app,
                        &record.app_id,
                        metrics.as_deref(),
                    );
                    let started = Instant::now();
                    if let Err(err) =
                        Self::persist_record(&pool, &tables, &record, metrics.clone()).await
                    {
                        error!(
                            shard,
                            app_id = %record.app_id,
                            channel = %record.channel,
                            serial = record.serial,
                            "History write failed: {err}"
                        );
                        if let Some(metrics) = metrics.as_ref() {
                            metrics.mark_history_write_failure(&record.app_id);
                        }
                        mark_channel_degraded(
                            &pool,
                            &tables,
                            &degraded_channels,
                            cache_manager.as_ref(),
                            metrics.as_deref(),
                            DegradeRequest {
                                app_id: &record.app_id,
                                channel: &record.channel,
                                reason: "durable_history_write_failed",
                                node_id: None,
                            },
                        )
                        .await;
                    } else if let Some(metrics) = metrics.as_ref() {
                        metrics.mark_history_write(&record.app_id);
                        metrics.track_history_write_latency(
                            &record.app_id,
                            started.elapsed().as_secs_f64() * 1000.0,
                        );
                    }
                }
            });
            self.writers.push(WriterHandle { tx });
        }
    }

    async fn persist_record(
        pool: &PgPool,
        tables: &HistoryTables,
        record: &HistoryAppendRecord,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    ) -> Result<()> {
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| Error::Internal(format!("Failed to begin history transaction: {e}")))?;

        let insert_sql = format!(
            r#"
            INSERT INTO {} (
                app_id, channel, stream_id, serial, published_at_ms, message_id, event_name,
                operation_kind, payload_bytes, payload_size_bytes
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT DO NOTHING
            "#,
            tables.entries
        );
        sqlx::query(&insert_sql)
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(&record.stream_id)
            .bind(record.serial as i64)
            .bind(record.published_at_ms)
            .bind(&record.message_id)
            .bind(&record.event_name)
            .bind(&record.operation_kind)
            .bind(record.payload_bytes.as_ref())
            .bind(record.payload_bytes.len() as i64)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to insert history row: {e}")))?;

        let cutoff_ms = record
            .published_at_ms
            .saturating_sub((record.retention.retention_window_seconds * 1000) as i64);
        let age_delete = format!(
            r#"
            DELETE FROM {}
            WHERE app_id = $1 AND channel = $2 AND published_at_ms < $3
            RETURNING payload_size_bytes
            "#,
            tables.entries
        );
        let age_rows = sqlx::query(&age_delete)
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(cutoff_ms)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to evict aged history rows: {e}")))?;

        let mut evicted_messages = age_rows.len() as u64;
        let mut evicted_bytes = age_rows
            .iter()
            .map(|row| row.get::<i64, _>("payload_size_bytes") as u64)
            .sum::<u64>();

        if let Some(max_messages) = record.retention.max_messages_per_channel {
            let count_sql = format!(
                "SELECT COUNT(*) AS count FROM {} WHERE app_id = $1 AND channel = $2",
                tables.entries
            );
            let row = sqlx::query(&count_sql)
                .bind(&record.app_id)
                .bind(&record.channel)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("Failed to count history rows: {e}")))?;
            let retained_messages = row.get::<i64, _>("count") as usize;
            if retained_messages > max_messages {
                let overflow = retained_messages - max_messages;
                let trim_sql = format!(
                    r#"
                    DELETE FROM {entries}
                    WHERE (app_id, channel, stream_id, serial) IN (
                        SELECT app_id, channel, stream_id, serial
                        FROM {entries}
                        WHERE app_id = $1 AND channel = $2
                        ORDER BY serial ASC
                        LIMIT {overflow}
                    )
                    RETURNING payload_size_bytes
                    "#,
                    entries = tables.entries
                );
                let trim_rows = sqlx::query(&trim_sql)
                    .bind(&record.app_id)
                    .bind(&record.channel)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("Failed to evict history rows by count: {e}"))
                    })?;
                evicted_messages = evicted_messages.saturating_add(trim_rows.len() as u64);
                evicted_bytes = evicted_bytes.saturating_add(
                    trim_rows
                        .iter()
                        .map(|row| row.get::<i64, _>("payload_size_bytes") as u64)
                        .sum::<u64>(),
                );
            }
        }

        if let Some(max_bytes) = record.retention.max_bytes_per_channel {
            let size_sql = format!(
                "SELECT serial, payload_size_bytes FROM {} WHERE app_id = $1 AND channel = $2 ORDER BY serial ASC",
                tables.entries
            );
            let rows = sqlx::query(&size_sql)
                .bind(&record.app_id)
                .bind(&record.channel)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("Failed to inspect history bytes: {e}")))?;

            let retained_bytes = rows
                .iter()
                .map(|row| row.get::<i64, _>("payload_size_bytes") as u64)
                .sum::<u64>();
            if retained_bytes > max_bytes {
                let overflow_bytes = retained_bytes - max_bytes;
                let mut removed = 0u64;
                let mut serials = Vec::new();
                for row in rows {
                    if removed >= overflow_bytes {
                        break;
                    }
                    removed =
                        removed.saturating_add(row.get::<i64, _>("payload_size_bytes") as u64);
                    serials.push(row.get::<i64, _>("serial"));
                }
                if !serials.is_empty() {
                    let trim_sql = format!(
                        "DELETE FROM {} WHERE app_id = $1 AND channel = $2 AND serial = ANY($3) RETURNING payload_size_bytes",
                        tables.entries
                    );
                    let trim_rows = sqlx::query(&trim_sql)
                        .bind(&record.app_id)
                        .bind(&record.channel)
                        .bind(&serials)
                        .fetch_all(&mut *tx)
                        .await
                        .map_err(|e| {
                            Error::Internal(format!("Failed to evict history rows by bytes: {e}"))
                        })?;
                    evicted_messages = evicted_messages.saturating_add(trim_rows.len() as u64);
                    evicted_bytes = evicted_bytes.saturating_add(
                        trim_rows
                            .iter()
                            .map(|row| row.get::<i64, _>("payload_size_bytes") as u64)
                            .sum::<u64>(),
                    );
                }
            }
        }

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
        let aggregates = sqlx::query(&aggregates_sql)
            .bind(&record.app_id)
            .bind(&record.channel)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to aggregate history rows: {e}")))?;

        let retained_messages = aggregates.get::<i64, _>("retained_messages") as u64;
        let retained_bytes = aggregates.get::<i64, _>("retained_bytes") as u64;
        let oldest_serial = aggregates
            .try_get::<Option<i64>, _>("oldest_serial")
            .unwrap_or(None);
        let newest_serial = aggregates
            .try_get::<Option<i64>, _>("newest_serial")
            .unwrap_or(None);
        let oldest_published_at_ms = aggregates
            .try_get::<Option<i64>, _>("oldest_published_at_ms")
            .unwrap_or(None);
        let newest_published_at_ms = aggregates
            .try_get::<Option<i64>, _>("newest_published_at_ms")
            .unwrap_or(None);

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
        sqlx::query(&update_sql)
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(retained_messages as i64)
            .bind(retained_bytes as i64)
            .bind(oldest_serial)
            .bind(newest_serial)
            .bind(oldest_published_at_ms)
            .bind(newest_published_at_ms)
            .bind(record.published_at_ms)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to update history stream metadata: {e}"))
            })?;

        let next_serial_sql = format!(
            "UPDATE {} SET next_serial = GREATEST(next_serial, $3) WHERE app_id = $1 AND channel = $2",
            tables.streams
        );
        sqlx::query(&next_serial_sql)
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(record.serial.saturating_add(1) as i64)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to advance history next_serial from append evidence: {e}"
                ))
            })?;

        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("Failed to commit history transaction: {e}")))?;

        if let Some(metrics) = metrics.as_ref() {
            metrics.update_history_retained(&record.app_id, retained_messages, retained_bytes);
            if evicted_messages > 0 || evicted_bytes > 0 {
                metrics.mark_history_eviction(&record.app_id, evicted_messages, evicted_bytes);
            }
        }

        Ok(())
    }

    fn select_writer(&self, app_id: &str, channel: &str) -> &WriterHandle {
        let shard = if self.writers.len() == 1 {
            0
        } else {
            let next = self.next_writer.fetch_add(1, Ordering::Relaxed);
            ((ahash::random_state::RandomState::with_seeds(1, 2, 3, 4)
                .hash_one(format!("{app_id}\0{channel}")) as usize)
                .wrapping_add(next))
                % self.writers.len()
        };
        &self.writers[shard]
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
        let row = sqlx::query(&sql)
            .bind(app_id)
            .bind(channel)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to read history retention stats: {e}")))?;

        Ok(row.map(|row| HistoryStreamRecord {
            stream_id: row.get::<String, _>("stream_id"),
            next_serial: row.get::<i64, _>("next_serial") as u64,
            retained_messages: row.get::<i64, _>("retained_messages") as u64,
            retained_bytes: row.get::<i64, _>("retained_bytes") as u64,
            oldest_serial: row
                .try_get::<Option<i64>, _>("oldest_available_serial")
                .unwrap_or(None)
                .map(|value| value as u64),
            newest_serial: row
                .try_get::<Option<i64>, _>("newest_available_serial")
                .unwrap_or(None)
                .map(|value| value as u64),
            oldest_published_at_ms: row
                .try_get::<Option<i64>, _>("oldest_available_published_at_ms")
                .unwrap_or(None),
            newest_published_at_ms: row
                .try_get::<Option<i64>, _>("newest_available_published_at_ms")
                .unwrap_or(None),
            durable_state: parse_history_durable_state(
                row.get::<String, _>("durable_state").as_str(),
            ),
            durable_state_reason: row
                .try_get::<Option<String>, _>("durable_state_reason")
                .unwrap_or(None),
            durable_state_node_id: row
                .try_get::<Option<String>, _>("durable_state_node_id")
                .unwrap_or(None),
            durable_state_changed_at_ms: row
                .try_get::<Option<i64>, _>("durable_state_changed_at_ms")
                .unwrap_or(None),
        }))
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
            Some(record) => HistoryStreamInspection {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                stream_id: Some(record.stream_id.clone()),
                next_serial: Some(record.next_serial),
                retained: record.retention_stats(),
                state: runtime_state,
            },
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
        let aggregates = sqlx::query(&aggregates_sql)
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
        sqlx::query(&update_sql)
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

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl HistoryStore for PostgresHistoryStore {
    async fn reserve_publish_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryWriteReservation> {
        let now_ms = sockudo_core::history::now_ms();
        let sql = format!(
            r#"
            INSERT INTO {} (app_id, channel, stream_id, next_serial, updated_at_ms)
            VALUES ($1, $2, $3, 2, $4)
            ON CONFLICT (app_id, channel)
            DO UPDATE SET
                next_serial = {}.next_serial + 1,
                updated_at_ms = EXCLUDED.updated_at_ms
            RETURNING stream_id, next_serial - 1 AS serial
            "#,
            self.tables.streams, self.tables.streams
        );
        let stream_id = uuid::Uuid::new_v4().to_string();
        let row = sqlx::query(&sql)
            .bind(app_id)
            .bind(channel)
            .bind(stream_id)
            .bind(now_ms)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to reserve history position: {e}")))?;

        Ok(HistoryWriteReservation {
            stream_id: row.get::<String, _>("stream_id"),
            serial: row.get::<i64, _>("serial") as u64,
        })
    }

    async fn append(&self, record: HistoryAppendRecord) -> Result<()> {
        self.queue_depth_total.fetch_add(1, Ordering::Relaxed);
        increment_app_queue_depth(
            &self.queue_depth_by_app,
            &record.app_id,
            self.metrics.as_deref(),
        );
        let send_result = self
            .select_writer(&record.app_id, &record.channel)
            .tx
            .try_send(record.clone());

        if let Err(e) = send_result {
            self.queue_depth_total.fetch_sub(1, Ordering::Relaxed);
            decrement_app_queue_depth(
                &self.queue_depth_by_app,
                &record.app_id,
                self.metrics.as_deref(),
            );
            mark_channel_degraded(
                &self.pool,
                &self.tables,
                &self.degraded_channels,
                self.cache_manager.as_ref(),
                self.metrics.as_deref(),
                DegradeRequest {
                    app_id: &record.app_id,
                    channel: &record.channel,
                    reason: "history_writer_queue_full",
                    node_id: None,
                },
            )
            .await;
            return Err(Error::Internal(format!(
                "History writer queue is full: {e}"
            )));
        }

        Ok(())
    }

    async fn read_page(&self, request: HistoryReadRequest) -> Result<HistoryPage> {
        request.validate()?;
        let stream_runtime = self
            .resolved_stream_runtime_state(&request.app_id, &request.channel)
            .await?;
        if !stream_runtime.recovery_allowed {
            return Err(Error::Internal(format!(
                "History stream state blocks cold reads for {}/{}: {}",
                request.app_id,
                request.channel,
                stream_runtime
                    .reason
                    .unwrap_or_else(|| stream_runtime.durable_state.as_str().to_string())
            )));
        }
        let retained = self
            .retained_stats(&request.app_id, &request.channel)
            .await?;

        if let Some(cursor) = request.cursor.as_ref() {
            if let Some(stream_id) = retained.stream_id.as_ref()
                && cursor.stream_id != *stream_id
            {
                return Err(Error::InvalidMessageFormat(
                    "Expired history cursor: channel stream changed".to_string(),
                ));
            }
            if let Some(oldest_serial) = retained.oldest_serial
                && cursor.serial < oldest_serial
            {
                return Err(Error::InvalidMessageFormat(
                    "Expired history cursor: cursor points before retained history".to_string(),
                ));
            }
        }

        let mut clauses: Vec<String> = Vec::new();
        let mut bind_stream = None;
        let mut bind_serial = None;
        let mut bind_start_serial = None;
        let mut bind_end_serial = None;
        let mut bind_start_time = None;
        let mut bind_end_time = None;

        if let Some(cursor) = request.cursor.as_ref() {
            bind_stream = Some(cursor.stream_id.clone());
            bind_serial = Some(cursor.serial as i64);
            clauses.push(match request.direction {
                HistoryDirection::NewestFirst => "stream_id = $3 AND serial < $4".to_string(),
                HistoryDirection::OldestFirst => "stream_id = $3 AND serial > $4".to_string(),
            });
        }
        let mut next_bind = if request.cursor.is_some() { 5 } else { 3 };
        if let Some(start_serial) = request.bounds.start_serial {
            bind_start_serial = Some(start_serial as i64);
            clauses.push(format!("serial >= ${next_bind}"));
            next_bind += 1;
        }
        if let Some(end_serial) = request.bounds.end_serial {
            bind_end_serial = Some(end_serial as i64);
            clauses.push(format!("serial <= ${next_bind}"));
            next_bind += 1;
        }
        if let Some(start_time_ms) = request.bounds.start_time_ms {
            bind_start_time = Some(start_time_ms);
            clauses.push(format!("published_at_ms >= ${next_bind}"));
            next_bind += 1;
        }
        if let Some(end_time_ms) = request.bounds.end_time_ms {
            bind_end_time = Some(end_time_ms);
            clauses.push(format!("published_at_ms <= ${next_bind}"));
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" AND {}", clauses.join(" AND "))
        };
        let order = match request.direction {
            HistoryDirection::NewestFirst => "DESC",
            HistoryDirection::OldestFirst => "ASC",
        };

        let sql = format!(
            r#"
            SELECT stream_id, serial, published_at_ms, message_id, event_name, operation_kind, payload_bytes, payload_size_bytes
            FROM {}
            WHERE app_id = $1 AND channel = $2
            {}
            ORDER BY serial {}
            LIMIT {}
            "#,
            self.tables.entries,
            where_clause,
            order,
            request.limit + 1
        );
        let mut query = sqlx::query(&sql)
            .bind(&request.app_id)
            .bind(&request.channel);
        if let Some(stream_id) = bind_stream {
            query = query.bind(stream_id);
        }
        if let Some(serial) = bind_serial {
            query = query.bind(serial);
        }
        if let Some(start_serial) = bind_start_serial {
            query = query.bind(start_serial);
        }
        if let Some(end_serial) = bind_end_serial {
            query = query.bind(end_serial);
        }
        if let Some(start_time_ms) = bind_start_time {
            query = query.bind(start_time_ms);
        }
        if let Some(end_time_ms) = bind_end_time {
            query = query.bind(end_time_ms);
        }
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to read history page: {e}")))?;

        let has_more = rows.len() > request.limit;
        let items: Vec<HistoryItem> = rows
            .into_iter()
            .take(request.limit)
            .map(|row| HistoryItem {
                stream_id: row.get::<String, _>("stream_id"),
                serial: row.get::<i64, _>("serial") as u64,
                published_at_ms: row.get::<i64, _>("published_at_ms"),
                message_id: row
                    .try_get::<Option<String>, _>("message_id")
                    .unwrap_or(None),
                event_name: row
                    .try_get::<Option<String>, _>("event_name")
                    .unwrap_or(None),
                operation_kind: row.get::<String, _>("operation_kind"),
                payload_size_bytes: row.get::<i64, _>("payload_size_bytes") as usize,
                payload_bytes: row.get::<Vec<u8>, _>("payload_bytes").into(),
            })
            .collect();

        let next_cursor = if has_more {
            items.last().map(|item| HistoryCursor {
                version: 1,
                app_id: request.app_id.clone(),
                channel: request.channel.clone(),
                stream_id: item.stream_id.clone(),
                serial: item.serial,
                direction: request.direction,
                bounds: request.bounds.clone(),
            })
        } else {
            None
        };

        let truncated_by_retention = is_truncated_by_retention(&request.bounds, &retained);

        Ok(HistoryPage {
            items,
            next_cursor,
            retained,
            has_more,
            complete: !has_more && !truncated_by_retention,
            truncated_by_retention,
        })
    }

    async fn runtime_status(&self) -> Result<HistoryRuntimeStatus> {
        let sql = format!(
            r#"
            SELECT
                COALESCE(SUM(CASE WHEN durable_state <> 'healthy' THEN 1 ELSE 0 END), 0) AS degraded_channels,
                COALESCE(SUM(CASE WHEN durable_state = 'reset_required' THEN 1 ELSE 0 END), 0) AS reset_required_channels
            FROM {}
            "#,
            self.tables.streams
        );
        let row = sqlx::query(&sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to read history runtime status: {e}")))?;

        Ok(HistoryRuntimeStatus {
            enabled: true,
            backend: "postgres".to_string(),
            state_authority: "durable_store".to_string(),
            degraded_channels: row.get::<i64, _>("degraded_channels") as usize,
            reset_required_channels: row.get::<i64, _>("reset_required_channels") as usize,
            queue_depth: self.queue_depth_total.load(Ordering::Relaxed),
        })
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamRuntimeState> {
        self.resolved_stream_runtime_state(app_id, channel).await
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamInspection> {
        self.resolved_stream_inspection(app_id, channel).await
    }

    async fn reset_stream(
        &self,
        app_id: &str,
        channel: &str,
        reason: &str,
        requested_by: Option<&str>,
    ) -> Result<HistoryResetResult> {
        if reason.trim().is_empty() {
            return Err(Error::InvalidMessageFormat(
                "Reset reason must not be empty".to_string(),
            ));
        }

        let inspection_before = self.resolved_stream_inspection(app_id, channel).await?;
        let previous_stream_id = inspection_before.stream_id.clone();
        let new_stream_id = uuid::Uuid::new_v4().to_string();
        let now_ms = sockudo_core::history::now_ms();

        let mut tx = self.pool.begin().await.map_err(|e| {
            Error::Internal(format!("Failed to begin history reset transaction: {e}"))
        })?;

        let delete_sql = format!(
            "DELETE FROM {} WHERE app_id = $1 AND channel = $2 RETURNING payload_size_bytes",
            self.tables.entries
        );
        let deleted_rows = sqlx::query(&delete_sql)
            .bind(app_id)
            .bind(channel)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to purge history rows during reset: {e}"))
            })?;
        let purged_messages = deleted_rows.len() as u64;
        let purged_bytes = deleted_rows
            .iter()
            .map(|row| row.get::<i64, _>("payload_size_bytes") as u64)
            .sum::<u64>();

        let upsert_sql = format!(
            r#"
            INSERT INTO {} (
                app_id, channel, stream_id, next_serial, durable_state, durable_state_reason,
                durable_state_node_id, durable_state_changed_at_ms, retained_messages, retained_bytes,
                oldest_available_serial, newest_available_serial, oldest_available_published_at_ms,
                newest_available_published_at_ms, updated_at_ms
            ) VALUES ($1, $2, $3, 1, 'healthy', NULL, NULL, $4, 0, 0, NULL, NULL, NULL, NULL, $4)
            ON CONFLICT (app_id, channel)
            DO UPDATE SET
                stream_id = EXCLUDED.stream_id,
                next_serial = 1,
                durable_state = 'healthy',
                durable_state_reason = NULL,
                durable_state_node_id = NULL,
                durable_state_changed_at_ms = EXCLUDED.durable_state_changed_at_ms,
                retained_messages = 0,
                retained_bytes = 0,
                oldest_available_serial = NULL,
                newest_available_serial = NULL,
                oldest_available_published_at_ms = NULL,
                newest_available_published_at_ms = NULL,
                updated_at_ms = EXCLUDED.updated_at_ms
            "#,
            self.tables.streams
        );
        sqlx::query(&upsert_sql)
            .bind(app_id)
            .bind(channel)
            .bind(&new_stream_id)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to rotate history stream: {e}")))?;

        tx.commit().await.map_err(|e| {
            Error::Internal(format!("Failed to commit history reset transaction: {e}"))
        })?;

        self.degraded_channels
            .remove(&degraded_channel_key(app_id, channel));
        if let Some(cache) = self.cache_manager.as_ref() {
            let _ = cache.remove(&degraded_cache_key(app_id, channel)).await;
        }
        if let Some(metrics) = self.metrics.as_deref() {
            let _ = refresh_history_state_metrics(&self.pool, &self.tables, metrics, app_id).await;
        }

        info!(
            app_id = %app_id,
            channel = %channel,
            previous_stream_id = ?previous_stream_id,
            new_stream_id = %new_stream_id,
            purged_messages,
            purged_bytes,
            reason = %reason,
            requested_by = ?requested_by,
            "Operator reset durable history stream"
        );

        let inspection = self.resolved_stream_inspection(app_id, channel).await?;
        Ok(HistoryResetResult {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            previous_stream_id,
            new_stream_id,
            purged_messages,
            purged_bytes,
            inspection,
        })
    }

    async fn purge_stream(
        &self,
        app_id: &str,
        channel: &str,
        request: HistoryPurgeRequest,
    ) -> Result<HistoryPurgeResult> {
        request.validate()?;
        let now_ms = sockudo_core::history::now_ms();

        let mut tx = self.pool.begin().await.map_err(|e| {
            Error::Internal(format!("Failed to begin history purge transaction: {e}"))
        })?;

        let delete_sql = match request.mode {
            HistoryPurgeMode::All => format!(
                "DELETE FROM {} WHERE app_id = $1 AND channel = $2 RETURNING payload_size_bytes",
                self.tables.entries
            ),
            HistoryPurgeMode::BeforeSerial => format!(
                "DELETE FROM {} WHERE app_id = $1 AND channel = $2 AND serial < $3 RETURNING payload_size_bytes",
                self.tables.entries
            ),
            HistoryPurgeMode::BeforeTimeMs => format!(
                "DELETE FROM {} WHERE app_id = $1 AND channel = $2 AND published_at_ms < $3 RETURNING payload_size_bytes",
                self.tables.entries
            ),
        };
        let mut query = sqlx::query(&delete_sql).bind(app_id).bind(channel);
        match request.mode {
            HistoryPurgeMode::All => {}
            HistoryPurgeMode::BeforeSerial => {
                query = query.bind(request.before_serial.unwrap_or_default() as i64);
            }
            HistoryPurgeMode::BeforeTimeMs => {
                query = query.bind(request.before_time_ms.unwrap_or_default());
            }
        }

        let deleted_rows = query
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to purge history rows: {e}")))?;
        let purged_messages = deleted_rows.len() as u64;
        let purged_bytes = deleted_rows
            .iter()
            .map(|row| row.get::<i64, _>("payload_size_bytes") as u64)
            .sum::<u64>();

        let retained = Self::update_stream_retention_from_entries(
            &mut tx,
            &self.tables,
            app_id,
            channel,
            now_ms,
        )
        .await?;

        tx.commit().await.map_err(|e| {
            Error::Internal(format!("Failed to commit history purge transaction: {e}"))
        })?;

        if let Some(metrics) = self.metrics.as_ref() {
            metrics.update_history_retained(
                app_id,
                retained.retained_messages,
                retained.retained_bytes,
            );
            let _ =
                refresh_history_state_metrics(&self.pool, &self.tables, metrics.as_ref(), app_id)
                    .await;
        }

        info!(
            app_id = %app_id,
            channel = %channel,
            mode = %request.mode.as_str(),
            before_serial = request.before_serial,
            before_time_ms = request.before_time_ms,
            purged_messages,
            purged_bytes,
            reason = %request.reason,
            requested_by = ?request.requested_by,
            "Operator purged durable history rows"
        );

        let inspection = self.resolved_stream_inspection(app_id, channel).await?;
        Ok(HistoryPurgeResult {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            mode: request.mode,
            before_serial: request.before_serial,
            before_time_ms: request.before_time_ms,
            purged_messages,
            purged_bytes,
            inspection,
        })
    }
}

#[cfg(feature = "postgres")]
fn is_truncated_by_retention(
    bounds: &HistoryQueryBounds,
    retained: &HistoryRetentionStats,
) -> bool {
    if let (Some(start_serial), Some(oldest_serial)) = (bounds.start_serial, retained.oldest_serial)
        && start_serial < oldest_serial
    {
        return true;
    }
    if let (Some(start_time_ms), Some(oldest_time_ms)) =
        (bounds.start_time_ms, retained.oldest_published_at_ms)
        && start_time_ms < oldest_time_ms
    {
        return true;
    }
    bounds.start_serial.is_none()
        && bounds.start_time_ms.is_none()
        && retained
            .oldest_serial
            .is_some_and(|oldest_serial| oldest_serial > 1)
}

#[cfg(feature = "postgres")]
fn parse_history_durable_state(raw: &str) -> HistoryDurableState {
    match raw {
        "healthy" => HistoryDurableState::Healthy,
        "reset_required" => HistoryDurableState::ResetRequired,
        "degraded" => HistoryDurableState::Degraded,
        _ => HistoryDurableState::Degraded,
    }
}

#[cfg(feature = "postgres")]
fn degraded_channel_key(app_id: &str, channel: &str) -> String {
    format!("{app_id}\0{channel}")
}

#[cfg(feature = "postgres")]
fn degraded_cache_key(app_id: &str, channel: &str) -> String {
    format!("history:degraded:{app_id}:{channel}")
}

#[cfg(feature = "postgres")]
fn resolve_runtime_state(
    durable_state: HistoryStreamRuntimeState,
    local_hint: Option<HistoryDegradedState>,
    cache_hint: Option<HistoryDegradedState>,
) -> HistoryStreamRuntimeState {
    let newest_hint = [local_hint, cache_hint]
        .into_iter()
        .flatten()
        .max_by_key(|hint| hint.last_transition_at_ms);
    let durable_transition_at = durable_state.last_transition_at_ms.unwrap_or_default();

    if let Some(hint) = newest_hint
        && hint.last_transition_at_ms > durable_transition_at
    {
        return HistoryStreamRuntimeState {
            app_id: durable_state.app_id,
            channel: durable_state.channel,
            stream_id: durable_state.stream_id,
            durable_state: hint.durable_state,
            recovery_allowed: hint.durable_state.recovery_allowed(),
            reset_required: hint.durable_state.reset_required(),
            reason: Some(hint.reason),
            node_id: hint.node_id,
            last_transition_at_ms: Some(hint.last_transition_at_ms),
            authoritative_source: durable_state.authoritative_source,
            observed_source: hint.observed_source.to_string(),
        };
    }

    durable_state
}

#[cfg(feature = "postgres")]
fn increment_app_queue_depth(
    queue_depth_by_app: &DashMap<String, usize>,
    app_id: &str,
    metrics: Option<&(dyn MetricsInterface + Send + Sync)>,
) {
    let mut depth = 1usize;
    if let Some(mut entry) = queue_depth_by_app.get_mut(app_id) {
        *entry += 1;
        depth = *entry;
    } else {
        queue_depth_by_app.insert(app_id.to_string(), 1);
    }
    if let Some(metrics) = metrics {
        metrics.update_history_queue_depth(app_id, depth);
    }
}

#[cfg(feature = "postgres")]
fn decrement_app_queue_depth(
    queue_depth_by_app: &DashMap<String, usize>,
    app_id: &str,
    metrics: Option<&(dyn MetricsInterface + Send + Sync)>,
) {
    let depth = if let Some(mut entry) = queue_depth_by_app.get_mut(app_id) {
        if *entry > 1 {
            *entry -= 1;
            *entry
        } else {
            drop(entry);
            queue_depth_by_app.remove(app_id);
            0
        }
    } else {
        0
    };
    if let Some(metrics) = metrics {
        metrics.update_history_queue_depth(app_id, depth);
    }
}

#[cfg(feature = "postgres")]
struct DegradeRequest<'a> {
    app_id: &'a str,
    channel: &'a str,
    reason: &'a str,
    node_id: Option<String>,
}

#[cfg(feature = "postgres")]
async fn mark_channel_degraded(
    pool: &PgPool,
    tables: &HistoryTables,
    degraded_channels: &DashMap<String, HistoryDegradedState>,
    cache_manager: Option<&Arc<dyn CacheManager + Send + Sync>>,
    metrics: Option<&(dyn MetricsInterface + Send + Sync)>,
    request: DegradeRequest<'_>,
) {
    let now_ms = sockudo_core::history::now_ms();
    let state = HistoryDegradedState {
        app_id: request.app_id.to_string(),
        channel: request.channel.to_string(),
        durable_state: HistoryDurableState::Degraded,
        reason: request.reason.to_string(),
        node_id: request.node_id,
        last_transition_at_ms: now_ms,
        observed_source: "local_memory_hint",
    };
    degraded_channels.insert(
        degraded_channel_key(request.app_id, request.channel),
        state.clone(),
    );
    if let Some(cache) = cache_manager {
        let _ = cache
            .set(
                &degraded_cache_key(request.app_id, request.channel),
                &sonic_rs::to_string(&json!({
                    "app_id": state.app_id,
                    "channel": state.channel,
                    "durable_state": state.durable_state.as_str(),
                    "reason": state.reason,
                    "node_id": state.node_id,
                    "last_transition_at_ms": state.last_transition_at_ms,
                }))
                .unwrap_or_else(|_| "{}".to_string()),
                3600,
            )
            .await;
    }

    let update_sql = format!(
        r#"
        UPDATE {}
        SET durable_state = $3,
            durable_state_reason = $4,
            durable_state_node_id = $5,
            durable_state_changed_at_ms = $6
        WHERE app_id = $1
          AND channel = $2
          AND COALESCE(durable_state_changed_at_ms, 0) <= $6
        "#,
        tables.streams
    );
    if let Err(err) = sqlx::query(&update_sql)
        .bind(request.app_id)
        .bind(request.channel)
        .bind(state.durable_state.as_str())
        .bind(&state.reason)
        .bind(&state.node_id)
        .bind(state.last_transition_at_ms)
        .execute(pool)
        .await
    {
        error!(
            app_id = %request.app_id,
            channel = %request.channel,
            "Failed to persist history degraded state: {err}"
        );
    }

    if let Some(metrics) = metrics {
        let _ = refresh_history_state_metrics(pool, tables, metrics, request.app_id).await;
    }
}

#[cfg(feature = "postgres")]
async fn get_cached_channel_degraded(
    cache_manager: Option<&Arc<dyn CacheManager + Send + Sync>>,
    app_id: &str,
    channel: &str,
) -> Result<Option<HistoryDegradedState>> {
    if let Some(cache) = cache_manager
        && let Some(raw) = cache.get(&degraded_cache_key(app_id, channel)).await?
    {
        let value: sonic_rs::Value = sonic_rs::from_str(&raw)
            .map_err(|e| Error::Internal(format!("Failed to parse degraded history state: {e}")))?;
        return Ok(Some(HistoryDegradedState {
            app_id: value
                .get("app_id")
                .and_then(sonic_rs::Value::as_str)
                .unwrap_or(app_id)
                .to_string(),
            channel: value
                .get("channel")
                .and_then(sonic_rs::Value::as_str)
                .unwrap_or(channel)
                .to_string(),
            durable_state: value
                .get("durable_state")
                .and_then(sonic_rs::Value::as_str)
                .map(parse_history_durable_state)
                .unwrap_or(HistoryDurableState::Degraded),
            reason: value
                .get("reason")
                .and_then(sonic_rs::Value::as_str)
                .unwrap_or("history_stream_degraded")
                .to_string(),
            node_id: value
                .get("node_id")
                .and_then(sonic_rs::Value::as_str)
                .map(str::to_string),
            last_transition_at_ms: value
                .get("last_transition_at_ms")
                .and_then(sonic_rs::Value::as_i64)
                .unwrap_or_default(),
            observed_source: "shared_cache_hint",
        }));
    }

    Ok(None)
}

#[cfg(feature = "postgres")]
async fn refresh_history_state_metrics(
    pool: &PgPool,
    tables: &HistoryTables,
    metrics: &(dyn MetricsInterface + Send + Sync),
    app_id: &str,
) -> Result<()> {
    let sql = format!(
        "SELECT COALESCE(SUM(CASE WHEN durable_state <> 'healthy' THEN 1 ELSE 0 END), 0) AS degraded_channels, COALESCE(SUM(CASE WHEN durable_state = 'reset_required' THEN 1 ELSE 0 END), 0) AS reset_required_channels FROM {} WHERE app_id = $1",
        tables.streams
    );
    let row = sqlx::query(&sql)
        .bind(app_id)
        .fetch_one(pool)
        .await
        .map_err(|e| Error::Internal(format!("Failed to refresh history state metrics: {e}")))?;
    metrics
        .update_history_degraded_channels(app_id, row.get::<i64, _>("degraded_channels") as usize);
    metrics.update_history_reset_required_channels(
        app_id,
        row.get::<i64, _>("reset_required_channels") as usize,
    );
    Ok(())
}
