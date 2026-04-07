use sockudo_core::error::{Error, Result};
#[cfg(feature = "postgres")]
use sockudo_core::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDirection, HistoryItem, HistoryPage,
    HistoryQueryBounds, HistoryReadRequest, HistoryRetentionStats, HistoryRuntimeStatus,
    HistoryWriteReservation,
};
use sockudo_core::cache::CacheManager;
use sockudo_core::history::{HistoryStore, MemoryHistoryStore, MemoryHistoryStoreConfig};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::{DatabaseConnection, DatabasePooling, HistoryBackend, HistoryConfig};
use std::sync::Arc;
#[cfg(feature = "postgres")]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "postgres")]
use std::time::{Duration, Instant};
#[cfg(not(feature = "postgres"))]
use std::time::Duration;
#[cfg(feature = "postgres")]
use tokio::sync::mpsc;
#[cfg(feature = "postgres")]
use tracing::error;
#[cfg(feature = "postgres")]
use dashmap::DashMap;

#[cfg(feature = "postgres")]
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
#[cfg(feature = "postgres")]
use sonic_rs::json;
#[cfg(feature = "postgres")]
use sonic_rs::JsonValueTrait;

pub async fn create_history_store(
    history_config: &HistoryConfig,
    db_config: &DatabaseConnection,
    pooling: &DatabasePooling,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
) -> Result<Arc<dyn HistoryStore + Send + Sync>> {
    if !history_config.enabled {
        return Ok(Arc::new(sockudo_core::history::NoopHistoryStore));
    }

    match history_config.backend {
        HistoryBackend::Memory => Ok(Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig {
            retention_window: Duration::from_secs(history_config.retention_window_seconds),
            max_messages_per_channel: history_config.max_messages_per_channel,
            max_bytes_per_channel: history_config.max_bytes_per_channel,
        }))),
        HistoryBackend::Postgres => {
            #[cfg(feature = "postgres")]
            {
                let store =
                    PostgresHistoryStore::new(
                        db_config,
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
    reason: String,
    node_id: Option<String>,
    last_error_at_ms: i64,
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
            .map_err(|e| Error::Internal(format!("Failed to connect history store to PostgreSQL: {e}")))?;

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

        for sql in [
            create_streams,
            create_entries,
            index_serial,
            index_time,
            add_oldest_time,
            add_newest_time,
        ] {
            sqlx::query(&sql)
                .execute(&self.pool)
                .await
                .map_err(|e| Error::Internal(format!("Failed to initialize history tables: {e}")))?;
        }

        Ok(())
    }

    fn start_writers(&mut self) {
        for shard in 0..self.config.writer_shards {
            let (tx, mut rx) = mpsc::channel::<HistoryAppendRecord>(self.config.writer_queue_capacity);
            let pool = self.pool.clone();
            let tables = self.tables.clone();
            let config = self.config.clone();
            let metrics = self.metrics.clone();
            let cache_manager = self.cache_manager.clone();
            let degraded_channels = self.degraded_channels.clone();
            let queue_depth_total = self.queue_depth_total.clone();
            let queue_depth_by_app = self.queue_depth_by_app.clone();
            tokio::spawn(async move {
                while let Some(record) = rx.recv().await {
                    queue_depth_total.fetch_sub(1, Ordering::Relaxed);
                    decrement_app_queue_depth(&queue_depth_by_app, &record.app_id, metrics.as_deref());
                    let started = Instant::now();
                    if let Err(err) =
                        Self::persist_record(
                            &pool,
                            &tables,
                            &config,
                            &record,
                            metrics.clone(),
                            cache_manager.clone(),
                            degraded_channels.clone(),
                        )
                        .await
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
                            &degraded_channels,
                            cache_manager.as_ref(),
                            metrics.as_deref(),
                            &record.app_id,
                            &record.channel,
                            "durable_history_write_failed",
                            None,
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
        config: &HistoryConfig,
        record: &HistoryAppendRecord,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
        cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
        degraded_channels: Arc<DashMap<String, HistoryDegradedState>>,
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
            .saturating_sub((config.retention_window_seconds * 1000) as i64);
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

        if let Some(max_messages) = config.max_messages_per_channel {
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

        if let Some(max_bytes) = config.max_bytes_per_channel {
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
                    removed = removed
                        .saturating_add(row.get::<i64, _>("payload_size_bytes") as u64);
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
                COALESCE(SUM(payload_size_bytes), 0) AS retained_bytes,
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
        let oldest_serial = aggregates.try_get::<Option<i64>, _>("oldest_serial").unwrap_or(None);
        let newest_serial = aggregates.try_get::<Option<i64>, _>("newest_serial").unwrap_or(None);
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
            .map_err(|e| Error::Internal(format!("Failed to update history stream metadata: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("Failed to commit history transaction: {e}")))?;

        if let Some(metrics) = metrics.as_ref() {
            metrics.update_history_retained(&record.app_id, retained_messages, retained_bytes);
            if evicted_messages > 0 || evicted_bytes > 0 {
                metrics.mark_history_eviction(&record.app_id, evicted_messages, evicted_bytes);
            }
        }

        clear_channel_degraded(
            &degraded_channels,
            cache_manager.as_ref(),
            metrics.as_deref(),
            &record.app_id,
            &record.channel,
        )
        .await;

        Ok(())
    }

    fn select_writer(&self, app_id: &str, channel: &str) -> &WriterHandle {
        let shard = if self.writers.len() == 1 {
            0
        } else {
            let next = self.next_writer.fetch_add(1, Ordering::Relaxed);
            ((ahash::random_state::RandomState::with_seeds(1, 2, 3, 4)
                .hash_one(format!("{app_id}\0{channel}"))
                as usize)
                .wrapping_add(next))
                % self.writers.len()
        };
        &self.writers[shard]
    }

    async fn retained_stats(&self, app_id: &str, channel: &str) -> Result<HistoryRetentionStats> {
        let sql = format!(
            "SELECT stream_id, retained_messages, retained_bytes, oldest_available_serial, newest_available_serial, oldest_available_published_at_ms, newest_available_published_at_ms FROM {} WHERE app_id = $1 AND channel = $2",
            self.tables.streams
        );
        let row = sqlx::query(&sql)
            .bind(app_id)
            .bind(channel)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to read history retention stats: {e}")))?;

        Ok(match row {
            Some(row) => HistoryRetentionStats {
                stream_id: Some(row.get::<String, _>("stream_id")),
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
            },
            None => HistoryRetentionStats::default(),
        })
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
        increment_app_queue_depth(&self.queue_depth_by_app, &record.app_id, self.metrics.as_deref());
        let send_result = self
            .select_writer(&record.app_id, &record.channel)
            .tx
            .try_send(record.clone());

        if let Err(e) = send_result {
            self.queue_depth_total.fetch_sub(1, Ordering::Relaxed);
            decrement_app_queue_depth(&self.queue_depth_by_app, &record.app_id, self.metrics.as_deref());
            mark_channel_degraded(
                &self.degraded_channels,
                self.cache_manager.as_ref(),
                self.metrics.as_deref(),
                &record.app_id,
                &record.channel,
                "history_writer_queue_full",
                None,
            )
            .await;
            return Err(Error::Internal(format!("History writer queue is full: {e}")));
        }

        Ok(())
    }

    async fn read_page(&self, request: HistoryReadRequest) -> Result<HistoryPage> {
        if let Some(state) =
            get_channel_degraded(&self.degraded_channels, self.cache_manager.as_ref(), &request.app_id, &request.channel).await?
        {
            return Err(Error::Internal(format!(
                "History stream is degraded for {}/{}: {}",
                state.app_id, state.channel, state.reason
            )));
        }

        request.validate()?;
        let retained = self.retained_stats(&request.app_id, &request.channel).await?;

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
        let mut query = sqlx::query(&sql).bind(&request.app_id).bind(&request.channel);
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
                message_id: row.try_get::<Option<String>, _>("message_id").unwrap_or(None),
                event_name: row.try_get::<Option<String>, _>("event_name").unwrap_or(None),
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
        Ok(HistoryRuntimeStatus {
            enabled: true,
            backend: "postgres".to_string(),
            degraded_channels: self.degraded_channels.len(),
            queue_depth: self.queue_depth_total.load(Ordering::Relaxed),
        })
    }
}

#[cfg(feature = "postgres")]
fn is_truncated_by_retention(bounds: &HistoryQueryBounds, retained: &HistoryRetentionStats) -> bool {
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
        && retained.oldest_serial.is_some_and(|oldest_serial| oldest_serial > 1)
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
async fn mark_channel_degraded(
    degraded_channels: &DashMap<String, HistoryDegradedState>,
    cache_manager: Option<&Arc<dyn CacheManager + Send + Sync>>,
    metrics: Option<&(dyn MetricsInterface + Send + Sync)>,
    app_id: &str,
    channel: &str,
    reason: &str,
    node_id: Option<String>,
) {
    let state = HistoryDegradedState {
        app_id: app_id.to_string(),
        channel: channel.to_string(),
        reason: reason.to_string(),
        node_id,
        last_error_at_ms: sockudo_core::history::now_ms(),
    };
    degraded_channels.insert(degraded_channel_key(app_id, channel), state.clone());
    if let Some(cache) = cache_manager {
        let _ = cache
            .set(
                &degraded_cache_key(app_id, channel),
                &sonic_rs::to_string(&json!({
                    "app_id": state.app_id,
                    "channel": state.channel,
                    "reason": state.reason,
                    "last_error_at_ms": state.last_error_at_ms,
                }))
                .unwrap_or_else(|_| "{}".to_string()),
                3600,
            )
            .await;
    }
    if let Some(metrics) = metrics {
        let mut degraded_for_app = 0usize;
        for entry in degraded_channels.iter() {
            if entry.value().app_id == app_id {
                degraded_for_app += 1;
            }
        }
        metrics.update_history_degraded_channels(app_id, degraded_for_app);
    }
}

#[cfg(feature = "postgres")]
async fn clear_channel_degraded(
    degraded_channels: &DashMap<String, HistoryDegradedState>,
    cache_manager: Option<&Arc<dyn CacheManager + Send + Sync>>,
    metrics: Option<&(dyn MetricsInterface + Send + Sync)>,
    app_id: &str,
    channel: &str,
) {
    degraded_channels.remove(&degraded_channel_key(app_id, channel));
    if let Some(cache) = cache_manager {
        let _ = cache.remove(&degraded_cache_key(app_id, channel)).await;
    }
    if let Some(metrics) = metrics {
        let mut degraded_for_app = 0usize;
        for entry in degraded_channels.iter() {
            if entry.value().app_id == app_id {
                degraded_for_app += 1;
            }
        }
        metrics.update_history_degraded_channels(app_id, degraded_for_app);
    }
}

#[cfg(feature = "postgres")]
async fn get_channel_degraded(
    degraded_channels: &DashMap<String, HistoryDegradedState>,
    cache_manager: Option<&Arc<dyn CacheManager + Send + Sync>>,
    app_id: &str,
    channel: &str,
) -> Result<Option<HistoryDegradedState>> {
    if let Some(entry) = degraded_channels.get(&degraded_channel_key(app_id, channel)) {
        let state = entry.value().clone();
        return Ok(Some(state));
    }

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
            reason: value
                .get("reason")
                .and_then(sonic_rs::Value::as_str)
                .unwrap_or("history_stream_degraded")
                .to_string(),
            node_id: None,
            last_error_at_ms: value
                .get("last_error_at_ms")
                .and_then(sonic_rs::Value::as_i64)
                .unwrap_or_default(),
        }));
    }

    Ok(None)
}
