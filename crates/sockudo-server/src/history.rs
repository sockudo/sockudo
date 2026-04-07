use sockudo_core::error::{Error, Result};
#[cfg(feature = "postgres")]
use sockudo_core::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDirection, HistoryItem, HistoryPage,
    HistoryReadRequest, HistoryRetentionStats, HistoryWriteReservation,
};
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
use sqlx::{PgPool, Row, postgres::PgPoolOptions};

pub async fn create_history_store(
    history_config: &HistoryConfig,
    db_config: &DatabaseConnection,
    pooling: &DatabasePooling,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
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
                    PostgresHistoryStore::new(db_config, pooling, history_config.clone(), metrics)
                        .await?;
                Ok(Arc::new(store))
            }
            #[cfg(not(feature = "postgres"))]
            {
                let _ = (db_config, pooling, metrics);
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
pub struct PostgresHistoryStore {
    pool: PgPool,
    config: HistoryConfig,
    tables: HistoryTables,
    writers: Vec<WriterHandle>,
    next_writer: AtomicUsize,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
}

#[cfg(feature = "postgres")]
impl PostgresHistoryStore {
    async fn new(
        db_config: &DatabaseConnection,
        pooling: &DatabasePooling,
        config: HistoryConfig,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
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

        for sql in [create_streams, create_entries, index_serial, index_time] {
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
            tokio::spawn(async move {
                while let Some(record) = rx.recv().await {
                    let started = Instant::now();
                    if let Err(err) =
                        Self::persist_record(&pool, &tables, &config, &record, metrics.clone()).await
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
                MAX(serial) AS newest_serial
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

        let update_sql = format!(
            r#"
            UPDATE {}
            SET retained_messages = $3,
                retained_bytes = $4,
                oldest_available_serial = $5,
                newest_available_serial = $6,
                updated_at_ms = $7
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
            "SELECT retained_messages, retained_bytes, oldest_available_serial, newest_available_serial FROM {} WHERE app_id = $1 AND channel = $2",
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
        self.select_writer(&record.app_id, &record.channel)
            .tx
            .try_send(record)
            .map_err(|e| Error::Internal(format!("History writer queue is full: {e}")))
    }

    async fn read_page(&self, request: HistoryReadRequest) -> Result<HistoryPage> {
        let (cursor_clause, order, bind_stream, bind_serial) = match request.direction {
            HistoryDirection::NewestFirst => (
                request
                    .cursor
                    .as_ref()
                    .map(|_| "AND stream_id = $3 AND serial < $4")
                    .unwrap_or(""),
                "DESC",
                request.cursor.as_ref().map(|cursor| cursor.stream_id.clone()),
                request.cursor.as_ref().map(|cursor| cursor.serial as i64),
            ),
            HistoryDirection::OldestFirst => (
                request
                    .cursor
                    .as_ref()
                    .map(|_| "AND stream_id = $3 AND serial > $4")
                    .unwrap_or(""),
                "ASC",
                request.cursor.as_ref().map(|cursor| cursor.stream_id.clone()),
                request.cursor.as_ref().map(|cursor| cursor.serial as i64),
            ),
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
            self.tables.entries, cursor_clause, order, request.limit
        );
        let mut query = sqlx::query(&sql).bind(&request.app_id).bind(&request.channel);
        if let Some(stream_id) = bind_stream {
            query = query.bind(stream_id);
        }
        if let Some(serial) = bind_serial {
            query = query.bind(serial);
        }
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to read history page: {e}")))?;

        let items: Vec<HistoryItem> = rows
            .into_iter()
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

        let next_cursor = items.last().map(|item| HistoryCursor {
            stream_id: item.stream_id.clone(),
            serial: item.serial,
            direction: request.direction,
        });

        Ok(HistoryPage {
            items,
            next_cursor,
            retained: self.retained_stats(&request.app_id, &request.channel).await?,
        })
    }
}
