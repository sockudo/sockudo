use sockudo_core::error::{Error, Result};
use sockudo_core::history::HistoryAppendRecord;
use sockudo_core::metrics::MetricsInterface;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::error;

use sqlx::{PgPool, Row};

use super::HistoryTables;
use super::history_store::PostgresHistoryStore;
use super::stream_state::{DegradeRequest, decrement_app_queue_depth, mark_channel_degraded};

#[derive(Clone)]
pub(super) struct WriterHandle {
    pub(super) tx: mpsc::Sender<HistoryAppendRecord>,
}

impl PostgresHistoryStore {
    pub(super) fn start_writers(&mut self) {
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
                            error = %err,
                            "history write failed"
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
        sqlx::query(sqlx::AssertSqlSafe(insert_sql.as_str()))
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
        let age_rows = sqlx::query(sqlx::AssertSqlSafe(age_delete.as_str()))
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
            let row = sqlx::query(sqlx::AssertSqlSafe(count_sql.as_str()))
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
                let trim_rows = sqlx::query(sqlx::AssertSqlSafe(trim_sql.as_str()))
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
            let rows = sqlx::query(sqlx::AssertSqlSafe(size_sql.as_str()))
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
                    let trim_rows = sqlx::query(sqlx::AssertSqlSafe(trim_sql.as_str()))
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
        let aggregates = sqlx::query(sqlx::AssertSqlSafe(aggregates_sql.as_str()))
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
        sqlx::query(sqlx::AssertSqlSafe(update_sql.as_str()))
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
        sqlx::query(sqlx::AssertSqlSafe(next_serial_sql.as_str()))
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

    pub(super) fn select_writer(&self, app_id: &str, channel: &str) -> &WriterHandle {
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
}
