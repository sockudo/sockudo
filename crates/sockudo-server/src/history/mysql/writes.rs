use sockudo_core::error::{Error, Result};
use sockudo_core::history::HistoryAppendRecord;
use sockudo_core::metrics::MetricsInterface;
use std::sync::Arc;

use sqlx::{MySqlPool, Row};

use super::{HistoryTables, MySqlHistoryStore};

impl MySqlHistoryStore {
    pub(super) async fn persist_record(
        pool: &MySqlPool,
        tables: &HistoryTables,
        record: &HistoryAppendRecord,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    ) -> Result<()> {
        let mut tx = pool.begin().await.map_err(|e| {
            Error::Internal(format!("Failed to begin MySQL history transaction: {e}"))
        })?;

        let insert_sql = format!(
            "INSERT IGNORE INTO {} (app_id, channel, stream_id, serial, published_at_ms, message_id, event_name, operation_kind, payload_bytes, payload_size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            .map_err(|e| Error::Internal(format!("Failed to insert MySQL history row: {e}")))?;

        let cutoff_ms = record
            .published_at_ms
            .saturating_sub((record.retention.retention_window_seconds * 1000) as i64);

        let age_stats_sql = format!(
            "SELECT COUNT(*) AS count, CAST(COALESCE(SUM(payload_size_bytes), 0) AS SIGNED) AS bytes FROM {} WHERE app_id = ? AND channel = ? AND published_at_ms < ?",
            tables.entries
        );
        let age_stats = sqlx::query(sqlx::AssertSqlSafe(age_stats_sql.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(cutoff_ms)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to inspect aged MySQL history rows: {e}"))
            })?;
        let mut evicted_messages = age_stats.get::<i64, _>("count") as u64;
        let mut evicted_bytes = age_stats.get::<i64, _>("bytes") as u64;

        let age_delete = format!(
            "DELETE FROM {} WHERE app_id = ? AND channel = ? AND published_at_ms < ?",
            tables.entries
        );
        sqlx::query(sqlx::AssertSqlSafe(age_delete.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(cutoff_ms)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to evict aged MySQL history rows: {e}"))
            })?;

        if let Some(max_messages) = record.retention.max_messages_per_channel {
            let count_sql = format!(
                "SELECT COUNT(*) AS count FROM {} WHERE app_id = ? AND channel = ?",
                tables.entries
            );
            let row = sqlx::query(sqlx::AssertSqlSafe(count_sql.as_str()))
                .bind(&record.app_id)
                .bind(&record.channel)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("Failed to count MySQL history rows: {e}")))?;
            let retained_messages = row.get::<i64, _>("count") as usize;
            if retained_messages > max_messages {
                let overflow = retained_messages - max_messages;
                let trim_stats_sql = format!(
                    "SELECT COUNT(*) AS count, CAST(COALESCE(SUM(payload_size_bytes), 0) AS SIGNED) AS bytes FROM (SELECT payload_size_bytes FROM {} WHERE app_id = ? AND channel = ? ORDER BY serial ASC LIMIT {}) t",
                    tables.entries, overflow
                );
                let trim_stats = sqlx::query(sqlx::AssertSqlSafe(trim_stats_sql.as_str()))
                    .bind(&record.app_id)
                    .bind(&record.channel)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("Failed to inspect MySQL trim rows: {e}"))
                    })?;
                evicted_messages =
                    evicted_messages.saturating_add(trim_stats.get::<i64, _>("count") as u64);
                evicted_bytes =
                    evicted_bytes.saturating_add(trim_stats.get::<i64, _>("bytes") as u64);

                let trim_sql = format!(
                    "DELETE e FROM {0} e JOIN (SELECT app_id, channel, stream_id, serial FROM {0} WHERE app_id = ? AND channel = ? ORDER BY serial ASC LIMIT {1}) old ON e.app_id = old.app_id AND e.channel = old.channel AND e.stream_id = old.stream_id AND e.serial = old.serial",
                    tables.entries, overflow
                );
                sqlx::query(sqlx::AssertSqlSafe(trim_sql.as_str()))
                    .bind(&record.app_id)
                    .bind(&record.channel)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("Failed to trim MySQL history rows by count: {e}"))
                    })?;
            }
        }

        if let Some(max_bytes) = record.retention.max_bytes_per_channel {
            let size_sql = format!(
                "SELECT serial, payload_size_bytes FROM {} WHERE app_id = ? AND channel = ? ORDER BY serial ASC",
                tables.entries
            );
            let rows = sqlx::query(sqlx::AssertSqlSafe(size_sql.as_str()))
                .bind(&record.app_id)
                .bind(&record.channel)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to inspect MySQL history bytes: {e}"))
                })?;
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
                    let placeholders = vec!["?"; serials.len()].join(", ");
                    let bytes_sql = format!(
                        "SELECT CAST(COALESCE(SUM(payload_size_bytes), 0) AS SIGNED) AS bytes FROM {} WHERE app_id = ? AND channel = ? AND serial IN ({})",
                        tables.entries, placeholders
                    );
                    let mut bytes_query = sqlx::query(sqlx::AssertSqlSafe(bytes_sql.as_str()))
                        .bind(&record.app_id)
                        .bind(&record.channel);
                    for serial in &serials {
                        bytes_query = bytes_query.bind(*serial);
                    }
                    let bytes_row = bytes_query.fetch_one(&mut *tx).await.map_err(|e| {
                        Error::Internal(format!("Failed to inspect MySQL byte trims: {e}"))
                    })?;
                    evicted_messages = evicted_messages.saturating_add(serials.len() as u64);
                    evicted_bytes =
                        evicted_bytes.saturating_add(bytes_row.get::<i64, _>("bytes") as u64);

                    let delete_sql = format!(
                        "DELETE FROM {} WHERE app_id = ? AND channel = ? AND serial IN ({})",
                        tables.entries, placeholders
                    );
                    let mut delete_query = sqlx::query(sqlx::AssertSqlSafe(delete_sql.as_str()))
                        .bind(&record.app_id)
                        .bind(&record.channel);
                    for serial in &serials {
                        delete_query = delete_query.bind(*serial);
                    }
                    delete_query.execute(&mut *tx).await.map_err(|e| {
                        Error::Internal(format!("Failed to trim MySQL history rows by bytes: {e}"))
                    })?;
                }
            }
        }

        let retained = Self::update_stream_retention_from_entries(
            &mut tx,
            tables,
            &record.app_id,
            &record.channel,
            record.published_at_ms,
        )
        .await?;

        let next_serial_sql = format!(
            "UPDATE {} SET next_serial = GREATEST(next_serial, ?) WHERE app_id = ? AND channel = ?",
            tables.streams
        );
        sqlx::query(sqlx::AssertSqlSafe(next_serial_sql.as_str()))
            .bind(record.serial.saturating_add(1) as i64)
            .bind(&record.app_id)
            .bind(&record.channel)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to advance MySQL history next_serial from append evidence: {e}"
                ))
            })?;

        tx.commit().await.map_err(|e| {
            Error::Internal(format!("Failed to commit MySQL history transaction: {e}"))
        })?;

        if let Some(metrics) = metrics.as_ref() {
            metrics.update_history_retained(
                &record.app_id,
                retained.retained_messages,
                retained.retained_bytes,
            );
            if evicted_messages > 0 || evicted_bytes > 0 {
                metrics.mark_history_eviction(&record.app_id, evicted_messages, evicted_bytes);
            }
        }
        Ok(())
    }
}
