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

        let state_sql = format!(
            "SELECT stream_id, retained_messages, retained_bytes, retention_initialized FROM {} WHERE app_id=? AND channel=? FOR UPDATE",
            tables.streams
        );
        let state = sqlx::query(sqlx::AssertSqlSafe(state_sql.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to lock MySQL history stream: {e}")))?;
        if state.get::<String, _>("stream_id") != record.stream_id {
            return Err(Error::InvalidMessageFormat(
                "History append stream changed".into(),
            ));
        }
        let mut retained_messages = state.get::<i64, _>("retained_messages").max(0) as u64;
        let mut retained_bytes = state.get::<i64, _>("retained_bytes").max(0) as u64;
        if !state.get::<bool, _>("retention_initialized") {
            let initialize = format!(
                "SELECT COUNT(*) AS count, CAST(COALESCE(SUM(payload_size_bytes),0) AS SIGNED) AS bytes FROM {} WHERE app_id=? AND channel=?",
                tables.entries
            );
            let actual = sqlx::query(sqlx::AssertSqlSafe(initialize.as_str()))
                .bind(&record.app_id)
                .bind(&record.channel)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to reconcile legacy history accounting: {e}"
                    ))
                })?;
            retained_messages = actual.get::<i64, _>("count") as u64;
            retained_bytes = actual.get::<i64, _>("bytes") as u64;
        }
        let insert_sql = format!(
            "INSERT IGNORE INTO {} (app_id, channel, stream_id, serial, published_at_ms, message_id, event_name, operation_kind, payload_bytes, payload_size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tables.entries
        );
        let inserted = sqlx::query(sqlx::AssertSqlSafe(insert_sql.as_str()))
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
            .map_err(|e| Error::Internal(format!("Failed to insert MySQL history row: {e}")))?
            .rows_affected();
        retained_messages += inserted;
        retained_bytes += inserted * record.payload_bytes.len() as u64;
        let cutoff_ms = record.published_at_ms.saturating_sub(
            record
                .retention
                .retention_window_seconds
                .saturating_mul(1000)
                .min(i64::MAX as u64) as i64,
        );
        let age_select = format!(
            "SELECT stream_id, serial, payload_size_bytes FROM {} WHERE app_id=? AND channel=? AND published_at_ms<? ORDER BY published_at_ms, serial LIMIT 256",
            tables.entries
        );
        let mut evicted_messages = 0;
        let mut evicted_bytes = 0;
        loop {
            let aged = sqlx::query(sqlx::AssertSqlSafe(age_select.as_str()))
                .bind(&record.app_id)
                .bind(&record.channel)
                .bind(cutoff_ms)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to inspect aged MySQL history rows: {e}"))
                })?;
            if aged.is_empty() {
                break;
            }
            let delete = format!(
                "DELETE FROM {} WHERE app_id=? AND channel=? AND (stream_id, serial) IN ({})",
                tables.entries,
                vec!["(?,?)"; aged.len()].join(",")
            );
            let mut query = sqlx::query(sqlx::AssertSqlSafe(delete.as_str()))
                .bind(&record.app_id)
                .bind(&record.channel);
            let mut bytes = 0;
            for row in &aged {
                bytes += row.get::<i64, _>("payload_size_bytes").max(0) as u64;
                query = query
                    .bind(row.get::<String, _>("stream_id"))
                    .bind(row.get::<i64, _>("serial"));
            }
            query.execute(&mut *tx).await.map_err(|e| {
                Error::Internal(format!("Failed to evict aged MySQL history rows: {e}"))
            })?;
            let count = aged.len() as u64;
            evicted_messages += count;
            evicted_bytes += bytes;
            retained_messages = retained_messages.saturating_sub(count);
            retained_bytes = retained_bytes.saturating_sub(bytes);
            if count < 256 {
                break;
            }
        }
        loop {
            let excess_count = record
                .retention
                .max_messages_per_channel
                .map_or(0, |limit| retained_messages.saturating_sub(limit as u64));
            let excess_bytes = record
                .retention
                .max_bytes_per_channel
                .map_or(0, |limit| retained_bytes.saturating_sub(limit));
            if excess_count == 0 && excess_bytes == 0 {
                break;
            }
            let select = format!(
                "SELECT stream_id, serial, payload_size_bytes FROM {} WHERE app_id=? AND channel=? ORDER BY serial ASC LIMIT 256",
                tables.entries
            );
            let rows = sqlx::query(sqlx::AssertSqlSafe(select.as_str()))
                .bind(&record.app_id)
                .bind(&record.channel)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to read MySQL history eviction prefix: {e}"))
                })?;
            if rows.is_empty() {
                break;
            }
            let mut count = 0u64;
            let mut bytes = 0u64;
            let mut keys = Vec::<(String, i64)>::new();
            for row in rows {
                if count >= excess_count && bytes >= excess_bytes {
                    break;
                }
                count += 1;
                bytes += row.get::<i64, _>("payload_size_bytes").max(0) as u64;
                keys.push((row.get("stream_id"), row.get("serial")));
            }
            let placeholders = vec!["(?,?)"; keys.len()].join(",");
            let delete = format!(
                "DELETE FROM {} WHERE app_id=? AND channel=? AND (stream_id,serial) IN ({placeholders})",
                tables.entries
            );
            let mut query = sqlx::query(sqlx::AssertSqlSafe(delete.as_str()))
                .bind(&record.app_id)
                .bind(&record.channel);
            for (stream, serial) in keys {
                query = query.bind(stream).bind(serial);
            }
            query.execute(&mut *tx).await.map_err(|e| {
                Error::Internal(format!("Failed to evict MySQL history prefix: {e}"))
            })?;
            retained_messages = retained_messages.saturating_sub(count);
            retained_bytes = retained_bytes.saturating_sub(bytes);
            evicted_messages += count;
            evicted_bytes += bytes;
        }
        let bounds = format!(
            "SELECT (SELECT serial FROM {0} WHERE app_id=? AND channel=? ORDER BY serial ASC LIMIT 1) AS oldest_serial, (SELECT serial FROM {0} WHERE app_id=? AND channel=? ORDER BY serial DESC LIMIT 1) AS newest_serial, (SELECT published_at_ms FROM {0} WHERE app_id=? AND channel=? ORDER BY published_at_ms ASC LIMIT 1) AS oldest_time, (SELECT published_at_ms FROM {0} WHERE app_id=? AND channel=? ORDER BY published_at_ms DESC LIMIT 1) AS newest_time",
            tables.entries
        );
        let bounds = sqlx::query(sqlx::AssertSqlSafe(bounds.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(&record.app_id)
            .bind(&record.channel)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to seek MySQL history bounds: {e}")))?;
        let update = format!(
            "UPDATE {} SET retention_initialized=TRUE, retained_messages=?, retained_bytes=?, oldest_available_serial=?, newest_available_serial=?, oldest_available_published_at_ms=?, newest_available_published_at_ms=?, updated_at_ms=?, next_serial=GREATEST(next_serial,?) WHERE app_id=? AND channel=?",
            tables.streams
        );
        sqlx::query(sqlx::AssertSqlSafe(update.as_str()))
            .bind(retained_messages as i64)
            .bind(retained_bytes as i64)
            .bind(bounds.get::<Option<i64>, _>("oldest_serial"))
            .bind(bounds.get::<Option<i64>, _>("newest_serial"))
            .bind(bounds.get::<Option<i64>, _>("oldest_time"))
            .bind(bounds.get::<Option<i64>, _>("newest_time"))
            .bind(record.published_at_ms)
            .bind(record.serial.saturating_add(1) as i64)
            .bind(&record.app_id)
            .bind(&record.channel)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to update MySQL history retention: {e}"))
            })?;

        tx.commit().await.map_err(|e| {
            Error::Internal(format!("Failed to commit MySQL history transaction: {e}"))
        })?;

        if let Some(metrics) = metrics.as_ref() {
            metrics.update_history_retained(&record.app_id, retained_messages, retained_bytes);
            if evicted_messages > 0 || evicted_bytes > 0 {
                metrics.mark_history_eviction(&record.app_id, evicted_messages, evicted_bytes);
            }
        }
        Ok(())
    }
}
