use sockudo_core::error::{Error, Result};
use sockudo_core::history::{
    HistoryAppendRecord, HistoryDirection, HistoryReadRequest, HistoryRetentionStats,
};

use super::{EntryKeyRecord, StoredEntryRecord, StoredStreamRecord, SurrealHistoryStore};

impl SurrealHistoryStore {
    pub(super) async fn load_entry_keys_for_stream(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
    ) -> Result<Vec<EntryKeyRecord>> {
        let mut response = self
            .db
            .query(format!(
                "SELECT serial, published_at_ms, payload_size_bytes FROM {} WHERE app_id = $app_id AND channel = $channel AND stream_id = $stream_id ORDER BY serial ASC",
                self.tables.entries
            ))
            .bind(("app_id", app_id.to_string()))
            .bind(("channel", channel.to_string()))
            .bind(("stream_id", stream_id.to_string()))
            .await
            .map_err(|e| Error::Internal(format!("Failed to query SurrealDB history entry keys: {e}")))?;
        response.take(0usize).map_err(|e| {
            Error::Internal(format!(
                "Failed to decode SurrealDB history entry keys: {e}"
            ))
        })
    }

    pub(super) async fn load_page_entries_for_stream(
        &self,
        request: &HistoryReadRequest,
        stream_id: &str,
    ) -> Result<Vec<StoredEntryRecord>> {
        let mut clauses = vec![
            "app_id = $app_id".to_string(),
            "channel = $channel".to_string(),
            "stream_id = $stream_id".to_string(),
        ];
        let mut cursor_serial = None;
        let mut start_serial_bind = None;
        let mut end_serial_bind = None;
        let mut start_time_ms_bind = None;
        let mut end_time_ms_bind = None;
        if let Some(cursor) = request.cursor.as_ref() {
            clauses.push(match request.direction {
                HistoryDirection::NewestFirst => "serial < $cursor_serial".to_string(),
                HistoryDirection::OldestFirst => "serial > $cursor_serial".to_string(),
            });
            cursor_serial = Some(cursor.serial as i64);
        }
        if let Some(start_serial) = request.bounds.start_serial {
            clauses.push("serial >= $start_serial".to_string());
            start_serial_bind = Some(start_serial as i64);
        }
        if let Some(end_serial) = request.bounds.end_serial {
            clauses.push("serial <= $end_serial".to_string());
            end_serial_bind = Some(end_serial as i64);
        }
        if let Some(start_time_ms) = request.bounds.start_time_ms {
            clauses.push("published_at_ms >= $start_time_ms".to_string());
            start_time_ms_bind = Some(start_time_ms);
        }
        if let Some(end_time_ms) = request.bounds.end_time_ms {
            clauses.push("published_at_ms <= $end_time_ms".to_string());
            end_time_ms_bind = Some(end_time_ms);
        }

        let order = match request.direction {
            HistoryDirection::NewestFirst => "DESC",
            HistoryDirection::OldestFirst => "ASC",
        };
        let sql = format!(
            "SELECT app_id, channel, stream_id, serial, published_at_ms, message_id, event_name, operation_kind, payload_bytes, payload_size_bytes FROM {} WHERE {} ORDER BY serial {} LIMIT {}",
            self.tables.entries,
            clauses.join(" AND "),
            order,
            request.limit + 1
        );
        let mut query = self.db.query(sql);
        query = query
            .bind(("app_id", request.app_id.clone()))
            .bind(("channel", request.channel.clone()))
            .bind(("stream_id", stream_id.to_string()));
        if let Some(value) = cursor_serial {
            query = query.bind(("cursor_serial", value));
        }
        if let Some(value) = start_serial_bind {
            query = query.bind(("start_serial", value));
        }
        if let Some(value) = end_serial_bind {
            query = query.bind(("end_serial", value));
        }
        if let Some(value) = start_time_ms_bind {
            query = query.bind(("start_time_ms", value));
        }
        if let Some(value) = end_time_ms_bind {
            query = query.bind(("end_time_ms", value));
        }
        let mut response = query
            .await
            .map_err(|e| Error::Internal(format!("Failed to query SurrealDB history page: {e}")))?;
        response.take(0usize).map_err(|e| {
            Error::Internal(format!(
                "Failed to decode SurrealDB history page entries: {e}"
            ))
        })
    }
    pub(super) async fn delete_entries(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        rows: &[EntryKeyRecord],
    ) -> Result<()> {
        for row in rows {
            let _: Option<StoredEntryRecord> = self
                .db
                .delete(self.entry_resource(app_id, channel, stream_id, row.serial as u64))
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to delete SurrealDB history row: {e}"))
                })?;
        }
        Ok(())
    }

    pub(super) async fn persist_record(&self, record: &HistoryAppendRecord) -> Result<()> {
        let stored = StoredEntryRecord {
            app_id: record.app_id.clone(),
            channel: record.channel.clone(),
            stream_id: record.stream_id.clone(),
            serial: record.serial as i64,
            published_at_ms: record.published_at_ms,
            message_id: record.message_id.clone(),
            event_name: record.event_name.clone(),
            operation_kind: record.operation_kind.clone(),
            payload_bytes: record.payload_bytes.to_vec(),
            payload_size_bytes: record.payload_bytes.len() as i64,
        };
        let _: Option<StoredEntryRecord> = self
            .db
            .create(self.entry_resource(
                &record.app_id,
                &record.channel,
                &record.stream_id,
                record.serial,
            ))
            .content(stored)
            .await
            .map_err(|e| Error::Internal(format!("Failed to create SurrealDB history row: {e}")))?;

        let mut rows = self
            .load_entry_keys_for_stream(&record.app_id, &record.channel, &record.stream_id)
            .await?;
        let cutoff_ms = record
            .published_at_ms
            .saturating_sub((record.retention.retention_window_seconds * 1000) as i64);
        let mut to_delete = Vec::new();

        while let Some(first) = rows.first() {
            if first.published_at_ms < cutoff_ms {
                to_delete.push(first.clone());
                rows.remove(0);
            } else {
                break;
            }
        }
        if let Some(max_messages) = record.retention.max_messages_per_channel {
            while rows.len() > max_messages {
                to_delete.push(rows.remove(0));
            }
        }
        if let Some(max_bytes) = record.retention.max_bytes_per_channel {
            let mut retained_bytes: u64 = rows
                .iter()
                .map(|row| row.payload_size_bytes.max(0) as u64)
                .sum();
            while retained_bytes > max_bytes && !rows.is_empty() {
                let removed = rows.remove(0);
                retained_bytes =
                    retained_bytes.saturating_sub(removed.payload_size_bytes.max(0) as u64);
                to_delete.push(removed);
            }
        }

        self.delete_entries(
            &record.app_id,
            &record.channel,
            &record.stream_id,
            &to_delete,
        )
        .await?;

        let retained = HistoryRetentionStats {
            stream_id: Some(record.stream_id.clone()),
            retained_messages: rows.len() as u64,
            retained_bytes: rows
                .iter()
                .map(|row| row.payload_size_bytes.max(0) as u64)
                .sum(),
            oldest_serial: rows.first().map(|row| row.serial as u64),
            newest_serial: rows.last().map(|row| row.serial as u64),
            oldest_published_at_ms: rows.first().map(|row| row.published_at_ms),
            newest_published_at_ms: rows.last().map(|row| row.published_at_ms),
        };
        let current = self
            .load_stream_raw(&record.app_id, &record.channel)
            .await?
            .ok_or_else(|| {
                Error::Internal(format!(
                    "Missing SurrealDB history stream row for {}/{}",
                    record.app_id, record.channel
                ))
            })?;
        let updated_stream = StoredStreamRecord {
            next_serial: current
                .next_serial
                .max(record.serial.saturating_add(1) as i64),
            retained_messages: retained.retained_messages as i64,
            retained_bytes: retained.retained_bytes as i64,
            oldest_available_serial: retained.oldest_serial.map(|value| value as i64),
            newest_available_serial: retained.newest_serial.map(|value| value as i64),
            oldest_available_published_at_ms: retained.oldest_published_at_ms,
            newest_available_published_at_ms: retained.newest_published_at_ms,
            updated_at_ms: record.published_at_ms,
            ..current
        };
        self.upsert_stream_raw(&record.app_id, &record.channel, &updated_stream)
            .await?;
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.update_history_retained(
                &record.app_id,
                retained.retained_messages,
                retained.retained_bytes,
            );
            if !to_delete.is_empty() {
                let evicted_bytes = to_delete
                    .iter()
                    .map(|row| row.payload_size_bytes.max(0) as u64)
                    .sum();
                metrics.mark_history_eviction(
                    &record.app_id,
                    to_delete.len() as u64,
                    evicted_bytes,
                );
            }
        }
        Ok(())
    }
}
