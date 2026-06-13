use futures_util::TryStreamExt;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::{HistoryAppendRecord, HistoryDirection, HistoryRetentionStats};

use super::{
    EntryKeyRow, EntryRow, ScyllaHistoryStore, StreamRetentionUpdateParams, StreamWriteParams,
};

impl ScyllaHistoryStore {
    pub(super) async fn load_entry_keys_for_stream(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
    ) -> Result<Vec<EntryKeyRow>> {
        let query = format!(
            "SELECT serial, published_at_ms, payload_size_bytes FROM {} WHERE app_id = ? AND channel = ? AND stream_id = ? ORDER BY serial ASC",
            self.tables.entries_fq()
        );
        let pager = self
            .session
            .query_iter(query, (app_id, channel, stream_id))
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to stream ScyllaDB history entry keys: {e}"))
            })?;
        let mut rows_stream = pager.rows_stream::<EntryKeyRow>().map_err(|e| {
            Error::Internal(format!("Failed to decode ScyllaDB history entry keys: {e}"))
        })?;
        let mut rows = Vec::new();
        while let Some(row) = rows_stream.try_next().await.map_err(|e| {
            Error::Internal(format!("Failed to read ScyllaDB history entry keys: {e}"))
        })? {
            rows.push(row);
        }
        Ok(rows)
    }

    pub(super) async fn load_history_items_for_stream(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        direction: HistoryDirection,
    ) -> Result<Vec<EntryRow>> {
        let order = match direction {
            HistoryDirection::NewestFirst => "DESC",
            HistoryDirection::OldestFirst => "ASC",
        };
        let query = format!(
            "SELECT stream_id, serial, published_at_ms, message_id, event_name, operation_kind, payload_bytes, payload_size_bytes FROM {} WHERE app_id = ? AND channel = ? AND stream_id = ? ORDER BY serial {}",
            self.tables.entries_fq(),
            order
        );
        let pager = self
            .session
            .query_iter(query, (app_id, channel, stream_id))
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to stream ScyllaDB history items: {e}"))
            })?;
        let mut rows_stream = pager.rows_stream::<EntryRow>().map_err(|e| {
            Error::Internal(format!("Failed to decode ScyllaDB history items: {e}"))
        })?;
        let mut rows = Vec::new();
        while let Some(row) = rows_stream
            .try_next()
            .await
            .map_err(|e| Error::Internal(format!("Failed to read ScyllaDB history items: {e}")))?
        {
            rows.push(row);
        }
        Ok(rows)
    }

    pub(super) async fn delete_entries(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        entries: &[EntryKeyRow],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let query = format!(
            "DELETE FROM {} WHERE app_id = ? AND channel = ? AND stream_id = ? AND serial = ?",
            self.tables.entries_fq()
        );
        for entry in entries {
            self.session
                .query_unpaged(query.as_str(), (app_id, channel, stream_id, entry.serial))
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to delete ScyllaDB history row: {e}"))
                })?;
        }
        Ok(())
    }

    pub(super) async fn write_stream_record(&self, params: StreamWriteParams<'_>) -> Result<()> {
        let query = format!(
            "INSERT INTO {} (app_id, channel, stream_id, next_serial, durable_state, durable_state_reason, durable_state_node_id, durable_state_changed_at_ms, retained_messages, retained_bytes, oldest_available_serial, newest_available_serial, oldest_available_published_at_ms, newest_available_published_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            self.tables.streams_fq()
        );
        self.session
            .query_unpaged(
                query,
                (
                    params.app_id,
                    params.channel,
                    params.stream_id,
                    params.next_serial as i64,
                    params.durable_state.as_str(),
                    params.durable_state_reason,
                    params.durable_state_node_id,
                    params.durable_state_changed_at_ms,
                    params.retained.retained_messages as i64,
                    params.retained.retained_bytes as i64,
                    params.retained.oldest_serial.map(|value| value as i64),
                    params.retained.newest_serial.map(|value| value as i64),
                    params.retained.oldest_published_at_ms,
                    params.retained.newest_published_at_ms,
                    params.updated_at_ms,
                ),
            )
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to write ScyllaDB history stream row: {e}"))
            })?;
        Ok(())
    }

    pub(super) async fn update_stream_retention_from_entries(
        &self,
        params: StreamRetentionUpdateParams<'_>,
    ) -> Result<HistoryRetentionStats> {
        let rows = self
            .load_entry_keys_for_stream(params.app_id, params.channel, params.stream_id)
            .await?;
        let retained = HistoryRetentionStats {
            stream_id: Some(params.stream_id.to_string()),
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
        self.write_stream_record(StreamWriteParams {
            app_id: params.app_id,
            channel: params.channel,
            stream_id: params.stream_id,
            next_serial: params.next_serial,
            durable_state: params.durable_state,
            durable_state_reason: params.durable_state_reason,
            durable_state_node_id: params.durable_state_node_id,
            durable_state_changed_at_ms: params.durable_state_changed_at_ms,
            retained: &retained,
            updated_at_ms: params.updated_at_ms,
        })
        .await?;
        Ok(retained)
    }

    pub(super) async fn persist_record(&self, record: &HistoryAppendRecord) -> Result<()> {
        let insert_query = format!(
            "INSERT INTO {} (app_id, channel, stream_id, serial, published_at_ms, message_id, event_name, operation_kind, payload_bytes, payload_size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            self.tables.entries_fq()
        );
        self.session
            .query_unpaged(
                insert_query,
                (
                    &record.app_id,
                    &record.channel,
                    &record.stream_id,
                    record.serial as i64,
                    record.published_at_ms,
                    record.message_id.as_deref(),
                    record.event_name.as_deref(),
                    record.operation_kind.as_str(),
                    record.payload_bytes.as_ref(),
                    record.payload_bytes.len() as i64,
                ),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to insert ScyllaDB history row: {e}")))?;

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

        let current = self
            .load_stream_record(&record.app_id, &record.channel)
            .await?
            .ok_or_else(|| {
                Error::Internal(format!(
                    "Missing ScyllaDB history stream row for {}/{}",
                    record.app_id, record.channel
                ))
            })?;
        let retained = self
            .update_stream_retention_from_entries(StreamRetentionUpdateParams {
                app_id: &record.app_id,
                channel: &record.channel,
                stream_id: &record.stream_id,
                next_serial: current.next_serial.max(record.serial.saturating_add(1)),
                durable_state: current.durable_state,
                durable_state_reason: current.durable_state_reason.as_deref(),
                durable_state_node_id: current.durable_state_node_id.as_deref(),
                durable_state_changed_at_ms: current.durable_state_changed_at_ms,
                updated_at_ms: record.published_at_ms,
            })
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
