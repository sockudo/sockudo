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
            "SELECT serial, published_at_ms, payload_size_bytes FROM {} WHERE app_id = ? AND channel = ? AND stream_id = ? AND serial >= 0 ORDER BY serial ASC",
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

    pub(super) async fn load_history_page(
        &self,
        request: &sockudo_core::history::HistoryReadRequest,
        stream_id: &str,
    ) -> Result<(Vec<EntryRow>, Option<u64>)> {
        let mut lower = request.bounds.start_serial.unwrap_or(0);
        let mut upper = request
            .bounds
            .end_serial
            .unwrap_or(i64::MAX as u64)
            .min(i64::MAX as u64);
        if let Some(cursor) = request.cursor.as_ref() {
            match request.direction {
                HistoryDirection::NewestFirst => {
                    let Some(before) = cursor.serial.checked_sub(1) else {
                        return Ok((Vec::new(), None));
                    };
                    upper = upper.min(before);
                }
                HistoryDirection::OldestFirst => {
                    let Some(after) = cursor.serial.checked_add(1) else {
                        return Ok((Vec::new(), None));
                    };
                    lower = lower.max(after);
                }
            }
        }
        if lower > upper {
            return Ok((Vec::new(), None));
        }
        let order = match request.direction {
            HistoryDirection::NewestFirst => "DESC",
            HistoryDirection::OldestFirst => "ASC",
        };
        let scan_budget = request.limit.saturating_mul(32).clamp(256, 4096);
        let query = format!(
            "SELECT stream_id, serial, published_at_ms, message_id, event_name, operation_kind, payload_bytes, payload_size_bytes FROM {} WHERE app_id = ? AND channel = ? AND stream_id = ? AND serial >= ? AND serial <= ? ORDER BY serial {} LIMIT ?",
            self.tables.entries_fq(),
            order
        );
        let mut statement = scylla::statement::unprepared::Statement::new(query);
        statement.set_page_size((request.limit + 1).min(256) as i32);
        let pager = self
            .session
            .query_iter(
                statement,
                (
                    &request.app_id,
                    &request.channel,
                    stream_id,
                    lower as i64,
                    upper as i64,
                    (scan_budget + 1) as i32,
                ),
            )
            .await
            .map_err(|e| Error::Internal(format!("failed to stream bounded history page: {e}")))?;
        let mut stream = pager
            .rows_stream::<EntryRow>()
            .map_err(|e| Error::Internal(format!("failed to decode bounded history page: {e}")))?;
        let mut rows = Vec::with_capacity(request.limit + 1);
        let mut last_scanned = None;
        let mut scanned = 0;
        while let Some(row) = stream
            .try_next()
            .await
            .map_err(|e| Error::Internal(format!("failed to read bounded history page: {e}")))?
        {
            if scanned == scan_budget {
                // Continue after the last inspected row, including when sparse
                // time filters produce an empty page. Never skip the lookahead.
                return Ok((rows, last_scanned));
            }
            scanned += 1;
            last_scanned = Some(row.serial as u64);
            if request
                .bounds
                .start_time_ms
                .is_none_or(|start| row.published_at_ms >= start)
                && request
                    .bounds
                    .end_time_ms
                    .is_none_or(|end| row.published_at_ms <= end)
            {
                rows.push(row);
                if rows.len() > request.limit {
                    break;
                }
            }
        }
        Ok((rows, None))
    }

    pub(super) async fn delete_entries(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        entries: &[EntryKeyRow],
    ) -> Result<()> {
        self.delete_accounted_entries(app_id, channel, stream_id, entries)
            .await
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
        self.persist_accounted_record(record).await
    }
}
