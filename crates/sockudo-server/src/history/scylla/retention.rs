//! Exact retention accounting colocated with entries in one conditional-batch partition.
use super::*;
use scylla::statement::batch::{Batch, BatchType};
use scylla::value::{CqlValue, Row};

#[derive(Clone, Debug, scylla::DeserializeRow)]
pub(super) struct RetentionState {
    retention_count: Option<i64>,
    retention_bytes: Option<i64>,
    retention_revision: Option<i64>,
    retention_oldest_serial: Option<i64>,
    retention_newest_serial: Option<i64>,
    retention_oldest_time: Option<i64>,
    retention_newest_time: Option<i64>,
}
impl RetentionState {
    fn revision(&self) -> i64 {
        self.retention_revision.unwrap_or(0)
    }
    fn count(&self) -> i64 {
        self.retention_count.unwrap_or(0)
    }
    fn bytes(&self) -> i64 {
        self.retention_bytes.unwrap_or(0)
    }
    pub(super) fn stats(&self, stream_id: &str) -> HistoryRetentionStats {
        HistoryRetentionStats {
            stream_id: Some(stream_id.to_string()),
            retained_messages: self.count().max(0) as u64,
            retained_bytes: self.bytes().max(0) as u64,
            oldest_serial: self.retention_oldest_serial.map(|v| v as u64),
            newest_serial: self.retention_newest_serial.map(|v| v as u64),
            oldest_published_at_ms: self.retention_oldest_time,
            newest_published_at_ms: self.retention_newest_time,
        }
    }
}
fn applied(result: scylla::response::query_result::QueryResult) -> Result<bool> {
    let rows = result.into_rows_result().map_err(|e| {
        Error::Internal(format!(
            "Failed to decode ScyllaDB retention transaction: {e}"
        ))
    })?;
    let row = rows.first_row::<Row>().map_err(|e| {
        Error::Internal(format!(
            "Failed to decode ScyllaDB retention condition: {e}"
        ))
    })?;
    match row.columns.first() {
        Some(Some(CqlValue::Boolean(applied))) => Ok(*applied),
        _ => Err(Error::Internal(
            "Missing ScyllaDB retention condition result".into(),
        )),
    }
}
impl ScyllaHistoryStore {
    pub(super) async fn load_partition_retention(
        &self,
        app: &str,
        channel: &str,
        stream: &str,
    ) -> Result<Option<RetentionState>> {
        let sql = format!(
            "SELECT retention_count,retention_bytes,retention_revision,retention_oldest_serial,retention_newest_serial,retention_oldest_time,retention_newest_time FROM {} WHERE app_id=? AND channel=? AND stream_id=? LIMIT 1",
            self.tables.entries_fq()
        );
        let rows = self
            .session
            .query_unpaged(sql, (app, channel, stream))
            .await
            .map_err(|e| Error::Internal(format!("Failed to read ScyllaDB retention state: {e}")))?
            .into_rows_result()
            .map_err(|e| {
                Error::Internal(format!("Failed to decode ScyllaDB retention state: {e}"))
            })?;
        Ok(rows
            .maybe_first_row::<RetentionState>()
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB retention state: {e}"
                ))
            })?
            .filter(|state| state.retention_revision.is_some()))
    }
    async fn ensure_partition_retention(
        &self,
        app: &str,
        channel: &str,
        stream: &str,
    ) -> Result<RetentionState> {
        for _ in 0..64 {
            if let Some(state) = self.load_partition_retention(app, channel, stream).await? {
                return Ok(state);
            }
            // Legacy partitions reconcile once. Conditional initialization ensures
            // concurrent new writers cannot replace a newer counter snapshot.
            let legacy = self
                .load_entry_keys_for_stream(app, channel, stream)
                .await?;
            let sql = format!(
                "UPDATE {} SET retention_count=?,retention_bytes=?,retention_revision=0,retention_oldest_serial=?,retention_newest_serial=?,retention_oldest_time=?,retention_newest_time=? WHERE app_id=? AND channel=? AND stream_id=? IF retention_revision=null",
                self.tables.entries_fq()
            );
            let mut stmt = Statement::new(sql);
            stmt.set_serial_consistency(Some(SerialConsistency::LocalSerial));
            let result = self
                .session
                .query_unpaged(
                    stmt,
                    (
                        legacy.len() as i64,
                        legacy.iter().map(|row| row.payload_size_bytes).sum::<i64>(),
                        legacy.first().map(|row| row.serial),
                        legacy.last().map(|row| row.serial),
                        legacy.first().map(|row| row.published_at_ms),
                        legacy.last().map(|row| row.published_at_ms),
                        app,
                        channel,
                        stream,
                    ),
                )
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to initialize ScyllaDB retention state: {e}"
                    ))
                })?;
            if applied(result)? {
                continue;
            }
            tokio::task::yield_now().await;
        }
        Err(Error::Internal(
            "ScyllaDB retention initialization contention limit reached".into(),
        ))
    }
    fn retention_write(
        &self,
        app: &str,
        channel: &str,
        stream: &str,
        current: &RetentionState,
        next: &RetentionState,
    ) -> (Statement, Vec<Option<CqlValue>>) {
        let query = format!(
            "UPDATE {} SET retention_count=?,retention_bytes=?,retention_revision=?,retention_oldest_serial=?,retention_newest_serial=?,retention_oldest_time=?,retention_newest_time=? WHERE app_id=? AND channel=? AND stream_id=? IF retention_revision=?",
            self.tables.entries_fq()
        );
        let values = vec![
            Some(CqlValue::BigInt(next.count())),
            Some(CqlValue::BigInt(next.bytes())),
            Some(CqlValue::BigInt(current.revision() + 1)),
            next.retention_oldest_serial.map(CqlValue::BigInt),
            next.retention_newest_serial.map(CqlValue::BigInt),
            next.retention_oldest_time.map(CqlValue::BigInt),
            next.retention_newest_time.map(CqlValue::BigInt),
            Some(CqlValue::Text(app.to_string())),
            Some(CqlValue::Text(channel.to_string())),
            Some(CqlValue::Text(stream.to_string())),
            Some(CqlValue::BigInt(current.revision())),
        ];
        (Statement::new(query), values)
    }
    async fn retention_prefix(
        &self,
        app: &str,
        channel: &str,
        stream: &str,
        newest: bool,
    ) -> Result<Vec<EntryKeyRow>> {
        let query = format!(
            "SELECT serial,published_at_ms,payload_size_bytes FROM {} WHERE app_id=? AND channel=? AND stream_id=? AND serial>=0 ORDER BY serial {} LIMIT 65",
            self.tables.entries_fq(),
            if newest { "DESC" } else { "ASC" }
        );
        let result = self
            .session
            .query_unpaged(query, (app, channel, stream))
            .await
            .map_err(|e| Error::Internal(format!("Failed to read ScyllaDB eviction prefix: {e}")))?
            .into_rows_result()
            .map_err(|e| {
                Error::Internal(format!("Failed to decode ScyllaDB eviction prefix: {e}"))
            })?;
        result
            .rows::<EntryKeyRow>()
            .map_err(|e| Error::Internal(format!("Failed to type ScyllaDB eviction prefix: {e}")))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB eviction prefix: {e}"
                ))
            })
    }
    pub(super) async fn delete_accounted_entries(
        &self,
        app: &str,
        channel: &str,
        stream: &str,
        entries: &[EntryKeyRow],
    ) -> Result<()> {
        self.delete_accounted_entries_at_revision(app, channel, stream, entries, None)
            .await
            .map(|_| ())
    }

    async fn delete_accounted_entries_at_revision(
        &self,
        app: &str,
        channel: &str,
        stream: &str,
        entries: &[EntryKeyRow],
        expected_revision: Option<i64>,
    ) -> Result<bool> {
        for chunk in entries.chunks(64) {
            let mut completed = false;
            for _ in 0..64 {
                let current = self
                    .ensure_partition_retention(app, channel, stream)
                    .await?;
                if expected_revision.is_some_and(|revision| revision != current.revision()) {
                    return Ok(false);
                }
                let serials: Vec<i64> = chunk.iter().map(|row| row.serial).collect();
                let query = format!(
                    "SELECT serial,published_at_ms,payload_size_bytes FROM {} WHERE app_id=? AND channel=? AND stream_id=? AND serial IN ?",
                    self.tables.entries_fq()
                );
                let result = self
                    .session
                    .query_unpaged(query, (app, channel, stream, &serials))
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("Failed to inspect ScyllaDB eviction keys: {e}"))
                    })?
                    .into_rows_result()
                    .map_err(|e| {
                        Error::Internal(format!("Failed to decode ScyllaDB eviction keys: {e}"))
                    })?;
                let present = result
                    .rows::<EntryKeyRow>()
                    .map_err(|e| {
                        Error::Internal(format!("Failed to type ScyllaDB eviction keys: {e}"))
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| {
                        Error::Internal(format!(
                            "Failed to deserialize ScyllaDB eviction keys: {e}"
                        ))
                    })?;
                if present.is_empty() {
                    completed = true;
                    break;
                }
                let removed: std::collections::HashSet<i64> =
                    present.iter().map(|row| row.serial).collect();
                let oldest = self.retention_prefix(app, channel, stream, false).await?;
                let newest = self.retention_prefix(app, channel, stream, true).await?;
                let first = oldest.iter().find(|row| !removed.contains(&row.serial));
                let last = newest.iter().find(|row| !removed.contains(&row.serial));
                let mut next = current.clone();
                next.retention_count = Some(current.count().saturating_sub(present.len() as i64));
                next.retention_bytes = Some(
                    current.bytes().saturating_sub(
                        present
                            .iter()
                            .map(|row| row.payload_size_bytes)
                            .sum::<i64>(),
                    ),
                );
                next.retention_oldest_serial = first.map(|row| row.serial);
                next.retention_newest_serial = last.map(|row| row.serial);
                next.retention_oldest_time = first.map(|row| row.published_at_ms);
                next.retention_newest_time = last.map(|row| row.published_at_ms);
                let (update, values) = self.retention_write(app, channel, stream, &current, &next);
                let mut batch = Batch::new(BatchType::Logged);
                batch.set_serial_consistency(Some(SerialConsistency::LocalSerial));
                batch.append_statement(update);
                let mut all_values = vec![values];
                for row in &present {
                    batch.append_statement(Statement::new(format!(
                        "DELETE FROM {} WHERE app_id=? AND channel=? AND stream_id=? AND serial=?",
                        self.tables.entries_fq()
                    )));
                    all_values.push(vec![
                        Some(CqlValue::Text(app.to_string())),
                        Some(CqlValue::Text(channel.to_string())),
                        Some(CqlValue::Text(stream.to_string())),
                        Some(CqlValue::BigInt(row.serial)),
                    ]);
                }
                let result = self.session.batch(&batch, all_values).await.map_err(|e| {
                    Error::Internal(format!("Failed to commit ScyllaDB eviction batch: {e}"))
                })?;
                if applied(result)? {
                    completed = true;
                    break;
                }
                tokio::task::yield_now().await;
            }
            if !completed {
                return Err(Error::Internal(
                    "ScyllaDB history eviction contention limit reached".into(),
                ));
            }
        }
        Ok(true)
    }
    pub(super) async fn persist_accounted_record(
        &self,
        record: &HistoryAppendRecord,
    ) -> Result<()> {
        let stream = self
            .load_stream_record(&record.app_id, &record.channel)
            .await?
            .ok_or_else(|| Error::Internal("Missing ScyllaDB history stream".into()))?;
        if stream.stream_id != record.stream_id {
            return Err(Error::InvalidMessageFormat(
                "History append stream changed".into(),
            ));
        }
        let mut inserted = false;
        for _ in 0..64 {
            let current = self
                .ensure_partition_retention(&record.app_id, &record.channel, &record.stream_id)
                .await?;
            let mut next = current.clone();
            next.retention_count = Some(current.count() + 1);
            next.retention_bytes = Some(current.bytes() + record.payload_bytes.len() as i64);
            if next
                .retention_oldest_serial
                .is_none_or(|serial| (record.serial as i64) < serial)
            {
                next.retention_oldest_serial = Some(record.serial as i64);
                next.retention_oldest_time = Some(record.published_at_ms);
            }
            if next
                .retention_newest_serial
                .is_none_or(|serial| (record.serial as i64) > serial)
            {
                next.retention_newest_serial = Some(record.serial as i64);
                next.retention_newest_time = Some(record.published_at_ms);
            }
            let (update, values) = self.retention_write(
                &record.app_id,
                &record.channel,
                &record.stream_id,
                &current,
                &next,
            );
            let mut batch = Batch::new(BatchType::Logged);
            batch.set_serial_consistency(Some(SerialConsistency::LocalSerial));
            batch.append_statement(update);
            batch.append_statement(Statement::new(format!("INSERT INTO {} (app_id,channel,stream_id,serial,published_at_ms,message_id,event_name,operation_kind,payload_bytes,payload_size_bytes) VALUES (?,?,?,?,?,?,?,?,?,?) IF NOT EXISTS",self.tables.entries_fq())));
            let result = self
                .session
                .batch(
                    &batch,
                    (
                        values,
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
                    ),
                )
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to commit ScyllaDB history append: {e}"))
                })?;
            if applied(result)? {
                inserted = true;
                break;
            }
            let query = format!(
                "SELECT payload_bytes,published_at_ms,message_id,event_name,operation_kind FROM {} WHERE app_id=? AND channel=? AND stream_id=? AND serial=?",
                self.tables.entries_fq()
            );
            let existing = self
                .session
                .query_unpaged(
                    query,
                    (
                        &record.app_id,
                        &record.channel,
                        &record.stream_id,
                        record.serial as i64,
                    ),
                )
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to inspect duplicate ScyllaDB append: {e}"))
                })?
                .into_rows_result()
                .map_err(|e| {
                    Error::Internal(format!("Failed to decode duplicate ScyllaDB append: {e}"))
                })?;
            if let Some((bytes, time, message, event, operation)) = existing
                .maybe_first_row::<(Vec<u8>, i64, Option<String>, Option<String>, String)>()
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to deserialize duplicate ScyllaDB append: {e}"
                    ))
                })?
            {
                if bytes != record.payload_bytes
                    || time != record.published_at_ms
                    || message != record.message_id
                    || event != record.event_name
                    || operation != record.operation_kind
                {
                    return Err(Error::InvalidMessageFormat(
                        "Conflicting ScyllaDB history append at existing serial".into(),
                    ));
                }
                inserted = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        if !inserted {
            return Err(Error::Internal(
                "ScyllaDB history append contention limit reached".into(),
            ));
        }
        let cutoff = record.published_at_ms.saturating_sub(
            record
                .retention
                .retention_window_seconds
                .saturating_mul(1000)
                .min(i64::MAX as u64) as i64,
        );
        let mut age_prefix_complete = false;
        let mut conflicts = 0;
        let mut evicted_messages = 0;
        let mut evicted_bytes = 0;
        loop {
            let current = self
                .ensure_partition_retention(&record.app_id, &record.channel, &record.stream_id)
                .await?;
            let rows = self
                .retention_prefix(&record.app_id, &record.channel, &record.stream_id, false)
                .await?;
            let mut count = current.count();
            let mut bytes = current.bytes();
            let mut remove = 0;
            let mut next_age_prefix_complete = age_prefix_complete;
            for row in rows.iter().take(64) {
                if !next_age_prefix_complete && row.published_at_ms >= cutoff {
                    next_age_prefix_complete = true;
                }
                if next_age_prefix_complete
                    && !record
                        .retention
                        .max_messages_per_channel
                        .is_some_and(|limit| count > limit as i64)
                    && !record
                        .retention
                        .max_bytes_per_channel
                        .is_some_and(|limit| bytes > limit as i64)
                {
                    break;
                }
                count -= 1;
                bytes -= row.payload_size_bytes;
                remove += 1;
            }
            if remove == 0 {
                break;
            }
            if self
                .delete_accounted_entries_at_revision(
                    &record.app_id,
                    &record.channel,
                    &record.stream_id,
                    &rows[..remove],
                    Some(current.revision()),
                )
                .await?
            {
                age_prefix_complete = next_age_prefix_complete;
                conflicts = 0;
                evicted_messages += remove as u64;
                evicted_bytes += rows[..remove]
                    .iter()
                    .map(|row| row.payload_size_bytes as u64)
                    .sum::<u64>();
            } else {
                conflicts += 1;
                if conflicts >= 64 {
                    return Err(Error::Internal(
                        "ScyllaDB history maintenance contention limit reached".into(),
                    ));
                }
                tokio::task::yield_now().await;
            }
        }
        // Serial reservation remains in its existing stream row; advance only
        // its floor, preserving concurrent reservations and operator state.
        let floor = record.serial.saturating_add(1) as i64;
        let mut floor_advanced = false;
        for _ in 0..64 {
            let current = self
                .load_stream_record(&record.app_id, &record.channel)
                .await?
                .ok_or_else(|| Error::Internal("Missing ScyllaDB history stream".into()))?;
            if current.stream_id != record.stream_id {
                return Err(Error::InvalidMessageFormat(
                    "History stream changed during append".into(),
                ));
            }
            if current.next_serial >= floor as u64 {
                floor_advanced = true;
                break;
            }
            let mut query = Statement::new(format!(
                "UPDATE {} SET next_serial=? WHERE app_id=? AND channel=? IF stream_id=? AND next_serial=?",
                self.tables.streams_fq()
            ));
            query.set_serial_consistency(Some(SerialConsistency::LocalSerial));
            if applied(
                self.session
                    .query_unpaged(
                        query,
                        (
                            floor,
                            &record.app_id,
                            &record.channel,
                            &record.stream_id,
                            current.next_serial as i64,
                        ),
                    )
                    .await
                    .map_err(|e| {
                        Error::Internal(format!(
                            "Failed to advance ScyllaDB history serial floor: {e}"
                        ))
                    })?,
            )? {
                floor_advanced = true;
                break;
            }
        }
        if !floor_advanced {
            return Err(Error::Internal(
                "ScyllaDB history serial floor contention limit reached".into(),
            ));
        }
        if let Some(metrics) = &self.metrics {
            if evicted_messages > 0 {
                metrics.mark_history_eviction(&record.app_id, evicted_messages, evicted_bytes);
            }
            let retained = self
                .ensure_partition_retention(&record.app_id, &record.channel, &record.stream_id)
                .await?
                .stats(&record.stream_id);
            metrics.update_history_retained(
                &record.app_id,
                retained.retained_messages,
                retained.retained_bytes,
            );
        }
        Ok(())
    }
}
