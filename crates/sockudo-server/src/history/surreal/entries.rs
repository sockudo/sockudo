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
    async fn accounted_state(&self, current: &StoredStreamRecord) -> Result<StoredStreamRecord> {
        let mut next = current.clone();
        if current.retention_revision.is_none() {
            let legacy = self
                .load_entry_keys_for_stream(&current.app_id, &current.channel, &current.stream_id)
                .await?;
            next.retained_messages = legacy.len() as i64;
            next.retained_bytes = legacy.iter().map(|row| row.payload_size_bytes).sum();
        }
        Ok(next)
    }

    pub(super) async fn retention_prefix(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        newest: bool,
    ) -> Result<Vec<EntryKeyRecord>> {
        let sql = format!(
            "SELECT serial,published_at_ms,payload_size_bytes FROM {} WHERE app_id=$app AND channel=$channel AND stream_id=$stream ORDER BY serial {} LIMIT {}",
            self.tables.entries,
            if newest { "DESC" } else { "ASC" },
            if newest { 1 } else { 256 }
        );
        let mut response = self
            .db
            .query(sql)
            .bind(("app", app_id.to_string()))
            .bind(("channel", channel.to_string()))
            .bind(("stream", stream_id.to_string()))
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to seek SurrealDB retention prefix: {e}"))
            })?;
        response.take(0usize).map_err(|e| {
            Error::Internal(format!("Failed to decode SurrealDB retention prefix: {e}"))
        })
    }

    /// Retained rows with `from_serial <= serial < from_serial + width`, sorted
    /// ascending in memory. The serial range keeps the database on an index
    /// range scan instead of sorting every payload-bearing document in the
    /// channel, which is what `retention_prefix` costs.
    pub(super) async fn retention_window(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        from_serial: i64,
        width: i64,
    ) -> Result<Vec<EntryKeyRecord>> {
        let sql = format!(
            "SELECT serial,published_at_ms,payload_size_bytes FROM {} WHERE app_id=$app AND channel=$channel AND stream_id=$stream AND serial >= $from AND serial < $to",
            self.tables.entries
        );
        let mut response = self
            .db
            .query(sql)
            .bind(("app", app_id.to_string()))
            .bind(("channel", channel.to_string()))
            .bind(("stream", stream_id.to_string()))
            .bind(("from", from_serial))
            .bind(("to", from_serial.saturating_add(width.max(1))))
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to read SurrealDB retention window: {e}"))
            })?;
        let mut rows: Vec<EntryKeyRecord> = response.take(0usize).map_err(|e| {
            Error::Internal(format!("Failed to decode SurrealDB retention window: {e}"))
        })?;
        rows.sort_by_key(|row| row.serial);
        Ok(rows)
    }

    async fn commit_retention(
        &self,
        current: &StoredStreamRecord,
        next: &StoredStreamRecord,
        insert: Option<&StoredEntryRecord>,
        delete: &[EntryKeyRecord],
    ) -> Result<bool> {
        let mut sql = "BEGIN TRANSACTION; LET $changed = UPDATE type::record($stream_table,$stream_key) SET retention_revision=$next_revision, retained_messages=$count, retained_bytes=$bytes, next_serial=$next, oldest_available_serial=$oldest, newest_available_serial=$newest, oldest_available_published_at_ms=$oldest_time, newest_available_published_at_ms=$newest_time, updated_at_ms=$now WHERE stream_id=$stream AND next_serial=$expected_next AND (retention_revision=$revision OR (retention_revision IS NONE AND $revision=0)) RETURN AFTER; IF array::len($changed) = 0 { THROW 'history_conflict'; };".to_string();
        if insert.is_some() {
            sql.push_str(
                " LET $created=CREATE ONLY type::record($entry_table,$entry_id) CONTENT $entry;",
            );
        }
        for index in 0..delete.len() {
            // Return only the serial: the vanish check needs a row, not the payload.
            sql.push_str(&format!(" LET $removed_{index}=DELETE type::record($entry_table,$delete_ids[{index}]) RETURN serial; IF array::len($removed_{index}) = 0 {{ THROW 'history_conflict'; }};"));
        }
        sql.push_str(" COMMIT TRANSACTION;");
        let mut query = self
            .db
            .query(sql)
            .bind(("stream_table", self.tables.streams.clone()))
            .bind((
                "stream_key",
                self.stream_resource(&current.app_id, &current.channel).1,
            ))
            .bind(("entry_table", self.tables.entries.clone()))
            .bind(("stream", current.stream_id.clone()))
            .bind(("revision", current.retention_revision.unwrap_or(0)))
            .bind(("next_revision", current.retention_revision.unwrap_or(0) + 1))
            .bind(("expected_next", current.next_serial))
            .bind(("count", next.retained_messages))
            .bind(("bytes", next.retained_bytes))
            .bind(("next", next.next_serial))
            .bind(("oldest", next.oldest_available_serial))
            .bind(("newest", next.newest_available_serial))
            .bind(("oldest_time", next.oldest_available_published_at_ms))
            .bind(("newest_time", next.newest_available_published_at_ms))
            .bind(("now", next.updated_at_ms));
        if let Some(insert) = insert {
            query = query
                .bind((
                    "entry_id",
                    self.entry_resource(
                        &current.app_id,
                        &current.channel,
                        &current.stream_id,
                        insert.serial as u64,
                    )
                    .1,
                ))
                .bind(("entry", insert.clone()));
        }
        if !delete.is_empty() {
            query = query.bind((
                "delete_ids",
                delete
                    .iter()
                    .map(|row| {
                        self.entry_resource(
                            &current.app_id,
                            &current.channel,
                            &current.stream_id,
                            row.serial as u64,
                        )
                        .1
                    })
                    .collect::<Vec<_>>(),
            ));
        }
        // A failed transaction marks earlier statements as cancelled. Inspect
        // every statement so a cancellation cannot hide the actual CAS conflict.
        let result = match query.await {
            Ok(mut response) => {
                let mut errors: Vec<_> = response.take_errors().into_iter().collect();
                errors.sort_by_key(|(index, _)| *index);
                let actual = errors.iter().position(|(_, error)| {
                    !error
                        .to_string()
                        .contains("query was not executed due to a failed transaction")
                });
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errors.swap_remove(actual.unwrap_or(0)).1)
                }
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(()) => Ok(true),
            Err(error)
                if error.to_string().contains("history_conflict")
                    || error.to_string().contains("already exists")
                    || error.to_string().contains("already been created")
                    || error
                        .to_string()
                        .to_ascii_lowercase()
                        .contains("read or write conflict")
                    || error
                        .to_string()
                        .to_ascii_lowercase()
                        .contains("transaction conflict")
                    || error
                        .to_string()
                        .to_ascii_lowercase()
                        .contains("write conflict") =>
            {
                Ok(false)
            }
            Err(error) => Err(Error::Internal(format!(
                "Failed to commit SurrealDB history retention: {error}"
            ))),
        }
    }

    pub(super) async fn refresh_retention_bounds(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
    ) -> Result<HistoryRetentionStats> {
        for attempt in 0..64 {
            let current = self
                .load_stream_raw(app_id, channel)
                .await?
                .ok_or_else(|| Error::Internal("Missing SurrealDB history stream".into()))?;
            if current.stream_id != stream_id {
                return Err(Error::InvalidMessageFormat(
                    "History stream changed during retention".into(),
                ));
            }
            let oldest = self
                .retention_prefix(app_id, channel, stream_id, false)
                .await?;
            let newest = self
                .retention_prefix(app_id, channel, stream_id, true)
                .await?;
            let mut next = self.accounted_state(&current).await?;
            next.oldest_available_serial = oldest.first().map(|row| row.serial);
            next.newest_available_serial = newest.first().map(|row| row.serial);
            next.oldest_available_published_at_ms = oldest.first().map(|row| row.published_at_ms);
            next.newest_available_published_at_ms = newest.first().map(|row| row.published_at_ms);
            if current.retention_revision.is_some()
                && current.retained_messages == next.retained_messages
                && current.retained_bytes == next.retained_bytes
                && current.oldest_available_serial == next.oldest_available_serial
                && current.newest_available_serial == next.newest_available_serial
                && current.oldest_available_published_at_ms == next.oldest_available_published_at_ms
                && current.newest_available_published_at_ms == next.newest_available_published_at_ms
            {
                return Ok(super::state::retained_from_stream_record(&current));
            }
            if self.commit_retention(&current, &next, None, &[]).await? {
                return Ok(super::state::retained_from_stream_record(&next));
            }
            tokio::time::sleep(retention_conflict_delay(attempt)).await;
        }
        Err(Error::Internal(
            "SurrealDB history bounds contention limit reached".into(),
        ))
    }

    pub(super) async fn delete_entries(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        rows: &[EntryKeyRecord],
    ) -> Result<u64> {
        let mut deleted = 0u64;
        for chunk in rows.chunks(64) {
            let mut completed = false;
            for attempt in 0..64 {
                let current = self
                    .load_stream_raw(app_id, channel)
                    .await?
                    .ok_or_else(|| Error::Internal("Missing SurrealDB history stream".into()))?;
                if current.stream_id != stream_id {
                    return Err(Error::InvalidMessageFormat(
                        "History stream changed during purge".into(),
                    ));
                }
                let sql = format!(
                    "SELECT serial,published_at_ms,payload_size_bytes FROM {} WHERE app_id=$app AND channel=$channel AND stream_id=$stream AND serial IN $serials",
                    self.tables.entries
                );
                let mut response = self
                    .db
                    .query(sql)
                    .bind(("app", app_id.to_string()))
                    .bind(("channel", channel.to_string()))
                    .bind(("stream", stream_id.to_string()))
                    .bind((
                        "serials",
                        chunk.iter().map(|row| row.serial).collect::<Vec<_>>(),
                    ))
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("Failed to read SurrealDB purge batch: {e}"))
                    })?;
                let present: Vec<EntryKeyRecord> = response.take(0usize).map_err(|e| {
                    Error::Internal(format!("Failed to decode SurrealDB purge batch: {e}"))
                })?;
                let mut next = self.accounted_state(&current).await?;
                next.retained_messages =
                    next.retained_messages.saturating_sub(present.len() as i64);
                next.retained_bytes = next.retained_bytes.saturating_sub(
                    present
                        .iter()
                        .map(|row| row.payload_size_bytes)
                        .sum::<i64>(),
                );
                if present.is_empty()
                    || self
                        .commit_retention(&current, &next, None, &present)
                        .await?
                {
                    deleted += present.len() as u64;
                    completed = true;
                    break;
                }
                tokio::time::sleep(retention_conflict_delay(attempt)).await;
            }
            if !completed {
                return Err(Error::Internal(
                    "SurrealDB purge contention limit reached".into(),
                ));
            }
        }
        self.refresh_retention_bounds(app_id, channel, stream_id)
            .await?;
        Ok(deleted)
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
        // The stream record we committed, when known, saves a reload for the
        // retention pass. A legacy stream (no retention revision yet) needs one
        // full bounds refresh after this append; afterwards bounds are kept
        // current by every commit.
        let mut committed: Option<StoredStreamRecord> = None;
        let mut legacy = false;
        let mut inserted = false;
        for attempt in 0..64 {
            let current = self
                .load_stream_raw(&record.app_id, &record.channel)
                .await?
                .ok_or_else(|| Error::Internal("Missing SurrealDB history stream".into()))?;
            if current.stream_id != record.stream_id {
                return Err(Error::InvalidMessageFormat(
                    "History append stream changed".into(),
                ));
            }
            legacy = current.retention_revision.is_none();
            let mut next = self.accounted_state(&current).await?;
            next.retained_messages += 1;
            next.retained_bytes += stored.payload_size_bytes;
            next.next_serial = next.next_serial.max(record.serial.saturating_add(1) as i64);
            next.updated_at_ms = record.published_at_ms;
            next.retention_revision = Some(current.retention_revision.unwrap_or(0) + 1);
            // The appended row itself moves the bounds; no read is required.
            if next
                .newest_available_serial
                .is_none_or(|serial| stored.serial > serial)
            {
                next.newest_available_serial = Some(stored.serial);
                next.newest_available_published_at_ms = Some(stored.published_at_ms);
            }
            if next
                .oldest_available_serial
                .is_none_or(|serial| stored.serial < serial)
            {
                next.oldest_available_serial = Some(stored.serial);
                next.oldest_available_published_at_ms = Some(stored.published_at_ms);
            }
            if self
                .commit_retention(&current, &next, Some(&stored), &[])
                .await?
            {
                inserted = true;
                committed = Some(next);
                break;
            }
            let existing: Option<StoredEntryRecord> = self
                .db
                .select(self.entry_resource(
                    &record.app_id,
                    &record.channel,
                    &record.stream_id,
                    record.serial,
                ))
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to inspect duplicate SurrealDB append: {e}"))
                })?;
            if let Some(existing) = existing {
                if existing.payload_bytes != stored.payload_bytes
                    || existing.message_id != stored.message_id
                    || existing.published_at_ms != stored.published_at_ms
                    || existing.event_name != stored.event_name
                    || existing.operation_kind != stored.operation_kind
                {
                    return Err(Error::InvalidMessageFormat(
                        "Conflicting SurrealDB history append at existing serial".into(),
                    ));
                }
                inserted = true;
                break;
            }
            tokio::time::sleep(retention_conflict_delay(attempt)).await;
        }
        if !inserted {
            return Err(Error::Internal(
                "SurrealDB history append contention limit reached".into(),
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
        let mut evicted_messages = 0u64;
        let mut evicted_bytes = 0u64;
        let mut conflicts = 0;
        // Set when a commit could not name the surviving oldest row; the bounds
        // are then recomputed once after the loop instead of being guessed.
        let mut bounds_stale = legacy;
        loop {
            let current = match committed.take() {
                Some(current) => current,
                None => self
                    .load_stream_raw(&record.app_id, &record.channel)
                    .await?
                    .ok_or_else(|| Error::Internal("Missing SurrealDB history stream".into()))?,
            };
            if current.stream_id != record.stream_id {
                return Err(Error::InvalidMessageFormat(
                    "History stream changed during retention".into(),
                ));
            }
            let over_messages = record
                .retention
                .max_messages_per_channel
                .is_some_and(|limit| current.retained_messages > limit as i64);
            let over_bytes = record
                .retention
                .max_bytes_per_channel
                .is_some_and(|limit| current.retained_bytes > limit as i64);
            let bounds_known = current.retained_messages <= 0
                || (current.oldest_available_serial.is_some()
                    && current.oldest_available_published_at_ms.is_some());
            let aged = current
                .oldest_available_published_at_ms
                .is_some_and(|oldest| oldest < cutoff);
            if !legacy && bounds_known && !over_messages && !over_bytes && !aged {
                // Nothing can be evicted; skip every retention read.
                committed = Some(current);
                break;
            }
            let from = current.oldest_available_serial.unwrap_or(0);
            let mut rows = self
                .retention_window(&record.app_id, &record.channel, &record.stream_id, from, 64)
                .await?;
            if rows.is_empty() && current.retained_messages > 0 {
                // Stale or missing oldest bound: fall back to the sorted prefix.
                rows = self
                    .retention_prefix(&record.app_id, &record.channel, &record.stream_id, false)
                    .await?;
                bounds_stale = true;
            }
            let mut next = self.accounted_state(&current).await?;
            let mut remove = 0;
            let mut next_age_prefix_complete = age_prefix_complete;
            for row in &rows {
                if !next_age_prefix_complete && row.published_at_ms >= cutoff {
                    next_age_prefix_complete = true;
                }
                if next_age_prefix_complete
                    && !record
                        .retention
                        .max_messages_per_channel
                        .is_some_and(|limit| next.retained_messages > limit as i64)
                    && !record
                        .retention
                        .max_bytes_per_channel
                        .is_some_and(|limit| next.retained_bytes > limit as i64)
                {
                    break;
                }
                next.retained_messages = next.retained_messages.saturating_sub(1);
                next.retained_bytes = next.retained_bytes.saturating_sub(row.payload_size_bytes);
                remove += 1;
            }
            if remove == 0 {
                committed = Some(current);
                break;
            }
            let survivor = rows.get(remove);
            match survivor {
                Some(survivor) => {
                    next.oldest_available_serial = Some(survivor.serial);
                    next.oldest_available_published_at_ms = Some(survivor.published_at_ms);
                }
                None if next.retained_messages <= 0 => {
                    next.oldest_available_serial = None;
                    next.oldest_available_published_at_ms = None;
                    next.newest_available_serial = None;
                    next.newest_available_published_at_ms = None;
                }
                None => bounds_stale = true,
            }
            next.retention_revision = Some(current.retention_revision.unwrap_or(0) + 1);
            if self
                .commit_retention(&current, &next, None, &rows[..remove])
                .await?
            {
                age_prefix_complete = next_age_prefix_complete;
                evicted_messages += remove as u64;
                evicted_bytes += rows[..remove]
                    .iter()
                    .map(|row| row.payload_size_bytes.max(0) as u64)
                    .sum::<u64>();
                conflicts = 0;
                committed = Some(next);
                if survivor.is_some() {
                    // The survivor satisfied every retention predicate, so no
                    // older row remains to evict.
                    break;
                }
            } else {
                conflicts += 1;
                if conflicts >= 64 {
                    return Err(Error::Internal(
                        "SurrealDB retention contention limit reached".into(),
                    ));
                }
                tokio::time::sleep(retention_conflict_delay(conflicts)).await;
            }
        }
        let retained = match committed {
            Some(state) if !bounds_stale => super::state::retained_from_stream_record(&state),
            _ => {
                self.refresh_retention_bounds(&record.app_id, &record.channel, &record.stream_id)
                    .await?
            }
        };
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.update_history_retained(
                &record.app_id,
                retained.retained_messages,
                retained.retained_bytes,
            );
            if evicted_messages > 0 {
                metrics.mark_history_eviction(&record.app_id, evicted_messages, evicted_bytes);
            }
        }
        Ok(())
    }
}

fn retention_conflict_delay(attempt: u32) -> std::time::Duration {
    let ceiling = (1u64 << attempt.min(4)).max(1);
    std::time::Duration::from_millis(1 + u64::from(uuid::Uuid::new_v4().as_bytes()[0]) % ceiling)
}
