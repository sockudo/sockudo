use super::state::*;
use super::*;

#[async_trait::async_trait]
impl HistoryStore for SurrealHistoryStore {
    async fn reserve_publish_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryWriteReservation> {
        loop {
            if let Some(existing) = self.load_stream_raw(app_id, channel).await? {
                let now_ms = sockudo_core::history::now_ms();
                let mut response = self
                    .db
                    .query(
                        "UPDATE ONLY type::record($table, $id) SET next_serial = $next_serial, updated_at_ms = $updated_at_ms WHERE next_serial = $expected RETURN AFTER"
                            .to_string(),
                    )
                    .bind(("table", self.tables.streams.clone()))
                    .bind(("id", deterministic_key([app_id, channel].into_iter())))
                    .bind(("next_serial", existing.next_serial + 1))
                    .bind(("updated_at_ms", now_ms))
                    .bind(("expected", existing.next_serial))
                    .await
                    .map_err(|e| Error::Internal(format!("Failed to advance SurrealDB history serial: {e}")))?;
                let updated: Option<StoredStreamRecord> = response.take(0usize).map_err(|e| {
                    Error::Internal(format!(
                        "Failed to decode SurrealDB serial advancement: {e}"
                    ))
                })?;
                if updated.is_some() {
                    return Ok(HistoryWriteReservation {
                        stream_id: existing.stream_id,
                        serial: existing.next_serial as u64,
                    });
                }
                continue;
            }

            let now_ms = sockudo_core::history::now_ms();
            let stream = StoredStreamRecord {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                stream_id: uuid::Uuid::new_v4().to_string(),
                next_serial: 2,
                retained_messages: 0,
                retained_bytes: 0,
                oldest_available_serial: None,
                newest_available_serial: None,
                oldest_available_published_at_ms: None,
                newest_available_published_at_ms: None,
                durable_state: HistoryDurableState::Healthy.as_str().to_string(),
                durable_state_reason: None,
                durable_state_node_id: None,
                durable_state_changed_at_ms: None,
                updated_at_ms: now_ms,
            };
            let create_result: Result<Option<StoredStreamRecord>> = self
                .db
                .create(self.stream_resource(app_id, channel))
                .content(stream.clone())
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to create SurrealDB history stream row: {e}"
                    ))
                });
            match create_result {
                Ok(Some(_)) | Ok(None) => {
                    return Ok(HistoryWriteReservation {
                        stream_id: stream.stream_id,
                        serial: 1,
                    });
                }
                Err(err) => {
                    let err_text = err.to_string();
                    if err_text.contains("already exists")
                        || err_text.contains("already been created")
                        || err_text.contains("Database record")
                    {
                        continue;
                    }
                    return Err(err);
                }
            }
        }
    }

    async fn append(&self, record: HistoryAppendRecord) -> Result<()> {
        let started = Instant::now();
        if let Err(err) = self.persist_record(&record).await {
            mark_channel_degraded(
                &self.db,
                &self.tables,
                &self.degraded_channels,
                self.cache_manager.as_ref(),
                self.metrics.as_deref(),
                DegradeRequest {
                    app_id: &record.app_id,
                    channel: &record.channel,
                    reason: "durable_history_write_failed",
                    node_id: None,
                },
            )
            .await;
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.mark_history_write_failure(&record.app_id);
            }
            return Err(err);
        }
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.mark_history_write(&record.app_id);
            metrics.track_history_write_latency(
                &record.app_id,
                started.elapsed().as_secs_f64() * 1000.0,
            );
        }
        Ok(())
    }

    async fn read_page(&self, request: HistoryReadRequest) -> Result<HistoryPage> {
        request.validate()?;
        let stream_runtime = self
            .resolved_stream_runtime_state(&request.app_id, &request.channel)
            .await?;
        if !stream_runtime.recovery_allowed {
            return Err(Error::Internal(format!(
                "History stream state blocks cold reads for {}/{}: {}",
                request.app_id,
                request.channel,
                stream_runtime
                    .reason
                    .unwrap_or_else(|| stream_runtime.durable_state.as_str().to_string())
            )));
        }

        let retained = self
            .retained_stats(&request.app_id, &request.channel)
            .await?;
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

        let Some(stream_id) = retained.stream_id.as_deref() else {
            return Ok(HistoryPage {
                items: Vec::new(),
                next_cursor: None,
                retained,
                has_more: false,
                complete: true,
                truncated_by_retention: false,
            });
        };

        let rows = self
            .load_page_entries_for_stream(&request, stream_id)
            .await?;
        let filtered: Vec<HistoryItem> = rows
            .into_iter()
            .map(|row| HistoryItem {
                stream_id: row.stream_id,
                serial: row.serial as u64,
                published_at_ms: row.published_at_ms,
                message_id: row.message_id,
                event_name: row.event_name,
                operation_kind: row.operation_kind,
                payload_size_bytes: row.payload_size_bytes as usize,
                payload_bytes: row.payload_bytes.into(),
            })
            .collect();
        let has_more = filtered.len() > request.limit;
        let items: Vec<HistoryItem> = filtered.into_iter().take(request.limit).collect();
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
        let mut response = self
            .db
            .query(format!("SELECT durable_state FROM {}", self.tables.streams))
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to query SurrealDB history runtime status: {e}"
                ))
            })?;
        let rows: Vec<DurableStateRow> = response.take(0usize).map_err(|e| {
            Error::Internal(format!(
                "Failed to decode SurrealDB history runtime status: {e}"
            ))
        })?;
        let mut degraded_channels = 0usize;
        let mut reset_required_channels = 0usize;
        for row in rows {
            let state = parse_history_durable_state(&row.durable_state);
            if state != HistoryDurableState::Healthy {
                degraded_channels += 1;
            }
            if state == HistoryDurableState::ResetRequired {
                reset_required_channels += 1;
            }
        }
        Ok(HistoryRuntimeStatus {
            enabled: true,
            backend: "surrealdb".to_string(),
            state_authority: "durable_store".to_string(),
            degraded_channels,
            reset_required_channels,
            queue_depth: self.queue_depth_total.load(Ordering::Relaxed),
        })
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamRuntimeState> {
        self.resolved_stream_runtime_state(app_id, channel).await
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamInspection> {
        self.resolved_stream_inspection(app_id, channel).await
    }

    async fn reset_stream(
        &self,
        app_id: &str,
        channel: &str,
        reason: &str,
        requested_by: Option<&str>,
    ) -> Result<HistoryResetResult> {
        if reason.trim().is_empty() {
            return Err(Error::InvalidMessageFormat(
                "Reset reason must not be empty".to_string(),
            ));
        }
        let inspection_before = self.resolved_stream_inspection(app_id, channel).await?;
        let previous_stream_id = inspection_before.stream_id.clone();
        let mut purged_messages = 0u64;
        let mut purged_bytes = 0u64;
        if let Some(stream_id) = previous_stream_id.as_deref() {
            let entries = self
                .load_entry_keys_for_stream(app_id, channel, stream_id)
                .await?;
            purged_messages = entries.len() as u64;
            purged_bytes = entries
                .iter()
                .map(|row| row.payload_size_bytes.max(0) as u64)
                .sum();
            self.delete_entries(app_id, channel, stream_id, &entries)
                .await?;
        }

        let now_ms = sockudo_core::history::now_ms();
        let new_stream = StoredStreamRecord {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: uuid::Uuid::new_v4().to_string(),
            next_serial: 1,
            retained_messages: 0,
            retained_bytes: 0,
            oldest_available_serial: None,
            newest_available_serial: None,
            oldest_available_published_at_ms: None,
            newest_available_published_at_ms: None,
            durable_state: HistoryDurableState::Healthy.as_str().to_string(),
            durable_state_reason: None,
            durable_state_node_id: None,
            durable_state_changed_at_ms: Some(now_ms),
            updated_at_ms: now_ms,
        };
        self.upsert_stream_raw(app_id, channel, &new_stream).await?;
        self.degraded_channels
            .remove(&degraded_channel_key(app_id, channel));
        if let Some(cache) = self.cache_manager.as_ref() {
            let _ = cache.remove(&degraded_cache_key(app_id, channel)).await;
        }
        if let Some(metrics) = self.metrics.as_deref() {
            let _ = refresh_history_state_metrics(&self.db, &self.tables, metrics, app_id).await;
        }
        info!(
            app_id = %app_id,
            channel = %channel,
            previous_stream_id = previous_stream_id.as_deref().unwrap_or(""),
            new_stream_id = %new_stream.stream_id,
            purged_messages,
            purged_bytes,
            reason = %reason,
            requested_by = requested_by.unwrap_or(""),
            "operator reset durable history stream"
        );
        let inspection = self.resolved_stream_inspection(app_id, channel).await?;
        Ok(HistoryResetResult {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            previous_stream_id,
            new_stream_id: new_stream.stream_id,
            purged_messages,
            purged_bytes,
            inspection,
        })
    }

    async fn purge_stream(
        &self,
        app_id: &str,
        channel: &str,
        request: HistoryPurgeRequest,
    ) -> Result<HistoryPurgeResult> {
        request.validate()?;
        let inspection_before = self.resolved_stream_inspection(app_id, channel).await?;
        let mut purged_messages = 0u64;
        let mut purged_bytes = 0u64;
        if let Some(stream_id) = inspection_before.stream_id.as_deref() {
            let entries = self
                .load_entry_keys_for_stream(app_id, channel, stream_id)
                .await?;
            let to_delete: Vec<EntryKeyRecord> = entries
                .iter()
                .filter(|row| match request.mode {
                    HistoryPurgeMode::All => true,
                    HistoryPurgeMode::BeforeSerial => {
                        (row.serial as u64) < request.before_serial.unwrap_or_default()
                    }
                    HistoryPurgeMode::BeforeTimeMs => {
                        row.published_at_ms < request.before_time_ms.unwrap_or_default()
                    }
                })
                .cloned()
                .collect();
            let retained_rows: Vec<EntryKeyRecord> = entries
                .into_iter()
                .filter(|row| match request.mode {
                    HistoryPurgeMode::All => false,
                    HistoryPurgeMode::BeforeSerial => {
                        (row.serial as u64) >= request.before_serial.unwrap_or_default()
                    }
                    HistoryPurgeMode::BeforeTimeMs => {
                        row.published_at_ms >= request.before_time_ms.unwrap_or_default()
                    }
                })
                .collect();
            purged_messages = to_delete.len() as u64;
            purged_bytes = to_delete
                .iter()
                .map(|row| row.payload_size_bytes.max(0) as u64)
                .sum();
            self.delete_entries(app_id, channel, stream_id, &to_delete)
                .await?;
            if let Some(current) = self.load_stream_raw(app_id, channel).await? {
                let retained = HistoryRetentionStats {
                    stream_id: Some(stream_id.to_string()),
                    retained_messages: retained_rows.len() as u64,
                    retained_bytes: retained_rows
                        .iter()
                        .map(|row| row.payload_size_bytes.max(0) as u64)
                        .sum(),
                    oldest_serial: retained_rows.first().map(|row| row.serial as u64),
                    newest_serial: retained_rows.last().map(|row| row.serial as u64),
                    oldest_published_at_ms: retained_rows.first().map(|row| row.published_at_ms),
                    newest_published_at_ms: retained_rows.last().map(|row| row.published_at_ms),
                };
                let updated_stream = StoredStreamRecord {
                    retained_messages: retained.retained_messages as i64,
                    retained_bytes: retained.retained_bytes as i64,
                    oldest_available_serial: retained.oldest_serial.map(|value| value as i64),
                    newest_available_serial: retained.newest_serial.map(|value| value as i64),
                    oldest_available_published_at_ms: retained.oldest_published_at_ms,
                    newest_available_published_at_ms: retained.newest_published_at_ms,
                    updated_at_ms: sockudo_core::history::now_ms(),
                    ..current
                };
                self.upsert_stream_raw(app_id, channel, &updated_stream)
                    .await?;
            }
            if let Some(metrics) = self.metrics.as_ref() {
                let retained = self.retained_stats(app_id, channel).await?;
                metrics.update_history_retained(
                    app_id,
                    retained.retained_messages,
                    retained.retained_bytes,
                );
            }
        }

        if let Some(metrics) = self.metrics.as_deref() {
            let _ = refresh_history_state_metrics(&self.db, &self.tables, metrics, app_id).await;
        }
        info!(
            app_id = %app_id,
            channel = %channel,
            mode = %request.mode.as_str(),
            before_serial = request.before_serial,
            before_time_ms = request.before_time_ms,
            purged_messages,
            purged_bytes,
            reason = %request.reason,
            requested_by = request.requested_by.as_deref().unwrap_or(""),
            "operator purged durable history rows"
        );
        let inspection = self.resolved_stream_inspection(app_id, channel).await?;
        Ok(HistoryPurgeResult {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            mode: request.mode,
            before_serial: request.before_serial,
            before_time_ms: request.before_time_ms,
            purged_messages,
            purged_bytes,
            inspection,
        })
    }

    async fn purge_before(&self, before_ms: i64, batch_size: usize) -> Result<(u64, bool)> {
        if batch_size == 0 {
            return Ok((0, false));
        }
        let limit = batch_size as i64;
        let table = self.tables.entries.clone();
        let db = self.db.clone();

        // SurrealDB has no LIMIT clause on DELETE, so select ids first and
        // then delete them — two round-trips but each is bounded by `limit`.
        let select_sql =
            format!("SELECT VALUE id FROM {table} WHERE published_at_ms < $cutoff LIMIT $limit");
        let mut response = db
            .query(select_sql)
            .bind(("cutoff", before_ms))
            .bind(("limit", limit))
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to select expired history entries in SurrealDB: {e}"
                ))
            })?;
        let ids: Vec<surrealdb::types::RecordId> = response.take(0usize).map_err(|e| {
            Error::Internal(format!(
                "Failed to decode expired history entry ids in SurrealDB: {e}"
            ))
        })?;
        let deleted = ids.len() as u64;
        if ids.is_empty() {
            return Ok((0, false));
        }
        db.query("DELETE $ids")
            .bind(("ids", ids))
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to delete expired history entries in SurrealDB: {e}"
                ))
            })?;
        Ok((deleted, deleted as i64 == limit))
    }
}
