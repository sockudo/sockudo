use super::state::*;
use super::*;

#[async_trait::async_trait]
impl HistoryStore for ScyllaHistoryStore {
    async fn reserve_publish_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryWriteReservation> {
        let select_query = format!(
            "SELECT stream_id, next_serial, durable_state, durable_state_reason, durable_state_node_id, durable_state_changed_at_ms, retained_messages, retained_bytes, oldest_available_serial, newest_available_serial, oldest_available_published_at_ms, newest_available_published_at_ms FROM {} WHERE app_id = ? AND channel = ?",
            self.tables.streams_fq()
        );
        let insert_query = format!(
            "INSERT INTO {} (app_id, channel, stream_id, next_serial, durable_state, durable_state_reason, durable_state_node_id, durable_state_changed_at_ms, retained_messages, retained_bytes, oldest_available_serial, newest_available_serial, oldest_available_published_at_ms, newest_available_published_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, 'healthy', null, null, null, 0, 0, null, null, null, null, ?) IF NOT EXISTS",
            self.tables.streams_fq()
        );
        let update_query = format!(
            "UPDATE {} SET next_serial = ?, updated_at_ms = ? WHERE app_id = ? AND channel = ? IF stream_id = ? AND next_serial = ?",
            self.tables.streams_fq()
        );

        loop {
            let rows = self
                .session
                .query_unpaged(select_query.as_str(), (app_id, channel))
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to read ScyllaDB history stream during reservation: {e}"
                    ))
                })?
                .into_rows_result()
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to decode ScyllaDB history stream during reservation: {e}"
                    ))
                })?;
            if let Some(row) = rows.maybe_first_row::<StreamRow>().map_err(|e| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB history stream during reservation: {e}"
                ))
            })? {
                let stream_id = row.stream_id;
                let serial = row.next_serial as u64;
                let now_ms = sockudo_core::history::now_ms();
                let mut stmt = Statement::new(update_query.clone());
                stmt.set_serial_consistency(Some(SerialConsistency::LocalSerial));
                let result = self
                    .session
                    .query_unpaged(
                        stmt,
                        (
                            (serial + 1) as i64,
                            now_ms,
                            app_id,
                            channel,
                            stream_id.as_str(),
                            serial as i64,
                        ),
                    )
                    .await
                    .map_err(|e| map_scylla_lwt_error("advance history serial", e))?;
                if lwt_applied(result)? {
                    return Ok(HistoryWriteReservation { stream_id, serial });
                }
                continue;
            }

            let stream_id = uuid::Uuid::new_v4().to_string();
            let now_ms = sockudo_core::history::now_ms();
            let mut stmt = Statement::new(insert_query.clone());
            stmt.set_serial_consistency(Some(SerialConsistency::LocalSerial));
            let result = self
                .session
                .query_unpaged(stmt, (app_id, channel, stream_id.as_str(), 2_i64, now_ms))
                .await
                .map_err(|e| map_scylla_lwt_error("create history stream row", e))?;
            if lwt_applied(result)? {
                return Ok(HistoryWriteReservation {
                    stream_id,
                    serial: 1,
                });
            }
        }
    }

    async fn append(&self, record: HistoryAppendRecord) -> Result<()> {
        let started = Instant::now();
        if let Err(err) = self.persist_record(&record).await {
            mark_channel_degraded(
                &self.session,
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
            .load_history_items_for_stream(
                &request.app_id,
                &request.channel,
                stream_id,
                request.direction,
            )
            .await?;
        let filtered: Vec<HistoryItem> = rows
            .into_iter()
            .filter(|row| {
                request
                    .bounds
                    .start_serial
                    .is_none_or(|start| row.serial as u64 >= start)
                    && request
                        .bounds
                        .end_serial
                        .is_none_or(|end| row.serial as u64 <= end)
                    && request
                        .bounds
                        .start_time_ms
                        .is_none_or(|start| row.published_at_ms >= start)
                    && request
                        .bounds
                        .end_time_ms
                        .is_none_or(|end| row.published_at_ms <= end)
                    && request
                        .cursor
                        .as_ref()
                        .is_none_or(|cursor| match request.direction {
                            HistoryDirection::NewestFirst => (row.serial as u64) < cursor.serial,
                            HistoryDirection::OldestFirst => (row.serial as u64) > cursor.serial,
                        })
            })
            .take(request.limit + 1)
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
        let query = format!("SELECT durable_state FROM {}", self.tables.streams_fq());
        let pager = self.session.query_iter(query, ()).await.map_err(|e| {
            Error::Internal(format!("Failed to stream ScyllaDB runtime status: {e}"))
        })?;
        let mut rows_stream = pager.rows_stream::<DurableStateRow>().map_err(|e| {
            Error::Internal(format!("Failed to decode ScyllaDB runtime status: {e}"))
        })?;
        let mut degraded = 0usize;
        let mut reset_required = 0usize;
        while let Some(row) = rows_stream
            .try_next()
            .await
            .map_err(|e| Error::Internal(format!("Failed to read ScyllaDB runtime status: {e}")))?
        {
            let state = parse_history_durable_state(&row.durable_state);
            if state != HistoryDurableState::Healthy {
                degraded += 1;
            }
            if state == HistoryDurableState::ResetRequired {
                reset_required += 1;
            }
        }
        Ok(HistoryRuntimeStatus {
            enabled: true,
            backend: "scylladb".to_string(),
            state_authority: "durable_store".to_string(),
            degraded_channels: degraded,
            reset_required_channels: reset_required,
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

        let new_stream_id = uuid::Uuid::new_v4().to_string();
        let now_ms = sockudo_core::history::now_ms();
        let retained = HistoryRetentionStats::default();
        self.write_stream_record(StreamWriteParams {
            app_id,
            channel,
            stream_id: &new_stream_id,
            next_serial: 1,
            durable_state: HistoryDurableState::Healthy,
            durable_state_reason: None,
            durable_state_node_id: None,
            durable_state_changed_at_ms: Some(now_ms),
            retained: &retained,
            updated_at_ms: now_ms,
        })
        .await?;
        self.degraded_channels
            .remove(&degraded_channel_key(app_id, channel));
        if let Some(cache) = self.cache_manager.as_ref() {
            let _ = cache.remove(&degraded_cache_key(app_id, channel)).await;
        }
        if let Some(metrics) = self.metrics.as_deref() {
            let _ =
                refresh_history_state_metrics(&self.session, &self.tables, metrics, app_id).await;
        }
        info!(
            app_id = %app_id,
            channel = %channel,
            previous_stream_id = previous_stream_id.as_deref().unwrap_or(""),
            new_stream_id = %new_stream_id,
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
            new_stream_id,
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
            let to_delete: Vec<EntryKeyRow> = entries
                .into_iter()
                .filter(|row| match request.mode {
                    HistoryPurgeMode::All => true,
                    HistoryPurgeMode::BeforeSerial => {
                        (row.serial as u64) < request.before_serial.unwrap_or_default()
                    }
                    HistoryPurgeMode::BeforeTimeMs => {
                        row.published_at_ms < request.before_time_ms.unwrap_or_default()
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
            if let Some(stream) = self.load_stream_record(app_id, channel).await? {
                let retained = self
                    .update_stream_retention_from_entries(StreamRetentionUpdateParams {
                        app_id,
                        channel,
                        stream_id,
                        next_serial: stream.next_serial,
                        durable_state: stream.durable_state,
                        durable_state_reason: stream.durable_state_reason.as_deref(),
                        durable_state_node_id: stream.durable_state_node_id.as_deref(),
                        durable_state_changed_at_ms: stream.durable_state_changed_at_ms,
                        updated_at_ms: sockudo_core::history::now_ms(),
                    })
                    .await?;
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.update_history_retained(
                        app_id,
                        retained.retained_messages,
                        retained.retained_bytes,
                    );
                }
            }
        }

        if let Some(metrics) = self.metrics.as_deref() {
            let _ =
                refresh_history_state_metrics(&self.session, &self.tables, metrics, app_id).await;
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
}
