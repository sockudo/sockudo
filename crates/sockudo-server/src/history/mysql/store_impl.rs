use super::state::*;
use super::*;

#[async_trait::async_trait]
impl HistoryStore for MySqlHistoryStore {
    async fn reserve_publish_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryWriteReservation> {
        let mut tx = self.pool.begin().await.map_err(|e| {
            Error::Internal(format!("Failed to begin MySQL reserve transaction: {e}"))
        })?;
        let select_sql = format!(
            "SELECT stream_id, next_serial FROM {} WHERE app_id = ? AND channel = ? FOR UPDATE",
            self.tables.streams
        );
        if let Some(row) = sqlx::query(sqlx::AssertSqlSafe(select_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to reserve MySQL history position: {e}"))
            })?
        {
            let stream_id = row.get::<String, _>("stream_id");
            let serial = row.get::<i64, _>("next_serial") as u64;
            let update_sql = format!(
                "UPDATE {} SET next_serial = ?, updated_at_ms = ? WHERE app_id = ? AND channel = ?",
                self.tables.streams
            );
            let now_ms = sockudo_core::history::now_ms();
            sqlx::query(sqlx::AssertSqlSafe(update_sql.as_str()))
                .bind((serial + 1) as i64)
                .bind(now_ms)
                .bind(app_id)
                .bind(channel)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to advance MySQL history serial: {e}"))
                })?;
            tx.commit().await.map_err(|e| {
                Error::Internal(format!("Failed to commit MySQL reserve transaction: {e}"))
            })?;
            return Ok(HistoryWriteReservation { stream_id, serial });
        }

        let stream_id = uuid::Uuid::new_v4().to_string();
        let now_ms = sockudo_core::history::now_ms();
        let insert_sql = format!(
            "INSERT INTO {} (app_id, channel, stream_id, next_serial, updated_at_ms) VALUES (?, ?, ?, 2, ?)",
            self.tables.streams
        );
        sqlx::query(sqlx::AssertSqlSafe(insert_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .bind(&stream_id)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to create MySQL history stream row: {e}"))
            })?;
        tx.commit().await.map_err(|e| {
            Error::Internal(format!("Failed to commit MySQL reserve transaction: {e}"))
        })?;
        Ok(HistoryWriteReservation {
            stream_id,
            serial: 1,
        })
    }

    async fn append(&self, record: HistoryAppendRecord) -> Result<()> {
        let started = Instant::now();
        if let Err(err) =
            Self::persist_record(&self.pool, &self.tables, &record, self.metrics.clone()).await
        {
            mark_channel_degraded(
                &self.pool,
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

        let mut clauses: Vec<String> = Vec::new();
        let mut bind_stream = None;
        let mut bind_serial = None;
        let mut bind_start_serial = None;
        let mut bind_end_serial = None;
        let mut bind_start_time = None;
        let mut bind_end_time = None;

        if let Some(cursor) = request.cursor.as_ref() {
            bind_stream = Some(cursor.stream_id.clone());
            bind_serial = Some(cursor.serial as i64);
            clauses.push(match request.direction {
                HistoryDirection::NewestFirst => "stream_id = ? AND serial < ?".to_string(),
                HistoryDirection::OldestFirst => "stream_id = ? AND serial > ?".to_string(),
            });
        }
        if let Some(start_serial) = request.bounds.start_serial {
            bind_start_serial = Some(start_serial as i64);
            clauses.push("serial >= ?".to_string());
        }
        if let Some(end_serial) = request.bounds.end_serial {
            bind_end_serial = Some(end_serial as i64);
            clauses.push("serial <= ?".to_string());
        }
        if let Some(start_time_ms) = request.bounds.start_time_ms {
            bind_start_time = Some(start_time_ms);
            clauses.push("published_at_ms >= ?".to_string());
        }
        if let Some(end_time_ms) = request.bounds.end_time_ms {
            bind_end_time = Some(end_time_ms);
            clauses.push("published_at_ms <= ?".to_string());
        }

        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" AND {}", clauses.join(" AND "))
        };
        let order = match request.direction {
            HistoryDirection::NewestFirst => "DESC",
            HistoryDirection::OldestFirst => "ASC",
        };
        let sql = format!(
            "SELECT stream_id, serial, published_at_ms, message_id, event_name, operation_kind, payload_bytes, payload_size_bytes FROM {} WHERE app_id = ? AND channel = ?{} ORDER BY serial {} LIMIT {}",
            self.tables.entries,
            where_clause,
            order,
            request.limit + 1
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&request.app_id)
            .bind(&request.channel);
        if let Some(stream_id) = bind_stream {
            query = query.bind(stream_id);
        }
        if let Some(serial) = bind_serial {
            query = query.bind(serial);
        }
        if let Some(start_serial) = bind_start_serial {
            query = query.bind(start_serial);
        }
        if let Some(end_serial) = bind_end_serial {
            query = query.bind(end_serial);
        }
        if let Some(start_time_ms) = bind_start_time {
            query = query.bind(start_time_ms);
        }
        if let Some(end_time_ms) = bind_end_time {
            query = query.bind(end_time_ms);
        }
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to read MySQL history page: {e}")))?;

        let has_more = rows.len() > request.limit;
        let items: Vec<HistoryItem> = rows
            .into_iter()
            .take(request.limit)
            .map(|row| HistoryItem {
                stream_id: row.get::<String, _>("stream_id"),
                serial: row.get::<i64, _>("serial") as u64,
                published_at_ms: row.get::<i64, _>("published_at_ms"),
                message_id: row
                    .try_get::<Option<String>, _>("message_id")
                    .unwrap_or(None),
                event_name: row
                    .try_get::<Option<String>, _>("event_name")
                    .unwrap_or(None),
                operation_kind: row.get::<String, _>("operation_kind"),
                payload_size_bytes: row.get::<i64, _>("payload_size_bytes") as usize,
                payload_bytes: row.get::<Vec<u8>, _>("payload_bytes").into(),
            })
            .collect();
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
        let sql = format!(
            "SELECT COALESCE(SUM(CASE WHEN durable_state <> 'healthy' THEN 1 ELSE 0 END), 0) AS degraded_channels, COALESCE(SUM(CASE WHEN durable_state = 'reset_required' THEN 1 ELSE 0 END), 0) AS reset_required_channels FROM {}",
            self.tables.streams
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to read MySQL history runtime status: {e}"))
            })?;
        Ok(HistoryRuntimeStatus {
            enabled: true,
            backend: "mysql".to_string(),
            state_authority: "durable_store".to_string(),
            degraded_channels: row.get::<i64, _>("degraded_channels") as usize,
            reset_required_channels: row.get::<i64, _>("reset_required_channels") as usize,
            queue_depth: 0,
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
        let new_stream_id = uuid::Uuid::new_v4().to_string();
        let now_ms = sockudo_core::history::now_ms();
        let mut tx = self.pool.begin().await.map_err(|e| {
            Error::Internal(format!(
                "Failed to begin MySQL history reset transaction: {e}"
            ))
        })?;
        let stats_sql = format!(
            "SELECT COUNT(*) AS count, CAST(COALESCE(SUM(payload_size_bytes), 0) AS SIGNED) AS bytes FROM {} WHERE app_id = ? AND channel = ?",
            self.tables.entries
        );
        let stats = sqlx::query(sqlx::AssertSqlSafe(stats_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to inspect MySQL history rows during reset: {e}"
                ))
            })?;
        let purged_messages = stats.get::<i64, _>("count") as u64;
        let purged_bytes = stats.get::<i64, _>("bytes") as u64;

        let delete_sql = format!(
            "DELETE FROM {} WHERE app_id = ? AND channel = ?",
            self.tables.entries
        );
        sqlx::query(sqlx::AssertSqlSafe(delete_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to purge MySQL history rows during reset: {e}"
                ))
            })?;

        let upsert_sql = format!(
            "INSERT INTO {} (app_id, channel, stream_id, next_serial, durable_state, durable_state_reason, durable_state_node_id, durable_state_changed_at_ms, retained_messages, retained_bytes, oldest_available_serial, newest_available_serial, oldest_available_published_at_ms, newest_available_published_at_ms, updated_at_ms) VALUES (?, ?, ?, 1, 'healthy', NULL, NULL, ?, 0, 0, NULL, NULL, NULL, NULL, ?) ON DUPLICATE KEY UPDATE stream_id = VALUES(stream_id), next_serial = 1, durable_state = 'healthy', durable_state_reason = NULL, durable_state_node_id = NULL, durable_state_changed_at_ms = VALUES(durable_state_changed_at_ms), retained_messages = 0, retained_bytes = 0, oldest_available_serial = NULL, newest_available_serial = NULL, oldest_available_published_at_ms = NULL, newest_available_published_at_ms = NULL, updated_at_ms = VALUES(updated_at_ms)",
            self.tables.streams
        );
        sqlx::query(sqlx::AssertSqlSafe(upsert_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .bind(&new_stream_id)
            .bind(now_ms)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to rotate MySQL history stream: {e}")))?;
        tx.commit().await.map_err(|e| {
            Error::Internal(format!(
                "Failed to commit MySQL history reset transaction: {e}"
            ))
        })?;
        self.degraded_channels
            .remove(&degraded_channel_key(app_id, channel));
        if let Some(cache) = self.cache_manager.as_ref() {
            let _ = cache.remove(&degraded_cache_key(app_id, channel)).await;
        }
        if let Some(metrics) = self.metrics.as_deref() {
            let _ = refresh_history_state_metrics(&self.pool, &self.tables, metrics, app_id).await;
        }
        info!(app_id = %app_id, channel = %channel, previous_stream_id = ?previous_stream_id, new_stream_id = %new_stream_id, purged_messages, purged_bytes, reason = %reason, requested_by = ?requested_by, "Operator reset durable history stream");
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
        let now_ms = sockudo_core::history::now_ms();
        let mut tx = self.pool.begin().await.map_err(|e| {
            Error::Internal(format!(
                "Failed to begin MySQL history purge transaction: {e}"
            ))
        })?;
        let stats_sql = match request.mode {
            HistoryPurgeMode::All => format!(
                "SELECT COUNT(*) AS count, CAST(COALESCE(SUM(payload_size_bytes), 0) AS SIGNED) AS bytes FROM {} WHERE app_id = ? AND channel = ?",
                self.tables.entries
            ),
            HistoryPurgeMode::BeforeSerial => format!(
                "SELECT COUNT(*) AS count, CAST(COALESCE(SUM(payload_size_bytes), 0) AS SIGNED) AS bytes FROM {} WHERE app_id = ? AND channel = ? AND serial < ?",
                self.tables.entries
            ),
            HistoryPurgeMode::BeforeTimeMs => format!(
                "SELECT COUNT(*) AS count, CAST(COALESCE(SUM(payload_size_bytes), 0) AS SIGNED) AS bytes FROM {} WHERE app_id = ? AND channel = ? AND published_at_ms < ?",
                self.tables.entries
            ),
        };
        let mut stats_query = sqlx::query(sqlx::AssertSqlSafe(stats_sql.as_str()))
            .bind(app_id)
            .bind(channel);
        match request.mode {
            HistoryPurgeMode::All => {}
            HistoryPurgeMode::BeforeSerial => {
                stats_query = stats_query.bind(request.before_serial.unwrap_or_default() as i64)
            }
            HistoryPurgeMode::BeforeTimeMs => {
                stats_query = stats_query.bind(request.before_time_ms.unwrap_or_default())
            }
        }
        let stats = stats_query.fetch_one(&mut *tx).await.map_err(|e| {
            Error::Internal(format!("Failed to inspect MySQL history purge rows: {e}"))
        })?;
        let purged_messages = stats.get::<i64, _>("count") as u64;
        let purged_bytes = stats.get::<i64, _>("bytes") as u64;

        let delete_sql = match request.mode {
            HistoryPurgeMode::All => format!(
                "DELETE FROM {} WHERE app_id = ? AND channel = ?",
                self.tables.entries
            ),
            HistoryPurgeMode::BeforeSerial => format!(
                "DELETE FROM {} WHERE app_id = ? AND channel = ? AND serial < ?",
                self.tables.entries
            ),
            HistoryPurgeMode::BeforeTimeMs => format!(
                "DELETE FROM {} WHERE app_id = ? AND channel = ? AND published_at_ms < ?",
                self.tables.entries
            ),
        };
        let mut delete_query = sqlx::query(sqlx::AssertSqlSafe(delete_sql.as_str()))
            .bind(app_id)
            .bind(channel);
        match request.mode {
            HistoryPurgeMode::All => {}
            HistoryPurgeMode::BeforeSerial => {
                delete_query = delete_query.bind(request.before_serial.unwrap_or_default() as i64)
            }
            HistoryPurgeMode::BeforeTimeMs => {
                delete_query = delete_query.bind(request.before_time_ms.unwrap_or_default())
            }
        }
        delete_query
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to purge MySQL history rows: {e}")))?;

        let retained = Self::update_stream_retention_from_entries(
            &mut tx,
            &self.tables,
            app_id,
            channel,
            now_ms,
        )
        .await?;
        tx.commit().await.map_err(|e| {
            Error::Internal(format!(
                "Failed to commit MySQL history purge transaction: {e}"
            ))
        })?;
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.update_history_retained(
                app_id,
                retained.retained_messages,
                retained.retained_bytes,
            );
            let _ =
                refresh_history_state_metrics(&self.pool, &self.tables, metrics.as_ref(), app_id)
                    .await;
        }
        info!(app_id = %app_id, channel = %channel, mode = %request.mode.as_str(), before_serial = request.before_serial, before_time_ms = request.before_time_ms, purged_messages, purged_bytes, reason = %request.reason, requested_by = ?request.requested_by, "Operator purged durable history rows");
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
        let sql = format!(
            "DELETE FROM {} WHERE published_at_ms < ? ORDER BY published_at_ms ASC LIMIT ?",
            self.tables.entries
        );
        let rows_deleted = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(before_ms)
            .bind(batch_size as i64)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to purge expired MySQL history rows: {e}"))
            })?
            .rows_affected();
        Ok((rows_deleted, rows_deleted as usize >= batch_size))
    }
}
