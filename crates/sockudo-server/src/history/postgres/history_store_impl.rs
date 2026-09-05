use sockudo_core::error::{Error, Result};
use sockudo_core::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDirection, HistoryItem, HistoryPage,
    HistoryPurgeMode, HistoryPurgeRequest, HistoryPurgeResult, HistoryQueryBounds,
    HistoryReadRequest, HistoryResetResult, HistoryRetentionStats, HistoryRuntimeStatus,
    HistoryStore, HistoryStreamInspection, HistoryStreamRuntimeState, HistoryWriteReservation,
};
use std::sync::atomic::Ordering;

use sqlx::Row;
use tracing::info;

use super::history_store::PostgresHistoryStore;
use super::stream_state::{
    DegradeRequest, decrement_app_queue_depth, degraded_cache_key, degraded_channel_key,
    increment_app_queue_depth, mark_channel_degraded, refresh_history_state_metrics,
};

#[async_trait::async_trait]
impl HistoryStore for PostgresHistoryStore {
    async fn reserve_publish_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryWriteReservation> {
        let now_ms = sockudo_core::history::now_ms();
        let sql = format!(
            r#"
            INSERT INTO {} (app_id, channel, stream_id, next_serial, updated_at_ms)
            VALUES ($1, $2, $3, 2, $4)
            ON CONFLICT (app_id, channel)
            DO UPDATE SET
                next_serial = {}.next_serial + 1,
                updated_at_ms = EXCLUDED.updated_at_ms
            RETURNING stream_id, next_serial - 1 AS serial
            "#,
            self.tables.streams, self.tables.streams
        );
        let stream_id = uuid::Uuid::new_v4().to_string();
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .bind(stream_id)
            .bind(now_ms)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to reserve history position: {e}")))?;

        Ok(HistoryWriteReservation {
            stream_id: row.get::<String, _>("stream_id"),
            serial: row.get::<i64, _>("serial") as u64,
        })
    }

    async fn append(&self, record: HistoryAppendRecord) -> Result<()> {
        self.queue_depth_total.fetch_add(1, Ordering::Relaxed);
        increment_app_queue_depth(
            &self.queue_depth_by_app,
            &record.app_id,
            self.metrics.as_deref(),
        );
        let send_result = self
            .select_writer(&record.app_id, &record.channel)
            .tx
            .try_send(record.clone());

        if let Err(e) = send_result {
            self.queue_depth_total.fetch_sub(1, Ordering::Relaxed);
            decrement_app_queue_depth(
                &self.queue_depth_by_app,
                &record.app_id,
                self.metrics.as_deref(),
            );
            mark_channel_degraded(
                &self.pool,
                &self.tables,
                &self.degraded_channels,
                self.cache_manager.as_ref(),
                self.metrics.as_deref(),
                DegradeRequest {
                    app_id: &record.app_id,
                    channel: &record.channel,
                    reason: "history_writer_queue_full",
                    node_id: None,
                },
            )
            .await;
            return Err(Error::Internal(format!(
                "History writer queue is full: {e}"
            )));
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
                HistoryDirection::NewestFirst => "stream_id = $3 AND serial < $4".to_string(),
                HistoryDirection::OldestFirst => "stream_id = $3 AND serial > $4".to_string(),
            });
        }
        let mut next_bind = if request.cursor.is_some() { 5 } else { 3 };
        if let Some(start_serial) = request.bounds.start_serial {
            bind_start_serial = Some(start_serial as i64);
            clauses.push(format!("serial >= ${next_bind}"));
            next_bind += 1;
        }
        if let Some(end_serial) = request.bounds.end_serial {
            bind_end_serial = Some(end_serial as i64);
            clauses.push(format!("serial <= ${next_bind}"));
            next_bind += 1;
        }
        if let Some(start_time_ms) = request.bounds.start_time_ms {
            bind_start_time = Some(start_time_ms);
            clauses.push(format!("published_at_ms >= ${next_bind}"));
            next_bind += 1;
        }
        if let Some(end_time_ms) = request.bounds.end_time_ms {
            bind_end_time = Some(end_time_ms);
            clauses.push(format!("published_at_ms <= ${next_bind}"));
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
            r#"
            SELECT stream_id, serial, published_at_ms, message_id, event_name, operation_kind, payload_bytes, payload_size_bytes
            FROM {}
            WHERE app_id = $1 AND channel = $2
            {}
            ORDER BY serial {}
            LIMIT {}
            "#,
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
            .map_err(|e| Error::Internal(format!("Failed to read history page: {e}")))?;

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
            r#"
            SELECT
                COALESCE(SUM(CASE WHEN durable_state <> 'healthy' THEN 1 ELSE 0 END), 0) AS degraded_channels,
                COALESCE(SUM(CASE WHEN durable_state = 'reset_required' THEN 1 ELSE 0 END), 0) AS reset_required_channels
            FROM {}
            "#,
            self.tables.streams
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to read history runtime status: {e}")))?;

        Ok(HistoryRuntimeStatus {
            enabled: true,
            backend: "postgres".to_string(),
            state_authority: "durable_store".to_string(),
            degraded_channels: row.get::<i64, _>("degraded_channels") as usize,
            reset_required_channels: row.get::<i64, _>("reset_required_channels") as usize,
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
        let new_stream_id = uuid::Uuid::new_v4().to_string();
        let now_ms = sockudo_core::history::now_ms();

        let mut tx = self.pool.begin().await.map_err(|e| {
            Error::Internal(format!("Failed to begin history reset transaction: {e}"))
        })?;

        let lock_sql = format!(
            "SELECT retained_messages FROM {} WHERE app_id=$1 AND channel=$2 FOR UPDATE",
            self.tables.streams
        );
        sqlx::query(sqlx::AssertSqlSafe(lock_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to lock Postgres history stream for purge: {e}"
                ))
            })?;
        let delete_sql = format!(
            "DELETE FROM {} WHERE app_id = $1 AND channel = $2 RETURNING payload_size_bytes",
            self.tables.entries
        );
        let deleted_rows = sqlx::query(sqlx::AssertSqlSafe(delete_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to purge history rows during reset: {e}"))
            })?;
        let purged_messages = deleted_rows.len() as u64;
        let purged_bytes = deleted_rows
            .iter()
            .map(|row| row.get::<i64, _>("payload_size_bytes") as u64)
            .sum::<u64>();

        let upsert_sql = format!(
            r#"
            INSERT INTO {} (
                app_id, channel, stream_id, next_serial, durable_state, durable_state_reason,
                durable_state_node_id, durable_state_changed_at_ms, retained_messages, retained_bytes,
                oldest_available_serial, newest_available_serial, oldest_available_published_at_ms,
                newest_available_published_at_ms, updated_at_ms
            ) VALUES ($1, $2, $3, 1, 'healthy', NULL, NULL, $4, 0, 0, NULL, NULL, NULL, NULL, $4)
            ON CONFLICT (app_id, channel)
            DO UPDATE SET
                stream_id = EXCLUDED.stream_id,
                next_serial = 1,
                durable_state = 'healthy',
                durable_state_reason = NULL,
                durable_state_node_id = NULL,
                durable_state_changed_at_ms = EXCLUDED.durable_state_changed_at_ms,
                retained_messages = 0,
                retained_bytes = 0,
                oldest_available_serial = NULL,
                newest_available_serial = NULL,
                oldest_available_published_at_ms = NULL,
                newest_available_published_at_ms = NULL,
                updated_at_ms = EXCLUDED.updated_at_ms
            "#,
            self.tables.streams
        );
        sqlx::query(sqlx::AssertSqlSafe(upsert_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .bind(&new_stream_id)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to rotate history stream: {e}")))?;

        tx.commit().await.map_err(|e| {
            Error::Internal(format!("Failed to commit history reset transaction: {e}"))
        })?;

        self.degraded_channels
            .remove(&degraded_channel_key(app_id, channel));
        if let Some(cache) = self.cache_manager.as_ref() {
            let _ = cache.remove(&degraded_cache_key(app_id, channel)).await;
        }
        if let Some(metrics) = self.metrics.as_deref() {
            let _ = refresh_history_state_metrics(&self.pool, &self.tables, metrics, app_id).await;
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
        let now_ms = sockudo_core::history::now_ms();

        let mut tx = self.pool.begin().await.map_err(|e| {
            Error::Internal(format!("Failed to begin history purge transaction: {e}"))
        })?;

        let lock_sql = format!(
            "SELECT retained_messages FROM {} WHERE app_id=$1 AND channel=$2 FOR UPDATE",
            self.tables.streams
        );
        sqlx::query(sqlx::AssertSqlSafe(lock_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to lock Postgres history stream for purge: {e}"
                ))
            })?;
        let delete_sql = match request.mode {
            HistoryPurgeMode::All => format!(
                "DELETE FROM {} WHERE app_id = $1 AND channel = $2 RETURNING payload_size_bytes",
                self.tables.entries
            ),
            HistoryPurgeMode::BeforeSerial => format!(
                "DELETE FROM {} WHERE app_id = $1 AND channel = $2 AND serial < $3 RETURNING payload_size_bytes",
                self.tables.entries
            ),
            HistoryPurgeMode::BeforeTimeMs => format!(
                "DELETE FROM {} WHERE app_id = $1 AND channel = $2 AND published_at_ms < $3 RETURNING payload_size_bytes",
                self.tables.entries
            ),
        };
        let mut query = sqlx::query(sqlx::AssertSqlSafe(delete_sql.as_str()))
            .bind(app_id)
            .bind(channel);
        match request.mode {
            HistoryPurgeMode::All => {}
            HistoryPurgeMode::BeforeSerial => {
                query = query.bind(request.before_serial.unwrap_or_default() as i64);
            }
            HistoryPurgeMode::BeforeTimeMs => {
                query = query.bind(request.before_time_ms.unwrap_or_default());
            }
        }

        let deleted_rows = query
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to purge history rows: {e}")))?;
        let purged_messages = deleted_rows.len() as u64;
        let purged_bytes = deleted_rows
            .iter()
            .map(|row| row.get::<i64, _>("payload_size_bytes") as u64)
            .sum::<u64>();

        let retained = Self::update_stream_retention_from_entries(
            &mut tx,
            &self.tables,
            app_id,
            channel,
            now_ms,
        )
        .await?;

        tx.commit().await.map_err(|e| {
            Error::Internal(format!("Failed to commit history purge transaction: {e}"))
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
        let select = format!(
            "SELECT app_id,channel,stream_id,serial FROM {} WHERE published_at_ms<$1 ORDER BY published_at_ms ASC LIMIT $2",
            self.tables.entries
        );
        let candidates = sqlx::query(sqlx::AssertSqlSafe(select.as_str()))
            .bind(before_ms)
            .bind(batch_size.min(4096) as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to select expired history batch: {e}")))?;
        let candidate_count = candidates.len();
        let mut channels =
            std::collections::BTreeMap::<(String, String), Vec<(String, i64)>>::new();
        for row in candidates {
            channels
                .entry((row.get("app_id"), row.get("channel")))
                .or_default()
                .push((row.get("stream_id"), row.get("serial")));
        }
        let mut total = 0;
        for ((app, channel), keys) in channels {
            let mut tx = self.pool.begin().await.map_err(|e| {
                Error::Internal(format!("Failed to begin history expiry batch: {e}"))
            })?;
            let lock = format!(
                "SELECT retained_messages FROM {} WHERE app_id=$1 AND channel=$2 FOR UPDATE",
                self.tables.streams
            );
            sqlx::query(sqlx::AssertSqlSafe(lock.as_str()))
                .bind(&app)
                .bind(&channel)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to lock history expiry channel: {e}"))
                })?;
            let (streams, serials): (Vec<_>, Vec<_>) = keys.into_iter().unzip();
            let delete = format!(
                "DELETE FROM {} WHERE app_id=$1 AND channel=$2 AND published_at_ms<$3 AND (stream_id,serial) IN (SELECT * FROM UNNEST($4::text[],$5::bigint[])) RETURNING payload_size_bytes",
                self.tables.entries
            );
            let removed = sqlx::query(sqlx::AssertSqlSafe(delete.as_str()))
                .bind(&app)
                .bind(&channel)
                .bind(before_ms)
                .bind(streams)
                .bind(serials)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to remove history expiry batch: {e}"))
                })?;
            let count = removed.len() as u64;
            let bytes = removed
                .iter()
                .map(|row| row.get::<i64, _>("payload_size_bytes"))
                .sum::<i64>();
            let update = format!(
                "UPDATE {streams} SET retained_messages=GREATEST(retained_messages-$3,0),retained_bytes=GREATEST(retained_bytes-$4,0),oldest_available_serial=(SELECT serial FROM {entries} WHERE app_id=$1 AND channel=$2 ORDER BY serial ASC LIMIT 1),newest_available_serial=(SELECT serial FROM {entries} WHERE app_id=$1 AND channel=$2 ORDER BY serial DESC LIMIT 1),oldest_available_published_at_ms=(SELECT published_at_ms FROM {entries} WHERE app_id=$1 AND channel=$2 ORDER BY published_at_ms ASC LIMIT 1),newest_available_published_at_ms=(SELECT published_at_ms FROM {entries} WHERE app_id=$1 AND channel=$2 ORDER BY published_at_ms DESC LIMIT 1) WHERE app_id=$1 AND channel=$2",
                streams = self.tables.streams,
                entries = self.tables.entries
            );
            sqlx::query(sqlx::AssertSqlSafe(update.as_str()))
                .bind(&app)
                .bind(&channel)
                .bind(count as i64)
                .bind(bytes)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to account history expiry batch: {e}"))
                })?;
            tx.commit().await.map_err(|e| {
                Error::Internal(format!("Failed to commit history expiry batch: {e}"))
            })?;
            total += count;
        }
        Ok((total, candidate_count >= batch_size.min(4096)))
    }
}

fn is_truncated_by_retention(
    bounds: &HistoryQueryBounds,
    retained: &HistoryRetentionStats,
) -> bool {
    if let (Some(start_serial), Some(oldest_serial)) = (bounds.start_serial, retained.oldest_serial)
        && start_serial < oldest_serial
    {
        return true;
    }
    if let (Some(start_time_ms), Some(oldest_time_ms)) =
        (bounds.start_time_ms, retained.oldest_published_at_ms)
        && start_time_ms < oldest_time_ms
    {
        return true;
    }
    bounds.start_serial.is_none()
        && bounds.start_time_ms.is_none()
        && retained
            .oldest_serial
            .is_some_and(|oldest_serial| oldest_serial > 1)
}
