use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDirection, HistoryDurableState, HistoryItem,
    HistoryPage, HistoryPurgeMode, HistoryPurgeRequest, HistoryPurgeResult, HistoryQueryBounds,
    HistoryReadRequest, HistoryResetResult, HistoryRetentionStats, HistoryRuntimeStatus,
    HistoryStore, HistoryStreamInspection, HistoryStreamRuntimeState, HistoryWriteReservation,
};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::{HistoryConfig, SurrealDbSettings};
use sonic_rs::JsonValueTrait;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::auth::Root;
use surrealdb_types::SurrealValue;
use tracing::{error, info};

#[derive(Clone)]
struct HistoryTables {
    streams: String,
    entries: String,
    entries_stream_serial_idx: String,
    entries_stream_time_idx: String,
    streams_app_idx: String,
}

#[derive(Debug, Clone)]
struct HistoryDegradedState {
    app_id: String,
    channel: String,
    durable_state: HistoryDurableState,
    reason: String,
    node_id: Option<String>,
    last_transition_at_ms: i64,
    observed_source: &'static str,
}

#[derive(Debug, Clone)]
struct HistoryStreamRecord {
    stream_id: String,
    next_serial: u64,
    durable_state: HistoryDurableState,
    durable_state_reason: Option<String>,
    durable_state_node_id: Option<String>,
    durable_state_changed_at_ms: Option<i64>,
    retained: HistoryRetentionStats,
}

impl HistoryStreamRecord {
    fn runtime_state(
        &self,
        app_id: &str,
        channel: &str,
        observed_source: &str,
    ) -> HistoryStreamRuntimeState {
        HistoryStreamRuntimeState {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: Some(self.stream_id.clone()),
            durable_state: self.durable_state,
            recovery_allowed: self.durable_state.recovery_allowed(),
            reset_required: self.durable_state.reset_required(),
            reason: self.durable_state_reason.clone(),
            node_id: self.durable_state_node_id.clone(),
            last_transition_at_ms: self.durable_state_changed_at_ms,
            authoritative_source: "durable_store".to_string(),
            observed_source: observed_source.to_string(),
        }
    }

    fn inspection(
        &self,
        app_id: &str,
        channel: &str,
        observed_source: &str,
    ) -> HistoryStreamInspection {
        HistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: Some(self.stream_id.clone()),
            next_serial: Some(self.next_serial),
            retained: self.retained.clone(),
            state: self.runtime_state(app_id, channel, observed_source),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct StoredStreamRecord {
    app_id: String,
    channel: String,
    stream_id: String,
    next_serial: i64,
    retained_messages: i64,
    retained_bytes: i64,
    oldest_available_serial: Option<i64>,
    newest_available_serial: Option<i64>,
    oldest_available_published_at_ms: Option<i64>,
    newest_available_published_at_ms: Option<i64>,
    durable_state: String,
    durable_state_reason: Option<String>,
    durable_state_node_id: Option<String>,
    durable_state_changed_at_ms: Option<i64>,
    updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct StoredEntryRecord {
    app_id: String,
    channel: String,
    stream_id: String,
    serial: i64,
    published_at_ms: i64,
    message_id: Option<String>,
    event_name: Option<String>,
    operation_kind: String,
    payload_bytes: Vec<u8>,
    payload_size_bytes: i64,
}

#[derive(Debug, Clone, Deserialize, SurrealValue)]
struct EntryKeyRecord {
    serial: i64,
    published_at_ms: i64,
    payload_size_bytes: i64,
}

#[derive(Debug, Clone, Deserialize, SurrealValue)]
struct DurableStateRow {
    durable_state: String,
}

pub struct SurrealHistoryStore {
    db: Surreal<Any>,
    tables: HistoryTables,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    degraded_channels: Arc<DashMap<String, HistoryDegradedState>>,
    queue_depth_total: AtomicUsize,
}

pub async fn create_surreal_history_store(
    db_config: &SurrealDbSettings,
    config: HistoryConfig,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
) -> Result<Arc<dyn HistoryStore + Send + Sync>> {
    let store = SurrealHistoryStore::new(db_config, config, metrics, cache_manager).await?;
    Ok(Arc::new(store))
}

impl SurrealHistoryStore {
    async fn new(
        db_config: &SurrealDbSettings,
        config: HistoryConfig,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
        cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    ) -> Result<Self> {
        let streams = format!("{}_streams", config.surrealdb.table_prefix);
        let entries = format!("{}_entries", config.surrealdb.table_prefix);
        validate_identifier(&streams, "streams table")?;
        validate_identifier(&entries, "entries table")?;

        let db = connect(db_config.url.as_str())
            .await
            .map_err(|e| Error::Internal(format!("Failed to connect to SurrealDB: {e}")))?;
        db.signin(Root {
            username: db_config.username.clone(),
            password: db_config.password.clone(),
        })
        .await
        .map_err(|e| Error::Internal(format!("Failed to authenticate with SurrealDB: {e}")))?;
        db.use_ns(db_config.namespace.as_str())
            .use_db(db_config.database.as_str())
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to select SurrealDB namespace/database: {e}"
                ))
            })?;

        let store = Self {
            db,
            tables: HistoryTables {
                streams_app_idx: format!("{}_app_idx", streams),
                entries_stream_serial_idx: format!("{}_stream_serial_idx", entries),
                entries_stream_time_idx: format!("{}_stream_time_idx", entries),
                streams,
                entries,
            },
            metrics,
            cache_manager,
            degraded_channels: Arc::new(DashMap::new()),
            queue_depth_total: AtomicUsize::new(0),
        };
        store.ensure_schema().await?;
        Ok(store)
    }

    async fn ensure_schema(&self) -> Result<()> {
        let query = format!(
            "DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
             DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS app_id;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS app_id, channel, stream_id, serial;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS app_id, channel, stream_id, published_at_ms;",
            self.tables.streams,
            self.tables.entries,
            self.tables.streams_app_idx,
            self.tables.streams,
            self.tables.entries_stream_serial_idx,
            self.tables.entries,
            self.tables.entries_stream_time_idx,
            self.tables.entries,
        );
        self.db.query(query).await.map_err(|e| {
            Error::Internal(format!(
                "Failed to initialize SurrealDB history schema: {e}"
            ))
        })?;
        Ok(())
    }

    fn stream_resource(&self, app_id: &str, channel: &str) -> (String, String) {
        (
            self.tables.streams.clone(),
            deterministic_key([app_id, channel].into_iter()),
        )
    }

    fn entry_resource(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        serial: u64,
    ) -> (String, String) {
        (
            self.tables.entries.clone(),
            deterministic_key(
                [
                    app_id.to_string(),
                    channel.to_string(),
                    stream_id.to_string(),
                    format!("{serial:020}"),
                ]
                .into_iter(),
            ),
        )
    }

    async fn load_stream_raw(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Option<StoredStreamRecord>> {
        self.db
            .select(self.stream_resource(app_id, channel))
            .await
            .map_err(|e| Error::Internal(format!("Failed to fetch SurrealDB history stream: {e}")))
    }

    async fn load_stream_record(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Option<HistoryStreamRecord>> {
        let Some(raw) = self.load_stream_raw(app_id, channel).await? else {
            return Ok(None);
        };
        Ok(Some(HistoryStreamRecord {
            stream_id: raw.stream_id.clone(),
            next_serial: raw.next_serial as u64,
            durable_state: parse_history_durable_state(&raw.durable_state),
            durable_state_reason: raw.durable_state_reason.clone(),
            durable_state_node_id: raw.durable_state_node_id.clone(),
            durable_state_changed_at_ms: raw.durable_state_changed_at_ms,
            retained: retained_from_stream_record(&raw),
        }))
    }

    async fn load_entry_keys_for_stream(
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

    async fn load_page_entries_for_stream(
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

    async fn retained_stats(&self, app_id: &str, channel: &str) -> Result<HistoryRetentionStats> {
        Ok(match self.load_stream_raw(app_id, channel).await? {
            Some(stream) => retained_from_stream_record(&stream),
            None => HistoryRetentionStats::default(),
        })
    }

    async fn upsert_stream_raw(
        &self,
        app_id: &str,
        channel: &str,
        record: &StoredStreamRecord,
    ) -> Result<()> {
        let _: Option<StoredStreamRecord> = self
            .db
            .upsert(self.stream_resource(app_id, channel))
            .content(record.clone())
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to upsert SurrealDB history stream: {e}"))
            })?;
        Ok(())
    }

    async fn resolved_stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamRuntimeState> {
        let durable_record = self.load_stream_record(app_id, channel).await?;
        let durable_state = durable_record
            .as_ref()
            .map(|record| record.runtime_state(app_id, channel, "durable_store"))
            .unwrap_or_else(|| {
                HistoryStreamRuntimeState::healthy(app_id, channel, None, "durable_store")
            });
        let local_hint = self
            .degraded_channels
            .get(&degraded_channel_key(app_id, channel))
            .map(|entry| entry.value().clone());
        let cache_hint =
            get_cached_channel_degraded(self.cache_manager.as_ref(), app_id, channel).await?;
        Ok(resolve_runtime_state(durable_state, local_hint, cache_hint))
    }

    async fn resolved_stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamInspection> {
        let durable_record = self.load_stream_record(app_id, channel).await?;
        let runtime_state = self.resolved_stream_runtime_state(app_id, channel).await?;
        Ok(match durable_record {
            Some(record) => record.inspection(app_id, channel, &runtime_state.observed_source),
            None => HistoryStreamInspection {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                stream_id: None,
                next_serial: None,
                retained: HistoryRetentionStats::default(),
                state: runtime_state,
            },
        })
    }

    async fn delete_entries(
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

    async fn persist_record(&self, record: &HistoryAppendRecord) -> Result<()> {
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
            previous_stream_id = ?previous_stream_id,
            new_stream_id = %new_stream.stream_id,
            purged_messages,
            purged_bytes,
            reason = %reason,
            requested_by = ?requested_by,
            "Operator reset durable history stream"
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
            requested_by = ?request.requested_by,
            "Operator purged durable history rows"
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

fn validate_identifier(identifier: &str, field_name: &str) -> Result<()> {
    let valid = identifier.chars().enumerate().all(|(idx, ch)| {
        if idx == 0 {
            ch.is_ascii_alphabetic() || ch == '_'
        } else {
            ch.is_ascii_alphanumeric() || ch == '_'
        }
    });
    if valid {
        Ok(())
    } else {
        Err(Error::Internal(format!(
            "Invalid SurrealDB {field_name} '{identifier}'. Use only letters, numbers, and underscores."
        )))
    }
}

fn deterministic_key<I, S>(parts: I) -> String
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let mut key = String::new();
    for part in parts {
        let part = part.as_ref();
        key.push_str(&part.len().to_string());
        key.push(':');
        key.push_str(part);
        key.push('|');
    }
    key
}

fn retained_from_stream_record(stream: &StoredStreamRecord) -> HistoryRetentionStats {
    HistoryRetentionStats {
        stream_id: Some(stream.stream_id.clone()),
        retained_messages: stream.retained_messages.max(0) as u64,
        retained_bytes: stream.retained_bytes.max(0) as u64,
        oldest_serial: stream.oldest_available_serial.map(|value| value as u64),
        newest_serial: stream.newest_available_serial.map(|value| value as u64),
        oldest_published_at_ms: stream.oldest_available_published_at_ms,
        newest_published_at_ms: stream.newest_available_published_at_ms,
    }
}

fn parse_history_durable_state(raw: &str) -> HistoryDurableState {
    match raw {
        "healthy" => HistoryDurableState::Healthy,
        "reset_required" => HistoryDurableState::ResetRequired,
        "degraded" => HistoryDurableState::Degraded,
        _ => HistoryDurableState::Degraded,
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

fn degraded_channel_key(app_id: &str, channel: &str) -> String {
    format!("{app_id}\0{channel}")
}

fn degraded_cache_key(app_id: &str, channel: &str) -> String {
    format!("history:degraded:{app_id}:{channel}")
}

fn resolve_runtime_state(
    durable_state: HistoryStreamRuntimeState,
    local_hint: Option<HistoryDegradedState>,
    cache_hint: Option<HistoryDegradedState>,
) -> HistoryStreamRuntimeState {
    let newest_hint = [local_hint, cache_hint]
        .into_iter()
        .flatten()
        .max_by_key(|hint| hint.last_transition_at_ms);
    let durable_transition_at = durable_state.last_transition_at_ms.unwrap_or_default();

    if let Some(hint) = newest_hint
        && hint.last_transition_at_ms > durable_transition_at
    {
        return HistoryStreamRuntimeState {
            app_id: durable_state.app_id,
            channel: durable_state.channel,
            stream_id: durable_state.stream_id,
            durable_state: hint.durable_state,
            recovery_allowed: hint.durable_state.recovery_allowed(),
            reset_required: hint.durable_state.reset_required(),
            reason: Some(hint.reason),
            node_id: hint.node_id,
            last_transition_at_ms: Some(hint.last_transition_at_ms),
            authoritative_source: durable_state.authoritative_source,
            observed_source: hint.observed_source.to_string(),
        };
    }

    durable_state
}

struct DegradeRequest<'a> {
    app_id: &'a str,
    channel: &'a str,
    reason: &'a str,
    node_id: Option<String>,
}

async fn mark_channel_degraded(
    db: &Surreal<Any>,
    tables: &HistoryTables,
    degraded_channels: &DashMap<String, HistoryDegradedState>,
    cache_manager: Option<&Arc<dyn CacheManager + Send + Sync>>,
    metrics: Option<&(dyn MetricsInterface + Send + Sync)>,
    request: DegradeRequest<'_>,
) {
    let now_ms = sockudo_core::history::now_ms();
    let state = HistoryDegradedState {
        app_id: request.app_id.to_string(),
        channel: request.channel.to_string(),
        durable_state: HistoryDurableState::Degraded,
        reason: request.reason.to_string(),
        node_id: request.node_id,
        last_transition_at_ms: now_ms,
        observed_source: "local_memory_hint",
    };
    degraded_channels.insert(
        degraded_channel_key(request.app_id, request.channel),
        state.clone(),
    );
    if let Some(cache) = cache_manager {
        let _ = cache
            .set(
                &degraded_cache_key(request.app_id, request.channel),
                &sonic_rs::to_string(&sonic_rs::json!({
                    "app_id": state.app_id,
                    "channel": state.channel,
                    "durable_state": state.durable_state.as_str(),
                    "reason": state.reason,
                    "node_id": state.node_id,
                    "last_transition_at_ms": state.last_transition_at_ms,
                }))
                .unwrap_or_else(|_| "{}".to_string()),
                3600,
            )
            .await;
    }

    let resource = (
        tables.streams.clone(),
        deterministic_key([request.app_id, request.channel].into_iter()),
    );
    let current: std::result::Result<Option<StoredStreamRecord>, _> =
        db.select(resource.clone()).await;
    match current {
        Ok(Some(stream)) => {
            let updated = StoredStreamRecord {
                durable_state: state.durable_state.as_str().to_string(),
                durable_state_reason: Some(state.reason.clone()),
                durable_state_node_id: state.node_id.clone(),
                durable_state_changed_at_ms: Some(state.last_transition_at_ms),
                updated_at_ms: now_ms,
                ..stream
            };
            let upsert_result: std::result::Result<Option<StoredStreamRecord>, _> =
                db.upsert(resource).content(updated).await;
            if let Err(err) = upsert_result {
                error!(app_id = %request.app_id, channel = %request.channel, "Failed to persist SurrealDB history degraded state: {err}");
            }
        }
        Ok(None) => {}
        Err(err) => {
            error!(app_id = %request.app_id, channel = %request.channel, "Failed to load SurrealDB history stream before degrade: {err}");
        }
    }
    if let Some(metrics) = metrics {
        let _ = refresh_history_state_metrics(db, tables, metrics, request.app_id).await;
    }
}

async fn get_cached_channel_degraded(
    cache_manager: Option<&Arc<dyn CacheManager + Send + Sync>>,
    app_id: &str,
    channel: &str,
) -> Result<Option<HistoryDegradedState>> {
    if let Some(cache) = cache_manager
        && let Some(raw) = cache.get(&degraded_cache_key(app_id, channel)).await?
    {
        let value: sonic_rs::Value = sonic_rs::from_str(&raw).map_err(|e| {
            Error::Internal(format!(
                "Failed to parse degraded SurrealDB history state: {e}"
            ))
        })?;
        return Ok(Some(HistoryDegradedState {
            app_id: value
                .get("app_id")
                .and_then(sonic_rs::Value::as_str)
                .unwrap_or(app_id)
                .to_string(),
            channel: value
                .get("channel")
                .and_then(sonic_rs::Value::as_str)
                .unwrap_or(channel)
                .to_string(),
            durable_state: value
                .get("durable_state")
                .and_then(sonic_rs::Value::as_str)
                .map(parse_history_durable_state)
                .unwrap_or(HistoryDurableState::Degraded),
            reason: value
                .get("reason")
                .and_then(sonic_rs::Value::as_str)
                .unwrap_or("history_stream_degraded")
                .to_string(),
            node_id: value
                .get("node_id")
                .and_then(sonic_rs::Value::as_str)
                .map(str::to_string),
            last_transition_at_ms: value
                .get("last_transition_at_ms")
                .and_then(sonic_rs::Value::as_i64)
                .unwrap_or_default(),
            observed_source: "shared_cache_hint",
        }));
    }
    Ok(None)
}

async fn refresh_history_state_metrics(
    db: &Surreal<Any>,
    tables: &HistoryTables,
    metrics: &(dyn MetricsInterface + Send + Sync),
    app_id: &str,
) -> Result<()> {
    let mut response = db
        .query(format!(
            "SELECT durable_state FROM {} WHERE app_id = $app_id",
            tables.streams
        ))
        .bind(("app_id", app_id.to_string()))
        .await
        .map_err(|e| Error::Internal(format!("Failed to query SurrealDB history metrics: {e}")))?;
    let rows: Vec<DurableStateRow> = response
        .take(0usize)
        .map_err(|e| Error::Internal(format!("Failed to decode SurrealDB history metrics: {e}")))?;
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
    metrics.update_history_degraded_channels(app_id, degraded_channels);
    metrics.update_history_reset_required_channels(app_id, reset_required_channels);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::history_conformance::HistoryStoreConformance;

    async fn is_surreal_available() -> bool {
        let db = connect("ws://127.0.0.1:18001").await;
        let Ok(db) = db else {
            return false;
        };
        if db
            .signin(Root {
                username: "root".to_string(),
                password: "root".to_string(),
            })
            .await
            .is_err()
        {
            return false;
        }
        db.health().await.is_ok()
    }

    async fn build_store() -> Arc<dyn HistoryStore + Send + Sync> {
        let settings = SurrealDbSettings {
            url: "ws://127.0.0.1:18001".to_string(),
            namespace: format!("sockudo_history_test_{}", uuid::Uuid::new_v4().simple()),
            database: "sockudo".to_string(),
            username: "root".to_string(),
            password: "root".to_string(),
            table_name: "applications".to_string(),
            cache_ttl: 300,
            cache_max_capacity: 100,
        };
        let config = HistoryConfig {
            enabled: true,
            backend: sockudo_core::options::HistoryBackend::SurrealDb,
            surrealdb: sockudo_core::options::SurrealDbHistoryConfig {
                table_prefix: format!("sockudo_history_{}", uuid::Uuid::new_v4().simple()),
                ..sockudo_core::options::SurrealDbHistoryConfig::default()
            },
            ..HistoryConfig::default()
        };
        create_surreal_history_store(&settings, config, None, None)
            .await
            .unwrap()
    }

    async fn seed_time_series(
        store: &Arc<dyn HistoryStore + Send + Sync>,
    ) -> sockudo_core::history::HistoryWriteReservation {
        let reservation = store.reserve_publish_position("app", "chat").await.unwrap();
        for offset in 0..5u64 {
            store
                .append(sockudo_core::history::HistoryAppendRecord {
                    app_id: "app".to_string(),
                    channel: "chat".to_string(),
                    stream_id: reservation.stream_id.clone(),
                    serial: reservation.serial + offset,
                    published_at_ms: 1_000 + offset as i64,
                    message_id: Some(format!("msg-{}", reservation.serial + offset)),
                    event_name: Some("event".to_string()),
                    operation_kind: "append".to_string(),
                    payload_bytes: tokio_util::bytes::Bytes::from(format!(
                        "payload-{}",
                        reservation.serial + offset
                    )),
                    retention: sockudo_core::history::HistoryRetentionPolicy {
                        retention_window_seconds: 3600,
                        max_messages_per_channel: None,
                        max_bytes_per_channel: None,
                    },
                })
                .await
                .unwrap();
        }
        reservation
    }

    #[tokio::test]
    async fn surreal_history_store_conformance_serial_and_stream_continuity() {
        if !is_surreal_available().await {
            eprintln!("Skipping test: SurrealDB not available");
            return;
        }
        let store = build_store().await;
        HistoryStoreConformance::assert_serial_monotonicity(store.clone())
            .await
            .unwrap();
        HistoryStoreConformance::assert_stream_id_continuity(store)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn surreal_history_store_conformance_pagination_and_reset_semantics() {
        if !is_surreal_available().await {
            eprintln!("Skipping test: SurrealDB not available");
            return;
        }
        let store = build_store().await;
        HistoryStoreConformance::assert_cursor_pagination(store.clone())
            .await
            .unwrap();
        HistoryStoreConformance::assert_purge_and_reset_semantics(store)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn surreal_history_store_time_bounded_pagination_uses_time_ordering() {
        if !is_surreal_available().await {
            eprintln!("Skipping test: SurrealDB not available");
            return;
        }
        let store = build_store().await;
        let reservation = seed_time_series(&store).await;

        let first = store
            .read_page(sockudo_core::history::HistoryReadRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                direction: sockudo_core::history::HistoryDirection::OldestFirst,
                limit: 2,
                cursor: None,
                bounds: sockudo_core::history::HistoryQueryBounds {
                    start_serial: None,
                    end_serial: None,
                    start_time_ms: Some(1_001),
                    end_time_ms: Some(1_004),
                },
            })
            .await
            .unwrap();
        assert_eq!(
            first
                .items
                .iter()
                .map(|item| item.serial)
                .collect::<Vec<_>>(),
            vec![reservation.serial + 1, reservation.serial + 2]
        );
        assert!(first.has_more);

        let second = store
            .read_page(sockudo_core::history::HistoryReadRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                direction: sockudo_core::history::HistoryDirection::OldestFirst,
                limit: 2,
                cursor: first.next_cursor,
                bounds: sockudo_core::history::HistoryQueryBounds {
                    start_serial: None,
                    end_serial: None,
                    start_time_ms: Some(1_001),
                    end_time_ms: Some(1_004),
                },
            })
            .await
            .unwrap();
        assert_eq!(
            second
                .items
                .iter()
                .map(|item| item.serial)
                .collect::<Vec<_>>(),
            vec![reservation.serial + 3, reservation.serial + 4]
        );
        assert!(!second.has_more);
    }
}
