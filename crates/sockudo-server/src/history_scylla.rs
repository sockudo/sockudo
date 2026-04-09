use dashmap::DashMap;
use futures_util::TryStreamExt;
use scylla::DeserializeRow;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::{SerialConsistency, Statement};
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDirection, HistoryDurableState, HistoryItem,
    HistoryPage, HistoryPurgeMode, HistoryPurgeRequest, HistoryPurgeResult, HistoryQueryBounds,
    HistoryReadRequest, HistoryResetResult, HistoryRetentionStats, HistoryRuntimeStatus,
    HistoryStore, HistoryStreamInspection, HistoryStreamRuntimeState, HistoryWriteReservation,
};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::{HistoryConfig, ScyllaDbSettings};
use sonic_rs::JsonValueTrait;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tracing::{error, info};

#[derive(Clone)]
struct HistoryTables {
    keyspace: String,
    streams: String,
    entries: String,
}

impl HistoryTables {
    fn streams_fq(&self) -> String {
        format!("{}.{}", self.keyspace, self.streams)
    }

    fn entries_fq(&self) -> String {
        format!("{}.{}", self.keyspace, self.entries)
    }
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
    retained_messages: u64,
    retained_bytes: u64,
    oldest_serial: Option<u64>,
    newest_serial: Option<u64>,
    oldest_published_at_ms: Option<i64>,
    newest_published_at_ms: Option<i64>,
    durable_state: HistoryDurableState,
    durable_state_reason: Option<String>,
    durable_state_node_id: Option<String>,
    durable_state_changed_at_ms: Option<i64>,
}

impl HistoryStreamRecord {
    fn retention_stats(&self) -> HistoryRetentionStats {
        HistoryRetentionStats {
            stream_id: Some(self.stream_id.clone()),
            retained_messages: self.retained_messages,
            retained_bytes: self.retained_bytes,
            oldest_serial: self.oldest_serial,
            newest_serial: self.newest_serial,
            oldest_published_at_ms: self.oldest_published_at_ms,
            newest_published_at_ms: self.newest_published_at_ms,
        }
    }

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
            retained: self.retention_stats(),
            state: self.runtime_state(app_id, channel, observed_source),
        }
    }
}

#[derive(Debug, DeserializeRow)]
struct StreamRow {
    stream_id: String,
    next_serial: i64,
    durable_state: String,
    durable_state_reason: Option<String>,
    durable_state_node_id: Option<String>,
    durable_state_changed_at_ms: Option<i64>,
    retained_messages: i64,
    retained_bytes: i64,
    oldest_available_serial: Option<i64>,
    newest_available_serial: Option<i64>,
    oldest_available_published_at_ms: Option<i64>,
    newest_available_published_at_ms: Option<i64>,
}

#[derive(Debug, DeserializeRow, Clone)]
struct EntryRow {
    stream_id: String,
    serial: i64,
    published_at_ms: i64,
    message_id: Option<String>,
    event_name: Option<String>,
    operation_kind: String,
    payload_bytes: Vec<u8>,
    payload_size_bytes: i64,
}

#[derive(Debug, DeserializeRow, Clone)]
struct EntryKeyRow {
    serial: i64,
    published_at_ms: i64,
    payload_size_bytes: i64,
}

#[derive(Debug, DeserializeRow)]
struct DurableStateRow {
    durable_state: String,
}

type LwtApplyOnlyRow = (bool,);
type LwtConditionalRow = (bool, Option<i64>, Option<String>);
type LwtStreamRow = (
    bool,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<i64>,
);

pub struct ScyllaHistoryStore {
    session: Arc<Session>,
    config: HistoryConfig,
    tables: HistoryTables,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    degraded_channels: Arc<DashMap<String, HistoryDegradedState>>,
    queue_depth_total: AtomicUsize,
}

pub async fn create_scylla_history_store(
    db_config: &ScyllaDbSettings,
    config: HistoryConfig,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
) -> Result<Arc<dyn HistoryStore + Send + Sync>> {
    let store = ScyllaHistoryStore::new(db_config, config, metrics, cache_manager).await?;
    Ok(Arc::new(store))
}

impl ScyllaHistoryStore {
    async fn new(
        db_config: &ScyllaDbSettings,
        config: HistoryConfig,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
        cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    ) -> Result<Self> {
        let mut builder = SessionBuilder::new().known_nodes(db_config.nodes.clone());
        if let (Some(username), Some(password)) = (&db_config.username, &db_config.password) {
            builder = builder.user(username, password);
        }

        let session = builder.build().await.map_err(|e| {
            Error::Internal(format!("Failed to connect history store to ScyllaDB: {e}"))
        })?;
        let session = Arc::new(session);
        let keyspace = if db_config.keyspace.trim().is_empty() {
            "sockudo".to_string()
        } else {
            db_config.keyspace.clone()
        };
        let tables = HistoryTables {
            keyspace,
            streams: format!("{}_streams", config.scylladb.table_prefix),
            entries: format!("{}_entries", config.scylladb.table_prefix),
        };

        let store = Self {
            session,
            config,
            tables,
            metrics,
            cache_manager,
            degraded_channels: Arc::new(DashMap::new()),
            queue_depth_total: AtomicUsize::new(0),
        };
        store.ensure_schema().await?;
        Ok(store)
    }

    async fn ensure_schema(&self) -> Result<()> {
        let create_keyspace = format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}} AND tablets = {{'enabled': false}}",
            self.tables.keyspace
        );
        self.session
            .query_unpaged(create_keyspace, ())
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to create ScyllaDB history keyspace: {e}"))
            })?;

        let create_streams = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                app_id text,
                channel text,
                stream_id text,
                next_serial bigint,
                durable_state text,
                durable_state_reason text,
                durable_state_node_id text,
                durable_state_changed_at_ms bigint,
                retained_messages bigint,
                retained_bytes bigint,
                oldest_available_serial bigint,
                newest_available_serial bigint,
                oldest_available_published_at_ms bigint,
                newest_available_published_at_ms bigint,
                updated_at_ms bigint,
                PRIMARY KEY ((app_id), channel)
            )",
            self.tables.streams_fq()
        );
        self.session
            .query_unpaged(create_streams, ())
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to create ScyllaDB history streams table: {e}"
                ))
            })?;

        let create_entries = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                app_id text,
                channel text,
                stream_id text,
                serial bigint,
                published_at_ms bigint,
                message_id text,
                event_name text,
                operation_kind text,
                payload_bytes blob,
                payload_size_bytes bigint,
                PRIMARY KEY ((app_id, channel, stream_id), serial)
            ) WITH CLUSTERING ORDER BY (serial ASC)",
            self.tables.entries_fq()
        );
        self.session
            .query_unpaged(create_entries, ())
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to create ScyllaDB history entries table: {e}"
                ))
            })?;
        Ok(())
    }

    async fn load_stream_record(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Option<HistoryStreamRecord>> {
        let query = format!(
            "SELECT stream_id, next_serial, durable_state, durable_state_reason, durable_state_node_id, durable_state_changed_at_ms, retained_messages, retained_bytes, oldest_available_serial, newest_available_serial, oldest_available_published_at_ms, newest_available_published_at_ms FROM {} WHERE app_id = ? AND channel = ?",
            self.tables.streams_fq()
        );
        let rows = self
            .session
            .query_unpaged(query, (app_id, channel))
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to read ScyllaDB history stream row: {e}"))
            })?
            .into_rows_result()
            .map_err(|e| {
                Error::Internal(format!("Failed to decode ScyllaDB history stream row: {e}"))
            })?;
        let row = rows.maybe_first_row::<StreamRow>().map_err(|e| {
            Error::Internal(format!(
                "Failed to deserialize ScyllaDB history stream row: {e}"
            ))
        })?;
        Ok(row.map(|row| HistoryStreamRecord {
            stream_id: row.stream_id,
            next_serial: row.next_serial as u64,
            retained_messages: row.retained_messages as u64,
            retained_bytes: row.retained_bytes as u64,
            oldest_serial: row.oldest_available_serial.map(|value| value as u64),
            newest_serial: row.newest_available_serial.map(|value| value as u64),
            oldest_published_at_ms: row.oldest_available_published_at_ms,
            newest_published_at_ms: row.newest_available_published_at_ms,
            durable_state: parse_history_durable_state(&row.durable_state),
            durable_state_reason: row.durable_state_reason,
            durable_state_node_id: row.durable_state_node_id,
            durable_state_changed_at_ms: row.durable_state_changed_at_ms,
        }))
    }

    async fn retained_stats(&self, app_id: &str, channel: &str) -> Result<HistoryRetentionStats> {
        Ok(self
            .load_stream_record(app_id, channel)
            .await?
            .map(|record| record.retention_stats())
            .unwrap_or_default())
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

    async fn load_entry_keys_for_stream(
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

    async fn load_history_items_for_stream(
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

    async fn delete_entries(
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

    async fn write_stream_record(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        next_serial: u64,
        durable_state: HistoryDurableState,
        durable_state_reason: Option<&str>,
        durable_state_node_id: Option<&str>,
        durable_state_changed_at_ms: Option<i64>,
        retained: &HistoryRetentionStats,
        updated_at_ms: i64,
    ) -> Result<()> {
        let query = format!(
            "INSERT INTO {} (app_id, channel, stream_id, next_serial, durable_state, durable_state_reason, durable_state_node_id, durable_state_changed_at_ms, retained_messages, retained_bytes, oldest_available_serial, newest_available_serial, oldest_available_published_at_ms, newest_available_published_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            self.tables.streams_fq()
        );
        self.session
            .query_unpaged(
                query,
                (
                    app_id,
                    channel,
                    stream_id,
                    next_serial as i64,
                    durable_state.as_str(),
                    durable_state_reason,
                    durable_state_node_id,
                    durable_state_changed_at_ms,
                    retained.retained_messages as i64,
                    retained.retained_bytes as i64,
                    retained.oldest_serial.map(|value| value as i64),
                    retained.newest_serial.map(|value| value as i64),
                    retained.oldest_published_at_ms,
                    retained.newest_published_at_ms,
                    updated_at_ms,
                ),
            )
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to write ScyllaDB history stream row: {e}"))
            })?;
        Ok(())
    }

    async fn update_stream_retention_from_entries(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        next_serial: u64,
        durable_state: HistoryDurableState,
        durable_state_reason: Option<&str>,
        durable_state_node_id: Option<&str>,
        durable_state_changed_at_ms: Option<i64>,
        updated_at_ms: i64,
    ) -> Result<HistoryRetentionStats> {
        let rows = self
            .load_entry_keys_for_stream(app_id, channel, stream_id)
            .await?;
        let retained = HistoryRetentionStats {
            stream_id: Some(stream_id.to_string()),
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
        self.write_stream_record(
            app_id,
            channel,
            stream_id,
            next_serial,
            durable_state,
            durable_state_reason,
            durable_state_node_id,
            durable_state_changed_at_ms,
            &retained,
            updated_at_ms,
        )
        .await?;
        Ok(retained)
    }

    async fn persist_record(&self, record: &HistoryAppendRecord) -> Result<()> {
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
            .update_stream_retention_from_entries(
                &record.app_id,
                &record.channel,
                &record.stream_id,
                current.next_serial.max(record.serial.saturating_add(1)),
                current.durable_state,
                current.durable_state_reason.as_deref(),
                current.durable_state_node_id.as_deref(),
                current.durable_state_changed_at_ms,
                record.published_at_ms,
            )
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
                &record.app_id,
                &record.channel,
                "durable_history_write_failed",
                None,
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
        self.write_stream_record(
            app_id,
            channel,
            &new_stream_id,
            1,
            HistoryDurableState::Healthy,
            None,
            None,
            Some(now_ms),
            &retained,
            now_ms,
        )
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
            previous_stream_id = ?previous_stream_id,
            new_stream_id = %new_stream_id,
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
                    .update_stream_retention_from_entries(
                        app_id,
                        channel,
                        stream_id,
                        stream.next_serial,
                        stream.durable_state,
                        stream.durable_state_reason.as_deref(),
                        stream.durable_state_node_id.as_deref(),
                        stream.durable_state_changed_at_ms,
                        sockudo_core::history::now_ms(),
                    )
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

fn parse_history_durable_state(raw: &str) -> HistoryDurableState {
    match raw {
        "healthy" => HistoryDurableState::Healthy,
        "reset_required" => HistoryDurableState::ResetRequired,
        "degraded" => HistoryDurableState::Degraded,
        _ => HistoryDurableState::Degraded,
    }
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

fn lwt_applied(result: scylla::response::query_result::QueryResult) -> Result<bool> {
    let rows = result
        .into_rows_result()
        .map_err(|e| Error::Internal(format!("Failed to decode ScyllaDB LWT result: {e}")))?;
    match rows.column_specs().len() {
        1 => rows
            .single_row::<LwtApplyOnlyRow>()
            .map(|row| row.0)
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB LWT apply-only row: {e}"
                ))
            }),
        3 => rows
            .single_row::<LwtConditionalRow>()
            .map(|row| row.0)
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB LWT conditional row: {e}"
                ))
            }),
        16 => rows
            .single_row::<LwtStreamRow>()
            .map(|row| row.0)
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB LWT stream row: {e}"
                ))
            }),
        columns => Err(Error::Internal(format!(
            "Unexpected ScyllaDB LWT result shape with {columns} columns"
        ))),
    }
}

fn map_scylla_lwt_error(operation: &str, error: impl std::fmt::Display) -> Error {
    let error_text = error.to_string();
    if error_text.contains("not yet supported with tablets") {
        return Error::Configuration(format!(
            "ScyllaDB history cannot {operation}: the keyspace uses tablets, but this backend requires tablets disabled for LWT-based serial reservation"
        ));
    }
    Error::Internal(format!(
        "Failed to {operation} in ScyllaDB history: {error_text}"
    ))
}

async fn mark_channel_degraded(
    session: &Session,
    tables: &HistoryTables,
    degraded_channels: &DashMap<String, HistoryDegradedState>,
    cache_manager: Option<&Arc<dyn CacheManager + Send + Sync>>,
    metrics: Option<&(dyn MetricsInterface + Send + Sync)>,
    app_id: &str,
    channel: &str,
    reason: &str,
    node_id: Option<String>,
) {
    let now_ms = sockudo_core::history::now_ms();
    let state = HistoryDegradedState {
        app_id: app_id.to_string(),
        channel: channel.to_string(),
        durable_state: HistoryDurableState::Degraded,
        reason: reason.to_string(),
        node_id,
        last_transition_at_ms: now_ms,
        observed_source: "local_memory_hint",
    };
    degraded_channels.insert(degraded_channel_key(app_id, channel), state.clone());
    if let Some(cache) = cache_manager {
        let _ = cache
            .set(
                &degraded_cache_key(app_id, channel),
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

    if let Ok(Some(current)) = {
        let query = format!(
            "SELECT stream_id, next_serial, durable_state, durable_state_reason, durable_state_node_id, durable_state_changed_at_ms, retained_messages, retained_bytes, oldest_available_serial, newest_available_serial, oldest_available_published_at_ms, newest_available_published_at_ms FROM {} WHERE app_id = ? AND channel = ?",
            tables.streams_fq()
        );
        async {
            session
                .query_unpaged(query, (app_id, channel))
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to read ScyllaDB stream before degrade: {e}"
                    ))
                })?
                .into_rows_result()
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to decode ScyllaDB stream before degrade: {e}"
                    ))
                })?
                .maybe_first_row::<StreamRow>()
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to deserialize ScyllaDB stream before degrade: {e}"
                    ))
                })
                .map(|row| {
                    row.map(|row| HistoryStreamRecord {
                        stream_id: row.stream_id,
                        next_serial: row.next_serial as u64,
                        retained_messages: row.retained_messages as u64,
                        retained_bytes: row.retained_bytes as u64,
                        oldest_serial: row.oldest_available_serial.map(|value| value as u64),
                        newest_serial: row.newest_available_serial.map(|value| value as u64),
                        oldest_published_at_ms: row.oldest_available_published_at_ms,
                        newest_published_at_ms: row.newest_available_published_at_ms,
                        durable_state: parse_history_durable_state(&row.durable_state),
                        durable_state_reason: row.durable_state_reason,
                        durable_state_node_id: row.durable_state_node_id,
                        durable_state_changed_at_ms: row.durable_state_changed_at_ms,
                    })
                })
        }
        .await
    } {
        let retained = current.retention_stats();
        let query = format!(
            "INSERT INTO {} (app_id, channel, stream_id, next_serial, durable_state, durable_state_reason, durable_state_node_id, durable_state_changed_at_ms, retained_messages, retained_bytes, oldest_available_serial, newest_available_serial, oldest_available_published_at_ms, newest_available_published_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tables.streams_fq()
        );
        if let Err(err) = session
            .query_unpaged(
                query,
                (
                    app_id,
                    channel,
                    current.stream_id.as_str(),
                    current.next_serial as i64,
                    state.durable_state.as_str(),
                    state.reason.as_str(),
                    state.node_id.as_deref(),
                    Some(state.last_transition_at_ms),
                    retained.retained_messages as i64,
                    retained.retained_bytes as i64,
                    retained.oldest_serial.map(|value| value as i64),
                    retained.newest_serial.map(|value| value as i64),
                    retained.oldest_published_at_ms,
                    retained.newest_published_at_ms,
                    now_ms,
                ),
            )
            .await
        {
            error!(app_id = %app_id, channel = %channel, "Failed to persist ScyllaDB history degraded state: {err}");
        }
    }
    if let Some(metrics) = metrics {
        let _ = refresh_history_state_metrics(session, tables, metrics, app_id).await;
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
                "Failed to parse degraded ScyllaDB history state: {e}"
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
    session: &Session,
    tables: &HistoryTables,
    metrics: &(dyn MetricsInterface + Send + Sync),
    app_id: &str,
) -> Result<()> {
    let query = format!(
        "SELECT durable_state FROM {} WHERE app_id = ?",
        tables.streams_fq()
    );
    let pager = session
        .query_iter(query, (app_id,))
        .await
        .map_err(|e| Error::Internal(format!("Failed to stream ScyllaDB history metrics: {e}")))?;
    let mut rows_stream = pager
        .rows_stream::<DurableStateRow>()
        .map_err(|e| Error::Internal(format!("Failed to decode ScyllaDB history metrics: {e}")))?;
    let mut degraded = 0usize;
    let mut reset_required = 0usize;
    while let Some(row) = rows_stream
        .try_next()
        .await
        .map_err(|e| Error::Internal(format!("Failed to read ScyllaDB history metrics: {e}")))?
    {
        let state = parse_history_durable_state(&row.durable_state);
        if state != HistoryDurableState::Healthy {
            degraded += 1;
        }
        if state == HistoryDurableState::ResetRequired {
            reset_required += 1;
        }
    }
    metrics.update_history_degraded_channels(app_id, degraded);
    metrics.update_history_reset_required_channels(app_id, reset_required);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::history_conformance::HistoryStoreConformance;

    async fn is_scylla_available() -> bool {
        let session = SessionBuilder::new()
            .known_nodes(["127.0.0.1:19042"])
            .build()
            .await;
        let Ok(session) = session else {
            return false;
        };
        session
            .query_unpaged("SELECT cluster_name FROM system.local", ())
            .await
            .is_ok()
    }

    async fn build_store() -> Arc<dyn HistoryStore + Send + Sync> {
        let db = ScyllaDbSettings {
            nodes: vec!["127.0.0.1:19042".to_string()],
            keyspace: format!("sockudo_history_test_{}", uuid::Uuid::new_v4().simple()),
            username: None,
            password: None,
            table_name: "applications".to_string(),
            replication_class: "SimpleStrategy".to_string(),
            replication_factor: 1,
        };
        let mut config = HistoryConfig::default();
        config.enabled = true;
        config.backend = sockudo_core::options::HistoryBackend::ScyllaDb;
        config.scylladb.table_prefix = format!("sockudo_history_{}", uuid::Uuid::new_v4().simple());
        create_scylla_history_store(&db, config, None, None)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn scylla_history_store_conformance_serial_and_stream_continuity() {
        if !is_scylla_available().await {
            eprintln!("Skipping test: ScyllaDB not available");
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
    async fn scylla_history_store_conformance_pagination_and_reset_semantics() {
        if !is_scylla_available().await {
            eprintln!("Skipping test: ScyllaDB not available");
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
}
