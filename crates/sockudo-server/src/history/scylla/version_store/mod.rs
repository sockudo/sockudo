mod store_impl;

use super::state::*;
use super::*;

// ── ScyllaDB VersionStore ─────────────────────────────────────────────────────

#[cfg(feature = "versioned-messages")]
use sockudo_core::version_store::{
    StoredVersionRecord, VersionCreateRejection, VersionCreateRequest, VersionCreateResult,
    VersionMutationRejection, VersionMutationRequest, VersionMutationResult, VersionReplayRequest,
    VersionStore, VersionStoreCursor, VersionStoreDirection, VersionStorePage,
    VersionStoreReadRequest, VersionStreamState, VersionWriteReservation,
    VersionWriteReservationBlock,
};

// LWT result types for version_streams table.
// INSERT IF NOT EXISTS returns: (applied, app_id, channel, next_delivery_serial,
//   oldest_available_delivery_serial, newest_available_delivery_serial,
//   migration_state, migration_state_changed_at_ms, updated_at_ms) = 9 columns on conflict.
#[cfg(feature = "versioned-messages")]
type VersionStreamInsertLwtRow = (
    bool,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<i64>,
);
// UPDATE ... IF next_delivery_serial = ? returns: (applied, next_delivery_serial) = 2 columns on failure.
#[cfg(feature = "versioned-messages")]
type VersionStreamUpdateLwtRow = (bool, Option<i64>);

#[cfg(feature = "versioned-messages")]
fn version_lwt_applied(result: scylla::response::query_result::QueryResult) -> Result<bool> {
    let rows = result.into_rows_result().map_err(|e| {
        Error::Internal(format!("Failed to decode ScyllaDB version LWT result: {e}"))
    })?;
    match rows.column_specs().len() {
        1 => rows
            .single_row::<LwtApplyOnlyRow>()
            .map(|r| r.0)
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB version LWT row (1 col): {e}"
                ))
            }),
        2 => rows
            .single_row::<VersionStreamUpdateLwtRow>()
            .map(|r| r.0)
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB version LWT row (2 col): {e}"
                ))
            }),
        9 => rows
            .single_row::<VersionStreamInsertLwtRow>()
            .map(|r| r.0)
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB version LWT row (9 col): {e}"
                ))
            }),
        n => Err(Error::Internal(format!(
            "Unexpected ScyllaDB version LWT result shape with {n} columns"
        ))),
    }
}

#[cfg(feature = "versioned-messages")]
fn version_batch_applied(result: scylla::response::query_result::QueryResult) -> Result<bool> {
    use scylla::deserialize::row::ColumnIterator;

    let rows = result.into_rows_result().map_err(|e| {
        Error::Internal(format!(
            "Failed to decode ScyllaDB version batch result: {e}"
        ))
    })?;
    let mut columns = rows
        .single_row::<ColumnIterator>()
        .map_err(|e| Error::Internal(format!("Failed to deserialize version batch result: {e}")))?;
    let applied = columns
        .next()
        .transpose()
        .map_err(|e| Error::Internal(format!("Failed to read version batch result: {e}")))?
        .and_then(|column| column.slice)
        .is_some_and(|slice| slice.as_slice() == [1]);
    Ok(applied)
}

#[cfg(feature = "versioned-messages")]
fn message_commit_key(message_serial: &str) -> String {
    format!("m:{message_serial}")
}

#[cfg(feature = "versioned-messages")]
fn version_commit_key(message_serial: &str, version_serial: &str) -> String {
    format!("v:{message_serial}:{version_serial}")
}

#[cfg(feature = "versioned-messages")]
fn delivery_commit_key(delivery_serial: u64) -> String {
    format!("d:{delivery_serial:020}")
}

#[cfg(feature = "versioned-messages")]
fn operation_commit_key(operation_key: &str) -> String {
    format!("o:{operation_key}")
}

#[cfg(feature = "versioned-messages")]
pub struct ScyllaVersionStore {
    session: Arc<Session>,
    tables: HistoryTables,
    retention_seconds: u64,
}

#[cfg(feature = "versioned-messages")]
pub async fn create_scylla_version_store(
    db_config: &ScyllaDbSettings,
    table_prefix: &str,
    retention_seconds: u64,
) -> Result<std::sync::Arc<dyn VersionStore + Send + Sync>> {
    let store = ScyllaVersionStore::new(db_config, table_prefix, retention_seconds).await?;
    Ok(std::sync::Arc::new(store))
}

#[cfg(feature = "versioned-messages")]
impl ScyllaVersionStore {
    /// `USING TTL` suffix appended to INSERTs (before semicolon, after
    /// `IF NOT EXISTS` if present). Empty when retention is disabled.
    fn ttl_suffix(&self) -> String {
        if self.retention_seconds > 0 {
            format!(" USING TTL {}", self.retention_seconds)
        } else {
            String::new()
        }
    }

    /// `USING TTL` clause placed between the table name and `SET` for
    /// UPDATEs. Includes trailing space when present.
    fn update_ttl_clause(&self) -> String {
        if self.retention_seconds > 0 {
            format!("USING TTL {} ", self.retention_seconds)
        } else {
            String::new()
        }
    }

    async fn new(
        db_config: &ScyllaDbSettings,
        table_prefix: &str,
        retention_seconds: u64,
    ) -> Result<Self> {
        let mut builder = SessionBuilder::new().known_nodes(db_config.nodes.clone());
        if let (Some(username), Some(password)) = (&db_config.username, &db_config.password) {
            builder = builder.user(username, password);
        }
        let session = builder.build().await.map_err(|e| {
            Error::Internal(format!("Failed to connect version store to ScyllaDB: {e}"))
        })?;
        let session = Arc::new(session);
        let keyspace = if db_config.keyspace.trim().is_empty() {
            "sockudo".to_string()
        } else {
            db_config.keyspace.clone()
        };
        let tables = HistoryTables {
            keyspace,
            streams: format!("{}_streams", table_prefix),
            entries: format!("{}_entries", table_prefix),
            version_streams: format!("{}_version_streams", table_prefix),
            version_messages: format!("{}_version_messages", table_prefix),
            version_entries_by_message: format!("{}_version_entries_by_message", table_prefix),
            version_entries_by_delivery: format!("{}_version_entries_by_delivery", table_prefix),
            version_commits: format!("{}_version_commits", table_prefix),
        };
        let store = Self {
            session,
            tables,
            retention_seconds,
        };
        store.ensure_version_tables().await?;
        Ok(store)
    }

    async fn ensure_version_tables(&self) -> Result<()> {
        let create_ks = format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}} AND tablets = {{'enabled': false}}",
            self.tables.keyspace
        );
        self.session
            .query_unpaged(create_ks, ())
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to create ScyllaDB keyspace for version store: {e}"
                ))
            })?;

        let stmts = [
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
                    app_id text, channel text,
                    next_delivery_serial bigint,
                    oldest_available_delivery_serial bigint,
                    newest_available_delivery_serial bigint,
                    migration_state text,
                    migration_state_changed_at_ms bigint,
                    updated_at_ms bigint,
                    PRIMARY KEY ((app_id), channel)
                )",
                self.tables.version_streams_fq()
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
                    app_id text, channel text, message_serial text,
                    history_serial bigint,
                    original_client_id text,
                    latest_version_serial text,
                    latest_delivery_serial bigint,
                    latest_action text,
                    created_at_ms bigint,
                    updated_at_ms bigint,
                    PRIMARY KEY ((app_id, channel), message_serial)
                )",
                self.tables.version_messages_fq()
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
                    app_id text, channel text, message_serial text,
                    version_serial text,
                    delivery_serial bigint,
                    history_serial bigint,
                    action text, client_id text, description text,
                    operation_metadata text, event_name text,
                    payload_bytes blob,
                    payload_size_bytes bigint,
                    version_timestamp_ms bigint,
                    created_at_ms bigint,
                    PRIMARY KEY ((app_id, channel, message_serial), version_serial)
                ) WITH CLUSTERING ORDER BY (version_serial DESC)",
                self.tables.version_entries_by_message_fq()
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
                    app_id text, channel text,
                    delivery_serial bigint,
                    message_serial text,
                    version_serial text,
                    history_serial bigint,
                    action text, client_id text, description text,
                    operation_metadata text, event_name text,
                    payload_bytes blob,
                    payload_size_bytes bigint,
                    version_timestamp_ms bigint,
                    created_at_ms bigint,
                    PRIMARY KEY ((app_id, channel), delivery_serial)
                ) WITH CLUSTERING ORDER BY (delivery_serial ASC)",
                self.tables.version_entries_by_delivery_fq()
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
                    app_id text, channel text, commit_key text,
                    payload_bytes blob,
                    latest_version_serial text,
                    latest_delivery_serial bigint,
                    history_serial bigint,
                    action text,
                    append_count bigint,
                    is_open_stream boolean,
                    next_delivery_serial bigint,
                    open_stream_count bigint,
                    created_at_ms bigint,
                    PRIMARY KEY ((app_id, channel), commit_key)
                ) WITH CLUSTERING ORDER BY (commit_key ASC)",
                self.tables.version_commits_fq()
            ),
        ];

        for stmt in &stmts {
            self.session
                .query_unpaged(stmt.as_str(), ())
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to create ScyllaDB version table: {e}"))
                })?;
        }
        Ok(())
    }
}
