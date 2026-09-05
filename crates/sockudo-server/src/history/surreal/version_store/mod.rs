mod encoding;
use encoding::VersionDeletedRow;
mod store_impl;

use super::*;

// =================== SurrealVersionStore ===================

#[cfg(feature = "versioned-messages")]
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct StoredVersionStreamRec {
    app_id: String,
    channel: String,
    stream_id: String,
    next_delivery_serial: i64,
    oldest_delivery_serial: Option<i64>,
    newest_delivery_serial: Option<i64>,
    #[serde(default)]
    open_stream_count: i64,
    updated_at_ms: i64,
}

#[cfg(feature = "versioned-messages")]
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct StoredVersionMessageRec {
    app_id: String,
    channel: String,
    message_serial: String,
    latest_version_serial: String,
    #[serde(default)]
    state_version_serial: Option<String>,
    latest_entry_key: String,
    history_serial: i64,
    #[serde(default)]
    latest_payload_bytes: Vec<u8>,
    #[serde(default)]
    append_count: i64,
    #[serde(default)]
    is_open_stream: bool,
    updated_at_ms: i64,
}

#[cfg(feature = "versioned-messages")]
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct StoredVersionEntryRec {
    app_id: String,
    channel: String,
    message_serial: String,
    version_serial: String,
    delivery_serial: i64,
    payload_bytes: Vec<u8>,
    #[serde(default)]
    text_snapshot_key: Option<String>,
    // Server-side append time (ms since epoch). Indexed for the purge worker
    // which deletes rows older than the retention cutoff in batched queries.
    created_at_ms: i64,
}

#[cfg(feature = "versioned-messages")]
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct StoredVersionReceiptRec {
    operation_fingerprint: String,
    payload_bytes: Vec<u8>,
    created_at_ms: i64,
}

#[cfg(feature = "versioned-messages")]
#[derive(Debug, Clone, Deserialize, SurrealValue)]
struct VersionLatestKeyRow {
    latest_entry_key: String,
}

#[cfg(feature = "versioned-messages")]
#[derive(Debug, Clone, Deserialize, SurrealValue)]
struct VersionPayloadRow {
    payload_bytes: Vec<u8>,
}

#[cfg(feature = "versioned-messages")]
#[derive(Clone)]
struct VersionStoreTables {
    streams: String,
    messages: String,
    entries: String,
    receipts: String,
    texts: String,
}

#[cfg(feature = "versioned-messages")]
pub struct SurrealVersionStore {
    db: Surreal<Any>,
    tables: VersionStoreTables,
}

#[cfg(feature = "versioned-messages")]
pub async fn create_surreal_version_store(
    db_config: &SurrealDbSettings,
    table_prefix: &str,
) -> Result<Arc<dyn VersionStore + Send + Sync>> {
    validate_identifier(
        &format!("{table_prefix}_version_streams"),
        "version streams table",
    )?;
    validate_identifier(
        &format!("{table_prefix}_version_receipts"),
        "version receipts table",
    )?;
    validate_identifier(
        &format!("{table_prefix}_version_messages"),
        "version messages table",
    )?;
    validate_identifier(
        &format!("{table_prefix}_version_entries"),
        "version entries table",
    )?;

    let db = connect(db_config.url.as_str()).await.map_err(|e| {
        Error::Internal(format!(
            "Failed to connect to SurrealDB for version store: {e}"
        ))
    })?;
    db.signin(Root {
        username: db_config.username.clone(),
        password: db_config.password.clone(),
    })
    .await
    .map_err(|e| {
        Error::Internal(format!(
            "Failed to authenticate SurrealDB version store: {e}"
        ))
    })?;
    db.use_ns(db_config.namespace.as_str())
        .use_db(db_config.database.as_str())
        .await
        .map_err(|e| {
            Error::Internal(format!(
                "Failed to select SurrealDB namespace for version store: {e}"
            ))
        })?;

    let tables = VersionStoreTables {
        streams: format!("{table_prefix}_version_streams"),
        messages: format!("{table_prefix}_version_messages"),
        entries: format!("{table_prefix}_version_entries"),
        receipts: format!("{table_prefix}_version_receipts"),
        texts: format!("{table_prefix}_version_texts"),
    };

    let query = format!(
        "DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
         DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
         DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
         DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
         DEFINE INDEX IF NOT EXISTS {}_app_idx ON TABLE {} FIELDS app_id;\
         DEFINE INDEX IF NOT EXISTS {}_message_idx ON TABLE {} FIELDS app_id, channel, message_serial;\
         DEFINE INDEX IF NOT EXISTS {}_history_idx ON TABLE {} FIELDS app_id, channel, history_serial;\
         DEFINE INDEX IF NOT EXISTS {}_updated_at_idx ON TABLE {} FIELDS updated_at_ms;\
         DEFINE INDEX IF NOT EXISTS {}_message_idx ON TABLE {} FIELDS app_id, channel, message_serial, version_serial;\
         DEFINE INDEX IF NOT EXISTS {}_delivery_idx ON TABLE {} FIELDS app_id, channel, delivery_serial;\
         DEFINE INDEX IF NOT EXISTS {}_created_at_idx ON TABLE {} FIELDS created_at_ms;",
        tables.streams,
        tables.messages,
        tables.entries,
        tables.receipts,
        tables.streams,
        tables.streams,
        tables.messages,
        tables.messages,
        tables.messages,
        tables.messages,
        tables.messages,
        tables.messages,
        tables.entries,
        tables.entries,
        tables.entries,
        tables.entries,
        tables.entries,
        tables.entries,
    );
    db.query(query).await.map_err(|e| {
        Error::Internal(format!(
            "Failed to initialize SurrealDB version store schema: {e}"
        ))
    })?;

    db.query(format!(
        "DEFINE TABLE IF NOT EXISTS {} SCHEMALESS; DEFINE INDEX IF NOT EXISTS {}_expiry ON TABLE {} FIELDS retain_for_receipts, updated_at_ms; DEFINE INDEX IF NOT EXISTS {}_text_ref ON TABLE {} FIELDS app_id, channel, text_snapshot_key;",
        tables.texts, tables.texts, tables.texts, tables.entries, tables.entries,
    )).await.map_err(|e| Error::Internal(format!("failed to initialize version text schema: {e}")))?
        .check().map_err(|e| Error::Internal(format!("failed to initialize version text schema: {e}")))?;
    Ok(Arc::new(SurrealVersionStore { db, tables }))
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct StoredVersionTextRec {
    app_id: String,
    channel: String,
    snapshot_key: String,
    text_data: String,
    updated_at_ms: i64,
    retain_for_receipts: bool,
}

#[cfg(test)]
mod tests;
