use sockudo_core::error::{Error, Result};

use super::SurrealHistoryStore;

impl SurrealHistoryStore {
    pub(super) async fn ensure_schema(&self) -> Result<()> {
        let query = format!(
            "DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
             DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
             DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
             DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
             DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS app_id;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS app_id, channel, stream_id, serial;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS app_id, channel, stream_id, published_at_ms;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS app_id;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS app_id, channel, message_serial;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS app_id, channel, history_serial;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS app_id, channel, message_serial, version_serial;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS app_id, channel, delivery_serial;",
            self.tables.streams,
            self.tables.entries,
            self.tables.version_streams,
            self.tables.version_messages,
            self.tables.version_entries,
            self.tables.streams_app_idx,
            self.tables.streams,
            self.tables.entries_stream_serial_idx,
            self.tables.entries,
            self.tables.entries_stream_time_idx,
            self.tables.entries,
            self.tables.version_streams_app_idx,
            self.tables.version_streams,
            self.tables.version_messages_message_idx,
            self.tables.version_messages,
            self.tables.version_messages_history_idx,
            self.tables.version_messages,
            self.tables.version_entries_message_idx,
            self.tables.version_entries,
            self.tables.version_entries_delivery_idx,
            self.tables.version_entries,
        );
        self.db.query(query).await.map_err(|e| {
            Error::Internal(format!(
                "Failed to initialize SurrealDB history schema: {e}"
            ))
        })?;
        Ok(())
    }
}
