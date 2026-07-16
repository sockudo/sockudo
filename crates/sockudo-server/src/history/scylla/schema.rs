use sockudo_core::error::{Error, Result};

use super::ScyllaHistoryStore;

impl ScyllaHistoryStore {
    pub(super) async fn ensure_schema(&self) -> Result<()> {
        super::ensure_scylla_keyspace(
            &self.session,
            &self.tables.keyspace,
            &self.tables.replication_class,
            self.tables.replication_factor,
        )
        .await?;

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
        let create_version_streams = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                app_id text,
                channel text,
                next_delivery_serial bigint,
                oldest_available_delivery_serial bigint,
                newest_available_delivery_serial bigint,
                migration_state text,
                migration_state_changed_at_ms bigint,
                updated_at_ms bigint,
                PRIMARY KEY ((app_id), channel)
            )",
            self.tables.version_streams_fq()
        );
        self.session
            .query_unpaged(create_version_streams, ())
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to create ScyllaDB version streams table: {e}"
                ))
            })?;
        let create_version_messages = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                app_id text,
                channel text,
                message_serial text,
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
        );
        self.session
            .query_unpaged(create_version_messages, ())
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to create ScyllaDB version messages table: {e}"
                ))
            })?;
        let create_version_entries_by_message = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                app_id text,
                channel text,
                message_serial text,
                version_serial text,
                delivery_serial bigint,
                history_serial bigint,
                action text,
                client_id text,
                description text,
                operation_metadata text,
                event_name text,
                payload_bytes blob,
                payload_size_bytes bigint,
                version_timestamp_ms bigint,
                created_at_ms bigint,
                PRIMARY KEY ((app_id, channel, message_serial), version_serial)
            ) WITH CLUSTERING ORDER BY (version_serial DESC)",
            self.tables.version_entries_by_message_fq()
        );
        self.session
            .query_unpaged(create_version_entries_by_message, ())
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to create ScyllaDB version entries-by-message table: {e}"
                ))
            })?;
        let create_version_entries_by_delivery = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                app_id text,
                channel text,
                delivery_serial bigint,
                message_serial text,
                version_serial text,
                history_serial bigint,
                action text,
                client_id text,
                description text,
                operation_metadata text,
                event_name text,
                payload_bytes blob,
                payload_size_bytes bigint,
                version_timestamp_ms bigint,
                created_at_ms bigint,
                PRIMARY KEY ((app_id, channel), delivery_serial)
            ) WITH CLUSTERING ORDER BY (delivery_serial ASC)",
            self.tables.version_entries_by_delivery_fq()
        );
        self.session
            .query_unpaged(create_version_entries_by_delivery, ())
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to create ScyllaDB version entries-by-delivery table: {e}"
                ))
            })?;
        Ok(())
    }
}
