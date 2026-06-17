use sockudo_core::error::{Error, Result};

use super::history_store::PostgresHistoryStore;
use super::{lock_postgres_schema, unlock_postgres_schema};

impl PostgresHistoryStore {
    pub(super) async fn ensure_tables(&self) -> Result<()> {
        let create_streams = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                next_serial BIGINT NOT NULL,
                durable_state TEXT NOT NULL DEFAULT 'healthy',
                durable_state_reason TEXT NULL,
                durable_state_node_id TEXT NULL,
                durable_state_changed_at_ms BIGINT NULL,
                retained_messages BIGINT NOT NULL DEFAULT 0,
                retained_bytes BIGINT NOT NULL DEFAULT 0,
                oldest_available_serial BIGINT NULL,
                newest_available_serial BIGINT NULL,
                oldest_available_published_at_ms BIGINT NULL,
                newest_available_published_at_ms BIGINT NULL,
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel)
            )
            "#,
            self.tables.streams
        );
        let create_entries = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                serial BIGINT NOT NULL,
                published_at_ms BIGINT NOT NULL,
                message_id TEXT NULL,
                event_name TEXT NULL,
                operation_kind TEXT NOT NULL,
                payload_bytes BYTEA NOT NULL,
                payload_size_bytes BIGINT NOT NULL,
                metadata JSONB NULL,
                tombstone BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (app_id, channel, stream_id, serial)
            )
            "#,
            self.tables.entries
        );
        let index_serial = format!(
            "CREATE INDEX IF NOT EXISTS {0}_app_channel_serial_idx ON {0} (app_id, channel, serial DESC)",
            self.tables.entries
        );
        let index_time = format!(
            "CREATE INDEX IF NOT EXISTS {0}_app_channel_time_idx ON {0} (app_id, channel, published_at_ms DESC, serial DESC)",
            self.tables.entries
        );
        let create_version_streams = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                next_delivery_serial BIGINT NOT NULL,
                oldest_available_delivery_serial BIGINT NULL,
                newest_available_delivery_serial BIGINT NULL,
                migration_state TEXT NOT NULL DEFAULT 'native_only',
                migration_state_changed_at_ms BIGINT NULL,
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel)
            )
            "#,
            self.tables.version_streams
        );
        let create_version_messages = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                message_serial TEXT NOT NULL,
                history_serial BIGINT NOT NULL,
                original_client_id TEXT NULL,
                latest_version_serial TEXT NOT NULL,
                latest_delivery_serial BIGINT NOT NULL,
                latest_action TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel, message_serial)
            )
            "#,
            self.tables.version_messages
        );
        let create_version_entries = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                message_serial TEXT NOT NULL,
                version_serial TEXT NOT NULL,
                delivery_serial BIGINT NOT NULL,
                history_serial BIGINT NOT NULL,
                action TEXT NOT NULL,
                client_id TEXT NULL,
                description TEXT NULL,
                operation_metadata JSONB NULL,
                event_name TEXT NULL,
                payload_bytes BYTEA NOT NULL,
                payload_size_bytes BIGINT NOT NULL,
                version_timestamp_ms BIGINT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel, message_serial, version_serial)
            )
            "#,
            self.tables.version_entries
        );
        let create_annotation_events = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                message_serial TEXT NOT NULL,
                annotation_serial TEXT NOT NULL,
                annotation_type TEXT NOT NULL,
                name TEXT NULL,
                client_id TEXT NULL,
                count_value BIGINT NULL,
                action TEXT NOT NULL,
                data_bytes BYTEA NULL,
                encoding TEXT NULL,
                annotation_timestamp_ms BIGINT NOT NULL,
                payload_bytes BYTEA NOT NULL,
                payload_size_bytes BIGINT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel, message_serial, annotation_serial)
            )
            "#,
            self.tables.annotation_events
        );
        let create_annotation_projections = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                message_serial TEXT NOT NULL,
                annotation_type TEXT NOT NULL,
                summary_json JSONB NOT NULL,
                last_annotation_serial TEXT NULL,
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel, message_serial, annotation_type)
            )
            "#,
            self.tables.annotation_projections
        );
        let index_version_stream_window = format!(
            "CREATE INDEX IF NOT EXISTS {0}_delivery_window_idx ON {0} (app_id, oldest_available_delivery_serial, newest_available_delivery_serial)",
            self.tables.version_streams
        );
        let index_version_messages_history = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {0}_history_serial_uidx ON {0} (app_id, channel, history_serial)",
            self.tables.version_messages
        );
        let index_version_messages_latest = format!(
            "CREATE INDEX IF NOT EXISTS {0}_latest_version_idx ON {0} (app_id, channel, latest_version_serial)",
            self.tables.version_messages
        );
        let index_version_entries_delivery = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {0}_delivery_uidx ON {0} (app_id, channel, delivery_serial)",
            self.tables.version_entries
        );
        let index_version_entries_message = format!(
            "CREATE INDEX IF NOT EXISTS {0}_message_version_idx ON {0} (app_id, channel, message_serial, version_serial DESC)",
            self.tables.version_entries
        );
        let index_version_entries_replay = format!(
            "CREATE INDEX IF NOT EXISTS {0}_replay_idx ON {0} (app_id, channel, delivery_serial)",
            self.tables.version_entries
        );
        let index_version_entries_history = format!(
            "CREATE INDEX IF NOT EXISTS {0}_history_version_idx ON {0} (app_id, channel, history_serial, version_serial DESC)",
            self.tables.version_entries
        );
        let index_annotation_events_summary = format!(
            "CREATE INDEX IF NOT EXISTS {0}_summary_idx ON {0} (app_id, channel, message_serial, annotation_type, annotation_serial)",
            self.tables.annotation_events
        );
        let index_annotation_events_dedup = format!(
            "CREATE INDEX IF NOT EXISTS {0}_dedup_idx ON {0} (app_id, channel, message_serial, annotation_serial)",
            self.tables.annotation_events
        );
        let index_annotation_events_raw_replay = format!(
            "CREATE INDEX IF NOT EXISTS {0}_raw_replay_idx ON {0} (app_id, channel, annotation_serial)",
            self.tables.annotation_events
        );
        let index_annotation_events_created_at = format!(
            "CREATE INDEX IF NOT EXISTS {0}_created_at_idx ON {0} (created_at_ms)",
            self.tables.annotation_events
        );
        let add_oldest_time = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS oldest_available_published_at_ms BIGINT NULL",
            self.tables.streams
        );
        let add_newest_time = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS newest_available_published_at_ms BIGINT NULL",
            self.tables.streams
        );
        let add_durable_state = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS durable_state TEXT NOT NULL DEFAULT 'healthy'",
            self.tables.streams
        );
        let add_durable_reason = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS durable_state_reason TEXT NULL",
            self.tables.streams
        );
        let add_durable_node_id = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS durable_state_node_id TEXT NULL",
            self.tables.streams
        );
        let add_durable_changed_at = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS durable_state_changed_at_ms BIGINT NULL",
            self.tables.streams
        );

        let ddl = [
            create_streams,
            create_entries,
            index_serial,
            index_time,
            create_version_streams,
            create_version_messages,
            create_version_entries,
            create_annotation_events,
            create_annotation_projections,
            index_version_stream_window,
            index_version_messages_history,
            index_version_messages_latest,
            index_version_entries_delivery,
            index_version_entries_message,
            index_version_entries_replay,
            index_version_entries_history,
            index_annotation_events_summary,
            index_annotation_events_dedup,
            index_annotation_events_raw_replay,
            index_annotation_events_created_at,
            add_oldest_time,
            add_newest_time,
            add_durable_state,
            add_durable_reason,
            add_durable_node_id,
            add_durable_changed_at,
        ];

        let mut conn = self.pool.acquire().await.map_err(|e| {
            Error::Internal(format!(
                "Failed to acquire PostgreSQL schema initialization connection: {e}"
            ))
        })?;
        lock_postgres_schema(&mut conn, "sockudo_history_schema").await?;
        let result: Result<()> = async {
            for sql in ddl {
                sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("Failed to initialize history tables: {e}"))
                    })?;
            }
            Ok(())
        }
        .await;
        unlock_postgres_schema(&mut conn, "sockudo_history_schema").await?;
        result?;

        Ok(())
    }
}
