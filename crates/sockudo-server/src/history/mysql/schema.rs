use sockudo_core::error::{Error, Result};
use sockudo_core::versioned_messages::MAX_VERSIONED_SERIAL_LENGTH;

use super::MySqlHistoryStore;

impl MySqlHistoryStore {
    async fn add_column_if_not_exists(
        &self,
        table_name: &str,
        column_name: &str,
        column_type: &str,
    ) -> Result<()> {
        let check_query = format!(
            r#"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_SCHEMA = DATABASE()
               AND TABLE_NAME = '{}'
               AND COLUMN_NAME = '{}'"#,
            table_name, column_name
        );
        let exists: Option<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(check_query.as_str()))
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to inspect MySQL history column {column_name}: {e}"
                ))
            })?;
        if exists.is_none() {
            let alter_query = format!(
                "ALTER TABLE {} ADD COLUMN {} {}",
                table_name, column_name, column_type
            );
            sqlx::query(sqlx::AssertSqlSafe(alter_query.as_str()))
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to add MySQL history column {column_name}: {e}"
                    ))
                })?;
        }
        Ok(())
    }

    pub(super) async fn ensure_tables(&self) -> Result<()> {
        let create_streams = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id VARCHAR(255) NOT NULL,
                channel VARCHAR(255) NOT NULL,
                stream_id VARCHAR(255) NOT NULL,
                next_serial BIGINT NOT NULL,
                durable_state VARCHAR(32) NOT NULL DEFAULT 'healthy',
                durable_state_reason TEXT NULL,
                durable_state_node_id VARCHAR(255) NULL,
                durable_state_changed_at_ms BIGINT NULL,
                retained_messages BIGINT NOT NULL DEFAULT 0,
                retained_bytes BIGINT NOT NULL DEFAULT 0,
                oldest_available_serial BIGINT NULL,
                newest_available_serial BIGINT NULL,
                oldest_available_published_at_ms BIGINT NULL,
                newest_available_published_at_ms BIGINT NULL,
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            "#,
            self.tables.streams
        );
        let create_entries = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id VARCHAR(255) NOT NULL,
                channel VARCHAR(255) NOT NULL,
                stream_id VARCHAR(255) NOT NULL,
                serial BIGINT NOT NULL,
                published_at_ms BIGINT NOT NULL,
                message_id VARCHAR(255) NULL,
                event_name VARCHAR(255) NULL,
                operation_kind VARCHAR(64) NOT NULL,
                payload_bytes LONGBLOB NOT NULL,
                payload_size_bytes BIGINT NOT NULL,
                metadata JSON NULL,
                tombstone BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (app_id, channel, stream_id, serial)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            "#,
            self.tables.entries
        );
        let create_version_streams = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id VARCHAR(255) NOT NULL,
                channel VARCHAR(255) NOT NULL,
                next_delivery_serial BIGINT NOT NULL,
                oldest_available_delivery_serial BIGINT NULL,
                newest_available_delivery_serial BIGINT NULL,
                migration_state VARCHAR(32) NOT NULL DEFAULT 'native_only',
                migration_state_changed_at_ms BIGINT NULL,
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            "#,
            self.tables.version_streams
        );
        let create_version_messages = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id VARCHAR(255) NOT NULL,
                channel VARCHAR(255) NOT NULL,
                message_serial VARCHAR({}) NOT NULL,
                history_serial BIGINT NOT NULL,
                original_client_id VARCHAR(255) NULL,
                latest_version_serial VARCHAR({}) NOT NULL,
                latest_delivery_serial BIGINT NOT NULL,
                latest_action VARCHAR(64) NOT NULL,
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel, message_serial),
                UNIQUE KEY {0}_history_serial_uidx (app_id, channel, history_serial)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            "#,
            self.tables.version_messages, MAX_VERSIONED_SERIAL_LENGTH, MAX_VERSIONED_SERIAL_LENGTH
        );
        let create_version_entries = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id VARCHAR(255) NOT NULL,
                channel VARCHAR(255) NOT NULL,
                message_serial VARCHAR({}) NOT NULL,
                version_serial VARCHAR({}) NOT NULL,
                delivery_serial BIGINT NOT NULL,
                history_serial BIGINT NOT NULL,
                action VARCHAR(64) NOT NULL,
                client_id VARCHAR(255) NULL,
                description TEXT NULL,
                operation_metadata JSON NULL,
                event_name VARCHAR(255) NULL,
                payload_bytes LONGBLOB NOT NULL,
                payload_size_bytes BIGINT NOT NULL,
                version_timestamp_ms BIGINT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel, message_serial, version_serial),
                UNIQUE KEY {0}_delivery_uidx (app_id, channel, delivery_serial)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            "#,
            self.tables.version_entries, MAX_VERSIONED_SERIAL_LENGTH, MAX_VERSIONED_SERIAL_LENGTH
        );
        let create_annotation_events = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id VARCHAR(255) NOT NULL,
                channel VARCHAR(255) NOT NULL,
                message_serial VARCHAR({}) NOT NULL,
                annotation_serial VARCHAR({}) NOT NULL,
                annotation_type VARCHAR(256) NOT NULL,
                name VARCHAR(255) NULL,
                client_id VARCHAR(255) NULL,
                count_value BIGINT NULL,
                action VARCHAR(64) NOT NULL,
                data_bytes LONGBLOB NULL,
                encoding VARCHAR(255) NULL,
                annotation_timestamp_ms BIGINT NOT NULL,
                payload_bytes LONGBLOB NOT NULL,
                payload_size_bytes BIGINT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel, message_serial, annotation_serial)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            "#,
            self.tables.annotation_events, MAX_VERSIONED_SERIAL_LENGTH, MAX_VERSIONED_SERIAL_LENGTH
        );
        let create_annotation_projections = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                app_id VARCHAR(255) NOT NULL,
                channel VARCHAR(255) NOT NULL,
                message_serial VARCHAR({}) NOT NULL,
                annotation_type VARCHAR(256) NOT NULL,
                summary_json JSON NOT NULL,
                last_annotation_serial VARCHAR({}) NULL,
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel, message_serial, annotation_type)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            "#,
            self.tables.annotation_projections,
            MAX_VERSIONED_SERIAL_LENGTH,
            MAX_VERSIONED_SERIAL_LENGTH
        );
        let index_serial = format!(
            "CREATE INDEX {0}_app_channel_serial_idx ON {0} (app_id, channel, serial DESC)",
            self.tables.entries
        );
        let index_time = format!(
            "CREATE INDEX {0}_app_channel_time_idx ON {0} (app_id, channel, published_at_ms DESC, serial DESC)",
            self.tables.entries
        );
        let index_version_stream_window = format!(
            "CREATE INDEX {0}_delivery_window_idx ON {0} (app_id, oldest_available_delivery_serial, newest_available_delivery_serial)",
            self.tables.version_streams
        );
        let index_version_messages_latest = format!(
            "CREATE INDEX {0}_latest_version_idx ON {0} (app_id, channel, latest_version_serial)",
            self.tables.version_messages
        );
        let index_version_entries_message = format!(
            "CREATE INDEX {0}_message_version_idx ON {0} (app_id, channel, message_serial, version_serial DESC)",
            self.tables.version_entries
        );
        let index_version_entries_replay = format!(
            "CREATE INDEX {0}_replay_idx ON {0} (app_id, channel, delivery_serial)",
            self.tables.version_entries
        );
        let index_version_entries_history = format!(
            "CREATE INDEX {0}_history_version_idx ON {0} (app_id, channel, history_serial, version_serial DESC)",
            self.tables.version_entries
        );
        let index_annotation_events_summary = format!(
            "CREATE INDEX {0}_summary_idx ON {0} (app_id, channel, message_serial, annotation_type, annotation_serial)",
            self.tables.annotation_events
        );
        let index_annotation_events_dedup = format!(
            "CREATE INDEX {0}_dedup_idx ON {0} (app_id, channel, message_serial, annotation_serial)",
            self.tables.annotation_events
        );
        let index_annotation_events_raw_replay = format!(
            "CREATE INDEX {0}_raw_replay_idx ON {0} (app_id, channel, annotation_serial)",
            self.tables.annotation_events
        );
        let index_annotation_events_created_at = format!(
            "CREATE INDEX {0}_created_at_idx ON {0} (created_at_ms)",
            self.tables.annotation_events
        );
        for sql in [
            create_streams,
            create_entries,
            create_version_streams,
            create_version_messages,
            create_version_entries,
            create_annotation_events,
            create_annotation_projections,
        ] {
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to initialize MySQL history tables: {e}"))
                })?;
        }

        self.add_column_if_not_exists(
            &self.tables.streams,
            "oldest_available_published_at_ms",
            "BIGINT NULL",
        )
        .await?;
        self.add_column_if_not_exists(
            &self.tables.streams,
            "newest_available_published_at_ms",
            "BIGINT NULL",
        )
        .await?;
        self.add_column_if_not_exists(
            &self.tables.streams,
            "durable_state",
            "VARCHAR(32) NOT NULL DEFAULT 'healthy'",
        )
        .await?;
        self.add_column_if_not_exists(&self.tables.streams, "durable_state_reason", "TEXT NULL")
            .await?;
        self.add_column_if_not_exists(
            &self.tables.streams,
            "durable_state_node_id",
            "VARCHAR(255) NULL",
        )
        .await?;
        self.add_column_if_not_exists(
            &self.tables.streams,
            "durable_state_changed_at_ms",
            "BIGINT NULL",
        )
        .await?;

        for sql in [
            index_serial,
            index_time,
            index_version_stream_window,
            index_version_messages_latest,
            index_version_entries_message,
            index_version_entries_replay,
            index_version_entries_history,
            index_annotation_events_summary,
            index_annotation_events_dedup,
            index_annotation_events_raw_replay,
            index_annotation_events_created_at,
        ] {
            let _ = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&self.pool)
                .await;
        }

        Ok(())
    }
}
