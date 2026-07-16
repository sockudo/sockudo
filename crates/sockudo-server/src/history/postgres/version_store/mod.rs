mod store_impl;

use sockudo_core::error::{Error, Result};
use sockudo_core::options::{DatabaseConnection, DatabasePooling};
use sockudo_core::version_store::{
    StoredVersionRecord, VersionCreateRejection, VersionCreateRequest, VersionCreateResult,
    VersionMutationRequest, VersionMutationResult, VersionReplayRequest, VersionStore,
    VersionStoreCursor, VersionStoreDirection, VersionStorePage, VersionStoreReadRequest,
    VersionStreamState, VersionWriteReservation, VersionWriteReservationBlock,
};
use std::time::Duration;

use sqlx::{PgPool, Row, postgres::PgPoolOptions};

use super::{HistoryTables, lock_postgres_schema, unlock_postgres_schema};

pub struct PostgresVersionStore {
    pool: PgPool,
    tables: HistoryTables,
}

impl PostgresVersionStore {
    pub(in crate::history) async fn new(
        db_config: &DatabaseConnection,
        pooling: &DatabasePooling,
        table_prefix: &str,
    ) -> Result<Self> {
        let password = urlencoding::encode(&db_config.password);
        let connection_string = format!(
            "postgresql://{}:{}@{}:{}/{}",
            db_config.username, password, db_config.host, db_config.port, db_config.database
        );

        let mut opts = PgPoolOptions::new();
        opts = if pooling.enabled {
            let min = db_config.pool_min.unwrap_or(pooling.min);
            let max = db_config.pool_max.unwrap_or(pooling.max);
            opts.min_connections(min).max_connections(max)
        } else {
            opts.max_connections(db_config.connection_pool_size)
        };

        let pool = opts
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(180))
            .connect(&connection_string)
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to connect version store to PostgreSQL: {e}"
                ))
            })?;

        let tables = HistoryTables {
            streams: format!("{}_streams", table_prefix),
            entries: format!("{}_entries", table_prefix),
            version_streams: format!("{}_version_streams", table_prefix),
            version_messages: format!("{}_version_messages", table_prefix),
            version_entries: format!("{}_version_entries", table_prefix),
            annotation_streams: format!("{}_annotation_streams", table_prefix),
            annotation_events: format!("{}_annotation_events", table_prefix),
            annotation_projections: format!("{}_annotation_projections", table_prefix),
        };

        let store = Self { pool, tables };
        store.ensure_version_tables().await?;
        Ok(store)
    }

    async fn ensure_version_tables(&self) -> Result<()> {
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
                is_open_stream BOOLEAN NOT NULL DEFAULT FALSE,
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
                operation_key TEXT NULL,
                operation_fingerprint TEXT NULL,
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
        let idx_messages_history = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {0}_history_serial_uidx ON {0} (app_id, channel, history_serial)",
            self.tables.version_messages
        );
        let idx_entries_delivery = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {0}_delivery_uidx ON {0} (app_id, channel, delivery_serial)",
            self.tables.version_entries
        );
        let idx_entries_message = format!(
            "CREATE INDEX IF NOT EXISTS {0}_message_version_idx ON {0} (app_id, channel, message_serial, version_serial DESC)",
            self.tables.version_entries
        );
        let idx_entries_replay = format!(
            "CREATE INDEX IF NOT EXISTS {0}_replay_idx ON {0} (app_id, channel, delivery_serial)",
            self.tables.version_entries
        );
        let idx_entries_history = format!(
            "CREATE INDEX IF NOT EXISTS {0}_history_version_idx ON {0} (app_id, channel, history_serial, version_serial DESC)",
            self.tables.version_entries
        );
        let idx_entries_created_at = format!(
            "CREATE INDEX IF NOT EXISTS {0}_created_at_idx ON {0} (created_at_ms)",
            self.tables.version_entries
        );
        let alter_messages_open = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS is_open_stream BOOLEAN NOT NULL DEFAULT FALSE",
            self.tables.version_messages
        );
        let alter_entries_operation_key = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS operation_key TEXT NULL",
            self.tables.version_entries
        );
        let alter_entries_operation_fingerprint = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS operation_fingerprint TEXT NULL",
            self.tables.version_entries
        );
        let idx_entries_operation = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {0}_operation_uidx ON {0} (app_id, channel, operation_key) WHERE operation_key IS NOT NULL",
            self.tables.version_entries
        );
        let idx_messages_updated_at = format!(
            "CREATE INDEX IF NOT EXISTS {0}_updated_at_idx ON {0} (updated_at_ms)",
            self.tables.version_messages
        );

        let ddl = [
            create_version_streams,
            create_version_messages,
            create_version_entries,
            idx_messages_history,
            idx_entries_delivery,
            idx_entries_message,
            idx_entries_replay,
            idx_entries_history,
            idx_entries_created_at,
            idx_messages_updated_at,
            alter_messages_open,
            alter_entries_operation_key,
            alter_entries_operation_fingerprint,
            idx_entries_operation,
        ];

        let mut conn = self.pool.acquire().await.map_err(|e| {
            Error::Internal(format!(
                "Failed to acquire PostgreSQL version schema initialization connection: {e}"
            ))
        })?;
        lock_postgres_schema(&mut conn, "sockudo_version_schema").await?;
        let result: Result<()> = async {
            for sql in ddl {
                sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("Failed to initialize version store tables: {e}"))
                    })?;
            }
            Ok(())
        }
        .await;
        unlock_postgres_schema(&mut conn, "sockudo_version_schema").await?;
        result?;
        Ok(())
    }
}
