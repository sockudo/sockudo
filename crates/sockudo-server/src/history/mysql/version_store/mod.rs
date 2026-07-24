mod store_impl;

use super::*;

// ── MySQL VersionStore ────────────────────────────────────────────────────────

#[cfg(feature = "versioned-messages")]
use sockudo_core::version_store::{
    StoredVersionRecord, VersionCreateRejection, VersionCreateRequest, VersionCreateResult,
    VersionMutationRequest, VersionMutationResult, VersionReplayRequest, VersionStore,
    VersionStoreCursor, VersionStoreDirection, VersionStorePage, VersionStoreReadRequest,
    VersionStreamState, VersionWriteReservation, VersionWriteReservationBlock,
};

#[cfg(feature = "versioned-messages")]
pub struct MysqlVersionStore {
    pool: MySqlPool,
    tables: HistoryTables,
}

#[cfg(feature = "versioned-messages")]
pub async fn create_mysql_version_store(
    db_config: &DatabaseConnection,
    pooling: &DatabasePooling,
    table_prefix: &str,
) -> Result<std::sync::Arc<dyn VersionStore + Send + Sync>> {
    let store = MysqlVersionStore::new(db_config, pooling, table_prefix).await?;
    Ok(std::sync::Arc::new(store))
}

#[cfg(feature = "versioned-messages")]
impl MysqlVersionStore {
    async fn new(
        db_config: &DatabaseConnection,
        pooling: &DatabasePooling,
        table_prefix: &str,
    ) -> Result<Self> {
        let password = urlencoding::encode(&db_config.password);
        let connection_string = format!(
            "mysql://{}:{}@{}:{}/{}",
            db_config.username, password, db_config.host, db_config.port, db_config.database
        );

        let mut opts = MySqlPoolOptions::new();
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
                Error::Internal(format!("Failed to connect version store to MySQL: {e}"))
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
            r#"CREATE TABLE IF NOT EXISTS `{}` (
                app_id VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NOT NULL,
                channel VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NOT NULL,
                next_delivery_serial BIGINT NOT NULL,
                oldest_available_delivery_serial BIGINT NULL,
                newest_available_delivery_serial BIGINT NULL,
                migration_state VARCHAR(32) NOT NULL DEFAULT 'native_only',
                migration_state_changed_at_ms BIGINT NULL,
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"#,
            self.tables.version_streams
        );
        let create_version_messages = format!(
            r#"CREATE TABLE IF NOT EXISTS `{}` (
                app_id VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NOT NULL,
                channel VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NOT NULL,
                message_serial VARCHAR({}) NOT NULL,
                history_serial BIGINT NOT NULL,
                original_client_id VARCHAR(255) NULL,
                latest_version_serial VARCHAR({}) NOT NULL,
                latest_delivery_serial BIGINT NOT NULL,
                latest_action VARCHAR(64) NOT NULL,
                is_open_stream BOOLEAN NOT NULL DEFAULT FALSE,
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel, message_serial),
                UNIQUE KEY {}_history_serial_uidx (app_id, channel, history_serial)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"#,
            self.tables.version_messages,
            MAX_VERSIONED_SERIAL_LENGTH,
            MAX_VERSIONED_SERIAL_LENGTH,
            self.tables.version_messages
        );
        let create_version_entries = format!(
            r#"CREATE TABLE IF NOT EXISTS `{}` (
                app_id VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NOT NULL,
                channel VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NOT NULL,
                message_serial VARCHAR({}) NOT NULL,
                version_serial VARCHAR({}) NOT NULL,
                delivery_serial BIGINT NOT NULL,
                history_serial BIGINT NOT NULL,
                action VARCHAR(64) NOT NULL,
                client_id VARCHAR(255) NULL,
                description TEXT NULL,
                operation_metadata JSON NULL,
                operation_key VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NULL,
                operation_fingerprint VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NULL,
                event_name VARCHAR(255) NULL,
                payload_bytes LONGBLOB NOT NULL,
                payload_size_bytes BIGINT NOT NULL,
                version_timestamp_ms BIGINT NOT NULL,
                created_at_ms BIGINT NOT NULL,
                PRIMARY KEY (app_id, channel, message_serial, version_serial),
                UNIQUE KEY {}_delivery_uidx (app_id, channel, delivery_serial),
                UNIQUE KEY {}_operation_uidx (app_id, channel, operation_key),
                INDEX {}_created_at_idx (created_at_ms)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"#,
            self.tables.version_entries,
            MAX_VERSIONED_SERIAL_LENGTH,
            MAX_VERSIONED_SERIAL_LENGTH,
            self.tables.version_entries,
            self.tables.version_entries,
            self.tables.version_entries,
        );

        for sql in [
            create_version_streams,
            create_version_messages,
            create_version_entries,
        ] {
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to initialize MySQL version tables: {e}"))
                })?;
        }

        // Ensure the purge indices exist on tables created before the purge
        // worker shipped. `CREATE INDEX IF NOT EXISTS` is MySQL 8+; we detect
        // via INFORMATION_SCHEMA to stay compatible with 5.7.
        self.ensure_index(&self.tables.version_entries, "created_at_ms")
            .await?;
        self.ensure_index(&self.tables.version_messages, "updated_at_ms")
            .await?;
        self.ensure_column(
            &self.tables.version_messages,
            "is_open_stream",
            "BOOLEAN NOT NULL DEFAULT FALSE",
        )
        .await?;
        self.ensure_column(
            &self.tables.version_entries,
            "operation_key",
            &format!("VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NULL"),
        )
        .await?;
        self.ensure_column(
            &self.tables.version_entries,
            "operation_fingerprint",
            &format!("VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NULL"),
        )
        .await?;
        self.ensure_operation_index().await?;

        Ok(())
    }

    async fn ensure_index(&self, table: &str, column: &str) -> Result<()> {
        let index_name = format!("{}_{}_idx", table, column);
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?",
        )
        .bind(table)
        .bind(&index_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| Error::Internal(format!("Failed to probe MySQL index: {e}")))?;
        if exists == 0 {
            let sql = format!(
                "CREATE INDEX `{}` ON `{}` (`{}`)",
                index_name, table, column
            );
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to create MySQL index {index_name} on {table}: {e}"
                    ))
                })?;
        }
        Ok(())
    }

    async fn ensure_column(&self, table: &str, column: &str, definition: &str) -> Result<()> {
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?",
        )
        .bind(table)
        .bind(column)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| Error::Internal(format!("Failed to probe MySQL column: {e}")))?;
        if exists == 0 {
            let sql = format!("ALTER TABLE `{table}` ADD COLUMN `{column}` {definition}");
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&self.pool)
                .await
                .map_err(|e| Error::Internal(format!("Failed to add MySQL column: {e}")))?;
        }
        Ok(())
    }

    async fn ensure_operation_index(&self) -> Result<()> {
        let index_name = format!("{}_operation_uidx", self.tables.version_entries);
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?",
        )
        .bind(&self.tables.version_entries)
        .bind(&index_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| Error::Internal(format!("Failed to probe MySQL operation index: {e}")))?;
        if exists == 0 {
            let sql = format!(
                "CREATE UNIQUE INDEX `{index_name}` ON `{}` (app_id, channel, operation_key)",
                self.tables.version_entries
            );
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&self.pool)
                .await
                .map_err(|e| Error::Internal(format!("Failed to add operation index: {e}")))?;
        }
        Ok(())
    }
}
