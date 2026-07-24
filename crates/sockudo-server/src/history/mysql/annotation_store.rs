use super::{HistoryTables, MYSQL_ASCII_IDENTIFIER_CHARSET};
use async_trait::async_trait;
use sockudo_core::annotations::{
    Annotation, AnnotationAction, AnnotationAppendOutcome, AnnotationEventLookupRequest,
    AnnotationEventsRequest, AnnotationProjection, AnnotationProjectionOptions,
    AnnotationProjectionRequest, AnnotationProjectionsForChannelRequest, AnnotationSerial,
    AnnotationStore, RawAnnotationReplayRequest, StoredAnnotationEvent, StoredAnnotationProjection,
};
use sockudo_core::error::{Error, Result};
use sockudo_core::history::now_ms;
use sockudo_core::options::{DatabaseConnection, DatabasePooling};
use sockudo_core::versioned_messages::{MAX_VERSIONED_SERIAL_LENGTH, MessageSerial};
use sqlx::{
    MySql, MySqlPool, Row, Transaction,
    mysql::{MySqlPoolOptions, MySqlRow},
    pool::PoolConnection,
};
use std::collections::BTreeSet;
use std::time::Duration;

const MAX_CHANNEL_PROJECTIONS: usize = 10_000;
const MAX_PROJECTION_EVENTS: usize = 100_000;
const MAX_RAW_REPLAY_EVENTS: usize = 10_000;
const MYSQL_BINARY_IDENTIFIER_CHARSET: &str = "CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";

pub(in crate::history) struct MysqlAnnotationStore {
    pool: MySqlPool,
    tables: HistoryTables,
}

impl MysqlAnnotationStore {
    pub(in crate::history) async fn new(
        db_config: &DatabaseConnection,
        pooling: &DatabasePooling,
        table_prefix: &str,
    ) -> Result<Self> {
        let password = urlencoding::encode(&db_config.password);
        let connection_string = format!(
            "mysql://{}:{}@{}:{}/{}",
            db_config.username, password, db_config.host, db_config.port, db_config.database
        );
        let mut options = MySqlPoolOptions::new();
        options = if pooling.enabled {
            let min = db_config.pool_min.unwrap_or(pooling.min);
            let max = db_config.pool_max.unwrap_or(pooling.max);
            options.min_connections(min).max_connections(max)
        } else {
            options.max_connections(db_config.connection_pool_size)
        };
        let pool = options
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(180))
            .connect(&connection_string)
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to connect annotation store to MySQL: {error}"
                ))
            })?;
        let tables = HistoryTables {
            streams: format!("{table_prefix}_streams"),
            entries: format!("{table_prefix}_entries"),
            version_streams: format!("{table_prefix}_version_streams"),
            version_messages: format!("{table_prefix}_version_messages"),
            version_entries: format!("{table_prefix}_version_entries"),
            annotation_streams: format!("{table_prefix}_annotation_streams"),
            annotation_events: format!("{table_prefix}_annotation_events"),
            annotation_projections: format!("{table_prefix}_annotation_projections"),
        };
        let store = Self { pool, tables };
        store.ensure_tables().await?;
        Ok(store)
    }

    async fn lock_schema(connection: &mut PoolConnection<MySql>) -> Result<()> {
        let acquired = sqlx::query_scalar::<_, Option<i64>>("SELECT GET_LOCK(?, 5)")
            .bind("sockudo_annotation_schema")
            .fetch_one(&mut **connection)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to lock MySQL annotation schema: {error}"))
            })?;
        if acquired != Some(1) {
            return Err(Error::Internal(
                "Timed out while locking MySQL annotation schema".to_string(),
            ));
        }
        Ok(())
    }

    async fn unlock_schema(connection: &mut PoolConnection<MySql>) -> Result<()> {
        let released = sqlx::query_scalar::<_, Option<i64>>("SELECT RELEASE_LOCK(?)")
            .bind("sockudo_annotation_schema")
            .fetch_one(&mut **connection)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to unlock MySQL annotation schema: {error}"))
            })?;
        if released != Some(1) {
            return Err(Error::Internal(
                "MySQL annotation schema lock was not held by this connection".to_string(),
            ));
        }
        Ok(())
    }

    async fn ensure_column(
        connection: &mut PoolConnection<MySql>,
        table: &str,
        column: &str,
        definition: &str,
    ) -> Result<()> {
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?",
        )
        .bind(table)
        .bind(column)
        .fetch_one(&mut **connection)
        .await
        .map_err(|error| {
            Error::Internal(format!("Failed to inspect MySQL annotation column: {error}"))
        })?;
        if exists == 0 {
            let sql = format!("ALTER TABLE `{table}` ADD COLUMN `{column}` {definition}");
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&mut **connection)
                .await
                .map_err(|error| {
                    Error::Internal(format!("Failed to add MySQL annotation column: {error}"))
                })?;
        }
        Ok(())
    }

    async fn ensure_index(
        connection: &mut PoolConnection<MySql>,
        table: &str,
        index: &str,
        definition: &str,
        unique: bool,
    ) -> Result<()> {
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?",
        )
        .bind(table)
        .bind(index)
        .fetch_one(&mut **connection)
        .await
        .map_err(|error| {
            Error::Internal(format!("Failed to inspect MySQL annotation index: {error}"))
        })?;
        if exists == 0 {
            let unique = if unique { "UNIQUE " } else { "" };
            let sql = format!("CREATE {unique}INDEX `{index}` ON `{table}` ({definition})");
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&mut **connection)
                .await
                .map_err(|error| {
                    Error::Internal(format!("Failed to add MySQL annotation index: {error}"))
                })?;
        }
        Ok(())
    }

    async fn ensure_tables(&self) -> Result<()> {
        let create_streams = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (app_id VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NOT NULL, channel VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NOT NULL, next_serial BIGINT NOT NULL, updated_at_ms BIGINT NOT NULL, PRIMARY KEY (app_id, channel)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
            self.tables.annotation_streams
        );
        let create_events = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (app_id VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NOT NULL, channel VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NOT NULL, message_serial VARCHAR({MAX_VERSIONED_SERIAL_LENGTH}) {MYSQL_BINARY_IDENTIFIER_CHARSET} NOT NULL, annotation_serial VARCHAR({MAX_VERSIONED_SERIAL_LENGTH}) {MYSQL_BINARY_IDENTIFIER_CHARSET} NOT NULL, annotation_type VARCHAR(256) {MYSQL_ASCII_IDENTIFIER_CHARSET} NOT NULL, annotation_id VARCHAR({MAX_VERSIONED_SERIAL_LENGTH}) {MYSQL_BINARY_IDENTIFIER_CHARSET} NULL, name VARCHAR(255) NULL, client_id VARCHAR(255) NULL, count_value BIGINT NULL, action VARCHAR(64) NOT NULL, create_annotation_id VARCHAR({MAX_VERSIONED_SERIAL_LENGTH}) {MYSQL_BINARY_IDENTIFIER_CHARSET} GENERATED ALWAYS AS (CASE WHEN action = 'annotation.create' THEN annotation_id ELSE NULL END) STORED, data_bytes LONGBLOB NULL, encoding VARCHAR(255) NULL, annotation_timestamp_ms BIGINT NOT NULL, payload_bytes LONGBLOB NOT NULL, payload_size_bytes BIGINT NOT NULL, created_at_ms BIGINT NOT NULL, PRIMARY KEY (app_id, channel, message_serial, annotation_serial)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
            self.tables.annotation_events
        );
        let create_projections = format!(
            "CREATE TABLE IF NOT EXISTS `{}` (app_id VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NOT NULL, channel VARCHAR(255) {MYSQL_ASCII_IDENTIFIER_CHARSET} NOT NULL, message_serial VARCHAR({MAX_VERSIONED_SERIAL_LENGTH}) {MYSQL_BINARY_IDENTIFIER_CHARSET} NOT NULL, annotation_type VARCHAR(256) {MYSQL_ASCII_IDENTIFIER_CHARSET} NOT NULL, summary_json JSON NOT NULL, last_annotation_serial VARCHAR({MAX_VERSIONED_SERIAL_LENGTH}) {MYSQL_BINARY_IDENTIFIER_CHARSET} NULL, updated_at_ms BIGINT NOT NULL, PRIMARY KEY (app_id, channel, message_serial, annotation_type)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
            self.tables.annotation_projections
        );

        let mut connection = self.pool.acquire().await.map_err(|error| {
            Error::Internal(format!(
                "Failed to acquire MySQL annotation schema connection: {error}"
            ))
        })?;
        Self::lock_schema(&mut connection).await?;
        let result: Result<()> = async {
            for sql in [create_streams, create_events, create_projections] {
                sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                    .execute(&mut *connection)
                    .await
                    .map_err(|error| {
                        Error::Internal(format!(
                            "Failed to initialize MySQL annotation tables: {error}"
                        ))
                    })?;
            }
            Self::ensure_column(
                &mut connection,
                &self.tables.annotation_events,
                "annotation_id",
                &format!(
                    "VARCHAR({MAX_VERSIONED_SERIAL_LENGTH}) {MYSQL_BINARY_IDENTIFIER_CHARSET} NULL"
                ),
            )
            .await?;
            Self::ensure_column(
                &mut connection,
                &self.tables.annotation_events,
                "create_annotation_id",
                &format!(
                    "VARCHAR({MAX_VERSIONED_SERIAL_LENGTH}) {MYSQL_BINARY_IDENTIFIER_CHARSET} GENERATED ALWAYS AS (CASE WHEN action = 'annotation.create' THEN annotation_id ELSE NULL END) STORED"
                ),
            )
            .await?;
            Self::ensure_index(
                &mut connection,
                &self.tables.annotation_events,
                "annotation_summary_idx",
                "app_id, channel, message_serial, annotation_type, annotation_serial",
                false,
            )
            .await?;
            Self::ensure_index(
                &mut connection,
                &self.tables.annotation_events,
                "annotation_raw_replay_idx",
                "app_id, channel, annotation_serial",
                false,
            )
            .await?;
            Self::ensure_index(
                &mut connection,
                &self.tables.annotation_events,
                "annotation_created_at_idx",
                "created_at_ms",
                false,
            )
            .await?;
            Self::ensure_index(
                &mut connection,
                &self.tables.annotation_events,
                "annotation_create_id_uidx",
                "app_id, channel, message_serial, annotation_type, create_annotation_id",
                true,
            )
            .await?;
            Ok(())
        }
        .await;
        let unlock_result = Self::unlock_schema(&mut connection).await;
        result?;
        unlock_result
    }

    fn projection_request(record: &StoredAnnotationEvent) -> AnnotationProjectionRequest {
        AnnotationProjectionRequest {
            app_id: record.app_id.clone(),
            channel_id: record.channel_id.clone(),
            message_serial: record.annotation.message_serial.clone(),
            annotation_type: record.annotation.annotation_type.clone(),
        }
    }

    fn build_projection(
        request: &AnnotationProjectionRequest,
        records: Vec<StoredAnnotationEvent>,
        options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        let projection = AnnotationProjection::rebuild_with_options(
            request.channel_id.clone(),
            request.message_serial.clone(),
            request.annotation_type.clone(),
            records.into_iter().map(|record| record.annotation),
            options,
        )?;
        Ok(StoredAnnotationProjection {
            app_id: request.app_id.clone(),
            channel_id: projection.key.channel_id,
            message_serial: projection.key.message_serial,
            annotation_type: projection.key.annotation_type,
            summary: projection.summary,
            last_annotation_serial: projection.last_serial,
            updated_at_ms: now_ms(),
        })
    }

    fn decode_record(payload: &[u8]) -> Result<StoredAnnotationEvent> {
        sonic_rs::from_slice(payload).map_err(|error| {
            Error::Internal(format!(
                "Failed to decode durable annotation event: {error}"
            ))
        })
    }

    fn decode_projection_row(
        app_id: &str,
        channel_id: &str,
        row: &MySqlRow,
    ) -> Result<StoredAnnotationProjection> {
        let summary_json: String = row.get("summary_json");
        let summary = serde_json::from_str(&summary_json).map_err(|error| {
            Error::Internal(format!("Failed to decode annotation projection: {error}"))
        })?;
        let message_serial = MessageSerial::new(row.get::<String, _>("message_serial"))?;
        let annotation_type = sockudo_core::annotations::AnnotationType::new(
            row.get::<String, _>("annotation_type"),
        )?;
        let last_annotation_serial = row
            .get::<Option<String>, _>("last_annotation_serial")
            .map(AnnotationSerial::new)
            .transpose()?;
        Ok(StoredAnnotationProjection {
            app_id: app_id.to_string(),
            channel_id: channel_id.to_string(),
            message_serial,
            annotation_type,
            summary,
            last_annotation_serial,
            updated_at_ms: row.get("updated_at_ms"),
        })
    }

    async fn lock_projection(
        &self,
        transaction: &mut Transaction<'_, MySql>,
        request: &AnnotationProjectionRequest,
    ) -> Result<()> {
        let placeholder =
            Self::build_projection(request, Vec::new(), AnnotationProjectionOptions::default())?;
        let summary_json = serde_json::to_string(&placeholder.summary).map_err(|error| {
            Error::Internal(format!("Failed to encode annotation projection: {error}"))
        })?;
        let sql = format!(
            "INSERT INTO `{}` (app_id, channel, message_serial, annotation_type, summary_json, last_annotation_serial, updated_at_ms) VALUES (?, ?, ?, ?, ?, NULL, ?) ON DUPLICATE KEY UPDATE updated_at_ms = VALUES(updated_at_ms)",
            self.tables.annotation_projections
        );
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&request.app_id)
            .bind(&request.channel_id)
            .bind(request.message_serial.as_str())
            .bind(request.annotation_type.as_str())
            .bind(summary_json)
            .bind(now_ms())
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to lock annotation projection: {error}"))
            })?;
        Ok(())
    }

    async fn events_in_transaction(
        &self,
        transaction: &mut Transaction<'_, MySql>,
        request: &AnnotationProjectionRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        let sql = format!(
            "SELECT payload_bytes FROM `{}` WHERE app_id = ? AND channel = ? AND message_serial = ? AND annotation_type = ? ORDER BY annotation_serial ASC LIMIT ?",
            self.tables.annotation_events
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&request.app_id)
            .bind(&request.channel_id)
            .bind(request.message_serial.as_str())
            .bind(request.annotation_type.as_str())
            .bind(i64::try_from(MAX_PROJECTION_EVENTS + 1).unwrap_or(i64::MAX))
            .fetch_all(&mut **transaction)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to read annotation events: {error}"))
            })?;
        if rows.len() > MAX_PROJECTION_EVENTS {
            return Err(Error::BufferFull(format!(
                "annotation projection exceeds the {MAX_PROJECTION_EVENTS} event bound"
            )));
        }
        rows.into_iter()
            .map(|row| Self::decode_record(&row.get::<Vec<u8>, _>("payload_bytes")))
            .collect()
    }

    async fn persist_projection(
        &self,
        transaction: &mut Transaction<'_, MySql>,
        request: &AnnotationProjectionRequest,
        options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        let records = self.events_in_transaction(transaction, request).await?;
        let projection = Self::build_projection(request, records, options)?;
        let summary_json = serde_json::to_string(&projection.summary).map_err(|error| {
            Error::Internal(format!("Failed to encode annotation projection: {error}"))
        })?;
        let sql = format!(
            "INSERT INTO `{}` (app_id, channel, message_serial, annotation_type, summary_json, last_annotation_serial, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE summary_json = VALUES(summary_json), last_annotation_serial = VALUES(last_annotation_serial), updated_at_ms = VALUES(updated_at_ms)",
            self.tables.annotation_projections
        );
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&projection.app_id)
            .bind(&projection.channel_id)
            .bind(projection.message_serial.as_str())
            .bind(projection.annotation_type.as_str())
            .bind(summary_json)
            .bind(
                projection
                    .last_annotation_serial
                    .as_ref()
                    .map(AnnotationSerial::as_str),
            )
            .bind(projection.updated_at_ms)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to persist annotation projection: {error}"))
            })?;
        Ok(projection)
    }

    async fn insert_record(
        &self,
        transaction: &mut Transaction<'_, MySql>,
        record: &StoredAnnotationEvent,
    ) -> Result<bool> {
        let payload = sonic_rs::to_vec(record)?;
        let payload_size = i64::try_from(payload.len()).map_err(|_| {
            Error::InvalidMessageFormat("annotation payload is too large".to_string())
        })?;
        let count = record
            .annotation
            .count
            .map(i64::try_from)
            .transpose()
            .map_err(|_| Error::InvalidMessageFormat("annotation count exceeds i64".to_string()))?;
        let data = record
            .annotation
            .data
            .as_ref()
            .map(sonic_rs::to_vec)
            .transpose()?;
        let sql = format!(
            "INSERT INTO `{}` (app_id, channel, message_serial, annotation_serial, annotation_type, annotation_id, name, client_id, count_value, action, data_bytes, encoding, annotation_timestamp_ms, payload_bytes, payload_size_bytes, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE annotation_serial = annotation_serial",
            self.tables.annotation_events
        );
        let result = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel_id)
            .bind(record.annotation.message_serial.as_str())
            .bind(record.annotation.serial.as_str())
            .bind(record.annotation.annotation_type.as_str())
            .bind(record.annotation.id.as_str())
            .bind(record.annotation.name.as_deref())
            .bind(record.annotation.client_id.as_deref())
            .bind(count)
            .bind(record.annotation.action.as_str())
            .bind(data)
            .bind(record.annotation.encoding.as_deref())
            .bind(record.annotation.timestamp)
            .bind(payload)
            .bind(payload_size)
            .bind(record.stored_at_ms)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to append durable annotation event: {error}"
                ))
            })?;
        Ok(result.rows_affected() == 1)
    }

    fn create_payload_matches(existing: &Annotation, incoming: &Annotation) -> bool {
        existing.name == incoming.name
            && existing.client_id == incoming.client_id
            && existing.count == incoming.count
            && existing.data == incoming.data
            && existing.encoding == incoming.encoding
    }

    async fn find_create_by_id(
        &self,
        transaction: &mut Transaction<'_, MySql>,
        record: &StoredAnnotationEvent,
    ) -> Result<Option<StoredAnnotationEvent>> {
        let sql = format!(
            "SELECT payload_bytes FROM `{}` WHERE app_id = ? AND channel = ? AND message_serial = ? AND annotation_type = ? AND annotation_id = ? AND action = 'annotation.create' LIMIT 1",
            self.tables.annotation_events
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel_id)
            .bind(record.annotation.message_serial.as_str())
            .bind(record.annotation.annotation_type.as_str())
            .bind(record.annotation.id.as_str())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to check annotation idempotency: {error}"))
            })?;
        row.map(|row| Self::decode_record(&row.get::<Vec<u8>, _>("payload_bytes")))
            .transpose()
    }
}

#[async_trait]
impl AnnotationStore for MysqlAnnotationStore {
    async fn reserve_annotation_serial(
        &self,
        app_id: &str,
        channel_id: &str,
    ) -> Result<Option<AnnotationSerial>> {
        let mut transaction = self.pool.begin().await.map_err(|error| {
            Error::Internal(format!(
                "Failed to begin annotation serial transaction: {error}"
            ))
        })?;
        let initialize_sql = format!(
            "INSERT INTO `{}` (app_id, channel, next_serial, updated_at_ms) VALUES (?, ?, 1, ?) ON DUPLICATE KEY UPDATE updated_at_ms = updated_at_ms",
            self.tables.annotation_streams
        );
        sqlx::query(sqlx::AssertSqlSafe(initialize_sql.as_str()))
            .bind(app_id)
            .bind(channel_id)
            .bind(now_ms())
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to initialize annotation serial: {error}"))
            })?;
        let select_sql = format!(
            "SELECT next_serial FROM `{}` WHERE app_id = ? AND channel = ? FOR UPDATE",
            self.tables.annotation_streams
        );
        let allocated = sqlx::query(sqlx::AssertSqlSafe(select_sql.as_str()))
            .bind(app_id)
            .bind(channel_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| Error::Internal(format!("Failed to lock annotation serial: {error}")))?
            .get::<i64, _>("next_serial");
        if allocated <= 0 {
            return Err(Error::Internal(
                "annotation serial allocator returned a non-positive value".to_string(),
            ));
        }
        let next = allocated.checked_add(1).ok_or_else(|| {
            Error::Internal("annotation serial allocator exhausted i64".to_string())
        })?;
        let update_sql = format!(
            "UPDATE `{}` SET next_serial = ?, updated_at_ms = ? WHERE app_id = ? AND channel = ?",
            self.tables.annotation_streams
        );
        sqlx::query(sqlx::AssertSqlSafe(update_sql.as_str()))
            .bind(next)
            .bind(now_ms())
            .bind(app_id)
            .bind(channel_id)
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to advance annotation serial: {error}"))
            })?;
        transaction.commit().await.map_err(|error| {
            Error::Internal(format!("Failed to commit annotation serial: {error}"))
        })?;
        Ok(Some(AnnotationSerial::new(format!("ann:{allocated:020}"))?))
    }

    async fn append_event(
        &self,
        mut record: StoredAnnotationEvent,
    ) -> Result<StoredAnnotationProjection> {
        record.validate()?;
        if record.stored_at_ms == 0 {
            record.stored_at_ms = now_ms();
        }
        let request = Self::projection_request(&record);
        let mut transaction = self.pool.begin().await.map_err(|error| {
            Error::Internal(format!("Failed to begin annotation transaction: {error}"))
        })?;
        self.lock_projection(&mut transaction, &request).await?;
        let inserted = self.insert_record(&mut transaction, &record).await?;
        if !inserted {
            let sql = format!(
                "SELECT payload_bytes FROM `{}` WHERE app_id = ? AND channel = ? AND message_serial = ? AND annotation_serial = ?",
                self.tables.annotation_events
            );
            let existing = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(&record.app_id)
                .bind(&record.channel_id)
                .bind(record.annotation.message_serial.as_str())
                .bind(record.annotation.serial.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|error| {
                    Error::Internal(format!("Failed to verify annotation retry: {error}"))
                })?
                .ok_or_else(|| {
                    Error::InvalidMessageFormat(
                        "annotation identity conflicts with another event".to_string(),
                    )
                })?;
            let existing = Self::decode_record(&existing.get::<Vec<u8>, _>("payload_bytes"))?;
            if existing.annotation != record.annotation {
                return Err(Error::InvalidMessageFormat(
                    "annotation serial was already used with a different payload".to_string(),
                ));
            }
        }
        let projection = self
            .persist_projection(
                &mut transaction,
                &request,
                AnnotationProjectionOptions::default(),
            )
            .await?;
        transaction.commit().await.map_err(|error| {
            Error::Internal(format!("Failed to commit annotation event: {error}"))
        })?;
        Ok(projection)
    }

    async fn append_create_idempotent(
        &self,
        mut record: StoredAnnotationEvent,
    ) -> Result<AnnotationAppendOutcome> {
        record.validate()?;
        if record.annotation.action != AnnotationAction::Create {
            let canonical_serial = record.annotation.serial.clone();
            let projection = self.append_event(record).await?;
            return Ok(AnnotationAppendOutcome {
                projection,
                canonical_serial,
                inserted: true,
            });
        }
        if record.stored_at_ms == 0 {
            record.stored_at_ms = now_ms();
        }
        let request = Self::projection_request(&record);
        let mut transaction = self.pool.begin().await.map_err(|error| {
            Error::Internal(format!(
                "Failed to begin annotation create transaction: {error}"
            ))
        })?;
        self.lock_projection(&mut transaction, &request).await?;
        let existing = self.find_create_by_id(&mut transaction, &record).await?;
        let (canonical_serial, inserted) = if let Some(existing) = existing {
            if !Self::create_payload_matches(&existing.annotation, &record.annotation) {
                return Err(Error::InvalidMessageFormat(format!(
                    "Annotation id '{}' was already used with a different payload",
                    record.annotation.id.as_str()
                )));
            }
            (existing.annotation.serial, false)
        } else if self.insert_record(&mut transaction, &record).await? {
            (record.annotation.serial.clone(), true)
        } else if let Some(existing) = self.find_create_by_id(&mut transaction, &record).await? {
            if !Self::create_payload_matches(&existing.annotation, &record.annotation) {
                return Err(Error::InvalidMessageFormat(format!(
                    "Annotation id '{}' was already used with a different payload",
                    record.annotation.id.as_str()
                )));
            }
            (existing.annotation.serial, false)
        } else {
            return Err(Error::InvalidMessageFormat(
                "annotation identity conflicts with another event".to_string(),
            ));
        };
        let projection = self
            .persist_projection(
                &mut transaction,
                &request,
                AnnotationProjectionOptions::default(),
            )
            .await?;
        transaction.commit().await.map_err(|error| {
            Error::Internal(format!("Failed to commit annotation create: {error}"))
        })?;
        Ok(AnnotationAppendOutcome {
            projection,
            canonical_serial,
            inserted,
        })
    }

    async fn get_events(
        &self,
        request: AnnotationEventsRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        request.validate()?;
        let sql = format!(
            "SELECT payload_bytes FROM `{}` WHERE app_id = ? AND channel = ? AND message_serial = ? AND annotation_type = ? ORDER BY annotation_serial ASC LIMIT ?",
            self.tables.annotation_events
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&request.app_id)
            .bind(&request.channel_id)
            .bind(request.message_serial.as_str())
            .bind(request.annotation_type.as_str())
            .bind(i64::try_from(MAX_PROJECTION_EVENTS + 1).unwrap_or(i64::MAX))
            .fetch_all(&self.pool)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to read durable annotation events: {error}"))
            })?;
        if rows.len() > MAX_PROJECTION_EVENTS {
            return Err(Error::BufferFull(format!(
                "annotation projection exceeds the {MAX_PROJECTION_EVENTS} event bound"
            )));
        }
        rows.into_iter()
            .map(|row| Self::decode_record(&row.get::<Vec<u8>, _>("payload_bytes")))
            .collect()
    }

    async fn replay_raw(
        &self,
        request: RawAnnotationReplayRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        request.validate()?;
        if request.limit > MAX_RAW_REPLAY_EVENTS {
            return Err(Error::BufferFull(format!(
                "annotation replay exceeds the {MAX_RAW_REPLAY_EVENTS} item bound"
            )));
        }
        let limit = i64::try_from(request.limit).map_err(|_| {
            Error::InvalidMessageFormat("annotation replay limit is too large".to_string())
        })?;
        let sql = format!(
            "SELECT payload_bytes FROM `{}` WHERE app_id = ? AND channel = ? AND (? IS NULL OR message_serial = ?) AND (? IS NULL OR annotation_serial > ?) ORDER BY annotation_serial ASC LIMIT ?",
            self.tables.annotation_events
        );
        let message_serial = request.message_serial.as_ref().map(MessageSerial::as_str);
        let after_serial = request
            .after_annotation_serial
            .as_ref()
            .map(AnnotationSerial::as_str);
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&request.app_id)
            .bind(&request.channel_id)
            .bind(message_serial)
            .bind(message_serial)
            .bind(after_serial)
            .bind(after_serial)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to replay durable annotations: {error}"))
            })?;
        rows.into_iter()
            .map(|row| Self::decode_record(&row.get::<Vec<u8>, _>("payload_bytes")))
            .collect()
    }

    async fn get_event_by_serial(
        &self,
        request: AnnotationEventLookupRequest,
    ) -> Result<Option<StoredAnnotationEvent>> {
        request.validate()?;
        let sql = format!(
            "SELECT payload_bytes FROM `{}` WHERE app_id = ? AND channel = ? AND annotation_serial = ? LIMIT 1",
            self.tables.annotation_events
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&request.app_id)
            .bind(&request.channel_id)
            .bind(request.annotation_serial.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to find durable annotation event: {error}"))
            })?;
        row.map(|row| Self::decode_record(&row.get::<Vec<u8>, _>("payload_bytes")))
            .transpose()
    }

    async fn get_projection(
        &self,
        request: AnnotationProjectionRequest,
    ) -> Result<Option<StoredAnnotationProjection>> {
        request.validate()?;
        let sql = format!(
            "SELECT message_serial, annotation_type, CAST(summary_json AS CHAR) AS summary_json, last_annotation_serial, updated_at_ms FROM `{}` WHERE app_id = ? AND channel = ? AND message_serial = ? AND annotation_type = ?",
            self.tables.annotation_projections
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&request.app_id)
            .bind(&request.channel_id)
            .bind(request.message_serial.as_str())
            .bind(request.annotation_type.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to read annotation projection: {error}"))
            })?;
        row.map(|row| Self::decode_projection_row(&request.app_id, &request.channel_id, &row))
            .transpose()
    }

    async fn list_projections_for_channel(
        &self,
        request: AnnotationProjectionsForChannelRequest,
    ) -> Result<Vec<StoredAnnotationProjection>> {
        let (projections, _) = self
            .list_projections_for_channel_with_rebuild_count(request)
            .await?;
        Ok(projections)
    }

    async fn list_projections_for_channel_with_rebuild_count(
        &self,
        request: AnnotationProjectionsForChannelRequest,
    ) -> Result<(Vec<StoredAnnotationProjection>, usize)> {
        request.validate()?;
        let sql = format!(
            "SELECT message_serial, annotation_type, CAST(summary_json AS CHAR) AS summary_json, last_annotation_serial, updated_at_ms FROM `{}` WHERE app_id = ? AND channel = ? ORDER BY message_serial ASC, annotation_type ASC LIMIT ?",
            self.tables.annotation_projections
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&request.app_id)
            .bind(&request.channel_id)
            .bind(i64::try_from(MAX_CHANNEL_PROJECTIONS + 1).unwrap_or(i64::MAX))
            .fetch_all(&self.pool)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to list annotation projections: {error}"))
            })?;
        if rows.len() > MAX_CHANNEL_PROJECTIONS {
            return Err(Error::BufferFull(format!(
                "annotation projection list exceeds the {MAX_CHANNEL_PROJECTIONS} item bound"
            )));
        }
        let projections = rows
            .into_iter()
            .map(|row| Self::decode_projection_row(&request.app_id, &request.channel_id, &row))
            .collect::<Result<Vec<_>>>()?;
        Ok((projections, 0))
    }

    async fn rebuild_projection(
        &self,
        request: AnnotationProjectionRequest,
    ) -> Result<StoredAnnotationProjection> {
        self.rebuild_projection_with_options(request, AnnotationProjectionOptions::default())
            .await
    }

    async fn rebuild_projection_with_options(
        &self,
        request: AnnotationProjectionRequest,
        options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        request.validate()?;
        let mut transaction = self.pool.begin().await.map_err(|error| {
            Error::Internal(format!("Failed to begin projection rebuild: {error}"))
        })?;
        self.lock_projection(&mut transaction, &request).await?;
        let projection = self
            .persist_projection(&mut transaction, &request, options)
            .await?;
        transaction.commit().await.map_err(|error| {
            Error::Internal(format!("Failed to commit projection rebuild: {error}"))
        })?;
        Ok(projection)
    }

    async fn purge_before(&self, before_ms: i64, batch_size: usize) -> Result<(u64, bool)> {
        if batch_size == 0 {
            return Ok((0, false));
        }
        let limit = i64::try_from(batch_size)
            .map_err(|_| Error::InvalidMessageFormat("purge batch is too large".to_string()))?;
        let candidates_sql = format!(
            "SELECT app_id, channel, message_serial, annotation_type, annotation_serial FROM `{}` WHERE created_at_ms < ? ORDER BY created_at_ms ASC, app_id ASC, channel ASC, annotation_serial ASC LIMIT ?",
            self.tables.annotation_events
        );
        let candidates = sqlx::query(sqlx::AssertSqlSafe(candidates_sql.as_str()))
            .bind(before_ms)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to select annotation purge batch: {error}"))
            })?;
        if candidates.is_empty() {
            return Ok((0, false));
        }
        let affected = candidates
            .iter()
            .map(|row| {
                (
                    row.get::<String, _>("app_id"),
                    row.get::<String, _>("channel"),
                    row.get::<String, _>("message_serial"),
                    row.get::<String, _>("annotation_type"),
                )
            })
            .collect::<BTreeSet<_>>();
        let mut transaction = self.pool.begin().await.map_err(|error| {
            Error::Internal(format!("Failed to begin annotation purge: {error}"))
        })?;
        let requests = affected
            .into_iter()
            .map(|(app_id, channel_id, message_serial, annotation_type)| {
                Ok(AnnotationProjectionRequest {
                    app_id,
                    channel_id,
                    message_serial: MessageSerial::new(message_serial)?,
                    annotation_type: sockudo_core::annotations::AnnotationType::new(
                        annotation_type,
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        for request in &requests {
            self.lock_projection(&mut transaction, request).await?;
        }
        let delete_sql = format!(
            "DELETE FROM `{}` WHERE app_id = ? AND channel = ? AND message_serial = ? AND annotation_serial = ? AND created_at_ms < ?",
            self.tables.annotation_events
        );
        let mut deleted = 0_u64;
        for candidate in candidates {
            deleted += sqlx::query(sqlx::AssertSqlSafe(delete_sql.as_str()))
                .bind(candidate.get::<String, _>("app_id"))
                .bind(candidate.get::<String, _>("channel"))
                .bind(candidate.get::<String, _>("message_serial"))
                .bind(candidate.get::<String, _>("annotation_serial"))
                .bind(before_ms)
                .execute(&mut *transaction)
                .await
                .map_err(|error| {
                    Error::Internal(format!("Failed to purge annotation event: {error}"))
                })?
                .rows_affected();
        }
        for request in &requests {
            let remaining = self
                .events_in_transaction(&mut transaction, request)
                .await?;
            if remaining.is_empty() {
                let sql = format!(
                    "DELETE FROM `{}` WHERE app_id = ? AND channel = ? AND message_serial = ? AND annotation_type = ?",
                    self.tables.annotation_projections
                );
                sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(&request.app_id)
                    .bind(&request.channel_id)
                    .bind(request.message_serial.as_str())
                    .bind(request.annotation_type.as_str())
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| {
                        Error::Internal(format!(
                            "Failed to delete expired annotation projection: {error}"
                        ))
                    })?;
            } else {
                self.persist_projection(
                    &mut transaction,
                    request,
                    AnnotationProjectionOptions::default(),
                )
                .await?;
            }
        }
        let has_more_sql = format!(
            "SELECT EXISTS(SELECT 1 FROM `{}` WHERE created_at_ms < ?) AS has_more",
            self.tables.annotation_events
        );
        let has_more = sqlx::query(sqlx::AssertSqlSafe(has_more_sql.as_str()))
            .bind(before_ms)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to inspect annotation purge backlog: {error}"
                ))
            })?
            .get::<bool, _>("has_more");
        transaction.commit().await.map_err(|error| {
            Error::Internal(format!("Failed to commit annotation purge: {error}"))
        })?;
        Ok((deleted, has_more))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::annotations::{
        AnnotationId, AnnotationSummary, AnnotationType, TotalAnnotationSummary,
    };
    use std::sync::Arc;

    fn database() -> DatabaseConnection {
        DatabaseConnection {
            host: "127.0.0.1".to_string(),
            port: 13306,
            username: "root".to_string(),
            password: "root123".to_string(),
            database: "sockudo".to_string(),
            ..DatabaseConnection::default()
        }
    }

    async fn mysql_available() -> bool {
        MySqlPoolOptions::new()
            .max_connections(1)
            .connect("mysql://root:root123@127.0.0.1:13306/sockudo")
            .await
            .is_ok()
    }

    fn create_event(
        id: &str,
        serial: AnnotationSerial,
        message_serial: &str,
        count: u64,
        stored_at_ms: i64,
    ) -> StoredAnnotationEvent {
        StoredAnnotationEvent {
            app_id: "app".to_string(),
            channel_id: "channel".to_string(),
            annotation: Annotation {
                id: AnnotationId::new(id).unwrap(),
                action: AnnotationAction::Create,
                serial,
                message_serial: MessageSerial::new(message_serial).unwrap(),
                annotation_type: AnnotationType::new("reaction:total.v1").unwrap(),
                name: None,
                client_id: Some("client-a".to_string()),
                count: Some(count),
                data: None,
                encoding: None,
                timestamp: now_ms(),
            },
            stored_at_ms,
        }
    }

    async fn reserve(store: &MysqlAnnotationStore) -> AnnotationSerial {
        store
            .reserve_annotation_serial("app", "channel")
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn two_mysql_nodes_allocate_and_apply_annotations_atomically() {
        if !mysql_available().await {
            eprintln!("Skipping test: MySQL test service is unavailable");
            return;
        }
        let prefix = format!(
            "sockudo_at_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        let pooling = DatabasePooling {
            enabled: true,
            min: 0,
            max: 4,
        };
        let first = Arc::new(
            MysqlAnnotationStore::new(&database(), &pooling, &prefix)
                .await
                .unwrap(),
        );
        let second = Arc::new(
            MysqlAnnotationStore::new(&database(), &pooling, &prefix)
                .await
                .unwrap(),
        );

        let first_reservations = {
            let store = Arc::clone(&first);
            tokio::spawn(async move {
                let mut serials = Vec::new();
                for _ in 0..50 {
                    serials.push(reserve(&store).await);
                }
                serials
            })
        };
        let second_reservations = {
            let store = Arc::clone(&second);
            tokio::spawn(async move {
                let mut serials = Vec::new();
                for _ in 0..50 {
                    serials.push(reserve(&store).await);
                }
                serials
            })
        };
        let mut serials = first_reservations.await.unwrap();
        serials.extend(second_reservations.await.unwrap());
        serials.sort();
        serials.dedup();
        assert_eq!(serials.len(), 100);
        assert_eq!(
            serials.first().unwrap().as_str(),
            "ann:00000000000000000001"
        );
        assert_eq!(serials.last().unwrap().as_str(), "ann:00000000000000000100");

        let first_event = create_event("stable-id", reserve(&first).await, "msg:1", 1, now_ms());
        let second_event = create_event("stable-id", reserve(&second).await, "msg:1", 1, now_ms());
        let (first_outcome, second_outcome) = tokio::join!(
            first.append_create_idempotent(first_event),
            second.append_create_idempotent(second_event),
        );
        let first_outcome = first_outcome.unwrap();
        let second_outcome = second_outcome.unwrap();
        assert_ne!(first_outcome.inserted, second_outcome.inserted);
        assert_eq!(
            first_outcome.canonical_serial,
            second_outcome.canonical_serial
        );

        let conflict = create_event("stable-id", reserve(&first).await, "msg:1", 2, now_ms());
        assert!(first.append_create_idempotent(conflict).await.is_err());

        let distinct_a = create_event("distinct-a", reserve(&first).await, "msg:2", 1, now_ms());
        let distinct_b = create_event("distinct-b", reserve(&second).await, "msg:2", 1, now_ms());
        let (distinct_a, distinct_b) = tokio::join!(
            first.append_create_idempotent(distinct_a),
            second.append_create_idempotent(distinct_b),
        );
        assert!(distinct_a.unwrap().inserted);
        distinct_b.unwrap();
        let projection = second
            .get_projection(AnnotationProjectionRequest {
                app_id: "app".to_string(),
                channel_id: "channel".to_string(),
                message_serial: MessageSerial::new("msg:2").unwrap(),
                annotation_type: AnnotationType::new("reaction:total.v1").unwrap(),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            projection.summary,
            AnnotationSummary::Total(TotalAnnotationSummary { total: 2 })
        );

        let replayed = second
            .replay_raw(RawAnnotationReplayRequest {
                app_id: "app".to_string(),
                channel_id: "channel".to_string(),
                message_serial: Some(MessageSerial::new("msg:2").unwrap()),
                after_annotation_serial: None,
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(replayed.len(), 2);

        let expired = create_event("expired", reserve(&first).await, "msg:3", 1, 1);
        first.append_create_idempotent(expired).await.unwrap();
        let (deleted, has_more) = second.purge_before(2, 10).await.unwrap();
        assert_eq!(deleted, 1);
        assert!(!has_more);
        assert!(
            first
                .get_projection(AnnotationProjectionRequest {
                    app_id: "app".to_string(),
                    channel_id: "channel".to_string(),
                    message_serial: MessageSerial::new("msg:3").unwrap(),
                    annotation_type: AnnotationType::new("reaction:total.v1").unwrap(),
                })
                .await
                .unwrap()
                .is_none()
        );

        for table in [
            &first.tables.annotation_projections,
            &first.tables.annotation_events,
            &first.tables.annotation_streams,
        ] {
            let sql = format!("DROP TABLE IF EXISTS `{table}`");
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&first.pool)
                .await
                .unwrap();
        }
    }
}
