use super::{HistoryTables, lock_postgres_schema, unlock_postgres_schema};
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
use sockudo_core::versioned_messages::MessageSerial;
use sqlx::{PgPool, Postgres, Row, Transaction, postgres::PgPoolOptions};
use std::collections::BTreeSet;
use std::time::Duration;

const MAX_CHANNEL_PROJECTIONS: usize = 10_000;
const MAX_PROJECTION_EVENTS: usize = 100_000;
const MAX_RAW_REPLAY_EVENTS: usize = 10_000;

pub(in crate::history) struct PostgresAnnotationStore {
    pool: PgPool,
    tables: HistoryTables,
}

impl PostgresAnnotationStore {
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
        let mut options = PgPoolOptions::new();
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
                    "Failed to connect annotation store to PostgreSQL: {error}"
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

    async fn ensure_tables(&self) -> Result<()> {
        let create_streams = format!(
            "CREATE TABLE IF NOT EXISTS {} (app_id TEXT NOT NULL, channel TEXT NOT NULL, next_serial BIGINT NOT NULL, updated_at_ms BIGINT NOT NULL, PRIMARY KEY (app_id, channel))",
            self.tables.annotation_streams
        );
        let create_events = format!(
            "CREATE TABLE IF NOT EXISTS {} (app_id TEXT NOT NULL, channel TEXT NOT NULL, message_serial TEXT NOT NULL, annotation_serial TEXT NOT NULL, annotation_type TEXT NOT NULL, annotation_id TEXT NULL, name TEXT NULL, client_id TEXT NULL, count_value BIGINT NULL, action TEXT NOT NULL, data_bytes BYTEA NULL, encoding TEXT NULL, annotation_timestamp_ms BIGINT NOT NULL, payload_bytes BYTEA NOT NULL, payload_size_bytes BIGINT NOT NULL, created_at_ms BIGINT NOT NULL, PRIMARY KEY (app_id, channel, message_serial, annotation_serial))",
            self.tables.annotation_events
        );
        let create_projections = format!(
            "CREATE TABLE IF NOT EXISTS {} (app_id TEXT NOT NULL, channel TEXT NOT NULL, message_serial TEXT NOT NULL, annotation_type TEXT NOT NULL, summary_json JSONB NOT NULL, last_annotation_serial TEXT NULL, updated_at_ms BIGINT NOT NULL, PRIMARY KEY (app_id, channel, message_serial, annotation_type))",
            self.tables.annotation_projections
        );
        let add_annotation_id = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS annotation_id TEXT NULL",
            self.tables.annotation_events
        );
        let projection_index = format!(
            "CREATE INDEX IF NOT EXISTS {0}_summary_idx ON {0} (app_id, channel, message_serial, annotation_type, annotation_serial)",
            self.tables.annotation_events
        );
        let raw_index = format!(
            "CREATE INDEX IF NOT EXISTS {0}_raw_replay_idx ON {0} (app_id, channel, annotation_serial)",
            self.tables.annotation_events
        );
        let created_index = format!(
            "CREATE INDEX IF NOT EXISTS {0}_created_at_idx ON {0} (created_at_ms)",
            self.tables.annotation_events
        );
        let create_id_index = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {0}_create_id_uidx ON {0} (app_id, channel, message_serial, annotation_type, annotation_id) WHERE action = 'annotation.create' AND annotation_id IS NOT NULL",
            self.tables.annotation_events
        );

        let mut connection = self.pool.acquire().await.map_err(|error| {
            Error::Internal(format!(
                "Failed to acquire annotation schema connection: {error}"
            ))
        })?;
        lock_postgres_schema(&mut connection, "sockudo_annotation_schema").await?;
        let result: Result<()> = async {
            for sql in [
                create_streams,
                create_events,
                create_projections,
                add_annotation_id,
                projection_index,
                raw_index,
                created_index,
                create_id_index,
            ] {
                sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                    .execute(&mut *connection)
                    .await
                    .map_err(|error| {
                        Error::Internal(format!("Failed to initialize annotation tables: {error}"))
                    })?;
            }
            Ok(())
        }
        .await;
        unlock_postgres_schema(&mut connection, "sockudo_annotation_schema").await?;
        result
    }

    async fn lock_projection(
        transaction: &mut Transaction<'_, Postgres>,
        request: &AnnotationProjectionRequest,
    ) -> Result<()> {
        let lock_key = format!(
            "{}:{}{}:{}{}:{}{}:{}",
            request.app_id.len(),
            request.app_id,
            request.channel_id.len(),
            request.channel_id,
            request.message_serial.as_str().len(),
            request.message_serial.as_str(),
            request.annotation_type.as_str().len(),
            request.annotation_type.as_str()
        );
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(lock_key)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to lock annotation projection: {error}"))
            })?;
        Ok(())
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

    async fn events_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        request: &AnnotationProjectionRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        let sql = format!(
            "SELECT payload_bytes FROM {} WHERE app_id = $1 AND channel = $2 AND message_serial = $3 AND annotation_type = $4 ORDER BY annotation_serial ASC LIMIT $5",
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
        transaction: &mut Transaction<'_, Postgres>,
        request: &AnnotationProjectionRequest,
        options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        let records = self.events_in_transaction(transaction, request).await?;
        let projection = Self::build_projection(request, records, options)?;
        let summary_json = serde_json::to_string(&projection.summary).map_err(|error| {
            Error::Internal(format!("Failed to encode annotation projection: {error}"))
        })?;
        let sql = format!(
            "INSERT INTO {} (app_id, channel, message_serial, annotation_type, summary_json, last_annotation_serial, updated_at_ms) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7) ON CONFLICT (app_id, channel, message_serial, annotation_type) DO UPDATE SET summary_json = EXCLUDED.summary_json, last_annotation_serial = EXCLUDED.last_annotation_serial, updated_at_ms = EXCLUDED.updated_at_ms",
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
        transaction: &mut Transaction<'_, Postgres>,
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
            "INSERT INTO {} (app_id, channel, message_serial, annotation_serial, annotation_type, annotation_id, name, client_id, count_value, action, data_bytes, encoding, annotation_timestamp_ms, payload_bytes, payload_size_bytes, created_at_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) ON CONFLICT DO NOTHING",
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

    fn decode_projection_row(
        app_id: &str,
        channel_id: &str,
        row: &sqlx::postgres::PgRow,
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
}

#[async_trait]
impl AnnotationStore for PostgresAnnotationStore {
    async fn reserve_annotation_serial(
        &self,
        app_id: &str,
        channel_id: &str,
    ) -> Result<Option<AnnotationSerial>> {
        let sql = format!(
            "INSERT INTO {} AS stream (app_id, channel, next_serial, updated_at_ms) VALUES ($1, $2, 2, $3) ON CONFLICT (app_id, channel) DO UPDATE SET next_serial = stream.next_serial + 1, updated_at_ms = EXCLUDED.updated_at_ms RETURNING next_serial - 1 AS allocated_serial",
            self.tables.annotation_streams
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(app_id)
            .bind(channel_id)
            .bind(now_ms())
            .fetch_one(&self.pool)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to reserve annotation serial: {error}"))
            })?;
        let allocated = row.get::<i64, _>("allocated_serial");
        if allocated <= 0 {
            return Err(Error::Internal(
                "annotation serial allocator returned a non-positive value".to_string(),
            ));
        }
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
        Self::lock_projection(&mut transaction, &request).await?;
        let inserted = self.insert_record(&mut transaction, &record).await?;
        if !inserted {
            let sql = format!(
                "SELECT payload_bytes FROM {} WHERE app_id = $1 AND channel = $2 AND message_serial = $3 AND annotation_serial = $4",
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
        Self::lock_projection(&mut transaction, &request).await?;
        let existing_sql = format!(
            "SELECT payload_bytes FROM {} WHERE app_id = $1 AND channel = $2 AND message_serial = $3 AND annotation_type = $4 AND annotation_id = $5 AND action = 'annotation.create' LIMIT 1",
            self.tables.annotation_events
        );
        let existing = sqlx::query(sqlx::AssertSqlSafe(existing_sql.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel_id)
            .bind(record.annotation.message_serial.as_str())
            .bind(record.annotation.annotation_type.as_str())
            .bind(record.annotation.id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to check annotation idempotency: {error}"))
            })?;
        let (canonical_serial, inserted) = if let Some(existing) = existing {
            let existing = Self::decode_record(&existing.get::<Vec<u8>, _>("payload_bytes"))?;
            if !Self::create_payload_matches(&existing.annotation, &record.annotation) {
                return Err(Error::InvalidMessageFormat(format!(
                    "Annotation id '{}' was already used with a different payload",
                    record.annotation.id.as_str()
                )));
            }
            (existing.annotation.serial, false)
        } else {
            if !self.insert_record(&mut transaction, &record).await? {
                return Err(Error::InvalidMessageFormat(
                    "annotation identity conflicts with another event".to_string(),
                ));
            }
            (record.annotation.serial.clone(), true)
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
            "SELECT payload_bytes FROM {} WHERE app_id = $1 AND channel = $2 AND message_serial = $3 AND annotation_type = $4 ORDER BY annotation_serial ASC LIMIT $5",
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
                "annotation event list exceeds the {MAX_PROJECTION_EVENTS} item bound"
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
            "SELECT payload_bytes FROM {} WHERE app_id = $1 AND channel = $2 AND ($3::text IS NULL OR message_serial = $3) AND ($4::text IS NULL OR annotation_serial > $4) ORDER BY annotation_serial ASC LIMIT $5",
            self.tables.annotation_events
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&request.app_id)
            .bind(&request.channel_id)
            .bind(request.message_serial.as_ref().map(MessageSerial::as_str))
            .bind(
                request
                    .after_annotation_serial
                    .as_ref()
                    .map(AnnotationSerial::as_str),
            )
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
            "SELECT payload_bytes FROM {} WHERE app_id = $1 AND channel = $2 AND annotation_serial = $3 LIMIT 1",
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
            "SELECT message_serial, annotation_type, summary_json::text AS summary_json, last_annotation_serial, updated_at_ms FROM {} WHERE app_id = $1 AND channel = $2 AND message_serial = $3 AND annotation_type = $4",
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
            "SELECT message_serial, annotation_type, summary_json::text AS summary_json, last_annotation_serial, updated_at_ms FROM {} WHERE app_id = $1 AND channel = $2 ORDER BY message_serial ASC, annotation_type ASC LIMIT $3",
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
        Self::lock_projection(&mut transaction, &request).await?;
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
        let mut transaction = self.pool.begin().await.map_err(|error| {
            Error::Internal(format!("Failed to begin annotation purge: {error}"))
        })?;
        let delete_sql = format!(
            "WITH selected AS (SELECT ctid FROM {} WHERE created_at_ms < $1 ORDER BY created_at_ms ASC LIMIT $2 FOR UPDATE SKIP LOCKED) DELETE FROM {} AS events USING selected WHERE events.ctid = selected.ctid RETURNING events.app_id, events.channel, events.message_serial, events.annotation_type",
            self.tables.annotation_events, self.tables.annotation_events
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(delete_sql.as_str()))
            .bind(before_ms)
            .bind(limit)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to purge annotation events: {error}"))
            })?;
        let deleted = rows.len() as u64;
        let affected = rows
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
        for (app_id, channel_id, message_serial, annotation_type) in affected {
            let request = AnnotationProjectionRequest {
                app_id,
                channel_id,
                message_serial: MessageSerial::new(message_serial)?,
                annotation_type: sockudo_core::annotations::AnnotationType::new(annotation_type)?,
            };
            Self::lock_projection(&mut transaction, &request).await?;
            let remaining = self
                .events_in_transaction(&mut transaction, &request)
                .await?;
            if remaining.is_empty() {
                let sql = format!(
                    "DELETE FROM {} WHERE app_id = $1 AND channel = $2 AND message_serial = $3 AND annotation_type = $4",
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
                    &request,
                    AnnotationProjectionOptions::default(),
                )
                .await?;
            }
        }
        let has_more_sql = format!(
            "SELECT EXISTS(SELECT 1 FROM {} WHERE created_at_ms < $1) AS has_more",
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

    fn database() -> DatabaseConnection {
        DatabaseConnection {
            host: "127.0.0.1".to_string(),
            port: 15432,
            username: "postgres".to_string(),
            password: "postgres123".to_string(),
            database: "sockudo_test".to_string(),
            ..DatabaseConnection::default()
        }
    }

    async fn postgres_available() -> bool {
        PgPoolOptions::new()
            .max_connections(1)
            .connect("postgresql://postgres:postgres123@127.0.0.1:15432/sockudo_test")
            .await
            .is_ok()
    }

    fn create_event(serial: AnnotationSerial) -> StoredAnnotationEvent {
        StoredAnnotationEvent {
            app_id: "app".to_string(),
            channel_id: "channel".to_string(),
            annotation: Annotation {
                id: AnnotationId::new("stable-id").unwrap(),
                action: AnnotationAction::Create,
                serial,
                message_serial: MessageSerial::new("msg:1").unwrap(),
                annotation_type: AnnotationType::new("reaction:total.v1").unwrap(),
                name: None,
                client_id: Some("client-a".to_string()),
                count: Some(1),
                data: None,
                encoding: None,
                timestamp: now_ms(),
            },
            stored_at_ms: now_ms(),
        }
    }

    #[tokio::test]
    async fn two_postgres_nodes_allocate_and_apply_annotations_atomically() {
        if !postgres_available().await {
            eprintln!("Skipping test: PostgreSQL test service is unavailable");
            return;
        }
        let prefix = format!(
            "sockudo_annotation_test_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        let pooling = DatabasePooling {
            enabled: true,
            min: 0,
            max: 4,
        };
        let first = std::sync::Arc::new(
            PostgresAnnotationStore::new(&database(), &pooling, &prefix)
                .await
                .unwrap(),
        );
        let second = std::sync::Arc::new(
            PostgresAnnotationStore::new(&database(), &pooling, &prefix)
                .await
                .unwrap(),
        );

        let first_reservations = {
            let store = std::sync::Arc::clone(&first);
            tokio::spawn(async move {
                let mut serials = Vec::new();
                for _ in 0..50 {
                    serials.push(
                        store
                            .reserve_annotation_serial("app", "channel")
                            .await
                            .unwrap()
                            .unwrap(),
                    );
                }
                serials
            })
        };
        let second_reservations = {
            let store = std::sync::Arc::clone(&second);
            tokio::spawn(async move {
                let mut serials = Vec::new();
                for _ in 0..50 {
                    serials.push(
                        store
                            .reserve_annotation_serial("app", "channel")
                            .await
                            .unwrap()
                            .unwrap(),
                    );
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

        let first_event = create_event(
            first
                .reserve_annotation_serial("app", "channel")
                .await
                .unwrap()
                .unwrap(),
        );
        let second_event = create_event(
            second
                .reserve_annotation_serial("app", "channel")
                .await
                .unwrap()
                .unwrap(),
        );
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
        assert_eq!(
            first
                .get_events(AnnotationEventsRequest {
                    app_id: "app".to_string(),
                    channel_id: "channel".to_string(),
                    message_serial: MessageSerial::new("msg:1").unwrap(),
                    annotation_type: AnnotationType::new("reaction:total.v1").unwrap(),
                })
                .await
                .unwrap()
                .len(),
            1
        );
        let projection = second_outcome.projection;
        assert_eq!(
            projection.summary,
            AnnotationSummary::Total(TotalAnnotationSummary { total: 1 })
        );

        for table in [
            &first.tables.annotation_projections,
            &first.tables.annotation_events,
            &first.tables.annotation_streams,
        ] {
            let sql = format!("DROP TABLE IF EXISTS {table}");
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&first.pool)
                .await
                .unwrap();
        }
    }
}
