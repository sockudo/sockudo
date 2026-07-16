use async_trait::async_trait;
use futures_util::TryStreamExt;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::deserialize::row::ColumnIterator;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::{Consistency, SerialConsistency, Statement};
use sockudo_core::annotations::{
    Annotation, AnnotationAction, AnnotationAppendOutcome, AnnotationEventLookupRequest,
    AnnotationEventsRequest, AnnotationProjection, AnnotationProjectionOptions,
    AnnotationProjectionRequest, AnnotationProjectionsForChannelRequest, AnnotationSerial,
    AnnotationStore, AnnotationSummary, AnnotationType, RawAnnotationReplayRequest,
    StoredAnnotationEvent, StoredAnnotationProjection,
};
use sockudo_core::error::{Error, Result};
use sockudo_core::history::now_ms;
use sockudo_core::options::ScyllaDbSettings;
use sockudo_core::versioned_messages::MessageSerial;
use std::sync::Arc;

use super::{ensure_scylla_keyspace, validate_cql_identifier};

const MAX_CAS_ATTEMPTS: usize = 8;
const MAX_PROJECTION_EVENTS: usize = 10_000;
const MAX_CHANNEL_PROJECTIONS: usize = 10_000;
const MAX_RAW_REPLAY: usize = 10_000;

pub(super) struct ScyllaAnnotationStore {
    session: Arc<Session>,
    keyspace: String,
    table: String,
    retention_seconds: u64,
}

#[derive(Clone)]
struct ProjectionState {
    projection: Option<StoredAnnotationProjection>,
    revision: u64,
    valid_until: Option<i64>,
}

struct ProjectionBuild {
    projection: StoredAnnotationProjection,
    valid_until: Option<i64>,
    expires_at: Option<i64>,
    event_count: usize,
}

pub(crate) async fn create_scylla_annotation_store(
    db_config: &ScyllaDbSettings,
    table_prefix: &str,
    retention_seconds: u64,
) -> Result<Arc<dyn AnnotationStore + Send + Sync>> {
    Ok(Arc::new(
        ScyllaAnnotationStore::new(db_config, table_prefix, retention_seconds).await?,
    ))
}

impl ScyllaAnnotationStore {
    pub(super) async fn new(
        db_config: &ScyllaDbSettings,
        table_prefix: &str,
        retention_seconds: u64,
    ) -> Result<Self> {
        validate_cql_identifier(table_prefix, "ScyllaDB annotation table prefix")?;
        let mut builder = SessionBuilder::new().known_nodes(db_config.nodes.clone());
        if let (Some(username), Some(password)) = (&db_config.username, &db_config.password) {
            builder = builder.user(username, password);
        }
        let session = Arc::new(builder.build().await.map_err(|error| {
            Error::Internal(format!(
                "Failed to connect annotation store to ScyllaDB: {error}"
            ))
        })?);
        Self::from_session(db_config, table_prefix, retention_seconds, session).await
    }

    async fn from_session(
        db_config: &ScyllaDbSettings,
        table_prefix: &str,
        retention_seconds: u64,
        session: Arc<Session>,
    ) -> Result<Self> {
        let keyspace = if db_config.keyspace.trim().is_empty() {
            "sockudo".to_string()
        } else {
            db_config.keyspace.clone()
        };
        ensure_scylla_keyspace(
            &session,
            &keyspace,
            &db_config.replication_class,
            db_config.replication_factor,
        )
        .await?;
        let store = Self {
            session,
            keyspace,
            table: format!("{table_prefix}_annotation_commits"),
            retention_seconds,
        };
        store.ensure_table().await?;
        Ok(store)
    }

    fn table_fq(&self) -> String {
        format!("{}.{}", self.keyspace, self.table)
    }

    fn quorum_statement(query: String) -> Statement {
        let mut statement = Statement::new(query);
        statement.set_consistency(Consistency::Quorum);
        statement
    }

    fn serial_statement(query: String) -> Statement {
        let mut statement = Self::quorum_statement(query);
        statement.set_serial_consistency(Some(SerialConsistency::Serial));
        statement
    }

    fn ttl_clause(&self) -> String {
        if self.retention_seconds == 0 {
            String::new()
        } else {
            format!(" USING TTL {}", self.retention_seconds)
        }
    }

    fn update_ttl_clause(&self) -> String {
        if self.retention_seconds == 0 {
            String::new()
        } else {
            format!("USING TTL {} ", self.retention_seconds)
        }
    }

    fn composite_key<const N: usize>(parts: [&str; N]) -> String {
        let mut key = String::new();
        for part in parts {
            key.push_str(&part.len().to_string());
            key.push(':');
            key.push_str(part);
        }
        key
    }

    fn event_key(serial: &AnnotationSerial) -> String {
        format!("e:{}", serial.as_str())
    }

    fn message_prefix(message_serial: &MessageSerial) -> String {
        format!("q:{}", Self::composite_key([message_serial.as_str()]))
    }

    fn message_event_key(annotation: &Annotation) -> String {
        format!(
            "{}:{}",
            Self::message_prefix(&annotation.message_serial),
            annotation.serial.as_str()
        )
    }

    fn projection_event_prefix(
        message_serial: &MessageSerial,
        annotation_type: &AnnotationType,
    ) -> String {
        format!(
            "m:{}",
            Self::composite_key([message_serial.as_str(), annotation_type.as_str()])
        )
    }

    fn projection_event_key(annotation: &Annotation) -> String {
        format!(
            "{}:{}",
            Self::projection_event_prefix(&annotation.message_serial, &annotation.annotation_type),
            annotation.serial.as_str()
        )
    }

    fn projection_key(request: &AnnotationProjectionRequest) -> String {
        format!(
            "p:{}",
            Self::composite_key([
                request.message_serial.as_str(),
                request.annotation_type.as_str()
            ])
        )
    }

    fn idempotency_key(annotation: &Annotation) -> String {
        format!(
            "i:{}",
            Self::composite_key([
                annotation.message_serial.as_str(),
                annotation.annotation_type.as_str(),
                annotation.id.as_str()
            ])
        )
    }

    fn expiration_seconds(&self, stored_at_ms: i64) -> Option<i64> {
        (self.retention_seconds > 0).then(|| {
            stored_at_ms.div_euclid(1_000).saturating_add(
                self.retention_seconds
                    .min(i64::MAX as u64)
                    .try_into()
                    .unwrap_or(i64::MAX),
            )
        })
    }

    fn projection_request(record: &StoredAnnotationEvent) -> AnnotationProjectionRequest {
        AnnotationProjectionRequest {
            app_id: record.app_id.clone(),
            channel_id: record.channel_id.clone(),
            message_serial: record.annotation.message_serial.clone(),
            annotation_type: record.annotation.annotation_type.clone(),
        }
    }

    fn create_payload_matches(existing: &Annotation, incoming: &Annotation) -> bool {
        existing.name == incoming.name
            && existing.client_id == incoming.client_id
            && existing.count == incoming.count
            && existing.data == incoming.data
            && existing.encoding == incoming.encoding
    }

    fn decode_event(payload: &[u8]) -> Result<StoredAnnotationEvent> {
        sonic_rs::from_slice(payload).map_err(|error| {
            Error::Internal(format!(
                "Failed to decode ScyllaDB annotation event: {error}"
            ))
        })
    }

    fn conditional_applied(result: scylla::response::query_result::QueryResult) -> Result<bool> {
        let rows = result.into_rows_result().map_err(|error| {
            Error::Internal(format!("Failed to decode ScyllaDB annotation LWT: {error}"))
        })?;
        let row_iter = rows.rows::<ColumnIterator>().map_err(|error| {
            Error::Internal(format!(
                "Failed to deserialize ScyllaDB annotation LWT: {error}"
            ))
        })?;
        let mut saw_row = false;
        for row in row_iter {
            let mut columns = row.map_err(|error| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB annotation LWT row: {error}"
                ))
            })?;
            let applied = columns
                .next()
                .transpose()
                .map_err(|error| {
                    Error::Internal(format!(
                        "Failed to read ScyllaDB annotation LWT result: {error}"
                    ))
                })?
                .and_then(|column| column.slice)
                .is_some_and(|slice| slice.as_slice() == [1]);
            saw_row = true;
            if !applied {
                return Ok(false);
            }
        }
        if !saw_row {
            return Err(Error::Internal(
                "ScyllaDB annotation LWT returned no applied rows".to_string(),
            ));
        }
        Ok(true)
    }

    async fn ensure_table(&self) -> Result<()> {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                app_id text,
                channel text,
                commit_key text,
                payload_bytes blob,
                summary_bytes blob,
                message_serial text,
                annotation_type text,
                annotation_serial text,
                last_annotation_serial text,
                revision bigint,
                next_annotation_serial bigint,
                valid_until_sec bigint,
                expires_at_sec bigint,
                created_at_ms bigint,
                updated_at_ms bigint,
                PRIMARY KEY ((app_id, channel), commit_key)
            ) WITH CLUSTERING ORDER BY (commit_key ASC)",
            self.table_fq()
        );
        self.session
            .query_unpaged(query, ())
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to create ScyllaDB annotation table: {error}"
                ))
            })?;
        Ok(())
    }

    async fn row_payload(
        &self,
        app_id: &str,
        channel_id: &str,
        commit_key: &str,
    ) -> Result<Option<(Vec<u8>, Option<i64>)>> {
        let query = Self::quorum_statement(format!(
            "SELECT payload_bytes, expires_at_sec FROM {} WHERE app_id = ? AND channel = ? AND commit_key = ?",
            self.table_fq()
        ));
        let rows = self
            .session
            .query_unpaged(query, (app_id, channel_id, commit_key))
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to read ScyllaDB annotation row: {error}"))
            })?
            .into_rows_result()
            .map_err(|error| {
                Error::Internal(format!("Failed to decode ScyllaDB annotation row: {error}"))
            })?;
        let row = rows
            .maybe_first_row::<(Option<Vec<u8>>, Option<i64>)>()
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB annotation row: {error}"
                ))
            })?;
        Ok(row.and_then(|(payload, expires_at)| payload.map(|payload| (payload, expires_at))))
    }

    async fn query_event_prefix(
        &self,
        app_id: &str,
        channel_id: &str,
        prefix: &str,
        after_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(StoredAnnotationEvent, Option<i64>)>> {
        let lower = after_key.unwrap_or(prefix);
        let upper = format!("{prefix};");
        let comparator = if after_key.is_some() { ">" } else { ">=" };
        let query = Self::quorum_statement(format!(
            "SELECT commit_key, payload_bytes, expires_at_sec FROM {} WHERE app_id = ? AND channel = ? AND commit_key {comparator} ? AND commit_key < ?",
            self.table_fq()
        ));
        let pager = self
            .session
            .query_iter(query, (app_id, channel_id, lower, upper.as_str()))
            .await
            .map_err(|error| {
                Error::Internal(format!("Failed to query ScyllaDB annotations: {error}"))
            })?;
        let mut rows = pager
            .rows_stream::<(String, Vec<u8>, Option<i64>)>()
            .map_err(|error| {
                Error::Internal(format!("Failed to stream ScyllaDB annotations: {error}"))
            })?;
        let now_seconds = now_ms().div_euclid(1_000);
        let mut result = Vec::with_capacity(limit.min(256));
        while let Some((_key, payload, expires_at)) = rows.try_next().await.map_err(|error| {
            Error::Internal(format!(
                "Failed to read ScyllaDB annotation stream: {error}"
            ))
        })? {
            if expires_at.is_some_and(|expiry| expiry <= now_seconds) {
                continue;
            }
            result.push((Self::decode_event(&payload)?, expires_at));
            if result.len() >= limit {
                break;
            }
        }
        Ok(result)
    }

    async fn events_for_projection(
        &self,
        request: &AnnotationProjectionRequest,
    ) -> Result<Vec<(StoredAnnotationEvent, Option<i64>)>> {
        let events = self
            .query_event_prefix(
                &request.app_id,
                &request.channel_id,
                &Self::projection_event_prefix(&request.message_serial, &request.annotation_type),
                None,
                MAX_PROJECTION_EVENTS.saturating_add(1),
            )
            .await?;
        if events.len() > MAX_PROJECTION_EVENTS {
            return Err(Error::BufferFull(format!(
                "annotation projection exceeds the {MAX_PROJECTION_EVENTS} event bound"
            )));
        }
        Ok(events)
    }

    fn build_projection(
        request: &AnnotationProjectionRequest,
        events: &[(StoredAnnotationEvent, Option<i64>)],
        options: AnnotationProjectionOptions,
    ) -> Result<ProjectionBuild> {
        let projection = AnnotationProjection::rebuild_with_options(
            request.channel_id.clone(),
            request.message_serial.clone(),
            request.annotation_type.clone(),
            events.iter().map(|(event, _)| event.annotation.clone()),
            options,
        )?;
        Ok(ProjectionBuild {
            projection: StoredAnnotationProjection {
                app_id: request.app_id.clone(),
                channel_id: projection.key.channel_id,
                message_serial: projection.key.message_serial,
                annotation_type: projection.key.annotation_type,
                summary: projection.summary,
                last_annotation_serial: projection.last_serial,
                updated_at_ms: now_ms(),
            },
            valid_until: events.iter().filter_map(|(_, expiry)| *expiry).min(),
            expires_at: events.iter().filter_map(|(_, expiry)| *expiry).max(),
            event_count: events.len(),
        })
    }

    async fn initialize_projection(&self, request: &AnnotationProjectionRequest) -> Result<()> {
        let query = Self::serial_statement(format!(
            "INSERT INTO {} (app_id, channel, commit_key, message_serial, annotation_type, revision, updated_at_ms) VALUES (?, ?, ?, ?, ?, 0, ?) IF NOT EXISTS{}",
            self.table_fq(),
            self.ttl_clause()
        ));
        self.session
            .query_unpaged(
                query,
                (
                    &request.app_id,
                    &request.channel_id,
                    Self::projection_key(request),
                    request.message_serial.as_str(),
                    request.annotation_type.as_str(),
                    now_ms(),
                ),
            )
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to initialize ScyllaDB annotation projection: {error}"
                ))
            })?;
        Ok(())
    }

    async fn projection_state(
        &self,
        request: &AnnotationProjectionRequest,
    ) -> Result<Option<ProjectionState>> {
        let query = Self::quorum_statement(format!(
            "SELECT summary_bytes, message_serial, annotation_type, last_annotation_serial, updated_at_ms, revision, valid_until_sec FROM {} WHERE app_id = ? AND channel = ? AND commit_key = ?",
            self.table_fq()
        ));
        let rows = self
            .session
            .query_unpaged(
                query,
                (
                    &request.app_id,
                    &request.channel_id,
                    Self::projection_key(request),
                ),
            )
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to read ScyllaDB annotation projection: {error}"
                ))
            })?
            .into_rows_result()
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to decode ScyllaDB annotation projection: {error}"
                ))
            })?;
        let Some((
            summary,
            message_serial,
            annotation_type,
            last_serial,
            updated_at,
            revision,
            valid_until,
        )) = rows
            .maybe_first_row::<(
                Option<Vec<u8>>,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<i64>,
                Option<i64>,
                Option<i64>,
            )>()
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB annotation projection: {error}"
                ))
            })?
        else {
            return Ok(None);
        };
        let projection = match (summary, message_serial, annotation_type) {
            (Some(summary), Some(message_serial), Some(annotation_type)) => {
                Some(StoredAnnotationProjection {
                    app_id: request.app_id.clone(),
                    channel_id: request.channel_id.clone(),
                    message_serial: MessageSerial::new(message_serial)?,
                    annotation_type: AnnotationType::new(annotation_type)?,
                    summary: sonic_rs::from_slice::<AnnotationSummary>(&summary).map_err(
                        |error| {
                            Error::Internal(format!(
                                "Failed to decode ScyllaDB annotation summary: {error}"
                            ))
                        },
                    )?,
                    last_annotation_serial: last_serial.map(AnnotationSerial::new).transpose()?,
                    updated_at_ms: updated_at.unwrap_or_default(),
                })
            }
            _ => None,
        };
        Ok(Some(ProjectionState {
            projection,
            revision: revision.unwrap_or_default().max(0) as u64,
            valid_until,
        }))
    }

    async fn canonical_create(
        &self,
        record: &StoredAnnotationEvent,
    ) -> Result<Option<StoredAnnotationEvent>> {
        let Some((payload, expires_at)) = self
            .row_payload(
                &record.app_id,
                &record.channel_id,
                &Self::idempotency_key(&record.annotation),
            )
            .await?
        else {
            return Ok(None);
        };
        if expires_at.is_some_and(|expiry| expiry <= now_ms().div_euclid(1_000)) {
            return Ok(None);
        }
        let canonical = Self::decode_event(&payload)?;
        if !Self::create_payload_matches(&canonical.annotation, &record.annotation) {
            return Err(Error::InvalidMessageFormat(format!(
                "Annotation id '{}' was already used with a different payload",
                record.annotation.id.as_str()
            )));
        }
        Ok(Some(canonical))
    }

    async fn committed_event(
        &self,
        record: &StoredAnnotationEvent,
    ) -> Result<Option<StoredAnnotationEvent>> {
        let Some((payload, expires_at)) = self
            .row_payload(
                &record.app_id,
                &record.channel_id,
                &Self::event_key(&record.annotation.serial),
            )
            .await?
        else {
            return Ok(None);
        };
        if expires_at.is_some_and(|expiry| expiry <= now_ms().div_euclid(1_000)) {
            return Ok(None);
        }
        let existing = Self::decode_event(&payload)?;
        if existing.annotation != record.annotation {
            return Err(Error::InvalidMessageFormat(
                "annotation serial was already used with a different payload".to_string(),
            ));
        }
        Ok(Some(existing))
    }

    async fn append_internal(
        &self,
        mut record: StoredAnnotationEvent,
        create_idempotent: bool,
    ) -> Result<AnnotationAppendOutcome> {
        record.validate()?;
        if record.stored_at_ms == 0 {
            record.stored_at_ms = now_ms();
        }
        let request = Self::projection_request(&record);
        let payload = sonic_rs::to_vec(&record).map_err(|error| {
            Error::Internal(format!(
                "Failed to encode ScyllaDB annotation event: {error}"
            ))
        })?;
        let expires_at = self.expiration_seconds(record.stored_at_ms);

        for _ in 0..MAX_CAS_ATTEMPTS {
            if create_idempotent && let Some(canonical) = self.canonical_create(&record).await? {
                let projection = self.get_projection(request.clone()).await?.ok_or_else(|| {
                    Error::Internal(
                        "canonical annotation create is missing its projection".to_string(),
                    )
                })?;
                return Ok(AnnotationAppendOutcome {
                    projection,
                    canonical_serial: canonical.annotation.serial,
                    inserted: false,
                });
            }
            if self.committed_event(&record).await?.is_some() {
                let projection = self.get_projection(request.clone()).await?.ok_or_else(|| {
                    Error::Internal(
                        "canonical annotation event is missing its projection".to_string(),
                    )
                })?;
                return Ok(AnnotationAppendOutcome {
                    projection,
                    canonical_serial: record.annotation.serial.clone(),
                    inserted: false,
                });
            }

            self.initialize_projection(&request).await?;
            let state = self.projection_state(&request).await?.ok_or_else(|| {
                Error::Internal("ScyllaDB projection initializer was not visible".to_string())
            })?;
            let mut events = self.events_for_projection(&request).await?;
            if events.len() >= MAX_PROJECTION_EVENTS {
                return Err(Error::BufferFull(format!(
                    "annotation projection exceeds the {MAX_PROJECTION_EVENTS} event bound"
                )));
            }
            events.push((record.clone(), expires_at));
            let build =
                Self::build_projection(&request, &events, AnnotationProjectionOptions::default())?;
            let summary = sonic_rs::to_vec(&build.projection.summary).map_err(|error| {
                Error::Internal(format!(
                    "Failed to encode ScyllaDB annotation summary: {error}"
                ))
            })?;
            let table = self.table_fq();
            let insert = format!(
                "INSERT INTO {table} (app_id, channel, commit_key, payload_bytes, message_serial, annotation_type, annotation_serial, expires_at_sec, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS{}",
                self.ttl_clause()
            );
            let projection_update = format!(
                "UPDATE {table} {}SET summary_bytes = ?, last_annotation_serial = ?, revision = ?, valid_until_sec = ?, expires_at_sec = ?, updated_at_ms = ? WHERE app_id = ? AND channel = ? AND commit_key = ? IF revision = ?",
                self.update_ttl_clause()
            );
            let mut batch = Batch::new(BatchType::Logged);
            batch.set_consistency(Consistency::Quorum);
            batch.set_serial_consistency(Some(SerialConsistency::Serial));
            batch.append_statement(Statement::new(insert.clone()));
            batch.append_statement(Statement::new(insert.clone()));
            batch.append_statement(Statement::new(insert.clone()));
            batch.append_statement(Statement::new(projection_update));
            if create_idempotent {
                batch.append_statement(Statement::new(insert));
            }
            let base = (
                (
                    &record.app_id,
                    &record.channel_id,
                    Self::event_key(&record.annotation.serial),
                    payload.as_slice(),
                    record.annotation.message_serial.as_str(),
                    record.annotation.annotation_type.as_str(),
                    record.annotation.serial.as_str(),
                    expires_at,
                    record.stored_at_ms,
                ),
                (
                    &record.app_id,
                    &record.channel_id,
                    Self::message_event_key(&record.annotation),
                    payload.as_slice(),
                    record.annotation.message_serial.as_str(),
                    record.annotation.annotation_type.as_str(),
                    record.annotation.serial.as_str(),
                    expires_at,
                    record.stored_at_ms,
                ),
                (
                    &record.app_id,
                    &record.channel_id,
                    Self::projection_event_key(&record.annotation),
                    payload.as_slice(),
                    record.annotation.message_serial.as_str(),
                    record.annotation.annotation_type.as_str(),
                    record.annotation.serial.as_str(),
                    expires_at,
                    record.stored_at_ms,
                ),
                (
                    summary.as_slice(),
                    build
                        .projection
                        .last_annotation_serial
                        .as_ref()
                        .map(AnnotationSerial::as_str),
                    state.revision as i64 + 1,
                    build.valid_until,
                    build.expires_at,
                    build.projection.updated_at_ms,
                    &record.app_id,
                    &record.channel_id,
                    Self::projection_key(&request),
                    state.revision as i64,
                ),
            );
            let result = if create_idempotent {
                self.session
                    .batch(
                        &batch,
                        (
                            base.0,
                            base.1,
                            base.2,
                            base.3,
                            (
                                &record.app_id,
                                &record.channel_id,
                                Self::idempotency_key(&record.annotation),
                                payload.as_slice(),
                                record.annotation.message_serial.as_str(),
                                record.annotation.annotation_type.as_str(),
                                record.annotation.serial.as_str(),
                                expires_at,
                                record.stored_at_ms,
                            ),
                        ),
                    )
                    .await
            } else {
                self.session.batch(&batch, base).await
            };
            match result {
                Ok(result) => {
                    if Self::conditional_applied(result)? {
                        return Ok(AnnotationAppendOutcome {
                            projection: build.projection,
                            canonical_serial: record.annotation.serial.clone(),
                            inserted: true,
                        });
                    }
                    continue;
                }
                Err(error) => {
                    if create_idempotent
                        && let Some(canonical) = self.canonical_create(&record).await?
                    {
                        let projection =
                            self.get_projection(request.clone()).await?.ok_or_else(|| {
                                Error::Internal(
                                    "committed annotation is missing its projection".to_string(),
                                )
                            })?;
                        return Ok(AnnotationAppendOutcome {
                            projection,
                            canonical_serial: canonical.annotation.serial,
                            inserted: false,
                        });
                    }
                    if self.committed_event(&record).await?.is_some() {
                        let projection =
                            self.get_projection(request.clone()).await?.ok_or_else(|| {
                                Error::Internal(
                                    "committed annotation is missing its projection".to_string(),
                                )
                            })?;
                        return Ok(AnnotationAppendOutcome {
                            projection,
                            canonical_serial: record.annotation.serial.clone(),
                            inserted: false,
                        });
                    }
                    return Err(Error::Internal(format!(
                        "Failed to transact ScyllaDB annotation event: {error}"
                    )));
                }
            }
        }
        Err(Error::Internal(
            "ScyllaDB annotation compare-and-apply contention exceeded the retry bound".to_string(),
        ))
    }

    async fn persist_rebuild(
        &self,
        request: &AnnotationProjectionRequest,
        options: AnnotationProjectionOptions,
    ) -> Result<ProjectionBuild> {
        for _ in 0..MAX_CAS_ATTEMPTS {
            self.initialize_projection(request).await?;
            let state = self.projection_state(request).await?.ok_or_else(|| {
                Error::Internal("ScyllaDB projection initializer was not visible".to_string())
            })?;
            let events = self.events_for_projection(request).await?;
            let build = Self::build_projection(request, &events, options)?;
            let query = if build.event_count == 0 {
                format!(
                    "DELETE FROM {} WHERE app_id = ? AND channel = ? AND commit_key = ? IF revision = ?",
                    self.table_fq()
                )
            } else {
                format!(
                    "UPDATE {} {}SET summary_bytes = ?, last_annotation_serial = ?, revision = ?, valid_until_sec = ?, expires_at_sec = ?, updated_at_ms = ? WHERE app_id = ? AND channel = ? AND commit_key = ? IF revision = ?",
                    self.table_fq(),
                    self.update_ttl_clause()
                )
            };
            let statement = Self::serial_statement(query);
            let result = if build.event_count == 0 {
                self.session
                    .query_unpaged(
                        statement,
                        (
                            &request.app_id,
                            &request.channel_id,
                            Self::projection_key(request),
                            state.revision as i64,
                        ),
                    )
                    .await
            } else {
                let summary = sonic_rs::to_vec(&build.projection.summary).map_err(|error| {
                    Error::Internal(format!(
                        "Failed to encode ScyllaDB annotation summary: {error}"
                    ))
                })?;
                self.session
                    .query_unpaged(
                        statement,
                        (
                            summary,
                            build
                                .projection
                                .last_annotation_serial
                                .as_ref()
                                .map(AnnotationSerial::as_str),
                            state.revision as i64 + 1,
                            build.valid_until,
                            build.expires_at,
                            build.projection.updated_at_ms,
                            &request.app_id,
                            &request.channel_id,
                            Self::projection_key(request),
                            state.revision as i64,
                        ),
                    )
                    .await
            }
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to rebuild ScyllaDB annotation projection: {error}"
                ))
            })?;
            if Self::conditional_applied(result)? {
                return Ok(build);
            }
        }
        Err(Error::Internal(
            "ScyllaDB projection rebuild contention exceeded the retry bound".to_string(),
        ))
    }
}

#[async_trait]
impl AnnotationStore for ScyllaAnnotationStore {
    async fn reserve_annotation_serial(
        &self,
        app_id: &str,
        channel_id: &str,
    ) -> Result<Option<AnnotationSerial>> {
        let table = self.table_fq();
        let insert = format!(
            "INSERT INTO {table} (app_id, channel, commit_key, next_annotation_serial, updated_at_ms) VALUES (?, ?, 's', 2, ?) IF NOT EXISTS"
        );
        let select = format!(
            "SELECT next_annotation_serial FROM {table} WHERE app_id = ? AND channel = ? AND commit_key = 's'"
        );
        let update = format!(
            "UPDATE {table} SET next_annotation_serial = ?, updated_at_ms = ? WHERE app_id = ? AND channel = ? AND commit_key = 's' IF next_annotation_serial = ?"
        );
        loop {
            let rows = self
                .session
                .query_unpaged(Self::quorum_statement(select.clone()), (app_id, channel_id))
                .await
                .map_err(|error| {
                    Error::Internal(format!(
                        "Failed to read ScyllaDB annotation allocator: {error}"
                    ))
                })?
                .into_rows_result()
                .map_err(|error| {
                    Error::Internal(format!(
                        "Failed to decode ScyllaDB annotation allocator: {error}"
                    ))
                })?;
            if let Some((next_serial,)) = rows.maybe_first_row::<(i64,)>().map_err(|error| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB annotation allocator: {error}"
                ))
            })? {
                if next_serial <= 0 {
                    return Err(Error::Internal(
                        "ScyllaDB annotation allocator returned a non-positive serial".to_string(),
                    ));
                }
                let result = self
                    .session
                    .query_unpaged(
                        Self::serial_statement(update.clone()),
                        (
                            next_serial.saturating_add(1),
                            now_ms(),
                            app_id,
                            channel_id,
                            next_serial,
                        ),
                    )
                    .await
                    .map_err(|error| {
                        Error::Internal(format!(
                            "Failed to advance ScyllaDB annotation allocator: {error}"
                        ))
                    })?;
                if Self::conditional_applied(result)? {
                    return Ok(Some(AnnotationSerial::new(format!(
                        "ann:{next_serial:020}"
                    ))?));
                }
                continue;
            }
            let result = self
                .session
                .query_unpaged(
                    Self::serial_statement(insert.clone()),
                    (app_id, channel_id, now_ms()),
                )
                .await
                .map_err(|error| {
                    Error::Internal(format!(
                        "Failed to initialize ScyllaDB annotation allocator: {error}"
                    ))
                })?;
            if Self::conditional_applied(result)? {
                return Ok(Some(AnnotationSerial::new("ann:00000000000000000001")?));
            }
        }
    }

    async fn append_event(
        &self,
        record: StoredAnnotationEvent,
    ) -> Result<StoredAnnotationProjection> {
        Ok(self.append_internal(record, false).await?.projection)
    }

    async fn append_create_idempotent(
        &self,
        record: StoredAnnotationEvent,
    ) -> Result<AnnotationAppendOutcome> {
        let idempotent = record.annotation.action == AnnotationAction::Create;
        self.append_internal(record, idempotent).await
    }

    async fn get_events(
        &self,
        request: AnnotationEventsRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        request.validate()?;
        let projection = AnnotationProjectionRequest {
            app_id: request.app_id,
            channel_id: request.channel_id,
            message_serial: request.message_serial,
            annotation_type: request.annotation_type,
        };
        Ok(self
            .events_for_projection(&projection)
            .await?
            .into_iter()
            .map(|(event, _)| event)
            .collect())
    }

    async fn replay_raw(
        &self,
        request: RawAnnotationReplayRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        request.validate()?;
        if request.limit > MAX_RAW_REPLAY {
            return Err(Error::BufferFull(format!(
                "annotation replay limit exceeds the {MAX_RAW_REPLAY} item bound"
            )));
        }
        let prefix = request
            .message_serial
            .as_ref()
            .map(Self::message_prefix)
            .unwrap_or_else(|| "e".to_string());
        let after_key = request.after_annotation_serial.as_ref().map(|serial| {
            if let Some(message) = request.message_serial.as_ref() {
                format!("{}:{}", Self::message_prefix(message), serial.as_str())
            } else {
                Self::event_key(serial)
            }
        });
        Ok(self
            .query_event_prefix(
                &request.app_id,
                &request.channel_id,
                &prefix,
                after_key.as_deref(),
                request.limit,
            )
            .await?
            .into_iter()
            .map(|(event, _)| event)
            .collect())
    }

    async fn get_event_by_serial(
        &self,
        request: AnnotationEventLookupRequest,
    ) -> Result<Option<StoredAnnotationEvent>> {
        request.validate()?;
        let Some((payload, expires_at)) = self
            .row_payload(
                &request.app_id,
                &request.channel_id,
                &Self::event_key(&request.annotation_serial),
            )
            .await?
        else {
            return Ok(None);
        };
        if expires_at.is_some_and(|expiry| expiry <= now_ms().div_euclid(1_000)) {
            return Ok(None);
        }
        Ok(Some(Self::decode_event(&payload)?))
    }

    async fn get_projection(
        &self,
        request: AnnotationProjectionRequest,
    ) -> Result<Option<StoredAnnotationProjection>> {
        request.validate()?;
        let Some(state) = self.projection_state(&request).await? else {
            return Ok(None);
        };
        if state
            .valid_until
            .is_some_and(|valid_until| valid_until <= now_ms().div_euclid(1_000))
        {
            let build = self
                .persist_rebuild(&request, AnnotationProjectionOptions::default())
                .await?;
            return Ok((build.event_count > 0).then_some(build.projection));
        }
        Ok(state.projection)
    }

    async fn list_projections_for_channel(
        &self,
        request: AnnotationProjectionsForChannelRequest,
    ) -> Result<Vec<StoredAnnotationProjection>> {
        request.validate()?;
        let lower = "p:";
        let upper = "p;";
        let query = Self::quorum_statement(format!(
            "SELECT summary_bytes, message_serial, annotation_type, last_annotation_serial, updated_at_ms, revision, valid_until_sec FROM {} WHERE app_id = ? AND channel = ? AND commit_key >= ? AND commit_key < ?",
            self.table_fq()
        ));
        let pager = self
            .session
            .query_iter(query, (&request.app_id, &request.channel_id, lower, upper))
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to list ScyllaDB annotation projections: {error}"
                ))
            })?;
        let mut rows = pager
            .rows_stream::<(
                Option<Vec<u8>>,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<i64>,
                Option<i64>,
                Option<i64>,
            )>()
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to stream ScyllaDB annotation projections: {error}"
                ))
            })?;
        let mut projections = Vec::new();
        let mut rows_seen = 0_usize;
        while let Some((
            summary,
            message_serial,
            annotation_type,
            last_serial,
            updated_at,
            _revision,
            valid_until,
        )) = rows.try_next().await.map_err(|error| {
            Error::Internal(format!(
                "Failed to read ScyllaDB annotation projections: {error}"
            ))
        })? {
            rows_seen = rows_seen.saturating_add(1);
            if rows_seen > MAX_CHANNEL_PROJECTIONS {
                return Err(Error::BufferFull(format!(
                    "annotation projection list exceeds the {MAX_CHANNEL_PROJECTIONS} item bound"
                )));
            }
            let (Some(summary), Some(message_serial), Some(annotation_type)) =
                (summary, message_serial, annotation_type)
            else {
                continue;
            };
            let projection_request = AnnotationProjectionRequest {
                app_id: request.app_id.clone(),
                channel_id: request.channel_id.clone(),
                message_serial: MessageSerial::new(message_serial)?,
                annotation_type: AnnotationType::new(annotation_type)?,
            };
            if valid_until.is_some_and(|expiry| expiry <= now_ms().div_euclid(1_000)) {
                if let Some(projection) = self.get_projection(projection_request).await? {
                    projections.push(projection);
                }
            } else {
                projections.push(StoredAnnotationProjection {
                    app_id: request.app_id.clone(),
                    channel_id: request.channel_id.clone(),
                    message_serial: projection_request.message_serial,
                    annotation_type: projection_request.annotation_type,
                    summary: sonic_rs::from_slice(&summary).map_err(|error| {
                        Error::Internal(format!(
                            "Failed to decode ScyllaDB annotation summary: {error}"
                        ))
                    })?,
                    last_annotation_serial: last_serial.map(AnnotationSerial::new).transpose()?,
                    updated_at_ms: updated_at.unwrap_or_default(),
                });
            }
        }
        Ok(projections)
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
        Ok(self.persist_rebuild(&request, options).await?.projection)
    }

    async fn purge_before(&self, _before_ms: i64, _batch_size: usize) -> Result<(u64, bool)> {
        // CQL TTL performs physical cleanup; every read additionally applies
        // the explicit expiry column so tombstone cleanup latency cannot
        // expose stale annotations.
        Ok((0, false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scylla::errors::TranslationError;
    use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
    use sockudo_core::annotations::{
        AnnotationId, AnnotationSummary, AnnotationType, TotalAnnotationSummary,
    };
    use std::net::SocketAddr;

    struct TestAddressTranslator {
        endpoint: SocketAddr,
    }

    #[async_trait]
    impl AddressTranslator for TestAddressTranslator {
        async fn translate_address(
            &self,
            _untranslated_peer: &UntranslatedPeer,
        ) -> std::result::Result<SocketAddr, TranslationError> {
            Ok(self.endpoint)
        }
    }

    fn settings(keyspace: String) -> ScyllaDbSettings {
        ScyllaDbSettings {
            nodes: vec!["127.0.0.1:19042".to_string()],
            keyspace,
            table_name: "unused".to_string(),
            username: None,
            password: None,
            replication_class: "SimpleStrategy".to_string(),
            replication_factor: 1,
        }
    }

    async fn fixture_session() -> Arc<Session> {
        let endpoint = "127.0.0.1:19042".parse().unwrap();
        Arc::new(
            SessionBuilder::new()
                .known_node("127.0.0.1:19042")
                // Docker publishes the regular CQL port while Scylla reports
                // its private container address and an unpublished shard-aware
                // port. Translate discovered peers back through that mapping.
                .address_translator(Arc::new(TestAddressTranslator { endpoint }))
                .disallow_shard_aware_port(true)
                .build()
                .await
                .unwrap(),
        )
    }

    fn event(serial: AnnotationSerial, id: &str, count: u64) -> StoredAnnotationEvent {
        StoredAnnotationEvent {
            app_id: "app".to_string(),
            channel_id: "channel".to_string(),
            annotation: Annotation {
                id: AnnotationId::new(id).unwrap(),
                action: AnnotationAction::Create,
                serial,
                message_serial: MessageSerial::new("msg:1").unwrap(),
                annotation_type: AnnotationType::new("reaction:total.v1").unwrap(),
                name: None,
                client_id: Some("client-a".to_string()),
                count: Some(count),
                data: None,
                encoding: None,
                timestamp: now_ms(),
            },
            stored_at_ms: now_ms(),
        }
    }

    #[tokio::test]
    async fn two_scylla_nodes_allocate_and_apply_annotations_atomically() {
        let keyspace = format!("sockudo_annotation_{}", uuid::Uuid::new_v4().simple());
        let prefix = "annotation_test";
        let first_session = fixture_session().await;
        let second_session = fixture_session().await;
        let first = Arc::new(
            ScyllaAnnotationStore::from_session(
                &settings(keyspace.clone()),
                prefix,
                3_600,
                first_session,
            )
            .await
            .unwrap(),
        );
        let second = Arc::new(
            ScyllaAnnotationStore::from_session(
                &settings(keyspace.clone()),
                prefix,
                3_600,
                second_session,
            )
            .await
            .unwrap(),
        );

        let left_allocations = {
            let store = Arc::clone(&first);
            tokio::spawn(async move {
                let mut serials = Vec::with_capacity(50);
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
        let right_allocations = {
            let store = Arc::clone(&second);
            tokio::spawn(async move {
                let mut serials = Vec::with_capacity(50);
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
        let mut serials = left_allocations.await.unwrap();
        serials.extend(right_allocations.await.unwrap());
        serials.sort();
        serials.dedup();
        assert_eq!(serials.len(), 100);
        assert_eq!(
            serials.first().unwrap().as_str(),
            "ann:00000000000000000001"
        );
        assert_eq!(serials.last().unwrap().as_str(), "ann:00000000000000000100");

        let left_event = event(
            first
                .reserve_annotation_serial("app", "channel")
                .await
                .unwrap()
                .unwrap(),
            "stable-id",
            1,
        );
        let right_event = event(
            second
                .reserve_annotation_serial("app", "channel")
                .await
                .unwrap()
                .unwrap(),
            "stable-id",
            1,
        );
        let (left, right) = tokio::join!(
            first.append_create_idempotent(left_event),
            second.append_create_idempotent(right_event)
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_ne!(left.inserted, right.inserted);
        assert_eq!(left.canonical_serial, right.canonical_serial);
        assert_eq!(
            right.projection.summary,
            AnnotationSummary::Total(TotalAnnotationSummary { total: 1 })
        );

        let conflict = event(
            first
                .reserve_annotation_serial("app", "channel")
                .await
                .unwrap()
                .unwrap(),
            "stable-id",
            2,
        );
        assert!(matches!(
            first.append_create_idempotent(conflict).await,
            Err(Error::InvalidMessageFormat(_))
        ));

        let distinct_left = event(
            first
                .reserve_annotation_serial("app", "channel")
                .await
                .unwrap()
                .unwrap(),
            "distinct-left",
            1,
        );
        let distinct_right = event(
            second
                .reserve_annotation_serial("app", "channel")
                .await
                .unwrap()
                .unwrap(),
            "distinct-right",
            1,
        );
        let (distinct_left, distinct_right) = tokio::join!(
            first.append_create_idempotent(distinct_left),
            second.append_create_idempotent(distinct_right)
        );
        let distinct_left = distinct_left.unwrap();
        let distinct_right = distinct_right.unwrap();
        assert!(distinct_left.inserted);
        assert!(distinct_right.inserted);
        let total = |summary: &AnnotationSummary| match summary {
            AnnotationSummary::Total(summary) => summary.total,
            other => panic!("expected total annotation summary, got {other:?}"),
        };
        let mut observed_totals = [
            total(&distinct_left.projection.summary),
            total(&distinct_right.projection.summary),
        ];
        observed_totals.sort_unstable();
        assert_eq!(observed_totals, [2, 3]);
        let final_projection = first
            .get_projection(AnnotationProjectionRequest {
                app_id: "app".to_string(),
                channel_id: "channel".to_string(),
                message_serial: MessageSerial::new("msg:1").unwrap(),
                annotation_type: AnnotationType::new("reaction:total.v1").unwrap(),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            final_projection.summary,
            AnnotationSummary::Total(TotalAnnotationSummary { total: 3 })
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
            3
        );

        first
            .session
            .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
            .await
            .unwrap();
    }
}
