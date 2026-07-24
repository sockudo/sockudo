use super::{deterministic_key, validate_identifier};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sockudo_core::annotations::{
    Annotation, AnnotationAction, AnnotationAppendOutcome, AnnotationEventLookupRequest,
    AnnotationEventsRequest, AnnotationProjection, AnnotationProjectionOptions,
    AnnotationProjectionRequest, AnnotationProjectionsForChannelRequest, AnnotationSerial,
    AnnotationStore, AnnotationSummary, RawAnnotationReplayRequest, StoredAnnotationEvent,
    StoredAnnotationProjection,
};
use sockudo_core::error::{Error, Result};
use sockudo_core::history::now_ms;
use sockudo_core::options::SurrealDbSettings;
use sockudo_core::versioned_messages::MessageSerial;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use surrealdb::Surreal;
use surrealdb::engine::any::{Any, connect};
use surrealdb::opt::auth::Root;
use surrealdb_types::{RecordId, SurrealValue};

const MAX_ANNOTATION_EVENTS_PER_PROJECTION: usize = 10_000;
const MAX_CHANNEL_PROJECTIONS: usize = 10_000;
const MAX_RAW_REPLAY_EVENTS: usize = 10_000;
const MAX_CAS_ATTEMPTS: usize = 64;

#[derive(Clone)]
struct AnnotationTables {
    streams: String,
    events: String,
    projections: String,
    create_receipts: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct StoredAnnotationStreamRecord {
    app_id: String,
    channel_id: String,
    next_serial: i64,
    updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct StoredAnnotationEventRecord {
    app_id: String,
    channel_id: String,
    message_serial: String,
    annotation_type: String,
    annotation_serial: String,
    annotation_id: String,
    action: String,
    payload_bytes: Vec<u8>,
    created_at_ms: i64,
}

#[derive(Debug, Deserialize, SurrealValue)]
struct StoredAnnotationEventRecordWithId {
    id: RecordId,
    app_id: String,
    channel_id: String,
    message_serial: String,
    annotation_type: String,
    annotation_serial: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct StoredAnnotationProjectionRecord {
    app_id: String,
    channel_id: String,
    message_serial: String,
    annotation_type: String,
    summary_payload_bytes: Vec<u8>,
    last_annotation_serial: Option<String>,
    revision: i64,
    updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct StoredAnnotationCreateReceipt {
    app_id: String,
    channel_id: String,
    message_serial: String,
    annotation_type: String,
    annotation_id: String,
    canonical_serial: String,
    annotation_payload_bytes: Vec<u8>,
    created_at_ms: i64,
}

pub(super) struct SurrealAnnotationStore {
    db: Surreal<Any>,
    tables: AnnotationTables,
}

pub(crate) async fn create_surreal_annotation_store(
    db_config: &SurrealDbSettings,
    table_prefix: &str,
) -> Result<Arc<dyn AnnotationStore + Send + Sync>> {
    Ok(Arc::new(
        SurrealAnnotationStore::new(db_config, table_prefix).await?,
    ))
}

impl SurrealAnnotationStore {
    async fn new(db_config: &SurrealDbSettings, table_prefix: &str) -> Result<Self> {
        let tables = AnnotationTables {
            streams: format!("{table_prefix}_annotation_streams"),
            events: format!("{table_prefix}_annotation_events"),
            projections: format!("{table_prefix}_annotation_projections"),
            create_receipts: format!("{table_prefix}_annotation_create_receipts"),
        };
        for (table, description) in [
            (&tables.streams, "annotation streams table"),
            (&tables.events, "annotation events table"),
            (&tables.projections, "annotation projections table"),
            (&tables.create_receipts, "annotation create receipts table"),
        ] {
            validate_identifier(table, description)?;
        }

        let db = connect(db_config.url.as_str()).await.map_err(|error| {
            Error::Internal(format!(
                "Failed to connect annotation store to SurrealDB: {error}"
            ))
        })?;
        db.signin(Root {
            username: db_config.username.clone(),
            password: db_config.password.clone(),
        })
        .await
        .map_err(|error| {
            Error::Internal(format!(
                "Failed to authenticate SurrealDB annotation store: {error}"
            ))
        })?;
        db.use_ns(db_config.namespace.as_str())
            .use_db(db_config.database.as_str())
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to select SurrealDB annotation namespace/database: {error}"
                ))
            })?;

        let store = Self { db, tables };
        store.ensure_schema().await?;
        Ok(store)
    }

    async fn ensure_schema(&self) -> Result<()> {
        let events_projection_index = format!("{}_projection_idx", self.tables.events);
        let events_raw_index = format!("{}_raw_idx", self.tables.events);
        let events_created_index = format!("{}_created_idx", self.tables.events);
        let projections_channel_index = format!("{}_channel_idx", self.tables.projections);
        let receipts_serial_index = format!("{}_serial_idx", self.tables.create_receipts);
        for (index, description) in [
            (&events_projection_index, "annotation projection index"),
            (&events_raw_index, "annotation raw replay index"),
            (&events_created_index, "annotation retention index"),
            (
                &projections_channel_index,
                "annotation channel projection index",
            ),
            (&receipts_serial_index, "annotation receipt serial index"),
        ] {
            validate_identifier(index, description)?;
        }

        let query = format!(
            "DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
             DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
             DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
             DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS app_id, channel_id, message_serial, annotation_type, annotation_serial;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS app_id, channel_id, annotation_serial;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS created_at_ms;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS app_id, channel_id, message_serial, annotation_type;\
             DEFINE INDEX IF NOT EXISTS {} ON TABLE {} FIELDS app_id, channel_id, canonical_serial;",
            self.tables.streams,
            self.tables.events,
            self.tables.projections,
            self.tables.create_receipts,
            events_projection_index,
            self.tables.events,
            events_raw_index,
            self.tables.events,
            events_created_index,
            self.tables.events,
            projections_channel_index,
            self.tables.projections,
            receipts_serial_index,
            self.tables.create_receipts,
        );
        let response = self.db.query(query).await.map_err(|error| {
            Error::Internal(format!(
                "Failed to initialize SurrealDB annotation schema: {error}"
            ))
        })?;
        if let Some(error) = Self::first_response_error(response) {
            return Err(Error::Internal(format!(
                "Failed to apply SurrealDB annotation schema: {error}"
            )));
        }
        Ok(())
    }

    fn stream_id(&self, app_id: &str, channel_id: &str) -> String {
        deterministic_key([app_id, channel_id].into_iter())
    }

    fn event_id(&self, app_id: &str, channel_id: &str, serial: &AnnotationSerial) -> String {
        deterministic_key([app_id, channel_id, serial.as_str()].into_iter())
    }

    fn projection_id(&self, request: &AnnotationProjectionRequest) -> String {
        deterministic_key(
            [
                request.app_id.as_str(),
                request.channel_id.as_str(),
                request.message_serial.as_str(),
                request.annotation_type.as_str(),
            ]
            .into_iter(),
        )
    }

    fn receipt_id(&self, record: &StoredAnnotationEvent) -> String {
        deterministic_key(
            [
                record.app_id.as_str(),
                record.channel_id.as_str(),
                record.annotation.message_serial.as_str(),
                record.annotation.annotation_type.as_str(),
                record.annotation.id.as_str(),
            ]
            .into_iter(),
        )
    }

    fn projection_request(record: &StoredAnnotationEvent) -> AnnotationProjectionRequest {
        AnnotationProjectionRequest {
            app_id: record.app_id.clone(),
            channel_id: record.channel_id.clone(),
            message_serial: record.annotation.message_serial.clone(),
            annotation_type: record.annotation.annotation_type.clone(),
        }
    }

    fn decode_event(record: StoredAnnotationEventRecord) -> Result<StoredAnnotationEvent> {
        sonic_rs::from_slice(&record.payload_bytes).map_err(|error| {
            Error::Internal(format!(
                "Failed to decode SurrealDB annotation event: {error}"
            ))
        })
    }

    fn projection_from_record(
        record: StoredAnnotationProjectionRecord,
    ) -> Result<StoredAnnotationProjection> {
        let summary: AnnotationSummary = sonic_rs::from_slice(&record.summary_payload_bytes)
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to decode SurrealDB annotation projection: {error}"
                ))
            })?;
        Ok(StoredAnnotationProjection {
            app_id: record.app_id,
            channel_id: record.channel_id,
            message_serial: MessageSerial::new(record.message_serial)?,
            annotation_type: sockudo_core::annotations::AnnotationType::new(
                record.annotation_type,
            )?,
            summary,
            last_annotation_serial: record
                .last_annotation_serial
                .map(AnnotationSerial::new)
                .transpose()?,
            updated_at_ms: record.updated_at_ms,
        })
    }

    fn projection_record(
        projection: &StoredAnnotationProjection,
        revision: i64,
    ) -> Result<StoredAnnotationProjectionRecord> {
        Ok(StoredAnnotationProjectionRecord {
            app_id: projection.app_id.clone(),
            channel_id: projection.channel_id.clone(),
            message_serial: projection.message_serial.as_str().to_string(),
            annotation_type: projection.annotation_type.as_str().to_string(),
            summary_payload_bytes: sonic_rs::to_vec(&projection.summary).map_err(|error| {
                Error::Internal(format!(
                    "Failed to encode SurrealDB annotation projection: {error}"
                ))
            })?,
            last_annotation_serial: projection
                .last_annotation_serial
                .as_ref()
                .map(|serial| serial.as_str().to_string()),
            revision,
            updated_at_ms: projection.updated_at_ms,
        })
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

    async fn load_projection_record(
        &self,
        request: &AnnotationProjectionRequest,
    ) -> Result<Option<StoredAnnotationProjectionRecord>> {
        self.db
            .select((self.tables.projections.clone(), self.projection_id(request)))
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to read SurrealDB annotation projection: {error}"
                ))
            })
    }

    async fn load_projection_events(
        &self,
        request: &AnnotationProjectionRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        let mut response = self
            .db
            .query(format!(
                "SELECT app_id, channel_id, message_serial, annotation_type, annotation_serial, annotation_id, action, payload_bytes, created_at_ms FROM {} WHERE app_id = $app_id AND channel_id = $channel_id AND message_serial = $message_serial AND annotation_type = $annotation_type ORDER BY annotation_serial ASC LIMIT {}",
                self.tables.events,
                MAX_ANNOTATION_EVENTS_PER_PROJECTION + 1
            ))
            .bind(("app_id", request.app_id.clone()))
            .bind(("channel_id", request.channel_id.clone()))
            .bind(("message_serial", request.message_serial.as_str().to_string()))
            .bind(("annotation_type", request.annotation_type.as_str().to_string()))
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to query SurrealDB annotation events: {error}"
                ))
            })?;
        let records: Vec<StoredAnnotationEventRecord> = response.take(0usize).map_err(|error| {
            Error::Internal(format!(
                "Failed to decode SurrealDB annotation events: {error}"
            ))
        })?;
        if records.len() > MAX_ANNOTATION_EVENTS_PER_PROJECTION {
            return Err(Error::BufferFull(format!(
                "annotation projection exceeds the {MAX_ANNOTATION_EVENTS_PER_PROJECTION} event bound"
            )));
        }
        records.into_iter().map(Self::decode_event).collect()
    }

    async fn load_event(
        &self,
        app_id: &str,
        channel_id: &str,
        serial: &AnnotationSerial,
    ) -> Result<Option<StoredAnnotationEvent>> {
        let record: Option<StoredAnnotationEventRecord> = self
            .db
            .select((
                self.tables.events.clone(),
                self.event_id(app_id, channel_id, serial),
            ))
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to find SurrealDB annotation event: {error}"
                ))
            })?;
        record.map(Self::decode_event).transpose()
    }

    async fn load_receipt(
        &self,
        record: &StoredAnnotationEvent,
    ) -> Result<Option<StoredAnnotationCreateReceipt>> {
        self.db
            .select((self.tables.create_receipts.clone(), self.receipt_id(record)))
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to read SurrealDB annotation create receipt: {error}"
                ))
            })
    }

    fn create_payload_matches(existing: &Annotation, incoming: &Annotation) -> bool {
        existing.name == incoming.name
            && existing.client_id == incoming.client_id
            && existing.count == incoming.count
            && existing.data == incoming.data
            && existing.encoding == incoming.encoding
    }

    fn is_transaction_conflict(error: &str) -> bool {
        let error = error.to_ascii_lowercase();
        error.contains("annotation_conflict")
            || error.contains("already exists")
            || error.contains("already been created")
            || error.contains("database record")
            || error.contains("transaction conflict")
            || error.contains("write conflict")
            || error.contains("can be retried")
            || error.contains("query was not executed due to a failed transaction")
    }

    fn first_response_error(mut response: surrealdb::IndexedResults) -> Option<String> {
        response
            .take_errors()
            .into_iter()
            .min_by_key(|(statement, _)| *statement)
            .map(|(statement, error)| format!("statement {statement} failed: {error}"))
    }

    async fn contention_backoff(operation: &'static str, attempt: usize) {
        let retry = attempt + 1;
        if retry.is_power_of_two() {
            tracing::warn!(
                operation,
                retry,
                max_retries = MAX_CAS_ATTEMPTS,
                "SurrealDB annotation transaction contention; backing off"
            );
        }
        if attempt == 0 {
            tokio::task::yield_now().await;
            return;
        }
        let exponent = attempt.min(5) as u32;
        let delay_ms = (1_u64 << exponent) + (attempt as u64 % 7);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }

    async fn projection_after_duplicate(
        &self,
        request: &AnnotationProjectionRequest,
    ) -> Result<StoredAnnotationProjection> {
        self.load_projection_record(request)
            .await?
            .map(Self::projection_from_record)
            .transpose()?
            .ok_or_else(|| {
                Error::Internal(
                    "annotation event exists without its authoritative projection".to_string(),
                )
            })
    }

    async fn duplicate_create_outcome(
        &self,
        record: &StoredAnnotationEvent,
        receipt: StoredAnnotationCreateReceipt,
    ) -> Result<AnnotationAppendOutcome> {
        let existing: Annotation = sonic_rs::from_slice(&receipt.annotation_payload_bytes)
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to decode SurrealDB annotation create receipt: {error}"
                ))
            })?;
        if !Self::create_payload_matches(&existing, &record.annotation) {
            return Err(Error::InvalidMessageFormat(format!(
                "Annotation id '{}' was already used with a different payload",
                record.annotation.id.as_str()
            )));
        }
        let canonical_serial = AnnotationSerial::new(receipt.canonical_serial)?;
        let projection = self
            .projection_after_duplicate(&Self::projection_request(record))
            .await?;
        Ok(AnnotationAppendOutcome {
            projection,
            canonical_serial,
            inserted: false,
        })
    }

    async fn append_transaction(
        &self,
        record: &StoredAnnotationEvent,
        create_receipt: bool,
    ) -> Result<AnnotationAppendOutcome> {
        let request = Self::projection_request(record);
        let event_id = self.event_id(
            &record.app_id,
            &record.channel_id,
            &record.annotation.serial,
        );
        let event_payload = sonic_rs::to_vec(record).map_err(|error| {
            Error::Internal(format!(
                "Failed to encode SurrealDB annotation event: {error}"
            ))
        })?;
        let event_content = StoredAnnotationEventRecord {
            app_id: record.app_id.clone(),
            channel_id: record.channel_id.clone(),
            message_serial: record.annotation.message_serial.as_str().to_string(),
            annotation_type: record.annotation.annotation_type.as_str().to_string(),
            annotation_serial: record.annotation.serial.as_str().to_string(),
            annotation_id: record.annotation.id.as_str().to_string(),
            action: record.annotation.action.as_str().to_string(),
            payload_bytes: event_payload,
            created_at_ms: record.stored_at_ms,
        };

        for attempt in 0..MAX_CAS_ATTEMPTS {
            if create_receipt && let Some(receipt) = self.load_receipt(record).await? {
                return self.duplicate_create_outcome(record, receipt).await;
            }
            if let Some(existing) = self
                .load_event(
                    &record.app_id,
                    &record.channel_id,
                    &record.annotation.serial,
                )
                .await?
            {
                if existing.annotation != record.annotation {
                    return Err(Error::InvalidMessageFormat(
                        "annotation serial was already used with a different payload".to_string(),
                    ));
                }
                let projection = self.projection_after_duplicate(&request).await?;
                return Ok(AnnotationAppendOutcome {
                    projection,
                    canonical_serial: record.annotation.serial.clone(),
                    inserted: false,
                });
            }

            let current_projection = self.load_projection_record(&request).await?;
            let mut events = self.load_projection_events(&request).await?;
            events.push(record.clone());
            let projection =
                Self::build_projection(&request, events, AnnotationProjectionOptions::default())?;
            let next_revision = current_projection
                .as_ref()
                .map_or(1, |projection| projection.revision.saturating_add(1));
            let projection_content = Self::projection_record(&projection, next_revision)?;

            let projection_statement = if current_projection.is_some() {
                "LET $projection_write = UPDATE ONLY type::record($projection_table, $projection_id) CONTENT $projection_content WHERE revision = $expected_revision RETURN AFTER; IF $projection_write = NONE { THROW 'annotation_conflict'; };"
            } else {
                "LET $projection_write = CREATE ONLY type::record($projection_table, $projection_id) CONTENT $projection_content;"
            };
            let receipt_statement = if create_receipt {
                "LET $receipt_write = CREATE ONLY type::record($receipt_table, $receipt_id) CONTENT $receipt_content;"
            } else {
                ""
            };
            let receipt_content = StoredAnnotationCreateReceipt {
                app_id: record.app_id.clone(),
                channel_id: record.channel_id.clone(),
                message_serial: record.annotation.message_serial.as_str().to_string(),
                annotation_type: record.annotation.annotation_type.as_str().to_string(),
                annotation_id: record.annotation.id.as_str().to_string(),
                canonical_serial: record.annotation.serial.as_str().to_string(),
                annotation_payload_bytes: sonic_rs::to_vec(&record.annotation).map_err(
                    |error| {
                        Error::Internal(format!(
                            "Failed to encode SurrealDB annotation receipt: {error}"
                        ))
                    },
                )?,
                created_at_ms: record.stored_at_ms,
            };
            let query = format!(
                "BEGIN TRANSACTION; {projection_statement} LET $event_write = CREATE ONLY type::record($event_table, $event_id) CONTENT $event_content; {receipt_statement} COMMIT TRANSACTION;"
            );
            let response = self
                .db
                .query(query)
                .bind(("projection_table", self.tables.projections.clone()))
                .bind(("projection_id", self.projection_id(&request)))
                .bind(("projection_content", projection_content))
                .bind((
                    "expected_revision",
                    current_projection
                        .as_ref()
                        .map_or(0, |value| value.revision),
                ))
                .bind(("event_table", self.tables.events.clone()))
                .bind(("event_id", event_id.clone()))
                .bind(("event_content", event_content.clone()))
                .bind(("receipt_table", self.tables.create_receipts.clone()))
                .bind(("receipt_id", self.receipt_id(record)))
                .bind(("receipt_content", receipt_content))
                .await;
            let response_error = match response {
                Ok(response) => Self::first_response_error(response),
                Err(error) => Some(error.to_string()),
            };
            match response_error {
                None => {
                    return Ok(AnnotationAppendOutcome {
                        projection,
                        canonical_serial: record.annotation.serial.clone(),
                        inserted: true,
                    });
                }
                Some(error) if Self::is_transaction_conflict(&error) => {
                    Self::contention_backoff("append", attempt).await;
                }
                Some(error) => {
                    return Err(Error::Internal(format!(
                        "Failed to transact SurrealDB annotation append: {error}"
                    )));
                }
            }
        }

        Err(Error::Internal(format!(
            "SurrealDB annotation append exceeded {MAX_CAS_ATTEMPTS} contention retries"
        )))
    }

    async fn persist_rebuilt_projection(
        &self,
        request: &AnnotationProjectionRequest,
        options: AnnotationProjectionOptions,
    ) -> Result<StoredAnnotationProjection> {
        for attempt in 0..MAX_CAS_ATTEMPTS {
            let current = self.load_projection_record(request).await?;
            let events = self.load_projection_events(request).await?;
            let projection = Self::build_projection(request, events, options)?;
            let next_revision = current
                .as_ref()
                .map_or(1, |value| value.revision.saturating_add(1));
            let content = Self::projection_record(&projection, next_revision)?;
            let statement = if current.is_some() {
                "LET $projection_write = UPDATE ONLY type::record($table, $id) CONTENT $content WHERE revision = $expected RETURN AFTER; IF $projection_write = NONE { THROW 'annotation_conflict'; };"
            } else {
                "LET $projection_write = CREATE ONLY type::record($table, $id) CONTENT $content;"
            };
            let response = self
                .db
                .query(format!(
                    "BEGIN TRANSACTION; {statement} COMMIT TRANSACTION;"
                ))
                .bind(("table", self.tables.projections.clone()))
                .bind(("id", self.projection_id(request)))
                .bind(("content", content))
                .bind((
                    "expected",
                    current.as_ref().map_or(0, |value| value.revision),
                ))
                .await;
            let response_error = match response {
                Ok(response) => Self::first_response_error(response),
                Err(error) => Some(error.to_string()),
            };
            match response_error {
                None => return Ok(projection),
                Some(error) if Self::is_transaction_conflict(&error) => {
                    Self::contention_backoff("projection_rebuild", attempt).await;
                }
                Some(error) => {
                    return Err(Error::Internal(format!(
                        "Failed to rebuild SurrealDB annotation projection: {error}"
                    )));
                }
            }
        }
        Err(Error::Internal(format!(
            "SurrealDB annotation rebuild exceeded {MAX_CAS_ATTEMPTS} contention retries"
        )))
    }

    async fn purge_projection_group(
        &self,
        request: &AnnotationProjectionRequest,
        selected: &[StoredAnnotationEventRecordWithId],
    ) -> Result<u64> {
        let selected_serials = selected
            .iter()
            .map(|row| row.annotation_serial.as_str())
            .collect::<HashSet<_>>();

        for attempt in 0..MAX_CAS_ATTEMPTS {
            let current = self.load_projection_record(request).await?;
            let all_events = self.load_projection_events(request).await?;
            let actual_serials = all_events
                .iter()
                .filter(|event| selected_serials.contains(event.annotation.serial.as_str()))
                .map(|event| event.annotation.serial.as_str().to_string())
                .collect::<HashSet<_>>();
            if actual_serials.is_empty() {
                return Ok(0);
            }
            let remaining = all_events
                .into_iter()
                .filter(|event| !actual_serials.contains(event.annotation.serial.as_str()))
                .collect::<Vec<_>>();
            let projection =
                Self::build_projection(request, remaining, AnnotationProjectionOptions::default())?;
            let next_revision = current
                .as_ref()
                .map_or(1, |value| value.revision.saturating_add(1));
            let projection_content = Self::projection_record(&projection, next_revision)?;
            let event_ids = selected
                .iter()
                .filter(|row| actual_serials.contains(&row.annotation_serial))
                .map(|row| row.id.clone())
                .collect::<Vec<_>>();

            let serial_values = actual_serials.iter().cloned().collect::<Vec<_>>();
            let mut receipt_response = self
                .db
                .query(format!(
                    "SELECT VALUE id FROM {} WHERE app_id = $app_id AND channel_id = $channel_id AND canonical_serial IN $serials",
                    self.tables.create_receipts
                ))
                .bind(("app_id", request.app_id.clone()))
                .bind(("channel_id", request.channel_id.clone()))
                .bind(("serials", serial_values))
                .await
                .map_err(|error| {
                    Error::Internal(format!(
                        "Failed to select SurrealDB annotation receipts for purge: {error}"
                    ))
                })?;
            let receipt_ids: Vec<RecordId> = receipt_response.take(0usize).map_err(|error| {
                Error::Internal(format!(
                    "Failed to decode SurrealDB annotation receipt ids: {error}"
                ))
            })?;

            let projection_statement = if current.is_some() {
                "LET $projection_write = UPDATE ONLY type::record($projection_table, $projection_id) CONTENT $projection_content WHERE revision = $expected_revision RETURN AFTER; IF $projection_write = NONE { THROW 'annotation_conflict'; };"
            } else {
                "LET $projection_write = CREATE ONLY type::record($projection_table, $projection_id) CONTENT $projection_content;"
            };
            let remove_projection = if projection.last_annotation_serial.is_none() {
                "DELETE ONLY type::record($projection_table, $projection_id);"
            } else {
                ""
            };
            let remove_receipts = if receipt_ids.is_empty() {
                ""
            } else {
                "DELETE $receipt_ids;"
            };
            let response = self
                .db
                .query(format!(
                    "BEGIN TRANSACTION; {projection_statement} DELETE $event_ids; {remove_receipts} {remove_projection} COMMIT TRANSACTION;"
                ))
                .bind(("projection_table", self.tables.projections.clone()))
                .bind(("projection_id", self.projection_id(request)))
                .bind(("projection_content", projection_content))
                .bind((
                    "expected_revision",
                    current.as_ref().map_or(0, |value| value.revision),
                ))
                .bind(("event_ids", event_ids))
                .bind(("receipt_ids", receipt_ids))
                .await;
            let response_error = match response {
                Ok(response) => Self::first_response_error(response),
                Err(error) => Some(error.to_string()),
            };
            match response_error {
                None => return Ok(actual_serials.len() as u64),
                Some(error) if Self::is_transaction_conflict(&error) => {
                    Self::contention_backoff("retention_purge", attempt).await;
                }
                Some(error) => {
                    return Err(Error::Internal(format!(
                        "Failed to transact SurrealDB annotation purge: {error}"
                    )));
                }
            }
        }
        Err(Error::Internal(format!(
            "SurrealDB annotation purge exceeded {MAX_CAS_ATTEMPTS} contention retries"
        )))
    }
}

#[async_trait]
impl AnnotationStore for SurrealAnnotationStore {
    async fn reserve_annotation_serial(
        &self,
        app_id: &str,
        channel_id: &str,
    ) -> Result<Option<AnnotationSerial>> {
        let stream_id = self.stream_id(app_id, channel_id);
        for attempt in 0..MAX_CAS_ATTEMPTS {
            let current: Option<StoredAnnotationStreamRecord> = self
                .db
                .select((self.tables.streams.clone(), stream_id.clone()))
                .await
                .map_err(|error| {
                    Error::Internal(format!(
                        "Failed to read SurrealDB annotation stream: {error}"
                    ))
                })?;
            if let Some(current) = current {
                if current.next_serial <= 0 {
                    return Err(Error::Internal(
                        "SurrealDB annotation allocator returned a non-positive serial".to_string(),
                    ));
                }
                let response = self
                    .db
                    .query("UPDATE ONLY type::record($table, $id) SET next_serial = $next, updated_at_ms = $now WHERE next_serial = $expected RETURN AFTER")
                    .bind(("table", self.tables.streams.clone()))
                    .bind(("id", stream_id.clone()))
                    .bind(("next", current.next_serial.saturating_add(1)))
                    .bind(("now", now_ms()))
                    .bind(("expected", current.next_serial))
                    .await;
                let mut response = match response {
                    Ok(response) => response,
                    Err(error) if Self::is_transaction_conflict(&error.to_string()) => {
                        Self::contention_backoff("serial_allocation", attempt).await;
                        continue;
                    }
                    Err(error) => {
                        return Err(Error::Internal(format!(
                            "Failed to advance SurrealDB annotation serial: {error}"
                        )));
                    }
                };
                let updated: Option<StoredAnnotationStreamRecord> = match response.take(0usize) {
                    Ok(updated) => updated,
                    Err(error) if Self::is_transaction_conflict(&error.to_string()) => {
                        Self::contention_backoff("serial_allocation", attempt).await;
                        continue;
                    }
                    Err(error) => {
                        return Err(Error::Internal(format!(
                            "Failed to decode SurrealDB annotation serial update: {error}"
                        )));
                    }
                };
                if updated.is_some() {
                    return Ok(Some(AnnotationSerial::new(format!(
                        "ann:{:020}",
                        current.next_serial
                    ))?));
                }
                Self::contention_backoff("serial_allocation", attempt).await;
                continue;
            }

            let stream = StoredAnnotationStreamRecord {
                app_id: app_id.to_string(),
                channel_id: channel_id.to_string(),
                next_serial: 2,
                updated_at_ms: now_ms(),
            };
            let created: std::result::Result<Option<StoredAnnotationStreamRecord>, _> = self
                .db
                .create((self.tables.streams.clone(), stream_id.clone()))
                .content(stream)
                .await;
            match created {
                Ok(_) => return Ok(Some(AnnotationSerial::new("ann:00000000000000000001")?)),
                Err(error) if Self::is_transaction_conflict(&error.to_string()) => {
                    Self::contention_backoff("serial_allocation", attempt).await;
                }
                Err(error) => {
                    return Err(Error::Internal(format!(
                        "Failed to create SurrealDB annotation stream: {error}"
                    )));
                }
            }
        }
        Err(Error::Internal(format!(
            "SurrealDB annotation allocator exceeded {MAX_CAS_ATTEMPTS} contention retries"
        )))
    }

    async fn append_event(
        &self,
        mut record: StoredAnnotationEvent,
    ) -> Result<StoredAnnotationProjection> {
        record.validate()?;
        if record.stored_at_ms == 0 {
            record.stored_at_ms = now_ms();
        }
        Ok(self.append_transaction(&record, false).await?.projection)
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
        self.append_transaction(&record, true).await
    }

    async fn get_events(
        &self,
        request: AnnotationEventsRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        request.validate()?;
        self.load_projection_events(&AnnotationProjectionRequest {
            app_id: request.app_id,
            channel_id: request.channel_id,
            message_serial: request.message_serial,
            annotation_type: request.annotation_type,
        })
        .await
    }

    async fn replay_raw(
        &self,
        request: RawAnnotationReplayRequest,
    ) -> Result<Vec<StoredAnnotationEvent>> {
        request.validate()?;
        if request.limit > MAX_RAW_REPLAY_EVENTS {
            return Err(Error::BufferFull(format!(
                "annotation replay limit exceeds the {MAX_RAW_REPLAY_EVENTS} item bound"
            )));
        }
        let mut clauses = vec!["app_id = $app_id", "channel_id = $channel_id"];
        if request.message_serial.is_some() {
            clauses.push("message_serial = $message_serial");
        }
        if request.after_annotation_serial.is_some() {
            clauses.push("annotation_serial > $after_serial");
        }
        let mut query = self
            .db
            .query(format!(
                "SELECT app_id, channel_id, message_serial, annotation_type, annotation_serial, annotation_id, action, payload_bytes, created_at_ms FROM {} WHERE {} ORDER BY annotation_serial ASC LIMIT {}",
                self.tables.events,
                clauses.join(" AND "),
                request.limit
            ))
            .bind(("app_id", request.app_id))
            .bind(("channel_id", request.channel_id));
        if let Some(message_serial) = request.message_serial {
            query = query.bind(("message_serial", message_serial.as_str().to_string()));
        }
        if let Some(after_serial) = request.after_annotation_serial {
            query = query.bind(("after_serial", after_serial.as_str().to_string()));
        }
        let mut response = query.await.map_err(|error| {
            Error::Internal(format!(
                "Failed to replay SurrealDB annotation events: {error}"
            ))
        })?;
        let records: Vec<StoredAnnotationEventRecord> = response.take(0usize).map_err(|error| {
            Error::Internal(format!(
                "Failed to decode SurrealDB annotation replay: {error}"
            ))
        })?;
        records.into_iter().map(Self::decode_event).collect()
    }

    async fn get_event_by_serial(
        &self,
        request: AnnotationEventLookupRequest,
    ) -> Result<Option<StoredAnnotationEvent>> {
        request.validate()?;
        self.load_event(
            &request.app_id,
            &request.channel_id,
            &request.annotation_serial,
        )
        .await
    }

    async fn get_projection(
        &self,
        request: AnnotationProjectionRequest,
    ) -> Result<Option<StoredAnnotationProjection>> {
        request.validate()?;
        self.load_projection_record(&request)
            .await?
            .map(Self::projection_from_record)
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
        let mut response = self
            .db
            .query(format!(
                "SELECT app_id, channel_id, message_serial, annotation_type, summary_payload_bytes, last_annotation_serial, revision, updated_at_ms FROM {} WHERE app_id = $app_id AND channel_id = $channel_id ORDER BY message_serial ASC, annotation_type ASC LIMIT {}",
                self.tables.projections,
                MAX_CHANNEL_PROJECTIONS + 1
            ))
            .bind(("app_id", request.app_id))
            .bind(("channel_id", request.channel_id))
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to list SurrealDB annotation projections: {error}"
                ))
            })?;
        let records: Vec<StoredAnnotationProjectionRecord> =
            response.take(0usize).map_err(|error| {
                Error::Internal(format!(
                    "Failed to decode SurrealDB annotation projections: {error}"
                ))
            })?;
        if records.len() > MAX_CHANNEL_PROJECTIONS {
            return Err(Error::BufferFull(format!(
                "annotation projection list exceeds the {MAX_CHANNEL_PROJECTIONS} item bound"
            )));
        }
        Ok((
            records
                .into_iter()
                .map(Self::projection_from_record)
                .collect::<Result<Vec<_>>>()?,
            0,
        ))
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
        self.persist_rebuilt_projection(&request, options).await
    }

    async fn purge_before(&self, before_ms: i64, batch_size: usize) -> Result<(u64, bool)> {
        if batch_size == 0 {
            return Ok((0, false));
        }
        let limit = i64::try_from(batch_size)
            .map_err(|_| Error::InvalidMessageFormat("purge batch is too large".to_string()))?;
        let mut response = self
            .db
            .query(format!(
                "SELECT * FROM {} WHERE created_at_ms < $cutoff ORDER BY created_at_ms ASC LIMIT $limit",
                self.tables.events
            ))
            .bind(("cutoff", before_ms))
            .bind(("limit", limit))
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to select expired SurrealDB annotations: {error}"
                ))
            })?;
        let selected: Vec<StoredAnnotationEventRecordWithId> =
            response.take(0usize).map_err(|error| {
                Error::Internal(format!(
                    "Failed to decode expired SurrealDB annotations: {error}"
                ))
            })?;
        if selected.is_empty() {
            return Ok((0, false));
        }

        let mut grouped = BTreeMap::<(String, String, String, String), Vec<_>>::new();
        for row in selected {
            grouped
                .entry((
                    row.app_id.clone(),
                    row.channel_id.clone(),
                    row.message_serial.clone(),
                    row.annotation_type.clone(),
                ))
                .or_default()
                .push(row);
        }
        let mut deleted = 0_u64;
        for ((app_id, channel_id, message_serial, annotation_type), rows) in grouped {
            deleted = deleted.saturating_add(
                self.purge_projection_group(
                    &AnnotationProjectionRequest {
                        app_id,
                        channel_id,
                        message_serial: MessageSerial::new(message_serial)?,
                        annotation_type: sockudo_core::annotations::AnnotationType::new(
                            annotation_type,
                        )?,
                    },
                    &rows,
                )
                .await?,
            );
        }

        let mut backlog_response = self
            .db
            .query(format!(
                "SELECT VALUE id FROM {} WHERE created_at_ms < $cutoff LIMIT 1",
                self.tables.events
            ))
            .bind(("cutoff", before_ms))
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to inspect SurrealDB annotation purge backlog: {error}"
                ))
            })?;
        let remaining: Vec<RecordId> = backlog_response.take(0usize).map_err(|error| {
            Error::Internal(format!(
                "Failed to decode SurrealDB annotation purge backlog: {error}"
            ))
        })?;
        Ok((deleted, !remaining.is_empty()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::annotations::{
        AnnotationId, AnnotationSummary, AnnotationType, TotalAnnotationSummary,
    };

    fn settings(namespace: String) -> SurrealDbSettings {
        SurrealDbSettings {
            url: std::env::var("SURREALDB_TEST_URL")
                .unwrap_or_else(|_| "ws://127.0.0.1:18001".to_string()),
            namespace,
            database: "sockudo".to_string(),
            username: "root".to_string(),
            password: "root".to_string(),
            table_name: "applications".to_string(),
            cache_ttl: 300,
            cache_max_capacity: 100,
        }
    }

    fn create_event(
        id: String,
        serial: AnnotationSerial,
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
                message_serial: MessageSerial::new("msg:1").unwrap(),
                annotation_type: AnnotationType::new("reaction:total.v1").unwrap(),
                name: None,
                client_id: Some("client-a".to_string()),
                count: Some(count),
                data: None,
                encoding: None,
                timestamp: stored_at_ms,
            },
            stored_at_ms,
        }
    }

    fn projection_request() -> AnnotationProjectionRequest {
        AnnotationProjectionRequest {
            app_id: "app".to_string(),
            channel_id: "channel".to_string(),
            message_serial: MessageSerial::new("msg:1").unwrap(),
            annotation_type: AnnotationType::new("reaction:total.v1").unwrap(),
        }
    }

    #[tokio::test]
    async fn two_surreal_nodes_keep_annotations_atomic_replayable_and_durable() {
        let namespace = format!("sockudo_annotation_test_{}", uuid::Uuid::new_v4().simple());
        let settings = settings(namespace);
        let prefix = format!(
            "sockudo_ann_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        let first = Arc::new(
            SurrealAnnotationStore::new(&settings, &prefix)
                .await
                .unwrap(),
        );
        let second = Arc::new(
            SurrealAnnotationStore::new(&settings, &prefix)
                .await
                .unwrap(),
        );

        let first_allocations = {
            let store = Arc::clone(&first);
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
        let second_allocations = {
            let store = Arc::clone(&second);
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
        let mut allocated = first_allocations.await.unwrap();
        allocated.extend(second_allocations.await.unwrap());
        allocated.sort();
        allocated.dedup();
        assert_eq!(allocated.len(), 100);
        assert_eq!(
            allocated.first().unwrap().as_str(),
            "ann:00000000000000000001"
        );
        assert_eq!(
            allocated.last().unwrap().as_str(),
            "ann:00000000000000000100"
        );

        let first_event = create_event(
            "stable-id".to_string(),
            first
                .reserve_annotation_serial("app", "channel")
                .await
                .unwrap()
                .unwrap(),
            1,
            1_000,
        );
        let second_event = create_event(
            "stable-id".to_string(),
            second
                .reserve_annotation_serial("app", "channel")
                .await
                .unwrap()
                .unwrap(),
            1,
            1_000,
        );
        let (first_outcome, second_outcome) = tokio::join!(
            first.append_create_idempotent(first_event.clone()),
            second.append_create_idempotent(second_event),
        );
        let first_outcome = first_outcome.unwrap();
        let second_outcome = second_outcome.unwrap();
        assert_ne!(first_outcome.inserted, second_outcome.inserted);
        assert_eq!(
            first_outcome.canonical_serial,
            second_outcome.canonical_serial
        );

        let conflicting = create_event(
            "stable-id".to_string(),
            first
                .reserve_annotation_serial("app", "channel")
                .await
                .unwrap()
                .unwrap(),
            2,
            1_001,
        );
        assert!(
            first
                .append_create_idempotent(conflicting)
                .await
                .unwrap_err()
                .to_string()
                .contains("different payload")
        );

        let mut tasks = tokio::task::JoinSet::new();
        for index in 0..20 {
            let store = if index % 2 == 0 {
                Arc::clone(&first)
            } else {
                Arc::clone(&second)
            };
            tasks.spawn(async move {
                let serial = store
                    .reserve_annotation_serial("app", "channel")
                    .await
                    .unwrap()
                    .unwrap();
                store
                    .append_create_idempotent(create_event(
                        format!("distinct-{index}"),
                        serial,
                        1,
                        1_100 + index,
                    ))
                    .await
                    .unwrap()
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }

        let projection = second
            .get_projection(projection_request())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            projection.summary,
            AnnotationSummary::Total(TotalAnnotationSummary { total: 21 })
        );
        let events = first
            .get_events(AnnotationEventsRequest {
                app_id: "app".to_string(),
                channel_id: "channel".to_string(),
                message_serial: MessageSerial::new("msg:1").unwrap(),
                annotation_type: AnnotationType::new("reaction:total.v1").unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(events.len(), 21);

        let replay = second
            .replay_raw(RawAnnotationReplayRequest {
                app_id: "app".to_string(),
                channel_id: "channel".to_string(),
                message_serial: None,
                after_annotation_serial: None,
                limit: 5,
            })
            .await
            .unwrap();
        assert_eq!(replay.len(), 5);
        let after = replay[0].annotation.serial.clone();
        let replay_after = first
            .replay_raw(RawAnnotationReplayRequest {
                app_id: "app".to_string(),
                channel_id: "channel".to_string(),
                message_serial: None,
                after_annotation_serial: Some(after.clone()),
                limit: 1,
            })
            .await
            .unwrap();
        assert_eq!(replay_after.len(), 1);
        assert!(replay_after[0].annotation.serial > after);

        let (deleted, has_more) = first.purge_before(2_000, 5).await.unwrap();
        assert_eq!(deleted, 5);
        assert!(has_more);
        let recreated = SurrealAnnotationStore::new(&settings, &prefix)
            .await
            .unwrap();
        let recreated_projection = recreated
            .get_projection(projection_request())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            recreated_projection.summary,
            AnnotationSummary::Total(TotalAnnotationSummary { total: 16 })
        );
        assert_eq!(
            recreated
                .replay_raw(RawAnnotationReplayRequest {
                    app_id: "app".to_string(),
                    channel_id: "channel".to_string(),
                    message_serial: None,
                    after_annotation_serial: None,
                    limit: 100,
                })
                .await
                .unwrap()
                .len(),
            16
        );
    }
}
