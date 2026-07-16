use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType, Put, ReturnValue,
    ScalarAttributeType, TableStatus, TimeToLiveSpecification, TimeToLiveStatus, TransactWriteItem,
};
use sockudo_core::annotations::{
    Annotation, AnnotationAction, AnnotationAppendOutcome, AnnotationEventLookupRequest,
    AnnotationEventsRequest, AnnotationProjection, AnnotationProjectionOptions,
    AnnotationProjectionRequest, AnnotationProjectionsForChannelRequest, AnnotationSerial,
    AnnotationStore, AnnotationSummary, AnnotationType, RawAnnotationReplayRequest,
    StoredAnnotationEvent, StoredAnnotationProjection,
};
use sockudo_core::error::{Error, Result};
use sockudo_core::history::now_ms;
use sockudo_core::options::DynamoDbSettings;
use sockudo_core::versioned_messages::MessageSerial;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const MAX_CAS_ATTEMPTS: usize = 8;
const MAX_PROJECTION_EVENTS: usize = 10_000;
const MAX_CHANNEL_PROJECTIONS: usize = 10_000;
const MAX_RAW_REPLAY: usize = 10_000;
const EXPIRES_AT_ATTR: &str = "expires_at";

/// DynamoDB annotation authority.
///
/// All rows for an app/channel share a partition. Canonical event rows and
/// their access-path rows are committed with the projection and create receipt
/// in one DynamoDB transaction. The duplicated rows avoid eventually
/// consistent secondary indexes in projection compare-and-apply.
pub(super) struct DynamoDbAnnotationStore {
    client: Client,
    table: String,
    retention_seconds: u64,
}

#[derive(Clone)]
struct ProjectionState {
    projection: StoredAnnotationProjection,
    revision: u64,
    valid_until: Option<i64>,
}

struct ProjectionBuild {
    projection: StoredAnnotationProjection,
    valid_until: Option<i64>,
    expires_at: Option<i64>,
    event_count: usize,
}

pub(crate) async fn create_dynamodb_annotation_store(
    db_config: &DynamoDbSettings,
    table_prefix: &str,
    retention_seconds: u64,
) -> Result<Arc<dyn AnnotationStore + Send + Sync>> {
    Ok(Arc::new(
        DynamoDbAnnotationStore::new(db_config, table_prefix, retention_seconds).await?,
    ))
}

impl DynamoDbAnnotationStore {
    pub(super) async fn new(
        db_config: &DynamoDbSettings,
        table_prefix: &str,
        retention_seconds: u64,
    ) -> Result<Self> {
        let mut config = aws_config::from_env().region(aws_sdk_dynamodb::config::Region::new(
            db_config.region.clone(),
        ));
        if let Some(endpoint) = &db_config.endpoint_url {
            config = config.endpoint_url(endpoint);
        }
        if let (Some(access_key), Some(secret_key)) = (
            &db_config.aws_access_key_id,
            &db_config.aws_secret_access_key,
        ) {
            config = config.credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                access_key, secret_key, None, None, "static",
            ));
        }
        if let Some(profile) = &db_config.aws_profile_name {
            config = config.profile_name(profile);
        }
        let store = Self {
            client: Client::new(&config.load().await),
            table: format!("{table_prefix}_annotation_commits"),
            retention_seconds,
        };
        store.ensure_table().await?;
        Ok(store)
    }

    fn attr_s(value: &str) -> AttributeValue {
        AttributeValue::S(value.to_string())
    }

    fn attr_n(value: impl ToString) -> AttributeValue {
        AttributeValue::N(value.to_string())
    }

    fn attr_b(value: Vec<u8>) -> AttributeValue {
        AttributeValue::B(Blob::new(value))
    }

    fn item_string(item: &HashMap<String, AttributeValue>, key: &str) -> Option<String> {
        item.get(key)
            .and_then(|value| value.as_s().ok())
            .map(ToString::to_string)
    }

    fn item_i64(item: &HashMap<String, AttributeValue>, key: &str) -> Option<i64> {
        item.get(key)
            .and_then(|value| value.as_n().ok())
            .and_then(|value| value.parse().ok())
    }

    fn item_bytes(item: &HashMap<String, AttributeValue>, key: &str) -> Result<Vec<u8>> {
        item.get(key)
            .and_then(|value| value.as_b().ok())
            .map(|value| value.as_ref().to_vec())
            .ok_or_else(|| Error::Internal(format!("DynamoDB annotation row is missing {key}")))
    }

    fn partition_key(app_id: &str, channel_id: &str) -> String {
        Self::composite_key([app_id, channel_id])
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
            stored_at_ms
                .div_euclid(1_000)
                .saturating_add(self.retention_seconds as i64)
        })
    }

    fn is_active(item: &HashMap<String, AttributeValue>, now_seconds: i64) -> bool {
        Self::item_i64(item, EXPIRES_AT_ATTR).is_none_or(|expiry| expiry > now_seconds)
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

    fn decode_event(item: &HashMap<String, AttributeValue>) -> Result<StoredAnnotationEvent> {
        sonic_rs::from_slice(&Self::item_bytes(item, "payload_bytes")?).map_err(|error| {
            Error::Internal(format!(
                "Failed to decode DynamoDB annotation event: {error}"
            ))
        })
    }

    fn event_item(
        partition: &str,
        commit_key: &str,
        record: &StoredAnnotationEvent,
        payload: &[u8],
        expires_at: Option<i64>,
    ) -> HashMap<String, AttributeValue> {
        let mut item = HashMap::new();
        item.insert("app_channel".to_string(), Self::attr_s(partition));
        item.insert("commit_key".to_string(), Self::attr_s(commit_key));
        item.insert("app_id".to_string(), Self::attr_s(&record.app_id));
        item.insert("channel_id".to_string(), Self::attr_s(&record.channel_id));
        item.insert(
            "message_serial".to_string(),
            Self::attr_s(record.annotation.message_serial.as_str()),
        );
        item.insert(
            "annotation_type".to_string(),
            Self::attr_s(record.annotation.annotation_type.as_str()),
        );
        item.insert(
            "annotation_serial".to_string(),
            Self::attr_s(record.annotation.serial.as_str()),
        );
        item.insert("payload_bytes".to_string(), Self::attr_b(payload.to_vec()));
        item.insert(
            "created_at_ms".to_string(),
            Self::attr_n(record.stored_at_ms),
        );
        if let Some(expires_at) = expires_at {
            item.insert(EXPIRES_AT_ATTR.to_string(), Self::attr_n(expires_at));
        }
        item
    }

    fn projection_item(
        partition: &str,
        key: &str,
        build: &ProjectionBuild,
        revision: u64,
    ) -> Result<HashMap<String, AttributeValue>> {
        let mut item = HashMap::new();
        item.insert("app_channel".to_string(), Self::attr_s(partition));
        item.insert("commit_key".to_string(), Self::attr_s(key));
        item.insert(
            "message_serial".to_string(),
            Self::attr_s(build.projection.message_serial.as_str()),
        );
        item.insert(
            "annotation_type".to_string(),
            Self::attr_s(build.projection.annotation_type.as_str()),
        );
        item.insert(
            "summary_bytes".to_string(),
            Self::attr_b(
                sonic_rs::to_vec(&build.projection.summary).map_err(|error| {
                    Error::Internal(format!("Failed to encode annotation projection: {error}"))
                })?,
            ),
        );
        if let Some(serial) = &build.projection.last_annotation_serial {
            item.insert("last_serial".to_string(), Self::attr_s(serial.as_str()));
        }
        item.insert(
            "updated_at_ms".to_string(),
            Self::attr_n(build.projection.updated_at_ms),
        );
        item.insert("revision".to_string(), Self::attr_n(revision));
        if let Some(valid_until) = build.valid_until {
            item.insert("valid_until".to_string(), Self::attr_n(valid_until));
        }
        if let Some(expires_at) = build.expires_at {
            item.insert(EXPIRES_AT_ATTR.to_string(), Self::attr_n(expires_at));
        }
        Ok(item)
    }

    fn decode_projection(
        app_id: &str,
        channel_id: &str,
        item: &HashMap<String, AttributeValue>,
    ) -> Result<ProjectionState> {
        let summary: AnnotationSummary =
            sonic_rs::from_slice(&Self::item_bytes(item, "summary_bytes")?).map_err(|error| {
                Error::Internal(format!(
                    "Failed to decode DynamoDB annotation projection: {error}"
                ))
            })?;
        let message_serial =
            MessageSerial::new(Self::item_string(item, "message_serial").ok_or_else(|| {
                Error::Internal("DynamoDB projection is missing message_serial".to_string())
            })?)?;
        let annotation_type =
            AnnotationType::new(Self::item_string(item, "annotation_type").ok_or_else(|| {
                Error::Internal("DynamoDB projection is missing annotation_type".to_string())
            })?)?;
        let last_annotation_serial = Self::item_string(item, "last_serial")
            .map(AnnotationSerial::new)
            .transpose()?;
        Ok(ProjectionState {
            projection: StoredAnnotationProjection {
                app_id: app_id.to_string(),
                channel_id: channel_id.to_string(),
                message_serial,
                annotation_type,
                summary,
                last_annotation_serial,
                updated_at_ms: Self::item_i64(item, "updated_at_ms").unwrap_or_default(),
            },
            revision: Self::item_i64(item, "revision").unwrap_or_default() as u64,
            valid_until: Self::item_i64(item, "valid_until"),
        })
    }

    async fn ensure_table(&self) -> Result<()> {
        if self
            .client
            .describe_table()
            .table_name(&self.table)
            .send()
            .await
            .is_err()
        {
            let create = self
                .client
                .create_table()
                .table_name(&self.table)
                .billing_mode(BillingMode::PayPerRequest)
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("app_channel")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .map_err(|error| {
                            Error::Internal(format!(
                                "Failed to build DynamoDB annotation partition: {error}"
                            ))
                        })?,
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("commit_key")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .map_err(|error| {
                            Error::Internal(format!(
                                "Failed to build DynamoDB annotation sort key: {error}"
                            ))
                        })?,
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("app_channel")
                        .key_type(KeyType::Hash)
                        .build()
                        .map_err(|error| {
                            Error::Internal(format!(
                                "Failed to build DynamoDB annotation key schema: {error}"
                            ))
                        })?,
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("commit_key")
                        .key_type(KeyType::Range)
                        .build()
                        .map_err(|error| {
                            Error::Internal(format!(
                                "Failed to build DynamoDB annotation key schema: {error}"
                            ))
                        })?,
                )
                .send()
                .await;
            if let Err(error) = create
                && !error.to_string().contains("ResourceInUse")
            {
                return Err(Error::Internal(format!(
                    "Failed to create DynamoDB annotation table: {error}"
                )));
            }
        }
        self.wait_for_table().await?;
        if self.retention_seconds > 0 {
            self.ensure_ttl().await?;
        }
        Ok(())
    }

    async fn wait_for_table(&self) -> Result<()> {
        for _ in 0..60 {
            if let Ok(response) = self
                .client
                .describe_table()
                .table_name(&self.table)
                .send()
                .await
                && response
                    .table()
                    .and_then(|table| table.table_status().cloned())
                    == Some(TableStatus::Active)
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Err(Error::Internal(format!(
            "Timed out waiting for DynamoDB annotation table {}",
            self.table
        )))
    }

    async fn ensure_ttl(&self) -> Result<()> {
        if let Ok(response) = self
            .client
            .describe_time_to_live()
            .table_name(&self.table)
            .send()
            .await
            && let Some(description) = response.time_to_live_description()
        {
            match (
                description.time_to_live_status(),
                description.attribute_name(),
            ) {
                (Some(TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling), Some(name))
                    if name == EXPIRES_AT_ATTR =>
                {
                    return Ok(());
                }
                (Some(TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling), Some(name)) => {
                    return Err(Error::Configuration(format!(
                        "DynamoDB annotation table {} uses TTL attribute '{name}', expected '{EXPIRES_AT_ATTR}'",
                        self.table
                    )));
                }
                _ => {}
            }
        }
        let specification = TimeToLiveSpecification::builder()
            .enabled(true)
            .attribute_name(EXPIRES_AT_ATTR)
            .build()
            .map_err(|error| {
                Error::Internal(format!("Failed to build DynamoDB annotation TTL: {error}"))
            })?;
        if let Err(error) = self
            .client
            .update_time_to_live()
            .table_name(&self.table)
            .time_to_live_specification(specification)
            .send()
            .await
        {
            tracing::warn!(
                table = %self.table,
                error = %error,
                "DynamoDB annotation TTL is unavailable; logical expiry remains active but cleanup is degraded"
            );
        }
        Ok(())
    }

    async fn get_item(
        &self,
        partition: &str,
        commit_key: &str,
    ) -> Result<Option<HashMap<String, AttributeValue>>> {
        self.client
            .get_item()
            .table_name(&self.table)
            .key("app_channel", Self::attr_s(partition))
            .key("commit_key", Self::attr_s(commit_key))
            .consistent_read(true)
            .send()
            .await
            .map(|response| response.item)
            .map_err(|error| {
                Error::Internal(format!("Failed to read DynamoDB annotation row: {error}"))
            })
    }

    async fn query_prefix(
        &self,
        partition: &str,
        prefix: &str,
        after_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<HashMap<String, AttributeValue>>> {
        let lower = after_key
            .map(ToString::to_string)
            .unwrap_or_else(|| format!("{prefix}:"));
        let upper = format!("{prefix};");
        let mut start_key = None;
        let mut result = Vec::with_capacity(limit.min(256));
        let now_seconds = now_ms().div_euclid(1_000);
        loop {
            let response = self
                .client
                .query()
                .table_name(&self.table)
                .key_condition_expression(
                    "app_channel = :partition AND commit_key BETWEEN :lower AND :upper",
                )
                .expression_attribute_values(":partition", Self::attr_s(partition))
                .expression_attribute_values(":lower", Self::attr_s(&lower))
                .expression_attribute_values(":upper", Self::attr_s(&upper))
                .consistent_read(true)
                .limit(i32::try_from(limit.saturating_add(1)).unwrap_or(i32::MAX))
                .set_exclusive_start_key(start_key)
                .send()
                .await
                .map_err(|error| {
                    Error::Internal(format!("Failed to query DynamoDB annotations: {error}"))
                })?;
            for item in response.items() {
                let key = Self::item_string(item, "commit_key").unwrap_or_default();
                if after_key.is_some_and(|after| key.as_str() <= after) {
                    continue;
                }
                if Self::is_active(item, now_seconds) {
                    result.push(item.clone());
                    if result.len() >= limit {
                        return Ok(result);
                    }
                }
            }
            start_key = response.last_evaluated_key;
            if start_key.is_none() {
                return Ok(result);
            }
        }
    }

    async fn events_for_projection(
        &self,
        request: &AnnotationProjectionRequest,
    ) -> Result<Vec<(StoredAnnotationEvent, Option<i64>)>> {
        let partition = Self::partition_key(&request.app_id, &request.channel_id);
        let prefix =
            Self::projection_event_prefix(&request.message_serial, &request.annotation_type);
        let items = self
            .query_prefix(
                &partition,
                &prefix,
                None,
                MAX_PROJECTION_EVENTS.saturating_add(1),
            )
            .await?;
        if items.len() > MAX_PROJECTION_EVENTS {
            return Err(Error::BufferFull(format!(
                "annotation projection exceeds the {MAX_PROJECTION_EVENTS} event bound"
            )));
        }
        items
            .into_iter()
            .map(|item| {
                let expires_at = Self::item_i64(&item, EXPIRES_AT_ATTR);
                Ok((Self::decode_event(&item)?, expires_at))
            })
            .collect()
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

    async fn projection_state(
        &self,
        request: &AnnotationProjectionRequest,
    ) -> Result<Option<ProjectionState>> {
        let partition = Self::partition_key(&request.app_id, &request.channel_id);
        self.get_item(&partition, &Self::projection_key(request))
            .await?
            .map(|item| Self::decode_projection(&request.app_id, &request.channel_id, &item))
            .transpose()
    }

    fn conditioned_put(
        table: &str,
        item: HashMap<String, AttributeValue>,
        expected_revision: Option<u64>,
    ) -> Result<TransactWriteItem> {
        let mut put = Put::builder().table_name(table).set_item(Some(item));
        if let Some(revision) = expected_revision {
            put = put
                .condition_expression("revision = :expected_revision")
                .expression_attribute_values(":expected_revision", Self::attr_n(revision));
        } else {
            put = put.condition_expression("attribute_not_exists(commit_key)");
        }
        Ok(TransactWriteItem::builder()
            .put(put.build().map_err(|error| {
                Error::Internal(format!(
                    "Failed to build annotation projection put: {error}"
                ))
            })?)
            .build())
    }

    fn insert_only_put(
        table: &str,
        item: HashMap<String, AttributeValue>,
    ) -> Result<TransactWriteItem> {
        let put = Put::builder()
            .table_name(table)
            .set_item(Some(item))
            .condition_expression("attribute_not_exists(commit_key)")
            .build()
            .map_err(|error| {
                Error::Internal(format!("Failed to build DynamoDB annotation put: {error}"))
            })?;
        Ok(TransactWriteItem::builder().put(put).build())
    }

    async fn canonical_create(
        &self,
        record: &StoredAnnotationEvent,
    ) -> Result<Option<StoredAnnotationEvent>> {
        let partition = Self::partition_key(&record.app_id, &record.channel_id);
        let Some(item) = self
            .get_item(&partition, &Self::idempotency_key(&record.annotation))
            .await?
        else {
            return Ok(None);
        };
        if !Self::is_active(&item, now_ms().div_euclid(1_000)) {
            return Ok(None);
        }
        let canonical = Self::decode_event(&item)?;
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
        let partition = Self::partition_key(&record.app_id, &record.channel_id);
        let Some(item) = self
            .get_item(&partition, &Self::event_key(&record.annotation.serial))
            .await?
        else {
            return Ok(None);
        };
        if !Self::is_active(&item, now_ms().div_euclid(1_000)) {
            return Ok(None);
        }
        let existing = Self::decode_event(&item)?;
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
        let partition = Self::partition_key(&record.app_id, &record.channel_id);
        let payload = sonic_rs::to_vec(&record).map_err(|error| {
            Error::Internal(format!(
                "Failed to encode DynamoDB annotation event: {error}"
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

            let state = self.projection_state(&request).await?;
            let mut events = self.events_for_projection(&request).await?;
            if events.len() >= MAX_PROJECTION_EVENTS {
                return Err(Error::BufferFull(format!(
                    "annotation projection exceeds the {MAX_PROJECTION_EVENTS} event bound"
                )));
            }
            events.push((record.clone(), expires_at));
            let build =
                Self::build_projection(&request, &events, AnnotationProjectionOptions::default())?;
            let next_revision = state.as_ref().map_or(1, |state| state.revision + 1);
            let projection_key = Self::projection_key(&request);

            let raw = Self::event_item(
                &partition,
                &Self::event_key(&record.annotation.serial),
                &record,
                &payload,
                expires_at,
            );
            let message = Self::event_item(
                &partition,
                &Self::message_event_key(&record.annotation),
                &record,
                &payload,
                expires_at,
            );
            let projection_event = Self::event_item(
                &partition,
                &Self::projection_event_key(&record.annotation),
                &record,
                &payload,
                expires_at,
            );
            let projection =
                Self::projection_item(&partition, &projection_key, &build, next_revision)?;
            let mut transaction = self
                .client
                .transact_write_items()
                .transact_items(Self::insert_only_put(&self.table, raw)?)
                .transact_items(Self::insert_only_put(&self.table, message)?)
                .transact_items(Self::insert_only_put(&self.table, projection_event)?)
                .transact_items(Self::conditioned_put(
                    &self.table,
                    projection,
                    state.as_ref().map(|state| state.revision),
                )?);
            if create_idempotent {
                let receipt = Self::event_item(
                    &partition,
                    &Self::idempotency_key(&record.annotation),
                    &record,
                    &payload,
                    expires_at,
                );
                transaction =
                    transaction.transact_items(Self::insert_only_put(&self.table, receipt)?);
            }
            match transaction.send().await {
                Ok(_) => {
                    return Ok(AnnotationAppendOutcome {
                        projection: build.projection,
                        canonical_serial: record.annotation.serial.clone(),
                        inserted: true,
                    });
                }
                Err(error)
                    if error.as_service_error().is_some_and(|service_error| {
                        service_error.is_transaction_canceled_exception()
                    }) =>
                {
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
                        "Failed to transact DynamoDB annotation event: {error}"
                    )));
                }
            }
        }
        Err(Error::Internal(
            "DynamoDB annotation compare-and-apply contention exceeded the retry bound".to_string(),
        ))
    }

    async fn persist_rebuild(
        &self,
        request: &AnnotationProjectionRequest,
        options: AnnotationProjectionOptions,
    ) -> Result<ProjectionBuild> {
        let partition = Self::partition_key(&request.app_id, &request.channel_id);
        let projection_key = Self::projection_key(request);
        for _ in 0..MAX_CAS_ATTEMPTS {
            let state = self.projection_state(request).await?;
            let events = self.events_for_projection(request).await?;
            let build = Self::build_projection(request, &events, options)?;
            if build.event_count == 0 {
                if let Some(state) = state {
                    let result = self
                        .client
                        .delete_item()
                        .table_name(&self.table)
                        .key("app_channel", Self::attr_s(&partition))
                        .key("commit_key", Self::attr_s(&projection_key))
                        .condition_expression("revision = :expected_revision")
                        .expression_attribute_values(
                            ":expected_revision",
                            Self::attr_n(state.revision),
                        )
                        .return_values(ReturnValue::None)
                        .send()
                        .await;
                    match result {
                        Ok(_) => return Ok(build),
                        Err(error)
                            if error.as_service_error().is_some_and(|service_error| {
                                service_error.is_conditional_check_failed_exception()
                            }) =>
                        {
                            continue;
                        }
                        Err(error) => {
                            return Err(Error::Internal(format!(
                                "Failed to remove expired DynamoDB annotation projection: {error}"
                            )));
                        }
                    }
                }
                return Ok(build);
            }
            let next_revision = state.as_ref().map_or(1, |state| state.revision + 1);
            let item = Self::projection_item(&partition, &projection_key, &build, next_revision)?;
            let put = Self::conditioned_put(
                &self.table,
                item,
                state.as_ref().map(|state| state.revision),
            )?;
            match self
                .client
                .transact_write_items()
                .transact_items(put)
                .send()
                .await
            {
                Ok(_) => return Ok(build),
                Err(error)
                    if error.as_service_error().is_some_and(|service_error| {
                        service_error.is_transaction_canceled_exception()
                    }) =>
                {
                    continue;
                }
                Err(error) => {
                    return Err(Error::Internal(format!(
                        "Failed to rebuild DynamoDB annotation projection: {error}"
                    )));
                }
            }
        }
        Err(Error::Internal(
            "DynamoDB projection rebuild contention exceeded the retry bound".to_string(),
        ))
    }
}

#[async_trait]
impl AnnotationStore for DynamoDbAnnotationStore {
    async fn reserve_annotation_serial(
        &self,
        app_id: &str,
        channel_id: &str,
    ) -> Result<Option<AnnotationSerial>> {
        let partition = Self::partition_key(app_id, channel_id);
        let response = self
            .client
            .update_item()
            .table_name(&self.table)
            .key("app_channel", Self::attr_s(&partition))
            .key("commit_key", Self::attr_s("s"))
            .update_expression(
                "SET next_serial = if_not_exists(next_serial, :zero) + :one, updated_at_ms = :now",
            )
            .expression_attribute_values(":zero", Self::attr_n(0))
            .expression_attribute_values(":one", Self::attr_n(1))
            .expression_attribute_values(":now", Self::attr_n(now_ms()))
            .return_values(ReturnValue::UpdatedNew)
            .send()
            .await
            .map_err(|error| {
                Error::Internal(format!(
                    "Failed to reserve DynamoDB annotation serial: {error}"
                ))
            })?;
        let serial = response
            .attributes
            .as_ref()
            .and_then(|attributes| Self::item_i64(attributes, "next_serial"))
            .filter(|serial| *serial > 0)
            .ok_or_else(|| {
                Error::Internal(
                    "DynamoDB annotation allocator returned no positive serial".to_string(),
                )
            })?;
        Ok(Some(AnnotationSerial::new(format!("ann:{serial:020}"))?))
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
        let partition = Self::partition_key(&request.app_id, &request.channel_id);
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
        self.query_prefix(&partition, &prefix, after_key.as_deref(), request.limit)
            .await?
            .into_iter()
            .map(|item| Self::decode_event(&item))
            .collect()
    }

    async fn get_event_by_serial(
        &self,
        request: AnnotationEventLookupRequest,
    ) -> Result<Option<StoredAnnotationEvent>> {
        request.validate()?;
        let partition = Self::partition_key(&request.app_id, &request.channel_id);
        let Some(item) = self
            .get_item(&partition, &Self::event_key(&request.annotation_serial))
            .await?
        else {
            return Ok(None);
        };
        if !Self::is_active(&item, now_ms().div_euclid(1_000)) {
            return Ok(None);
        }
        Ok(Some(Self::decode_event(&item)?))
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
        Ok(Some(state.projection))
    }

    async fn list_projections_for_channel(
        &self,
        request: AnnotationProjectionsForChannelRequest,
    ) -> Result<Vec<StoredAnnotationProjection>> {
        request.validate()?;
        let partition = Self::partition_key(&request.app_id, &request.channel_id);
        let items = self
            .query_prefix(
                &partition,
                "p",
                None,
                MAX_CHANNEL_PROJECTIONS.saturating_add(1),
            )
            .await?;
        if items.len() > MAX_CHANNEL_PROJECTIONS {
            return Err(Error::BufferFull(format!(
                "annotation projection list exceeds the {MAX_CHANNEL_PROJECTIONS} item bound"
            )));
        }
        let mut projections = Vec::with_capacity(items.len());
        for item in items {
            let state = Self::decode_projection(&request.app_id, &request.channel_id, &item)?;
            if state
                .valid_until
                .is_some_and(|valid_until| valid_until <= now_ms().div_euclid(1_000))
            {
                if let Some(projection) = self
                    .get_projection(state.projection.projection_key())
                    .await?
                {
                    projections.push(projection);
                }
            } else {
                projections.push(state.projection);
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
        // DynamoDB TTL performs physical cleanup. Every read also enforces the
        // logical expiry timestamp, so asynchronous deletion cannot expose
        // stale events or projections.
        Ok((0, false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::annotations::{
        AnnotationId, AnnotationSummary, AnnotationType, TotalAnnotationSummary,
    };

    fn settings() -> DynamoDbSettings {
        DynamoDbSettings {
            region: "us-east-1".to_string(),
            table_name: "unused".to_string(),
            endpoint_url: Some("http://127.0.0.1:18000".to_string()),
            aws_access_key_id: Some("dummy".to_string()),
            aws_secret_access_key: Some("dummy".to_string()),
            aws_profile_name: None,
        }
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
    async fn two_dynamodb_nodes_allocate_and_apply_annotations_atomically() {
        let prefix = format!(
            "sockudo_annotation_test_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        let first = Arc::new(
            DynamoDbAnnotationStore::new(&settings(), &prefix, 3_600)
                .await
                .unwrap(),
        );
        let second = Arc::new(
            DynamoDbAnnotationStore::new(&settings(), &prefix, 3_600)
                .await
                .unwrap(),
        );

        let first_allocations = {
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
        let second_allocations = {
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
        let mut serials = first_allocations.await.unwrap();
        serials.extend(second_allocations.await.unwrap());
        serials.sort();
        serials.dedup();
        assert_eq!(serials.len(), 100);
        assert_eq!(
            serials.first().unwrap().as_str(),
            "ann:00000000000000000001"
        );
        assert_eq!(serials.last().unwrap().as_str(), "ann:00000000000000000100");

        let first_event = event(
            first
                .reserve_annotation_serial("app", "channel")
                .await
                .unwrap()
                .unwrap(),
            "stable-id",
            1,
        );
        let second_event = event(
            second
                .reserve_annotation_serial("app", "channel")
                .await
                .unwrap()
                .unwrap(),
            "stable-id",
            1,
        );
        let (left, right) = tokio::join!(
            first.append_create_idempotent(first_event),
            second.append_create_idempotent(second_event)
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_ne!(left.inserted, right.inserted);
        assert_eq!(left.canonical_serial, right.canonical_serial);
        let events = first
            .get_events(AnnotationEventsRequest {
                app_id: "app".to_string(),
                channel_id: "channel".to_string(),
                message_serial: MessageSerial::new("msg:1").unwrap(),
                annotation_type: AnnotationType::new("reaction:total.v1").unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
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
        assert!(distinct_left.unwrap().inserted);
        let distinct_right = distinct_right.unwrap();
        assert!(distinct_right.inserted);
        assert_eq!(
            distinct_right.projection.summary,
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
            .client
            .delete_table()
            .table_name(&first.table)
            .send()
            .await
            .unwrap();
    }
}
