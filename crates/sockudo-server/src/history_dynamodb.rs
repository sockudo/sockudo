use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement,
    KeyType, Projection, ProjectionType, ScalarAttributeType, TableStatus,
};
use dashmap::DashMap;
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDirection, HistoryDurableState, HistoryItem,
    HistoryPage, HistoryPurgeMode, HistoryPurgeRequest, HistoryPurgeResult, HistoryQueryBounds,
    HistoryReadRequest, HistoryResetResult, HistoryRetentionStats, HistoryRuntimeStatus,
    HistoryStore, HistoryStreamInspection, HistoryStreamRuntimeState, HistoryWriteReservation,
};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::{DynamoDbSettings, HistoryConfig};
use sonic_rs::JsonValueTrait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tracing::{error, info};

#[derive(Clone)]
struct HistoryTables {
    streams: String,
    entries: String,
    entries_time_index: String,
}

#[derive(Debug, Clone)]
struct HistoryDegradedState {
    app_id: String,
    channel: String,
    durable_state: HistoryDurableState,
    reason: String,
    node_id: Option<String>,
    last_transition_at_ms: i64,
    observed_source: &'static str,
}

#[derive(Debug, Clone)]
struct HistoryStreamRecord {
    stream_id: String,
    next_serial: u64,
    durable_state: HistoryDurableState,
    durable_state_reason: Option<String>,
    durable_state_node_id: Option<String>,
    durable_state_changed_at_ms: Option<i64>,
    retained: HistoryRetentionStats,
}

impl HistoryStreamRecord {
    fn runtime_state(
        &self,
        app_id: &str,
        channel: &str,
        observed_source: &str,
    ) -> HistoryStreamRuntimeState {
        HistoryStreamRuntimeState {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: Some(self.stream_id.clone()),
            durable_state: self.durable_state,
            recovery_allowed: self.durable_state.recovery_allowed(),
            reset_required: self.durable_state.reset_required(),
            reason: self.durable_state_reason.clone(),
            node_id: self.durable_state_node_id.clone(),
            last_transition_at_ms: self.durable_state_changed_at_ms,
            authoritative_source: "durable_store".to_string(),
            observed_source: observed_source.to_string(),
        }
    }

    fn inspection(
        &self,
        app_id: &str,
        channel: &str,
        observed_source: &str,
    ) -> HistoryStreamInspection {
        HistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: Some(self.stream_id.clone()),
            next_serial: Some(self.next_serial),
            retained: self.retained.clone(),
            state: self.runtime_state(app_id, channel, observed_source),
        }
    }
}

#[derive(Debug, Clone)]
struct StoredStreamRecord {
    app_id: String,
    channel: String,
    stream_id: String,
    next_serial: u64,
    retained_messages: u64,
    retained_bytes: u64,
    oldest_available_serial: Option<u64>,
    newest_available_serial: Option<u64>,
    oldest_available_published_at_ms: Option<i64>,
    newest_available_published_at_ms: Option<i64>,
    durable_state: HistoryDurableState,
    durable_state_reason: Option<String>,
    durable_state_node_id: Option<String>,
    durable_state_changed_at_ms: Option<i64>,
    updated_at_ms: i64,
}

#[derive(Debug, Clone)]
struct StoredEntryRecord {
    app_id: String,
    channel: String,
    stream_id: String,
    serial: u64,
    published_at_ms: i64,
    message_id: Option<String>,
    event_name: Option<String>,
    operation_kind: String,
    payload_bytes: Vec<u8>,
    payload_size_bytes: u64,
}

pub struct DynamoDbHistoryStore {
    client: Client,
    tables: HistoryTables,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    degraded_channels: Arc<DashMap<String, HistoryDegradedState>>,
    queue_depth_total: AtomicUsize,
}

pub async fn create_dynamodb_history_store(
    db_config: &DynamoDbSettings,
    config: HistoryConfig,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
) -> Result<Arc<dyn HistoryStore + Send + Sync>> {
    let store = DynamoDbHistoryStore::new(db_config, config, metrics, cache_manager).await?;
    Ok(Arc::new(store))
}

impl DynamoDbHistoryStore {
    async fn new(
        db_config: &DynamoDbSettings,
        _config: HistoryConfig,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
        cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
    ) -> Result<Self> {
        let mut aws_config_builder = aws_config::from_env().region(
            aws_sdk_dynamodb::config::Region::new(db_config.region.clone()),
        );

        if let Some(endpoint) = &db_config.endpoint_url {
            aws_config_builder = aws_config_builder.endpoint_url(endpoint);
        }
        if let (Some(access_key), Some(secret_key)) = (
            &db_config.aws_access_key_id,
            &db_config.aws_secret_access_key,
        ) {
            let credentials_provider = aws_sdk_dynamodb::config::Credentials::new(
                access_key, secret_key, None, None, "static",
            );
            aws_config_builder = aws_config_builder.credentials_provider(credentials_provider);
        }
        if let Some(profile) = &db_config.aws_profile_name {
            aws_config_builder = aws_config_builder.profile_name(profile);
        }

        let client = Client::new(&aws_config_builder.load().await);
        let store = Self {
            client,
            tables: HistoryTables {
                streams: format!("{}_streams", db_config.table_name),
                entries: format!("{}_entries", db_config.table_name),
                entries_time_index: format!("{}_time_idx", db_config.table_name),
            },
            metrics,
            cache_manager,
            degraded_channels: Arc::new(DashMap::new()),
            queue_depth_total: AtomicUsize::new(0),
        };
        store.ensure_tables().await?;
        Ok(store)
    }

    async fn ensure_tables(&self) -> Result<()> {
        self.ensure_streams_table().await?;
        self.ensure_entries_table().await?;
        Ok(())
    }

    async fn ensure_streams_table(&self) -> Result<()> {
        if self
            .client
            .describe_table()
            .table_name(&self.tables.streams)
            .send()
            .await
            .is_ok()
        {
            return Ok(());
        }

        self.client
            .create_table()
            .table_name(&self.tables.streams)
            .billing_mode(BillingMode::PayPerRequest)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("stream_key")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| {
                        Error::Internal(format!(
                            "Failed to build DynamoDB attribute definition: {e}"
                        ))
                    })?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("stream_key")
                    .key_type(KeyType::Hash)
                    .build()
                    .map_err(|e| {
                        Error::Internal(format!("Failed to build DynamoDB key schema: {e}"))
                    })?,
            )
            .send()
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to create DynamoDB history streams table: {e}"
                ))
            })?;
        self.wait_for_active(&self.tables.streams).await
    }

    async fn ensure_entries_table(&self) -> Result<()> {
        if self
            .client
            .describe_table()
            .table_name(&self.tables.entries)
            .send()
            .await
            .is_ok()
        {
            return Ok(());
        }

        self.client
            .create_table()
            .table_name(&self.tables.entries)
            .billing_mode(BillingMode::PayPerRequest)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("stream_partition")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| {
                        Error::Internal(format!(
                            "Failed to build DynamoDB attribute definition: {e}"
                        ))
                    })?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("serial_key")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| {
                        Error::Internal(format!(
                            "Failed to build DynamoDB attribute definition: {e}"
                        ))
                    })?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("published_at_serial_key")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(|e| {
                        Error::Internal(format!(
                            "Failed to build DynamoDB attribute definition: {e}"
                        ))
                    })?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("stream_partition")
                    .key_type(KeyType::Hash)
                    .build()
                    .map_err(|e| {
                        Error::Internal(format!("Failed to build DynamoDB key schema: {e}"))
                    })?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("serial_key")
                    .key_type(KeyType::Range)
                    .build()
                    .map_err(|e| {
                        Error::Internal(format!("Failed to build DynamoDB key schema: {e}"))
                    })?,
            )
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name(&self.tables.entries_time_index)
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("stream_partition")
                            .key_type(KeyType::Hash)
                            .build()
                            .map_err(|e| {
                                Error::Internal(format!(
                                    "Failed to build DynamoDB GSI key schema: {e}"
                                ))
                            })?,
                    )
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("published_at_serial_key")
                            .key_type(KeyType::Range)
                            .build()
                            .map_err(|e| {
                                Error::Internal(format!(
                                    "Failed to build DynamoDB GSI key schema: {e}"
                                ))
                            })?,
                    )
                    .projection(
                        Projection::builder()
                            .projection_type(ProjectionType::All)
                            .build(),
                    )
                    .build()
                    .map_err(|e| {
                        Error::Internal(format!("Failed to build DynamoDB history time index: {e}"))
                    })?,
            )
            .send()
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to create DynamoDB history entries table: {e}"
                ))
            })?;
        self.wait_for_active(&self.tables.entries).await
    }

    async fn wait_for_active(&self, table_name: &str) -> Result<()> {
        for _ in 0..20 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            if let Ok(response) = self
                .client
                .describe_table()
                .table_name(table_name)
                .send()
                .await
                && let Some(table) = response.table()
                && let Some(status) = table.table_status()
                && status == &TableStatus::Active
            {
                return Ok(());
            }
        }
        Err(Error::Internal(format!(
            "Timeout waiting for DynamoDB table {table_name} to become active"
        )))
    }

    fn stream_key(app_id: &str, channel: &str) -> String {
        deterministic_key([app_id, channel].into_iter())
    }

    fn stream_partition(app_id: &str, channel: &str, stream_id: &str) -> String {
        deterministic_key([app_id, channel, stream_id].into_iter())
    }

    fn serial_key(serial: u64) -> String {
        format!("{serial:020}")
    }

    fn published_at_serial_key(published_at_ms: i64, serial: u64) -> String {
        format!("{published_at_ms:020}#{serial:020}")
    }

    fn attr_string(value: &str) -> AttributeValue {
        AttributeValue::S(value.to_string())
    }

    fn attr_number(value: impl ToString) -> AttributeValue {
        AttributeValue::N(value.to_string())
    }

    fn stream_item(
        stream_key: &str,
        record: &StoredStreamRecord,
    ) -> HashMap<String, AttributeValue> {
        let mut item = HashMap::new();
        item.insert("stream_key".to_string(), Self::attr_string(stream_key));
        item.insert("app_id".to_string(), Self::attr_string(&record.app_id));
        item.insert("channel".to_string(), Self::attr_string(&record.channel));
        item.insert(
            "stream_id".to_string(),
            Self::attr_string(&record.stream_id),
        );
        item.insert(
            "next_serial".to_string(),
            Self::attr_number(record.next_serial),
        );
        item.insert(
            "retained_messages".to_string(),
            Self::attr_number(record.retained_messages),
        );
        item.insert(
            "retained_bytes".to_string(),
            Self::attr_number(record.retained_bytes),
        );
        item.insert(
            "durable_state".to_string(),
            Self::attr_string(record.durable_state.as_str()),
        );
        item.insert(
            "updated_at_ms".to_string(),
            Self::attr_number(record.updated_at_ms),
        );
        if let Some(reason) = &record.durable_state_reason {
            item.insert(
                "durable_state_reason".to_string(),
                Self::attr_string(reason),
            );
        }
        if let Some(node_id) = &record.durable_state_node_id {
            item.insert(
                "durable_state_node_id".to_string(),
                Self::attr_string(node_id),
            );
        }
        if let Some(changed_at) = record.durable_state_changed_at_ms {
            item.insert(
                "durable_state_changed_at_ms".to_string(),
                Self::attr_number(changed_at),
            );
        }
        if let Some(value) = record.oldest_available_serial {
            item.insert(
                "oldest_available_serial".to_string(),
                Self::attr_number(value),
            );
        }
        if let Some(value) = record.newest_available_serial {
            item.insert(
                "newest_available_serial".to_string(),
                Self::attr_number(value),
            );
        }
        if let Some(value) = record.oldest_available_published_at_ms {
            item.insert(
                "oldest_available_published_at_ms".to_string(),
                Self::attr_number(value),
            );
        }
        if let Some(value) = record.newest_available_published_at_ms {
            item.insert(
                "newest_available_published_at_ms".to_string(),
                Self::attr_number(value),
            );
        }
        item
    }

    fn entry_item(
        stream_partition: &str,
        record: &StoredEntryRecord,
    ) -> HashMap<String, AttributeValue> {
        let mut item = HashMap::new();
        item.insert(
            "stream_partition".to_string(),
            Self::attr_string(stream_partition),
        );
        item.insert(
            "serial_key".to_string(),
            Self::attr_string(&Self::serial_key(record.serial)),
        );
        item.insert("app_id".to_string(), Self::attr_string(&record.app_id));
        item.insert("channel".to_string(), Self::attr_string(&record.channel));
        item.insert(
            "stream_id".to_string(),
            Self::attr_string(&record.stream_id),
        );
        item.insert("serial".to_string(), Self::attr_number(record.serial));
        item.insert(
            "published_at_ms".to_string(),
            Self::attr_number(record.published_at_ms),
        );
        item.insert(
            "published_at_serial_key".to_string(),
            Self::attr_string(&Self::published_at_serial_key(
                record.published_at_ms,
                record.serial,
            )),
        );
        item.insert(
            "operation_kind".to_string(),
            Self::attr_string(&record.operation_kind),
        );
        item.insert(
            "payload_bytes".to_string(),
            AttributeValue::B(Blob::new(record.payload_bytes.clone())),
        );
        item.insert(
            "payload_size_bytes".to_string(),
            Self::attr_number(record.payload_size_bytes),
        );
        if let Some(message_id) = &record.message_id {
            item.insert("message_id".to_string(), Self::attr_string(message_id));
        }
        if let Some(event_name) = &record.event_name {
            item.insert("event_name".to_string(), Self::attr_string(event_name));
        }
        item
    }

    fn item_attr_string(item: &HashMap<String, AttributeValue>, key: &str) -> Result<String> {
        match item.get(key) {
            Some(AttributeValue::S(value)) => Ok(value.clone()),
            _ => Err(Error::Internal(format!(
                "Missing or invalid DynamoDB string attribute {key}"
            ))),
        }
    }

    fn item_attr_u64(item: &HashMap<String, AttributeValue>, key: &str) -> Result<u64> {
        match item.get(key) {
            Some(AttributeValue::N(value)) => value.parse::<u64>().map_err(|e| {
                Error::Internal(format!("Invalid DynamoDB numeric attribute {key}: {e}"))
            }),
            _ => Err(Error::Internal(format!(
                "Missing or invalid DynamoDB numeric attribute {key}"
            ))),
        }
    }

    fn item_attr_i64(item: &HashMap<String, AttributeValue>, key: &str) -> Result<i64> {
        match item.get(key) {
            Some(AttributeValue::N(value)) => value.parse::<i64>().map_err(|e| {
                Error::Internal(format!("Invalid DynamoDB numeric attribute {key}: {e}"))
            }),
            _ => Err(Error::Internal(format!(
                "Missing or invalid DynamoDB numeric attribute {key}"
            ))),
        }
    }

    fn item_opt_string(item: &HashMap<String, AttributeValue>, key: &str) -> Option<String> {
        match item.get(key) {
            Some(AttributeValue::S(value)) => Some(value.clone()),
            _ => None,
        }
    }

    fn item_opt_i64(item: &HashMap<String, AttributeValue>, key: &str) -> Option<i64> {
        match item.get(key) {
            Some(AttributeValue::N(value)) => value.parse::<i64>().ok(),
            _ => None,
        }
    }

    fn stream_from_item(item: HashMap<String, AttributeValue>) -> Result<StoredStreamRecord> {
        Ok(StoredStreamRecord {
            app_id: Self::item_attr_string(&item, "app_id")?,
            channel: Self::item_attr_string(&item, "channel")?,
            stream_id: Self::item_attr_string(&item, "stream_id")?,
            next_serial: Self::item_attr_u64(&item, "next_serial")?,
            retained_messages: Self::item_attr_u64(&item, "retained_messages").unwrap_or(0),
            retained_bytes: Self::item_attr_u64(&item, "retained_bytes").unwrap_or(0),
            oldest_available_serial: Self::item_opt_i64(&item, "oldest_available_serial")
                .map(|value| value as u64),
            newest_available_serial: Self::item_opt_i64(&item, "newest_available_serial")
                .map(|value| value as u64),
            oldest_available_published_at_ms: Self::item_opt_i64(
                &item,
                "oldest_available_published_at_ms",
            ),
            newest_available_published_at_ms: Self::item_opt_i64(
                &item,
                "newest_available_published_at_ms",
            ),
            durable_state: parse_history_durable_state(&Self::item_attr_string(
                &item,
                "durable_state",
            )?),
            durable_state_reason: Self::item_opt_string(&item, "durable_state_reason"),
            durable_state_node_id: Self::item_opt_string(&item, "durable_state_node_id"),
            durable_state_changed_at_ms: Self::item_opt_i64(&item, "durable_state_changed_at_ms"),
            updated_at_ms: Self::item_attr_i64(&item, "updated_at_ms")?,
        })
    }

    fn entry_from_item(item: HashMap<String, AttributeValue>) -> Result<StoredEntryRecord> {
        Ok(StoredEntryRecord {
            app_id: Self::item_attr_string(&item, "app_id")?,
            channel: Self::item_attr_string(&item, "channel")?,
            stream_id: Self::item_attr_string(&item, "stream_id")?,
            serial: Self::item_attr_u64(&item, "serial")?,
            published_at_ms: Self::item_attr_i64(&item, "published_at_ms")?,
            message_id: Self::item_opt_string(&item, "message_id"),
            event_name: Self::item_opt_string(&item, "event_name"),
            operation_kind: Self::item_attr_string(&item, "operation_kind")?,
            payload_bytes: match item.get("payload_bytes") {
                Some(AttributeValue::B(value)) => value.clone().into_inner(),
                _ => {
                    return Err(Error::Internal(
                        "Missing or invalid DynamoDB binary attribute payload_bytes".to_string(),
                    ));
                }
            },
            payload_size_bytes: Self::item_attr_u64(&item, "payload_size_bytes")?,
        })
    }

    async fn load_stream_raw(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Option<StoredStreamRecord>> {
        let response = self
            .client
            .get_item()
            .table_name(&self.tables.streams)
            .key(
                "stream_key",
                Self::attr_string(&Self::stream_key(app_id, channel)),
            )
            .send()
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to fetch DynamoDB history stream: {e}"))
            })?;
        match response.item {
            Some(item) => Ok(Some(Self::stream_from_item(item)?)),
            None => Ok(None),
        }
    }

    async fn query_entries(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        direction: HistoryDirection,
    ) -> Result<Vec<StoredEntryRecord>> {
        let partition = Self::stream_partition(app_id, channel, stream_id);
        let mut items = Vec::new();
        let mut start_key = None;
        loop {
            let response = self
                .client
                .query()
                .table_name(&self.tables.entries)
                .key_condition_expression("#pk = :pk")
                .expression_attribute_names("#pk", "stream_partition")
                .expression_attribute_values(":pk", Self::attr_string(&partition))
                .scan_index_forward(matches!(direction, HistoryDirection::OldestFirst))
                .set_exclusive_start_key(start_key)
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to query DynamoDB history entries: {e}"))
                })?;
            for item in response.items() {
                items.push(Self::entry_from_item(item.clone())?);
            }
            start_key = response.last_evaluated_key;
            if start_key.is_none() {
                break;
            }
        }
        Ok(items)
    }

    async fn query_page_entries(
        &self,
        request: &HistoryReadRequest,
        stream_id: &str,
    ) -> Result<Vec<StoredEntryRecord>> {
        if request.bounds.start_time_ms.is_some() || request.bounds.end_time_ms.is_some() {
            return self.query_time_page_entries(request, stream_id).await;
        }

        let partition = Self::stream_partition(&request.app_id, &request.channel, stream_id);
        let lower_serial = match request.direction {
            HistoryDirection::NewestFirst => request.bounds.start_serial,
            HistoryDirection::OldestFirst => {
                match (request.bounds.start_serial, request.cursor.as_ref()) {
                    (Some(value), Some(cursor)) => Some(value.max(cursor.serial + 1)),
                    (Some(value), None) => Some(value),
                    (None, Some(cursor)) => Some(cursor.serial + 1),
                    (None, None) => None,
                }
            }
        };
        let upper_serial = match request.direction {
            HistoryDirection::NewestFirst => {
                match (request.bounds.end_serial, request.cursor.as_ref()) {
                    (Some(end), Some(cursor)) => Some(end.min(cursor.serial.saturating_sub(1))),
                    (Some(end), None) => Some(end),
                    (None, Some(cursor)) => Some(cursor.serial.saturating_sub(1)),
                    (None, None) => None,
                }
            }
            HistoryDirection::OldestFirst => request.bounds.end_serial,
        };

        let mut key_condition = "#pk = :pk".to_string();
        let mut query = self
            .client
            .query()
            .table_name(&self.tables.entries)
            .expression_attribute_names("#pk", "stream_partition")
            .expression_attribute_values(":pk", Self::attr_string(&partition))
            .scan_index_forward(matches!(request.direction, HistoryDirection::OldestFirst))
            .limit(((request.limit + 1).saturating_mul(4).min(200)) as i32);

        match (lower_serial, upper_serial) {
            (Some(lower), Some(upper)) => {
                query = query.expression_attribute_names("#sk", "serial_key");
                key_condition.push_str(" AND #sk BETWEEN :lower AND :upper");
                query = query
                    .expression_attribute_values(
                        ":lower",
                        Self::attr_string(&Self::serial_key(lower)),
                    )
                    .expression_attribute_values(
                        ":upper",
                        Self::attr_string(&Self::serial_key(upper)),
                    );
            }
            (Some(lower), None) => {
                query = query.expression_attribute_names("#sk", "serial_key");
                key_condition.push_str(" AND #sk >= :lower");
                query = query.expression_attribute_values(
                    ":lower",
                    Self::attr_string(&Self::serial_key(lower)),
                );
            }
            (None, Some(upper)) => {
                query = query.expression_attribute_names("#sk", "serial_key");
                key_condition.push_str(" AND #sk <= :upper");
                query = query.expression_attribute_values(
                    ":upper",
                    Self::attr_string(&Self::serial_key(upper)),
                );
            }
            (None, None) => {}
        }
        query = query.key_condition_expression(key_condition);

        let mut filter_parts = Vec::new();
        if let Some(start_time_ms) = request.bounds.start_time_ms {
            filter_parts.push("published_at_ms >= :start_time_ms".to_string());
            query = query
                .expression_attribute_values(":start_time_ms", Self::attr_number(start_time_ms));
        }
        if let Some(end_time_ms) = request.bounds.end_time_ms {
            filter_parts.push("published_at_ms <= :end_time_ms".to_string());
            query =
                query.expression_attribute_values(":end_time_ms", Self::attr_number(end_time_ms));
        }
        if !filter_parts.is_empty() {
            query = query.filter_expression(filter_parts.join(" AND "));
        }

        let mut items = Vec::new();
        let mut start_key = None;
        while items.len() <= request.limit {
            let response = query
                .clone()
                .set_exclusive_start_key(start_key)
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to query DynamoDB paged history entries: {e:?}"
                    ))
                })?;
            for item in response.items() {
                items.push(Self::entry_from_item(item.clone())?);
                if items.len() > request.limit {
                    break;
                }
            }
            start_key = response.last_evaluated_key;
            if start_key.is_none() {
                break;
            }
        }
        Ok(items)
    }

    async fn load_entry_by_serial(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        serial: u64,
    ) -> Result<Option<StoredEntryRecord>> {
        let response = self
            .client
            .get_item()
            .table_name(&self.tables.entries)
            .key(
                "stream_partition",
                Self::attr_string(&Self::stream_partition(app_id, channel, stream_id)),
            )
            .key("serial_key", Self::attr_string(&Self::serial_key(serial)))
            .send()
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to fetch DynamoDB history cursor row: {e}"))
            })?;
        match response.item {
            Some(item) => Ok(Some(Self::entry_from_item(item)?)),
            None => Ok(None),
        }
    }

    async fn query_time_page_entries(
        &self,
        request: &HistoryReadRequest,
        stream_id: &str,
    ) -> Result<Vec<StoredEntryRecord>> {
        let partition = Self::stream_partition(&request.app_id, &request.channel, stream_id);
        let cursor_time_key = if let Some(cursor) = request.cursor.as_ref() {
            let cursor_row = self
                .load_entry_by_serial(&request.app_id, &request.channel, stream_id, cursor.serial)
                .await?
                .ok_or_else(|| {
                    Error::InvalidMessageFormat(
                        "Expired history cursor: cursor item no longer retained".to_string(),
                    )
                })?;
            Some(Self::published_at_serial_key(
                cursor_row.published_at_ms,
                cursor_row.serial,
            ))
        } else {
            None
        };

        let lower_time_key = request
            .bounds
            .start_time_ms
            .map(|value| Self::published_at_serial_key(value, 0));
        let upper_time_key = request
            .bounds
            .end_time_ms
            .map(|value| Self::published_at_serial_key(value, u64::MAX));
        let lower_key = match request.direction {
            HistoryDirection::OldestFirst => match (&lower_time_key, &cursor_time_key) {
                (Some(lower), Some(cursor)) => Some(lower.max(cursor).clone()),
                (Some(lower), None) => Some(lower.clone()),
                (None, Some(cursor)) => Some(cursor.clone()),
                (None, None) => None,
            },
            HistoryDirection::NewestFirst => lower_time_key.clone(),
        };
        let upper_key = match request.direction {
            HistoryDirection::OldestFirst => upper_time_key.clone(),
            HistoryDirection::NewestFirst => match (&upper_time_key, &cursor_time_key) {
                (Some(upper), Some(cursor)) => Some(upper.min(cursor).clone()),
                (Some(upper), None) => Some(upper.clone()),
                (None, Some(cursor)) => Some(cursor.clone()),
                (None, None) => None,
            },
        };

        let mut key_condition = "#pk = :pk".to_string();
        let mut query = self
            .client
            .query()
            .table_name(&self.tables.entries)
            .index_name(&self.tables.entries_time_index)
            .expression_attribute_names("#pk", "stream_partition")
            .expression_attribute_names("#ts", "published_at_serial_key")
            .expression_attribute_values(":pk", Self::attr_string(&partition))
            .scan_index_forward(matches!(request.direction, HistoryDirection::OldestFirst))
            .limit(((request.limit + 1).saturating_mul(4).min(200)) as i32);
        match (&lower_key, &upper_key) {
            (Some(lower), Some(upper)) => {
                key_condition.push_str(" AND #ts BETWEEN :lower_ts AND :upper_ts");
                query = query
                    .expression_attribute_values(":lower_ts", Self::attr_string(lower))
                    .expression_attribute_values(":upper_ts", Self::attr_string(upper));
            }
            (Some(lower), None) => {
                key_condition.push_str(" AND #ts >= :lower_ts");
                query = query.expression_attribute_values(":lower_ts", Self::attr_string(lower));
            }
            (None, Some(upper)) => {
                key_condition.push_str(" AND #ts <= :upper_ts");
                query = query.expression_attribute_values(":upper_ts", Self::attr_string(upper));
            }
            (None, None) => {}
        }
        query = query.key_condition_expression(key_condition);

        let mut filter_parts = Vec::new();
        if let Some(start_serial) = request.bounds.start_serial {
            filter_parts.push("serial >= :start_serial".to_string());
            query =
                query.expression_attribute_values(":start_serial", Self::attr_number(start_serial));
        }
        if let Some(end_serial) = request.bounds.end_serial {
            filter_parts.push("serial <= :end_serial".to_string());
            query = query.expression_attribute_values(":end_serial", Self::attr_number(end_serial));
        }
        if !filter_parts.is_empty() {
            query = query.filter_expression(filter_parts.join(" AND "));
        }

        let mut items = Vec::new();
        let mut start_key = None;
        while items.len() <= request.limit {
            let response = query
                .clone()
                .set_exclusive_start_key(start_key)
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to query DynamoDB time-index history entries: {e:?}"
                    ))
                })?;
            for item in response.items() {
                let entry = Self::entry_from_item(item.clone())?;
                if let Some(cursor_key) = &cursor_time_key
                    && Self::published_at_serial_key(entry.published_at_ms, entry.serial)
                        == *cursor_key
                {
                    continue;
                }
                items.push(entry);
                if items.len() > request.limit {
                    break;
                }
            }
            start_key = response.last_evaluated_key;
            if start_key.is_none() {
                break;
            }
        }
        Ok(items)
    }

    async fn delete_entries(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        rows: &[StoredEntryRecord],
    ) -> Result<()> {
        for row in rows {
            self.client
                .delete_item()
                .table_name(&self.tables.entries)
                .key(
                    "stream_partition",
                    Self::attr_string(&Self::stream_partition(app_id, channel, stream_id)),
                )
                .key(
                    "serial_key",
                    Self::attr_string(&Self::serial_key(row.serial)),
                )
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to delete DynamoDB history row: {e}"))
                })?;
        }
        Ok(())
    }

    async fn load_stream_record(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Option<HistoryStreamRecord>> {
        let Some(stream) = self.load_stream_raw(app_id, channel).await? else {
            return Ok(None);
        };
        Ok(Some(HistoryStreamRecord {
            stream_id: stream.stream_id.clone(),
            next_serial: stream.next_serial,
            durable_state: stream.durable_state,
            durable_state_reason: stream.durable_state_reason.clone(),
            durable_state_node_id: stream.durable_state_node_id.clone(),
            durable_state_changed_at_ms: stream.durable_state_changed_at_ms,
            retained: retained_from_stream_record(&stream),
        }))
    }

    async fn retained_stats(&self, app_id: &str, channel: &str) -> Result<HistoryRetentionStats> {
        Ok(match self.load_stream_raw(app_id, channel).await? {
            Some(stream) => retained_from_stream_record(&stream),
            None => HistoryRetentionStats::default(),
        })
    }

    async fn upsert_stream_raw(
        &self,
        app_id: &str,
        channel: &str,
        record: &StoredStreamRecord,
    ) -> Result<()> {
        self.client
            .put_item()
            .table_name(&self.tables.streams)
            .set_item(Some(Self::stream_item(
                &Self::stream_key(app_id, channel),
                record,
            )))
            .send()
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to upsert DynamoDB history stream: {e}"))
            })?;
        Ok(())
    }

    async fn resolved_stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamRuntimeState> {
        let durable_record = self.load_stream_record(app_id, channel).await?;
        let durable_state = durable_record
            .as_ref()
            .map(|record| record.runtime_state(app_id, channel, "durable_store"))
            .unwrap_or_else(|| {
                HistoryStreamRuntimeState::healthy(app_id, channel, None, "durable_store")
            });
        let local_hint = self
            .degraded_channels
            .get(&degraded_channel_key(app_id, channel))
            .map(|entry| entry.value().clone());
        let cache_hint =
            get_cached_channel_degraded(self.cache_manager.as_ref(), app_id, channel).await?;
        Ok(resolve_runtime_state(durable_state, local_hint, cache_hint))
    }

    async fn resolved_stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamInspection> {
        let durable_record = self.load_stream_record(app_id, channel).await?;
        let runtime_state = self.resolved_stream_runtime_state(app_id, channel).await?;
        Ok(match durable_record {
            Some(record) => record.inspection(app_id, channel, &runtime_state.observed_source),
            None => HistoryStreamInspection {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                stream_id: None,
                next_serial: None,
                retained: HistoryRetentionStats::default(),
                state: runtime_state,
            },
        })
    }

    async fn persist_record(&self, record: &HistoryAppendRecord) -> Result<()> {
        let stored = StoredEntryRecord {
            app_id: record.app_id.clone(),
            channel: record.channel.clone(),
            stream_id: record.stream_id.clone(),
            serial: record.serial,
            published_at_ms: record.published_at_ms,
            message_id: record.message_id.clone(),
            event_name: record.event_name.clone(),
            operation_kind: record.operation_kind.clone(),
            payload_bytes: record.payload_bytes.to_vec(),
            payload_size_bytes: record.payload_bytes.len() as u64,
        };
        let partition = Self::stream_partition(&record.app_id, &record.channel, &record.stream_id);
        self.client
            .put_item()
            .table_name(&self.tables.entries)
            .set_item(Some(Self::entry_item(&partition, &stored)))
            .condition_expression(
                "attribute_not_exists(stream_partition) AND attribute_not_exists(serial_key)",
            )
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create DynamoDB history row: {e}")))?;

        let mut rows = self
            .query_entries(
                &record.app_id,
                &record.channel,
                &record.stream_id,
                HistoryDirection::OldestFirst,
            )
            .await?;
        let cutoff_ms = record
            .published_at_ms
            .saturating_sub((record.retention.retention_window_seconds * 1000) as i64);
        let mut to_delete = Vec::new();

        while let Some(first) = rows.first() {
            if first.published_at_ms < cutoff_ms {
                to_delete.push(rows.remove(0));
            } else {
                break;
            }
        }
        if let Some(max_messages) = record.retention.max_messages_per_channel {
            while rows.len() > max_messages {
                to_delete.push(rows.remove(0));
            }
        }
        if let Some(max_bytes) = record.retention.max_bytes_per_channel {
            let mut retained_bytes: u64 = rows.iter().map(|row| row.payload_size_bytes).sum();
            while retained_bytes > max_bytes && !rows.is_empty() {
                let removed = rows.remove(0);
                retained_bytes = retained_bytes.saturating_sub(removed.payload_size_bytes);
                to_delete.push(removed);
            }
        }
        self.delete_entries(
            &record.app_id,
            &record.channel,
            &record.stream_id,
            &to_delete,
        )
        .await?;

        if let Some(current) = self
            .load_stream_raw(&record.app_id, &record.channel)
            .await?
            && current.next_serial < record.serial.saturating_add(1)
        {
            let _ = self
                .client
                .update_item()
                .table_name(&self.tables.streams)
                .key(
                    "stream_key",
                    Self::attr_string(&Self::stream_key(&record.app_id, &record.channel)),
                )
                .condition_expression("#next_serial < :floor")
                .update_expression("SET #next_serial = :floor, updated_at_ms = :updated_at_ms")
                .expression_attribute_names("#next_serial", "next_serial")
                .expression_attribute_values(
                    ":floor",
                    Self::attr_number(record.serial.saturating_add(1)),
                )
                .expression_attribute_values(
                    ":updated_at_ms",
                    Self::attr_number(record.published_at_ms),
                )
                .send()
                .await;
        }

        let retained = HistoryRetentionStats {
            stream_id: Some(record.stream_id.clone()),
            retained_messages: rows.len() as u64,
            retained_bytes: rows.iter().map(|row| row.payload_size_bytes).sum(),
            oldest_serial: rows.first().map(|row| row.serial),
            newest_serial: rows.last().map(|row| row.serial),
            oldest_published_at_ms: rows.first().map(|row| row.published_at_ms),
            newest_published_at_ms: rows.last().map(|row| row.published_at_ms),
        };
        if let Some(current) = self
            .load_stream_raw(&record.app_id, &record.channel)
            .await?
        {
            let updated = StoredStreamRecord {
                next_serial: current.next_serial.max(record.serial.saturating_add(1)),
                retained_messages: retained.retained_messages,
                retained_bytes: retained.retained_bytes,
                oldest_available_serial: retained.oldest_serial,
                newest_available_serial: retained.newest_serial,
                oldest_available_published_at_ms: retained.oldest_published_at_ms,
                newest_available_published_at_ms: retained.newest_published_at_ms,
                updated_at_ms: record.published_at_ms,
                ..current
            };
            self.upsert_stream_raw(&record.app_id, &record.channel, &updated)
                .await?;
        }
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.update_history_retained(
                &record.app_id,
                retained.retained_messages,
                retained.retained_bytes,
            );
            if !to_delete.is_empty() {
                let evicted_bytes = to_delete.iter().map(|row| row.payload_size_bytes).sum();
                metrics.mark_history_eviction(
                    &record.app_id,
                    to_delete.len() as u64,
                    evicted_bytes,
                );
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl HistoryStore for DynamoDbHistoryStore {
    async fn reserve_publish_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryWriteReservation> {
        let stream_key = Self::stream_key(app_id, channel);
        loop {
            if let Some(existing) = self.load_stream_raw(app_id, channel).await? {
                let now_ms = sockudo_core::history::now_ms();
                let result = self
                    .client
                    .update_item()
                    .table_name(&self.tables.streams)
                    .key("stream_key", Self::attr_string(&stream_key))
                    .condition_expression("#next_serial = :expected")
                    .update_expression(
                        "SET #next_serial = :next_serial, updated_at_ms = :updated_at_ms",
                    )
                    .expression_attribute_names("#next_serial", "next_serial")
                    .expression_attribute_values(
                        ":expected",
                        Self::attr_number(existing.next_serial),
                    )
                    .expression_attribute_values(
                        ":next_serial",
                        Self::attr_number(existing.next_serial + 1),
                    )
                    .expression_attribute_values(":updated_at_ms", Self::attr_number(now_ms))
                    .send()
                    .await;
                match result {
                    Ok(_) => {
                        return Ok(HistoryWriteReservation {
                            stream_id: existing.stream_id,
                            serial: existing.next_serial,
                        });
                    }
                    Err(err) => {
                        if err.to_string().contains("ConditionalCheckFailed") {
                            continue;
                        }
                        return Err(Error::Internal(format!(
                            "Failed to advance DynamoDB history serial: {err}"
                        )));
                    }
                }
            }

            let now_ms = sockudo_core::history::now_ms();
            let stream = StoredStreamRecord {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                stream_id: uuid::Uuid::new_v4().to_string(),
                next_serial: 2,
                retained_messages: 0,
                retained_bytes: 0,
                oldest_available_serial: None,
                newest_available_serial: None,
                oldest_available_published_at_ms: None,
                newest_available_published_at_ms: None,
                durable_state: HistoryDurableState::Healthy,
                durable_state_reason: None,
                durable_state_node_id: None,
                durable_state_changed_at_ms: None,
                updated_at_ms: now_ms,
            };
            let create_result = self
                .client
                .put_item()
                .table_name(&self.tables.streams)
                .set_item(Some(Self::stream_item(&stream_key, &stream)))
                .condition_expression("attribute_not_exists(stream_key)")
                .send()
                .await;
            match create_result {
                Ok(_) => {
                    return Ok(HistoryWriteReservation {
                        stream_id: stream.stream_id,
                        serial: 1,
                    });
                }
                Err(err) => {
                    if err.to_string().contains("ConditionalCheckFailed") {
                        continue;
                    }
                    return Err(Error::Internal(format!(
                        "Failed to create DynamoDB history stream row: {err}"
                    )));
                }
            }
        }
    }

    async fn append(&self, record: HistoryAppendRecord) -> Result<()> {
        let started = Instant::now();
        if let Err(err) = self.persist_record(&record).await {
            mark_channel_degraded(
                &self.client,
                &self.tables,
                &self.degraded_channels,
                self.cache_manager.as_ref(),
                self.metrics.as_deref(),
                DegradeRequest {
                    app_id: &record.app_id,
                    channel: &record.channel,
                    reason: "durable_history_write_failed",
                    node_id: None,
                },
            )
            .await;
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.mark_history_write_failure(&record.app_id);
            }
            return Err(err);
        }
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.mark_history_write(&record.app_id);
            metrics.track_history_write_latency(
                &record.app_id,
                started.elapsed().as_secs_f64() * 1000.0,
            );
        }
        Ok(())
    }

    async fn read_page(&self, request: HistoryReadRequest) -> Result<HistoryPage> {
        request.validate()?;
        let stream_runtime = self
            .resolved_stream_runtime_state(&request.app_id, &request.channel)
            .await?;
        if !stream_runtime.recovery_allowed {
            return Err(Error::Internal(format!(
                "History stream state blocks cold reads for {}/{}: {}",
                request.app_id,
                request.channel,
                stream_runtime
                    .reason
                    .unwrap_or_else(|| stream_runtime.durable_state.as_str().to_string())
            )));
        }

        let retained = self
            .retained_stats(&request.app_id, &request.channel)
            .await?;
        if let Some(cursor) = request.cursor.as_ref() {
            if let Some(stream_id) = retained.stream_id.as_ref()
                && cursor.stream_id != *stream_id
            {
                return Err(Error::InvalidMessageFormat(
                    "Expired history cursor: channel stream changed".to_string(),
                ));
            }
            if let Some(oldest_serial) = retained.oldest_serial
                && cursor.serial < oldest_serial
            {
                return Err(Error::InvalidMessageFormat(
                    "Expired history cursor: cursor points before retained history".to_string(),
                ));
            }
        }

        let Some(stream_id) = retained.stream_id.as_deref() else {
            return Ok(HistoryPage {
                items: Vec::new(),
                next_cursor: None,
                retained,
                has_more: false,
                complete: true,
                truncated_by_retention: false,
            });
        };

        let rows = self.query_page_entries(&request, stream_id).await?;
        let filtered: Vec<HistoryItem> = rows
            .into_iter()
            .map(|row| HistoryItem {
                stream_id: row.stream_id,
                serial: row.serial,
                published_at_ms: row.published_at_ms,
                message_id: row.message_id,
                event_name: row.event_name,
                operation_kind: row.operation_kind,
                payload_size_bytes: row.payload_size_bytes as usize,
                payload_bytes: row.payload_bytes.into(),
            })
            .collect();
        let has_more = filtered.len() > request.limit;
        let items: Vec<HistoryItem> = filtered.into_iter().take(request.limit).collect();
        let next_cursor = if has_more {
            items.last().map(|item| HistoryCursor {
                version: 1,
                app_id: request.app_id.clone(),
                channel: request.channel.clone(),
                stream_id: item.stream_id.clone(),
                serial: item.serial,
                direction: request.direction,
                bounds: request.bounds.clone(),
            })
        } else {
            None
        };
        let truncated_by_retention = is_truncated_by_retention(&request.bounds, &retained);
        Ok(HistoryPage {
            items,
            next_cursor,
            retained,
            has_more,
            complete: !has_more && !truncated_by_retention,
            truncated_by_retention,
        })
    }

    async fn runtime_status(&self) -> Result<HistoryRuntimeStatus> {
        let mut degraded_channels = 0usize;
        let mut reset_required_channels = 0usize;
        let mut start_key = None;
        loop {
            let response = self
                .client
                .scan()
                .table_name(&self.tables.streams)
                .set_exclusive_start_key(start_key)
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to scan DynamoDB history runtime status: {e}"
                    ))
                })?;
            for item in response.items() {
                let stream = Self::stream_from_item(item.clone())?;
                if stream.durable_state != HistoryDurableState::Healthy {
                    degraded_channels += 1;
                }
                if stream.durable_state == HistoryDurableState::ResetRequired {
                    reset_required_channels += 1;
                }
            }
            start_key = response.last_evaluated_key;
            if start_key.is_none() {
                break;
            }
        }
        Ok(HistoryRuntimeStatus {
            enabled: true,
            backend: "dynamodb".to_string(),
            state_authority: "durable_store".to_string(),
            degraded_channels,
            reset_required_channels,
            queue_depth: self.queue_depth_total.load(Ordering::Relaxed),
        })
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamRuntimeState> {
        self.resolved_stream_runtime_state(app_id, channel).await
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamInspection> {
        self.resolved_stream_inspection(app_id, channel).await
    }

    async fn reset_stream(
        &self,
        app_id: &str,
        channel: &str,
        reason: &str,
        requested_by: Option<&str>,
    ) -> Result<HistoryResetResult> {
        if reason.trim().is_empty() {
            return Err(Error::InvalidMessageFormat(
                "Reset reason must not be empty".to_string(),
            ));
        }
        let inspection_before = self.resolved_stream_inspection(app_id, channel).await?;
        let previous_stream_id = inspection_before.stream_id.clone();
        let mut purged_messages = 0u64;
        let mut purged_bytes = 0u64;
        if let Some(stream_id) = previous_stream_id.as_deref() {
            let entries = self
                .query_entries(app_id, channel, stream_id, HistoryDirection::OldestFirst)
                .await?;
            purged_messages = entries.len() as u64;
            purged_bytes = entries.iter().map(|row| row.payload_size_bytes).sum();
            self.delete_entries(app_id, channel, stream_id, &entries)
                .await?;
        }

        let now_ms = sockudo_core::history::now_ms();
        let new_stream = StoredStreamRecord {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: uuid::Uuid::new_v4().to_string(),
            next_serial: 1,
            retained_messages: 0,
            retained_bytes: 0,
            oldest_available_serial: None,
            newest_available_serial: None,
            oldest_available_published_at_ms: None,
            newest_available_published_at_ms: None,
            durable_state: HistoryDurableState::Healthy,
            durable_state_reason: None,
            durable_state_node_id: None,
            durable_state_changed_at_ms: Some(now_ms),
            updated_at_ms: now_ms,
        };
        self.upsert_stream_raw(app_id, channel, &new_stream).await?;
        self.degraded_channels
            .remove(&degraded_channel_key(app_id, channel));
        if let Some(cache) = self.cache_manager.as_ref() {
            let _ = cache.remove(&degraded_cache_key(app_id, channel)).await;
        }
        if let Some(metrics) = self.metrics.as_deref() {
            let _ =
                refresh_history_state_metrics(&self.client, &self.tables, metrics, app_id).await;
        }
        info!(
            app_id = %app_id,
            channel = %channel,
            previous_stream_id = ?previous_stream_id,
            new_stream_id = %new_stream.stream_id,
            purged_messages,
            purged_bytes,
            reason = %reason,
            requested_by = ?requested_by,
            "Operator reset durable history stream"
        );
        let inspection = self.resolved_stream_inspection(app_id, channel).await?;
        Ok(HistoryResetResult {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            previous_stream_id,
            new_stream_id: new_stream.stream_id,
            purged_messages,
            purged_bytes,
            inspection,
        })
    }

    async fn purge_stream(
        &self,
        app_id: &str,
        channel: &str,
        request: HistoryPurgeRequest,
    ) -> Result<HistoryPurgeResult> {
        request.validate()?;
        let inspection_before = self.resolved_stream_inspection(app_id, channel).await?;
        let mut purged_messages = 0u64;
        let mut purged_bytes = 0u64;
        if let Some(stream_id) = inspection_before.stream_id.as_deref() {
            let entries = self
                .query_entries(app_id, channel, stream_id, HistoryDirection::OldestFirst)
                .await?;
            let to_delete: Vec<StoredEntryRecord> = entries
                .iter()
                .filter(|row| match request.mode {
                    HistoryPurgeMode::All => true,
                    HistoryPurgeMode::BeforeSerial => {
                        row.serial < request.before_serial.unwrap_or_default()
                    }
                    HistoryPurgeMode::BeforeTimeMs => {
                        row.published_at_ms < request.before_time_ms.unwrap_or_default()
                    }
                })
                .cloned()
                .collect();
            let retained_rows: Vec<StoredEntryRecord> = entries
                .into_iter()
                .filter(|row| match request.mode {
                    HistoryPurgeMode::All => false,
                    HistoryPurgeMode::BeforeSerial => {
                        row.serial >= request.before_serial.unwrap_or_default()
                    }
                    HistoryPurgeMode::BeforeTimeMs => {
                        row.published_at_ms >= request.before_time_ms.unwrap_or_default()
                    }
                })
                .collect();
            purged_messages = to_delete.len() as u64;
            purged_bytes = to_delete.iter().map(|row| row.payload_size_bytes).sum();
            self.delete_entries(app_id, channel, stream_id, &to_delete)
                .await?;
            if let Some(current) = self.load_stream_raw(app_id, channel).await? {
                let retained = HistoryRetentionStats {
                    stream_id: Some(stream_id.to_string()),
                    retained_messages: retained_rows.len() as u64,
                    retained_bytes: retained_rows.iter().map(|row| row.payload_size_bytes).sum(),
                    oldest_serial: retained_rows.first().map(|row| row.serial),
                    newest_serial: retained_rows.last().map(|row| row.serial),
                    oldest_published_at_ms: retained_rows.first().map(|row| row.published_at_ms),
                    newest_published_at_ms: retained_rows.last().map(|row| row.published_at_ms),
                };
                let updated = StoredStreamRecord {
                    retained_messages: retained.retained_messages,
                    retained_bytes: retained.retained_bytes,
                    oldest_available_serial: retained.oldest_serial,
                    newest_available_serial: retained.newest_serial,
                    oldest_available_published_at_ms: retained.oldest_published_at_ms,
                    newest_available_published_at_ms: retained.newest_published_at_ms,
                    updated_at_ms: sockudo_core::history::now_ms(),
                    ..current
                };
                self.upsert_stream_raw(app_id, channel, &updated).await?;
            }
            if let Some(metrics) = self.metrics.as_ref() {
                let retained = self.retained_stats(app_id, channel).await?;
                metrics.update_history_retained(
                    app_id,
                    retained.retained_messages,
                    retained.retained_bytes,
                );
            }
        }
        if let Some(metrics) = self.metrics.as_deref() {
            let _ =
                refresh_history_state_metrics(&self.client, &self.tables, metrics, app_id).await;
        }
        info!(
            app_id = %app_id,
            channel = %channel,
            mode = %request.mode.as_str(),
            before_serial = request.before_serial,
            before_time_ms = request.before_time_ms,
            purged_messages,
            purged_bytes,
            reason = %request.reason,
            requested_by = ?request.requested_by,
            "Operator purged durable history rows"
        );
        let inspection = self.resolved_stream_inspection(app_id, channel).await?;
        Ok(HistoryPurgeResult {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            mode: request.mode,
            before_serial: request.before_serial,
            before_time_ms: request.before_time_ms,
            purged_messages,
            purged_bytes,
            inspection,
        })
    }
}

fn parse_history_durable_state(raw: &str) -> HistoryDurableState {
    match raw {
        "healthy" => HistoryDurableState::Healthy,
        "reset_required" => HistoryDurableState::ResetRequired,
        "degraded" => HistoryDurableState::Degraded,
        _ => HistoryDurableState::Degraded,
    }
}

fn is_truncated_by_retention(
    bounds: &HistoryQueryBounds,
    retained: &HistoryRetentionStats,
) -> bool {
    if let (Some(start_serial), Some(oldest_serial)) = (bounds.start_serial, retained.oldest_serial)
        && start_serial < oldest_serial
    {
        return true;
    }
    if let (Some(start_time_ms), Some(oldest_time_ms)) =
        (bounds.start_time_ms, retained.oldest_published_at_ms)
        && start_time_ms < oldest_time_ms
    {
        return true;
    }
    bounds.start_serial.is_none()
        && bounds.start_time_ms.is_none()
        && retained
            .oldest_serial
            .is_some_and(|oldest_serial| oldest_serial > 1)
}

fn deterministic_key<I, S>(parts: I) -> String
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let mut key = String::new();
    for part in parts {
        let part = part.as_ref();
        key.push_str(&part.len().to_string());
        key.push(':');
        key.push_str(part);
        key.push('|');
    }
    key
}

fn retained_from_stream_record(stream: &StoredStreamRecord) -> HistoryRetentionStats {
    HistoryRetentionStats {
        stream_id: Some(stream.stream_id.clone()),
        retained_messages: stream.retained_messages,
        retained_bytes: stream.retained_bytes,
        oldest_serial: stream.oldest_available_serial,
        newest_serial: stream.newest_available_serial,
        oldest_published_at_ms: stream.oldest_available_published_at_ms,
        newest_published_at_ms: stream.newest_available_published_at_ms,
    }
}

fn degraded_channel_key(app_id: &str, channel: &str) -> String {
    format!("{app_id}\0{channel}")
}

fn degraded_cache_key(app_id: &str, channel: &str) -> String {
    format!("history:degraded:{app_id}:{channel}")
}

fn resolve_runtime_state(
    durable_state: HistoryStreamRuntimeState,
    local_hint: Option<HistoryDegradedState>,
    cache_hint: Option<HistoryDegradedState>,
) -> HistoryStreamRuntimeState {
    let newest_hint = [local_hint, cache_hint]
        .into_iter()
        .flatten()
        .max_by_key(|hint| hint.last_transition_at_ms);
    let durable_transition_at = durable_state.last_transition_at_ms.unwrap_or_default();

    if let Some(hint) = newest_hint
        && hint.last_transition_at_ms > durable_transition_at
    {
        return HistoryStreamRuntimeState {
            app_id: durable_state.app_id,
            channel: durable_state.channel,
            stream_id: durable_state.stream_id,
            durable_state: hint.durable_state,
            recovery_allowed: hint.durable_state.recovery_allowed(),
            reset_required: hint.durable_state.reset_required(),
            reason: Some(hint.reason),
            node_id: hint.node_id,
            last_transition_at_ms: Some(hint.last_transition_at_ms),
            authoritative_source: durable_state.authoritative_source,
            observed_source: hint.observed_source.to_string(),
        };
    }

    durable_state
}

struct DegradeRequest<'a> {
    app_id: &'a str,
    channel: &'a str,
    reason: &'a str,
    node_id: Option<String>,
}

async fn mark_channel_degraded(
    client: &Client,
    tables: &HistoryTables,
    degraded_channels: &DashMap<String, HistoryDegradedState>,
    cache_manager: Option<&Arc<dyn CacheManager + Send + Sync>>,
    metrics: Option<&(dyn MetricsInterface + Send + Sync)>,
    request: DegradeRequest<'_>,
) {
    let now_ms = sockudo_core::history::now_ms();
    let state = HistoryDegradedState {
        app_id: request.app_id.to_string(),
        channel: request.channel.to_string(),
        durable_state: HistoryDurableState::Degraded,
        reason: request.reason.to_string(),
        node_id: request.node_id,
        last_transition_at_ms: now_ms,
        observed_source: "local_memory_hint",
    };
    degraded_channels.insert(
        degraded_channel_key(request.app_id, request.channel),
        state.clone(),
    );
    if let Some(cache) = cache_manager {
        let _ = cache
            .set(
                &degraded_cache_key(request.app_id, request.channel),
                &sonic_rs::to_string(&sonic_rs::json!({
                    "app_id": state.app_id,
                    "channel": state.channel,
                    "durable_state": state.durable_state.as_str(),
                    "reason": state.reason,
                    "node_id": state.node_id,
                    "last_transition_at_ms": state.last_transition_at_ms,
                }))
                .unwrap_or_else(|_| "{}".to_string()),
                3600,
            )
            .await;
    }

    let response = client
        .get_item()
        .table_name(&tables.streams)
        .key(
            "stream_key",
            AttributeValue::S(DynamoDbHistoryStore::stream_key(
                request.app_id,
                request.channel,
            )),
        )
        .send()
        .await;
    match response {
        Ok(output) => {
            if let Some(item) = output.item
                && let Ok(stream) = DynamoDbHistoryStore::stream_from_item(item)
            {
                let updated = StoredStreamRecord {
                    durable_state: state.durable_state,
                    durable_state_reason: Some(state.reason.clone()),
                    durable_state_node_id: state.node_id.clone(),
                    durable_state_changed_at_ms: Some(state.last_transition_at_ms),
                    updated_at_ms: now_ms,
                    ..stream
                };
                let put_result = client
                    .put_item()
                    .table_name(&tables.streams)
                    .set_item(Some(DynamoDbHistoryStore::stream_item(
                        &DynamoDbHistoryStore::stream_key(request.app_id, request.channel),
                        &updated,
                    )))
                    .send()
                    .await;
                if let Err(err) = put_result {
                    error!(app_id = %request.app_id, channel = %request.channel, "Failed to persist DynamoDB history degraded state: {err}");
                }
            }
        }
        Err(err) => {
            error!(app_id = %request.app_id, channel = %request.channel, "Failed to load DynamoDB history stream before degrade: {err}");
        }
    }
    if let Some(metrics) = metrics {
        let _ = refresh_history_state_metrics(client, tables, metrics, request.app_id).await;
    }
}

async fn get_cached_channel_degraded(
    cache_manager: Option<&Arc<dyn CacheManager + Send + Sync>>,
    app_id: &str,
    channel: &str,
) -> Result<Option<HistoryDegradedState>> {
    if let Some(cache) = cache_manager
        && let Some(raw) = cache.get(&degraded_cache_key(app_id, channel)).await?
    {
        let value: sonic_rs::Value = sonic_rs::from_str(&raw).map_err(|e| {
            Error::Internal(format!(
                "Failed to parse degraded DynamoDB history state: {e}"
            ))
        })?;
        return Ok(Some(HistoryDegradedState {
            app_id: value
                .get("app_id")
                .and_then(sonic_rs::Value::as_str)
                .unwrap_or(app_id)
                .to_string(),
            channel: value
                .get("channel")
                .and_then(sonic_rs::Value::as_str)
                .unwrap_or(channel)
                .to_string(),
            durable_state: value
                .get("durable_state")
                .and_then(sonic_rs::Value::as_str)
                .map(parse_history_durable_state)
                .unwrap_or(HistoryDurableState::Degraded),
            reason: value
                .get("reason")
                .and_then(sonic_rs::Value::as_str)
                .unwrap_or("history_stream_degraded")
                .to_string(),
            node_id: value
                .get("node_id")
                .and_then(sonic_rs::Value::as_str)
                .map(str::to_string),
            last_transition_at_ms: value
                .get("last_transition_at_ms")
                .and_then(sonic_rs::Value::as_i64)
                .unwrap_or_default(),
            observed_source: "shared_cache_hint",
        }));
    }
    Ok(None)
}

async fn refresh_history_state_metrics(
    client: &Client,
    tables: &HistoryTables,
    metrics: &(dyn MetricsInterface + Send + Sync),
    app_id: &str,
) -> Result<()> {
    let mut degraded = 0usize;
    let mut reset_required = 0usize;
    let mut start_key = None;
    loop {
        let response = client
            .scan()
            .table_name(&tables.streams)
            .filter_expression("app_id = :app_id")
            .expression_attribute_values(":app_id", AttributeValue::S(app_id.to_string()))
            .set_exclusive_start_key(start_key)
            .send()
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to scan DynamoDB history metrics: {e}"))
            })?;
        for item in response.items() {
            let stream = DynamoDbHistoryStore::stream_from_item(item.clone())?;
            if stream.durable_state != HistoryDurableState::Healthy {
                degraded += 1;
            }
            if stream.durable_state == HistoryDurableState::ResetRequired {
                reset_required += 1;
            }
        }
        start_key = response.last_evaluated_key;
        if start_key.is_none() {
            break;
        }
    }
    metrics.update_history_degraded_channels(app_id, degraded);
    metrics.update_history_reset_required_channels(app_id, reset_required);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::history_conformance::HistoryStoreConformance;

    async fn is_dynamodb_available() -> bool {
        let settings = DynamoDbSettings {
            region: "us-east-1".to_string(),
            table_name: "sockudo_history_test_probe".to_string(),
            endpoint_url: Some("http://127.0.0.1:18000".to_string()),
            aws_access_key_id: Some("dummy".to_string()),
            aws_secret_access_key: Some("dummy".to_string()),
            aws_profile_name: None,
        };
        let mut aws_config_builder = aws_config::from_env()
            .region(aws_sdk_dynamodb::config::Region::new(
                settings.region.clone(),
            ))
            .endpoint_url(settings.endpoint_url.clone().unwrap());
        let credentials_provider =
            aws_sdk_dynamodb::config::Credentials::new("dummy", "dummy", None, None, "static");
        aws_config_builder = aws_config_builder.credentials_provider(credentials_provider);
        let client = Client::new(&aws_config_builder.load().await);
        client.list_tables().send().await.is_ok()
    }

    async fn build_store() -> Arc<dyn HistoryStore + Send + Sync> {
        let settings = DynamoDbSettings {
            region: "us-east-1".to_string(),
            table_name: format!("sockudo_history_{}", uuid::Uuid::new_v4().simple()),
            endpoint_url: Some("http://127.0.0.1:18000".to_string()),
            aws_access_key_id: Some("dummy".to_string()),
            aws_secret_access_key: Some("dummy".to_string()),
            aws_profile_name: None,
        };
        let config = HistoryConfig {
            enabled: true,
            backend: sockudo_core::options::HistoryBackend::DynamoDb,
            ..HistoryConfig::default()
        };

        create_dynamodb_history_store(&settings, config, None, None)
            .await
            .unwrap()
    }

    async fn seed_time_series(
        store: &Arc<dyn HistoryStore + Send + Sync>,
    ) -> sockudo_core::history::HistoryWriteReservation {
        let reservation = store.reserve_publish_position("app", "chat").await.unwrap();
        for offset in 0..5u64 {
            store
                .append(sockudo_core::history::HistoryAppendRecord {
                    app_id: "app".to_string(),
                    channel: "chat".to_string(),
                    stream_id: reservation.stream_id.clone(),
                    serial: reservation.serial + offset,
                    published_at_ms: 1_000 + offset as i64,
                    message_id: Some(format!("msg-{}", reservation.serial + offset)),
                    event_name: Some("event".to_string()),
                    operation_kind: "append".to_string(),
                    payload_bytes: tokio_util::bytes::Bytes::from(format!(
                        "payload-{}",
                        reservation.serial + offset
                    )),
                    retention: sockudo_core::history::HistoryRetentionPolicy {
                        retention_window_seconds: 3600,
                        max_messages_per_channel: None,
                        max_bytes_per_channel: None,
                    },
                })
                .await
                .unwrap();
        }
        reservation
    }

    #[tokio::test]
    async fn dynamodb_history_store_conformance_serial_and_stream_continuity() {
        if !is_dynamodb_available().await {
            eprintln!("Skipping test: DynamoDB not available");
            return;
        }
        let store = build_store().await;
        HistoryStoreConformance::assert_serial_monotonicity(store.clone())
            .await
            .unwrap();
        HistoryStoreConformance::assert_stream_id_continuity(store)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn dynamodb_history_store_conformance_pagination_and_reset_semantics() {
        if !is_dynamodb_available().await {
            eprintln!("Skipping test: DynamoDB not available");
            return;
        }
        let store = build_store().await;
        HistoryStoreConformance::assert_cursor_pagination(store.clone())
            .await
            .unwrap();
        HistoryStoreConformance::assert_purge_and_reset_semantics(store)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn dynamodb_history_store_time_bounded_pagination_uses_time_ordering() {
        if !is_dynamodb_available().await {
            eprintln!("Skipping test: DynamoDB not available");
            return;
        }
        let store = build_store().await;
        let reservation = seed_time_series(&store).await;

        let first = store
            .read_page(sockudo_core::history::HistoryReadRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                direction: sockudo_core::history::HistoryDirection::OldestFirst,
                limit: 2,
                cursor: None,
                bounds: sockudo_core::history::HistoryQueryBounds {
                    start_serial: None,
                    end_serial: None,
                    start_time_ms: Some(1_001),
                    end_time_ms: Some(1_004),
                },
            })
            .await
            .unwrap();
        assert_eq!(
            first
                .items
                .iter()
                .map(|item| item.serial)
                .collect::<Vec<_>>(),
            vec![reservation.serial + 1, reservation.serial + 2]
        );
        assert!(first.has_more);

        let second = store
            .read_page(sockudo_core::history::HistoryReadRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                direction: sockudo_core::history::HistoryDirection::OldestFirst,
                limit: 2,
                cursor: first.next_cursor,
                bounds: sockudo_core::history::HistoryQueryBounds {
                    start_serial: None,
                    end_serial: None,
                    start_time_ms: Some(1_001),
                    end_time_ms: Some(1_004),
                },
            })
            .await
            .unwrap();
        assert_eq!(
            second
                .items
                .iter()
                .map(|item| item.serial)
                .collect::<Vec<_>>(),
            vec![reservation.serial + 3, reservation.serial + 4]
        );
        assert!(!second.has_more);
    }
}
