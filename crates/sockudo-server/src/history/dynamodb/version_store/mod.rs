// ── DynamoDB VersionStore ─────────────────────────────────────────────────────

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement,
    KeyType, Projection, ProjectionType, Put, ScalarAttributeType, TableStatus, TransactWriteItem,
    Update,
};
use sockudo_core::error::{Error, Result};
use sockudo_core::options::DynamoDbSettings;
use std::collections::HashMap;

use super::HistoryTables;

#[cfg(feature = "versioned-messages")]
use sockudo_core::version_store::{
    StoredVersionRecord, VersionCreateRejection, VersionCreateRequest, VersionCreateResult,
    VersionMutationRejection, VersionMutationRequest, VersionMutationResult, VersionReplayRequest,
    VersionStore, VersionStoreCursor, VersionStoreDirection, VersionStorePage,
    VersionStoreReadRequest, VersionStreamState, VersionWriteReservation,
    VersionWriteReservationBlock,
};

#[cfg(feature = "versioned-messages")]
pub struct DynamoDbVersionStore {
    client: Client,
    tables: HistoryTables,
    retention_seconds: u64,
}

#[cfg(feature = "versioned-messages")]
pub async fn create_dynamodb_version_store(
    db_config: &DynamoDbSettings,
    table_prefix: &str,
    retention_seconds: u64,
) -> Result<std::sync::Arc<dyn VersionStore + Send + Sync>> {
    let store = DynamoDbVersionStore::new(db_config, table_prefix, retention_seconds).await?;
    Ok(std::sync::Arc::new(store))
}

#[cfg(feature = "versioned-messages")]
impl DynamoDbVersionStore {
    /// DynamoDB TTL attribute name. The table-level TTL configuration points
    /// here so the service asynchronously deletes expired items.
    const EXPIRES_AT_ATTR: &'static str = "expires_at";

    /// Compute the Unix-epoch seconds value to store in the `expires_at`
    /// attribute, or `None` when retention is disabled.
    fn expires_at_value(&self) -> Option<AttributeValue> {
        if self.retention_seconds == 0 {
            return None;
        }
        let now_secs = (sockudo_core::history::now_ms() / 1000) as u64;
        let expiry = now_secs.saturating_add(self.retention_seconds);
        Some(Self::attr_n(expiry))
    }

    async fn new(
        db_config: &DynamoDbSettings,
        table_prefix: &str,
        retention_seconds: u64,
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
            retention_seconds,
            tables: HistoryTables {
                streams: format!("{}_streams", table_prefix),
                entries: format!("{}_entries", table_prefix),
                entries_time_index: format!("{}_time_idx", table_prefix),
                version_streams: format!("{}_version_streams", table_prefix),
                version_messages: format!("{}_version_messages", table_prefix),
                version_messages_history_index: format!(
                    "{}_version_messages_history_idx",
                    table_prefix
                ),
                version_entries: format!("{}_version_entries", table_prefix),
                version_entries_delivery_index: format!(
                    "{}_version_entries_delivery_idx",
                    table_prefix
                ),
                version_entries_history_index: format!(
                    "{}_version_entries_history_idx",
                    table_prefix
                ),
                version_entries_message_index: format!(
                    "{}_version_entries_message_idx",
                    table_prefix
                ),
            },
        };
        store.ensure_version_tables().await?;
        Ok(store)
    }

    async fn ensure_version_tables(&self) -> Result<()> {
        // version_streams table
        if self
            .client
            .describe_table()
            .table_name(&self.tables.version_streams)
            .send()
            .await
            .is_err()
        {
            self.client
                .create_table()
                .table_name(&self.tables.version_streams)
                .billing_mode(BillingMode::PayPerRequest)
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("app_channel")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .map_err(|e| {
                            Error::Internal(format!("Failed to build DynamoDB attr def: {e}"))
                        })?,
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("app_channel")
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
                        "Failed to create DynamoDB version_streams table: {e}"
                    ))
                })?;
            self.wait_for_table(&self.tables.version_streams).await?;
        }

        // version_messages table
        if self
            .client
            .describe_table()
            .table_name(&self.tables.version_messages)
            .send()
            .await
            .is_err()
        {
            self.client
                .create_table()
                .table_name(&self.tables.version_messages)
                .billing_mode(BillingMode::PayPerRequest)
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("app_channel")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .map_err(|e| Error::Internal(format!("Failed to build attr def: {e}")))?,
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("message_serial")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .map_err(|e| Error::Internal(format!("Failed to build attr def: {e}")))?,
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("app_channel")
                        .key_type(KeyType::Hash)
                        .build()
                        .map_err(|e| Error::Internal(format!("Failed to build key schema: {e}")))?,
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("message_serial")
                        .key_type(KeyType::Range)
                        .build()
                        .map_err(|e| Error::Internal(format!("Failed to build key schema: {e}")))?,
                )
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to create DynamoDB version_messages table: {e}"
                    ))
                })?;
            self.wait_for_table(&self.tables.version_messages).await?;
        }

        // version_entries table with delivery and message GSIs
        if self
            .client
            .describe_table()
            .table_name(&self.tables.version_entries)
            .send()
            .await
            .is_err()
        {
            self.client
                .create_table()
                .table_name(&self.tables.version_entries)
                .billing_mode(BillingMode::PayPerRequest)
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("app_channel")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .map_err(|e| Error::Internal(format!("Failed to build attr def: {e}")))?,
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("message_version_key")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .map_err(|e| Error::Internal(format!("Failed to build attr def: {e}")))?,
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("delivery_serial")
                        .attribute_type(ScalarAttributeType::N)
                        .build()
                        .map_err(|e| Error::Internal(format!("Failed to build attr def: {e}")))?,
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("app_channel_message")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .map_err(|e| Error::Internal(format!("Failed to build attr def: {e}")))?,
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("version_serial")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .map_err(|e| Error::Internal(format!("Failed to build attr def: {e}")))?,
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("app_channel")
                        .key_type(KeyType::Hash)
                        .build()
                        .map_err(|e| Error::Internal(format!("Failed to build key schema: {e}")))?,
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("message_version_key")
                        .key_type(KeyType::Range)
                        .build()
                        .map_err(|e| Error::Internal(format!("Failed to build key schema: {e}")))?,
                )
                .global_secondary_indexes(
                    GlobalSecondaryIndex::builder()
                        .index_name(&self.tables.version_entries_delivery_index)
                        .key_schema(
                            KeySchemaElement::builder()
                                .attribute_name("app_channel")
                                .key_type(KeyType::Hash)
                                .build()
                                .map_err(|e| {
                                    Error::Internal(format!("Failed to build GSI key: {e}"))
                                })?,
                        )
                        .key_schema(
                            KeySchemaElement::builder()
                                .attribute_name("delivery_serial")
                                .key_type(KeyType::Range)
                                .build()
                                .map_err(|e| {
                                    Error::Internal(format!("Failed to build GSI key: {e}"))
                                })?,
                        )
                        .projection(
                            Projection::builder()
                                .projection_type(ProjectionType::All)
                                .build(),
                        )
                        .build()
                        .map_err(|e| Error::Internal(format!("Failed to build GSI: {e}")))?,
                )
                .global_secondary_indexes(
                    GlobalSecondaryIndex::builder()
                        .index_name(&self.tables.version_entries_message_index)
                        .key_schema(
                            KeySchemaElement::builder()
                                .attribute_name("app_channel_message")
                                .key_type(KeyType::Hash)
                                .build()
                                .map_err(|e| {
                                    Error::Internal(format!("Failed to build GSI key: {e}"))
                                })?,
                        )
                        .key_schema(
                            KeySchemaElement::builder()
                                .attribute_name("version_serial")
                                .key_type(KeyType::Range)
                                .build()
                                .map_err(|e| {
                                    Error::Internal(format!("Failed to build GSI key: {e}"))
                                })?,
                        )
                        .projection(
                            Projection::builder()
                                .projection_type(ProjectionType::All)
                                .build(),
                        )
                        .build()
                        .map_err(|e| Error::Internal(format!("Failed to build GSI: {e}")))?,
                )
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to create DynamoDB version_entries table: {e}"
                    ))
                })?;
            self.wait_for_table(&self.tables.version_entries).await?;
        }

        // Enable native TTL on the two tables that carry `expires_at`. When
        // retention is disabled we skip — items are written without the
        // attribute so they never expire.
        if self.retention_seconds > 0 {
            self.ensure_ttl_enabled(&self.tables.version_entries)
                .await?;
            self.ensure_ttl_enabled(&self.tables.version_messages)
                .await?;
        }

        Ok(())
    }

    /// Enable DynamoDB native TTL on `table_name` using `EXPIRES_AT_ATTR`.
    /// Idempotent: tolerates tables where TTL is already enabled with the
    /// same attribute. If TTL is enabled with a different attribute name we
    /// surface a configuration error so the operator can resolve it.
    async fn ensure_ttl_enabled(&self, table_name: &str) -> Result<()> {
        use aws_sdk_dynamodb::types::{TimeToLiveSpecification, TimeToLiveStatus};

        let current = self
            .client
            .describe_time_to_live()
            .table_name(table_name)
            .send()
            .await;
        if let Ok(resp) = current
            && let Some(desc) = resp.time_to_live_description()
        {
            let status = desc.time_to_live_status();
            let attr = desc.attribute_name();
            match (status, attr) {
                (Some(TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling), Some(name))
                    if name == Self::EXPIRES_AT_ATTR =>
                {
                    return Ok(());
                }
                (Some(TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling), Some(name)) => {
                    return Err(Error::Configuration(format!(
                        "DynamoDB table {table_name} has TTL enabled on attribute '{name}', expected '{}'. Disable TTL or choose a different table prefix.",
                        Self::EXPIRES_AT_ATTR
                    )));
                }
                _ => {}
            }
        }

        let spec = TimeToLiveSpecification::builder()
            .enabled(true)
            .attribute_name(Self::EXPIRES_AT_ATTR)
            .build()
            .map_err(|e| Error::Internal(format!("Failed to build TTL spec: {e}")))?;

        if let Err(e) = self
            .client
            .update_time_to_live()
            .table_name(table_name)
            .time_to_live_specification(spec)
            .send()
            .await
        {
            // Some DynamoDB-compatible environments (DynamoDB Local, certain
            // emulators) do not support the TTL API. Log and continue rather
            // than fail startup — the periodic purge worker is still a no-op
            // for native-TTL backends, so users on emulators just won't get
            // expiry until they migrate to real DynamoDB.
            tracing::warn!(
                table = %table_name,
                error = %e,
                "unable to enable dynamodb ttl on version store table; entries will not be auto-expired"
            );
        }

        Ok(())
    }

    async fn wait_for_table(&self, table_name: &str) -> Result<()> {
        for _ in 0..30 {
            let resp = self
                .client
                .describe_table()
                .table_name(table_name)
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to describe table {table_name}: {e}"))
                })?;
            if resp
                .table()
                .and_then(|t| t.table_status().cloned())
                .map(|s| s == TableStatus::Active)
                .unwrap_or(false)
            {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        Err(Error::Internal(format!(
            "Timed out waiting for DynamoDB table {table_name} to become active"
        )))
    }

    fn app_channel_key(app_id: &str, channel: &str) -> String {
        format!("{}#{}", app_id, channel)
    }

    fn app_channel_message_key(app_id: &str, channel: &str, message_serial: &str) -> String {
        format!("{}#{}#{}", app_id, channel, message_serial)
    }

    fn message_version_key(message_serial: &str, version_serial: &str) -> String {
        format!("{}#{}", message_serial, version_serial)
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

    fn item_str(item: &HashMap<String, AttributeValue>, key: &str) -> Option<String> {
        item.get(key)
            .and_then(|v| v.as_s().ok())
            .map(|s| s.to_string())
    }

    fn item_num(item: &HashMap<String, AttributeValue>, key: &str) -> Option<i64> {
        item.get(key)
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<i64>().ok())
    }

    fn operation_receipt_key(operation_key: &str) -> String {
        format!("__operation__#{operation_key}")
    }

    fn entry_item(
        &self,
        record: &StoredVersionRecord,
        operation: Option<&sockudo_core::message_envelope::PublishIdempotencyMetadata>,
    ) -> Result<HashMap<String, AttributeValue>> {
        let payload = sonic_rs::to_vec(record)
            .map_err(|e| Error::Internal(format!("Failed to serialize version record: {e}")))?;
        let app_channel = Self::app_channel_key(&record.app_id, &record.channel);
        let mut item = HashMap::new();
        item.insert("app_channel".to_string(), Self::attr_s(&app_channel));
        item.insert(
            "message_version_key".to_string(),
            Self::attr_s(&Self::message_version_key(
                record.message_serial().as_str(),
                record.version_serial().as_str(),
            )),
        );
        item.insert(
            "app_channel_message".to_string(),
            Self::attr_s(&Self::app_channel_message_key(
                &record.app_id,
                &record.channel,
                record.message_serial().as_str(),
            )),
        );
        item.insert("app_id".to_string(), Self::attr_s(&record.app_id));
        item.insert("channel".to_string(), Self::attr_s(&record.channel));
        item.insert(
            "message_serial".to_string(),
            Self::attr_s(record.message_serial().as_str()),
        );
        item.insert(
            "version_serial".to_string(),
            Self::attr_s(record.version_serial().as_str()),
        );
        item.insert(
            "delivery_serial".to_string(),
            Self::attr_n(record.delivery_serial()),
        );
        item.insert(
            "history_serial".to_string(),
            Self::attr_n(record.history_serial()),
        );
        item.insert(
            "action".to_string(),
            Self::attr_s(record.message.action.as_str()),
        );
        item.insert("payload_bytes".to_string(), Self::attr_b(payload));
        item.insert(
            "created_at_ms".to_string(),
            Self::attr_n(sockudo_core::history::now_ms()),
        );
        if let Some(operation) = operation {
            item.insert(
                "operation_key".to_string(),
                Self::attr_s(&operation.cache_key),
            );
            item.insert(
                "operation_fingerprint".to_string(),
                Self::attr_s(&operation.payload_fingerprint),
            );
        }
        if let Some(expires_at) = self.expires_at_value() {
            item.insert(Self::EXPIRES_AT_ATTR.to_string(), expires_at);
        }
        Ok(item)
    }
}

#[cfg(feature = "versioned-messages")]
mod store_impl;
