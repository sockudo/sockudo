use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType, TableStatus,
};
use dashmap::DashMap;
use sockudo_core::cache::CacheManager;
use sockudo_core::error::{Error, Result};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::{DynamoDbSettings, HistoryConfig};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use super::{DynamoDbHistoryStore, HistoryTables};

impl DynamoDbHistoryStore {
    pub(super) async fn new(
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
                version_streams: format!("{}_version_streams", db_config.table_name),
                version_messages: format!("{}_version_messages", db_config.table_name),
                version_messages_history_index: format!(
                    "{}_version_messages_history_idx",
                    db_config.table_name
                ),
                version_entries: format!("{}_version_entries", db_config.table_name),
                version_entries_delivery_index: format!(
                    "{}_version_entries_delivery_idx",
                    db_config.table_name
                ),
                version_entries_history_index: format!(
                    "{}_version_entries_history_idx",
                    db_config.table_name
                ),
                version_entries_message_index: format!(
                    "{}_version_entries_message_idx",
                    db_config.table_name
                ),
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
        self.ensure_version_streams_table().await?;
        self.ensure_version_messages_table().await?;
        self.ensure_version_entries_table().await?;
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

    async fn ensure_version_streams_table(&self) -> Result<()> {
        if self
            .client
            .describe_table()
            .table_name(&self.tables.version_streams)
            .send()
            .await
            .is_ok()
        {
            return Ok(());
        }

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
                        Error::Internal(format!(
                            "Failed to build DynamoDB attribute definition: {e}"
                        ))
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
                    "Failed to create DynamoDB version streams table: {e}"
                ))
            })?;
        self.wait_for_active(&self.tables.version_streams).await
    }

    async fn ensure_version_messages_table(&self) -> Result<()> {
        if self
            .client
            .describe_table()
            .table_name(&self.tables.version_messages)
            .send()
            .await
            .is_ok()
        {
            return Ok(());
        }

        self.client
            .create_table()
            .table_name(&self.tables.version_messages)
            .billing_mode(BillingMode::PayPerRequest)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("app_channel")
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
                    .attribute_name("message_serial")
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
                    .attribute_name("history_serial")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .map_err(|e| {
                        Error::Internal(format!(
                            "Failed to build DynamoDB attribute definition: {e}"
                        ))
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
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("message_serial")
                    .key_type(KeyType::Range)
                    .build()
                    .map_err(|e| {
                        Error::Internal(format!("Failed to build DynamoDB key schema: {e}"))
                    })?,
            )
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name(&self.tables.version_messages_history_index)
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("app_channel")
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
                            .attribute_name("history_serial")
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
                        Error::Internal(format!(
                            "Failed to build DynamoDB version messages history index: {e}"
                        ))
                    })?,
            )
            .send()
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to create DynamoDB version messages table: {e}"
                ))
            })?;
        self.wait_for_active(&self.tables.version_messages).await
    }

    async fn ensure_version_entries_table(&self) -> Result<()> {
        if self
            .client
            .describe_table()
            .table_name(&self.tables.version_entries)
            .send()
            .await
            .is_ok()
        {
            return Ok(());
        }

        self.client
            .create_table()
            .table_name(&self.tables.version_entries)
            .billing_mode(BillingMode::PayPerRequest)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("app_channel")
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
                    .attribute_name("message_version_key")
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
                    .attribute_name("delivery_serial")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .map_err(|e| {
                        Error::Internal(format!(
                            "Failed to build DynamoDB attribute definition: {e}"
                        ))
                    })?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("history_serial")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .map_err(|e| {
                        Error::Internal(format!(
                            "Failed to build DynamoDB attribute definition: {e}"
                        ))
                    })?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("app_channel_message")
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
                    .attribute_name("version_serial")
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
                    .attribute_name("app_channel")
                    .key_type(KeyType::Hash)
                    .build()
                    .map_err(|e| {
                        Error::Internal(format!("Failed to build DynamoDB key schema: {e}"))
                    })?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("message_version_key")
                    .key_type(KeyType::Range)
                    .build()
                    .map_err(|e| {
                        Error::Internal(format!("Failed to build DynamoDB key schema: {e}"))
                    })?,
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
                                Error::Internal(format!(
                                    "Failed to build DynamoDB GSI key schema: {e}"
                                ))
                            })?,
                    )
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("delivery_serial")
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
                        Error::Internal(format!(
                            "Failed to build DynamoDB version entries delivery index: {e}"
                        ))
                    })?,
            )
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name(&self.tables.version_entries_history_index)
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("app_channel")
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
                            .attribute_name("history_serial")
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
                        Error::Internal(format!(
                            "Failed to build DynamoDB version entries history index: {e}"
                        ))
                    })?,
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
                                Error::Internal(format!(
                                    "Failed to build DynamoDB GSI key schema: {e}"
                                ))
                            })?,
                    )
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("version_serial")
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
                        Error::Internal(format!(
                            "Failed to build DynamoDB version entries message index: {e}"
                        ))
                    })?,
            )
            .send()
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to create DynamoDB version entries table: {e}"
                ))
            })?;
        self.wait_for_active(&self.tables.version_entries).await
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
}
