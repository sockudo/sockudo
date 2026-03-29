use ahash::AHashMap;
use async_trait::async_trait;
use sockudo_core::app::{
    App, AppChannelsPolicy, AppConnectionRecoveryConfig, AppFeaturesPolicy, AppIdempotencyConfig,
    AppLimitsPolicy, AppManager, AppPolicy,
};
use sockudo_core::delta_types::ChannelDeltaConfig;
use sockudo_core::error::{Error, Result};
use sockudo_core::webhook_types::Webhook;
use std::collections::HashMap;

/// Configuration for DynamoDB App Manager
#[derive(Debug, Clone)]
pub struct DynamoDbConfig {
    pub region: String,
    pub table_name: String,
    pub endpoint: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub profile_name: Option<String>,
}

impl Default for DynamoDbConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            table_name: "sockudo-applications".to_string(),
            endpoint: None,
            access_key: None,
            secret_key: None,
            profile_name: None,
        }
    }
}

type DynamoClient = aws_sdk_dynamodb::Client;

pub struct DynamoDbAppManager {
    config: DynamoDbConfig,
    client: DynamoClient,
}

impl DynamoDbAppManager {
    pub async fn new(config: DynamoDbConfig) -> Result<Self> {
        let mut aws_config_builder = aws_config::from_env();

        aws_config_builder =
            aws_config_builder.region(aws_sdk_dynamodb::config::Region::new(config.region.clone()));

        if let Some(endpoint) = &config.endpoint {
            aws_config_builder = aws_config_builder.endpoint_url(endpoint);
        }

        if let (Some(access_key), Some(secret_key)) = (&config.access_key, &config.secret_key) {
            let credentials_provider = aws_sdk_dynamodb::config::Credentials::new(
                access_key, secret_key, None, None, "static",
            );
            aws_config_builder = aws_config_builder.credentials_provider(credentials_provider);
        }

        if let Some(profile) = &config.profile_name {
            aws_config_builder = aws_config_builder.profile_name(profile);
        }

        let aws_config = aws_config_builder.load().await;

        let client = aws_sdk_dynamodb::Client::new(&aws_config);

        let manager = Self { config, client };

        Ok(manager)
    }

    /// Convert a DynamoDB item to an App struct
    fn item_to_app(&self, item: aws_sdk_dynamodb::types::AttributeValue) -> Result<App> {
        if let aws_sdk_dynamodb::types::AttributeValue::M(map) = item {
            let get_string = |key: &str| -> Result<String> {
                if let Some(aws_sdk_dynamodb::types::AttributeValue::S(s)) = map.get(key) {
                    Ok(s.clone())
                } else {
                    Err(Error::Internal(format!(
                        "Missing or invalid {key} attribute"
                    )))
                }
            };

            let get_bool = |key: &str, default: bool| -> bool {
                if let Some(aws_sdk_dynamodb::types::AttributeValue::Bool(b)) = map.get(key) {
                    *b
                } else {
                    default
                }
            };

            let get_u32 = |key: &str, default: Option<u32>| -> Option<u32> {
                if let Some(aws_sdk_dynamodb::types::AttributeValue::N(n)) = map.get(key) {
                    n.parse::<u32>().ok()
                } else {
                    default
                }
            };

            if let Some(aws_sdk_dynamodb::types::AttributeValue::S(json_str)) = map.get("policy") {
                let policy = sonic_rs::from_str::<AppPolicy>(json_str).map_err(|e| {
                    Error::Internal(format!("Failed to parse app policy from DynamoDB: {e}"))
                })?;
                return Ok(App::from_policy(
                    get_string("id")?,
                    get_string("key")?,
                    get_string("secret")?,
                    get_bool("enabled", true),
                    policy,
                ));
            }

            Ok(App::from_policy(
                get_string("id")?,
                get_string("key")?,
                get_string("secret")?,
                get_bool("enabled", true),
                AppPolicy {
                    limits: AppLimitsPolicy {
                        max_connections: get_u32("max_connections", Some(0)).unwrap_or(0),
                        max_backend_events_per_second: get_u32(
                            "max_backend_events_per_second",
                            None,
                        ),
                        max_client_events_per_second: get_u32(
                            "max_client_events_per_second",
                            Some(0),
                        )
                        .unwrap_or(0),
                        max_read_requests_per_second: get_u32("max_read_requests_per_second", None),
                        max_presence_members_per_channel: get_u32(
                            "max_presence_members_per_channel",
                            None,
                        ),
                        max_presence_member_size_in_kb: get_u32(
                            "max_presence_member_size_in_kb",
                            None,
                        ),
                        max_channel_name_length: get_u32("max_channel_name_length", None),
                        max_event_channels_at_once: get_u32("max_event_channels_at_once", None),
                        max_event_name_length: get_u32("max_event_name_length", None),
                        max_event_payload_in_kb: get_u32("max_event_payload_in_kb", None),
                        max_event_batch_size: get_u32("max_event_batch_size", None),
                    },
                    features: AppFeaturesPolicy {
                        enable_client_messages: get_bool("enable_client_messages", false),
                        enable_user_authentication: if let Some(
                            aws_sdk_dynamodb::types::AttributeValue::Bool(b),
                        ) = map.get("enable_user_authentication")
                        {
                            Some(*b)
                        } else {
                            None
                        },
                        enable_watchlist_events: if let Some(
                            aws_sdk_dynamodb::types::AttributeValue::Bool(b),
                        ) = map.get("enable_watchlist_events")
                        {
                            Some(*b)
                        } else {
                            None
                        },
                    },
                    channels: AppChannelsPolicy {
                        allowed_origins: if let Some(aws_sdk_dynamodb::types::AttributeValue::L(
                            list,
                        )) = map.get("allowed_origins")
                        {
                            let origins: Vec<String> = list
                                .iter()
                                .filter_map(|item| {
                                    if let aws_sdk_dynamodb::types::AttributeValue::S(s) = item {
                                        Some(s.clone())
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            if origins.is_empty() {
                                None
                            } else {
                                Some(origins)
                            }
                        } else {
                            None
                        },
                        channel_delta_compression: if let Some(
                            aws_sdk_dynamodb::types::AttributeValue::S(json_str),
                        ) = map.get("channel_delta_compression")
                        {
                            sonic_rs::from_str::<AHashMap<String, ChannelDeltaConfig>>(json_str)
                                .map_err(|e| {
                                    tracing::warn!(
                                        "Failed to parse channel_delta_compression JSON: {}",
                                        e
                                    );
                                    e
                                })
                                .ok()
                        } else {
                            None
                        },
                        channel_namespaces: None,
                    },
                    webhooks: if let Some(aws_sdk_dynamodb::types::AttributeValue::S(json_str)) =
                        map.get("webhooks")
                    {
                        sonic_rs::from_str::<Vec<Webhook>>(json_str)
                            .map_err(|e| {
                                tracing::warn!("Failed to parse webhooks JSON: {}", e);
                                e
                            })
                            .ok()
                    } else {
                        None
                    },
                    idempotency: if let Some(aws_sdk_dynamodb::types::AttributeValue::S(json_str)) =
                        map.get("idempotency")
                    {
                        sonic_rs::from_str::<AppIdempotencyConfig>(json_str)
                            .map_err(|e| {
                                tracing::warn!("Failed to parse idempotency JSON: {}", e);
                                e
                            })
                            .ok()
                    } else {
                        None
                    },
                    connection_recovery: if let Some(aws_sdk_dynamodb::types::AttributeValue::S(
                        json_str,
                    )) = map.get("connection_recovery")
                    {
                        sonic_rs::from_str::<AppConnectionRecoveryConfig>(json_str)
                            .map_err(|e| {
                                tracing::warn!("Failed to parse connection_recovery JSON: {}", e);
                                e
                            })
                            .ok()
                    } else {
                        None
                    },
                },
            ))
        } else {
            Err(Error::Internal("Invalid DynamoDB item format".to_string()))
        }
    }

    /// Convert an App struct to DynamoDB item
    fn app_to_item(&self, app: &App) -> HashMap<String, aws_sdk_dynamodb::types::AttributeValue> {
        let policy = app.policy();
        let mut item = HashMap::new();

        item.insert(
            "id".to_string(),
            aws_sdk_dynamodb::types::AttributeValue::S(app.id.clone()),
        );
        item.insert(
            "key".to_string(),
            aws_sdk_dynamodb::types::AttributeValue::S(app.key.clone()),
        );
        item.insert(
            "secret".to_string(),
            aws_sdk_dynamodb::types::AttributeValue::S(app.secret.clone()),
        );
        item.insert(
            "enabled".to_string(),
            aws_sdk_dynamodb::types::AttributeValue::Bool(app.enabled),
        );
        let policy_json = sonic_rs::to_string(&policy)
            .expect("Failed to serialize app policy to JSON. This indicates a bug.");
        item.insert(
            "policy".to_string(),
            aws_sdk_dynamodb::types::AttributeValue::S(policy_json),
        );

        item
    }

    /// Check if the DynamoDB table exists
    async fn table_exists(&self) -> Result<bool> {
        let result = self
            .client
            .describe_table()
            .table_name(&self.config.table_name)
            .send()
            .await;

        Ok(result.is_ok())
    }

    /// Create the DynamoDB table if it doesn't exist
    async fn ensure_table_exists(&self) -> Result<()> {
        if self.table_exists().await? {
            return Ok(());
        }

        self.client
            .create_table()
            .table_name(&self.config.table_name)
            .key_schema(
                aws_sdk_dynamodb::types::KeySchemaElement::builder()
                    .attribute_name("id")
                    .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                aws_sdk_dynamodb::types::AttributeDefinition::builder()
                    .attribute_name("id")
                    .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .global_secondary_indexes(
                aws_sdk_dynamodb::types::GlobalSecondaryIndex::builder()
                    .index_name("KeyIndex")
                    .key_schema(
                        aws_sdk_dynamodb::types::KeySchemaElement::builder()
                            .attribute_name("key")
                            .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                            .build()
                            .unwrap(),
                    )
                    .projection(
                        aws_sdk_dynamodb::types::Projection::builder()
                            .projection_type(aws_sdk_dynamodb::types::ProjectionType::All)
                            .build(),
                    )
                    .provisioned_throughput(
                        aws_sdk_dynamodb::types::ProvisionedThroughput::builder()
                            .read_capacity_units(5)
                            .write_capacity_units(5)
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )
            .provisioned_throughput(
                aws_sdk_dynamodb::types::ProvisionedThroughput::builder()
                    .read_capacity_units(5)
                    .write_capacity_units(5)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create DynamoDB table: {e}")))?;

        let mut retries = 0;
        while retries < 10 {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            let table_status = self
                .client
                .describe_table()
                .table_name(&self.config.table_name)
                .send()
                .await;

            if let Ok(response) = table_status
                && let Some(table) = response.table()
                && let Some(status) = table.table_status()
                && status == &aws_sdk_dynamodb::types::TableStatus::Active
            {
                return Ok(());
            }

            retries += 1;
        }

        Err(Error::Internal(
            "Timeout waiting for DynamoDB table to be created".to_string(),
        ))
    }

    /// Get an app from DynamoDB
    async fn get_app_internal(&self, app_id: &str) -> Result<Option<App>> {
        let response = self
            .client
            .get_item()
            .table_name(&self.config.table_name)
            .key(
                "id",
                aws_sdk_dynamodb::types::AttributeValue::S(app_id.to_string()),
            )
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to get item from DynamoDB: {e}")))?;

        if let Some(item) = response.item() {
            let app = self.item_to_app(aws_sdk_dynamodb::types::AttributeValue::M(item.clone()))?;
            Ok(Some(app))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl AppManager for DynamoDbAppManager {
    async fn init(&self) -> Result<()> {
        self.ensure_table_exists().await
    }

    async fn create_app(&self, config: App) -> Result<()> {
        let item = self.app_to_item(&config);

        self.client
            .put_item()
            .table_name(&self.config.table_name)
            .set_item(Some(item))
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to insert app into DynamoDB: {e}")))?;

        Ok(())
    }

    async fn update_app(&self, config: App) -> Result<()> {
        let item = self.app_to_item(&config);

        self.client
            .put_item()
            .table_name(&self.config.table_name)
            .set_item(Some(item))
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to update app in DynamoDB: {e}")))?;
        Ok(())
    }

    async fn delete_app(&self, app_id: &str) -> Result<()> {
        self.client
            .delete_item()
            .table_name(&self.config.table_name)
            .key(
                "id",
                aws_sdk_dynamodb::types::AttributeValue::S(app_id.to_string()),
            )
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to delete app from DynamoDB: {e}")))?;
        Ok(())
    }

    async fn get_apps(&self) -> Result<Vec<App>> {
        let response = self
            .client
            .scan()
            .table_name(&self.config.table_name)
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to scan DynamoDB: {e}")))?;

        let mut apps = Vec::new();
        let items = response.items();
        if !items.is_empty() {
            for item in items {
                let app =
                    self.item_to_app(aws_sdk_dynamodb::types::AttributeValue::M(item.clone()))?;
                apps.push(app);
            }
        }

        Ok(apps)
    }

    async fn find_by_key(&self, key: &str) -> Result<Option<App>> {
        let response = self
            .client
            .query()
            .table_name(&self.config.table_name)
            .index_name("KeyIndex")
            .key_condition_expression("#app_key = :key_val")
            .expression_attribute_names("#app_key", "key")
            .expression_attribute_values(
                ":key_val",
                aws_sdk_dynamodb::types::AttributeValue::S(key.to_string()),
            )
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to query DynamoDB: {e}")))?;

        let items = response.items();
        if !items.is_empty()
            && let Some(item) = items.first()
        {
            let app = self.item_to_app(aws_sdk_dynamodb::types::AttributeValue::M(item.clone()))?;
            return Ok(Some(app));
        }

        Ok(None)
    }

    async fn find_by_id(&self, app_id: &str) -> Result<Option<App>> {
        self.get_app_internal(app_id).await
    }

    async fn check_health(&self) -> Result<()> {
        self.client
            .list_tables()
            .send()
            .await
            .map_err(|e| Error::Internal(format!("App manager DynamoDB connection failed: {e}")))?;
        Ok(())
    }
}
