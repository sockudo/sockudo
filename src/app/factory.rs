// src/app/factory.rs
use crate::app::dynamodb_app_manager::{DynamoDbAppManager, DynamoDbConfig};
use crate::app::manager::AppManager;
use crate::app::memory_app_manager::MemoryAppManager;
use crate::app::mysql_app_manager::MySQLAppManager;
use crate::error::Result;
use crate::log::Log;
use crate::options::{AppManagerConfig, AppManagerDriver, DatabaseConfig}; // Import AppManagerDriver
use std::sync::Arc;

pub struct AppManagerFactory;

impl AppManagerFactory {
    pub async fn create(
        config: &AppManagerConfig,
        db_config: &DatabaseConfig,
    ) -> Result<Arc<dyn AppManager + Send + Sync>> {
        Log::info(format!(
            "Initializing AppManager with driver: {:?}",
            config.driver
        ));
        match config.driver {
            // Match on the enum
            AppManagerDriver::Mysql => {
                let mysql_db_config = db_config.mysql.clone();
                match MySQLAppManager::new(mysql_db_config).await {
                    Ok(manager) => Ok(Arc::new(manager)),
                    Err(e) => {
                        Log::warning(format!("Failed to initialize MySQL app manager: {}, falling back to memory manager", e));
                        Ok(Arc::new(MemoryAppManager::new()))
                    }
                }
            }
            AppManagerDriver::Dynamodb => {
                let dynamo_settings = &db_config.dynamodb; // Use the new dedicated settings

                let dynamo_app_config = DynamoDbConfig {
                    // This is from app::dynamodb_app_manager
                    region: dynamo_settings.region.clone(),
                    table_name: dynamo_settings.table_name.clone(),
                    endpoint: dynamo_settings.endpoint_url.clone(),
                    access_key: dynamo_settings.aws_access_key_id.clone(),
                    secret_key: dynamo_settings.aws_secret_access_key.clone(),
                    cache_ttl: config.cache.ttl, // This 'config' is AppManagerConfig
                    profile_name: dynamo_settings.aws_profile_name.clone(),
                };
                match DynamoDbAppManager::new(dynamo_app_config).await {
                    Ok(manager) => Ok(Arc::new(manager)),
                    Err(e) => {
                        Log::warning(format!("Failed to initialize DynamoDB app manager: {}, falling back to memory manager", e));
                        Ok(Arc::new(MemoryAppManager::new()))
                    }
                }
            }
            AppManagerDriver::Memory | _ => {
                // Handle unknown as Memory or make it an error
                Log::info("Using memory app manager.".to_string());
                Ok(Arc::new(MemoryAppManager::new()))
            }
        }
    }
}
