use crate::cached_app_manager::CachedAppManager;
#[cfg(feature = "dynamodb")]
use crate::dynamodb_app_manager::{DynamoDbAppManager, DynamoDbConfig};
use crate::memory_app_manager::MemoryAppManager;
#[cfg(feature = "mysql")]
use crate::mysql_app_manager::MySQLAppManager;
#[cfg(feature = "postgres")]
use crate::pg_app_manager::PgSQLAppManager;
#[cfg(feature = "scylladb")]
use crate::scylla_app_manager::{ScyllaDbAppManager, ScyllaDbConfig};
#[cfg(feature = "surrealdb")]
use crate::surrealdb_app_manager::{SurrealDbAppManager, SurrealDbConfig};
use sockudo_core::app::AppManager;
use sockudo_core::cache::CacheManager;
use sockudo_core::error::Result;
use sockudo_core::options::{AppManagerConfig, AppManagerDriver, DatabaseConfig, DatabasePooling};
use std::sync::Arc;
use tracing::{info, warn};

pub struct AppManagerFactory;

impl AppManagerFactory {
    #[allow(unused_variables)]
    pub async fn create(
        config: &AppManagerConfig,
        db_config: &DatabaseConfig,
        pooling: &DatabasePooling,
        cache_manager: Arc<dyn CacheManager + Send + Sync>,
    ) -> Result<Arc<dyn AppManager + Send + Sync>> {
        info!(
            "{}",
            format!("Initializing AppManager with driver: {:?}", config.driver)
        );
        let inner: Arc<dyn AppManager + Send + Sync> = match config.driver {
            #[cfg(feature = "mysql")]
            AppManagerDriver::Mysql => {
                let mysql_db_config = db_config.mysql.clone();
                match MySQLAppManager::new(mysql_db_config, pooling.clone()).await {
                    Ok(manager) => Arc::new(manager),
                    Err(e) => {
                        warn!(
                            "{}",
                            format!(
                                "Failed to initialize MySQL app manager: {}, falling back to memory manager",
                                e
                            )
                        );
                        Arc::new(MemoryAppManager::new())
                    }
                }
            }
            #[cfg(feature = "dynamodb")]
            AppManagerDriver::Dynamodb => {
                let dynamo_settings = &db_config.dynamodb;

                let dynamo_app_config = DynamoDbConfig {
                    region: dynamo_settings.region.clone(),
                    table_name: dynamo_settings.table_name.clone(),
                    endpoint: dynamo_settings.endpoint_url.clone(),
                    access_key: dynamo_settings.aws_access_key_id.clone(),
                    secret_key: dynamo_settings.aws_secret_access_key.clone(),
                    profile_name: dynamo_settings.aws_profile_name.clone(),
                };
                match DynamoDbAppManager::new(dynamo_app_config).await {
                    Ok(manager) => Arc::new(manager),
                    Err(e) => {
                        warn!(
                            "{}",
                            format!(
                                "Failed to initialize DynamoDB app manager: {}, falling back to memory manager",
                                e
                            )
                        );
                        Arc::new(MemoryAppManager::new())
                    }
                }
            }
            #[cfg(feature = "postgres")]
            AppManagerDriver::PgSql => {
                let pgsql_db_config = db_config.postgres.clone();
                match PgSQLAppManager::new(pgsql_db_config, pooling.clone()).await {
                    Ok(manager) => Arc::new(manager),
                    Err(e) => {
                        warn!(
                            "{}",
                            format!(
                                "Failed to initialize PgSQL app manager: {}, falling back to memory manager",
                                e
                            )
                        );
                        Arc::new(MemoryAppManager::new())
                    }
                }
            }
            #[cfg(feature = "surrealdb")]
            AppManagerDriver::SurrealDb => {
                let surreal_settings = &db_config.surrealdb;

                let surreal_config = SurrealDbConfig {
                    url: surreal_settings.url.clone(),
                    namespace: surreal_settings.namespace.clone(),
                    database: surreal_settings.database.clone(),
                    username: surreal_settings.username.clone(),
                    password: surreal_settings.password.clone(),
                    table_name: surreal_settings.table_name.clone(),
                    cache_ttl: surreal_settings.cache_ttl,
                    cache_max_capacity: surreal_settings.cache_max_capacity,
                };
                match SurrealDbAppManager::new(surreal_config).await {
                    Ok(manager) => Arc::new(manager),
                    Err(e) => {
                        warn!(
                            "{}",
                            format!(
                                "Failed to initialize SurrealDB app manager: {}, falling back to memory manager",
                                e
                            )
                        );
                        Arc::new(MemoryAppManager::new())
                    }
                }
            }
            #[cfg(feature = "scylladb")]
            AppManagerDriver::ScyllaDb => {
                let scylla_settings = &db_config.scylladb;

                let scylla_config = ScyllaDbConfig {
                    nodes: scylla_settings.nodes.clone(),
                    keyspace: scylla_settings.keyspace.clone(),
                    table_name: scylla_settings.table_name.clone(),
                    username: scylla_settings.username.clone(),
                    password: scylla_settings.password.clone(),
                    replication_class: scylla_settings.replication_class.clone(),
                    replication_factor: scylla_settings.replication_factor,
                };
                match ScyllaDbAppManager::new(scylla_config).await {
                    Ok(manager) => Arc::new(manager),
                    Err(e) => {
                        warn!(
                            "{}",
                            format!(
                                "Failed to initialize ScyllaDB app manager: {}, falling back to memory manager",
                                e
                            )
                        );
                        Arc::new(MemoryAppManager::new())
                    }
                }
            }
            AppManagerDriver::Memory => {
                info!("{}", "Using memory app manager.".to_string());
                Arc::new(MemoryAppManager::new())
            }
            #[cfg(not(feature = "mysql"))]
            AppManagerDriver::Mysql => {
                warn!(
                    "{}",
                    "MySQL app manager requested but not compiled in. Falling back to memory manager."
                );
                Arc::new(MemoryAppManager::new())
            }
            #[cfg(not(feature = "dynamodb"))]
            AppManagerDriver::Dynamodb => {
                warn!(
                    "{}",
                    "DynamoDB app manager requested but not compiled in. Falling back to memory manager."
                );
                Arc::new(MemoryAppManager::new())
            }
            #[cfg(not(feature = "postgres"))]
            AppManagerDriver::PgSql => {
                warn!(
                    "{}",
                    "PostgreSQL app manager requested but not compiled in. Falling back to memory manager."
                );
                Arc::new(MemoryAppManager::new())
            }
            #[cfg(not(feature = "surrealdb"))]
            AppManagerDriver::SurrealDb => {
                warn!(
                    "{}",
                    "SurrealDB app manager requested but not compiled in. Falling back to memory manager."
                );
                Arc::new(MemoryAppManager::new())
            }
            #[cfg(not(feature = "scylladb"))]
            AppManagerDriver::ScyllaDb => {
                warn!(
                    "{}",
                    "ScyllaDB app manager requested but not compiled in. Falling back to memory manager."
                );
                Arc::new(MemoryAppManager::new())
            }
        };

        if config.cache.enabled {
            Ok(Arc::new(CachedAppManager::new(
                inner,
                cache_manager,
                config.cache.clone(),
            )))
        } else {
            Ok(inner)
        }
    }
}
