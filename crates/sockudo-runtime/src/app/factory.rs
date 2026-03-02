use crate::app::cached_app_manager::CachedAppManager;
use crate::app::manager::AppManager;
use crate::app::memory_app_manager::MemoryAppManager;
use crate::cache::manager::CacheManager;
use crate::error::Result;
use crate::options::{AppManagerConfig, DatabaseConfig, DatabasePooling};
use sockudo_config::drivers::AppManagerDriver;
#[cfg(any(
    feature = "mysql",
    feature = "postgres",
    feature = "dynamodb",
    feature = "scylladb"
))]
use sockudo_core::app_store::AppStore;
#[cfg(any(feature = "mysql", feature = "postgres", feature = "dynamodb", feature = "scylladb"))]
use crate::error::Error;
#[cfg(any(feature = "mysql", feature = "postgres", feature = "dynamodb", feature = "scylladb"))]
use async_trait::async_trait;
#[cfg(any(feature = "mysql", feature = "postgres", feature = "dynamodb", feature = "scylladb"))]
use sockudo_types::app::App;
#[cfg(any(feature = "mysql", feature = "postgres", feature = "dynamodb", feature = "scylladb"))]
use std::fmt::Display;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

#[cfg(any(
    feature = "mysql",
    feature = "postgres",
    feature = "dynamodb",
    feature = "scylladb"
))]
struct AppStoreManagerAdapter<T: AppStore + Send + Sync + 'static> {
    inner: T,
}

#[cfg(any(
    feature = "mysql",
    feature = "postgres",
    feature = "dynamodb",
    feature = "scylladb"
))]
impl<T: AppStore + Send + Sync + 'static> AppStoreManagerAdapter<T> {
    fn new(inner: T) -> Self {
        Self { inner }
    }
}

#[cfg(any(
    feature = "mysql",
    feature = "postgres",
    feature = "dynamodb",
    feature = "scylladb"
))]
fn map_store_error<E: Display>(error: E) -> Error {
    Error::Internal(format!("App store error: {error}"))
}

#[cfg(any(
    feature = "mysql",
    feature = "postgres",
    feature = "dynamodb",
    feature = "scylladb"
))]
fn adapt_app_store<T>(store: T) -> Arc<dyn AppManager + Send + Sync>
where
    T: AppStore + Send + Sync + 'static,
    T::Error: Display + Send + Sync + 'static,
{
    Arc::new(AppStoreManagerAdapter::new(store))
}

#[cfg(any(
    feature = "mysql",
    feature = "postgres",
    feature = "dynamodb",
    feature = "scylladb"
))]
#[async_trait]
impl<T> AppManager for AppStoreManagerAdapter<T>
where
    T: AppStore + Send + Sync + 'static,
    T::Error: Display + Send + Sync + 'static,
{
    async fn init(&self) -> Result<()> {
        self.inner.init().await.map_err(map_store_error)
    }

    async fn create_app(&self, config: App) -> Result<()> {
        self.inner.create_app(config).await.map_err(map_store_error)
    }

    async fn update_app(&self, config: App) -> Result<()> {
        self.inner.update_app(config).await.map_err(map_store_error)
    }

    async fn delete_app(&self, app_id: &str) -> Result<()> {
        self.inner.delete_app(app_id).await.map_err(map_store_error)
    }

    async fn get_apps(&self) -> Result<Vec<App>> {
        self.inner.get_apps().await.map_err(map_store_error)
    }

    async fn find_by_key(&self, key: &str) -> Result<Option<App>> {
        self.inner.find_by_key(key).await.map_err(map_store_error)
    }

    async fn find_by_id(&self, app_id: &str) -> Result<Option<App>> {
        self.inner.find_by_id(app_id).await.map_err(map_store_error)
    }

    async fn check_health(&self) -> Result<()> {
        self.inner.check_health().await.map_err(map_store_error)
    }
}

pub struct AppManagerFactory;

impl AppManagerFactory {
    #[allow(unused_variables)]
    pub async fn create(
        config: &AppManagerConfig,
        db_config: &DatabaseConfig,
        pooling: &DatabasePooling,
        cache_manager: Arc<Mutex<dyn CacheManager + Send + Sync>>,
    ) -> Result<Arc<dyn AppManager + Send + Sync>> {
        info!(
            "{}",
            format!("Initializing AppManager with driver: {:?}", config.driver)
        );

        let inner: Arc<dyn AppManager + Send + Sync> = match config.driver {
            #[cfg(feature = "mysql")]
            AppManagerDriver::Mysql => {
                let mysql = sockudo_store_mysql::DatabaseConnection {
                    host: db_config.mysql.host.clone(),
                    port: db_config.mysql.port,
                    username: db_config.mysql.username.clone(),
                    password: db_config.mysql.password.clone(),
                    database: db_config.mysql.database.clone(),
                    table_name: db_config.mysql.table_name.clone(),
                    connection_pool_size: db_config.mysql.connection_pool_size,
                    pool_min: db_config.mysql.pool_min,
                    pool_max: db_config.mysql.pool_max,
                    cache_ttl: db_config.mysql.cache_ttl,
                    cache_max_capacity: db_config.mysql.cache_max_capacity,
                };
                let pool = sockudo_store_mysql::DatabasePooling {
                    enabled: pooling.enabled,
                    min: pooling.min,
                    max: pooling.max,
                };

                match sockudo_store_mysql::new_mysql_store(mysql, pool).await {
                    Ok(store) => adapt_app_store(store),
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
                let dynamo = sockudo_store_dynamodb::DynamoDbConfig {
                    region: db_config.dynamodb.region.clone(),
                    table_name: db_config.dynamodb.table_name.clone(),
                    endpoint: db_config.dynamodb.endpoint_url.clone(),
                    access_key: db_config.dynamodb.aws_access_key_id.clone(),
                    secret_key: db_config.dynamodb.aws_secret_access_key.clone(),
                    profile_name: db_config.dynamodb.aws_profile_name.clone(),
                };

                match sockudo_store_dynamodb::new_dynamodb_store(dynamo).await {
                    Ok(store) => adapt_app_store(store),
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
                let postgres = sockudo_store_postgres::DatabaseConnection {
                    host: db_config.postgres.host.clone(),
                    port: db_config.postgres.port,
                    username: db_config.postgres.username.clone(),
                    password: db_config.postgres.password.clone(),
                    database: db_config.postgres.database.clone(),
                    table_name: db_config.postgres.table_name.clone(),
                    connection_pool_size: db_config.postgres.connection_pool_size,
                    pool_min: db_config.postgres.pool_min,
                    pool_max: db_config.postgres.pool_max,
                    cache_ttl: db_config.postgres.cache_ttl,
                    cache_max_capacity: db_config.postgres.cache_max_capacity,
                };
                let pool = sockudo_store_postgres::DatabasePooling {
                    enabled: pooling.enabled,
                    min: pooling.min,
                    max: pooling.max,
                };

                match sockudo_store_postgres::new_postgres_store(postgres, pool).await {
                    Ok(store) => adapt_app_store(store),
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

            #[cfg(feature = "scylladb")]
            AppManagerDriver::ScyllaDb => {
                let scylla = sockudo_store_scylla::ScyllaDbConfig {
                    nodes: db_config.scylladb.nodes.clone(),
                    keyspace: db_config.scylladb.keyspace.clone(),
                    table_name: db_config.scylladb.table_name.clone(),
                    username: db_config.scylladb.username.clone(),
                    password: db_config.scylladb.password.clone(),
                    replication_class: db_config.scylladb.replication_class.clone(),
                    replication_factor: db_config.scylladb.replication_factor,
                };

                match sockudo_store_scylla::new_scylla_store(scylla).await {
                    Ok(store) => adapt_app_store(store),
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
