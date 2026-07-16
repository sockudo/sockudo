use sockudo_core::cache::CacheManager;
use sockudo_core::error::Result;
use sockudo_core::history::{HistoryStore, MemoryHistoryStore, MemoryHistoryStoreConfig};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::{DatabaseConfig, DatabasePooling, HistoryBackend, HistoryConfig};
#[cfg(feature = "versioned-messages")]
use sockudo_core::options::{VersionStoreDriver, VersionedMessagesConfig};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "dynamodb")]
mod dynamodb;
#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "scylladb")]
mod scylla;
#[cfg(feature = "surrealdb")]
mod surreal;

#[cfg(feature = "postgres")]
use postgres::PostgresHistoryStore;
#[cfg(all(feature = "postgres", feature = "versioned-messages"))]
use postgres::{PostgresAnnotationStore, PostgresVersionStore};

#[cfg(feature = "versioned-messages")]
pub async fn create_annotation_store(
    versioned_config: &VersionedMessagesConfig,
    history_config: &HistoryConfig,
    db_config: &DatabaseConfig,
    pooling: &DatabasePooling,
) -> Result<Arc<dyn sockudo_core::annotations::AnnotationStore + Send + Sync>> {
    use sockudo_core::annotations::MemoryAnnotationStore;

    match versioned_config.driver {
        VersionStoreDriver::Memory => Ok(Arc::new(MemoryAnnotationStore::new())),
        VersionStoreDriver::Postgres => {
            #[cfg(feature = "postgres")]
            {
                let store = PostgresAnnotationStore::new(
                    &db_config.postgres,
                    pooling,
                    &history_config.postgres.table_prefix,
                )
                .await?;
                tracing::info!("AnnotationStore initialized with driver: postgres");
                Ok(Arc::new(store))
            }
            #[cfg(not(feature = "postgres"))]
            {
                let _ = (history_config, db_config, pooling);
                Err(sockudo_core::error::Error::Configuration(
                    "Annotation store driver 'postgres' requires the 'postgres' feature"
                        .to_string(),
                ))
            }
        }
        VersionStoreDriver::Mysql => {
            #[cfg(feature = "mysql")]
            {
                let store = mysql::MysqlAnnotationStore::new(
                    &db_config.mysql,
                    pooling,
                    &history_config.mysql.table_prefix,
                )
                .await?;
                tracing::info!("AnnotationStore initialized with driver: mysql");
                Ok(Arc::new(store))
            }
            #[cfg(not(feature = "mysql"))]
            {
                let _ = (history_config, db_config, pooling);
                Err(sockudo_core::error::Error::Configuration(
                    "Annotation store driver 'mysql' requires the 'mysql' feature".to_string(),
                ))
            }
        }
        VersionStoreDriver::DynamoDb => {
            #[cfg(feature = "dynamodb")]
            {
                let store = dynamodb::create_dynamodb_annotation_store(
                    &db_config.dynamodb,
                    &history_config.dynamodb.table_prefix,
                    versioned_config.retention_window_seconds,
                )
                .await?;
                tracing::info!("AnnotationStore initialized with driver: dynamodb");
                Ok(store)
            }
            #[cfg(not(feature = "dynamodb"))]
            {
                let _ = (history_config, db_config, pooling);
                Err(sockudo_core::error::Error::Configuration(
                    "Annotation store driver 'dynamodb' requires the 'dynamodb' feature"
                        .to_string(),
                ))
            }
        }
        VersionStoreDriver::ScyllaDb => {
            #[cfg(feature = "scylladb")]
            {
                let store = scylla::create_scylla_annotation_store(
                    &db_config.scylladb,
                    &history_config.scylladb.table_prefix,
                    versioned_config.retention_window_seconds,
                )
                .await?;
                tracing::info!("AnnotationStore initialized with driver: scylladb");
                Ok(store)
            }
            #[cfg(not(feature = "scylladb"))]
            {
                let _ = (history_config, db_config, pooling);
                Err(sockudo_core::error::Error::Configuration(
                    "Annotation store driver 'scylladb' requires the 'scylladb' feature"
                        .to_string(),
                ))
            }
        }
        VersionStoreDriver::SurrealDb => {
            #[cfg(feature = "surrealdb")]
            {
                let store = surreal::create_surreal_annotation_store(
                    &db_config.surrealdb,
                    &history_config.surrealdb.table_prefix,
                )
                .await?;
                tracing::info!("AnnotationStore initialized with driver: surrealdb");
                Ok(store)
            }
            #[cfg(not(feature = "surrealdb"))]
            {
                let _ = (history_config, db_config, pooling);
                Err(sockudo_core::error::Error::Configuration(
                    "Annotation store driver 'surrealdb' requires the 'surrealdb' feature"
                        .to_string(),
                ))
            }
        }
    }
}

pub async fn create_history_store(
    history_config: &HistoryConfig,
    db_config: &DatabaseConfig,
    pooling: &DatabasePooling,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    cache_manager: Option<Arc<dyn CacheManager + Send + Sync>>,
) -> Result<Arc<dyn HistoryStore + Send + Sync>> {
    if !history_config.enabled {
        return Ok(Arc::new(sockudo_core::history::NoopHistoryStore));
    }

    match history_config.backend {
        HistoryBackend::Memory => Ok(Arc::new(MemoryHistoryStore::new(
            MemoryHistoryStoreConfig {
                retention_window: Duration::from_secs(history_config.retention_window_seconds),
                max_messages_per_channel: history_config.max_messages_per_channel,
                max_bytes_per_channel: history_config.max_bytes_per_channel,
            },
        ))),
        HistoryBackend::Mysql => {
            #[cfg(feature = "mysql")]
            {
                mysql::create_mysql_history_store(
                    &db_config.mysql,
                    pooling,
                    history_config.clone(),
                    metrics,
                    cache_manager,
                )
                .await
            }
            #[cfg(not(feature = "mysql"))]
            {
                let _ = (db_config, pooling, metrics, cache_manager);
                Err(sockudo_core::error::Error::Configuration(
                    "History backend 'mysql' requires the 'mysql' feature".to_string(),
                ))
            }
        }
        HistoryBackend::DynamoDb => {
            #[cfg(feature = "dynamodb")]
            {
                dynamodb::create_dynamodb_history_store(
                    &db_config.dynamodb,
                    history_config.clone(),
                    metrics,
                    cache_manager,
                )
                .await
            }
            #[cfg(not(feature = "dynamodb"))]
            {
                let _ = (db_config, pooling, metrics, cache_manager);
                Err(sockudo_core::error::Error::Configuration(
                    "History backend 'dynamodb' requires the 'dynamodb' feature".to_string(),
                ))
            }
        }
        HistoryBackend::SurrealDb => {
            #[cfg(feature = "surrealdb")]
            {
                surreal::create_surreal_history_store(
                    &db_config.surrealdb,
                    history_config.clone(),
                    metrics,
                    cache_manager,
                )
                .await
            }
            #[cfg(not(feature = "surrealdb"))]
            {
                let _ = (db_config, pooling, metrics, cache_manager);
                Err(sockudo_core::error::Error::Configuration(
                    "History backend 'surrealdb' requires the 'surrealdb' feature".to_string(),
                ))
            }
        }
        HistoryBackend::Postgres => {
            #[cfg(feature = "postgres")]
            {
                let store = PostgresHistoryStore::new(
                    &db_config.postgres,
                    pooling,
                    history_config.clone(),
                    metrics,
                    cache_manager,
                )
                .await?;
                Ok(Arc::new(store))
            }
            #[cfg(not(feature = "postgres"))]
            {
                let _ = (db_config, pooling, metrics, cache_manager);
                Err(sockudo_core::error::Error::Configuration(
                    "History backend 'postgres' requires the 'postgres' feature".to_string(),
                ))
            }
        }
        HistoryBackend::ScyllaDb => {
            #[cfg(feature = "scylladb")]
            {
                scylla::create_scylla_history_store(
                    &db_config.scylladb,
                    history_config.clone(),
                    metrics,
                    cache_manager,
                )
                .await
            }
            #[cfg(not(feature = "scylladb"))]
            {
                let _ = (db_config, pooling, metrics, cache_manager);
                Err(sockudo_core::error::Error::Configuration(
                    "History backend 'scylladb' requires the 'scylladb' feature".to_string(),
                ))
            }
        }
    }
}

#[cfg(feature = "versioned-messages")]
pub async fn create_version_store(
    versioned_config: &VersionedMessagesConfig,
    history_config: &HistoryConfig,
    db_config: &DatabaseConfig,
    pooling: &DatabasePooling,
) -> Result<Arc<dyn sockudo_core::version_store::VersionStore + Send + Sync>> {
    use sockudo_core::version_store::MemoryVersionStore;

    match versioned_config.driver {
        VersionStoreDriver::Memory => Ok(Arc::new(MemoryVersionStore::new())),
        VersionStoreDriver::Postgres => {
            #[cfg(feature = "postgres")]
            {
                let store = PostgresVersionStore::new(
                    &db_config.postgres,
                    pooling,
                    &history_config.postgres.table_prefix,
                )
                .await?;
                tracing::info!("VersionStore initialized with driver: postgres");
                Ok(Arc::new(store))
            }
            #[cfg(not(feature = "postgres"))]
            {
                let _ = (history_config, db_config, pooling);
                Err(sockudo_core::error::Error::Configuration(
                    "Version store driver 'postgres' requires the 'postgres' feature".to_string(),
                ))
            }
        }
        VersionStoreDriver::Mysql => {
            #[cfg(feature = "mysql")]
            {
                mysql::create_mysql_version_store(
                    &db_config.mysql,
                    pooling,
                    &history_config.mysql.table_prefix,
                )
                .await
            }
            #[cfg(not(feature = "mysql"))]
            {
                let _ = (history_config, db_config, pooling);
                Err(sockudo_core::error::Error::Configuration(
                    "Version store driver 'mysql' requires the 'mysql' feature".to_string(),
                ))
            }
        }
        VersionStoreDriver::DynamoDb => {
            #[cfg(feature = "dynamodb")]
            {
                dynamodb::create_dynamodb_version_store(
                    &db_config.dynamodb,
                    &history_config.dynamodb.table_prefix,
                    versioned_config.retention_window_seconds,
                )
                .await
            }
            #[cfg(not(feature = "dynamodb"))]
            {
                let _ = (history_config, db_config, pooling);
                Err(sockudo_core::error::Error::Configuration(
                    "Version store driver 'dynamodb' requires the 'dynamodb' feature".to_string(),
                ))
            }
        }
        VersionStoreDriver::ScyllaDb => {
            #[cfg(feature = "scylladb")]
            {
                scylla::create_scylla_version_store(
                    &db_config.scylladb,
                    &history_config.scylladb.table_prefix,
                    versioned_config.retention_window_seconds,
                )
                .await
            }
            #[cfg(not(feature = "scylladb"))]
            {
                let _ = (history_config, db_config, pooling);
                Err(sockudo_core::error::Error::Configuration(
                    "Version store driver 'scylladb' requires the 'scylladb' feature".to_string(),
                ))
            }
        }
        VersionStoreDriver::SurrealDb => {
            #[cfg(feature = "surrealdb")]
            {
                surreal::create_surreal_version_store(
                    &db_config.surrealdb,
                    &history_config.surrealdb.table_prefix,
                )
                .await
            }
            #[cfg(not(feature = "surrealdb"))]
            {
                let _ = (history_config, db_config, pooling);
                Err(sockudo_core::error::Error::Configuration(
                    "Version store driver 'surrealdb' requires the 'surrealdb' feature".to_string(),
                ))
            }
        }
    }
}
