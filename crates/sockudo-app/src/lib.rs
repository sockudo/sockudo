pub mod cached_app_manager;
#[cfg(feature = "dynamodb")]
pub mod dynamodb_app_manager;
pub mod factory;
pub mod memory_app_manager;
#[cfg(feature = "mysql")]
pub mod mysql_app_manager;
#[cfg(feature = "postgres")]
pub mod pg_app_manager;
#[cfg(feature = "scylladb")]
pub mod scylla_app_manager;
#[cfg(feature = "surrealdb")]
pub mod surrealdb_app_manager;

pub use cached_app_manager::CachedAppManager;
#[cfg(feature = "dynamodb")]
pub use dynamodb_app_manager::{DynamoDbAppManager, DynamoDbConfig};
pub use factory::AppManagerFactory;
pub use memory_app_manager::MemoryAppManager;
#[cfg(feature = "mysql")]
pub use mysql_app_manager::MySQLAppManager;
#[cfg(feature = "postgres")]
pub use pg_app_manager::PgSQLAppManager;
#[cfg(feature = "scylladb")]
pub use scylla_app_manager::{ScyllaDbAppManager, ScyllaDbConfig};
#[cfg(feature = "surrealdb")]
pub use surrealdb_app_manager::{SurrealDbAppManager, SurrealDbConfig};
