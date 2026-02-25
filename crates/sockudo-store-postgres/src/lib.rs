//! PostgreSQL store backend exports.

pub mod backend;

pub use backend::{
    DatabaseConnection, DatabasePooling, Error, PgSqlAppStore, new_postgres_store,
    new_postgres_store_with_default_pooling,
};

pub fn crate_status() -> &'static str {
    "implemented"
}
