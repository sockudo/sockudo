//! MySQL store backend exports.

pub mod backend;

pub use backend::{
    DatabaseConnection, DatabasePooling, Error, MySqlAppStore, new_mysql_store,
    new_mysql_store_with_default_pooling,
};

pub fn crate_status() -> &'static str {
    "implemented"
}
