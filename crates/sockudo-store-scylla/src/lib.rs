//! ScyllaDB store backend exports.

pub mod backend;

pub use backend::{Error, ScyllaDbAppStore, ScyllaDbConfig, new_scylla_store};

pub fn crate_status() -> &'static str {
    "implemented"
}
