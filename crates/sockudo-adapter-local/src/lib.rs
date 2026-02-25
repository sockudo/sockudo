//! Local adapter implementation crate.

pub mod adapter;
pub mod app;
pub use sockudo_core::delta_compression;
pub use sockudo_core::error;
pub mod metrics;
pub use sockudo_core::namespace;
pub use sockudo_core::websocket;

pub mod backend;
mod local_adapter;

pub use adapter::connection_manager::{
    CompressionParams, ConnectionManager, HorizontalAdapterInterface,
};
pub use backend::{
    into_connection_manager, new_local_adapter, new_local_adapter_with_buffer_multiplier,
};
pub use local_adapter::LocalAdapter as LocalAdapterBackend;

pub fn crate_status() -> &'static str {
    "implemented"
}
