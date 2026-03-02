//! NATS adapter implementation crate.

pub(crate) use sockudo_adapter_local::{
    app, delta_compression, error, metrics, namespace, websocket,
};

pub mod adapter;
pub mod backend;

pub use adapter::connection_manager::{
    CompressionParams, ConnectionManager, HorizontalAdapterInterface,
};
pub use backend::{
    NatsAdapterBackend, NatsAdapterConfig, into_connection_manager, nats_adapter_with_servers,
    new_nats_adapter,
};

pub fn crate_status() -> &'static str {
    "implemented"
}
