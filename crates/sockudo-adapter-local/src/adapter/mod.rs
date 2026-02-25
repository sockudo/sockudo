pub use sockudo_core::connection_manager;
pub mod horizontal_adapter;
pub mod local_adapter {
    pub use crate::local_adapter::LocalAdapter;
}

pub use connection_manager::{CompressionParams, ConnectionManager, HorizontalAdapterInterface};
