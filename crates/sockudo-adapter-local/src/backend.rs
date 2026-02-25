use std::sync::Arc;

pub use crate::local_adapter::LocalAdapter as LocalAdapterBackend;

pub fn new_local_adapter() -> LocalAdapterBackend {
    LocalAdapterBackend::new()
}

pub fn new_local_adapter_with_buffer_multiplier(multiplier: usize) -> LocalAdapterBackend {
    LocalAdapterBackend::new_with_buffer_multiplier(multiplier)
}

pub fn into_connection_manager(
    adapter: LocalAdapterBackend,
) -> Arc<dyn crate::adapter::ConnectionManager + Send + Sync> {
    Arc::new(adapter)
}
