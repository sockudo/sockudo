mod incremental;
mod memory;
mod noop;
mod types;

#[doc(hidden)]
pub use incremental::IncrementalProjection;
pub use memory::MemoryAnnotationStore;
pub use noop::NoopAnnotationStore;
pub use types::*;

#[cfg(test)]
mod tests;
