mod memory;
mod noop;
mod types;

pub use memory::MemoryAnnotationStore;
pub use noop::NoopAnnotationStore;
pub use types::*;

#[cfg(test)]
mod tests;
