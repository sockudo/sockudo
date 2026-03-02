//! In-memory queue backend.

pub mod backend;
pub use sockudo_core::queue::{ArcJobProcessorFn, JobProcessorFnAsync, QueueBackend, QueueResult};
