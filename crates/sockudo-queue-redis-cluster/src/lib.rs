//! Redis Cluster queue backend.

pub mod backend;

use async_trait::async_trait;
use sockudo_types::webhook::JobData;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub type QueueResult<T> = Result<T, String>;

pub type JobProcessorFnAsync = Box<
    dyn Fn(JobData) -> Pin<Box<dyn Future<Output = QueueResult<()>> + Send>>
        + Send
        + Sync
        + 'static,
>;

pub type ArcJobProcessorFn = Arc<
    dyn Fn(JobData) -> Pin<Box<dyn Future<Output = QueueResult<()>> + Send>>
        + Send
        + Sync
        + 'static,
>;

#[async_trait]
pub trait QueueBackend: Send + Sync {
    async fn add_to_queue(&self, queue_name: &str, data: JobData) -> QueueResult<()>;
    async fn process_queue(
        &self,
        queue_name: &str,
        callback: JobProcessorFnAsync,
    ) -> QueueResult<()>;
    async fn disconnect(&self) -> QueueResult<()>;
    async fn check_health(&self) -> QueueResult<()>;
}
