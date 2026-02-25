//! AWS SQS queue backend.

pub mod backend;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SqsQueueConfig {
    pub region: String,
    pub queue_url_prefix: Option<String>,
    pub visibility_timeout: i32,
    pub endpoint_url: Option<String>,
    pub max_messages: i32,
    pub wait_time_seconds: i32,
    pub concurrency: u32,
    pub fifo: bool,
    pub message_group_id: Option<String>,
}

impl Default for SqsQueueConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            queue_url_prefix: None,
            visibility_timeout: 30,
            endpoint_url: None,
            max_messages: 10,
            wait_time_seconds: 5,
            concurrency: 5,
            fifo: false,
            message_group_id: Some("default".to_string()),
        }
    }
}
