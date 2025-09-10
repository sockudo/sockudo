use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::adapter::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use crate::error::Result;
use async_trait::async_trait;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Handlers for transport events
pub struct TransportHandlers {
    pub on_broadcast: Arc<dyn Fn(BroadcastMessage) -> BoxFuture<'static, ()> + Send + Sync>,
    pub on_request:
        Arc<dyn Fn(RequestBody) -> BoxFuture<'static, Result<ResponseBody>> + Send + Sync>,
    pub on_response: Arc<dyn Fn(ResponseBody) -> BoxFuture<'static, ()> + Send + Sync>,
}

/// Transport abstraction for horizontal adapter messaging
#[async_trait]
pub trait HorizontalTransport: Send + Sync + Clone {
    type Config: Send + Sync;

    /// Create a new transport instance
    async fn new(config: Self::Config) -> Result<Self>;

    /// Publish a broadcast message to all nodes
    async fn publish_broadcast(&self, message: &BroadcastMessage) -> Result<()>;

    /// Publish a request message to all nodes
    async fn publish_request(&self, request: &RequestBody) -> Result<()>;

    /// Publish a response message
    async fn publish_response(&self, response: &ResponseBody) -> Result<()>;

    /// Start listening for messages with provided handlers
    async fn start_listeners(&self, handlers: TransportHandlers) -> Result<()>;

    /// Get the current number of nodes in the cluster
    async fn get_node_count(&self) -> Result<usize>;

    /// Check transport health
    async fn check_health(&self) -> Result<()>;
}

/// Common configuration traits for transport implementations
pub trait TransportConfig: Send + Sync + Clone {
    fn request_timeout_ms(&self) -> u64;
    fn prefix(&self) -> &str;
}
