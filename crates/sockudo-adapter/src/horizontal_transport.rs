use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::horizontal_adapter::{BroadcastMessage, RequestBody, ResponseBody};
use async_trait::async_trait;
use sockudo_core::error::Result;
use sockudo_core::metrics::MetricsInterface;

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

    /// Whether real-time nodes counting is reliable or not
    fn node_count_is_real_time(&self) -> bool {
        false
    }

    /// Check transport health
    async fn check_health(&self) -> Result<()>;

    /// Attach metrics for transport-level instrumentation.
    fn set_metrics(&self, _metrics: Arc<dyn MetricsInterface + Send + Sync>) {}

    /// Generate a unique inbox subject. Returns None if transport doesn't support direct reply.
    fn new_inbox(&self) -> Option<String> {
        None
    }

    /// Publish a request with a reply-to subject so responders reply directly to the requester.
    /// Default: ignores reply_to and publishes normally.
    async fn publish_request_with_reply(
        &self,
        request: &RequestBody,
        _reply_to: &str,
    ) -> Result<()> {
        self.publish_request(request).await
    }
}

/// Common configuration traits for transport implementations
pub trait TransportConfig: Send + Sync + Clone {
    fn request_timeout_ms(&self) -> u64;
    fn prefix(&self) -> &str;
}
