use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::horizontal_adapter::{
    BroadcastMessage, HorizontalAdapter, RequestBody, RequestType, ResponseBody,
    generate_request_id,
};
use async_trait::async_trait;
use sockudo_core::error::Result;
use sockudo_core::metrics::MetricsInterface;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Handlers for transport events
pub struct TransportHandlers {
    pub node_id: String,
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

    /// Publish a request to a specific node by ID
    /// Default: broadcasts via publish_request (receiver-side filtering)
    async fn publish_request_to_node(
        &self,
        request: &RequestBody,
        _target_node_id: &str,
    ) -> Result<()> {
        self.publish_request(request).await
    }

    /// Sync this node's full presence registry snapshot to a target node
    /// Default: single PresenceStateSync message via publish_request_to_node
    async fn sync_presence_state_to_node(
        &self,
        horizontal: &Arc<HorizontalAdapter>,
        target_node_id: &str,
    ) -> Result<()> {
        send_presence_state_to_node(self, horizontal, target_node_id).await
    }
}

/// Helper function to send presence state to a new node
pub(crate) async fn send_presence_state_to_node<T: HorizontalTransport>(
    transport: &T,
    horizontal: &Arc<HorizontalAdapter>,
    target_node_id: &str,
) -> Result<()> {
    // Get our presence data
    let (our_node_id, payload) = {
        let registry = horizontal.cluster_presence_registry.read().await;
        // Get only our node's data
        let Some(our_data) = registry.get(&horizontal.node_id) else {
            tracing::debug!("No presence data to send to new node: {}", target_node_id);
            return Ok(());
        };
        if our_data.is_empty() {
            tracing::debug!("Empty presence data for new node: {}", target_node_id);
            return Ok(());
        };
        // Clone the data to avoid holding the lock
        (horizontal.node_id.clone(), sonic_rs::to_value(our_data)?)
    };

    // Serialize the presence data
    let request = RequestBody {
        request_id: generate_request_id(),
        node_id: our_node_id,
        app_id: "cluster".to_string(),
        request_type: RequestType::PresenceStateSync,
        target_node_id: Some(target_node_id.to_string()),
        user_info: Some(payload), // Reuse this field for bulk data
        channel: None,
        socket_id: None,
        user_id: None,
        timestamp: None,
        dead_node_id: None,
        reply_to: None,
        channels: None,
    };

    transport
        .publish_request_to_node(&request, target_node_id)
        .await?;

    tracing::info!(
        "Sent presence state to new node: {} (single message)",
        target_node_id
    );
    Ok(())
}

/// Common configuration traits for transport implementations
pub trait TransportConfig: Send + Sync + Clone {
    fn request_timeout_ms(&self) -> u64;
    fn prefix(&self) -> &str;
}
