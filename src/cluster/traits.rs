// src/cluster/traits.rs
use crate::error::Result;
use async_trait::async_trait;
use std::sync::Arc;

/// Configuration for cluster node tracking
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub heartbeat_interval_ms: u64,
    pub node_timeout_ms: u64,
    pub cleanup_interval_ms: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 30000,  // 30 seconds
            node_timeout_ms: 90000,        // 90 seconds (3x heartbeat)
            cleanup_interval_ms: 60000,    // 60 seconds
        }
    }
}

/// Trait for cluster service implementations that handle dead node detection and cleanup
#[async_trait]
pub trait ClusterService: Send + Sync {
    /// Start the cluster service (heartbeat and cleanup loops)
    async fn start(&self) -> Result<()>;
    
    /// Track when a user joins a presence channel on this node
    async fn track_presence_join(&self, channel: &str, user_id: &str) -> Result<()>;
    
    /// Track when a user leaves a presence channel on this node
    async fn track_presence_leave(&self, channel: &str, user_id: &str) -> Result<()>;
    
    /// Get this node's unique identifier
    fn node_id(&self) -> &str;
}

/// Trait for adapters that support cluster node tracking and dead node cleanup
#[async_trait]
pub trait ClusterNodeTracking: Send + Sync {
    /// Create a cluster service instance using this adapter's storage mechanism
    async fn create_cluster_service(
        &self,
        node_id: String,
        config: ClusterConfig,
    ) -> Result<Arc<dyn ClusterService>>;
    
    /// Whether this adapter supports cluster node tracking
    fn supports_cluster_tracking(&self) -> bool {
        true
    }
}