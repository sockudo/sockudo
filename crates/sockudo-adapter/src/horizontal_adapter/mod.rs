#![allow(dead_code)]

mod operations;
mod utils;

use crate::local_adapter::LocalAdapter;
use ahash::AHashMap;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sockudo_core::channel::PresenceMemberInfo;
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::websocket::SocketId;
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::sync::{Notify, RwLock};
use uuid::Uuid;

pub use utils::{current_timestamp, generate_request_id};

/// Request types for horizontal communication
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RequestType {
    // Original request types
    ChannelMembers,           // Get members in a channel
    ChannelSockets,           // Get sockets in a channel
    ChannelSocketsCount,      // Get count of sockets in a channel
    SocketExistsInChannel,    // Check if socket exists in a channel
    TerminateUserConnections, // Terminate user connections
    ChannelsWithSocketsCount, // Get channels with socket counts

    // New request types
    Sockets,                       // Get all sockets
    Channels,                      // Get all channels
    SocketsCount,                  // Get count of all sockets
    ChannelMembersCount,           // Get count of members in a channel
    CountUserConnectionsInChannel, // Count user's connections in a specific channel

    // Presence replication requests
    PresenceMemberJoined,  // Replicate presence member join across nodes
    PresenceMemberLeft,    // Replicate presence member leave across nodes
    PresenceMemberUpdated, // Replicate presence member data update across nodes

    // Node health requests
    Heartbeat, // Node health heartbeat
    NodeDead,  // Dead node notification

    // State synchronization
    PresenceStateSync,        // Send bulk presence state to a specific node
    BatchChannelSocketsCount, // Get socket counts for a list of channels in one round-trip
}

/// Request body for horizontal communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestBody {
    pub request_id: String,
    pub node_id: String,
    pub app_id: String,
    pub request_type: RequestType,
    pub channel: Option<String>,
    pub socket_id: Option<String>,
    pub user_id: Option<String>,

    // Additional fields for cluster presence replication
    pub user_info: Option<sonic_rs::Value>, // For presence member info (needed for rich presence data)
    pub timestamp: Option<u64>,             // For heartbeat timestamp
    pub dead_node_id: Option<String>,       // For dead node notifications
    pub target_node_id: Option<String>,     // Which node should process this request

    // List of channels for BatchChannelSocketsCount
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channels: Option<Vec<String>>,

    // Inbox channel for targeted response routing, falls back to global channel
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
}

/// Response body for horizontal requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseBody {
    pub request_id: String,
    pub node_id: String,
    pub app_id: String,
    pub members: AHashMap<String, PresenceMemberInfo>,
    pub channels_with_sockets_count: AHashMap<String, usize>,
    pub socket_ids: Vec<String>,
    pub sockets_count: usize,
    pub exists: bool,
    pub channels: HashSet<String>,
    pub members_count: usize, // New field for ChannelMembersCount
    #[serde(default)]
    pub responses_received: usize,
    #[serde(default)]
    pub expected_responses: usize,
    #[serde(default = "default_true")]
    pub complete: bool,
}

fn default_true() -> bool {
    true
}

/// Message for broadcasting events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastMessage {
    pub node_id: String,
    pub app_id: String,
    pub channel: String,
    pub message: String,
    pub except_socket_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<f64>, // Timestamp when broadcast was initiated (milliseconds since epoch with microsecond precision)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_metadata: Option<CompressionMetadata>,
    /// Idempotency key from the original publish request.
    /// Receiving nodes register this in their local cache for cross-region deduplication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Ephemeral flag from extras. Receiving nodes must not buffer this message
    /// in the recovery replay buffer.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ephemeral: bool,
}

/// Metadata for delta compression in broadcasts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionMetadata {
    pub conflation_key: Option<String>,
    pub enabled: bool,
    /// Sequence number for this message in the delta chain
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u32>,
    /// Whether this is a full message (not a delta)
    pub is_full_message: bool,
    /// Event name for tracking delta chains
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_name: Option<String>,
}

/// Request tracking struct
#[derive(Clone)]
pub struct PendingRequest {
    pub(crate) start_time: Instant,
    pub(crate) app_id: String,
    pub(crate) responses: Vec<ResponseBody>,
    pub(crate) notify: Arc<Notify>,
}

#[derive(Debug, Clone, Copy)]
pub struct AggregationStats {
    pub responses_received: usize,
    pub expected_responses: usize,
    pub complete: bool,
}

/// Presence entry for cluster-wide presence tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceEntry {
    pub user_info: Option<sonic_rs::Value>,
    pub node_id: String,      // Which node owns this connection
    pub app_id: String,       // Which app this member belongs to
    pub user_id: String,      // Which user this socket belongs to
    pub socket_id: String,    // Socket ID for this connection
    pub sequence_number: u64, // Sequence number for conflict resolution
}

/// Type alias for the cluster presence registry structure
///AHashMap<node_id,AHashMap<channel,AHashMap<socket_id, PresenceEntry>>>
pub type ClusterPresenceRegistry =
    AHashMap<String, AHashMap<String, AHashMap<String, PresenceEntry>>>;

/// Event emitted when a node dies and orphaned presence members need cleanup
#[derive(Debug, Clone)]
pub struct DeadNodeEvent {
    pub dead_node_id: String,
    pub orphaned_members: Vec<OrphanedMember>,
}

/// Information about an orphaned presence member that needs cleanup
#[derive(Debug, Clone)]
pub struct OrphanedMember {
    pub app_id: String,
    pub channel: String,
    pub user_id: String,
    pub user_info: Option<sonic_rs::Value>,
}

/// Base horizontal adapter
pub struct HorizontalAdapter {
    /// Unique node ID
    pub node_id: String,

    /// Local adapter for handling local connections (Arc for sharing across components)
    pub local_adapter: Arc<LocalAdapter>,

    /// Pending requests map - Use DashMap for thread-safe access
    pub pending_requests: Arc<DashMap<String, PendingRequest>>,

    /// Timeout for requests in milliseconds
    pub requests_timeout: AtomicU64,

    pub metrics: OnceLock<Arc<dyn MetricsInterface + Send + Sync>>,

    /// Complete cluster-wide presence registry (node-first structure for efficient cleanup)
    ///AHashMap<node_id,AHashMap<channel,AHashMap<socket_id, PresenceEntry>>>
    pub cluster_presence_registry: Arc<RwLock<ClusterPresenceRegistry>>,

    /// Track node heartbeats:AHashMap<node_id, last_heartbeat_received_time>
    pub node_heartbeats: Arc<RwLock<AHashMap<String, Instant>>>,

    /// Sequence counter for conflict resolution
    pub sequence_counter: Arc<AtomicU64>,
}

impl Default for HorizontalAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
