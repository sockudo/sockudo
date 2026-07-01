#![allow(unused_imports)]

mod connection_manager_impl;
mod core;
mod interface_impl;

use ahash::AHashMap as HashMap;
use std::any::Any;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::connection_manager::{
    ConnectionManager, DeadNodeEventBusReceiver, DeadNodeEventBusSender, HorizontalAdapterInterface,
};
use crate::horizontal_adapter::{
    AggregationStats, BroadcastMessage, DeadNodeEvent, HorizontalAdapter, OrphanedMember,
    PendingRequest, RequestBody, RequestType, ResponseBody, current_timestamp, generate_request_id,
};
use crate::horizontal_transport::{HorizontalTransport, TransportConfig, TransportHandlers};
use crate::local_adapter::LocalAdapter;
use async_trait::async_trait;
use crossfire::mpsc;
use sockudo_core::app::AppManager;
use sockudo_core::channel::PresenceMemberInfo;
use sockudo_core::error::{Error, Result};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::namespace::Namespace;
use sockudo_core::options::ClusterHealthConfig;
use sockudo_core::websocket::{SocketId, WebSocketRef};
use sockudo_protocol::messages::PusherMessage;
use sockudo_ws::axum_integration::WebSocketWriter;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Maximum hash-based spread (ms) when staggering presence-state sync on new-node detection
const PRESENCE_SYNC_STAGGER_MAX_MS: u64 = 5_000;

/// Generic base adapter that handles all common horizontal scaling logic
pub struct HorizontalAdapterBase<T: HorizontalTransport> {
    pub horizontal: Arc<HorizontalAdapter>,
    pub local_adapter: Arc<LocalAdapter>,
    pub transport: T,
    pub config: T::Config,
    pub event_bus: Arc<OnceLock<DeadNodeEventBusSender>>,
    pub node_id: String,
    pub cluster_health_enabled: bool,
    pub heartbeat_interval_ms: u64,
    pub node_timeout_ms: u64,
    pub cleanup_interval_ms: u64,
    pub enable_socket_counting: bool,
    /// Tier 1A: when true, channel counts are read from the gossiped registry
    /// (local + peers) instead of cross-node request/reply.
    pub aggregate_counts: bool,
    /// When true, first-join/last-leave presence transition checks use the
    /// replicated presence registry instead of request/reply.
    pub fast_presence_transitions: bool,
    #[cfg(feature = "delta")]
    // Delta compression manager for bandwidth optimization
    delta_compression: Option<Arc<sockudo_delta::DeltaCompressionManager>>,
    #[cfg(feature = "delta")]
    // App manager for getting channel-specific delta settings
    app_manager: Option<Arc<dyn AppManager + Send + Sync>>,
    // Cache manager for cross-region idempotency deduplication (set once via OnceLock)
    cache_manager: Arc<OnceLock<Arc<dyn sockudo_core::cache::CacheManager + Send + Sync>>>,
    // Idempotency TTL (seconds) used when registering keys from remote broadcasts
    idempotency_ttl: AtomicU64,
    // Shared run flag for background loops so dropping the adapter stops heartbeats/cleanup tasks.
    is_running: Arc<AtomicBool>,
}

/// Check if we should skip horizontal communication (single node optimization)
/// This is determined by checking if cluster health is enabled and
/// if the effective node count is 1 or less.
async fn should_skip_horizontal_communication_impl(
    cluster_health_enabled: bool,
    horizontal: &Arc<HorizontalAdapter>,
) -> bool {
    // Don't skip sending if cluster health is disabled, we have no way of knowing other nodes
    if !cluster_health_enabled {
        return false;
    }
    let effective_node_count = horizontal.get_effective_node_count().await;
    effective_node_count <= 1
}

impl<T: HorizontalTransport> HorizontalAdapterBase<T> {
    /// Check if we should skip horizontal communication (single node optimization)
    /// This is determined by checking if cluster health is enabled and
    /// if the effective node count is 1 or less.
    pub async fn should_skip_horizontal_communication(&self) -> bool {
        should_skip_horizontal_communication_impl(self.cluster_health_enabled, &self.horizontal)
            .await
    }
}

impl<T: HorizontalTransport> Drop for HorizontalAdapterBase<T> {
    fn drop(&mut self) {
        self.is_running.store(false, Ordering::Relaxed);
    }
}
