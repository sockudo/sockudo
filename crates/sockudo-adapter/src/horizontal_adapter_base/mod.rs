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
use crate::horizontal_transport::{
    HorizontalTransport, ListenerState, TransportConfig, TransportHandlers,
};
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
use tracing::{Instrument, debug, error, info, warn};
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
    /// API-only mode: publish-only, no listeners or request/reply.
    api_only: bool,
    #[cfg(feature = "delta")]
    // Delta compression manager for bandwidth optimization
    delta_compression: Option<Arc<sockudo_delta::DeltaCompressionManager>>,
    #[cfg(feature = "delta")]
    // App manager for getting channel-specific delta settings
    app_manager: Option<Arc<dyn AppManager + Send + Sync>>,
    // Cache manager for cross-region idempotency deduplication (set once via OnceLock)
    cache_manager: Arc<OnceLock<Arc<dyn sockudo_core::cache::CacheManager + Send + Sync>>>,
    realtime_egress_tap: Arc<OnceLock<Arc<dyn crate::handler::RealtimeEgressTap>>>,
    // Idempotency TTL (seconds) used when registering keys from remote broadcasts
    idempotency_ttl: AtomicU64,
    // Shared run flag for background loops so dropping the adapter stops heartbeats/cleanup tasks.
    is_running: Arc<AtomicBool>,
    discovery: Arc<PeerDiscovery>,
    heartbeat_now: Arc<Notify>,
}

struct PeerDiscovery {
    epoch: Instant,
    deadline_ms: AtomicU64,
}

impl PeerDiscovery {
    const NOT_STARTED: u64 = u64::MAX;
    const DOWN: u64 = u64::MAX - 1;

    fn new() -> Self {
        Self {
            epoch: Instant::now(),
            deadline_ms: AtomicU64::new(Self::NOT_STARTED),
        }
    }

    fn elapsed_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis().min(u64::MAX as u128) as u64
    }

    fn deadline(&self, timeout_ms: u64) -> u64 {
        self.elapsed_ms()
            .saturating_add(timeout_ms)
            .min(Self::DOWN - 1)
    }

    fn start(&self, timeout_ms: u64) {
        let _ = self.deadline_ms.compare_exchange(
            Self::NOT_STARTED,
            self.deadline(timeout_ms),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    fn lost(&self) {
        self.deadline_ms.store(Self::DOWN, Ordering::Release);
    }

    fn restored(&self, timeout_ms: u64) {
        self.deadline_ms
            .store(self.deadline(timeout_ms), Ordering::Release);
    }

    fn is_settled(&self) -> bool {
        let deadline = self.deadline_ms.load(Ordering::Acquire);
        deadline < Self::DOWN && self.elapsed_ms() >= deadline
    }

    fn settle_for_test(&self) {
        self.deadline_ms.store(0, Ordering::Release);
    }
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
        if self.api_only || !self.discovery_settled() {
            return false;
        }
        should_skip_horizontal_communication_impl(self.cluster_health_enabled, &self.horizontal)
            .await
    }

    /// Whether this node has listened for peers long enough to trust an empty heartbeat map.
    ///
    /// A node learns about a peer only from that peer's heartbeats, so right after startup it
    /// knows of no one even when peers exist. Taking the single-node shortcut then would drop
    /// its broadcasts until the first heartbeat arrived. Until `node_timeout_ms` has passed
    /// since discovery started (the same window after which a silent peer is declared dead),
    /// a node with no known peers keeps publishing; at worst that sends a few messages
    /// nobody consumes.
    ///
    /// The same holds after the transport's subscription was lost: while it is down the node
    /// hears no heartbeats and expires its peers, and once it is back it needs a heartbeat
    /// before it knows of them again. So a lost subscription unsettles discovery until it is
    /// restored, and a restored one reopens the window (see `on_listener_state`).
    fn discovery_settled(&self) -> bool {
        self.discovery.is_settled()
    }

    /// Treat peer discovery as complete, so an empty heartbeat map means a single node at once.
    pub fn settle_discovery_for_test(&self) {
        self.discovery.settle_for_test();
    }
}

impl<T: HorizontalTransport> Drop for HorizontalAdapterBase<T> {
    fn drop(&mut self) {
        self.is_running.store(false, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::PeerDiscovery;

    #[test]
    fn startup_cannot_settle_discovery_after_listener_loss() {
        let discovery = PeerDiscovery::new();

        discovery.lost();
        discovery.start(0);
        assert!(!discovery.is_settled());

        discovery.restored(0);
        assert!(discovery.is_settled());
    }
}
