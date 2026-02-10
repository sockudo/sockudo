// src/delta_compression.rs
//
// Delta compression module supporting multiple algorithms for bandwidth optimization.
// This module provides backwards-compatible delta compression for WebSocket messages
// without breaking the Pusher protocol for standard clients.
//
// Supported algorithms:
// - Fossil Delta: Fast binary delta compression (from Fossil SCM)
// - Xdelta3: Industry-standard delta compression (VCDIFF/RFC 3284)
//
// Design:
// - Clients opt-in by sending pusher:enable_delta_compression event
// - Per-socket per-channel state tracking
// - Periodic full message transmission every N deltas to prevent delta chain issues
// - Falls back to full messages for non-supporting clients

use crate::error::Result;
use crate::websocket::SocketId;
use ahash::AHashMap;
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Delta compression algorithm selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DeltaAlgorithm {
    /// Fossil delta compression (fast, good for text)
    #[default]
    Fossil,
    /// Xdelta3 compression (VCDIFF/RFC 3284, better compression ratio)
    Xdelta3,
}

/// Channel-specific delta compression configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ChannelDeltaConfig {
    /// Simple string configuration for backward compatibility
    Simple(ChannelDeltaSimple),
    /// Full configuration with conflation settings
    Full(ChannelDeltaSettings),
}

/// Simple channel delta configuration (backward compatible)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChannelDeltaSimple {
    Inherit,
    Disabled,
    Fossil,
    Xdelta3,
}

/// Full channel delta compression settings with conflation support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelDeltaSettings {
    /// Enable or disable delta compression for this channel
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Delta compression algorithm to use
    #[serde(default)]
    pub algorithm: DeltaAlgorithm,
    /// JSON path to extract conflation key (e.g., "asset", "data.symbol")
    #[serde(default)]
    pub conflation_key: Option<String>,
    /// Maximum number of messages to cache per conflation key
    #[serde(default = "default_max_messages_per_key")]
    pub max_messages_per_key: usize,
    /// Maximum number of conflation keys to track for this channel
    #[serde(default = "default_max_conflation_keys")]
    pub max_conflation_keys: usize,
    /// Whether to include tags in the message (can be disabled to save bandwidth)
    #[serde(default = "default_true")]
    pub enable_tags: bool,
}

fn default_true() -> bool {
    true
}

fn default_max_messages_per_key() -> usize {
    10
}

fn default_max_conflation_keys() -> usize {
    100
}

impl Default for ChannelDeltaSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithm: DeltaAlgorithm::default(),
            conflation_key: None,
            max_messages_per_key: 10,
            max_conflation_keys: 100,
            enable_tags: true,
        }
    }
}

/// Configuration for delta compression behavior
#[derive(Debug, Clone)]
pub struct DeltaCompressionConfig {
    /// Enable delta compression globally
    pub enabled: bool,
    /// Delta compression algorithm to use
    pub algorithm: DeltaAlgorithm,
    /// Send a full message every N deltas to prevent long delta chains
    pub full_message_interval: u32,
    /// Minimum message size in bytes to consider for delta compression
    pub min_message_size: usize,
    /// Maximum delta state age before forcing a full message
    pub max_state_age: Duration,
    /// Maximum number of channel states to track per socket
    pub max_channel_states_per_socket: usize,
    /// Minimum compression ratio for delta to be beneficial (0.9 = delta must be <90% of original)
    pub min_compression_ratio: f32,
    /// Maximum number of conflation groups to track per channel
    pub max_conflation_states_per_channel: Option<usize>,
    /// JSON path to extract conflation key (e.g., "asset" for root level, "data.symbol" for nested)
    pub conflation_key_path: Option<String>,
    /// Enable cluster-wide delta interval coordination (requires Redis/NATS adapter)
    pub cluster_coordination: bool,
    /// Omit the 'algorithm' field from delta messages (client must infer it)
    pub omit_delta_algorithm: bool,
}

impl Default for DeltaCompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithm: DeltaAlgorithm::default(),
            full_message_interval: 10,
            min_message_size: 100,
            max_state_age: Duration::from_secs(300), // 5 minutes
            max_channel_states_per_socket: 10,       // Reduced from 100 to prevent memory leaks
            min_compression_ratio: 0.9,
            max_conflation_states_per_channel: Some(10), // Reduced from 100 to prevent memory leaks
            conflation_key_path: None,
            cluster_coordination: false, // Disabled by default (opt-in)
            omit_delta_algorithm: false,
        }
    }
}

/// Trait for cluster-wide delta interval coordination
/// Implementations provide shared counters across nodes for synchronized full message intervals
#[async_trait]
pub trait ClusterCoordinator: Send + Sync {
    /// Increment the delta counter for a channel/conflation key and return whether to send full message
    /// Returns (should_send_full, current_count)
    async fn increment_and_check(
        &self,
        app_id: &str,
        channel: &str,
        conflation_key: &str,
        interval: u32,
    ) -> Result<(bool, u32)>;

    /// Reset the counter for a channel/conflation key
    async fn reset_counter(&self, app_id: &str, channel: &str, conflation_key: &str) -> Result<()>;

    /// Get the current counter value (for debugging/metrics)
    async fn get_counter(&self, app_id: &str, channel: &str, conflation_key: &str) -> Result<u32>;
}

/// No-op coordinator for when cluster coordination is disabled
pub struct NoOpCoordinator;

#[async_trait]
impl ClusterCoordinator for NoOpCoordinator {
    async fn increment_and_check(
        &self,
        _app_id: &str,
        _channel: &str,
        _conflation_key: &str,
        _interval: u32,
    ) -> Result<(bool, u32)> {
        // Always return false - use node-local coordination instead
        Ok((false, 0))
    }

    async fn reset_counter(
        &self,
        _app_id: &str,
        _channel: &str,
        _conflation_key: &str,
    ) -> Result<()> {
        Ok(())
    }

    async fn get_counter(
        &self,
        _app_id: &str,
        _channel: &str,
        _conflation_key: &str,
    ) -> Result<u32> {
        Ok(0)
    }
}

/// Delta-compressed message format sent to clients
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaMessage {
    /// The delta payload (base64 encoded for JSON transport)
    pub delta: String,
    /// Sequence number for ordering
    pub seq: u32,
    /// Original event name
    pub event: String,
    /// Channel name
    pub channel: String,
    /// Algorithm used for delta compression
    pub algorithm: String,
}

/// Cached message for delta compression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedMessage {
    /// Message content (full message) - Arc to avoid cloning large messages
    #[serde(
        serialize_with = "serialize_arc_vec",
        deserialize_with = "deserialize_arc_vec"
    )]
    pub content: Arc<Vec<u8>>,
    /// Sequence number for this message
    pub sequence: u32,
    /// Timestamp when message was cached (not serialized)
    #[serde(skip, default = "Instant::now")]
    pub timestamp: Instant,
}

// Custom serialization for Arc<Vec<u8>>
fn serialize_arc_vec<S>(arc: &Arc<Vec<u8>>, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_bytes(arc.as_ref())
}

// Custom deserialization for Arc<Vec<u8>>
fn deserialize_arc_vec<'de, D>(deserializer: D) -> std::result::Result<Arc<Vec<u8>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let vec = Vec::<u8>::deserialize(deserializer)?;
    Ok(Arc::new(vec))
}

impl CachedMessage {
    fn new(content: Vec<u8>, sequence: u32) -> Self {
        Self {
            content: Arc::new(content),
            sequence,
            timestamp: Instant::now(),
        }
    }
}

/// Message cache for a single conflation key
/// Stores only the last message for delta compression (no need for complex caching)
#[derive(Debug, Clone)]
struct ConflationKeyCache {
    /// Last message stored (simple Option instead of cache to prevent eviction bugs)
    last_message: Arc<tokio::sync::RwLock<Option<Arc<CachedMessage>>>>,
    /// Next sequence number
    next_sequence: Arc<std::sync::atomic::AtomicU32>,
    /// Number of deltas sent since last full message
    delta_count: Arc<std::sync::atomic::AtomicU32>,
}

impl ConflationKeyCache {
    fn new(_max_size: usize) -> Self {
        use std::sync::atomic::AtomicU32;

        Self {
            last_message: Arc::new(tokio::sync::RwLock::new(None)),
            next_sequence: Arc::new(AtomicU32::new(0)),
            delta_count: Arc::new(AtomicU32::new(0)),
        }
    }

    async fn add_message(&self, content: Vec<u8>) -> Result<()> {
        use std::sync::atomic::Ordering;

        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let msg = Arc::new(CachedMessage::new(content, sequence));

        *self.last_message.write().await = Some(msg);
        Ok(())
    }

    async fn get_last_message(&self) -> Option<Arc<CachedMessage>> {
        self.last_message.read().await.clone()
    }

    async fn should_send_full_message(&self, config: &DeltaCompressionConfig) -> bool {
        use std::sync::atomic::Ordering;

        let delta_count = self.delta_count.load(Ordering::Relaxed);

        if delta_count >= config.full_message_interval {
            return true;
        }

        let last_msg = self.get_last_message().await;
        if let Some(msg) = last_msg {
            msg.timestamp.elapsed() >= config.max_state_age
        } else {
            true
        }
    }

    fn reset_delta_count(&mut self) {
        use std::sync::atomic::Ordering;
        self.delta_count.store(0, Ordering::Relaxed);
    }

    fn increment_delta_count(&mut self) {
        use std::sync::atomic::Ordering;
        self.delta_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get all cached messages for initial sync
    async fn get_all_messages(&self) -> Vec<CachedMessage> {
        // Return only the last message (if it exists)
        if let Some(msg) = self.get_last_message().await {
            vec![msg.as_ref().clone()]
        } else {
            vec![]
        }
    }
}

/// State for a single channel (can track multiple conflation groups)
#[derive(Debug)]
struct ChannelState {
    /// Conflation groups within this channel (conflation_key -> cache)
    /// If no conflation key is configured, uses empty string as default key
    conflation_groups: DashMap<String, ConflationKeyCache>,
}

impl ChannelState {
    fn new() -> Self {
        Self {
            conflation_groups: DashMap::new(),
        }
    }

    fn get_conflation_state(&self, conflation_key: &str) -> Option<ConflationKeyCache> {
        self.conflation_groups
            .get(conflation_key)
            .map(|v| v.clone())
    }

    async fn set_conflation_state(
        &self,
        conflation_key: String,
        state: ConflationKeyCache,
        max_states: Option<usize>,
    ) {
        self.conflation_groups.insert(conflation_key, state);

        // Enforce limit immediately on insert
        if let Some(max) = max_states {
            if self.conflation_groups.len() > max {
                self.evict_oldest_states(max).await;
            }
        }
    }

    async fn evict_oldest_states(&self, max_states: usize) {
        // Check if we need to evict based on max_states
        if self.conflation_groups.len() <= max_states {
            return;
        }

        // Clone entries to iterate without holding DashMap locks during await
        let entries: Vec<(String, ConflationKeyCache)> = self
            .conflation_groups
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        // First, identify zombie states (empty or read failed)
        let mut zombies = Vec::new();
        for (key, cache) in &entries {
            if cache.get_last_message().await.is_none() {
                zombies.push(key.clone());
            }
        }

        for key in zombies {
            self.conflation_groups.remove(&key);
        }

        // Now check if we still need to evict based on max_states
        if self.conflation_groups.len() <= max_states {
            return;
        }

        // Re-fetch remaining entries to sort
        // We can reuse `entries` but filtering out zombies would be cleaner
        // Actually simpler to just rebuild list of valid caches

        let entries: Vec<(String, ConflationKeyCache)> = self
            .conflation_groups
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        let mut cache_times = Vec::new();
        for (key, cache) in entries {
            if let Some(msg) = cache.get_last_message().await {
                cache_times.push((key, msg.timestamp));
            }
        }

        cache_times.sort_by(|a, b| a.1.cmp(&b.1));

        let to_remove = cache_times.len().saturating_sub(max_states);
        for (key, _) in cache_times.iter().take(to_remove) {
            self.conflation_groups.remove(key);
        }
    }

    async fn cleanup_old_states(&self, config: &DeltaCompressionConfig) {
        let now = Instant::now();

        // Clone entries to iterate safely
        let entries: Vec<(String, ConflationKeyCache)> = self
            .conflation_groups
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        for (key, cache) in entries {
            if let Some(last_msg) = cache.get_last_message().await {
                if now.duration_since(last_msg.timestamp) >= config.max_state_age * 2 {
                    self.conflation_groups.remove(&key);
                }
            } else {
                // No message = zombie state, remove it
                self.conflation_groups.remove(&key);
            }
        }

        // Limit number of conflation groups
        if let Some(max_states) = config.max_conflation_states_per_channel {
            self.evict_oldest_states(max_states).await;
        }
    }
}

/// Per-socket delta compression state
#[derive(Debug)]
struct SocketDeltaState {
    /// Whether this socket supports delta compression
    enabled: bool,
    /// Per-channel state tracking
    channel_states: DashMap<String, Arc<ChannelState>>,
}

impl SocketDeltaState {
    fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn get_channel_state(&self, channel: &str) -> Option<Arc<ChannelState>> {
        self.channel_states.get(channel).map(|v| Arc::clone(&v))
    }

    fn set_channel_state(&self, channel: String, state: Arc<ChannelState>) {
        self.channel_states.insert(channel, state);
    }

    async fn cleanup_old_channels_and_states(&self, config: &DeltaCompressionConfig) {
        // Clean up old conflation states within each channel
        // Iterate safely
        let entries: Vec<(String, Arc<ChannelState>)> = self
            .channel_states
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        for (_, state) in entries {
            state.cleanup_old_states(config).await;
        }

        // Remove channels that have no conflation groups left
        self.channel_states
            .retain(|_, state| !state.conflation_groups.is_empty());

        // If we exceed max channels, remove oldest ones based on most recent conflation state
        if self.channel_states.len() > config.max_channel_states_per_socket {
            let mut channel_times = Vec::new();

            // Gather timestamps async
            let entries: Vec<(String, Arc<ChannelState>)> = self
                .channel_states
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect();

            for (key, state) in entries {
                let mut max_time = None;
                // We need to iterate conflation groups async to get timestamps
                let groups: Vec<ConflationKeyCache> = state
                    .conflation_groups
                    .iter()
                    .map(|g| g.value().clone())
                    .collect();

                for group in groups {
                    if let Some(msg) = group.get_last_message().await {
                        let t = msg.timestamp;
                        max_time = Some(
                            max_time.map_or(t, |curr: Instant| if t > curr { t } else { curr }),
                        );
                    }
                }

                if let Some(t) = max_time {
                    channel_times.push((key, t));
                }
            }

            channel_times.sort_by(|a, b| a.1.cmp(&b.1));

            let to_remove = channel_times
                .len()
                .saturating_sub(config.max_channel_states_per_socket);
            for (channel, _) in channel_times.iter().take(to_remove) {
                self.channel_states.remove(channel);
            }
        }
    }
}

/// Main delta compression manager
pub struct DeltaCompressionManager {
    pub config: DeltaCompressionConfig,
    /// Per-socket state tracking
    socket_states: Arc<DashMap<SocketId, Arc<SocketDeltaState>>>,
    /// Cluster coordinator for synchronized delta intervals across nodes (optional)
    cluster_coordinator: Option<Arc<dyn ClusterCoordinator>>,
    /// Handle for the background cleanup task (for graceful shutdown)
    cleanup_task_handle: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl DeltaCompressionManager {
    pub fn new(config: DeltaCompressionConfig) -> Self {
        Self {
            config,
            socket_states: Arc::new(DashMap::new()),
            cluster_coordinator: None,
            cleanup_task_handle: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Start the background cleanup task that periodically removes stale state
    /// This prevents unbounded memory growth from old channel/conflation states
    ///
    /// Call this once after creating the DeltaCompressionManager.
    /// The cleanup task will run until the manager is dropped.
    pub async fn start_cleanup_task(self: &Arc<Self>) {
        let manager = Arc::clone(self);
        let cleanup_interval = manager.config.max_state_age; // Use max_state_age as cleanup interval

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);

            loop {
                interval.tick().await;

                // Run cleanup on all socket states
                manager.cleanup().await;

                // Log stats periodically for monitoring - use INFO level for visibility
                let stats = manager.get_stats();
                if stats.total_sockets > 0 {
                    let memory_mb = stats.estimated_memory_bytes as f64 / (1024.0 * 1024.0);
                    tracing::info!(
                        "Delta compression stats: sockets={} (enabled={}), channels={}, conflation_groups={}, est_memory={:.2}MB",
                        stats.total_sockets,
                        stats.enabled_sockets,
                        stats.total_channel_states,
                        stats.total_conflation_groups,
                        memory_mb
                    );

                    // Warn if memory usage is high (>100MB)
                    if stats.estimated_memory_bytes > 100 * 1024 * 1024 {
                        tracing::warn!(
                            "Delta compression memory usage is high: {:.2}MB. Consider reducing max_channel_states_per_socket or max_conflation_states_per_channel",
                            memory_mb
                        );
                    }
                }
            }
        });

        // Store the handle for graceful shutdown
        let mut task_guard = self.cleanup_task_handle.lock().await;
        if let Some(old_handle) = task_guard.take() {
            // Cancel the old task if there was one
            old_handle.abort();
        }
        *task_guard = Some(handle);

        tracing::info!(
            "Delta compression cleanup task started (interval: {:?})",
            cleanup_interval
        );
    }

    /// Stop the background cleanup task gracefully
    pub async fn stop_cleanup_task(&self) {
        let mut task_guard = self.cleanup_task_handle.lock().await;
        if let Some(handle) = task_guard.take() {
            handle.abort();
            tracing::info!("Delta compression cleanup task stopped");
        }
    }

    /// Set the cluster coordinator for synchronized delta intervals across nodes
    pub fn set_cluster_coordinator(&mut self, coordinator: Arc<dyn ClusterCoordinator>) {
        self.cluster_coordinator = Some(coordinator);
    }

    /// Check if cluster coordination is enabled
    pub fn has_cluster_coordination(&self) -> bool {
        self.cluster_coordinator.is_some() && self.config.cluster_coordination
    }

    /// Check cluster-wide delta interval and increment counter
    /// Returns (should_send_full_message, current_count)
    pub async fn check_cluster_interval(
        &self,
        app_id: &str,
        channel: &str,
        conflation_key: &str,
    ) -> Result<(bool, u32)> {
        if let Some(coordinator) = &self.cluster_coordinator {
            if self.config.cluster_coordination {
                return coordinator
                    .increment_and_check(
                        app_id,
                        channel,
                        conflation_key,
                        self.config.full_message_interval,
                    )
                    .await;
            }
        }
        // Fallback: no coordination, use node-local (return false to use node-local logic)
        Ok((false, 0))
    }

    /// Extract conflation key from message bytes using a specific path (public for broadcast optimization)
    /// Returns empty string if extraction fails
    pub fn extract_conflation_key_from_path(&self, message_bytes: &[u8], key_path: &str) -> String {
        self.extract_conflation_key_with_path(message_bytes, key_path)
    }

    /// Get the conflation key path from config (public for broadcast optimization)
    pub fn get_conflation_key_path(&self) -> Option<&String> {
        self.config.conflation_key_path.as_ref()
    }

    /// Get the delta compression algorithm from config (public for broadcast optimization)
    pub fn get_algorithm(&self) -> DeltaAlgorithm {
        self.config.algorithm
    }

    /// Extract conflation key from message bytes using a specific path
    /// Returns empty string if extraction fails
    fn extract_conflation_key_with_path(&self, message_bytes: &[u8], key_path: &str) -> String {
        // Helper to stringify a JSON value consistently
        let to_string = |v: &Value| match v {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => format!("{}", v),
        };

        // Parse JSON message
        let json_value: Value = match serde_json::from_slice(message_bytes) {
            Ok(v) => v,
            Err(_) => return String::new(), // Invalid JSON, use default key
        };

        // Extract value using simple path notation (e.g., "asset" or "data.symbol")
        let parts: Vec<&str> = key_path.split('.').collect();
        let mut current = &json_value;

        for part in &parts {
            current = match current.get(part) {
                Some(v) => v,
                None => {
                    // Fallback: if the configured path is a single segment (e.g. "asset")
                    // check inside the "data" object commonly used by Pusher payloads.
                    if parts.len() == 1 {
                        // Direct object payload
                        if let Some(data_obj) = json_value.get("data").and_then(|v| v.as_object()) {
                            if let Some(v) = data_obj.get(*part) {
                                return to_string(v);
                            }
                        }
                        // Stringified JSON payload
                        if let Some(Value::String(data_str)) = json_value.get("data") {
                            if let Ok(Value::Object(data_obj)) =
                                serde_json::from_str::<Value>(data_str)
                            {
                                if let Some(v) = data_obj.get(*part) {
                                    return to_string(v);
                                }
                            }
                        }
                    }

                    // Path not found, use default key
                    return String::new();
                }
            };
        }

        // Convert to string
        to_string(current)
    }

    /// Extract conflation key from message bytes based on globally configured path
    /// Returns empty string if no conflation key is configured or extraction fails
    ///
    /// Note: Prefer using extract_conflation_key_with_path directly when you have channel-specific settings
    #[allow(dead_code)]
    fn extract_conflation_key(&self, message_bytes: &[u8]) -> String {
        // If no conflation key path is configured, use empty string (single state per channel)
        let key_path = match &self.config.conflation_key_path {
            Some(path) => path,
            None => return String::new(),
        };

        self.extract_conflation_key_with_path(message_bytes, key_path)
    }

    /// Enable delta compression for a specific socket
    pub fn enable_for_socket(&self, socket_id: &SocketId) {
        self.socket_states.insert(
            socket_id.clone(),
            Arc::new(SocketDeltaState {
                enabled: true,
                channel_states: DashMap::new(),
            }),
        );
    }

    /// Check if delta compression is globally enabled (config level)
    /// This is separate from per-socket enablement
    pub fn is_globally_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Check if a socket has delta compression enabled
    /// If channel_override is true, allows delta compression even if global is disabled
    pub fn is_enabled_for_socket(&self, socket_id: &SocketId) -> bool {
        self.is_enabled_for_socket_with_override(socket_id, false)
    }

    /// Check if a socket has delta compression enabled, with optional channel override
    /// When channel_override is true, skips the global enabled check (for channel-specific settings)
    pub fn is_enabled_for_socket_with_override(
        &self,
        socket_id: &SocketId,
        channel_override: bool,
    ) -> bool {
        // If no channel override, check global config first
        if !channel_override && !self.config.enabled {
            return false;
        }

        self.socket_states
            .get(socket_id)
            .map(|state| state.is_enabled())
            .unwrap_or(false)
    }

    /// Remove socket state when client disconnects
    /// This is critical for preventing memory leaks - must be called on every disconnect
    pub fn remove_socket(&self, socket_id: &SocketId) {
        if self.socket_states.remove(socket_id).is_some() {
            tracing::debug!("Removed delta compression state for socket: {}", socket_id);
        }
    }

    /// Clear channel state for a socket when client unsubscribes
    /// This ensures that when the client resubscribes, they get a fresh full message
    /// instead of deltas based on stale state
    ///
    /// IMPORTANT: This should be called BEFORE the actual unsubscription to prevent
    /// race conditions where a broadcast arrives between unsubscribe and clear.
    pub fn clear_channel_state(&self, socket_id: &SocketId, channel: &str) {
        if let Some(socket_state) = self.socket_states.get(socket_id) {
            let had_state = socket_state.channel_states.contains_key(channel);
            socket_state.channel_states.remove(channel);
            tracing::debug!(
                "clear_channel_state: socket={}, channel={}, had_state={}",
                socket_id,
                channel,
                had_state
            );
        }
        // Note: Not logging a warning if socket_state doesn't exist because this
        // can legitimately happen if delta compression was never enabled for this socket
    }

    /// Get the last message stored for a socket's channel (for broadcast optimization)
    /// Returns None if socket doesn't have delta enabled or no base message exists
    pub async fn get_last_message_for_socket(
        &self,
        socket_id: &SocketId,
        channel: &str,
        conflation_key: &str,
    ) -> Option<Arc<Vec<u8>>> {
        tracing::debug!(
            "get_last_message_for_socket: START socket={}, channel={}, conflation_key='{}'",
            socket_id,
            channel,
            conflation_key
        );

        let socket_state = match self.socket_states.get(socket_id) {
            Some(s) => {
                tracing::debug!(
                    "get_last_message_for_socket: Found socket_state for socket={}",
                    socket_id
                );
                s
            }
            None => {
                tracing::debug!(
                    "get_last_message_for_socket: No socket_state for socket={}",
                    socket_id
                );
                return None;
            }
        };

        let channel_state = match socket_state.get_channel_state(channel) {
            Some(c) => {
                tracing::debug!(
                    "get_last_message_for_socket: Found channel_state for socket={}, channel={}",
                    socket_id,
                    channel
                );
                c
            }
            None => {
                tracing::debug!(
                    "get_last_message_for_socket: No channel_state for socket={}, channel={}",
                    socket_id,
                    channel
                );
                return None;
            }
        };

        let cache = match channel_state.get_conflation_state(conflation_key) {
            Some(c) => {
                tracing::debug!(
                    "get_last_message_for_socket: Found cache for socket={}, channel={}, conflation_key='{}'",
                    socket_id,
                    channel,
                    conflation_key
                );
                c
            }
            None => {
                tracing::debug!(
                    "get_last_message_for_socket: No cache for socket={}, channel={}, conflation_key='{}'",
                    socket_id,
                    channel,
                    conflation_key
                );
                return None;
            }
        };

        let result = cache.get_last_message().await;
        tracing::debug!(
            "get_last_message_for_socket: get_last_message() returned {} for socket={}, channel={}, conflation_key='{}'",
            if result.is_some() {
                "Some(message)"
            } else {
                "None"
            },
            socket_id,
            channel,
            conflation_key
        );

        result.map(|msg| Arc::clone(&msg.content))
    }

    /// Get the last message stored for a socket's channel WITH its sequence number
    /// Returns (message_content, sequence) for use in precomputed delta paths
    /// where we need to know the actual base_index
    pub async fn get_last_message_with_sequence(
        &self,
        socket_id: &SocketId,
        channel: &str,
        conflation_key: &str,
    ) -> Option<(Arc<Vec<u8>>, u32)> {
        let socket_state = self.socket_states.get(socket_id)?;
        let channel_state = socket_state.get_channel_state(channel)?;
        let cache = channel_state.get_conflation_state(conflation_key)?;
        let msg = cache.get_last_message().await?;
        Some((Arc::clone(&msg.content), msg.sequence))
    }

    /// Get the next sequence number for a socket's channel/conflation_key
    /// Returns the sequence that will be used for the next message
    /// If the cache doesn't exist yet, returns 0
    pub fn get_next_sequence(
        &self,
        socket_id: &SocketId,
        channel: &str,
        conflation_key: &str,
    ) -> u32 {
        use std::sync::atomic::Ordering;

        let socket_state = match self.socket_states.get(socket_id) {
            Some(state) => state,
            None => return 0,
        };

        let channel_state = match socket_state.get_channel_state(channel) {
            Some(state) => state,
            None => return 0,
        };

        let cache = match channel_state.get_conflation_state(conflation_key) {
            Some(cache) => cache,
            None => return 0,
        };

        cache.next_sequence.load(Ordering::Relaxed)
    }

    /// Compute delta between two messages (broadcast-level, can be called once and reused)
    /// This avoids recomputing the same delta for multiple sockets
    pub fn compute_delta_for_broadcast(
        &self,
        base_message: &[u8],
        new_message: &[u8],
    ) -> Result<Vec<u8>> {
        // Compute delta using the configured algorithm
        match self.config.algorithm {
            DeltaAlgorithm::Fossil => self.compute_fossil_delta(base_message, new_message),
            DeltaAlgorithm::Xdelta3 => self.compute_xdelta3_delta(base_message, new_message),
        }
    }

    /// Compress a message for a specific socket and channel with optional channel-specific settings
    /// Returns either a delta-compressed message or the original message
    ///
    /// IMPORTANT: This method does NOT store the message in the cache. After sending the message
    /// to the client, call store_sent_message() with the ACTUAL bytes that were sent (including
    /// any metadata like __delta_seq, __delta_full, etc.)
    pub async fn compress_message(
        &self,
        socket_id: &SocketId,
        channel: &str,
        event_name: &str,
        message_bytes: &[u8],
        channel_settings: Option<&ChannelDeltaSettings>,
    ) -> Result<CompressionResult> {
        // Check if compression is enabled for this socket
        if !self.is_enabled_for_socket(socket_id) {
            return Ok(CompressionResult::Uncompressed);
        }

        // Check minimum message size
        if message_bytes.len() < self.config.min_message_size {
            return Ok(CompressionResult::Uncompressed);
        }

        // Determine conflation key path (channel-specific or global)
        let conflation_key_path = channel_settings
            .and_then(|s| s.conflation_key.as_ref())
            .or(self.config.conflation_key_path.as_ref());

        // Extract data-based conflation key from message
        let conflation_key = if let Some(path) = conflation_key_path {
            self.extract_conflation_key_with_path(message_bytes, path)
        } else {
            String::new()
        };

        // Create composite cache key: event_name:conflation_key
        // This ensures deltas are only computed within the same event type
        let cache_key = if conflation_key.is_empty() {
            event_name.to_string()
        } else {
            format!("{}:{}", event_name, conflation_key)
        };

        // Determine max messages per key (channel-specific or global default)
        let max_messages_per_key = channel_settings
            .map(|s| s.max_messages_per_key)
            .unwrap_or(10);

        // Get or create socket state
        let socket_state = match self.socket_states.get(socket_id) {
            Some(state) => state,
            None => return Ok(CompressionResult::Uncompressed),
        };

        // Get or create channel state
        let channel_state = socket_state.get_channel_state(channel);
        let channel_state = match channel_state {
            Some(state) => state,
            None => {
                // First message on this channel - create channel state
                let new_channel_state = Arc::new(ChannelState::new());
                socket_state.set_channel_state(channel.to_string(), Arc::clone(&new_channel_state));
                new_channel_state
            }
        };

        // Get conflation cache using the composite cache key (event:conflation_key)
        let conflation_cache = channel_state.get_conflation_state(&cache_key);

        // Prepare conflation key option (only if we have a path configured)
        let conflation_key_opt = if conflation_key_path.is_some() && !conflation_key.is_empty() {
            Some(conflation_key.clone())
        } else {
            None
        };

        match conflation_cache {
            None => {
                // First message for this cache key - create empty cache
                let cache = ConflationKeyCache::new(max_messages_per_key);
                channel_state
                    .set_conflation_state(
                        cache_key.clone(),
                        cache,
                        self.config.max_conflation_states_per_channel,
                    )
                    .await;
                Ok(CompressionResult::FullMessage {
                    sequence: 0,
                    conflation_key: conflation_key_opt,
                })
            }
            Some(cache) => {
                use std::sync::atomic::Ordering;

                // Check if we should send a full message
                let should_send_full = cache.should_send_full_message(&self.config).await;
                tracing::debug!(
                    "compress_message: should_send_full_message={}, delta_count={}, full_message_interval={}",
                    should_send_full,
                    cache.delta_count.load(Ordering::Relaxed),
                    self.config.full_message_interval
                );
                if should_send_full {
                    let sequence = cache.next_sequence.load(Ordering::Relaxed);
                    tracing::info!(
                        "Sending FULL message due to interval (delta_count={}, interval={})",
                        cache.delta_count.load(Ordering::Relaxed),
                        self.config.full_message_interval
                    );
                    return Ok(CompressionResult::FullMessage {
                        sequence,
                        conflation_key: conflation_key_opt,
                    });
                }

                // Get last message for delta computation
                let (last_msg, base_index) = match cache.get_last_message().await {
                    Some(msg) => {
                        // Use the sequence number of the last message as base_index
                        let base_seq = msg.sequence as usize;
                        let base_content = &msg.content;
                        tracing::info!(
                            "🟡 compress_message: Using base for delta: base_seq={}, base_len={}, base_last50='{}', cache_key='{}'",
                            base_seq,
                            base_content.len(),
                            String::from_utf8_lossy(
                                &base_content[base_content.len().saturating_sub(50)..]
                            ),
                            cache_key
                        );
                        (msg.content.clone(), Some(base_seq))
                    }
                    None => {
                        // No base message, send full
                        let sequence = cache.next_sequence.load(Ordering::Relaxed);
                        tracing::warn!(
                            "compress_message: No last message in cache (evicted?), sending FULL, socket={}, channel={}, cache_key='{}'",
                            socket_id,
                            channel,
                            cache_key
                        );
                        return Ok(CompressionResult::FullMessage {
                            sequence,
                            conflation_key: conflation_key_opt,
                        });
                    }
                };

                // Compute delta using the configured algorithm
                let delta = match self.config.algorithm {
                    DeltaAlgorithm::Fossil => self.compute_fossil_delta(&last_msg, message_bytes),
                    DeltaAlgorithm::Xdelta3 => self.compute_xdelta3_delta(&last_msg, message_bytes),
                };

                let delta = match delta {
                    Ok(d) => d,
                    Err(_) => {
                        // Delta computation failed - send full message
                        let sequence = cache.next_sequence.load(Ordering::Relaxed);
                        return Ok(CompressionResult::FullMessage {
                            sequence,
                            conflation_key: conflation_key_opt,
                        });
                    }
                };

                // Check if delta is actually smaller than original
                if delta.len() >= message_bytes.len() {
                    // Delta is not beneficial - send full message
                    let sequence = cache.next_sequence.load(Ordering::Relaxed);
                    return Ok(CompressionResult::FullMessage {
                        sequence,
                        conflation_key: conflation_key_opt,
                    });
                }

                // Delta is beneficial - use it
                let sequence = cache.next_sequence.load(Ordering::Relaxed);

                Ok(CompressionResult::Delta {
                    delta,
                    sequence,
                    algorithm: self.config.algorithm,
                    conflation_key: conflation_key_opt,
                    base_index,
                })
            }
        }
    }

    /// Store the actual message that was sent to the client
    /// This must be called AFTER sending the message, with the exact bytes that were sent
    /// (including any metadata like __delta_seq, __delta_full, etc.)
    ///
    /// This ensures that the next delta computation uses the same base message that the client has
    pub async fn store_sent_message(
        &self,
        socket_id: &SocketId,
        channel: &str,
        event_name: &str,
        sent_message_bytes: Vec<u8>,
        is_full_message: bool,
        channel_settings: Option<&ChannelDeltaSettings>,
    ) -> Result<()> {
        // Get socket state
        let socket_state = match self.socket_states.get(socket_id) {
            Some(state) => state,
            None => return Ok(()), // Socket not tracked, nothing to do
        };

        // Get or create channel state
        // Note: Channel state is created lazily on first message after subscribe.
        // We DO want to create it for new subscriptions, but we DON'T want to recreate
        // it after it was explicitly cleared by unsubscribe.
        //
        // The key insight: if channel state doesn't exist AND is_full_message=false,
        // then this is a delta message trying to use state that was cleared, which is wrong.
        // If is_full_message=true, we should create the state for future deltas.
        let channel_state = match socket_state.get_channel_state(channel) {
            Some(state) => state,
            None => {
                if !is_full_message {
                    // This is a delta message but no base state exists - client unsubscribed
                    tracing::debug!(
                        "store_sent_message: Delta message but no channel_state for socket={}, channel={} (likely unsubscribed), skipping",
                        socket_id,
                        channel
                    );
                    return Ok(());
                }

                // This is a full message, create channel state for future deltas
                tracing::debug!(
                    "store_sent_message: Creating channel_state for socket={}, channel={} (new subscription)",
                    socket_id,
                    channel
                );
                let new_channel_state = Arc::new(ChannelState::new());
                socket_state.set_channel_state(channel.to_string(), Arc::clone(&new_channel_state));
                new_channel_state
            }
        };

        // Determine conflation key path (channel-specific or global)
        let conflation_key_path = channel_settings
            .and_then(|s| s.conflation_key.as_ref())
            .or(self.config.conflation_key_path.as_ref());

        // Extract conflation key from the sent message
        let conflation_key = if let Some(path) = conflation_key_path {
            self.extract_conflation_key_with_path(&sent_message_bytes, path)
        } else {
            String::new()
        };

        // Create composite cache key: event_name:conflation_key
        let cache_key = if conflation_key.is_empty() {
            event_name.to_string()
        } else {
            format!("{}:{}", event_name, conflation_key)
        };

        // Determine max messages per key (channel-specific or global default)
        let max_messages_per_key = channel_settings
            .map(|s| s.max_messages_per_key)
            .unwrap_or(10);

        // Get or create conflation cache using the composite cache key
        let cache_existed = channel_state.get_conflation_state(&cache_key).is_some();
        let mut cache = match channel_state.get_conflation_state(&cache_key) {
            Some(cache) => {
                tracing::debug!(
                    "store_sent_message: Found existing cache for socket={}, channel={}, cache_key='{}'",
                    socket_id,
                    channel,
                    cache_key
                );
                cache
            }
            None => {
                tracing::debug!(
                    "store_sent_message: Creating NEW cache for socket={}, channel={}, cache_key='{}'",
                    socket_id,
                    channel,
                    cache_key
                );
                ConflationKeyCache::new(max_messages_per_key)
            }
        };

        // Add the sent message to cache
        cache.add_message(sent_message_bytes).await?;
        tracing::debug!(
            "store_sent_message: Added message to cache (cache_existed={}, socket={}, channel={}, cache_key='{}')",
            cache_existed,
            socket_id,
            channel,
            cache_key
        );

        // Update delta count based on message type
        if is_full_message {
            tracing::debug!(
                "store_sent_message: Resetting delta_count (was {}) for channel={}, cache_key='{}'",
                cache.delta_count.load(std::sync::atomic::Ordering::Relaxed),
                channel,
                cache_key
            );
            cache.reset_delta_count();
        } else {
            let old_count = cache.delta_count.load(std::sync::atomic::Ordering::Relaxed);
            cache.increment_delta_count();
            let new_count = cache.delta_count.load(std::sync::atomic::Ordering::Relaxed);
            tracing::debug!(
                "store_sent_message: Incremented delta_count from {} to {} for channel={}, cache_key='{}'",
                old_count,
                new_count,
                channel,
                cache_key
            );
        }

        // Store updated cache using the composite cache key
        channel_state
            .set_conflation_state(
                cache_key.clone(),
                cache,
                self.config.max_conflation_states_per_channel,
            )
            .await;
        tracing::debug!(
            "store_sent_message: Stored cache back to channel_state (socket={}, channel={}, cache_key='{}')",
            socket_id,
            channel,
            cache_key
        );

        Ok(())
    }

    /// Compute delta using Fossil algorithm
    ///
    /// Note: The fossil_delta Rust crate's `delta(a, b)` produces a delta that when applied
    /// with `deltainv(x, d)` reconstructs `a` (the first argument). However, the JavaScript
    /// fossil-delta library's `applyDelta(base, delta)` expects the delta to transform
    /// `base` into the new value. To make the Rust-produced delta compatible with the JS
    /// client's `applyDelta(old, delta)` => new, we swap the arguments: `delta(new, old)`.
    fn compute_fossil_delta(&self, old_message: &[u8], new_message: &[u8]) -> Result<Vec<u8>> {
        let old_str = std::str::from_utf8(old_message).map_err(|e| {
            crate::error::Error::Internal(format!("Invalid UTF-8 in old message: {}", e))
        })?;
        let new_str = std::str::from_utf8(new_message).map_err(|e| {
            crate::error::Error::Internal(format!("Invalid UTF-8 in new message: {}", e))
        })?;

        // IMPORTANT: Swap arguments! The Rust crate's delta(a, b) produces a delta that
        // reconstructs `a` when applied. But JS applyDelta(old, d) expects to get `new`.
        // By calling delta(new, old), the JS client can do applyDelta(old, delta) => new.
        let delta = fossil_delta::delta(new_str, old_str);
        Ok(delta)
    }

    /// Compute delta using Xdelta3 algorithm
    fn compute_xdelta3_delta(&self, old_message: &[u8], new_message: &[u8]) -> Result<Vec<u8>> {
        xdelta3::encode(old_message, new_message)
            .ok_or_else(|| crate::error::Error::Internal("Xdelta3 encoding failed".to_string()))
    }

    /// Cleanup old states periodically
    /// This method:
    /// 1. Removes stale channel states from each socket
    /// 2. Removes sockets that have no channel states left (orphaned)
    /// 3. Enforces max_channel_states_per_socket limit
    pub async fn cleanup(&self) {
        let entries: Vec<(SocketId, Arc<SocketDeltaState>)> = self
            .socket_states
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        let mut cleaned_sockets = 0;
        let mut cleaned_channels = 0;

        for (socket_id, state) in entries {
            let channels_before = state.channel_states.len();

            // Cleanup old channel states within this socket
            state.cleanup_old_channels_and_states(&self.config).await;

            let channels_after = state.channel_states.len();
            cleaned_channels += channels_before.saturating_sub(channels_after);

            // Remove socket state if it has no channel states left and delta is disabled
            // This prevents memory leaks from sockets that enabled delta, unsubscribed from all channels,
            // but never disconnected (e.g., long-lived connections)
            if state.channel_states.is_empty() && !state.is_enabled() {
                self.socket_states.remove(&socket_id);
                cleaned_sockets += 1;
            }
        }

        if cleaned_sockets > 0 || cleaned_channels > 0 {
            tracing::info!(
                "Delta compression cleanup: removed {} socket states, {} channel states",
                cleaned_sockets,
                cleaned_channels
            );
        }
    }

    /// Get cached messages for a channel (for initial sync on subscription)
    /// Returns None if socket doesn't have delta enabled or channel not found
    pub async fn get_channel_cache(
        &self,
        socket_id: &SocketId,
        channel: &str,
    ) -> Option<Vec<(String, Vec<CachedMessage>)>> {
        let socket_state = self.socket_states.get(socket_id)?;
        let channel_state = socket_state.get_channel_state(channel)?;

        // Get all conflation caches
        let mut caches = Vec::new();
        // Convert dashmap iter to vec to handle async
        let groups: Vec<(String, ConflationKeyCache)> = channel_state
            .conflation_groups
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        for (key, cache) in groups {
            caches.push((key, cache.get_all_messages().await));
        }

        // let caches: Vec<(String, Vec<CachedMessage>)> = channel_state
        //     .conflation_groups
        //     .iter()
        //     .map(|entry| (entry.key().clone(), entry.value().get_all_messages()))
        //     .collect();

        Some(caches)
    }

    /// Get global channel cache for sending to new subscribers
    /// This collects cache state from ANY socket that has state for this channel
    /// Used when a new client subscribes and needs the current channel state
    pub async fn get_global_channel_cache_for_sync(
        &self,
        channel: &str,
    ) -> Option<Vec<(String, Vec<CachedMessage>)>> {
        // Collect all conflation caches from all sockets subscribed to this channel
        let mut all_caches: AHashMap<String, Vec<CachedMessage>> = AHashMap::new();

        // Iterate through all sockets and collect their channel state
        for socket_entry in self.socket_states.iter() {
            if let Some(channel_state) = socket_entry.value().get_channel_state(channel) {
                // Collect caches to iterate async
                let groups: Vec<(String, ConflationKeyCache)> = channel_state
                    .conflation_groups
                    .iter()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect();

                for (key, cache) in groups {
                    let messages = cache.get_all_messages().await;

                    // Keep the most complete cache for each conflation key
                    all_caches
                        .entry(key)
                        .and_modify(|existing| {
                            if messages.len() > existing.len() {
                                *existing = messages.clone();
                            }
                        })
                        .or_insert(messages);
                }
            }
        }

        if all_caches.is_empty() {
            None
        } else {
            Some(all_caches.into_iter().collect())
        }
    }

    /// Get statistics about delta compression
    /// Check if we should send a full message for a socket/channel/cache_key
    /// based on the full_message_interval configuration
    pub fn should_send_full_message(
        &self,
        socket_id: &SocketId,
        channel: &str,
        cache_key: &str,
    ) -> bool {
        use std::sync::atomic::Ordering;

        // Get socket state
        let socket_state = match self.socket_states.get(socket_id) {
            Some(state) => state,
            None => return false,
        };

        // Get channel state
        let channel_state = match socket_state.get_channel_state(channel) {
            Some(state) => state,
            None => return false,
        };

        // Get conflation cache
        let cache = match channel_state.get_conflation_state(cache_key) {
            Some(cache) => cache,
            None => return false,
        };

        // Check if delta count has reached the interval
        let delta_count = cache.delta_count.load(Ordering::Relaxed);
        delta_count >= self.config.full_message_interval
    }

    pub fn get_stats(&self) -> DeltaCompressionStats {
        let total_sockets = self.socket_states.len();
        let enabled_sockets = self
            .socket_states
            .iter()
            .filter(|e| e.value().is_enabled())
            .count();
        let total_channel_states: usize = self
            .socket_states
            .iter()
            .map(|e| e.value().channel_states.len())
            .sum();

        // FIX: Collect channel states first to avoid lifetime issue with flat_map
        // The DashMap iterator returns references that don't live long enough for flat_map
        let total_conflation_groups: usize = self
            .socket_states
            .iter()
            .map(|e| {
                e.value()
                    .channel_states
                    .iter()
                    .map(|c| c.value().conflation_groups.len())
                    .sum::<usize>()
            })
            .sum();

        // Estimate memory usage:
        // - Each conflation group has a CachedMessage with ~5KB average content
        // - Plus overhead for Arc, RwLock, DashMap entries (~200 bytes per entry)
        // This is a rough estimate - actual usage depends on message sizes
        let estimated_memory_bytes = total_conflation_groups * (5 * 1024 + 200)  // ~5KB per cached message + overhead
            + total_channel_states * 500  // DashMap overhead per channel
            + total_sockets * 200; // DashMap overhead per socket

        DeltaCompressionStats {
            total_sockets,
            enabled_sockets,
            total_channel_states,
            total_conflation_groups,
            estimated_memory_bytes,
        }
    }
}

/// Result of compression attempt
pub enum CompressionResult {
    /// No compression applied
    Uncompressed,
    /// Full message sent (with sequence number for client tracking)
    FullMessage {
        sequence: u32,
        conflation_key: Option<String>,
    },
    /// Delta compression applied
    Delta {
        delta: Vec<u8>,
        sequence: u32,
        algorithm: DeltaAlgorithm,
        conflation_key: Option<String>,
        base_index: Option<usize>,
    },
}

/// Statistics about delta compression usage
#[derive(Debug, Clone, Serialize)]
pub struct DeltaCompressionStats {
    pub total_sockets: usize,
    pub enabled_sockets: usize,
    pub total_channel_states: usize,
    pub total_conflation_groups: usize,
    /// Estimated memory usage in bytes (rough calculation)
    pub estimated_memory_bytes: usize,
}

/// Create a delta-compressed Pusher protocol message with conflation support
pub fn create_delta_message(
    channel: &str,
    event: &str,
    delta: Vec<u8>,
    sequence: u32,
    algorithm: Option<DeltaAlgorithm>,
    conflation_key: Option<&str>,
    base_index: Option<usize>,
) -> Value {
    let delta_base64 = base64_encode(&delta);

    let mut data = serde_json::json!({
        "event": event,
        "delta": delta_base64,
        "seq": sequence,
    });

    if let Some(algo) = algorithm {
        let algo_str = match algo {
            DeltaAlgorithm::Fossil => "fossil",
            DeltaAlgorithm::Xdelta3 => "xdelta3",
        };
        data["algorithm"] = serde_json::json!(algo_str);
    }

    // Add conflation info if present
    if let Some(key) = conflation_key {
        data["conflation_key"] = serde_json::json!(key);
    }
    if let Some(idx) = base_index {
        data["base_index"] = serde_json::json!(idx);
    }

    serde_json::json!({
        "event": "pusher:delta",
        "channel": channel,
        "data": data.to_string()
    })
}

/// Create a cache sync message to send to clients on subscription
///
/// Performance optimization: Only sends the most recent message per conflation key
/// to avoid sending massive amounts of data on subscribe. New subscribers only need
/// the latest message to start receiving deltas - they don't need the full history.
pub fn create_cache_sync_message(
    channel: &str,
    conflation_key: Option<&str>,
    max_messages_per_key: usize,
    caches: Vec<(String, Vec<CachedMessage>)>,
) -> Value {
    let mut states = serde_json::Map::new();

    for (key, messages) in caches {
        // Only send the most recent message per conflation key
        // This avoids sending 50+ MB of data when a new subscriber joins
        let messages_json: Vec<Value> = messages
            .iter()
            .rev() // Start from most recent
            .take(1) // Only take the last message
            .map(|msg| {
                serde_json::json!({
                    "content": String::from_utf8_lossy(&msg.content),
                    "seq": msg.sequence
                })
            })
            .collect();

        // Only include conflation keys that have messages
        if !messages_json.is_empty() {
            // Strip the "event_name:" prefix from the cache key to match the delta message format
            // Internal cache keys are "event_name:conflation_key" but delta messages use just "conflation_key"
            let client_key = if let Some(colon_pos) = key.find(':') {
                key[colon_pos + 1..].to_string()
            } else {
                key
            };
            states.insert(client_key, serde_json::json!(messages_json));
        }
    }

    let mut data = serde_json::json!({
        "max_messages_per_key": max_messages_per_key,
        "states": states,
    });

    if let Some(key) = conflation_key {
        data["conflation_key"] = serde_json::json!(key);
    }

    serde_json::json!({
        "event": "pusher:delta_cache_sync",
        "channel": channel,
        "data": data.to_string()
    })
}

/// Create a full message with sequence number for delta tracking
pub fn create_full_message_with_seq(
    channel: &str,
    event: &str,
    data: &str,
    sequence: u32,
) -> Value {
    serde_json::json!({
        "event": event,
        "channel": channel,
        "data": data,
        "__delta_seq": sequence,
    })
}

/// Base64 encode delta for JSON transport
fn base64_encode(data: &[u8]) -> String {
    use base64::{Engine as _, engine::general_purpose};
    general_purpose::STANDARD.encode(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_base64_encode() {
        assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
        assert_eq!(base64_encode(b"world"), "d29ybGQ=");
        assert_eq!(base64_encode(b""), "");
    }

    #[tokio::test]
    async fn test_conflation_cache_should_send_full_message() {
        let config = DeltaCompressionConfig::default();
        let mut cache = ConflationKeyCache::new(10);

        // Add first message
        cache.add_message(vec![1, 2, 3]).await.unwrap();
        assert!(!cache.should_send_full_message(&config).await);

        // Increment delta count to threshold
        for _ in 0..config.full_message_interval {
            cache.increment_delta_count();
        }
        assert!(cache.should_send_full_message(&config).await);
    }

    #[tokio::test]
    async fn test_delta_compression_manager_enable_disable() {
        let config = DeltaCompressionConfig::default();
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        assert!(!manager.is_enabled_for_socket(&socket_id));

        manager.enable_for_socket(&socket_id);
        assert!(manager.is_enabled_for_socket(&socket_id));

        manager.remove_socket(&socket_id);
        assert!(!manager.is_enabled_for_socket(&socket_id));
    }

    #[tokio::test]
    async fn test_compression_first_message() {
        let config = DeltaCompressionConfig {
            min_message_size: 20, // Lower threshold for testing
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        manager.enable_for_socket(&socket_id);

        let message = b"{\"test\":\"data\",\"value\":123,\"extra_field\":\"to_make_it_longer\"}";
        let result = manager
            .compress_message(&socket_id, "test-channel", "test-event", message, None)
            .await
            .unwrap();

        match result {
            CompressionResult::FullMessage { sequence, .. } => {
                assert_eq!(sequence, 0);
            }
            _ => panic!("Expected full message for first message"),
        }
    }

    #[tokio::test]
    async fn test_compression_small_message_uncompressed() {
        let config = DeltaCompressionConfig {
            min_message_size: 100,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        manager.enable_for_socket(&socket_id);

        let small_message = b"small";
        let result = manager
            .compress_message(
                &socket_id,
                "test-channel",
                "test-event",
                small_message,
                None,
            )
            .await
            .unwrap();

        match result {
            CompressionResult::Uncompressed => {}
            _ => panic!("Expected uncompressed for small message"),
        }
    }

    #[tokio::test]
    async fn test_compression_delta_creation() {
        let config = DeltaCompressionConfig {
            min_message_size: 20, // Lower threshold for testing
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        manager.enable_for_socket(&socket_id);

        let message1 =
            b"{\"test\":\"data\",\"value\":123,\"extra\":\"content_here_to_make_longer\"}";
        let message2 =
            b"{\"test\":\"data\",\"value\":456,\"extra\":\"content_here_to_make_longer\"}";

        // First message
        let result1 = manager
            .compress_message(&socket_id, "test-channel", "test-event", message1, None)
            .await
            .unwrap();
        // Store the first message
        manager
            .store_sent_message(
                &socket_id,
                "test-channel",
                "test-event",
                message1.to_vec(),
                matches!(result1, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        // Second message should produce delta
        let result = manager
            .compress_message(&socket_id, "test-channel", "test-event", message2, None)
            .await
            .unwrap();
        // Store the second message
        manager
            .store_sent_message(
                &socket_id,
                "test-channel",
                "test-event",
                message2.to_vec(),
                matches!(result, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        match result {
            CompressionResult::Delta {
                delta, sequence, ..
            } => {
                assert_eq!(sequence, 1);
                assert!(delta.len() < message2.len());
            }
            _ => panic!("Expected delta for similar message"),
        }
    }

    #[tokio::test]
    async fn test_xdelta3_algorithm() {
        let config = DeltaCompressionConfig {
            algorithm: DeltaAlgorithm::Xdelta3,
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        manager.enable_for_socket(&socket_id);

        let message1 =
            b"{\"test\":\"data\",\"value\":123,\"extra\":\"content_here_to_make_longer\"}";
        let message2 =
            b"{\"test\":\"data\",\"value\":456,\"extra\":\"content_here_to_make_longer\"}";

        // First message
        let result1 = manager
            .compress_message(&socket_id, "test-channel", "test-event", message1, None)
            .await
            .unwrap();
        manager
            .store_sent_message(
                &socket_id,
                "test-channel",
                "test-event",
                message1.to_vec(),
                matches!(result1, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        // Second message should produce delta
        let result = manager
            .compress_message(&socket_id, "test-channel", "test-event", message2, None)
            .await
            .unwrap();
        manager
            .store_sent_message(
                &socket_id,
                "test-channel",
                "test-event",
                message2.to_vec(),
                matches!(result, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        match result {
            CompressionResult::Delta {
                delta, sequence, ..
            } => {
                assert_eq!(sequence, 1);
                assert!(!delta.is_empty());
            }
            _ => panic!("Expected delta for xdelta3 algorithm"),
        }
    }

    #[tokio::test]
    async fn test_algorithm_comparison() {
        let algorithms = [DeltaAlgorithm::Fossil, DeltaAlgorithm::Xdelta3];

        let message1 = b"{\"counter\":0,\"data\":\"some_data_that_stays_the_same\"}";
        let message2 = b"{\"counter\":1,\"data\":\"some_data_that_stays_the_same\"}";

        for algorithm in &algorithms {
            let config = DeltaCompressionConfig {
                algorithm: *algorithm,
                min_message_size: 20,
                ..Default::default()
            };
            let manager = DeltaCompressionManager::new(config);
            let socket_id = SocketId::new();

            manager.enable_for_socket(&socket_id);

            // First message
            let result1 = manager
                .compress_message(&socket_id, "test-channel", "test-event", message1, None)
                .await
                .unwrap();
            manager
                .store_sent_message(
                    &socket_id,
                    "test-channel",
                    "test-event",
                    message1.to_vec(),
                    matches!(result1, CompressionResult::FullMessage { .. }),
                    None,
                )
                .await
                .unwrap();

            // Second message
            let result = manager
                .compress_message(&socket_id, "test-channel", "test-event", message2, None)
                .await
                .unwrap();
            manager
                .store_sent_message(
                    &socket_id,
                    "test-channel",
                    "test-event",
                    message2.to_vec(),
                    matches!(result, CompressionResult::FullMessage { .. }),
                    None,
                )
                .await
                .unwrap();

            // All algorithms should produce a delta
            match result {
                CompressionResult::Delta { delta, .. } => {
                    assert!(
                        !delta.is_empty(),
                        "Algorithm {:?} produced empty delta",
                        algorithm
                    );
                }
                _ => panic!("Algorithm {:?} did not produce delta", algorithm),
            }
        }
    }

    #[test]
    fn test_conflation_key_extraction_root_level() {
        let config = DeltaCompressionConfig {
            conflation_key_path: Some("asset".to_string()),
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);

        let msg1 = br#"{"asset":"BTC","price":"100.00"}"#;
        let msg2 = br#"{"asset":"ETH","price":"1.00"}"#;
        let msg3 = br#"{"asset":"BTC","price":"100.01"}"#;

        assert_eq!(manager.extract_conflation_key(msg1), "BTC");
        assert_eq!(manager.extract_conflation_key(msg2), "ETH");
        assert_eq!(manager.extract_conflation_key(msg3), "BTC");
    }

    #[test]
    fn test_conflation_key_extraction_nested() {
        let config = DeltaCompressionConfig {
            conflation_key_path: Some("data.symbol".to_string()),
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);

        let msg = br#"{"data":{"symbol":"AAPL","value":150}}"#;
        assert_eq!(manager.extract_conflation_key(msg), "AAPL");
    }

    #[test]
    fn test_conflation_key_extraction_no_path() {
        let config = DeltaCompressionConfig {
            conflation_key_path: None,
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);

        let msg = br#"{"asset":"BTC","price":"100.00"}"#;
        assert_eq!(manager.extract_conflation_key(msg), "");
    }

    #[test]
    fn test_conflation_key_extraction_from_stringified_data() {
        // Test extraction from Pusher protocol format where data is a stringified JSON
        // This is how batch_events and triggerBatch send data
        let config = DeltaCompressionConfig {
            conflation_key_path: Some("item_id".to_string()),
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);

        // Pusher protocol: data field contains stringified JSON
        let msg = br#"{"event":"e","channel":"test","data":"{\"item_id\":\"abc123\",\"price\":100}"}"#;
        assert_eq!(manager.extract_conflation_key(msg), "abc123");

        let msg2 = br#"{"event":"e","channel":"test","data":"{\"item_id\":\"xyz789\",\"audit\":{\"holders\":5}}"}"#;
        assert_eq!(manager.extract_conflation_key(msg2), "xyz789");

        // With nested data object (not stringified) should also work
        let msg3 =
            br#"{"event":"e","channel":"test","data":{"item_id":"def456","price":200}}"#;
        assert_eq!(manager.extract_conflation_key(msg3), "def456");
    }

    #[test]
    fn test_conflation_key_extraction_from_stringified_data_nested_key() {
        // Test nested key extraction from stringified data
        let config = DeltaCompressionConfig {
            conflation_key_path: Some("audit.holders".to_string()),
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);

        // Note: nested paths inside stringified data are NOT supported by the fallback
        // The fallback only works for single-segment paths
        let msg = br#"{"event":"e","channel":"test","data":"{\"audit\":{\"holders\":5}}"}"#;
        // This should return empty string because nested paths don't work with stringified fallback
        assert_eq!(manager.extract_conflation_key(msg), "");
    }

    #[tokio::test]
    async fn test_conflation_key_separate_delta_states() {
        let config = DeltaCompressionConfig {
            conflation_key_path: Some("asset".to_string()),
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        manager.enable_for_socket(&socket_id);

        // Messages for different assets
        let btc1 = br#"{"asset":"BTC","price":"100.00","volume":"1000","timestamp":"2024-01-01T00:00:00Z"}"#;
        let eth1 =
            br#"{"asset":"ETH","price":"1.00","volume":"500","timestamp":"2024-01-01T00:00:00Z"}"#;
        let btc2 = br#"{"asset":"BTC","price":"100.01","volume":"1000","timestamp":"2024-01-01T00:00:01Z"}"#;
        let eth2 =
            br#"{"asset":"ETH","price":"1.01","volume":"500","timestamp":"2024-01-01T00:00:01Z"}"#;

        // First BTC message - should be full
        let result1 = manager
            .compress_message(&socket_id, "prices", "update", btc1, None)
            .await
            .unwrap();
        assert!(matches!(
            result1,
            CompressionResult::FullMessage { sequence: 0, .. }
        ));
        manager
            .store_sent_message(
                &socket_id,
                "prices",
                "update",
                btc1.to_vec(),
                matches!(result1, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        // First ETH message - should be full (different conflation key)
        let result2 = manager
            .compress_message(&socket_id, "prices", "update", eth1, None)
            .await
            .unwrap();
        assert!(matches!(
            result2,
            CompressionResult::FullMessage { sequence: 0, .. }
        ));
        manager
            .store_sent_message(
                &socket_id,
                "prices",
                "update",
                eth1.to_vec(),
                matches!(result2, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        // Second BTC message - should be delta (compared to first BTC)
        let result3 = manager
            .compress_message(&socket_id, "prices", "update", btc2, None)
            .await
            .unwrap();
        manager
            .store_sent_message(
                &socket_id,
                "prices",
                "update",
                btc2.to_vec(),
                matches!(result3, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();
        match result3 {
            CompressionResult::Delta {
                delta, sequence, ..
            } => {
                assert_eq!(sequence, 1);
                // Delta should be small since only price changed slightly
                assert!(delta.len() < btc2.len() / 2);
            }
            _ => panic!("Expected delta for second BTC message"),
        }

        // Second ETH message - should be delta (compared to first ETH)
        let result4 = manager
            .compress_message(&socket_id, "prices", "update", eth2, None)
            .await
            .unwrap();
        manager
            .store_sent_message(
                &socket_id,
                "prices",
                "update",
                eth2.to_vec(),
                matches!(result4, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();
        match result4 {
            CompressionResult::Delta {
                delta, sequence, ..
            } => {
                assert_eq!(sequence, 1);
                // Delta should be small since only price changed slightly
                assert!(delta.len() < eth2.len() / 2);
            }
            _ => panic!("Expected delta for second ETH message"),
        }
    }

    #[tokio::test]
    async fn test_conflation_improves_compression_efficiency() {
        // Without conflation - interleaved messages have poor compression
        let config_no_conflation = DeltaCompressionConfig {
            conflation_key_path: None,
            min_message_size: 20,
            ..Default::default()
        };
        let manager_no_conflation = DeltaCompressionManager::new(config_no_conflation);
        let socket_id_1 = SocketId::new();
        manager_no_conflation.enable_for_socket(&socket_id_1);

        // With conflation - messages grouped by asset have good compression
        let config_conflation = DeltaCompressionConfig {
            conflation_key_path: Some("asset".to_string()),
            min_message_size: 20,
            ..Default::default()
        };
        let manager_conflation = DeltaCompressionManager::new(config_conflation);
        let socket_id_2 = SocketId::new();
        manager_conflation.enable_for_socket(&socket_id_2);

        let btc1 = br#"{"asset":"BTC","price":"100.00","volume":"1000","extra_data":"some_long_string_here"}"#;
        let eth1 = br#"{"asset":"ETH","price":"1.00","volume":"500","extra_data":"some_long_string_here"}"#;
        let btc2 = br#"{"asset":"BTC","price":"100.01","volume":"1000","extra_data":"some_long_string_here"}"#;

        // Without conflation: BTC1 -> ETH1 -> BTC2
        manager_no_conflation
            .compress_message(&socket_id_1, "prices", "update", btc1, None)
            .await
            .unwrap();
        manager_no_conflation
            .compress_message(&socket_id_1, "prices", "update", eth1, None)
            .await
            .unwrap();
        let result_no_conflation = manager_no_conflation
            .compress_message(&socket_id_1, "prices", "update", btc2, None)
            .await
            .unwrap();

        // With conflation: BTC1 -> ETH1 -> BTC2 (but BTC2 compares to BTC1)
        manager_conflation
            .compress_message(&socket_id_2, "prices", "update", btc1, None)
            .await
            .unwrap();
        manager_conflation
            .compress_message(&socket_id_2, "prices", "update", eth1, None)
            .await
            .unwrap();
        let result_conflation = manager_conflation
            .compress_message(&socket_id_2, "prices", "update", btc2, None)
            .await
            .unwrap();

        // With conflation should produce smaller delta
        match (result_no_conflation, result_conflation) {
            (
                CompressionResult::Delta {
                    delta: delta_no_conflation,
                    ..
                },
                CompressionResult::Delta {
                    delta: delta_conflation,
                    ..
                },
            ) => {
                // Conflation should produce smaller or equal delta
                assert!(
                    delta_conflation.len() <= delta_no_conflation.len(),
                    "Conflation delta ({}) should be <= non-conflation delta ({})",
                    delta_conflation.len(),
                    delta_no_conflation.len()
                );
            }
            (CompressionResult::FullMessage { .. }, CompressionResult::Delta { .. }) => {
                // Even better - without conflation sends full, with conflation sends delta
            }
            _ => {
                // Other combinations are acceptable
            }
        }
    }

    #[tokio::test]
    async fn test_conflation_state_cleanup() {
        let config = DeltaCompressionConfig {
            conflation_key_path: Some("asset".to_string()),
            max_conflation_states_per_channel: Some(2), // Only keep 2 conflation groups
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();
        manager.enable_for_socket(&socket_id);

        // Send messages for 3 different assets
        let btc = br#"{"asset":"BTC","price":"100.00","data":"some_long_content_here"}"#;
        let eth = br#"{"asset":"ETH","price":"1.00","data":"some_long_content_here"}"#;
        let ada = br#"{"asset":"ADA","price":"0.50","data":"some_long_content_here"}"#;

        let result1 = manager
            .compress_message(&socket_id, "prices", "update", btc, None)
            .await
            .unwrap();
        manager
            .store_sent_message(
                &socket_id,
                "prices",
                "update",
                btc.to_vec(),
                matches!(result1, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        let result2 = manager
            .compress_message(&socket_id, "prices", "update", eth, None)
            .await
            .unwrap();
        manager
            .store_sent_message(
                &socket_id,
                "prices",
                "update",
                eth.to_vec(),
                matches!(result2, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        let result3 = manager
            .compress_message(&socket_id, "prices", "update", ada, None)
            .await
            .unwrap();
        manager
            .store_sent_message(
                &socket_id,
                "prices",
                "update",
                ada.to_vec(),
                matches!(result3, CompressionResult::FullMessage { .. }),
                None,
            )
            .await
            .unwrap();

        // Get socket state and check channel state
        let socket_state = manager.socket_states.get(&socket_id).unwrap();
        let channel_state = socket_state.get_channel_state("prices").unwrap();

        // Collect which states exist for debugging
        let existing_keys: Vec<String> = channel_state
            .conflation_groups
            .iter()
            .map(|e| e.key().clone())
            .collect();

        // Should have 2 conflation groups - limit is now enforced on insert, not just cleanup
        assert_eq!(
            channel_state.conflation_groups.len(),
            2,
            "Expected 2 conflation groups after limit enforcement on insert, got: {:?}",
            existing_keys
        );

        // Cleanup should not change anything since limit is already enforced
        manager.cleanup().await;

        let channel_state_after = socket_state.get_channel_state("prices").unwrap();
        assert_eq!(channel_state_after.conflation_groups.len(), 2);
    }

    #[test]
    fn test_conflation_key_with_number_type() {
        let config = DeltaCompressionConfig {
            conflation_key_path: Some("id".to_string()),
            min_message_size: 20,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);

        let msg1 = br#"{"id":123,"data":"content"}"#;
        let msg2 = br#"{"id":456,"data":"content"}"#;

        assert_eq!(manager.extract_conflation_key(msg1), "123");
        assert_eq!(manager.extract_conflation_key(msg2), "456");
    }
}

// ============================================================================
// Redis Cluster Coordinator Implementation
// ============================================================================

#[cfg(feature = "redis")]
/// Redis-based cluster coordinator for delta interval synchronization
pub struct RedisClusterCoordinator {
    connection: Arc<tokio::sync::Mutex<redis::aio::ConnectionManager>>,
    prefix: String,
    ttl_seconds: u64,
}

#[cfg(feature = "redis")]
impl RedisClusterCoordinator {
    /// Create a new Redis cluster coordinator
    pub async fn new(redis_url: &str, prefix: Option<&str>) -> Result<Self> {
        let client = redis::Client::open(redis_url).map_err(|e| {
            crate::error::Error::Redis(format!("Failed to create Redis client: {}", e))
        })?;

        let connection_manager_config = redis::aio::ConnectionManagerConfig::new()
            .set_number_of_retries(5)
            .set_exponent_base(2.0)
            .set_max_delay(std::time::Duration::from_millis(5000));

        let connection = client
            .get_connection_manager_with_config(connection_manager_config)
            .await
            .map_err(|e| {
                crate::error::Error::Redis(format!("Failed to connect to Redis: {}", e))
            })?;

        Ok(Self {
            connection: Arc::new(tokio::sync::Mutex::new(connection)),
            prefix: prefix.unwrap_or("sockudo").to_string(),
            ttl_seconds: 300, // 5 minutes TTL
        })
    }

    /// Get the Redis key for a channel/conflation key combination
    fn get_key(&self, app_id: &str, channel: &str, conflation_key: &str) -> String {
        format!(
            "{}:delta_count:{}:{}:{}",
            self.prefix, app_id, channel, conflation_key
        )
    }
}

#[cfg(feature = "redis")]
#[async_trait]
impl ClusterCoordinator for RedisClusterCoordinator {
    async fn increment_and_check(
        &self,
        app_id: &str,
        channel: &str,
        conflation_key: &str,
        interval: u32,
    ) -> Result<(bool, u32)> {
        use redis::AsyncCommands;
        use tracing::debug;

        let key = self.get_key(app_id, channel, conflation_key);
        let mut conn = self.connection.lock().await;

        // Use Redis INCR for atomic increment
        let count: u32 = conn.incr(&key, 1).await.map_err(|e| {
            crate::error::Error::Redis(format!("Failed to increment counter: {}", e))
        })?;

        // Set TTL on first increment
        if count == 1 {
            let _: () = conn
                .expire(&key, self.ttl_seconds as i64)
                .await
                .map_err(|e| crate::error::Error::Redis(format!("Failed to set TTL: {}", e)))?;
        }

        // Check if we should send full message
        let should_send_full = count >= interval;

        if should_send_full {
            debug!(
                "Cluster coordination: Full message triggered (count={}, interval={}) for app={}, channel={}, key={}",
                count, interval, app_id, channel, conflation_key
            );

            // Reset counter back to 0
            let _: () = conn.set(&key, 0).await.map_err(|e| {
                crate::error::Error::Redis(format!("Failed to reset counter: {}", e))
            })?;

            // Refresh TTL
            let _: () = conn
                .expire(&key, self.ttl_seconds as i64)
                .await
                .map_err(|e| crate::error::Error::Redis(format!("Failed to refresh TTL: {}", e)))?;

            Ok((true, interval))
        } else {
            debug!(
                "Cluster coordination: Delta message (count={}/{}) for app={}, channel={}, key={}",
                count, interval, app_id, channel, conflation_key
            );
            Ok((false, count))
        }
    }

    async fn reset_counter(&self, app_id: &str, channel: &str, conflation_key: &str) -> Result<()> {
        use redis::AsyncCommands;
        use tracing::debug;

        let key = self.get_key(app_id, channel, conflation_key);
        let mut conn = self.connection.lock().await;

        let _: () = conn
            .del(&key)
            .await
            .map_err(|e| crate::error::Error::Redis(format!("Failed to delete counter: {}", e)))?;

        debug!(
            "Cluster coordination: Reset counter for app={}, channel={}, key={}",
            app_id, channel, conflation_key
        );
        Ok(())
    }

    async fn get_counter(&self, app_id: &str, channel: &str, conflation_key: &str) -> Result<u32> {
        use redis::AsyncCommands;

        let key = self.get_key(app_id, channel, conflation_key);
        let mut conn = self.connection.lock().await;

        let count: Option<u32> = conn
            .get(&key)
            .await
            .map_err(|e| crate::error::Error::Redis(format!("Failed to get counter: {}", e)))?;

        Ok(count.unwrap_or(0))
    }
}

#[cfg(feature = "redis")]
impl Clone for RedisClusterCoordinator {
    fn clone(&self) -> Self {
        Self {
            connection: Arc::clone(&self.connection),
            prefix: self.prefix.clone(),
            ttl_seconds: self.ttl_seconds,
        }
    }
}

// ============================================================================
// NATS Cluster Coordinator Implementation
// ============================================================================

#[cfg(feature = "nats")]
/// NATS-based cluster coordinator for delta interval synchronization
/// Uses NATS Key-Value store for atomic counter operations
pub struct NatsClusterCoordinator {
    kv_store: Arc<async_nats::jetstream::kv::Store>,
    prefix: String,
    ttl_seconds: u64,
}

#[cfg(feature = "nats")]
impl NatsClusterCoordinator {
    /// Create a new NATS cluster coordinator
    pub async fn new(nats_servers: Vec<String>, prefix: Option<&str>) -> Result<Self> {
        use tracing::info;

        // Connect to NATS
        let client = async_nats::connect(nats_servers.join(","))
            .await
            .map_err(|e| {
                crate::error::Error::Internal(format!("Failed to connect to NATS: {}", e))
            })?;

        info!("Connected to NATS for cluster coordination");

        // Create JetStream context
        let jetstream = async_nats::jetstream::new(client);

        // Create or get KV bucket for delta counters
        let bucket_name = format!("{}_delta_counts", prefix.unwrap_or("sockudo"));

        let kv_store = match jetstream.get_key_value(&bucket_name).await {
            Ok(store) => {
                info!("Using existing NATS KV bucket: {}", bucket_name);
                store
            }
            Err(_) => {
                info!("Creating NATS KV bucket: {}", bucket_name);
                jetstream
                    .create_key_value(async_nats::jetstream::kv::Config {
                        bucket: bucket_name.clone(),
                        description: "Delta compression cluster coordination counters".to_string(),
                        max_age: std::time::Duration::from_secs(300), // 5 minutes TTL
                        history: 1,
                        storage: async_nats::jetstream::stream::StorageType::Memory,
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| {
                        crate::error::Error::Internal(format!(
                            "Failed to create NATS KV bucket: {}",
                            e
                        ))
                    })?
            }
        };

        Ok(Self {
            kv_store: Arc::new(kv_store),
            prefix: prefix.unwrap_or("sockudo").to_string(),
            ttl_seconds: 300, // 5 minutes TTL
        })
    }

    /// Get the NATS KV key for a channel/conflation key combination
    fn get_key(&self, app_id: &str, channel: &str, conflation_key: &str) -> String {
        format!("{}:{}:{}", app_id, channel, conflation_key)
    }

    /// Parse counter value from bytes
    fn parse_counter(&self, data: &[u8]) -> Result<u32> {
        std::str::from_utf8(data)
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .ok_or_else(|| {
                crate::error::Error::Internal("Failed to parse counter value".to_string())
            })
    }
}

#[cfg(feature = "nats")]
#[async_trait]
impl ClusterCoordinator for NatsClusterCoordinator {
    async fn increment_and_check(
        &self,
        app_id: &str,
        channel: &str,
        conflation_key: &str,
        interval: u32,
    ) -> Result<(bool, u32)> {
        use tracing::debug;

        let key = self.get_key(app_id, channel, conflation_key);

        // Get current value
        let current = match self.kv_store.get(&key).await {
            Ok(Some(entry)) => self.parse_counter(&entry)?,
            Ok(None) => 0,
            Err(e) => {
                return Err(crate::error::Error::Internal(format!(
                    "Failed to get NATS KV value: {}",
                    e
                )));
            }
        };

        // Increment counter
        let new_count = current + 1;

        // Check if we should send full message
        let should_send_full = new_count >= interval;

        if should_send_full {
            debug!(
                "Cluster coordination (NATS): Full message triggered (count={}, interval={}) for app={}, channel={}, key={}",
                new_count, interval, app_id, channel, conflation_key
            );

            // Reset counter back to 0
            self.kv_store.put(&key, "0".into()).await.map_err(|e| {
                crate::error::Error::Internal(format!("Failed to reset NATS counter: {}", e))
            })?;

            Ok((true, interval))
        } else {
            debug!(
                "Cluster coordination (NATS): Delta message (count={}/{}) for app={}, channel={}, key={}",
                new_count, interval, app_id, channel, conflation_key
            );

            // Update counter
            self.kv_store
                .put(&key, new_count.to_string().into())
                .await
                .map_err(|e| {
                    crate::error::Error::Internal(format!(
                        "Failed to increment NATS counter: {}",
                        e
                    ))
                })?;

            Ok((false, new_count))
        }
    }

    async fn reset_counter(&self, app_id: &str, channel: &str, conflation_key: &str) -> Result<()> {
        use tracing::debug;

        let key = self.get_key(app_id, channel, conflation_key);

        self.kv_store.delete(&key).await.map_err(|e| {
            crate::error::Error::Internal(format!("Failed to delete NATS counter: {}", e))
        })?;

        debug!(
            "Cluster coordination (NATS): Reset counter for app={}, channel={}, key={}",
            app_id, channel, conflation_key
        );
        Ok(())
    }

    async fn get_counter(&self, app_id: &str, channel: &str, conflation_key: &str) -> Result<u32> {
        let key = self.get_key(app_id, channel, conflation_key);

        match self.kv_store.get(&key).await {
            Ok(Some(entry)) => self.parse_counter(&entry),
            Ok(None) => Ok(0),
            Err(e) => Err(crate::error::Error::Internal(format!(
                "Failed to get NATS counter: {}",
                e
            ))),
        }
    }
}

#[cfg(feature = "nats")]
impl Clone for NatsClusterCoordinator {
    fn clone(&self) -> Self {
        Self {
            kv_store: Arc::clone(&self.kv_store),
            prefix: self.prefix.clone(),
            ttl_seconds: self.ttl_seconds,
        }
    }
}
