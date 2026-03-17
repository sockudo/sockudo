// sockudo-delta: Delta compression manager for Sockudo WebSocket messages
//
// This crate contains the runtime delta compression logic including the
// DeltaCompressionManager and related types. Configuration/enum types live
// in sockudo_core::delta_types and are re-exported here for convenience.

// Re-export core delta types so consumers can use `sockudo_delta::*`
pub use sockudo_core::delta_types::*;

pub mod coordination;

use ahash::AHashMap;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sockudo_core::error::{Error, Result};
use sockudo_core::websocket::SocketId;
use sonic_rs::Value;
use sonic_rs::prelude::*;
use std::sync::Arc;
use std::time::Instant;

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

fn serialize_arc_vec<S>(arc: &Arc<Vec<u8>>, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_bytes(arc.as_ref())
}

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
/// Stores only the last message for delta compression
#[derive(Debug, Clone)]
struct ConflationKeyCache {
    last_message: Arc<tokio::sync::RwLock<Option<Arc<CachedMessage>>>>,
    next_sequence: Arc<std::sync::atomic::AtomicU32>,
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

    async fn get_all_messages(&self) -> Vec<CachedMessage> {
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

        if let Some(max) = max_states
            && self.conflation_groups.len() > max
        {
            self.evict_oldest_states(max).await;
        }
    }

    async fn evict_oldest_states(&self, max_states: usize) {
        if self.conflation_groups.len() <= max_states {
            return;
        }

        let entries: Vec<(String, ConflationKeyCache)> = self
            .conflation_groups
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        let mut zombies = Vec::new();
        for (key, cache) in &entries {
            if cache.get_last_message().await.is_none() {
                zombies.push(key.clone());
            }
        }

        for key in zombies {
            self.conflation_groups.remove(&key);
        }

        if self.conflation_groups.len() <= max_states {
            return;
        }

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
                self.conflation_groups.remove(&key);
            }
        }

        if let Some(max_states) = config.max_conflation_states_per_channel {
            self.evict_oldest_states(max_states).await;
        }
    }
}

/// Per-channel delta settings for a socket (from subscription negotiation)
#[derive(Debug, Clone)]
pub struct PerChannelDeltaSettings {
    /// Whether delta compression is enabled for this channel subscription
    pub enabled: bool,
    /// Algorithm to use for this channel (overrides global default)
    pub algorithm: Option<DeltaAlgorithm>,
}

impl Default for PerChannelDeltaSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithm: None,
        }
    }
}

/// Per-socket delta compression state
#[derive(Debug)]
struct SocketDeltaState {
    enabled: bool,
    channel_states: DashMap<String, Arc<ChannelState>>,
    channel_delta_settings: DashMap<String, PerChannelDeltaSettings>,
}

impl SocketDeltaState {
    fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn is_enabled_for_channel(&self, channel: &str) -> bool {
        if !self.enabled {
            return false;
        }

        if let Some(settings) = self.channel_delta_settings.get(channel) {
            return settings.enabled;
        }

        true
    }

    fn get_algorithm_for_channel(&self, channel: &str, default: DeltaAlgorithm) -> DeltaAlgorithm {
        self.channel_delta_settings
            .get(channel)
            .and_then(|s| s.algorithm)
            .unwrap_or(default)
    }

    fn set_channel_delta_settings(&self, channel: String, settings: PerChannelDeltaSettings) {
        self.channel_delta_settings.insert(channel, settings);
    }

    fn remove_channel_delta_settings(&self, channel: &str) {
        self.channel_delta_settings.remove(channel);
    }

    fn get_channel_state(&self, channel: &str) -> Option<Arc<ChannelState>> {
        self.channel_states.get(channel).map(|v| Arc::clone(&v))
    }

    fn set_channel_state(&self, channel: String, state: Arc<ChannelState>) {
        self.channel_states.insert(channel, state);
    }

    async fn cleanup_old_channels_and_states(&self, config: &DeltaCompressionConfig) {
        let entries: Vec<(String, Arc<ChannelState>)> = self
            .channel_states
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        for (_, state) in entries {
            state.cleanup_old_states(config).await;
        }

        self.channel_states
            .retain(|_, state| !state.conflation_groups.is_empty());

        if self.channel_states.len() > config.max_channel_states_per_socket {
            let mut channel_times = Vec::new();

            let entries: Vec<(String, Arc<ChannelState>)> = self
                .channel_states
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect();

            for (key, state) in entries {
                let mut max_time = None;
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
    socket_states: Arc<DashMap<SocketId, Arc<SocketDeltaState>>>,
    cluster_coordinator: Option<Arc<dyn ClusterCoordinator>>,
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

    /// Start the background cleanup task that periodically removes stale state.
    /// Call this once after creating the DeltaCompressionManager.
    pub async fn start_cleanup_task(self: &Arc<Self>) {
        let manager = Arc::clone(self);
        let cleanup_interval = manager.config.max_state_age;

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);

            loop {
                interval.tick().await;

                manager.cleanup().await;

                let stats = manager.get_stats();
                if stats.total_sockets > 0 {
                    tracing::debug!(
                        "Delta compression cleanup: {} total sockets, {} enabled, {} channel states",
                        stats.total_sockets,
                        stats.enabled_sockets,
                        stats.total_channel_states
                    );
                }
            }
        });

        let mut task_guard = self.cleanup_task_handle.lock().await;
        if let Some(old_handle) = task_guard.take() {
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

    /// Check cluster-wide delta interval and increment counter.
    /// Returns (should_send_full_message, current_count)
    pub async fn check_cluster_interval(
        &self,
        app_id: &str,
        channel: &str,
        conflation_key: &str,
    ) -> Result<(bool, u32)> {
        if let Some(coordinator) = &self.cluster_coordinator
            && self.config.cluster_coordination
        {
            return coordinator
                .increment_and_check(
                    app_id,
                    channel,
                    conflation_key,
                    self.config.full_message_interval,
                )
                .await;
        }
        Ok((false, 0))
    }

    /// Extract conflation key from message bytes using a specific path (public for broadcast optimization).
    /// Returns empty string if extraction fails.
    pub fn extract_conflation_key_from_path(&self, message_bytes: &[u8], key_path: &str) -> String {
        self.extract_conflation_key_with_path(message_bytes, key_path)
    }

    /// Get the conflation key path from config (public for broadcast optimization)
    pub fn get_conflation_key_path(&self) -> Option<&String> {
        self.config.conflation_key_path.as_ref()
    }

    /// Get the delta compression algorithm from config
    pub fn get_algorithm(&self) -> DeltaAlgorithm {
        self.config.algorithm
    }

    /// Extract conflation key from message bytes using a specific path.
    /// Returns empty string if extraction fails.
    fn extract_conflation_key_with_path(&self, message_bytes: &[u8], key_path: &str) -> String {
        let to_string = |v: &Value| {
            if let Some(s) = v.as_str() {
                s.to_string()
            } else if let Some(n) = v.as_number() {
                n.to_string()
            } else if let Some(b) = v.as_bool() {
                b.to_string()
            } else {
                format!("{}", v)
            }
        };

        let json_value: Value = match sonic_rs::from_slice(message_bytes) {
            Ok(v) => v,
            Err(_) => return String::new(),
        };

        let parts: Vec<&str> = key_path.split('.').collect();
        let mut current = &json_value;

        for part in &parts {
            current = match current.get(part) {
                Some(v) => v,
                None => {
                    if parts.len() == 1 {
                        if let Some(data_obj) = json_value.get("data").and_then(|v| v.as_object())
                            && let Some(v) = data_obj.get(part)
                        {
                            return to_string(v);
                        }
                        if let Some(data_str_val) = json_value.get("data")
                            && let Some(data_str) = data_str_val.as_str()
                            && let Ok(data_obj) = sonic_rs::from_str::<sonic_rs::Object>(data_str)
                            && let Some(v) = data_obj.get(part)
                        {
                            return to_string(v);
                        }
                    }

                    return String::new();
                }
            };
        }

        to_string(current)
    }

    /// Extract conflation key from message bytes based on globally configured path.
    /// Returns empty string if no conflation key is configured or extraction fails.
    #[allow(dead_code)]
    fn extract_conflation_key(&self, message_bytes: &[u8]) -> String {
        let key_path = match &self.config.conflation_key_path {
            Some(path) => path,
            None => return String::new(),
        };

        self.extract_conflation_key_with_path(message_bytes, key_path)
    }

    /// Enable delta compression for a specific socket
    pub fn enable_for_socket(&self, socket_id: &SocketId) {
        self.socket_states.insert(
            *socket_id,
            Arc::new(SocketDeltaState {
                enabled: true,
                channel_states: DashMap::new(),
                channel_delta_settings: DashMap::new(),
            }),
        );
    }

    /// Set per-channel delta settings for a socket (from subscription negotiation)
    pub fn set_channel_delta_settings(
        &self,
        socket_id: &SocketId,
        channel: &str,
        enabled: Option<bool>,
        algorithm: Option<DeltaAlgorithm>,
    ) {
        let socket_state = self.socket_states.entry(*socket_id).or_insert_with(|| {
            Arc::new(SocketDeltaState {
                enabled: true,
                channel_states: DashMap::new(),
                channel_delta_settings: DashMap::new(),
            })
        });

        let settings = PerChannelDeltaSettings {
            enabled: enabled.unwrap_or(true),
            algorithm,
        };

        socket_state.set_channel_delta_settings(channel.to_string(), settings);

        tracing::debug!(
            "Set per-channel delta settings for socket {} channel {}: enabled={:?}, algorithm={:?}",
            socket_id,
            channel,
            enabled,
            algorithm
        );
    }

    /// Check if a specific channel has per-subscription delta settings
    pub fn has_channel_delta_settings(&self, socket_id: &SocketId, channel: &str) -> bool {
        self.socket_states
            .get(socket_id)
            .map(|s| s.channel_delta_settings.contains_key(channel))
            .unwrap_or(false)
    }

    /// Get the algorithm to use for a specific socket and channel
    pub fn get_algorithm_for_channel(&self, socket_id: &SocketId, channel: &str) -> DeltaAlgorithm {
        self.socket_states
            .get(socket_id)
            .map(|s| s.get_algorithm_for_channel(channel, self.config.algorithm))
            .unwrap_or(self.config.algorithm)
    }

    /// Check if a socket has delta compression enabled
    pub fn is_enabled_for_socket(&self, socket_id: &SocketId) -> bool {
        if !self.config.enabled {
            return false;
        }

        self.socket_states
            .get(socket_id)
            .map(|state| state.is_enabled())
            .unwrap_or(false)
    }

    /// Check if delta compression is enabled for a specific socket and channel.
    /// This considers both global socket state and per-channel subscription settings.
    pub fn is_enabled_for_socket_channel(&self, socket_id: &SocketId, channel: &str) -> bool {
        if !self.config.enabled {
            return false;
        }

        if Self::is_encrypted_channel(channel) {
            return false;
        }

        self.socket_states
            .get(socket_id)
            .map(|state| state.is_enabled_for_channel(channel))
            .unwrap_or(false)
    }

    /// Remove socket state when client disconnects.
    /// This is critical for preventing memory leaks - must be called on every disconnect.
    pub fn remove_socket(&self, socket_id: &SocketId) {
        if self.socket_states.remove(socket_id).is_some() {
            tracing::debug!("Removed delta compression state for socket: {}", socket_id);
        }
    }

    /// Clear channel state for a socket when client unsubscribes.
    /// This ensures that when the client resubscribes, they get a fresh full message.
    ///
    /// IMPORTANT: This should be called BEFORE the actual unsubscription to prevent
    /// race conditions where a broadcast arrives between unsubscribe and clear.
    pub fn clear_channel_state(&self, socket_id: &SocketId, channel: &str) {
        if let Some(socket_state) = self.socket_states.get(socket_id) {
            let had_state = socket_state.channel_states.contains_key(channel);
            socket_state.channel_states.remove(channel);
            socket_state.remove_channel_delta_settings(channel);
            tracing::debug!(
                "clear_channel_state: socket={}, channel={}, had_state={}",
                socket_id,
                channel,
                had_state
            );
        }
    }

    /// Get the last message stored for a socket's channel (for broadcast optimization).
    /// Returns None if socket doesn't have delta enabled or no base message exists.
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

    /// Get the last message stored for a socket's channel WITH its sequence number.
    /// Returns (message_content, sequence) for use in precomputed delta paths.
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

    /// Get the next sequence number for a socket's channel/conflation_key.
    /// Returns 0 if the cache doesn't exist yet.
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

    /// Compute delta between two messages (broadcast-level, can be called once and reused).
    /// This avoids recomputing the same delta for multiple sockets.
    pub fn compute_delta_for_broadcast(
        &self,
        base_message: &[u8],
        new_message: &[u8],
    ) -> Result<Vec<u8>> {
        match self.config.algorithm {
            DeltaAlgorithm::Fossil => self.compute_fossil_delta(base_message, new_message),
            DeltaAlgorithm::Xdelta3 => self.compute_xdelta3_delta(base_message, new_message),
        }
    }

    /// Compress a message for a specific socket and channel with optional channel-specific settings.
    /// Returns either a delta-compressed message or the original message.
    ///
    /// IMPORTANT: This method does NOT store the message in the cache. After sending the message
    /// to the client, call store_sent_message() with the ACTUAL bytes that were sent.
    pub async fn compress_message(
        &self,
        socket_id: &SocketId,
        channel: &str,
        event_name: &str,
        message_bytes: &[u8],
        channel_settings: Option<&ChannelDeltaSettings>,
    ) -> Result<CompressionResult> {
        if !self.is_enabled_for_socket_channel(socket_id, channel) {
            return Ok(CompressionResult::Uncompressed);
        }

        if message_bytes.len() < self.config.min_message_size {
            return Ok(CompressionResult::Uncompressed);
        }

        let conflation_key_path = channel_settings
            .and_then(|s| s.conflation_key.as_ref())
            .or(self.config.conflation_key_path.as_ref());

        let conflation_key = if let Some(path) = conflation_key_path {
            self.extract_conflation_key_with_path(message_bytes, path)
        } else {
            String::new()
        };

        let cache_key = if conflation_key.is_empty() {
            event_name.to_string()
        } else {
            format!("{}:{}", event_name, conflation_key)
        };

        let max_messages_per_key = channel_settings
            .map(|s| s.max_messages_per_key)
            .unwrap_or(10);

        let socket_state = match self.socket_states.get(socket_id) {
            Some(state) => state,
            None => return Ok(CompressionResult::Uncompressed),
        };

        let channel_state = socket_state.get_channel_state(channel);
        let channel_state = match channel_state {
            Some(state) => state,
            None => {
                let new_channel_state = Arc::new(ChannelState::new());
                socket_state.set_channel_state(channel.to_string(), Arc::clone(&new_channel_state));
                new_channel_state
            }
        };

        let conflation_cache = channel_state.get_conflation_state(&cache_key);

        let conflation_key_opt = if conflation_key_path.is_some() && !conflation_key.is_empty() {
            Some(conflation_key.clone())
        } else {
            None
        };

        match conflation_cache {
            None => {
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

                let (last_msg, base_index) = match cache.get_last_message().await {
                    Some(msg) => {
                        let base_seq = msg.sequence as usize;
                        let base_content = &msg.content;
                        tracing::info!(
                            "compress_message: Using base for delta: base_seq={}, base_len={}, base_last50='{}', cache_key='{}'",
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

                let algorithm = self.get_algorithm_for_channel(socket_id, channel);

                let delta = match algorithm {
                    DeltaAlgorithm::Fossil => self.compute_fossil_delta(&last_msg, message_bytes),
                    DeltaAlgorithm::Xdelta3 => self.compute_xdelta3_delta(&last_msg, message_bytes),
                };

                let delta = match delta {
                    Ok(d) => d,
                    Err(_) => {
                        let sequence = cache.next_sequence.load(Ordering::Relaxed);
                        return Ok(CompressionResult::FullMessage {
                            sequence,
                            conflation_key: conflation_key_opt,
                        });
                    }
                };

                if delta.len() >= message_bytes.len() {
                    let sequence = cache.next_sequence.load(Ordering::Relaxed);
                    return Ok(CompressionResult::FullMessage {
                        sequence,
                        conflation_key: conflation_key_opt,
                    });
                }

                let sequence = cache.next_sequence.load(Ordering::Relaxed);

                Ok(CompressionResult::Delta {
                    delta,
                    sequence,
                    algorithm,
                    conflation_key: conflation_key_opt,
                    base_index,
                })
            }
        }
    }

    /// Store the actual message that was sent to the client.
    /// This must be called AFTER sending the message, with the exact bytes that were sent.
    pub async fn store_sent_message(
        &self,
        socket_id: &SocketId,
        channel: &str,
        event_name: &str,
        sent_message_bytes: Vec<u8>,
        is_full_message: bool,
        channel_settings: Option<&ChannelDeltaSettings>,
    ) -> Result<()> {
        let socket_state = match self.socket_states.get(socket_id) {
            Some(state) => state,
            None => return Ok(()),
        };

        let channel_state = match socket_state.get_channel_state(channel) {
            Some(state) => state,
            None => {
                if !is_full_message {
                    tracing::debug!(
                        "store_sent_message: Delta message but no channel_state for socket={}, channel={} (likely unsubscribed), skipping",
                        socket_id,
                        channel
                    );
                    return Ok(());
                }

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

        let conflation_key_path = channel_settings
            .and_then(|s| s.conflation_key.as_ref())
            .or(self.config.conflation_key_path.as_ref());

        let conflation_key = if let Some(path) = conflation_key_path {
            self.extract_conflation_key_with_path(&sent_message_bytes, path)
        } else {
            String::new()
        };

        let cache_key = if conflation_key.is_empty() {
            event_name.to_string()
        } else {
            format!("{}:{}", event_name, conflation_key)
        };

        let max_messages_per_key = channel_settings
            .map(|s| s.max_messages_per_key)
            .unwrap_or(10);

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

        cache.add_message(sent_message_bytes).await?;
        tracing::debug!(
            "store_sent_message: Added message to cache (cache_existed={}, socket={}, channel={}, cache_key='{}')",
            cache_existed,
            socket_id,
            channel,
            cache_key
        );

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

    /// Check if a channel is an encrypted channel (private-encrypted-*)
    #[inline]
    pub fn is_encrypted_channel(channel: &str) -> bool {
        channel.starts_with("private-encrypted-")
    }

    /// Compute delta using Fossil algorithm
    ///
    /// Note: The fossil_delta Rust crate's `delta(a, b)` produces a delta that when applied
    /// with `deltainv(x, d)` reconstructs `a` (the first argument). However, the JavaScript
    /// fossil-delta library's `applyDelta(base, delta)` expects the delta to transform
    /// `base` into the new value. To make the Rust-produced delta compatible with the JS
    /// client's `applyDelta(old, delta)` => new, we swap the arguments: `delta(new, old)`.
    fn compute_fossil_delta(&self, old_message: &[u8], new_message: &[u8]) -> Result<Vec<u8>> {
        let old_str = std::str::from_utf8(old_message)
            .map_err(|e| Error::Internal(format!("Invalid UTF-8 in old message: {}", e)))?;
        let new_str = std::str::from_utf8(new_message)
            .map_err(|e| Error::Internal(format!("Invalid UTF-8 in new message: {}", e)))?;

        // IMPORTANT: Swap arguments! The Rust crate's delta(a, b) produces a delta that
        // reconstructs `a` when applied. But JS applyDelta(old, d) expects to get `new`.
        // By calling delta(new, old), the JS client can do applyDelta(old, delta) => new.
        let delta = fossil_delta::delta(new_str, old_str);
        Ok(delta)
    }

    /// Compute delta using Xdelta3 algorithm
    fn compute_xdelta3_delta(&self, old_message: &[u8], new_message: &[u8]) -> Result<Vec<u8>> {
        let mut delta = Vec::new();
        oxidelta::compress::encoder::encode_all(
            &mut delta,
            old_message,
            new_message,
            oxidelta::compress::encoder::CompressOptions::default(),
        )
        .map_err(|e| Error::Internal(format!("Xdelta3 encoding failed: {e}")))?;
        Ok(delta)
    }

    /// Cleanup old states periodically
    pub async fn cleanup(&self) {
        let entries: Vec<(SocketId, Arc<SocketDeltaState>)> = self
            .socket_states
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();

        let mut cleaned_sockets = 0;
        let mut cleaned_channels = 0;

        for (socket_id, state) in entries {
            let channels_before = state.channel_states.len();

            state.cleanup_old_channels_and_states(&self.config).await;

            let channels_after = state.channel_states.len();
            cleaned_channels += channels_before.saturating_sub(channels_after);

            if state.channel_states.is_empty() && !state.is_enabled() {
                self.socket_states.remove(&socket_id);
                cleaned_sockets += 1;
            }
        }

        if cleaned_sockets > 0 || cleaned_channels > 0 {
            tracing::debug!(
                "Delta compression cleanup: removed {} socket states, {} channel states",
                cleaned_sockets,
                cleaned_channels
            );
        }
    }

    /// Get cached messages for a channel (for initial sync on subscription).
    /// Returns None if socket doesn't have delta enabled or channel not found.
    pub async fn get_channel_cache(
        &self,
        socket_id: &SocketId,
        channel: &str,
    ) -> Option<Vec<(String, Vec<CachedMessage>)>> {
        let socket_state = self.socket_states.get(socket_id)?;
        let channel_state = socket_state.get_channel_state(channel)?;

        let mut caches = Vec::new();
        let groups: Vec<(String, ConflationKeyCache)> = channel_state
            .conflation_groups
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        for (key, cache) in groups {
            caches.push((key, cache.get_all_messages().await));
        }

        Some(caches)
    }

    /// Get global channel cache for sending to new subscribers.
    /// This collects cache state from ANY socket that has state for this channel.
    pub async fn get_global_channel_cache_for_sync(
        &self,
        channel: &str,
    ) -> Option<Vec<(String, Vec<CachedMessage>)>> {
        let mut all_caches: AHashMap<String, Vec<CachedMessage>> = AHashMap::new();

        for socket_entry in self.socket_states.iter() {
            if let Some(channel_state) = socket_entry.value().get_channel_state(channel) {
                let groups: Vec<(String, ConflationKeyCache)> = channel_state
                    .conflation_groups
                    .iter()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect();

                for (key, cache) in groups {
                    let messages = cache.get_all_messages().await;

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

    /// Check if we should send a full message for a socket/channel/cache_key
    /// based on the full_message_interval configuration
    pub fn should_send_full_message(
        &self,
        socket_id: &SocketId,
        channel: &str,
        cache_key: &str,
    ) -> bool {
        use std::sync::atomic::Ordering;

        let socket_state = match self.socket_states.get(socket_id) {
            Some(state) => state,
            None => return false,
        };

        let channel_state = match socket_state.get_channel_state(channel) {
            Some(state) => state,
            None => return false,
        };

        let cache = match channel_state.get_conflation_state(cache_key) {
            Some(cache) => cache,
            None => return false,
        };

        let delta_count = cache.delta_count.load(Ordering::Relaxed);
        delta_count >= self.config.full_message_interval
    }

    /// Get statistics about delta compression
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

        DeltaCompressionStats {
            total_sockets,
            enabled_sockets,
            total_channel_states,
            total_conflation_groups,
        }
    }
}

/// Result of compression attempt
#[derive(Debug)]
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

    let mut data = sonic_rs::json!({
        "event": event,
        "delta": delta_base64,
        "seq": sequence,
    });

    if let Some(algo) = algorithm {
        let algo_str = match algo {
            DeltaAlgorithm::Fossil => "fossil",
            DeltaAlgorithm::Xdelta3 => "xdelta3",
        };
        data["algorithm"] = sonic_rs::json!(algo_str);
    }

    if let Some(key) = conflation_key {
        data["conflation_key"] = sonic_rs::json!(key);
    }
    if let Some(idx) = base_index {
        data["base_index"] = sonic_rs::json!(idx);
    }

    sonic_rs::json!({
        "event": "pusher:delta",
        "channel": channel,
        "data": data.to_string()
    })
}

/// Create a cache sync message to send to clients on subscription
pub fn create_cache_sync_message(
    channel: &str,
    conflation_key: Option<&str>,
    max_messages_per_key: usize,
    caches: Vec<(String, Vec<CachedMessage>)>,
) -> Value {
    let mut states = sonic_rs::Object::new();

    for (key, messages) in caches {
        let messages_json: Vec<Value> = messages
            .iter()
            .rev()
            .take(1)
            .map(|msg| {
                sonic_rs::json!({
                    "content": String::from_utf8_lossy(&msg.content),
                    "seq": msg.sequence
                })
            })
            .collect();

        if !messages_json.is_empty() {
            let client_key = if let Some(colon_pos) = key.find(':') {
                key[colon_pos + 1..].to_string()
            } else {
                key
            };
            states.insert(&client_key, sonic_rs::json!(messages_json));
        }
    }

    let mut data = sonic_rs::json!({
        "max_messages_per_key": max_messages_per_key,
        "states": states,
    });

    if let Some(key) = conflation_key {
        data["conflation_key"] = sonic_rs::json!(key);
    }

    sonic_rs::json!({
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
    sonic_rs::json!({
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
