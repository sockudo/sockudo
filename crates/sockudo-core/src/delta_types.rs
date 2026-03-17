use crate::error::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;

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
            max_channel_states_per_socket: 100,
            min_compression_ratio: 0.9,
            max_conflation_states_per_channel: Some(100),
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
