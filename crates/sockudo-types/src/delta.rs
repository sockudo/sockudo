use serde::{Deserialize, Serialize};

/// Delta compression algorithm selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DeltaAlgorithm {
    /// Fossil delta compression (fast, good for text).
    #[default]
    Fossil,
    /// Xdelta3 compression (VCDIFF/RFC 3284, better compression ratio).
    Xdelta3,
}

/// Channel-specific delta compression configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ChannelDeltaConfig {
    /// Simple string configuration for backward compatibility.
    Simple(ChannelDeltaSimple),
    /// Full configuration with conflation settings.
    Full(ChannelDeltaSettings),
}

/// Simple channel delta configuration (backward compatible).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChannelDeltaSimple {
    Inherit,
    Disabled,
    Fossil,
    Xdelta3,
}

/// Full channel delta compression settings with conflation support.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelDeltaSettings {
    /// Enable or disable delta compression for this channel.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Delta compression algorithm to use.
    #[serde(default)]
    pub algorithm: DeltaAlgorithm,
    /// JSON path to extract conflation key (e.g., "asset", "data.symbol").
    #[serde(default)]
    pub conflation_key: Option<String>,
    /// Maximum number of messages to cache per conflation key.
    #[serde(default = "default_max_messages_per_key")]
    pub max_messages_per_key: usize,
    /// Maximum number of conflation keys to track for this channel.
    #[serde(default = "default_max_conflation_keys")]
    pub max_conflation_keys: usize,
    /// Whether to include tags in the message.
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
