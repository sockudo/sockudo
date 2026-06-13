use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ChannelLimits {
    pub max_name_length: u32,
    pub cache_ttl: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EventLimits {
    pub max_channels_at_once: u32,
    pub max_name_length: u32,
    pub max_payload_in_kb: u32,
    pub max_batch_size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PresenceConfig {
    pub max_members_per_channel: u32,
    pub max_member_size_in_kb: u32,
    pub update_rate_limit_per_member_per_second: u32,
    pub ungraceful_timeout_seconds: u64,
}

impl Default for ChannelLimits {
    fn default() -> Self {
        Self {
            max_name_length: 200,
            cache_ttl: 3600,
        }
    }
}

impl Default for EventLimits {
    fn default() -> Self {
        Self {
            max_channels_at_once: 100,
            max_name_length: 200,
            max_payload_in_kb: 100,
            max_batch_size: 10,
        }
    }
}

impl Default for PresenceConfig {
    fn default() -> Self {
        Self {
            max_members_per_channel: 100,
            max_member_size_in_kb: 2,
            update_rate_limit_per_member_per_second: 10,
            ungraceful_timeout_seconds: 0,
        }
    }
}
