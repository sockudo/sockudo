use crate::app::App;
use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppManagerConfig {
    pub driver: AppManagerDriver,
    pub array: ArrayConfig,
    pub cache: CacheSettings,
    pub scylladb: ScyllaDbSettings,
    /// How often each node re-checks the apps of its live connections and
    /// closes sockets of apps that were disabled (4003) or removed (4001).
    /// The effective delay also includes the app cache TTL. `0` disables it.
    pub availability_check_interval_seconds: u64,
}

impl Default for AppManagerConfig {
    fn default() -> Self {
        Self {
            driver: AppManagerDriver::default(),
            array: ArrayConfig::default(),
            cache: CacheSettings::default(),
            scylladb: ScyllaDbSettings::default(),
            availability_check_interval_seconds: 15,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ArrayConfig {
    pub apps: Vec<App>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CacheSettings {
    pub enabled: bool,
    pub ttl: u64,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl: 300,
        }
    }
}
