use crate::app::App;
use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AppManagerConfig {
    pub driver: AppManagerDriver,
    pub array: ArrayConfig,
    pub cache: CacheSettings,
    pub scylladb: ScyllaDbSettings,
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
