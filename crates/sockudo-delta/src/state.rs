// Per-socket and per-channel delta compression bookkeeping state.

use dashmap::DashMap;
use sockudo_core::delta_types::{DeltaAlgorithm, DeltaCompressionConfig};
use sockudo_core::error::Result;
use sockudo_core::utils::{is_wildcard_subscription_pattern, wildcard_pattern_matches};
use std::sync::Arc;
use std::time::Instant;

use crate::messages::CachedMessage;

/// Message cache for a single conflation key
/// Stores only the last message for delta compression
#[derive(Debug, Clone)]
pub(crate) struct ConflationKeyCache {
    last_message: Arc<tokio::sync::RwLock<Option<Arc<CachedMessage>>>>,
    pub(crate) next_sequence: Arc<std::sync::atomic::AtomicU32>,
    pub(crate) delta_count: Arc<std::sync::atomic::AtomicU32>,
}

impl ConflationKeyCache {
    pub(crate) fn new(_max_size: usize) -> Self {
        use std::sync::atomic::AtomicU32;

        Self {
            last_message: Arc::new(tokio::sync::RwLock::new(None)),
            next_sequence: Arc::new(AtomicU32::new(0)),
            delta_count: Arc::new(AtomicU32::new(0)),
        }
    }

    pub(crate) async fn add_message(&self, content: Vec<u8>) -> Result<()> {
        use std::sync::atomic::Ordering;

        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let msg = Arc::new(CachedMessage::new(content, sequence));

        *self.last_message.write().await = Some(msg);
        Ok(())
    }

    pub(crate) async fn get_last_message(&self) -> Option<Arc<CachedMessage>> {
        self.last_message.read().await.clone()
    }

    pub(crate) async fn should_send_full_message(&self, config: &DeltaCompressionConfig) -> bool {
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

    pub(crate) fn reset_delta_count(&mut self) {
        use std::sync::atomic::Ordering;
        self.delta_count.store(0, Ordering::Relaxed);
    }

    pub(crate) fn increment_delta_count(&mut self) {
        use std::sync::atomic::Ordering;
        self.delta_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) async fn get_all_messages(&self) -> Vec<CachedMessage> {
        if let Some(msg) = self.get_last_message().await {
            vec![msg.as_ref().clone()]
        } else {
            vec![]
        }
    }
}

/// State for a single channel (can track multiple conflation groups)
#[derive(Debug)]
pub(crate) struct ChannelState {
    pub(crate) conflation_groups: DashMap<String, ConflationKeyCache>,
}

impl ChannelState {
    pub(crate) fn new() -> Self {
        Self {
            conflation_groups: DashMap::new(),
        }
    }

    pub(crate) fn get_conflation_state(&self, conflation_key: &str) -> Option<ConflationKeyCache> {
        self.conflation_groups
            .get(conflation_key)
            .map(|v| v.clone())
    }

    pub(crate) async fn set_conflation_state(
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

        cache_times.sort_by_key(|a| a.1);

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
pub(crate) struct SocketDeltaState {
    pub(crate) enabled: bool,
    pub(crate) channel_states: DashMap<String, Arc<ChannelState>>,
    pub(crate) channel_delta_settings: DashMap<String, PerChannelDeltaSettings>,
}

impl SocketDeltaState {
    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn is_enabled_for_channel(&self, channel: &str) -> bool {
        if !self.enabled {
            return false;
        }

        if let Some(settings) = self.channel_delta_settings.get(channel) {
            tracing::debug!(
                channel,
                enabled = settings.enabled,
                "delta channel exact setting matched"
            );
            return settings.enabled;
        }

        for entry in self.channel_delta_settings.iter() {
            let subscribed_channel = entry.key();
            if is_wildcard_subscription_pattern(subscribed_channel)
                && wildcard_pattern_matches(channel, subscribed_channel)
            {
                let enabled = entry.value().enabled;
                tracing::debug!(channel, subscribed_channel = %subscribed_channel, enabled, "delta channel wildcard setting matched");
                return enabled;
            }
        }

        tracing::debug!(channel, "delta channel setting fell back to enabled");
        true
    }

    pub(crate) fn get_algorithm_for_channel(
        &self,
        channel: &str,
        default: DeltaAlgorithm,
    ) -> DeltaAlgorithm {
        if let Some(settings) = self.channel_delta_settings.get(channel)
            && let Some(algorithm) = settings.algorithm
        {
            return algorithm;
        }

        for entry in self.channel_delta_settings.iter() {
            let subscribed_channel = entry.key();
            if is_wildcard_subscription_pattern(subscribed_channel)
                && wildcard_pattern_matches(channel, subscribed_channel)
                && let Some(algorithm) = entry.algorithm
            {
                return algorithm;
            }
        }

        default
    }

    pub(crate) fn set_channel_delta_settings(
        &self,
        channel: String,
        settings: PerChannelDeltaSettings,
    ) {
        self.channel_delta_settings.insert(channel, settings);
    }

    pub(crate) fn remove_channel_delta_settings(&self, channel: &str) {
        self.channel_delta_settings.remove(channel);
    }

    pub(crate) fn get_channel_state(&self, channel: &str) -> Option<Arc<ChannelState>> {
        self.channel_states.get(channel).map(|v| Arc::clone(&v))
    }

    pub(crate) fn set_channel_state(&self, channel: String, state: Arc<ChannelState>) {
        self.channel_states.insert(channel, state);
    }

    pub(crate) async fn cleanup_old_channels_and_states(&self, config: &DeltaCompressionConfig) {
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

            channel_times.sort_by_key(|a| a.1);

            let to_remove = channel_times
                .len()
                .saturating_sub(config.max_channel_states_per_socket);
            for (channel, _) in channel_times.iter().take(to_remove) {
                self.channel_states.remove(channel);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    // =========================================================================
    // PER-CHANNEL DELTA SETTINGS STRUCT TESTS
    // =========================================================================

    #[test]
    fn test_per_channel_delta_settings_default() {
        let settings = PerChannelDeltaSettings::default();
        assert!(settings.enabled);
        assert!(settings.algorithm.is_none());
    }

    // =========================================================================
    // SOCKET STATE MANAGEMENT TESTS
    // =========================================================================

    #[tokio::test]
    async fn test_socket_delta_state_channel_enabled_check() {
        let state = SocketDeltaState {
            enabled: true,
            channel_states: DashMap::new(),
            channel_delta_settings: DashMap::new(),
        };

        // Default: follows global enabled state
        assert!(state.is_enabled_for_channel("any-channel"));

        // Explicitly disable a channel
        state.set_channel_delta_settings(
            "disabled-channel".to_string(),
            PerChannelDeltaSettings {
                enabled: false,
                algorithm: None,
            },
        );

        assert!(!state.is_enabled_for_channel("disabled-channel"));
        assert!(state.is_enabled_for_channel("other-channel"));
    }

    #[tokio::test]
    async fn test_socket_delta_state_algorithm_selection() {
        let state = SocketDeltaState {
            enabled: true,
            channel_states: DashMap::new(),
            channel_delta_settings: DashMap::new(),
        };

        let default_algo = DeltaAlgorithm::Fossil;

        // Default algorithm when no per-channel settings
        assert_eq!(
            state.get_algorithm_for_channel("channel", default_algo),
            DeltaAlgorithm::Fossil
        );

        // Set per-channel algorithm
        state.set_channel_delta_settings(
            "xdelta-channel".to_string(),
            PerChannelDeltaSettings {
                enabled: true,
                algorithm: Some(DeltaAlgorithm::Xdelta3),
            },
        );

        assert_eq!(
            state.get_algorithm_for_channel("xdelta-channel", default_algo),
            DeltaAlgorithm::Xdelta3
        );
        assert_eq!(
            state.get_algorithm_for_channel("other-channel", default_algo),
            DeltaAlgorithm::Fossil
        );
    }

    #[tokio::test]
    async fn test_remove_channel_delta_settings() {
        let state = SocketDeltaState {
            enabled: true,
            channel_states: DashMap::new(),
            channel_delta_settings: DashMap::new(),
        };

        state.set_channel_delta_settings(
            "channel".to_string(),
            PerChannelDeltaSettings {
                enabled: false,
                algorithm: Some(DeltaAlgorithm::Xdelta3),
            },
        );

        assert!(!state.is_enabled_for_channel("channel"));

        state.remove_channel_delta_settings("channel");

        // Back to default (enabled follows global)
        assert!(state.is_enabled_for_channel("channel"));
    }
}
