// DeltaCompressionManager definition, lifecycle, cluster coordination, and
// per-socket/per-channel settings management.

use dashmap::DashMap;
use serde::Serialize;
use sockudo_core::delta_types::{ClusterCoordinator, DeltaAlgorithm, DeltaCompressionConfig};
use sockudo_core::error::Result;
use sockudo_core::websocket::SocketId;
use std::sync::Arc;

use crate::state::{PerChannelDeltaSettings, SocketDeltaState};

/// Main delta compression manager
pub struct DeltaCompressionManager {
    pub config: DeltaCompressionConfig,
    pub(crate) socket_states: Arc<DashMap<SocketId, Arc<SocketDeltaState>>>,
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

/// Statistics about delta compression usage
#[derive(Debug, Clone, Serialize)]
pub struct DeltaCompressionStats {
    pub total_sockets: usize,
    pub enabled_sockets: usize,
    pub total_channel_states: usize,
    pub total_conflation_groups: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

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

    // =========================================================================
    // ENCRYPTED CHANNEL DETECTION TESTS
    // =========================================================================

    #[tokio::test]
    async fn test_is_enabled_for_socket_channel_respects_encrypted() {
        let config = DeltaCompressionConfig::default();
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        manager.enable_for_socket(&socket_id);

        // Regular channels should be enabled
        assert!(manager.is_enabled_for_socket_channel(&socket_id, "private-chat"));
        assert!(manager.is_enabled_for_socket_channel(&socket_id, "presence-room"));
        assert!(manager.is_enabled_for_socket_channel(&socket_id, "public-channel"));

        // Encrypted channels should always return false
        assert!(!manager.is_enabled_for_socket_channel(&socket_id, "private-encrypted-chat"));
        assert!(!manager.is_enabled_for_socket_channel(&socket_id, "private-encrypted-secret"));
    }

    // =========================================================================
    // PER-SUBSCRIPTION DELTA SETTINGS TESTS
    // =========================================================================

    #[tokio::test]
    async fn test_per_channel_delta_settings_enable_disable() {
        let config = DeltaCompressionConfig::default();
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        // Enable globally first
        manager.enable_for_socket(&socket_id);
        assert!(manager.is_enabled_for_socket_channel(&socket_id, "channel-a"));
        assert!(manager.is_enabled_for_socket_channel(&socket_id, "channel-b"));

        // Disable for specific channel
        manager.set_channel_delta_settings(&socket_id, "channel-a", Some(false), None);

        assert!(!manager.is_enabled_for_socket_channel(&socket_id, "channel-a"));
        assert!(manager.is_enabled_for_socket_channel(&socket_id, "channel-b")); // unaffected
    }

    #[tokio::test]
    async fn wildcard_channel_delta_settings_apply_to_matching_channels() {
        let config = DeltaCompressionConfig::default();
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        manager.enable_for_socket(&socket_id);
        manager.set_channel_delta_settings(&socket_id, "ticker:*", Some(false), None);

        assert!(!manager.is_enabled_for_socket_channel(&socket_id, "ticker:BTC"));
        assert!(!manager.is_enabled_for_socket_channel(&socket_id, "ticker:ETH"));
        assert!(manager.is_enabled_for_socket_channel(&socket_id, "price:BTC"));
    }

    #[tokio::test]
    async fn wildcard_channel_delta_settings_apply_algorithm_override() {
        let config = DeltaCompressionConfig {
            algorithm: DeltaAlgorithm::Fossil,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        manager.enable_for_socket(&socket_id);
        manager.set_channel_delta_settings(
            &socket_id,
            "ticker:*",
            Some(true),
            Some(DeltaAlgorithm::Xdelta3),
        );

        assert_eq!(
            manager.get_algorithm_for_channel(&socket_id, "ticker:BTC"),
            DeltaAlgorithm::Xdelta3
        );
        assert_eq!(
            manager.get_algorithm_for_channel(&socket_id, "price:BTC"),
            DeltaAlgorithm::Fossil
        );
    }

    #[tokio::test]
    async fn test_per_channel_delta_settings_without_global_enable() {
        let config = DeltaCompressionConfig::default();
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        // Socket is NOT globally enabled
        assert!(!manager.is_enabled_for_socket(&socket_id));

        // Set per-channel settings - this should auto-enable the socket
        manager.set_channel_delta_settings(&socket_id, "channel-a", Some(true), None);

        // Now the channel should be enabled (and global socket state too)
        assert!(manager.is_enabled_for_socket_channel(&socket_id, "channel-a"));
    }

    #[tokio::test]
    async fn test_per_channel_algorithm_override() {
        let config = DeltaCompressionConfig {
            algorithm: DeltaAlgorithm::Fossil, // Global default
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        manager.enable_for_socket(&socket_id);

        // Default algorithm
        assert_eq!(
            manager.get_algorithm_for_channel(&socket_id, "channel-a"),
            DeltaAlgorithm::Fossil
        );

        // Override for specific channel
        manager.set_channel_delta_settings(
            &socket_id,
            "channel-a",
            Some(true),
            Some(DeltaAlgorithm::Xdelta3),
        );

        // Channel A uses Xdelta3
        assert_eq!(
            manager.get_algorithm_for_channel(&socket_id, "channel-a"),
            DeltaAlgorithm::Xdelta3
        );

        // Channel B still uses global default
        assert_eq!(
            manager.get_algorithm_for_channel(&socket_id, "channel-b"),
            DeltaAlgorithm::Fossil
        );
    }

    #[tokio::test]
    async fn test_has_channel_delta_settings() {
        let config = DeltaCompressionConfig::default();
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        manager.enable_for_socket(&socket_id);

        // No per-channel settings yet
        assert!(!manager.has_channel_delta_settings(&socket_id, "channel-a"));

        // Set per-channel settings
        manager.set_channel_delta_settings(&socket_id, "channel-a", Some(true), None);

        // Now it has settings
        assert!(manager.has_channel_delta_settings(&socket_id, "channel-a"));
        assert!(!manager.has_channel_delta_settings(&socket_id, "channel-b"));
    }

    #[tokio::test]
    async fn test_clear_channel_state_clears_delta_settings() {
        let config = DeltaCompressionConfig::default();
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        manager.enable_for_socket(&socket_id);

        // Set per-channel settings
        manager.set_channel_delta_settings(
            &socket_id,
            "channel-a",
            Some(false),
            Some(DeltaAlgorithm::Xdelta3),
        );

        assert!(manager.has_channel_delta_settings(&socket_id, "channel-a"));
        assert!(!manager.is_enabled_for_socket_channel(&socket_id, "channel-a"));

        // Clear channel state (simulates unsubscribe)
        manager.clear_channel_state(&socket_id, "channel-a");

        // Per-channel settings should be cleared
        assert!(!manager.has_channel_delta_settings(&socket_id, "channel-a"));

        // Should now fall back to global (enabled)
        assert!(manager.is_enabled_for_socket_channel(&socket_id, "channel-a"));
    }

    #[tokio::test]
    async fn test_per_channel_settings_ignored_for_encrypted_channels() {
        let config = DeltaCompressionConfig::default();
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        manager.enable_for_socket(&socket_id);

        // Try to enable delta for encrypted channel
        manager.set_channel_delta_settings(
            &socket_id,
            "private-encrypted-chat",
            Some(true),
            Some(DeltaAlgorithm::Fossil),
        );

        // Should still be disabled because is_enabled_for_socket_channel checks encrypted
        assert!(!manager.is_enabled_for_socket_channel(&socket_id, "private-encrypted-chat"));
    }

    // =========================================================================
    // GLOBAL DISABLED TESTS
    // =========================================================================

    #[tokio::test]
    async fn test_global_disabled_ignores_per_channel() {
        let config = DeltaCompressionConfig {
            enabled: false, // Globally disabled
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        // Even with per-socket enabled
        manager.enable_for_socket(&socket_id);

        // And per-channel enabled
        manager.set_channel_delta_settings(&socket_id, "channel", Some(true), None);

        // Should still be disabled because global config is disabled
        assert!(!manager.is_enabled_for_socket_channel(&socket_id, "channel"));
    }

    // =========================================================================
    // INTEGRATION TEST - FULL FLOW
    // =========================================================================

    #[tokio::test]
    async fn test_full_per_subscription_flow() {
        let config = DeltaCompressionConfig {
            algorithm: DeltaAlgorithm::Fossil,
            min_message_size: 20,
            full_message_interval: 5,
            ..Default::default()
        };
        let manager = DeltaCompressionManager::new(config);
        let socket_id = SocketId::new();

        // Step 1: Socket connects (no global enable yet)
        assert!(!manager.is_enabled_for_socket(&socket_id));

        // Step 2: Client subscribes to channel-a with delta: { enabled: true, algorithm: 'xdelta3' }
        manager.set_channel_delta_settings(
            &socket_id,
            "channel-a",
            Some(true),
            Some(DeltaAlgorithm::Xdelta3),
        );

        // Socket should now be enabled (auto-enabled by per-channel request)
        assert!(manager.is_enabled_for_socket_channel(&socket_id, "channel-a"));
        assert_eq!(
            manager.get_algorithm_for_channel(&socket_id, "channel-a"),
            DeltaAlgorithm::Xdelta3
        );

        // Step 3: Client subscribes to channel-b with delta: false
        manager.set_channel_delta_settings(&socket_id, "channel-b", Some(false), None);
        assert!(!manager.is_enabled_for_socket_channel(&socket_id, "channel-b"));

        // Step 4: Client subscribes to channel-c without delta settings (uses global)
        // No set_channel_delta_settings call needed
        assert!(manager.is_enabled_for_socket_channel(&socket_id, "channel-c"));
        assert_eq!(
            manager.get_algorithm_for_channel(&socket_id, "channel-c"),
            DeltaAlgorithm::Fossil // Global default
        );

        // Step 5: Client unsubscribes from channel-a
        manager.clear_channel_state(&socket_id, "channel-a");
        assert!(!manager.has_channel_delta_settings(&socket_id, "channel-a"));
        // Falls back to global enabled
        assert!(manager.is_enabled_for_socket_channel(&socket_id, "channel-a"));

        // Step 6: Client disconnects
        manager.remove_socket(&socket_id);
        assert!(!manager.is_enabled_for_socket(&socket_id));
    }
}
