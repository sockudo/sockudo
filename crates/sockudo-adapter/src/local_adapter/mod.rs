mod broadcast;
mod connection_manager;
mod helpers;

use self::helpers::{PendingPresenceMember, pending_presence_channel_key};
#[cfg(feature = "tag-filtering")]
use crate::filter_index::FilterIndex;
use compact_str::CompactString;
use dashmap::DashMap;
#[cfg(feature = "delta")]
use sockudo_core::app::AppManager;
use sockudo_core::namespace::Namespace;
use std::sync::Arc;
#[cfg(feature = "delta")]
use std::sync::OnceLock;
#[cfg(feature = "tag-filtering")]
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;
use tracing::info;

pub(super) type FastDashMap<K, V> = DashMap<K, V, ahash::RandomState>;

pub(super) fn fast_dashmap<K: Eq + std::hash::Hash, V>() -> FastDashMap<K, V> {
    DashMap::with_hasher(ahash::RandomState::new())
}

pub struct LocalAdapter {
    pub namespaces: Arc<FastDashMap<String, Arc<Namespace>>>,
    pending_presence_members: Arc<FastDashMap<CompactString, PendingPresenceMember>>,
    pending_presence_by_channel:
        Arc<FastDashMap<CompactString, Arc<FastDashMap<CompactString, ()>>>>,
    pub buffer_multiplier_per_cpu: usize,
    pub max_concurrent: usize,
    // Global semaphore to limit total concurrent broadcast operations across all channels
    broadcast_semaphore: Arc<Semaphore>,
    #[cfg(feature = "delta")]
    // Delta compression manager for bandwidth optimization (OnceLock for lock-free reads after init)
    delta_compression: Arc<OnceLock<Arc<sockudo_delta::DeltaCompressionManager>>>,
    #[cfg(feature = "delta")]
    // App manager for getting channel-specific delta settings (OnceLock for lock-free reads after init)
    app_manager: Arc<OnceLock<Arc<dyn AppManager + Send + Sync>>>,
    #[cfg(feature = "tag-filtering")]
    // Server options for checking if tag filtering is enabled (AtomicBool for lock-free access)
    tag_filtering_enabled: Arc<AtomicBool>,
    #[cfg(feature = "tag-filtering")]
    // Global setting for whether to include tags in messages (AtomicBool for lock-free access)
    enable_tags_globally: Arc<AtomicBool>,
    #[cfg(feature = "tag-filtering")]
    // Filter index for O(1) message-to-subscriber matching (avoids O(N) iteration on broadcast)
    filter_index: Arc<FilterIndex>,
}

// Manual Clone implementation that shares all inner data
impl Clone for LocalAdapter {
    fn clone(&self) -> Self {
        Self {
            namespaces: Arc::clone(&self.namespaces),
            pending_presence_members: Arc::clone(&self.pending_presence_members),
            pending_presence_by_channel: Arc::clone(&self.pending_presence_by_channel),
            buffer_multiplier_per_cpu: self.buffer_multiplier_per_cpu,
            max_concurrent: self.max_concurrent,
            broadcast_semaphore: Arc::clone(&self.broadcast_semaphore),
            #[cfg(feature = "delta")]
            delta_compression: Arc::clone(&self.delta_compression),
            #[cfg(feature = "delta")]
            app_manager: Arc::clone(&self.app_manager),
            #[cfg(feature = "tag-filtering")]
            tag_filtering_enabled: Arc::clone(&self.tag_filtering_enabled),
            #[cfg(feature = "tag-filtering")]
            enable_tags_globally: Arc::clone(&self.enable_tags_globally),
            #[cfg(feature = "tag-filtering")]
            filter_index: Arc::clone(&self.filter_index),
        }
    }
}

impl Default for LocalAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalAdapter {
    pub fn new() -> Self {
        Self::new_with_buffer_multiplier(64)
    }

    #[cfg(feature = "tag-filtering")]
    /// Set whether tag filtering is enabled (lock-free atomic operation)
    pub fn set_tag_filtering_enabled(&self, enabled: bool) {
        self.tag_filtering_enabled.store(enabled, Ordering::Release);
    }

    #[cfg(feature = "tag-filtering")]
    /// Get whether tag filtering is enabled (lock-free atomic operation)
    pub fn is_tag_filtering_enabled(&self) -> bool {
        self.tag_filtering_enabled.load(Ordering::Acquire)
    }

    #[cfg(feature = "tag-filtering")]
    /// Set whether tags are included in messages globally (lock-free atomic operation)
    pub fn set_enable_tags_globally(&self, enabled: bool) {
        self.enable_tags_globally.store(enabled, Ordering::Release);
    }

    #[cfg(feature = "tag-filtering")]
    /// Get whether tags are included in messages globally (lock-free atomic operation)
    pub fn get_enable_tags_globally(&self) -> bool {
        self.enable_tags_globally.load(Ordering::Acquire)
    }

    pub fn new_with_buffer_multiplier(multiplier: usize) -> Self {
        let cpu_cores = num_cpus::get();
        let max_concurrent = cpu_cores * multiplier;

        info!(
            "LocalAdapter initialized with {} CPU cores, buffer multiplier {}, max concurrent {}",
            cpu_cores, multiplier, max_concurrent
        );

        Self {
            namespaces: Arc::new(fast_dashmap()),
            pending_presence_members: Arc::new(fast_dashmap()),
            pending_presence_by_channel: Arc::new(fast_dashmap()),
            buffer_multiplier_per_cpu: multiplier,
            max_concurrent,
            broadcast_semaphore: Arc::new(Semaphore::new(max_concurrent * 8)),
            #[cfg(feature = "delta")]
            delta_compression: Arc::new(OnceLock::new()),
            #[cfg(feature = "delta")]
            app_manager: Arc::new(OnceLock::new()),
            #[cfg(feature = "tag-filtering")]
            tag_filtering_enabled: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "tag-filtering")]
            enable_tags_globally: Arc::new(AtomicBool::new(true)), // Enabled by default
            #[cfg(feature = "tag-filtering")]
            filter_index: Arc::new(FilterIndex::new()),
        }
    }

    #[cfg(feature = "tag-filtering")]
    /// Get a reference to the filter index (for external registration of filters)
    pub fn get_filter_index(&self) -> Arc<FilterIndex> {
        Arc::clone(&self.filter_index)
    }

    #[cfg(feature = "delta")]
    /// Set delta compression manager and app manager for delta compression support
    /// Note: This can only be called once; subsequent calls will be ignored (OnceLock semantics)
    pub async fn set_delta_compression(
        &self,
        delta_compression: Arc<sockudo_delta::DeltaCompressionManager>,
        app_manager: Arc<dyn AppManager + Send + Sync>,
    ) {
        // OnceLock::set returns Err if already set, which we ignore (first-write-wins)
        let _ = self.delta_compression.set(delta_compression);
        let _ = self.app_manager.set(app_manager);
    }

    #[cfg(feature = "delta")]
    /// Get the delta compression manager if available (lock-free read)
    #[inline]
    pub fn get_delta_compression(&self) -> Option<&Arc<sockudo_delta::DeltaCompressionManager>> {
        self.delta_compression.get()
    }

    #[cfg(feature = "delta")]
    /// Get the app manager if available (lock-free read)
    #[inline]
    pub fn get_app_manager(&self) -> Option<&Arc<dyn AppManager + Send + Sync>> {
        self.app_manager.get()
    }

    pub fn get_total_sockets_count(&self) -> usize {
        self.namespaces
            .iter()
            .map(|entry| entry.value().sockets.len())
            .sum()
    }

    fn remove_pending_presence_index_entry(&self, app_id: &str, channel: &str, pending_key: &str) {
        let channel_key = pending_presence_channel_key(app_id, channel);
        let should_remove_channel = self
            .pending_presence_by_channel
            .get(&channel_key)
            .is_some_and(|index| {
                index.remove(pending_key);
                index.is_empty()
            });
        if should_remove_channel {
            self.pending_presence_by_channel.remove(&channel_key);
        }
    }
}

#[cfg(test)]
mod tests;
