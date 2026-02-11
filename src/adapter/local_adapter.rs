use crate::adapter::ConnectionManager;
use crate::app::manager::AppManager;
use crate::channel::PresenceMemberInfo;
use crate::error::{Error, Result};
use crate::filter::index::FilterIndex;
use crate::namespace::Namespace;
use crate::protocol::messages::PusherMessage;
use crate::websocket::{SocketId, WebSocketRef};
use ahash::AHashMap as HashMap;
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use sockudo_ws::axum_integration::WebSocketWriter;
use std::any::Any;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

pub struct LocalAdapter {
    pub namespaces: Arc<DashMap<String, Arc<Namespace>>>,
    pub buffer_multiplier_per_cpu: usize,
    pub max_concurrent: usize,
    // Global semaphore to limit total concurrent broadcast operations across all channels
    broadcast_semaphore: Arc<Semaphore>,
    // Delta compression manager for bandwidth optimization (OnceLock for lock-free reads after init)
    delta_compression: Arc<OnceLock<Arc<crate::delta_compression::DeltaCompressionManager>>>,
    // App manager for getting channel-specific delta settings (OnceLock for lock-free reads after init)
    app_manager: Arc<OnceLock<Arc<dyn AppManager + Send + Sync>>>,
    // Server options for checking if tag filtering is enabled (AtomicBool for lock-free access)
    tag_filtering_enabled: Arc<AtomicBool>,
    // Global setting for whether to include tags in messages (AtomicBool for lock-free access)
    enable_tags_globally: Arc<AtomicBool>,
    // Filter index for O(1) message-to-subscriber matching (avoids O(N) iteration on broadcast)
    filter_index: Arc<FilterIndex>,
}

// Manual Clone implementation that shares all inner data
impl Clone for LocalAdapter {
    fn clone(&self) -> Self {
        Self {
            namespaces: Arc::clone(&self.namespaces),
            buffer_multiplier_per_cpu: self.buffer_multiplier_per_cpu,
            max_concurrent: self.max_concurrent,
            broadcast_semaphore: Arc::clone(&self.broadcast_semaphore),
            delta_compression: Arc::clone(&self.delta_compression),
            app_manager: Arc::clone(&self.app_manager),
            tag_filtering_enabled: Arc::clone(&self.tag_filtering_enabled),
            enable_tags_globally: Arc::clone(&self.enable_tags_globally),
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

    /// Set whether tag filtering is enabled (lock-free atomic operation)
    pub fn set_tag_filtering_enabled(&self, enabled: bool) {
        self.tag_filtering_enabled.store(enabled, Ordering::Release);
    }

    /// Get whether tag filtering is enabled (lock-free atomic operation)
    pub fn is_tag_filtering_enabled(&self) -> bool {
        self.tag_filtering_enabled.load(Ordering::Acquire)
    }

    /// Set whether tags are included in messages globally (lock-free atomic operation)
    pub fn set_enable_tags_globally(&self, enabled: bool) {
        self.enable_tags_globally.store(enabled, Ordering::Release);
    }

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
            namespaces: Arc::new(DashMap::new()),
            buffer_multiplier_per_cpu: multiplier,
            max_concurrent,
            broadcast_semaphore: Arc::new(Semaphore::new(max_concurrent * 8)),
            delta_compression: Arc::new(OnceLock::new()),
            app_manager: Arc::new(OnceLock::new()),
            tag_filtering_enabled: Arc::new(AtomicBool::new(false)),
            enable_tags_globally: Arc::new(AtomicBool::new(true)), // Enabled by default
            filter_index: Arc::new(FilterIndex::new()),
        }
    }

    /// Get a reference to the filter index (for external registration of filters)
    pub fn get_filter_index(&self) -> Arc<FilterIndex> {
        Arc::clone(&self.filter_index)
    }

    /// Set delta compression manager and app manager for delta compression support
    /// Note: This can only be called once; subsequent calls will be ignored (OnceLock semantics)
    pub async fn set_delta_compression(
        &self,
        delta_compression: Arc<crate::delta_compression::DeltaCompressionManager>,
        app_manager: Arc<dyn AppManager + Send + Sync>,
    ) {
        // OnceLock::set returns Err if already set, which we ignore (first-write-wins)
        let _ = self.delta_compression.set(delta_compression);
        let _ = self.app_manager.set(app_manager);
    }

    /// Get the delta compression manager if available (lock-free read)
    #[inline]
    pub fn get_delta_compression(
        &self,
    ) -> Option<&Arc<crate::delta_compression::DeltaCompressionManager>> {
        self.delta_compression.get()
    }

    /// Get the app manager if available (lock-free read)
    #[inline]
    pub fn get_app_manager(&self) -> Option<&Arc<dyn AppManager + Send + Sync>> {
        self.app_manager.get()
    }
}

/// Parameters for sending a message to a socket with compression
#[allow(dead_code)]
struct SocketMessageParams<'a> {
    socket_ref: WebSocketRef,
    base_message: PusherMessage,
    base_message_bytes: Vec<u8>,
    channel: &'a str,
    event_name: &'a str,
    delta_compression: Arc<crate::delta_compression::DeltaCompressionManager>,
    channel_settings: Option<&'a crate::delta_compression::ChannelDeltaSettings>,
    tag_filtering_enabled: bool,
}

/// Parameters for sending a message with pre-computed delta
#[allow(dead_code)]
struct PrecomputedDeltaParams<'a> {
    socket_ref: WebSocketRef,
    base_message: PusherMessage,
    base_message_bytes: Vec<u8>,
    channel: &'a str,
    event_name: &'a str,
    delta_compression: Arc<crate::delta_compression::DeltaCompressionManager>,
    channel_settings: Option<&'a crate::delta_compression::ChannelDeltaSettings>,
    tag_filtering_enabled: bool,
    /// Pre-computed delta bytes and the sequence number of the base message used to compute it
    precomputed_delta: Option<(Arc<Vec<u8>>, u32)>,
    conflation_key: String,
    /// Hash of the base message used to compute the precomputed delta (for verification)
    base_hash: u64,
}

impl LocalAdapter {
    /// Send messages using chunked processing with semaphore-controlled concurrency
    async fn send_messages_concurrent(
        &self,
        target_socket_refs: Vec<WebSocketRef>,
        message_bytes: Bytes,
    ) -> Vec<Result<()>> {
        use futures::stream::{self, StreamExt};

        let socket_count = target_socket_refs.len();

        // Determine target number of chunks (1-8 based on socket count vs max concurrency)
        let target_chunks = socket_count.div_ceil(self.max_concurrent).clamp(1, 8);

        // Calculate socket chunk size based on socket count divided by target chunks
        // With a max of self.max_concurrent sockets per chunk (better utilization)
        let socket_chunk_size = (socket_count / target_chunks)
            .min(self.max_concurrent)
            .max(1);

        // Process chunks sequentially with controlled concurrency
        let mut results = Vec::with_capacity(socket_count);

        for socket_chunk in target_socket_refs.chunks(socket_chunk_size) {
            let chunk_size = socket_chunk.len();

            // Acquire permits for the entire chunk
            match self
                .broadcast_semaphore
                .acquire_many(chunk_size as u32)
                .await
            {
                Ok(_permits) => {
                    // Process sockets in this chunk using buffered unordered streaming
                    let chunk_vec: Vec<_> = socket_chunk.to_vec();
                    let chunk_results: Vec<Result<()>> = stream::iter(chunk_vec)
                        .map(|socket_ref| {
                            let bytes = message_bytes.clone();
                            async move { socket_ref.send_broadcast(bytes) }
                        })
                        .buffer_unordered(chunk_size)
                        .collect()
                        .await;

                    results.extend(chunk_results);
                }
                Err(_) => {
                    // Return errors for all sockets if semaphore fails
                    for _ in 0..chunk_size {
                        results.push(Err(Error::Connection(
                            "Broadcast semaphore unavailable".to_string(),
                        )));
                    }
                }
            }
        }

        results
    }

    /// Send messages with delta compression support using chunked processing
    async fn send_messages_with_compression(
        &self,
        target_socket_refs: Vec<WebSocketRef>,
        base_message: PusherMessage,
        base_message_bytes: Vec<u8>,
        channel: &str,
        event_name: &str,
        compression: crate::adapter::connection_manager::CompressionParams<'_>,
    ) -> Vec<Result<()>> {
        let delta_compression = compression.delta_compression;
        let channel_settings = compression.channel_settings;

        use futures::stream::{self, StreamExt};

        let socket_count = target_socket_refs.len();
        let tag_filtering = self.tag_filtering_enabled.load(Ordering::Acquire);

        // OPTIMIZATION: Pre-compute deltas at broadcast level to avoid recomputing for each socket
        // Strategy:
        // 1. Group sockets by their base message (for delta computation)
        // 2. Compute delta ONCE per unique base message
        // 3. Reuse the same delta for all sockets with that base

        // Group sockets by base message hash (for delta computation)
        // Key: (conflation_key, base_message_hash) -> list of sockets
        let mut socket_groups: HashMap<(String, u64), Vec<WebSocketRef>> = HashMap::new();
        let mut non_delta_sockets = Vec::new();

        // First pass: categorize sockets
        for socket_ref in target_socket_refs {
            let socket_id = socket_ref.get_socket_id_sync();

            // Check if socket has delta enabled
            if delta_compression.is_enabled_for_socket(&socket_id) {
                tracing::debug!(
                    "Socket {} has delta compression enabled for channel {}",
                    socket_id,
                    channel
                );
                // Extract conflation key
                let conflation_key_path = channel_settings
                    .and_then(|s| s.conflation_key.as_ref())
                    .or(delta_compression.get_conflation_key_path());

                tracing::debug!(
                    "Conflation key path for socket {}: {:?}",
                    socket_id,
                    conflation_key_path
                );

                let conflation_key = if let Some(path) = conflation_key_path {
                    let extracted = delta_compression
                        .extract_conflation_key_from_path(&base_message_bytes, path);
                    tracing::debug!(
                        "Extracted conflation key from path '{}': '{}' (base_message_bytes len={})",
                        path,
                        extracted,
                        base_message_bytes.len()
                    );
                    extracted
                } else {
                    tracing::debug!("No conflation key path configured");
                    String::new()
                };

                // Create composite cache key
                let cache_key = if conflation_key.is_empty() {
                    event_name.to_string()
                } else {
                    format!("{}:{}", event_name, conflation_key)
                };

                // Get base message for delta computation
                tracing::debug!(
                    "Looking for base message: socket={}, channel={}, cache_key='{}', event_name='{}'",
                    socket_id,
                    channel,
                    cache_key,
                    event_name
                );
                let base_msg =
                    delta_compression.get_last_message_for_socket(&socket_id, channel, &cache_key);
                let base_msg_opt = base_msg.await;
                tracing::debug!(
                    "Base message lookup result: socket={}, found={}",
                    socket_id,
                    base_msg_opt.is_some()
                );

                // Hash the base message to group sockets with same base
                let base_hash = if let Some(base) = base_msg_opt {
                    use std::hash::{Hash, Hasher};
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    base.as_ref().hash(&mut hasher);
                    hasher.finish()
                } else {
                    0 // No base message = send full message
                };

                tracing::debug!(
                    "Socket {} grouped with conflation_key='{}', base_hash={} (has_base={})",
                    socket_id,
                    conflation_key,
                    base_hash,
                    base_hash != 0
                );
                socket_groups
                    .entry((conflation_key, base_hash))
                    .or_default()
                    .push(socket_ref);
            } else {
                tracing::debug!(
                    "Socket {} does NOT have delta compression enabled",
                    socket_id
                );
                non_delta_sockets.push(socket_ref);
            }
        }

        // Pre-compute deltas for each unique base message
        // Stores (delta_bytes, base_sequence) - base_sequence is needed to tell client which message to use as base
        let mut precomputed_deltas: HashMap<(String, u64), Option<(Arc<Vec<u8>>, u32)>> =
            HashMap::new();

        tracing::debug!(
            "Pre-computing deltas for {} socket groups ({} non-delta sockets)",
            socket_groups.len(),
            non_delta_sockets.len()
        );

        for ((conflation_key, base_hash), group_sockets) in &socket_groups {
            if *base_hash == 0 {
                // No base message, will send full message
                tracing::debug!(
                    "Group (conflation_key='{}', base_hash=0) has {} sockets - will send FULL messages (first message)",
                    conflation_key,
                    group_sockets.len()
                );
                precomputed_deltas.insert((conflation_key.clone(), *base_hash), None);
                continue;
            }

            // Get the base message from the first socket in the group (they all have the same base)
            if let Some(first_socket) = group_sockets.first() {
                let socket_id = first_socket.get_socket_id_sync();
                let cache_key = if conflation_key.is_empty() {
                    event_name.to_string()
                } else {
                    format!("{}:{}", event_name, conflation_key)
                };

                // Check if we should send a full message due to interval
                let should_send_full =
                    delta_compression.should_send_full_message(&socket_id, channel, &cache_key);

                if should_send_full {
                    tracing::debug!(
                        "Group (conflation_key='{}', base_hash={}) has {} sockets - sending FULL message due to interval",
                        conflation_key,
                        base_hash,
                        group_sockets.len()
                    );
                    precomputed_deltas.insert((conflation_key.clone(), *base_hash), None);
                    continue;
                }

                // Use the new function that returns both message content AND sequence
                if let Some((base_msg, base_sequence)) = delta_compression
                    .get_last_message_with_sequence(&socket_id, channel, &cache_key)
                    .await
                {
                    // Compute delta ONCE for this group
                    match delta_compression
                        .compute_delta_for_broadcast(&base_msg, &base_message_bytes)
                    {
                        Ok(delta) => {
                            // Check if delta is beneficial
                            if delta.len() < base_message_bytes.len() {
                                tracing::debug!(
                                    "Group (conflation_key='{}', base_hash={}) has {} sockets - computed delta: {} bytes (vs {} bytes original, {:.1}% savings), base_seq={}",
                                    conflation_key,
                                    base_hash,
                                    group_sockets.len(),
                                    delta.len(),
                                    base_message_bytes.len(),
                                    (1.0 - (delta.len() as f64 / base_message_bytes.len() as f64))
                                        * 100.0,
                                    base_sequence
                                );
                                precomputed_deltas.insert(
                                    (conflation_key.clone(), *base_hash),
                                    Some((Arc::new(delta), base_sequence)),
                                );
                            } else {
                                // Delta not beneficial
                                tracing::debug!(
                                    "Group (conflation_key='{}', base_hash={}) has {} sockets - delta NOT beneficial ({} >= {} bytes), sending FULL",
                                    conflation_key,
                                    base_hash,
                                    group_sockets.len(),
                                    delta.len(),
                                    base_message_bytes.len()
                                );
                                precomputed_deltas
                                    .insert((conflation_key.clone(), *base_hash), None);
                            }
                        }
                        Err(e) => {
                            // Delta computation failed
                            tracing::warn!(
                                "Group (conflation_key='{}', base_hash={}) delta computation FAILED: {}, sending FULL",
                                conflation_key,
                                base_hash,
                                e
                            );
                            precomputed_deltas.insert((conflation_key.clone(), *base_hash), None);
                        }
                    }
                } else {
                    precomputed_deltas.insert((conflation_key.clone(), *base_hash), None);
                }
            }
        }

        // Determine target number of chunks (1-8 based on socket count vs max concurrency)
        let target_chunks = socket_count.div_ceil(self.max_concurrent).clamp(1, 8);

        // Calculate socket chunk size based on socket count divided by target chunks
        let socket_chunk_size = (socket_count / target_chunks)
            .min(self.max_concurrent)
            .max(1);

        // Process chunks sequentially with controlled concurrency
        let mut results = Vec::with_capacity(socket_count);

        // Process delta-enabled socket groups
        for ((conflation_key, base_hash), group_sockets) in socket_groups {
            let precomputed_delta = precomputed_deltas
                .get(&(conflation_key.clone(), base_hash))
                .and_then(|d| d.as_ref())
                .cloned();

            for socket_chunk in group_sockets.chunks(socket_chunk_size) {
                let chunk_size = socket_chunk.len();

                // Acquire permits for the entire chunk
                match self
                    .broadcast_semaphore
                    .acquire_many(chunk_size as u32)
                    .await
                {
                    Ok(_permits) => {
                        // Process sockets in this chunk using buffered unordered streaming
                        let chunk_vec: Vec<_> = socket_chunk.to_vec();
                        let base_msg = base_message.clone();
                        let base_bytes = base_message_bytes.clone();
                        let channel_str = channel.to_string();
                        let event_str = event_name.to_string();
                        let delta_comp = Arc::clone(&delta_compression);
                        let ch_settings = channel_settings.cloned();
                        let precomp_delta = precomputed_delta.clone();
                        let conf_key = conflation_key.clone();

                        let chunk_results: Vec<Result<()>> = stream::iter(chunk_vec)
                            .map(|socket_ref| {
                                let msg = base_msg.clone();
                                let bytes = base_bytes.clone();
                                let ch = channel_str.clone();
                                let ev = event_str.clone();
                                let dc = Arc::clone(&delta_comp);
                                let settings = ch_settings.clone();
                                let delta = precomp_delta.clone();
                                let ck = conf_key.clone();

                                async move {
                                    Self::send_to_socket_with_precomputed_delta(
                                        PrecomputedDeltaParams {
                                            socket_ref,
                                            base_message: msg,
                                            base_message_bytes: bytes,
                                            channel: &ch,
                                            event_name: &ev,
                                            delta_compression: dc,
                                            channel_settings: settings.as_ref(),
                                            tag_filtering_enabled: tag_filtering,
                                            precomputed_delta: delta,
                                            conflation_key: ck,
                                            base_hash,
                                        },
                                    )
                                    .await
                                }
                            })
                            .buffer_unordered(chunk_size)
                            .collect()
                            .await;

                        results.extend(chunk_results);
                    }
                    Err(_) => {
                        // Return errors for all sockets if semaphore fails
                        for _ in 0..chunk_size {
                            results.push(Err(Error::Connection(
                                "Broadcast semaphore unavailable".to_string(),
                            )));
                        }
                    }
                }
            }
        }

        // Process non-delta sockets
        for socket_chunk in non_delta_sockets.chunks(socket_chunk_size) {
            let chunk_size = socket_chunk.len();

            match self
                .broadcast_semaphore
                .acquire_many(chunk_size as u32)
                .await
            {
                Ok(_permits) => {
                    let chunk_vec: Vec<_> = socket_chunk.to_vec();
                    let base_msg = base_message.clone();
                    let base_bytes = base_message_bytes.clone();
                    let channel_str = channel.to_string();
                    let event_str = event_name.to_string();
                    let delta_comp = Arc::clone(&delta_compression);
                    let ch_settings = channel_settings.cloned();

                    let chunk_results: Vec<Result<()>> = stream::iter(chunk_vec)
                        .map(|socket_ref| {
                            let msg = base_msg.clone();
                            let bytes = base_bytes.clone();
                            let ch = channel_str.clone();
                            let ev = event_str.clone();
                            let dc = Arc::clone(&delta_comp);
                            let settings = ch_settings.clone();

                            async move {
                                Self::send_to_socket_with_compression(SocketMessageParams {
                                    socket_ref,
                                    base_message: msg,
                                    base_message_bytes: bytes,
                                    channel: &ch,
                                    event_name: &ev,
                                    delta_compression: dc,
                                    channel_settings: settings.as_ref(),
                                    tag_filtering_enabled: tag_filtering,
                                })
                                .await
                            }
                        })
                        .buffer_unordered(chunk_size)
                        .collect()
                        .await;

                    results.extend(chunk_results);
                }
                Err(_) => {
                    for _ in 0..chunk_size {
                        results.push(Err(Error::Connection(
                            "Broadcast semaphore unavailable".to_string(),
                        )));
                    }
                }
            }
        }

        results
    }

    /// Send a message to a single socket with delta compression support
    ///
    /// # Arguments
    /// * `params` - Parameters for sending the message
    async fn send_to_socket_with_compression(params: SocketMessageParams<'_>) -> Result<()> {
        use crate::delta_compression::CompressionResult;

        let SocketMessageParams {
            socket_ref,
            base_message: _,
            base_message_bytes,
            channel,
            event_name,
            delta_compression,
            channel_settings,
            tag_filtering_enabled: _,
        } = params;

        // Get socket ID and filter for this channel subscription (lock-free)
        let socket_id = socket_ref.get_socket_id_sync();

        // NOTE: Tag filtering already applied at broadcast level - no redundant check needed

        // Only process delta compression if it's enabled for this socket
        let (compression_result, message_with_sequence) = if !delta_compression
            .is_enabled_for_socket(&socket_id)
        {
            (CompressionResult::Uncompressed, base_message_bytes.clone())
        } else {
            // First, we need to determine what sequence number this message will have
            // and add it to the message BEFORE computing delta compression
            let conflation_key_path = channel_settings
                .and_then(|s| s.conflation_key.as_ref())
                .or(delta_compression.get_conflation_key_path());
            let conflation_key = if let Some(path) = conflation_key_path {
                delta_compression.extract_conflation_key_from_path(&base_message_bytes, path)
            } else {
                String::new()
            };

            // Create composite cache key: event_name:conflation_key
            let cache_key = if conflation_key.is_empty() {
                event_name.to_string()
            } else {
                format!("{}:{}", event_name, conflation_key)
            };

            // Compute compression WITHOUT sequence metadata
            // Sequence changes every message and should not be part of delta base
            let result = delta_compression
                .compress_message(
                    &socket_id,
                    channel,
                    event_name,
                    &base_message_bytes,
                    channel_settings,
                )
                .await?;

            // Get the sequence number that will be used for this message
            let next_sequence =
                delta_compression.get_next_sequence(&socket_id, channel, &cache_key);

            // Add sequence metadata to the message AFTER delta compression for sending to client
            // IMPORTANT: Use string manipulation instead of JSON parse/re-serialize
            // to preserve exact byte ordering. JSON re-serialization can change key order,
            // causing checksum mismatches when computing deltas.
            let msg_with_seq = if let Ok(base_str) = std::str::from_utf8(&base_message_bytes) {
                if let Some(last_brace) = base_str.rfind('}') {
                    let mut result = String::with_capacity(base_str.len() + 100);
                    result.push_str(&base_str[..last_brace]);
                    if last_brace > 1
                        && base_str[..last_brace]
                            .trim_end()
                            .ends_with(|c| c != '{' && c != ',')
                    {
                        result.push(',');
                    }
                    result.push_str(&format!("\"__delta_seq\":{}", next_sequence));
                    if !conflation_key.is_empty() {
                        result.push_str(&format!(",\"__conflation_key\":\"{}\"", conflation_key));
                    }
                    result.push('}');
                    result.into_bytes()
                } else {
                    base_message_bytes.clone()
                }
            } else {
                base_message_bytes.clone()
            };

            (result, msg_with_seq)
        };

        match compression_result {
            CompressionResult::Uncompressed => {
                // Send original message without compression
                socket_ref.send_broadcast(Bytes::from(base_message_bytes))
            }
            CompressionResult::FullMessage {
                sequence,
                conflation_key,
            } => {
                // Store the message WITHOUT sequence/conflation_key for future delta compression
                // The client strips these fields before storing, so we must match that behavior
                tracing::info!(
                    "🔵 STORING FULL base message: seq={}, conflation_key={:?}, len={}, last50='{}'",
                    sequence,
                    conflation_key,
                    base_message_bytes.len(),
                    String::from_utf8_lossy(
                        &base_message_bytes[base_message_bytes.len().saturating_sub(50)..]
                    )
                );
                tracing::info!(
                    "🔵 SENDING FULL message: len={}, last100='{}'",
                    message_with_sequence.len(),
                    String::from_utf8_lossy(
                        &message_with_sequence[message_with_sequence.len().saturating_sub(100)..]
                    )
                );
                let _ = delta_compression.store_sent_message(
                    &socket_id,
                    channel,
                    event_name,
                    base_message_bytes.clone(),
                    true,
                    channel_settings,
                );

                // Send the message with sequence metadata to client
                socket_ref.send_broadcast(Bytes::from(message_with_sequence))
            }
            CompressionResult::Delta {
                delta,
                sequence,
                algorithm,
                conflation_key,
                base_index,
            } => {
                // Send delta message
                tracing::info!(
                    "🔴 SENDING DELTA: seq={}, base_index={:?}, conflation_key={:?}, delta_len={}, new_msg_len={}, new_msg_last50='{}'",
                    sequence,
                    base_index,
                    conflation_key,
                    delta.len(),
                    base_message_bytes.len(),
                    String::from_utf8_lossy(
                        &base_message_bytes[base_message_bytes.len().saturating_sub(50)..]
                    )
                );
                let algorithm_str = match algorithm {
                    crate::delta_compression::DeltaAlgorithm::Fossil => "fossil",
                    crate::delta_compression::DeltaAlgorithm::Xdelta3 => "xdelta3",
                };

                // Create delta message data
                let mut delta_data = serde_json::json!({
                    "event": event_name,
                    "delta": base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        &delta,
                    ),
                    "seq": sequence,
                    "algorithm": algorithm_str,
                });

                // Add conflation key and base index if available
                if let Some(key) = conflation_key.clone() {
                    delta_data["conflation_key"] = serde_json::Value::String(key);
                }
                if let Some(idx) = base_index {
                    delta_data["base_index"] = serde_json::Value::Number(idx.into());
                }

                // Wrap in Pusher message format
                let pusher_msg = PusherMessage {
                    event: Some("pusher:delta".to_string()),
                    channel: Some(channel.to_string()),
                    data: Some(crate::protocol::messages::MessageData::Json(delta_data)),
                    name: None,
                    user_id: None,
                    tags: None,
                    sequence: None,
                    conflation_key: None,
                };

                let bytes = serde_json::to_vec(&pusher_msg).map_err(|e| {
                    Error::InvalidMessageFormat(format!("Serialization failed: {e}"))
                })?;

                // Store the ORIGINAL message bytes (without sequence/conflation_key metadata) for future delta computation
                // The sequence changes every message, so it should not be part of the delta base
                let _ = delta_compression.store_sent_message(
                    &socket_id,
                    channel,
                    event_name,
                    base_message_bytes.clone(),
                    false,
                    channel_settings,
                );

                socket_ref.send_broadcast(Bytes::from(bytes))
            }
        }
    }

    /// Send a message to a single socket with pre-computed delta (broadcast optimization)
    ///
    /// This is used when delta has been computed once at the broadcast level and can be
    /// reused for multiple sockets with the same base message.
    async fn send_to_socket_with_precomputed_delta(
        params: PrecomputedDeltaParams<'_>,
    ) -> Result<()> {
        let PrecomputedDeltaParams {
            socket_ref,
            base_message: _,
            base_message_bytes,
            channel,
            event_name,
            delta_compression,
            channel_settings,
            tag_filtering_enabled: _,
            precomputed_delta,
            conflation_key,
            base_hash,
        } = params;

        // Get socket ID (lock-free sync access)
        let socket_id = socket_ref.get_socket_id_sync();

        // NOTE: Tag filtering already applied at broadcast level - no redundant check needed

        // Create composite cache key
        let cache_key = if conflation_key.is_empty() {
            event_name.to_string()
        } else {
            format!("{}:{}", event_name, conflation_key)
        };

        // Get the sequence number for this socket
        let sequence = delta_compression.get_next_sequence(&socket_id, channel, &cache_key);

        // Check if we should send delta or full message
        // IMPORTANT: Even if we have a precomputed delta from other sockets, we must verify
        // that THIS socket has a base message (sequence > 0). If sequence == 0, this is the
        // first message for this cache_key on this socket, so we MUST send a full message.
        // This can happen after resubscribe when the socket's cache was cleared.
        //
        // SAFETY: Always send full messages for the first 2 messages (sequence 0 and 1).
        // This ensures the client has a solid base before we start sending deltas, avoiding
        // race conditions where the client's stored base might not match the server's.
        //
        // CRITICAL: We must also verify that THIS socket's base message matches the one used
        // to compute the precomputed delta. The base_hash grouping can have collisions or
        // group sockets incorrectly after resubscribe. We verify by checking if THIS socket's
        // last message content hash matches the precomputed delta's base.
        let (can_use_precomputed, actual_base_sequence) = if let Some((socket_base, base_seq)) =
            delta_compression
                .get_last_message_with_sequence(&socket_id, channel, &cache_key)
                .await
        {
            // Verify this socket's base matches the group's base by hashing
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            socket_base.as_ref().hash(&mut hasher);
            let socket_base_hash = hasher.finish();

            // The base_hash from the grouping should match this socket's actual base hash
            // If they don't match, this socket has a different base message and we can't use the precomputed delta
            (socket_base_hash == base_hash, Some(base_seq))
        } else {
            (false, None)
        };

        if let Some((delta_bytes, _precomputed_base_seq)) = precomputed_delta.filter(|_| {
            sequence >= 2
                && can_use_precomputed
                && !delta_compression.should_send_full_message(&socket_id, channel, &cache_key)
        }) {
            // Use the ACTUAL base message sequence from the stored message, not sequence - 1
            // This is critical for multi-node setups where sequences may not be contiguous
            let base_sequence =
                actual_base_sequence.unwrap_or(if sequence > 0 { sequence - 1 } else { 0 });
            // We have a pre-computed delta, use it
            // base_sequence is the actual sequence number of the message used to compute this delta
            tracing::debug!(
                "Sending DELTA message to socket {} on channel {} (seq={}, base_seq={}, delta_size={} bytes)",
                socket_id,
                channel,
                sequence,
                base_sequence,
                delta_bytes.len()
            );
            let algorithm = delta_compression.get_algorithm();
            let omit_algorithm = delta_compression.config.omit_delta_algorithm;

            let algorithm_str = match algorithm {
                crate::delta_compression::DeltaAlgorithm::Fossil => "fossil",
                crate::delta_compression::DeltaAlgorithm::Xdelta3 => "xdelta3",
            };

            // Use the ACTUAL base_sequence from when the delta was computed
            // NOT sequence - 1, which is incorrect when messages have been evicted or skipped
            let base_index = base_sequence as usize;

            // Create delta message data
            let mut delta_data = serde_json::json!({
                "event": event_name,
                "delta": base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    delta_bytes.as_ref(),
                ),
                "seq": sequence,
            });

            if !omit_algorithm {
                delta_data["algorithm"] = serde_json::json!(algorithm_str);
            }

            // Add conflation key and base index
            if !conflation_key.is_empty() {
                delta_data["conflation_key"] = serde_json::Value::String(conflation_key.clone());
            }
            delta_data["base_index"] = serde_json::Value::Number(base_index.into());

            // Wrap in Pusher message format
            let pusher_msg = PusherMessage {
                event: Some("pusher:delta".to_string()),
                channel: Some(channel.to_string()),
                data: Some(crate::protocol::messages::MessageData::Json(delta_data)),
                name: None,
                user_id: None,
                tags: None, // Tags are handled at broadcast level or stripped
                sequence: None,
                conflation_key: None,
            };

            let bytes = serde_json::to_vec(&pusher_msg)
                .map_err(|e| Error::InvalidMessageFormat(format!("Serialization failed: {e}")))?;

            // CRITICAL: Store the NEW message WITHOUT metadata (base_message_bytes).
            // The client strips __delta_seq and __conflation_key before storing, so we must
            // store the same sanitized version for consistent delta computation.
            // base_message_bytes IS the new message - no reconstruction needed.
            let _ = delta_compression
                .store_sent_message(
                    &socket_id,
                    channel,
                    event_name,
                    base_message_bytes.clone(),
                    false,
                    channel_settings,
                )
                .await;

            socket_ref.send_broadcast(Bytes::from(bytes))
        } else {
            // No delta available (first message or delta not beneficial), send full message
            tracing::debug!(
                "Sending FULL message to socket {} on channel {} (seq={}, size={} bytes)",
                socket_id,
                channel,
                sequence,
                base_message_bytes.len()
            );
            // Create message with metadata for sending to client
            // IMPORTANT: Use string manipulation instead of JSON parse/re-serialize
            // to preserve exact byte ordering. JSON re-serialization can change key order,
            // causing checksum mismatches when computing deltas.
            let msg_with_seq = if let Ok(base_str) = std::str::from_utf8(&base_message_bytes) {
                // Find the last '}' and inject metadata before it
                if let Some(last_brace) = base_str.rfind('}') {
                    let mut result = String::with_capacity(base_str.len() + 100);
                    result.push_str(&base_str[..last_brace]);
                    // Add comma if there's content before the brace
                    if last_brace > 1
                        && base_str[..last_brace]
                            .trim_end()
                            .ends_with(|c| c != '{' && c != ',')
                    {
                        result.push(',');
                    }
                    result.push_str(&format!("\"__delta_seq\":{}", sequence));
                    if !conflation_key.is_empty() {
                        result.push_str(&format!(",\"__conflation_key\":\"{}\"", conflation_key));
                    }
                    result.push('}');
                    result.into_bytes()
                } else {
                    base_message_bytes.clone()
                }
            } else {
                base_message_bytes.clone()
            };

            // CRITICAL: Store the message WITHOUT metadata (base_message_bytes).
            // The client strips __delta_seq and __conflation_key before storing, creating a
            // "sanitized" base. We must store the same sanitized version so that when we
            // compute deltas later, we use the same base the client has.
            // Previously we stored msg_with_seq (with metadata), causing delta decode failures
            // because the server's base had metadata but the client's base did not.
            match delta_compression
                .store_sent_message(
                    &socket_id,
                    channel,
                    event_name,
                    base_message_bytes.clone(),
                    true,
                    channel_settings,
                )
                .await
            {
                Ok(_) => {
                    tracing::debug!(
                        "Successfully stored FULL message for socket {} on channel {} (seq={}, will be base for future deltas)",
                        socket_id,
                        channel,
                        sequence
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to store FULL message for socket {} on channel {}: {}",
                        socket_id,
                        channel,
                        e
                    );
                }
            }

            // Send the full message
            socket_ref.send_broadcast(Bytes::from(msg_with_seq))
        }
    }

    // Helper function to get or create namespace
    async fn get_or_create_namespace(&self, app_id: &str) -> Arc<Namespace> {
        if !self.namespaces.contains_key(app_id) {
            let namespace = Arc::new(Namespace::new(app_id.to_string()));
            self.namespaces.insert(app_id.to_string(), namespace);
        }
        self.namespaces.get(app_id).unwrap().clone()
    }

    // Updated to return WebSocketRef instead of Arc<Mutex<WebSocket>>
    pub async fn get_all_connections(&self, app_id: &str) -> DashMap<SocketId, WebSocketRef> {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.sockets.clone()
    }

    /// Fast-path channel join for LocalAdapter - atomic operation without locks
    /// Returns Some(connection_count) if successful, None if socket not found or already in channel
    pub fn join_channel_fast(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<usize> {
        let t_start = std::time::Instant::now();

        // Get namespace (read-only operation on DashMap)
        let t_before_ns_get = t_start.elapsed().as_nanos();
        let namespace = self.namespaces.get(app_id)?;
        let t_after_ns_get = t_start.elapsed().as_nanos();

        // Check if socket exists
        let t_before_socket_check = t_start.elapsed().as_nanos();
        if !namespace.sockets.contains_key(socket_id) {
            tracing::debug!(
                "PERF[FAST_PATH_FAIL] channel={} socket={} reason=socket_not_found at={}ns",
                channel,
                socket_id,
                t_before_socket_check
            );
            return None;
        }
        let t_after_socket_check = t_start.elapsed().as_nanos();

        // Check if already in channel - if so, just return current count
        let t_before_chan_check = t_start.elapsed().as_nanos();
        if namespace.is_in_channel(channel, socket_id) {
            let t_before_count = t_start.elapsed().as_nanos();
            let count = namespace.get_channel_socket_count(channel);
            let t_after_count = t_start.elapsed().as_nanos();

            tracing::debug!(
                "PERF[FAST_PATH_ALREADY] channel={} socket={} total={}ns ns_get={}ns socket_check={}ns chan_check={}ns count={}ns",
                channel,
                socket_id,
                t_after_count,
                t_after_ns_get - t_before_ns_get,
                t_after_socket_check - t_before_socket_check,
                t_before_count - t_before_chan_check,
                t_after_count - t_before_count
            );
            return Some(count);
        }
        let t_after_chan_check = t_start.elapsed().as_nanos();

        // Atomically add socket to channel
        let t_before_add = t_start.elapsed().as_nanos();
        namespace.add_channel_to_socket(channel, socket_id);
        let t_after_add = t_start.elapsed().as_nanos();

        // Return the new connection count
        let t_before_count = t_start.elapsed().as_nanos();
        let count = namespace.get_channel_socket_count(channel);
        let t_after_count = t_start.elapsed().as_nanos();

        tracing::debug!(
            "PERF[FAST_PATH_NEW] channel={} socket={} total={}ns ns_get={}ns socket_check={}ns chan_check={}ns add={}ns count={}ns",
            channel,
            socket_id,
            t_after_count,
            t_after_ns_get - t_before_ns_get,
            t_after_socket_check - t_before_socket_check,
            t_after_chan_check - t_before_chan_check,
            t_after_add - t_before_add,
            t_after_count - t_before_count
        );

        Some(count)
    }
}

#[async_trait]
impl ConnectionManager for LocalAdapter {
    async fn init(&self) {
        info!("Initializing local adapter");
    }

    async fn get_namespace(&self, app_id: &str) -> Option<Arc<Namespace>> {
        Some(self.get_or_create_namespace(app_id).await)
    }

    async fn add_socket(
        &self,
        socket_id: SocketId,
        socket: WebSocketWriter,
        app_id: &str,
        app_manager: Arc<dyn AppManager + Send + Sync>,
        buffer_config: crate::websocket::WebSocketBufferConfig,
    ) -> Result<()> {
        tracing::debug!(
            "LocalAdapter::add_socket: adding socket {} for app {}",
            &socket_id,
            app_id
        );
        let namespace = self.get_or_create_namespace(app_id).await;
        let socket_id_clone = socket_id.clone();
        namespace
            .add_socket(socket_id, socket, app_manager, buffer_config)
            .await?;
        tracing::debug!(
            "LocalAdapter::add_socket: successfully added socket {} for app {}",
            socket_id_clone,
            app_id
        );
        Ok(())
    }

    // Updated to return WebSocketRef instead of Arc<Mutex<WebSocket>>
    async fn get_connection(&self, socket_id: &SocketId, app_id: &str) -> Option<WebSocketRef> {
        tracing::debug!(
            "LocalAdapter::get_connection: looking for socket {} in app {}",
            socket_id,
            app_id
        );
        let namespace = self.get_or_create_namespace(app_id).await;
        let result = namespace.get_connection(socket_id);
        tracing::debug!(
            "LocalAdapter::get_connection: socket {} in app {} found: {}",
            socket_id,
            app_id,
            result.is_some()
        );
        result
    }

    async fn remove_connection(&self, socket_id: &SocketId, app_id: &str) -> Result<()> {
        if let Some(namespace) = self.namespaces.get(app_id) {
            namespace.remove_connection(socket_id);
            Ok(())
        } else {
            Err(Error::Connection("Namespace not found".to_string()))
        }
    }

    // Updated to use WebSocketRef methods
    async fn send_message(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        message: PusherMessage,
    ) -> Result<()> {
        let connection = self
            .get_connection(socket_id, app_id)
            .await
            .ok_or_else(|| Error::Connection("Connection not found".to_string()))?;

        connection.send_message(&message).await
    }

    async fn send(
        &self,
        channel: &str,
        message: PusherMessage,
        except: Option<&SocketId>,
        app_id: &str,
        _start_time_ms: Option<f64>,
    ) -> Result<()> {
        debug!("Sending message to channel: {}", channel);
        debug!("Message: {:?}", message);

        // Check if delta compression is available (lock-free read via OnceLock)
        let delta_compression = self.delta_compression.get();
        let app_manager = self.app_manager.get();

        if let (Some(delta_compression), Some(app_manager)) = (delta_compression, app_manager) {
            // Get app config to check for channel-specific delta settings
            if let Ok(Some(app)) = app_manager.find_by_id(app_id).await {
                // Get channel-specific delta compression settings
                let channel_settings = app
                    .channel_delta_compression
                    .as_ref()
                    .and_then(|map| map.get(channel))
                    .and_then(|config| {
                        use crate::delta_compression::ChannelDeltaConfig;
                        match config {
                            ChannelDeltaConfig::Full(settings) => Some(settings.clone()),
                            _ => None,
                        }
                    });

                // Use compression-aware sending if we have settings with conflation key
                if channel_settings
                    .as_ref()
                    .and_then(|s| s.conflation_key.as_ref())
                    .is_some()
                {
                    return self
                        .send_with_compression(
                            channel,
                            message,
                            except,
                            app_id,
                            _start_time_ms,
                            crate::adapter::connection_manager::CompressionParams {
                                delta_compression: Arc::clone(delta_compression),
                                channel_settings: channel_settings.as_ref(),
                            },
                        )
                        .await;
                }
            }
        }

        // Fall back to regular sending without delta compression
        let serialized_message = serde_json::to_vec(&message)
            .map_err(|e| Error::InvalidMessageFormat(format!("Serialization failed: {e}")))?;
        let message_bytes = Bytes::from(serialized_message);

        let namespace = self.get_namespace(app_id).await.unwrap();

        // Get target socket references based on channel type
        let target_socket_refs = if channel.starts_with("#server-to-user-") {
            let user_id = channel.trim_start_matches("#server-to-user-");
            let socket_refs = namespace.get_user_sockets(user_id).await?;

            let mut target_refs = Vec::new();
            for socket_ref in socket_refs.iter() {
                let socket_id = socket_ref.get_socket_id_sync();
                if except != Some(socket_id) {
                    target_refs.push(socket_ref.clone());
                }
            }
            target_refs
        } else {
            namespace.get_channel_socket_refs_except(channel, except)
        };

        // Apply tag filtering using O(1) filter index lookup
        let tag_filtering_enabled = self.tag_filtering_enabled.load(Ordering::Acquire);
        let filtered_socket_refs = if tag_filtering_enabled && message.tags.is_some() {
            // Use filter index for O(1) lookup instead of O(N) iteration
            let tags = message.tags.as_ref().unwrap();

            // Convert HashMap to BTreeMap for lookup (required by FilterIndex)
            let tags_btree: std::collections::BTreeMap<String, String> =
                tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

            let lookup_result = self.filter_index.lookup(channel, &tags_btree);

            debug!(
                "FilterIndex lookup: channel={}, tags={:?}, indexed_matches={}, no_filter={}, needs_evaluation={}",
                channel,
                tags.keys().collect::<Vec<_>>(),
                lookup_result.indexed_matches.len(),
                lookup_result.no_filter.len(),
                lookup_result.needs_evaluation.len()
            );

            // Combine results: indexed matches + no_filter sockets (they receive all)
            // Skip complex filters for now (needs_evaluation) - they're rare
            let mut result = Vec::with_capacity(
                lookup_result.indexed_matches.len() + lookup_result.no_filter.len(),
            );

            // Resolve SocketIds to WebSocketRefs using namespace's socket registry
            // This is the key optimization: FilterIndex stores lightweight SocketId (16 bytes, Copy)
            // instead of heavy WebSocketRef (Arc clone), reducing allocation pressure

            // Add indexed matches (sockets whose filters match the message tags)
            for socket_id in lookup_result.indexed_matches {
                if except.is_some_and(|e| *e == socket_id) {
                    continue;
                }
                if let Some(socket_ref) = namespace.sockets.get(&socket_id) {
                    result.push(socket_ref.value().clone());
                }
            }

            // Add sockets with no filter (they receive all messages)
            for socket_id in lookup_result.no_filter {
                if except.is_some_and(|e| *e == socket_id) {
                    continue;
                }
                if let Some(socket_ref) = namespace.sockets.get(&socket_id) {
                    result.push(socket_ref.value().clone());
                }
            }

            // Handle complex filters by evaluating them (rare case)
            for socket_id in lookup_result.needs_evaluation {
                if except.is_some_and(|e| *e == socket_id) {
                    continue;
                }
                if let Some(socket_ref) = namespace.sockets.get(&socket_id) {
                    let socket_ref = socket_ref.value().clone();
                    let channel_filter = socket_ref.get_channel_filter_sync(channel);
                    if let Some(filter) = channel_filter {
                        if crate::filter::matches(&*filter, tags) {
                            result.push(socket_ref);
                        }
                    } else {
                        result.push(socket_ref); // No filter = receive all
                    }
                }
            }

            debug!(
                "FilterIndex result: {} sockets to receive message",
                result.len()
            );

            result
        } else if tag_filtering_enabled {
            // Tag filtering enabled but message has no tags - only no_filter sockets receive it
            let empty_tags = std::collections::BTreeMap::new();
            let lookup_result = self.filter_index.lookup(channel, &empty_tags);

            // Resolve SocketIds to WebSocketRefs
            lookup_result
                .no_filter
                .into_iter()
                .filter(|socket_id| except.map_or(true, |e| e != socket_id))
                .filter_map(|socket_id| {
                    namespace.sockets.get(&socket_id).map(|r| r.value().clone())
                })
                .collect()
        } else {
            // Tag filtering disabled - send to all sockets in channel
            target_socket_refs
        };

        // Send messages using concurrent tasks with semaphore-controlled concurrency
        let results = self
            .send_messages_concurrent(filtered_socket_refs, message_bytes)
            .await;

        // Handle any errors from concurrent messaging
        for send_result in results {
            if let Err(e) = send_result {
                match &e {
                    Error::ConnectionClosed(_) => {
                        debug!("Failed to send message to closed connection: {}", e);
                    }
                    _ => {
                        warn!("Failed to send message: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn send_with_compression(
        &self,
        channel: &str,
        message: PusherMessage,
        except: Option<&SocketId>,
        app_id: &str,
        _start_time_ms: Option<f64>,
        compression: crate::adapter::connection_manager::CompressionParams<'_>,
    ) -> Result<()> {
        let delta_compression = compression.delta_compression;
        let channel_settings = compression.channel_settings;
        debug!(
            "Sending message to channel with compression support: {}",
            channel
        );
        debug!("Message: {:?}", message);

        let namespace = self.get_namespace(app_id).await.unwrap();

        // Get target socket references based on channel type
        let target_socket_refs = if channel.starts_with("#server-to-user-") {
            let user_id = channel.trim_start_matches("#server-to-user-");
            let socket_refs = namespace.get_user_sockets(user_id).await?;

            let mut target_refs = Vec::new();
            for socket_ref in socket_refs.iter() {
                let socket_id = socket_ref.get_socket_id_sync();
                if except != Some(socket_id) {
                    target_refs.push(socket_ref.clone());
                }
            }
            target_refs
        } else {
            namespace.get_channel_socket_refs_except(channel, except)
        };

        // Apply tag filtering using O(1) filter index lookup
        let tag_filtering_enabled = self.tag_filtering_enabled.load(Ordering::Acquire);
        let filtered_socket_refs = if tag_filtering_enabled && message.tags.is_some() {
            // Use filter index for O(1) lookup instead of O(N) iteration
            let tags = message.tags.as_ref().unwrap();

            // Convert HashMap to BTreeMap for lookup (required by FilterIndex)
            let tags_btree: std::collections::BTreeMap<String, String> =
                tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

            let lookup_result = self.filter_index.lookup(channel, &tags_btree);

            // Combine results: indexed matches + no_filter sockets (they receive all)
            let mut result = Vec::with_capacity(
                lookup_result.indexed_matches.len() + lookup_result.no_filter.len(),
            );

            // Resolve SocketIds to WebSocketRefs using namespace's socket registry
            // This is the key optimization: FilterIndex stores lightweight SocketId (16 bytes, Copy)
            // instead of heavy WebSocketRef (Arc clone), reducing allocation pressure

            // Add indexed matches (sockets whose filters match the message tags)
            for socket_id in lookup_result.indexed_matches {
                if except.is_some_and(|e| *e == socket_id) {
                    continue;
                }
                if let Some(socket_ref) = namespace.sockets.get(&socket_id) {
                    result.push(socket_ref.value().clone());
                }
            }

            // Add sockets with no filter (they receive all messages)
            for socket_id in lookup_result.no_filter {
                if except.is_some_and(|e| *e == socket_id) {
                    continue;
                }
                if let Some(socket_ref) = namespace.sockets.get(&socket_id) {
                    result.push(socket_ref.value().clone());
                }
            }

            // Handle complex filters by evaluating them (rare case)
            for socket_id in lookup_result.needs_evaluation {
                if except.is_some_and(|e| *e == socket_id) {
                    continue;
                }
                if let Some(socket_ref) = namespace.sockets.get(&socket_id) {
                    let socket_ref = socket_ref.value().clone();
                    let channel_filter = socket_ref.get_channel_filter_sync(channel);
                    if let Some(filter) = channel_filter {
                        if crate::filter::matches(&*filter, tags) {
                            result.push(socket_ref);
                        }
                    } else {
                        result.push(socket_ref); // No filter = receive all
                    }
                }
            }

            result
        } else if tag_filtering_enabled {
            // Tag filtering enabled but message has no tags - only no_filter sockets receive it
            let empty_tags = std::collections::BTreeMap::new();
            let lookup_result = self.filter_index.lookup(channel, &empty_tags);

            // Resolve SocketIds to WebSocketRefs
            lookup_result
                .no_filter
                .into_iter()
                .filter(|socket_id| except.map_or(true, |e| e != socket_id))
                .filter_map(|socket_id| {
                    namespace.sockets.get(&socket_id).map(|r| r.value().clone())
                })
                .collect()
        } else {
            target_socket_refs
        };

        // Strip tags if disabled for this channel (bandwidth optimization)
        // We do this AFTER filtering logic (which needs tags) but BEFORE serialization
        // Priority: per-channel setting > global setting
        let global_enable_tags = self.enable_tags_globally.load(Ordering::Acquire);
        let enable_tags = channel_settings
            .map(|s| s.enable_tags)
            .unwrap_or(global_enable_tags);

        let message = if !enable_tags && message.tags.is_some() {
            let mut msg = message;
            msg.tags = None;
            msg
        } else {
            message
        };

        // Extract event name for delta compression before serialization
        let event_name = message.event.as_deref().unwrap_or("").to_string();

        // Serialize the base message once
        let base_message_bytes = serde_json::to_vec(&message)
            .map_err(|e| Error::InvalidMessageFormat(format!("Serialization failed: {e}")))?;

        // Process each socket with potential delta compression (already filtered)
        let results = self
            .send_messages_with_compression(
                filtered_socket_refs,
                message,
                base_message_bytes,
                channel,
                &event_name,
                crate::adapter::connection_manager::CompressionParams {
                    delta_compression,
                    channel_settings,
                },
            )
            .await;

        // Handle any errors from concurrent messaging
        for send_result in results {
            if let Err(e) = send_result {
                match &e {
                    Error::ConnectionClosed(_) => {
                        debug!("Failed to send message to closed connection: {}", e);
                    }
                    _ => {
                        warn!("Failed to send message: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn get_channel_members(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HashMap<String, PresenceMemberInfo>> {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.get_channel_members(channel).await
    }

    async fn get_channel_sockets(&self, app_id: &str, channel: &str) -> Result<Vec<SocketId>> {
        let namespace = self.get_or_create_namespace(app_id).await;
        Ok(namespace.get_channel_sockets(channel))
    }

    async fn remove_channel(&self, app_id: &str, channel: &str) {
        // MEMORY LEAK FIX: Clear filter index entries for this channel
        // This must happen before removing the channel from namespace
        self.filter_index.clear_channel(channel);

        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.remove_channel(channel);

        if namespace.sockets.is_empty()
            && namespace.channels.is_empty()
            && namespace.users.is_empty()
        {
            self.namespaces.remove(app_id);
            tracing::debug!(
                "Removed empty namespace for app_id: {} after channel removal",
                app_id
            );
        }
    }

    async fn is_in_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        let namespace = self.get_or_create_namespace(app_id).await;
        Ok(namespace.is_in_channel(channel, socket_id))
    }

    async fn get_user_sockets(&self, user_id: &str, app_id: &str) -> Result<Vec<WebSocketRef>> {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.get_user_sockets(user_id).await
    }

    async fn cleanup_connection(&self, app_id: &str, ws: WebSocketRef) {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.cleanup_connection(ws).await;

        if namespace.sockets.is_empty()
            && namespace.channels.is_empty()
            && namespace.users.is_empty()
        {
            self.namespaces.remove(app_id);
            tracing::debug!("Removed empty namespace for app_id: {}", app_id);
        }
    }

    async fn terminate_connection(&self, app_id: &str, user_id: &str) -> Result<()> {
        let namespace = self.get_or_create_namespace(app_id).await;
        if let Err(e) = namespace.terminate_user_connections(user_id).await {
            error!("Failed to terminate adapter: {}", e);
        }
        Ok(())
    }

    async fn add_channel_to_sockets(&self, app_id: &str, channel: &str, socket_id: &SocketId) {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.add_channel_to_socket(channel, socket_id);
    }

    async fn get_channel_socket_count(&self, app_id: &str, channel: &str) -> usize {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.get_channel_socket_count(channel)
    }

    async fn add_to_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        let t_start = std::time::Instant::now();
        let t_before_ns = t_start.elapsed().as_micros();
        let namespace = self.get_or_create_namespace(app_id).await;
        let t_after_ns = t_start.elapsed().as_micros();

        let t_before_add = t_start.elapsed().as_micros();
        let result = namespace.add_channel_to_socket(channel, socket_id);
        let t_after_add = t_start.elapsed().as_micros();

        tracing::debug!(
            "PERF[LOCAL_ADD_CHAN] channel={} socket={} total={}μs get_ns={}μs add={}μs",
            channel,
            socket_id,
            t_after_add,
            t_after_ns - t_before_ns,
            t_after_add - t_before_add
        );

        Ok(result)
    }

    async fn remove_from_channel(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Result<bool> {
        let namespace = self.get_or_create_namespace(app_id).await;

        // MEMORY LEAK FIX: Clean up filter index BEFORE removing from channel
        // Get the socket's filter for this channel so we can remove it from the index
        if let Some(socket_ref) = namespace.sockets.get(socket_id) {
            let filter_node = socket_ref.get_channel_filter_sync(channel);
            self.filter_index
                .remove_socket_filter(channel, *socket_id, filter_node.as_deref());
            // Also remove from the socket's channel_filters map
            socket_ref.channel_filters.remove(channel);
        }

        Ok(namespace.remove_channel_from_socket(channel, socket_id))
    }

    async fn get_presence_member(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<PresenceMemberInfo> {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.get_presence_member(channel, socket_id).await
    }

    async fn terminate_user_connections(&self, app_id: &str, user_id: &str) -> Result<()> {
        let namespace = self.get_or_create_namespace(app_id).await;
        if let Err(e) = namespace.terminate_user_connections(user_id).await {
            error!("Failed to terminate user connections: {}", e);
        }
        Ok(())
    }

    // Updated to use WebSocketRef
    async fn add_user(&self, ws_ref: WebSocketRef) -> Result<()> {
        // Get app_id using WebSocketRef async method
        let app_id = {
            let ws_guard = ws_ref.inner.lock().await;
            ws_guard.state.get_app_id()
        };
        let namespace = self.get_namespace(&app_id).await.unwrap();
        namespace.add_user(ws_ref).await
    }

    // Updated to use WebSocketRef
    async fn remove_user(&self, ws_ref: WebSocketRef) -> Result<()> {
        // Get app_id using WebSocketRef async method
        let app_id = {
            let ws_guard = ws_ref.inner.lock().await;
            ws_guard.state.get_app_id()
        };
        let namespace = self.get_namespace(&app_id).await.unwrap();
        namespace.remove_user(ws_ref).await
    }

    async fn remove_user_socket(
        &self,
        user_id: &str,
        socket_id: &SocketId,
        app_id: &str,
    ) -> Result<()> {
        let namespace = self.get_namespace(app_id).await.unwrap();
        namespace.remove_user_socket(user_id, socket_id).await
    }

    async fn count_user_connections_in_channel(
        &self,
        user_id: &str,
        app_id: &str,
        channel: &str,
        excluding_socket: Option<&SocketId>,
    ) -> Result<usize> {
        let namespace = self.get_namespace(app_id).await.unwrap();
        namespace
            .count_user_connections_in_channel(user_id, channel, excluding_socket)
            .await
    }

    async fn get_channels_with_socket_count(&self, app_id: &str) -> Result<HashMap<String, usize>> {
        let namespace = self.get_or_create_namespace(app_id).await;
        namespace.get_channels_with_socket_count().await
    }

    async fn get_sockets_count(&self, app_id: &str) -> Result<usize> {
        if let Some(namespace) = self.namespaces.get(app_id) {
            let count = namespace.sockets.len();
            Ok(count)
        } else {
            Ok(0) // No namespace means no sockets
        }
    }

    async fn get_namespaces(&self) -> Result<Vec<(String, Arc<Namespace>)>> {
        Ok(self
            .namespaces
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect())
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    async fn check_health(&self) -> Result<()> {
        // Local adapter is always healthy since it's in-memory
        Ok(())
    }

    fn get_node_id(&self) -> String {
        // Local adapter doesn't have a node ID concept (single node)
        "local".to_string()
    }

    fn as_horizontal_adapter(
        &self,
    ) -> Option<&dyn crate::adapter::connection_manager::HorizontalAdapterInterface> {
        // Local adapter doesn't support horizontal scaling
        None
    }
}
