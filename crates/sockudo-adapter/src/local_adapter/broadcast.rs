use super::LocalAdapter;
#[cfg(feature = "delta")]
use ahash::AHashMap as HashMap;
use bytes::Bytes;
use sockudo_core::error::{Error, Result};
use sockudo_core::namespace::Namespace;
use sockudo_core::websocket::{SocketId, WebSocketRef};
use sockudo_protocol::messages::PusherMessage;
use sockudo_protocol::versioned_messages::extract_runtime_action;
#[cfg(feature = "delta")]
use std::hash::{Hash, Hasher};
use std::sync::Arc;
#[cfg(feature = "tag-filtering")]
use std::sync::atomic::Ordering;
#[cfg(feature = "delta")]
use tracing::info;
use tracing::{debug, warn};

#[cfg(feature = "delta")]
fn hash_cached_base_message(base: &[u8]) -> u64 {
    let mut hasher = ahash::AHasher::default();
    base.hash(&mut hasher);
    hasher.finish()
}

#[cfg(feature = "delta")]
/// Parameters for sending a message to a socket with compression
#[allow(dead_code)]
struct SocketMessageParams<'a> {
    socket_ref: WebSocketRef,
    base_message: PusherMessage,
    base_message_bytes: Vec<u8>,
    channel: &'a str,
    event_name: &'a str,
    delta_compression: Arc<sockudo_delta::DeltaCompressionManager>,
    channel_settings: Option<&'a sockudo_delta::ChannelDeltaSettings>,
    tag_filtering_enabled: bool,
}

#[cfg(feature = "delta")]
/// Parameters for sending a message with pre-computed delta
#[allow(dead_code)]
struct PrecomputedDeltaParams<'a> {
    socket_ref: WebSocketRef,
    base_message: PusherMessage,
    base_message_bytes: Vec<u8>,
    channel: &'a str,
    event_name: &'a str,
    delta_compression: Arc<sockudo_delta::DeltaCompressionManager>,
    channel_settings: Option<&'a sockudo_delta::ChannelDeltaSettings>,
    tag_filtering_enabled: bool,
    /// Pre-computed delta bytes and the sequence number of the base message used to compute it
    precomputed_delta: Option<(Arc<Vec<u8>>, u32)>,
    conflation_key: String,
    /// Hash of the base message used to compute the precomputed delta (for verification)
    base_hash: u64,
}

impl LocalAdapter {
    pub(super) async fn send_protocol_messages_concurrent(
        &self,
        target_socket_refs: Vec<WebSocketRef>,
        message: PusherMessage,
    ) -> Vec<Result<()>> {
        use futures::stream::{self, StreamExt};

        let socket_count = target_socket_refs.len();
        let target_chunks = socket_count.div_ceil(self.max_concurrent).clamp(1, 8);
        let socket_chunk_size = (socket_count / target_chunks)
            .min(self.max_concurrent)
            .max(1);

        let mut results = Vec::with_capacity(socket_count);

        for socket_chunk in target_socket_refs.chunks(socket_chunk_size) {
            let chunk_size = socket_chunk.len();
            match self
                .broadcast_semaphore
                .acquire_many(chunk_size as u32)
                .await
            {
                Ok(_permits) => {
                    let chunk_vec: Vec<_> = socket_chunk.to_vec();
                    let chunk_results: Vec<Result<()>> = stream::iter(chunk_vec)
                        .map(|socket_ref| {
                            let msg = message.clone();
                            async move { socket_ref.send_message(&msg) }
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

    /// Send messages using chunked processing with semaphore-controlled concurrency
    pub(super) async fn send_messages_concurrent(
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

    #[cfg(feature = "delta")]
    /// Send messages with delta compression support using chunked processing
    pub(super) async fn send_messages_with_compression(
        &self,
        target_socket_refs: Vec<WebSocketRef>,
        base_message: PusherMessage,
        base_message_bytes: Vec<u8>,
        channel: &str,
        event_name: &str,
        compression: crate::connection_manager::CompressionParams<'_>,
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

            // Check if socket has delta enabled for this specific channel
            if delta_compression.is_enabled_for_socket_channel(socket_id, channel) {
                debug!(
                    "Socket {} has delta compression enabled for channel {}",
                    socket_id, channel
                );
                // Extract conflation key
                let conflation_key_path = channel_settings
                    .and_then(|s| s.conflation_key.as_ref())
                    .or(delta_compression.get_conflation_key_path());

                debug!(
                    "Conflation key path for socket {}: {:?}",
                    socket_id, conflation_key_path
                );

                let conflation_key = if let Some(path) = conflation_key_path {
                    let extracted = delta_compression
                        .extract_conflation_key_from_path(&base_message_bytes, path);
                    debug!(
                        "Extracted conflation key from path '{}': '{}' (base_message_bytes len={})",
                        path,
                        extracted,
                        base_message_bytes.len()
                    );
                    extracted
                } else {
                    debug!("No conflation key path configured");
                    String::new()
                };

                // Create composite cache key
                let cache_key = if conflation_key.is_empty() {
                    event_name.to_string()
                } else {
                    format!("{}:{}", event_name, conflation_key)
                };

                // Get base message for delta computation
                debug!(
                    "Looking for base message: socket={}, channel={}, cache_key='{}', event_name='{}'",
                    socket_id, channel, cache_key, event_name
                );
                let base_msg =
                    delta_compression.get_last_message_for_socket(socket_id, channel, &cache_key);
                let base_msg_opt = base_msg.await;
                debug!(
                    "Base message lookup result: socket={}, found={}",
                    socket_id,
                    base_msg_opt.is_some()
                );

                // Hash the base message to group sockets with same base
                let base_hash = if let Some(base) = base_msg_opt.as_ref() {
                    hash_cached_base_message(base.as_slice())
                } else {
                    0 // No base message = send full message
                };

                debug!(
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
                debug!(
                    "Socket {} does NOT have delta compression enabled",
                    socket_id
                );
                non_delta_sockets.push(socket_ref);
            }
        }

        // Pre-compute deltas for each unique base message
        // Stores (delta_bytes, base_sequence) - base_sequence is needed to tell client which message to use as base
        type PrecomputedDelta = Option<(Arc<Vec<u8>>, u32)>;
        let mut precomputed_deltas: HashMap<(String, u64), PrecomputedDelta> = HashMap::new();

        debug!(
            "Pre-computing deltas for {} socket groups ({} non-delta sockets)",
            socket_groups.len(),
            non_delta_sockets.len()
        );

        for ((conflation_key, base_hash), group_sockets) in &socket_groups {
            if *base_hash == 0 {
                // No base message, will send full message
                debug!(
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
                    delta_compression.should_send_full_message(socket_id, channel, &cache_key);

                if should_send_full {
                    debug!(
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
                    .get_last_message_with_sequence(socket_id, channel, &cache_key)
                    .await
                {
                    // Compute delta ONCE for this group
                    match delta_compression
                        .compute_delta_for_broadcast(&base_msg, &base_message_bytes)
                    {
                        Ok(delta) => {
                            // Check if delta is beneficial
                            if delta.len() < base_message_bytes.len() {
                                debug!(
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
                                debug!(
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
                            warn!(
                                "Group (conflation_key='{}', base_hash={}) delta computation FAILED: {}, sending FULL",
                                conflation_key, base_hash, e
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

    #[cfg(feature = "delta")]
    /// Send a message to a single socket with delta compression support
    ///
    /// # Arguments
    /// * `params` - Parameters for sending the message
    async fn send_to_socket_with_compression(params: SocketMessageParams<'_>) -> Result<()> {
        use sockudo_delta::CompressionResult;

        let SocketMessageParams {
            socket_ref,
            base_message,
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
            .is_enabled_for_socket_channel(socket_id, channel)
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
                    socket_id,
                    channel,
                    event_name,
                    &base_message_bytes,
                    channel_settings,
                )
                .await?;

            // Get the sequence number that will be used for this message
            let next_sequence = delta_compression.get_next_sequence(socket_id, channel, &cache_key);

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
            CompressionResult::Uncompressed => socket_ref.send_message(&base_message),
            CompressionResult::FullMessage {
                sequence,
                conflation_key,
            } => {
                // Store the message WITHOUT sequence/conflation_key for future delta compression
                // The client strips these fields before storing, so we must match that behavior
                info!(
                    "🔵 STORING FULL base message: seq={}, conflation_key={:?}, len={}, last50='{}'",
                    sequence,
                    conflation_key,
                    base_message_bytes.len(),
                    String::from_utf8_lossy(
                        &base_message_bytes[base_message_bytes.len().saturating_sub(50)..]
                    )
                );
                info!(
                    "🔵 SENDING FULL message: len={}, last100='{}'",
                    message_with_sequence.len(),
                    String::from_utf8_lossy(
                        &message_with_sequence[message_with_sequence.len().saturating_sub(100)..]
                    )
                );
                if let Err(e) = delta_compression
                    .store_sent_message(
                        socket_id,
                        channel,
                        event_name,
                        base_message_bytes.clone(),
                        true,
                        channel_settings,
                    )
                    .await
                {
                    warn!("Failed to store full message for delta state: {e}");
                }

                let mut full_message = base_message;
                full_message.delta_sequence = Some(sequence.into());
                full_message.delta_conflation_key = conflation_key;
                socket_ref.send_message(&full_message)
            }
            CompressionResult::Delta {
                delta,
                sequence,
                algorithm,
                conflation_key,
                base_index,
            } => {
                // Send delta message
                info!(
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
                    sockudo_delta::DeltaAlgorithm::Fossil => "fossil",
                    sockudo_delta::DeltaAlgorithm::Xdelta3 => "xdelta3",
                };

                // Create delta message data
                let mut delta_data = sonic_rs::json!({
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
                    delta_data["conflation_key"] = sonic_rs::Value::from(&key);
                }
                if let Some(idx) = base_index {
                    delta_data["base_index"] = sonic_rs::Value::from(idx as u64);
                }

                // Wrap in Pusher message format
                let mut pusher_msg = PusherMessage {
                    event: Some("pusher:delta".to_string()),
                    channel: Some(channel.to_string()),
                    data: Some(sockudo_protocol::messages::MessageData::Json(delta_data)),
                    name: None,
                    user_id: None,
                    tags: None,
                    sequence: None,
                    conflation_key: None,
                    message_id: None,
                    stream_id: None,
                    serial: None,
                    idempotency_key: None,
                    extras: None,
                    delta_sequence: None,
                    delta_conflation_key: None,
                };
                if socket_ref.protocol_version == sockudo_protocol::ProtocolVersion::V2 {
                    pusher_msg.rewrite_prefix(sockudo_protocol::ProtocolVersion::V2);
                }

                // Store the ORIGINAL message bytes (without sequence/conflation_key metadata) for future delta computation
                // The sequence changes every message, so it should not be part of the delta base
                if let Err(e) = delta_compression
                    .store_sent_message(
                        socket_id,
                        channel,
                        event_name,
                        base_message_bytes.clone(),
                        false,
                        channel_settings,
                    )
                    .await
                {
                    warn!("Failed to store delta base message state: {e}");
                }
                socket_ref.send_message(&pusher_msg)
            }
        }
    }

    #[cfg(feature = "delta")]
    /// Send a message to a single socket with pre-computed delta (broadcast optimization)
    ///
    /// This is used when delta has been computed once at the broadcast level and can be
    /// reused for multiple sockets with the same base message.
    async fn send_to_socket_with_precomputed_delta(
        params: PrecomputedDeltaParams<'_>,
    ) -> Result<()> {
        let PrecomputedDeltaParams {
            socket_ref,
            base_message,
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
        let sequence = delta_compression.get_next_sequence(socket_id, channel, &cache_key);

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
                .get_last_message_with_sequence(socket_id, channel, &cache_key)
                .await
        {
            // Verify this socket's base matches the group's base by hashing
            let socket_base_hash = hash_cached_base_message(socket_base.as_slice());

            // The base_hash from the grouping should match this socket's actual base hash
            // If they don't match, this socket has a different base message and we can't use the precomputed delta
            (socket_base_hash == base_hash, Some(base_seq))
        } else {
            (false, None)
        };

        if let Some((delta_bytes, _precomputed_base_seq)) = precomputed_delta.filter(|_| {
            sequence >= 2
                && can_use_precomputed
                && !delta_compression.should_send_full_message(socket_id, channel, &cache_key)
        }) {
            // Use the ACTUAL base message sequence from the stored message, not sequence - 1
            // This is critical for multi-node setups where sequences may not be contiguous
            let base_sequence =
                actual_base_sequence.unwrap_or(if sequence > 0 { sequence - 1 } else { 0 });
            // We have a pre-computed delta, use it
            // base_sequence is the actual sequence number of the message used to compute this delta
            debug!(
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
                sockudo_delta::DeltaAlgorithm::Fossil => "fossil",
                sockudo_delta::DeltaAlgorithm::Xdelta3 => "xdelta3",
            };

            // Use the ACTUAL base_sequence from when the delta was computed
            // NOT sequence - 1, which is incorrect when messages have been evicted or skipped
            let base_index = base_sequence as usize;

            // Create delta message data
            let mut delta_data = sonic_rs::json!({
                "event": event_name,
                "delta": base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    delta_bytes.as_ref(),
                ),
                "seq": sequence,
            });

            if !omit_algorithm {
                delta_data["algorithm"] = sonic_rs::json!(algorithm_str);
            }

            // Add conflation key and base index
            if !conflation_key.is_empty() {
                delta_data["conflation_key"] = sonic_rs::Value::from(&conflation_key);
            }
            delta_data["base_index"] = sonic_rs::Value::from(base_index as u64);

            // Wrap in Pusher message format
            let mut pusher_msg = PusherMessage {
                event: Some("pusher:delta".to_string()),
                channel: Some(channel.to_string()),
                data: Some(sockudo_protocol::messages::MessageData::Json(delta_data)),
                name: None,
                user_id: None,
                tags: None, // Tags are handled at broadcast level or stripped
                sequence: None,
                conflation_key: None,
                message_id: None,
                stream_id: None,
                serial: None,
                idempotency_key: None,
                extras: None,
                delta_sequence: None,
                delta_conflation_key: None,
            };
            if socket_ref.protocol_version == sockudo_protocol::ProtocolVersion::V2 {
                pusher_msg.rewrite_prefix(sockudo_protocol::ProtocolVersion::V2);
            }

            // CRITICAL: Store the NEW message WITHOUT metadata (base_message_bytes).
            // The client strips __delta_seq and __conflation_key before storing, so we must
            // store the same sanitized version for consistent delta computation.
            // base_message_bytes IS the new message - no reconstruction needed.
            let _ = delta_compression
                .store_sent_message(
                    socket_id,
                    channel,
                    event_name,
                    base_message_bytes.clone(),
                    false,
                    channel_settings,
                )
                .await;
            socket_ref.send_message(&pusher_msg)
        } else {
            // No delta available (first message or delta not beneficial), send full message
            debug!(
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
            let _msg_with_seq = if let Ok(base_str) = std::str::from_utf8(&base_message_bytes) {
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
                    socket_id,
                    channel,
                    event_name,
                    base_message_bytes.clone(),
                    true,
                    channel_settings,
                )
                .await
            {
                Ok(_) => {
                    debug!(
                        "Successfully stored FULL message for socket {} on channel {} (seq={}, will be base for future deltas)",
                        socket_id, channel, sequence
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to store FULL message for socket {} on channel {}: {}",
                        socket_id, channel, e
                    );
                }
            }

            let mut full_message = base_message;
            full_message.delta_sequence = Some(sequence.into());
            if !conflation_key.is_empty() {
                full_message.delta_conflation_key = Some(conflation_key);
            }
            socket_ref.send_message(&full_message)
        }
    }

    // Helper function to get or create namespace
    pub(super) async fn get_or_create_namespace(&self, app_id: &str) -> Arc<Namespace> {
        self.namespaces
            .entry(app_id.to_string())
            .or_insert_with(|| Arc::new(Namespace::new(app_id.to_string())))
            .clone()
    }

    pub(super) fn existing_namespace(&self, app_id: &str) -> Option<Arc<Namespace>> {
        self.namespaces
            .get(app_id)
            .map(|namespace| namespace.value().clone())
    }

    pub(super) fn should_assign_v2_message_id(message: &PusherMessage) -> bool {
        message.message_id.is_none() && !message.is_protocol_ping_or_pong()
    }

    pub(super) fn v1_compatible_message(message: &PusherMessage) -> Option<PusherMessage> {
        if extract_runtime_action(message).is_some() {
            return None;
        }

        let mut v1_message = message.clone();
        v1_message.serial = None;
        v1_message.message_id = None;
        v1_message.stream_id = None;
        v1_message.tags = None;
        v1_message.idempotency_key = None;
        v1_message.extras = None;
        Some(v1_message)
    }

    /// Send a message to V1 sockets (strips serial/message_id/tags, plain Pusher format).
    pub(super) async fn send_to_v1_sockets(
        &self,
        sockets: Vec<WebSocketRef>,
        message: &PusherMessage,
    ) -> Result<()> {
        if sockets.is_empty() {
            return Ok(());
        }
        let Some(v1_message) = Self::v1_compatible_message(message) else {
            return Ok(());
        };
        let v1_bytes = Bytes::from(
            sonic_rs::to_vec(&v1_message)
                .map_err(|e| Error::InvalidMessageFormat(format!("Serialization failed: {e}")))?,
        );
        Self::log_send_errors(self.send_messages_concurrent(sockets, v1_bytes).await);
        Ok(())
    }

    /// Log send errors (debug for closed connections, warn for others).
    pub(super) fn log_send_errors(results: Vec<Result<()>>) {
        for r in results {
            if let Err(e) = r {
                match &e {
                    Error::ConnectionClosed(_) => debug!("Send error: {}", e),
                    _ => warn!("Send error: {}", e),
                }
            }
        }
    }

    /// Apply tag filtering to V2 sockets. When the `tag-filtering` feature is
    /// disabled this is a no-op that returns the input unchanged.
    pub(super) fn filter_v2_sockets_in_place(
        &self,
        channel: &str,
        message: &PusherMessage,
        v2_sockets: &mut Vec<WebSocketRef>,
        except: Option<&SocketId>,
        namespace: &Namespace,
    ) {
        #[cfg(feature = "tag-filtering")]
        {
            crate::v2_broadcast::apply_tag_filter_in_place(
                &self.filter_index,
                self.tag_filtering_enabled.load(Ordering::Acquire),
                channel,
                message,
                v2_sockets,
                except,
                namespace,
            )
        }
        #[cfg(not(feature = "tag-filtering"))]
        {
            let _ = (channel, message, v2_sockets, except, namespace);
        }
    }

    /// Apply tag filtering to V2 sockets, also verifying protocol version on
    /// matched sockets (used by `send_with_compression` where the filter index
    /// may contain both V1 and V2 socket IDs).
    #[cfg(feature = "delta")]
    pub(super) fn filter_v2_sockets_strict_in_place(
        &self,
        channel: &str,
        message: &PusherMessage,
        v2_sockets: &mut Vec<WebSocketRef>,
        except: Option<&SocketId>,
        namespace: &Namespace,
    ) {
        #[cfg(feature = "tag-filtering")]
        {
            crate::v2_broadcast::apply_tag_filter_v2_only_in_place(
                &self.filter_index,
                self.tag_filtering_enabled.load(Ordering::Acquire),
                channel,
                message,
                v2_sockets,
                except,
                namespace,
            )
        }
        #[cfg(not(feature = "tag-filtering"))]
        {
            let _ = (channel, message, v2_sockets, except, namespace);
        }
    }

    /// Strip tags from message if tag inclusion is disabled for this channel.
    /// When the `tag-filtering` feature is disabled, returns the message unchanged.
    #[cfg(feature = "delta")]
    pub(super) fn maybe_strip_tags(
        &self,
        message: PusherMessage,
        _channel_settings: Option<&sockudo_delta::ChannelDeltaSettings>,
    ) -> PusherMessage {
        #[cfg(feature = "tag-filtering")]
        {
            let global_enable_tags = self.enable_tags_globally.load(Ordering::Acquire);
            let enable_tags =
                crate::v2_broadcast::should_enable_tags(_channel_settings, global_enable_tags);
            crate::v2_broadcast::strip_tags_if_disabled(message, enable_tags)
        }
        #[cfg(not(feature = "tag-filtering"))]
        {
            message
        }
    }

    pub(super) async fn split_rewind_gated_sockets_in_place(
        &self,
        channel: &str,
        message: &PusherMessage,
        sockets: &mut Vec<WebSocketRef>,
    ) {
        let mut index = 0;
        while index < sockets.len() {
            if sockets[index].buffer_rewind_message(channel, message).await {
                sockets.swap_remove(index);
            } else {
                index += 1;
            }
        }
    }

    // Updated to return WebSocketRef instead of Arc<Mutex<WebSocket>>
    pub async fn get_all_connections(&self, app_id: &str) -> Vec<SocketId> {
        self.existing_namespace(app_id)
            .map(|namespace| namespace.sockets.iter().map(|entry| *entry.key()).collect())
            .unwrap_or_default()
    }

    /// Fast-path channel join for LocalAdapter - atomic operation without locks
    /// Returns Some(connection_count) if successful, None if socket not found or already in channel
    /// Fast-path channel join for the local adapter.
    ///
    /// `activated` reports the first socket joining the channel. Do not re-derive
    /// it from `socket_count`.
    pub fn join_channel_fast(
        &self,
        app_id: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> Option<(usize, bool, bool)> {
        let t_start = std::time::Instant::now();

        // Get namespace (read-only operation on DashMap)
        let t_before_ns_get = t_start.elapsed().as_nanos();
        let namespace = self.namespaces.get(app_id)?;
        let t_after_ns_get = t_start.elapsed().as_nanos();

        // Check if socket exists
        let t_before_socket_check = t_start.elapsed().as_nanos();
        if !namespace.sockets.contains_key(socket_id) {
            debug!(
                "PERF[FAST_PATH_FAIL] channel={} socket={} reason=socket_not_found at={}ns",
                channel, socket_id, t_before_socket_check
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

            debug!(
                "PERF[FAST_PATH_ALREADY] channel={} socket={} total={}ns ns_get={}ns socket_check={}ns chan_check={}ns count={}ns",
                channel,
                socket_id,
                t_after_count,
                t_after_ns_get - t_before_ns_get,
                t_after_socket_check - t_before_socket_check,
                t_before_count - t_before_chan_check,
                t_after_count - t_before_count
            );
            return Some((count, false, false));
        }
        let t_after_chan_check = t_start.elapsed().as_nanos();

        // Atomically add socket to channel
        let t_before_add = t_start.elapsed().as_nanos();
        let (newly_subscribed, activated) = namespace.add_channel_to_socket(channel, socket_id);
        let t_after_add = t_start.elapsed().as_nanos();

        // Connection count is a separate read for the ack, not the gauge.
        let t_before_count = t_start.elapsed().as_nanos();
        let count = namespace.get_channel_socket_count(channel);
        let t_after_count = t_start.elapsed().as_nanos();

        debug!(
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

        Some((count, newly_subscribed, activated))
    }
}
