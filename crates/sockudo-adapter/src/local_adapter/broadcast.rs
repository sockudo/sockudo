use super::LocalAdapter;
#[cfg(feature = "delta")]
use ahash::AHashMap as HashMap;
use bytes::Bytes;
use sockudo_core::error::{Error, Result};
use sockudo_core::namespace::Namespace;
use sockudo_core::websocket::{SocketId, WebSocketRef};
use sockudo_protocol::WireFormat;
use sockudo_protocol::messages::PusherMessage;
use sockudo_protocol::versioned_messages::{MessageAction, extract_runtime_action};
#[cfg(feature = "delta")]
use std::hash::{Hash, Hasher};
use std::sync::Arc;
#[cfg(feature = "tag-filtering")]
use std::sync::atomic::Ordering;
use tracing::{debug, warn};

#[cfg(feature = "delta")]
fn hash_cached_base_message(base: &[u8]) -> u64 {
    let mut hasher = ahash::AHasher::default();
    base.hash(&mut hasher);
    hasher.finish()
}

#[cfg(feature = "delta")]
/// Parameters for sending a message with pre-computed delta
#[allow(dead_code)]
struct PrecomputedDeltaParams {
    socket_ref: WebSocketRef,
    base_message: Arc<PusherMessage>,
    base_message_bytes: Arc<Vec<u8>>,
    channel: Arc<str>,
    event_name: Arc<str>,
    delta_compression: Arc<sockudo_delta::DeltaCompressionManager>,
    channel_settings: Option<Arc<sockudo_delta::ChannelDeltaSettings>>,
    tag_filtering_enabled: bool,
    /// Pre-computed delta bytes and the sequence number of the base message used to compute it
    precomputed_delta: Option<(Arc<Vec<u8>>, u32)>,
    conflation_key: Arc<str>,
    /// Hash of the base message used to compute the precomputed delta (for verification)
    base_hash: u64,
}

impl LocalAdapter {
    /// Send messages using chunked processing with semaphore-controlled concurrency
    pub(super) async fn send_messages_concurrent(
        &self,
        target_socket_refs: Vec<WebSocketRef>,
        message_bytes: Bytes,
        wire_format: WireFormat,
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
                            async move { socket_ref.send_broadcast_frame(bytes, wire_format) }
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
        prepared: crate::v2_broadcast::PreparedV2Message,
        channel: &str,
        event_name: &str,
        compression: crate::connection_manager::CompressionParams<'_>,
    ) -> Vec<Result<()>> {
        let crate::v2_broadcast::PreparedV2Message {
            message: base_message,
            payload: base_message_bytes,
            wire_format,
        } = prepared;
        let delta_compression = compression.delta_compression;
        let channel_settings = compression.channel_settings;

        // Shared fanout keeps the refcounted Bytes; the delta path materializes
        // an owned byte slice only when a delta-enabled socket exists.

        use futures::stream::{self, StreamExt};

        let socket_count = target_socket_refs.len();
        #[cfg(feature = "tag-filtering")]
        let tag_filtering = self.tag_filtering_enabled.load(Ordering::Acquire);
        #[cfg(not(feature = "tag-filtering"))]
        let tag_filtering = false;

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
            // Delta compression operates on JSON bytes only; binary wire formats
            // always take the shared-payload fanout below.
            if !wire_format.is_binary()
                && delta_compression.is_enabled_for_socket_channel(socket_id, channel)
            {
                debug!(socket_id = %socket_id, channel = %channel, "socket has delta compression enabled");
                // Extract conflation key
                let conflation_key_path = channel_settings
                    .and_then(|s| s.conflation_key.as_ref())
                    .or(delta_compression.get_conflation_key_path());

                debug!(socket_id = %socket_id, conflation_key_path = ?conflation_key_path, "conflation key path for socket");

                let conflation_key = if let Some(path) = conflation_key_path {
                    let extracted = delta_compression
                        .extract_conflation_key_from_path(base_message_bytes.as_ref(), path);
                    debug!(
                        path = %path,
                        conflation_key = %extracted,
                        base_bytes_len = base_message_bytes.len(),
                        "extracted conflation key from path"
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
                    socket_id = %socket_id,
                    channel = %channel,
                    cache_key = %cache_key,
                    event = %event_name,
                    "looking for base message"
                );
                let base_msg =
                    delta_compression.get_last_message_for_socket(socket_id, channel, &cache_key);
                let base_msg_opt = base_msg.await;
                debug!(socket_id = %socket_id, found = base_msg_opt.is_some(), "base message lookup result");

                // Hash the base message to group sockets with same base
                let base_hash = if let Some(base) = base_msg_opt.as_ref() {
                    hash_cached_base_message(base.as_slice())
                } else {
                    0 // No base message = send full message
                };

                debug!(
                    socket_id = %socket_id,
                    conflation_key = %conflation_key,
                    base_hash = base_hash,
                    has_base = base_hash != 0,
                    "socket grouped for delta"
                );
                socket_groups
                    .entry((conflation_key, base_hash))
                    .or_default()
                    .push(socket_ref);
            } else {
                debug!(socket_id = %socket_id, "socket does not have delta compression enabled");
                non_delta_sockets.push(socket_ref);
            }
        }

        // Pre-compute deltas for each unique base message
        // Stores (delta_bytes, base_sequence) - base_sequence is needed to tell client which message to use as base
        type PrecomputedDelta = Option<(Arc<Vec<u8>>, u32)>;
        let mut precomputed_deltas: HashMap<(String, u64), PrecomputedDelta> = HashMap::new();

        debug!(
            socket_groups = socket_groups.len(),
            non_delta_sockets = non_delta_sockets.len(),
            "pre-computing deltas for socket groups"
        );

        for ((conflation_key, base_hash), group_sockets) in &socket_groups {
            if *base_hash == 0 {
                // No base message, will send full message
                debug!(
                    conflation_key = %conflation_key,
                    socket_count = group_sockets.len(),
                    "group will send full messages (first message, no base)"
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
                        conflation_key = %conflation_key,
                        base_hash = base_hash,
                        socket_count = group_sockets.len(),
                        "group sending full message due to interval"
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
                        .compute_delta_for_broadcast(&base_msg, base_message_bytes.as_ref())
                    {
                        Ok(delta) => {
                            // Check if delta is beneficial
                            if delta.len() < base_message_bytes.len() {
                                debug!(
                                    conflation_key = %conflation_key,
                                    base_hash = base_hash,
                                    socket_count = group_sockets.len(),
                                    delta_bytes = delta.len(),
                                    original_bytes = base_message_bytes.len(),
                                    base_seq = base_sequence,
                                    "group computed delta successfully"
                                );
                                precomputed_deltas.insert(
                                    (conflation_key.clone(), *base_hash),
                                    Some((Arc::new(delta), base_sequence)),
                                );
                            } else {
                                // Delta not beneficial
                                debug!(
                                    conflation_key = %conflation_key,
                                    base_hash = base_hash,
                                    socket_count = group_sockets.len(),
                                    delta_bytes = delta.len(),
                                    original_bytes = base_message_bytes.len(),
                                    "group delta not beneficial, sending full"
                                );
                                precomputed_deltas
                                    .insert((conflation_key.clone(), *base_hash), None);
                            }
                        }
                        Err(e) => {
                            // Delta computation failed
                            warn!(
                                conflation_key = %conflation_key,
                                base_hash = base_hash,
                                error = %e,
                                "group delta computation failed, sending full"
                            );
                            precomputed_deltas.insert((conflation_key.clone(), *base_hash), None);
                        }
                    }
                } else {
                    precomputed_deltas.insert((conflation_key.clone(), *base_hash), None);
                }
            }
        }

        let base_message = Arc::new(base_message);
        let channel: Arc<str> = Arc::from(channel);
        let event_name: Arc<str> = Arc::from(event_name);
        let channel_settings = channel_settings.cloned().map(Arc::new);

        // Determine target number of chunks (1-8 based on socket count vs max concurrency)
        let target_chunks = socket_count.div_ceil(self.max_concurrent).clamp(1, 8);

        // Calculate socket chunk size based on socket count divided by target chunks
        let socket_chunk_size = (socket_count / target_chunks)
            .min(self.max_concurrent)
            .max(1);

        // Process chunks sequentially with controlled concurrency
        let mut results = Vec::with_capacity(socket_count);

        // Process delta-enabled socket groups. Only these sockets need an owned
        // payload copy for their cached base messages, so it is materialized
        // here and never for pure shared-payload fanout.
        let delta_bytes =
            (!socket_groups.is_empty()).then(|| Arc::new(base_message_bytes.to_vec()));
        if let Some(delta_bytes) = delta_bytes {
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
                            let base_msg = Arc::clone(&base_message);
                            let base_bytes = Arc::clone(&delta_bytes);
                            let channel_str = Arc::clone(&channel);
                            let event_str = Arc::clone(&event_name);
                            let delta_comp = Arc::clone(&delta_compression);
                            let ch_settings = channel_settings.clone();
                            let precomp_delta = precomputed_delta.clone();
                            let conf_key: Arc<str> = Arc::from(conflation_key.as_str());

                            let chunk_results: Vec<Result<()>> = stream::iter(chunk_vec)
                                .map(|socket_ref| {
                                    let msg = Arc::clone(&base_msg);
                                    let bytes = Arc::clone(&base_bytes);
                                    let ch = Arc::clone(&channel_str);
                                    let ev = Arc::clone(&event_str);
                                    let dc = Arc::clone(&delta_comp);
                                    let settings = ch_settings.clone();
                                    let delta = precomp_delta.clone();
                                    let ck = Arc::clone(&conf_key);

                                    async move {
                                        Self::send_to_socket_with_precomputed_delta(
                                            PrecomputedDeltaParams {
                                                socket_ref,
                                                base_message: msg,
                                                base_message_bytes: bytes,
                                                channel: ch,
                                                event_name: ev,
                                                delta_compression: dc,
                                                channel_settings: settings,
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
        }

        // Process non-delta sockets. Every socket in this group shares the same
        // pre-serialized payload: JSON goes out as a Text frame, binary wire
        // formats as a Binary frame.
        for socket_chunk in non_delta_sockets.chunks(socket_chunk_size) {
            let chunk_size = socket_chunk.len();

            match self
                .broadcast_semaphore
                .acquire_many(chunk_size as u32)
                .await
            {
                Ok(_permits) => {
                    let chunk_vec: Vec<_> = socket_chunk.to_vec();
                    let shared = base_message_bytes.clone();

                    let chunk_results: Vec<Result<()>> = stream::iter(chunk_vec)
                        .map(|socket_ref| {
                            let bytes = shared.clone();
                            async move { socket_ref.send_broadcast_frame(bytes, wire_format) }
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
    /// Send a message to a single socket with pre-computed delta (broadcast optimization)
    ///
    /// This is used when delta has been computed once at the broadcast level and can be
    /// reused for multiple sockets with the same base message.
    async fn send_to_socket_with_precomputed_delta(params: PrecomputedDeltaParams) -> Result<()> {
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
        let channel = channel.as_ref();
        let event_name = event_name.as_ref();
        let channel_settings = channel_settings.as_deref();

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
                socket_id = %socket_id,
                channel = %channel,
                sequence = sequence,
                base_seq = base_sequence,
                delta_bytes = delta_bytes.len(),
                "sending delta message to socket"
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
                delta_data["conflation_key"] = sonic_rs::Value::from(conflation_key.as_ref());
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
                .store_shared_sent_message(
                    socket_id,
                    channel,
                    event_name,
                    Arc::clone(&base_message_bytes),
                    false,
                    channel_settings,
                )
                .await;
            socket_ref.send_message(&pusher_msg)
        } else {
            // No delta available (first message or delta not beneficial), send full message
            debug!(
                socket_id = %socket_id,
                channel = %channel,
                sequence = sequence,
                size_bytes = base_message_bytes.len(),
                "sending full message to socket"
            );
            // CRITICAL: Store the message WITHOUT metadata (base_message_bytes).
            // The client strips __delta_seq and __conflation_key before storing, creating a
            // "sanitized" base. We must store the same sanitized version so that when we
            // compute deltas later, we use the same base the client has.
            // Previously we stored msg_with_seq (with metadata), causing delta decode failures
            // because the server's base had metadata but the client's base did not.
            match delta_compression
                .store_shared_sent_message(
                    socket_id,
                    channel,
                    event_name,
                    Arc::clone(&base_message_bytes),
                    true,
                    channel_settings,
                )
                .await
            {
                Ok(_) => {
                    debug!(
                        socket_id = %socket_id,
                        channel = %channel,
                        sequence = sequence,
                        "stored full message for future delta base"
                    );
                }
                Err(e) => {
                    warn!(
                        socket_id = %socket_id,
                        channel = %channel,
                        error = %e,
                        "failed to store full message"
                    );
                }
            }

            let mut full_message = base_message.as_ref().clone();
            full_message.delta_sequence = Some(sequence.into());
            if !conflation_key.is_empty() {
                full_message.delta_conflation_key = Some(conflation_key.to_string());
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
        let runtime_action = extract_runtime_action(message);
        match runtime_action {
            Some(MessageAction::Create) | None => {}
            Some(_) => return None,
        }

        let mut v1_message = message.clone();
        if runtime_action == Some(MessageAction::Create) {
            v1_message.rewrite_prefix(sockudo_protocol::ProtocolVersion::V1);
        }
        v1_message.serial = None;
        v1_message.message_id = None;
        v1_message.stream_id = None;
        v1_message.tags = None;
        v1_message.sequence = None;
        v1_message.conflation_key = None;
        v1_message.idempotency_key = None;
        v1_message.extras = None;
        v1_message.delta_sequence = None;
        v1_message.delta_conflation_key = None;
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
        Self::log_send_errors(
            self.send_messages_concurrent(sockets, v1_bytes, WireFormat::Json)
                .await,
        );
        Ok(())
    }

    /// Log send errors (debug for closed connections, warn for others).
    pub(super) fn log_send_errors(results: Vec<Result<()>>) {
        for r in results {
            if let Err(e) = r {
                match &e {
                    Error::ConnectionClosed(_) => debug!(error = %e, "send error"),
                    _ => warn!(error = %e, "send error"),
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
        let filtering_enabled = self.tag_filtering_enabled.load(Ordering::Acquire);
        #[cfg(not(feature = "tag-filtering"))]
        let filtering_enabled = false;
        let _ = (except, namespace);
        crate::v2_broadcast::apply_subscription_predicates_in_place(
            filtering_enabled,
            channel,
            message,
            v2_sockets,
        );
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
        let filtering_enabled = self.tag_filtering_enabled.load(Ordering::Acquire);
        #[cfg(not(feature = "tag-filtering"))]
        let filtering_enabled = false;
        let _ = (except, namespace);
        v2_sockets
            .retain(|socket| socket.protocol_version == sockudo_protocol::ProtocolVersion::V2);
        crate::v2_broadcast::apply_subscription_predicates_in_place(
            filtering_enabled,
            channel,
            message,
            v2_sockets,
        );
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
        let mut shared_message = None;
        let mut message_size = None;
        let mut index = 0;
        while index < sockets.len() {
            if !sockets[index].has_rewind_gate(channel) {
                index += 1;
                continue;
            }

            // A socket whose subscription view is installed can be predicate-checked now:
            // buffering nonmatching traffic would only burn bounded gate capacity and can
            // close the connection at overflow, for messages the drain would discard anyway.
            // A socket still between joining the channel and installing its view must buffer
            // everything; the gate drain applies the predicates once the view is in place.
            #[cfg(feature = "recovery")]
            let view_installed =
                crate::v2_broadcast::has_matching_subscription(&sockets[index], channel);
            #[cfg(not(feature = "recovery"))]
            let view_installed = sockets[index].channel_state.contains_key(channel);
            #[cfg(feature = "tag-filtering")]
            let filtering_enabled = self
                .tag_filtering_enabled
                .load(std::sync::atomic::Ordering::Acquire);
            #[cfg(not(feature = "tag-filtering"))]
            let filtering_enabled = false;
            if view_installed
                && !crate::v2_broadcast::socket_allows_message(
                    &sockets[index],
                    channel,
                    message,
                    filtering_enabled,
                )
            {
                sockets.swap_remove(index);
                continue;
            }

            let shared_message =
                Arc::clone(shared_message.get_or_insert_with(|| Arc::new(message.clone())));
            let message_size = *message_size.get_or_insert_with(|| {
                sonic_rs::to_vec(message).map_or(usize::MAX, |bytes| bytes.len())
            });
            match sockets[index]
                .buffer_rewind_message(channel, shared_message, message_size)
                .await
            {
                Ok(true) => {
                    sockets.swap_remove(index);
                }
                Ok(false) => {
                    // The drain won the race and closed the detached gate. Keep
                    // this socket in the live fanout set so the message is not lost.
                    index += 1;
                }
                Err(error) => {
                    warn!(
                        socket_id = %sockets[index].get_socket_id_sync(),
                        channel,
                        %error,
                        "attach gate overflowed; closing connection because continuity cannot be proven"
                    );
                    sockets[index].shutdown();
                    sockets.swap_remove(index);
                }
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
                channel = %channel,
                socket_id = %socket_id,
                elapsed_ns = t_before_socket_check,
                reason = "socket_not_found",
                "perf fast path failed"
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
                channel = %channel,
                socket_id = %socket_id,
                total_ns = t_after_count,
                ns_get_ns = t_after_ns_get - t_before_ns_get,
                socket_check_ns = t_after_socket_check - t_before_socket_check,
                chan_check_ns = t_before_count - t_before_chan_check,
                count_ns = t_after_count - t_before_count,
                "perf fast path already in channel"
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
            channel = %channel,
            socket_id = %socket_id,
            total_ns = t_after_count,
            ns_get_ns = t_after_ns_get - t_before_ns_get,
            socket_check_ns = t_after_socket_check - t_before_socket_check,
            chan_check_ns = t_after_chan_check - t_before_chan_check,
            add_ns = t_after_add - t_before_add,
            count_ns = t_after_count - t_before_count,
            "perf fast path new channel subscription"
        );

        Some((count, newly_subscribed, activated))
    }
}

#[cfg(all(test, feature = "delta"))]
mod wire_format_fanout_tests {
    use super::*;
    use crate::connection_manager::CompressionParams;
    use crate::local_adapter::tests::socket_pair::{create_ws_pair, next_frame, socket_ref};
    use sockudo_delta::{DeltaCompressionConfig, DeltaCompressionManager};
    use sockudo_protocol::{AppendMode, WireFormat, wire};

    fn disabled_delta() -> Arc<DeltaCompressionManager> {
        let config = DeltaCompressionConfig {
            enabled: false,
            ..Default::default()
        };
        Arc::new(DeltaCompressionManager::new(config))
    }

    async fn fanout_one(wire_format: WireFormat) -> (Bytes, PusherMessage) {
        let adapter = LocalAdapter::new();
        let (writer, mut client) = create_ws_pair().await;
        let socket = socket_ref(writer, wire_format, AppendMode::Full);

        let message = PusherMessage::channel_event(
            "test:event",
            "test-channel",
            sonic_rs::json!({ "payload": "x".repeat(1024) }),
        );
        let prepared = crate::v2_broadcast::prepare_v2_message(message, wire_format).unwrap();
        let expected_message = prepared.message.clone();
        let expected_payload = prepared.payload.clone();

        let results = adapter
            .send_messages_with_compression(
                vec![socket.clone()],
                prepared,
                "test-channel",
                "test:event",
                CompressionParams {
                    delta_compression: disabled_delta(),
                    channel_settings: None,
                    envelope: None,
                },
            )
            .await;
        assert!(results.iter().all(Result::is_ok), "{results:?}");

        let frame = next_frame(&mut client).await;
        match (&frame, wire_format) {
            (sockudo_ws::Message::Text(got), WireFormat::Json) => {
                assert_eq!(got, &expected_payload)
            }
            (sockudo_ws::Message::Binary(got), wf) => {
                assert!(wf.is_binary());
                assert_eq!(got, &expected_payload);
            }
            _ => panic!("unexpected frame for {wire_format:?}: {frame:?}"),
        }

        (expected_payload, expected_message)
    }

    #[tokio::test]
    async fn json_fanout_sends_shared_text_payload() {
        let (payload, prepared) = fanout_one(WireFormat::Json).await;
        let decoded = wire::deserialize_message(&payload, WireFormat::Json).unwrap();
        assert_eq!(decoded, prepared);
        // Pin the bytes to the serializer the delta caches and rewind gates
        // assume; a field-order change must fail here, not silently in prod.
        let legacy_bytes = Bytes::from(sonic_rs::to_vec(&prepared).unwrap());
        assert_eq!(payload, legacy_bytes);
    }

    #[tokio::test]
    async fn messagepack_fanout_sends_shared_binary_payload() {
        let (payload, prepared) = fanout_one(WireFormat::MessagePack).await;
        let decoded = wire::deserialize_message(&payload, WireFormat::MessagePack).unwrap();
        assert_eq!(decoded, prepared);
    }

    #[tokio::test]
    async fn protobuf_fanout_sends_shared_binary_payload() {
        let (payload, prepared) = fanout_one(WireFormat::Protobuf).await;
        let decoded = wire::deserialize_message(&payload, WireFormat::Protobuf).unwrap();
        assert_eq!(decoded, prepared);
    }

    async fn send_prepared_group(
        adapter: &LocalAdapter,
        message: &PusherMessage,
        sockets: Vec<WebSocketRef>,
        append_mode: AppendMode,
        wire_format: WireFormat,
    ) -> Bytes {
        let mode_message = crate::v2_broadcast::apply_append_mode(message.clone(), append_mode);
        let prepared = crate::v2_broadcast::prepare_v2_message(mode_message, wire_format).unwrap();
        let expected_payload = prepared.payload.clone();
        let results = adapter
            .send_messages_with_compression(
                sockets,
                prepared,
                "test-channel",
                "test:event",
                CompressionParams {
                    delta_compression: disabled_delta(),
                    channel_settings: None,
                    envelope: None,
                },
            )
            .await;
        assert!(results.iter().all(Result::is_ok), "{results:?}");
        expected_payload
    }

    #[tokio::test]
    async fn mixed_wire_format_groups_receive_their_own_encoding() {
        let adapter = LocalAdapter::new();
        let (json_writer, mut json_client) = create_ws_pair().await;
        let (msgpack_writer, mut msgpack_client) = create_ws_pair().await;

        let json_socket = socket_ref(json_writer, WireFormat::Json, AppendMode::Full);
        let msgpack_socket = socket_ref(msgpack_writer, WireFormat::MessagePack, AppendMode::Full);

        let message = PusherMessage::channel_event(
            "test:event",
            "test-channel",
            sonic_rs::json!({ "payload": "x".repeat(1024) }),
        );

        let groups =
            crate::local_adapter::connection_manager::partition_by_append_and_wire_format(vec![
                json_socket.clone(),
                msgpack_socket.clone(),
            ]);
        assert_eq!(groups.len(), 2, "one group per wire format");

        for ((append_mode, wire_format), sockets) in groups {
            let expected =
                send_prepared_group(&adapter, &message, sockets, append_mode, wire_format).await;

            if wire_format.is_binary() {
                match next_frame(&mut msgpack_client).await {
                    sockudo_ws::Message::Binary(got) => assert_eq!(got, expected),
                    other => panic!("expected binary frame for {wire_format:?}, got {other:?}"),
                }
            } else {
                match next_frame(&mut json_client).await {
                    sockudo_ws::Message::Text(got) => assert_eq!(got, expected),
                    other => panic!("expected text frame for {wire_format:?}, got {other:?}"),
                }
            }
        }
    }

    fn enabled_delta() -> Arc<DeltaCompressionManager> {
        let config = DeltaCompressionConfig {
            enabled: true,
            ..Default::default()
        };
        Arc::new(DeltaCompressionManager::new(config))
    }

    async fn send_one_with_delta(
        adapter: &LocalAdapter,
        socket: &WebSocketRef,
        manager: &Arc<DeltaCompressionManager>,
        wire_format: WireFormat,
        tick: usize,
    ) {
        let message = PusherMessage::channel_event(
            "test:event",
            "test-channel",
            sonic_rs::json!({ "payload": "x".repeat(1024), "tick": tick }),
        );
        let prepared = crate::v2_broadcast::prepare_v2_message(message, wire_format).unwrap();
        let results = adapter
            .send_messages_with_compression(
                vec![socket.clone()],
                prepared,
                "test-channel",
                "test:event",
                CompressionParams {
                    delta_compression: Arc::clone(manager),
                    channel_settings: None,
                    envelope: None,
                },
            )
            .await;
        assert!(results.iter().all(Result::is_ok), "{results:?}");
    }

    #[tokio::test]
    async fn delta_enabled_socket_still_receives_delta_frames() {
        let adapter = LocalAdapter::new();
        let (writer, mut client) = create_ws_pair().await;
        let socket = socket_ref(writer, WireFormat::Json, AppendMode::Full);
        let manager = enabled_delta();
        manager.enable_for_socket(&socket.socket_id);

        for tick in 0..3 {
            send_one_with_delta(&adapter, &socket, &manager, WireFormat::Json, tick).await;
            let sockudo_ws::Message::Text(bytes) = next_frame(&mut client).await else {
                panic!("delta fanout must stay on text frames");
            };
            let message: PusherMessage =
                wire::deserialize_message(&bytes, WireFormat::Json).unwrap();
            let event = message.event.unwrap_or_default();
            if tick < 2 {
                assert_eq!(event, "test:event", "first two messages must be full bases");
            } else {
                assert_eq!(event, "sockudo:delta", "third message must be a delta");
            }
        }
    }

    #[tokio::test]
    async fn binary_socket_with_delta_enabled_receives_full_binary_frames() {
        let adapter = LocalAdapter::new();
        let (writer, mut client) = create_ws_pair().await;
        let socket = socket_ref(writer, WireFormat::MessagePack, AppendMode::Full);
        let manager = enabled_delta();
        manager.enable_for_socket(&socket.socket_id);

        for tick in 0..3 {
            send_one_with_delta(&adapter, &socket, &manager, WireFormat::MessagePack, tick).await;
            let sockudo_ws::Message::Binary(bytes) = next_frame(&mut client).await else {
                panic!("binary fanout must stay on binary frames");
            };
            let message: PusherMessage =
                wire::deserialize_message(&bytes, WireFormat::MessagePack).unwrap();
            assert_eq!(
                message.event.as_deref(),
                Some("test:event"),
                "delta compression is JSON-only; binary sockets must receive full messages"
            );
        }
    }
}
