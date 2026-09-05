use super::LocalAdapter;
#[cfg(feature = "delta")]
use ahash::AHashMap as HashMap;
use bytes::Bytes;
use sockudo_core::error::{Error, Result};
use sockudo_core::namespace::Namespace;
use sockudo_core::websocket::{SocketId, WebSocketRef};
use sockudo_protocol::messages::PusherMessage;
use sockudo_protocol::versioned_messages::{MessageAction, extract_runtime_action};
use std::sync::Arc;
#[cfg(feature = "tag-filtering")]
use std::sync::atomic::Ordering;
use tracing::{debug, warn};

impl LocalAdapter {
    /// Send messages using chunked processing with semaphore-controlled concurrency
    pub(super) async fn send_messages_concurrent(
        &self,
        target_socket_refs: Vec<WebSocketRef>,
        message_bytes: Bytes,
    ) -> Vec<Result<()>> {
        use futures::stream::{self, StreamExt};
        use sockudo_protocol::wire::{WireFormat, serialize_message};
        let mut prepared = std::collections::HashMap::new();
        prepared.insert(WireFormat::Json, message_bytes.clone());
        if target_socket_refs
            .iter()
            .any(|socket| socket.wire_format.is_binary())
        {
            let message = sonic_rs::from_slice::<PusherMessage>(&message_bytes);
            for format in [WireFormat::MessagePack, WireFormat::Protobuf] {
                if target_socket_refs
                    .iter()
                    .any(|socket| socket.wire_format == format)
                {
                    match message
                        .as_ref()
                        .map_err(|error| error.to_string())
                        .and_then(|message| serialize_message(message, format))
                    {
                        Ok(bytes) => {
                            prepared.insert(format, Bytes::from(bytes));
                        }
                        Err(error) => {
                            warn!(error = %error, "broadcast encoding failed");
                        }
                    }
                }
            }
        }
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
                            let bytes = prepared.get(&socket_ref.wire_format).cloned();
                            async move {
                                match bytes {
                                    Some(bytes) => socket_ref
                                        .send_prepared_broadcast(bytes, socket_ref.wire_format),
                                    None => Err(Error::InvalidMessageFormat(
                                        "Broadcast encoding failed".into(),
                                    )),
                                }
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

        results
    }

    #[cfg(feature = "delta")]
    /// Prepare equivalent delta/full envelopes once, then retain per-socket bases
    /// only after bounded queue admission succeeds. Pointer identities are held
    /// alive in the cache and are therefore an exact base proof, not a hash guess.
    pub(super) async fn send_messages_with_compression(
        &self,
        sockets: Vec<WebSocketRef>,
        base_message: PusherMessage,
        base_message_bytes: Vec<u8>,
        channel: &str,
        event_name: &str,
        compression: crate::connection_manager::CompressionParams<'_>,
    ) -> Vec<Result<()>> {
        use sockudo_protocol::wire::{WireFormat, serialize_message};
        let manager = compression.delta_compression;
        let settings = compression.channel_settings;
        let path = settings
            .and_then(|s| s.conflation_key.as_ref())
            .or(manager.get_conflation_key_path());
        let conflation_key = path
            .map(|path| manager.extract_conflation_key_from_path(&base_message_bytes, path))
            .unwrap_or_default();
        let cache_key = if conflation_key.is_empty() {
            event_name.to_owned()
        } else {
            format!("{event_name}:{conflation_key}")
        };
        let shared_base = Arc::new(base_message_bytes);
        // Bound temporary preparation independently of fanout size and payload.
        const MAX_PREPARED_BYTES: usize = 16 * 1024 * 1024;
        const MAX_PREPARED_ENTRIES: usize = 256;
        type DeltaCache = HashMap<(usize, u8), (Arc<Vec<u8>>, Option<Arc<str>>)>;
        let mut deltas = DeltaCache::new();
        let mut frames: HashMap<(usize, u8, u32, u32, WireFormat, bool), Bytes> = HashMap::new();
        let mut prepared_bytes = 0usize;
        let mut results = Vec::with_capacity(sockets.len());
        for (index, socket) in sockets.into_iter().enumerate() {
            if index % self.max_concurrent.max(1) == 0 {
                tokio::task::yield_now().await;
            }
            if prepared_bytes >= MAX_PREPARED_BYTES
                || frames.len() + deltas.len() >= MAX_PREPARED_ENTRIES
            {
                frames.clear();
                deltas.clear();
                prepared_bytes = 0;
            }
            let socket_id = socket.get_socket_id_sync();
            let enabled = manager.is_enabled_for_socket_channel(socket_id, channel);
            let sequence = if enabled {
                manager.get_next_sequence(socket_id, channel, &cache_key)
            } else {
                0
            };
            let algorithm = manager.get_algorithm_for_channel(socket_id, channel);
            let algorithm_id = match algorithm {
                sockudo_delta::DeltaAlgorithm::Fossil => 0,
                sockudo_delta::DeltaAlgorithm::Xdelta3 => 1,
            };
            let mut delta = None;
            let mut memoize_frame = true;
            let mut base_identity = 0;
            let mut base_sequence = 0;
            if enabled
                && sequence >= 2
                && !manager.should_send_full_message(socket_id, channel, &cache_key)
                && let Some((base, serial)) = manager
                    .get_last_message_with_sequence(socket_id, channel, &cache_key)
                    .await
            {
                base_identity = Arc::as_ptr(&base) as usize;
                base_sequence = serial;
                if let Some(entry) = deltas.get(&(base_identity, algorithm_id)) {
                    delta = entry.1.clone();
                } else {
                    delta = match manager.compute_delta_for_algorithm(
                        &base,
                        &shared_base,
                        algorithm,
                    ) {
                        Ok(bytes) if bytes.len() < shared_base.len() => {
                            Some(Arc::<str>::from(base64::Engine::encode(
                                &base64::engine::general_purpose::STANDARD,
                                bytes,
                            )))
                        }
                        Ok(_) => None,
                        Err(error) => {
                            warn!(error = %error, channel, "delta computation failed using full message");
                            None
                        }
                    };
                    let retained_bytes = base
                        .len()
                        .saturating_add(delta.as_ref().map_or(0, |text| text.len()));
                    if prepared_bytes.saturating_add(retained_bytes) <= MAX_PREPARED_BYTES
                        && frames.len() + deltas.len() < MAX_PREPARED_ENTRIES
                    {
                        prepared_bytes += retained_bytes;
                        deltas.insert((base_identity, algorithm_id), (base, delta.clone()));
                    } else {
                        // A delta frame may use pointer identity only while its exact
                        // base is retained in the memo. Oversized bases remain local.
                        memoize_frame = delta.is_none();
                    }
                }
            }
            let frame_key = (
                if delta.is_some() { base_identity } else { 0 },
                if enabled { algorithm_id + 1 } else { 0 },
                sequence,
                if delta.is_some() { base_sequence } else { 0 },
                socket.wire_format,
                socket.protocol_version == sockudo_protocol::ProtocolVersion::V2,
            );
            let frame = if let Some(frame) = frames.get(&frame_key).filter(|_| memoize_frame) {
                Ok(frame.clone())
            } else {
                let mut message = if let Some(delta) = &delta {
                    let mut data = sonic_rs::json!({"event":event_name,"delta":delta.as_ref(),"seq":sequence,"base_index":base_sequence});
                    if !manager.config.omit_delta_algorithm {
                        data["algorithm"] = sonic_rs::json!(if algorithm_id == 0 {
                            "fossil"
                        } else {
                            "xdelta3"
                        });
                    }
                    if !conflation_key.is_empty() {
                        data["conflation_key"] = sonic_rs::json!(conflation_key);
                    }
                    let mut message = PusherMessage::ping();
                    message.event = Some("pusher:delta".into());
                    message.channel = Some(channel.into());
                    message.data = Some(sockudo_protocol::messages::MessageData::Json(data));
                    message
                } else {
                    let mut message = base_message.clone();
                    if enabled {
                        message.delta_sequence = Some(sequence.into());
                        if !conflation_key.is_empty() {
                            message.delta_conflation_key = Some(conflation_key.clone());
                        }
                    }
                    message
                };
                message.rewrite_prefix(socket.protocol_version);
                serialize_message(&message, socket.wire_format)
                    .map(|bytes| {
                        let bytes = Bytes::from(bytes);
                        if memoize_frame
                            && prepared_bytes.saturating_add(bytes.len()) <= MAX_PREPARED_BYTES
                            && frames.len() + deltas.len() < MAX_PREPARED_ENTRIES
                        {
                            prepared_bytes += bytes.len();
                            frames.insert(frame_key, bytes.clone());
                        }
                        bytes
                    })
                    .map_err(|error| {
                        Error::InvalidMessageFormat(format!("Serialization failed: {error}"))
                    })
            };
            debug_assert!(prepared_bytes <= MAX_PREPARED_BYTES);
            debug_assert!(frames.len() + deltas.len() <= MAX_PREPARED_ENTRIES);
            let outcome =
                frame.and_then(|bytes| socket.admit_prepared_broadcast(bytes, socket.wire_format));
            if matches!(outcome, Ok(true))
                && enabled
                && let Err(error) = manager
                    .store_shared_sent_message_with_key(
                        socket_id,
                        channel,
                        event_name,
                        Arc::clone(&shared_base),
                        delta.is_none(),
                        settings,
                        &conflation_key,
                    )
                    .await
            {
                warn!(error = %error, socket_id = %socket_id, channel, "failed to store admitted delta base");
            }
            results.push(outcome.map(|_| ()));
        }
        results
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
        Self::log_send_errors(self.send_messages_concurrent(sockets, v1_bytes).await);
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

#[cfg(all(test, feature = "delta", feature = "tag-filtering"))]
#[path = "broadcast_tests.rs"]
mod regression_tests;
