// Delta compression pipeline: compress/store paths, delta algorithms, and
// cached-base message reads used by broadcast optimization.

use ahash::AHashMap;
use sockudo_core::delta_types::{ChannelDeltaSettings, DeltaAlgorithm};
use sockudo_core::error::{Error, Result};
use sockudo_core::websocket::SocketId;
use std::sync::Arc;

use crate::manager::DeltaCompressionManager;
use crate::messages::CachedMessage;
use crate::state::{ChannelState, ConflationKeyCache};

/// Compute an RFC 3284 VCDIFF payload without applying Sockudo's native
/// per-socket policy. Protocol adapters use this when their wire contract
/// explicitly negotiated VCDIFF and own the delivery-local base chain.
pub fn compute_vcdiff(base_message: &[u8], new_message: &[u8]) -> Result<Vec<u8>> {
    let mut delta = Vec::new();
    oxidelta::compress::encoder::encode_all(
        &mut delta,
        base_message,
        new_message,
        oxidelta::compress::encoder::CompressOptions {
            // The pinned Ably decoder implements RFC 3284 data/instruction
            // windows but does not consume the optional Adler-32 trailer.
            checksum: false,
            ..Default::default()
        },
    )
    .map_err(|error| Error::Internal(format!("VCDIFF encoding failed: {error}")))?;
    Ok(delta)
}

impl DeltaCompressionManager {
    /// Get the delta compression algorithm from config
    pub fn get_algorithm(&self) -> DeltaAlgorithm {
        self.config.algorithm
    }

    /// Get the last message stored for a socket's channel (for broadcast optimization).
    /// Returns None if socket doesn't have delta enabled or no base message exists.
    pub async fn get_last_message_for_socket(
        &self,
        socket_id: &SocketId,
        channel: &str,
        conflation_key: &str,
    ) -> Option<Arc<Vec<u8>>> {
        tracing::debug!(
            "get_last_message_for_socket: START socket={}, channel={}, conflation_key='{}'",
            socket_id,
            channel,
            conflation_key
        );

        let socket_state = match self.socket_states.get(socket_id) {
            Some(s) => {
                tracing::debug!(
                    "get_last_message_for_socket: Found socket_state for socket={}",
                    socket_id
                );
                s
            }
            None => {
                tracing::debug!(
                    "get_last_message_for_socket: No socket_state for socket={}",
                    socket_id
                );
                return None;
            }
        };

        let channel_state = match socket_state.get_channel_state(channel) {
            Some(c) => {
                tracing::debug!(
                    "get_last_message_for_socket: Found channel_state for socket={}, channel={}",
                    socket_id,
                    channel
                );
                c
            }
            None => {
                tracing::debug!(
                    "get_last_message_for_socket: No channel_state for socket={}, channel={}",
                    socket_id,
                    channel
                );
                return None;
            }
        };

        let cache = match channel_state.get_conflation_state(conflation_key) {
            Some(c) => {
                tracing::debug!(
                    "get_last_message_for_socket: Found cache for socket={}, channel={}, conflation_key='{}'",
                    socket_id,
                    channel,
                    conflation_key
                );
                c
            }
            None => {
                tracing::debug!(
                    "get_last_message_for_socket: No cache for socket={}, channel={}, conflation_key='{}'",
                    socket_id,
                    channel,
                    conflation_key
                );
                return None;
            }
        };

        let result = cache.get_last_message().await;
        tracing::debug!(
            "get_last_message_for_socket: get_last_message() returned {} for socket={}, channel={}, conflation_key='{}'",
            if result.is_some() {
                "Some(message)"
            } else {
                "None"
            },
            socket_id,
            channel,
            conflation_key
        );

        result.map(|msg| Arc::clone(&msg.content))
    }

    /// Get the last message stored for a socket's channel WITH its sequence number.
    /// Returns (message_content, sequence) for use in precomputed delta paths.
    pub async fn get_last_message_with_sequence(
        &self,
        socket_id: &SocketId,
        channel: &str,
        conflation_key: &str,
    ) -> Option<(Arc<Vec<u8>>, u32)> {
        let socket_state = self.socket_states.get(socket_id)?;
        let channel_state = socket_state.get_channel_state(channel)?;
        let cache = channel_state.get_conflation_state(conflation_key)?;
        let msg = cache.get_last_message().await?;
        Some((Arc::clone(&msg.content), msg.sequence))
    }

    /// Get the next sequence number for a socket's channel/conflation_key.
    /// Returns 0 if the cache doesn't exist yet.
    pub fn get_next_sequence(
        &self,
        socket_id: &SocketId,
        channel: &str,
        conflation_key: &str,
    ) -> u32 {
        use std::sync::atomic::Ordering;

        let socket_state = match self.socket_states.get(socket_id) {
            Some(state) => state,
            None => return 0,
        };

        let channel_state = match socket_state.get_channel_state(channel) {
            Some(state) => state,
            None => return 0,
        };

        let cache = match channel_state.get_conflation_state(conflation_key) {
            Some(cache) => cache,
            None => return 0,
        };

        cache.next_sequence.load(Ordering::Relaxed)
    }

    /// Compute delta between two messages (broadcast-level, can be called once and reused).
    /// This avoids recomputing the same delta for multiple sockets.
    pub fn compute_delta_for_broadcast(
        &self,
        base_message: &[u8],
        new_message: &[u8],
    ) -> Result<Vec<u8>> {
        match self.config.algorithm {
            DeltaAlgorithm::Fossil => self.compute_fossil_delta(base_message, new_message),
            DeltaAlgorithm::Xdelta3 => self.compute_xdelta3_delta(base_message, new_message),
        }
    }

    /// Compress a message for a specific socket and channel with optional channel-specific settings.
    /// Returns either a delta-compressed message or the original message.
    ///
    /// IMPORTANT: This method does NOT store the message in the cache. After sending the message
    /// to the client, call store_sent_message() with the ACTUAL bytes that were sent.
    pub async fn compress_message(
        &self,
        socket_id: &SocketId,
        channel: &str,
        event_name: &str,
        message_bytes: &[u8],
        channel_settings: Option<&ChannelDeltaSettings>,
    ) -> Result<CompressionResult> {
        if !self.is_enabled_for_socket_channel(socket_id, channel) {
            return Ok(CompressionResult::Uncompressed);
        }

        if message_bytes.len() < self.config.min_message_size {
            return Ok(CompressionResult::Uncompressed);
        }

        let conflation_key_path = channel_settings
            .and_then(|s| s.conflation_key.as_ref())
            .or(self.config.conflation_key_path.as_ref());

        let conflation_key = if let Some(path) = conflation_key_path {
            self.extract_conflation_key_with_path(message_bytes, path)
        } else {
            String::new()
        };

        let cache_key = if conflation_key.is_empty() {
            event_name.to_string()
        } else {
            format!("{}:{}", event_name, conflation_key)
        };

        let max_messages_per_key = channel_settings
            .map(|s| s.max_messages_per_key)
            .unwrap_or(10);

        let socket_state = match self.socket_states.get(socket_id) {
            Some(state) => state,
            None => return Ok(CompressionResult::Uncompressed),
        };

        let channel_state = socket_state.get_channel_state(channel);
        let channel_state = match channel_state {
            Some(state) => state,
            None => {
                let new_channel_state = Arc::new(ChannelState::new());
                socket_state.set_channel_state(channel.to_string(), Arc::clone(&new_channel_state));
                new_channel_state
            }
        };

        let conflation_cache = channel_state.get_conflation_state(&cache_key);

        let conflation_key_opt = if conflation_key_path.is_some() && !conflation_key.is_empty() {
            Some(conflation_key.clone())
        } else {
            None
        };

        match conflation_cache {
            None => {
                let cache = ConflationKeyCache::new(max_messages_per_key);
                channel_state
                    .set_conflation_state(
                        cache_key.clone(),
                        cache,
                        self.config.max_conflation_states_per_channel,
                    )
                    .await;
                Ok(CompressionResult::FullMessage {
                    sequence: 0,
                    conflation_key: conflation_key_opt,
                })
            }
            Some(cache) => {
                use std::sync::atomic::Ordering;

                let should_send_full = cache.should_send_full_message(&self.config).await;
                tracing::debug!(
                    "compress_message: should_send_full_message={}, delta_count={}, full_message_interval={}",
                    should_send_full,
                    cache.delta_count.load(Ordering::Relaxed),
                    self.config.full_message_interval
                );
                if should_send_full {
                    let sequence = cache.next_sequence.load(Ordering::Relaxed);
                    tracing::info!(
                        "Sending FULL message due to interval (delta_count={}, interval={})",
                        cache.delta_count.load(Ordering::Relaxed),
                        self.config.full_message_interval
                    );
                    return Ok(CompressionResult::FullMessage {
                        sequence,
                        conflation_key: conflation_key_opt,
                    });
                }

                let (last_msg, base_index) = match cache.get_last_message().await {
                    Some(msg) => {
                        let base_seq = msg.sequence as usize;
                        let base_content = &msg.content;
                        tracing::info!(
                            "compress_message: Using base for delta: base_seq={}, base_len={}, base_last50='{}', cache_key='{}'",
                            base_seq,
                            base_content.len(),
                            String::from_utf8_lossy(
                                &base_content[base_content.len().saturating_sub(50)..]
                            ),
                            cache_key
                        );
                        (msg.content.clone(), Some(base_seq))
                    }
                    None => {
                        let sequence = cache.next_sequence.load(Ordering::Relaxed);
                        tracing::warn!(
                            "compress_message: No last message in cache (evicted?), sending FULL, socket={}, channel={}, cache_key='{}'",
                            socket_id,
                            channel,
                            cache_key
                        );
                        return Ok(CompressionResult::FullMessage {
                            sequence,
                            conflation_key: conflation_key_opt,
                        });
                    }
                };

                let algorithm = self.get_algorithm_for_channel(socket_id, channel);

                let delta = match algorithm {
                    DeltaAlgorithm::Fossil => self.compute_fossil_delta(&last_msg, message_bytes),
                    DeltaAlgorithm::Xdelta3 => self.compute_xdelta3_delta(&last_msg, message_bytes),
                };

                let delta = match delta {
                    Ok(d) => d,
                    Err(_) => {
                        let sequence = cache.next_sequence.load(Ordering::Relaxed);
                        return Ok(CompressionResult::FullMessage {
                            sequence,
                            conflation_key: conflation_key_opt,
                        });
                    }
                };

                if delta.len() >= message_bytes.len() {
                    let sequence = cache.next_sequence.load(Ordering::Relaxed);
                    return Ok(CompressionResult::FullMessage {
                        sequence,
                        conflation_key: conflation_key_opt,
                    });
                }

                let sequence = cache.next_sequence.load(Ordering::Relaxed);

                Ok(CompressionResult::Delta {
                    delta,
                    sequence,
                    algorithm,
                    conflation_key: conflation_key_opt,
                    base_index,
                })
            }
        }
    }

    /// Store the actual message that was sent to the client.
    /// This must be called AFTER sending the message, with the exact bytes that were sent.
    pub async fn store_sent_message(
        &self,
        socket_id: &SocketId,
        channel: &str,
        event_name: &str,
        sent_message_bytes: Vec<u8>,
        is_full_message: bool,
        channel_settings: Option<&ChannelDeltaSettings>,
    ) -> Result<()> {
        let socket_state = match self.socket_states.get(socket_id) {
            Some(state) => state,
            None => return Ok(()),
        };

        let channel_state = match socket_state.get_channel_state(channel) {
            Some(state) => state,
            None => {
                if !is_full_message {
                    tracing::debug!(
                        "store_sent_message: Delta message but no channel_state for socket={}, channel={} (likely unsubscribed), skipping",
                        socket_id,
                        channel
                    );
                    return Ok(());
                }

                tracing::debug!(
                    "store_sent_message: Creating channel_state for socket={}, channel={} (new subscription)",
                    socket_id,
                    channel
                );
                let new_channel_state = Arc::new(ChannelState::new());
                socket_state.set_channel_state(channel.to_string(), Arc::clone(&new_channel_state));
                new_channel_state
            }
        };

        let conflation_key_path = channel_settings
            .and_then(|s| s.conflation_key.as_ref())
            .or(self.config.conflation_key_path.as_ref());

        let conflation_key = if let Some(path) = conflation_key_path {
            self.extract_conflation_key_with_path(&sent_message_bytes, path)
        } else {
            String::new()
        };

        let cache_key = if conflation_key.is_empty() {
            event_name.to_string()
        } else {
            format!("{}:{}", event_name, conflation_key)
        };

        let max_messages_per_key = channel_settings
            .map(|s| s.max_messages_per_key)
            .unwrap_or(10);

        let cache_existed = channel_state.get_conflation_state(&cache_key).is_some();
        let mut cache = match channel_state.get_conflation_state(&cache_key) {
            Some(cache) => {
                tracing::debug!(
                    "store_sent_message: Found existing cache for socket={}, channel={}, cache_key='{}'",
                    socket_id,
                    channel,
                    cache_key
                );
                cache
            }
            None => {
                tracing::debug!(
                    "store_sent_message: Creating NEW cache for socket={}, channel={}, cache_key='{}'",
                    socket_id,
                    channel,
                    cache_key
                );
                ConflationKeyCache::new(max_messages_per_key)
            }
        };

        cache.add_message(sent_message_bytes).await?;
        tracing::debug!(
            "store_sent_message: Added message to cache (cache_existed={}, socket={}, channel={}, cache_key='{}')",
            cache_existed,
            socket_id,
            channel,
            cache_key
        );

        if is_full_message {
            tracing::debug!(
                "store_sent_message: Resetting delta_count (was {}) for channel={}, cache_key='{}'",
                cache.delta_count.load(std::sync::atomic::Ordering::Relaxed),
                channel,
                cache_key
            );
            cache.reset_delta_count();
        } else {
            let old_count = cache.delta_count.load(std::sync::atomic::Ordering::Relaxed);
            cache.increment_delta_count();
            let new_count = cache.delta_count.load(std::sync::atomic::Ordering::Relaxed);
            tracing::debug!(
                "store_sent_message: Incremented delta_count from {} to {} for channel={}, cache_key='{}'",
                old_count,
                new_count,
                channel,
                cache_key
            );
        }

        channel_state
            .set_conflation_state(
                cache_key.clone(),
                cache,
                self.config.max_conflation_states_per_channel,
            )
            .await;
        tracing::debug!(
            "store_sent_message: Stored cache back to channel_state (socket={}, channel={}, cache_key='{}')",
            socket_id,
            channel,
            cache_key
        );

        Ok(())
    }

    /// Check if a channel is an encrypted channel (private-encrypted-*)
    #[inline]
    pub fn is_encrypted_channel(channel: &str) -> bool {
        channel.starts_with("private-encrypted-")
    }

    /// Compute delta using Fossil algorithm
    ///
    /// Note: The fossil_delta Rust crate's `delta(a, b)` produces a delta that when applied
    /// with `deltainv(x, d)` reconstructs `a` (the first argument). However, the JavaScript
    /// fossil-delta library's `applyDelta(base, delta)` expects the delta to transform
    /// `base` into the new value. To make the Rust-produced delta compatible with the JS
    /// client's `applyDelta(old, delta)` => new, we swap the arguments: `delta(new, old)`.
    fn compute_fossil_delta(&self, old_message: &[u8], new_message: &[u8]) -> Result<Vec<u8>> {
        let old_str = std::str::from_utf8(old_message)
            .map_err(|e| Error::Internal(format!("Invalid UTF-8 in old message: {}", e)))?;
        let new_str = std::str::from_utf8(new_message)
            .map_err(|e| Error::Internal(format!("Invalid UTF-8 in new message: {}", e)))?;

        // IMPORTANT: Swap arguments! The Rust crate's delta(a, b) produces a delta that
        // reconstructs `a` when applied. But JS applyDelta(old, d) expects to get `new`.
        // By calling delta(new, old), the JS client can do applyDelta(old, delta) => new.
        let delta = fossil_delta::delta(new_str, old_str);
        Ok(delta)
    }

    /// Compute delta using Xdelta3 algorithm
    fn compute_xdelta3_delta(&self, old_message: &[u8], new_message: &[u8]) -> Result<Vec<u8>> {
        compute_vcdiff(old_message, new_message)
    }

    /// Get cached messages for a channel (for initial sync on subscription).
    /// Returns None if socket doesn't have delta enabled or channel not found.
    pub async fn get_channel_cache(
        &self,
        socket_id: &SocketId,
        channel: &str,
    ) -> Option<Vec<(String, Vec<CachedMessage>)>> {
        let socket_state = self.socket_states.get(socket_id)?;
        let channel_state = socket_state.get_channel_state(channel)?;

        let mut caches = Vec::new();
        let groups: Vec<(String, ConflationKeyCache)> = channel_state
            .conflation_groups
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        for (key, cache) in groups {
            caches.push((key, cache.get_all_messages().await));
        }

        Some(caches)
    }

    /// Get global channel cache for sending to new subscribers.
    /// This collects cache state from ANY socket that has state for this channel.
    pub async fn get_global_channel_cache_for_sync(
        &self,
        channel: &str,
    ) -> Option<Vec<(String, Vec<CachedMessage>)>> {
        let mut all_caches: AHashMap<String, Vec<CachedMessage>> = AHashMap::new();

        for socket_entry in self.socket_states.iter() {
            if let Some(channel_state) = socket_entry.value().get_channel_state(channel) {
                let groups: Vec<(String, ConflationKeyCache)> = channel_state
                    .conflation_groups
                    .iter()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect();

                for (key, cache) in groups {
                    let messages = cache.get_all_messages().await;

                    all_caches
                        .entry(key)
                        .and_modify(|existing| {
                            if messages.len() > existing.len() {
                                *existing = messages.clone();
                            }
                        })
                        .or_insert(messages);
                }
            }
        }

        if all_caches.is_empty() {
            None
        } else {
            Some(all_caches.into_iter().collect())
        }
    }

    /// Check if we should send a full message for a socket/channel/cache_key
    /// based on the full_message_interval configuration
    pub fn should_send_full_message(
        &self,
        socket_id: &SocketId,
        channel: &str,
        cache_key: &str,
    ) -> bool {
        use std::sync::atomic::Ordering;

        let socket_state = match self.socket_states.get(socket_id) {
            Some(state) => state,
            None => return false,
        };

        let channel_state = match socket_state.get_channel_state(channel) {
            Some(state) => state,
            None => return false,
        };

        let cache = match channel_state.get_conflation_state(cache_key) {
            Some(cache) => cache,
            None => return false,
        };

        let delta_count = cache.delta_count.load(Ordering::Relaxed);
        delta_count >= self.config.full_message_interval
    }
}

/// Result of compression attempt
#[derive(Debug)]
pub enum CompressionResult {
    /// No compression applied
    Uncompressed,
    /// Full message sent (with sequence number for client tracking)
    FullMessage {
        sequence: u32,
        conflation_key: Option<String>,
    },
    /// Delta compression applied
    Delta {
        delta: Vec<u8>,
        sequence: u32,
        algorithm: DeltaAlgorithm,
        conflation_key: Option<String>,
        base_index: Option<usize>,
    },
}

#[cfg(test)]
mod tests;
