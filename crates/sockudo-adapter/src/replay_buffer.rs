use bytes::Bytes;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

struct BufferedMessage {
    stream_id: Option<String>,
    serial: u64,
    message_bytes: Bytes,
    timestamp: Instant,
}

struct ChannelBuffer {
    messages: Mutex<VecDeque<BufferedMessage>>,
    next_serial: AtomicU64,
    current_stream_id: Mutex<Option<String>>,
}

pub enum ReplayLookup {
    Recovered(Vec<Bytes>),
    Expired,
    StreamReset { current_stream_id: Option<String> },
}

/// Per-channel replay buffer for connection recovery.
///
/// When connection recovery is enabled, every broadcast message is assigned a
/// monotonically increasing serial number and stored in a bounded, time-limited
/// buffer. Reconnecting clients send the last serial they received; the server
/// replays all messages with a higher serial from the buffer.
pub struct ReplayBuffer {
    /// Key: "app_id\0channel" → ChannelBuffer
    buffers: DashMap<String, ChannelBuffer>,
    max_buffer_size: usize,
    buffer_ttl: Duration,
}

impl ReplayBuffer {
    pub fn new(max_buffer_size: usize, buffer_ttl: Duration) -> Self {
        Self {
            buffers: DashMap::new(),
            max_buffer_size,
            buffer_ttl,
        }
    }

    fn buffer_key(app_id: &str, channel: &str) -> String {
        format!("{}\0{}", app_id, channel)
    }

    fn prune_expired_locked(
        messages: &mut VecDeque<BufferedMessage>,
        buffer_ttl: Duration,
        now: Instant,
    ) {
        while let Some(front) = messages.front() {
            if now.duration_since(front.timestamp) >= buffer_ttl {
                messages.pop_front();
            } else {
                break;
            }
        }
    }

    /// Atomically increment and return the next serial for a channel.
    pub fn next_serial(&self, app_id: &str, channel: &str) -> u64 {
        let key = Self::buffer_key(app_id, channel);
        let entry = self.buffers.entry(key).or_insert_with(|| ChannelBuffer {
            messages: Mutex::new(VecDeque::with_capacity(self.max_buffer_size)),
            next_serial: AtomicU64::new(1),
            current_stream_id: Mutex::new(None),
        });
        entry.next_serial.fetch_add(1, Ordering::Relaxed)
    }

    /// Store a serialized message in the replay buffer.
    pub fn store(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: Option<&str>,
        serial: u64,
        message_bytes: Bytes,
    ) {
        let key = Self::buffer_key(app_id, channel);
        let entry = self.buffers.entry(key).or_insert_with(|| ChannelBuffer {
            messages: Mutex::new(VecDeque::with_capacity(self.max_buffer_size)),
            next_serial: AtomicU64::new(serial + 1),
            current_stream_id: Mutex::new(stream_id.map(ToString::to_string)),
        });

        {
            let mut current_stream_id = entry.current_stream_id.lock().unwrap();
            *current_stream_id = stream_id.map(ToString::to_string);
        }

        let mut messages = entry.messages.lock().unwrap();
        // Evict oldest if at capacity
        while messages.len() >= self.max_buffer_size {
            messages.pop_front();
        }
        messages.push_back(BufferedMessage {
            stream_id: stream_id.map(ToString::to_string),
            serial,
            message_bytes,
            timestamp: Instant::now(),
        });
    }

    /// Get all messages with serial > `last_serial` for a given channel.
    ///
    /// Returns `Some(vec)` if the buffer can satisfy the request (even if vec is empty).
    /// Returns `None` if the buffer no longer contains messages old enough
    /// (i.e., the client is too far behind and needs a full re-subscribe).
    pub fn get_messages_after(
        &self,
        app_id: &str,
        channel: &str,
        last_serial: u64,
    ) -> Option<Vec<Bytes>> {
        match self.get_messages_after_position(app_id, channel, None, last_serial) {
            ReplayLookup::Recovered(messages) => Some(messages),
            ReplayLookup::Expired | ReplayLookup::StreamReset { .. } => None,
        }
    }

    pub fn get_messages_after_position(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: Option<&str>,
        last_serial: u64,
    ) -> ReplayLookup {
        let key = Self::buffer_key(app_id, channel);
        let Some(entry) = self.buffers.get(&key) else {
            return ReplayLookup::Expired;
        };
        let current_stream_id = entry.current_stream_id.lock().unwrap().clone();

        if let Some(expected_stream_id) = stream_id
            && current_stream_id.as_deref() != Some(expected_stream_id)
        {
            return ReplayLookup::StreamReset { current_stream_id };
        }
        let now = Instant::now();
        let mut messages = entry.messages.lock().unwrap();
        Self::prune_expired_locked(&mut messages, self.buffer_ttl, now);

        if messages.is_empty() {
            if stream_id.is_some() {
                return ReplayLookup::Expired;
            }
            // No buffered messages — client is either up-to-date or buffer was evicted.
            // If the requested serial matches or exceeds the next serial, they're caught up.
            let next = entry.next_serial.load(Ordering::Relaxed);
            return if last_serial >= next.saturating_sub(1) {
                ReplayLookup::Recovered(Vec::new())
            } else {
                ReplayLookup::Expired
            };
        }

        let newest_serial = messages.back().map(|m| m.serial).unwrap_or(0);
        if last_serial >= newest_serial {
            return ReplayLookup::Recovered(Vec::new());
        }

        // Check if the buffer goes back far enough
        let oldest_serial = messages.front().map(|m| m.serial).unwrap_or(0);
        if last_serial > 0 && last_serial < oldest_serial.saturating_sub(1) {
            // The client missed messages that were already evicted
            return ReplayLookup::Expired;
        }

        let contiguous = messages.make_contiguous();
        let start_idx = contiguous.partition_point(|message| message.serial <= last_serial);
        let mut result = Vec::with_capacity(contiguous.len().saturating_sub(start_idx));
        for message in &contiguous[start_idx..] {
            let _ = &message.stream_id;
            result.push(message.message_bytes.clone());
        }

        ReplayLookup::Recovered(result)
    }

    /// Evict messages older than `buffer_ttl` and remove empty channel buffers.
    pub fn evict_expired(&self) {
        let now = Instant::now();
        let mut empty_keys = Vec::new();

        for entry in self.buffers.iter() {
            let mut messages = entry.value().messages.lock().unwrap();
            Self::prune_expired_locked(&mut messages, self.buffer_ttl, now);
            if messages.is_empty() {
                empty_keys.push(entry.key().clone());
            }
        }

        for key in empty_keys {
            // Only remove if still empty (avoid race with concurrent store)
            self.buffers
                .remove_if(&key, |_, v| v.messages.lock().unwrap().is_empty());
        }
    }
}
