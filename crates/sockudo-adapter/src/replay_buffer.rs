use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

struct BufferedMessage {
    serial: u64,
    message_bytes: Vec<u8>,
    timestamp: Instant,
}

struct ChannelBuffer {
    messages: Mutex<VecDeque<BufferedMessage>>,
    next_serial: AtomicU64,
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

    /// Atomically increment and return the next serial for a channel.
    pub fn next_serial(&self, app_id: &str, channel: &str) -> u64 {
        let key = Self::buffer_key(app_id, channel);
        let entry = self.buffers.entry(key).or_insert_with(|| ChannelBuffer {
            messages: Mutex::new(VecDeque::with_capacity(self.max_buffer_size)),
            next_serial: AtomicU64::new(1),
        });
        entry.next_serial.fetch_add(1, Ordering::Relaxed)
    }

    /// Store a serialized message in the replay buffer.
    pub fn store(&self, app_id: &str, channel: &str, serial: u64, message_bytes: Vec<u8>) {
        let key = Self::buffer_key(app_id, channel);
        let entry = self.buffers.entry(key).or_insert_with(|| ChannelBuffer {
            messages: Mutex::new(VecDeque::with_capacity(self.max_buffer_size)),
            next_serial: AtomicU64::new(serial + 1),
        });

        let mut messages = entry.messages.lock().unwrap();
        // Evict oldest if at capacity
        while messages.len() >= self.max_buffer_size {
            messages.pop_front();
        }
        messages.push_back(BufferedMessage {
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
    ) -> Option<Vec<Vec<u8>>> {
        let key = Self::buffer_key(app_id, channel);
        let entry = self.buffers.get(&key)?;
        let messages = entry.messages.lock().unwrap();

        if messages.is_empty() {
            // No buffered messages — client is either up-to-date or buffer was evicted.
            // If the requested serial matches or exceeds the next serial, they're caught up.
            let next = entry.next_serial.load(Ordering::Relaxed);
            return if last_serial >= next.saturating_sub(1) {
                Some(Vec::new())
            } else {
                None
            };
        }

        // Check if the buffer goes back far enough
        let oldest_serial = messages.front().map(|m| m.serial).unwrap_or(0);
        if last_serial > 0 && last_serial < oldest_serial.saturating_sub(1) {
            // The client missed messages that were already evicted
            return None;
        }

        let now = Instant::now();
        let result: Vec<Vec<u8>> = messages
            .iter()
            .filter(|m| m.serial > last_serial && now.duration_since(m.timestamp) < self.buffer_ttl)
            .map(|m| m.message_bytes.clone())
            .collect();

        Some(result)
    }

    /// Evict messages older than `buffer_ttl` and remove empty channel buffers.
    pub fn evict_expired(&self) {
        let now = Instant::now();
        let mut empty_keys = Vec::new();

        for entry in self.buffers.iter() {
            let mut messages = entry.value().messages.lock().unwrap();
            while let Some(front) = messages.front() {
                if now.duration_since(front.timestamp) >= self.buffer_ttl {
                    messages.pop_front();
                } else {
                    break;
                }
            }
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
