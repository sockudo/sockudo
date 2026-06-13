use bytes::Bytes;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayPosition {
    pub stream_id: String,
    pub serial: u64,
}

struct BufferedMessage {
    stream_id: Option<String>,
    serial: u64,
    message_bytes: Bytes,
    timestamp: Instant,
}

struct ChannelBufferState {
    messages: VecDeque<BufferedMessage>,
    current_stream_id: Option<String>,
    last_touched: Instant,
}

struct ChannelBuffer {
    state: Mutex<ChannelBufferState>,
    next_serial: AtomicU64,
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

    fn new_stream_id() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    fn new_channel_buffer(
        max_buffer_size: usize,
        stream_id: Option<String>,
        next_serial: u64,
        now: Instant,
    ) -> ChannelBuffer {
        ChannelBuffer {
            state: Mutex::new(ChannelBufferState {
                messages: VecDeque::with_capacity(max_buffer_size),
                current_stream_id: stream_id,
                last_touched: now,
            }),
            next_serial: AtomicU64::new(next_serial),
        }
    }

    fn raise_next_serial(entry: &ChannelBuffer, next_serial: u64) {
        let mut current = entry.next_serial.load(Ordering::Relaxed);
        while current < next_serial {
            match entry.next_serial.compare_exchange(
                current,
                next_serial,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
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

    pub fn current_position(&self, app_id: &str, channel: &str) -> ReplayPosition {
        let key = Self::buffer_key(app_id, channel);
        let now = Instant::now();
        let entry = self.buffers.entry(key).or_insert_with(|| {
            Self::new_channel_buffer(self.max_buffer_size, Some(Self::new_stream_id()), 1, now)
        });

        let mut state = entry.state.lock().unwrap();
        let stream_id = state
            .current_stream_id
            .get_or_insert_with(Self::new_stream_id)
            .clone();
        state.last_touched = now;
        let serial = entry.next_serial.load(Ordering::Relaxed).saturating_sub(1);

        ReplayPosition { stream_id, serial }
    }

    pub fn ensure_position(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        serial: u64,
    ) -> ReplayPosition {
        let key = Self::buffer_key(app_id, channel);
        let now = Instant::now();
        let next_serial = serial.saturating_add(1);
        let entry = self.buffers.entry(key).or_insert_with(|| {
            Self::new_channel_buffer(
                self.max_buffer_size,
                Some(stream_id.to_string()),
                next_serial,
                now,
            )
        });

        {
            let mut state = entry.state.lock().unwrap();
            if state.messages.is_empty() || state.current_stream_id.is_none() {
                state.current_stream_id = Some(stream_id.to_string());
            }
            state.last_touched = now;
        }
        Self::raise_next_serial(entry.value(), next_serial);

        ReplayPosition {
            stream_id: stream_id.to_string(),
            serial,
        }
    }

    pub fn next_position(&self, app_id: &str, channel: &str) -> ReplayPosition {
        let key = Self::buffer_key(app_id, channel);
        let now = Instant::now();
        let entry = self.buffers.entry(key).or_insert_with(|| {
            Self::new_channel_buffer(self.max_buffer_size, Some(Self::new_stream_id()), 1, now)
        });

        let mut state = entry.state.lock().unwrap();
        let stream_id = state
            .current_stream_id
            .get_or_insert_with(Self::new_stream_id)
            .clone();
        state.last_touched = now;
        let serial = entry.next_serial.fetch_add(1, Ordering::Relaxed);

        ReplayPosition { stream_id, serial }
    }

    /// Atomically increment and return the next serial for a channel.
    pub fn next_serial(&self, app_id: &str, channel: &str) -> u64 {
        self.next_position(app_id, channel).serial
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
        let now = Instant::now();
        let entry = self.buffers.entry(key).or_insert_with(|| {
            Self::new_channel_buffer(
                self.max_buffer_size,
                stream_id.map(ToString::to_string),
                serial.saturating_add(1),
                now,
            )
        });

        let mut state = entry.state.lock().unwrap();
        state.current_stream_id = stream_id.map(ToString::to_string);
        state.last_touched = now;
        Self::raise_next_serial(entry.value(), serial.saturating_add(1));
        // Evict oldest if at capacity
        while state.messages.len() >= self.max_buffer_size {
            state.messages.pop_front();
        }
        state.messages.push_back(BufferedMessage {
            stream_id: stream_id.map(ToString::to_string),
            serial,
            message_bytes,
            timestamp: now,
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

        let now = Instant::now();
        let mut state = entry.state.lock().unwrap();

        if let Some(expected_stream_id) = stream_id
            && state.current_stream_id.as_deref() != Some(expected_stream_id)
        {
            return ReplayLookup::StreamReset {
                current_stream_id: state.current_stream_id.clone(),
            };
        }

        Self::prune_expired_locked(&mut state.messages, self.buffer_ttl, now);

        if state.messages.is_empty() {
            if now.duration_since(state.last_touched) >= self.buffer_ttl {
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

        let newest_serial = state.messages.back().map(|m| m.serial).unwrap_or(0);
        if last_serial >= newest_serial {
            return ReplayLookup::Recovered(Vec::new());
        }

        // Check if the buffer goes back far enough
        let oldest_serial = state.messages.front().map(|m| m.serial).unwrap_or(0);
        if last_serial > 0 && last_serial < oldest_serial.saturating_sub(1) {
            // The client missed messages that were already evicted
            return ReplayLookup::Expired;
        }

        let contiguous = state.messages.make_contiguous();
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
            let mut state = entry.value().state.lock().unwrap();
            Self::prune_expired_locked(&mut state.messages, self.buffer_ttl, now);
            if state.messages.is_empty()
                && now.duration_since(state.last_touched) >= self.buffer_ttl
            {
                empty_keys.push(entry.key().clone());
            }
        }

        for key in empty_keys {
            // Only remove if still empty (avoid race with concurrent store)
            self.buffers.remove_if(&key, |_, v| {
                let state = v.state.lock().unwrap();
                state.messages.is_empty()
                    && now.duration_since(state.last_touched) >= self.buffer_ttl
            });
        }
    }
}
