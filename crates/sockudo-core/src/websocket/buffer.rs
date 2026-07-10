use bytes::Bytes;
use crossfire::mpsc;
use sockudo_protocol::messages::PusherMessage;
use sockudo_ws::Message;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Buffer limit strategy for WebSocket connections
/// Supports message count, byte size, or both (whichever triggers first)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferLimit {
    /// Limit by number of messages only (fastest, no size tracking)
    Messages(usize),
    /// Limit by total bytes only (tracks cumulative size)
    Bytes(usize),
    /// Limit by both - whichever triggers first (most precise)
    Both { messages: usize, bytes: usize },
}

impl Default for BufferLimit {
    fn default() -> Self {
        BufferLimit::Messages(1000)
    }
}

impl BufferLimit {
    #[inline]
    pub fn channel_capacity(&self) -> usize {
        match self {
            BufferLimit::Messages(n) => *n,
            BufferLimit::Bytes(_) => 10_000,
            BufferLimit::Both { messages, .. } => *messages,
        }
    }

    #[inline]
    pub fn tracks_bytes(&self) -> bool {
        matches!(self, BufferLimit::Bytes(_) | BufferLimit::Both { .. })
    }

    #[inline]
    pub fn byte_limit(&self) -> Option<usize> {
        match self {
            BufferLimit::Messages(_) => None,
            BufferLimit::Bytes(n) => Some(*n),
            BufferLimit::Both { bytes, .. } => Some(*bytes),
        }
    }

    #[inline]
    pub fn message_limit(&self) -> Option<usize> {
        match self {
            BufferLimit::Messages(n) => Some(*n),
            BufferLimit::Bytes(_) => None,
            BufferLimit::Both { messages, .. } => Some(*messages),
        }
    }
}

/// Configuration for WebSocket connection buffers
#[derive(Debug, Clone, Copy)]
pub struct WebSocketBufferConfig {
    pub limit: BufferLimit,
    pub disconnect_on_full: bool,
}

impl Default for WebSocketBufferConfig {
    fn default() -> Self {
        Self {
            limit: BufferLimit::default(),
            disconnect_on_full: true,
        }
    }
}

impl WebSocketBufferConfig {
    pub fn with_message_limit(max_messages: usize, disconnect_on_full: bool) -> Self {
        Self {
            limit: BufferLimit::Messages(max_messages),
            disconnect_on_full,
        }
    }

    pub fn with_byte_limit(max_bytes: usize, disconnect_on_full: bool) -> Self {
        Self {
            limit: BufferLimit::Bytes(max_bytes),
            disconnect_on_full,
        }
    }

    pub fn with_both_limits(
        max_messages: usize,
        max_bytes: usize,
        disconnect_on_full: bool,
    ) -> Self {
        Self {
            limit: BufferLimit::Both {
                messages: max_messages,
                bytes: max_bytes,
            },
            disconnect_on_full,
        }
    }

    pub fn new(capacity: usize, disconnect_on_full: bool) -> Self {
        Self::with_message_limit(capacity, disconnect_on_full)
    }

    #[inline]
    pub fn channel_capacity(&self) -> usize {
        self.limit.channel_capacity()
    }

    #[inline]
    pub fn tracks_bytes(&self) -> bool {
        self.limit.tracks_bytes()
    }
}

/// Atomic byte counter for tracking buffer memory usage
#[derive(Debug, Default)]
pub struct ByteCounter {
    bytes: AtomicUsize,
}

impl ByteCounter {
    pub fn new() -> Self {
        Self {
            bytes: AtomicUsize::new(0),
        }
    }

    #[inline]
    pub fn add(&self, size: usize) -> usize {
        self.bytes.fetch_add(size, Ordering::Relaxed) + size
    }

    #[inline]
    pub fn sub(&self, size: usize) {
        self.bytes.fetch_sub(size, Ordering::Relaxed);
    }

    #[inline]
    pub fn get(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn would_exceed(&self, size: usize, limit: usize) -> bool {
        self.get().saturating_add(size) > limit
    }
}

/// Message wrapper that includes size for byte tracking
pub struct SizedMessage {
    pub bytes: Bytes,
    pub size: usize,
}

#[derive(Debug, Clone)]
pub struct BufferedRewindMessage {
    pub serial: Option<u64>,
    pub message_id: Option<String>,
    pub message: PusherMessage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RewindGateLimit {
    Messages,
    Bytes,
}

impl RewindGateLimit {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Messages => "messages",
            Self::Bytes => "bytes",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RewindGateOverflow {
    pub(super) limit_kind: RewindGateLimit,
    pub(super) limit: usize,
    pub(super) buffered_messages: usize,
    pub(super) buffered_bytes: usize,
    pub(super) incoming_message_bytes: usize,
}

#[derive(Debug)]
pub struct RewindGate {
    buffered: Vec<BufferedRewindMessage>,
    buffered_bytes: usize,
    max_messages: usize,
    max_bytes: Option<usize>,
    overflowed: bool,
}

impl Default for RewindGate {
    fn default() -> Self {
        Self::new(WebSocketBufferConfig::default())
    }
}

impl RewindGate {
    pub(super) fn new(config: WebSocketBufferConfig) -> Self {
        Self {
            buffered: Vec::new(),
            buffered_bytes: 0,
            max_messages: config.channel_capacity(),
            max_bytes: config.limit.byte_limit(),
            overflowed: false,
        }
    }

    pub(super) fn try_buffer(
        &mut self,
        message: BufferedRewindMessage,
        message_bytes: usize,
    ) -> Result<(), RewindGateOverflow> {
        debug_assert!(!self.overflowed, "invalid rewind gate accepted new work");

        if self.buffered.len() >= self.max_messages {
            return Err(self.invalidate(RewindGateOverflow {
                limit_kind: RewindGateLimit::Messages,
                limit: self.max_messages,
                buffered_messages: self.buffered.len(),
                buffered_bytes: self.buffered_bytes,
                incoming_message_bytes: message_bytes,
            }));
        }

        let Some(next_buffered_bytes) = self.buffered_bytes.checked_add(message_bytes) else {
            return Err(self.invalidate(RewindGateOverflow {
                limit_kind: RewindGateLimit::Bytes,
                limit: self.max_bytes.unwrap_or(usize::MAX),
                buffered_messages: self.buffered.len(),
                buffered_bytes: self.buffered_bytes,
                incoming_message_bytes: message_bytes,
            }));
        };
        if let Some(max_bytes) = self.max_bytes
            && next_buffered_bytes > max_bytes
        {
            return Err(self.invalidate(RewindGateOverflow {
                limit_kind: RewindGateLimit::Bytes,
                limit: max_bytes,
                buffered_messages: self.buffered.len(),
                buffered_bytes: self.buffered_bytes,
                incoming_message_bytes: message_bytes,
            }));
        }

        self.buffered.push(message);
        self.buffered_bytes = next_buffered_bytes;
        Ok(())
    }

    pub(super) fn drain(&mut self) -> Vec<BufferedRewindMessage> {
        self.buffered_bytes = 0;
        std::mem::take(&mut self.buffered)
    }

    pub(super) fn is_overflowed(&self) -> bool {
        self.overflowed
    }

    pub(super) fn fail_closed(&mut self) {
        self.buffered.clear();
        self.buffered_bytes = 0;
        self.overflowed = true;
    }

    fn invalidate(&mut self, overflow: RewindGateOverflow) -> RewindGateOverflow {
        self.fail_closed();
        overflow
    }
}

pub(super) type MessageChannelFlavor = mpsc::Array<Message>;
pub(super) type MessageSenderHandle = crossfire::MAsyncTx<MessageChannelFlavor>;
pub(super) type SizedMessageChannelFlavor = mpsc::Array<SizedMessage>;
pub type SizedMessageSenderHandle = crossfire::MAsyncTx<SizedMessageChannelFlavor>;
pub(super) type SizedMessageReceiverHandle = crossfire::AsyncRx<SizedMessageChannelFlavor>;

impl SizedMessage {
    #[inline]
    pub fn new(bytes: Bytes) -> Self {
        let size = bytes.len();
        Self { bytes, size }
    }
}
