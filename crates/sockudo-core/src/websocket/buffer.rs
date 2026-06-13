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

#[derive(Debug, Default)]
pub struct RewindGate {
    pub buffered: Vec<BufferedRewindMessage>,
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
