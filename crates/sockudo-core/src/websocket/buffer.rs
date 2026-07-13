use bytes::Bytes;
use crossfire::mpsc;
use sockudo_protocol::messages::PusherMessage;
use sockudo_ws::Message;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const DEFAULT_REWIND_GATE_MAX_MESSAGES: usize = 1_000;
const DEFAULT_REWIND_GATE_MAX_BYTES: usize = 16 * 1024 * 1024;

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

    #[inline]
    pub(crate) fn rewind_gate_limits(&self) -> (usize, usize) {
        (
            self.limit
                .message_limit()
                .unwrap_or(DEFAULT_REWIND_GATE_MAX_MESSAGES),
            self.limit
                .byte_limit()
                .unwrap_or(DEFAULT_REWIND_GATE_MAX_BYTES),
        )
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

    /// Atomically reserve `size` bytes without exceeding `limit`.
    ///
    /// Callers must release the reservation with [`Self::sub`] if the
    /// corresponding message is not enqueued.
    #[inline]
    pub fn try_reserve(&self, size: usize, limit: usize) -> bool {
        self.bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(size).filter(|next| *next <= limit)
            })
            .is_ok()
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
    pub message: Arc<PusherMessage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RewindGateOverflow {
    pub messages: usize,
    pub bytes: usize,
    pub max_messages: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RewindGateState {
    Open,
    Closed,
    Overflowed(RewindGateOverflow),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RewindGateAdmission {
    Buffered,
    Closed,
    Overflowed(RewindGateOverflow),
}

#[derive(Debug)]
pub(crate) enum RewindGateDrain {
    Buffered(Vec<BufferedRewindMessage>),
    Overflowed(RewindGateOverflow),
}

#[derive(Debug)]
pub struct RewindGate {
    buffered: Vec<BufferedRewindMessage>,
    buffered_bytes: usize,
    max_messages: usize,
    max_bytes: usize,
    state: RewindGateState,
}

impl Default for RewindGate {
    fn default() -> Self {
        Self::new(
            DEFAULT_REWIND_GATE_MAX_MESSAGES,
            DEFAULT_REWIND_GATE_MAX_BYTES,
        )
    }
}

impl RewindGate {
    pub(crate) fn new(max_messages: usize, max_bytes: usize) -> Self {
        Self {
            buffered: Vec::with_capacity(max_messages.min(64)),
            buffered_bytes: 0,
            max_messages,
            max_bytes,
            state: RewindGateState::Open,
        }
    }

    pub(crate) fn try_buffer(
        &mut self,
        message: BufferedRewindMessage,
        message_size: usize,
    ) -> RewindGateAdmission {
        match self.state {
            RewindGateState::Closed => return RewindGateAdmission::Closed,
            RewindGateState::Overflowed(overflow) => {
                return RewindGateAdmission::Overflowed(overflow);
            }
            RewindGateState::Open => {}
        }

        let messages = self.buffered.len().saturating_add(1);
        let bytes = self.buffered_bytes.saturating_add(message_size);
        if messages > self.max_messages || bytes > self.max_bytes {
            let overflow = RewindGateOverflow {
                messages,
                bytes,
                max_messages: self.max_messages,
                max_bytes: self.max_bytes,
            };
            self.buffered.clear();
            self.buffered_bytes = 0;
            self.state = RewindGateState::Overflowed(overflow);
            return RewindGateAdmission::Overflowed(overflow);
        }

        self.buffered.push(message);
        self.buffered_bytes = bytes;
        RewindGateAdmission::Buffered
    }

    pub(crate) fn finish(&mut self) -> RewindGateDrain {
        let state = std::mem::replace(&mut self.state, RewindGateState::Closed);
        self.buffered_bytes = 0;
        match state {
            RewindGateState::Open => RewindGateDrain::Buffered(std::mem::take(&mut self.buffered)),
            RewindGateState::Closed => RewindGateDrain::Buffered(Vec::new()),
            RewindGateState::Overflowed(overflow) => {
                self.buffered.clear();
                RewindGateDrain::Overflowed(overflow)
            }
        }
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
