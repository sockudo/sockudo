#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferLimit {
    Messages(usize),
    Bytes(usize),
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
