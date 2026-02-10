#![allow(async_fn_in_trait)]

// src/websocket.rs
use crate::app::config::App;
use crate::channel::PresenceMemberInfo;
use crate::error::{Error, Result};
#[cfg(feature = "tag-filtering")]
use crate::filter::FilterNode;
use crate::protocol::messages::PusherMessage;
use bytes::Bytes;
use dashmap::DashMap;
use fastwebsockets::{Frame, Payload, WebSocketWrite};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::io::WriteHalf;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

// Re-export FilterNode for convenience when feature is enabled
#[cfg(feature = "tag-filtering")]
pub use crate::filter::FilterNode as SubscriptionFilter;

// When tag-filtering is disabled, use a unit type placeholder
#[cfg(not(feature = "tag-filtering"))]
pub type SubscriptionFilter = ();

/// Buffer limit strategy for WebSocket connections
/// Supports message count, byte size, both (whichever triggers first), or unlimited
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferLimit {
    /// Limit by number of messages only (fastest, no size tracking)
    Messages(usize),
    /// Limit by total bytes only (tracks cumulative size)
    Bytes(usize),
    /// Limit by both - whichever triggers first (most precise)
    Both { messages: usize, bytes: usize },
    /// No limits - use unbounded channel (use with caution, can cause memory exhaustion)
    Unlimited,
}

impl Default for BufferLimit {
    fn default() -> Self {
        BufferLimit::Messages(1000)
    }
}

impl BufferLimit {
    /// Get the channel capacity to use (message count)
    /// For byte-only limits, uses a large default capacity
    /// For unlimited, returns None to indicate unbounded channel should be used
    #[inline]
    pub fn channel_capacity(&self) -> Option<usize> {
        match self {
            BufferLimit::Messages(n) => Some(*n),
            BufferLimit::Bytes(_) => Some(10_000),
            BufferLimit::Both { messages, .. } => Some(*messages),
            BufferLimit::Unlimited => None,
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
            BufferLimit::Unlimited => None,
        }
    }

    #[inline]
    pub fn message_limit(&self) -> Option<usize> {
        match self {
            BufferLimit::Messages(n) => Some(*n),
            BufferLimit::Bytes(_) => None,
            BufferLimit::Both { messages, .. } => Some(*messages),
            BufferLimit::Unlimited => None,
        }
    }

    #[inline]
    pub fn is_unlimited(&self) -> bool {
        matches!(self, BufferLimit::Unlimited)
    }
}

/// Configuration for WebSocket connection buffers
/// Controls backpressure handling for slow consumers
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
    pub fn channel_capacity(&self) -> Option<usize> {
        self.limit.channel_capacity()
    }

    #[inline]
    pub fn is_unlimited(&self) -> bool {
        self.limit.is_unlimited()
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

impl SizedMessage {
    #[inline]
    pub fn new(bytes: Bytes) -> Self {
        let size = bytes.len();
        Self { bytes, size }
    }
}

/// Sender abstraction that supports both bounded and unbounded channels
#[derive(Clone)]
pub enum BroadcastSender {
    Bounded(mpsc::Sender<SizedMessage>),
    Unbounded(mpsc::UnboundedSender<SizedMessage>),
}

impl BroadcastSender {
    #[inline]
    pub fn try_send(
        &self,
        msg: SizedMessage,
    ) -> std::result::Result<(), TrySendError<SizedMessage>> {
        match self {
            BroadcastSender::Bounded(sender) => sender.try_send(msg),
            BroadcastSender::Unbounded(sender) => sender
                .send(msg)
                .map_err(|SendError(msg)| TrySendError::Closed(msg)),
        }
    }

    #[inline]
    pub async fn send(
        &self,
        msg: SizedMessage,
    ) -> std::result::Result<(), SendError<SizedMessage>> {
        match self {
            BroadcastSender::Bounded(sender) => sender.send(msg).await,
            BroadcastSender::Unbounded(sender) => sender.send(msg).map_err(|e| SendError(e.0)),
        }
    }

    #[inline]
    pub fn is_unbounded(&self) -> bool {
        matches!(self, BroadcastSender::Unbounded(_))
    }
}

/// Receiver abstraction that supports both bounded and unbounded channels
pub enum BroadcastReceiver {
    Bounded(mpsc::Receiver<SizedMessage>),
    Unbounded(mpsc::UnboundedReceiver<SizedMessage>),
}

impl BroadcastReceiver {
    #[inline]
    pub async fn recv(&mut self) -> Option<SizedMessage> {
        match self {
            BroadcastReceiver::Bounded(receiver) => receiver.recv().await,
            BroadcastReceiver::Unbounded(receiver) => receiver.recv().await,
        }
    }
}

/// Create a broadcast channel pair based on buffer configuration
pub fn create_broadcast_channel(
    buffer_config: &WebSocketBufferConfig,
) -> (BroadcastSender, BroadcastReceiver) {
    if buffer_config.is_unlimited() {
        let (tx, rx) = mpsc::unbounded_channel::<SizedMessage>();
        (
            BroadcastSender::Unbounded(tx),
            BroadcastReceiver::Unbounded(rx),
        )
    } else {
        let capacity = buffer_config.channel_capacity().unwrap_or(1000);
        let (tx, rx) = mpsc::channel::<SizedMessage>(capacity);
        (BroadcastSender::Bounded(tx), BroadcastReceiver::Bounded(rx))
    }
}

/// Zero-copy SocketId using (u64, u64) for ultra-fast cloning.
/// Format: "high.low" for backward-compatible display/serialization.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct SocketId {
    pub high: u64,
    pub low: u64,
}

impl std::fmt::Display for SocketId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.high, self.low)
    }
}

impl Serialize for SocketId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{}.{}", self.high, self.low))
    }
}

impl<'de> Deserialize<'de> for SocketId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl std::str::FromStr for SocketId {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() == 2 {
            if let (Ok(high), Ok(low)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
                return Ok(SocketId { high, low });
            }
        }

        // Fallback: Hash the string to create a deterministic ID for backward compatibility
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        let high = hasher.finish();

        let mut hasher = DefaultHasher::new();
        (s.as_bytes()).hash(&mut hasher);
        hasher.write_u8(0xFF);
        let low = hasher.finish();

        Ok(SocketId { high, low })
    }
}

impl Default for SocketId {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq<String> for SocketId {
    fn eq(&self, other: &String) -> bool {
        self.to_string() == *other
    }
}

impl SocketId {
    pub fn new() -> Self {
        let mut rng = rand::rng();
        let max: u64 = 10_000_000_000;
        SocketId {
            high: rng.random_range(0..=max),
            low: rng.random_range(0..=max),
        }
    }

    /// Create from string for backward compatibility (e.g., parsing from API requests)
    pub fn from_string(s: &str) -> std::result::Result<Self, String> {
        s.parse()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserInfo {
    pub id: String,
    pub watchlist: Option<Vec<String>>,
    pub info: Option<Value>,
}

#[derive(Debug)]
pub struct ConnectionTimeouts {
    pub activity_timeout_handle: Option<JoinHandle<()>>,
    pub auth_timeout_handle: Option<JoinHandle<()>>,
}

impl Default for ConnectionTimeouts {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionTimeouts {
    pub fn new() -> Self {
        Self {
            activity_timeout_handle: None,
            auth_timeout_handle: None,
        }
    }

    pub fn clear_activity_timeout(&mut self) {
        if let Some(handle) = self.activity_timeout_handle.take() {
            handle.abort();
        }
    }

    pub fn clear_auth_timeout(&mut self) {
        if let Some(handle) = self.auth_timeout_handle.take() {
            handle.abort();
        }
    }

    pub fn clear_all(&mut self) {
        self.clear_activity_timeout();
        self.clear_auth_timeout();
    }
}

impl Drop for ConnectionTimeouts {
    fn drop(&mut self) {
        self.clear_all();
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectionStatus {
    Active,
    PingSent(Instant),
    Closing,
    Closed,
}

/// Subscription filter type - uses FilterNode when tag-filtering is enabled,
/// or unit type when disabled. This allows the ConnectionState to compile
/// regardless of feature flags.
#[cfg(feature = "tag-filtering")]
type ChannelFilter = Option<FilterNode>;

#[cfg(not(feature = "tag-filtering"))]
type ChannelFilter = ();

#[derive(Debug)]
pub struct ConnectionState {
    pub socket_id: SocketId,
    pub app: Option<App>,
    pub subscribed_channels: HashMap<String, ChannelFilter>,
    pub user_id: Option<String>,
    pub user_info: Option<UserInfo>,
    pub last_ping: Instant,
    pub presence: Option<HashMap<String, PresenceMemberInfo>>,
    pub user: Option<Value>,
    pub timeouts: ConnectionTimeouts,
    pub status: ConnectionStatus,
    pub disconnecting: bool,
    pub delta_compression_enabled: bool,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionState {
    pub fn new() -> Self {
        Self {
            socket_id: SocketId::new(),
            app: None,
            subscribed_channels: HashMap::new(),
            user_id: None,
            user_info: None,
            last_ping: Instant::now(),
            presence: None,
            user: None,
            timeouts: ConnectionTimeouts::new(),
            status: ConnectionStatus::Active,
            disconnecting: false,
            delta_compression_enabled: false,
        }
    }

    pub fn with_socket_id(socket_id: SocketId) -> Self {
        Self {
            socket_id,
            app: None,
            subscribed_channels: HashMap::new(),
            user_id: None,
            user_info: None,
            last_ping: Instant::now(),
            presence: None,
            user: None,
            timeouts: ConnectionTimeouts::new(),
            status: ConnectionStatus::Active,
            disconnecting: false,
            delta_compression_enabled: false,
        }
    }

    pub fn is_presence(&self) -> bool {
        self.presence.is_some()
    }

    pub fn is_subscribed(&self, channel: &str) -> bool {
        self.subscribed_channels.contains_key(channel)
    }

    pub fn add_subscription(&mut self, channel: String) {
        #[cfg(feature = "tag-filtering")]
        {
            self.subscribed_channels.insert(channel, None);
        }
        #[cfg(not(feature = "tag-filtering"))]
        {
            self.subscribed_channels.insert(channel, ());
        }
    }

    #[cfg(feature = "tag-filtering")]
    pub fn add_subscription_with_filter(&mut self, channel: String, filter: Option<FilterNode>) {
        self.subscribed_channels.insert(channel, filter);
    }

    #[cfg(feature = "tag-filtering")]
    pub fn get_channel_filter(&self, channel: &str) -> Option<&FilterNode> {
        self.subscribed_channels
            .get(channel)
            .and_then(|f| f.as_ref())
    }

    pub fn remove_subscription(&mut self, channel: &str) -> bool {
        self.subscribed_channels.remove(channel).is_some()
    }

    pub fn get_subscribed_channels_list(&self) -> Vec<String> {
        self.subscribed_channels.keys().cloned().collect()
    }

    pub fn update_ping(&mut self) {
        self.last_ping = Instant::now();
    }

    pub fn get_app_key(&self) -> String {
        self.app
            .as_ref()
            .map(|app| app.key.clone())
            .unwrap_or_default()
    }

    pub fn get_app_id(&self) -> String {
        self.app
            .as_ref()
            .map(|app| app.id.clone())
            .unwrap_or_default()
    }

    pub fn time_since_last_ping(&self) -> std::time::Duration {
        self.last_ping.elapsed()
    }

    pub fn is_authenticated(&self) -> bool {
        self.user.is_some()
    }

    pub fn clear_timeouts(&mut self) {
        self.timeouts.clear_all();
    }
}

impl PartialEq for ConnectionState {
    fn eq(&self, other: &Self) -> bool {
        self.socket_id == other.socket_id
    }
}

// Message sender for async message handling
#[derive(Debug)]
pub struct MessageSender {
    sender: mpsc::Sender<Frame<'static>>,
    _receiver_handle: JoinHandle<()>,
}

#[derive(Debug, Clone, Copy)]
enum SocketOperation {
    WriteFrame,
    SendCloseFrame,
}

impl std::fmt::Display for SocketOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SocketOperation::WriteFrame => write!(f, "write frame to WebSocket"),
            SocketOperation::SendCloseFrame => write!(f, "send close frame"),
        }
    }
}

impl SocketOperation {
    fn is_close_operation(&self) -> bool {
        matches!(self, SocketOperation::SendCloseFrame)
    }
}

impl MessageSender {
    pub fn new_with_broadcast(
        mut socket: WebSocketWrite<WriteHalf<TokioIo<Upgraded>>>,
        mut broadcast_rx: BroadcastReceiver,
        buffer_capacity: usize,
        byte_counter: Option<Arc<ByteCounter>>,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel::<Frame<'static>>(buffer_capacity);

        let receiver_handle = tokio::spawn(async move {
            let mut frame_count = 0;
            let mut is_shutting_down = false;

            loop {
                tokio::select! {
                    // Priority for broadcasts
                    biased;

                    Some(sized_msg) = broadcast_rx.recv() => {
                        frame_count += 1;
                        let msg_size = sized_msg.size;
                        let frame = Frame::text(Payload::from(sized_msg.bytes.as_ref()));

                        if let Err(e) = socket.write_frame(frame).await {
                            Self::log_connection_error(
                                &e,
                                SocketOperation::WriteFrame,
                                frame_count,
                                is_shutting_down,
                            );
                            break;
                        }

                        // Decrement byte counter after successful write
                        if let Some(ref counter) = byte_counter {
                            counter.sub(msg_size);
                        }
                    }
                    // Regular messages
                    Some(frame) = receiver.recv() => {
                        frame_count += 1;

                        if matches!(frame.opcode, fastwebsockets::OpCode::Close) {
                            is_shutting_down = true;
                        }

                        if let Err(e) = socket.write_frame(frame).await {
                            Self::log_connection_error(
                                &e,
                                SocketOperation::WriteFrame,
                                frame_count,
                                is_shutting_down,
                            );
                            break;
                        }
                    }
                    else => break,
                }
            }

            // Try to close gracefully
            if let Err(e) = socket
                .write_frame(Frame::close(1000, b"Normal closure"))
                .await
            {
                Self::log_connection_error(&e, SocketOperation::SendCloseFrame, frame_count, true);
            }
        });

        Self {
            sender,
            _receiver_handle: receiver_handle,
        }
    }

    fn is_connection_error(error: &fastwebsockets::WebSocketError) -> bool {
        if let Some(source) = StdError::source(error) {
            if let Some(io_err) = source.downcast_ref::<std::io::Error>() {
                matches!(
                    io_err.kind(),
                    std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                )
            } else {
                false
            }
        } else {
            false
        }
    }

    fn log_connection_error(
        error: &fastwebsockets::WebSocketError,
        operation: SocketOperation,
        frame_count: usize,
        is_shutting_down: bool,
    ) {
        let is_conn_err = Self::is_connection_error(error);

        if is_conn_err && is_shutting_down {
            debug!("{} failed during shutdown (expected): {}", operation, error);
        } else if is_conn_err && frame_count <= 2 {
            warn!(
                "Early connection {} failed (after {} frames): {}",
                operation, frame_count, error
            );
        } else if is_conn_err {
            warn!(
                "Connection {} failed during operation (after {} frames): {}",
                operation, frame_count, error
            );
        } else if operation.is_close_operation() {
            warn!("Failed to {}: {}", operation, error);
        } else {
            error!("Failed to {}: {}", operation, error);
        }
    }

    pub fn new(
        mut socket: WebSocketWrite<WriteHalf<TokioIo<Upgraded>>>,
        buffer_capacity: usize,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel::<Frame<'static>>(buffer_capacity);

        let receiver_handle = tokio::spawn(async move {
            let mut frame_count = 0;
            let mut is_shutting_down = false;

            while let Some(frame) = receiver.recv().await {
                frame_count += 1;

                if matches!(frame.opcode, fastwebsockets::OpCode::Close) {
                    is_shutting_down = true;
                }

                if let Err(e) = socket.write_frame(frame).await {
                    Self::log_connection_error(
                        &e,
                        SocketOperation::WriteFrame,
                        frame_count,
                        is_shutting_down,
                    );
                    break;
                }
            }

            // Try to close gracefully
            if let Err(e) = socket
                .write_frame(Frame::close(1000, b"Normal closure"))
                .await
            {
                Self::log_connection_error(&e, SocketOperation::SendCloseFrame, frame_count, true);
            }
        });

        Self {
            sender,
            _receiver_handle: receiver_handle,
        }
    }

    pub fn try_send(
        &self,
        frame: Frame<'static>,
    ) -> std::result::Result<(), TrySendError<Frame<'static>>> {
        self.sender.try_send(frame)
    }

    pub fn send(&self, frame: Frame<'static>) -> Result<()> {
        self.sender.try_send(frame).map_err(|e| match e {
            TrySendError::Full(_) => Error::BufferFull("Message buffer is full".into()),
            TrySendError::Closed(_) => Error::Connection("Message channel closed".into()),
        })
    }

    pub fn send_json<T: serde::Serialize>(&self, message: &T) -> Result<()> {
        let payload = serde_json::to_vec(message)
            .map_err(|e| Error::InvalidMessageFormat(format!("Serialization failed: {e}")))?;

        let frame = Frame::text(Payload::from(payload));
        self.send(frame)
    }

    pub fn send_text(&self, text: String) -> Result<()> {
        let frame = Frame::text(Payload::from(text.into_bytes()));
        self.send(frame)
    }

    pub fn send_close(&self, code: u16, reason: &str) -> Result<()> {
        let frame = Frame::close(code, reason.as_bytes());
        self.send(frame)
    }
}

pub struct WebSocket {
    pub state: ConnectionState,
    pub message_sender: MessageSender,
    pub broadcast_tx: BroadcastSender,
    pub buffer_config: WebSocketBufferConfig,
    pub byte_counter: Option<Arc<ByteCounter>>,
}

impl WebSocket {
    pub fn new(socket_id: SocketId, socket: WebSocketWrite<WriteHalf<TokioIo<Upgraded>>>) -> Self {
        Self::with_buffer_config(socket_id, socket, WebSocketBufferConfig::default())
    }

    pub fn with_buffer_config(
        socket_id: SocketId,
        socket: WebSocketWrite<WriteHalf<TokioIo<Upgraded>>>,
        buffer_config: WebSocketBufferConfig,
    ) -> Self {
        let byte_counter = if buffer_config.tracks_bytes() {
            Some(Arc::new(ByteCounter::new()))
        } else {
            None
        };

        let (broadcast_tx, broadcast_rx) = create_broadcast_channel(&buffer_config);

        let channel_capacity = buffer_config.channel_capacity().unwrap_or(10_000);

        let message_sender = MessageSender::new_with_broadcast(
            socket,
            broadcast_rx,
            channel_capacity,
            byte_counter.clone(),
        );

        WebSocket {
            state: ConnectionState::with_socket_id(socket_id),
            message_sender,
            broadcast_tx,
            buffer_config,
            byte_counter,
        }
    }

    pub fn get_socket_id(&self) -> &SocketId {
        &self.state.socket_id
    }

    fn ensure_can_send(&self) -> Result<()> {
        if self.is_connected() {
            Ok(())
        } else {
            Err(Error::ConnectionClosed(
                "Cannot send message on closed connection".to_string(),
            ))
        }
    }

    pub async fn close(&mut self, code: u16, reason: String) -> Result<()> {
        match self.state.status {
            ConnectionStatus::Closing | ConnectionStatus::Closed => {
                debug!("Connection already closing or closed, skipping close frames");
                return Ok(());
            }
            _ => {}
        }

        self.state.status = ConnectionStatus::Closing;

        if code >= 4000 {
            let error_message = PusherMessage::error(code, reason.clone(), None);
            if let Err(e) = self.message_sender.send_json(&error_message) {
                warn!("Failed to send error message before close: {}", e);
            }
        }

        self.message_sender.send_close(code, &reason)?;
        self.state.clear_timeouts();
        self.state.status = ConnectionStatus::Closed;

        Ok(())
    }

    pub fn send_message(&self, message: &PusherMessage) -> Result<()> {
        self.ensure_can_send()?;
        self.message_sender.send_json(message)
    }

    pub fn send_text(&self, text: String) -> Result<()> {
        self.ensure_can_send()?;
        self.message_sender.send_text(text)
    }

    pub fn send_frame(&self, frame: Frame<'static>) -> Result<()> {
        self.message_sender.send(frame)
    }

    pub fn is_connected(&self) -> bool {
        matches!(
            self.state.status,
            ConnectionStatus::Active | ConnectionStatus::PingSent(_)
        )
    }

    pub fn update_activity(&mut self) {
        self.state.update_ping();
    }

    pub fn set_user_info(&mut self, user_info: UserInfo) {
        self.state.user_id = Some(user_info.id.clone());
        self.state.user_info = Some(user_info.clone());

        if let Some(info) = &user_info.info {
            self.state.user = Some(info.clone());
        }
    }

    pub fn add_presence_info(&mut self, channel: String, member_info: PresenceMemberInfo) {
        if self.state.presence.is_none() {
            self.state.presence = Some(HashMap::new());
        }

        if let Some(ref mut presence) = self.state.presence {
            presence.insert(channel, member_info);
        }
    }

    pub fn remove_presence_info(&mut self, channel: &str) -> Option<PresenceMemberInfo> {
        self.state.presence.as_mut()?.remove(channel)
    }

    pub fn subscribe_to_channel(&mut self, channel: String) {
        self.state.add_subscription(channel);
    }

    pub fn unsubscribe_from_channel(&mut self, channel: &str) -> bool {
        self.state.remove_subscription(channel)
    }

    pub fn is_subscribed_to(&self, channel: &str) -> bool {
        self.state.is_subscribed(channel)
    }

    pub fn get_subscribed_channels(&self) -> Vec<String> {
        self.state.subscribed_channels.keys().cloned().collect()
    }

    #[cfg(feature = "tag-filtering")]
    pub fn get_channel_filter(&self, channel: &str) -> Option<&FilterNode> {
        self.state.get_channel_filter(channel)
    }

    #[cfg(feature = "tag-filtering")]
    pub fn subscribe_to_channel_with_filter(
        &mut self,
        channel: String,
        filter: Option<FilterNode>,
    ) {
        self.state.add_subscription_with_filter(channel, filter);
    }
}

impl PartialEq for WebSocket {
    fn eq(&self, other: &Self) -> bool {
        self.state == other.state
    }
}

impl Hash for WebSocket {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.state.socket_id.hash(state);
    }
}

#[derive(Clone)]
pub struct WebSocketRef {
    pub broadcast_tx: BroadcastSender,

    #[cfg(feature = "tag-filtering")]
    pub channel_filters: Arc<DashMap<String, Option<Arc<FilterNode>>>>,

    pub socket_id: SocketId,
    pub buffer_config: WebSocketBufferConfig,
    pub byte_counter: Option<Arc<ByteCounter>>,
    pub inner: Arc<Mutex<WebSocket>>,
}

impl WebSocketRef {
    pub fn new(websocket: WebSocket) -> Self {
        let broadcast_tx = websocket.broadcast_tx.clone();
        let socket_id = *websocket.get_socket_id();
        let buffer_config = websocket.buffer_config;
        let byte_counter = websocket.byte_counter.clone();

        #[cfg(feature = "tag-filtering")]
        let channel_filters = {
            let filters = Arc::new(DashMap::new());
            for (channel, filter) in &websocket.state.subscribed_channels {
                filters.insert(channel.clone(), filter.clone().map(Arc::new));
            }
            filters
        };

        Self {
            broadcast_tx,
            #[cfg(feature = "tag-filtering")]
            channel_filters,
            socket_id,
            buffer_config,
            byte_counter,
            inner: Arc::new(Mutex::new(websocket)),
        }
    }

    /// Async broadcast send that waits for space if channel is full.
    /// Ensures 100% message delivery - no messages are ever dropped.
    #[inline]
    pub async fn send_broadcast(&self, bytes: Bytes) -> Result<()> {
        let msg_size = bytes.len();
        let sized_msg = SizedMessage::new(bytes);

        match self.broadcast_tx.send(sized_msg).await {
            Ok(()) => {
                if let Some(ref counter) = self.byte_counter {
                    counter.add(msg_size);
                }
                Ok(())
            }
            Err(_) => Err(Error::ConnectionClosed(
                "Broadcast channel closed".to_string(),
            )),
        }
    }

    pub fn buffer_stats(&self) -> BufferStats {
        BufferStats {
            pending_bytes: self.byte_counter.as_ref().map(|c| c.get()),
            byte_limit: self.buffer_config.limit.byte_limit(),
            message_limit: self.buffer_config.limit.message_limit(),
        }
    }

    pub async fn send_message(&self, message: &PusherMessage) -> Result<()> {
        let ws = self.inner.lock().await;
        ws.send_message(message)
    }

    pub async fn close(&self, code: u16, reason: String) -> Result<()> {
        let mut ws = self.inner.lock().await;
        ws.close(code, reason).await
    }

    /// Lock-free synchronous access to socket_id (for hot path)
    pub fn get_socket_id_sync(&self) -> &SocketId {
        &self.socket_id
    }

    pub async fn get_socket_id(&self) -> SocketId {
        self.socket_id
    }

    pub async fn is_subscribed_to(&self, channel: &str) -> bool {
        let ws = self.inner.lock().await;
        ws.is_subscribed_to(channel)
    }

    pub async fn get_user_id(&self) -> Option<String> {
        let ws = self.inner.lock().await;
        ws.state.user_id.clone()
    }

    pub async fn update_activity(&self) {
        let mut ws = self.inner.lock().await;
        ws.update_activity();
    }

    pub async fn subscribe_to_channel(&self, channel: String) {
        let mut ws = self.inner.lock().await;
        ws.subscribe_to_channel(channel.clone());

        #[cfg(feature = "tag-filtering")]
        {
            self.channel_filters.insert(channel, None);
        }
    }

    #[cfg(feature = "tag-filtering")]
    pub async fn subscribe_to_channel_with_filter(
        &self,
        channel: String,
        mut filter: Option<FilterNode>,
    ) {
        if let Some(ref mut f) = filter {
            f.optimize();
        }

        let mut ws = self.inner.lock().await;
        ws.subscribe_to_channel_with_filter(channel.clone(), filter.clone());
        self.channel_filters.insert(channel, filter.map(Arc::new));
    }

    pub async fn unsubscribe_from_channel(&self, channel: &str) -> bool {
        let mut ws = self.inner.lock().await;
        let result = ws.unsubscribe_from_channel(channel);

        #[cfg(feature = "tag-filtering")]
        {
            self.channel_filters.remove(channel);
        }

        result
    }

    #[cfg(feature = "tag-filtering")]
    pub async fn get_channel_filter(&self, channel: &str) -> Option<Arc<FilterNode>> {
        self.channel_filters
            .get(channel)
            .and_then(|entry| entry.value().clone())
    }

    #[cfg(feature = "tag-filtering")]
    pub fn get_channel_filter_sync(&self, channel: &str) -> Option<Arc<FilterNode>> {
        self.channel_filters
            .get(channel)
            .and_then(|entry| entry.value().clone())
    }
}

impl Hash for WebSocketRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let ptr = Arc::as_ptr(&self.inner) as *const () as usize;
        ptr.hash(state);
    }
}

impl PartialEq for WebSocketRef {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Eq for WebSocketRef {}

impl Debug for WebSocketRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketRef")
            .field("ptr", &Arc::as_ptr(&self.inner))
            .finish()
    }
}

// Helper trait for easier WebSocket operations
pub trait WebSocketExt {
    async fn send_pusher_message(&self, message: PusherMessage) -> Result<()>;
    async fn send_error(&self, code: u16, message: String, channel: Option<String>) -> Result<()>;
    async fn send_pong(&self) -> Result<()>;
}

impl WebSocketExt for WebSocketRef {
    async fn send_pusher_message(&self, message: PusherMessage) -> Result<()> {
        self.send_message(&message).await
    }

    async fn send_error(&self, code: u16, message: String, channel: Option<String>) -> Result<()> {
        let error_msg = PusherMessage::error(code, message, channel);
        self.send_message(&error_msg).await
    }

    async fn send_pong(&self) -> Result<()> {
        let pong_msg = PusherMessage::pong();
        self.send_message(&pong_msg).await
    }
}

/// Buffer usage statistics for monitoring
#[derive(Debug, Clone)]
pub struct BufferStats {
    pub pending_bytes: Option<usize>,
    pub byte_limit: Option<usize>,
    pub message_limit: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_socket_id_generation() {
        let id1 = SocketId::new();
        let id2 = SocketId::new();

        assert_ne!(id1, id2);
        let id1_str = id1.to_string();
        let id2_str = id2.to_string();
        assert!(id1_str.contains('.'));
        assert!(id2_str.contains('.'));
    }

    #[test]
    fn test_connection_state() {
        let mut state = ConnectionState::new();

        assert!(!state.is_subscribed("test-channel"));
        state.add_subscription("test-channel".to_string());
        assert!(state.is_subscribed("test-channel"));
        assert!(state.remove_subscription("test-channel"));
        assert!(!state.is_subscribed("test-channel"));
    }

    #[test]
    fn test_socket_id_display() {
        let id = SocketId::from_string("123.456").unwrap();
        assert_eq!(format!("{id}"), "123.456");
    }

    #[test]
    fn test_socket_id_copy() {
        let id1 = SocketId::new();
        let id2 = id1; // Copy, not move
        assert_eq!(id1, id2); // id1 still usable
    }

    #[test]
    fn test_socket_id_from_string() {
        let id = SocketId::from_string("12345.67890").unwrap();
        assert_eq!(id.high, 12345);
        assert_eq!(id.low, 67890);
        assert_eq!(id.to_string(), "12345.67890");
    }

    #[test]
    fn test_socket_id_from_arbitrary_string() {
        // Backward compatibility: arbitrary strings get hashed
        let id = SocketId::from_string("some-random-id").unwrap();
        let id2 = SocketId::from_string("some-random-id").unwrap();
        assert_eq!(id, id2); // Deterministic
    }

    #[test]
    fn test_buffer_limit_messages_only() {
        let limit = BufferLimit::Messages(1000);
        assert_eq!(limit.channel_capacity(), Some(1000));
        assert!(!limit.tracks_bytes());
        assert_eq!(limit.message_limit(), Some(1000));
        assert_eq!(limit.byte_limit(), None);
        assert!(!limit.is_unlimited());
    }

    #[test]
    fn test_buffer_limit_bytes_only() {
        let limit = BufferLimit::Bytes(1_048_576);
        assert_eq!(limit.channel_capacity(), Some(10_000));
        assert!(limit.tracks_bytes());
        assert_eq!(limit.message_limit(), None);
        assert_eq!(limit.byte_limit(), Some(1_048_576));
    }

    #[test]
    fn test_buffer_limit_both() {
        let limit = BufferLimit::Both {
            messages: 1000,
            bytes: 1_048_576,
        };
        assert_eq!(limit.channel_capacity(), Some(1000));
        assert!(limit.tracks_bytes());
        assert_eq!(limit.message_limit(), Some(1000));
        assert_eq!(limit.byte_limit(), Some(1_048_576));
    }

    #[test]
    fn test_buffer_limit_unlimited() {
        let limit = BufferLimit::Unlimited;
        assert_eq!(limit.channel_capacity(), None);
        assert!(!limit.tracks_bytes());
        assert!(limit.is_unlimited());
    }

    #[test]
    fn test_byte_counter_basic() {
        let counter = ByteCounter::new();
        assert_eq!(counter.get(), 0);

        assert_eq!(counter.add(100), 100);
        assert_eq!(counter.get(), 100);

        assert_eq!(counter.add(50), 150);
        assert_eq!(counter.get(), 150);

        counter.sub(30);
        assert_eq!(counter.get(), 120);
    }

    #[test]
    fn test_byte_counter_would_exceed() {
        let counter = ByteCounter::new();
        counter.add(900);

        assert!(!counter.would_exceed(100, 1000));
        assert!(counter.would_exceed(101, 1000));
    }

    #[test]
    fn test_broadcast_channel_bounded() {
        let config = WebSocketBufferConfig::with_message_limit(100, true);
        let (tx, _rx) = create_broadcast_channel(&config);
        assert!(!tx.is_unbounded());
    }

    #[test]
    fn test_broadcast_channel_unbounded() {
        let config = WebSocketBufferConfig {
            limit: BufferLimit::Unlimited,
            disconnect_on_full: false,
        };
        let (tx, _rx) = create_broadcast_channel(&config);
        assert!(tx.is_unbounded());
    }
}
