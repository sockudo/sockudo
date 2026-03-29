#![allow(async_fn_in_trait)]

use crate::app::App;
use crate::channel::PresenceMemberInfo;
use crate::error::{Error, Result};
use crate::utils::wildcard_pattern_matches;
use ahash::AHashMap as HashMap;
use bytes::Bytes;
use crossfire::{TrySendError, mpsc};
use dashmap::DashMap;
use rand::Rng;
use serde::{Deserialize, Serialize};
use sockudo_filter::FilterNode;
use sockudo_protocol::messages::PusherMessage;
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sockudo_ws::Message;
use sockudo_ws::axum_integration::WebSocketWriter;
use sonic_rs::Value;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

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

type MessageChannelFlavor = mpsc::Array<Message>;
type MessageSenderHandle = crossfire::MAsyncTx<MessageChannelFlavor>;
type SizedMessageChannelFlavor = mpsc::Array<SizedMessage>;
type SizedMessageSenderHandle = crossfire::MAsyncTx<SizedMessageChannelFlavor>;
type SizedMessageReceiverHandle = crossfire::AsyncRx<SizedMessageChannelFlavor>;

impl SizedMessage {
    #[inline]
    pub fn new(bytes: Bytes) -> Self {
        let size = bytes.len();
        Self { bytes, size }
    }
}

/// Zero-copy SocketId using (u64, u64) for ultra-fast cloning.
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
        if parts.len() == 2
            && let (Ok(high), Ok(low)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>())
        {
            return Ok(SocketId { high, low });
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
        other
            .parse::<SocketId>()
            .is_ok_and(|parsed| parsed == *self)
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

    pub fn from_string(s: &str) -> std::result::Result<Self, String> {
        s.parse()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserInfo {
    pub id: String,
    pub watchlist: Option<Vec<String>>,
    pub info: Option<Value>,
    pub capabilities: Option<ConnectionCapabilities>,
    pub meta: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(default)]
pub struct ConnectionCapabilities {
    pub subscribe: Option<Vec<String>>,
    pub publish: Option<Vec<String>>,
    pub presence: Option<Vec<String>>,
}

impl ConnectionCapabilities {
    fn matches_any(patterns: &[String], channel: &str) -> bool {
        patterns.iter().any(|pattern| {
            pattern == "*" || pattern == channel || wildcard_pattern_matches(channel, pattern)
        })
    }

    pub fn allows_subscribe(&self, channel: &str) -> bool {
        if channel.starts_with("presence-")
            && let Some(patterns) = self.presence.as_deref()
        {
            return Self::matches_any(patterns, channel);
        }

        self.subscribe
            .as_deref()
            .is_none_or(|patterns| Self::matches_any(patterns, channel))
    }

    pub fn allows_publish(&self, channel: &str) -> bool {
        self.publish
            .as_deref()
            .is_none_or(|patterns| Self::matches_any(patterns, channel))
    }
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

#[derive(Debug)]
pub struct ConnectionState {
    pub socket_id: SocketId,
    pub app: Option<App>,
    pub subscribed_channels: HashMap<String, Option<FilterNode>>,
    pub user_id: Option<String>,
    pub user_info: Option<UserInfo>,
    pub connection_capabilities: Option<ConnectionCapabilities>,
    pub connection_meta: Option<Value>,
    pub last_ping: Instant,
    pub presence: Option<HashMap<String, PresenceMemberInfo>>,
    pub user: Option<Value>,
    pub timeouts: ConnectionTimeouts,
    pub status: ConnectionStatus,
    pub disconnecting: bool,
    pub delta_compression_enabled: bool,
    pub protocol_version: ProtocolVersion,
    pub wire_format: WireFormat,
    /// V2 only. Whether the publisher receives their own messages back.
    /// Default: true (echo enabled). Set from sockudo:connect options.
    pub echo_messages: bool,
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
            connection_capabilities: None,
            connection_meta: None,
            last_ping: Instant::now(),
            presence: None,
            user: None,
            timeouts: ConnectionTimeouts::new(),
            status: ConnectionStatus::Active,
            disconnecting: false,
            delta_compression_enabled: false,
            protocol_version: ProtocolVersion::V1,
            wire_format: WireFormat::Json,
            echo_messages: true,
        }
    }

    pub fn with_socket_id(socket_id: SocketId) -> Self {
        Self {
            socket_id,
            app: None,
            subscribed_channels: HashMap::new(),
            user_id: None,
            user_info: None,
            connection_capabilities: None,
            connection_meta: None,
            last_ping: Instant::now(),
            presence: None,
            user: None,
            timeouts: ConnectionTimeouts::new(),
            status: ConnectionStatus::Active,
            disconnecting: false,
            delta_compression_enabled: false,
            protocol_version: ProtocolVersion::V1,
            wire_format: WireFormat::Json,
            echo_messages: true,
        }
    }

    pub fn with_protocol_version(mut self, version: ProtocolVersion) -> Self {
        self.protocol_version = version;
        self
    }

    pub fn with_wire_format(mut self, format: WireFormat) -> Self {
        self.wire_format = format;
        self
    }

    pub fn is_presence(&self) -> bool {
        self.presence.is_some()
    }

    pub fn is_subscribed(&self, channel: &str) -> bool {
        self.subscribed_channels.contains_key(channel)
    }

    pub fn add_subscription(&mut self, channel: String) {
        self.subscribed_channels.insert(channel, None);
    }

    pub fn add_subscription_with_filter(&mut self, channel: String, filter: Option<FilterNode>) {
        self.subscribed_channels.insert(channel, filter);
    }

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
    sender: MessageSenderHandle,
    receiver_handle: Option<JoinHandle<()>>,
}

impl Drop for MessageSender {
    fn drop(&mut self) {
        if let Some(handle) = self.receiver_handle.take() {
            handle.abort();
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SocketOperation {
    WriteFrame,
    SendCloseFrame,
}

impl std::fmt::Display for SocketOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SocketOperation::WriteFrame => write!(f, "write message to WebSocket"),
            SocketOperation::SendCloseFrame => write!(f, "send close message"),
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
        mut socket: WebSocketWriter,
        broadcast_rx: SizedMessageReceiverHandle,
        buffer_capacity: usize,
        byte_counter: Option<Arc<ByteCounter>>,
        shutdown_token: CancellationToken,
    ) -> Self {
        let (sender, receiver) = mpsc::bounded_async::<Message>(buffer_capacity);

        let receiver_handle = tokio::spawn(async move {
            let mut msg_count = 0;
            let mut is_shutting_down = false;
            let mut broadcast_closed = false;
            let mut receiver_closed = false;

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_token.cancelled() => {
                        debug!("Receiver task shutting down via cancellation token");
                        break;
                    }
                    recv_result = broadcast_rx.recv(), if !broadcast_closed => {
                        match recv_result {
                            Ok(sized_msg) => {
                                msg_count += 1;
                                let msg_size = sized_msg.size;
                                let msg = Message::Text(sized_msg.bytes);

                                if let Err(e) = socket.send(msg).await {
                                    Self::log_connection_error(
                                        &e,
                                        SocketOperation::WriteFrame,
                                        msg_count,
                                        is_shutting_down,
                                    );
                                    break;
                                }

                                if let Some(ref counter) = byte_counter {
                                    counter.sub(msg_size);
                                }
                            }
                            Err(_) => {
                                broadcast_closed = true;
                            }
                        }
                    }
                    recv_result = receiver.recv(), if !receiver_closed => {
                        match recv_result {
                            Ok(message) => {
                                msg_count += 1;

                                if matches!(message, Message::Close(_)) {
                                    is_shutting_down = true;
                                }

                                if let Err(e) = socket.send(message).await {
                                    Self::log_connection_error(
                                        &e,
                                        SocketOperation::WriteFrame,
                                        msg_count,
                                        is_shutting_down,
                                    );
                                    break;
                                }
                            }
                            Err(_) => {
                                receiver_closed = true;
                            }
                        }
                    }
                    else => break,
                }
            }

            if let Err(e) = socket.close(1000, "Normal closure").await {
                Self::log_connection_error(&e, SocketOperation::SendCloseFrame, msg_count, true);
            }
        });

        Self {
            sender,
            receiver_handle: Some(receiver_handle),
        }
    }

    fn is_connection_error(error: &sockudo_ws::Error) -> bool {
        matches!(
            error,
            sockudo_ws::Error::ConnectionClosed
                | sockudo_ws::Error::ConnectionReset
                | sockudo_ws::Error::Closed(_)
                | sockudo_ws::Error::Io(_)
        )
    }

    fn log_connection_error(
        error: &sockudo_ws::Error,
        operation: SocketOperation,
        msg_count: usize,
        is_shutting_down: bool,
    ) {
        let is_conn_err = Self::is_connection_error(error);

        if is_conn_err && is_shutting_down {
            debug!("{} failed during shutdown (expected): {}", operation, error);
        } else if is_conn_err && msg_count <= 2 {
            warn!(
                "Early connection {} failed (after {} messages): {}",
                operation, msg_count, error
            );
        } else if is_conn_err {
            warn!(
                "Connection {} failed during operation (after {} messages): {}",
                operation, msg_count, error
            );
        } else if operation.is_close_operation() {
            warn!("Failed to {}: {}", operation, error);
        } else {
            error!("Failed to {}: {}", operation, error);
        }
    }

    pub fn new(mut socket: WebSocketWriter, buffer_capacity: usize) -> Self {
        let (sender, receiver) = mpsc::bounded_async::<Message>(buffer_capacity);

        let receiver_handle = tokio::spawn(async move {
            let mut msg_count = 0;
            let mut is_shutting_down = false;

            while let Ok(message) = receiver.recv().await {
                msg_count += 1;

                if matches!(message, Message::Close(_)) {
                    is_shutting_down = true;
                }

                if let Err(e) = socket.send(message).await {
                    Self::log_connection_error(
                        &e,
                        SocketOperation::WriteFrame,
                        msg_count,
                        is_shutting_down,
                    );
                    break;
                }
            }

            if let Err(e) = socket.close(1000, "Normal closure").await {
                Self::log_connection_error(&e, SocketOperation::SendCloseFrame, msg_count, true);
            }
        });

        Self {
            sender,
            receiver_handle: Some(receiver_handle),
        }
    }

    pub fn try_send(&self, message: Message) -> std::result::Result<(), TrySendError<Message>> {
        self.sender.try_send(message)
    }

    pub fn send(&self, message: Message) -> Result<()> {
        self.sender.try_send(message).map_err(|e| match e {
            TrySendError::Full(_) => Error::BufferFull("Message buffer is full".into()),
            TrySendError::Disconnected(_) => Error::Connection("Message channel closed".into()),
        })
    }

    pub fn send_json<T: serde::Serialize>(&self, message: &T) -> Result<()> {
        let payload = sonic_rs::to_string(message)
            .map_err(|e| Error::InvalidMessageFormat(format!("Serialization failed: {e}")))?;

        self.send(Message::text(payload))
    }

    pub fn send_text(&self, text: String) -> Result<()> {
        self.send(Message::text(text))
    }

    pub fn send_close(&self, code: u16, reason: &str) -> Result<()> {
        self.send(Message::Close(Some(sockudo_ws::error::CloseReason::new(
            code, reason,
        ))))
    }
}

pub struct WebSocket {
    pub state: ConnectionState,
    pub message_sender: MessageSender,
    pub broadcast_tx: SizedMessageSenderHandle,
    pub buffer_config: WebSocketBufferConfig,
    pub byte_counter: Option<Arc<ByteCounter>>,
    pub shutdown_token: CancellationToken,
}

impl WebSocket {
    pub fn new(socket_id: SocketId, socket: WebSocketWriter) -> Self {
        Self::with_buffer_config(socket_id, socket, WebSocketBufferConfig::default())
    }

    pub fn with_buffer_config(
        socket_id: SocketId,
        socket: WebSocketWriter,
        buffer_config: WebSocketBufferConfig,
    ) -> Self {
        let byte_counter = if buffer_config.tracks_bytes() {
            Some(Arc::new(ByteCounter::new()))
        } else {
            None
        };

        let channel_capacity = buffer_config.channel_capacity();
        let (broadcast_tx, broadcast_rx) = mpsc::bounded_async::<SizedMessage>(channel_capacity);
        let shutdown_token = CancellationToken::new();

        let message_sender = MessageSender::new_with_broadcast(
            socket,
            broadcast_rx,
            channel_capacity,
            byte_counter.clone(),
            shutdown_token.clone(),
        );

        WebSocket {
            state: ConnectionState::with_socket_id(socket_id),
            message_sender,
            broadcast_tx,
            buffer_config,
            byte_counter,
            shutdown_token,
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
            if let Err(e) = self.send_message(&error_message) {
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
        let payload = sockudo_protocol::wire::serialize_message(message, self.state.wire_format)
            .map_err(|e| Error::InvalidMessageFormat(format!("Serialization failed: {e}")))?;
        if self.state.wire_format.is_binary() {
            self.message_sender
                .send(Message::Binary(Bytes::from(payload)))
        } else {
            self.message_sender
                .send(Message::text(String::from_utf8(payload).map_err(|e| {
                    Error::InvalidMessageFormat(format!("JSON payload is not UTF-8: {e}"))
                })?))
        }
    }

    pub fn send_text(&self, text: String) -> Result<()> {
        self.ensure_can_send()?;
        self.message_sender.send_text(text)
    }

    pub fn send_frame(&self, message: Message) -> Result<()> {
        self.message_sender.send(message)
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
        self.state.connection_capabilities = user_info.capabilities.clone();
        self.state.connection_meta = user_info.meta.clone();
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

    pub fn get_channel_filter(&self, channel: &str) -> Option<&FilterNode> {
        self.state.get_channel_filter(channel)
    }

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
    pub broadcast_tx: SizedMessageSenderHandle,
    pub channel_filters: Arc<DashMap<String, Option<Arc<FilterNode>>>>,
    /// V2 event name filters per channel. None = receive all events.
    pub event_name_filters: Arc<DashMap<String, Option<Vec<String>>>>,
    pub socket_id: SocketId,
    pub buffer_config: WebSocketBufferConfig,
    pub byte_counter: Option<Arc<ByteCounter>>,
    pub shutdown_token: CancellationToken,
    pub inner: Arc<Mutex<WebSocket>>,
    pub protocol_version: ProtocolVersion,
    pub wire_format: WireFormat,
    /// V2 only. Connection-level echo setting. Default: true.
    pub echo_messages: bool,
}

impl WebSocketRef {
    pub fn new(websocket: WebSocket) -> Self {
        let broadcast_tx = websocket.broadcast_tx.clone();
        let socket_id = *websocket.get_socket_id();
        let buffer_config = websocket.buffer_config;
        let byte_counter = websocket.byte_counter.clone();
        let shutdown_token = websocket.shutdown_token.clone();
        let protocol_version = websocket.state.protocol_version;
        let wire_format = websocket.state.wire_format;
        let echo_messages = websocket.state.echo_messages;

        let channel_filters = Arc::new(DashMap::new());
        for (channel, filter) in &websocket.state.subscribed_channels {
            channel_filters.insert(channel.clone(), filter.clone().map(Arc::new));
        }

        let event_name_filters = Arc::new(DashMap::new());

        Self {
            broadcast_tx,
            channel_filters,
            event_name_filters,
            socket_id,
            buffer_config,
            byte_counter,
            shutdown_token,
            protocol_version,
            wire_format,
            echo_messages,
            inner: Arc::new(Mutex::new(websocket)),
        }
    }

    #[inline]
    pub fn send_broadcast(&self, bytes: Bytes) -> Result<()> {
        let msg_size = bytes.len();

        if let Some(ref counter) = self.byte_counter
            && let Some(byte_limit) = self.buffer_config.limit.byte_limit()
            && counter.would_exceed(msg_size, byte_limit)
        {
            return self.handle_buffer_full("byte limit", byte_limit, Some(msg_size));
        }

        let sized_msg = SizedMessage::new(bytes);

        match self.broadcast_tx.try_send(sized_msg) {
            Ok(()) => {
                if let Some(ref counter) = self.byte_counter {
                    counter.add(msg_size);
                }
                Ok(())
            }
            Err(TrySendError::Full(_)) => {
                let limit = self.buffer_config.limit.message_limit().unwrap_or(0);
                self.handle_buffer_full("message limit", limit, None)
            }
            Err(TrySendError::Disconnected(_)) => Err(Error::ConnectionClosed(
                "Broadcast channel closed".to_string(),
            )),
        }
    }

    #[inline]
    fn handle_buffer_full(
        &self,
        limit_type: &str,
        limit_value: usize,
        msg_size: Option<usize>,
    ) -> Result<()> {
        if self.buffer_config.disconnect_on_full {
            let size_info = msg_size
                .map(|s| format!(", message size: {} bytes", s))
                .unwrap_or_default();
            Err(Error::BufferFull(format!(
                "Client buffer full ({}: {}{}), disconnecting slow consumer",
                limit_type, limit_value, size_info
            )))
        } else {
            warn!(
                socket_id = %self.socket_id,
                limit_type = limit_type,
                limit_value = limit_value,
                "Dropping message for slow consumer (buffer full)"
            );
            Ok(())
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

    /// Signal both reader and writer tasks to shut down.
    pub fn shutdown(&self) {
        self.shutdown_token.cancel();
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

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

    pub async fn get_connection_capabilities(&self) -> Option<ConnectionCapabilities> {
        let ws = self.inner.lock().await;
        ws.state.connection_capabilities.clone()
    }

    pub async fn get_connection_meta(&self) -> Option<Value> {
        let ws = self.inner.lock().await;
        ws.state.connection_meta.clone()
    }

    pub async fn update_activity(&self) {
        let mut ws = self.inner.lock().await;
        ws.update_activity();
    }

    pub async fn subscribe_to_channel(&self, channel: String) {
        let mut ws = self.inner.lock().await;
        ws.subscribe_to_channel(channel.clone());
        self.channel_filters.insert(channel.clone(), None);
        self.event_name_filters.insert(channel, None);
    }

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
        self.channel_filters
            .insert(channel.clone(), filter.map(Arc::new));
        self.event_name_filters.insert(channel, None);
    }

    /// Subscribe with both tag filter and event name filter (V2).
    pub async fn subscribe_to_channel_with_filters(
        &self,
        channel: String,
        mut tag_filter: Option<FilterNode>,
        event_name_filter: Option<Vec<String>>,
    ) {
        if let Some(ref mut f) = tag_filter {
            f.optimize();
        }

        let mut ws = self.inner.lock().await;
        ws.subscribe_to_channel_with_filter(channel.clone(), tag_filter.clone());
        self.channel_filters
            .insert(channel.clone(), tag_filter.map(Arc::new));
        self.event_name_filters.insert(channel, event_name_filter);
    }

    pub async fn unsubscribe_from_channel(&self, channel: &str) -> bool {
        let mut ws = self.inner.lock().await;
        let result = ws.unsubscribe_from_channel(channel);
        self.channel_filters.remove(channel);
        self.event_name_filters.remove(channel);
        result
    }

    pub async fn get_channel_filter(&self, channel: &str) -> Option<Arc<FilterNode>> {
        self.channel_filters
            .get(channel)
            .and_then(|entry| entry.value().clone())
    }

    pub fn get_channel_filter_sync(&self, channel: &str) -> Option<Arc<FilterNode>> {
        self.channel_filters
            .get(channel)
            .and_then(|entry| entry.value().clone())
    }

    /// Get the event name filter for a channel. Returns None if no filter (all events).
    pub fn get_event_name_filter_sync(&self, channel: &str) -> Option<Vec<String>> {
        self.event_name_filters
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
    fn test_connection_capabilities_allow_matching_channels() {
        let capabilities = ConnectionCapabilities {
            subscribe: Some(vec!["chat:*".to_string()]),
            publish: Some(vec!["private-chat:*".to_string()]),
            presence: Some(vec!["presence-chat:*".to_string()]),
        };

        assert!(capabilities.allows_subscribe("chat:room-1"));
        assert!(capabilities.allows_subscribe("presence-chat:room-1"));
        assert!(capabilities.allows_publish("private-chat:room-1"));
        assert!(!capabilities.allows_publish("private-news:room-1"));
    }

    #[test]
    fn test_connection_capabilities_default_to_unrestricted_when_missing() {
        let capabilities = ConnectionCapabilities::default();

        assert!(capabilities.allows_subscribe("chat:room-1"));
        assert!(capabilities.allows_publish("private-chat:room-1"));
    }

    #[test]
    fn test_socket_id_display() {
        let id = SocketId::from_string("123.456").unwrap();
        assert_eq!(format!("{id}"), "123.456");
    }

    #[test]
    fn test_buffer_limit_messages_only() {
        let limit = BufferLimit::Messages(1000);
        assert_eq!(limit.channel_capacity(), 1000);
        assert!(!limit.tracks_bytes());
        assert_eq!(limit.message_limit(), Some(1000));
        assert_eq!(limit.byte_limit(), None);
    }

    #[test]
    fn test_buffer_limit_bytes_only() {
        let limit = BufferLimit::Bytes(1_048_576);
        assert_eq!(limit.channel_capacity(), 10_000);
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
        assert_eq!(limit.channel_capacity(), 1000);
        assert!(limit.tracks_bytes());
        assert_eq!(limit.message_limit(), Some(1000));
        assert_eq!(limit.byte_limit(), Some(1_048_576));
    }

    #[test]
    fn test_websocket_buffer_config_default() {
        let config = WebSocketBufferConfig::default();
        assert_eq!(config.limit, BufferLimit::Messages(1000));
        assert!(config.disconnect_on_full);
        assert!(!config.tracks_bytes());
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
        assert!(counter.would_exceed(200, 1000));
    }

    #[test]
    fn test_sized_message() {
        let bytes = Bytes::from("hello world");
        let msg = SizedMessage::new(bytes.clone());
        assert_eq!(msg.size, 11);
        assert_eq!(msg.bytes, bytes);
    }

    #[test]
    fn test_websocket_buffer_config_message_limit() {
        let config = WebSocketBufferConfig::with_message_limit(500, false);
        assert_eq!(config.channel_capacity(), 500);
        assert!(!config.disconnect_on_full);
        assert!(!config.tracks_bytes());
    }

    #[test]
    fn test_websocket_buffer_config_byte_limit() {
        let config = WebSocketBufferConfig::with_byte_limit(1_048_576, true);
        assert_eq!(config.channel_capacity(), 10_000);
        assert!(config.disconnect_on_full);
        assert!(config.tracks_bytes());
    }

    #[test]
    fn test_websocket_buffer_config_both_limits() {
        let config = WebSocketBufferConfig::with_both_limits(1000, 1_048_576, true);
        assert_eq!(config.channel_capacity(), 1000);
        assert!(config.disconnect_on_full);
        assert!(config.tracks_bytes());
    }

    #[test]
    fn test_websocket_buffer_config_legacy_new() {
        let config = WebSocketBufferConfig::new(500, false);
        assert_eq!(config.channel_capacity(), 500);
        assert!(!config.disconnect_on_full);
    }
}
