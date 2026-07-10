use super::buffer::{
    BufferedRewindMessage, ByteCounter, MessageSenderHandle, RewindGate, SizedMessage,
    SizedMessageSenderHandle, WebSocketBufferConfig,
};
use super::capabilities::ConnectionCapabilities;
use super::connection::WebSocket;
use super::socket_id::SocketId;
use crate::capability_token::TokenAuthContext;
use crate::error::{Error, Result};
use bytes::Bytes;
use crossfire::TrySendError;
use dashmap::DashMap;
use sockudo_filter::FilterNode;
use sockudo_protocol::messages::PusherMessage;
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sonic_rs::Value;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::warn;

#[derive(Clone)]
pub struct WebSocketRef {
    pub broadcast_tx: SizedMessageSenderHandle,
    pub message_sender: MessageSenderHandle,
    pub channel_state: Arc<DashMap<Arc<str>, PerChannelState>>,
    pub socket_id: SocketId,
    pub buffer_config: WebSocketBufferConfig,
    pub byte_counter: Option<Arc<ByteCounter>>,
    pub shutdown_token: CancellationToken,
    // Set at the start of close(), before the close frame is queued, so no data
    // frame can slip in behind it. The shutdown_token is only cancelled after the
    // close frame is queued (cancelling earlier would make the writer task drop it).
    closing: Arc<std::sync::atomic::AtomicBool>,
    pub inner: Arc<Mutex<WebSocket>>,
    pub protocol_version: ProtocolVersion,
    pub wire_format: WireFormat,
    /// V2 only. Connection-level echo setting. Default: true.
    pub echo_messages: bool,
    /// V2 only. Live append frame mode for this socket.
    pub append_mode: sockudo_protocol::AppendMode,
}

#[derive(Default)]
pub struct PerChannelState {
    pub filter: Option<Arc<FilterNode>>,
    /// V2 event name filter. None = receive all events.
    pub event_name_filter: Option<Vec<String>>,
    /// V2 raw annotation delivery mode.
    pub annotation_subscribe: bool,
    /// V2 history head captured at subscription acknowledgement time.
    pub attach_serial: Option<u64>,
    pub rewind_gate: Option<Arc<Mutex<RewindGate>>>,
}

impl WebSocketRef {
    pub fn new(websocket: WebSocket) -> Self {
        let broadcast_tx = websocket.broadcast_tx.clone();
        let message_sender = websocket.message_sender.sender_handle();
        let socket_id = *websocket.get_socket_id();
        let buffer_config = websocket.buffer_config;
        let byte_counter = websocket.byte_counter.clone();
        let shutdown_token = websocket.shutdown_token.clone();
        let protocol_version = websocket.state.protocol_version;
        let wire_format = websocket.state.wire_format;
        let echo_messages = websocket.state.echo_messages;
        let append_mode = websocket.state.append_mode;

        let channel_state = Arc::new(DashMap::with_capacity(
            websocket.state.subscribed_channels.len(),
        ));
        for (channel, filter) in &websocket.state.subscribed_channels {
            channel_state.insert(
                Arc::<str>::from(channel.as_str()),
                PerChannelState {
                    filter: filter.clone().map(Arc::new),
                    ..PerChannelState::default()
                },
            );
        }

        Self {
            broadcast_tx,
            message_sender,
            channel_state,
            socket_id,
            buffer_config,
            byte_counter,
            shutdown_token,
            closing: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            protocol_version,
            wire_format,
            echo_messages,
            append_mode,
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

    pub fn send_message(&self, message: &PusherMessage) -> Result<()> {
        if self.closing.load(Ordering::Acquire) || self.shutdown_token.is_cancelled() {
            return Err(Error::ConnectionClosed("Connection shutting down".into()));
        }

        let payload = sockudo_protocol::wire::serialize_message(message, self.wire_format)
            .map_err(|e| Error::InvalidMessageFormat(format!("Serialization failed: {e}")))?;

        if self.wire_format.is_binary() {
            self.message_sender
                .try_send(sockudo_ws::Message::Binary(Bytes::from(payload)))
                .map_err(|e| match e {
                    TrySendError::Full(_) => Error::BufferFull("Message buffer full".into()),
                    TrySendError::Disconnected(_) => {
                        Error::ConnectionClosed("Channel closed".into())
                    }
                })
        } else {
            let text = String::from_utf8(payload).map_err(|e| {
                Error::InvalidMessageFormat(format!("JSON payload is not UTF-8: {e}"))
            })?;

            self.message_sender
                .try_send(sockudo_ws::Message::text(text))
                .map_err(|e| match e {
                    TrySendError::Full(_) => Error::BufferFull("Message buffer full".into()),
                    TrySendError::Disconnected(_) => {
                        Error::ConnectionClosed("Channel closed".into())
                    }
                })
        }
    }

    pub async fn close(&self, code: u16, reason: String) -> Result<()> {
        // Reject new sends before queueing the close frame so no data frame can be
        // enqueued behind it. Token cancellation must stay AFTER ws.close(): the
        // writer task breaks on cancellation and would drop the queued close frame.
        self.closing.store(true, Ordering::Release);
        let result = {
            let mut ws = self.inner.lock().await;
            ws.close(code, reason).await
        };
        self.shutdown_token.cancel();
        result
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

    pub async fn get_token_auth_context(&self) -> Option<TokenAuthContext> {
        let ws = self.inner.lock().await;
        ws.state.token_auth_context.clone()
    }

    pub async fn set_token_auth_context(&self, context: TokenAuthContext) {
        let mut ws = self.inner.lock().await;
        ws.set_token_auth_context(context);
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
        self.upsert_channel_state(Arc::<str>::from(channel), None, None, false);
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
        self.upsert_channel_state(Arc::<str>::from(channel), filter.map(Arc::new), None, false);
    }

    /// Subscribe with both tag filter and event name filter (V2).
    pub async fn subscribe_to_channel_with_filters(
        &self,
        channel: String,
        mut tag_filter: Option<FilterNode>,
        event_name_filter: Option<Vec<String>>,
        annotation_subscribe: bool,
    ) {
        if let Some(ref mut f) = tag_filter {
            f.optimize();
        }

        let mut ws = self.inner.lock().await;
        ws.subscribe_to_channel_with_filter(channel.clone(), tag_filter.clone());
        self.upsert_channel_state(
            Arc::<str>::from(channel),
            tag_filter.map(Arc::new),
            event_name_filter,
            annotation_subscribe,
        );
    }

    pub async fn unsubscribe_from_channel(&self, channel: &str) -> bool {
        let mut ws = self.inner.lock().await;
        let result = ws.unsubscribe_from_channel(channel);
        self.channel_state.remove(channel);
        result
    }

    pub async fn get_channel_filter(&self, channel: &str) -> Option<Arc<FilterNode>> {
        self.channel_state
            .get(channel)
            .and_then(|entry| entry.value().filter.clone())
    }

    pub fn get_channel_filter_sync(&self, channel: &str) -> Option<Arc<FilterNode>> {
        self.channel_state
            .get(channel)
            .and_then(|entry| entry.value().filter.clone())
    }

    /// Get the event name filter for a channel. Returns None if no filter (all events).
    pub fn get_event_name_filter_sync(&self, channel: &str) -> Option<Vec<String>> {
        self.channel_state
            .get(channel)
            .and_then(|entry| entry.value().event_name_filter.clone())
    }

    pub fn allows_annotation_events_sync(&self, channel: &str) -> bool {
        self.channel_state
            .get(channel)
            .is_some_and(|entry| entry.value().annotation_subscribe)
    }

    pub fn set_attach_serial(&self, channel: String, serial: u64) {
        self.channel_state
            .entry(Arc::<str>::from(channel))
            .or_default()
            .attach_serial = Some(serial);
    }

    pub fn attach_serial(&self, channel: &str) -> Option<u64> {
        self.channel_state
            .get(channel)
            .and_then(|entry| entry.value().attach_serial)
    }

    pub fn start_rewind_gate(&self, channel: String) {
        self.channel_state
            .entry(Arc::<str>::from(channel))
            .or_default()
            .rewind_gate = Some(Arc::new(Mutex::new(RewindGate::new(self.buffer_config))));
    }

    pub async fn buffer_rewind_message(&self, channel: &str, message: &PusherMessage) -> bool {
        let Some(gate) = self
            .channel_state
            .get(channel)
            .and_then(|entry| entry.value().rewind_gate.clone())
        else {
            return false;
        };

        let mut gate = gate.lock().await;
        if gate.is_overflowed() {
            return true;
        }

        let message_bytes = match sockudo_protocol::wire::serialize_message(
            message,
            self.wire_format,
        ) {
            Ok(payload) => payload.len(),
            Err(_) => {
                warn!(
                    socket_id = %self.socket_id,
                    channel,
                    reason = "serialization_failed",
                    "Rewind gate rejected a live message; shutting down connection to preserve continuity"
                );
                gate.fail_closed();
                drop(gate);
                self.shutdown();
                return true;
            }
        };
        let buffered = BufferedRewindMessage {
            serial: message.serial,
            message_id: message.message_id.clone(),
            message: message.clone(),
        };
        if let Err(overflow) = gate.try_buffer(buffered, message_bytes) {
            warn!(
                socket_id = %self.socket_id,
                channel,
                limit_kind = overflow.limit_kind.as_str(),
                limit = overflow.limit,
                buffered_messages = overflow.buffered_messages,
                buffered_bytes = overflow.buffered_bytes,
                incoming_message_bytes = overflow.incoming_message_bytes,
                "Rewind gate overflow; shutting down connection to preserve continuity"
            );
            drop(gate);
            self.shutdown();
        }
        true
    }

    pub async fn finish_rewind_gate(&self, channel: &str) -> Vec<BufferedRewindMessage> {
        let Some(gate) = self
            .channel_state
            .get_mut(channel)
            .and_then(|mut entry| entry.rewind_gate.take())
        else {
            return Vec::new();
        };
        let mut gate = gate.lock().await;
        gate.drain()
    }

    fn upsert_channel_state(
        &self,
        channel: Arc<str>,
        filter: Option<Arc<FilterNode>>,
        event_name_filter: Option<Vec<String>>,
        annotation_subscribe: bool,
    ) {
        let mut state = self.channel_state.entry(channel).or_default();
        // Preserve any rewind gate started just before subscription setup.
        state.filter = filter;
        state.event_name_filter = event_name_filter;
        state.annotation_subscribe = annotation_subscribe;
        state.attach_serial = None;
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
        self.send_message(&message)
    }

    async fn send_error(&self, code: u16, message: String, channel: Option<String>) -> Result<()> {
        let error_msg = PusherMessage::error(u32::from(code), message, channel);
        self.send_message(&error_msg)
    }

    async fn send_pong(&self) -> Result<()> {
        let pong_msg = PusherMessage::pong();
        self.send_message(&pong_msg)
    }
}

/// Buffer usage statistics for monitoring
#[derive(Debug, Clone)]
pub struct BufferStats {
    pub pending_bytes: Option<usize>,
    pub byte_limit: Option<usize>,
    pub message_limit: Option<usize>,
}
