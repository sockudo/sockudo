use super::buffer::{ByteCounter, SizedMessage, SizedMessageSenderHandle, WebSocketBufferConfig};
use super::capabilities::UserInfo;
use super::sender::MessageSender;
use super::socket_id::SocketId;
use super::state::{ConnectionState, ConnectionStatus};
use crate::capability_token::TokenAuthContext;
use crate::channel::PresenceMemberInfo;
use crate::error::{Error, Result};
use ahash::AHashMap as HashMap;
use bytes::Bytes;
use crossfire::mpsc;
use sockudo_filter::FilterNode;
use sockudo_protocol::messages::PusherMessage;
use sockudo_ws::Message;
use sockudo_ws::axum_integration::WebSocketWriter;
use std::hash::Hash;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

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

        // Send error message while connection still active
        if code >= 4000 {
            let error_message = PusherMessage::error(u32::from(code), reason.clone(), None);
            if let Err(e) = self.send_message(&error_message) {
                warn!(error = %e, "failed to send error message before close");
            }
        }

        self.state.status = ConnectionStatus::Closing;
        self.message_sender.send_close(code, &reason).await?;
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

    pub fn set_token_auth_context(&mut self, context: TokenAuthContext) {
        self.state.user_id = Some(context.client_id.clone());
        self.state.connection_capabilities = Some(context.capabilities.clone());
        self.state.connection_meta = None;
        self.state.user_info = Some(UserInfo {
            id: context.client_id.clone(),
            watchlist: None,
            info: None,
            capabilities: Some(context.capabilities.clone()),
            meta: None,
        });
        self.state.user = Some(sonic_rs::json!({
            "id": context.client_id.clone(),
        }));
        self.state.token_auth_context = Some(context);
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

    pub fn update_presence_info(
        &mut self,
        channel: &str,
        user_info: sonic_rs::Value,
    ) -> Option<PresenceMemberInfo> {
        let member = self.state.presence.as_mut()?.get_mut(channel)?;
        member.user_info = Some(user_info);
        Some(member.clone())
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
