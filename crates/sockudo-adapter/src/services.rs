//! Typed protocol-independent operations shared by HTTP projections.

use crate::ConnectionHandler;
use sockudo_core::{
    app::App,
    error::Result,
    history::{HistoryPage, HistoryReadRequest},
    message_envelope::MessageEnvelope,
    version_store::{StoredVersionRecord, VersionStorePage, VersionStoreReadRequest},
    versioned_messages::MessageSerial,
    websocket::SocketId,
};
use sockudo_protocol::messages::PusherMessage;
use std::sync::Arc;

/// Actor and delivery metadata for a publish operation.
///
/// Actor identity is deliberately distinct from `exclude_socket`; the latter
/// controls fanout only and must never be used for authorization.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PublishContext {
    pub actor_client_id: Option<String>,
    pub publisher_socket_id: Option<SocketId>,
    pub publisher_connection_id: Option<String>,
    pub exclude_socket: Option<SocketId>,
    pub idempotency_key: Option<String>,
    /// Optional commit-time envelope supplied by a protocol edge that has
    /// facts not represented by `PusherMessage`, such as an Ably encoding chain.
    pub envelope: Option<MessageEnvelope>,
}

/// Native publish/read service façade used by protocol projections.
#[derive(Clone)]
pub struct MessageService {
    handler: Arc<ConnectionHandler>,
}

impl MessageService {
    #[must_use]
    pub fn new(handler: Arc<ConnectionHandler>) -> Self {
        Self { handler }
    }

    /// Publish through the native durable/fanout pipeline.
    pub async fn publish_message(
        &self,
        app: &App,
        channel: &str,
        message: PusherMessage,
        context: PublishContext,
    ) -> Result<Option<crate::handler::connection_management::PublishAck>> {
        self.publish_message_with_timing(app, channel, message, context, None, true)
            .await
    }

    pub async fn publish_message_with_timing(
        &self,
        app: &App,
        channel: &str,
        mut message: PusherMessage,
        context: PublishContext,
        timestamp_ms: Option<f64>,
        force_full: bool,
    ) -> Result<Option<crate::handler::connection_management::PublishAck>> {
        if message.user_id.is_none() {
            message.user_id = context.actor_client_id;
        }
        if message.idempotency_key.is_none() {
            message.idempotency_key = context.idempotency_key;
        }
        self.handler
            .publish_to_channel_with_timing_and_envelope(
                app,
                channel,
                message,
                context.exclude_socket.as_ref(),
                timestamp_ms,
                force_full,
                context.envelope,
            )
            .await
    }

    pub async fn get_message(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        self.handler
            .version_store()
            .get_latest(app_id, channel, message_serial)
            .await
    }

    pub async fn get_message_versions(
        &self,
        request: VersionStoreReadRequest,
    ) -> Result<VersionStorePage> {
        self.handler.version_store().get_versions(request).await
    }

    pub async fn read_history(&self, request: HistoryReadRequest) -> Result<HistoryPage> {
        self.handler.history_store().read_page(request).await
    }

    #[must_use]
    pub fn handler(&self) -> &Arc<ConnectionHandler> {
        &self.handler
    }
}
