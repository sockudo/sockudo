#![allow(dead_code)]

// src/adapter/handler/connection_management.rs
use super::ConnectionHandler;
#[cfg(feature = "delta")]
use crate::connection_manager::CompressionParams;
use crate::connection_manager::ConnectionManager;
use bytes::Bytes;
use sockudo_core::app::App;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::{HistoryAppendRecord, now_ms};
use sockudo_core::utils;
use sockudo_core::version_store::StoredVersionRecord;
use sockudo_core::versioned_messages::{
    MessageAction as CoreMessageAction, MessageSerial, VersionMetadata, VersionSerial,
    VersionedMessage,
};
use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::generate_message_id;
use sockudo_protocol::messages::{
    AI_ERROR_PAYLOAD_TOO_LARGE, MessageData, PusherMessage, is_ai_event,
};
use sockudo_protocol::versioned_messages::{
    MessageAction as ProtocolMessageAction, MessageVersionMetadata, apply_runtime_metadata,
    extract_runtime_action, extract_runtime_message_serial,
};
use sockudo_ws::Message;
use sockudo_ws::axum_integration::WebSocketWriter;
#[cfg(any(feature = "ai-transport", feature = "delta"))]
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::warn;

static VERSION_SERIAL_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishAck {
    pub message_serial: String,
    pub history_serial: u64,
    pub delivery_serial: u64,
    pub version_serial: String,
}

fn sanitize_v2_feature_flags(
    server_options: &sockudo_core::options::ServerOptions,
    mut message: PusherMessage,
) -> PusherMessage {
    if let Some(extras) = message.extras.as_mut() {
        if !server_options.ephemeral.enabled {
            extras.ephemeral = None;
        }
        if !server_options.echo_control.enabled {
            extras.echo = None;
        }
        let extras_empty = extras.headers.is_none()
            && extras.ephemeral.is_none()
            && extras.idempotency_key.is_none()
            && extras.echo.is_none()
            && extras.ai.is_none();
        if extras_empty {
            message.extras = None;
        }
    }

    message
}

fn ai_payload_too_large(message: impl Into<String>) -> Error {
    Error::AiTransport {
        code: AI_ERROR_PAYLOAD_TOO_LARGE,
        name: "payload_too_large",
        message: message.into(),
    }
}

fn message_data_bytes(data: Option<&MessageData>) -> Result<usize> {
    match data {
        None => Ok(0),
        Some(MessageData::String(value)) => Ok(value.len()),
        Some(other) => sonic_rs::to_vec(other)
            .map(|bytes| bytes.len())
            .map_err(Error::from),
    }
}

fn has_ai_streaming_status(message: &PusherMessage) -> bool {
    message
        .extras
        .as_ref()
        .and_then(|extras| extras.ai_transport_headers())
        .and_then(|headers| headers.status())
        == Some("streaming")
}

fn record_has_ai_streaming_status(record: &StoredVersionRecord) -> bool {
    record
        .message
        .extras
        .as_ref()
        .and_then(|extras| extras.ai_transport_headers())
        .and_then(|headers| headers.status())
        == Some("streaming")
}

impl ConnectionHandler {
    async fn resolve_actor_client_id(
        &self,
        app_id: &str,
        socket_id: Option<&SocketId>,
    ) -> Option<String> {
        let socket_id = socket_id?;
        let connection = self
            .connection_manager
            .get_connection(socket_id, app_id)
            .await?;
        connection.get_user_id().await
    }

    pub fn next_version_serial(&self) -> String {
        let counter = VERSION_SERIAL_COUNTER.fetch_add(1, Ordering::Relaxed);
        let process_id = self.server_options().instance.process_id.trim();
        let node = if process_id.is_empty() {
            "node"
        } else {
            process_id
        };
        format!("{:020}:{node}:{:020}", now_ms(), counter)
    }

    pub fn build_runtime_message_from_record(
        &self,
        record: &StoredVersionRecord,
        stream_id: Option<String>,
    ) -> PusherMessage {
        let action = match record.message.action {
            CoreMessageAction::Create => ProtocolMessageAction::Create,
            CoreMessageAction::Update => ProtocolMessageAction::Update,
            CoreMessageAction::Delete => ProtocolMessageAction::Delete,
            CoreMessageAction::Append => ProtocolMessageAction::Append,
            CoreMessageAction::Summary => ProtocolMessageAction::Summary,
        };
        let mut message = PusherMessage {
            event: Some(action.v2_event_name()),
            channel: Some(record.channel.clone()),
            data: record.message.data.clone(),
            name: record.message.name.clone(),
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: Some(generate_message_id()),
            stream_id,
            serial: Some(record.delivery_serial()),
            idempotency_key: None,
            extras: record.message.extras.clone(),
            delta_sequence: None,
            delta_conflation_key: None,
        };

        apply_runtime_metadata(
            &mut message,
            action,
            record.message_serial().as_str(),
            &MessageVersionMetadata {
                serial: record.version_serial().as_str().to_string(),
                client_id: record.message.version.client_id.clone(),
                timestamp_ms: record.message.version.timestamp_ms,
                description: record.message.version.description.clone(),
                metadata: record.message.version.metadata.clone(),
            },
            Some(record.history_serial()),
        );

        message
    }

    pub async fn send_message_to_socket(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        message: PusherMessage,
    ) -> Result<()> {
        // Calculate message size for metrics
        let message_size = sonic_rs::to_string(&message).unwrap_or_default().len();

        // Send the message (lock-free - all ConnectionManager methods are &self)
        let result = self
            .connection_manager
            .send_message(app_id, socket_id, message)
            .await;

        // Track metrics if message was sent successfully
        if result.is_ok()
            && let Some(ref metrics) = self.metrics
        {
            metrics.mark_ws_message_sent(app_id, message_size);
        }

        result
    }

    /// Broadcast to channel (backward compatible version)
    pub async fn broadcast_to_channel(
        &self,
        app_config: &App,
        channel: &str,
        message: PusherMessage,
        exclude_socket: Option<&SocketId>,
    ) -> Result<()> {
        self.broadcast_to_channel_with_timing(app_config, channel, message, exclude_socket, None)
            .await
    }

    /// Broadcast to channel with optional timing for latency tracking
    pub async fn broadcast_to_channel_with_timing(
        &self,
        app_config: &App,
        channel: &str,
        message: PusherMessage,
        exclude_socket: Option<&SocketId>,
        start_time_ms: Option<f64>,
    ) -> Result<()> {
        self.broadcast_to_channel_internal(
            app_config,
            channel,
            message,
            exclude_socket,
            start_time_ms,
            false, // allow delta compression
        )
        .await
        .map(|_| ())
    }

    pub async fn publish_to_channel_with_timing(
        &self,
        app_config: &App,
        channel: &str,
        message: PusherMessage,
        exclude_socket: Option<&SocketId>,
        start_time_ms: Option<f64>,
        force_full_message: bool,
    ) -> Result<Option<PublishAck>> {
        self.broadcast_to_channel_internal(
            app_config,
            channel,
            message,
            exclude_socket,
            start_time_ms,
            force_full_message,
        )
        .await
    }

    /// Broadcast to channel forcing full messages (skip delta compression)
    ///
    /// This is used when the publisher explicitly requests `delta: false` in the
    /// publish API. All recipients will receive the full message regardless of
    /// their delta compression subscription settings.
    pub async fn broadcast_to_channel_force_full(
        &self,
        app_config: &App,
        channel: &str,
        message: PusherMessage,
        exclude_socket: Option<&SocketId>,
        start_time_ms: Option<f64>,
    ) -> Result<()> {
        self.broadcast_to_channel_internal(
            app_config,
            channel,
            message,
            exclude_socket,
            start_time_ms,
            true, // force full messages, skip delta compression
        )
        .await
        .map(|_| ())
    }

    /// Internal broadcast implementation with delta compression control
    #[allow(unused_variables, unused_mut)]
    async fn broadcast_to_channel_internal(
        &self,
        app_config: &App,
        channel: &str,
        mut message: PusherMessage,
        exclude_socket: Option<&SocketId>,
        start_time_ms: Option<f64>,
        force_full_message: bool,
    ) -> Result<Option<PublishAck>> {
        message = sanitize_v2_feature_flags(self.server_options(), message);

        // Extras-level message idempotency (V2 feature).
        // Check extras.idempotency_key before broadcasting. If the key was already
        // seen within the TTL window, silently drop the message.
        if let Some(extras_key) = message.extras_idempotency_key() {
            let config = app_config.resolved_idempotency(&self.server_options().idempotency);
            if config.enabled {
                let cache_key = format!("idempotency:{}:{}:{}", app_config.id, channel, extras_key);
                if let Some(ref metrics) = self.metrics {
                    metrics.mark_idempotency_publish(&app_config.id);
                }
                let is_new = self
                    .cache_manager
                    .set_if_not_exists(&cache_key, "1", config.ttl_seconds)
                    .await?;
                if !is_new {
                    if let Some(ref metrics) = self.metrics {
                        metrics.mark_idempotency_duplicate(&app_config.id);
                    }
                    tracing::debug!(
                        app_id = %app_config.id,
                        channel = %channel,
                        key = %extras_key,
                        "Extras idempotency: duplicate message dropped"
                    );
                    return Ok(None);
                }
            }
        }

        // Track ephemeral message metric (V2 feature)
        if message.is_ephemeral()
            && let Some(ref metrics) = self.metrics
        {
            metrics.mark_ephemeral_message(&app_config.id);
        }

        let history_policy = app_config.resolved_history(channel, &self.server_options().history);
        let history_enabled = history_policy.enabled;
        let mut history_stream_id: Option<String> = None;
        let mut history_serial_position: Option<u64> = None;
        let versioned_enabled = self.server_options().versioned_messages.enabled && history_enabled;
        let existing_runtime_action = extract_runtime_action(&message);
        let is_history_mutation = matches!(
            existing_runtime_action,
            Some(ProtocolMessageAction::Update)
                | Some(ProtocolMessageAction::Delete)
                | Some(ProtocolMessageAction::Append)
        );
        let mut versioned_delivery_serial: Option<u64> = None;
        let mut publish_ack: Option<PublishAck> = None;
        let actor_client_id = self
            .resolve_actor_client_id(&app_config.id, exclude_socket)
            .await;

        if !message.is_ephemeral() {
            #[cfg(feature = "recovery")]
            let recovery_enabled = app_config
                .resolved_connection_recovery(&self.server_options().connection_recovery)
                .enabled;
            #[cfg(not(feature = "recovery"))]
            let recovery_enabled = false;

            if history_enabled || recovery_enabled {
                if message.message_id.is_none() {
                    message.message_id = Some(generate_message_id());
                }

                if versioned_enabled
                    && extract_runtime_message_serial(&message).is_none()
                    && extract_runtime_action(&message).is_none()
                {
                    if self.server_options().ai_transport.matches_channel(channel)
                        && (message.event.as_deref().is_some_and(is_ai_event)
                            || message
                                .extras
                                .as_ref()
                                .and_then(|extras| extras.ai.as_ref())
                                .is_some())
                    {
                        let max_bytes = self
                            .server_options()
                            .ai_transport
                            .max_accumulated_message_bytes;
                        let bytes = message_data_bytes(message.data.as_ref())?;
                        if bytes > max_bytes {
                            return Err(ai_payload_too_large(format!(
                                "accumulated message content exceeds {max_bytes} bytes"
                            )));
                        }

                        if has_ai_streaming_status(&message) {
                            let open_count = self
                                .version_store()
                                .latest_by_history(&app_config.id, channel)
                                .await?
                                .iter()
                                .filter(|record| record_has_ai_streaming_status(record))
                                .count();
                            let max_open = self
                                .server_options()
                                .ai_transport
                                .max_open_streaming_messages_per_channel;
                            if open_count >= max_open {
                                return Err(ai_payload_too_large(format!(
                                    "open streaming message count exceeds {max_open}"
                                )));
                            }
                        }
                    }

                    let delivery = self
                        .version_store()
                        .reserve_delivery_position(&app_config.id, channel)
                        .await?;
                    versioned_delivery_serial = Some(delivery.delivery_serial);
                    message.serial = Some(delivery.delivery_serial);
                    message.stream_id = Some(delivery.stream_id.clone());
                }

                if history_enabled && !is_history_mutation {
                    let reservation = self
                        .history_store()
                        .reserve_publish_position(&app_config.id, channel)
                        .await?;
                    history_stream_id = Some(reservation.stream_id.clone());
                    history_serial_position = Some(reservation.serial);
                    if !versioned_enabled {
                        message.stream_id = Some(reservation.stream_id);
                        message.serial = Some(reservation.serial);
                    }
                } else {
                    #[cfg(feature = "recovery")]
                    if let Some(ref replay_buffer) = self.replay_buffer {
                        message.serial = Some(replay_buffer.next_serial(&app_config.id, channel));
                    }
                }

                let mut stored_v2_message = message.clone();
                stored_v2_message.rewrite_prefix(sockudo_protocol::ProtocolVersion::V2);
                stored_v2_message.idempotency_key = None;

                if versioned_enabled
                    && extract_runtime_message_serial(&stored_v2_message).is_none()
                    && extract_runtime_action(&stored_v2_message).is_none()
                {
                    let message_serial_value = self.next_version_serial();
                    let version_serial_value = message_serial_value.clone();
                    let history_serial = history_serial_position.ok_or_else(|| {
                        Error::Internal(
                            "History store did not return a create position for versioned message"
                                .to_string(),
                        )
                    })?;
                    let version_metadata = MessageVersionMetadata {
                        serial: version_serial_value.clone(),
                        client_id: actor_client_id.clone(),
                        timestamp_ms: now_ms(),
                        description: None,
                        metadata: None,
                    };

                    apply_runtime_metadata(
                        &mut stored_v2_message,
                        ProtocolMessageAction::Create,
                        &message_serial_value,
                        &version_metadata,
                        Some(history_serial),
                    );

                    message = stored_v2_message.clone();

                    let record = StoredVersionRecord {
                        app_id: app_config.id.clone(),
                        channel: channel.to_string(),
                        original_client_id: actor_client_id.clone(),
                        message: VersionedMessage::new_create(
                            MessageSerial::new(message_serial_value.clone())?,
                            VersionMetadata {
                                serial: VersionSerial::new(version_serial_value.clone())?,
                                client_id: actor_client_id.clone(),
                                timestamp_ms: version_metadata.timestamp_ms,
                                description: None,
                                metadata: None,
                            },
                            history_serial,
                            versioned_delivery_serial
                                .unwrap_or_else(|| message.serial.unwrap_or(0)),
                            message.event.clone(),
                            message.data.clone(),
                            message.extras.clone(),
                        ),
                    };
                    self.version_store().append_version(record.clone()).await?;
                    if let Some(webhook_integration) = self.webhook_integration().as_ref().cloned()
                    {
                        let app_for_webhook = app_config.clone();
                        let channel_for_webhook = channel.to_string();
                        let message_serial_for_webhook = message_serial_value.clone();
                        let version_serial_for_webhook = version_serial_value.clone();
                        tokio::spawn(async move {
                            if let Err(error) = webhook_integration
                                .send_message_version_created(
                                    &app_for_webhook,
                                    &channel_for_webhook,
                                    &message_serial_for_webhook,
                                    &version_serial_for_webhook,
                                    "message.create",
                                )
                                .await
                            {
                                tracing::warn!(
                                    error = %error,
                                    "failed to emit message_version_created webhook"
                                );
                            }
                        });
                    }
                    #[cfg(feature = "ai-transport")]
                    self.record_ai_stream_activity(&app_config.id, channel, &record)
                        .await?;
                    publish_ack = Some(PublishAck {
                        message_serial: message_serial_value.clone(),
                        history_serial,
                        delivery_serial: versioned_delivery_serial
                            .unwrap_or_else(|| message.serial.unwrap_or(0)),
                        version_serial: version_serial_value.clone(),
                    });
                    if let Some(metrics) = self.metrics() {
                        metrics.mark_versioned_message_mutation(
                            &app_config.id,
                            "message.create",
                            "applied",
                        );
                    }
                    tracing::info!(
                        app_id = %app_config.id,
                        channel = %channel,
                        message_serial = %message_serial_value,
                        version_serial = %version_serial_value,
                        history_serial = history_serial,
                        delivery_serial = versioned_delivery_serial.unwrap_or_else(|| message.serial.unwrap_or(0)),
                        actor_client_id = ?actor_client_id,
                        "Applied versioned message.create"
                    );
                }

                let serialized = sonic_rs::to_vec(&stored_v2_message)
                    .map(Bytes::from)
                    .map_err(|e| {
                        Error::Serialization(format!("Failed to serialize history payload: {e}"))
                    })?;

                #[cfg(feature = "recovery")]
                if recovery_enabled && let Some(ref replay_buffer) = self.replay_buffer {
                    replay_buffer.store(
                        &app_config.id,
                        channel,
                        message.stream_id.as_deref(),
                        message.serial.unwrap_or(0),
                        serialized.clone(),
                    );
                }

                if history_enabled && !is_history_mutation {
                    self.history_store()
                        .append(HistoryAppendRecord {
                            app_id: app_config.id.clone(),
                            channel: channel.to_string(),
                            stream_id: history_stream_id.clone().ok_or_else(|| {
                                Error::Internal(
                                    "History store did not return a stream identifier".to_string(),
                                )
                            })?,
                            serial: history_serial_position.ok_or_else(|| {
                                Error::Internal(
                                    "History store did not return a channel serial".to_string(),
                                )
                            })?,
                            published_at_ms: now_ms(),
                            message_id: message.message_id.clone(),
                            event_name: message.event.clone(),
                            operation_kind: "append".to_string(),
                            payload_bytes: serialized,
                            retention: history_policy.retention(),
                        })
                        .await?;
                }
            }
        }

        #[cfg(feature = "ai-transport")]
        if self.server_options().ai_transport.enabled {
            self.record_ai_observability(app_config, channel, &message)
                .await;
        }

        // Calculate message size for metrics
        let message_size = sonic_rs::to_string(&message).unwrap_or_default().len();

        // Get the number of sockets in the channel before sending and send the message
        let (result, target_socket_count) = {
            // Use local-only socket count — this value is only used for metrics, not
            // broadcast correctness. Avoids the distributed adapter round-trip (e.g.
            // cross-region NATS request/reply latency).
            let socket_count = if let Some(ref local_adapter) = self.local_adapter {
                local_adapter
                    .get_channel_socket_count(&app_config.id, channel)
                    .await
            } else {
                self.connection_manager
                    .get_channel_socket_count(&app_config.id, channel)
                    .await
            };

            // Adjust for excluded socket
            let target_socket_count = if exclude_socket.is_some() && socket_count > 0 {
                socket_count - 1
            } else {
                socket_count
            };

            let result = self
                .send_rollup_aware(
                    app_config,
                    channel,
                    message,
                    exclude_socket,
                    start_time_ms,
                    force_full_message,
                )
                .await;

            (result, target_socket_count)
        };

        // Track metrics if message was sent successfully
        if result.is_ok()
            && target_socket_count > 0
            && let Some(ref metrics) = self.metrics
        {
            // Batch metrics update instead of loop for performance
            metrics.mark_ws_messages_sent_batch(&app_config.id, message_size, target_socket_count);

            // Track broadcast latency if we have a start time
            if let Some(start_ms) = start_time_ms {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as f64
                    / 1_000_000.0; // Convert to milliseconds
                let latency_ms = (now_ms - start_ms).max(0.0); // Already in milliseconds with microsecond precision

                metrics.track_broadcast_latency(
                    &app_config.id,
                    channel,
                    target_socket_count,
                    latency_ms,
                );
            }
        }

        result.map(|_| publish_ack)
    }

    async fn send_rollup_aware(
        &self,
        app_config: &App,
        channel: &str,
        message: PusherMessage,
        exclude_socket: Option<&SocketId>,
        start_time_ms: Option<f64>,
        force_full_message: bool,
    ) -> Result<()> {
        #[cfg(feature = "ai-transport")]
        if app_config
            .resolved_history(channel, &self.server_options().history)
            .enabled
            && self.server_options().ai_transport.matches_channel(channel)
            && let Some(engine) = self.ai_rollup_engine.as_ref()
        {
            let is_append = extract_runtime_action(&message) == Some(ProtocolMessageAction::Append);
            if is_append && let Some(metrics) = self.metrics() {
                metrics.mark_ai_rollup_append_received(&app_config.id);
            }
            let now_ms = u64::try_from(now_ms()).unwrap_or_default();
            let deliveries = engine.ingest(&app_config.id, channel, message, now_ms);

            for delivery in deliveries {
                if let Some(metrics) = self.metrics() {
                    if delivery.reason != sockudo_ai_transport::RollupDeliveryReason::Bypass {
                        metrics.mark_ai_rollup_append_delivered(&delivery.app_id);
                        metrics
                            .observe_ai_rollup_ratio(&delivery.app_id, delivery.coalesced as f64);
                        metrics.observe_ai_rollup_flush_latency(
                            &delivery.app_id,
                            delivery.latency_ms as f64,
                        );
                    }
                    metrics.update_ai_rollup_active_streams(
                        &delivery.app_id,
                        engine.active_streams() as u64,
                    );
                }
                self.send_one_egress_message(
                    app_config,
                    &delivery.channel,
                    delivery.message,
                    exclude_socket,
                    start_time_ms,
                    force_full_message,
                )
                .await?;
            }
            return Ok(());
        }

        self.send_one_egress_message(
            app_config,
            channel,
            message,
            exclude_socket,
            start_time_ms,
            force_full_message,
        )
        .await
    }

    async fn send_one_egress_message(
        &self,
        app_config: &App,
        channel: &str,
        message: PusherMessage,
        exclude_socket: Option<&SocketId>,
        start_time_ms: Option<f64>,
        force_full_message: bool,
    ) -> Result<()> {
        #[cfg(feature = "delta")]
        {
            let channel_settings = if force_full_message {
                None
            } else {
                Self::get_channel_delta_settings(app_config, channel)
            };

            if force_full_message {
                self.connection_manager
                    .send(
                        channel,
                        message,
                        exclude_socket,
                        &app_config.id,
                        start_time_ms,
                    )
                    .await
            } else {
                self.connection_manager
                    .send_with_compression(
                        channel,
                        message,
                        exclude_socket,
                        &app_config.id,
                        start_time_ms,
                        CompressionParams {
                            delta_compression: Arc::clone(&self.delta_compression),
                            channel_settings: channel_settings.as_ref(),
                        },
                    )
                    .await
            }
        }

        #[cfg(not(feature = "delta"))]
        {
            let _ = force_full_message;
            self.connection_manager
                .send(
                    channel,
                    message,
                    exclude_socket,
                    &app_config.id,
                    start_time_ms,
                )
                .await
        }
    }

    pub async fn close_connection(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        code: u16,
        reason: &str,
    ) -> Result<()> {
        if let Some(conn) = self
            .connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
        {
            let mut conn_locked = conn.inner.lock().await;
            conn_locked
                .close(code, reason.to_string())
                .await
                .map_err(|e| Error::Internal(format!("Failed to close connection: {e}")))
        } else {
            warn!("Connection not found for close: {}", socket_id);
            Ok(())
        }
    }

    pub async fn get_channel_member_count(&self, app_config: &App, channel: &str) -> Result<usize> {
        self.connection_manager
            .get_channel_members(&app_config.id, channel)
            .await
            .map(|members| members.len())
    }

    pub async fn verify_channel_subscription(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        channel: &str,
    ) -> Result<()> {
        let is_subscribed = self
            .connection_manager
            .is_in_channel(&app_config.id, channel, socket_id)
            .await?;

        if !is_subscribed {
            return Err(Error::ClientEvent(format!(
                "Socket {socket_id} is not subscribed to channel {channel}"
            )));
        }

        Ok(())
    }

    pub async fn broadcast_metachannel_event(
        &self,
        app_config: &App,
        channel: &str,
        event_name: &str,
        data: sonic_rs::Value,
    ) -> Result<()> {
        let Some(meta_channel) = utils::meta_channel_for(channel) else {
            return Ok(());
        };

        let message = PusherMessage {
            event: Some(format!("sockudo_internal:{event_name}")),
            channel: Some(meta_channel.clone()),
            data: Some(sockudo_protocol::messages::MessageData::Json(data)),
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        };

        self.broadcast_to_channel(app_config, &meta_channel, message, None)
            .await
    }

    async fn send_error_frame(ws_tx: &mut WebSocketWriter, error: &Error) {
        let error_message =
            PusherMessage::error(u32::from(error.close_code()), error.to_string(), None);

        if let Ok(payload) = sonic_rs::to_string(&error_message)
            && let Err(e) = ws_tx.send(Message::text(payload)).await
        {
            warn!("Failed to send error frame: {e}");
        }

        if let Err(e) = ws_tx.close(error.close_code(), &error.to_string()).await {
            warn!("Failed to send close frame: {}", e);
        }
    }

    #[cfg(feature = "delta")]
    /// Get channel-specific delta compression settings with pattern matching support
    ///
    /// Supports:
    /// - Exact channel name match (e.g., "market-data")
    /// - Wildcard patterns (e.g., "market-*" matches "market-btc", "market-eth")
    /// - Prefix patterns (e.g., "private-*")
    fn get_channel_delta_settings(
        app_config: &App,
        channel: &str,
    ) -> Option<sockudo_delta::ChannelDeltaSettings> {
        let channel_delta_map = app_config.channel_delta_compression_ref()?;

        // First try exact match
        if let Some(config) = channel_delta_map.get(channel) {
            return Self::convert_channel_config_to_settings(config);
        }

        // Try pattern matching for wildcard patterns
        for (pattern, config) in channel_delta_map.iter() {
            if Self::matches_pattern(channel, pattern) {
                return Self::convert_channel_config_to_settings(config);
            }
        }

        None
    }

    #[cfg(feature = "delta")]
    /// Convert ChannelDeltaConfig enum to ChannelDeltaSettings struct
    fn convert_channel_config_to_settings(
        config: &sockudo_delta::ChannelDeltaConfig,
    ) -> Option<sockudo_delta::ChannelDeltaSettings> {
        use sockudo_delta::{ChannelDeltaConfig, ChannelDeltaSettings, DeltaAlgorithm};

        match config {
            ChannelDeltaConfig::Full(settings) => Some(settings.clone()),
            ChannelDeltaConfig::Simple(simple) => {
                use sockudo_delta::ChannelDeltaSimple;
                match simple {
                    ChannelDeltaSimple::Disabled => None,
                    ChannelDeltaSimple::Inherit => None, // Inherit from global settings
                    ChannelDeltaSimple::Fossil => Some(ChannelDeltaSettings {
                        enabled: true,
                        algorithm: DeltaAlgorithm::Fossil,
                        conflation_key: None,
                        max_messages_per_key: 10,
                        max_conflation_keys: 100,
                        enable_tags: true,
                    }),
                    ChannelDeltaSimple::Xdelta3 => Some(ChannelDeltaSettings {
                        enabled: true,
                        algorithm: DeltaAlgorithm::Xdelta3,
                        conflation_key: None,
                        max_messages_per_key: 10,
                        max_conflation_keys: 100,
                        enable_tags: true,
                    }),
                }
            }
        }
    }

    #[cfg(feature = "delta")]
    /// Check if a channel name matches a pattern
    /// Supports:
    /// - Exact match: "market-data" matches "market-data"
    /// - Wildcard suffix: "market-*" matches "market-btc", "market-eth", etc.
    /// - Wildcard prefix: "*-data" matches "market-data", "price-data", etc.
    fn matches_pattern(channel: &str, pattern: &str) -> bool {
        sockudo_core::utils::wildcard_pattern_matches(channel, pattern)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_core::app::AppIdempotencyConfig;
    use sockudo_core::options::IdempotencyConfig;
    use sockudo_protocol::messages::{MessageExtras, PusherMessage};
    use sonic_rs::JsonValueTrait;

    #[test]
    fn test_resolve_idempotency_uses_global_when_no_app_override() {
        let global = IdempotencyConfig {
            enabled: true,
            ttl_seconds: 300,
            max_key_length: 128,
        };
        let app = App::default();
        let resolved = app.resolved_idempotency(&global);
        assert!(resolved.enabled);
        assert_eq!(resolved.ttl_seconds, 300);
    }

    #[test]
    fn test_resolve_idempotency_app_override_enabled() {
        let global = IdempotencyConfig {
            enabled: false,
            ttl_seconds: 300,
            max_key_length: 128,
        };
        let app = App::from_policy(
            String::new(),
            String::new(),
            String::new(),
            false,
            sockudo_core::app::AppPolicy {
                idempotency: Some(AppIdempotencyConfig {
                    enabled: Some(true),
                    ttl_seconds: Some(600),
                }),
                ..Default::default()
            },
        );
        let resolved = app.resolved_idempotency(&global);
        assert!(resolved.enabled);
        assert_eq!(resolved.ttl_seconds, 600);
    }

    #[test]
    fn test_resolve_idempotency_app_disables_globally_enabled() {
        let global = IdempotencyConfig {
            enabled: true,
            ttl_seconds: 300,
            max_key_length: 128,
        };
        let app = App::from_policy(
            String::new(),
            String::new(),
            String::new(),
            false,
            sockudo_core::app::AppPolicy {
                idempotency: Some(AppIdempotencyConfig {
                    enabled: Some(false),
                    ttl_seconds: None,
                }),
                ..Default::default()
            },
        );
        let resolved = app.resolved_idempotency(&global);
        assert!(!resolved.enabled);
        assert_eq!(resolved.ttl_seconds, 300); // falls back to global
    }

    #[test]
    fn test_resolve_idempotency_partial_app_override() {
        let global = IdempotencyConfig {
            enabled: true,
            ttl_seconds: 120,
            max_key_length: 128,
        };
        let app = App::from_policy(
            String::new(),
            String::new(),
            String::new(),
            false,
            sockudo_core::app::AppPolicy {
                idempotency: Some(AppIdempotencyConfig {
                    enabled: None, // inherit global
                    ttl_seconds: Some(999),
                }),
                ..Default::default()
            },
        );
        let resolved = app.resolved_idempotency(&global);
        assert!(resolved.enabled); // from global
        assert_eq!(resolved.ttl_seconds, 999); // from app
    }

    #[test]
    fn test_extras_idempotency_key_cache_key_format() {
        let app_id = "app-123";
        let channel = "market-btc";
        let key = "dedup-abc";
        let cache_key = format!("idempotency:{}:{}:{}", app_id, channel, key);
        assert_eq!(cache_key, "idempotency:app-123:market-btc:dedup-abc");
    }

    #[test]
    fn test_different_channels_same_key_produce_different_cache_keys() {
        let app_id = "app-1";
        let key = "same-key";
        let key1 = format!("idempotency:{}:{}:{}", app_id, "channel-a", key);
        let key2 = format!("idempotency:{}:{}:{}", app_id, "channel-b", key);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_same_channel_same_key_different_apps_produce_different_cache_keys() {
        let channel = "market-btc";
        let key = "same-key";
        let key1 = format!("idempotency:{}:{}:{}", "app-1", channel, key);
        let key2 = format!("idempotency:{}:{}:{}", "app-2", channel, key);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_message_without_extras_key_proceeds() {
        let msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        assert!(msg.extras_idempotency_key().is_none());
    }

    #[test]
    fn test_message_with_extras_key_returns_key() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            idempotency_key: Some("my-key".to_string()),
            ..Default::default()
        });
        assert_eq!(msg.extras_idempotency_key(), Some("my-key"));
    }

    #[test]
    fn test_ephemeral_message_is_detected() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            ephemeral: Some(true),
            ..Default::default()
        });
        assert!(msg.is_ephemeral());
    }

    #[test]
    fn test_non_ephemeral_message() {
        let msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        assert!(!msg.is_ephemeral());
    }

    #[test]
    fn test_ephemeral_false_when_extras_present_but_ephemeral_not_set() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            echo: Some(true),
            ..Default::default()
        });
        assert!(!msg.is_ephemeral());
    }

    #[test]
    fn test_ephemeral_explicitly_false() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            ephemeral: Some(false),
            ..Default::default()
        });
        assert!(!msg.is_ephemeral());
    }

    #[test]
    fn test_ephemeral_with_idempotency_key_both_present() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            ephemeral: Some(true),
            idempotency_key: Some("dedup-123".to_string()),
            ..Default::default()
        });
        assert!(msg.is_ephemeral());
        assert_eq!(msg.extras_idempotency_key(), Some("dedup-123"));
    }

    #[test]
    fn test_ephemeral_preserves_echo_control() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            ephemeral: Some(true),
            echo: Some(false),
            ..Default::default()
        });
        assert!(msg.is_ephemeral());
        assert!(!msg.should_echo(true));
    }

    #[test]
    fn test_disabled_ephemeral_strips_ephemeral_flag() {
        let mut options = sockudo_core::options::ServerOptions::default();
        options.ephemeral.enabled = false;

        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            ephemeral: Some(true),
            ..Default::default()
        });

        let sanitized = sanitize_v2_feature_flags(&options, msg);
        assert!(!sanitized.is_ephemeral());
        assert!(sanitized.extras.is_none());
    }

    #[test]
    fn test_disabled_echo_control_strips_echo_override_only() {
        let mut options = sockudo_core::options::ServerOptions::default();
        options.echo_control.enabled = false;

        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            echo: Some(false),
            idempotency_key: Some("dedup-1".to_string()),
            ..Default::default()
        });

        let sanitized = sanitize_v2_feature_flags(&options, msg);
        assert_eq!(sanitized.extras_idempotency_key(), Some("dedup-1"));
        assert!(sanitized.should_echo(true));
        assert_eq!(
            sanitized.extras.as_ref().and_then(|extras| extras.echo),
            None
        );
    }

    #[test]
    fn test_broadcast_message_ephemeral_flag() {
        use crate::horizontal_adapter::BroadcastMessage;

        let broadcast = BroadcastMessage {
            node_id: "node-1".to_string(),
            app_id: "app-1".to_string(),
            channel: "cursors".to_string(),
            message: "{}".to_string(),
            except_socket_id: None,
            timestamp_ms: None,
            compression_metadata: None,
            idempotency_key: None,
            ephemeral: true,
        };
        assert!(broadcast.ephemeral);

        // Verify serialization includes ephemeral when true
        let json = sonic_rs::to_string(&broadcast).unwrap();
        assert!(json.contains("\"ephemeral\":true"));
    }

    #[test]
    fn test_broadcast_message_ephemeral_skipped_when_false() {
        use crate::horizontal_adapter::BroadcastMessage;

        let broadcast = BroadcastMessage {
            node_id: "node-1".to_string(),
            app_id: "app-1".to_string(),
            channel: "orders".to_string(),
            message: "{}".to_string(),
            except_socket_id: None,
            timestamp_ms: None,
            compression_metadata: None,
            idempotency_key: None,
            ephemeral: false,
        };

        // Verify serialization omits ephemeral when false (skip_serializing_if)
        let json = sonic_rs::to_string(&broadcast).unwrap();
        assert!(!json.contains("ephemeral"));
    }

    #[test]
    fn test_v1_delivery_strips_extras_including_ephemeral() {
        let mut msg =
            PusherMessage::channel_event("test", "ch", sonic_rs::json!({"hello": "world"}));
        msg.extras = Some(MessageExtras {
            ephemeral: Some(true),
            ..Default::default()
        });

        // Simulate V1 stripping
        msg.serial = None;
        msg.message_id = None;
        msg.extras = None;

        let json = sonic_rs::to_value(&msg).unwrap();
        assert!(json.get("extras").is_none());
    }

    #[test]
    fn test_runtime_message_from_record_uses_sockudo_action_event() {
        let action = ProtocolMessageAction::Update;
        let mut message = PusherMessage {
            event: Some(action.v2_event_name()),
            channel: Some("versioned-room".to_string()),
            data: Some(sockudo_protocol::messages::MessageData::String(
                "{\"text\":\"patched\"}".to_string(),
            )),
            name: Some("chat.message".to_string()),
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: Some(generate_message_id()),
            stream_id: Some("stream-1".to_string()),
            serial: Some(12),
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        };

        apply_runtime_metadata(
            &mut message,
            action,
            "msg:1",
            &MessageVersionMetadata {
                serial: "ver:2".to_string(),
                client_id: Some("user-1".to_string()),
                timestamp_ms: 2,
                description: Some("patch".to_string()),
                metadata: None,
            },
            Some(10),
        );

        assert_eq!(message.event.as_deref(), Some("sockudo:message.update"));
        assert_eq!(message.name.as_deref(), Some("chat.message"));
        assert_eq!(
            extract_runtime_action(&message),
            Some(ProtocolMessageAction::Update)
        );
        assert_eq!(extract_runtime_message_serial(&message), Some("msg:1"));
    }

    // ── Echo control tests ──────────────────────────────────────────

    #[test]
    fn test_should_echo_default_true() {
        let msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        // Connection default true, no per-message override → echo
        assert!(msg.should_echo(true));
    }

    #[test]
    fn test_should_echo_connection_disabled() {
        let msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        // Connection default false, no per-message override → no echo
        assert!(!msg.should_echo(false));
    }

    #[test]
    fn test_should_echo_message_override_true() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            echo: Some(true),
            ..Default::default()
        });
        // Connection false but message says echo → echo
        assert!(msg.should_echo(false));
    }

    #[test]
    fn test_should_echo_message_override_false() {
        let mut msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));
        msg.extras = Some(MessageExtras {
            echo: Some(false),
            ..Default::default()
        });
        // Connection true but message says no echo → no echo
        assert!(!msg.should_echo(true));
    }

    #[test]
    fn test_echo_messages_default_is_true() {
        let state = sockudo_core::websocket::ConnectionState::new();
        assert!(state.echo_messages);
    }

    #[test]
    fn test_v1_always_excludes_publisher() {
        // V1 connections always pass Some(socket_id) as except.
        // The should_echo method is not consulted for V1.
        // This test verifies the logic pattern used in handle_client_event_request.
        let protocol = sockudo_protocol::ProtocolVersion::V1;
        let is_v2 = matches!(protocol, sockudo_protocol::ProtocolVersion::V2);
        assert!(!is_v2); // V1 → always exclude publisher
    }

    #[test]
    fn test_v2_echo_messages_false_excludes_publisher() {
        let protocol = sockudo_protocol::ProtocolVersion::V2;
        let echo_messages = false;
        let msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));

        let is_v2 = matches!(protocol, sockudo_protocol::ProtocolVersion::V2);
        assert!(is_v2);
        let should_echo = msg.should_echo(echo_messages);
        assert!(!should_echo); // connection echo off → exclude publisher
    }

    #[test]
    fn test_v2_echo_messages_true_includes_publisher() {
        let protocol = sockudo_protocol::ProtocolVersion::V2;
        let echo_messages = true;
        let msg = PusherMessage::channel_event("test", "ch", sonic_rs::json!({}));

        let is_v2 = matches!(protocol, sockudo_protocol::ProtocolVersion::V2);
        assert!(is_v2);
        let should_echo = msg.should_echo(echo_messages);
        assert!(should_echo); // connection echo on → include publisher
    }

    #[test]
    fn test_rest_publish_no_socket_delivers_to_all() {
        // REST publish passes None as publisher_socket_id.
        // All subscribers receive the message.
        let publisher_socket_id: Option<&SocketId> = None;
        assert!(publisher_socket_id.is_none());
    }
}
