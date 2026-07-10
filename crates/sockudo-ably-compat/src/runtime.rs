//! Reduced Ably REST and Realtime compatibility for Ably AI Transport tests.
//!
//! This is an additive compatibility surface. Sockudo and Pusher clients still
//! use their native routes and protocol frames; Ably ProtocolMessages are
//! translated at the edge into Sockudo's existing publish, history, and version
//! stores.

use axum::{
    Json,
    body::Bytes,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::Engine as _;
use dashmap::DashMap;
use serde::{Serialize, de::Visitor};
use sockudo_adapter::{
    ConnectionHandler, RealtimeEgressTap,
    services::{MessageService, PublishContext},
};
use sockudo_core::{
    app::App,
    error::{Error as SockudoError, Result as SockudoResult},
    history::{HistoryCursor, HistoryDirection, HistoryQueryBounds, HistoryReadRequest, now_ms},
    message_envelope::{
        MessageContent, MessageEnvelope, VersionProjection, decode_stored_message_payload,
    },
    origin_validation::OriginValidator,
    utils::validate_channel_name,
    websocket::ConnectionCapabilities,
};
use sockudo_protocol::{
    messages::{
        AI_EVENT_CANCEL, AI_EVENT_INPUT, AI_HEADER_INPUT_CLIENT_ID, AI_HEADER_RUN_CLIENT_ID,
        MessageData, MessageExtras, PusherMessage, is_ai_event,
    },
    versioned_messages::{
        AppendMessageRequest, HEADER_VERSION_SERIAL, HEADER_VERSION_TIMESTAMP_MS,
        MessageAction as ProtocolMessageAction, UpdateMessageRequest, extract_runtime_action,
        extract_runtime_append_fragment, extract_runtime_message_serial,
    },
};
use sockudo_ws::{Message, axum_integration::WebSocketUpgrade};
use sonic_rs::{JsonValueTrait, Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::AblyCompatError;
use crate::auth::{basic_credential, bearer_token, parse_ably_key};
use crate::codec::encode_protocol_bytes;
use crate::outbound::{
    AblyOutbound, AblySender, OutboundLimits, OutboundMetrics, OutboundMetricsSnapshot,
    OutboundPriority,
};
use crate::protocol::*;
use crate::services::{
    VersionMutationPath, apply_append_message, apply_delete_message, apply_update_message,
    parse_message_serial,
};

type AppError = AblyCompatError;
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AblyErrorBody {
    error: AblyErrorInfo,
}

#[derive(Debug, Clone)]
struct AblyTokenRecord {
    app_key: String,
    client_id: Option<String>,
    expires_ms: i64,
    capabilities: Option<ConnectionCapabilities>,
}

#[derive(Debug)]
struct ResolvedAblyAuth {
    app: App,
    client_id: Option<String>,
    capabilities: Option<ConnectionCapabilities>,
}

#[derive(Debug)]
struct AblyAuthError {
    status: StatusCode,
    code: u32,
    message: String,
}

#[derive(Debug, Clone)]
struct AblySessionRecord {
    app_id: String,
    connection_id: String,
    client_id: Option<String>,
    expires_at_ms: i64,
}

#[derive(Debug, Clone)]
struct AblyChannelPosition {
    stream_id: String,
    serial: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AblyChannelKey {
    app_id: String,
    channel: String,
}

#[derive(Debug, Clone)]
struct AblyRecoveryFailure {
    code: u32,
    status: StatusCode,
    message: String,
}

#[derive(Debug, Default)]
struct AblyChannelState {
    subscribers: HashMap<String, AblySender>,
    current_stream_id: Option<String>,
    attach_position: Option<AblyChannelPosition>,
    last_touched_ms: i64,
}

enum AblyConnectionStart {
    Fresh,
    Resumed { connection_id: String },
    Failed { error: AblyErrorInfo },
}

impl AblyAuthError {
    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: 40140,
            message: message.into(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: 40160,
            message: message.into(),
        }
    }
}

#[derive(Default)]
pub struct AblyCompatHub {
    channels: DashMap<AblyChannelKey, Arc<Mutex<AblyChannelState>>>,
    sessions: DashMap<String, AblySessionRecord>,
    tokens: DashMap<String, AblyTokenRecord>,
    metrics: Arc<OutboundMetrics>,
}

/// Dependencies used to construct an isolated compatibility runtime.
#[derive(Debug, Clone, Default)]
pub struct AblyCompatDependencies;

/// Explicitly constructed Ably compatibility runtime.
pub struct AblyCompatRuntime {
    hub: Arc<AblyCompatHub>,
}

impl AblyCompatRuntime {
    #[must_use]
    pub fn new(_dependencies: AblyCompatDependencies) -> Self {
        let hub = Arc::new(AblyCompatHub::default());
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let weak_hub = Arc::downgrade(&hub);
            handle.spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_millis(ABLY_COMPAT_EXPIRY_SWEEP_MS));
                while let Some(hub) = weak_hub.upgrade() {
                    interval.tick().await;
                    hub.expire(now_ms());
                }
            });
        }
        Self { hub }
    }

    /// Return a lock-free snapshot of compatibility delivery counters.
    pub fn metrics(&self) -> OutboundMetricsSnapshot {
        self.hub.metrics.snapshot()
    }

    /// Build the compatibility router with this runtime installed as an
    /// extension. Native connection-handler state is supplied by the server.
    pub fn realtime_router(self: &Arc<Self>) -> axum::Router<Arc<ConnectionHandler>> {
        use axum::{Extension, Router, routing::get};

        Router::new()
            .route("/", get(handle_ably_realtime_upgrade))
            .layer(Extension(Arc::clone(self)))
    }

    pub fn rest_router(self: &Arc<Self>) -> axum::Router<Arc<ConnectionHandler>> {
        use axum::{
            Extension, Router,
            routing::{get, post},
        };

        Router::new()
            .route("/time", get(ably_time))
            .route("/keys/{keyName}/requestToken", post(ably_request_token))
            .route("/channels/{channelName}", get(ably_channel_status))
            .route(
                "/channels/{channelName}/messages",
                get(ably_channel_history).post(ably_channel_publish),
            )
            .route(
                "/channels/{channelName}/messages/{messageSerial}",
                get(ably_channel_message).patch(ably_channel_message_mutation),
            )
            .route(
                "/channels/{channelName}/messages/{messageSerial}/versions",
                get(ably_channel_message_versions),
            )
            .layer(Extension(Arc::clone(self)))
    }

    pub fn router(self: &Arc<Self>) -> axum::Router<Arc<ConnectionHandler>> {
        self.realtime_router().merge(self.rest_router())
    }

    pub fn egress_tap(self: &Arc<Self>) -> Arc<dyn RealtimeEgressTap> {
        Arc::clone(self) as Arc<dyn RealtimeEgressTap>
    }
}

impl RealtimeEgressTap for AblyCompatRuntime {
    fn has_subscribers(&self, app_id: &str, channel: &str) -> bool {
        self.hub.has_subscribers(app_id, channel)
    }

    fn deliver(
        &self,
        app_id: &str,
        channel: &str,
        message: &PusherMessage,
        envelope: &MessageEnvelope,
    ) -> SockudoResult<()> {
        self.hub.deliver(app_id, channel, message, envelope)
    }
}

impl RealtimeEgressTap for AblyCompatHub {
    fn has_subscribers(&self, app_id: &str, channel: &str) -> bool {
        self.has_subscribers(app_id, channel)
    }

    fn deliver(
        &self,
        app_id: &str,
        channel: &str,
        message: &PusherMessage,
        envelope: &MessageEnvelope,
    ) -> SockudoResult<()> {
        let ably_message =
            match envelope_to_ably_message(envelope, message, AblyMessageProjection::Mutation) {
                Ok(message) => message,
                Err(error) => {
                    warn!(
                        app_id = %app_id,
                        channel = %channel,
                        error = %error,
                        "failed to translate Sockudo message for Ably compatibility"
                    );
                    return Ok(());
                }
            };
        let channel_serial = envelope
            .stream_id
            .as_deref()
            .zip(envelope.delivery_serial)
            .map(|(stream_id, serial)| encode_ably_channel_serial(stream_id, serial));
        self.broadcast(
            app_id,
            channel,
            AblyProtocolMessage {
                action: ACTION_MESSAGE,
                timestamp: Some(now_ms()),
                channel: Some(channel.to_string()),
                channel_serial: channel_serial.clone(),
                messages: Some(vec![ably_message]),
                ..empty_protocol_message(ACTION_MESSAGE)
            },
        );
        if envelope.action == Some(sockudo_core::versioned_messages::MessageAction::Append)
            && let Ok(aggregate) =
                envelope_to_ably_message(envelope, message, AblyMessageProjection::Aggregate)
        {
            self.broadcast(
                app_id,
                channel,
                AblyProtocolMessage {
                    action: ACTION_MESSAGE,
                    timestamp: Some(now_ms()),
                    channel: Some(channel.to_string()),
                    channel_serial,
                    messages: Some(vec![aggregate]),
                    ..empty_protocol_message(ACTION_MESSAGE)
                },
            );
        }
        Ok(())
    }
}

impl AblyCompatHub {
    fn expire(&self, now: i64) {
        self.sessions.retain(|_, record| record.expires_at_ms > now);
        self.tokens.retain(|_, record| record.expires_ms > now);

        let mut stale_channels = Vec::new();
        for entry in &self.channels {
            let state = lock_channel_state(entry.value());
            if state.subscribers.is_empty()
                && state.attach_position.is_none()
                && now.saturating_sub(state.last_touched_ms)
                    > i64::try_from(DEFAULT_CONNECTION_STATE_TTL_MS).unwrap_or(i64::MAX)
            {
                stale_channels.push(entry.key().clone());
            }
        }
        for key in stale_channels {
            self.channels.remove(&key);
            self.metrics.expiry.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn bound_sessions(&self) {
        if self.sessions.len() < ABLY_COMPAT_MAX_SESSIONS {
            return;
        }
        if let Some(key) = self
            .sessions
            .iter()
            .min_by_key(|entry| entry.expires_at_ms)
            .map(|entry| entry.key().clone())
        {
            self.sessions.remove(&key);
        }
    }

    fn bound_tokens(&self) {
        if self.tokens.len() < ABLY_COMPAT_MAX_TOKENS {
            return;
        }
        if let Some(key) = self
            .tokens
            .iter()
            .min_by_key(|entry| entry.expires_ms)
            .map(|entry| entry.key().clone())
        {
            self.tokens.remove(&key);
        }
    }

    fn has_subscribers(&self, app_id: &str, channel: &str) -> bool {
        let key = channel_key(app_id, channel);
        self.channels
            .get(&key)
            .is_some_and(|state| !lock_channel_state(state.value()).subscribers.is_empty())
    }

    fn channel_state(&self, app_id: &str, channel: &str) -> Arc<Mutex<AblyChannelState>> {
        self.channels
            .entry(channel_key(app_id, channel))
            .or_insert_with(|| Arc::new(Mutex::new(AblyChannelState::default())))
            .clone()
    }

    fn attach_clean(
        &self,
        app_id: &str,
        channel: &str,
        session_id: &str,
        sender: AblySender,
        channel_serial: Option<String>,
    ) {
        let now = now_ms();
        let state = self.channel_state(app_id, channel);
        let mut state = lock_channel_state(&state);
        if let Some(position) = channel_serial
            .as_deref()
            .and_then(|serial| parse_ably_channel_serial(serial).ok())
        {
            state.current_stream_id = Some(position.stream_id.clone());
            state.attach_position = Some(position);
            state.last_touched_ms = now;
        }
        state
            .subscribers
            .insert(session_id.to_string(), sender.clone());
        send_ably_attached(&sender, channel, channel_serial, None, None, Vec::new());
    }

    fn unsubscribe(&self, app_id: &str, channel: &str, session_id: &str) {
        let key = channel_key(app_id, channel);
        let Some(state) = self.channels.get(&key).map(|entry| entry.clone()) else {
            return;
        };
        let mut state = lock_channel_state(&state);
        state.subscribers.remove(session_id);
        if state.subscribers.is_empty() {
            state.attach_position = None;
            drop(state);
            self.channels.remove(&key);
        }
    }

    fn until_attach_position(&self, app_id: &str, channel: &str) -> Option<AblyChannelPosition> {
        let state = self.channel_state(app_id, channel);
        lock_channel_state(&state).attach_position.clone()
    }

    fn broadcast(&self, app_id: &str, channel: &str, message: AblyProtocolMessage) {
        let now = now_ms();
        let state = self.channel_state(app_id, channel);
        let subscribers = {
            let mut state = lock_channel_state(&state);
            if let Some(position) = message
                .channel_serial
                .as_deref()
                .and_then(|serial| parse_ably_channel_serial(serial).ok())
            {
                state.current_stream_id = Some(position.stream_id);
                state.last_touched_ms = now;
            }

            state
                .subscribers
                .iter()
                .map(|(session_id, sender)| (session_id.clone(), sender.clone()))
                .collect::<Vec<_>>()
        };

        let mut grouped = HashMap::<AblyFormat, Vec<(String, AblySender)>>::new();
        for (session_id, sender) in subscribers {
            grouped
                .entry(sender.format())
                .or_default()
                .push((session_id, sender));
        }

        let mut stale = Vec::new();
        for (format, subscribers) in grouped {
            let encoded = match encode_protocol_bytes(&message, format) {
                Ok(bytes) => Arc::new(bytes),
                Err(error) => {
                    warn!(app_id = %app_id, channel = %channel, error = %error, "failed to encode Ably compatibility delivery");
                    continue;
                }
            };
            self.metrics.encoded.fetch_add(1, Ordering::Relaxed);
            for (session_id, subscriber) in subscribers {
                if subscriber.send_data(Arc::clone(&encoded)).is_err() {
                    stale.push(session_id);
                } else {
                    self.metrics.fanout.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        if !stale.is_empty() {
            let mut state = lock_channel_state(&state);
            for session_id in stale {
                state.subscribers.remove(&session_id);
            }
        }
    }

    fn attach_cold_recovery(
        &self,
        app_id: &str,
        channel: &str,
        session_id: &str,
        sender: AblySender,
        position: &AblyChannelPosition,
        replay: Vec<AblyProtocolMessage>,
    ) {
        let now = now_ms();
        let state = self.channel_state(app_id, channel);
        let mut state = lock_channel_state(&state);
        if state
            .current_stream_id
            .as_deref()
            .is_some_and(|current| current != position.stream_id)
        {
            send_ably_attached(
                &sender,
                channel,
                Some(encode_ably_channel_serial(
                    &position.stream_id,
                    position.serial,
                )),
                None,
                Some(AblyRecoveryFailure::channel(
                    90005,
                    format!("unable to recover channel '{channel}' because the stream changed"),
                )),
                Vec::new(),
            );
            return;
        }

        state.current_stream_id = Some(position.stream_id.clone());
        state.last_touched_ms = now;
        let flags = if replay.is_empty() {
            FLAG_RESUMED
        } else {
            FLAG_RESUMED | FLAG_HAS_BACKLOG
        };
        state
            .subscribers
            .insert(session_id.to_string(), sender.clone());
        send_ably_attached(
            &sender,
            channel,
            Some(encode_ably_channel_serial(
                &position.stream_id,
                position.serial,
            )),
            Some(flags),
            None,
            replay,
        );
    }

    fn attach_failed(
        &self,
        app_id: &str,
        channel: &str,
        session_id: &str,
        sender: AblySender,
        channel_serial: Option<String>,
        failure: AblyRecoveryFailure,
    ) {
        let state = self.channel_state(app_id, channel);
        let mut state = lock_channel_state(&state);
        state
            .subscribers
            .insert(session_id.to_string(), sender.clone());
        send_ably_attached(
            &sender,
            channel,
            channel_serial,
            None,
            Some(failure),
            Vec::new(),
        );
    }

    fn begin_connection(
        &self,
        app_id: &str,
        client_id: Option<&str>,
        resume: Option<&str>,
        recover: Option<&str>,
    ) -> AblyConnectionStart {
        let requested_key = resume.or(recover);
        let Some(requested_key) = requested_key else {
            return AblyConnectionStart::Fresh;
        };
        let now = now_ms();
        let Some(record) = self.sessions.get(requested_key).map(|entry| entry.clone()) else {
            return AblyConnectionStart::Failed {
                error: error_info(
                    StatusCode::BAD_REQUEST,
                    80008,
                    "unable to recover connection (connection expired)",
                ),
            };
        };
        if record.expires_at_ms <= now {
            self.sessions.remove(requested_key);
            return AblyConnectionStart::Failed {
                error: error_info(
                    StatusCode::BAD_REQUEST,
                    80008,
                    "unable to recover connection (connection expired)",
                ),
            };
        }
        if record.app_id != app_id || record.client_id.as_deref() != client_id {
            return AblyConnectionStart::Failed {
                error: error_info(
                    StatusCode::BAD_REQUEST,
                    80011,
                    "unable to recover connection (incompatible auth params)",
                ),
            };
        }
        AblyConnectionStart::Resumed {
            connection_id: record.connection_id,
        }
    }

    fn remember_connection(
        &self,
        connection_key: String,
        app_id: &str,
        connection_id: &str,
        client_id: Option<String>,
    ) {
        self.bound_sessions();
        self.sessions.insert(
            connection_key,
            AblySessionRecord {
                app_id: app_id.to_string(),
                connection_id: connection_id.to_string(),
                client_id,
                expires_at_ms: now_ms()
                    .saturating_add(i64::try_from(DEFAULT_CONNECTION_STATE_TTL_MS).unwrap_or(0)),
            },
        );
    }

    fn resolve_connection_id(&self, connection_key: &str) -> Option<String> {
        self.sessions
            .get(connection_key)
            .filter(|record| record.expires_at_ms > now_ms())
            .map(|record| record.connection_id.clone())
    }

    fn issue_token(
        &self,
        app: &App,
        client_id: Option<String>,
        ttl_ms: i64,
        capability: Option<String>,
        capabilities: Option<ConnectionCapabilities>,
    ) -> AblyTokenDetails {
        let issued = now_ms();
        let expires = issued.saturating_add(ttl_ms.max(1));
        let token = format!("sockudo-ably-{}", Uuid::new_v4());
        self.bound_tokens();
        self.tokens.insert(
            token.clone(),
            AblyTokenRecord {
                app_key: app.key.clone(),
                client_id: client_id.clone(),
                expires_ms: expires,
                capabilities,
            },
        );
        AblyTokenDetails {
            token,
            key_name: app.key.clone(),
            issued,
            expires,
            client_id,
            capability,
        }
    }

    fn resolve_token(&self, token: &str) -> Option<AblyTokenRecord> {
        let record = self.tokens.get(token)?.clone();
        if record.expires_ms <= now_ms() {
            self.tokens.remove(token);
            return None;
        }
        Some(record)
    }
}

pub async fn handle_ably_realtime_upgrade(
    Query(params): Query<AblyConnectQuery>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    if !handler.is_accepting() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let format = match parse_ably_format(params.format.as_deref()) {
        Ok(format) => format,
        Err(message) => return ably_error_response(StatusCode::BAD_REQUEST, 40000, message),
    };

    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        params.key.as_deref(),
        params.access_token.as_deref(),
        params.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => return ably_error_response(error.status, error.code, error.message),
    };

    if let Some(allowed_origins) = resolved.app.allowed_origins_ref()
        && !allowed_origins.is_empty()
    {
        let origin = headers
            .get(header::ORIGIN)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        if !OriginValidator::validate_origin(origin, allowed_origins) {
            return ably_error_response(
                StatusCode::FORBIDDEN,
                40300,
                "Origin is not allowed for this app",
            );
        }
    }

    let ws_cfg = handler.server_options().websocket.to_sockudo_ws_config(
        handler.server_options().websocket_max_payload_kb,
        handler.server_options().activity_timeout,
    );
    let hub = Arc::clone(&runtime.hub);
    let resume = params.resume.clone();
    let recover = params.recover.clone();

    ws.config(ws_cfg)
        .on_upgrade(move |socket| async move {
            if let Err(error) = run_ably_realtime_socket(
                socket,
                AblyRealtimeSocketContext {
                    handler,
                    hub,
                    app: resolved.app,
                    client_id: resolved.client_id,
                    capabilities: resolved.capabilities,
                    resume,
                    recover,
                    format,
                },
            )
            .await
            {
                warn!(error = %error, "Ably compatibility socket closed with error");
            }
        })
        .into_response()
}

struct AblyRealtimeSocketContext {
    handler: Arc<ConnectionHandler>,
    hub: Arc<AblyCompatHub>,
    app: App,
    client_id: Option<String>,
    capabilities: Option<ConnectionCapabilities>,
    resume: Option<String>,
    recover: Option<String>,
    format: AblyFormat,
}

async fn run_ably_realtime_socket(
    socket: sockudo_ws::axum_integration::WebSocket,
    context: AblyRealtimeSocketContext,
) -> SockudoResult<()> {
    let AblyRealtimeSocketContext {
        handler,
        hub,
        app,
        client_id,
        capabilities,
        resume,
        recover,
        format,
    } = context;

    let connection_start = hub.begin_connection(
        &app.id,
        client_id.as_deref(),
        resume.as_deref(),
        recover.as_deref(),
    );
    let connection_id = match &connection_start {
        AblyConnectionStart::Resumed { connection_id } => connection_id.clone(),
        AblyConnectionStart::Fresh | AblyConnectionStart::Failed { .. } => {
            format!("sockudo-ably-{}", Uuid::new_v4().simple())
        }
    };
    let connection_key = format!("{}:{}", app.id, Uuid::new_v4().simple());
    let connection_error = match &connection_start {
        AblyConnectionStart::Failed { error } => Some(error.clone()),
        AblyConnectionStart::Fresh | AblyConnectionStart::Resumed { .. } => None,
    };
    hub.remember_connection(
        connection_key.clone(),
        &app.id,
        &connection_id,
        client_id.clone(),
    );
    let session_id = connection_id.clone();
    let (mut reader, mut writer) = socket.split();
    let (sender, mut outbound) =
        AblyOutbound::channel(format, OutboundLimits::default(), Arc::clone(&hub.metrics));
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = outbound.recv().await {
            let frame = match format {
                AblyFormat::Json => Message::Text((*frame.bytes).clone()),
                AblyFormat::MsgPack => Message::Binary((*frame.bytes).clone()),
            };
            if let Err(error) = writer.send(frame).await {
                debug!(error = %error, "Ably compatibility socket writer closed");
                break;
            }
        }
    });
    let heartbeat_sender = sender.clone();
    let heartbeat_task = tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(Duration::from_millis(DEFAULT_MAX_IDLE_INTERVAL_MS / 2));
        loop {
            interval.tick().await;
            if heartbeat_sender
                .send_protocol(
                    &AblyProtocolMessage {
                        action: ACTION_HEARTBEAT,
                        ..empty_protocol_message(ACTION_HEARTBEAT)
                    },
                    OutboundPriority::Control,
                )
                .is_err()
            {
                break;
            }
        }
    });

    send_protocol(
        &sender,
        connected_message(
            &connection_id,
            &connection_key,
            client_id.clone(),
            connection_error,
        ),
    );

    let mut attached_channels = HashSet::new();
    while let Some(frame) = reader.next().await {
        let frame = frame.map_err(|error| SockudoError::Internal(error.to_string()))?;
        let bytes = match frame {
            Message::Text(bytes) | Message::Binary(bytes) => bytes,
            Message::Ping(payload) => {
                send_protocol(
                    &sender,
                    AblyProtocolMessage {
                        action: ACTION_HEARTBEAT,
                        ..empty_protocol_message(ACTION_HEARTBEAT)
                    },
                );
                let _ = payload;
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(_) => break,
        };
        let inbound = decode_ably_protocol_message(bytes.as_ref(), format)
            .map_err(|error| SockudoError::InvalidMessageFormat(error.to_string()))?;
        handle_ably_protocol_message(
            &handler,
            &hub,
            &app,
            &connection_id,
            client_id.as_deref(),
            capabilities.as_ref(),
            &session_id,
            &sender,
            &mut attached_channels,
            inbound,
        )
        .await?;
    }

    for channel in attached_channels {
        hub.unsubscribe(&app.id, &channel, &session_id);
    }
    heartbeat_task.abort();
    let _ = heartbeat_task.await;
    drop(sender);
    let _ = writer_task.await;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_ably_protocol_message(
    handler: &Arc<ConnectionHandler>,
    hub: &Arc<AblyCompatHub>,
    app: &App,
    connection_id: &str,
    client_id: Option<&str>,
    capabilities: Option<&ConnectionCapabilities>,
    session_id: &str,
    sender: &AblySender,
    attached_channels: &mut HashSet<String>,
    inbound: AblyProtocolMessage,
) -> SockudoResult<()> {
    match inbound.action {
        ACTION_HEARTBEAT => {
            send_protocol(
                sender,
                AblyProtocolMessage {
                    action: ACTION_HEARTBEAT,
                    ..empty_protocol_message(ACTION_HEARTBEAT)
                },
            );
        }
        ACTION_CONNECT | ACTION_AUTH => {
            let connection_key = format!("{}:{}", app.id, Uuid::new_v4().simple());
            hub.remember_connection(
                connection_key.clone(),
                &app.id,
                connection_id,
                client_id.map(str::to_string),
            );
            send_protocol(
                sender,
                connected_message(
                    connection_id,
                    &connection_key,
                    client_id.map(str::to_string),
                    None,
                ),
            );
        }
        ACTION_ATTACH => {
            let Some(channel) = inbound.channel else {
                send_protocol_error(sender, 40000, "ATTACH requires channel");
                return Ok(());
            };
            if let Err(error) = validate_channel_name(app, &channel).await {
                send_protocol_error(sender, 40000, error.to_string());
                return Ok(());
            }
            if let Err(error) =
                ensure_ably_capability(capabilities, &channel, AblyCapabilityCheck::Subscribe)
            {
                send_protocol_error(sender, error.code, error.message);
                return Ok(());
            }
            attached_channels.insert(channel.clone());
            handle_ably_attach(
                handler,
                hub,
                app,
                session_id,
                sender,
                &channel,
                inbound.channel_serial,
                inbound.params,
            )
            .await;
        }
        ACTION_DETACH => {
            let Some(channel) = inbound.channel else {
                send_protocol_error(sender, 40000, "DETACH requires channel");
                return Ok(());
            };
            hub.unsubscribe(&app.id, &channel, session_id);
            attached_channels.remove(&channel);
            send_protocol(
                sender,
                AblyProtocolMessage {
                    action: ACTION_DETACHED,
                    channel: Some(channel),
                    ..empty_protocol_message(ACTION_DETACHED)
                },
            );
        }
        ACTION_MESSAGE => {
            handle_ably_publish(
                handler,
                app,
                connection_id,
                client_id,
                capabilities,
                sender,
                inbound,
            )
            .await?;
        }
        ACTION_PRESENCE => {
            handle_ably_presence(
                hub,
                app,
                connection_id,
                client_id,
                capabilities,
                sender,
                inbound,
            )
            .await;
        }
        ACTION_DISCONNECT | ACTION_CLOSE => {
            send_protocol(
                sender,
                AblyProtocolMessage {
                    action: ACTION_CLOSED,
                    ..empty_protocol_message(ACTION_CLOSED)
                },
            );
        }
        _ => {
            send_protocol_error(
                sender,
                40000,
                format!("Unsupported Ably ProtocolMessage action {}", inbound.action),
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_ably_attach(
    handler: &Arc<ConnectionHandler>,
    hub: &Arc<AblyCompatHub>,
    app: &App,
    session_id: &str,
    sender: &AblySender,
    channel: &str,
    channel_serial: Option<String>,
    params: Option<HashMap<String, String>>,
) {
    let rewind_requested = params
        .as_ref()
        .is_some_and(|params| params.contains_key("rewind") || params.contains_key("rewindCount"));
    let Some(channel_serial) = channel_serial else {
        let attach_serial = current_ably_channel_serial(handler, app, channel);
        hub.attach_clean(&app.id, channel, session_id, sender.clone(), attach_serial);
        if rewind_requested {
            send_ably_rewind_latest(handler, app, channel, sender).await;
        }
        return;
    };

    let position = match parse_ably_channel_serial(&channel_serial) {
        Ok(position) => position,
        Err(failure) => {
            hub.attach_failed(
                &app.id,
                channel,
                session_id,
                sender.clone(),
                Some(channel_serial),
                failure,
            );
            return;
        }
    };

    hub.metrics.replay_source.fetch_add(1, Ordering::Relaxed);
    match collect_ably_cold_recovery(handler, app, channel, &position).await {
        Ok(replay) => {
            hub.attach_cold_recovery(
                &app.id,
                channel,
                session_id,
                sender.clone(),
                &position,
                replay,
            );
            if rewind_requested {
                send_ably_rewind_latest(handler, app, channel, sender).await;
            }
        }
        Err(failure) => hub.attach_failed(
            &app.id,
            channel,
            session_id,
            sender.clone(),
            Some(channel_serial),
            failure,
        ),
    }
}

fn current_ably_channel_serial(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
) -> Option<String> {
    #[cfg(feature = "recovery")]
    {
        handler.replay_buffer().map(|replay_buffer| {
            let position = replay_buffer.current_position(&app.id, channel);
            encode_ably_channel_serial(&position.stream_id, position.serial)
        })
    }
    #[cfg(not(feature = "recovery"))]
    {
        let _ = (handler, app, channel);
        None
    }
}

async fn collect_ably_cold_recovery(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    position: &AblyChannelPosition,
) -> Result<Vec<AblyProtocolMessage>, AblyRecoveryFailure> {
    if handler.server_options().versioned_messages.enabled {
        return collect_ably_version_recovery(handler, app, channel, position).await;
    }

    collect_ably_history_recovery(handler, app, channel, position).await
}

async fn send_ably_rewind_latest(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    sender: &AblySender,
) {
    let policy = app.resolved_history(channel, &handler.server_options().history);
    if !policy.enabled {
        return;
    }
    let Ok(page) = handler
        .history_store()
        .read_page(HistoryReadRequest {
            app_id: app.id.clone(),
            channel: channel.to_string(),
            direction: HistoryDirection::NewestFirst,
            limit: 1,
            cursor: None,
            bounds: HistoryQueryBounds {
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
            },
        })
        .await
    else {
        return;
    };
    let Some(item) = page.items.into_iter().next() else {
        return;
    };
    let Ok(stored) = decode_stored_message_payload(item.payload_bytes.as_ref()) else {
        return;
    };
    let projected = match stored.envelope.as_ref() {
        Some(envelope) => ably_protocol_message_from_envelope(
            channel,
            &stored.message,
            envelope,
            AblyMessageProjection::Aggregate,
            Some(encode_ably_channel_serial(&item.stream_id, item.serial)),
        ),
        None => ably_protocol_message_from_pusher(
            channel,
            &stored.message,
            AblyMessageProjection::Aggregate,
            Some(encode_ably_channel_serial(&item.stream_id, item.serial)),
        ),
    };
    if let Ok(message) = projected {
        send_protocol(sender, message);
    }
}

async fn collect_ably_version_recovery(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    position: &AblyChannelPosition,
) -> Result<Vec<AblyProtocolMessage>, AblyRecoveryFailure> {
    let stream_state = handler
        .version_store()
        .stream_state(&app.id, channel)
        .await
        .map_err(|error| {
            AblyRecoveryFailure::channel(
                90000,
                format!("unable to recover channel '{channel}': {error}"),
            )
        })?;

    if stream_state.stream_id.as_deref() != Some(position.stream_id.as_str()) {
        return Err(AblyRecoveryFailure::channel(
            90005,
            format!("unable to recover channel '{channel}' because the stream changed"),
        ));
    }

    if let Some(oldest) = stream_state.oldest_available_delivery_serial
        && position.serial.saturating_add(1) < oldest
    {
        return Err(AblyRecoveryFailure::channel(
            90003,
            format!("unable to recover channel '{channel}' because messages expired"),
        ));
    }

    let newest = stream_state
        .newest_available_delivery_serial
        .unwrap_or(position.serial);
    if newest <= position.serial {
        return Ok(Vec::new());
    }
    let missing = newest.saturating_sub(position.serial);
    let limit = usize::try_from(missing).map_err(|_| {
        AblyRecoveryFailure::channel(
            90004,
            format!("unable to recover channel '{channel}' because the message limit was exceeded"),
        )
    })?;
    if limit > ABLY_COMPAT_MAX_REPLAY_MESSAGES {
        return Err(AblyRecoveryFailure::channel(
            90004,
            format!("unable to recover channel '{channel}' because the message limit was exceeded"),
        ));
    }

    let records = handler
        .version_store()
        .replay_after(sockudo_core::version_store::VersionReplayRequest {
            app_id: app.id.clone(),
            channel: channel.to_string(),
            after_delivery_serial: position.serial,
            limit,
        })
        .await
        .map_err(|error| {
            AblyRecoveryFailure::channel(
                90003,
                format!("unable to recover channel '{channel}': {error}"),
            )
        })?;

    if records.len() != limit {
        return Err(AblyRecoveryFailure::channel(
            90003,
            format!("unable to recover channel '{channel}' because continuity is unprovable"),
        ));
    }

    let mut replay = Vec::with_capacity(records.len());
    for record in records {
        let delivery_serial = record.delivery_serial();
        let runtime =
            handler.build_runtime_message_from_record(&record, Some(position.stream_id.clone()));
        replay.push(ably_protocol_message_from_pusher(
            channel,
            &runtime,
            AblyMessageProjection::Mutation,
            Some(encode_ably_channel_serial(
                &position.stream_id,
                delivery_serial,
            )),
        )?);
    }
    Ok(replay)
}

async fn collect_ably_history_recovery(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    position: &AblyChannelPosition,
) -> Result<Vec<AblyProtocolMessage>, AblyRecoveryFailure> {
    let history_policy = app.resolved_history(channel, &handler.server_options().history);
    if !history_policy.enabled {
        return Err(AblyRecoveryFailure::channel(
            90003,
            format!("unable to recover channel '{channel}' because messages expired"),
        ));
    }

    let stream_state = handler
        .history_store()
        .stream_runtime_state(&app.id, channel)
        .await
        .map_err(|error| {
            AblyRecoveryFailure::channel(
                90000,
                format!("unable to recover channel '{channel}': {error}"),
            )
        })?;
    if !stream_state.recovery_allowed {
        return Err(AblyRecoveryFailure::channel(
            if stream_state.reset_required {
                90005
            } else {
                90000
            },
            stream_state.reason.unwrap_or_else(|| {
                format!("unable to recover channel '{channel}' because continuity is unprovable")
            }),
        ));
    }
    if stream_state.stream_id.as_deref() != Some(position.stream_id.as_str()) {
        return Err(AblyRecoveryFailure::channel(
            90005,
            format!("unable to recover channel '{channel}' because the stream changed"),
        ));
    }

    let mut replay = Vec::new();
    let mut cursor = None;
    let bounds = HistoryQueryBounds {
        start_serial: Some(position.serial.saturating_add(1)),
        end_serial: None,
        start_time_ms: None,
        end_time_ms: None,
    };
    loop {
        if replay.len() >= ABLY_COMPAT_MAX_REPLAY_MESSAGES {
            return Err(AblyRecoveryFailure::channel(
                90004,
                format!(
                    "unable to recover channel '{channel}' because the message limit was exceeded"
                ),
            ));
        }
        let page = handler
            .history_store()
            .read_page(HistoryReadRequest {
                app_id: app.id.clone(),
                channel: channel.to_string(),
                direction: HistoryDirection::OldestFirst,
                limit: history_policy
                    .max_page_size
                    .min(ABLY_COMPAT_MAX_REPLAY_MESSAGES.saturating_sub(replay.len()))
                    .max(1),
                cursor: cursor.clone(),
                bounds: bounds.clone(),
            })
            .await
            .map_err(|error| {
                AblyRecoveryFailure::channel(
                    90003,
                    format!("unable to recover channel '{channel}': {error}"),
                )
            })?;

        if page.retained.stream_id.as_deref() != Some(position.stream_id.as_str()) {
            return Err(AblyRecoveryFailure::channel(
                90005,
                format!("unable to recover channel '{channel}' because the stream changed"),
            ));
        }
        if page.truncated_by_retention {
            return Err(AblyRecoveryFailure::channel(
                90003,
                format!("unable to recover channel '{channel}' because messages expired"),
            ));
        }

        for item in page.items {
            let stored =
                decode_stored_message_payload(item.payload_bytes.as_ref()).map_err(|error| {
                    AblyRecoveryFailure::channel(
                        90000,
                        format!("unable to recover channel '{channel}': {error}"),
                    )
                })?;
            let raw_message = stored.message;
            replay.push(match stored.envelope.as_ref() {
                Some(envelope) => ably_protocol_message_from_envelope(
                    channel,
                    &raw_message,
                    envelope,
                    AblyMessageProjection::Mutation,
                    Some(encode_ably_channel_serial(&item.stream_id, item.serial)),
                )?,
                None => ably_protocol_message_from_pusher(
                    channel,
                    &raw_message,
                    AblyMessageProjection::Mutation,
                    Some(encode_ably_channel_serial(&item.stream_id, item.serial)),
                )?,
            });
        }

        if !page.has_more {
            break;
        }
        cursor = page.next_cursor;
    }

    Ok(replay)
}

async fn handle_ably_publish(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    connection_id: &str,
    client_id: Option<&str>,
    capabilities: Option<&ConnectionCapabilities>,
    sender: &AblySender,
    inbound: AblyProtocolMessage,
) -> SockudoResult<()> {
    let channel = match inbound.channel.clone() {
        Some(channel) => channel,
        None => {
            send_publish_nack(sender, &inbound, 40000, "MESSAGE requires channel");
            return Ok(());
        }
    };
    if let Err(error) = validate_channel_name(app, &channel).await {
        send_publish_nack(sender, &inbound, 40000, error.to_string());
        return Ok(());
    }
    if let Err(error) = ensure_ably_capability(capabilities, &channel, AblyCapabilityCheck::Publish)
    {
        send_publish_nack(sender, &inbound, error.code, error.message);
        return Ok(());
    }

    let messages = inbound.messages.clone().unwrap_or_default();
    let mut serials = Vec::with_capacity(messages.len());
    for (index, message) in messages.into_iter().enumerate() {
        let result =
            publish_ably_message(handler, app, &channel, connection_id, client_id, message).await;

        match result {
            Ok(serial) => serials.push(serial),
            Err(error) => {
                send_publish_nack(
                    sender,
                    &inbound,
                    40000,
                    format!("Failed to publish message {index}: {error}"),
                );
                return Ok(());
            }
        }
    }

    let count = u64::try_from(serials.len()).unwrap_or(u64::MAX);
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_ACK,
            msg_serial: inbound.msg_serial,
            count: Some(count),
            res: Some(json!([{ "serials": serials }])),
            ..empty_protocol_message(ACTION_ACK)
        },
    );
    Ok(())
}

async fn publish_ably_message(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    connection_id: &str,
    client_id: Option<&str>,
    message: AblyMessage,
) -> Result<String, AppError> {
    let action = message.action.unwrap_or(MESSAGE_CREATE);
    match action {
        MESSAGE_CREATE => {
            publish_ably_create(handler, app, channel, connection_id, client_id, message).await
        }
        MESSAGE_APPEND => publish_ably_append(handler, app, channel, client_id, message).await,
        MESSAGE_UPDATE => publish_ably_update(handler, app, channel, client_id, message).await,
        MESSAGE_DELETE => publish_ably_delete(handler, app, channel, client_id, message).await,
        MESSAGE_SUMMARY => Err(AppError::InvalidInput(format!(
            "Ably message action {action} is not implemented by this compatibility layer"
        ))),
        other => Err(AppError::InvalidInput(format!(
            "Unsupported Ably message action {other}"
        ))),
    }
}

async fn publish_ably_create(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    connection_id: &str,
    client_id: Option<&str>,
    message: AblyMessage,
) -> Result<String, AppError> {
    let event_name = message.name.clone();
    let encoding = message.encoding.clone();
    let raw_data = message.data.clone();
    let data = message
        .data
        .map(|value| ably_message_data_to_message_data(value, encoding.as_deref()));
    let mut extras = ably_extras_to_message_extras(message.extras)?;
    if let Some(client_id) = client_id
        && event_name.as_deref().is_some_and(is_ai_event)
    {
        stamp_ai_identity(
            &mut extras,
            event_name.as_deref().unwrap_or_default(),
            client_id,
        )?;
    }
    if let Some(extras) = extras.as_ref() {
        extras
            .validate_ai_headers()
            .map_err(|error| AppError::InvalidInput(error.message))?;
    }
    let pusher_message = PusherMessage {
        event: event_name.clone(),
        channel: Some(channel.to_string()),
        data,
        name: None,
        user_id: client_id.map(str::to_string),
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: message.id.clone(),
        stream_id: None,
        serial: None,
        idempotency_key: message.id.clone(),
        extras,
        delta_sequence: None,
        delta_conflation_key: None,
    };
    let mut envelope = MessageEnvelope::from_message(
        &pusher_message,
        None,
        Some(connection_id.to_string()),
        now_ms(),
    )
    .map_err(AppError::InvalidInput)?;
    envelope.name = message.name;
    envelope.encoding = encoding.map(sockudo_core::message_envelope::EncodingChain);
    envelope.message_id = pusher_message.message_id.clone();
    if let Some(raw_data) = raw_data
        && envelope.encoding.is_some()
        && let Some(raw) = raw_data.as_str()
    {
        envelope.data = Some(MessageContent::Text(raw.to_string()));
    }
    let ack = MessageService::new(Arc::clone(handler))
        .publish_message(
            app,
            channel,
            pusher_message,
            PublishContext {
                actor_client_id: client_id.map(str::to_string),
                publisher_connection_id: Some(connection_id.to_string()),
                envelope: Some(envelope),
                ..Default::default()
            },
        )
        .await?;
    Ok(ack
        .map(|ack| ack.message_serial)
        .or(message.serial)
        .unwrap_or_else(|| format!("{connection_id}:{}", now_ms())))
}

async fn publish_ably_append(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    client_id: Option<&str>,
    message: AblyMessage,
) -> Result<String, AppError> {
    let serial = message.serial.clone().ok_or_else(|| {
        AppError::InvalidInput("appendMessage requires message.serial".to_string())
    })?;
    let data = match message.data.as_ref() {
        Some(value) => value.as_str().ok_or_else(|| {
            AppError::InvalidInput("appendMessage data must be a string".to_string())
        })?,
        None => "",
    }
    .to_string();
    let request = AppendMessageRequest {
        data,
        extras: ably_extras_to_message_extras(message.extras)?,
        client_id: message
            .version
            .as_ref()
            .and_then(|version| version.client_id.clone())
            .or_else(|| client_id.map(str::to_string)),
        socket_id: None,
        description: message
            .version
            .as_ref()
            .and_then(|version| version.description.clone()),
        metadata: message
            .version
            .as_ref()
            .and_then(|version| version.metadata.clone()),
        op_id: message.id,
    };
    let payload = apply_append_message(
        VersionMutationPath {
            app_id: app.id.clone(),
            channel_name: channel.to_string(),
            message_serial: serial,
        },
        app.clone(),
        Arc::clone(handler),
        request,
    )
    .await?;
    Ok(payload.version_serial.unwrap_or(payload.message_serial))
}

async fn publish_ably_update(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    client_id: Option<&str>,
    message: AblyMessage,
) -> Result<String, AppError> {
    let serial = message.serial.clone().ok_or_else(|| {
        AppError::InvalidInput("updateMessage requires message.serial".to_string())
    })?;
    let request = UpdateMessageRequest {
        name: message.name,
        data: message
            .data
            .map(|value| ably_message_data_to_message_data(value, message.encoding.as_deref())),
        extras: ably_extras_to_message_extras(message.extras)?,
        clear_fields: Vec::new(),
        client_id: message
            .version
            .as_ref()
            .and_then(|version| version.client_id.clone())
            .or_else(|| client_id.map(str::to_string)),
        socket_id: None,
        description: message
            .version
            .as_ref()
            .and_then(|version| version.description.clone()),
        metadata: message
            .version
            .as_ref()
            .and_then(|version| version.metadata.clone()),
        op_id: message.id,
    };
    let payload = apply_update_message(
        VersionMutationPath {
            app_id: app.id.clone(),
            channel_name: channel.to_string(),
            message_serial: serial,
        },
        app.clone(),
        Arc::clone(handler),
        request,
    )
    .await?;
    Ok(payload.version_serial.unwrap_or(payload.message_serial))
}

async fn publish_ably_delete(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    client_id: Option<&str>,
    message: AblyMessage,
) -> Result<String, AppError> {
    let serial = message.serial.clone().ok_or_else(|| {
        AppError::InvalidInput("deleteMessage requires message.serial".to_string())
    })?;
    let request = sockudo_protocol::versioned_messages::DeleteMessageRequest {
        data: message
            .data
            .map(|value| ably_message_data_to_message_data(value, message.encoding.as_deref())),
        extras: ably_extras_to_message_extras(message.extras)?,
        clear_fields: Vec::new(),
        client_id: message
            .version
            .as_ref()
            .and_then(|version| version.client_id.clone())
            .or_else(|| client_id.map(str::to_string)),
        socket_id: None,
        description: message
            .version
            .as_ref()
            .and_then(|version| version.description.clone()),
        metadata: message
            .version
            .as_ref()
            .and_then(|version| version.metadata.clone()),
        op_id: message.id,
    };
    let payload = apply_delete_message(
        VersionMutationPath {
            app_id: app.id.clone(),
            channel_name: channel.to_string(),
            message_serial: serial,
        },
        app.clone(),
        Arc::clone(handler),
        request,
    )
    .await?;
    Ok(payload.version_serial.unwrap_or(payload.message_serial))
}

async fn handle_ably_presence(
    hub: &Arc<AblyCompatHub>,
    app: &App,
    connection_id: &str,
    client_id: Option<&str>,
    capabilities: Option<&ConnectionCapabilities>,
    sender: &AblySender,
    inbound: AblyProtocolMessage,
) {
    let Some(channel) = inbound.channel.clone() else {
        send_publish_nack(sender, &inbound, 40000, "PRESENCE requires channel");
        return;
    };
    if let Err(error) = validate_channel_name(app, &channel).await {
        send_publish_nack(sender, &inbound, 40000, error.to_string());
        return;
    }
    if let Err(error) =
        ensure_ably_capability(capabilities, &channel, AblyCapabilityCheck::Presence)
    {
        send_publish_nack(sender, &inbound, error.code, error.message);
        return;
    }
    let mut presence = inbound.presence.unwrap_or_default();
    for member in &mut presence {
        member
            .connection_id
            .get_or_insert_with(|| connection_id.to_string());
        if let Some(client_id) = client_id {
            member
                .client_id
                .get_or_insert_with(|| client_id.to_string());
        }
        member.timestamp.get_or_insert_with(now_ms);
    }
    hub.broadcast(
        &app.id,
        &channel,
        AblyProtocolMessage {
            action: ACTION_PRESENCE,
            channel: Some(channel.clone()),
            presence: Some(presence),
            timestamp: Some(now_ms()),
            ..empty_protocol_message(ACTION_PRESENCE)
        },
    );
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_ACK,
            msg_serial: inbound.msg_serial,
            count: inbound.count.or(Some(1)),
            ..empty_protocol_message(ACTION_ACK)
        },
    );
}

pub async fn ably_time() -> Response {
    (StatusCode::OK, Json(vec![now_ms()])).into_response()
}

pub async fn ably_channel_history(
    Path(channel_name): Path<String>,
    Query(query): Query<AblyHistoryQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    match ably_channel_history_inner(&runtime.hub, channel_name, query, headers, handler).await {
        Ok(response) => response,
        Err(error) => ably_app_error_response(error),
    }
}

pub async fn ably_channel_publish(
    Path(channel_name): Path<String>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    match ably_channel_publish_inner(&runtime.hub, channel_name, query, headers, handler, body)
        .await
    {
        Ok(response) => response,
        Err(error) => ably_app_error_response(error),
    }
}

async fn ably_channel_publish_inner(
    hub: &AblyCompatHub,
    channel_name: String,
    query: AblyRestQuery,
    headers: HeaderMap,
    handler: Arc<ConnectionHandler>,
    body: Bytes,
) -> Result<Response, AppError> {
    let request_format = ably_rest_request_format(&headers);
    let response_format = ably_rest_response_format(&headers, request_format);
    let resolved = resolve_ably_auth(
        hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    .map_err(|error| AppError::ApiAuthFailed(error.message))?;
    validate_channel_name(&resolved.app, &channel_name).await?;
    ensure_ably_capability_app_error(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::Publish,
    )?;

    let messages = decode_ably_publish_payload(body.as_ref(), request_format)?;
    if messages.is_empty() {
        return Err(AppError::InvalidInput(
            "Ably REST publish requires at least one message".to_string(),
        ));
    }

    let rest_connection_id = format!("rest-{}", Uuid::new_v4().simple());
    let mut serials = Vec::with_capacity(messages.len());
    for (index, message) in messages.into_iter().enumerate() {
        let effective_client_id =
            effective_ably_client_id(resolved.client_id.as_deref(), &message)?;
        let connection_id = message
            .connection_id
            .as_deref()
            .and_then(|key| hub.resolve_connection_id(key))
            .unwrap_or_else(|| rest_connection_id.clone());
        let serial = publish_ably_message(
            &handler,
            &resolved.app,
            &channel_name,
            &connection_id,
            effective_client_id.as_deref(),
            message,
        )
        .await
        .map_err(|error| {
            AppError::InvalidInput(format!("Failed to publish message {index}: {error}"))
        })?;
        serials.push(Some(serial));
    }

    encode_ably_rest_response(
        StatusCode::CREATED,
        response_format,
        &AblyPublishResponse { serials },
    )
}

async fn ably_channel_history_inner(
    hub: &AblyCompatHub,
    channel_name: String,
    query: AblyHistoryQuery,
    headers: HeaderMap,
    handler: Arc<ConnectionHandler>,
) -> Result<Response, AppError> {
    let response_format = ably_rest_response_format(&headers, AblyFormat::Json);
    let resolved = resolve_ably_auth(
        hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    .map_err(|error| AppError::ApiAuthFailed(error.message))?;
    validate_channel_name(&resolved.app, &channel_name).await?;
    ensure_ably_capability_app_error(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::History,
    )?;
    let history_policy = resolved
        .app
        .resolved_history(&channel_name, &handler.server_options().history);
    if !history_policy.enabled {
        return encode_ably_rest_response(
            StatusCode::OK,
            response_format,
            &Vec::<AblyMessage>::new(),
        );
    }
    let limit = query
        .limit
        .unwrap_or(history_policy.max_page_size)
        .min(history_policy.max_page_size)
        .max(1);
    let direction = parse_ably_history_direction(query.direction.as_deref())?;
    let cursor = match query.cursor.as_deref() {
        Some(cursor) => Some(HistoryCursor::decode(cursor)?),
        None => None,
    };
    let attach_position = query
        .from_serial
        .as_deref()
        .and_then(|serial| parse_ably_channel_serial(serial).ok())
        .or_else(|| {
            query
                .until_attach
                .unwrap_or(false)
                .then(|| hub.until_attach_position(&resolved.app.id, &channel_name))
                .flatten()
        });
    let bounds = HistoryQueryBounds {
        start_serial: None,
        end_serial: attach_position.as_ref().map(|position| position.serial),
        start_time_ms: query.start,
        end_time_ms: query.end,
    };
    let page = handler
        .history_store()
        .read_page(HistoryReadRequest {
            app_id: resolved.app.id.clone(),
            channel: channel_name.clone(),
            direction,
            limit,
            cursor,
            bounds,
        })
        .await?;

    let mut items = Vec::with_capacity(page.items.len());
    for item in page.items {
        let stored = decode_stored_message_payload(item.payload_bytes.as_ref())
            .map_err(AppError::InternalError)?;
        let raw_message = stored.message;
        let mut envelope = stored.envelope;
        let message = if handler.server_options().versioned_messages.enabled {
            if let Some(message_serial) = extract_runtime_message_serial(&raw_message) {
                match handler
                    .version_store()
                    .get_latest(
                        &resolved.app.id,
                        &channel_name,
                        &parse_message_serial(message_serial)?,
                    )
                    .await?
                {
                    Some(latest) => {
                        envelope = latest.envelope.clone().or(envelope);
                        handler.build_runtime_message_from_record(
                            &latest,
                            Some(item.stream_id.clone()),
                        )
                    }
                    None => raw_message,
                }
            } else {
                raw_message
            }
        } else {
            raw_message
        };
        items.push(match envelope.as_ref() {
            Some(envelope) => {
                envelope_to_ably_message(envelope, &message, AblyMessageProjection::Aggregate)
                    .map_err(AppError::InvalidInput)?
            }
            None => pusher_to_ably_message(&message, AblyMessageProjection::Aggregate)
                .map_err(AppError::InvalidInput)?,
        });
    }
    encode_ably_rest_response(StatusCode::OK, response_format, &items)
}

pub async fn ably_channel_status(
    Path(channel_name): Path<String>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => return ably_error_response(error.status, error.code, error.message),
    };
    if let Err(error) = validate_channel_name(&resolved.app, &channel_name).await {
        return ably_error_response(StatusCode::BAD_REQUEST, 40000, error.to_string());
    }
    if let Err(error) = ensure_ably_capability(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::AnyChannelAccess,
    ) {
        return ably_error_response(error.status, error.code, error.message);
    }
    let occupancy = handler
        .connection_manager()
        .get_channel_socket_count(&resolved.app.id, &channel_name)
        .await;
    (
        StatusCode::OK,
        Json(json!({
            "channelId": channel_name,
            "status": {
                "isActive": occupancy > 0,
                "occupancy": {
                    "metrics": {
                        "connections": occupancy,
                        "publishers": 0,
                        "subscribers": occupancy,
                        "presenceConnections": 0,
                        "presenceMembers": 0,
                        "presenceSubscribers": 0,
                    }
                }
            }
        })),
    )
        .into_response()
}

pub async fn ably_channel_message(
    Path((channel_name, message_serial)): Path<(String, String)>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => return ably_error_response(error.status, error.code, error.message),
    };
    if let Err(error) = validate_channel_name(&resolved.app, &channel_name).await {
        return ably_error_response(StatusCode::BAD_REQUEST, 40000, error.to_string());
    }
    if let Err(error) = ensure_ably_capability(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::History,
    ) {
        return ably_error_response(error.status, error.code, error.message);
    }
    let response_format = ably_rest_response_format(&headers, AblyFormat::Json);
    match ably_channel_message_inner(handler, resolved.app, channel_name, message_serial).await {
        Ok(message) => encode_ably_rest_response(StatusCode::OK, response_format, &message)
            .unwrap_or_else(ably_app_error_response),
        Err(error) => ably_app_error_response(error),
    }
}

async fn ably_channel_message_inner(
    handler: Arc<ConnectionHandler>,
    app: App,
    channel_name: String,
    message_serial: String,
) -> Result<AblyMessage, AppError> {
    validate_channel_name(&app, &channel_name).await?;
    let message_serial_value = parse_message_serial(&message_serial)?;
    let item = MessageService::new(Arc::clone(&handler))
        .get_message(&app.id, &channel_name, &message_serial_value)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!(
                "Message '{}' was not found in channel '{}'",
                message_serial, channel_name
            ))
        })?;
    let runtime_message = handler.build_runtime_message_from_record(&item, None);
    match item.envelope.as_ref() {
        Some(envelope) => {
            envelope_to_ably_message(envelope, &runtime_message, AblyMessageProjection::Aggregate)
                .map_err(AppError::InvalidInput)
        }
        None => pusher_to_ably_message(&runtime_message, AblyMessageProjection::Aggregate)
            .map_err(AppError::InvalidInput),
    }
}

pub async fn ably_channel_message_versions(
    Path((channel_name, message_serial)): Path<(String, String)>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => return ably_error_response(error.status, error.code, error.message),
    };
    if let Err(error) = validate_channel_name(&resolved.app, &channel_name).await {
        return ably_error_response(StatusCode::BAD_REQUEST, 40000, error.to_string());
    }
    if let Err(error) = ensure_ably_capability(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::History,
    ) {
        return ably_error_response(error.status, error.code, error.message);
    }
    let response_format = ably_rest_response_format(&headers, AblyFormat::Json);
    match ably_channel_message_versions_inner(handler, resolved.app, channel_name, message_serial)
        .await
    {
        Ok(messages) => encode_ably_rest_response(StatusCode::OK, response_format, &messages)
            .unwrap_or_else(ably_app_error_response),
        Err(error) => ably_app_error_response(error),
    }
}

pub async fn ably_channel_message_mutation(
    Path((channel_name, _message_serial)): Path<(String, String)>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let response_format = ably_rest_response_format(&headers, request_format);
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => return ably_error_response(error.status, error.code, error.message),
    };
    let mut messages = match decode_ably_publish_payload(body.as_ref(), request_format) {
        Ok(messages) => messages,
        Err(error) => return ably_app_error_response(error),
    };
    let Some(message) = messages.pop() else {
        return ably_error_response(
            StatusCode::BAD_REQUEST,
            40000,
            "mutation message is required",
        );
    };
    let effective_client_id =
        match effective_ably_client_id(resolved.client_id.as_deref(), &message) {
            Ok(client_id) => client_id,
            Err(error) => return ably_app_error_response(error),
        };
    let connection_id = format!("rest-{}", Uuid::new_v4().simple());
    match publish_ably_message(
        &handler,
        &resolved.app,
        &channel_name,
        &connection_id,
        effective_client_id.as_deref(),
        message,
    )
    .await
    {
        Ok(version_serial) => encode_ably_rest_response(
            StatusCode::OK,
            response_format,
            &json!({"versionSerial": version_serial}),
        )
        .unwrap_or_else(ably_app_error_response),
        Err(error) => ably_app_error_response(error),
    }
}

async fn ably_channel_message_versions_inner(
    handler: Arc<ConnectionHandler>,
    app: App,
    channel_name: String,
    message_serial: String,
) -> Result<Vec<AblyMessage>, AppError> {
    validate_channel_name(&app, &channel_name).await?;
    let message_serial_value = parse_message_serial(&message_serial)?;
    let versions = MessageService::new(Arc::clone(&handler))
        .get_message_versions(sockudo_core::version_store::VersionStoreReadRequest {
            app_id: app.id.clone(),
            channel: channel_name.clone(),
            message_serial: message_serial_value,
            direction: sockudo_core::version_store::VersionStoreDirection::NewestFirst,
            limit: handler
                .server_options()
                .versioned_messages
                .max_page_size
                .max(1),
            cursor: None,
        })
        .await?;
    versions
        .items
        .iter()
        .map(|record| {
            let runtime_message = handler.build_runtime_message_from_record(record, None);
            match record.envelope.as_ref() {
                Some(envelope) => envelope_to_ably_message(
                    envelope,
                    &runtime_message,
                    AblyMessageProjection::Mutation,
                )
                .map_err(AppError::InvalidInput),
                None => pusher_to_ably_message(&runtime_message, AblyMessageProjection::Mutation)
                    .map_err(AppError::InvalidInput),
            }
        })
        .collect()
}

pub async fn ably_request_token(
    Path(key_name): Path<String>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(request): Json<AblyTokenRequest>,
) -> Response {
    let body_key_name = request.key_name.as_deref().unwrap_or(&key_name);
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        Some(body_key_name),
        None,
        request.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => return ably_error_response(error.status, error.code, error.message),
    };
    if resolved.app.key != body_key_name {
        return ably_error_response(
            StatusCode::FORBIDDEN,
            40160,
            "Token keyName does not match authenticated app",
        );
    }
    let (capability, capabilities) = match normalise_ably_token_capability(request.capability) {
        Ok(parsed) => parsed,
        Err(error) => {
            return ably_error_response(StatusCode::BAD_REQUEST, 40000, error.to_string());
        }
    };
    let token = runtime.hub.issue_token(
        &resolved.app,
        request.client_id.or(resolved.client_id),
        request.ttl.unwrap_or(DEFAULT_TOKEN_TTL_MS),
        capability,
        capabilities,
    );
    (StatusCode::OK, Json(token)).into_response()
}

impl AblyRecoveryFailure {
    fn channel(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
}

fn lock_channel_state(
    state: &Arc<Mutex<AblyChannelState>>,
) -> std::sync::MutexGuard<'_, AblyChannelState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn encode_ably_channel_serial(stream_id: &str, serial: u64) -> String {
    format!("{stream_id}:{serial}")
}

fn parse_ably_channel_serial(raw: &str) -> Result<AblyChannelPosition, AblyRecoveryFailure> {
    let Some((stream_id, serial)) = raw.rsplit_once(':') else {
        return Err(AblyRecoveryFailure::channel(
            90005,
            "unable to recover channel (no matching epoch)",
        ));
    };
    if stream_id.is_empty() {
        return Err(AblyRecoveryFailure::channel(
            90005,
            "unable to recover channel (no matching epoch)",
        ));
    }
    let serial = serial.parse::<u64>().map_err(|_| {
        AblyRecoveryFailure::channel(90005, "unable to recover channel (invalid channel serial)")
    })?;
    Ok(AblyChannelPosition {
        stream_id: stream_id.to_string(),
        serial,
    })
}

fn send_ably_attached(
    sender: &AblySender,
    channel: &str,
    channel_serial: Option<String>,
    flags: Option<u64>,
    failure: Option<AblyRecoveryFailure>,
    replay: Vec<AblyProtocolMessage>,
) {
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_ATTACHED,
            channel: Some(channel.to_string()),
            flags,
            channel_serial,
            error: failure.map(|failure| error_info(failure.status, failure.code, failure.message)),
            ..empty_protocol_message(ACTION_ATTACHED)
        },
    );
    for message in replay {
        send_protocol(sender, message);
    }
}

fn ably_protocol_message_from_pusher(
    channel: &str,
    message: &PusherMessage,
    projection: AblyMessageProjection,
    channel_serial: Option<String>,
) -> Result<AblyProtocolMessage, AblyRecoveryFailure> {
    let ably_message = pusher_to_ably_message(message, projection).map_err(|error| {
        AblyRecoveryFailure::channel(
            90000,
            format!("unable to recover channel '{channel}': {error}"),
        )
    })?;
    Ok(AblyProtocolMessage {
        action: ACTION_MESSAGE,
        timestamp: None,
        channel: Some(channel.to_string()),
        channel_serial,
        messages: Some(vec![ably_message]),
        ..empty_protocol_message(ACTION_MESSAGE)
    })
}

fn ably_protocol_message_from_envelope(
    channel: &str,
    message: &PusherMessage,
    envelope: &MessageEnvelope,
    projection: AblyMessageProjection,
    channel_serial: Option<String>,
) -> Result<AblyProtocolMessage, AblyRecoveryFailure> {
    let ably_message =
        envelope_to_ably_message(envelope, message, projection).map_err(|error| {
            AblyRecoveryFailure::channel(
                90000,
                format!("unable to recover channel '{channel}': {error}"),
            )
        })?;
    Ok(AblyProtocolMessage {
        action: ACTION_MESSAGE,
        timestamp: envelope.published_at_ms,
        channel: Some(channel.to_string()),
        channel_serial,
        messages: Some(vec![ably_message]),
        ..empty_protocol_message(ACTION_MESSAGE)
    })
}

fn connected_message(
    connection_id: &str,
    connection_key: &str,
    client_id: Option<String>,
    error: Option<AblyErrorInfo>,
) -> AblyProtocolMessage {
    AblyProtocolMessage {
        action: ACTION_CONNECTED,
        timestamp: None,
        error,
        connection_id: Some(connection_id.to_string()),
        connection_details: Some(AblyConnectionDetails {
            client_id,
            connection_key: connection_key.to_string(),
            connection_state_ttl: DEFAULT_CONNECTION_STATE_TTL_MS,
            max_idle_interval: DEFAULT_MAX_IDLE_INTERVAL_MS,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            max_frame_size: DEFAULT_MAX_MESSAGE_SIZE,
        }),
        ..empty_protocol_message(ACTION_CONNECTED)
    }
}

#[derive(Debug, Clone, Copy)]
enum AblyCapabilityCheck {
    Publish,
    Subscribe,
    History,
    Presence,
    AnyChannelAccess,
}

impl AblyCapabilityCheck {
    fn label(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Subscribe => "subscribe",
            Self::History => "history",
            Self::Presence => "presence",
            Self::AnyChannelAccess => "channel access",
        }
    }
}

fn ensure_ably_capability(
    capabilities: Option<&ConnectionCapabilities>,
    channel: &str,
    check: AblyCapabilityCheck,
) -> Result<(), AblyAuthError> {
    let Some(capabilities) = capabilities else {
        return Ok(());
    };

    let allowed = match check {
        AblyCapabilityCheck::Publish => capabilities.allows_publish(channel),
        AblyCapabilityCheck::Subscribe => capabilities.allows_subscribe(channel),
        AblyCapabilityCheck::History => capabilities.allows_history(channel),
        AblyCapabilityCheck::Presence => capabilities
            .presence
            .as_deref()
            .is_some_and(|patterns| ConnectionCapabilities::matches_any(patterns, channel)),
        AblyCapabilityCheck::AnyChannelAccess => {
            capabilities.allows_publish(channel)
                || capabilities.allows_subscribe(channel)
                || capabilities.allows_history(channel)
                || capabilities
                    .presence
                    .as_deref()
                    .is_some_and(|patterns| ConnectionCapabilities::matches_any(patterns, channel))
                || capabilities.allows_annotation_subscribe(channel)
                || capabilities.allows_annotation_publish(channel)
                || capabilities.allows_annotation_delete_own(channel)
                || capabilities.allows_annotation_delete_any(channel)
                || capabilities.allows_message_mutation_own(
                    sockudo_core::versioned_message_auth::MutationKind::Append,
                    channel,
                )
                || capabilities.allows_message_mutation_any(
                    sockudo_core::versioned_message_auth::MutationKind::Append,
                    channel,
                )
                || capabilities.allows_message_mutation_own(
                    sockudo_core::versioned_message_auth::MutationKind::Update,
                    channel,
                )
                || capabilities.allows_message_mutation_any(
                    sockudo_core::versioned_message_auth::MutationKind::Update,
                    channel,
                )
                || capabilities.allows_message_mutation_own(
                    sockudo_core::versioned_message_auth::MutationKind::Delete,
                    channel,
                )
                || capabilities.allows_message_mutation_any(
                    sockudo_core::versioned_message_auth::MutationKind::Delete,
                    channel,
                )
        }
    };

    if allowed {
        Ok(())
    } else {
        Err(AblyAuthError::forbidden(format!(
            "Ably token lacks {} capability for channel '{}'",
            check.label(),
            channel
        )))
    }
}

fn ensure_ably_capability_app_error(
    capabilities: Option<&ConnectionCapabilities>,
    channel: &str,
    check: AblyCapabilityCheck,
) -> Result<(), AppError> {
    ensure_ably_capability(capabilities, channel, check)
        .map_err(|error| AppError::Forbidden(error.message))
}

fn normalise_ably_token_capability(
    capability: Option<serde_json::Value>,
) -> Result<(Option<String>, Option<ConnectionCapabilities>), AppError> {
    let Some(capability) = capability else {
        return Ok((None, None));
    };

    let parsed = match &capability {
        serde_json::Value::String(raw) => {
            serde_json::from_str::<serde_json::Value>(raw).map_err(|error| {
                AppError::InvalidInput(format!("Invalid Ably token capability JSON: {error}"))
            })?
        }
        serde_json::Value::Object(_) => capability.clone(),
        _ => {
            return Err(AppError::InvalidInput(
                "Ably token capability must be a JSON object or JSON object string".to_string(),
            ));
        }
    };

    let capabilities = ably_capability_value_to_sockudo(&parsed)?;
    let capability = match capability {
        serde_json::Value::String(raw) => raw,
        _ => serde_json::to_string(&parsed).map_err(|error| {
            AppError::InvalidInput(format!("Invalid Ably token capability: {error}"))
        })?,
    };
    Ok((Some(capability), Some(capabilities)))
}

fn ably_capability_value_to_sockudo(
    value: &serde_json::Value,
) -> Result<ConnectionCapabilities, AppError> {
    let object = value.as_object().ok_or_else(|| {
        AppError::InvalidInput("Ably token capability must be a JSON object".to_string())
    })?;
    let mut capabilities = restricted_ably_capabilities();

    for (resource, operations) in object {
        if resource.is_empty() {
            return Err(AppError::InvalidInput(
                "Ably token capability resource must not be empty".to_string(),
            ));
        }
        let operations = operations.as_array().ok_or_else(|| {
            AppError::InvalidInput(format!(
                "Ably token capability for '{resource}' must be an array"
            ))
        })?;
        for operation in operations {
            let operation = operation.as_str().ok_or_else(|| {
                AppError::InvalidInput(format!(
                    "Ably token capability operation for '{resource}' must be a string"
                ))
            })?;
            add_ably_capability_operation(&mut capabilities, resource, operation)?;
        }
    }

    Ok(capabilities)
}

fn restricted_ably_capabilities() -> ConnectionCapabilities {
    ConnectionCapabilities {
        subscribe: Some(Vec::new()),
        publish: Some(Vec::new()),
        history: Some(Vec::new()),
        presence: Some(Vec::new()),
        annotation_subscribe: Some(Vec::new()),
        annotation_publish: Some(Vec::new()),
        annotation_delete_own: Some(Vec::new()),
        annotation_delete_any: Some(Vec::new()),
        message_update_own: Some(Vec::new()),
        message_update_any: Some(Vec::new()),
        message_delete_own: Some(Vec::new()),
        message_delete_any: Some(Vec::new()),
        message_append_own: Some(Vec::new()),
        message_append_any: Some(Vec::new()),
    }
}

fn add_ably_capability_operation(
    capabilities: &mut ConnectionCapabilities,
    resource: &str,
    operation: &str,
) -> Result<(), AppError> {
    match operation {
        "*" => {
            add_all_supported_ably_capabilities(capabilities, resource);
            Ok(())
        }
        "publish" => {
            add_capability_pattern(&mut capabilities.publish, resource);
            Ok(())
        }
        "subscribe" => {
            add_capability_pattern(&mut capabilities.subscribe, resource);
            Ok(())
        }
        "history" => {
            add_capability_pattern(&mut capabilities.history, resource);
            Ok(())
        }
        "presence" => {
            add_capability_pattern(&mut capabilities.presence, resource);
            Ok(())
        }
        "annotation-subscribe" => {
            add_capability_pattern(&mut capabilities.annotation_subscribe, resource);
            Ok(())
        }
        "annotation-publish" => {
            add_capability_pattern(&mut capabilities.annotation_publish, resource);
            Ok(())
        }
        "message-update-own" => {
            add_capability_pattern(&mut capabilities.message_update_own, resource);
            Ok(())
        }
        "message-update-any" => {
            add_capability_pattern(&mut capabilities.message_update_any, resource);
            Ok(())
        }
        "message-delete-own" => {
            add_capability_pattern(&mut capabilities.message_delete_own, resource);
            Ok(())
        }
        "message-delete-any" => {
            add_capability_pattern(&mut capabilities.message_delete_any, resource);
            Ok(())
        }
        "object-subscribe" | "object-publish" | "stats" | "channel-metadata" | "push-subscribe"
        | "push-admin" | "privileged-headers" => Ok(()),
        other => Err(AppError::InvalidInput(format!(
            "Unsupported Ably token capability operation '{other}'"
        ))),
    }
}

fn add_all_supported_ably_capabilities(capabilities: &mut ConnectionCapabilities, resource: &str) {
    add_capability_pattern(&mut capabilities.publish, resource);
    add_capability_pattern(&mut capabilities.subscribe, resource);
    add_capability_pattern(&mut capabilities.history, resource);
    add_capability_pattern(&mut capabilities.presence, resource);
    add_capability_pattern(&mut capabilities.annotation_subscribe, resource);
    add_capability_pattern(&mut capabilities.annotation_publish, resource);
    add_capability_pattern(&mut capabilities.annotation_delete_own, resource);
    add_capability_pattern(&mut capabilities.annotation_delete_any, resource);
    add_capability_pattern(&mut capabilities.message_update_own, resource);
    add_capability_pattern(&mut capabilities.message_update_any, resource);
    add_capability_pattern(&mut capabilities.message_delete_own, resource);
    add_capability_pattern(&mut capabilities.message_delete_any, resource);
    add_capability_pattern(&mut capabilities.message_append_own, resource);
    add_capability_pattern(&mut capabilities.message_append_any, resource);
}

fn add_capability_pattern(patterns: &mut Option<Vec<String>>, pattern: &str) {
    patterns
        .get_or_insert_with(Vec::new)
        .push(pattern.to_string());
}

async fn resolve_ably_auth(
    hub: &AblyCompatHub,
    handler: &Arc<ConnectionHandler>,
    headers: &HeaderMap,
    query_key: Option<&str>,
    access_token: Option<&str>,
    query_client_id: Option<&str>,
) -> Result<ResolvedAblyAuth, AblyAuthError> {
    let header_client_id = headers
        .get("x-ably-clientid")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            base64::engine::general_purpose::STANDARD
                .decode(value)
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .or_else(|| Some(value.to_string()))
        });
    let requested_client_id = query_client_id.or(header_client_id.as_deref());
    let access_token = access_token
        .map(str::to_string)
        .or_else(|| bearer_token(headers));
    if let Some(access_token) = access_token {
        let record = hub
            .resolve_token(&access_token)
            .ok_or_else(|| AblyAuthError::unauthorized("Invalid or expired Ably token"))?;
        let app = find_enabled_app_by_key(handler, &record.app_key).await?;
        let client_id = resolve_ably_token_client_id(record.client_id, requested_client_id)?;
        return Ok(ResolvedAblyAuth {
            app,
            client_id,
            capabilities: record.capabilities,
        });
    }

    let credential = query_key
        .map(str::to_string)
        .or_else(|| basic_credential(headers))
        .ok_or_else(|| AblyAuthError::unauthorized("Missing Ably key credentials"))?;
    let (app_key, app_secret) = parse_ably_key(&credential);
    let app = find_enabled_app_by_key(handler, app_key).await?;
    if let Some(app_secret) = app_secret
        && app_secret != app.secret
    {
        return Err(AblyAuthError::forbidden("Invalid Ably key secret"));
    }
    Ok(ResolvedAblyAuth {
        app,
        client_id: requested_client_id.map(str::to_string),
        capabilities: None,
    })
}

fn resolve_ably_token_client_id(
    token_client_id: Option<String>,
    query_client_id: Option<&str>,
) -> Result<Option<String>, AblyAuthError> {
    match (token_client_id, query_client_id) {
        (Some(token_client_id), Some(query_client_id)) if token_client_id != query_client_id => {
            Err(AblyAuthError::forbidden(
                "Token clientId does not match requested clientId",
            ))
        }
        (Some(token_client_id), _) => Ok(Some(token_client_id)),
        (None, Some(query_client_id)) => Ok(Some(query_client_id.to_string())),
        (None, None) => Ok(None),
    }
}

async fn find_enabled_app_by_key(
    handler: &Arc<ConnectionHandler>,
    app_key: &str,
) -> Result<App, AblyAuthError> {
    match handler.app_manager().find_by_key(app_key).await {
        Ok(Some(app)) if app.enabled => Ok(app),
        Ok(Some(_)) => Err(AblyAuthError::forbidden("Application is disabled")),
        Ok(None) => Err(AblyAuthError::unauthorized("Application was not found")),
        Err(error) => Err(AblyAuthError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: 50000,
            message: error.to_string(),
        }),
    }
}

fn ably_rest_request_format(headers: &HeaderMap) -> AblyFormat {
    if header_contains(headers, header::CONTENT_TYPE, "msgpack") {
        AblyFormat::MsgPack
    } else {
        AblyFormat::Json
    }
}

fn ably_rest_response_format(headers: &HeaderMap, fallback: AblyFormat) -> AblyFormat {
    if header_contains(headers, header::ACCEPT, "msgpack") {
        AblyFormat::MsgPack
    } else if header_contains(headers, header::ACCEPT, "json") {
        AblyFormat::Json
    } else {
        fallback
    }
}

fn header_contains(headers: &HeaderMap, name: header::HeaderName, needle: &str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains(needle))
}

fn parse_ably_format(raw: Option<&str>) -> Result<AblyFormat, String> {
    match raw.unwrap_or("json").trim().to_ascii_lowercase().as_str() {
        "" | "json" => Ok(AblyFormat::Json),
        "msgpack" | "messagepack" => Ok(AblyFormat::MsgPack),
        other => Err(format!("Unsupported Ably protocol format '{other}'")),
    }
}

fn decode_ably_protocol_message(
    body: &[u8],
    format: AblyFormat,
) -> Result<AblyProtocolMessage, String> {
    match format {
        AblyFormat::Json => {
            let value = serde_json::from_slice::<serde_json::Value>(body)
                .map_err(|error| error.to_string())?;
            ably_protocol_message_from_json_value(value).map_err(|error| error.to_string())
        }
        AblyFormat::MsgPack => {
            let value = decode_msgpack_json_value(body)?;
            ably_protocol_message_from_json_value(value).map_err(|error| error.to_string())
        }
    }
}

fn decode_msgpack_json_value(body: &[u8]) -> Result<serde_json::Value, String> {
    let mut deserializer = rmp_serde::Deserializer::new(std::io::Cursor::new(body));
    serde::de::Deserializer::deserialize_any(&mut deserializer, MsgpackJsonVisitor)
        .map_err(|error| error.to_string())
}

struct MsgpackJsonVisitor;

impl<'de> Visitor<'de> for MsgpackJsonVisitor {
    type Value = serde_json::Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a MessagePack value representable as JSON")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(serde_json::Value::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        serde_json::Number::from_u128(value as u128)
            .map(serde_json::Value::Number)
            .ok_or_else(|| E::custom("MessagePack integer exceeds JSON range"))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| E::custom("non-finite MessagePack number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(serde_json::Value::String(value.to_string()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(serde_json::Value::String(value))
    }

    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E> {
        Ok(serde_json::json!({
            "__sockudo_msgpack_binary": base64::engine::general_purpose::STANDARD.encode(value)
        }))
    }

    fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_bytes(&value)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = access.next_element_seed(MsgpackJsonSeed)? {
            values.push(value);
        }
        Ok(serde_json::Value::Array(values))
    }

    fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut values = serde_json::Map::new();
        while let Some(key) = access.next_key::<String>()? {
            let value = access.next_value_seed(MsgpackJsonSeed)?;
            values.insert(key, value);
        }
        Ok(serde_json::Value::Object(values))
    }
}

struct MsgpackJsonSeed;

impl<'de> serde::de::DeserializeSeed<'de> for MsgpackJsonSeed {
    type Value = serde_json::Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(MsgpackJsonVisitor)
    }
}

fn ably_protocol_message_from_json_value(
    value: serde_json::Value,
) -> Result<AblyProtocolMessage, AppError> {
    let serde_json::Value::Object(object) = value else {
        return Err(AppError::InvalidInput(
            "Ably ProtocolMessage must be an object".to_string(),
        ));
    };
    let action = json_u8_field(&object, "action").ok_or_else(|| {
        AppError::InvalidInput("Ably ProtocolMessage.action is required".to_string())
    })?;

    Ok(AblyProtocolMessage {
        action,
        id: json_string_field(&object, "id"),
        flags: json_u64_field(&object, "flags"),
        timestamp: json_i64_field(&object, "timestamp"),
        count: json_u64_field(&object, "count"),
        error: None,
        connection_id: json_string_field(&object, "connectionId")
            .or_else(|| json_string_field(&object, "connectionKey")),
        channel: json_string_field(&object, "channel"),
        channel_serial: json_string_field(&object, "channelSerial"),
        msg_serial: json_u64_field(&object, "msgSerial"),
        messages: object
            .get("messages")
            .map(ably_messages_from_json_value)
            .transpose()?,
        presence: object
            .get("presence")
            .map(ably_presence_from_json_value)
            .transpose()?,
        auth: object
            .get("auth")
            .cloned()
            .map(json_value_to_sonic_value)
            .transpose()?,
        connection_details: None,
        params: object
            .get("params")
            .map(json_string_map_from_json_value)
            .transpose()?,
        res: object
            .get("res")
            .cloned()
            .map(json_value_to_sonic_value)
            .transpose()?,
    })
}

fn ably_messages_from_json_value(value: &serde_json::Value) -> Result<Vec<AblyMessage>, AppError> {
    match value {
        serde_json::Value::Null => Ok(Vec::new()),
        serde_json::Value::Array(items) => items
            .iter()
            .cloned()
            .map(ably_message_from_json_value)
            .collect::<Result<Vec<_>, _>>(),
        _ => Err(AppError::InvalidInput(
            "Ably ProtocolMessage.messages must be an array".to_string(),
        )),
    }
}

fn ably_presence_from_json_value(
    value: &serde_json::Value,
) -> Result<Vec<AblyPresenceMessage>, AppError> {
    match value {
        serde_json::Value::Null => Ok(Vec::new()),
        serde_json::Value::Array(items) => items
            .iter()
            .map(ably_presence_message_from_json_value)
            .collect::<Result<Vec<_>, _>>(),
        _ => Err(AppError::InvalidInput(
            "Ably ProtocolMessage.presence must be an array".to_string(),
        )),
    }
}

fn ably_presence_message_from_json_value(
    value: &serde_json::Value,
) -> Result<AblyPresenceMessage, AppError> {
    let serde_json::Value::Object(object) = value else {
        return Err(AppError::InvalidInput(
            "Ably presence items must be objects".to_string(),
        ));
    };
    Ok(AblyPresenceMessage {
        id: json_string_field(object, "id"),
        action: json_u8_field(object, "action"),
        client_id: json_string_field(object, "clientId"),
        connection_id: json_string_field(object, "connectionId"),
        data: object
            .get("data")
            .cloned()
            .map(json_value_to_sonic_value)
            .transpose()?,
        encoding: json_string_field(object, "encoding"),
        timestamp: json_i64_field(object, "timestamp"),
    })
}

fn json_string_map_from_json_value(
    value: &serde_json::Value,
) -> Result<HashMap<String, String>, AppError> {
    let serde_json::Value::Object(object) = value else {
        return Err(AppError::InvalidInput(
            "Ably params must be an object".to_string(),
        ));
    };
    let mut params = HashMap::with_capacity(object.len());
    for (key, value) in object {
        let value = value
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| value.to_string());
        params.insert(key.clone(), value);
    }
    Ok(params)
}

fn decode_ably_publish_payload(
    body: &[u8],
    format: AblyFormat,
) -> Result<Vec<AblyMessage>, AppError> {
    if body.is_empty() {
        return Err(AppError::InvalidInput(
            "Ably REST publish body is required".to_string(),
        ));
    }

    let value = match format {
        AblyFormat::Json => serde_json::from_slice::<serde_json::Value>(body)
            .map_err(|error| AppError::InvalidInput(format!("Invalid Ably JSON body: {error}")))?,
        AblyFormat::MsgPack => decode_msgpack_json_value(body).map_err(|error| {
            AppError::InvalidInput(format!("Invalid Ably MsgPack body: {error}"))
        })?,
    };
    ably_publish_value_to_messages(value)
}

fn ably_publish_value_to_messages(value: serde_json::Value) -> Result<Vec<AblyMessage>, AppError> {
    match value {
        serde_json::Value::Array(items) => items
            .into_iter()
            .map(ably_message_from_json_value)
            .collect::<Result<Vec<_>, _>>(),
        serde_json::Value::Object(_) => Ok(vec![ably_message_from_json_value(value)?]),
        _ => Err(AppError::InvalidInput(
            "Ably REST publish body must be a message object or array".to_string(),
        )),
    }
}

fn ably_message_from_json_value(value: serde_json::Value) -> Result<AblyMessage, AppError> {
    let serde_json::Value::Object(object) = value else {
        return Err(AppError::InvalidInput(
            "Ably REST publish items must be message objects".to_string(),
        ));
    };

    Ok(AblyMessage {
        id: json_string_field(&object, "id"),
        name: json_string_field(&object, "name"),
        data: object
            .get("data")
            .cloned()
            .map(|value| json_data_to_sonic_value(value).map(|(data, _)| data))
            .transpose()?,
        encoding: json_string_field(&object, "encoding").or_else(|| {
            object
                .get("data")
                .and_then(msgpack_binary_value)
                .map(|_| "base64".to_string())
        }),
        client_id: json_string_field(&object, "clientId"),
        connection_id: json_string_field(&object, "connectionId")
            .or_else(|| json_string_field(&object, "connectionKey")),
        timestamp: json_i64_field(&object, "timestamp"),
        extras: object
            .get("extras")
            .cloned()
            .map(json_value_to_sonic_value)
            .transpose()?,
        serial: json_string_field(&object, "serial"),
        action: json_action_field(&object),
        version: object
            .get("version")
            .or_else(|| object.get("operation"))
            .map(|version| {
                ably_message_version_from_json_value(version, json_string_field(&object, "serial"))
            })
            .transpose()?
            .flatten(),
    })
}

fn msgpack_binary_value(value: &serde_json::Value) -> Option<&str> {
    value.as_object()?.get("__sockudo_msgpack_binary")?.as_str()
}

fn json_data_to_sonic_value(value: serde_json::Value) -> Result<(Value, bool), AppError> {
    if let Some(encoded) = msgpack_binary_value(&value) {
        return Ok((
            json_value_to_sonic_value(serde_json::Value::String(encoded.to_string()))?,
            true,
        ));
    }
    Ok((json_value_to_sonic_value(value)?, false))
}

fn ably_message_version_from_json_value(
    value: &serde_json::Value,
    message_serial: Option<String>,
) -> Result<Option<AblyMessageVersion>, AppError> {
    if value.is_null() {
        return Ok(None);
    }
    let serde_json::Value::Object(object) = value else {
        return Err(AppError::InvalidInput(
            "Ably message.version must be an object".to_string(),
        ));
    };
    let Some(serial) = json_string_field(object, "serial").or(message_serial) else {
        return Ok(None);
    };
    Ok(Some(AblyMessageVersion {
        serial,
        timestamp: json_i64_field(object, "timestamp"),
        client_id: json_string_field(object, "clientId"),
        description: json_string_field(object, "description"),
        metadata: object
            .get("metadata")
            .cloned()
            .map(json_value_to_sonic_value)
            .transpose()?,
    }))
}

fn json_string_field(
    object: &serde_json::Map<String, serde_json::Value>,
    name: &str,
) -> Option<String> {
    object
        .get(name)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn json_u64_field(object: &serde_json::Map<String, serde_json::Value>, name: &str) -> Option<u64> {
    object.get(name).and_then(serde_json::Value::as_u64)
}

fn json_u8_field(object: &serde_json::Map<String, serde_json::Value>, name: &str) -> Option<u8> {
    json_u64_field(object, name).and_then(|raw| u8::try_from(raw).ok())
}

fn json_i64_field(object: &serde_json::Map<String, serde_json::Value>, name: &str) -> Option<i64> {
    object.get(name).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|raw| i64::try_from(raw).ok()))
    })
}

fn json_action_field(object: &serde_json::Map<String, serde_json::Value>) -> Option<u8> {
    let value = object.get("action")?;
    if let Some(raw) = value.as_u64() {
        return u8::try_from(raw).ok();
    }
    match value.as_str()? {
        "message.create" => Some(MESSAGE_CREATE),
        "message.update" => Some(MESSAGE_UPDATE),
        "message.delete" => Some(MESSAGE_DELETE),
        "message.summary" => Some(MESSAGE_SUMMARY),
        "message.append" => Some(MESSAGE_APPEND),
        _ => None,
    }
}

fn json_value_to_sonic_value(value: serde_json::Value) -> Result<Value, AppError> {
    let body =
        serde_json::to_vec(&value).map_err(|error| AppError::InvalidInput(error.to_string()))?;
    sonic_rs::from_slice(&body).map_err(|error| AppError::InvalidInput(error.to_string()))
}

fn encode_ably_rest_response<T: Serialize>(
    status: StatusCode,
    format: AblyFormat,
    value: &T,
) -> Result<Response, AppError> {
    match format {
        AblyFormat::Json => {
            let body = sonic_rs::to_vec(value)
                .map_err(|error| AppError::InternalError(error.to_string()))?;
            Ok((status, [(header::CONTENT_TYPE, "application/json")], body).into_response())
        }
        AblyFormat::MsgPack => {
            let body = rmp_serde::to_vec_named(value)
                .map_err(|error| AppError::InternalError(error.to_string()))?;
            Ok((
                status,
                [(header::CONTENT_TYPE, "application/x-msgpack")],
                body,
            )
                .into_response())
        }
    }
}

fn effective_ably_client_id(
    authenticated_client_id: Option<&str>,
    message: &AblyMessage,
) -> Result<Option<String>, AppError> {
    match (authenticated_client_id, message.client_id.as_deref()) {
        (Some(authenticated), Some(message_client_id)) if authenticated != message_client_id => {
            Err(AppError::InvalidInput(
                "message.clientId must match authenticated clientId".to_string(),
            ))
        }
        (Some(authenticated), _) => Ok(Some(authenticated.to_string())),
        (None, Some(message_client_id)) => Ok(Some(message_client_id.to_string())),
        (None, None) => Ok(None),
    }
}

fn pusher_to_ably_message(
    message: &PusherMessage,
    projection: AblyMessageProjection,
) -> Result<AblyMessage, String> {
    let action = extract_runtime_action(message);
    let exposed_action = match (projection, action) {
        (AblyMessageProjection::Aggregate, Some(ProtocolMessageAction::Append)) => {
            Some(ProtocolMessageAction::Update)
        }
        _ => action,
    };
    let serial = extract_runtime_message_serial(message)
        .map(str::to_string)
        .or_else(|| message.serial.map(|serial| serial.to_string()));
    let data = match (projection, action) {
        (AblyMessageProjection::Mutation, Some(ProtocolMessageAction::Append)) => {
            match extract_runtime_append_fragment(message) {
                Some(fragment) => Some(json!(fragment)),
                None => message
                    .data
                    .as_ref()
                    .map(message_data_to_ably_value)
                    .transpose()?,
            }
        }
        _ => message
            .data
            .as_ref()
            .map(message_data_to_ably_value)
            .transpose()?,
    };
    Ok(AblyMessage {
        id: message.message_id.clone(),
        name: message.name.clone().or_else(|| message.event.clone()),
        data,
        encoding: None,
        client_id: message.user_id.clone().or_else(|| ai_client_id(message)),
        connection_id: None,
        timestamp: None,
        extras: message
            .extras
            .as_ref()
            .and_then(ably_extras_from_message_extras),
        serial,
        action: exposed_action.map(protocol_action_to_ably),
        version: message_version_from_runtime_headers(message),
    })
}

/// Deterministic compatibility projection of commit-time facts.  It does not
/// consult the clock, generate identifiers, or recover operation metadata from
/// runtime headers.
fn envelope_to_ably_message(
    envelope: &MessageEnvelope,
    fallback: &PusherMessage,
    projection: AblyMessageProjection,
) -> Result<AblyMessage, String> {
    let action = envelope
        .action
        .map(core_action_to_protocol)
        .or_else(|| extract_runtime_action(fallback));
    let data = match (
        projection,
        envelope.version.as_ref().map(|version| version.projection),
    ) {
        (AblyMessageProjection::Mutation, Some(VersionProjection::AppendFragment)) => {
            extract_runtime_append_fragment(fallback)
                .map(|fragment| json!(fragment))
                .or_else(|| {
                    envelope
                        .data
                        .as_ref()
                        .map(message_content_to_ably_value)
                        .transpose()
                        .ok()
                        .flatten()
                })
        }
        _ => envelope_data_to_ably_value(envelope, projection)?,
    };
    let exposed_action = match (projection, action) {
        (AblyMessageProjection::Aggregate, Some(ProtocolMessageAction::Append)) => {
            Some(ProtocolMessageAction::Update)
        }
        _ => action,
    };
    Ok(AblyMessage {
        id: envelope.message_id.clone(),
        name: envelope.name.clone(),
        data,
        encoding: envelope
            .encoding
            .as_ref()
            .and_then(|encoding| ably_projected_encoding(encoding.as_str(), projection)),
        client_id: envelope.publisher_client_id.clone(),
        connection_id: envelope.publisher_connection_id.clone(),
        timestamp: envelope.published_at_ms,
        extras: envelope
            .extras
            .as_ref()
            .and_then(ably_extras_from_message_extras),
        serial: envelope
            .message_serial
            .as_ref()
            .map(|serial| serial.as_str().to_string())
            .or_else(|| fallback.serial.map(|serial| serial.to_string())),
        action: exposed_action.map(protocol_action_to_ably),
        version: envelope.version.as_ref().map(|version| AblyMessageVersion {
            serial: version.serial.as_str().to_string(),
            timestamp: Some(version.timestamp_ms),
            client_id: version.client_id.clone(),
            description: version.description.clone(),
            metadata: version.metadata.clone(),
        }),
    })
}

fn message_content_to_ably_value(content: &MessageContent) -> Result<Value, String> {
    match content {
        MessageContent::Text(value) => Ok(json!(value)),
        MessageContent::Structured(value) => Ok(value.clone()),
        MessageContent::Binary(value) => {
            sonic_rs::to_value(value).map_err(|error| error.to_string())
        }
    }
}

fn envelope_data_to_ably_value(
    envelope: &MessageEnvelope,
    projection: AblyMessageProjection,
) -> Result<Option<Value>, String> {
    let Some(content) = envelope.data.as_ref() else {
        return Ok(None);
    };
    if let MessageContent::Text(raw) = content
        && let Some(encoding) = envelope.encoding.as_ref()
        && projection == AblyMessageProjection::Mutation
        && encoding
            .as_str()
            .split('/')
            .any(|part| part.eq_ignore_ascii_case("json"))
        && let Ok(value) = sonic_rs::from_str::<Value>(raw)
    {
        return Ok(Some(value));
    }
    message_content_to_ably_value(content).map(Some)
}

fn ably_projected_encoding(encoding: &str, projection: AblyMessageProjection) -> Option<String> {
    if projection == AblyMessageProjection::Aggregate {
        return Some(encoding.to_string());
    }
    match encoding {
        "json" | "utf-8/base64" => None,
        other => Some(other.to_string()),
    }
}

fn core_action_to_protocol(
    action: sockudo_core::versioned_messages::MessageAction,
) -> ProtocolMessageAction {
    match action {
        sockudo_core::versioned_messages::MessageAction::Create => ProtocolMessageAction::Create,
        sockudo_core::versioned_messages::MessageAction::Update => ProtocolMessageAction::Update,
        sockudo_core::versioned_messages::MessageAction::Delete => ProtocolMessageAction::Delete,
        sockudo_core::versioned_messages::MessageAction::Append => ProtocolMessageAction::Append,
        sockudo_core::versioned_messages::MessageAction::Summary => ProtocolMessageAction::Summary,
    }
}

fn protocol_action_to_ably(action: ProtocolMessageAction) -> u8 {
    match action {
        ProtocolMessageAction::Create => MESSAGE_CREATE,
        ProtocolMessageAction::Update => MESSAGE_UPDATE,
        ProtocolMessageAction::Delete => MESSAGE_DELETE,
        ProtocolMessageAction::Append => MESSAGE_APPEND,
        ProtocolMessageAction::Summary => MESSAGE_SUMMARY,
    }
}

fn message_version_from_runtime_headers(message: &PusherMessage) -> Option<AblyMessageVersion> {
    let headers = message
        .extras
        .as_ref()
        .and_then(|extras| extras.headers.as_ref())?;
    let serial = match headers.get(HEADER_VERSION_SERIAL)? {
        sockudo_protocol::messages::ExtrasValue::String(value) => value.clone(),
        _ => return None,
    };
    let timestamp = match headers.get(HEADER_VERSION_TIMESTAMP_MS) {
        Some(sockudo_protocol::messages::ExtrasValue::Number(value)) => Some(*value as i64),
        _ => None,
    };
    Some(AblyMessageVersion {
        serial,
        timestamp,
        client_id: None,
        description: None,
        metadata: None,
    })
}

fn ably_extras_from_message_extras(extras: &MessageExtras) -> Option<Value> {
    let mut visible = extras.clone();
    visible.ephemeral = None;
    visible.idempotency_key = None;
    visible.echo = None;
    if visible.headers.is_none()
        && visible.push.is_none()
        && visible.ai.is_none()
        && visible.opaque.is_empty()
    {
        return None;
    }
    sonic_rs::to_value(&visible).ok()
}

fn ably_extras_to_message_extras(extras: Option<Value>) -> Result<Option<MessageExtras>, AppError> {
    let Some(extras) = extras else {
        return Ok(None);
    };
    let encoded = sonic_rs::to_string(&extras)
        .map_err(|error| AppError::InvalidInput(format!("Invalid extras: {error}")))?;
    let decoded: MessageExtras = sonic_rs::from_str(&encoded)
        .map_err(|error| AppError::InvalidInput(format!("Invalid extras: {error}")))?;
    decoded.validate_opaque().map_err(AppError::InvalidInput)?;
    Ok(Some(decoded))
}

fn message_data_to_ably_value(data: &MessageData) -> Result<Value, String> {
    sonic_rs::to_value(data).map_err(|error| error.to_string())
}

fn ably_value_to_message_data(value: Value) -> MessageData {
    value
        .as_str()
        .map(|value| MessageData::String(value.to_string()))
        .unwrap_or(MessageData::Json(value))
}

fn ably_message_data_to_message_data(value: Value, encoding: Option<&str>) -> MessageData {
    if encoding
        .unwrap_or_default()
        .split('/')
        .any(|part| part.eq_ignore_ascii_case("json"))
        && let Some(raw) = value.as_str()
        && let Ok(decoded) = sonic_rs::from_str::<Value>(raw)
    {
        return MessageData::Json(decoded);
    }
    ably_value_to_message_data(value)
}

fn stamp_ai_identity(
    extras: &mut Option<MessageExtras>,
    event_name: &str,
    client_id: &str,
) -> Result<(), AppError> {
    let key = match event_name {
        AI_EVENT_INPUT => AI_HEADER_INPUT_CLIENT_ID,
        AI_EVENT_CANCEL => AI_HEADER_RUN_CLIENT_ID,
        _ => return Ok(()),
    };
    let extras = extras.get_or_insert_with(Default::default);
    let ai = extras.ai.get_or_insert_with(Default::default);
    let transport = ai.transport.get_or_insert_with(Default::default);
    match transport.get(key) {
        Some(existing) if existing != client_id => Err(AppError::InvalidInput(format!(
            "extras.ai.transport.{key} must match authenticated clientId"
        ))),
        Some(_) => Ok(()),
        None => {
            transport.insert(key.to_string(), client_id.to_string());
            Ok(())
        }
    }
}

fn ai_client_id(message: &PusherMessage) -> Option<String> {
    let transport = message
        .extras
        .as_ref()
        .and_then(MessageExtras::ai_transport_headers)?;
    transport
        .input_client_id()
        .or_else(|| transport.run_client_identity())
        .or_else(|| transport.step_client_id())
        .map(str::to_string)
}

fn send_protocol_error(sender: &AblySender, code: u32, message: impl Into<String>) {
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_ERROR,
            error: Some(error_info(StatusCode::BAD_REQUEST, code, message)),
            ..empty_protocol_message(ACTION_ERROR)
        },
    );
}

fn send_publish_nack(
    sender: &AblySender,
    inbound: &AblyProtocolMessage,
    code: u32,
    message: impl Into<String>,
) {
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_NACK,
            msg_serial: inbound.msg_serial,
            count: inbound.count.or(Some(1)),
            error: Some(error_info(StatusCode::BAD_REQUEST, code, message)),
            ..empty_protocol_message(ACTION_NACK)
        },
    );
}

fn error_info(status: StatusCode, code: u32, message: impl Into<String>) -> AblyErrorInfo {
    AblyErrorInfo {
        message: message.into(),
        code,
        status_code: status.as_u16(),
    }
}

fn ably_error_response(status: StatusCode, code: u32, message: impl Into<String>) -> Response {
    let message = message.into();
    (
        status,
        Json(AblyErrorBody {
            error: error_info(status, code, message),
        }),
    )
        .into_response()
}

fn ably_app_error_response(error: AppError) -> Response {
    match error {
        AppError::NotFound(message) => ably_error_response(StatusCode::NOT_FOUND, 40400, message),
        AppError::InvalidInput(message) => {
            ably_error_response(StatusCode::BAD_REQUEST, 40000, message)
        }
        AppError::ApiAuthFailed(message) => {
            ably_error_response(StatusCode::UNAUTHORIZED, 40140, message)
        }
        AppError::Forbidden(message) => ably_error_response(StatusCode::FORBIDDEN, 40160, message),
        AppError::FeatureDisabled(message) => {
            ably_error_response(StatusCode::BAD_REQUEST, 40000, message)
        }
        other => ably_error_response(StatusCode::INTERNAL_SERVER_ERROR, 50000, other.to_string()),
    }
}

fn parse_ably_history_direction(raw: Option<&str>) -> Result<HistoryDirection, AppError> {
    match raw.unwrap_or("backwards").to_ascii_lowercase().as_str() {
        "backwards" | "newest_first" | "reverse" => Ok(HistoryDirection::NewestFirst),
        "forwards" | "oldest_first" | "forward" => Ok(HistoryDirection::OldestFirst),
        other => Err(AppError::InvalidInput(format!(
            "Invalid Ably history direction '{other}'"
        ))),
    }
}

fn channel_key(app_id: &str, channel: &str) -> AblyChannelKey {
    AblyChannelKey {
        app_id: app_id.to_string(),
        channel: channel.to_string(),
    }
}

fn send_protocol(sender: &AblySender, message: AblyProtocolMessage) {
    if let Err(error) = sender.send_protocol(&message, OutboundPriority::Control) {
        debug!(error = %error, "Ably compatibility outbound queue is unavailable");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose;
    use sockudo_protocol::messages::{AiExtras, MessageExtras};
    use sockudo_protocol::versioned_messages::apply_runtime_metadata;

    #[test]
    fn ably_key_parses_key_and_secret() {
        assert_eq!(
            parse_ably_key("app-key:secret"),
            ("app-key", Some("secret"))
        );
        assert_eq!(parse_ably_key("app-key"), ("app-key", None));
    }

    #[test]
    fn bearer_token_accepts_raw_and_ably_base64_values() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Bearer sockudo-ably-raw".parse().unwrap(),
        );
        assert_eq!(bearer_token(&headers).as_deref(), Some("sockudo-ably-raw"));

        let encoded = general_purpose::STANDARD.encode("sockudo-ably-encoded");
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {encoded}").parse().unwrap(),
        );
        assert_eq!(
            bearer_token(&headers).as_deref(),
            Some("sockudo-ably-encoded")
        );
    }

    #[test]
    fn pusher_to_ably_keeps_ai_extras_and_hides_runtime_headers() {
        let message = PusherMessage {
            event: Some("ai-input".to_string()),
            channel: Some("chat".to_string()),
            data: Some(MessageData::String("hello".to_string())),
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: None,
            serial: Some(42),
            idempotency_key: None,
            extras: Some(MessageExtras {
                ai: Some(AiExtras {
                    transport: Some(HashMap::from([(
                        "input-client-id".to_string(),
                        "client-1".to_string(),
                    )])),
                    codec: None,
                }),
                ..Default::default()
            }),
            delta_sequence: None,
            delta_conflation_key: None,
        };

        let converted = pusher_to_ably_message(&message, AblyMessageProjection::Mutation).unwrap();
        assert_eq!(converted.name.as_deref(), Some("ai-input"));
        assert_eq!(converted.serial.as_deref(), Some("42"));
        assert_eq!(converted.client_id.as_deref(), Some("client-1"));
        assert!(converted.extras.unwrap().get("ai").is_some());
    }

    #[test]
    fn stamp_ai_identity_rejects_client_id_spoofing() {
        let mut extras = Some(MessageExtras {
            ai: Some(AiExtras {
                transport: Some(HashMap::from([(
                    AI_HEADER_INPUT_CLIENT_ID.to_string(),
                    "other-client".to_string(),
                )])),
                codec: None,
            }),
            ..Default::default()
        });

        let error = stamp_ai_identity(&mut extras, AI_EVENT_INPUT, "client-1").unwrap_err();
        assert!(error.to_string().contains("authenticated clientId"));
    }

    #[test]
    fn rest_publish_payload_decodes_json_and_msgpack_arrays() {
        let messages = vec![AblyMessage {
            name: Some("chat".to_string()),
            data: Some(json!({ "ok": true })),
            encoding: Some("json".to_string()),
            client_id: Some("client-1".to_string()),
            ..Default::default()
        }];

        let json_body = sonic_rs::to_vec(&messages).unwrap();
        let decoded_json = decode_ably_publish_payload(&json_body, AblyFormat::Json).unwrap();
        assert_eq!(decoded_json[0].name.as_deref(), Some("chat"));
        assert_eq!(decoded_json[0].client_id.as_deref(), Some("client-1"));

        let msgpack_value = serde_json::json!([
            {
                "name": "chat",
                "data": { "ok": true },
                "encoding": "json",
                "clientId": "client-1"
            }
        ]);
        let msgpack_body = rmp_serde::to_vec(&msgpack_value).unwrap();
        let decoded_msgpack =
            decode_ably_publish_payload(&msgpack_body, AblyFormat::MsgPack).unwrap();
        assert_eq!(decoded_msgpack[0].name.as_deref(), Some("chat"));
        assert_eq!(decoded_msgpack[0].client_id.as_deref(), Some("client-1"));
    }

    #[test]
    fn rest_publish_payload_decodes_single_json_object() {
        let message = AblyMessage {
            name: Some("chat".to_string()),
            data: Some(json!("hello")),
            ..Default::default()
        };
        let body = sonic_rs::to_vec(&message).unwrap();
        let decoded = decode_ably_publish_payload(&body, AblyFormat::Json).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].name.as_deref(), Some("chat"));
    }

    #[test]
    fn rest_msgpack_response_uses_named_message_fields() {
        let messages = vec![AblyMessage {
            name: Some("chat".to_string()),
            data: Some(json!("hello")),
            ..Default::default()
        }];
        let body = rmp_serde::to_vec_named(&messages).unwrap();
        let decoded: serde_json::Value = rmp_serde::from_slice(&body).unwrap();
        assert_eq!(decoded[0]["name"], "chat");
        assert_eq!(decoded[0]["data"], "hello");
    }

    #[test]
    fn realtime_msgpack_protocol_message_round_trips_named_fields() {
        let wire = serde_json::json!({
            "action": ACTION_MESSAGE,
            "channel": "chat",
            "msgSerial": 7,
            "messages": [
                {
                    "name": "chat-message",
                    "data": { "ok": true },
                    "encoding": "json"
                }
            ]
        });
        let body = rmp_serde::to_vec(&wire).unwrap();
        let decoded = decode_ably_protocol_message(&body, AblyFormat::MsgPack).unwrap();
        assert_eq!(decoded.action, ACTION_MESSAGE);
        assert_eq!(decoded.channel.as_deref(), Some("chat"));
        assert_eq!(decoded.msg_serial, Some(7));
        assert_eq!(
            decoded
                .messages
                .as_ref()
                .and_then(|messages| messages.first())
                .and_then(|message| message.name.as_deref()),
            Some("chat-message")
        );

        let encoded_body = encode_protocol_bytes(&decoded, AblyFormat::MsgPack).unwrap();
        let encoded_value: serde_json::Value =
            rmp_serde::from_slice(encoded_body.as_ref()).unwrap();
        assert_eq!(encoded_value["action"], ACTION_MESSAGE);
        assert_eq!(encoded_value["channel"], "chat");
        assert_eq!(encoded_value["msgSerial"], 7);
        assert_eq!(encoded_value["messages"][0]["name"], "chat-message");
    }

    #[test]
    fn ably_protocol_format_defaults_to_json_and_accepts_msgpack() {
        assert_eq!(parse_ably_format(None).unwrap(), AblyFormat::Json);
        assert_eq!(parse_ably_format(Some("json")).unwrap(), AblyFormat::Json);
        assert_eq!(
            parse_ably_format(Some("msgpack")).unwrap(),
            AblyFormat::MsgPack
        );
        assert!(parse_ably_format(Some("xml")).is_err());
    }

    #[test]
    fn rest_publish_rejects_message_client_id_spoofing() {
        let message = AblyMessage {
            client_id: Some("other-client".to_string()),
            ..Default::default()
        };
        let error = effective_ably_client_id(Some("client-1"), &message).unwrap_err();
        assert!(error.to_string().contains("authenticated clientId"));
    }

    #[test]
    fn ably_token_capability_maps_to_sockudo_capabilities() {
        let (_, capabilities) = normalise_ably_token_capability(Some(serde_json::json!({
            "chat:*": ["publish", "subscribe", "history"],
            "presence-chat:*": ["presence"],
            "mutable:*": ["message-update-any", "message-delete-own"],
            "object:*": ["object-subscribe"]
        })))
        .unwrap();
        let capabilities = capabilities.unwrap();

        assert!(capabilities.allows_publish("chat:one"));
        assert!(capabilities.allows_subscribe("chat:one"));
        assert!(capabilities.allows_history("chat:one"));
        assert!(!capabilities.allows_publish("other:one"));
        assert!(capabilities.presence.as_deref().is_some_and(|patterns| {
            ConnectionCapabilities::matches_any(patterns, "presence-chat:one")
        }));
        assert!(capabilities.allows_message_mutation_any(
            sockudo_core::versioned_message_auth::MutationKind::Update,
            "mutable:one"
        ));
        assert!(capabilities.allows_message_mutation_own(
            sockudo_core::versioned_message_auth::MutationKind::Delete,
            "mutable:one"
        ));
        assert!(
            ensure_ably_capability(
                Some(&capabilities),
                "other:one",
                AblyCapabilityCheck::Publish
            )
            .is_err()
        );
    }

    #[test]
    fn ably_token_capability_accepts_json_string_and_wildcard() {
        let (capability, capabilities) = normalise_ably_token_capability(Some(
            serde_json::Value::String(r#"{"*":["*"]}"#.to_string()),
        ))
        .unwrap();
        let capabilities = capabilities.unwrap();

        assert_eq!(capability.as_deref(), Some(r#"{"*":["*"]}"#));
        assert!(capabilities.allows_publish("any-channel"));
        assert!(capabilities.allows_subscribe("any-channel"));
        assert!(capabilities.allows_history("any-channel"));
        assert!(
            ensure_ably_capability(
                Some(&capabilities),
                "any-channel",
                AblyCapabilityCheck::AnyChannelAccess
            )
            .is_ok()
        );
    }

    #[test]
    fn ably_token_client_id_cannot_be_overridden() {
        assert_eq!(
            resolve_ably_token_client_id(Some("client-1".to_string()), Some("client-1")).unwrap(),
            Some("client-1".to_string())
        );
        assert!(
            resolve_ably_token_client_id(Some("client-1".to_string()), Some("other-client"))
                .is_err()
        );
        assert_eq!(
            resolve_ably_token_client_id(None, Some("client-1")).unwrap(),
            Some("client-1".to_string())
        );
    }

    #[test]
    fn append_projection_uses_delta_for_mutations_and_aggregate_for_history() {
        let mut message = PusherMessage {
            event: Some("sockudo:message.append".to_string()),
            channel: Some("chat".to_string()),
            data: Some(MessageData::String("hello world".to_string())),
            name: Some("ai-output".to_string()),
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: Some("stream-1".to_string()),
            serial: Some(2),
            idempotency_key: None,
            extras: Some(MessageExtras {
                ai: Some(AiExtras {
                    transport: None,
                    codec: Some(HashMap::from([(
                        "status".to_string(),
                        "complete".to_string(),
                    )])),
                }),
                ..Default::default()
            }),
            delta_sequence: None,
            delta_conflation_key: None,
        };
        apply_runtime_metadata(
            &mut message,
            ProtocolMessageAction::Append,
            "msg:1",
            &sockudo_protocol::versioned_messages::MessageVersionMetadata {
                serial: "ver:2".to_string(),
                client_id: Some("client-1".to_string()),
                timestamp_ms: 2,
                description: None,
                metadata: None,
            },
            Some(10),
        );
        sockudo_protocol::versioned_messages::set_runtime_append_fragment(&mut message, " world");

        let mutation = pusher_to_ably_message(&message, AblyMessageProjection::Mutation).unwrap();
        assert_eq!(mutation.action, Some(MESSAGE_APPEND));
        assert_eq!(
            mutation.data.as_ref().and_then(Value::as_str),
            Some(" world")
        );
        assert_eq!(mutation.serial.as_deref(), Some("msg:1"));
        assert_eq!(
            mutation
                .version
                .as_ref()
                .map(|version| version.serial.as_str()),
            Some("ver:2")
        );

        let aggregate = pusher_to_ably_message(&message, AblyMessageProjection::Aggregate).unwrap();
        assert_eq!(aggregate.action, Some(MESSAGE_UPDATE));
        assert_eq!(
            aggregate.data.as_ref().and_then(Value::as_str),
            Some("hello world")
        );
        assert_eq!(aggregate.serial.as_deref(), Some("msg:1"));
    }

    #[test]
    fn ably_channel_serial_round_trips_stream_position() {
        let encoded = encode_ably_channel_serial("stream-1", 42);
        let parsed = parse_ably_channel_serial(&encoded).unwrap();
        assert_eq!(parsed.stream_id, "stream-1");
        assert_eq!(parsed.serial, 42);

        let failure = parse_ably_channel_serial("not-a-position").unwrap_err();
        assert_eq!(failure.code, 90005);
    }

    #[test]
    fn interest_check_does_not_create_compatibility_channel_state() {
        let hub = AblyCompatHub::default();
        assert!(!hub.has_subscribers("app", "chat"));
        assert!(hub.channels.is_empty());
    }

    #[test]
    fn constructed_runtimes_do_not_share_compatibility_state() {
        let first = Arc::new(AblyCompatRuntime::new(AblyCompatDependencies));
        let second = Arc::new(AblyCompatRuntime::new(AblyCompatDependencies));
        let first_state = first.hub.channel_state("app", "channel");
        let second_state = second.hub.channel_state("app", "channel");

        assert!(!Arc::ptr_eq(&first.hub, &second.hub));
        assert!(!Arc::ptr_eq(&first_state, &second_state));
    }

    #[test]
    fn expiry_sweep_removes_expired_sessions_and_tokens() {
        let hub = AblyCompatHub::default();
        hub.sessions.insert(
            "expired-session".to_string(),
            AblySessionRecord {
                app_id: "app".to_string(),
                connection_id: "connection".to_string(),
                client_id: None,
                expires_at_ms: 10,
            },
        );
        hub.tokens.insert(
            "expired-token".to_string(),
            AblyTokenRecord {
                app_key: "app".to_string(),
                client_id: None,
                expires_ms: 10,
                capabilities: None,
            },
        );

        hub.expire(11);

        assert!(hub.sessions.is_empty());
        assert!(hub.tokens.is_empty());
    }

    #[test]
    fn mutation_operation_without_serial_uses_message_serial() {
        let message = ably_message_from_json_value(serde_json::json!({
            "serial": "stream:1",
            "action": "message.update",
            "version": {
                "clientId": "updater",
                "description": "changed",
                "metadata": {"reason": "test"}
            }
        }))
        .unwrap();

        let version = message.version.unwrap();
        assert_eq!(version.serial, "stream:1");
        assert_eq!(version.client_id.as_deref(), Some("updater"));
        assert_eq!(version.description.as_deref(), Some("changed"));
    }

    #[test]
    fn msgpack_binary_payload_is_projected_to_base64_without_rejection() {
        #[derive(Serialize)]
        struct BinaryMessage {
            name: &'static str,
            data: Vec<u8>,
        }
        #[derive(Serialize)]
        struct Protocol {
            action: u8,
            messages: Vec<BinaryMessage>,
        }
        let wire = Protocol {
            action: ACTION_MESSAGE,
            messages: vec![BinaryMessage {
                name: "binary",
                data: vec![0xde, 0xad, 0xbe, 0xef],
            }],
        };
        let mut body = Vec::new();
        let mut serializer = rmp_serde::Serializer::new(&mut body)
            .with_struct_map()
            .with_bytes(rmp_serde::config::BytesMode::ForceAll);
        wire.serialize(&mut serializer).unwrap();
        let decoded = decode_ably_protocol_message(&body, AblyFormat::MsgPack).unwrap();
        let message = decoded.messages.unwrap().pop().unwrap();
        assert_eq!(message.encoding.as_deref(), Some("base64"));
        assert_eq!(message.data.unwrap().as_str(), Some("3q2+7w=="));
    }
}
