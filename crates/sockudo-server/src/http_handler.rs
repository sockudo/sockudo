use ahash::AHashMap;
use axum::{
    Json,
    extract::{Extension, Path, Query, RawQuery, State},
    http::{HeaderMap, HeaderValue, StatusCode, Uri, header},
    response::{IntoResponse, Response as AxumResponse},
};
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use sockudo_adapter::ConnectionHandler;
use sockudo_adapter::channel_manager::ChannelManager;
use sockudo_adapter::handler::annotations::{
    DeleteAnnotationRuntimeRequest, PublishAnnotationRuntimeRequest,
};
use sockudo_adapter::handler::auth_tokens::RevocationRequest as CapabilityRevocationRequest;
use sockudo_core::annotations::{
    Annotation, AnnotationAction, AnnotationEventLookupRequest, AnnotationEventsRequest,
    AnnotationSerial, AnnotationType, RawAnnotationReplayRequest,
};
use sockudo_core::app::App;
use sockudo_core::error::HealthStatus;
use sockudo_core::history::{
    HistoryCursor, HistoryDirection, HistoryPurgeMode, HistoryPurgeRequest, HistoryQueryBounds,
    HistoryReadRequest,
};
use sockudo_core::presence_history::{
    PresenceHistoryCursor, PresenceHistoryDirection, PresenceHistoryQueryBounds,
    PresenceHistoryReadRequest, PresenceHistoryResetResult, PresenceHistoryStreamInspection,
    PresenceHistoryStreamRuntimeState, PresenceSnapshotRequest,
};
use sockudo_core::utils::{self, validate_channel_name};
use sockudo_core::version_store::{
    StoredVersionRecord, VersionStoreDirection, VersionStoreReadRequest,
};
use sockudo_core::versioned_message_auth::{
    MutationAuthorizationRequest, MutationKind, authorize_message_mutation,
};
use sockudo_core::versioned_messages::MessageSerial;
use sockudo_core::versioned_messages::{
    FieldPatch, MessageAction as CoreMessageAction, MessageAppend, MessageFieldDelta,
    VersionMetadata, VersionSerial,
};
use sockudo_core::websocket::SocketId;
use sockudo_protocol::constants::EVENT_NAME_MAX_LENGTH as DEFAULT_EVENT_NAME_MAX_LENGTH;
use sockudo_protocol::messages::{
    AI_ERROR_EVENT_NOT_PERMITTED, AI_ERROR_HEADER_TOO_LARGE, AI_ERROR_INVALID_TRANSPORT_HEADER,
    AI_ERROR_MUTABLE_NOT_PERMITTED, AI_ERROR_PAYLOAD_TOO_LARGE, AI_MESSAGE_ID_MAX_BYTES,
    AiHeaderValidationError, AiPublishTrust, AnnotationEventAction, AnnotationEventData,
    ApiMessageData, BatchPusherApiMessage, InfoQueryParser, MessageData, MessageExtras,
    PusherApiMessage, PusherMessage, is_ai_agent_publish_event, is_ai_event,
};
use sockudo_protocol::versioned_messages::{
    AppendMessageRequest, DeleteMessageRequest, GetMessageResponse, ListMessageVersionsResponse,
    MessageAction as ProtocolMessageAction, MessageVersionMetadata, MessageVersionsQuery,
    MutationResponse, UpdateMessageRequest, VersionDirection, VersionedRealtimeMessage,
    extract_runtime_message_serial,
};
use sonic_rs::{Value, json};
use std::{collections::HashMap, sync::Arc, time::Duration};
use sysinfo::System;
use thiserror::Error;
use tokio::time::timeout;
use tracing::{debug, error, field, info, instrument, warn};

// --- Custom Error Type ---

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Application not found: {0}")]
    AppNotFound(String),
    #[error("Application validation failed: {0}")]
    AppValidationFailed(String),
    #[error("API request authentication failed: {0}")]
    ApiAuthFailed(String),
    #[error("Forbidden: {0}")]
    Forbidden(String),
    #[error("Channel validation failed: Missing 'channels' or 'channel' field")]
    MissingChannelInfo,
    #[error("User connection termination failed: {0}")]
    TerminationFailed(String),
    #[error("Internal Server Error: {0}")]
    InternalError(String),
    #[error("Serialization Error: {0}")]
    SerializationError(#[from] sonic_rs::Error),
    #[error("HTTP Header Build Error: {0}")]
    HeaderBuildError(#[from] axum::http::Error),
    #[error("Limit exceeded: {0}")]
    LimitExceeded(String),
    #[error("Too many requests: {message}")]
    TooManyRequests {
        message: String,
        retry_after_seconds: u64,
    },
    #[error("Service unavailable due to backpressure: {message}")]
    Backpressure {
        message: String,
        retry_after_seconds: u64,
    },
    #[error("Payload too large: {0}")]
    PayloadTooLarge(String),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("{message}")]
    AiTransport {
        status: StatusCode,
        code: u32,
        name: &'static str,
        message: String,
    },
    #[error("Feature disabled: {0}")]
    FeatureDisabled(String),
    #[error("Not implemented: {0}")]
    NotImplemented(String),
    #[error("Not found: {0}")]
    NotFound(String),
}

#[derive(Debug, Deserialize)]
pub struct RevokeCapabilityTokensRequest {
    pub jti: Option<String>,
    pub client_id: Option<String>,
    pub expires_at: Option<i64>,
    pub ttl_seconds: Option<u64>,
    pub reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RevokeCapabilityTokensResponse {
    pub revoked_jti: bool,
    pub revoked_client_id: bool,
    pub closed_connections: usize,
}

impl IntoResponse for AppError {
    fn into_response(self) -> AxumResponse {
        let ai_registry_code = match &self {
            AppError::AiTransport { code, .. } => Some(*code),
            _ => None,
        };
        let (status, code, msg) = match &self {
            AppError::AppNotFound(msg) => (StatusCode::NOT_FOUND, "app_not_found", msg.clone()),
            AppError::AppValidationFailed(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "app_validation_failed",
                msg.clone(),
            ),
            AppError::ApiAuthFailed(msg) => (StatusCode::UNAUTHORIZED, "auth_failed", msg.clone()),
            AppError::Forbidden(msg) => (StatusCode::FORBIDDEN, "forbidden", msg.clone()),
            AppError::MissingChannelInfo => (
                StatusCode::BAD_REQUEST,
                "missing_channel_info",
                "Request must contain 'channels' (list) or 'channel' (string)".to_string(),
            ),
            AppError::TerminationFailed(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "termination_failed",
                msg.clone(),
            ),
            AppError::SerializationError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "serialization_error",
                format!("Internal error during serialization: {e}"),
            ),
            AppError::HeaderBuildError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "header_build_error",
                format!("Internal error building response: {e}"),
            ),
            AppError::InternalError(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                msg.clone(),
            ),
            AppError::LimitExceeded(msg) => {
                (StatusCode::BAD_REQUEST, "limit_exceeded", msg.clone())
            }
            AppError::TooManyRequests { message, .. } => (
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                message.clone(),
            ),
            AppError::Backpressure { message, .. } => (
                StatusCode::SERVICE_UNAVAILABLE,
                "backpressure",
                message.clone(),
            ),
            AppError::PayloadTooLarge(msg) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                msg.clone(),
            ),
            AppError::InvalidInput(msg) => (StatusCode::BAD_REQUEST, "invalid_input", msg.clone()),
            AppError::AiTransport {
                status,
                code,
                name,
                message,
            } => (*status, *name, message.clone()),
            AppError::FeatureDisabled(msg) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "feature_disabled",
                msg.clone(),
            ),
            AppError::NotImplemented(msg) => {
                (StatusCode::NOT_IMPLEMENTED, "not_implemented", msg.clone())
            }
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, "not_found", msg.clone()),
        };
        error!(error.message = %self, status_code = %status, "HTTP request failed");
        let error_message = if let Some(ai_code) = ai_registry_code {
            json!({ "error": msg, "code": code, "ai_code": ai_code, "status": status.as_u16() })
        } else {
            json!({ "error": msg, "code": code, "status": status.as_u16() })
        };
        let mut response = (status, Json(error_message)).into_response();
        match &self {
            AppError::TooManyRequests {
                retry_after_seconds,
                ..
            }
            | AppError::Backpressure {
                retry_after_seconds,
                ..
            } => {
                if let Ok(value) = HeaderValue::from_str(&retry_after_seconds.to_string()) {
                    response.headers_mut().insert(header::RETRY_AFTER, value);
                }
            }
            _ => {}
        }
        response
    }
}

impl From<sockudo_core::error::Error> for AppError {
    fn from(err: sockudo_core::error::Error) -> Self {
        warn!(original_error = ?err, "Converting internal error to AppError for HTTP response");
        match err {
            sockudo_core::error::Error::InvalidAppKey => {
                AppError::AppNotFound(format!("Application key not found or invalid: {err}"))
            }
            sockudo_core::error::Error::ApplicationNotFound => {
                AppError::AppNotFound(err.to_string())
            }
            sockudo_core::error::Error::InvalidChannelName(s) => {
                AppError::InvalidInput(format!("Invalid channel name: {s}"))
            }
            sockudo_core::error::Error::Channel(s) => AppError::InvalidInput(s),
            sockudo_core::error::Error::InvalidMessageFormat(s) => AppError::InvalidInput(s),
            sockudo_core::error::Error::Auth(s) => AppError::ApiAuthFailed(s),
            sockudo_core::error::Error::AiTransport {
                code,
                name,
                message,
            } => {
                let status = match code {
                    AI_ERROR_PAYLOAD_TOO_LARGE => StatusCode::PAYLOAD_TOO_LARGE,
                    AI_ERROR_MUTABLE_NOT_PERMITTED => StatusCode::FORBIDDEN,
                    _ => StatusCode::BAD_REQUEST,
                };
                AppError::AiTransport {
                    status,
                    code,
                    name,
                    message,
                }
            }
            _ => AppError::InternalError(err.to_string()),
        }
    }
}

// --- Query Parameter Structs ---

// Re-export EventQuery from core auth module (shared with AuthValidator)
pub use sockudo_core::auth::EventQuery;

#[derive(Debug)]
pub struct ChannelQuery {
    pub info: Option<String>,
    pub auth_params: EventQuery,
}

#[derive(Debug)]
pub struct ChannelsQuery {
    pub filter_by_prefix: Option<String>,
    pub info: Option<String>,
    pub auth_params: EventQuery,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct HistoryQuery {
    pub limit: Option<usize>,
    pub direction: Option<String>,
    pub cursor: Option<String>,
    pub start_serial: Option<u64>,
    pub end_serial: Option<u64>,
    pub start_time_ms: Option<i64>,
    pub end_time_ms: Option<i64>,
    /// Ably-compatible alias for `start_time_ms`
    pub start: Option<i64>,
    /// Ably-compatible alias for `end_time_ms`
    pub end: Option<i64>,
}

impl HistoryQuery {
    pub fn resolved_start_time_ms(&self) -> Option<i64> {
        self.start_time_ms.or(self.start)
    }

    pub fn resolved_end_time_ms(&self) -> Option<i64> {
        self.end_time_ms.or(self.end)
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct PresenceSnapshotQuery {
    /// Reconstruct membership as of this timestamp (inclusive)
    pub at_time_ms: Option<i64>,
    /// Ably-compatible alias for `at_time_ms`
    pub at: Option<i64>,
    /// Reconstruct membership as of this serial (inclusive)
    pub at_serial: Option<u64>,
}

impl PresenceSnapshotQuery {
    pub fn resolved_at_time_ms(&self) -> Option<i64> {
        self.at_time_ms.or(self.at)
    }
}

#[derive(Debug, Deserialize)]
pub struct HistoryResetRequestBody {
    pub confirm_channel: String,
    pub confirm_operation: String,
    pub reason: String,
    pub requested_by: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct HistoryPurgeRequestBody {
    pub confirm_channel: String,
    pub confirm_operation: String,
    pub mode: HistoryPurgeMode,
    pub before_serial: Option<u64>,
    pub before_time_ms: Option<i64>,
    pub reason: String,
    pub requested_by: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct VersionMutationPath {
    #[serde(rename = "appId")]
    pub app_id: String,
    #[serde(rename = "channelName")]
    pub channel_name: String,
    #[serde(rename = "messageSerial")]
    pub message_serial: String,
}

#[derive(Debug, Deserialize)]
pub struct AnnotationMutationPath {
    #[serde(rename = "appId")]
    pub app_id: String,
    #[serde(rename = "channelName")]
    pub channel_name: String,
    #[serde(rename = "messageSerial")]
    pub message_serial: String,
    #[serde(rename = "annotationSerial")]
    pub annotation_serial: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct AnnotationEventsQuery {
    #[serde(rename = "type")]
    pub annotation_type: Option<String>,
    pub limit: Option<usize>,
    pub from_serial: Option<String>,
    pub socket_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishAnnotationRequest {
    #[serde(rename = "type")]
    pub annotation_type: String,
    pub name: Option<String>,
    pub client_id: Option<String>,
    pub socket_id: Option<String>,
    pub count: Option<u64>,
    pub data: Option<Value>,
    pub encoding: Option<String>,
}

impl PublishAnnotationRequest {
    fn validate(&self) -> Result<(), String> {
        if self.count == Some(0) {
            return Err("Annotation count must be greater than 0".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishAnnotationResponse {
    pub annotation_serial: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteAnnotationResponse {
    pub annotation_serial: String,
    pub deleted_annotation_serial: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnnotationEventsResponse {
    pub channel: String,
    pub message_serial: String,
    pub limit: usize,
    pub has_more: bool,
    pub next_cursor: Option<String>,
    pub items: Vec<AnnotationEventData>,
}

impl<'de> Deserialize<'de> for ChannelQuery {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut map = std::collections::HashMap::<String, String>::deserialize(deserializer)?;
        let info = map.remove("info");
        let auth_params = EventQuery {
            auth_key: map.remove("auth_key").unwrap_or_default(),
            auth_timestamp: map.remove("auth_timestamp").unwrap_or_default(),
            auth_version: map.remove("auth_version").unwrap_or_default(),
            body_md5: map.remove("body_md5").unwrap_or_default(),
            auth_signature: map.remove("auth_signature").unwrap_or_default(),
        };
        Ok(Self { info, auth_params })
    }
}

impl<'de> Deserialize<'de> for ChannelsQuery {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut map = std::collections::HashMap::<String, String>::deserialize(deserializer)?;
        let filter_by_prefix = map.remove("filter_by_prefix");
        let info = map.remove("info");
        let auth_params = EventQuery {
            auth_key: map.remove("auth_key").unwrap_or_default(),
            auth_timestamp: map.remove("auth_timestamp").unwrap_or_default(),
            auth_version: map.remove("auth_version").unwrap_or_default(),
            body_md5: map.remove("body_md5").unwrap_or_default(),
            auth_signature: map.remove("auth_signature").unwrap_or_default(),
        };
        Ok(Self {
            filter_by_prefix,
            info,
            auth_params,
        })
    }
}

// --- Response Structs ---

#[derive(Serialize)]
struct MemoryStats {
    free: u64,
    used: u64,
    total: u64,
    percent: f64,
}

#[derive(Serialize)]
struct UsageResponse {
    memory: MemoryStats,
}

#[derive(Serialize, Default)]
struct OccupancyStats {
    channels: usize,
    subscriptions: usize,
    presence_channels: usize,
    presence_subscriptions: usize,
    presence_members: usize,
}

#[derive(Serialize, Default)]
struct AppStats {
    app_id: String,
    connections: usize,
    authenticated_connections: usize,
    users: usize,
    connections_with_meta: usize,
    occupancy: OccupancyStats,
}

#[derive(Serialize, Default)]
struct GlobalStats {
    apps: usize,
    connections: usize,
    authenticated_connections: usize,
    users: usize,
    connections_with_meta: usize,
    occupancy: OccupancyStats,
}

#[derive(Serialize)]
struct StatsResponse {
    memory: MemoryStats,
    history: HistoryStatusResponse,
    presence_history: PresenceHistoryStatusResponse,
    totals: GlobalStats,
    apps: Vec<AppStats>,
}

#[derive(Serialize, Default)]
struct HistoryStatusResponse {
    enabled: bool,
    backend: String,
    state_authority: String,
    degraded_channels: usize,
    reset_required_channels: usize,
    queue_depth: usize,
}

#[derive(Serialize, Default)]
struct PresenceHistoryStatusResponse {
    enabled: bool,
    backend: String,
    state_authority: String,
    degraded_channels: usize,
    reset_required_channels: usize,
    queue_depth: usize,
}

// --- Helper Functions ---

fn build_history_stream_state_payload(
    state: &sockudo_core::history::HistoryStreamRuntimeState,
) -> Value {
    json!({
        "stream_id": state.stream_id,
        "durable_state": state.durable_state.as_str(),
        "recovery_allowed": state.recovery_allowed,
        "reset_required": state.reset_required,
        "reason": state.reason,
        "node_id": state.node_id,
        "last_transition_at_ms": state.last_transition_at_ms,
        "authoritative_source": state.authoritative_source,
        "observed_source": state.observed_source,
    })
}

fn build_history_stream_inspection_payload(
    inspection: &sockudo_core::history::HistoryStreamInspection,
) -> Value {
    json!({
        "stream_id": inspection.stream_id,
        "next_serial": inspection.next_serial,
        "retained": {
            "stream_id": inspection.retained.stream_id,
            "retained_messages": inspection.retained.retained_messages,
            "retained_bytes": inspection.retained.retained_bytes,
            "oldest_available_serial": inspection.retained.oldest_serial,
            "newest_available_serial": inspection.retained.newest_serial,
            "oldest_available_published_at_ms": inspection.retained.oldest_published_at_ms,
            "newest_available_published_at_ms": inspection.retained.newest_published_at_ms,
        },
        "state": build_history_stream_state_payload(&inspection.state),
    })
}

fn build_presence_history_stream_state_payload(state: &PresenceHistoryStreamRuntimeState) -> Value {
    json!({
        "stream_id": state.stream_id,
        "durable_state": state.durable_state.as_str(),
        "continuity_proven": state.continuity_proven,
        "reset_required": state.reset_required,
        "reason": state.reason,
        "node_id": state.node_id,
        "last_transition_at_ms": state.last_transition_at_ms,
        "authoritative_source": state.authoritative_source,
        "observed_source": state.observed_source,
    })
}

fn build_presence_history_stream_inspection_payload(
    inspection: &PresenceHistoryStreamInspection,
) -> Value {
    json!({
        "stream_id": inspection.stream_id,
        "next_serial": inspection.next_serial,
        "retained": {
            "stream_id": inspection.retained.stream_id,
            "retained_events": inspection.retained.retained_events,
            "retained_bytes": inspection.retained.retained_bytes,
            "oldest_available_serial": inspection.retained.oldest_serial,
            "newest_available_serial": inspection.retained.newest_serial,
            "oldest_available_published_at_ms": inspection.retained.oldest_published_at_ms,
            "newest_available_published_at_ms": inspection.retained.newest_published_at_ms,
        },
        "state": build_presence_history_stream_state_payload(&inspection.state),
    })
}

fn parse_history_direction(raw: Option<&str>) -> Result<HistoryDirection, AppError> {
    match raw.unwrap_or("newest_first").to_ascii_lowercase().as_str() {
        "newest_first" | "backwards" | "reverse" => Ok(HistoryDirection::NewestFirst),
        "oldest_first" | "forwards" | "forward" => Ok(HistoryDirection::OldestFirst),
        other => Err(AppError::InvalidInput(format!(
            "Invalid direction '{other}'. Accepted values: newest_first, oldest_first, backwards, forwards"
        ))),
    }
}

fn parse_presence_history_direction(
    raw: Option<&str>,
) -> Result<PresenceHistoryDirection, AppError> {
    match raw.unwrap_or("newest_first").to_ascii_lowercase().as_str() {
        "newest_first" | "backwards" | "reverse" => Ok(PresenceHistoryDirection::NewestFirst),
        "oldest_first" | "forwards" | "forward" => Ok(PresenceHistoryDirection::OldestFirst),
        other => Err(AppError::InvalidInput(format!(
            "Invalid direction '{other}'. Accepted values: newest_first, oldest_first, backwards, forwards"
        ))),
    }
}

fn parse_version_direction(raw: Option<VersionDirection>) -> VersionStoreDirection {
    match raw.unwrap_or(VersionDirection::NewestFirst) {
        VersionDirection::NewestFirst => VersionStoreDirection::NewestFirst,
        VersionDirection::OldestFirst => VersionStoreDirection::OldestFirst,
    }
}

fn validate_history_destructive_request(
    path_channel: &str,
    confirm_channel: &str,
    confirm_operation: &str,
    expected_operation: &str,
    reason: &str,
) -> Result<(), AppError> {
    if confirm_channel != path_channel {
        return Err(AppError::InvalidInput(
            "confirm_channel must exactly match the channel path".to_string(),
        ));
    }
    if confirm_operation != expected_operation {
        return Err(AppError::InvalidInput(format!(
            "confirm_operation must be '{expected_operation}'"
        )));
    }
    if reason.trim().is_empty() {
        return Err(AppError::InvalidInput(
            "reason must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn require_versioned_messages_enabled(
    handler: &ConnectionHandler,
    channel_name: &str,
) -> Result<(), AppError> {
    if !handler.server_options().versioned_messages.enabled {
        if handler
            .server_options()
            .ai_transport
            .matches_channel(channel_name)
        {
            return Err(mutable_not_permitted());
        }
        return Err(AppError::FeatureDisabled(format!(
            "Versioned messages are disabled for channel '{channel_name}'"
        )));
    }
    Ok(())
}

fn require_annotations_enabled(
    handler: &ConnectionHandler,
    app: &App,
    channel_name: &str,
) -> Result<(), AppError> {
    if !handler.server_options().annotations.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Annotations are disabled globally for channel '{channel_name}'"
        )));
    }
    if !app.annotations_enabled_for_channel(channel_name) {
        return Err(AppError::Forbidden(format!(
            "Annotations are disabled by channel policy for channel '{channel_name}'"
        )));
    }
    Ok(())
}

fn parse_message_serial(raw: &str) -> Result<MessageSerial, AppError> {
    MessageSerial::new(raw.to_string()).map_err(AppError::from)
}

fn parse_annotation_serial(raw: &str) -> Result<AnnotationSerial, AppError> {
    AnnotationSerial::new(raw.to_string()).map_err(AppError::from)
}

fn parse_annotation_type(raw: &str) -> Result<AnnotationType, AppError> {
    AnnotationType::new(raw.to_string()).map_err(AppError::from)
}

fn annotation_wire_event(annotation: &Annotation) -> AnnotationEventData {
    AnnotationEventData {
        action: match annotation.action {
            AnnotationAction::Create => AnnotationEventAction::Create,
            AnnotationAction::Delete => AnnotationEventAction::Delete,
        },
        id: matches!(annotation.action, AnnotationAction::Create)
            .then(|| annotation.id.as_str().to_string()),
        serial: annotation.serial.as_str().to_string(),
        message_serial: annotation.message_serial.as_str().to_string(),
        annotation_type: annotation.annotation_type.as_str().to_string(),
        name: annotation.name.clone(),
        client_id: annotation.client_id.clone(),
        count: annotation.count,
        data: annotation.data.clone(),
        encoding: annotation.encoding.clone(),
        timestamp: annotation.timestamp,
    }
}

fn protocol_action(
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

fn build_versioned_realtime_message(record: &StoredVersionRecord) -> VersionedRealtimeMessage {
    let action = protocol_action(record.message.action);
    VersionedRealtimeMessage {
        message: PusherMessage {
            event: Some(action.v2_event_name()),
            channel: Some(record.channel.clone()),
            data: record.message.data.clone(),
            name: record.message.name.clone(),
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: None,
            serial: Some(record.delivery_serial()),
            idempotency_key: None,
            extras: record.message.extras.clone(),
            delta_sequence: None,
            delta_conflation_key: None,
        },
        action,
        message_serial: record.message_serial().as_str().to_string(),
        history_serial: Some(record.history_serial()),
        delivery_serial: Some(record.delivery_serial()),
        version: Some(MessageVersionMetadata {
            serial: record.version_serial().as_str().to_string(),
            client_id: record.message.version.client_id.clone(),
            timestamp_ms: record.message.version.timestamp_ms,
            description: record.message.version.description.clone(),
            metadata: record.message.version.metadata.clone(),
        }),
    }
}

fn build_mutation_version_metadata(
    handler: &ConnectionHandler,
    client_id: Option<String>,
    description: Option<String>,
    metadata: Option<Value>,
) -> Result<VersionMetadata, AppError> {
    Ok(VersionMetadata {
        serial: VersionSerial::new(handler.next_version_serial().to_string())?,
        client_id,
        timestamp_ms: sockudo_core::history::now_ms(),
        description,
        metadata,
    })
}

async fn resolve_mutation_actor_identity(
    handler: &Arc<ConnectionHandler>,
    app_id: &str,
    channel: &str,
    kind: MutationKind,
    original_client_id: Option<&str>,
    requested_client_id: Option<&str>,
    requested_socket_id: Option<&str>,
) -> Result<Option<String>, AppError> {
    let action_metric = format!("message.{}", kind.as_verb());
    if let Some(raw_socket_id) = requested_socket_id {
        let socket_id = SocketId::from_string(raw_socket_id)
            .map_err(|e| AppError::InvalidInput(format!("Invalid socket_id: {e}")))?;
        let connection = handler
            .connection_manager()
            .get_connection(&socket_id, app_id)
            .await
            .ok_or_else(|| {
                if let Some(metrics) = handler.metrics() {
                    metrics.mark_versioned_message_mutation(app_id, &action_metric, "auth_failed");
                }
                AppError::ApiAuthFailed(format!(
                    "Mutation actor socket '{raw_socket_id}' is not connected"
                ))
            })?;

        if connection.protocol_version != sockudo_protocol::ProtocolVersion::V2 {
            if let Some(metrics) = handler.metrics() {
                metrics.mark_versioned_message_mutation(app_id, &action_metric, "auth_failed");
            }
            return Err(AppError::ApiAuthFailed(
                "Mutation actor socket must use protocol V2".to_string(),
            ));
        }

        let actor_client_id = connection.get_user_id().await;
        if let Some(requested_client_id) = requested_client_id {
            let authenticated_client_id = actor_client_id.as_deref().ok_or_else(|| {
                if let Some(metrics) = handler.metrics() {
                    metrics.mark_versioned_message_mutation(app_id, &action_metric, "auth_failed");
                }
                AppError::ApiAuthFailed(
                    "Mutation actor socket is not signed in with an identified client".to_string(),
                )
            })?;
            if authenticated_client_id != requested_client_id {
                if let Some(metrics) = handler.metrics() {
                    metrics.mark_versioned_message_mutation(app_id, &action_metric, "auth_failed");
                }
                return Err(AppError::ApiAuthFailed(format!(
                    "Requested client_id '{}' does not match authenticated actor '{}'",
                    requested_client_id, authenticated_client_id
                )));
            }
        }

        let capabilities = connection.get_connection_capabilities().await;
        if connection.get_token_auth_context().await.is_some()
            && capabilities
                .as_ref()
                .is_none_or(|capabilities| !capabilities.allows_publish(channel))
        {
            if let Some(metrics) = handler.metrics() {
                metrics.mark_versioned_message_mutation(app_id, &action_metric, "auth_failed");
            }
            warn!(
                app_id = %app_id,
                channel = %channel,
                action = %kind.as_verb(),
                "Denied versioned message mutation because token lacks publish capability"
            );
            return Err(mutable_not_permitted());
        }
        if let Err(err) = authorize_message_mutation(MutationAuthorizationRequest {
            channel,
            kind,
            original_client_id,
            actor_client_id: actor_client_id.as_deref(),
            capabilities: capabilities.as_ref(),
            privileged_server: false,
        }) {
            if let Some(metrics) = handler.metrics() {
                metrics.mark_versioned_message_mutation(app_id, &action_metric, "auth_failed");
            }
            warn!(
                app_id = %app_id,
                channel = %channel,
                action = %kind.as_verb(),
                original_client_id = ?original_client_id,
                actor_client_id = ?actor_client_id,
                "Denied versioned message mutation authorization"
            );
            let _ = err;
            return Err(mutable_not_permitted());
        }

        return Ok(actor_client_id.or_else(|| requested_client_id.map(str::to_string)));
    }

    authorize_message_mutation(MutationAuthorizationRequest {
        channel,
        kind,
        original_client_id,
        actor_client_id: requested_client_id,
        capabilities: None,
        privileged_server: true,
    })?;

    Ok(requested_client_id.map(str::to_string))
}

async fn resolve_annotation_publish_actor(
    handler: &Arc<ConnectionHandler>,
    app_id: &str,
    channel: &str,
    requested_client_id: Option<&str>,
    requested_socket_id: Option<&str>,
) -> Result<Option<String>, AppError> {
    if let Some(raw_socket_id) = requested_socket_id {
        let socket_id = SocketId::from_string(raw_socket_id)
            .map_err(|e| AppError::InvalidInput(format!("Invalid socket_id: {e}")))?;
        let connection = handler
            .connection_manager()
            .get_connection(&socket_id, app_id)
            .await
            .ok_or_else(|| {
                AppError::ApiAuthFailed(format!(
                    "Annotation actor socket '{raw_socket_id}' is not connected"
                ))
            })?;

        if connection.protocol_version != sockudo_protocol::ProtocolVersion::V2 {
            return Err(AppError::ApiAuthFailed(
                "Annotation actor socket must use protocol V2".to_string(),
            ));
        }

        let actor_client_id = connection.get_user_id().await;
        if let Some(requested_client_id) = requested_client_id
            && actor_client_id.as_deref() != Some(requested_client_id)
        {
            return Err(AppError::ApiAuthFailed(format!(
                "Requested client_id '{}' does not match authenticated actor",
                requested_client_id
            )));
        }

        if connection
            .get_connection_capabilities()
            .await
            .is_none_or(|capabilities| {
                !capabilities.allows_publish(channel)
                    || !capabilities.allows_annotation_publish(channel)
            })
        {
            return Err(AppError::Forbidden(format!(
                "annotation-publish capability is required for channel '{channel}'"
            )));
        }

        return Ok(actor_client_id.or_else(|| requested_client_id.map(str::to_string)));
    }

    Ok(requested_client_id.map(str::to_string))
}

async fn authorize_annotation_delete(
    handler: &Arc<ConnectionHandler>,
    app_id: &str,
    channel: &str,
    target_client_id: Option<&str>,
    requested_socket_id: Option<&str>,
) -> Result<Option<String>, AppError> {
    let Some(raw_socket_id) = requested_socket_id else {
        return Ok(None);
    };

    let socket_id = SocketId::from_string(raw_socket_id)
        .map_err(|e| AppError::InvalidInput(format!("Invalid socket_id: {e}")))?;
    let connection = handler
        .connection_manager()
        .get_connection(&socket_id, app_id)
        .await
        .ok_or_else(|| {
            AppError::ApiAuthFailed(format!(
                "Annotation actor socket '{raw_socket_id}' is not connected"
            ))
        })?;

    if connection.protocol_version != sockudo_protocol::ProtocolVersion::V2 {
        return Err(AppError::ApiAuthFailed(
            "Annotation actor socket must use protocol V2".to_string(),
        ));
    }

    let actor_client_id = connection.get_user_id().await;
    let capabilities = connection.get_connection_capabilities().await;
    let any_allowed = capabilities
        .as_ref()
        .is_some_and(|caps| caps.allows_annotation_delete_any(channel));
    let own_allowed = capabilities
        .as_ref()
        .is_some_and(|caps| caps.allows_annotation_delete_own(channel))
        && actor_client_id.as_deref().is_some()
        && actor_client_id.as_deref() == target_client_id;

    if any_allowed && actor_client_id.is_none() {
        return Err(AppError::Forbidden(
            "annotation-delete-any requires an identified client".to_string(),
        ));
    }

    if !any_allowed && !own_allowed {
        return Err(AppError::Forbidden(format!(
            "annotation-delete-own or annotation-delete-any capability is required for channel '{channel}'"
        )));
    }

    Ok(actor_client_id)
}

async fn authorize_annotation_subscribe(
    handler: &Arc<ConnectionHandler>,
    app_id: &str,
    channel: &str,
    requested_socket_id: Option<&str>,
) -> Result<(), AppError> {
    let Some(raw_socket_id) = requested_socket_id else {
        return Ok(());
    };

    let socket_id = SocketId::from_string(raw_socket_id)
        .map_err(|e| AppError::InvalidInput(format!("Invalid socket_id: {e}")))?;
    let connection = handler
        .connection_manager()
        .get_connection(&socket_id, app_id)
        .await
        .ok_or_else(|| {
            AppError::ApiAuthFailed(format!(
                "Annotation subscriber socket '{raw_socket_id}' is not connected"
            ))
        })?;

    if connection.protocol_version != sockudo_protocol::ProtocolVersion::V2 {
        return Err(AppError::ApiAuthFailed(
            "Annotation subscriber socket must use protocol V2".to_string(),
        ));
    }

    if connection
        .get_connection_capabilities()
        .await
        .is_none_or(|capabilities| {
            !capabilities.allows_subscribe(channel)
                || !capabilities.allows_annotation_subscribe(channel)
        })
    {
        return Err(AppError::Forbidden(format!(
            "annotation-subscribe capability is required for channel '{channel}'"
        )));
    }

    Ok(())
}

fn update_delta_from_request(request: &UpdateMessageRequest) -> MessageFieldDelta {
    let mut delta = MessageFieldDelta::default();
    if let Some(name) = request.name.clone() {
        delta.name = FieldPatch::Replace(name);
    }
    if let Some(data) = request.data.clone() {
        delta.data = FieldPatch::Replace(data);
    }
    if let Some(extras) = request.extras.clone() {
        delta.extras = FieldPatch::Replace(extras);
    }
    for field in &request.clear_fields {
        match field {
            sockudo_protocol::versioned_messages::ClearField::Name => {
                delta.name = FieldPatch::Clear
            }
            sockudo_protocol::versioned_messages::ClearField::Data => {
                delta.data = FieldPatch::Clear
            }
            sockudo_protocol::versioned_messages::ClearField::Extras => {
                delta.extras = FieldPatch::Clear
            }
        }
    }
    delta
}

fn delete_delta_from_request(request: &DeleteMessageRequest) -> MessageFieldDelta {
    let mut delta = MessageFieldDelta::default();
    if let Some(data) = request.data.clone() {
        delta.data = FieldPatch::Replace(data);
    }
    if let Some(extras) = request.extras.clone() {
        delta.extras = FieldPatch::Replace(extras);
    }
    for field in &request.clear_fields {
        match field {
            sockudo_protocol::versioned_messages::ClearField::Name => {
                delta.name = FieldPatch::Clear
            }
            sockudo_protocol::versioned_messages::ClearField::Data => {
                delta.data = FieldPatch::Clear
            }
            sockudo_protocol::versioned_messages::ClearField::Extras => {
                delta.extras = FieldPatch::Clear
            }
        }
    }
    delta
}

/// Helper to build cache payload string
fn build_cache_payload(
    event_name: &str,
    event_data: &Value,
    channel: &str,
) -> Result<String, sonic_rs::Error> {
    sonic_rs::to_string(&json!({
        "event": event_name,
        "channel": channel,
        "data": event_data,
    }))
}

/// Records API metrics (helper async function)
#[instrument(skip(handler, incoming_request_size, outgoing_response_size), fields(app_id = %app_id))]
async fn record_api_metrics(
    handler: &Arc<ConnectionHandler>,
    app_id: &str,
    incoming_request_size: usize,
    outgoing_response_size: usize,
) {
    if let Some(metrics_arc) = handler.metrics() {
        metrics_arc.mark_api_message(app_id, incoming_request_size, outgoing_response_size);
        debug!(
            incoming_bytes = incoming_request_size,
            outgoing_bytes = outgoing_response_size,
            "Recorded API message metrics"
        );
    } else {
        debug!(
            "{}",
            "Metrics system not available, skipping metrics recording."
        );
    }
}

async fn ai_channel_stats(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    app_id: &str,
    channel: &str,
) -> Result<Option<Value>, AppError> {
    if !handler
        .server_options()
        .ai_transport
        .matches_channel(channel)
    {
        return Ok(None);
    }

    let history_policy = app.resolved_history(channel, &handler.server_options().history);
    let last_history_serial = if history_policy.enabled {
        let page = handler
            .history_store()
            .read_page(HistoryReadRequest {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                direction: HistoryDirection::NewestFirst,
                limit: 1,
                cursor: None,
                bounds: HistoryQueryBounds::default(),
            })
            .await?;
        page.items.first().map(|item| item.serial)
    } else {
        None
    };

    let latest_messages = if handler.server_options().versioned_messages.enabled {
        handler
            .version_store()
            .latest_by_history(app_id, channel)
            .await?
    } else {
        Vec::new()
    };
    let active_streams = latest_messages
        .iter()
        .filter(|record| {
            record
                .message
                .extras
                .as_ref()
                .and_then(|extras| extras.ai_transport_headers())
                .and_then(|headers| headers.status())
                == Some("streaming")
        })
        .count();

    Ok(Some(json!({
        "active_streams": active_streams,
        "last_history_serial": last_history_serial,
        "message_count": latest_messages.len()
    })))
}

// --- API Handlers ---

/// GET /usage
#[instrument(skip_all, fields(service = "usage_monitor"))]
pub async fn usage() -> Result<impl IntoResponse, AppError> {
    let mut sys = System::new_all();
    sys.refresh_all();

    let total = sys.total_memory() * 1024;
    let used = sys.used_memory() * 1024;
    let free = total.saturating_sub(used);
    let percent = if total > 0 {
        (used as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    let memory_stats = MemoryStats {
        free,
        used,
        total,
        percent,
    };
    let response_payload = UsageResponse {
        memory: memory_stats,
    };

    info!(
        total_bytes = total,
        used_bytes = used,
        free_bytes = free,
        usage_percent = format!("{:.2}", percent),
        "Memory usage queried"
    );

    Ok((StatusCode::OK, Json(response_payload)))
}

/// GET /stats
#[instrument(skip(handler), fields(service = "stats"))]
pub async fn stats(
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    let mut sys = System::new_all();
    sys.refresh_all();

    let total = sys.total_memory() * 1024;
    let used = sys.used_memory() * 1024;
    let free = total.saturating_sub(used);
    let percent = if total > 0 {
        (used as f64 / total as f64) * 100.0
    } else {
        0.0
    };

    let memory = MemoryStats {
        free,
        used,
        total,
        percent,
    };
    let history_status = handler
        .history_store()
        .runtime_status()
        .await
        .unwrap_or_default();
    let presence_history_status = handler
        .presence_history_store()
        .runtime_status()
        .await
        .unwrap_or_default();

    let mut app_stats = Vec::new();
    let mut totals = GlobalStats::default();

    for (app_id, namespace) in handler.connection_manager().get_namespaces().await? {
        let mut app_stat = AppStats {
            app_id,
            connections: namespace.sockets.len(),
            users: namespace.users.len(),
            ..Default::default()
        };

        let channel_names: Vec<String> = namespace
            .channels
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        app_stat.occupancy.channels = channel_names.len();
        app_stat.occupancy.subscriptions = namespace.channels.iter().map(|entry| entry.len()).sum();

        for socket in namespace.sockets.iter() {
            if socket.value().get_user_id().await.is_some() {
                app_stat.authenticated_connections += 1;
            }
            if socket.value().get_connection_meta().await.is_some() {
                app_stat.connections_with_meta += 1;
            }
        }

        for channel_name in channel_names {
            if channel_name.starts_with("presence-") {
                app_stat.occupancy.presence_channels += 1;
                app_stat.occupancy.presence_subscriptions +=
                    namespace.get_channel_socket_count(&channel_name);
                app_stat.occupancy.presence_members +=
                    namespace.get_channel_members(&channel_name).await?.len();
            }
        }

        totals.apps += 1;
        totals.connections += app_stat.connections;
        totals.authenticated_connections += app_stat.authenticated_connections;
        totals.users += app_stat.users;
        totals.connections_with_meta += app_stat.connections_with_meta;
        totals.occupancy.channels += app_stat.occupancy.channels;
        totals.occupancy.subscriptions += app_stat.occupancy.subscriptions;
        totals.occupancy.presence_channels += app_stat.occupancy.presence_channels;
        totals.occupancy.presence_subscriptions += app_stat.occupancy.presence_subscriptions;
        totals.occupancy.presence_members += app_stat.occupancy.presence_members;

        app_stats.push(app_stat);
    }

    app_stats.sort_by(|a, b| a.app_id.cmp(&b.app_id));

    Ok((
        StatusCode::OK,
        Json(StatsResponse {
            memory,
            history: HistoryStatusResponse {
                enabled: history_status.enabled,
                backend: history_status.backend,
                state_authority: history_status.state_authority,
                degraded_channels: history_status.degraded_channels,
                reset_required_channels: history_status.reset_required_channels,
                queue_depth: history_status.queue_depth,
            },
            presence_history: PresenceHistoryStatusResponse {
                enabled: presence_history_status.enabled,
                backend: presence_history_status.backend,
                state_authority: presence_history_status.state_authority,
                degraded_channels: presence_history_status.degraded_channels,
                reset_required_channels: presence_history_status.reset_required_channels,
                queue_depth: presence_history_status.queue_depth,
            },
            totals,
            apps: app_stats,
        }),
    ))
}

#[derive(Clone, Copy)]
struct MessageIdIdempotencyContext {
    enabled: bool,
    ttl_seconds: u64,
}

/// Helper to process a single event and return channel info if requested
#[instrument(skip(handler, event_data, app, start_time_ms, message_id_idempotency), fields(app_id = app.id, event_name = field::Empty))]
async fn process_single_event_parallel(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    event_data: PusherApiMessage,
    collect_info: bool,
    start_time_ms: Option<f64>,
    idempotency_key: Option<String>,
    message_id_idempotency: MessageIdIdempotencyContext,
) -> Result<HashMap<String, Value>, AppError> {
    let PusherApiMessage {
        name,
        data: event_payload_data,
        channels,
        channel,
        socket_id: original_socket_id_str,
        info,
        tags,
        delta: delta_flag,
        idempotency_key: _,
        message_id,
        extras,
    } = event_data;

    let event_name_str = name
        .as_deref()
        .ok_or_else(|| AppError::InvalidInput("Event name is required".to_string()))?;
    tracing::Span::current().record("event_name", event_name_str);

    let max_event_name_len = app
        .event_name_limit()
        .unwrap_or(DEFAULT_EVENT_NAME_MAX_LENGTH as u32);
    if event_name_str.len() > max_event_name_len as usize {
        return Err(AppError::LimitExceeded(format!(
            "Event name '{event_name_str}' exceeds maximum length of {max_event_name_len}"
        )));
    }

    if let Some(max_payload_kb) = app.event_payload_limit_kb() {
        let value_for_size_calc = match &event_payload_data {
            Some(ApiMessageData::String(s)) => json!(s),
            Some(ApiMessageData::Json(j_val)) => j_val.clone(),
            None => json!(null),
        };
        let payload_size_bytes = utils::data_to_bytes_flexible(vec![value_for_size_calc]);
        if payload_size_bytes > (max_payload_kb as usize * 1024) {
            return Err(AppError::PayloadTooLarge(format!(
                "Event payload size ({payload_size_bytes} bytes) for event '{event_name_str}' exceeds limit ({max_payload_kb}KB)"
            )));
        }
    }

    let mapped_socket_id: Option<SocketId> = original_socket_id_str
        .as_ref()
        .and_then(|s| SocketId::from_string(s).ok());

    let target_channels: Vec<String> = match channels {
        Some(ch_list) if !ch_list.is_empty() => {
            if let Some(max_ch_at_once) = app.event_channels_at_once_limit()
                && ch_list.len() > max_ch_at_once as usize
            {
                return Err(AppError::LimitExceeded(format!(
                    "Number of channels ({}) exceeds limit ({})",
                    ch_list.len(),
                    max_ch_at_once
                )));
            }
            ch_list
        }
        None => match channel {
            Some(ch_str) => vec![ch_str],
            None => {
                warn!("{}", "Missing 'channels' or 'channel' in event");
                return Err(AppError::MissingChannelInfo);
            }
        },
        Some(_) => {
            warn!("{}", "Empty 'channels' list provided in event");
            return Err(AppError::MissingChannelInfo);
        }
    };

    let channel_processing_futures = target_channels.into_iter().map(|target_channel_str| {
        let handler_clone = Arc::clone(handler);
        let name_for_task = name.clone();
        let payload_for_task = event_payload_data.clone();
        let socket_id_for_task = mapped_socket_id;
        let info_for_task = info.clone();
        let event_name_for_task = event_name_str.to_string();
        let tags_for_task: Option<std::collections::BTreeMap<String, String>> = tags
            .clone()
            .map(|h| h.into_iter().collect());
        let delta_flag_for_task = delta_flag;
        let idempotency_key_for_task = idempotency_key.clone();
        let message_id_for_task = message_id.clone();
        let extras_for_task = extras.clone();

        async move {
            debug!(channel = %target_channel_str, "Processing channel for event (parallel task)");

            validate_channel_name(app, &target_channel_str).await?;
            validate_ai_http_publish(
                handler_clone.as_ref(),
                app,
                &target_channel_str,
                event_name_for_task.as_str(),
                message_id_for_task.as_deref(),
                extras_for_task.as_ref(),
            )
            .await?;

            if let Some(message_id) = message_id_for_task.as_deref()
                && message_id_idempotency.enabled
            {
                let cache_key = message_id_cache_key(&app.id, &target_channel_str, message_id);
                match handler_clone.cache_manager().get(&cache_key).await {
                    Ok(Some(cached)) if cached != "__processing__" => {
                        if let Some(metrics) = handler_clone.metrics() {
                            metrics.mark_idempotency_duplicate(&app.id);
                        }
                        let cached_ack = sonic_rs::from_str(&cached).unwrap_or_else(|_| json!({}));
                        return Ok(Some((target_channel_str.clone(), cached_ack)));
                    }
                    Ok(Some(_)) => {
                        if let Some(cached_ack) =
                            wait_for_message_id_ack(handler_clone.as_ref(), &cache_key).await
                        {
                            if let Some(metrics) = handler_clone.metrics() {
                                metrics.mark_idempotency_duplicate(&app.id);
                            }
                            return Ok(Some((target_channel_str.clone(), cached_ack)));
                        }
                        return Err(AppError::Backpressure {
                            message: "message_id duplicate is still processing".to_string(),
                            retry_after_seconds: 1,
                        });
                    }
                    Ok(None) => {
                        if let Some(metrics) = handler_clone.metrics() {
                            metrics.mark_idempotency_publish(&app.id);
                        }
                        match handler_clone
                            .cache_manager()
                            .set_if_not_exists(
                                &cache_key,
                                "__processing__",
                                message_id_idempotency.ttl_seconds,
                            )
                            .await
                        {
                            Ok(true) => {}
                            Ok(false) => {
                                if let Some(cached_ack) =
                                    wait_for_message_id_ack(handler_clone.as_ref(), &cache_key)
                                        .await
                                {
                                    if let Some(metrics) = handler_clone.metrics() {
                                        metrics.mark_idempotency_duplicate(&app.id);
                                    }
                                    return Ok(Some((target_channel_str.clone(), cached_ack)));
                                }
                                return Err(AppError::Backpressure {
                                    message: "message_id duplicate is still processing".to_string(),
                                    retry_after_seconds: 1,
                                });
                            }
                            Err(e) => {
                                warn!(message_id = %message_id, error = %e, "Failed to claim message_id idempotency key, proceeding without dedup");
                            }
                        }
                    }
                    Err(e) => {
                        warn!(message_id = %message_id, error = %e, "Failed to check message_id idempotency cache, proceeding without dedup");
                    }
                }
            }

            let message_data = match payload_for_task {
                Some(ApiMessageData::String(s)) => {
                    MessageData::String(s)
                },
                Some(ApiMessageData::Json(j_val)) => {
                    MessageData::String(j_val.to_string())
                },
                None => MessageData::String("null".to_string()),
            };
            let _message_to_send = PusherMessage {
                channel: Some(target_channel_str.clone()),
                name: None,
                event: name_for_task,
                data: Some(message_data.clone()),
                user_id: None,
                tags: tags_for_task.clone(),
                sequence: None,
                conflation_key: None,
                message_id: if handler_clone.server_options().connection_recovery.enabled {
                    message_id_for_task
                        .clone()
                        .or_else(|| Some(uuid::Uuid::new_v4().to_string()))
                } else {
                    message_id_for_task.clone()
                },
                stream_id: None,
                serial: None,
                idempotency_key: idempotency_key_for_task.clone(),
                extras: extras_for_task.clone(),
                delta_sequence: None,
                delta_conflation_key: None,
            };
            let timestamp_ms = start_time_ms;

            let force_full = matches!(delta_flag_for_task, Some(false));
            let publish_ack = handler_clone
                .publish_to_channel_with_timing(
                    app,
                    &target_channel_str,
                    _message_to_send,
                    socket_id_for_task.as_ref(),
                    timestamp_ms,
                    force_full,
                )
                .await?;

            let mut collected_channel_specific_info: Option<(String, Value)> = None;
            if collect_info {
                let is_presence = target_channel_str.starts_with("presence-");
                let mut current_channel_info_map = sonic_rs::Object::new();

                if let Some(ack) = publish_ack {
                    current_channel_info_map.insert("message_serial", json!(ack.message_serial));
                    current_channel_info_map.insert("history_serial", json!(ack.history_serial));
                    current_channel_info_map.insert("delivery_serial", json!(ack.delivery_serial));
                    current_channel_info_map.insert("version_serial", json!(ack.version_serial));
                }

                if is_presence && info_for_task.as_deref().is_some_and(|s| s.contains("user_count")) {
                    match ChannelManager::get_channel_members(
                        handler_clone.connection_manager(),
                        &app.id,
                        &target_channel_str
                    )
                    .await
                    {
                        Ok(members_map) => {
                            current_channel_info_map
                                .insert("user_count", json!(members_map.len()));
                        }
                        Err(e) => {
                            warn!(
                                "Failed to get user count for channel {}: {} (internal error: {:?})",
                                target_channel_str, e, e
                            );
                        }
                    }
                }

                if info_for_task
                    .as_deref()
                    .is_some_and(|s| s.contains("subscription_count"))
                {
                    let count = handler_clone
                        .connection_manager()
                        .get_channel_socket_count(&app.id, &target_channel_str)
                        .await;
                    current_channel_info_map.insert("subscription_count", json!(count));
                }

                if !current_channel_info_map.is_empty() {
                    if let Some(message_id) = message_id_for_task.as_deref()
                        && message_id_idempotency.enabled
                        && let Ok(serialized) =
                            sonic_rs::to_string(&current_channel_info_map.clone().into_value())
                    {
                        let cache_key = message_id_cache_key(&app.id, &target_channel_str, message_id);
                        let _ = handler_clone
                            .cache_manager()
                            .set(&cache_key, &serialized, message_id_idempotency.ttl_seconds)
                            .await;
                    }
                    collected_channel_specific_info = Some((
                        target_channel_str.clone(),
                        current_channel_info_map.into_value(),
                    ));
                }
            }

            // Handle caching for cacheable channels.
            if utils::is_cache_channel(&target_channel_str) {
                let message_data = sonic_rs::to_value(&message_data)
                    .map_err(AppError::SerializationError)?;
                match build_cache_payload(&event_name_for_task, &message_data, &target_channel_str) {
                    Ok(cache_payload_str) => {
                        let cache_key_str =
                            format!("app:{}:channel:{}:cache_miss", &app.id, target_channel_str);

                        match handler_clone
                            .cache_manager()
                            .set(&cache_key_str, &cache_payload_str, 3600)
                            .await
                        {
                            Ok(_) => {
                                debug!(channel = %target_channel_str, cache_key = %cache_key_str, "Cached event for channel");
                            }
                            Err(e) => {
                                error!(channel = %target_channel_str, cache_key = %cache_key_str, error = %e, "Failed to cache event (internal error: {:?})", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!(channel = %target_channel_str, error = %e, "Failed to serialize event data for caching");
                    }
                }
            }
            Ok(collected_channel_specific_info)
        }
    });

    let results: Vec<Result<Option<(String, Value)>, AppError>> =
        join_all(channel_processing_futures).await;

    let mut final_channels_info_map = HashMap::new();
    for result in results {
        match result {
            Ok(Some((channel_name, info_value))) => {
                final_channels_info_map.insert(channel_name, info_value);
            }
            Ok(None) => {}
            Err(e) => {
                return Err(e);
            }
        }
    }

    Ok(final_channels_info_map)
}

/// Resolve the idempotency key from the request body field or the `X-Idempotency-Key` header.
/// Body field takes precedence. Returns `None` when idempotency is disabled or no key was
/// provided.
fn resolve_idempotency_key(
    body_key: &Option<String>,
    headers: &HeaderMap,
    config: &sockudo_core::options::IdempotencyConfig,
) -> Result<Option<String>, AppError> {
    if !config.enabled {
        return Ok(None);
    }

    let key = body_key.clone().or_else(|| {
        headers
            .get("x-idempotency-key")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    });

    if let Some(ref k) = key {
        if k.is_empty() {
            return Err(AppError::InvalidInput(
                "Idempotency key must not be empty".to_string(),
            ));
        }
        if k.len() > config.max_key_length {
            return Err(AppError::InvalidInput(format!(
                "Idempotency key exceeds maximum length of {} characters",
                config.max_key_length
            )));
        }
    }

    Ok(key)
}

#[cfg(feature = "push")]
fn push_rule_target_channels(event: &PusherApiMessage) -> Vec<String> {
    if let Some(channels) = event.channels.as_ref() {
        return channels.clone();
    }
    event.channel.iter().cloned().collect()
}

/// Build the cache key used for idempotency storage.
fn idempotency_cache_key(app_id: &str, key: &str) -> String {
    format!("app:{}:idempotency:{}", app_id, key)
}

async fn wait_for_message_id_ack(handler: &ConnectionHandler, cache_key: &str) -> Option<Value> {
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Ok(Some(cached)) = handler.cache_manager().get(cache_key).await
            && cached != "__processing__"
        {
            return Some(sonic_rs::from_str(&cached).unwrap_or_else(|_| json!({})));
        }
    }
    None
}

fn message_id_cache_key(app_id: &str, channel: &str, message_id: &str) -> String {
    format!("app:{app_id}:channel:{channel}:message_id:{message_id}")
}

fn mutation_op_cache_key(
    app_id: &str,
    channel: &str,
    message_serial: &str,
    action: ProtocolMessageAction,
    op_id: &str,
) -> String {
    format!(
        "app:{app_id}:channel:{channel}:message:{message_serial}:action:{}:op_id:{op_id}",
        action.as_str()
    )
}

fn idempotency_ttl(app: &App, handler: &ConnectionHandler) -> u64 {
    app.resolved_idempotency(&handler.server_options().idempotency)
        .ttl_seconds
}

fn ai_validation_app_error(error: AiHeaderValidationError) -> AppError {
    let status = match error.code {
        AI_ERROR_HEADER_TOO_LARGE => StatusCode::PAYLOAD_TOO_LARGE,
        _ => StatusCode::BAD_REQUEST,
    };

    AppError::AiTransport {
        status,
        code: error.code,
        name: error.name,
        message: error.message,
    }
}

fn ai_payload_too_large(message: impl Into<String>) -> AppError {
    AppError::AiTransport {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        code: AI_ERROR_PAYLOAD_TOO_LARGE,
        name: "payload_too_large",
        message: message.into(),
    }
}

fn mutable_not_permitted() -> AppError {
    AppError::AiTransport {
        status: StatusCode::FORBIDDEN,
        code: AI_ERROR_MUTABLE_NOT_PERMITTED,
        name: "mutable_not_permitted",
        message: "mutations not permitted on this channel".to_string(),
    }
}

fn message_data_bytes(data: Option<&MessageData>) -> Result<usize, AppError> {
    match data {
        None => Ok(0),
        Some(MessageData::String(value)) => Ok(value.len()),
        Some(other) => sonic_rs::to_vec(other)
            .map(|bytes| bytes.len())
            .map_err(AppError::from),
    }
}

fn ai_transport_applies(handler: &ConnectionHandler, channel: &str) -> bool {
    handler
        .server_options()
        .ai_transport
        .matches_channel(channel)
}

async fn validate_ai_append_caps(
    handler: &ConnectionHandler,
    current: &StoredVersionRecord,
    append_bytes: usize,
) -> Result<(), AppError> {
    if !ai_transport_applies(handler, &current.channel) {
        return Ok(());
    }

    let config = &handler.server_options().ai_transport;
    let current_bytes = message_data_bytes(current.message.data.as_ref())?;
    let next_bytes = current_bytes.saturating_add(append_bytes);
    if next_bytes > config.max_accumulated_message_bytes {
        return Err(ai_payload_too_large(format!(
            "accumulated message content exceeds {} bytes",
            config.max_accumulated_message_bytes
        )));
    }

    let page = handler
        .version_store()
        .get_versions(VersionStoreReadRequest {
            app_id: current.app_id.clone(),
            channel: current.channel.clone(),
            message_serial: current.message_serial().clone(),
            direction: VersionStoreDirection::OldestFirst,
            limit: config.max_appends_per_message.saturating_add(1),
            cursor: None,
        })
        .await?;
    let append_count = page
        .items
        .iter()
        .filter(|record| record.message.action == CoreMessageAction::Append)
        .count();
    if append_count >= config.max_appends_per_message {
        return Err(ai_payload_too_large(format!(
            "message append count exceeds {}",
            config.max_appends_per_message
        )));
    }

    Ok(())
}

fn validate_ai_update_caps(
    handler: &ConnectionHandler,
    channel: &str,
    data: Option<&MessageData>,
) -> Result<(), AppError> {
    if !ai_transport_applies(handler, channel) {
        return Ok(());
    }
    let max_bytes = handler
        .server_options()
        .ai_transport
        .max_accumulated_message_bytes;
    let bytes = message_data_bytes(data)?;
    if bytes > max_bytes {
        return Err(ai_payload_too_large(format!(
            "accumulated message content exceeds {max_bytes} bytes"
        )));
    }
    Ok(())
}

async fn validate_ai_http_publish(
    handler: &ConnectionHandler,
    app: &App,
    channel: &str,
    event_name: &str,
    message_id: Option<&str>,
    extras: Option<&MessageExtras>,
) -> Result<(), AppError> {
    let ai_channel = handler
        .server_options()
        .ai_transport
        .matches_channel(channel);
    let has_ai_headers = extras.and_then(|extras| extras.ai.as_ref()).is_some();
    if !ai_channel && !has_ai_headers && !is_ai_event(event_name) {
        return Ok(());
    }

    if !ai_channel {
        return Err(AppError::AiTransport {
            status: StatusCode::FORBIDDEN,
            code: AI_ERROR_EVENT_NOT_PERMITTED,
            name: "ai_event_not_permitted",
            message: "AI Transport is not enabled for this channel".to_string(),
        });
    }

    if is_ai_event(event_name)
        && let Some(metrics) = handler.metrics()
    {
        metrics.mark_ai_transport_validated(&app.id, event_name);
    }

    if is_ai_agent_publish_event(event_name) {
        // HTTP API requests are signed with the app secret before reaching this handler.
    }

    if let Some(message_id) = message_id {
        if message_id.is_empty() {
            return Err(AppError::AiTransport {
                status: StatusCode::BAD_REQUEST,
                code: AI_ERROR_INVALID_TRANSPORT_HEADER,
                name: "ai_invalid_transport_header",
                message: "message_id must not be empty".to_string(),
            });
        }
        if message_id.len() > AI_MESSAGE_ID_MAX_BYTES {
            return Err(AppError::AiTransport {
                status: StatusCode::BAD_REQUEST,
                code: AI_ERROR_HEADER_TOO_LARGE,
                name: "ai_header_too_large",
                message: format!("message_id exceeds {AI_MESSAGE_ID_MAX_BYTES} bytes"),
            });
        }
    }

    if let Some(extras) = extras {
        extras
            .validate_ai_headers()
            .map_err(ai_validation_app_error)?;
        let probe = PusherMessage {
            event: Some(event_name.to_string()),
            channel: Some(channel.to_string()),
            data: None,
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: message_id.map(ToOwned::to_owned),
            stream_id: None,
            serial: None,
            idempotency_key: None,
            extras: Some(extras.clone()),
            delta_sequence: None,
            delta_conflation_key: None,
        };
        sockudo_adapter::handler::validation::validate_ai_client_id_headers(
            &probe,
            None,
            AiPublishTrust::TrustedApp,
        )
        .map_err(ai_validation_app_error)?;
    }

    Ok(())
}

/// Merge per-app idempotency overrides with the global config.
/// Only fields explicitly set at the app level take precedence.
/// POST /apps/{app_id}/events
#[instrument(skip_all, fields(app_id = %app_id))]
#[allow(clippy::too_many_arguments)]
pub async fn events(
    Path(app_id): Path<String>,
    Query(_auth_q_params_struct): Query<EventQuery>,
    Extension(app): Extension<App>,
    #[cfg(feature = "push")] Extension(push_store): Extension<sockudo_push::DynPushStore>,
    #[cfg(feature = "push")] Extension(push_queue): Extension<sockudo_push::DynPushQueue>,
    State(handler): State<Arc<ConnectionHandler>>,
    headers: HeaderMap,
    _uri: Uri,
    RawQuery(_raw_query_str_option): RawQuery,
    Json(event_payload): Json<PusherApiMessage>,
) -> Result<impl IntoResponse, AppError> {
    let start_time_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as f64
        / 1_000_000.0;

    let incoming_request_size_bytes = sonic_rs::to_vec(&event_payload)?.len();

    let idempotency_config = app.resolved_idempotency(&handler.server_options().idempotency);
    let idempotency_key = resolve_idempotency_key(
        &event_payload.idempotency_key,
        &headers,
        &idempotency_config,
    )?;

    // Check for cached response or claim the idempotency key atomically
    if let Some(ref key) = idempotency_key {
        if let Some(metrics_arc) = handler.metrics() {
            metrics_arc.mark_idempotency_publish(&app_id);
        }
        let cache_key = idempotency_cache_key(&app_id, key);
        let ttl = idempotency_config.ttl_seconds;

        // First, check if there's already a completed response
        match handler.cache_manager().get(&cache_key).await {
            Ok(Some(cached)) if cached != "__processing__" => {
                if let Some(metrics_arc) = handler.metrics() {
                    metrics_arc.mark_idempotency_duplicate(&app_id);
                }
                debug!(idempotency_key = %key, "Returning cached idempotent response");
                let response_payload: Value =
                    sonic_rs::from_str(&cached).unwrap_or_else(|_| json!({ "ok": true }));
                let outgoing_response_size_bytes = sonic_rs::to_vec(&response_payload)?.len();
                record_api_metrics(
                    &handler,
                    &app_id,
                    incoming_request_size_bytes,
                    outgoing_response_size_bytes,
                )
                .await;
                return Ok((StatusCode::OK, Json(response_payload)));
            }
            Ok(Some(_)) => {
                // Another request is processing — wait briefly then return cached result
                for _ in 0..6 {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    if let Ok(Some(cached)) = handler.cache_manager().get(&cache_key).await
                        && cached != "__processing__"
                    {
                        if let Some(metrics_arc) = handler.metrics() {
                            metrics_arc.mark_idempotency_duplicate(&app_id);
                        }
                        let response_payload: Value =
                            sonic_rs::from_str(&cached).unwrap_or_else(|_| json!({ "ok": true }));
                        let outgoing_response_size_bytes =
                            sonic_rs::to_vec(&response_payload)?.len();
                        record_api_metrics(
                            &handler,
                            &app_id,
                            incoming_request_size_bytes,
                            outgoing_response_size_bytes,
                        )
                        .await;
                        return Ok((StatusCode::OK, Json(response_payload)));
                    }
                }
                // Timeout waiting — proceed without dedup to avoid blocking forever
                debug!(idempotency_key = %key, "Timeout waiting for concurrent idempotent request, proceeding");
            }
            Ok(None) => {
                // Try to claim this key atomically
                match handler
                    .cache_manager()
                    .set_if_not_exists(&cache_key, "__processing__", ttl)
                    .await
                {
                    Ok(false) => {
                        // Another request claimed it — wait for their result
                        for _ in 0..6 {
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            if let Ok(Some(cached)) = handler.cache_manager().get(&cache_key).await
                                && cached != "__processing__"
                            {
                                if let Some(metrics_arc) = handler.metrics() {
                                    metrics_arc.mark_idempotency_duplicate(&app_id);
                                }
                                let response_payload: Value = sonic_rs::from_str(&cached)
                                    .unwrap_or_else(|_| json!({ "ok": true }));
                                let outgoing_response_size_bytes =
                                    sonic_rs::to_vec(&response_payload)?.len();
                                record_api_metrics(
                                    &handler,
                                    &app_id,
                                    incoming_request_size_bytes,
                                    outgoing_response_size_bytes,
                                )
                                .await;
                                return Ok((StatusCode::OK, Json(response_payload)));
                            }
                        }
                        debug!(idempotency_key = %key, "Timeout waiting for concurrent idempotent request, proceeding");
                    }
                    Ok(true) => {
                        // We claimed it — proceed to process below
                    }
                    Err(e) => {
                        warn!(idempotency_key = %key, error = %e, "Failed to claim idempotency key, proceeding without dedup");
                    }
                }
            }
            Err(e) => {
                warn!(idempotency_key = %key, error = %e, "Failed to check idempotency cache, proceeding without dedup");
            }
        }
    }

    let need_channel_info = event_payload.info.is_some()
        || event_payload.message_id.is_some()
        || event_payload.name.as_deref().is_some_and(is_ai_event);

    #[cfg(feature = "push")]
    let push_rule_requests = {
        let rules = &handler.server_options().push_rules;
        if rules.is_empty() {
            Vec::new()
        } else {
            let channels = push_rule_target_channels(&event_payload);
            crate::push_http::build_channel_push_rule_requests(
                &app_id,
                event_payload.name.as_deref(),
                event_payload.data.as_ref(),
                &channels,
                rules,
            )?
        }
    };

    #[cfg(feature = "push")]
    if let Some(extras_push) = event_payload
        .extras
        .as_ref()
        .and_then(|extras| extras.push.as_ref())
    {
        if let Some(channel) = event_payload.channel.as_deref() {
            crate::push_http::enqueue_v2_channel_push_from_extras(
                &app_id,
                channel,
                extras_push,
                &push_store,
                &push_queue,
            )
            .await?;
        }
        if let Some(channels) = event_payload.channels.as_ref() {
            for channel in channels {
                crate::push_http::enqueue_v2_channel_push_from_extras(
                    &app_id,
                    channel,
                    extras_push,
                    &push_store,
                    &push_queue,
                )
                .await?;
            }
        }
    }

    let channels_info_map = process_single_event_parallel(
        &handler,
        &app,
        event_payload,
        need_channel_info,
        Some(start_time_ms),
        idempotency_key.clone(),
        MessageIdIdempotencyContext {
            enabled: idempotency_config.enabled,
            ttl_seconds: idempotency_config.ttl_seconds,
        },
    )
    .await?;

    #[cfg(feature = "push")]
    crate::push_http::spawn_channel_push_rule_requests(
        app_id.clone(),
        push_rule_requests,
        push_store.clone(),
        push_queue.clone(),
    );

    let response_payload = if need_channel_info && !channels_info_map.is_empty() {
        json!({
            "channels": channels_info_map
        })
    } else {
        json!({ "ok": true })
    };

    // Store final response in idempotency cache (overwrites __processing__ sentinel)
    if let Some(ref key) = idempotency_key {
        let cache_key = idempotency_cache_key(&app_id, key);
        let ttl = idempotency_config.ttl_seconds;
        if let Ok(serialized) = sonic_rs::to_string(&response_payload)
            && let Err(e) = handler
                .cache_manager()
                .set(&cache_key, &serialized, ttl)
                .await
        {
            warn!(idempotency_key = %key, error = %e, "Failed to store idempotency response in cache");
        }
    }

    let outgoing_response_size_bytes = sonic_rs::to_vec(&response_payload)?.len();
    record_api_metrics(
        &handler,
        &app_id,
        incoming_request_size_bytes,
        outgoing_response_size_bytes,
    )
    .await;

    Ok((StatusCode::OK, Json(response_payload)))
}

/// POST /apps/{app_id}/batch_events
#[instrument(skip_all, fields(app_id = %app_id, batch_len = field::Empty))]
#[allow(clippy::too_many_arguments)]
pub async fn batch_events(
    Path(app_id): Path<String>,
    Query(_auth_q_params_struct): Query<EventQuery>,
    Extension(app_config): Extension<App>,
    #[cfg(feature = "push")] Extension(push_store): Extension<sockudo_push::DynPushStore>,
    #[cfg(feature = "push")] Extension(push_queue): Extension<sockudo_push::DynPushQueue>,
    State(handler): State<Arc<ConnectionHandler>>,
    headers: HeaderMap,
    _uri: Uri,
    RawQuery(_raw_query_str_option): RawQuery,
    Json(batch_message_payload): Json<BatchPusherApiMessage>,
) -> Result<impl IntoResponse, AppError> {
    let start_time_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as f64
        / 1_000_000.0;

    let body_bytes = sonic_rs::to_vec(&batch_message_payload)?;

    // Batch-level idempotency: check X-Idempotency-Key header
    let idempotency_config = app_config.resolved_idempotency(&handler.server_options().idempotency);
    let idempotency_key = resolve_idempotency_key(&None, &headers, &idempotency_config)?;

    if let Some(ref key) = idempotency_key {
        if let Some(metrics_arc) = handler.metrics() {
            metrics_arc.mark_idempotency_publish(&app_id);
        }
        let cache_key = idempotency_cache_key(&app_id, key);
        let ttl = idempotency_config.ttl_seconds;
        match handler.cache_manager().get(&cache_key).await {
            Ok(Some(cached)) if cached != "__processing__" => {
                if let Some(metrics_arc) = handler.metrics() {
                    metrics_arc.mark_idempotency_duplicate(&app_id);
                }
                debug!(idempotency_key = %key, "Returning cached idempotent batch response");
                let response_payload: Value =
                    sonic_rs::from_str(&cached).unwrap_or_else(|_| json!({}));
                let outgoing_response_size_bytes = sonic_rs::to_vec(&response_payload)?.len();
                record_api_metrics(
                    &handler,
                    &app_id,
                    body_bytes.len(),
                    outgoing_response_size_bytes,
                )
                .await;
                return Ok((StatusCode::OK, Json(response_payload)));
            }
            Ok(Some(_)) | Ok(None) => {
                // Claim key atomically or wait for concurrent request
                if let Ok(false) = handler
                    .cache_manager()
                    .set_if_not_exists(&cache_key, "__processing__", ttl)
                    .await
                {
                    for _ in 0..6 {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        if let Ok(Some(cached)) = handler.cache_manager().get(&cache_key).await
                            && cached != "__processing__"
                        {
                            if let Some(metrics_arc) = handler.metrics() {
                                metrics_arc.mark_idempotency_duplicate(&app_id);
                            }
                            let response_payload: Value =
                                sonic_rs::from_str(&cached).unwrap_or_else(|_| json!({}));
                            let outgoing_response_size_bytes =
                                sonic_rs::to_vec(&response_payload)?.len();
                            record_api_metrics(
                                &handler,
                                &app_id,
                                body_bytes.len(),
                                outgoing_response_size_bytes,
                            )
                            .await;
                            return Ok((StatusCode::OK, Json(response_payload)));
                        }
                    }
                }
            }
            Err(e) => {
                warn!(idempotency_key = %key, error = %e, "Failed to check batch idempotency cache");
            }
        }
    }

    let batch_events_vec = batch_message_payload.batch;
    let batch_len = batch_events_vec.len();
    tracing::Span::current().record("batch_len", batch_len);
    debug!("Received batch events request with {} events", batch_len);

    for (i, event) in batch_events_vec.iter().enumerate().take(3) {
        debug!("Batch event #{}: tags={:?}", i, event.tags);
    }

    if let Some(max_batch) = app_config.event_batch_size_limit()
        && batch_len > max_batch as usize
    {
        return Err(AppError::LimitExceeded(format!(
            "Batch size ({batch_len}) exceeds limit ({max_batch})"
        )));
    }

    let incoming_request_size_bytes = body_bytes.len();
    let mut any_message_requests_info = false;

    for single_event_message in &batch_events_vec {
        if single_event_message.info.is_some()
            || single_event_message.message_id.is_some()
            || single_event_message
                .name
                .as_deref()
                .is_some_and(is_ai_event)
        {
            any_message_requests_info = true;
            break;
        }
    }

    let mut processed_event_data = Vec::with_capacity(batch_len);

    for single_event_message in batch_events_vec {
        // Per-event idempotency: skip events whose idempotency key has already been seen
        if let Some(ref evt_key) = single_event_message.idempotency_key
            && idempotency_config.enabled
            && !evt_key.is_empty()
        {
            if let Some(metrics_arc) = handler.metrics() {
                metrics_arc.mark_idempotency_publish(&app_id);
            }
            let evt_cache_key = idempotency_cache_key(&app_id, evt_key);
            if let Ok(Some(_)) = handler.cache_manager().get(&evt_cache_key).await {
                if let Some(metrics_arc) = handler.metrics() {
                    metrics_arc.mark_idempotency_duplicate(&app_id);
                }
                debug!(idempotency_key = %evt_key, "Skipping duplicate batch event");
                processed_event_data.push((single_event_message, HashMap::new()));
                continue;
            }
        }

        let should_collect_info_for_this_event = single_event_message.info.is_some()
            || single_event_message.message_id.is_some()
            || single_event_message
                .name
                .as_deref()
                .is_some_and(is_ai_event);

        #[cfg(feature = "push")]
        let push_rule_requests = {
            let rules = &handler.server_options().push_rules;
            if rules.is_empty() {
                Vec::new()
            } else {
                let channels = push_rule_target_channels(&single_event_message);
                crate::push_http::build_channel_push_rule_requests(
                    &app_id,
                    single_event_message.name.as_deref(),
                    single_event_message.data.as_ref(),
                    &channels,
                    rules,
                )?
            }
        };

        #[cfg(feature = "push")]
        if let Some(extras_push) = single_event_message
            .extras
            .as_ref()
            .and_then(|extras| extras.push.as_ref())
        {
            if let Some(channel) = single_event_message.channel.as_deref() {
                crate::push_http::enqueue_v2_channel_push_from_extras(
                    &app_id,
                    channel,
                    extras_push,
                    &push_store,
                    &push_queue,
                )
                .await?;
            }
            if let Some(channels) = single_event_message.channels.as_ref() {
                for channel in channels {
                    crate::push_http::enqueue_v2_channel_push_from_extras(
                        &app_id,
                        channel,
                        extras_push,
                        &push_store,
                        &push_queue,
                    )
                    .await?;
                }
            }
        }

        let channel_info_map = process_single_event_parallel(
            &handler,
            &app_config,
            single_event_message.clone(),
            should_collect_info_for_this_event,
            Some(start_time_ms),
            single_event_message.idempotency_key.clone(),
            MessageIdIdempotencyContext {
                enabled: idempotency_config.enabled,
                ttl_seconds: idempotency_config.ttl_seconds,
            },
        )
        .await?;

        #[cfg(feature = "push")]
        crate::push_http::spawn_channel_push_rule_requests(
            app_id.clone(),
            push_rule_requests,
            push_store.clone(),
            push_queue.clone(),
        );

        // Store per-event idempotency key
        if let Some(ref evt_key) = single_event_message.idempotency_key
            && idempotency_config.enabled
            && !evt_key.is_empty()
        {
            let evt_cache_key = idempotency_cache_key(&app_id, evt_key);
            let _ = handler
                .cache_manager()
                .set(&evt_cache_key, "1", idempotency_config.ttl_seconds)
                .await;
        }

        processed_event_data.push((single_event_message, channel_info_map));
    }

    let mut batch_response_info_vec = Vec::with_capacity(batch_len);

    if any_message_requests_info {
        for (original_msg, channel_info_map_for_event) in processed_event_data {
            if let Some(main_channel_for_event) = original_msg
                .channel
                .as_ref()
                .or_else(|| original_msg.channels.as_ref().and_then(|chs| chs.first()))
            {
                batch_response_info_vec.push(
                    channel_info_map_for_event
                        .get(main_channel_for_event)
                        .cloned()
                        .unwrap_or_else(|| json!({})),
                );
            } else {
                batch_response_info_vec.push(json!({}));
            }
        }
    }

    let final_response_payload = if any_message_requests_info {
        json!({ "batch": batch_response_info_vec })
    } else {
        json!({})
    };

    // Store batch response in idempotency cache
    if let Some(ref key) = idempotency_key {
        let cache_key = idempotency_cache_key(&app_id, key);
        let ttl = idempotency_config.ttl_seconds;
        if let Ok(serialized) = sonic_rs::to_string(&final_response_payload)
            && let Err(e) = handler
                .cache_manager()
                .set(&cache_key, &serialized, ttl)
                .await
        {
            warn!(idempotency_key = %key, error = %e, "Failed to store batch idempotency response in cache");
        }
    }

    let outgoing_response_size_bytes_vec = sonic_rs::to_vec(&final_response_payload)?;
    record_api_metrics(
        &handler,
        &app_id,
        incoming_request_size_bytes,
        outgoing_response_size_bytes_vec.len(),
    )
    .await;
    debug!("{}", "Batch events processed successfully");
    Ok((StatusCode::OK, Json(final_response_payload)))
}

/// GET /apps/{app_id}/channels/{channel_name}/messages/{message_serial}
#[instrument(skip(handler), fields(app_id = %path.app_id, channel = %path.channel_name, message_serial = %path.message_serial))]
pub async fn channel_message(
    Path(path): Path<VersionMutationPath>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    validate_channel_name(&app, &path.channel_name).await?;
    require_versioned_messages_enabled(&handler, &path.channel_name)?;

    let message_serial = parse_message_serial(&path.message_serial)?;
    let item = handler
        .version_store()
        .get_latest(&path.app_id, &path.channel_name, &message_serial)
        .await?;
    let item = match item {
        Some(item) => {
            if let Some(metrics) = handler.metrics() {
                metrics.mark_versioned_message_retrieval(&path.app_id, "latest", "hit");
            }
            item
        }
        None => {
            if let Some(metrics) = handler.metrics() {
                metrics.mark_versioned_message_retrieval(&path.app_id, "latest", "miss");
            }
            return Err(AppError::NotFound(format!(
                "Message '{}' was not found in channel '{}'",
                path.message_serial, path.channel_name
            )));
        }
    };

    let payload = GetMessageResponse {
        channel: path.channel_name,
        item: build_versioned_realtime_message(&item),
    };
    let response_json_bytes = sonic_rs::to_vec(&payload)?;
    record_api_metrics(&handler, &path.app_id, 0, response_json_bytes.len()).await;
    Ok((StatusCode::OK, Json(payload)))
}

/// GET /apps/{app_id}/channels/{channel_name}/messages/{message_serial}/versions
#[instrument(skip(handler), fields(app_id = %path.app_id, channel = %path.channel_name, message_serial = %path.message_serial))]
pub async fn channel_message_versions(
    Path(path): Path<VersionMutationPath>,
    Query(query_params): Query<MessageVersionsQuery>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    validate_channel_name(&app, &path.channel_name).await?;
    require_versioned_messages_enabled(&handler, &path.channel_name)?;
    query_params.validate().map_err(AppError::InvalidInput)?;

    let message_serial = parse_message_serial(&path.message_serial)?;
    if handler
        .version_store()
        .get_latest(&path.app_id, &path.channel_name, &message_serial)
        .await?
        .is_none()
    {
        if let Some(metrics) = handler.metrics() {
            metrics.mark_versioned_message_retrieval(&path.app_id, "versions", "miss");
        }
        return Err(AppError::NotFound(format!(
            "Message '{}' was not found in channel '{}'",
            path.message_serial, path.channel_name
        )));
    }
    if let Some(metrics) = handler.metrics() {
        metrics.mark_versioned_message_retrieval(&path.app_id, "versions", "hit");
    }

    let requested_direction = query_params
        .direction
        .unwrap_or(VersionDirection::NewestFirst);
    let direction = parse_version_direction(Some(requested_direction));
    let limit = query_params
        .limit
        .unwrap_or(handler.server_options().versioned_messages.max_page_size)
        .min(handler.server_options().versioned_messages.max_page_size);
    if limit == 0 {
        return Err(AppError::InvalidInput(
            "Version-history limit must be greater than 0".to_string(),
        ));
    }

    let cursor = match query_params.cursor.as_deref() {
        Some(raw) => Some(sockudo_core::version_store::VersionStoreCursor {
            version: 1,
            version_serial: sockudo_core::versioned_messages::VersionSerial::new(raw.to_string())?,
            direction,
        }),
        None => None,
    };

    let page = handler
        .version_store()
        .get_versions(VersionStoreReadRequest {
            app_id: path.app_id.clone(),
            channel: path.channel_name.clone(),
            message_serial,
            direction,
            limit,
            cursor,
        })
        .await?;

    let payload = ListMessageVersionsResponse {
        channel: path.channel_name,
        direction: requested_direction,
        limit,
        has_more: page.has_more,
        next_cursor: page
            .next_cursor
            .map(|cursor| cursor.version_serial.as_str().to_string()),
        items: page
            .items
            .iter()
            .map(build_versioned_realtime_message)
            .collect(),
    };
    let response_json_bytes = sonic_rs::to_vec(&payload)?;
    record_api_metrics(&handler, &path.app_id, 0, response_json_bytes.len()).await;
    Ok((StatusCode::OK, Json(payload)))
}

/// POST /apps/{app_id}/channels/{channel_name}/messages/{message_serial}/update
#[instrument(skip(handler, request), fields(app_id = %path.app_id, channel = %path.channel_name, message_serial = %path.message_serial))]
pub async fn update_message(
    Path(path): Path<VersionMutationPath>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(request): Json<UpdateMessageRequest>,
) -> Result<AxumResponse, AppError> {
    validate_channel_name(&app, &path.channel_name).await?;
    require_versioned_messages_enabled(&handler, &path.channel_name)?;
    let history_policy =
        app.resolved_history(&path.channel_name, &handler.server_options().history);
    if !history_policy.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Durable history is disabled by policy for channel '{}'",
            path.channel_name
        )));
    }
    let message_serial = parse_message_serial(&path.message_serial)?;
    request.validate().map_err(AppError::InvalidInput)?;

    let current = handler
        .version_store()
        .get_latest(&path.app_id, &path.channel_name, &message_serial)
        .await?;
    let current = match current {
        Some(current) => current,
        None => {
            if let Some(metrics) = handler.metrics() {
                metrics.mark_versioned_message_mutation(
                    &path.app_id,
                    "message.update",
                    "not_found",
                );
            }
            return Err(AppError::NotFound(format!(
                "Message '{}' was not found in channel '{}'",
                path.message_serial, path.channel_name
            )));
        }
    };

    let actor_client_id = resolve_mutation_actor_identity(
        &handler,
        &path.app_id,
        &path.channel_name,
        MutationKind::Update,
        current.original_client_id.as_deref(),
        request.client_id.as_deref(),
        request.socket_id.as_deref(),
    )
    .await?;
    if let Some(op_id) = request.op_id.as_deref() {
        if op_id.is_empty() || op_id.len() > AI_MESSAGE_ID_MAX_BYTES {
            return Err(AppError::InvalidInput(format!(
                "op_id must be 1..={AI_MESSAGE_ID_MAX_BYTES} bytes"
            )));
        }
        let cache_key = mutation_op_cache_key(
            &path.app_id,
            &path.channel_name,
            &path.message_serial,
            ProtocolMessageAction::Update,
            op_id,
        );
        if handler.cache_manager().get(&cache_key).await?.is_some() {
            let payload = MutationResponse {
                channel: path.channel_name,
                message_serial: path.message_serial,
                action: ProtocolMessageAction::Update,
                accepted: true,
                version_serial: Some(current.version_serial().as_str().to_string()),
                history_serial: Some(current.history_serial()),
                delivery_serial: Some(current.delivery_serial()),
                status: "duplicate".to_string(),
            };
            return Ok((StatusCode::OK, Json(payload)).into_response());
        }
    }
    if let Some(metrics) = handler.metrics() {
        metrics.mark_versioned_message_mutation(&path.app_id, "message.update", "applied");
    }
    if request.data.is_some() {
        validate_ai_update_caps(&handler, &path.channel_name, request.data.as_ref())?;
    }

    let reservation = handler
        .version_store()
        .reserve_delivery_position_after(
            &path.app_id,
            &path.channel_name,
            current.delivery_serial(),
        )
        .await?;
    let updated_message = current
        .message
        .apply_mutation(
            CoreMessageAction::Update,
            build_mutation_version_metadata(
                &handler,
                actor_client_id,
                request.description.clone(),
                request.metadata.clone(),
            )?,
            reservation.delivery_serial,
            update_delta_from_request(&request),
        )
        .map_err(AppError::from)?;
    let updated = StoredVersionRecord {
        app_id: current.app_id.clone(),
        channel: current.channel.clone(),
        original_client_id: current.original_client_id.clone(),
        message: updated_message,
    };
    handler
        .version_store()
        .append_version(updated.clone())
        .await?;
    #[cfg(feature = "ai-transport")]
    handler
        .record_ai_stream_activity(&path.app_id, &path.channel_name, &updated)
        .await?;

    let runtime_message =
        handler.build_runtime_message_from_record(&updated, Some(reservation.stream_id));
    handler
        .broadcast_to_channel_force_full(&app, &path.channel_name, runtime_message, None, None)
        .await?;
    info!(
        app_id = %path.app_id,
        channel = %path.channel_name,
        message_serial = %path.message_serial,
        version_serial = %updated.version_serial().as_str(),
        history_serial = updated.history_serial(),
        delivery_serial = updated.delivery_serial(),
        actor_client_id = ?updated.message.version.client_id,
        "Applied versioned message.update"
    );

    let payload = MutationResponse {
        channel: path.channel_name,
        message_serial: path.message_serial,
        action: ProtocolMessageAction::Update,
        accepted: true,
        version_serial: Some(updated.version_serial().as_str().to_string()),
        history_serial: Some(updated.history_serial()),
        delivery_serial: Some(updated.delivery_serial()),
        status: "applied".to_string(),
    };
    if let Some(op_id) = request.op_id.as_deref() {
        let cache_key = mutation_op_cache_key(
            &path.app_id,
            &payload.channel,
            &payload.message_serial,
            ProtocolMessageAction::Update,
            op_id,
        );
        let _ = handler
            .cache_manager()
            .set(&cache_key, "1", idempotency_ttl(&app, &handler))
            .await;
    }
    Ok((StatusCode::OK, Json(payload)).into_response())
}

/// POST /apps/{app_id}/channels/{channel_name}/messages/{message_serial}/delete
#[instrument(skip(handler, request), fields(app_id = %path.app_id, channel = %path.channel_name, message_serial = %path.message_serial))]
pub async fn delete_message(
    Path(path): Path<VersionMutationPath>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(request): Json<DeleteMessageRequest>,
) -> Result<AxumResponse, AppError> {
    validate_channel_name(&app, &path.channel_name).await?;
    require_versioned_messages_enabled(&handler, &path.channel_name)?;
    let history_policy =
        app.resolved_history(&path.channel_name, &handler.server_options().history);
    if !history_policy.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Durable history is disabled by policy for channel '{}'",
            path.channel_name
        )));
    }
    let message_serial = parse_message_serial(&path.message_serial)?;
    request.validate().map_err(AppError::InvalidInput)?;

    let current = handler
        .version_store()
        .get_latest(&path.app_id, &path.channel_name, &message_serial)
        .await?;
    let current = match current {
        Some(current) => current,
        None => {
            if let Some(metrics) = handler.metrics() {
                metrics.mark_versioned_message_mutation(
                    &path.app_id,
                    "message.delete",
                    "not_found",
                );
            }
            return Err(AppError::NotFound(format!(
                "Message '{}' was not found in channel '{}'",
                path.message_serial, path.channel_name
            )));
        }
    };

    let actor_client_id = resolve_mutation_actor_identity(
        &handler,
        &path.app_id,
        &path.channel_name,
        MutationKind::Delete,
        current.original_client_id.as_deref(),
        request.client_id.as_deref(),
        request.socket_id.as_deref(),
    )
    .await?;
    if let Some(op_id) = request.op_id.as_deref() {
        if op_id.is_empty() || op_id.len() > AI_MESSAGE_ID_MAX_BYTES {
            return Err(AppError::InvalidInput(format!(
                "op_id must be 1..={AI_MESSAGE_ID_MAX_BYTES} bytes"
            )));
        }
        let cache_key = mutation_op_cache_key(
            &path.app_id,
            &path.channel_name,
            &path.message_serial,
            ProtocolMessageAction::Delete,
            op_id,
        );
        if handler.cache_manager().get(&cache_key).await?.is_some() {
            let payload = MutationResponse {
                channel: path.channel_name,
                message_serial: path.message_serial,
                action: ProtocolMessageAction::Delete,
                accepted: true,
                version_serial: Some(current.version_serial().as_str().to_string()),
                history_serial: Some(current.history_serial()),
                delivery_serial: Some(current.delivery_serial()),
                status: "duplicate".to_string(),
            };
            return Ok((StatusCode::OK, Json(payload)).into_response());
        }
    }
    if let Some(metrics) = handler.metrics() {
        metrics.mark_versioned_message_mutation(&path.app_id, "message.delete", "applied");
    }

    let reservation = handler
        .version_store()
        .reserve_delivery_position_after(
            &path.app_id,
            &path.channel_name,
            current.delivery_serial(),
        )
        .await?;
    let deleted_message = current
        .message
        .apply_mutation(
            CoreMessageAction::Delete,
            build_mutation_version_metadata(
                &handler,
                actor_client_id,
                request.description.clone(),
                request.metadata.clone(),
            )?,
            reservation.delivery_serial,
            delete_delta_from_request(&request),
        )
        .map_err(AppError::from)?;
    let deleted = StoredVersionRecord {
        app_id: current.app_id.clone(),
        channel: current.channel.clone(),
        original_client_id: current.original_client_id.clone(),
        message: deleted_message,
    };
    handler
        .version_store()
        .append_version(deleted.clone())
        .await?;
    #[cfg(feature = "ai-transport")]
    handler
        .record_ai_stream_activity(&path.app_id, &path.channel_name, &deleted)
        .await?;

    let runtime_message =
        handler.build_runtime_message_from_record(&deleted, Some(reservation.stream_id));
    handler
        .broadcast_to_channel_force_full(&app, &path.channel_name, runtime_message, None, None)
        .await?;
    info!(
        app_id = %path.app_id,
        channel = %path.channel_name,
        message_serial = %path.message_serial,
        version_serial = %deleted.version_serial().as_str(),
        history_serial = deleted.history_serial(),
        delivery_serial = deleted.delivery_serial(),
        actor_client_id = ?deleted.message.version.client_id,
        "Applied versioned message.delete"
    );

    let payload = MutationResponse {
        channel: path.channel_name,
        message_serial: path.message_serial,
        action: ProtocolMessageAction::Delete,
        accepted: true,
        version_serial: Some(deleted.version_serial().as_str().to_string()),
        history_serial: Some(deleted.history_serial()),
        delivery_serial: Some(deleted.delivery_serial()),
        status: "applied".to_string(),
    };
    if let Some(op_id) = request.op_id.as_deref() {
        let cache_key = mutation_op_cache_key(
            &path.app_id,
            &payload.channel,
            &payload.message_serial,
            ProtocolMessageAction::Delete,
            op_id,
        );
        let _ = handler
            .cache_manager()
            .set(&cache_key, "1", idempotency_ttl(&app, &handler))
            .await;
    }
    Ok((StatusCode::OK, Json(payload)).into_response())
}

/// POST /apps/{app_id}/channels/{channel_name}/messages/{message_serial}/append
#[instrument(skip(handler, request), fields(app_id = %path.app_id, channel = %path.channel_name, message_serial = %path.message_serial))]
pub async fn append_message(
    Path(path): Path<VersionMutationPath>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(request): Json<AppendMessageRequest>,
) -> Result<AxumResponse, AppError> {
    validate_channel_name(&app, &path.channel_name).await?;
    require_versioned_messages_enabled(&handler, &path.channel_name)?;
    let history_policy =
        app.resolved_history(&path.channel_name, &handler.server_options().history);
    if !history_policy.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Durable history is disabled by policy for channel '{}'",
            path.channel_name
        )));
    }
    let message_serial = parse_message_serial(&path.message_serial)?;
    request.validate().map_err(AppError::InvalidInput)?;

    let current = handler
        .version_store()
        .get_latest(&path.app_id, &path.channel_name, &message_serial)
        .await?;
    let current = match current {
        Some(current) => current,
        None => {
            if let Some(metrics) = handler.metrics() {
                metrics.mark_versioned_message_mutation(
                    &path.app_id,
                    "message.append",
                    "not_found",
                );
            }
            return Err(AppError::NotFound(format!(
                "Message '{}' was not found in channel '{}'",
                path.message_serial, path.channel_name
            )));
        }
    };

    let actor_client_id = resolve_mutation_actor_identity(
        &handler,
        &path.app_id,
        &path.channel_name,
        MutationKind::Append,
        current.original_client_id.as_deref(),
        request.client_id.as_deref(),
        request.socket_id.as_deref(),
    )
    .await?;
    if let Some(op_id) = request.op_id.as_deref() {
        if op_id.is_empty() || op_id.len() > AI_MESSAGE_ID_MAX_BYTES {
            return Err(AppError::InvalidInput(format!(
                "op_id must be 1..={AI_MESSAGE_ID_MAX_BYTES} bytes"
            )));
        }
        let cache_key = mutation_op_cache_key(
            &path.app_id,
            &path.channel_name,
            &path.message_serial,
            ProtocolMessageAction::Append,
            op_id,
        );
        if handler.cache_manager().get(&cache_key).await?.is_some() {
            let payload = MutationResponse {
                channel: path.channel_name,
                message_serial: path.message_serial,
                action: ProtocolMessageAction::Append,
                accepted: true,
                version_serial: Some(current.version_serial().as_str().to_string()),
                history_serial: Some(current.history_serial()),
                delivery_serial: Some(current.delivery_serial()),
                status: "duplicate".to_string(),
            };
            return Ok((StatusCode::OK, Json(payload)).into_response());
        }
    }
    if let Some(metrics) = handler.metrics() {
        metrics.mark_versioned_message_mutation(&path.app_id, "message.append", "applied");
    }
    validate_ai_append_caps(&handler, &current, request.data.len()).await?;

    let reservation = handler
        .version_store()
        .reserve_delivery_position_after(
            &path.app_id,
            &path.channel_name,
            current.delivery_serial(),
        )
        .await?;
    let appended_message = current
        .message
        .apply_append(
            build_mutation_version_metadata(
                &handler,
                actor_client_id,
                request.description.clone(),
                request.metadata.clone(),
            )?,
            reservation.delivery_serial,
            MessageAppend {
                data_fragment: request.data.clone(),
                extras: request.extras.clone(),
            },
        )
        .map_err(AppError::from)?;
    let appended = StoredVersionRecord {
        app_id: current.app_id.clone(),
        channel: current.channel.clone(),
        original_client_id: current.original_client_id.clone(),
        message: appended_message,
    };
    handler
        .version_store()
        .append_version(appended.clone())
        .await?;
    #[cfg(feature = "ai-transport")]
    handler
        .record_ai_stream_activity(&path.app_id, &path.channel_name, &appended)
        .await?;

    let runtime_message =
        handler.build_runtime_message_from_record(&appended, Some(reservation.stream_id));
    handler
        .broadcast_to_channel_force_full(&app, &path.channel_name, runtime_message, None, None)
        .await?;
    info!(
        app_id = %path.app_id,
        channel = %path.channel_name,
        message_serial = %path.message_serial,
        version_serial = %appended.version_serial().as_str(),
        history_serial = appended.history_serial(),
        delivery_serial = appended.delivery_serial(),
        actor_client_id = ?appended.message.version.client_id,
        "Applied versioned message.append"
    );

    let payload = MutationResponse {
        channel: path.channel_name,
        message_serial: path.message_serial,
        action: ProtocolMessageAction::Append,
        accepted: true,
        version_serial: Some(appended.version_serial().as_str().to_string()),
        history_serial: Some(appended.history_serial()),
        delivery_serial: Some(appended.delivery_serial()),
        status: "applied".to_string(),
    };
    if let Some(op_id) = request.op_id.as_deref() {
        let cache_key = mutation_op_cache_key(
            &path.app_id,
            &payload.channel,
            &payload.message_serial,
            ProtocolMessageAction::Append,
            op_id,
        );
        let _ = handler
            .cache_manager()
            .set(&cache_key, "1", idempotency_ttl(&app, &handler))
            .await;
    }
    Ok((StatusCode::OK, Json(payload)).into_response())
}

/// POST /apps/{app_id}/channels/{channel_name}/messages/{message_serial}/annotations
#[instrument(skip(handler, request), fields(app_id = %path.app_id, channel = %path.channel_name, message_serial = %path.message_serial))]
pub async fn publish_annotation(
    Path(path): Path<VersionMutationPath>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(request): Json<PublishAnnotationRequest>,
) -> Result<AxumResponse, AppError> {
    validate_channel_name(&app, &path.channel_name).await?;
    require_versioned_messages_enabled(&handler, &path.channel_name)?;
    require_annotations_enabled(&handler, &app, &path.channel_name)?;
    let history_policy =
        app.resolved_history(&path.channel_name, &handler.server_options().history);
    if !history_policy.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Durable history is disabled by policy for channel '{}'",
            path.channel_name
        )));
    }
    request.validate().map_err(AppError::InvalidInput)?;

    let message_serial = parse_message_serial(&path.message_serial)?;
    if handler
        .version_store()
        .get_latest(&path.app_id, &path.channel_name, &message_serial)
        .await?
        .is_none()
    {
        return Err(AppError::NotFound(format!(
            "Message '{}' was not found in channel '{}'",
            path.message_serial, path.channel_name
        )));
    }

    let annotation_type = parse_annotation_type(&request.annotation_type)?;
    let actor_client_id = resolve_annotation_publish_actor(
        &handler,
        &path.app_id,
        &path.channel_name,
        request.client_id.as_deref(),
        request.socket_id.as_deref(),
    )
    .await?;
    let result = handler
        .publish_annotation_runtime(PublishAnnotationRuntimeRequest {
            app,
            channel: path.channel_name,
            message_serial,
            annotation_type,
            name: request.name,
            client_id: actor_client_id,
            count: request.count,
            data: request.data,
            encoding: request.encoding,
        })
        .await?;

    Ok((
        StatusCode::OK,
        Json(PublishAnnotationResponse {
            annotation_serial: result.annotation_serial.as_str().to_string(),
        }),
    )
        .into_response())
}

/// DELETE /apps/{app_id}/channels/{channel_name}/messages/{message_serial}/annotations/{annotation_serial}
#[instrument(skip(handler), fields(app_id = %path.app_id, channel = %path.channel_name, message_serial = %path.message_serial, annotation_serial = %path.annotation_serial))]
pub async fn delete_annotation(
    Path(path): Path<AnnotationMutationPath>,
    Query(query): Query<HashMap<String, String>>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<AxumResponse, AppError> {
    validate_channel_name(&app, &path.channel_name).await?;
    require_versioned_messages_enabled(&handler, &path.channel_name)?;
    require_annotations_enabled(&handler, &app, &path.channel_name)?;
    let message_serial = parse_message_serial(&path.message_serial)?;
    let target_serial = parse_annotation_serial(&path.annotation_serial)?;
    let target = handler
        .annotation_store()
        .get_event_by_serial(AnnotationEventLookupRequest {
            app_id: path.app_id.clone(),
            channel_id: path.channel_name.clone(),
            annotation_serial: target_serial.clone(),
        })
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!(
                "Annotation '{}' was not found in channel '{}'",
                path.annotation_serial, path.channel_name
            ))
        })?;

    if target.message_serial() != &message_serial {
        return Err(AppError::NotFound(format!(
            "Annotation '{}' does not target message '{}'",
            path.annotation_serial, path.message_serial
        )));
    }
    if target.annotation.action != AnnotationAction::Create {
        return Err(AppError::InvalidInput(
            "Only annotation.create events can be deleted".to_string(),
        ));
    }

    authorize_annotation_delete(
        &handler,
        &path.app_id,
        &path.channel_name,
        target.annotation.client_id.as_deref(),
        query.get("socket_id").map(String::as_str),
    )
    .await?;

    let existing = handler
        .annotation_store()
        .get_events(AnnotationEventsRequest {
            app_id: path.app_id.clone(),
            channel_id: path.channel_name.clone(),
            message_serial: message_serial.clone(),
            annotation_type: target.annotation.annotation_type.clone(),
        })
        .await?;
    if existing.iter().any(|record| {
        record.annotation.action == AnnotationAction::Delete
            && record.annotation.id == target.annotation.id
    }) {
        return Ok((
            StatusCode::OK,
            Json(DeleteAnnotationResponse {
                annotation_serial: path.annotation_serial,
                deleted_annotation_serial: target_serial.as_str().to_string(),
            }),
        )
            .into_response());
    }

    let result = handler
        .delete_annotation_runtime(DeleteAnnotationRuntimeRequest {
            app,
            channel: path.channel_name,
            message_serial,
            target_serial,
        })
        .await?;

    Ok((
        StatusCode::OK,
        Json(DeleteAnnotationResponse {
            annotation_serial: result.annotation_serial.as_str().to_string(),
            deleted_annotation_serial: result.deleted_annotation_serial.as_str().to_string(),
        }),
    )
        .into_response())
}

/// GET /apps/{app_id}/channels/{channel_name}/messages/{message_serial}/annotations
#[instrument(skip(handler), fields(app_id = %path.app_id, channel = %path.channel_name, message_serial = %path.message_serial))]
pub async fn channel_message_annotations(
    Path(path): Path<VersionMutationPath>,
    Query(query): Query<AnnotationEventsQuery>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    validate_channel_name(&app, &path.channel_name).await?;
    require_versioned_messages_enabled(&handler, &path.channel_name)?;
    require_annotations_enabled(&handler, &app, &path.channel_name)?;
    let history_policy =
        app.resolved_history(&path.channel_name, &handler.server_options().history);
    if !history_policy.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Durable history is disabled by policy for channel '{}'",
            path.channel_name
        )));
    }
    authorize_annotation_subscribe(
        &handler,
        &path.app_id,
        &path.channel_name,
        query.socket_id.as_deref(),
    )
    .await?;
    let message_serial = parse_message_serial(&path.message_serial)?;
    let limit = query
        .limit
        .unwrap_or(handler.server_options().versioned_messages.max_page_size)
        .min(handler.server_options().versioned_messages.max_page_size);
    if limit == 0 {
        return Err(AppError::InvalidInput(
            "Annotation event limit must be greater than 0".to_string(),
        ));
    }
    let from_serial = match query.from_serial.as_deref() {
        Some(raw) => Some(parse_annotation_serial(raw)?),
        None => None,
    };

    let mut items = if let Some(raw_type) = query.annotation_type.as_deref() {
        let annotation_type = parse_annotation_type(raw_type)?;
        handler
            .annotation_store()
            .get_events(AnnotationEventsRequest {
                app_id: path.app_id.clone(),
                channel_id: path.channel_name.clone(),
                message_serial: message_serial.clone(),
                annotation_type,
            })
            .await?
            .into_iter()
            .filter(|record| {
                from_serial
                    .as_ref()
                    .is_none_or(|serial| record.annotation_serial() > serial)
            })
            .take(limit + 1)
            .collect::<Vec<_>>()
    } else {
        handler
            .annotation_store()
            .replay_raw(RawAnnotationReplayRequest {
                app_id: path.app_id.clone(),
                channel_id: path.channel_name.clone(),
                after_annotation_serial: from_serial,
                limit: usize::MAX,
            })
            .await?
            .into_iter()
            .filter(|record| record.message_serial() == &message_serial)
            .take(limit + 1)
            .collect::<Vec<_>>()
    };
    items.sort_by(|left, right| left.annotation_serial().cmp(right.annotation_serial()));
    let has_more = items.len() > limit;
    items.truncate(limit);
    let next_cursor = has_more
        .then(|| {
            items
                .last()
                .map(|record| record.annotation_serial().as_str().to_string())
        })
        .flatten();

    let payload = AnnotationEventsResponse {
        channel: path.channel_name,
        message_serial: path.message_serial,
        limit,
        has_more,
        next_cursor,
        items: items
            .iter()
            .map(|record| annotation_wire_event(&record.annotation))
            .collect(),
    };
    Ok((StatusCode::OK, Json(payload)))
}

/// GET /apps/{app_id}/channels/{channel_name}
#[instrument(skip(handler), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel(
    Path((app_id, channel_name)): Path<(String, String)>,
    Query(query_params_specific): Query<ChannelQuery>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    _uri: Uri,
    RawQuery(_raw_query_str_option): RawQuery,
) -> Result<impl IntoResponse, AppError> {
    debug!("Request for channel info for channel: {}", channel_name);
    validate_channel_name(&app, &channel_name).await?;

    let info_query_str = query_params_specific.info.as_ref();
    let wants_subscription_count = info_query_str.wants_subscription_count();
    let wants_user_count = info_query_str.wants_user_count();
    let wants_cache_data = info_query_str.wants_cache();

    let socket_count_info = handler
        .connection_manager()
        .get_channel_socket_count_info(&app_id, &channel_name)
        .await;
    let socket_count_val = socket_count_info.count;

    let user_count_val = if wants_user_count {
        if channel_name.starts_with("presence-") {
            let members_map = ChannelManager::get_channel_members(
                handler.connection_manager(),
                &app_id,
                &channel_name,
            )
            .await?;
            Some(members_map.len() as u64)
        } else {
            return Err(AppError::InvalidInput(
                "user_count is only available for presence channels".to_string(),
            ));
        }
    } else {
        None
    };

    let cache_data_tuple = if wants_cache_data && utils::is_cache_channel(&channel_name) {
        let cache_key_str = format!("app:{app_id}:channel:{channel_name}:cache_miss");

        match handler.cache_manager().get(&cache_key_str).await? {
            Some(cache_content_str) => {
                let ttl_duration = handler
                    .cache_manager()
                    .ttl(&cache_key_str)
                    .await?
                    .unwrap_or_else(|| core::time::Duration::from_secs(3600));
                Some((cache_content_str, ttl_duration))
            }
            _ => None,
        }
    } else {
        None
    };

    let subscription_count_val = if wants_subscription_count {
        Some(socket_count_val as u64)
    } else {
        None
    };
    let mut response_payload = PusherMessage::channel_info(
        socket_count_val > 0,
        subscription_count_val,
        user_count_val,
        cache_data_tuple,
    );
    if wants_subscription_count && !socket_count_info.complete {
        response_payload["subscription_count_complete"] = json!(false);
    }
    if let Some(ai_stats) = ai_channel_stats(&handler, &app, &app_id, &channel_name).await? {
        response_payload["ai"] = ai_stats;
    }
    let response_json_bytes = sonic_rs::to_vec(&response_payload)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    debug!("Channel info for '{}' retrieved successfully", channel_name);
    Ok((StatusCode::OK, Json(response_payload)))
}

/// GET /apps/{app_id}/channels
#[instrument(skip(handler), fields(app_id = %app_id))]
pub async fn channels(
    Path(app_id): Path<String>,
    Query(query_params_specific): Query<ChannelsQuery>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    _uri: Uri,
    RawQuery(_raw_query_str_option): RawQuery,
) -> Result<impl IntoResponse, AppError> {
    debug!("Request for channels list for app_id: {}", app_id);

    let filter_prefix_str = query_params_specific
        .filter_by_prefix
        .as_deref()
        .unwrap_or("");
    let wants_user_count = query_params_specific.info.as_ref().wants_user_count();
    let wants_subscription_count = query_params_specific
        .info
        .as_ref()
        .wants_subscription_count();

    let channels_map = handler
        .connection_manager()
        .get_channels_with_socket_count(&app_id)
        .await?;

    let mut channels_info_response_map = AHashMap::new();
    for (channel_name_str, socket_count) in &channels_map {
        if !channel_name_str.starts_with(filter_prefix_str) {
            continue;
        }
        validate_channel_name(&app, channel_name_str).await?;
        let mut current_channel_info_map = sonic_rs::Object::new();
        if wants_subscription_count {
            current_channel_info_map.insert("subscription_count", json!(*socket_count));
        }
        if wants_user_count {
            if channel_name_str.starts_with("presence-") {
                let members_map = ChannelManager::get_channel_members(
                    handler.connection_manager(),
                    &app_id,
                    channel_name_str,
                )
                .await?;
                current_channel_info_map.insert("user_count", json!(members_map.len()));
            } else if !filter_prefix_str.starts_with("presence-") {
                return Err(AppError::InvalidInput(
                    "user_count is only available for presence channels. Use filter_by_prefix=presence-".to_string()
                ));
            }
        }
        if !current_channel_info_map.is_empty() {
            channels_info_response_map.insert(
                channel_name_str.clone(),
                current_channel_info_map.into_value(),
            );
        } else if query_params_specific.info.is_none() {
            channels_info_response_map.insert(channel_name_str.clone(), json!({}));
        }
    }

    let response_payload = PusherMessage::channels_list(channels_info_response_map);
    let response_json_bytes = sonic_rs::to_vec(&response_payload)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    debug!("Channels list for app '{}' retrieved successfully", app_id);
    Ok((StatusCode::OK, Json(response_payload)))
}

/// GET /apps/{app_id}/channels/{channel_name}/users
#[instrument(skip(handler), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel_users(
    Path((app_id, channel_name)): Path<(String, String)>,
    Query(_auth_q_params_struct): Query<EventQuery>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    debug!("Request for users in channel: {}", channel_name);
    validate_channel_name(&app, &channel_name).await?;

    if !channel_name.starts_with("presence-") {
        return Err(AppError::InvalidInput(
            "Only presence channels support this endpoint".to_string(),
        ));
    }

    let channel_members_map =
        ChannelManager::get_channel_members(handler.connection_manager(), &app_id, &channel_name)
            .await?;
    let users_vec = channel_members_map
        .keys()
        .map(|user_id_str| json!({ "id": user_id_str }))
        .collect::<Vec<_>>();
    let response_payload_val = json!({ "users": users_vec });
    let response_json_bytes = sonic_rs::to_vec(&response_payload_val)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    info!(
        user_count = users_vec.len(),
        "Channel users for '{}' retrieved successfully", channel_name
    );
    Ok((StatusCode::OK, Json(response_payload_val)))
}

/// GET /apps/{app_id}/channels/{channel_name}/history
#[instrument(skip(handler), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel_history(
    Path((app_id, channel_name)): Path<(String, String)>,
    Query(query_params): Query<HistoryQuery>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    validate_channel_name(&app, &channel_name).await?;

    let history_policy = app.resolved_history(&channel_name, &handler.server_options().history);
    if !history_policy.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Durable history is disabled by policy for channel '{channel_name}'"
        )));
    }

    let direction = parse_history_direction(query_params.direction.as_deref())?;

    let limit = query_params
        .limit
        .unwrap_or(history_policy.max_page_size)
        .min(history_policy.max_page_size);
    if limit == 0 {
        return Err(AppError::InvalidInput(
            "History limit must be greater than 0".to_string(),
        ));
    }

    let cursor = match query_params.cursor.as_deref() {
        Some(encoded) => Some(HistoryCursor::decode(encoded)?),
        None => None,
    };
    let bounds = HistoryQueryBounds {
        start_serial: query_params.start_serial,
        end_serial: query_params.end_serial,
        start_time_ms: query_params.resolved_start_time_ms(),
        end_time_ms: query_params.resolved_end_time_ms(),
    };
    let stream_state = handler
        .history_store()
        .stream_runtime_state(&app_id, &channel_name)
        .await?;

    let page = handler
        .history_store()
        .read_page(HistoryReadRequest {
            app_id: app_id.clone(),
            channel: channel_name.clone(),
            direction,
            limit,
            cursor,
            bounds: bounds.clone(),
        })
        .await?;

    let mut items = Vec::with_capacity(page.items.len());
    for item in page.items {
        let mut event_name = item.event_name.clone();
        let mut operation_kind = item.operation_kind.clone();
        let mut payload_size_bytes = item.payload_size_bytes;
        let message: Value = if handler.server_options().versioned_messages.enabled {
            let raw_message: PusherMessage = sonic_rs::from_slice(item.payload_bytes.as_ref())
                .map_err(|e| {
                    AppError::InternalError(format!("Failed to decode history payload: {e}"))
                })?;
            if let Some(message_serial) = extract_runtime_message_serial(&raw_message) {
                match handler
                    .version_store()
                    .get_latest(
                        &app_id,
                        &channel_name,
                        &parse_message_serial(message_serial)?,
                    )
                    .await?
                {
                    Some(latest) => {
                        if let Some(metrics) = handler.metrics() {
                            metrics.mark_versioned_history_substitution(&app_id, "applied");
                        }
                        let latest_message = build_versioned_realtime_message(&latest);
                        let latest_bytes = sonic_rs::to_vec(&latest_message)?;
                        event_name = latest_message.message.event.clone();
                        operation_kind = latest_message.action.as_str().to_string();
                        payload_size_bytes = latest_bytes.len();
                        sonic_rs::to_value(&latest_message)?
                    }
                    None => {
                        if let Some(metrics) = handler.metrics() {
                            metrics.mark_versioned_history_substitution(&app_id, "missing_latest");
                        }
                        warn!(
                            app_id = %app_id,
                            channel = %channel_name,
                            history_serial = item.serial,
                            message_serial = %message_serial,
                            "History row referenced a versioned message without a latest winner; returning stored payload"
                        );
                        sonic_rs::to_value(&raw_message)?
                    }
                }
            } else {
                if let Some(metrics) = handler.metrics() {
                    metrics.mark_versioned_history_substitution(&app_id, "not_versioned");
                }
                sonic_rs::to_value(&raw_message)?
            }
        } else {
            sonic_rs::from_slice(item.payload_bytes.as_ref()).map_err(|e| {
                AppError::InternalError(format!("Failed to decode history payload: {e}"))
            })?
        };
        items.push(json!({
            "stream_id": item.stream_id,
            "serial": item.serial,
            "published_at_ms": item.published_at_ms,
            "message_id": item.message_id,
            "event_name": event_name,
            "operation_kind": operation_kind,
            "payload_size_bytes": payload_size_bytes,
            "message": message,
        }));
    }

    let response_payload = json!({
        "items": items,
        "direction": direction.as_str(),
        "limit": limit,
        "has_more": page.has_more,
        "next_cursor": page.next_cursor.and_then(|cursor| cursor.encode().ok()),
        "bounds": {
            "start_serial": bounds.start_serial,
            "end_serial": bounds.end_serial,
            "start_time_ms": bounds.start_time_ms,
            "end_time_ms": bounds.end_time_ms,
        },
        "continuity": {
            "stream_id": page.retained.stream_id,
            "oldest_available_serial": page.retained.oldest_serial,
            "newest_available_serial": page.retained.newest_serial,
            "oldest_available_published_at_ms": page.retained.oldest_published_at_ms,
            "newest_available_published_at_ms": page.retained.newest_published_at_ms,
            "retained_messages": page.retained.retained_messages,
            "retained_bytes": page.retained.retained_bytes,
            "complete": page.complete,
            "truncated_by_retention": page.truncated_by_retention,
        },
        "stream_state": build_history_stream_state_payload(&stream_state),
    });
    let response_json_bytes = sonic_rs::to_vec(&response_payload)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    Ok((StatusCode::OK, Json(response_payload)))
}

/// GET /apps/{app_id}/channels/{channel_name}/presence/history
#[instrument(skip(handler), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel_presence_history(
    Path((app_id, channel_name)): Path<(String, String)>,
    Query(query_params): Query<HistoryQuery>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    validate_channel_name(&app, &channel_name).await?;

    if !channel_name.starts_with("presence-") {
        return Err(AppError::InvalidInput(
            "Only presence channels support this endpoint".to_string(),
        ));
    }

    let history_policy =
        app.resolved_presence_history(&channel_name, &handler.server_options().presence_history);
    if !history_policy.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Presence history is disabled by policy for channel '{channel_name}'"
        )));
    }

    let direction = parse_presence_history_direction(query_params.direction.as_deref())?;

    let limit = query_params
        .limit
        .unwrap_or(history_policy.max_page_size)
        .min(history_policy.max_page_size);
    if limit == 0 {
        return Err(AppError::InvalidInput(
            "Presence history limit must be greater than 0".to_string(),
        ));
    }

    let cursor = match query_params.cursor.as_deref() {
        Some(encoded) => Some(PresenceHistoryCursor::decode(encoded)?),
        None => None,
    };
    let bounds = PresenceHistoryQueryBounds {
        start_serial: query_params.start_serial,
        end_serial: query_params.end_serial,
        start_time_ms: query_params.resolved_start_time_ms(),
        end_time_ms: query_params.resolved_end_time_ms(),
    };
    let stream_state = handler
        .presence_history_store()
        .stream_runtime_state(&app_id, &channel_name)
        .await?;

    let page = handler
        .presence_history_store()
        .read_page(PresenceHistoryReadRequest {
            app_id: app_id.clone(),
            channel: channel_name.clone(),
            direction,
            limit,
            cursor,
            bounds: bounds.clone(),
        })
        .await?;

    let mut items = Vec::with_capacity(page.items.len());
    for item in page.items {
        let presence_event: Value =
            sonic_rs::from_slice(item.payload_bytes.as_ref()).map_err(|e| {
                AppError::InternalError(format!("Failed to decode presence history payload: {e}"))
            })?;
        items.push(json!({
            "stream_id": item.stream_id,
            "serial": item.serial,
            "published_at_ms": item.published_at_ms,
            "event": item.event.as_str(),
            "cause": item.cause.as_str(),
            "user_id": item.user_id,
            "connection_id": item.connection_id,
            "dead_node_id": item.dead_node_id,
            "payload_size_bytes": item.payload_size_bytes,
            "presence_event": presence_event,
        }));
    }

    let response_payload = json!({
        "items": items,
        "direction": direction.as_str(),
        "limit": limit,
        "has_more": page.has_more,
        "next_cursor": page.next_cursor.and_then(|cursor| cursor.encode().ok()),
        "bounds": {
            "start_serial": bounds.start_serial,
            "end_serial": bounds.end_serial,
            "start_time_ms": bounds.start_time_ms,
            "end_time_ms": bounds.end_time_ms,
        },
        "continuity": {
            "stream_id": page.retained.stream_id,
            "oldest_available_serial": page.retained.oldest_serial,
            "newest_available_serial": page.retained.newest_serial,
            "oldest_available_published_at_ms": page.retained.oldest_published_at_ms,
            "newest_available_published_at_ms": page.retained.newest_published_at_ms,
            "retained_events": page.retained.retained_events,
            "retained_bytes": page.retained.retained_bytes,
            "degraded": page.degraded,
            "complete": page.complete,
            "truncated_by_retention": page.truncated_by_retention,
        },
        "stream_state": build_presence_history_stream_state_payload(&stream_state),
    });
    let response_json_bytes = sonic_rs::to_vec(&response_payload)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    Ok((StatusCode::OK, Json(response_payload)))
}

/// GET /apps/{app_id}/channels/{channel_name}/presence/history/state
#[instrument(skip(handler), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel_presence_history_state(
    Path((app_id, channel_name)): Path<(String, String)>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    validate_channel_name(&app, &channel_name).await?;

    if !channel_name.starts_with("presence-") {
        return Err(AppError::InvalidInput(
            "Only presence channels support this endpoint".to_string(),
        ));
    }

    let history_policy =
        app.resolved_presence_history(&channel_name, &handler.server_options().presence_history);
    if !history_policy.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Presence history is disabled by policy for channel '{channel_name}'"
        )));
    }

    let stream_inspection = handler
        .presence_history_store()
        .stream_inspection(&app_id, &channel_name)
        .await?;
    let response_payload = json!({
        "channel": channel_name,
        "stream": build_presence_history_stream_inspection_payload(&stream_inspection),
    });
    let response_json_bytes = sonic_rs::to_vec(&response_payload)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    Ok((StatusCode::OK, Json(response_payload)))
}

/// POST /apps/{app_id}/channels/{channel_name}/presence/history/reset
#[instrument(skip(handler, body), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel_presence_history_reset(
    Path((app_id, channel_name)): Path<(String, String)>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(body): Json<HistoryResetRequestBody>,
) -> Result<impl IntoResponse, AppError> {
    validate_channel_name(&app, &channel_name).await?;

    if !channel_name.starts_with("presence-") {
        return Err(AppError::InvalidInput(
            "Only presence channels support this endpoint".to_string(),
        ));
    }

    let history_policy =
        app.resolved_presence_history(&channel_name, &handler.server_options().presence_history);
    if !history_policy.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Presence history is disabled by policy for channel '{channel_name}'"
        )));
    }

    validate_history_destructive_request(
        &channel_name,
        &body.confirm_channel,
        &body.confirm_operation,
        "reset",
        &body.reason,
    )?;

    let result: PresenceHistoryResetResult = handler
        .presence_history_store()
        .reset_stream(
            &app_id,
            &channel_name,
            &body.reason,
            body.requested_by.as_deref(),
        )
        .await?;

    let response_payload = json!({
        "ok": true,
        "operation": "reset",
        "channel": channel_name,
        "reason": body.reason,
        "requested_by": body.requested_by,
        "previous_stream_id": result.previous_stream_id,
        "new_stream_id": result.new_stream_id,
        "purged_events": result.purged_events,
        "purged_bytes": result.purged_bytes,
        "stream": build_presence_history_stream_inspection_payload(&result.inspection),
    });
    let response_json_bytes = sonic_rs::to_vec(&response_payload)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    Ok((StatusCode::OK, Json(response_payload)))
}

/// GET /apps/{app_id}/channels/{channel_name}/presence/history/snapshot
///
/// Reconstructs effective presence membership at a point in time by replaying
/// retained history events. Without query params, returns the latest state
/// derived from the retained event stream.
#[instrument(skip(handler), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel_presence_history_snapshot(
    Path((app_id, channel_name)): Path<(String, String)>,
    Query(query_params): Query<PresenceSnapshotQuery>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    validate_channel_name(&app, &channel_name).await?;

    if !channel_name.starts_with("presence-") {
        return Err(AppError::InvalidInput(
            "Only presence channels support this endpoint".to_string(),
        ));
    }

    let history_policy =
        app.resolved_presence_history(&channel_name, &handler.server_options().presence_history);
    if !history_policy.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Presence history is disabled by policy for channel '{channel_name}'"
        )));
    }

    let snapshot = handler
        .presence_history_store()
        .snapshot_at(PresenceSnapshotRequest {
            app_id: app_id.clone(),
            channel: channel_name.clone(),
            at_time_ms: query_params.resolved_at_time_ms(),
            at_serial: query_params.at_serial,
        })
        .await?;

    let members: Vec<Value> = snapshot
        .members
        .iter()
        .map(|m| {
            json!({
                "user_id": m.user_id,
                "last_event": m.last_event.as_str(),
                "last_event_serial": m.last_event_serial,
                "last_event_at_ms": m.last_event_at_ms,
            })
        })
        .collect();

    let response_payload = json!({
        "channel": channel_name,
        "members": members,
        "member_count": snapshot.members.len(),
        "events_replayed": snapshot.events_replayed,
        "snapshot_serial": snapshot.snapshot_serial,
        "snapshot_time_ms": snapshot.snapshot_time_ms,
        "continuity": {
            "stream_id": snapshot.retained.stream_id,
            "oldest_available_serial": snapshot.retained.oldest_serial,
            "newest_available_serial": snapshot.retained.newest_serial,
            "oldest_available_published_at_ms": snapshot.retained.oldest_published_at_ms,
            "newest_available_published_at_ms": snapshot.retained.newest_published_at_ms,
            "retained_events": snapshot.retained.retained_events,
            "retained_bytes": snapshot.retained.retained_bytes,
            "complete": snapshot.complete,
            "truncated_by_retention": snapshot.truncated_by_retention,
        },
    });
    let response_json_bytes = sonic_rs::to_vec(&response_payload)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    Ok((StatusCode::OK, Json(response_payload)))
}

/// GET /apps/{app_id}/channels/{channel_name}/history/state
#[instrument(skip(handler), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel_history_state(
    Path((app_id, channel_name)): Path<(String, String)>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    validate_channel_name(&app, &channel_name).await?;

    let history_policy = app.resolved_history(&channel_name, &handler.server_options().history);
    if !history_policy.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Durable history is disabled by policy for channel '{channel_name}'"
        )));
    }

    let stream_inspection = handler
        .history_store()
        .stream_inspection(&app_id, &channel_name)
        .await?;
    let response_payload = json!({
        "channel": channel_name,
        "stream": build_history_stream_inspection_payload(&stream_inspection),
    });
    let response_json_bytes = sonic_rs::to_vec(&response_payload)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    Ok((StatusCode::OK, Json(response_payload)))
}

/// POST /apps/{app_id}/revocations
#[instrument(skip(handler, body), fields(app_id = %app_id))]
pub async fn revoke_capability_tokens(
    Path(app_id): Path<String>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(body): Json<RevokeCapabilityTokensRequest>,
) -> Result<impl IntoResponse, AppError> {
    let result = handler
        .revoke_capability_tokens(
            &app,
            CapabilityRevocationRequest {
                jti: body.jti,
                client_id: body.client_id,
                expires_at: body.expires_at,
                ttl_seconds: body.ttl_seconds,
                reason: body.reason,
            },
        )
        .await?;

    let response = RevokeCapabilityTokensResponse {
        revoked_jti: result.revoked_jti,
        revoked_client_id: result.revoked_client_id,
        closed_connections: result.closed_connections,
    };
    let response_json_bytes = sonic_rs::to_vec(&response)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    Ok((StatusCode::OK, Json(response)))
}

/// POST /apps/{app_id}/channels/{channel_name}/history/reset
#[instrument(skip(handler, body), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel_history_reset(
    Path((app_id, channel_name)): Path<(String, String)>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(body): Json<HistoryResetRequestBody>,
) -> Result<impl IntoResponse, AppError> {
    validate_channel_name(&app, &channel_name).await?;
    let history_policy = app.resolved_history(&channel_name, &handler.server_options().history);
    if !history_policy.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Durable history is disabled by policy for channel '{channel_name}'"
        )));
    }

    validate_history_destructive_request(
        &channel_name,
        &body.confirm_channel,
        &body.confirm_operation,
        "reset",
        &body.reason,
    )?;

    let result = handler
        .history_store()
        .reset_stream(
            &app_id,
            &channel_name,
            &body.reason,
            body.requested_by.as_deref(),
        )
        .await?;

    let response_payload = json!({
        "ok": true,
        "operation": "reset",
        "channel": channel_name,
        "reason": body.reason,
        "requested_by": body.requested_by,
        "previous_stream_id": result.previous_stream_id,
        "new_stream_id": result.new_stream_id,
        "purged_messages": result.purged_messages,
        "purged_bytes": result.purged_bytes,
        "stream": build_history_stream_inspection_payload(&result.inspection),
    });
    let response_json_bytes = sonic_rs::to_vec(&response_payload)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    Ok((StatusCode::OK, Json(response_payload)))
}

/// POST /apps/{app_id}/channels/{channel_name}/history/purge
#[instrument(skip(handler, body), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel_history_purge(
    Path((app_id, channel_name)): Path<(String, String)>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(body): Json<HistoryPurgeRequestBody>,
) -> Result<impl IntoResponse, AppError> {
    validate_channel_name(&app, &channel_name).await?;
    let history_policy = app.resolved_history(&channel_name, &handler.server_options().history);
    if !history_policy.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Durable history is disabled by policy for channel '{channel_name}'"
        )));
    }

    validate_history_destructive_request(
        &channel_name,
        &body.confirm_channel,
        &body.confirm_operation,
        "purge",
        &body.reason,
    )?;

    let result = handler
        .history_store()
        .purge_stream(
            &app_id,
            &channel_name,
            HistoryPurgeRequest {
                mode: body.mode,
                before_serial: body.before_serial,
                before_time_ms: body.before_time_ms,
                reason: body.reason.clone(),
                requested_by: body.requested_by.clone(),
            },
        )
        .await?;

    let response_payload = json!({
        "ok": true,
        "operation": "purge",
        "channel": channel_name,
        "mode": result.mode.as_str(),
        "before_serial": result.before_serial,
        "before_time_ms": result.before_time_ms,
        "reason": body.reason,
        "requested_by": body.requested_by,
        "purged_messages": result.purged_messages,
        "purged_bytes": result.purged_bytes,
        "stream": build_history_stream_inspection_payload(&result.inspection),
    });
    let response_json_bytes = sonic_rs::to_vec(&response_payload)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    Ok((StatusCode::OK, Json(response_payload)))
}

/// POST /apps/{app_id}/users/{user_id}/terminate_connections
#[instrument(skip(handler), fields(app_id = %app_id, user_id = %user_id))]
pub async fn terminate_user_connections(
    Path((app_id, user_id)): Path<(String, String)>,
    Query(_auth_q_params_struct): Query<EventQuery>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    _uri: Uri,
    RawQuery(_raw_query_str_option): RawQuery,
) -> Result<impl IntoResponse, AppError> {
    info!(
        "Received request to terminate user connections for user_id: {}",
        user_id
    );

    handler
        .connection_manager()
        .terminate_connection(&app.id, &user_id)
        .await?;

    info!(
        "Successfully initiated termination for user_id: {}",
        user_id
    );

    let response_payload = json!({ "ok": true });
    let response_size = sonic_rs::to_vec(&response_payload)?.len();
    record_api_metrics(&handler, &app.id, 0, response_size).await;

    Ok((StatusCode::OK, Json(response_payload)))
}

/// System health check function. Subsystem checks (adapter, cache, webhook
/// queue) run in parallel under per-check timeouts so a slow subsystem cannot
/// starve the others.
async fn check_system_health(
    handler: &Arc<ConnectionHandler>,
    timeout_duration: Duration,
) -> HealthStatus {
    let cache_enabled =
        handler.server_options().cache.driver != sockudo_core::options::CacheDriver::None;
    let webhook_integration = handler.webhook_integration();

    let adapter_fut = timeout(
        timeout_duration,
        handler.connection_manager().check_health(),
    );
    let cache_fut = async {
        if !cache_enabled {
            return None;
        }
        Some(timeout(timeout_duration, handler.cache_manager().check_health()).await)
    };
    let queue_fut = async {
        let Some(integration) = webhook_integration else {
            return None;
        };
        Some(timeout(timeout_duration, integration.check_queue_health()).await)
    };

    let (adapter_check, cache_check, queue_check) = tokio::join!(adapter_fut, cache_fut, queue_fut);

    let mut critical_issues = Vec::new();
    let mut non_critical_issues = Vec::new();

    // CRITICAL CHECK 1: Adapter health
    match adapter_check {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            critical_issues.push(format!("Adapter: {e}"));
        }
        Err(_) => {
            critical_issues.push("Adapter health check timeout".to_string());
        }
    }

    // CRITICAL CHECK 2: Cache manager health
    if let Some(cache_check) = cache_check {
        match cache_check {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                critical_issues.push(format!("Cache: {e}"));
            }
            Err(_) => {
                critical_issues.push("Cache health check timeout".to_string());
            }
        }
    }

    // NON-CRITICAL CHECK: Queue system health
    if let Some(queue_check) = queue_check {
        match queue_check {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                non_critical_issues.push(format!("Webhooks: {e}"));
            }
            Err(_) => {
                non_critical_issues.push("Webhook queue health check timeout".to_string());
            }
        }
    }

    if !critical_issues.is_empty() {
        HealthStatus::Error(critical_issues)
    } else if !non_critical_issues.is_empty() {
        HealthStatus::Degraded(non_critical_issues)
    } else {
        HealthStatus::Ok
    }
}

/// GET /live
pub async fn live() -> StatusCode {
    StatusCode::OK
}

/// GET /up or /up/{app_id}
#[instrument(skip(handler), fields(app_id = field::Empty))]
pub async fn up(
    app_id: Option<Path<String>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    let timeout_duration = Duration::from_millis(handler.server_options().health_check_timeout_ms);

    let (health_status, app_id_str) = if let Some(Path(app_id)) = app_id {
        tracing::Span::current().record("app_id", &app_id);
        debug!("Health check received for app_id: {}", app_id);

        let app_check = timeout(timeout_duration, handler.app_manager().find_by_id(&app_id)).await;

        let app_status = match app_check {
            Ok(Ok(Some(app))) if app.enabled => {
                check_system_health(&handler, timeout_duration).await
            }
            Ok(Ok(Some(_))) => HealthStatus::Error(vec!["App is disabled".to_string()]),
            Ok(Ok(None)) => HealthStatus::NotFound,
            Ok(Err(e)) => HealthStatus::Error(vec![format!("App manager: {e}")]),
            Err(_) => HealthStatus::Error(vec![format!(
                "App manager timeout (>{}ms)",
                timeout_duration.as_millis()
            )]),
        };

        (app_status, app_id)
    } else {
        debug!("General health check received (no app_id)");

        let apps_check = timeout(timeout_duration, handler.app_manager().get_apps()).await;

        let app_status = match apps_check {
            Ok(Ok(apps)) if !apps.is_empty() => {
                debug!("Found {} configured apps", apps.len());
                check_system_health(&handler, timeout_duration).await
            }
            Ok(Ok(_)) => HealthStatus::Error(vec!["No apps configured".to_string()]),
            Ok(Err(e)) => HealthStatus::Error(vec![format!("App manager: {e}")]),
            Err(_) => HealthStatus::Error(vec![format!(
                "App manager timeout (>{}ms)",
                timeout_duration.as_millis()
            )]),
        };

        (app_status, "system".to_string())
    };

    match &health_status {
        HealthStatus::Ok => {
            debug!("Health check passed for {}", app_id_str);
        }
        HealthStatus::Degraded(reasons) => {
            warn!(
                "Health check degraded for {}: {}",
                app_id_str,
                reasons.join(", ")
            );
        }
        HealthStatus::Error(reasons) => {
            error!(
                "Health check failed for {}: {}",
                app_id_str,
                reasons.join(", ")
            );
        }
        HealthStatus::NotFound => {
            warn!("Health check for non-existent app_id: {}", app_id_str);
        }
    }

    let (status_code, status_text, header_value) = match health_status {
        HealthStatus::Ok => (StatusCode::OK, "OK", "OK"),
        HealthStatus::Degraded(_) => (StatusCode::OK, "DEGRADED", "DEGRADED"),
        HealthStatus::Error(_) => (StatusCode::SERVICE_UNAVAILABLE, "ERROR", "ERROR"),
        HealthStatus::NotFound => (StatusCode::NOT_FOUND, "NOT_FOUND", "NOT_FOUND"),
    };

    if handler.metrics().is_some() {
        let response_size = status_text.len();
        record_api_metrics(&handler, &app_id_str, 0, response_size).await;
    }

    let response_val = axum::http::Response::builder()
        .status(status_code)
        .header("X-Health-Check", header_value)
        .body(status_text.to_string())?;

    Ok(response_val)
}

/// Fallback handler for unmatched routes.
/// Returns a plain text 404.
pub async fn fallback_404(uri: Uri) -> impl IntoResponse {
    debug!("No route matched for: {}", uri);
    (
        StatusCode::NOT_FOUND,
        [(header::CONTENT_TYPE, "text/plain")],
        "404 NOT FOUND",
    )
}

/// GET /metrics (Prometheus format)
#[instrument(skip(handler), fields(service = "metrics_exporter"))]
pub async fn metrics(
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    debug!("{}", "Metrics endpoint called");
    let plaintext_metrics_str = match handler.metrics().clone() {
        Some(metrics_arc) => metrics_arc.get_metrics_as_plaintext().await,
        None => {
            info!(
                "{}",
                "No metrics data available (metrics collection is not enabled)."
            );
            "# Metrics collection is not enabled.\n".to_string()
        }
    };
    #[cfg(feature = "push")]
    let plaintext_metrics_str = {
        let mut plaintext_metrics_str = plaintext_metrics_str;
        plaintext_metrics_str.push_str(&crate::push_http::push_metrics_plaintext());
        plaintext_metrics_str
    };
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    debug!(
        bytes = plaintext_metrics_str.len(),
        "Successfully generated Prometheus metrics"
    );
    Ok((StatusCode::OK, response_headers, plaintext_metrics_str))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::response::IntoResponse;
    use sockudo_adapter::ConnectionHandlerBuilder;
    use sockudo_adapter::local_adapter::LocalAdapter;
    use sockudo_app::memory_app_manager::MemoryAppManager;
    use sockudo_cache::memory_cache_manager::MemoryCacheManager;
    use sockudo_core::annotations::{
        AnnotationId, AnnotationProjectionRequest, AnnotationSummary, StoredAnnotationEvent,
        TotalAnnotationSummary,
    };
    use sockudo_core::app::AppManager;
    use sockudo_core::app::{
        App, AppChannelsPolicy, AppHistoryConfig, AppPolicy, ChannelNamespace,
        NamespaceHistoryConfig,
    };
    use sockudo_core::history::{
        HistoryDurableState, HistoryPage, HistoryPurgeRequest, HistoryPurgeResult,
        HistoryReadRequest, HistoryResetResult, HistoryRuntimeStatus, HistoryStore,
        HistoryStreamInspection, HistoryStreamRuntimeState, HistoryWriteReservation,
        MemoryHistoryStore, MemoryHistoryStoreConfig,
    };
    use sockudo_core::namespace::Namespace;
    use sockudo_core::options::MemoryCacheOptions;
    use sockudo_core::presence_history::{
        MemoryPresenceHistoryStore, PresenceHistoryDurableState, PresenceHistoryEventCause,
        PresenceHistoryEventKind, PresenceHistoryResetResult, PresenceHistoryRetentionPolicy,
        PresenceHistoryRuntimeStatus, PresenceHistoryStore, PresenceHistoryStreamInspection,
        PresenceHistoryStreamRuntimeState, PresenceHistoryTransitionRecord,
        TrackingPresenceHistoryStore,
    };
    use sockudo_core::version_store::{MemoryVersionStore, StoredVersionRecord, VersionStore};
    use sockudo_core::versioned_messages::{
        MessageSerial, VersionMetadata, VersionSerial, VersionedMessage,
    };
    use sockudo_core::websocket::{
        ConnectionCapabilities, SocketId, UserInfo, WebSocketBufferConfig,
    };
    use sockudo_protocol::messages::{
        AiExtras, ApiMessageData, MessageData, PusherApiMessage, PusherMessage,
    };
    use sockudo_protocol::versioned_messages::{
        AppendMessageRequest, DeleteMessageRequest, MessageVersionsQuery, UpdateMessageRequest,
        VersionDirection,
    };
    use sockudo_protocol::{ProtocolVersion, WireFormat};
    #[cfg(feature = "push")]
    use sockudo_push::{
        ChannelSubscription, DeviceDetails, DevicePushDetails, DevicePushState, FormFactor,
        MemoryPushQueue, MemoryPushStore, Platform, PushDeviceStore, PushPublishLogStore,
        PushRecipient, PushSubscriptionStore, SecretString, hash_device_identity_token,
    };
    use sockudo_ws::axum_integration::WebSocketWriter;
    use sockudo_ws::client::WebSocketClient;
    use sockudo_ws::{Config as WsConfig, Http1, Stream as WsStream, WebSocketStream};
    use sonic_rs::JsonContainerTrait;
    use sonic_rs::JsonValueTrait;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::net::{TcpListener, TcpStream};

    #[cfg(feature = "push")]
    fn test_push_store() -> Extension<sockudo_push::DynPushStore> {
        Extension(Arc::new(sockudo_push::MemoryPushStore::new()))
    }

    #[cfg(feature = "push")]
    fn test_push_queue() -> Extension<sockudo_push::DynPushQueue> {
        Extension(Arc::new(sockudo_push::MemoryPushQueue::new()))
    }

    fn test_app() -> App {
        test_app_with_policy(Default::default())
    }

    fn test_annotation_app() -> App {
        test_app_with_policy(AppPolicy {
            channels: AppChannelsPolicy {
                annotations_enabled: Some(true),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    #[cfg(feature = "push")]
    fn test_notifications_app() -> App {
        test_app_with_policy(AppPolicy {
            channels: AppChannelsPolicy {
                channel_namespaces: Some(vec![ChannelNamespace {
                    name: "notifications".to_string(),
                    channel_name_pattern: None,
                    max_channel_name_length: None,
                    annotations_enabled: None,
                    allow_user_limited_channels: None,
                    allow_subscribe_for_client: None,
                    allow_publish_for_client: None,
                    allow_presence_for_client: None,
                    history: None,
                    presence_history: None,
                }]),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn test_app_with_policy(policy: AppPolicy) -> App {
        App::from_policy(
            "app-1".to_string(),
            "key".to_string(),
            "secret".to_string(),
            true,
            policy,
        )
    }

    fn test_history_message(channel: &str, index: usize) -> PusherMessage {
        PusherMessage {
            event: Some(format!("message-created-{index}")),
            channel: Some(channel.to_string()),
            data: Some(MessageData::String(format!("{{\"text\":\"msg-{index}\"}}"))),
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
        }
    }

    fn test_history_handler(max_page_size: usize) -> Arc<ConnectionHandler> {
        test_history_handler_with_store(
            max_page_size,
            Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default())),
        )
    }

    fn test_history_handler_with_memory_store(
        max_page_size: usize,
    ) -> (Arc<ConnectionHandler>, Arc<MemoryHistoryStore>) {
        let store = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        (
            test_history_handler_with_store(max_page_size, store.clone()),
            store,
        )
    }

    fn test_history_handler_with_store(
        max_page_size: usize,
        history_store: Arc<dyn HistoryStore + Send + Sync>,
    ) -> Arc<ConnectionHandler> {
        let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
        let adapter = Arc::new(LocalAdapter::new())
            as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));

        let mut options = sockudo_core::options::ServerOptions::default();
        options.history.enabled = true;
        options.history.max_page_size = max_page_size;

        Arc::new(
            ConnectionHandlerBuilder::new(app_manager, adapter, cache, options)
                .history_store(history_store)
                .build(),
        )
    }

    fn test_presence_history_handler(max_page_size: usize) -> Arc<ConnectionHandler> {
        test_presence_history_handler_with_store(
            max_page_size,
            Arc::new(MemoryPresenceHistoryStore::new(Default::default())),
        )
    }

    fn test_versioned_handler_harness(
        max_page_size: usize,
        version_store: Arc<dyn VersionStore + Send + Sync>,
    ) -> (
        Arc<ConnectionHandler>,
        Arc<LocalAdapter>,
        Arc<MemoryAppManager>,
    ) {
        test_versioned_handler_harness_with_annotations(max_page_size, version_store, true)
    }

    fn test_versioned_handler_harness_with_annotations(
        max_page_size: usize,
        version_store: Arc<dyn VersionStore + Send + Sync>,
        annotations_enabled: bool,
    ) -> (
        Arc<ConnectionHandler>,
        Arc<LocalAdapter>,
        Arc<MemoryAppManager>,
    ) {
        let app_manager = Arc::new(MemoryAppManager::new());
        let adapter = Arc::new(LocalAdapter::new());
        let app_manager_dyn = app_manager.clone() as Arc<dyn AppManager + Send + Sync>;
        let adapter_dyn =
            adapter.clone() as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));

        let mut options = sockudo_core::options::ServerOptions::default();
        options.versioned_messages.enabled = true;
        options.versioned_messages.max_page_size = max_page_size;
        options.history.enabled = true;
        options.annotations.enabled = annotations_enabled;

        let handler = Arc::new(
            ConnectionHandlerBuilder::new(app_manager_dyn, adapter_dyn, cache, options)
                .history_store(Arc::new(MemoryHistoryStore::new(
                    MemoryHistoryStoreConfig::default(),
                )))
                .version_store(version_store)
                .build(),
        );

        (handler, adapter, app_manager)
    }

    fn test_versioned_handler_with_store(
        max_page_size: usize,
        version_store: Arc<dyn VersionStore + Send + Sync>,
    ) -> Arc<ConnectionHandler> {
        let (handler, _, _) = test_versioned_handler_harness(max_page_size, version_store);
        handler
    }

    fn test_ai_versioned_handler_with_store(
        max_page_size: usize,
        version_store: Arc<dyn VersionStore + Send + Sync>,
        max_accumulated_message_bytes: usize,
        max_appends_per_message: usize,
        max_open_streaming_messages_per_channel: usize,
    ) -> Arc<ConnectionHandler> {
        let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
        let adapter = Arc::new(LocalAdapter::new())
            as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));

        let mut options = sockudo_core::options::ServerOptions::default();
        options.versioned_messages.enabled = true;
        options.versioned_messages.max_page_size = max_page_size;
        options.history.enabled = true;
        options.ai_transport.enabled = true;
        options.ai_transport.channels = vec![sockudo_core::options::AiTransportChannelConfig {
            prefix: "versioned-".to_string(),
        }];
        options.ai_transport.max_accumulated_message_bytes = max_accumulated_message_bytes;
        options.ai_transport.max_appends_per_message = max_appends_per_message;
        options.ai_transport.max_open_streaming_messages_per_channel =
            max_open_streaming_messages_per_channel;

        Arc::new(
            ConnectionHandlerBuilder::new(app_manager, adapter, cache, options)
                .history_store(Arc::new(MemoryHistoryStore::new(
                    MemoryHistoryStoreConfig::default(),
                )))
                .version_store(version_store)
                .build(),
        )
    }

    #[cfg(feature = "push")]
    fn test_handler_with_push_rules(
        rules: Vec<sockudo_core::options::PushRuleConfig>,
    ) -> Arc<ConnectionHandler> {
        let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
        let adapter = Arc::new(LocalAdapter::new())
            as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));
        let mut options = sockudo_core::options::ServerOptions::default();
        options.push_rules = rules;

        Arc::new(ConnectionHandlerBuilder::new(app_manager, adapter, cache, options).build())
    }

    #[cfg(feature = "push")]
    fn test_push_device(device_id: &str) -> DeviceDetails {
        DeviceDetails {
            app_id: "app-1".to_string(),
            id: device_id.to_string(),
            client_id: Some("user-1".to_string()),
            form_factor: FormFactor::Phone,
            platform: Platform::Android,
            metadata: serde_json::json!({}),
            device_secret: hash_device_identity_token(&SecretString::new("device-token").unwrap()),
            timezone: "UTC".to_string(),
            locale: "en".to_string(),
            last_active_at_ms: 1,
            push: DevicePushDetails {
                recipient: PushRecipient::Fcm {
                    registration_token: SecretString::new(format!("token-{device_id}")).unwrap(),
                },
                state: DevicePushState::Active,
                failure_count: 0,
                error_reason: None,
            },
            push_rate_policy: None,
        }
    }

    fn test_versioned_record(
        message_serial: &str,
        version_serial: &str,
        history_serial: u64,
        delivery_serial: u64,
        text: &str,
    ) -> StoredVersionRecord {
        StoredVersionRecord {
            app_id: "app-1".to_string(),
            channel: "versioned-room".to_string(),
            original_client_id: Some("user-1".to_string()),
            message: VersionedMessage::new_create(
                MessageSerial::new(message_serial.to_string()).unwrap(),
                VersionMetadata {
                    serial: VersionSerial::new(version_serial.to_string()).unwrap(),
                    client_id: Some("user-1".to_string()),
                    timestamp_ms: 1,
                    description: None,
                    metadata: None,
                },
                history_serial,
                delivery_serial,
                Some("chat.message".to_string()),
                Some(MessageData::String(format!("{{\"text\":\"{text}\"}}"))),
                None,
            ),
        }
    }

    fn ai_extras(status: &str) -> MessageExtras {
        MessageExtras {
            ai: Some(AiExtras {
                transport: Some(HashMap::from([("status".to_string(), status.to_string())])),
                codec: Some(HashMap::from([(
                    "content-type".to_string(),
                    "text/plain".to_string(),
                )])),
            }),
            ..Default::default()
        }
    }

    async fn publish_ai_http_event(
        handler: Arc<ConnectionHandler>,
        app: App,
        event_name: &str,
        message_id: Option<&str>,
        idempotency_header: Option<&str>,
        status: &str,
        data: &str,
    ) -> Value {
        let mut headers = HeaderMap::new();
        if let Some(key) = idempotency_header {
            headers.insert(
                header::HeaderName::from_static("x-idempotency-key"),
                HeaderValue::from_str(key).unwrap(),
            );
        }

        let response = events(
            Path("app-1".to_string()),
            Query(empty_event_query()),
            Extension(app),
            #[cfg(feature = "push")]
            test_push_store(),
            #[cfg(feature = "push")]
            test_push_queue(),
            State(handler),
            headers,
            Uri::from_static("/apps/app-1/events"),
            RawQuery(None),
            Json(PusherApiMessage {
                name: Some(event_name.to_string()),
                data: Some(ApiMessageData::String(data.to_string())),
                channel: Some("versioned-room".to_string()),
                channels: None,
                socket_id: None,
                info: None,
                tags: None,
                delta: None,
                idempotency_key: None,
                message_id: message_id.map(str::to_string),
                extras: Some(ai_extras(status)),
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        sonic_rs::from_slice(&body).unwrap()
    }

    async fn test_websocket_writer() -> WebSocketWriter {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = sockudo_ws::handshake::server_handshake(&mut stream)
                .await
                .unwrap();
            let ws = sockudo_ws::axum_integration::WebSocket::from_tcp(stream, WsConfig::default());
            let (_reader, writer) = ws.split();
            writer
        });

        let client_stream = TcpStream::connect(addr).await.unwrap();
        let client = WebSocketClient::<Http1>::new(WsConfig::default());
        let (client_ws, _): (WebSocketStream<WsStream<Http1>>, _) = client
            .connect(client_stream, &addr.to_string(), "/", None)
            .await
            .unwrap();
        let (_reader, _writer) = client_ws.split();

        server_task.await.unwrap()
    }

    fn empty_event_query() -> EventQuery {
        EventQuery {
            auth_key: String::new(),
            auth_timestamp: String::new(),
            auth_version: String::new(),
            body_md5: String::new(),
            auth_signature: String::new(),
        }
    }

    fn test_realtime_handler_harness() -> (Arc<ConnectionHandler>, Arc<MemoryAppManager>) {
        let app_manager = Arc::new(MemoryAppManager::new());
        let adapter = Arc::new(LocalAdapter::new());
        let app_manager_dyn = app_manager.clone() as Arc<dyn AppManager + Send + Sync>;
        let adapter_dyn =
            adapter.clone() as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));
        let handler = Arc::new(
            ConnectionHandlerBuilder::new(
                app_manager_dyn,
                adapter_dyn,
                cache,
                sockudo_core::options::ServerOptions::default(),
            )
            .build(),
        );

        (handler, app_manager)
    }

    async fn attach_signed_in_mutation_actor(
        handler: &Arc<ConnectionHandler>,
        app_manager: Arc<MemoryAppManager>,
        app: &App,
        socket_id: SocketId,
        user_id: &str,
        capabilities: ConnectionCapabilities,
    ) {
        app_manager.create_app(app.clone()).await.unwrap();

        handler
            .connection_manager()
            .add_socket(
                socket_id,
                test_websocket_writer().await,
                &app.id,
                app_manager as Arc<dyn AppManager + Send + Sync>,
                WebSocketBufferConfig::default(),
                ProtocolVersion::V2,
                WireFormat::Json,
                true,
            )
            .await
            .unwrap();

        handler
            .update_connection_with_user_info(
                &socket_id,
                app,
                &UserInfo {
                    id: user_id.to_string(),
                    watchlist: None,
                    info: None,
                    capabilities: Some(capabilities),
                    meta: None,
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn channels_list_includes_subscription_count_when_requested() {
        let (handler, app_manager) = test_realtime_handler_harness();
        let app = test_app();
        app_manager.create_app(app.clone()).await.unwrap();

        let socket_id = SocketId::from_string("31.31").unwrap();
        handler
            .connection_manager()
            .add_socket(
                socket_id,
                test_websocket_writer().await,
                &app.id,
                app_manager as Arc<dyn AppManager + Send + Sync>,
                WebSocketBufferConfig::default(),
                ProtocolVersion::V2,
                WireFormat::Json,
                true,
            )
            .await
            .unwrap();
        handler
            .connection_manager()
            .add_to_channel(&app.id, "private-your-channel", &socket_id)
            .await
            .unwrap();

        let response = channels(
            Path(app.id.clone()),
            Query(ChannelsQuery {
                filter_by_prefix: Some("private-".to_string()),
                info: Some("subscription_count".to_string()),
                auth_params: empty_event_query(),
            }),
            Extension(app),
            State(handler),
            Uri::from_static(
                "/apps/app-1/channels?filter_by_prefix=private-&info=subscription_count",
            ),
            RawQuery(Some(
                "filter_by_prefix=private-&info=subscription_count".to_string(),
            )),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(
            json["channels"]["private-your-channel"]["subscription_count"].as_u64(),
            Some(1)
        );
    }

    fn test_presence_history_handler_with_memory_store(
        max_page_size: usize,
    ) -> (Arc<ConnectionHandler>, Arc<MemoryPresenceHistoryStore>) {
        let store = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        (
            test_presence_history_handler_with_store(max_page_size, store.clone()),
            store,
        )
    }

    fn test_presence_history_handler_with_store(
        max_page_size: usize,
        presence_history_store: Arc<dyn PresenceHistoryStore + Send + Sync>,
    ) -> Arc<ConnectionHandler> {
        let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
        let adapter = Arc::new(LocalAdapter::new())
            as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));

        let mut options = sockudo_core::options::ServerOptions::default();
        options.presence_history.enabled = true;
        options.presence_history.max_page_size = max_page_size;

        Arc::new(
            ConnectionHandlerBuilder::new(app_manager, adapter, cache, options)
                .presence_history_store(presence_history_store)
                .build(),
        )
    }

    async fn seed_presence_history(
        store: &Arc<MemoryPresenceHistoryStore>,
        app_id: &str,
        channel: &str,
        count: usize,
        base_ts: i64,
    ) {
        for index in 1..=count {
            store
                .record_transition(PresenceHistoryTransitionRecord {
                    app_id: app_id.to_string(),
                    channel: channel.to_string(),
                    event_kind: if index % 2 == 0 {
                        PresenceHistoryEventKind::MemberRemoved
                    } else {
                        PresenceHistoryEventKind::MemberAdded
                    },
                    cause: if index % 2 == 0 {
                        PresenceHistoryEventCause::Disconnect
                    } else {
                        PresenceHistoryEventCause::Join
                    },
                    user_id: format!("user-{index}"),
                    connection_id: Some(format!("socket-{index}")),
                    user_info: Some(sonic_rs::json!({ "n": index })),
                    dead_node_id: None,
                    dedupe_key: format!("transition-{index}"),
                    published_at_ms: base_ts + index as i64,
                    retention: PresenceHistoryRetentionPolicy {
                        retention_window_seconds: 3600,
                        max_events_per_channel: None,
                        max_bytes_per_channel: None,
                    },
                })
                .await
                .unwrap();
        }
    }

    #[derive(Clone)]
    struct InspectableHistoryStore {
        inner: Arc<MemoryHistoryStore>,
        state: HistoryStreamRuntimeState,
    }

    #[async_trait]
    impl HistoryStore for InspectableHistoryStore {
        async fn reserve_publish_position(
            &self,
            app_id: &str,
            channel: &str,
        ) -> sockudo_core::error::Result<HistoryWriteReservation> {
            self.inner.reserve_publish_position(app_id, channel).await
        }

        async fn append(
            &self,
            record: sockudo_core::history::HistoryAppendRecord,
        ) -> sockudo_core::error::Result<()> {
            self.inner.append(record).await
        }

        async fn read_page(
            &self,
            request: HistoryReadRequest,
        ) -> sockudo_core::error::Result<HistoryPage> {
            if !self.state.recovery_allowed {
                return Err(sockudo_core::error::Error::Internal(
                    "history_stream_state_blocks_reads".to_string(),
                ));
            }
            self.inner.read_page(request).await
        }

        async fn runtime_status(&self) -> sockudo_core::error::Result<HistoryRuntimeStatus> {
            Ok(HistoryRuntimeStatus {
                enabled: true,
                backend: "memory".to_string(),
                state_authority: "test_state".to_string(),
                degraded_channels: usize::from(!self.state.recovery_allowed),
                reset_required_channels: usize::from(self.state.reset_required),
                queue_depth: 0,
            })
        }

        async fn stream_runtime_state(
            &self,
            _app_id: &str,
            _channel: &str,
        ) -> sockudo_core::error::Result<HistoryStreamRuntimeState> {
            Ok(self.state.clone())
        }

        async fn stream_inspection(
            &self,
            app_id: &str,
            channel: &str,
        ) -> sockudo_core::error::Result<HistoryStreamInspection> {
            Ok(HistoryStreamInspection {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                stream_id: self.state.stream_id.clone(),
                next_serial: Some(10),
                retained: self
                    .inner
                    .read_page(HistoryReadRequest {
                        app_id: app_id.to_string(),
                        channel: channel.to_string(),
                        direction: HistoryDirection::NewestFirst,
                        limit: 1,
                        cursor: None,
                        bounds: HistoryQueryBounds::default(),
                    })
                    .await
                    .map(|page| page.retained)
                    .unwrap_or_default(),
                state: self.state.clone(),
            })
        }

        async fn reset_stream(
            &self,
            app_id: &str,
            channel: &str,
            reason: &str,
            requested_by: Option<&str>,
        ) -> sockudo_core::error::Result<HistoryResetResult> {
            self.inner
                .reset_stream(app_id, channel, reason, requested_by)
                .await
        }

        async fn purge_stream(
            &self,
            app_id: &str,
            channel: &str,
            request: HistoryPurgeRequest,
        ) -> sockudo_core::error::Result<HistoryPurgeResult> {
            self.inner.purge_stream(app_id, channel, request).await
        }
    }

    #[derive(Clone)]
    struct InspectablePresenceHistoryStore {
        inner: Arc<MemoryPresenceHistoryStore>,
        state: PresenceHistoryStreamRuntimeState,
    }

    #[async_trait]
    impl PresenceHistoryStore for InspectablePresenceHistoryStore {
        async fn record_transition(
            &self,
            record: PresenceHistoryTransitionRecord,
        ) -> sockudo_core::error::Result<()> {
            self.inner.record_transition(record).await
        }

        async fn read_page(
            &self,
            request: PresenceHistoryReadRequest,
        ) -> sockudo_core::error::Result<sockudo_core::presence_history::PresenceHistoryPage>
        {
            let mut page = self.inner.read_page(request).await?;
            if !self.state.continuity_proven {
                page.complete = false;
                page.degraded = true;
            }
            Ok(page)
        }

        async fn runtime_status(
            &self,
        ) -> sockudo_core::error::Result<PresenceHistoryRuntimeStatus> {
            Ok(PresenceHistoryRuntimeStatus {
                enabled: true,
                backend: "memory".to_string(),
                state_authority: "test_state".to_string(),
                degraded_channels: usize::from(
                    self.state.durable_state != PresenceHistoryDurableState::Healthy,
                ),
                reset_required_channels: usize::from(self.state.reset_required),
                queue_depth: 0,
            })
        }

        async fn stream_runtime_state(
            &self,
            _app_id: &str,
            _channel: &str,
        ) -> sockudo_core::error::Result<PresenceHistoryStreamRuntimeState> {
            Ok(self.state.clone())
        }

        async fn stream_inspection(
            &self,
            app_id: &str,
            channel: &str,
        ) -> sockudo_core::error::Result<PresenceHistoryStreamInspection> {
            Ok(PresenceHistoryStreamInspection {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                stream_id: self.state.stream_id.clone(),
                next_serial: Some(10),
                retained: self
                    .inner
                    .read_page(PresenceHistoryReadRequest {
                        app_id: app_id.to_string(),
                        channel: channel.to_string(),
                        direction: PresenceHistoryDirection::NewestFirst,
                        limit: 1,
                        cursor: None,
                        bounds: PresenceHistoryQueryBounds::default(),
                    })
                    .await
                    .map(|page| page.retained)
                    .unwrap_or_default(),
                state: self.state.clone(),
            })
        }

        async fn reset_stream(
            &self,
            app_id: &str,
            channel: &str,
            reason: &str,
            requested_by: Option<&str>,
        ) -> sockudo_core::error::Result<PresenceHistoryResetResult> {
            self.inner
                .reset_stream(app_id, channel, reason, requested_by)
                .await
        }
    }

    #[tokio::test]
    async fn stats_endpoint_returns_empty_totals_for_empty_server() {
        let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
        let adapter = Arc::new(LocalAdapter::new())
            as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));

        let handler = Arc::new(
            ConnectionHandlerBuilder::new(
                app_manager,
                adapter,
                cache,
                sockudo_core::options::ServerOptions::default(),
            )
            .build(),
        );

        let response = stats(State(handler)).await.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn stats_endpoint_reports_non_empty_occupancy_counts() {
        let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
        let concrete_adapter = Arc::new(LocalAdapter::new());

        let namespace = Arc::new(Namespace::new("app-1".to_string()));
        namespace.add_channel_to_socket(
            "chat:room-1",
            &sockudo_core::websocket::SocketId::from_string("1.1").unwrap(),
        );
        namespace.add_channel_to_socket(
            "chat:room-1",
            &sockudo_core::websocket::SocketId::from_string("1.2").unwrap(),
        );
        namespace.add_channel_to_socket(
            "presence-chat:room-1",
            &sockudo_core::websocket::SocketId::from_string("2.1").unwrap(),
        );

        concrete_adapter
            .namespaces
            .insert("app-1".to_string(), namespace);

        let adapter = concrete_adapter as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));

        let handler = Arc::new(
            ConnectionHandlerBuilder::new(
                app_manager,
                adapter,
                cache,
                sockudo_core::options::ServerOptions::default(),
            )
            .build(),
        );

        let response = stats(State(handler)).await.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();

        assert_eq!(json["totals"]["apps"].as_u64(), Some(1));
        assert_eq!(json["totals"]["occupancy"]["channels"].as_u64(), Some(2));
        assert_eq!(
            json["totals"]["occupancy"]["subscriptions"].as_u64(),
            Some(3)
        );
        assert_eq!(
            json["totals"]["occupancy"]["presence_channels"].as_u64(),
            Some(1)
        );
        assert_eq!(
            json["apps"][0]["occupancy"]["subscriptions"].as_u64(),
            Some(3)
        );
        assert_eq!(
            json["history"]["state_authority"].as_str(),
            Some("disabled")
        );
        assert_eq!(json["history"]["reset_required_channels"].as_u64(), Some(0));
        assert_eq!(
            json["presence_history"]["state_authority"].as_str(),
            Some("disabled")
        );
        assert_eq!(
            json["presence_history"]["reset_required_channels"].as_u64(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn stats_endpoint_reports_presence_history_health_summary() {
        let app_manager = Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>;
        let adapter = Arc::new(LocalAdapter::new())
            as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));
        let inner = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        let presence_store = Arc::new(InspectablePresenceHistoryStore {
            inner,
            state: PresenceHistoryStreamRuntimeState {
                app_id: "app-1".to_string(),
                channel: "presence-room".to_string(),
                stream_id: Some("presence-stream-9".to_string()),
                durable_state: PresenceHistoryDurableState::ResetRequired,
                continuity_proven: false,
                reset_required: true,
                reason: Some("presence_history_reset_required_after_write_failure".to_string()),
                node_id: Some("node-a".to_string()),
                last_transition_at_ms: Some(4321),
                authoritative_source: "test_state".to_string(),
                observed_source: "test_state".to_string(),
            },
        });

        let mut options = sockudo_core::options::ServerOptions::default();
        options.presence_history.enabled = true;

        let handler = Arc::new(
            ConnectionHandlerBuilder::new(app_manager, adapter, cache, options)
                .presence_history_store(presence_store)
                .build(),
        );

        let response = stats(State(handler)).await.unwrap().into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();

        assert_eq!(
            json["presence_history"]["state_authority"].as_str(),
            Some("test_state")
        );
        assert_eq!(
            json["presence_history"]["degraded_channels"].as_u64(),
            Some(1)
        );
        assert_eq!(
            json["presence_history"]["reset_required_channels"].as_u64(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn channel_history_state_endpoint_returns_authoritative_stream_state() {
        let stateful_store = Arc::new(InspectableHistoryStore {
            inner: Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default())),
            state: HistoryStreamRuntimeState {
                app_id: "app-1".to_string(),
                channel: "public-room".to_string(),
                stream_id: Some("stream-9".to_string()),
                durable_state: HistoryDurableState::Degraded,
                recovery_allowed: false,
                reset_required: false,
                reason: Some("durable_history_write_failed".to_string()),
                node_id: Some("node-a".to_string()),
                last_transition_at_ms: Some(1234),
                authoritative_source: "durable_store".to_string(),
                observed_source: "shared_cache_hint".to_string(),
            },
        });
        let handler = test_history_handler_with_store(100, stateful_store);
        let app = test_annotation_app();

        let response = channel_history_state(
            Path(("app-1".to_string(), "public-room".to_string())),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();

        assert_eq!(
            json["stream"]["state"]["durable_state"].as_str(),
            Some("degraded")
        );
        assert_eq!(
            json["stream"]["state"]["recovery_allowed"].as_bool(),
            Some(false)
        );
        assert_eq!(
            json["stream"]["state"]["authoritative_source"].as_str(),
            Some("durable_store")
        );
        assert_eq!(
            json["stream"]["state"]["observed_source"].as_str(),
            Some("shared_cache_hint")
        );
        assert_eq!(json["stream"]["next_serial"].as_u64(), Some(10));
    }

    #[tokio::test]
    async fn channel_presence_history_state_endpoint_returns_authoritative_stream_state() {
        let inner = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        seed_presence_history(
            &inner,
            "app-1",
            "presence-room",
            2,
            sockudo_core::history::now_ms(),
        )
        .await;
        let stateful_store = Arc::new(InspectablePresenceHistoryStore {
            inner,
            state: PresenceHistoryStreamRuntimeState {
                app_id: "app-1".to_string(),
                channel: "presence-room".to_string(),
                stream_id: Some("presence-stream-9".to_string()),
                durable_state: PresenceHistoryDurableState::ResetRequired,
                continuity_proven: false,
                reset_required: true,
                reason: Some("presence_history_reset_required_after_write_failure".to_string()),
                node_id: Some("node-a".to_string()),
                last_transition_at_ms: Some(4321),
                authoritative_source: "test_state".to_string(),
                observed_source: "test_state".to_string(),
            },
        });
        let handler = test_presence_history_handler_with_store(100, stateful_store);
        let app = test_app();

        let response = channel_presence_history_state(
            Path(("app-1".to_string(), "presence-room".to_string())),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();

        assert_eq!(
            json["stream"]["state"]["durable_state"].as_str(),
            Some("reset_required")
        );
        assert_eq!(
            json["stream"]["state"]["continuity_proven"].as_bool(),
            Some(false)
        );
        assert_eq!(
            json["stream"]["state"]["reset_required"].as_bool(),
            Some(true)
        );
        assert_eq!(
            json["stream"]["state"]["reason"].as_str(),
            Some("presence_history_reset_required_after_write_failure")
        );
        assert_eq!(json["stream"]["next_serial"].as_u64(), Some(10));
    }

    #[tokio::test]
    async fn channel_presence_history_reports_degraded_stream_fail_closed() {
        let inner = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        seed_presence_history(
            &inner,
            "app-1",
            "presence-room",
            2,
            sockudo_core::history::now_ms(),
        )
        .await;
        let stateful_store = Arc::new(InspectablePresenceHistoryStore {
            inner,
            state: PresenceHistoryStreamRuntimeState {
                app_id: "app-1".to_string(),
                channel: "presence-room".to_string(),
                stream_id: Some("presence-stream-9".to_string()),
                durable_state: PresenceHistoryDurableState::Degraded,
                continuity_proven: false,
                reset_required: false,
                reason: Some("presence_history_write_failed".to_string()),
                node_id: Some("node-a".to_string()),
                last_transition_at_ms: Some(1234),
                authoritative_source: "test_state".to_string(),
                observed_source: "test_state".to_string(),
            },
        });
        let handler = test_presence_history_handler_with_store(100, stateful_store);
        let app = test_app();

        let response = channel_presence_history(
            Path(("app-1".to_string(), "presence-room".to_string())),
            Query(HistoryQuery {
                limit: Some(10),
                direction: Some("newest_first".to_string()),
                cursor: None,
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["continuity"]["degraded"].as_bool(), Some(true));
        assert_eq!(json["continuity"]["complete"].as_bool(), Some(false));
        assert_eq!(
            json["stream_state"]["durable_state"].as_str(),
            Some("degraded")
        );
        assert_eq!(
            json["stream_state"]["continuity_proven"].as_bool(),
            Some(false)
        );
    }

    #[tokio::test]
    async fn channel_presence_history_reset_rotates_stream_and_purges_history() {
        let inner = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));
        seed_presence_history(
            &inner,
            "app-1",
            "presence-room",
            2,
            sockudo_core::history::now_ms(),
        )
        .await;
        let tracked = Arc::new(TrackingPresenceHistoryStore::new(
            inner.clone(),
            None,
            "in_memory",
        ));
        let handler = test_presence_history_handler_with_store(100, tracked.clone());
        let app = test_app();

        let before = tracked
            .stream_inspection("app-1", "presence-room")
            .await
            .unwrap();
        let previous_stream_id = before.stream_id.clone().unwrap();

        let response = channel_presence_history_reset(
            Path(("app-1".to_string(), "presence-room".to_string())),
            Extension(app),
            State(handler),
            Json(HistoryResetRequestBody {
                confirm_channel: "presence-room".to_string(),
                confirm_operation: "reset".to_string(),
                reason: "operator cleanup".to_string(),
                requested_by: Some("ops".to_string()),
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["operation"].as_str(), Some("reset"));
        assert_eq!(
            json["previous_stream_id"].as_str(),
            Some(previous_stream_id.as_str())
        );
        assert_ne!(
            json["new_stream_id"].as_str(),
            Some(previous_stream_id.as_str())
        );
        assert_eq!(json["purged_events"].as_u64(), Some(2));
        assert_eq!(
            json["stream"]["retained"]["retained_events"].as_u64(),
            Some(0)
        );
        assert_eq!(
            json["stream"]["state"]["durable_state"].as_str(),
            Some("healthy")
        );
    }

    #[tokio::test]
    async fn dead_node_cleanup_replay_does_not_duplicate_presence_history_rows() {
        let app = test_app();
        let app_manager = Arc::new(MemoryAppManager::new());
        app_manager.create_app(app.clone()).await.unwrap();
        let adapter = Arc::new(LocalAdapter::new())
            as Arc<dyn sockudo_adapter::ConnectionManager + Send + Sync>;
        let cache = Arc::new(MemoryCacheManager::new(
            "test".to_string(),
            MemoryCacheOptions::default(),
        ));
        let store = Arc::new(MemoryPresenceHistoryStore::new(Default::default()));

        let mut options = sockudo_core::options::ServerOptions::default();
        options.presence_history.enabled = true;

        let handler = Arc::new(
            ConnectionHandlerBuilder::new(app_manager, adapter, cache, options)
                .presence_history_store(store.clone())
                .build(),
        );

        store
            .record_transition(PresenceHistoryTransitionRecord {
                app_id: "app-1".to_string(),
                channel: "presence-room".to_string(),
                event_kind: PresenceHistoryEventKind::MemberAdded,
                cause: PresenceHistoryEventCause::Join,
                user_id: "user-1".to_string(),
                connection_id: Some("socket-1".to_string()),
                user_info: Some(sonic_rs::json!({ "name": "Ada" })),
                dead_node_id: None,
                dedupe_key: "join-1".to_string(),
                published_at_ms: sockudo_core::history::now_ms(),
                retention: PresenceHistoryRetentionPolicy {
                    retention_window_seconds: 3600,
                    max_events_per_channel: None,
                    max_bytes_per_channel: None,
                },
            })
            .await
            .unwrap();

        let cleanup_event = sockudo_adapter::horizontal_adapter::DeadNodeEvent {
            dead_node_id: "dead-node".to_string(),
            orphaned_members: vec![sockudo_adapter::horizontal_adapter::OrphanedMember {
                app_id: "app-1".to_string(),
                channel: "presence-room".to_string(),
                user_id: "user-1".to_string(),
                user_info: Some(sonic_rs::json!({ "name": "Ada" })),
            }],
        };

        handler
            .handle_dead_node_cleanup(cleanup_event.clone())
            .await
            .unwrap();
        handler
            .handle_dead_node_cleanup(cleanup_event)
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: "app-1".to_string(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: PresenceHistoryQueryBounds::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
        assert_eq!(page.items[1].event, PresenceHistoryEventKind::MemberRemoved);
        assert_eq!(
            page.items[1].cause,
            PresenceHistoryEventCause::OrphanCleanup
        );
    }

    #[tokio::test]
    async fn channel_history_reset_rotates_stream_and_purges_history() {
        let (handler, store) = test_history_handler_with_memory_store(100);
        let app = test_app();

        handler
            .broadcast_to_channel(
                &app,
                "public-room",
                test_history_message("public-room", 1),
                None,
            )
            .await
            .unwrap();
        handler
            .broadcast_to_channel(
                &app,
                "public-room",
                test_history_message("public-room", 2),
                None,
            )
            .await
            .unwrap();

        let before = store
            .stream_inspection("app-1", "public-room")
            .await
            .unwrap();
        let previous_stream_id = before.stream_id.clone().unwrap();
        let response = channel_history_reset(
            Path(("app-1".to_string(), "public-room".to_string())),
            Extension(app.clone()),
            State(handler.clone()),
            Json(HistoryResetRequestBody {
                confirm_channel: "public-room".to_string(),
                confirm_operation: "reset".to_string(),
                reason: "operator cleanup".to_string(),
                requested_by: Some("ops".to_string()),
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["operation"].as_str(), Some("reset"));
        assert_eq!(
            json["previous_stream_id"].as_str(),
            Some(previous_stream_id.as_str())
        );
        assert_ne!(
            json["new_stream_id"].as_str(),
            Some(previous_stream_id.as_str())
        );
        assert_eq!(json["purged_messages"].as_u64(), Some(2));
        assert_eq!(
            json["stream"]["retained"]["retained_messages"].as_u64(),
            Some(0)
        );

        handler
            .broadcast_to_channel(
                &app,
                "public-room",
                test_history_message("public-room", 3),
                None,
            )
            .await
            .unwrap();
        let after = store
            .stream_inspection("app-1", "public-room")
            .await
            .unwrap();
        assert_ne!(
            after.stream_id.as_deref(),
            Some(previous_stream_id.as_str())
        );
        assert_eq!(after.next_serial, Some(2));
        assert_eq!(after.retained.retained_messages, 1);
    }

    #[tokio::test]
    async fn channel_history_purge_before_serial_advances_retained_floor_without_rotation() {
        let (handler, store) = test_history_handler_with_memory_store(100);
        let app = test_app();

        for index in 1..=4 {
            handler
                .broadcast_to_channel(
                    &app,
                    "public-room",
                    test_history_message("public-room", index),
                    None,
                )
                .await
                .unwrap();
        }

        let before = store
            .stream_inspection("app-1", "public-room")
            .await
            .unwrap();
        let response = channel_history_purge(
            Path(("app-1".to_string(), "public-room".to_string())),
            Extension(app),
            State(handler),
            Json(HistoryPurgeRequestBody {
                confirm_channel: "public-room".to_string(),
                confirm_operation: "purge".to_string(),
                mode: HistoryPurgeMode::BeforeSerial,
                before_serial: Some(3),
                before_time_ms: None,
                reason: "trim old backlog".to_string(),
                requested_by: Some("ops".to_string()),
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["operation"].as_str(), Some("purge"));
        assert_eq!(json["mode"].as_str(), Some("before_serial"));
        assert_eq!(json["purged_messages"].as_u64(), Some(2));
        assert_eq!(
            json["stream"]["retained"]["oldest_available_serial"].as_u64(),
            Some(3)
        );

        let after = store
            .stream_inspection("app-1", "public-room")
            .await
            .unwrap();
        assert_eq!(after.stream_id, before.stream_id);
        assert_eq!(after.retained.oldest_serial, Some(3));
        assert_eq!(after.retained.newest_serial, Some(4));
        assert_eq!(after.next_serial, before.next_serial);
    }

    #[tokio::test]
    async fn channel_history_reset_rejects_mismatched_confirmation() {
        let handler = test_history_handler(100);
        let app = test_app();

        let result = channel_history_reset(
            Path(("app-1".to_string(), "public-room".to_string())),
            Extension(app),
            State(handler),
            Json(HistoryResetRequestBody {
                confirm_channel: "wrong-room".to_string(),
                confirm_operation: "reset".to_string(),
                reason: "operator cleanup".to_string(),
                requested_by: None,
            }),
        )
        .await;

        match result {
            Ok(_) => panic!("expected mismatched confirmation to fail"),
            Err(AppError::InvalidInput(message)) => {
                assert!(message.contains("confirm_channel"));
            }
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn channel_history_returns_published_messages() {
        let handler = test_history_handler(100);
        let app = test_app();

        handler
            .broadcast_to_channel(
                &app,
                "public-room",
                test_history_message("public-room", 1),
                None,
            )
            .await
            .unwrap();

        let response = channel_history(
            Path(("app-1".to_string(), "public-room".to_string())),
            Query(HistoryQuery {
                limit: Some(10),
                direction: Some("newest_first".to_string()),
                cursor: None,
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["items"].as_array().unwrap().len(), 1);
        assert_eq!(
            json["items"][0]["message"]["event"].as_str(),
            Some("message-created-1")
        );
        assert_eq!(json["items"][0]["serial"].as_u64(), Some(1));
        assert_eq!(json["has_more"].as_bool(), Some(false));
        assert_eq!(
            json["continuity"]["stream_id"]
                .as_str()
                .map(|value| !value.is_empty()),
            Some(true)
        );
    }

    #[tokio::test]
    async fn channel_history_paginates_newest_first_and_oldest_first() {
        let handler = test_history_handler(2);
        let app = test_app();

        for index in 1..=4 {
            handler
                .broadcast_to_channel(
                    &app,
                    "public-room",
                    test_history_message("public-room", index),
                    None,
                )
                .await
                .unwrap();
        }

        let newest = channel_history(
            Path(("app-1".to_string(), "public-room".to_string())),
            Query(HistoryQuery {
                limit: Some(2),
                direction: Some("newest_first".to_string()),
                cursor: None,
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app.clone()),
            State(handler.clone()),
        )
        .await
        .unwrap()
        .into_response();
        let newest_json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(newest.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(newest_json["items"][0]["serial"].as_u64(), Some(4));
        assert_eq!(newest_json["items"][1]["serial"].as_u64(), Some(3));
        let newest_cursor = newest_json["next_cursor"].as_str().unwrap().to_string();

        let newest_page_2 = channel_history(
            Path(("app-1".to_string(), "public-room".to_string())),
            Query(HistoryQuery {
                limit: Some(2),
                direction: Some("newest_first".to_string()),
                cursor: Some(newest_cursor),
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app.clone()),
            State(handler.clone()),
        )
        .await
        .unwrap()
        .into_response();
        let newest_page_2_json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(newest_page_2.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(newest_page_2_json["items"][0]["serial"].as_u64(), Some(2));
        assert_eq!(newest_page_2_json["items"][1]["serial"].as_u64(), Some(1));

        let oldest = channel_history(
            Path(("app-1".to_string(), "public-room".to_string())),
            Query(HistoryQuery {
                limit: Some(2),
                direction: Some("oldest_first".to_string()),
                cursor: None,
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();
        let oldest_json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(oldest.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(oldest_json["items"][0]["serial"].as_u64(), Some(1));
        assert_eq!(oldest_json["items"][1]["serial"].as_u64(), Some(2));
    }

    #[tokio::test]
    async fn channel_history_rejects_invalid_cursor() {
        let handler = test_history_handler(2);
        let app = test_app();

        handler
            .broadcast_to_channel(
                &app,
                "public-room",
                test_history_message("public-room", 1),
                None,
            )
            .await
            .unwrap();

        let result = channel_history(
            Path(("app-1".to_string(), "public-room".to_string())),
            Query(HistoryQuery {
                limit: Some(2),
                direction: Some("newest_first".to_string()),
                cursor: Some("not-a-valid-cursor".to_string()),
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await;

        let response = match result {
            Ok(_) => panic!("expected invalid cursor request to fail"),
            Err(err) => err.into_response(),
        };

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn channel_history_filters_by_serial_range() {
        let handler = test_history_handler(100);
        let app = test_app();

        for index in 1..=5 {
            handler
                .broadcast_to_channel(
                    &app,
                    "public-room",
                    test_history_message("public-room", index),
                    None,
                )
                .await
                .unwrap();
        }

        let response = channel_history(
            Path(("app-1".to_string(), "public-room".to_string())),
            Query(HistoryQuery {
                limit: Some(10),
                direction: Some("oldest_first".to_string()),
                cursor: None,
                start_serial: Some(2),
                end_serial: Some(4),
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();
        let json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        assert_eq!(json["items"].as_array().unwrap().len(), 3);
        assert_eq!(json["items"][0]["serial"].as_u64(), Some(2));
        assert_eq!(json["items"][2]["serial"].as_u64(), Some(4));
        assert_eq!(json["bounds"]["start_serial"].as_u64(), Some(2));
        assert_eq!(json["bounds"]["end_serial"].as_u64(), Some(4));
    }

    #[tokio::test]
    async fn channel_history_large_read_is_bounded_and_fast() {
        let handler = test_history_handler(100);
        let app = test_app();

        for index in 1..=250 {
            handler
                .broadcast_to_channel(
                    &app,
                    "public-room",
                    test_history_message("public-room", index),
                    None,
                )
                .await
                .unwrap();
        }

        let started = Instant::now();
        let response = channel_history(
            Path(("app-1".to_string(), "public-room".to_string())),
            Query(HistoryQuery {
                limit: Some(100),
                direction: Some("newest_first".to_string()),
                cursor: None,
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();
        let elapsed = started.elapsed();
        let json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        assert_eq!(json["items"].as_array().unwrap().len(), 100);
        assert_eq!(json["has_more"].as_bool(), Some(true));
        assert!(elapsed.as_secs_f64() < 2.0);
    }

    #[tokio::test]
    async fn channel_history_rejects_when_history_is_disabled_by_app_policy() {
        let handler = test_history_handler(100);
        let app = test_app_with_policy(AppPolicy {
            history: Some(AppHistoryConfig {
                enabled: Some(false),
                rewind_enabled: None,
                retention_window_seconds: None,
                max_messages_per_channel: None,
                max_bytes_per_channel: None,
            }),
            ..Default::default()
        });

        let response = channel_history(
            Path(("app-1".to_string(), "public-room".to_string())),
            Query(HistoryQuery {
                limit: Some(10),
                direction: Some("newest_first".to_string()),
                cursor: None,
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await;

        let response = match response {
            Ok(_) => panic!("expected policy-disabled history request to fail"),
            Err(err) => err.into_response(),
        };

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn namespace_history_retention_override_beats_app_default() {
        let handler = test_history_handler(100);
        let app = test_app_with_policy(AppPolicy {
            history: Some(AppHistoryConfig {
                enabled: Some(true),
                rewind_enabled: None,
                retention_window_seconds: None,
                max_messages_per_channel: Some(1),
                max_bytes_per_channel: None,
            }),
            channels: AppChannelsPolicy {
                channel_namespaces: Some(vec![ChannelNamespace {
                    name: "chat".to_string(),
                    channel_name_pattern: None,
                    max_channel_name_length: None,
                    annotations_enabled: None,
                    allow_user_limited_channels: None,
                    allow_subscribe_for_client: None,
                    allow_publish_for_client: None,
                    allow_presence_for_client: None,
                    history: Some(NamespaceHistoryConfig {
                        rewind_enabled: None,
                        retention_window_seconds: None,
                        max_messages_per_channel: Some(3),
                        max_bytes_per_channel: None,
                    }),
                    presence_history: None,
                }]),
                ..Default::default()
            },
            ..Default::default()
        });

        for index in 1..=3 {
            handler
                .broadcast_to_channel(
                    &app,
                    "public-room",
                    test_history_message("public-room", index),
                    None,
                )
                .await
                .unwrap();
            handler
                .broadcast_to_channel(
                    &app,
                    "chat:room-1",
                    test_history_message("chat:room-1", index),
                    None,
                )
                .await
                .unwrap();
        }

        let public_response = channel_history(
            Path(("app-1".to_string(), "public-room".to_string())),
            Query(HistoryQuery {
                limit: Some(10),
                direction: Some("oldest_first".to_string()),
                cursor: None,
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app.clone()),
            State(handler.clone()),
        )
        .await
        .unwrap()
        .into_response();
        let public_json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(public_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        let namespaced_response = channel_history(
            Path(("app-1".to_string(), "chat:room-1".to_string())),
            Query(HistoryQuery {
                limit: Some(10),
                direction: Some("oldest_first".to_string()),
                cursor: None,
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();
        let namespaced_json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(namespaced_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        assert_eq!(public_json["items"].as_array().unwrap().len(), 1);
        assert_eq!(namespaced_json["items"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn channel_presence_history_returns_presence_events() {
        let (handler, store) = test_presence_history_handler_with_memory_store(100);
        let app = test_app();
        let base_ts = sockudo_core::history::now_ms();
        seed_presence_history(&store, "app-1", "presence-room", 2, base_ts).await;

        let response = channel_presence_history(
            Path(("app-1".to_string(), "presence-room".to_string())),
            Query(HistoryQuery {
                limit: Some(10),
                direction: Some("newest_first".to_string()),
                cursor: None,
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["items"].as_array().unwrap().len(), 2);
        assert_eq!(json["items"][0]["event"].as_str(), Some("member_removed"));
        assert_eq!(
            json["items"][0]["presence_event"]["user_id"].as_str(),
            Some("user-2")
        );
        assert_eq!(
            json["continuity"]["stream_id"]
                .as_str()
                .map(|value| !value.is_empty()),
            Some(true)
        );
        assert_eq!(json["continuity"]["retained_events"].as_u64(), Some(2));
    }

    #[tokio::test]
    async fn channel_presence_history_paginates_newest_first_and_oldest_first() {
        let (handler, store) = test_presence_history_handler_with_memory_store(2);
        let app = test_app();
        let base_ts = sockudo_core::history::now_ms();
        seed_presence_history(&store, "app-1", "presence-room", 4, base_ts).await;

        let newest = channel_presence_history(
            Path(("app-1".to_string(), "presence-room".to_string())),
            Query(HistoryQuery {
                limit: Some(2),
                direction: Some("newest_first".to_string()),
                cursor: None,
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app.clone()),
            State(handler.clone()),
        )
        .await
        .unwrap()
        .into_response();
        let newest_json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(newest.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(newest_json["items"][0]["serial"].as_u64(), Some(4));
        assert_eq!(newest_json["items"][1]["serial"].as_u64(), Some(3));
        let newest_cursor = newest_json["next_cursor"].as_str().unwrap().to_string();

        let newest_page_2 = channel_presence_history(
            Path(("app-1".to_string(), "presence-room".to_string())),
            Query(HistoryQuery {
                limit: Some(2),
                direction: Some("newest_first".to_string()),
                cursor: Some(newest_cursor),
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app.clone()),
            State(handler.clone()),
        )
        .await
        .unwrap()
        .into_response();
        let newest_page_2_json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(newest_page_2.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(newest_page_2_json["items"][0]["serial"].as_u64(), Some(2));
        assert_eq!(newest_page_2_json["items"][1]["serial"].as_u64(), Some(1));

        let oldest = channel_presence_history(
            Path(("app-1".to_string(), "presence-room".to_string())),
            Query(HistoryQuery {
                limit: Some(2),
                direction: Some("oldest_first".to_string()),
                cursor: None,
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();
        let oldest_json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(oldest.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(oldest_json["items"][0]["serial"].as_u64(), Some(1));
        assert_eq!(oldest_json["items"][1]["serial"].as_u64(), Some(2));
    }

    #[tokio::test]
    async fn channel_presence_history_filters_by_serial_and_time() {
        let (handler, store) = test_presence_history_handler_with_memory_store(100);
        let app = test_app();
        let base_ts = sockudo_core::history::now_ms();
        seed_presence_history(&store, "app-1", "presence-room", 5, base_ts).await;

        let response = channel_presence_history(
            Path(("app-1".to_string(), "presence-room".to_string())),
            Query(HistoryQuery {
                limit: Some(10),
                direction: Some("oldest_first".to_string()),
                cursor: None,
                start_serial: Some(2),
                end_serial: Some(4),
                start_time_ms: Some(base_ts + 2),
                end_time_ms: Some(base_ts + 4),
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();
        let json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        assert_eq!(json["items"].as_array().unwrap().len(), 3);
        assert_eq!(json["items"][0]["serial"].as_u64(), Some(2));
        assert_eq!(json["items"][2]["serial"].as_u64(), Some(4));
        assert_eq!(json["bounds"]["start_serial"].as_u64(), Some(2));
        assert_eq!(json["bounds"]["end_serial"].as_u64(), Some(4));
    }

    #[tokio::test]
    async fn channel_presence_history_rejects_non_presence_channels() {
        let handler = test_presence_history_handler(100);
        let app = test_app();

        let response = channel_presence_history(
            Path(("app-1".to_string(), "public-room".to_string())),
            Query(HistoryQuery {
                limit: Some(10),
                direction: Some("newest_first".to_string()),
                cursor: None,
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await;

        let response = match response {
            Ok(_) => panic!("expected non-presence channel request to fail"),
            Err(err) => err.into_response(),
        };

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn channel_presence_history_rejects_cursor_with_mismatched_bounds() {
        let (handler, store) = test_presence_history_handler_with_memory_store(2);
        let app = test_app();
        let base_ts = sockudo_core::history::now_ms();
        seed_presence_history(&store, "app-1", "presence-room", 3, base_ts).await;

        let first_page = channel_presence_history(
            Path(("app-1".to_string(), "presence-room".to_string())),
            Query(HistoryQuery {
                limit: Some(2),
                direction: Some("newest_first".to_string()),
                cursor: None,
                start_serial: None,
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app.clone()),
            State(handler.clone()),
        )
        .await
        .unwrap()
        .into_response();
        let first_page_json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(first_page.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let cursor = first_page_json["next_cursor"].as_str().unwrap().to_string();

        let response = channel_presence_history(
            Path(("app-1".to_string(), "presence-room".to_string())),
            Query(HistoryQuery {
                limit: Some(2),
                direction: Some("newest_first".to_string()),
                cursor: Some(cursor),
                start_serial: Some(2),
                end_serial: None,
                start_time_ms: None,
                end_time_ms: None,
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await;

        let response = match response {
            Ok(_) => panic!("expected mismatched cursor bounds to fail"),
            Err(err) => err.into_response(),
        };

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn channel_presence_history_snapshot_reconstructs_membership() {
        let (handler, store) = test_presence_history_handler_with_memory_store(100);
        let app = test_app();
        let base_ts = sockudo_core::history::now_ms();

        // Seed: u1 joins, u2 joins, u1 leaves
        seed_presence_history(&store, "app-1", "presence-room", 3, base_ts).await;

        let response = channel_presence_history_snapshot(
            Path(("app-1".to_string(), "presence-room".to_string())),
            Query(PresenceSnapshotQuery::default()),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 64)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["channel"].as_str(), Some("presence-room"));
        assert!(json["member_count"].as_u64().unwrap() > 0);
        assert!(json["events_replayed"].as_u64().unwrap() > 0);
        assert!(json["continuity"]["stream_id"].as_str().is_some());
    }

    #[tokio::test]
    async fn channel_presence_history_snapshot_at_serial_bound() {
        let (handler, store) = test_presence_history_handler_with_memory_store(100);
        let app = test_app();
        let base_ts = sockudo_core::history::now_ms();

        // Seed: alternating joins/leaves for u1, u2
        seed_presence_history(&store, "app-1", "presence-room", 4, base_ts).await;

        // Snapshot at serial 2 only replays events with serial <= 2
        let response = channel_presence_history_snapshot(
            Path(("app-1".to_string(), "presence-room".to_string())),
            Query(PresenceSnapshotQuery {
                at_serial: Some(2),
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 64)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["snapshot_serial"].as_u64(), Some(2));
    }

    #[tokio::test]
    async fn channel_presence_history_snapshot_rejects_non_presence_channel() {
        let handler = test_presence_history_handler(100);
        let app = test_app();

        let response = channel_presence_history_snapshot(
            Path(("app-1".to_string(), "private-room".to_string())),
            Query(PresenceSnapshotQuery::default()),
            Extension(app),
            State(handler),
        )
        .await;

        let response = match response {
            Ok(_) => panic!("expected non-presence channel to fail"),
            Err(err) => err.into_response(),
        };
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn error_responses_include_code_and_status_fields() {
        let handler = test_presence_history_handler(100);
        let app = test_app();

        // Non-presence channel triggers BAD_REQUEST
        let response = channel_presence_history(
            Path(("app-1".to_string(), "public-room".to_string())),
            Query(HistoryQuery::default()),
            Extension(app),
            State(handler),
        )
        .await;

        let response = match response {
            Ok(_) => panic!("expected error"),
            Err(err) => err.into_response(),
        };
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert!(
            json["code"].as_str().is_some(),
            "error must include 'code' field"
        );
        assert!(
            json["status"].as_u64().is_some(),
            "error must include 'status' field"
        );
        assert!(
            json["error"].as_str().is_some(),
            "error must include 'error' field"
        );
    }

    #[tokio::test]
    async fn continuity_includes_degraded_field() {
        let (handler, store) = test_presence_history_handler_with_memory_store(100);
        let app = test_app();
        let base_ts = sockudo_core::history::now_ms();
        seed_presence_history(&store, "app-1", "presence-room", 3, base_ts).await;

        let response = channel_presence_history(
            Path(("app-1".to_string(), "presence-room".to_string())),
            Query(HistoryQuery {
                limit: Some(10),
                direction: Some("newest_first".to_string()),
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert!(
            json["continuity"]["degraded"].is_boolean(),
            "continuity must include 'degraded' boolean field"
        );
    }

    #[tokio::test]
    async fn ably_time_aliases_resolve_correctly() {
        let (handler, store) = test_presence_history_handler_with_memory_store(100);
        let app = test_app();
        let base_ts = sockudo_core::history::now_ms();
        seed_presence_history(&store, "app-1", "presence-room", 5, base_ts).await;

        // Use start/end (Ably aliases) instead of start_time_ms/end_time_ms
        let response = channel_presence_history(
            Path(("app-1".to_string(), "presence-room".to_string())),
            Query(HistoryQuery {
                limit: Some(10),
                direction: Some("oldest_first".to_string()),
                start: Some(base_ts + 2),
                end: Some(base_ts + 4),
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        let items = json["items"].as_array().unwrap();
        // All returned items should fall within the time bounds
        for item in items {
            let ts = item["published_at_ms"].as_i64().unwrap();
            assert!(
                ts >= base_ts + 2 && ts <= base_ts + 4,
                "item at ts={ts} outside Ably alias bounds [{}, {}]",
                base_ts + 2,
                base_ts + 4
            );
        }
    }

    #[tokio::test]
    async fn channel_message_returns_latest_versioned_item() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_versioned_handler_with_store(100, store.clone());
        let app = test_app();

        let original = test_versioned_record(
            "msg:1",
            "00000000000000000001:test:00000000000000000001",
            10,
            1,
            "hello",
        );
        store.append_version(original.clone()).await.unwrap();
        store
            .append_version(StoredVersionRecord {
                message: original
                    .message
                    .apply_mutation(
                        sockudo_core::versioned_messages::MessageAction::Update,
                        VersionMetadata {
                            serial: VersionSerial::new(
                                "00000000000000000002:test:00000000000000000002".to_string(),
                            )
                            .unwrap(),
                            client_id: Some("user-1".to_string()),
                            timestamp_ms: 2,
                            description: Some("patched".to_string()),
                            metadata: None,
                        },
                        2,
                        sockudo_core::versioned_messages::MessageFieldDelta {
                            data: sockudo_core::versioned_messages::FieldPatch::Replace(
                                MessageData::String("{\"text\":\"patched\"}".to_string()),
                            ),
                            ..Default::default()
                        },
                    )
                    .unwrap(),
                ..original
            })
            .await
            .unwrap();

        let response = channel_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["item"]["message_serial"], "msg:1");
        assert_eq!(json["item"]["action"], "update");
        assert_eq!(
            json["item"]["version"]["serial"],
            "00000000000000000002:test:00000000000000000002"
        );
        assert_eq!(json["item"]["event"], "sockudo:message.update");
    }

    #[tokio::test]
    async fn events_create_then_update_substitutes_latest_visible_history() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_versioned_handler_with_store(100, store.clone());
        let app = test_app();

        let create_response = events(
            Path("app-1".to_string()),
            Query(EventQuery {
                auth_key: String::new(),
                auth_timestamp: String::new(),
                auth_version: String::new(),
                body_md5: String::new(),
                auth_signature: String::new(),
            }),
            Extension(app.clone()),
            #[cfg(feature = "push")]
            test_push_store(),
            #[cfg(feature = "push")]
            test_push_queue(),
            State(handler.clone()),
            HeaderMap::new(),
            Uri::from_static("/apps/app-1/events"),
            RawQuery(None),
            Json(PusherApiMessage {
                name: Some("chat.message".to_string()),
                data: Some(ApiMessageData::Json(json!({"text": "hello"}))),
                channel: Some("versioned-room".to_string()),
                channels: None,
                socket_id: None,
                info: None,
                tags: None,
                delta: None,
                idempotency_key: None,
                message_id: None,
                extras: None,
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(create_response.status(), StatusCode::OK);

        let latest = handler
            .version_store()
            .latest_by_history("app-1", "versioned-room")
            .await
            .unwrap();
        assert_eq!(latest.len(), 1);
        let message_serial = latest[0].message_serial().as_str().to_string();

        let update_response = update_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: message_serial.clone(),
            }),
            Extension(app.clone()),
            State(handler.clone()),
            Json(UpdateMessageRequest {
                name: None,
                data: Some(MessageData::String("{\"text\":\"patched\"}".to_string())),
                extras: None,
                clear_fields: Vec::new(),
                client_id: None,
                socket_id: None,
                description: Some("patch".to_string()),
                metadata: None,
                op_id: None,
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(update_response.status(), StatusCode::OK);

        let history_response = channel_history(
            Path(("app-1".to_string(), "versioned-room".to_string())),
            Query(HistoryQuery {
                limit: Some(10),
                direction: Some("oldest_first".to_string()),
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(history_response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(history_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["items"].as_array().unwrap().len(), 1);
        assert_eq!(json["items"][0]["serial"], 1);
        assert_eq!(
            json["items"][0]["message"]["message_serial"],
            message_serial
        );
        assert_eq!(json["items"][0]["message"]["action"], "update");
        assert_eq!(
            json["items"][0]["message"]["data"],
            "{\"text\":\"patched\"}"
        );
        assert_eq!(json["items"][0]["event_name"], "sockudo:message.update");
        assert_eq!(json["items"][0]["operation_kind"], "message.update");
    }

    #[tokio::test]
    async fn events_create_then_delete_substitutes_latest_visible_history() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_versioned_handler_with_store(100, store.clone());
        let app = test_app();

        let create_response = events(
            Path("app-1".to_string()),
            Query(EventQuery {
                auth_key: String::new(),
                auth_timestamp: String::new(),
                auth_version: String::new(),
                body_md5: String::new(),
                auth_signature: String::new(),
            }),
            Extension(app.clone()),
            #[cfg(feature = "push")]
            test_push_store(),
            #[cfg(feature = "push")]
            test_push_queue(),
            State(handler.clone()),
            HeaderMap::new(),
            Uri::from_static("/apps/app-1/events"),
            RawQuery(None),
            Json(PusherApiMessage {
                name: Some("chat.message".to_string()),
                data: Some(ApiMessageData::Json(json!({"text": "hello"}))),
                channel: Some("versioned-room".to_string()),
                channels: None,
                socket_id: None,
                info: None,
                tags: None,
                delta: None,
                idempotency_key: None,
                message_id: None,
                extras: None,
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(create_response.status(), StatusCode::OK);

        let latest = handler
            .version_store()
            .latest_by_history("app-1", "versioned-room")
            .await
            .unwrap();
        let message_serial = latest[0].message_serial().as_str().to_string();

        let delete_response = delete_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: message_serial.clone(),
            }),
            Extension(app.clone()),
            State(handler.clone()),
            Json(DeleteMessageRequest {
                data: None,
                extras: None,
                clear_fields: vec![
                    sockudo_protocol::versioned_messages::ClearField::Data,
                    sockudo_protocol::versioned_messages::ClearField::Extras,
                ],
                client_id: None,
                socket_id: None,
                description: Some("deleted".to_string()),
                metadata: None,
                op_id: None,
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(delete_response.status(), StatusCode::OK);

        let history_response = channel_history(
            Path(("app-1".to_string(), "versioned-room".to_string())),
            Query(HistoryQuery {
                limit: Some(10),
                direction: Some("oldest_first".to_string()),
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        let body = axum::body::to_bytes(history_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["items"][0]["serial"], 1);
        assert_eq!(
            json["items"][0]["message"]["message_serial"],
            message_serial
        );
        assert_eq!(json["items"][0]["message"]["action"], "delete");
        assert!(json["items"][0]["message"]["data"].is_null());
        assert_eq!(json["items"][0]["event_name"], "sockudo:message.delete");
        assert_eq!(json["items"][0]["operation_kind"], "message.delete");
    }

    #[tokio::test]
    async fn events_create_then_append_substitutes_latest_visible_history() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_versioned_handler_with_store(100, store.clone());
        let app = test_app();

        let create_response = events(
            Path("app-1".to_string()),
            Query(EventQuery {
                auth_key: String::new(),
                auth_timestamp: String::new(),
                auth_version: String::new(),
                body_md5: String::new(),
                auth_signature: String::new(),
            }),
            Extension(app.clone()),
            #[cfg(feature = "push")]
            test_push_store(),
            #[cfg(feature = "push")]
            test_push_queue(),
            State(handler.clone()),
            HeaderMap::new(),
            Uri::from_static("/apps/app-1/events"),
            RawQuery(None),
            Json(PusherApiMessage {
                name: Some("chat.message".to_string()),
                data: Some(ApiMessageData::String("hello".to_string())),
                channel: Some("versioned-room".to_string()),
                channels: None,
                socket_id: None,
                info: None,
                tags: None,
                delta: None,
                idempotency_key: None,
                message_id: None,
                extras: None,
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(create_response.status(), StatusCode::OK);

        let latest = handler
            .version_store()
            .latest_by_history("app-1", "versioned-room")
            .await
            .unwrap();
        let message_serial = latest[0].message_serial().as_str().to_string();

        let append_response = append_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: message_serial.clone(),
            }),
            Extension(app.clone()),
            State(handler.clone()),
            Json(AppendMessageRequest {
                data: " world".to_string(),
                extras: None,
                client_id: None,
                socket_id: None,
                description: Some("append".to_string()),
                metadata: None,
                op_id: None,
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(append_response.status(), StatusCode::OK);

        let history_response = channel_history(
            Path(("app-1".to_string(), "versioned-room".to_string())),
            Query(HistoryQuery {
                limit: Some(10),
                direction: Some("oldest_first".to_string()),
                ..Default::default()
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        let body = axum::body::to_bytes(history_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["items"][0]["serial"], 1);
        assert_eq!(
            json["items"][0]["message"]["message_serial"],
            message_serial
        );
        assert_eq!(json["items"][0]["message"]["action"], "append");
        assert_eq!(json["items"][0]["message"]["data"], "hello world");
        assert_eq!(json["items"][0]["event_name"], "sockudo:message.append");
        assert_eq!(json["items"][0]["operation_kind"], "message.append");
    }

    #[tokio::test]
    async fn ai_http_publish_returns_serial_ack_and_message_id_dedupes() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_ai_versioned_handler_with_store(100, store.clone(), 1024, 4096, 1024);
        let app = test_app();

        let first = publish_ai_http_event(
            handler.clone(),
            app.clone(),
            "ai-output",
            Some("sdk-msg-1"),
            None,
            "streaming",
            "hello",
        )
        .await;
        let first_ack = &first["channels"]["versioned-room"];
        assert!(first_ack["message_serial"].as_str().is_some());
        assert_eq!(first_ack["history_serial"].as_u64(), Some(1));
        assert_eq!(first_ack["delivery_serial"].as_u64(), Some(1));
        assert!(first_ack["version_serial"].as_str().is_some());

        let duplicate = publish_ai_http_event(
            handler.clone(),
            app,
            "ai-output",
            Some("sdk-msg-1"),
            None,
            "streaming",
            "hello again",
        )
        .await;
        assert_eq!(
            duplicate["channels"]["versioned-room"],
            first["channels"]["versioned-room"]
        );

        let latest = store
            .latest_by_history("app-1", "versioned-room")
            .await
            .unwrap();
        assert_eq!(latest.len(), 1);
        assert_eq!(
            latest[0].message.data.as_ref().unwrap().as_string(),
            Some("hello")
        );
    }

    #[tokio::test]
    async fn ai_http_publish_idempotency_key_header_returns_cached_serial_ack() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_ai_versioned_handler_with_store(100, store.clone(), 1024, 4096, 1024);
        let app = test_app();

        let first = publish_ai_http_event(
            handler.clone(),
            app.clone(),
            "ai-turn-start",
            None,
            Some("turn-start-request-1"),
            "streaming",
            "{\"turn\":\"1\"}",
        )
        .await;
        let duplicate = publish_ai_http_event(
            handler,
            app,
            "ai-turn-start",
            None,
            Some("turn-start-request-1"),
            "streaming",
            "{\"turn\":\"1-retry\"}",
        )
        .await;

        assert_eq!(
            duplicate["channels"]["versioned-room"],
            first["channels"]["versioned-room"]
        );
        assert_eq!(
            store
                .latest_by_history("app-1", "versioned-room")
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[cfg(feature = "push")]
    #[tokio::test]
    async fn matching_channel_push_rule_accepts_existing_channel_subscription_fanout() {
        let handler = test_handler_with_push_rules(vec![sockudo_core::options::PushRuleConfig {
            channel_pattern: "notifications:*".to_string(),
            event_filter: vec!["agent-complete".to_string()],
            ..Default::default()
        }]);
        let app = test_notifications_app();
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());
        let device = test_push_device("device-1");
        store.upsert_device(device.clone()).await.unwrap();
        store
            .upsert_subscription(ChannelSubscription::from_device(
                "notifications:user-1",
                &device,
            ))
            .await
            .unwrap();

        let response = events(
            Path("app-1".to_string()),
            Query(empty_event_query()),
            Extension(app),
            #[cfg(feature = "push")]
            Extension(store.clone() as sockudo_push::DynPushStore),
            #[cfg(feature = "push")]
            Extension(queue.clone() as sockudo_push::DynPushQueue),
            State(handler),
            HeaderMap::new(),
            Uri::from_static("/apps/app-1/events"),
            RawQuery(None),
            Json(PusherApiMessage {
                name: Some("agent-complete".to_string()),
                data: Some(ApiMessageData::Json(json!({
                    "title": "Agent complete",
                    "body": "Your answer is ready",
                    "sessionId": "sess-1"
                }))),
                channel: Some("notifications:user-1".to_string()),
                channels: None,
                socket_id: None,
                info: None,
                tags: None,
                delta: None,
                idempotency_key: None,
                message_id: None,
                extras: None,
            }),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let mut log_event = None;
        for _ in 0..20 {
            let page = store
                .list_publish_log_events("app-1", 10, None)
                .await
                .unwrap();
            if let Some(event) = page.items.into_iter().next() {
                log_event = Some(event);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let log_event = log_event.expect("rule-triggered push publish log event");
        assert_eq!(log_event.expected_recipients, 1);
        assert_eq!(
            log_event.intent.targets,
            vec![sockudo_push::PublishTarget::Channel {
                channel: "notifications:user-1".to_string()
            }]
        );
        assert_eq!(
            log_event.intent.payload.title.as_deref(),
            Some("Agent complete")
        );
        assert_eq!(
            log_event.intent.payload.template_data["data"]["sessionId"],
            "sess-1"
        );
    }

    #[cfg(feature = "push")]
    #[tokio::test]
    async fn non_matching_channel_push_rule_does_not_enqueue_push() {
        let handler = test_handler_with_push_rules(vec![sockudo_core::options::PushRuleConfig {
            channel_pattern: "notifications:*".to_string(),
            event_filter: vec!["agent-complete".to_string()],
            ..Default::default()
        }]);
        let app = test_notifications_app();
        let store = Arc::new(MemoryPushStore::new());
        let queue = Arc::new(MemoryPushQueue::new());

        let response = events(
            Path("app-1".to_string()),
            Query(empty_event_query()),
            Extension(app),
            #[cfg(feature = "push")]
            Extension(store.clone() as sockudo_push::DynPushStore),
            #[cfg(feature = "push")]
            Extension(queue.clone() as sockudo_push::DynPushQueue),
            State(handler),
            HeaderMap::new(),
            Uri::from_static("/apps/app-1/events"),
            RawQuery(None),
            Json(PusherApiMessage {
                name: Some("agent-start".to_string()),
                data: Some(ApiMessageData::Json(json!({
                    "title": "Agent started",
                    "body": "Working",
                    "sessionId": "sess-1"
                }))),
                channel: Some("notifications:user-1".to_string()),
                channels: None,
                socket_id: None,
                info: None,
                tags: None,
                delta: None,
                idempotency_key: None,
                message_id: None,
                extras: None,
            }),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let page = store
            .list_publish_log_events("app-1", 10, None)
            .await
            .unwrap();
        assert!(page.items.is_empty());
    }

    #[tokio::test]
    async fn ai_batch_http_publish_returns_serial_acks_in_request_order() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_ai_versioned_handler_with_store(100, store.clone(), 1024, 4096, 1024);
        let app = test_app();

        let response = batch_events(
            Path("app-1".to_string()),
            Query(empty_event_query()),
            Extension(app),
            #[cfg(feature = "push")]
            test_push_store(),
            #[cfg(feature = "push")]
            test_push_queue(),
            State(handler),
            HeaderMap::new(),
            Uri::from_static("/apps/app-1/batch_events"),
            RawQuery(None),
            Json(BatchPusherApiMessage {
                batch: vec![
                    PusherApiMessage {
                        name: Some("ai-output".to_string()),
                        data: Some(ApiMessageData::String("first".to_string())),
                        channel: Some("versioned-room".to_string()),
                        channels: None,
                        socket_id: None,
                        info: None,
                        tags: None,
                        delta: None,
                        idempotency_key: None,
                        message_id: Some("batch-msg-1".to_string()),
                        extras: Some(ai_extras("streaming")),
                    },
                    PusherApiMessage {
                        name: Some("ai-output".to_string()),
                        data: Some(ApiMessageData::String("second".to_string())),
                        channel: Some("versioned-room".to_string()),
                        channels: None,
                        socket_id: None,
                        info: None,
                        tags: None,
                        delta: None,
                        idempotency_key: None,
                        message_id: Some("batch-msg-2".to_string()),
                        extras: Some(ai_extras("complete")),
                    },
                ],
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(json["batch"].as_array().unwrap().len(), 2);
        assert_eq!(json["batch"][0]["history_serial"].as_u64(), Some(1));
        assert_eq!(json["batch"][0]["delivery_serial"].as_u64(), Some(1));
        assert_eq!(json["batch"][1]["history_serial"].as_u64(), Some(2));
        assert_eq!(json["batch"][1]["delivery_serial"].as_u64(), Some(2));
        assert_ne!(
            json["batch"][0]["message_serial"],
            json["batch"][1]["message_serial"]
        );
        assert_eq!(
            store
                .latest_by_history("app-1", "versioned-room")
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn append_message_op_id_replay_returns_duplicate_serial_ack() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_ai_versioned_handler_with_store(100, store.clone(), 1024, 4096, 1024);
        let app = test_app();

        store
            .append_version(test_versioned_record(
                "msg:1",
                "00000000000000000001:test:00000000000000000001",
                10,
                1,
                "hello",
            ))
            .await
            .unwrap();

        let request = AppendMessageRequest {
            data: " world".to_string(),
            extras: Some(ai_extras("complete")),
            client_id: None,
            socket_id: None,
            description: Some("append".to_string()),
            metadata: None,
            op_id: Some("append-op-1".to_string()),
        };

        let first = append_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app.clone()),
            State(handler.clone()),
            Json(request.clone()),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);
        let first_json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(first.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first_json["status"], "applied");
        assert_eq!(first_json["history_serial"].as_u64(), Some(10));
        assert_eq!(first_json["delivery_serial"].as_u64(), Some(2));

        let duplicate = append_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app),
            State(handler),
            Json(request),
        )
        .await
        .unwrap()
        .into_response();
        let duplicate_json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(duplicate.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        assert_eq!(duplicate_json["status"], "duplicate");
        assert_eq!(
            duplicate_json["version_serial"],
            first_json["version_serial"]
        );
        assert_eq!(
            duplicate_json["history_serial"],
            first_json["history_serial"]
        );
        assert_eq!(
            duplicate_json["delivery_serial"],
            first_json["delivery_serial"]
        );
        assert_eq!(
            store
                .get_versions(VersionStoreReadRequest {
                    app_id: "app-1".to_string(),
                    channel: "versioned-room".to_string(),
                    message_serial: MessageSerial::new("msg:1").unwrap(),
                    direction: VersionStoreDirection::OldestFirst,
                    limit: 10,
                    cursor: None,
                })
                .await
                .unwrap()
                .items
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn channel_state_includes_ai_block_for_ai_transport_channels() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_ai_versioned_handler_with_store(100, store, 1024, 4096, 1024);
        let app = test_app();

        publish_ai_http_event(
            handler.clone(),
            app.clone(),
            "ai-output",
            Some("sdk-msg-streaming"),
            None,
            "streaming",
            "streaming",
        )
        .await;
        publish_ai_http_event(
            handler.clone(),
            app.clone(),
            "ai-output",
            Some("sdk-msg-complete"),
            None,
            "complete",
            "complete",
        )
        .await;

        let response = channel(
            Path(("app-1".to_string(), "versioned-room".to_string())),
            Query(ChannelQuery {
                info: Some("subscription_count".to_string()),
                auth_params: empty_event_query(),
            }),
            Extension(app),
            State(handler),
            Uri::from_static("/apps/app-1/channels/versioned-room?info=subscription_count"),
            RawQuery(Some("info=subscription_count".to_string())),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let json: Value = sonic_rs::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(json["ai"]["active_streams"].as_u64(), Some(1));
        assert_eq!(json["ai"]["last_history_serial"].as_u64(), Some(2));
        assert_eq!(json["ai"]["message_count"].as_u64(), Some(2));
    }

    #[tokio::test]
    async fn append_message_rejects_ai_accumulated_content_over_cap() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_ai_versioned_handler_with_store(100, store.clone(), 8, 4096, 1024);
        let app = test_app();

        store
            .append_version(test_versioned_record(
                "msg:1",
                "00000000000000000001:test:00000000000000000001",
                10,
                1,
                "hello",
            ))
            .await
            .unwrap();

        let response = match append_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app),
            State(handler),
            Json(AppendMessageRequest {
                data: " oversized".to_string(),
                extras: None,
                client_id: None,
                socket_id: None,
                description: Some("too large".to_string()),
                metadata: None,
                op_id: None,
            }),
        )
        .await
        {
            Err(err) => err.into_response(),
            Ok(_) => panic!("expected accumulated content cap rejection"),
        };

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["ai_code"], 40009);
        assert_eq!(json["code"], "payload_too_large");
    }

    #[tokio::test]
    async fn append_message_rejects_ai_append_count_over_cap() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_ai_versioned_handler_with_store(100, store.clone(), 1024, 1, 1024);
        let app = test_app();
        let original = test_versioned_record(
            "msg:1",
            "00000000000000000001:test:00000000000000000001",
            10,
            1,
            "hello",
        );
        let first_append = StoredVersionRecord {
            message: original
                .message
                .apply_append(
                    VersionMetadata {
                        serial: VersionSerial::new(
                            "00000000000000000002:test:00000000000000000002".to_string(),
                        )
                        .unwrap(),
                        client_id: Some("user-1".to_string()),
                        timestamp_ms: 2,
                        description: None,
                        metadata: None,
                    },
                    2,
                    MessageAppend {
                        data_fragment: "a".to_string(),
                        extras: None,
                    },
                )
                .unwrap(),
            ..original.clone()
        };
        store.append_version(original).await.unwrap();
        store.append_version(first_append).await.unwrap();

        let response = match append_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app),
            State(handler),
            Json(AppendMessageRequest {
                data: "b".to_string(),
                extras: None,
                client_id: None,
                socket_id: None,
                description: Some("over cap".to_string()),
                metadata: None,
                op_id: None,
            }),
        )
        .await
        {
            Err(err) => err.into_response(),
            Ok(_) => panic!("expected append count cap rejection"),
        };

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["ai_code"], 40009);
    }

    #[tokio::test]
    async fn append_message_persists_terminal_status_in_latest_record() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_ai_versioned_handler_with_store(100, store.clone(), 1024, 4096, 1024);
        let app = test_app();

        store
            .append_version(test_versioned_record(
                "msg:1",
                "00000000000000000001:test:00000000000000000001",
                10,
                1,
                "hello",
            ))
            .await
            .unwrap();

        let extras = MessageExtras {
            ai: Some(sockudo_protocol::messages::AiExtras {
                transport: Some(HashMap::from([(
                    "status".to_string(),
                    "complete".to_string(),
                )])),
                codec: None,
            }),
            ..Default::default()
        };

        let response = append_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app),
            State(handler.clone()),
            Json(AppendMessageRequest {
                data: " world".to_string(),
                extras: Some(extras),
                client_id: None,
                socket_id: None,
                description: Some("terminal append".to_string()),
                metadata: None,
                op_id: None,
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let latest = handler
            .version_store()
            .get_latest(
                "app-1",
                "versioned-room",
                &MessageSerial::new("msg:1").unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            latest
                .message
                .extras
                .as_ref()
                .and_then(|extras| extras.ai_transport_headers())
                .and_then(|headers| headers.status()),
            Some("complete")
        );
        assert_eq!(
            latest.message.data.unwrap().into_string().as_deref(),
            Some("{\"text\":\"hello\"} world")
        );
    }

    #[tokio::test]
    async fn events_rejects_ai_create_when_open_stream_cap_is_reached() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_ai_versioned_handler_with_store(100, store, 1024, 4096, 1);
        let app = test_app();
        let streaming_extras = || MessageExtras {
            ai: Some(sockudo_protocol::messages::AiExtras {
                transport: Some(HashMap::from([(
                    "status".to_string(),
                    "streaming".to_string(),
                )])),
                codec: None,
            }),
            ..Default::default()
        };

        let first = events(
            Path("app-1".to_string()),
            Query(empty_event_query()),
            Extension(app.clone()),
            #[cfg(feature = "push")]
            test_push_store(),
            #[cfg(feature = "push")]
            test_push_queue(),
            State(handler.clone()),
            HeaderMap::new(),
            Uri::from_static("/apps/app-1/events"),
            RawQuery(None),
            Json(PusherApiMessage {
                name: Some("ai-output".to_string()),
                data: Some(ApiMessageData::String("hello".to_string())),
                channel: Some("versioned-room".to_string()),
                channels: None,
                socket_id: None,
                info: None,
                tags: None,
                delta: None,
                idempotency_key: None,
                message_id: None,
                extras: Some(streaming_extras()),
            }),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(first.status(), StatusCode::OK);

        let second = match events(
            Path("app-1".to_string()),
            Query(empty_event_query()),
            Extension(app),
            #[cfg(feature = "push")]
            test_push_store(),
            #[cfg(feature = "push")]
            test_push_queue(),
            State(handler),
            HeaderMap::new(),
            Uri::from_static("/apps/app-1/events"),
            RawQuery(None),
            Json(PusherApiMessage {
                name: Some("ai-output".to_string()),
                data: Some(ApiMessageData::String("hello again".to_string())),
                channel: Some("versioned-room".to_string()),
                channels: None,
                socket_id: None,
                info: None,
                tags: None,
                delta: None,
                idempotency_key: None,
                message_id: None,
                extras: Some(streaming_extras()),
            }),
        )
        .await
        {
            Err(err) => err.into_response(),
            Ok(_) => panic!("expected open-stream cap rejection"),
        };

        assert_eq!(second.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = axum::body::to_bytes(second.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["ai_code"], 40009);
    }

    #[tokio::test]
    async fn channel_message_versions_pages_newest_first() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_versioned_handler_with_store(2, store.clone());
        let app = test_app();

        let original = test_versioned_record(
            "msg:1",
            "00000000000000000001:test:00000000000000000001",
            10,
            1,
            "hello",
        );
        let update_1 = StoredVersionRecord {
            message: original
                .message
                .apply_mutation(
                    sockudo_core::versioned_messages::MessageAction::Update,
                    VersionMetadata {
                        serial: VersionSerial::new(
                            "00000000000000000002:test:00000000000000000002".to_string(),
                        )
                        .unwrap(),
                        client_id: Some("user-1".to_string()),
                        timestamp_ms: 2,
                        description: None,
                        metadata: None,
                    },
                    2,
                    sockudo_core::versioned_messages::MessageFieldDelta::default(),
                )
                .unwrap(),
            ..original.clone()
        };
        let update_2 = StoredVersionRecord {
            message: update_1
                .message
                .apply_mutation(
                    sockudo_core::versioned_messages::MessageAction::Delete,
                    VersionMetadata {
                        serial: VersionSerial::new(
                            "00000000000000000003:test:00000000000000000003".to_string(),
                        )
                        .unwrap(),
                        client_id: Some("user-1".to_string()),
                        timestamp_ms: 3,
                        description: None,
                        metadata: None,
                    },
                    3,
                    sockudo_core::versioned_messages::MessageFieldDelta::default(),
                )
                .unwrap(),
            ..original.clone()
        };

        store.append_version(original).await.unwrap();
        store.append_version(update_1).await.unwrap();
        store.append_version(update_2).await.unwrap();

        let response = channel_message_versions(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Query(MessageVersionsQuery {
                limit: Some(2),
                direction: Some(VersionDirection::NewestFirst),
                cursor: None,
            }),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["items"].as_array().unwrap().len(), 2);
        assert_eq!(
            json["items"][0]["version"]["serial"],
            "00000000000000000003:test:00000000000000000003"
        );
        assert_eq!(
            json["items"][1]["version"]["serial"],
            "00000000000000000002:test:00000000000000000002"
        );
        assert_eq!(json["direction"], "newest_first");
        assert!(json["has_more"].as_bool().unwrap());
        assert_eq!(
            json["next_cursor"],
            "00000000000000000002:test:00000000000000000002"
        );
    }

    #[tokio::test]
    async fn versioned_message_endpoints_require_feature_flag() {
        let handler = test_history_handler(100);
        let app = test_app();

        let response = match channel_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app),
            State(handler),
        )
        .await
        {
            Err(err) => err.into_response(),
            Ok(_) => panic!("expected feature-disabled error"),
        };

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn annotation_publish_requires_feature_flag() {
        let store = Arc::new(MemoryVersionStore::new());
        let (handler, _adapter, _app_manager) =
            test_versioned_handler_harness_with_annotations(100, store.clone(), false);
        let app = test_app();

        store
            .append_version(test_versioned_record(
                "msg:1",
                "00000000000000000001:test:00000000000000000001",
                10,
                1,
                "hello",
            ))
            .await
            .unwrap();

        let response = match publish_annotation(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app),
            State(handler),
            Json(PublishAnnotationRequest {
                annotation_type: "reactions:total.v1".to_string(),
                name: None,
                client_id: None,
                socket_id: None,
                count: None,
                data: None,
                encoding: None,
            }),
        )
        .await
        {
            Err(err) => err.into_response(),
            Ok(_) => panic!("expected annotations feature-disabled error"),
        };

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn annotation_publish_requires_channel_policy_flag() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_versioned_handler_with_store(100, store.clone());
        let app = test_app();

        store
            .append_version(test_versioned_record(
                "msg:1",
                "00000000000000000001:test:00000000000000000001",
                10,
                1,
                "hello",
            ))
            .await
            .unwrap();

        let response = match publish_annotation(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app),
            State(handler),
            Json(PublishAnnotationRequest {
                annotation_type: "reactions:total.v1".to_string(),
                name: None,
                client_id: None,
                socket_id: None,
                count: None,
                data: None,
                encoding: None,
            }),
        )
        .await
        {
            Err(err) => err.into_response(),
            Ok(_) => panic!("expected annotation channel policy denial"),
        };

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert!(
            json["error"]
                .as_str()
                .unwrap()
                .contains("Annotations are disabled by channel policy")
        );
    }

    #[tokio::test]
    async fn annotation_publish_denies_socket_without_annotation_publish_capability() {
        let store = Arc::new(MemoryVersionStore::new());
        let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store.clone());
        let app = test_annotation_app();

        store
            .append_version(test_versioned_record(
                "msg:1",
                "00000000000000000001:test:00000000000000000001",
                10,
                1,
                "hello",
            ))
            .await
            .unwrap();

        let socket_id = SocketId::from_string("21.21").unwrap();
        attach_signed_in_mutation_actor(
            &handler,
            app_manager,
            &app,
            socket_id,
            "user-1",
            ConnectionCapabilities {
                publish: Some(vec!["versioned-room".to_string()]),
                ..Default::default()
            },
        )
        .await;

        let response = match publish_annotation(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app),
            State(handler),
            Json(PublishAnnotationRequest {
                annotation_type: "reactions:distinct.v1".to_string(),
                name: Some("thumbsup".to_string()),
                client_id: Some("user-1".to_string()),
                socket_id: Some(socket_id.to_string()),
                count: None,
                data: None,
                encoding: None,
            }),
        )
        .await
        {
            Err(err) => err.into_response(),
            Ok(_) => panic!("expected annotation-publish capability denial"),
        };

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn annotation_publish_rejects_unidentified_ownership_summarizer() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_versioned_handler_with_store(100, store.clone());
        let app = test_annotation_app();

        store
            .append_version(test_versioned_record(
                "msg:1",
                "00000000000000000001:test:00000000000000000001",
                10,
                1,
                "hello",
            ))
            .await
            .unwrap();

        let response = match publish_annotation(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app),
            State(handler),
            Json(PublishAnnotationRequest {
                annotation_type: "reactions:flag.v1".to_string(),
                name: None,
                client_id: None,
                socket_id: None,
                count: None,
                data: None,
                encoding: None,
            }),
        )
        .await
        {
            Err(err) => err.into_response(),
            Ok(_) => panic!("expected missing client_id validation"),
        };

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn annotation_publish_and_delete_update_summary_projection() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_versioned_handler_with_store(100, store.clone());
        let app = test_annotation_app();

        store
            .append_version(test_versioned_record(
                "msg:1",
                "00000000000000000001:test:00000000000000000001",
                10,
                1,
                "hello",
            ))
            .await
            .unwrap();

        let publish_response = publish_annotation(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app.clone()),
            State(handler.clone()),
            Json(PublishAnnotationRequest {
                annotation_type: "reactions:total.v1".to_string(),
                name: None,
                client_id: None,
                socket_id: None,
                count: None,
                data: Some(json!({"emoji": "thumbsup"})),
                encoding: None,
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(publish_response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(publish_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        let annotation_serial = json["annotationSerial"].as_str().unwrap().to_string();

        let projection_request = AnnotationProjectionRequest {
            app_id: "app-1".to_string(),
            channel_id: "versioned-room".to_string(),
            message_serial: MessageSerial::new("msg:1").unwrap(),
            annotation_type: AnnotationType::new("reactions:total.v1").unwrap(),
        };
        let projection = handler
            .annotation_store()
            .get_projection(projection_request.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            projection.summary,
            AnnotationSummary::Total(TotalAnnotationSummary { total: 1 })
        );

        let delete_response = delete_annotation(
            Path(AnnotationMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
                annotation_serial,
            }),
            Query(HashMap::new()),
            Extension(app),
            State(handler.clone()),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(delete_response.status(), StatusCode::OK);
        let projection = handler
            .annotation_store()
            .get_projection(projection_request)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            projection.summary,
            AnnotationSummary::Total(TotalAnnotationSummary { total: 0 })
        );
    }

    #[tokio::test]
    async fn annotation_delete_missing_annotation_returns_404() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_versioned_handler_with_store(100, store);
        let app = test_annotation_app();

        let response = match delete_annotation(
            Path(AnnotationMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
                annotation_serial: "ann:missing".to_string(),
            }),
            Query(HashMap::new()),
            Extension(app),
            State(handler),
        )
        .await
        {
            Err(err) => err.into_response(),
            Ok(_) => panic!("expected missing annotation 404"),
        };

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn annotation_delete_own_denies_identity_mismatch() {
        let store = Arc::new(MemoryVersionStore::new());
        let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store);
        let app = test_annotation_app();

        handler
            .annotation_store()
            .append_event(StoredAnnotationEvent {
                app_id: "app-1".to_string(),
                channel_id: "versioned-room".to_string(),
                stored_at_ms: sockudo_core::history::now_ms(),
                annotation: Annotation {
                    id: AnnotationId::new("ann-1").unwrap(),
                    action: AnnotationAction::Create,
                    serial: AnnotationSerial::new("ann:1").unwrap(),
                    message_serial: MessageSerial::new("msg:1").unwrap(),
                    annotation_type: AnnotationType::new("reactions:distinct.v1").unwrap(),
                    name: Some("thumbsup".to_string()),
                    client_id: Some("user-1".to_string()),
                    count: None,
                    data: None,
                    encoding: None,
                    timestamp: sockudo_core::history::now_ms(),
                },
            })
            .await
            .unwrap();

        let socket_id = SocketId::from_string("22.22").unwrap();
        attach_signed_in_mutation_actor(
            &handler,
            app_manager,
            &app,
            socket_id,
            "user-2",
            ConnectionCapabilities {
                annotation_delete_own: Some(vec!["versioned-room".to_string()]),
                ..Default::default()
            },
        )
        .await;

        let response = match delete_annotation(
            Path(AnnotationMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
                annotation_serial: "ann:1".to_string(),
            }),
            Query(HashMap::from([(
                "socket_id".to_string(),
                socket_id.to_string(),
            )])),
            Extension(app),
            State(handler),
        )
        .await
        {
            Err(err) => err.into_response(),
            Ok(_) => panic!("expected annotation-delete-own identity mismatch"),
        };

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn annotation_delete_own_allows_annotation_owner() {
        let store = Arc::new(MemoryVersionStore::new());
        let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store);
        let app = test_annotation_app();

        handler
            .annotation_store()
            .append_event(StoredAnnotationEvent {
                app_id: "app-1".to_string(),
                channel_id: "versioned-room".to_string(),
                stored_at_ms: sockudo_core::history::now_ms(),
                annotation: Annotation {
                    id: AnnotationId::new("ann-own").unwrap(),
                    action: AnnotationAction::Create,
                    serial: AnnotationSerial::new("ann:own").unwrap(),
                    message_serial: MessageSerial::new("msg:1").unwrap(),
                    annotation_type: AnnotationType::new("reactions:distinct.v1").unwrap(),
                    name: Some("thumbsup".to_string()),
                    client_id: Some("user-1".to_string()),
                    count: None,
                    data: None,
                    encoding: None,
                    timestamp: sockudo_core::history::now_ms(),
                },
            })
            .await
            .unwrap();

        let socket_id = SocketId::from_string("23.23").unwrap();
        attach_signed_in_mutation_actor(
            &handler,
            app_manager,
            &app,
            socket_id,
            "user-1",
            ConnectionCapabilities {
                annotation_delete_own: Some(vec!["versioned-room".to_string()]),
                ..Default::default()
            },
        )
        .await;

        let response = delete_annotation(
            Path(AnnotationMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
                annotation_serial: "ann:own".to_string(),
            }),
            Query(HashMap::from([(
                "socket_id".to_string(),
                socket_id.to_string(),
            )])),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn annotation_delete_any_allows_moderator() {
        let store = Arc::new(MemoryVersionStore::new());
        let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store);
        let app = test_annotation_app();

        handler
            .annotation_store()
            .append_event(StoredAnnotationEvent {
                app_id: "app-1".to_string(),
                channel_id: "versioned-room".to_string(),
                stored_at_ms: sockudo_core::history::now_ms(),
                annotation: Annotation {
                    id: AnnotationId::new("ann-any").unwrap(),
                    action: AnnotationAction::Create,
                    serial: AnnotationSerial::new("ann:any").unwrap(),
                    message_serial: MessageSerial::new("msg:1").unwrap(),
                    annotation_type: AnnotationType::new("reactions:distinct.v1").unwrap(),
                    name: Some("thumbsup".to_string()),
                    client_id: Some("user-1".to_string()),
                    count: None,
                    data: None,
                    encoding: None,
                    timestamp: sockudo_core::history::now_ms(),
                },
            })
            .await
            .unwrap();

        let socket_id = SocketId::from_string("24.24").unwrap();
        attach_signed_in_mutation_actor(
            &handler,
            app_manager,
            &app,
            socket_id,
            "moderator",
            ConnectionCapabilities {
                annotation_delete_any: Some(vec!["versioned-room".to_string()]),
                ..Default::default()
            },
        )
        .await;

        let response = delete_annotation(
            Path(AnnotationMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
                annotation_serial: "ann:any".to_string(),
            }),
            Query(HashMap::from([(
                "socket_id".to_string(),
                socket_id.to_string(),
            )])),
            Extension(app),
            State(handler),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn update_message_validates_request_body_before_runtime() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_versioned_handler_with_store(100, store);
        let app = test_app();

        let response = match update_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app),
            State(handler),
            Json(UpdateMessageRequest::default()),
        )
        .await
        {
            Err(err) => err.into_response(),
            Ok(_) => panic!("expected validation error"),
        };

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn update_message_denies_authenticated_socket_without_mutation_capability() {
        let store = Arc::new(MemoryVersionStore::new());
        let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store.clone());
        let app = test_app();

        store
            .append_version(test_versioned_record(
                "msg:1",
                "00000000000000000001:test:00000000000000000001",
                10,
                1,
                "hello",
            ))
            .await
            .unwrap();

        let socket_id = SocketId::from_string("11.11").unwrap();
        attach_signed_in_mutation_actor(
            &handler,
            app_manager.clone(),
            &app,
            socket_id,
            "user-2",
            ConnectionCapabilities::default(),
        )
        .await;

        let response = match update_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app),
            State(handler),
            Json(UpdateMessageRequest {
                name: None,
                data: Some(MessageData::String("{\"text\":\"blocked\"}".to_string())),
                extras: None,
                clear_fields: Vec::new(),
                client_id: Some("user-2".to_string()),
                socket_id: Some(socket_id.to_string()),
                description: Some("blocked".to_string()),
                metadata: None,
                op_id: None,
            }),
        )
        .await
        {
            Err(err) => err.into_response(),
            Ok(_) => panic!("expected capability check to fail"),
        };

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["ai_code"], 93002);
        assert_eq!(json["error"], "mutations not permitted on this channel");
    }

    #[tokio::test]
    async fn update_message_allows_authenticated_owner_with_own_capability() {
        let store = Arc::new(MemoryVersionStore::new());
        let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store.clone());
        let app = test_app();

        store
            .append_version(test_versioned_record(
                "msg:1",
                "00000000000000000001:test:00000000000000000001",
                10,
                1,
                "hello",
            ))
            .await
            .unwrap();

        let socket_id = SocketId::from_string("12.12").unwrap();
        attach_signed_in_mutation_actor(
            &handler,
            app_manager.clone(),
            &app,
            socket_id,
            "user-1",
            ConnectionCapabilities {
                message_update_own: Some(vec!["versioned-room".to_string()]),
                ..Default::default()
            },
        )
        .await;

        let response = update_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app.clone()),
            State(handler.clone()),
            Json(UpdateMessageRequest {
                name: None,
                data: Some(MessageData::String("{\"text\":\"patched\"}".to_string())),
                extras: None,
                clear_fields: Vec::new(),
                client_id: Some("user-1".to_string()),
                socket_id: Some(socket_id.to_string()),
                description: Some("owner patch".to_string()),
                metadata: None,
                op_id: None,
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);

        let latest = handler
            .version_store()
            .get_latest(
                "app-1",
                "versioned-room",
                &MessageSerial::new("msg:1").unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.message.version.client_id.as_deref(), Some("user-1"));
        assert_eq!(
            latest.message.data.unwrap().into_string().as_deref(),
            Some("{\"text\":\"patched\"}")
        );
    }

    #[tokio::test]
    async fn delete_message_allows_authenticated_any_capability() {
        let store = Arc::new(MemoryVersionStore::new());
        let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store.clone());
        let app = test_app();

        store
            .append_version(test_versioned_record(
                "msg:1",
                "00000000000000000001:test:00000000000000000001",
                10,
                1,
                "hello",
            ))
            .await
            .unwrap();

        let socket_id = SocketId::from_string("13.13").unwrap();
        attach_signed_in_mutation_actor(
            &handler,
            app_manager.clone(),
            &app,
            socket_id,
            "moderator",
            ConnectionCapabilities {
                message_delete_any: Some(vec!["versioned-room".to_string()]),
                ..Default::default()
            },
        )
        .await;

        let response = delete_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app),
            State(handler),
            Json(DeleteMessageRequest {
                data: None,
                extras: None,
                clear_fields: vec![sockudo_protocol::versioned_messages::ClearField::Data],
                client_id: Some("moderator".to_string()),
                socket_id: Some(socket_id.to_string()),
                description: Some("moderation".to_string()),
                metadata: None,
                op_id: None,
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn append_message_denies_non_owner_with_own_capability() {
        let store = Arc::new(MemoryVersionStore::new());
        let (handler, _adapter, app_manager) = test_versioned_handler_harness(100, store.clone());
        let app = test_app();

        store
            .append_version(test_versioned_record(
                "msg:1",
                "00000000000000000001:test:00000000000000000001",
                10,
                1,
                "hello",
            ))
            .await
            .unwrap();

        let socket_id = SocketId::from_string("14.14").unwrap();
        attach_signed_in_mutation_actor(
            &handler,
            app_manager.clone(),
            &app,
            socket_id,
            "user-2",
            ConnectionCapabilities {
                message_append_own: Some(vec!["versioned-room".to_string()]),
                ..Default::default()
            },
        )
        .await;

        let response = match append_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app),
            State(handler),
            Json(AppendMessageRequest {
                data: " world".to_string(),
                extras: None,
                client_id: Some("user-2".to_string()),
                socket_id: Some(socket_id.to_string()),
                description: Some("unauthorized append".to_string()),
                metadata: None,
                op_id: None,
            }),
        )
        .await
        {
            Err(err) => err.into_response(),
            Ok(_) => panic!("expected own-scope append to be denied"),
        };

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["ai_code"], 93002);
        assert_eq!(json["error"], "mutations not permitted on this channel");
    }

    #[tokio::test]
    async fn append_message_reports_not_implemented_after_validation() {
        let store = Arc::new(MemoryVersionStore::new());
        let handler = test_versioned_handler_with_store(100, store.clone());
        let app = test_app();

        let original = test_versioned_record(
            "msg:1",
            "00000000000000000001:test:00000000000000000001",
            10,
            1,
            "hello",
        );
        store.append_version(original).await.unwrap();

        let response = append_message(
            Path(VersionMutationPath {
                app_id: "app-1".to_string(),
                channel_name: "versioned-room".to_string(),
                message_serial: "msg:1".to_string(),
            }),
            Extension(app),
            State(handler),
            Json(AppendMessageRequest {
                data: "hello".to_string(),
                extras: None,
                client_id: None,
                socket_id: None,
                description: None,
                metadata: None,
                op_id: None,
            }),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(json["accepted"], true);
        assert_eq!(json["action"], "append");
    }
}
