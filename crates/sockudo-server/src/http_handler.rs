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
use sockudo_core::app::App;
use sockudo_core::error::{HEALTH_CHECK_TIMEOUT_MS, HealthStatus};
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
use sockudo_core::websocket::SocketId;
use sockudo_protocol::constants::EVENT_NAME_MAX_LENGTH as DEFAULT_EVENT_NAME_MAX_LENGTH;
use sockudo_protocol::messages::{
    ApiMessageData, BatchPusherApiMessage, InfoQueryParser, MessageData, PusherApiMessage,
    PusherMessage,
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
    #[error("Payload too large: {0}")]
    PayloadTooLarge(String),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("Feature disabled: {0}")]
    FeatureDisabled(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> AxumResponse {
        let (status, code, msg) = match &self {
            AppError::AppNotFound(msg) => (StatusCode::NOT_FOUND, "app_not_found", msg.clone()),
            AppError::AppValidationFailed(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "app_validation_failed", msg.clone())
            }
            AppError::ApiAuthFailed(msg) => (StatusCode::UNAUTHORIZED, "auth_failed", msg.clone()),
            AppError::MissingChannelInfo => (
                StatusCode::BAD_REQUEST,
                "missing_channel_info",
                "Request must contain 'channels' (list) or 'channel' (string)".to_string(),
            ),
            AppError::TerminationFailed(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "termination_failed", msg.clone())
            }
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
            AppError::InternalError(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", msg.clone())
            }
            AppError::LimitExceeded(msg) => (StatusCode::BAD_REQUEST, "limit_exceeded", msg.clone()),
            AppError::PayloadTooLarge(msg) => {
                (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large", msg.clone())
            }
            AppError::InvalidInput(msg) => (StatusCode::BAD_REQUEST, "invalid_input", msg.clone()),
            AppError::FeatureDisabled(msg) => {
                (StatusCode::UNPROCESSABLE_ENTITY, "feature_disabled", msg.clone())
            }
        };
        error!(error.message = %self, status_code = %status, "HTTP request failed");
        let error_message = json!({ "error": msg, "code": code, "status": status.as_u16() });
        (status, Json(error_message)).into_response()
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

/// Helper to process a single event and return channel info if requested
#[instrument(skip(handler, event_data, app, start_time_ms), fields(app_id = app.id, event_name = field::Empty))]
async fn process_single_event_parallel(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    event_data: PusherApiMessage,
    collect_info: bool,
    start_time_ms: Option<f64>,
    idempotency_key: Option<String>,
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
        let extras_for_task = extras.clone();

        async move {
            debug!(channel = %target_channel_str, "Processing channel for event (parallel task)");

            validate_channel_name(app, &target_channel_str).await?;

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
                    Some(uuid::Uuid::new_v4().to_string())
                } else {
                    None
                },
                stream_id: None,
                serial: None,
                idempotency_key: idempotency_key_for_task.clone(),
                extras: extras_for_task.clone(),
                delta_sequence: None,
                delta_conflation_key: None,
            };
            let timestamp_ms = start_time_ms;

            match delta_flag_for_task {
                Some(true) => {
                    handler_clone.broadcast_to_channel_with_timing(
                        app,
                        &target_channel_str,
                        _message_to_send,
                        socket_id_for_task.as_ref(),
                        timestamp_ms,
                    )
                    .await?;
                }
                Some(false) => {
                    handler_clone.broadcast_to_channel_force_full(
                        app,
                        &target_channel_str,
                        _message_to_send,
                        socket_id_for_task.as_ref(),
                        timestamp_ms,
                    )
                    .await?;
                }
                None => {
                    handler_clone.broadcast_to_channel_with_timing(
                        app,
                        &target_channel_str,
                        _message_to_send,
                        socket_id_for_task.as_ref(),
                        timestamp_ms,
                    )
                    .await?;
                }
            }

            let mut collected_channel_specific_info: Option<(String, Value)> = None;
            if collect_info {
                let is_presence = target_channel_str.starts_with("presence-");
                let mut current_channel_info_map = sonic_rs::Object::new();

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

/// Build the cache key used for idempotency storage.
fn idempotency_cache_key(app_id: &str, key: &str) -> String {
    format!("app:{}:idempotency:{}", app_id, key)
}

/// Merge per-app idempotency overrides with the global config.
/// Only fields explicitly set at the app level take precedence.
/// POST /apps/{app_id}/events
#[instrument(skip(handler, headers, event_payload), fields(app_id = %app_id))]
#[allow(clippy::too_many_arguments)]
pub async fn events(
    Path(app_id): Path<String>,
    Query(_auth_q_params_struct): Query<EventQuery>,
    Extension(app): Extension<App>,
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

    let need_channel_info = event_payload.info.is_some();

    let channels_info_map = process_single_event_parallel(
        &handler,
        &app,
        event_payload,
        need_channel_info,
        Some(start_time_ms),
        idempotency_key.clone(),
    )
    .await?;

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
        if single_event_message.info.is_some() {
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

        let should_collect_info_for_this_event = single_event_message.info.is_some();
        let channel_info_map = process_single_event_parallel(
            &handler,
            &app_config,
            single_event_message.clone(),
            should_collect_info_for_this_event,
            Some(start_time_ms),
            single_event_message.idempotency_key.clone(),
        )
        .await?;

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

    let channels_map = handler
        .connection_manager()
        .get_channels_with_socket_count(&app_id)
        .await?;

    let mut channels_info_response_map = AHashMap::new();
    for (channel_name_str, _socket_count) in &channels_map {
        if !channel_name_str.starts_with(filter_prefix_str) {
            continue;
        }
        validate_channel_name(&app, channel_name_str).await?;
        let mut current_channel_info_map = sonic_rs::Object::new();
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
        let message: Value = sonic_rs::from_slice(item.payload_bytes.as_ref()).map_err(|e| {
            AppError::InternalError(format!("Failed to decode history payload: {e}"))
        })?;
        items.push(json!({
            "stream_id": item.stream_id,
            "serial": item.serial,
            "published_at_ms": item.published_at_ms,
            "message_id": item.message_id,
            "event_name": item.event_name,
            "operation_kind": item.operation_kind,
            "payload_size_bytes": item.payload_size_bytes,
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

/// System health check function
async fn check_system_health(handler: &Arc<ConnectionHandler>) -> HealthStatus {
    let mut critical_issues = Vec::new();
    let mut non_critical_issues = Vec::new();

    // CRITICAL CHECK 1: Adapter health
    let adapter_check = timeout(
        Duration::from_millis(HEALTH_CHECK_TIMEOUT_MS),
        handler.connection_manager().check_health(),
    )
    .await;

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
    if handler.server_options().cache.driver != sockudo_core::options::CacheDriver::None {
        let cache_check = timeout(
            Duration::from_millis(HEALTH_CHECK_TIMEOUT_MS),
            handler.cache_manager().check_health(),
        )
        .await;

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
    if let Some(webhook_integration) = handler.webhook_integration() {
        let queue_check = timeout(
            Duration::from_millis(HEALTH_CHECK_TIMEOUT_MS),
            webhook_integration.check_queue_health(),
        )
        .await;

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

/// GET /up or /up/{app_id}
#[instrument(skip(handler), fields(app_id = field::Empty))]
pub async fn up(
    app_id: Option<Path<String>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    let (health_status, app_id_str) = if let Some(Path(app_id)) = app_id {
        tracing::Span::current().record("app_id", &app_id);
        debug!("Health check received for app_id: {}", app_id);

        let app_check = timeout(
            Duration::from_millis(HEALTH_CHECK_TIMEOUT_MS),
            handler.app_manager().find_by_id(&app_id),
        )
        .await;

        let app_status = match app_check {
            Ok(Ok(Some(app))) if app.enabled => check_system_health(&handler).await,
            Ok(Ok(Some(_))) => HealthStatus::Error(vec!["App is disabled".to_string()]),
            Ok(Ok(None)) => HealthStatus::NotFound,
            Ok(Err(e)) => HealthStatus::Error(vec![format!("App manager: {e}")]),
            Err(_) => HealthStatus::Error(vec![format!(
                "App manager timeout (>{HEALTH_CHECK_TIMEOUT_MS}ms)"
            )]),
        };

        (app_status, app_id)
    } else {
        debug!("General health check received (no app_id)");

        let apps_check = timeout(
            Duration::from_millis(HEALTH_CHECK_TIMEOUT_MS),
            handler.app_manager().get_apps(),
        )
        .await;

        let app_status = match apps_check {
            Ok(Ok(apps)) if !apps.is_empty() => {
                debug!("Found {} configured apps", apps.len());
                check_system_health(&handler).await
            }
            Ok(Ok(_)) => HealthStatus::Error(vec!["No apps configured".to_string()]),
            Ok(Err(e)) => HealthStatus::Error(vec![format!("App manager: {e}")]),
            Err(_) => HealthStatus::Error(vec![format!(
                "App manager timeout (>{HEALTH_CHECK_TIMEOUT_MS}ms)"
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
    use sockudo_protocol::messages::{MessageData, PusherMessage};
    use sonic_rs::JsonContainerTrait;
    use sonic_rs::JsonValueTrait;
    use std::sync::Arc;
    use std::time::Instant;

    fn test_app() -> App {
        test_app_with_policy(Default::default())
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
        let app = test_app();

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
        assert!(json["code"].as_str().is_some(), "error must include 'code' field");
        assert!(json["status"].as_u64().is_some(), "error must include 'status' field");
        assert!(json["error"].as_str().is_some(), "error must include 'error' field");
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
            assert!(ts >= base_ts + 2 && ts <= base_ts + 4,
                "item at ts={ts} outside Ably alias bounds [{}, {}]", base_ts + 2, base_ts + 4);
        }
    }
}
