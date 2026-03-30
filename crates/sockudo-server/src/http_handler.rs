use ahash::AHashMap;
use axum::{
    Json,
    extract::{Extension, Path, Query, RawQuery, State},
    http::{HeaderMap, HeaderValue, StatusCode, Uri, header},
    response::{IntoResponse, Response as AxumResponse},
};
use futures_util::future::join_all;
use serde::de::Error as _;
use serde::{Deserialize, Serialize};
use sockudo_adapter::ConnectionHandler;
use sockudo_adapter::channel_manager::ChannelManager;
use sockudo_core::app::App;
use sockudo_core::error::{HEALTH_CHECK_TIMEOUT_MS, HealthStatus};
use sockudo_core::utils::{self, validate_channel_name};
use sockudo_core::websocket::SocketId;
use sockudo_protocol::constants::EVENT_NAME_MAX_LENGTH as DEFAULT_EVENT_NAME_MAX_LENGTH;
use sockudo_protocol::messages::{
    ApiMessageData, BatchPusherApiMessage, InfoQueryParser, MessageData, PusherApiMessage,
    PusherMessage,
};
use sonic_rs::prelude::*;
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
}

impl IntoResponse for AppError {
    fn into_response(self) -> AxumResponse {
        let (status, error_message) = match &self {
            AppError::AppNotFound(msg) => (StatusCode::NOT_FOUND, json!({ "error": msg })),
            AppError::AppValidationFailed(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, json!({ "error": msg }))
            }
            AppError::ApiAuthFailed(msg) => (StatusCode::UNAUTHORIZED, json!({ "error": msg })),
            AppError::MissingChannelInfo => (
                StatusCode::BAD_REQUEST,
                json!({ "error": "Request must contain 'channels' (list) or 'channel' (string)" }),
            ),
            AppError::TerminationFailed(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, json!({ "error": msg }))
            }
            AppError::SerializationError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": format!("Internal error during serialization: {}", e) }),
            ),
            AppError::HeaderBuildError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": format!("Internal error building response: {}", e) }),
            ),
            AppError::InternalError(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, json!({ "error": msg }))
            }
            AppError::LimitExceeded(msg) => (StatusCode::BAD_REQUEST, json!({ "error": msg })),
            AppError::PayloadTooLarge(msg) => {
                (StatusCode::PAYLOAD_TOO_LARGE, json!({ "error": msg }))
            }
            AppError::InvalidInput(msg) => (StatusCode::BAD_REQUEST, json!({ "error": msg })),
        };
        error!(error.message = %self, status_code = %status, "HTTP request failed");
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

impl<'de> Deserialize<'de> for ChannelQuery {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut obj = sonic_rs::Object::deserialize(deserializer)?;
        let info = obj
            .remove(&"info")
            .and_then(|v| v.as_str().map(ToString::to_string));
        let auth_params: EventQuery = sonic_rs::from_value(&obj.into_value())
            .map_err(|e| D::Error::custom(format!("invalid auth query params: {e}")))?;
        Ok(Self { info, auth_params })
    }
}

impl<'de> Deserialize<'de> for ChannelsQuery {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut obj = sonic_rs::Object::deserialize(deserializer)?;
        let filter_by_prefix = obj
            .remove(&"filter_by_prefix")
            .and_then(|v| v.as_str().map(ToString::to_string));
        let info = obj
            .remove(&"info")
            .and_then(|v| v.as_str().map(ToString::to_string));
        let auth_params: EventQuery = sonic_rs::from_value(&obj.into_value())
            .map_err(|e| D::Error::custom(format!("invalid auth query params: {e}")))?;
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
    totals: GlobalStats,
    apps: Vec<AppStats>,
}

// --- Helper Functions ---

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
    use axum::response::IntoResponse;
    use sockudo_adapter::ConnectionHandlerBuilder;
    use sockudo_adapter::local_adapter::LocalAdapter;
    use sockudo_app::memory_app_manager::MemoryAppManager;
    use sockudo_cache::memory_cache_manager::MemoryCacheManager;
    use sockudo_core::app::AppManager;
    use sockudo_core::namespace::Namespace;
    use sockudo_core::options::MemoryCacheOptions;

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
    }
}
