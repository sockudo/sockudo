use crate::adapter::ConnectionHandler;

use crate::protocol::messages::{
    BatchPusherApiMessage, InfoQueryParser, PusherApiMessage, PusherMessage,
};
use crate::utils;
use crate::websocket::SocketId;
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderValue, Response, StatusCode},
    response::{IntoResponse, Response as AxumResponse},
    Json,
};
// use chrono::Duration; // Duration seems unused in this file after changes
use crate::app::config::App; // To access app limits
use crate::protocol::constants::EVENT_NAME_MAX_LENGTH as DEFAULT_EVENT_NAME_MAX_LENGTH;
use crate::utils::validate_channel_name;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, fmt, sync::Arc}; // Added fmt for AppError Display
use sysinfo::System;
use thiserror::Error;
use tracing::{error, field, info, instrument, warn};
// --- Custom Error Type ---

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Application not found: {0}")]
    AppNotFound(String),
    #[error("Application validation failed: {0}")]
    AppValidationFailed(String),
    #[error("Channel validation failed: Missing 'channels' or 'channel' field")]
    MissingChannelInfo,
    #[error("User connection termination failed: {0}")]
    TerminationFailed(String),
    #[error("Internal Server Error: {0}")]
    InternalError(String),
    #[error("Serialization Error: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("HTTP Header Build Error: {0}")]
    HeaderBuildError(#[from] axum::http::Error),
    #[error("Limit exceeded: {0}")]
    LimitExceeded(String),
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
            AppError::InvalidInput(msg) => (StatusCode::BAD_REQUEST, json!({ "error": msg })),
        };

        // Log the error using the tracing crate, including the specific error message and status code.
        // The `Display` implementation from `thiserror` will be used for `%self`.
        error!(error.message = %self, status_code = %status, "HTTP request failed");
        (status, Json(error_message)).into_response()
    }
}

// Implementation to convert from the global `crate::error::Error` to the local `AppError`.
// This allows the `?` operator to work seamlessly in Axum handlers.
impl From<crate::error::Error> for AppError {
    fn from(err: crate::error::Error) -> Self {
        // Log the original error for more detailed backend logs before converting.
        warn!(original_error = ?err, "Converting internal error to AppError for HTTP response");
        match err {
            // Specific mappings:
            crate::error::Error::InvalidAppKey => {
                AppError::AppNotFound(format!("Application key not found or invalid: {}", err))
            }
            crate::error::Error::ApplicationNotFound => AppError::AppNotFound(err.to_string()),
            crate::error::Error::InvalidChannelName(s) => {
                AppError::InvalidInput(format!("Invalid channel name: {}", s))
            }
            crate::error::Error::ChannelError(s) => AppError::InvalidInput(s), // Or map to InternalError if more appropriate
            crate::error::Error::AuthError(s) => AppError::AppValidationFailed(s), // Or a more specific auth-related AppError

            // General fallback mapping:
            // Most other internal errors can be mapped to a generic InternalServerError for the client.
            // The specific `err.to_string()` will provide some context.
            _ => AppError::InternalError(err.to_string()),
        }
    }
}

// --- Query Parameter Structs ---

#[derive(Deserialize, Debug)]
pub struct EventQuery {
    #[serde(default)]
    pub auth_key: String,
    #[serde(default)]
    pub auth_timestamp: String,
    #[serde(default)]
    pub auth_version: String,
    #[serde(default)]
    pub body_md5: String,
    #[serde(default)]
    pub auth_signature: String,
}

#[derive(Deserialize, Debug)]
pub struct ChannelQuery {
    #[serde(default)]
    pub info: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ChannelsQuery {
    #[serde(default)]
    pub filter_by_prefix: Option<String>,
    #[serde(default)]
    pub info: Option<String>,
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

// --- Helper Functions ---

/// Helper to build cache payload string
fn build_cache_payload(event_name: &str, event_data: &Value) -> Result<String, serde_json::Error> {
    serde_json::to_string(&json!({
        "event": event_name,
        "data": event_data,
    }))
}

/// Records API metrics (helper async function)
#[instrument(skip(handler, incoming_request_size, outgoing_response_size), fields(app_id = %app_id))] // Renamed parameters for clarity
async fn record_api_metrics(
    handler: &Arc<ConnectionHandler>,
    app_id: &str,
    incoming_request_size: usize, // Renamed for clarity (e.g. size of incoming batch or single event)
    outgoing_response_size: usize, // Renamed for clarity (size of HTTP response body)
) {
    if let Some(metrics_arc) = &handler.metrics {
        let metrics = metrics_arc.lock().await;
        // Pass the sizes as they are: incoming_request_size and outgoing_response_size
        metrics.mark_api_message(app_id, incoming_request_size, outgoing_response_size);
        info!(
            incoming_bytes = incoming_request_size,
            outgoing_bytes = outgoing_response_size,
            "Recorded API message metrics"
        );
    } else {
        info!(
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

/// Helper to process a single event and return channel info if requested
#[instrument(skip(handler, event_data, app), fields(app_id = app.id, event_name = field::Empty))]
async fn process_single_event(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    event_data: PusherApiMessage,
    collect_info: bool,
) -> Result<HashMap<String, Value>, AppError> {
    let PusherApiMessage {
        name,
        data: event_payload_data,
        channels,
        channel,
        socket_id: original_socket_id_str,
        info,
    } = event_data;

    let event_name_str = name
        .as_deref()
        .ok_or_else(|| AppError::InvalidInput("Event name is required".to_string()))?;
    tracing::Span::current().record("event_name", event_name_str);

    let max_event_name_len = app
        .max_event_name_length
        .unwrap_or(DEFAULT_EVENT_NAME_MAX_LENGTH as u32);
    if event_name_str.len() > max_event_name_len as usize {
        return Err(AppError::LimitExceeded(format!(
            "Event name '{}' exceeds maximum length of {}",
            event_name_str, max_event_name_len
        )));
    }

    if let Some(max_payload_kb) = app.max_event_payload_in_kb {
        let value_for_size_calc = match &event_payload_data {
            Some(crate::protocol::messages::ApiMessageData::String(s)) => json!(s),
            Some(crate::protocol::messages::ApiMessageData::Json(j_val)) => j_val.clone(),
            None => json!(null),
        };
        let payload_size_bytes = utils::data_to_bytes_flexible(vec![value_for_size_calc]);
        if payload_size_bytes > (max_payload_kb as usize * 1024) {
            return Err(AppError::LimitExceeded(format!(
                "Event payload size ({} bytes) for event '{}' exceeds limit ({}KB)",
                payload_size_bytes, event_name_str, max_payload_kb
            )));
        }
    }

    let mapped_socket_id: Option<SocketId> = original_socket_id_str.map(SocketId);
    let name_clone_for_message = name.clone();

    let target_channels: Vec<String> = match channels {
        Some(ch_list) if !ch_list.is_empty() => {
            if let Some(max_ch_at_once) = app.max_event_channels_at_once {
                if ch_list.len() > max_ch_at_once as usize {
                    return Err(AppError::LimitExceeded(format!(
                        "Number of channels ({}) exceeds limit ({})",
                        ch_list.len(),
                        max_ch_at_once
                    )));
                }
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

    let mut channels_info_map = HashMap::new();

    for target_channel_str in target_channels {
        info!(channel = %target_channel_str, "Processing channel for event");

        validate_channel_name(&app, &target_channel_str).await?;

        let message_to_send = PusherApiMessage {
            name: name_clone_for_message.clone(),
            data: event_payload_data.clone(),
            channels: None,
            channel: Some(target_channel_str.clone()),
            socket_id: mapped_socket_id.as_ref().map(|sid| sid.0.clone()),
            info: info.clone(),
        };

        handler
            .send_message(
                &app.id,
                mapped_socket_id.as_ref(),
                message_to_send,
                &target_channel_str,
            )
            .await;

        if collect_info {
            let is_presence_channel = target_channel_str.starts_with("presence-");
            let mut current_channel_info_map = serde_json::Map::new();

            if is_presence_channel && info.as_deref().map_or(false, |s| s.contains("user_count")) {
                match handler
                    .channel_manager
                    .read()
                    .await
                    .get_channel_members(&app.id, &target_channel_str)
                    .await
                {
                    Ok(members_map) => {
                        current_channel_info_map
                            .insert("user_count".to_string(), json!(members_map.len()));
                    }
                    Err(e) => {
                        warn!(
                            "{}",
                            format!(
                            "Failed to get user count for channel {}: {} (internal error: {:?})",
                            target_channel_str, e, e
                        )
                        );
                    }
                }
            }

            if info
                .as_deref()
                .map_or(false, |s| s.contains("subscription_count"))
            {
                let count = handler
                    .connection_manager
                    .lock()
                    .await
                    .get_channel_socket_count(&app.id, &target_channel_str)
                    .await;
                current_channel_info_map.insert("subscription_count".to_string(), json!(count));
            }

            if !current_channel_info_map.is_empty() {
                channels_info_map.insert(
                    target_channel_str.clone(),
                    Value::Object(current_channel_info_map),
                );
            }
        }

        if utils::is_cache_channel(&target_channel_str) {
            let payload_for_cache = match event_payload_data.clone() {
                Some(data_val) => serde_json::to_value(data_val)?,
                None => json!(null),
            };

            let cache_payload_str = match build_cache_payload(event_name_str, &payload_for_cache) {
                Ok(payload) => payload,
                Err(e) => {
                    error!( channel = %target_channel_str, error = %e, "Failed to serialize event data for caching");
                    continue;
                }
            };

            let mut cache_manager_locked = handler.cache_manager.lock().await;
            let cache_key_str =
                format!("app:{}:channel:{}:cache_miss", &app.id, target_channel_str);

            match cache_manager_locked
                .set(&cache_key_str, &cache_payload_str, 3600)
                .await
            {
                Ok(_) => {
                    info!(channel = %target_channel_str, cache_key = %cache_key_str, "Cached event for channel");
                }
                Err(e) => {
                    error!(channel = %target_channel_str, cache_key = %cache_key_str, error = %e, "Failed to cache event (internal error: {:?})", e);
                }
            }
        }
    }

    Ok(channels_info_map)
}

/// POST /apps/{app_id}/events
#[instrument(skip(handler, event_payload), fields(app_id = %app_id))]
pub async fn events(
    Path(app_id): Path<String>,
    Query(_query): Query<EventQuery>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(event_payload): Json<PusherApiMessage>,
) -> Result<impl IntoResponse, AppError> {
    info!("{}", format!("Received event: {:?}", event_payload));

    let app = handler
        .app_manager
        .find_by_id(app_id.as_str())
        .await?
        .ok_or_else(|| AppError::AppNotFound(app_id.clone()))?;

    let need_channel_info = event_payload.info.is_some();

    let channels_info_map =
        process_single_event(&handler, &app, event_payload, need_channel_info).await?;

    if need_channel_info && !channels_info_map.is_empty() {
        let response_payload = json!({
            "channels": channels_info_map
        });
        Ok((StatusCode::OK, Json(response_payload)))
    } else {
        Ok((StatusCode::OK, Json(json!({ "ok": true }))))
    }
}

/// POST /apps/{app_id}/batch_events
#[instrument(skip(handler, batch_message_payload), fields(app_id = %app_id, batch_len = field::Empty))]
pub async fn batch_events(
    Path(app_id): Path<String>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(batch_message_payload): Json<BatchPusherApiMessage>,
) -> Result<impl IntoResponse, AppError> {
    let batch_events_vec = batch_message_payload.batch;
    let batch_len = batch_events_vec.len();
    tracing::Span::current().record("batch_len", &batch_len);
    info!("Received batch events request with {} events", batch_len);

    let app_config = handler
        .app_manager
        .find_by_id(app_id.as_str())
        .await?
        .ok_or_else(|| AppError::AppNotFound(app_id.clone()))?;

    if let Some(max_batch) = app_config.max_event_batch_size {
        if batch_len > max_batch as usize {
            return Err(AppError::LimitExceeded(format!(
                "Batch size ({}) exceeds limit ({})",
                batch_len, max_batch
            )));
        }
    }

    let incoming_request_size_bytes = serde_json::to_vec(&batch_events_vec)?.len();

    let mut batch_response_info_vec = Vec::new();
    let mut any_message_requests_info = false;

    for single_event_message in batch_events_vec {
        if single_event_message.info.is_some() {
            any_message_requests_info = true;
        }

        let channel_info_map_for_event = process_single_event(
            &handler,
            &app_config,
            single_event_message.clone(),
            single_event_message.info.is_some(),
        )
        .await?;

        if any_message_requests_info {
            if let Some(main_channel_for_event) =
                single_event_message.channel.as_ref().or_else(|| {
                    single_event_message
                        .channels
                        .as_ref()
                        .and_then(|chs| chs.first())
                })
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
        json!({ "ok": true })
    };

    let outgoing_response_size_bytes_vec = serde_json::to_vec(&final_response_payload)?;

    record_api_metrics(
        &handler,
        &app_id,
        incoming_request_size_bytes,
        outgoing_response_size_bytes_vec.len(),
    )
    .await;

    info!("{}", "Batch events processed successfully");
    Ok((StatusCode::OK, Json(final_response_payload)))
}

/// GET /apps/{app_id}/channels/{channel_name}
#[instrument(skip(handler), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel(
    Path((app_id, channel_name)): Path<(String, String)>,
    Query(query_params): Query<ChannelQuery>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    info!("Request for channel info for channel: {}", channel_name);

    let app = handler
        .app_manager
        .find_by_id(&app_id)
        .await?
        .ok_or_else(|| AppError::AppNotFound(app_id.clone()))?;

    validate_channel_name(&app, &channel_name).await?;

    let info_query_str = query_params.info.as_ref();
    let wants_subscription_count = info_query_str.wants_subscription_count();
    let wants_user_count = info_query_str.wants_user_count();
    let wants_cache_data = info_query_str.wants_cache();

    let socket_count_val;
    {
        let mut connection_manager_locked = handler.connection_manager.lock().await;
        socket_count_val = connection_manager_locked
            .get_channel_socket_count(&app_id, &channel_name)
            .await;
    }

    let user_count_val = if wants_user_count {
        if channel_name.starts_with("presence-") {
            let members_map = handler
                .channel_manager
                .read()
                .await
                .get_channel_members(&app_id, &channel_name)
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
        let mut cache_manager_locked = handler.cache_manager.lock().await;
        let cache_key_str = format!("app:{}:channel:{}:cache_miss", app_id, channel_name);

        match cache_manager_locked.get(&cache_key_str).await? {
            Some(cache_content_str) => {
                let ttl_duration = cache_manager_locked
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
    let response_payload = PusherMessage::channel_info(
        socket_count_val > 0,
        subscription_count_val,
        user_count_val,
        cache_data_tuple,
    );

    let response_json_bytes = serde_json::to_vec(&response_payload)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;

    info!("Channel info for '{}' retrieved successfully", channel_name);
    Ok((StatusCode::OK, Json(response_payload)))
}

/// GET /apps/{app_id}/channels
#[instrument(skip(handler), fields(app_id = %app_id))]
pub async fn channels(
    Path(app_id): Path<String>,
    Query(query_params): Query<ChannelsQuery>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    info!("Request for channels list for app_id: {}", app_id);

    let filter_prefix_str = query_params.filter_by_prefix.as_deref().unwrap_or("");
    let wants_user_count = query_params.info.as_ref().wants_user_count();
    let app = handler
        .app_manager
        .find_by_id(&app_id)
        .await?
        .ok_or_else(|| AppError::AppNotFound(app_id.clone()))?;

    let channels_map;
    {
        let mut connection_manager_locked = handler.connection_manager.lock().await;
        channels_map = connection_manager_locked
            .get_channels_with_socket_count(&app_id)
            .await?;
    }

    let mut channels_info_response_map = HashMap::new();

    for entry in channels_map.iter() {
        let channel_name_str = entry.key();
        if !channel_name_str.starts_with(filter_prefix_str) {
            continue;
        }
        validate_channel_name(&app, channel_name_str).await?;

        let mut current_channel_info_map = serde_json::Map::new();

        if wants_user_count {
            if channel_name_str.starts_with("presence-") {
                let members_map = handler
                    .channel_manager
                    .read()
                    .await
                    .get_channel_members(&app_id, channel_name_str)
                    .await?;
                current_channel_info_map.insert("user_count".to_string(), json!(members_map.len()));
            } else if !filter_prefix_str.starts_with("presence-") {
                return Err(AppError::InvalidInput(
                    "user_count is only available for presence channels. Use filter_by_prefix=presence-".to_string()
                ));
            }
        }

        if !current_channel_info_map.is_empty() {
            channels_info_response_map.insert(
                channel_name_str.clone(),
                Value::Object(current_channel_info_map),
            );
        } else if query_params.info.is_none() {
            channels_info_response_map.insert(channel_name_str.clone(), json!({}));
        }
    }

    let response_payload = PusherMessage::channels_list(channels_info_response_map);

    let response_json_bytes = serde_json::to_vec(&response_payload)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;

    info!("Channels list for app '{}' retrieved successfully", app_id);
    Ok((StatusCode::OK, Json(response_payload)))
}

/// GET /apps/{app_id}/channels/{channel_name}/users
#[instrument(skip(handler), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel_users(
    Path((app_id, channel_name)): Path<(String, String)>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    let app = handler
        .app_manager
        .find_by_id(&app_id)
        .await?
        .ok_or_else(|| AppError::AppNotFound(app_id.clone()))?;
    info!("Request for users in channel: {}", channel_name);
    validate_channel_name(&app, &channel_name).await?;

    if !channel_name.starts_with("presence-") {
        return Err(AppError::InvalidInput(
            "Only presence channels support this endpoint".to_string(),
        ));
    }

    let channel_members_map = handler
        .channel_manager
        .read()
        .await
        .get_channel_members(&app_id, &channel_name)
        .await?;

    let users_vec = channel_members_map
        .keys()
        .map(|user_id_str| json!({ "id": user_id_str }))
        .collect::<Vec<_>>();

    let response_payload_val = json!({ "users": users_vec });
    let response_json_bytes = serde_json::to_vec(&response_payload_val)?;

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
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    info!(
        "Received request to terminate user connections for user_id: {}",
        user_id
    );

    let connection_manager_arc = handler.connection_manager.clone();
    connection_manager_arc
        .lock()
        .await
        .terminate_connection(&app_id, &user_id)
        .await?;

    info!(
        "Successfully initiated termination for user_id: {}",
        user_id
    );
    Ok((StatusCode::OK, Json(json!({ "ok": true }))))
}

/// GET /up/{app_id}
#[instrument(skip(handler), fields(app_id = %app_id))]
pub async fn up(
    Path(app_id): Path<String>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    info!("Health check received for app_id: {}", app_id);

    if handler.app_manager.find_by_id(&app_id).await?.is_none() {
        warn!("Health check for non-existent app_id: {}", app_id);
    }

    if handler.metrics.is_some() {
        record_api_metrics(&handler, &app_id, 0, 2).await;
    } else {
        info!(
            "Metrics system not available for health check for app_id: {}.",
            app_id
        );
    }

    let response_val = Response::builder()
        .status(StatusCode::OK)
        .header("X-Health-Check", "OK")
        .body("OK".to_string())?;

    Ok(response_val)
}

/// GET /metrics (Prometheus format)
#[instrument(skip(handler), fields(service = "metrics_exporter"))]
pub async fn metrics(
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    info!("{}", "Metrics endpoint called");

    let plaintext_metrics_str = match handler.metrics.clone() {
        Some(metrics_arc) => {
            let metrics_data_guard = metrics_arc.lock().await;
            metrics_data_guard.get_metrics_as_plaintext().await
        }
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

    info!(
        bytes = plaintext_metrics_str.len(),
        "Successfully generated Prometheus metrics"
    );
    Ok((StatusCode::OK, response_headers, plaintext_metrics_str))
}
