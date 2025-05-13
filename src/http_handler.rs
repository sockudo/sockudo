use crate::adapter::ConnectionHandler;
use crate::log::Log;
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
use chrono::Duration;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, fmt, sync::Arc};
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
        };

        error!(error.message = %self, status_code = %status, "Request failed");
        (status, Json(error_message)).into_response()
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
#[instrument(skip(handler, batch_size, response_size), fields(app_id = %app_id))]
async fn record_api_metrics(
    handler: &Arc<ConnectionHandler>,
    app_id: &str,
    batch_size: usize,
    response_size: usize,
) {
    if let Some(metrics_arc) = &handler.metrics {
        let metrics = metrics_arc.lock().await;
        metrics.mark_api_message(app_id, batch_size, response_size);
        info!(
            incoming_bytes = batch_size,
            outgoing_bytes = response_size,
            "Recorded API message metrics"
        );
    } else {
        info!("Metrics system not available, skipping metrics recording.");
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
#[instrument(skip(handler, event_data), fields(app_id = app_id, event_name = field::Empty))]
async fn process_single_event(
    handler: &Arc<ConnectionHandler>,
    app_id: &str,
    event_data: PusherApiMessage,
    collect_info: bool,
) -> Result<HashMap<String, Value>, AppError> {
    let PusherApiMessage {
        name,
        data,
        channels,
        channel,
        socket_id: original_socket_id_str,
        info,
    } = event_data;

    if let Some(name_val) = &name {
        tracing::Span::current().record("event_name", name_val);
    }

    // Map the socket_id
    let mapped_socket_id: Option<SocketId> = original_socket_id_str.map(SocketId);

    // Name and data for message
    let name_clone = name;

    // Determine target channels
    let target_channels: Vec<String> = match channels {
        Some(ch_list) if !ch_list.is_empty() => ch_list,
        None => match channel {
            Some(ch) => vec![ch],
            None => {
                warn!("Missing 'channels' or 'channel' in event");
                return Err(AppError::MissingChannelInfo);
            }
        },
        Some(_) => {
            warn!("Empty 'channels' list provided in event");
            return Err(AppError::MissingChannelInfo);
        }
    };

    // Store channel info if needed
    let mut channels_info = HashMap::new();

    // Process each target channel
    for target_channel in target_channels {
        info!(channel = %target_channel, "Processing channel");

        // Create the specific message payload
        let message_to_send = PusherApiMessage {
            name: name_clone.clone(),
            data: data.clone(),
            channels: None,
            channel: Some(target_channel.clone()),
            socket_id: mapped_socket_id.as_ref().map(|sid| sid.0.clone()),
            info: info.clone(),
        };

        // Send the message
        handler
            .send_message(
                app_id,
                mapped_socket_id.as_ref(),
                message_to_send,
                &target_channel,
            )
            .await;

        // Collect channel info if requested
        if collect_info {
            // Different info based on channel type
            let is_presence = target_channel.starts_with("presence-");
            let mut channel_info = serde_json::Map::new();

            // Get user_count for presence channels
            if is_presence && info.as_deref().map_or(false, |s| s.contains("user_count")) {
                match handler
                    .channel_manager
                    .read()
                    .await
                    .get_channel_members(app_id, &target_channel)
                    .await
                {
                    Ok(members) => {
                        channel_info.insert("user_count".to_string(), json!(members.len()));
                    }
                    Err(e) => {
                        Log::warning(format!(
                            "Failed to get user count for channel {}: {}",
                            target_channel, e
                        ));
                    }
                }
            }

            // Get subscription_count if requested
            if info
                .as_deref()
                .map_or(false, |s| s.contains("subscription_count"))
            {
                let count = handler
                    .connection_manager
                    .lock()
                    .await
                    .get_channel_socket_count(app_id, &target_channel)
                    .await;
                channel_info.insert("subscription_count".to_string(), json!(count));
            }

            if !channel_info.is_empty() {
                channels_info.insert(target_channel.clone(), Value::Object(channel_info));
            }
        }

        // Handle caching
        if utils::is_cache_channel(&target_channel) {
            let data = match data.clone() {
                Some(data_val) => serde_json::to_value(data_val)?,
                None => json!(null),
            };

            let event_name = name_clone.as_deref().unwrap_or("event");
            let cache_payload_str = match build_cache_payload(event_name, &data) {
                Ok(payload) => payload,
                Err(e) => {
                    error!(channel = %target_channel, error = %e, "Failed to serialize event data for caching");
                    continue;
                }
            };

            let mut cache_manager = handler.cache_manager.lock().await;
            let key = format!("app:{}:channel:{}:cache_miss", app_id, target_channel);

            match cache_manager.set(&key, &cache_payload_str, 3600).await {
                Ok(_) => {
                    info!(channel = %target_channel, cache_key = %key, "Cached event for channel");
                }
                Err(e) => {
                    error!(channel = %target_channel, cache_key = %key, error = %e, "Failed to cache event");
                }
            }
        }
    }

    Ok(channels_info)
}

/// POST /apps/{app_id}/events
#[instrument(skip(handler, event), fields(app_id = %app_id))]
pub async fn events(
    Path(app_id): Path<String>,
    Query(_query): Query<EventQuery>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(event): Json<PusherApiMessage>,
) -> Result<impl IntoResponse, AppError> {
    Log::info(format!("Received event: {:?}", event));

    // Application Validation
    match handler.app_manager.get_app(app_id.as_str()).await {
        Ok(Some(_app)) => {
            info!("App found, processing event.");
        }
        Ok(None) => {
            warn!("App not found during event processing.");
            return Err(AppError::AppNotFound(app_id));
        }
        Err(e) => {
            error!(error = %e, "Failed to fetch app details.");
            return Err(AppError::AppValidationFailed(format!(
                "Error fetching app: {}",
                e
            )));
        }
    }

    // Check if info parameter is provided
    let need_channel_info = event.info.is_some();

    // Process the event
    let channels_info = process_single_event(&handler, &app_id, event, need_channel_info).await?;

    // Return response based on whether info was requested
    if need_channel_info && !channels_info.is_empty() {
        let response = json!({
            "channels": channels_info
        });
        Ok((StatusCode::OK, Json(response)))
    } else {
        Ok((StatusCode::OK, Json(json!({ "ok": true }))))
    }
}

/// POST /apps/{app_id}/batch_events
#[instrument(skip(handler, batch_message), fields(app_id = %app_id, batch_len = field::Empty))]
pub async fn batch_events(
    Path(app_id): Path<String>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(batch_message): Json<BatchPusherApiMessage>,
) -> Result<impl IntoResponse, AppError> {
    let batch_len = batch_message.batch.len();
    tracing::Span::current().record("batch_len", &batch_len);
    info!("Received batch events request");

    let batch = batch_message.batch;
    let batch_size = serde_json::to_vec(&batch)
        .map_err(|e| {
            warn!(error = %e, "Failed to serialize incoming batch for size calculation");
            AppError::SerializationError(e)
        })?
        .len();

    // Process each event and collect info if needed
    let mut batch_info = Vec::new();
    let mut need_info = false;

    for message in batch {
        // Check if any message requests info
        if message.info.is_some() {
            need_info = true;
        }

        // Process the event
        let channel_info =
            process_single_event(&handler, &app_id, message.clone(), message.info.is_some())
                .await?;

        // Add info to the batch response if needed
        if need_info {
            if let Some(channel) = message.channel {
                batch_info.push(
                    channel_info
                        .get(&channel)
                        .cloned()
                        .unwrap_or_else(|| json!({})),
                );
            } else if let Some(channels) = message.channels {
                if !channels.is_empty() {
                    batch_info.push(
                        channel_info
                            .get(&channels[0])
                            .cloned()
                            .unwrap_or_else(|| json!({})),
                    );
                } else {
                    batch_info.push(json!({}));
                }
            } else {
                batch_info.push(json!({}));
            }
        }
    }

    // Prepare response
    let response_payload = if need_info {
        json!({ "batch": batch_info })
    } else {
        json!({ "ok": true })
    };

    let response_size = serde_json::to_vec(&response_payload)?.len();
    record_api_metrics(&handler, &app_id, batch_size, response_size).await;

    info!("Batch events processed successfully");
    Ok((StatusCode::OK, Json(response_payload)))
}

/// GET /apps/{app_id}/channels/{channel_name}
#[instrument(skip(handler), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel(
    Path((app_id, channel_name)): Path<(String, String)>,
    Query(query): Query<ChannelQuery>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    info!("Request for channel info");

    // Parse the info parameter using the helper trait
    let info = query.info.as_ref();
    let wants_subscription_count = info.wants_subscription_count();
    let wants_user_count = info.wants_user_count();
    let wants_cache = info.wants_cache();

    // Get basic channel data
    let mut connection_manager = handler.connection_manager.lock().await;
    let socket_count = connection_manager
        .get_channel_socket_count(&app_id, &channel_name)
        .await;
    drop(connection_manager); // Release the lock early

    // Get user_count for presence channels if requested
    let user_count = if wants_user_count {
        if channel_name.starts_with("presence-") {
            match handler
                .channel_manager
                .read()
                .await
                .get_channel_members(&app_id, &channel_name)
                .await
            {
                Ok(members) => Some(members.len() as u64),
                Err(e) => {
                    return Err(AppError::InternalError(format!(
                        "Failed to get channel members: {}",
                        e
                    )));
                }
            }
        } else {
            return Err(AppError::InternalError(
                "user_count is only available for presence channels".to_string(),
            ));
        }
    } else {
        None
    };

    // Get cache data if requested and if it's a cache channel
    let cache_data = if wants_cache && utils::is_cache_channel(&channel_name) {
        let mut cache_manager = handler.cache_manager.lock().await;
        let key = format!("app:{}:channel:{}:cache_miss", app_id, channel_name);

        match cache_manager.get(&key).await {
            Ok(Some(cache_content)) => {
                // Get TTL for the cache entry
                let ttl = match cache_manager.ttl(&key).await {
                    Ok(ttl) => ttl,
                    _ => Some(core::time::Duration::from_secs(60)),
                };

                Some((cache_content, ttl.unwrap()))
            }
            _ => None,
        }
    } else {
        None
    };

    // Use the helper to create the response
    let subscription_count = if wants_subscription_count {
        Some(socket_count as u64)
    } else {
        None
    };
    let response =
        PusherMessage::channel_info(socket_count > 0, subscription_count, user_count, cache_data);

    let response_json = serde_json::to_vec(&response)?;
    record_api_metrics(&handler, &app_id, 0, response_json.len()).await;

    info!("Channel info retrieved successfully");
    Ok((StatusCode::OK, Json(response)))
}

/// GET /apps/{app_id}/channels
#[instrument(skip(handler), fields(app_id = %app_id))]
pub async fn channels(
    Path(app_id): Path<String>,
    Query(query): Query<ChannelsQuery>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    info!("Request for channels list");

    // Parse query parameters
    let filter_prefix = query.filter_by_prefix.as_deref().unwrap_or("");
    let wants_user_count = query.info.as_ref().wants_user_count();

    // Get channels with socket count
    let mut connection_manager = handler.connection_manager.lock().await;
    let channels = match connection_manager
        .get_channels_with_socket_count(&app_id)
        .await
    {
        Ok(ch) => ch,
        Err(e) => {
            return Err(AppError::InternalError(format!(
                "Failed to get channels: {}",
                e
            )))
        }
    };
    drop(connection_manager); // Release lock early

    // Prepare response
    let mut channels_info = HashMap::new();

    // Process each channel
    for entry in channels.iter() {
        let channel_name = entry.key();
        let _socket_count = *entry.value();

        // Apply prefix filter
        if !channel_name.starts_with(filter_prefix) {
            continue;
        }

        let mut channel_info = serde_json::Map::new();

        // Add user_count for presence channels if requested
        if wants_user_count {
            if channel_name.starts_with("presence-") {
                match handler
                    .channel_manager
                    .read()
                    .await
                    .get_channel_members(&app_id, &channel_name)
                    .await
                {
                    Ok(members) => {
                        channel_info.insert("user_count".to_string(), json!(members.len()));
                    }
                    Err(e) => {
                        Log::warning(format!(
                            "Failed to get user count for channel {}: {}",
                            channel_name, e
                        ));
                        continue;
                    }
                }
            } else if !filter_prefix.starts_with("presence-") {
                // If user_count is requested but not all channels are presence channels, return error
                return Err(AppError::InternalError(
                    "user_count is only available for presence channels. Use filter_by_prefix=presence-".to_string()
                ));
            }
        }

        if !channel_info.is_empty() {
            channels_info.insert(channel_name.clone(), Value::Object(channel_info));
        } else if query.info.is_none() {
            // Include channel with empty attributes when no info is requested
            channels_info.insert(channel_name.clone(), json!({}));
        }
    }

    // Use the helper to create the response
    let response = PusherMessage::channels_list(channels_info);

    let response_json = serde_json::to_vec(&response)?;
    record_api_metrics(&handler, &app_id, 0, response_json.len()).await;

    info!("Channels list retrieved successfully");
    Ok((StatusCode::OK, Json(response)))
}

/// GET /apps/{app_id}/channels/{channel_name}/users
#[instrument(skip(handler), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel_users(
    Path((app_id, channel_name)): Path<(String, String)>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    info!("Request for users in channel");

    // Verify it's a presence channel
    if !channel_name.starts_with("presence-") {
        return Err(AppError::InternalError(
            "Only presence channels support this endpoint".to_string(),
        ));
    }

    // Get channel members
    let channel_members = match handler
        .channel_manager
        .read()
        .await
        .get_channel_members(&app_id, &channel_name)
        .await
    {
        Ok(members) => members,
        Err(e) => {
            return Err(AppError::InternalError(format!(
                "Failed to get channel members: {}",
                e
            )))
        }
    };

    // Convert to user list format
    let users = channel_members
        .keys()
        .map(|user_id| json!({ "id": user_id }))
        .collect::<Vec<_>>();

    let response_payload = json!({ "users": users });
    let response_json = serde_json::to_vec(&response_payload)?;

    record_api_metrics(&handler, &app_id, 0, response_json.len()).await;

    info!(
        user_count = users.len(),
        "Channel users retrieved successfully"
    );
    Ok((StatusCode::OK, Json(response_payload)))
}

/// POST /apps/{app_id}/users/{user_id}/terminate_connections
#[instrument(skip(handler), fields(app_id = %app_id, user_id = %user_id))]
pub async fn terminate_user_connections(
    Path((app_id, user_id)): Path<(String, String)>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    info!("Received request to terminate user connections");

    let connection_manager = handler.connection_manager.clone();

    let result = connection_manager
        .lock()
        .await
        .terminate_connection(&app_id, &user_id)
        .await;

    match result {
        Ok(_) => {
            info!("Successfully initiated termination for user");
            Ok((StatusCode::OK, Json(json!({ "ok": true }))))
        }
        Err(e) => {
            error!(error = %e, "Failed to terminate connections for user");
            Err(AppError::TerminationFailed(format!(
                "Failed to terminate connections for user {} in app {}: {}",
                user_id, app_id, e
            )))
        }
    }
}

/// GET /up/{app_id}
#[instrument(skip(handler), fields(app_id = %app_id))]
pub async fn up(
    Path(app_id): Path<String>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    info!("Health check received");

    if handler.metrics.is_some() {
        record_api_metrics(&handler, &app_id, 0, 2).await; // "OK" has 2 bytes
    } else {
        info!("Metrics system not available for health check.");
    }

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("X-Health-Check", "OK")
        .body("OK".to_string())
        .map_err(AppError::HeaderBuildError)?;

    Ok(response)
}

/// GET /metrics (Prometheus format)
#[instrument(skip(handler), fields(service = "metrics_exporter"))]
pub async fn metrics(
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    info!("Metrics endpoint called");

    let plaintext_metrics = match handler.metrics.clone() {
        Some(metrics_arc) => {
            let metrics_data = metrics_arc.lock().await;
            metrics_data.get_metrics_as_plaintext().await
        }
        None => {
            info!("No metrics data available.");
            "# Metrics collection is not enabled.\n".to_string()
        }
    };

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );

    info!(
        bytes = plaintext_metrics.len(),
        "Successfully generated metrics"
    );
    Ok((StatusCode::OK, headers, plaintext_metrics))
}
