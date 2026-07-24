//! Durable channel-history HTTP surfaces: paged reads, stream state inspection,
//! and the destructive reset/purge operator endpoints.

use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use sockudo_adapter::ConnectionHandler;
use sockudo_core::app::App;
use sockudo_core::history::{
    HistoryCursor, HistoryDirection, HistoryPurgeMode, HistoryPurgeRequest, HistoryQueryBounds,
    HistoryReadRequest,
};
use sockudo_core::message_envelope::decode_stored_message_payload;
use sockudo_core::utils::validate_channel_name;
use sockudo_protocol::versioned_messages::extract_runtime_message_serial;
use sonic_rs::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use tracing::{instrument, warn};

use super::AppError;
use super::system::record_api_metrics;
use super::versioned_messages::{build_versioned_realtime_message, parse_message_serial};

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

#[derive(Serialize, Default)]
pub(super) struct HistoryStatusResponse {
    pub(super) enabled: bool,
    pub(super) backend: String,
    pub(super) state_authority: String,
    pub(super) degraded_channels: usize,
    pub(super) reset_required_channels: usize,
    pub(super) queue_depth: usize,
}

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

fn parse_history_direction(raw: Option<&str>) -> Result<HistoryDirection, AppError> {
    match raw.unwrap_or("newest_first").to_ascii_lowercase().as_str() {
        "newest_first" | "backwards" | "reverse" => Ok(HistoryDirection::NewestFirst),
        "oldest_first" | "forwards" | "forward" => Ok(HistoryDirection::OldestFirst),
        other => Err(AppError::InvalidInput(format!(
            "Invalid direction '{other}'. Accepted values: newest_first, oldest_first, backwards, forwards"
        ))),
    }
}

pub(super) fn validate_history_destructive_request(
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

    let versioned_messages_enabled = handler.server_options().versioned_messages.enabled;
    let decoded_items = page
        .items
        .into_iter()
        .map(|item| {
            let raw_message = decode_stored_message_payload(item.payload_bytes.as_ref())
                .map(|payload| payload.message)
                .map_err(|e| {
                    AppError::InternalError(format!("Failed to decode history payload: {e}"))
                })?;
            let message_serial = versioned_messages_enabled
                .then(|| extract_runtime_message_serial(&raw_message))
                .flatten()
                .map(parse_message_serial)
                .transpose()?;
            Ok((item, raw_message, message_serial))
        })
        .collect::<Result<Vec<_>, AppError>>()?;
    let message_serials = decoded_items
        .iter()
        .filter_map(|(_, _, serial)| serial.clone())
        .collect::<Vec<_>>();
    let latest_by_serial = if versioned_messages_enabled {
        handler
            .version_store()
            .get_latest_batch(&app_id, &channel_name, &message_serials)
            .await?
    } else {
        BTreeMap::new()
    };

    let mut items = Vec::with_capacity(decoded_items.len());
    for (item, raw_message, message_serial) in decoded_items {
        let mut event_name = item.event_name.clone();
        let mut operation_kind = item.operation_kind.clone();
        let mut payload_size_bytes = item.payload_size_bytes;
        let message: Value = if versioned_messages_enabled {
            if let Some(message_serial) = message_serial {
                match latest_by_serial.get(&message_serial) {
                    Some(latest) => {
                        if let Some(metrics) = handler.metrics() {
                            metrics.mark_versioned_history_substitution(&app_id, "applied");
                        }
                        let latest_message = build_versioned_realtime_message(latest);
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
                            message_serial = %message_serial.as_str(),
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
            sonic_rs::to_value(&raw_message)?
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

#[cfg(test)]
mod tests;
