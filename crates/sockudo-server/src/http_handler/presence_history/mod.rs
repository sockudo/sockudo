//! Presence-history HTTP surfaces: paged reads, stream state, reset, and
//! point-in-time membership snapshots for presence channels.

use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use sockudo_adapter::ConnectionHandler;
use sockudo_core::app::App;
use sockudo_core::presence_history::{
    PresenceHistoryCursor, PresenceHistoryDirection, PresenceHistoryQueryBounds,
    PresenceHistoryReadRequest, PresenceHistoryResetResult, PresenceHistoryStreamInspection,
    PresenceHistoryStreamRuntimeState, PresenceSnapshotRequest,
};
use sockudo_core::utils::validate_channel_name;
use sonic_rs::{Value, json};
use std::sync::Arc;
use tracing::instrument;

use super::AppError;
use super::history::{HistoryQuery, HistoryResetRequestBody, validate_history_destructive_request};
use super::system::record_api_metrics;

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

#[derive(Serialize, Default)]
pub(super) struct PresenceHistoryStatusResponse {
    pub(super) enabled: bool,
    pub(super) backend: String,
    pub(super) state_authority: String,
    pub(super) degraded_channels: usize,
    pub(super) reset_required_channels: usize,
    pub(super) queue_depth: usize,
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

#[cfg(test)]
mod tests;
