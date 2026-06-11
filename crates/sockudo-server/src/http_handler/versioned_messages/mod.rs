//! Versioned mutable-message HTTP surfaces: latest reads, version history, and
//! the shared serial/identity/wire-shape helpers used by the mutation endpoints.

mod mutations;

pub use mutations::{append_message, delete_message, update_message};

use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;
use sockudo_adapter::ConnectionHandler;
use sockudo_core::app::App;
use sockudo_core::utils::validate_channel_name;
use sockudo_core::version_store::{
    StoredVersionRecord, VersionStoreDirection, VersionStoreReadRequest,
};
use sockudo_core::versioned_message_auth::{
    MutationAuthorizationRequest, MutationKind, authorize_message_mutation,
};
use sockudo_core::versioned_messages::MessageSerial;
use sockudo_core::versioned_messages::{VersionMetadata, VersionSerial};
use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::PusherMessage;
use sockudo_protocol::versioned_messages::{
    GetMessageResponse, ListMessageVersionsResponse, MessageAction as ProtocolMessageAction,
    MessageVersionMetadata, MessageVersionsQuery, VersionDirection, VersionedRealtimeMessage,
};
use sonic_rs::Value;
use std::sync::Arc;
use tracing::{instrument, warn};

use super::AppError;
use super::ai::mutable_not_permitted;
use super::system::record_api_metrics;

#[derive(Debug, Deserialize)]
pub struct VersionMutationPath {
    #[serde(rename = "appId")]
    pub app_id: String,
    #[serde(rename = "channelName")]
    pub channel_name: String,
    #[serde(rename = "messageSerial")]
    pub message_serial: String,
}

fn parse_version_direction(raw: Option<VersionDirection>) -> VersionStoreDirection {
    match raw.unwrap_or(VersionDirection::NewestFirst) {
        VersionDirection::NewestFirst => VersionStoreDirection::NewestFirst,
        VersionDirection::OldestFirst => VersionStoreDirection::OldestFirst,
    }
}

pub(super) fn require_versioned_messages_enabled(
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

pub(super) fn parse_message_serial(raw: &str) -> Result<MessageSerial, AppError> {
    MessageSerial::new(raw.to_string()).map_err(AppError::from)
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

pub(super) fn build_versioned_realtime_message(
    record: &StoredVersionRecord,
) -> VersionedRealtimeMessage {
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

#[cfg(test)]
mod tests;
