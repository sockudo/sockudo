//! Annotation HTTP surfaces: publish/delete annotations, annotation event reads,
//! annotation authorization, and annotation webhooks.

use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response as AxumResponse},
};
use serde::{Deserialize, Serialize};
use sockudo_adapter::ConnectionHandler;
use sockudo_adapter::handler::annotations::{
    DeleteAnnotationRuntimeRequest, PublishAnnotationRuntimeRequest,
};
use sockudo_core::annotations::{
    Annotation, AnnotationAction, AnnotationEventLookupRequest, AnnotationEventsRequest,
    AnnotationSerial, AnnotationType, RawAnnotationReplayRequest,
};
use sockudo_core::app::App;
use sockudo_core::utils::validate_channel_name;
use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::{AnnotationEventAction, AnnotationEventData};
use sonic_rs::Value;
use std::{collections::HashMap, sync::Arc};
use tracing::{instrument, warn};

use super::AppError;
use super::versioned_messages::{
    VersionMutationPath, parse_message_serial, require_versioned_messages_enabled,
};

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

fn enqueue_annotation_created_webhook(
    handler: &ConnectionHandler,
    app: &App,
    channel: &str,
    message_serial: &str,
    annotation_serial: &str,
    annotation_type: &str,
) {
    let Some(webhook_integration) = handler.webhook_integration().as_ref().cloned() else {
        return;
    };
    let app = app.clone();
    let channel = channel.to_string();
    let message_serial = message_serial.to_string();
    let annotation_serial = annotation_serial.to_string();
    let annotation_type = annotation_type.to_string();
    tokio::spawn(async move {
        if let Err(error) = webhook_integration
            .send_annotation_created(
                &app,
                &channel,
                &message_serial,
                &annotation_serial,
                &annotation_type,
            )
            .await
        {
            warn!(error = %error, "failed to emit annotation_created webhook");
        }
    });
}

fn enqueue_annotation_deleted_webhook(
    handler: &ConnectionHandler,
    app: &App,
    channel: &str,
    message_serial: &str,
    annotation_serial: &str,
    deleted_annotation_serial: &str,
    annotation_type: &str,
) {
    let Some(webhook_integration) = handler.webhook_integration().as_ref().cloned() else {
        return;
    };
    let app = app.clone();
    let channel = channel.to_string();
    let message_serial = message_serial.to_string();
    let annotation_serial = annotation_serial.to_string();
    let deleted_annotation_serial = deleted_annotation_serial.to_string();
    let annotation_type = annotation_type.to_string();
    tokio::spawn(async move {
        if let Err(error) = webhook_integration
            .send_annotation_deleted(
                &app,
                &channel,
                &message_serial,
                &annotation_serial,
                &deleted_annotation_serial,
                &annotation_type,
            )
            .await
        {
            warn!(error = %error, "failed to emit annotation_deleted webhook");
        }
    });
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
    let webhook_app = app.clone();
    let webhook_channel = path.channel_name.clone();
    let webhook_message_serial = path.message_serial.clone();
    let webhook_annotation_type = request.annotation_type.clone();
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
    enqueue_annotation_created_webhook(
        &handler,
        &webhook_app,
        &webhook_channel,
        &webhook_message_serial,
        result.annotation_serial.as_str(),
        &webhook_annotation_type,
    );

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

    let webhook_app = app.clone();
    let webhook_channel = path.channel_name.clone();
    let webhook_message_serial = path.message_serial.clone();
    let webhook_deleted_annotation_serial = target.annotation.serial.as_str().to_string();
    let webhook_annotation_type = target.annotation.annotation_type.as_str().to_string();
    let result = handler
        .delete_annotation_runtime(DeleteAnnotationRuntimeRequest {
            app,
            channel: path.channel_name,
            message_serial,
            target_serial,
        })
        .await?;
    enqueue_annotation_deleted_webhook(
        &handler,
        &webhook_app,
        &webhook_channel,
        &webhook_message_serial,
        result.annotation_serial.as_str(),
        &webhook_deleted_annotation_serial,
        &webhook_annotation_type,
    );

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

#[cfg(test)]
mod tests;
