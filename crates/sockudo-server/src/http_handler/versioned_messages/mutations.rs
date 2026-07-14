//! Native HTTP projections over the shared atomic mutable-message service.

use axum::{
    Json,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response as AxumResponse},
};
use sockudo_adapter::{
    ConnectionHandler,
    services::{MutableMessageOperation, MutableMessageResult, MutableMessageService},
};
use sockudo_core::{
    app::App,
    error::Error,
    utils::validate_channel_name,
    version_store::VersionMutation,
    versioned_message_auth::MutationKind,
    versioned_messages::{FieldPatch, MessageAppend, MessageFieldDelta},
};
use sockudo_protocol::{
    messages::AI_MESSAGE_ID_MAX_BYTES,
    versioned_messages::{
        AppendMessageRequest, ClearField, DeleteMessageRequest,
        MessageAction as ProtocolMessageAction, MutationResponse, UpdateMessageRequest,
    },
};
use std::sync::Arc;
use tracing::instrument;

use super::super::AppError;
use super::{
    VersionMutationPath, parse_message_serial, require_versioned_messages_enabled,
    resolve_mutation_actor,
};
use crate::http_handler::ai::mutable_not_permitted;

fn apply_clear_fields(delta: &mut MessageFieldDelta, fields: &[ClearField]) {
    for field in fields {
        match field {
            ClearField::Name => delta.name = FieldPatch::Clear,
            ClearField::Data => delta.data = FieldPatch::Clear,
            ClearField::Extras => delta.extras = FieldPatch::Clear,
        }
    }
}

fn update_delta(request: &UpdateMessageRequest) -> MessageFieldDelta {
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
    apply_clear_fields(&mut delta, &request.clear_fields);
    delta
}

fn delete_delta(request: &DeleteMessageRequest) -> MessageFieldDelta {
    let mut delta = MessageFieldDelta::default();
    if let Some(data) = request.data.clone() {
        delta.data = FieldPatch::Replace(data);
    }
    if let Some(extras) = request.extras.clone() {
        delta.extras = FieldPatch::Replace(extras);
    }
    apply_clear_fields(&mut delta, &request.clear_fields);
    delta
}

fn validate_operation_id(operation_id: Option<&str>) -> Result<(), AppError> {
    if let Some(operation_id) = operation_id
        && (operation_id.is_empty() || operation_id.len() > AI_MESSAGE_ID_MAX_BYTES)
    {
        return Err(AppError::InvalidInput(format!(
            "op_id must be 1..={AI_MESSAGE_ID_MAX_BYTES} bytes"
        )));
    }
    Ok(())
}

fn response(
    path: VersionMutationPath,
    action: ProtocolMessageAction,
    result: MutableMessageResult,
) -> MutationResponse {
    MutationResponse {
        channel: path.channel_name,
        message_serial: path.message_serial,
        action,
        accepted: true,
        version_serial: Some(result.record.version_serial().as_str().to_string()),
        history_serial: Some(result.record.history_serial()),
        delivery_serial: Some(result.record.delivery_serial()),
        status: if result.duplicate {
            "duplicate".to_string()
        } else {
            "applied".to_string()
        },
    }
}

async fn apply_operation(
    path: VersionMutationPath,
    app: App,
    handler: Arc<ConnectionHandler>,
    context: MutationProjectionContext,
    operation: MutableMessageOperation,
) -> Result<MutationResponse, AppError> {
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
    let current = handler
        .version_store()
        .get_latest(&path.app_id, &path.channel_name, &message_serial)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!(
                "Message '{}' was not found in channel '{}'",
                path.message_serial, path.channel_name
            ))
        })?;
    let actor = resolve_mutation_actor(
        &handler,
        &path.app_id,
        &path.channel_name,
        context.kind,
        current.original_client_id.as_deref(),
        context.requested_client_id.as_deref(),
        context.requested_socket_id.as_deref(),
    )
    .await?;

    let result = MutableMessageService::new(Arc::clone(&handler))
        .apply(&app, &path.channel_name, message_serial, actor, operation)
        .await
        .map_err(|error| match error {
            Error::Auth(_) => mutable_not_permitted(),
            other => AppError::from(other),
        })?;
    if let Some(metrics) = handler.metrics() {
        metrics.mark_versioned_message_mutation(
            &path.app_id,
            context.action.as_str(),
            if result.duplicate {
                "duplicate"
            } else {
                "applied"
            },
        );
    }
    Ok(response(path, context.action, result))
}

struct MutationProjectionContext {
    kind: MutationKind,
    requested_client_id: Option<String>,
    requested_socket_id: Option<String>,
    action: ProtocolMessageAction,
}

/// POST /apps/{app_id}/channels/{channel_name}/messages/{message_serial}/update
#[instrument(skip(handler, request), fields(app_id = %path.app_id, channel = %path.channel_name, message_serial = %path.message_serial))]
pub async fn update_message(
    Path(path): Path<VersionMutationPath>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(request): Json<UpdateMessageRequest>,
) -> Result<AxumResponse, AppError> {
    let payload = apply_update_message(path, app, handler, request).await?;
    Ok((StatusCode::OK, Json(payload)).into_response())
}

pub(crate) async fn apply_update_message(
    path: VersionMutationPath,
    app: App,
    handler: Arc<ConnectionHandler>,
    request: UpdateMessageRequest,
) -> Result<MutationResponse, AppError> {
    request.validate().map_err(AppError::InvalidInput)?;
    validate_operation_id(request.op_id.as_deref())?;
    let operation = MutableMessageOperation {
        mutation: VersionMutation::Update(update_delta(&request)),
        requested_client_id: request.client_id.clone(),
        description: request.description,
        metadata: request.metadata,
        operation_id: request.op_id,
        version_serial: None,
        timestamp_ms: None,
    };
    apply_operation(
        path,
        app,
        handler,
        MutationProjectionContext {
            kind: MutationKind::Update,
            requested_client_id: request.client_id,
            requested_socket_id: request.socket_id,
            action: ProtocolMessageAction::Update,
        },
        operation,
    )
    .await
}

/// POST /apps/{app_id}/channels/{channel_name}/messages/{message_serial}/delete
#[instrument(skip(handler, request), fields(app_id = %path.app_id, channel = %path.channel_name, message_serial = %path.message_serial))]
pub async fn delete_message(
    Path(path): Path<VersionMutationPath>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(request): Json<DeleteMessageRequest>,
) -> Result<AxumResponse, AppError> {
    let payload = apply_delete_message(path, app, handler, request).await?;
    Ok((StatusCode::OK, Json(payload)).into_response())
}

pub(crate) async fn apply_delete_message(
    path: VersionMutationPath,
    app: App,
    handler: Arc<ConnectionHandler>,
    request: DeleteMessageRequest,
) -> Result<MutationResponse, AppError> {
    request.validate().map_err(AppError::InvalidInput)?;
    validate_operation_id(request.op_id.as_deref())?;
    let operation = MutableMessageOperation {
        mutation: VersionMutation::Delete(delete_delta(&request)),
        requested_client_id: request.client_id.clone(),
        description: request.description,
        metadata: request.metadata,
        operation_id: request.op_id,
        version_serial: None,
        timestamp_ms: None,
    };
    apply_operation(
        path,
        app,
        handler,
        MutationProjectionContext {
            kind: MutationKind::Delete,
            requested_client_id: request.client_id,
            requested_socket_id: request.socket_id,
            action: ProtocolMessageAction::Delete,
        },
        operation,
    )
    .await
}

/// POST /apps/{app_id}/channels/{channel_name}/messages/{message_serial}/append
#[instrument(skip(handler, request), fields(app_id = %path.app_id, channel = %path.channel_name, message_serial = %path.message_serial))]
pub async fn append_message(
    Path(path): Path<VersionMutationPath>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(request): Json<AppendMessageRequest>,
) -> Result<AxumResponse, AppError> {
    let payload = apply_append_message(path, app, handler, request).await?;
    Ok((StatusCode::OK, Json(payload)).into_response())
}

pub(crate) async fn apply_append_message(
    path: VersionMutationPath,
    app: App,
    handler: Arc<ConnectionHandler>,
    request: AppendMessageRequest,
) -> Result<MutationResponse, AppError> {
    request.validate().map_err(AppError::InvalidInput)?;
    validate_operation_id(request.op_id.as_deref())?;
    let operation = MutableMessageOperation {
        mutation: VersionMutation::Append(MessageAppend {
            data_fragment: request.data,
            extras: request.extras,
        }),
        requested_client_id: request.client_id.clone(),
        description: request.description,
        metadata: request.metadata,
        operation_id: request.op_id,
        version_serial: None,
        timestamp_ms: None,
    };
    apply_operation(
        path,
        app,
        handler,
        MutationProjectionContext {
            kind: MutationKind::Append,
            requested_client_id: request.client_id,
            requested_socket_id: request.socket_id,
            action: ProtocolMessageAction::Append,
        },
        operation,
    )
    .await
}

#[cfg(test)]
mod tests;
