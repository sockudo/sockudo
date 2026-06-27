//! Mutation endpoints for versioned messages: update, delete, and append, plus
//! their field-delta builders, op-id idempotency keys, and version webhooks.

use axum::{
    Json,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response as AxumResponse},
};
use sockudo_adapter::ConnectionHandler;
use sockudo_core::app::App;
use sockudo_core::utils::validate_channel_name;
use sockudo_core::version_store::StoredVersionRecord;
use sockudo_core::versioned_message_auth::MutationKind;
use sockudo_core::versioned_messages::{
    FieldPatch, MessageAction as CoreMessageAction, MessageAppend, MessageFieldDelta,
};
use sockudo_protocol::messages::AI_MESSAGE_ID_MAX_BYTES;
use sockudo_protocol::versioned_messages::{
    AppendMessageRequest, DeleteMessageRequest, MessageAction as ProtocolMessageAction,
    MutationResponse, UpdateMessageRequest, set_runtime_append_fragment,
};
use std::sync::Arc;
use tracing::{info, instrument, warn};

use super::super::AppError;
use super::super::ai::{
    reserve_ai_append_capacity, rollback_ai_append_capacity, validate_ai_append_caps,
    validate_ai_update_caps,
};
use super::super::events::idempotency_ttl;
use super::{
    VersionMutationPath, build_mutation_version_metadata, parse_message_serial,
    require_versioned_messages_enabled, resolve_mutation_actor_identity,
};

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

fn enqueue_message_version_webhook(
    handler: &ConnectionHandler,
    app: &App,
    channel: &str,
    message_serial: &str,
    version_serial: &str,
    action: ProtocolMessageAction,
) {
    let Some(webhook_integration) = handler.webhook_integration().as_ref().cloned() else {
        return;
    };
    let app = app.clone();
    let channel = channel.to_string();
    let message_serial = message_serial.to_string();
    let version_serial = version_serial.to_string();
    tokio::spawn(async move {
        if let Err(error) = webhook_integration
            .send_message_version_created(
                &app,
                &channel,
                &message_serial,
                &version_serial,
                action.as_str(),
            )
            .await
        {
            warn!(error = %error, "failed to emit message_version_created webhook");
        }
    });
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
        validate_ai_update_caps(
            &handler,
            &path.app_id,
            &path.channel_name,
            request.data.as_ref(),
        )?;
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
    enqueue_message_version_webhook(
        &handler,
        &app,
        &path.channel_name,
        path.message_serial.as_str(),
        updated.version_serial().as_str(),
        ProtocolMessageAction::Update,
    );
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
    enqueue_message_version_webhook(
        &handler,
        &app,
        &path.channel_name,
        path.message_serial.as_str(),
        deleted.version_serial().as_str(),
        ProtocolMessageAction::Delete,
    );
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
    let append_capacity_key = reserve_ai_append_capacity(&handler, &current).await?;
    let appended_message = match current.message.apply_append(
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
    ) {
        Ok(appended_message) => appended_message,
        Err(error) => {
            rollback_ai_append_capacity(&handler, append_capacity_key.as_deref()).await;
            return Err(error.into());
        }
    };
    let appended = StoredVersionRecord {
        app_id: current.app_id.clone(),
        channel: current.channel.clone(),
        original_client_id: current.original_client_id.clone(),
        message: appended_message,
    };
    if let Err(error) = handler
        .version_store()
        .append_version(appended.clone())
        .await
    {
        rollback_ai_append_capacity(&handler, append_capacity_key.as_deref()).await;
        return Err(error.into());
    }
    enqueue_message_version_webhook(
        &handler,
        &app,
        &path.channel_name,
        path.message_serial.as_str(),
        appended.version_serial().as_str(),
        ProtocolMessageAction::Append,
    );
    handler
        .record_ai_stream_activity(&path.app_id, &path.channel_name, &appended)
        .await?;

    let mut runtime_message =
        handler.build_runtime_message_from_record(&appended, Some(reservation.stream_id));
    set_runtime_append_fragment(&mut runtime_message, request.data.clone());
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

#[cfg(test)]
mod tests;
