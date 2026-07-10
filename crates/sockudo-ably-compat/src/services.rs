//! Compatibility-facing adapters over Sockudo's typed native services.

use crate::AblyCompatError;
use sockudo_adapter::ConnectionHandler;
use sockudo_core::{
    app::App,
    message_envelope::{
        MessageContent, MessageEnvelope, VersionOperationMetadata, VersionProjection,
    },
    versioned_messages::MessageSerial,
};
use sockudo_protocol::versioned_messages::{
    AppendMessageRequest, DeleteMessageRequest, MutationResponse, UpdateMessageRequest,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct VersionMutationPath {
    pub app_id: String,
    pub channel_name: String,
    pub message_serial: String,
}

fn envelope_for_version(
    current: &sockudo_core::version_store::StoredVersionRecord,
    message: &sockudo_core::versioned_messages::VersionedMessage,
) -> Option<MessageEnvelope> {
    Some(MessageEnvelope {
        message_id: current
            .envelope
            .as_ref()
            .and_then(|envelope| envelope.message_id.clone()),
        acknowledgement_id: Some(message.identity.message_serial.as_str().to_string()),
        name: message.name.clone(),
        data: message
            .data
            .as_ref()
            .and_then(|data| MessageContent::from_message_data(data).ok()),
        encoding: current
            .envelope
            .as_ref()
            .and_then(|envelope| envelope.encoding.clone()),
        publisher_client_id: message.version.client_id.clone().or_else(|| {
            current
                .envelope
                .as_ref()
                .and_then(|envelope| envelope.publisher_client_id.clone())
        }),
        publisher_socket_id: current
            .envelope
            .as_ref()
            .and_then(|envelope| envelope.publisher_socket_id.clone()),
        publisher_connection_id: current
            .envelope
            .as_ref()
            .and_then(|envelope| envelope.publisher_connection_id.clone()),
        published_at_ms: current
            .envelope
            .as_ref()
            .and_then(|envelope| envelope.published_at_ms),
        extras: message.extras.clone(),
        stream_id: current
            .envelope
            .as_ref()
            .and_then(|envelope| envelope.stream_id.clone()),
        history_serial: Some(message.identity.history_serial),
        delivery_serial: Some(message.replay_position.delivery_serial),
        action: Some(message.action),
        message_serial: Some(message.identity.message_serial.clone()),
        version: Some(VersionOperationMetadata {
            serial: message.version.serial.clone(),
            timestamp_ms: message.version.timestamp_ms,
            client_id: message.version.client_id.clone(),
            description: message.version.description.clone(),
            metadata: message.version.metadata.clone(),
            projection: if matches!(
                message.action,
                sockudo_core::versioned_messages::MessageAction::Append
            ) {
                VersionProjection::AppendFragment
            } else {
                VersionProjection::Aggregate
            },
        }),
    })
}

pub fn parse_message_serial(raw: &str) -> Result<MessageSerial, AblyCompatError> {
    MessageSerial::new(raw.to_string()).map_err(AblyCompatError::from)
}

pub async fn apply_update_message(
    path: VersionMutationPath,
    app: App,
    handler: Arc<ConnectionHandler>,
    request: UpdateMessageRequest,
) -> Result<MutationResponse, AblyCompatError> {
    let serial = parse_message_serial(&path.message_serial)?;
    request.validate().map_err(AblyCompatError::InvalidInput)?;
    let current = handler
        .version_store()
        .get_latest(&path.app_id, &path.channel_name, &serial)
        .await?
        .ok_or_else(|| {
            AblyCompatError::NotFound(format!(
                "Message '{}' was not found in channel '{}'",
                path.message_serial, path.channel_name
            ))
        })?;
    let reservation = handler
        .version_store()
        .reserve_delivery_position_after(
            &path.app_id,
            &path.channel_name,
            current.delivery_serial(),
        )
        .await?;
    let metadata = sockudo_core::versioned_messages::VersionMetadata {
        serial: sockudo_core::versioned_messages::VersionSerial::new(
            handler.next_version_serial(),
        )?,
        client_id: request.client_id.clone(),
        timestamp_ms: sockudo_core::history::now_ms(),
        description: request.description.clone(),
        metadata: request.metadata.clone(),
    };
    let mut delta = sockudo_core::versioned_messages::MessageFieldDelta::default();
    if let Some(name) = request.name {
        delta.name = sockudo_core::versioned_messages::FieldPatch::Replace(name);
    }
    if let Some(data) = request.data {
        delta.data = sockudo_core::versioned_messages::FieldPatch::Replace(data);
    }
    if let Some(extras) = request.extras {
        delta.extras = sockudo_core::versioned_messages::FieldPatch::Replace(extras);
    }
    for field in request.clear_fields {
        match field {
            sockudo_protocol::versioned_messages::ClearField::Name => {
                delta.name = sockudo_core::versioned_messages::FieldPatch::Clear
            }
            sockudo_protocol::versioned_messages::ClearField::Data => {
                delta.data = sockudo_core::versioned_messages::FieldPatch::Clear
            }
            sockudo_protocol::versioned_messages::ClearField::Extras => {
                delta.extras = sockudo_core::versioned_messages::FieldPatch::Clear
            }
        }
    }
    let updated_message = current.message.apply_mutation(
        sockudo_core::versioned_messages::MessageAction::Update,
        metadata,
        reservation.delivery_serial,
        delta,
    )?;
    let updated = sockudo_core::version_store::StoredVersionRecord {
        app_id: current.app_id.clone(),
        channel: current.channel.clone(),
        original_client_id: current.original_client_id.clone(),
        envelope: envelope_for_version(&current, &updated_message),
        message: updated_message,
    };
    handler
        .version_store()
        .append_version(updated.clone())
        .await?;
    handler
        .record_ai_stream_activity(&path.app_id, &path.channel_name, &updated)
        .await?;
    let runtime = handler.build_runtime_message_from_record(&updated, Some(reservation.stream_id));
    handler
        .broadcast_to_channel_force_full_with_envelope(
            &app,
            &path.channel_name,
            runtime,
            None,
            None,
            updated.envelope.clone().unwrap_or_default(),
        )
        .await?;
    Ok(MutationResponse {
        channel: path.channel_name,
        message_serial: path.message_serial,
        action: sockudo_protocol::versioned_messages::MessageAction::Update,
        accepted: true,
        version_serial: Some(updated.version_serial().as_str().to_string()),
        history_serial: Some(updated.history_serial()),
        delivery_serial: Some(updated.delivery_serial()),
        status: "applied".to_string(),
    })
}

pub async fn apply_delete_message(
    path: VersionMutationPath,
    app: App,
    handler: Arc<ConnectionHandler>,
    request: DeleteMessageRequest,
) -> Result<MutationResponse, AblyCompatError> {
    let serial = parse_message_serial(&path.message_serial)?;
    request.validate().map_err(AblyCompatError::InvalidInput)?;
    let current = handler
        .version_store()
        .get_latest(&path.app_id, &path.channel_name, &serial)
        .await?
        .ok_or_else(|| {
            AblyCompatError::NotFound(format!(
                "Message '{}' was not found in channel '{}'",
                path.message_serial, path.channel_name
            ))
        })?;
    let reservation = handler
        .version_store()
        .reserve_delivery_position_after(
            &path.app_id,
            &path.channel_name,
            current.delivery_serial(),
        )
        .await?;
    let metadata = sockudo_core::versioned_messages::VersionMetadata {
        serial: sockudo_core::versioned_messages::VersionSerial::new(
            handler.next_version_serial(),
        )?,
        client_id: request.client_id.clone(),
        timestamp_ms: sockudo_core::history::now_ms(),
        description: request.description.clone(),
        metadata: request.metadata.clone(),
    };
    let mut delta = sockudo_core::versioned_messages::MessageFieldDelta::default();
    if let Some(data) = request.data {
        delta.data = sockudo_core::versioned_messages::FieldPatch::Replace(data);
    }
    if let Some(extras) = request.extras {
        delta.extras = sockudo_core::versioned_messages::FieldPatch::Replace(extras);
    }
    for field in request.clear_fields {
        match field {
            sockudo_protocol::versioned_messages::ClearField::Name => {
                delta.name = sockudo_core::versioned_messages::FieldPatch::Clear
            }
            sockudo_protocol::versioned_messages::ClearField::Data => {
                delta.data = sockudo_core::versioned_messages::FieldPatch::Clear
            }
            sockudo_protocol::versioned_messages::ClearField::Extras => {
                delta.extras = sockudo_core::versioned_messages::FieldPatch::Clear
            }
        }
    }
    let deleted_message = current.message.apply_mutation(
        sockudo_core::versioned_messages::MessageAction::Delete,
        metadata,
        reservation.delivery_serial,
        delta,
    )?;
    let deleted = sockudo_core::version_store::StoredVersionRecord {
        app_id: current.app_id.clone(),
        channel: current.channel.clone(),
        original_client_id: current.original_client_id.clone(),
        envelope: envelope_for_version(&current, &deleted_message),
        message: deleted_message,
    };
    handler
        .version_store()
        .append_version(deleted.clone())
        .await?;
    handler
        .record_ai_stream_activity(&path.app_id, &path.channel_name, &deleted)
        .await?;
    let runtime = handler.build_runtime_message_from_record(&deleted, Some(reservation.stream_id));
    handler
        .broadcast_to_channel_force_full_with_envelope(
            &app,
            &path.channel_name,
            runtime,
            None,
            None,
            deleted.envelope.clone().unwrap_or_default(),
        )
        .await?;
    Ok(MutationResponse {
        channel: path.channel_name,
        message_serial: path.message_serial,
        action: sockudo_protocol::versioned_messages::MessageAction::Delete,
        accepted: true,
        version_serial: Some(deleted.version_serial().as_str().to_string()),
        history_serial: Some(deleted.history_serial()),
        delivery_serial: Some(deleted.delivery_serial()),
        status: "applied".to_string(),
    })
}

pub async fn apply_append_message(
    path: VersionMutationPath,
    app: App,
    handler: Arc<ConnectionHandler>,
    request: AppendMessageRequest,
) -> Result<MutationResponse, AblyCompatError> {
    let serial = parse_message_serial(&path.message_serial)?;
    request.validate().map_err(AblyCompatError::InvalidInput)?;
    let current = handler
        .version_store()
        .get_latest(&path.app_id, &path.channel_name, &serial)
        .await?
        .ok_or_else(|| {
            AblyCompatError::NotFound(format!(
                "Message '{}' was not found in channel '{}'",
                path.message_serial, path.channel_name
            ))
        })?;
    let reservation = handler
        .version_store()
        .reserve_delivery_position_after(
            &path.app_id,
            &path.channel_name,
            current.delivery_serial(),
        )
        .await?;
    let metadata = sockudo_core::versioned_messages::VersionMetadata {
        serial: sockudo_core::versioned_messages::VersionSerial::new(
            handler.next_version_serial(),
        )?,
        client_id: request.client_id.clone(),
        timestamp_ms: sockudo_core::history::now_ms(),
        description: request.description.clone(),
        metadata: request.metadata.clone(),
    };
    let appended_message = current.message.apply_append(
        metadata,
        reservation.delivery_serial,
        sockudo_core::versioned_messages::MessageAppend {
            data_fragment: request.data.clone(),
            extras: request.extras.clone(),
        },
    )?;
    let appended = sockudo_core::version_store::StoredVersionRecord {
        app_id: current.app_id.clone(),
        channel: current.channel.clone(),
        original_client_id: current.original_client_id.clone(),
        envelope: envelope_for_version(&current, &appended_message),
        message: appended_message,
    };
    handler
        .version_store()
        .append_version(appended.clone())
        .await?;
    handler
        .record_ai_stream_activity(&path.app_id, &path.channel_name, &appended)
        .await?;
    let mut runtime =
        handler.build_runtime_message_from_record(&appended, Some(reservation.stream_id));
    sockudo_protocol::versioned_messages::set_runtime_append_fragment(&mut runtime, request.data);
    handler
        .broadcast_to_channel_force_full_with_envelope(
            &app,
            &path.channel_name,
            runtime,
            None,
            None,
            appended.envelope.clone().unwrap_or_default(),
        )
        .await?;
    Ok(MutationResponse {
        channel: path.channel_name,
        message_serial: path.message_serial,
        action: sockudo_protocol::versioned_messages::MessageAction::Append,
        accepted: true,
        version_serial: Some(appended.version_serial().as_str().to_string()),
        history_serial: Some(appended.history_serial()),
        delivery_serial: Some(appended.delivery_serial()),
        status: "applied".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use sockudo_adapter::services::PublishContext;

    #[test]
    fn publish_context_keeps_actor_identity_separate_from_fanout_exclusion() {
        let context = PublishContext {
            actor_client_id: Some("actor".to_string()),
            publisher_connection_id: Some("connection".to_string()),
            exclude_socket: None,
            ..Default::default()
        };

        assert_eq!(context.actor_client_id.as_deref(), Some("actor"));
        assert!(context.exclude_socket.is_none());
    }
}
