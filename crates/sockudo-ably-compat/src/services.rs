//! Compatibility-facing projections over Sockudo's typed native services.

use crate::AblyCompatError;
use sockudo_adapter::{
    ConnectionHandler,
    services::{
        MutableMessageActor, MutableMessageOperation, MutableMessageResult, MutableMessageService,
    },
};
use sockudo_core::{
    app::App,
    version_store::VersionMutation,
    versioned_messages::{
        FieldPatch, MessageAppend, MessageFieldDelta, MessageSerial, VersionSerial,
    },
};
use sockudo_protocol::versioned_messages::{
    AppendMessageRequest, ClearField, DeleteMessageRequest, MessageAction, MutationResponse,
    UpdateMessageRequest,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct VersionMutationPath {
    pub app_id: String,
    pub channel_name: String,
    pub message_serial: String,
}

pub fn parse_message_serial(raw: &str) -> Result<MessageSerial, AblyCompatError> {
    MessageSerial::new(raw.to_string()).map_err(AblyCompatError::from)
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

fn apply_clear_fields(delta: &mut MessageFieldDelta, fields: &[ClearField]) {
    for field in fields {
        match field {
            ClearField::Name => delta.name = FieldPatch::Clear,
            ClearField::Data => delta.data = FieldPatch::Clear,
            ClearField::Extras => delta.extras = FieldPatch::Clear,
        }
    }
}

fn mutation_response(
    path: VersionMutationPath,
    action: MessageAction,
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
    actor: MutableMessageActor,
    operation: MutableMessageOperation,
    action: MessageAction,
) -> Result<MutationResponse, AblyCompatError> {
    if path.app_id != app.id {
        return Err(AblyCompatError::InvalidInput(
            "mutation app path does not match authenticated app".to_string(),
        ));
    }
    let serial = parse_message_serial(&path.message_serial)?;
    let result = MutableMessageService::new(handler)
        .apply(&app, &path.channel_name, serial, actor, operation)
        .await?;
    Ok(mutation_response(path, action, result))
}

pub async fn apply_update_message(
    path: VersionMutationPath,
    app: App,
    handler: Arc<ConnectionHandler>,
    actor: MutableMessageActor,
    request: UpdateMessageRequest,
    version_serial: Option<VersionSerial>,
    timestamp_ms: Option<i64>,
) -> Result<MutationResponse, AblyCompatError> {
    request.validate().map_err(AblyCompatError::InvalidInput)?;
    let operation = MutableMessageOperation {
        mutation: VersionMutation::Update(update_delta(&request)),
        requested_client_id: request.client_id,
        description: request.description,
        metadata: request.metadata,
        operation_id: request.op_id,
        version_serial,
        timestamp_ms,
    };
    apply_operation(path, app, handler, actor, operation, MessageAction::Update).await
}

pub async fn apply_delete_message(
    path: VersionMutationPath,
    app: App,
    handler: Arc<ConnectionHandler>,
    actor: MutableMessageActor,
    request: DeleteMessageRequest,
    version_serial: Option<VersionSerial>,
    timestamp_ms: Option<i64>,
) -> Result<MutationResponse, AblyCompatError> {
    request.validate().map_err(AblyCompatError::InvalidInput)?;
    let operation = MutableMessageOperation {
        mutation: VersionMutation::Delete(delete_delta(&request)),
        requested_client_id: request.client_id,
        description: request.description,
        metadata: request.metadata,
        operation_id: request.op_id,
        version_serial,
        timestamp_ms,
    };
    apply_operation(path, app, handler, actor, operation, MessageAction::Delete).await
}

pub async fn apply_append_message(
    path: VersionMutationPath,
    app: App,
    handler: Arc<ConnectionHandler>,
    actor: MutableMessageActor,
    request: AppendMessageRequest,
    version_serial: Option<VersionSerial>,
    timestamp_ms: Option<i64>,
) -> Result<MutationResponse, AblyCompatError> {
    request.validate().map_err(AblyCompatError::InvalidInput)?;
    let operation = MutableMessageOperation {
        mutation: VersionMutation::Append(MessageAppend {
            data_fragment: request.data,
            extras: request.extras,
        }),
        requested_client_id: request.client_id,
        description: request.description,
        metadata: request.metadata,
        operation_id: request.op_id,
        version_serial,
        timestamp_ms,
    };
    apply_operation(path, app, handler, actor, operation, MessageAction::Append).await
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
