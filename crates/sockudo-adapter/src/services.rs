//! Typed protocol-independent operations shared by HTTP projections.

use crate::ConnectionHandler;
use serde::Serialize;
use sockudo_core::{
    app::App,
    error::{Error, Result},
    history::{HistoryDirection, HistoryPage, HistoryQueryBounds, HistoryReadRequest, now_ms},
    idempotency::{
        IdempotencyReceipt, IdempotencyStart, abort_publish, begin_publish, commit_publish,
        commit_recovered_publish, mutation_idempotency_key, publish_fingerprint,
        publish_idempotency_cache_key,
    },
    message_envelope::{
        MessageEnvelope, PublishIdempotencyMetadata, decode_stored_message_payload,
    },
    presence_history::{
        PresenceHistoryEventCause, PresenceHistoryEventKind, PresenceHistoryRetentionPolicy,
        PresenceHistoryTransitionRecord,
    },
    presence_registry::{PresenceRecord, PresenceRemoval, PresenceTransition},
    version_store::{
        StoredVersionRecord, VersionMutation, VersionMutationLimits, VersionMutationRejection,
        VersionMutationRequest, VersionMutationResult, VersionPrecondition, VersionStorePage,
        VersionStoreReadRequest,
    },
    versioned_message_auth::{
        MutationAuthorizationRequest, MutationGrant, MutationKind, authorize_message_mutation,
    },
    versioned_messages::{MessageSerial, VersionMetadata, VersionSerial},
    websocket::{ConnectionCapabilities, SocketId},
};
use sockudo_protocol::{
    messages::{AI_ERROR_PAYLOAD_TOO_LARGE, PusherMessage},
    versioned_messages::set_runtime_append_fragment,
};
use std::{collections::BTreeMap, sync::Arc};
use tracing::warn;

const IDEMPOTENCY_RECOVERY_PAGE_SIZE: usize = 64;
const IDEMPOTENCY_RECOVERY_MAX_PAGES: usize = 16;
const MUTATION_COMPARE_RETRIES: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PresenceOperation {
    Enter,
    Update,
    Leave,
}

impl PresenceOperation {
    fn history_event(self) -> PresenceHistoryEventKind {
        match self {
            Self::Enter => PresenceHistoryEventKind::MemberAdded,
            Self::Update => PresenceHistoryEventKind::MemberUpdated,
            Self::Leave => PresenceHistoryEventKind::MemberRemoved,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PresenceServiceResult {
    pub operation: PresenceOperation,
    pub transition: PresenceTransition,
}

/// Native current-presence and durable-history service used by protocol edges.
#[derive(Clone)]
pub struct PresenceService {
    handler: Arc<ConnectionHandler>,
}

impl PresenceService {
    #[must_use]
    pub fn new(handler: Arc<ConnectionHandler>) -> Self {
        Self { handler }
    }

    pub fn register_connection(&self, app_id: &str, connection_id: &str) -> bool {
        self.handler
            .presence_registry()
            .register_connection(app_id, connection_id)
    }

    pub async fn snapshot(&self, app_id: &str, channel: &str) -> Result<Vec<PresenceRecord>> {
        let mut records = self
            .handler
            .presence_registry()
            .snapshot(app_id, channel)
            .into_iter()
            .map(|record| {
                (
                    (record.connection_id.clone(), record.client_id.clone()),
                    record,
                )
            })
            .collect::<BTreeMap<_, _>>();
        for record in self
            .handler
            .connection_manager()
            .replicated_presence_records(app_id, channel)
            .await?
        {
            let key = (record.connection_id.clone(), record.client_id.clone());
            match records.entry(key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(record);
                }
                std::collections::btree_map::Entry::Occupied(mut entry)
                    if (record.timestamp_ms, &record.id)
                        > (entry.get().timestamp_ms, &entry.get().id) =>
                {
                    entry.insert(record);
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
        }
        Ok(records.into_values().collect())
    }

    fn virtual_socket_id(member: &PresenceRecord) -> String {
        format!("ably:{}:{}", member.connection_id, member.client_id)
    }

    async fn replicate_native_presence(
        &self,
        app_id: &str,
        channel: &str,
        operation: PresenceOperation,
        member: &PresenceRecord,
    ) {
        let Some(horizontal) = self.handler.connection_manager().as_horizontal_adapter() else {
            return;
        };
        let socket_id = Self::virtual_socket_id(member);
        let result = match operation {
            PresenceOperation::Enter => {
                let user_info = match sonic_rs::to_value(member) {
                    Ok(user_info) => user_info,
                    Err(error) => {
                        warn!(
                            app_id,
                            channel,
                            connection_id = %member.connection_id,
                            error = %error,
                            "failed to encode replicated native presence member"
                        );
                        return;
                    }
                };
                horizontal
                    .broadcast_presence_join(
                        app_id,
                        channel,
                        &member.client_id,
                        &socket_id,
                        Some(user_info),
                    )
                    .await
            }
            PresenceOperation::Update => {
                let user_info = match sonic_rs::to_value(member) {
                    Ok(user_info) => user_info,
                    Err(error) => {
                        warn!(
                            app_id,
                            channel,
                            connection_id = %member.connection_id,
                            error = %error,
                            "failed to encode replicated native presence member"
                        );
                        return;
                    }
                };
                horizontal
                    .broadcast_presence_update(
                        app_id,
                        channel,
                        &member.client_id,
                        &socket_id,
                        user_info,
                    )
                    .await
            }
            PresenceOperation::Leave => {
                horizontal
                    .broadcast_presence_leave(app_id, channel, &member.client_id, &socket_id)
                    .await
            }
        };
        if let Err(error) = result {
            warn!(
                app_id,
                channel,
                connection_id = %member.connection_id,
                error = %error,
                "failed to replicate native presence transition"
            );
        }
    }

    async fn record_transition(
        &self,
        app: &App,
        channel: &str,
        operation: PresenceOperation,
        cause: PresenceHistoryEventCause,
        member: &PresenceRecord,
    ) {
        let policy =
            app.resolved_presence_history(channel, &self.handler.server_options().presence_history);
        if !policy.enabled {
            return;
        }
        let user_info = match sonic_rs::to_value(member) {
            Ok(user_info) => user_info,
            Err(error) => {
                warn!(
                    app_id = %app.id,
                    channel = %channel,
                    connection_id = %member.connection_id,
                    error = %error,
                    "failed to encode native presence-history transition"
                );
                return;
            }
        };
        if let Err(error) = self
            .handler
            .presence_history_store()
            .record_transition(PresenceHistoryTransitionRecord {
                app_id: app.id.clone(),
                channel: channel.to_string(),
                event_kind: operation.history_event(),
                cause,
                user_id: member.client_id.clone(),
                connection_id: Some(member.connection_id.clone()),
                user_info: Some(user_info),
                dead_node_id: None,
                dedupe_key: format!(
                    "native-presence:{}:{}:{}",
                    operation.history_event().as_str(),
                    member.connection_id,
                    member.id
                ),
                published_at_ms: member.timestamp_ms,
                retention: PresenceHistoryRetentionPolicy {
                    retention_window_seconds: policy.retention_window_seconds,
                    max_events_per_channel: policy.max_events_per_channel,
                    max_bytes_per_channel: policy.max_bytes_per_channel,
                },
            })
            .await
        {
            // Current membership remains authoritative when the durable tier is
            // degraded. Tracking stores expose that state so continuity-sensitive
            // history reads fail closed, matching native Sockudo presence.
            warn!(
                app_id = %app.id,
                channel = %channel,
                connection_id = %member.connection_id,
                error = %error,
                "failed to persist native presence-history transition"
            );
        }
    }

    pub async fn apply(
        &self,
        app: &App,
        channel: &str,
        operation: PresenceOperation,
        cause: PresenceHistoryEventCause,
        member: PresenceRecord,
    ) -> Result<Option<PresenceServiceResult>> {
        let transition = match operation {
            PresenceOperation::Enter => {
                self.handler
                    .presence_registry()
                    .enter(&app.id, channel, member.clone())?
            }
            PresenceOperation::Update => {
                self.handler
                    .presence_registry()
                    .update(&app.id, channel, member.clone())?
            }
            PresenceOperation::Leave => {
                let Some(transition) = self.handler.presence_registry().leave(
                    &app.id,
                    channel,
                    &member.connection_id,
                    &member.client_id,
                )?
                else {
                    return Ok(None);
                };
                transition
            }
        };
        let history_member = &member;
        self.record_transition(app, channel, operation, cause, history_member)
            .await;
        self.replicate_native_presence(&app.id, channel, operation, history_member)
            .await;
        Ok(Some(PresenceServiceResult {
            operation,
            transition,
        }))
    }

    pub async fn remove_superseded_client(
        &self,
        app: &App,
        channel: &str,
        client_id: &str,
        connection_id: &str,
    ) -> Result<Vec<PresenceRemoval>> {
        let mut removals = self.handler.presence_registry().remove_client_except(
            &app.id,
            channel,
            client_id,
            connection_id,
        );
        for removal in &mut removals {
            removal.member.timestamp_ms = now_ms();
            self.record_transition(
                app,
                &removal.channel,
                PresenceOperation::Leave,
                PresenceHistoryEventCause::Disconnect,
                &removal.member,
            )
            .await;
            self.replicate_native_presence(
                &app.id,
                &removal.channel,
                PresenceOperation::Leave,
                &removal.member,
            )
            .await;
        }
        Ok(removals)
    }

    pub async fn unregister_connection(
        &self,
        app: &App,
        connection_id: &str,
        cause: PresenceHistoryEventCause,
    ) -> Result<Vec<PresenceRemoval>> {
        let mut removals = self
            .handler
            .presence_registry()
            .unregister_connection(&app.id, connection_id);
        for removal in &mut removals {
            removal.member.timestamp_ms = now_ms();
            self.record_transition(
                app,
                &removal.channel,
                PresenceOperation::Leave,
                cause,
                &removal.member,
            )
            .await;
            self.replicate_native_presence(
                &app.id,
                &removal.channel,
                PresenceOperation::Leave,
                &removal.member,
            )
            .await;
        }
        Ok(removals)
    }

    pub async fn detach_connection(
        &self,
        app: &App,
        channel: &str,
        connection_id: &str,
    ) -> Result<Vec<PresenceRemoval>> {
        let mut removals =
            self.handler
                .presence_registry()
                .detach_connection(&app.id, channel, connection_id);
        for removal in &mut removals {
            removal.member.timestamp_ms = now_ms();
            self.record_transition(
                app,
                &removal.channel,
                PresenceOperation::Leave,
                PresenceHistoryEventCause::Disconnect,
                &removal.member,
            )
            .await;
            self.replicate_native_presence(
                &app.id,
                &removal.channel,
                PresenceOperation::Leave,
                &removal.member,
            )
            .await;
        }
        Ok(removals)
    }
}

/// Actor and delivery metadata for a publish operation.
///
/// Actor identity is deliberately distinct from `exclude_socket`; the latter
/// controls fanout only and must never be used for authorization.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PublishContext {
    pub actor_client_id: Option<String>,
    pub publisher_socket_id: Option<SocketId>,
    pub publisher_connection_id: Option<String>,
    pub exclude_socket: Option<SocketId>,
    pub idempotency_key: Option<String>,
    /// Optional commit-time envelope supplied by a protocol edge that has
    /// facts not represented by `PusherMessage`, such as an Ably encoding chain.
    pub envelope: Option<MessageEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishResult {
    pub receipt: IdempotencyReceipt,
    pub duplicate: bool,
}

/// Verified actor facts supplied by an authenticated protocol edge.
#[derive(Debug, Clone, Default)]
pub struct MutableMessageActor {
    pub client_id: Option<String>,
    pub capabilities: Option<ConnectionCapabilities>,
    pub privileged_server: bool,
}

/// Protocol-neutral mutable-message operation.
#[derive(Debug, Clone)]
pub struct MutableMessageOperation {
    pub mutation: VersionMutation,
    pub requested_client_id: Option<String>,
    pub description: Option<String>,
    pub metadata: Option<sonic_rs::Value>,
    pub operation_id: Option<String>,
    pub version_serial: Option<VersionSerial>,
    pub timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct MutableMessageResult {
    pub record: StoredVersionRecord,
    pub stream_id: String,
    pub duplicate: bool,
}

#[derive(Serialize)]
struct MutationFingerprint<'a> {
    app_id: &'a str,
    channel: &'a str,
    message_serial: &'a str,
    mutation: &'a VersionMutation,
    client_id: &'a Option<String>,
    description: &'a Option<String>,
    metadata: &'a Option<sonic_rs::Value>,
    version_serial: &'a Option<VersionSerial>,
    timestamp_ms: Option<i64>,
    actor_client_id: &'a Option<String>,
}

/// Atomic mutable-message service shared by native and compatibility edges.
#[derive(Clone)]
pub struct MutableMessageService {
    handler: Arc<ConnectionHandler>,
}

impl MutableMessageService {
    #[must_use]
    pub fn new(handler: Arc<ConnectionHandler>) -> Self {
        Self { handler }
    }

    fn mutation_kind(mutation: &VersionMutation) -> MutationKind {
        match mutation {
            VersionMutation::Update(_) => MutationKind::Update,
            VersionMutation::Delete(_) => MutationKind::Delete,
            VersionMutation::Append(_) => MutationKind::Append,
        }
    }

    fn actor_client_id(
        actor: &MutableMessageActor,
        requested_client_id: Option<&str>,
    ) -> Result<Option<String>> {
        match (actor.client_id.as_deref(), requested_client_id) {
            (Some(verified), Some(requested)) if verified != requested => Err(Error::Auth(
                "message operation client identity does not match the authenticated actor"
                    .to_string(),
            )),
            (Some(verified), _) => Ok(Some(verified.to_string())),
            // An unverified operation client ID may be preserved as attribution
            // only after an `Any` authorization grant. It is never actor proof.
            (None, Some(_)) => Ok(None),
            (None, None) => Ok(None),
        }
    }

    fn operation_idempotency(
        app_id: &str,
        channel: &str,
        message_serial: &MessageSerial,
        actor: &MutableMessageActor,
        operation: &MutableMessageOperation,
    ) -> Result<Option<PublishIdempotencyMetadata>> {
        let Some(operation_id) = operation.operation_id.as_deref() else {
            return Ok(None);
        };
        if operation_id.is_empty() {
            return Err(Error::InvalidMessageFormat(
                "operation ID must not be empty".to_string(),
            ));
        }
        let action = operation.mutation.action();
        let bytes = sonic_rs::to_vec(&MutationFingerprint {
            app_id,
            channel,
            message_serial: message_serial.as_str(),
            mutation: &operation.mutation,
            client_id: &operation.requested_client_id,
            description: &operation.description,
            metadata: &operation.metadata,
            version_serial: &operation.version_serial,
            timestamp_ms: operation.timestamp_ms,
            actor_client_id: &actor.client_id,
        })?;
        Ok(Some(PublishIdempotencyMetadata {
            cache_key: mutation_idempotency_key(
                app_id,
                channel,
                message_serial.as_str(),
                action.as_str(),
                operation_id,
            ),
            payload_fingerprint: publish_fingerprint(&bytes),
        }))
    }

    fn limits(&self, channel: &str) -> VersionMutationLimits {
        if !self
            .handler
            .server_options()
            .ai_transport
            .matches_channel(channel)
        {
            return VersionMutationLimits::default();
        }
        let config = &self.handler.server_options().ai_transport;
        VersionMutationLimits {
            max_accumulated_message_bytes: Some(config.max_accumulated_message_bytes),
            max_appends_per_message: Some(config.max_appends_per_message),
            max_open_streaming_messages_per_channel: Some(
                config.max_open_streaming_messages_per_channel,
            ),
            reject_append_after_terminal: true,
        }
    }

    fn rejected_error(rejection: VersionMutationRejection) -> Error {
        match rejection {
            VersionMutationRejection::AccumulatedMessageBytes { limit } => Error::AiTransport {
                code: AI_ERROR_PAYLOAD_TOO_LARGE,
                name: "payload_too_large",
                message: format!("accumulated message content exceeds {limit} bytes"),
            },
            VersionMutationRejection::AppendCount { limit } => Error::AiTransport {
                code: AI_ERROR_PAYLOAD_TOO_LARGE,
                name: "payload_too_large",
                message: format!("message append count exceeds {limit}"),
            },
            VersionMutationRejection::OpenStreamingMessages { limit } => Error::AiTransport {
                code: AI_ERROR_PAYLOAD_TOO_LARGE,
                name: "payload_too_large",
                message: format!("open streaming message count exceeds {limit}"),
            },
            VersionMutationRejection::TerminalMessage => {
                Error::InvalidMessageFormat("cannot append to a terminal message".to_string())
            }
        }
    }

    async fn emit_applied_side_effects(
        &self,
        app: &App,
        result: &MutableMessageResult,
    ) -> Result<()> {
        let record = &result.record;
        self.handler
            .record_ai_stream_activity(&record.app_id, &record.channel, record)
            .await?;

        let mut runtime = self
            .handler
            .build_runtime_message_from_record(record, Some(result.stream_id.clone()));
        if let Some(fragment) = record.message.append_fragment.as_ref() {
            set_runtime_append_fragment(&mut runtime, fragment.clone());
        }
        self.handler
            .broadcast_to_channel_force_full_with_envelope(
                app,
                &record.channel,
                runtime,
                None,
                None,
                record.envelope.clone().unwrap_or_default(),
            )
            .await?;

        if let Some(webhooks) = self.handler.webhook_integration().as_ref().cloned() {
            let app = app.clone();
            let channel = record.channel.clone();
            let message_serial = record.message_serial().as_str().to_string();
            let version_serial = record.version_serial().as_str().to_string();
            let action = record.message.action.as_str();
            tokio::spawn(async move {
                if let Err(error) = webhooks
                    .send_message_version_created(
                        &app,
                        &channel,
                        &message_serial,
                        &version_serial,
                        action,
                    )
                    .await
                {
                    warn!(error = %error, "failed to emit message_version_created webhook");
                }
            });
        }
        Ok(())
    }

    /// Authorize, compare, apply, persist, and fan out one mutation.
    pub async fn apply(
        &self,
        app: &App,
        channel: &str,
        message_serial: MessageSerial,
        actor: MutableMessageActor,
        operation: MutableMessageOperation,
    ) -> Result<MutableMessageResult> {
        let actor_client_id =
            Self::actor_client_id(&actor, operation.requested_client_id.as_deref())?;
        let idempotency =
            Self::operation_idempotency(&app.id, channel, &message_serial, &actor, &operation)?;
        let kind = Self::mutation_kind(&operation.mutation);
        let limits = self.limits(channel);

        for _ in 0..MUTATION_COMPARE_RETRIES {
            let Some(current) = self
                .handler
                .version_store()
                .get_latest(&app.id, channel, &message_serial)
                .await?
            else {
                return Err(Error::MessageNotFound(format!(
                    "{} in channel {}",
                    message_serial.as_str(),
                    channel
                )));
            };
            let grant = authorize_message_mutation(MutationAuthorizationRequest {
                channel,
                kind,
                original_client_id: current.original_client_id.as_deref(),
                actor_client_id: actor_client_id.as_deref(),
                capabilities: actor.capabilities.as_ref(),
                privileged_server: actor.privileged_server,
            })?;
            let version_client_id = match grant {
                MutationGrant::Own => actor_client_id.clone(),
                MutationGrant::Any => operation
                    .requested_client_id
                    .clone()
                    .or_else(|| actor_client_id.clone()),
            };

            let version_serial = operation
                .version_serial
                .clone()
                .map(Ok)
                .unwrap_or_else(|| VersionSerial::new(self.handler.next_version_serial()))?;
            let request = VersionMutationRequest {
                app_id: app.id.clone(),
                channel: channel.to_string(),
                message_serial: message_serial.clone(),
                expected: VersionPrecondition::from_record(&current),
                version: VersionMetadata {
                    serial: version_serial,
                    client_id: version_client_id,
                    timestamp_ms: operation.timestamp_ms.unwrap_or_else(now_ms),
                    description: operation.description.clone(),
                    metadata: operation.metadata.clone(),
                },
                mutation: operation.mutation.clone(),
                idempotency: idempotency.clone(),
                limits,
            };
            match self
                .handler
                .version_store()
                .compare_and_apply(request)
                .await?
            {
                VersionMutationResult::Applied { record, stream_id } => {
                    let result = MutableMessageResult {
                        record,
                        stream_id,
                        duplicate: false,
                    };
                    self.emit_applied_side_effects(app, &result).await?;
                    return Ok(result);
                }
                VersionMutationResult::Duplicate { record, stream_id } => {
                    return Ok(MutableMessageResult {
                        record,
                        stream_id,
                        duplicate: true,
                    });
                }
                VersionMutationResult::Conflict { .. } => continue,
                VersionMutationResult::Rejected(rejection) => {
                    return Err(Self::rejected_error(rejection));
                }
            }
        }

        Err(Error::OverCapacity)
    }
}

#[derive(Serialize)]
struct PublishFingerprint<'a> {
    channel: &'a str,
    message: &'a PusherMessage,
    envelope: &'a MessageEnvelope,
}

fn canonical_publish_fingerprint(
    channel: &str,
    message: &PusherMessage,
    envelope: &MessageEnvelope,
) -> Result<String> {
    let mut canonical_message = message.clone();
    canonical_message.stream_id = None;
    canonical_message.serial = None;
    canonical_message.idempotency_key = None;

    let mut canonical_envelope = envelope.clone();
    canonical_envelope.acknowledgement_id = None;
    canonical_envelope.published_at_ms = None;
    // The origin transport is not payload identity: a REST retry of a realtime
    // publish (or the inverse) must observe the same committed receipt.
    canonical_envelope.publisher_socket_id = None;
    canonical_envelope.publisher_connection_id = None;
    canonical_envelope.stream_id = None;
    canonical_envelope.history_serial = None;
    canonical_envelope.delivery_serial = None;
    canonical_envelope.action = None;
    canonical_envelope.message_serial = None;
    canonical_envelope.version = None;
    canonical_envelope.idempotency = None;

    sonic_rs::to_vec(&PublishFingerprint {
        channel,
        message: &canonical_message,
        envelope: &canonical_envelope,
    })
    .map(|bytes| publish_fingerprint(&bytes))
    .map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_protocol::messages::MessageData;

    fn test_message(user_id: &str) -> PusherMessage {
        PusherMessage {
            event: Some("event".to_string()),
            channel: Some("channel".to_string()),
            data: Some(MessageData::String("payload".to_string())),
            name: None,
            user_id: Some(user_id.to_string()),
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: Some("stable-id".to_string()),
            stream_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        }
    }

    #[test]
    fn fingerprint_is_stable_across_transport_origin_and_commit_metadata() {
        let message = test_message("publisher");
        let mut realtime = MessageEnvelope::from_message(
            &message,
            Some("socket-id".to_string()),
            Some("connection-id".to_string()),
            100,
        )
        .unwrap();
        realtime.acknowledgement_id = Some("ack-one".to_string());
        realtime.history_serial = Some(9);

        let mut rest = MessageEnvelope::from_message(&message, None, None, 200).unwrap();
        rest.acknowledgement_id = Some("ack-two".to_string());
        rest.history_serial = Some(10);

        assert_eq!(
            canonical_publish_fingerprint("channel", &message, &realtime).unwrap(),
            canonical_publish_fingerprint("channel", &message, &rest).unwrap()
        );
    }

    #[test]
    fn fingerprint_changes_with_publisher_identity_or_payload() {
        let message = test_message("publisher-a");
        let envelope = MessageEnvelope::from_message(&message, None, None, 100).unwrap();
        let first = canonical_publish_fingerprint("channel", &message, &envelope).unwrap();

        let mut changed = message.clone();
        changed.user_id = Some("publisher-b".to_string());
        let changed_envelope = MessageEnvelope::from_message(&changed, None, None, 100).unwrap();
        assert_ne!(
            first,
            canonical_publish_fingerprint("channel", &changed, &changed_envelope).unwrap()
        );
    }

    #[test]
    fn mutation_fingerprint_includes_supplied_version_identity_and_timestamp() {
        let base = MutableMessageOperation {
            mutation: VersionMutation::Append(sockudo_core::versioned_messages::MessageAppend {
                data_fragment: "fragment".to_string(),
                extras: None,
            }),
            requested_client_id: Some("client".to_string()),
            description: Some("append".to_string()),
            metadata: Some(sonic_rs::json!({"source": "test"})),
            operation_id: Some("operation".to_string()),
            version_serial: Some(VersionSerial::new("version:1").unwrap()),
            timestamp_ms: Some(100),
        };
        let message_serial = MessageSerial::new("message:1").unwrap();
        let fingerprint = MutableMessageService::operation_idempotency(
            "app",
            "channel",
            &message_serial,
            &MutableMessageActor::default(),
            &base,
        )
        .unwrap()
        .unwrap()
        .payload_fingerprint;

        let mut changed_serial = base.clone();
        changed_serial.version_serial = Some(VersionSerial::new("version:2").unwrap());
        let serial_fingerprint = MutableMessageService::operation_idempotency(
            "app",
            "channel",
            &message_serial,
            &MutableMessageActor::default(),
            &changed_serial,
        )
        .unwrap()
        .unwrap()
        .payload_fingerprint;
        assert_ne!(fingerprint, serial_fingerprint);

        let mut changed_timestamp = base;
        changed_timestamp.timestamp_ms = Some(101);
        let timestamp_fingerprint = MutableMessageService::operation_idempotency(
            "app",
            "channel",
            &message_serial,
            &MutableMessageActor::default(),
            &changed_timestamp,
        )
        .unwrap()
        .unwrap()
        .payload_fingerprint;
        assert_ne!(fingerprint, timestamp_fingerprint);

        let actor_fingerprint = MutableMessageService::operation_idempotency(
            "app",
            "channel",
            &message_serial,
            &MutableMessageActor {
                client_id: Some("other-actor".to_string()),
                ..Default::default()
            },
            &changed_timestamp,
        )
        .unwrap()
        .unwrap()
        .payload_fingerprint;
        assert_ne!(timestamp_fingerprint, actor_fingerprint);
    }
}

fn publish_receipt(
    acknowledgement_id: String,
    ack: Option<crate::handler::connection_management::PublishAck>,
) -> IdempotencyReceipt {
    let stable_acknowledgement_id = ack
        .as_ref()
        .map(|value| value.message_serial.clone())
        .unwrap_or(acknowledgement_id);
    IdempotencyReceipt {
        acknowledgement_id: stable_acknowledgement_id,
        message_serial: ack.as_ref().map(|value| value.message_serial.clone()),
        history_serial: ack.as_ref().map(|value| value.history_serial),
        delivery_serial: ack.as_ref().map(|value| value.delivery_serial),
        version_serial: ack.map(|value| value.version_serial),
    }
}

#[derive(Debug, Clone)]
struct PublishRecoveryIdentity {
    cache_key: String,
    fingerprint: String,
    ttl_seconds: u64,
}

fn receipt_from_history_item(
    item: &sockudo_core::history::HistoryItem,
) -> Result<Option<IdempotencyReceipt>> {
    let payload =
        decode_stored_message_payload(&item.payload_bytes).map_err(Error::InvalidMessageFormat)?;
    let Some(envelope) = payload.envelope else {
        return Ok(None);
    };
    let acknowledgement_id = envelope
        .acknowledgement_id
        .clone()
        .or_else(|| {
            envelope
                .message_serial
                .as_ref()
                .map(|serial| serial.as_str().to_string())
        })
        .or(payload.message.message_id)
        .ok_or_else(|| {
            Error::Internal(
                "durable idempotent publish is missing an acknowledgement identity".to_string(),
            )
        })?;
    let is_versioned = envelope.message_serial.is_some();
    Ok(Some(IdempotencyReceipt {
        acknowledgement_id,
        message_serial: envelope
            .message_serial
            .as_ref()
            .map(|serial| serial.as_str().to_string()),
        history_serial: is_versioned.then(|| envelope.history_serial.unwrap_or(item.serial)),
        delivery_serial: is_versioned
            .then(|| envelope.delivery_serial.or(payload.message.serial))
            .flatten(),
        version_serial: is_versioned
            .then(|| {
                envelope
                    .version
                    .as_ref()
                    .map(|version| version.serial.as_str().to_string())
            })
            .flatten(),
    }))
}

/// Native publish/read service façade used by protocol projections.
#[derive(Clone)]
pub struct MessageService {
    handler: Arc<ConnectionHandler>,
}

impl MessageService {
    #[must_use]
    pub fn new(handler: Arc<ConnectionHandler>) -> Self {
        Self { handler }
    }

    async fn find_durable_publish_receipt(
        &self,
        app_id: &str,
        channel: &str,
        recovery: &PublishRecoveryIdentity,
    ) -> Result<Option<IdempotencyReceipt>> {
        let mut cursor = None;
        for _ in 0..IDEMPOTENCY_RECOVERY_MAX_PAGES {
            let page = match self
                .handler
                .history_store()
                .read_page(HistoryReadRequest {
                    app_id: app_id.to_string(),
                    channel: channel.to_string(),
                    direction: HistoryDirection::NewestFirst,
                    limit: IDEMPOTENCY_RECOVERY_PAGE_SIZE,
                    cursor,
                    bounds: HistoryQueryBounds::default(),
                })
                .await
            {
                Ok(page) => page,
                Err(Error::Configuration(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            for item in &page.items {
                let payload = decode_stored_message_payload(&item.payload_bytes)
                    .map_err(Error::InvalidMessageFormat)?;
                let matches = payload
                    .envelope
                    .as_ref()
                    .and_then(|envelope| envelope.idempotency.as_ref())
                    .is_some_and(|metadata| {
                        metadata.cache_key == recovery.cache_key
                            && metadata.payload_fingerprint == recovery.fingerprint
                    });
                if matches {
                    return receipt_from_history_item(item);
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                return Ok(None);
            };
            cursor = Some(next_cursor);
        }

        tracing::warn!(
            app_id,
            channel,
            idempotency_cache_key = %recovery.cache_key,
            "bounded durable idempotency recovery scan reached its page limit"
        );
        Ok(None)
    }

    async fn recover_durable_publish(
        &self,
        app_id: &str,
        channel: &str,
        recovery: &PublishRecoveryIdentity,
    ) -> Result<Option<IdempotencyReceipt>> {
        let Some(receipt) = self
            .find_durable_publish_receipt(app_id, channel, recovery)
            .await?
        else {
            return Ok(None);
        };
        let committed = commit_recovered_publish(
            self.handler.cache_manager().as_ref(),
            &recovery.cache_key,
            &recovery.fingerprint,
            &receipt,
            recovery.ttl_seconds,
        )
        .await?;
        tracing::info!(
            app_id,
            channel,
            idempotency_cache_key = %recovery.cache_key,
            history_serial = ?committed.history_serial,
            delivery_serial = ?committed.delivery_serial,
            "recovered publish receipt from durable history"
        );
        Ok(Some(committed))
    }

    /// Publish through the native durable/fanout pipeline.
    pub async fn publish_message(
        &self,
        app: &App,
        channel: &str,
        message: PusherMessage,
        context: PublishContext,
    ) -> Result<PublishResult> {
        self.publish_message_with_timing(app, channel, message, context, None, true)
            .await
    }

    pub async fn publish_message_with_timing(
        &self,
        app: &App,
        channel: &str,
        mut message: PusherMessage,
        context: PublishContext,
        timestamp_ms: Option<f64>,
        force_full: bool,
    ) -> Result<PublishResult> {
        if let Some(actor_client_id) = context.actor_client_id.as_ref() {
            if message
                .user_id
                .as_ref()
                .is_some_and(|claimed| claimed != actor_client_id)
            {
                return Err(Error::Auth(
                    "publisher client identity does not match the authenticated actor".to_string(),
                ));
            }
            message.user_id = Some(actor_client_id.clone());
        }
        if message.idempotency_key.is_none() {
            message.idempotency_key.clone_from(&context.idempotency_key);
        }

        let published_at_ms = timestamp_ms
            .map(|value| value.round() as i64)
            .unwrap_or_else(sockudo_core::history::now_ms);
        let mut envelope = match context.envelope {
            Some(envelope) => envelope,
            None => MessageEnvelope::from_message(
                &message,
                context.publisher_socket_id.map(|socket| socket.to_string()),
                context.publisher_connection_id,
                published_at_ms,
            )
            .map_err(Error::InvalidMessageFormat)?,
        };
        let acknowledgement_id = envelope
            .acknowledgement_id
            .clone()
            .or_else(|| message.message_id.clone())
            .unwrap_or_else(sockudo_protocol::messages::generate_message_id);
        envelope.acknowledgement_id = Some(acknowledgement_id.clone());

        let idempotency_key = message
            .message_id
            .as_deref()
            .or_else(|| message.extras_idempotency_key())
            .or(message.idempotency_key.as_deref());
        let idempotency_config =
            app.resolved_idempotency(&self.handler.server_options().idempotency);
        let mut recovery_identity = None;
        let claim = if idempotency_config.enabled {
            if let Some(key) = idempotency_key {
                if key.is_empty() {
                    return Err(Error::InvalidMessageFormat(
                        "idempotency key must not be empty".to_string(),
                    ));
                }
                if key.len() > idempotency_config.max_key_length {
                    return Err(Error::InvalidMessageFormat(format!(
                        "idempotency key exceeds {} bytes",
                        idempotency_config.max_key_length
                    )));
                }
                if let Some(metrics) = self.handler.metrics() {
                    metrics.mark_idempotency_publish(&app.id);
                }
                let fingerprint = canonical_publish_fingerprint(channel, &message, &envelope)?;
                let cache_key = publish_idempotency_cache_key(&app.id, channel, key);
                let recovery = PublishRecoveryIdentity {
                    cache_key: cache_key.clone(),
                    fingerprint: fingerprint.clone(),
                    ttl_seconds: idempotency_config.ttl_seconds,
                };
                tracing::debug!(
                    app_id = %app.id,
                    channel,
                    idempotency_cache_key = %cache_key,
                    fingerprint = %fingerprint,
                    "Starting idempotent publish"
                );
                match begin_publish(
                    self.handler.cache_manager().as_ref(),
                    cache_key,
                    fingerprint,
                    idempotency_config.ttl_seconds,
                )
                .await
                {
                    Ok(IdempotencyStart::Acquired(claim)) => {
                        envelope.idempotency = Some(PublishIdempotencyMetadata {
                            cache_key: recovery.cache_key.clone(),
                            payload_fingerprint: recovery.fingerprint.clone(),
                        });
                        recovery_identity = Some(recovery);
                        Some(claim)
                    }
                    Ok(IdempotencyStart::Replay(receipt)) => {
                        if let Some(metrics) = self.handler.metrics() {
                            metrics.mark_idempotency_duplicate(&app.id);
                        }
                        return Ok(PublishResult {
                            receipt,
                            duplicate: true,
                        });
                    }
                    Err(Error::IdempotencyInProgress) => {
                        if let Some(receipt) = self
                            .recover_durable_publish(&app.id, channel, &recovery)
                            .await?
                        {
                            if let Some(metrics) = self.handler.metrics() {
                                metrics.mark_idempotency_duplicate(&app.id);
                            }
                            return Ok(PublishResult {
                                receipt,
                                duplicate: true,
                            });
                        }
                        return Err(Error::IdempotencyInProgress);
                    }
                    Err(error) => return Err(error),
                }
            } else {
                None
            }
        } else {
            None
        };

        let result = self
            .handler
            .publish_to_channel_with_context(
                app,
                channel,
                message,
                context.publisher_socket_id.as_ref(),
                context.exclude_socket.as_ref(),
                timestamp_ms,
                force_full,
                Some(envelope),
            )
            .await;
        let ack = match result {
            Ok(ack) => ack,
            Err(error) => {
                if let Some(claim) = claim.as_ref() {
                    let _ = abort_publish(self.handler.cache_manager().as_ref(), claim).await;
                }
                return Err(error);
            }
        };
        let receipt = publish_receipt(acknowledgement_id, ack);
        if let Some(claim) = claim.as_ref()
            && let Err(error) =
                commit_publish(self.handler.cache_manager().as_ref(), claim, &receipt).await
        {
            if matches!(error, Error::IdempotencyInProgress)
                && let Some(recovery) = recovery_identity.as_ref()
                && let Some(recovered) = self
                    .recover_durable_publish(&app.id, channel, recovery)
                    .await?
            {
                return Ok(PublishResult {
                    receipt: recovered,
                    duplicate: false,
                });
            }
            return Err(error);
        }
        Ok(PublishResult {
            receipt,
            duplicate: false,
        })
    }

    pub async fn get_message(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        self.handler
            .version_store()
            .get_latest(app_id, channel, message_serial)
            .await
    }

    pub async fn get_message_versions(
        &self,
        request: VersionStoreReadRequest,
    ) -> Result<VersionStorePage> {
        self.handler.version_store().get_versions(request).await
    }

    pub async fn read_history(&self, request: HistoryReadRequest) -> Result<HistoryPage> {
        self.handler.history_store().read_page(request).await
    }

    #[must_use]
    pub fn handler(&self) -> &Arc<ConnectionHandler> {
        &self.handler
    }
}
