use crate::error::{Error, Result};
use crate::message_envelope::{
    MessageContent, MessageEnvelope, PublishIdempotencyMetadata, VersionOperationMetadata,
    VersionProjection,
};
use crate::versioned_messages::{
    MessageAction, MessageAppend, MessageFieldDelta, MessageSerial, VersionMetadata, VersionSerial,
    VersionedMessage,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VersionStoreDirection {
    NewestFirst,
    OldestFirst,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionStoreCursor {
    pub version: u8,
    pub version_serial: VersionSerial,
    pub direction: VersionStoreDirection,
}

#[derive(Debug, Clone)]
pub struct VersionStoreReadRequest {
    pub app_id: String,
    pub channel: String,
    pub message_serial: MessageSerial,
    pub direction: VersionStoreDirection,
    pub limit: usize,
    pub cursor: Option<VersionStoreCursor>,
}

impl VersionStoreReadRequest {
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0 {
            return Err(Error::InvalidMessageFormat(
                "version-history limit must be greater than 0".to_string(),
            ));
        }

        if let Some(cursor) = self.cursor.as_ref() {
            if cursor.version != 1 {
                return Err(Error::InvalidMessageFormat(format!(
                    "unsupported version-history cursor version: {}",
                    cursor.version
                )));
            }
            if cursor.direction != self.direction {
                return Err(Error::InvalidMessageFormat(
                    "version-history cursor direction does not match request".to_string(),
                ));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct VersionStorePage {
    pub items: Vec<StoredVersionRecord>,
    pub next_cursor: Option<VersionStoreCursor>,
    pub has_more: bool,
}

#[derive(Debug, Clone)]
pub struct VersionWriteReservation {
    pub stream_id: String,
    pub delivery_serial: u64,
}

#[derive(Debug, Clone)]
pub struct VersionWriteReservationBlock {
    pub stream_id: String,
    pub start_delivery_serial: u64,
    pub len: u64,
}

/// The exact predecessor that a mutation was derived from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionPrecondition {
    pub version_serial: VersionSerial,
    pub delivery_serial: u64,
}

impl VersionPrecondition {
    #[must_use]
    pub fn from_record(record: &StoredVersionRecord) -> Self {
        Self {
            version_serial: record.version_serial().clone(),
            delivery_serial: record.delivery_serial(),
        }
    }

    #[must_use]
    pub fn matches(&self, record: &StoredVersionRecord) -> bool {
        self.version_serial == *record.version_serial()
            && self.delivery_serial == record.delivery_serial()
    }
}

/// Typed mutation payload applied by a [`super::VersionStore`] transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", content = "payload", rename_all = "snake_case")]
pub enum VersionMutation {
    Update(MessageFieldDelta),
    Delete(MessageFieldDelta),
    Append(MessageAppend),
}

impl VersionMutation {
    #[must_use]
    pub fn action(&self) -> MessageAction {
        match self {
            Self::Update(_) => MessageAction::Update,
            Self::Delete(_) => MessageAction::Delete,
            Self::Append(_) => MessageAction::Append,
        }
    }
}

impl VersionMutationRequest {
    /// Evaluate mutation-domain invariants against one backend transaction
    /// snapshot. Persistence and operation-receipt uniqueness remain the
    /// backend's responsibility.
    pub fn apply_to(
        &self,
        current: &StoredVersionRecord,
        stream_id: &str,
        delivery_serial: u64,
        append_count: usize,
    ) -> Result<VersionMutationResult> {
        if !self.expected.matches(current) {
            return Ok(VersionMutationResult::Conflict {
                current: Some(current.clone()),
            });
        }
        if matches!(self.mutation, VersionMutation::Append(_)) {
            if self.limits.reject_append_after_terminal
                && current
                    .message
                    .extras
                    .as_ref()
                    .and_then(|extras| extras.ai_transport_headers())
                    .and_then(|headers| headers.status())
                    .is_some_and(|status| matches!(status, "complete" | "cancelled"))
            {
                return Ok(VersionMutationResult::Rejected(
                    VersionMutationRejection::TerminalMessage,
                ));
            }
            if let Some(limit) = self.limits.max_appends_per_message
                && append_count >= limit
            {
                return Ok(VersionMutationResult::Rejected(
                    VersionMutationRejection::AppendCount { limit },
                ));
            }
        }

        let record = current.apply_mutation(self, stream_id, delivery_serial)?;
        if let Some(limit) = self.limits.max_accumulated_message_bytes
            && record.data_bytes()? > limit
        {
            return Ok(VersionMutationResult::Rejected(
                VersionMutationRejection::AccumulatedMessageBytes { limit },
            ));
        }
        Ok(VersionMutationResult::Applied {
            record,
            stream_id: stream_id.to_string(),
        })
    }
}

/// Limits that must be checked against the same snapshot as a mutation commit.
#[derive(Debug, Clone, Copy, Default)]
pub struct VersionMutationLimits {
    pub max_accumulated_message_bytes: Option<usize>,
    pub max_appends_per_message: Option<usize>,
    pub max_open_streaming_messages_per_channel: Option<usize>,
    pub reject_append_after_terminal: bool,
}

/// Limits evaluated while the first version and its delivery position are
/// committed. In particular, open-stream admission must not depend on an
/// evictable cache observed before the write.
#[derive(Debug, Clone, Copy, Default)]
pub struct VersionCreateLimits {
    pub max_accumulated_message_bytes: Option<usize>,
    pub max_open_streaming_messages_per_channel: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct VersionCreateRequest {
    pub record: StoredVersionRecord,
    pub limits: VersionCreateLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionCreateRejection {
    AccumulatedMessageBytes { limit: usize },
    OpenStreamingMessages { limit: usize },
}

#[derive(Debug, Clone)]
pub enum VersionCreateResult {
    Applied {
        record: StoredVersionRecord,
        stream_id: String,
    },
    Conflict {
        current: Option<StoredVersionRecord>,
    },
    Rejected(VersionCreateRejection),
}

/// One compare-and-apply command. The operation identity contains only hashed
/// material and is persisted with the committed record for crash recovery.
#[derive(Debug, Clone)]
pub struct VersionMutationRequest {
    pub app_id: String,
    pub channel: String,
    pub message_serial: MessageSerial,
    pub expected: VersionPrecondition,
    pub version: VersionMetadata,
    pub mutation: VersionMutation,
    pub idempotency: Option<PublishIdempotencyMetadata>,
    pub limits: VersionMutationLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionMutationRejection {
    AccumulatedMessageBytes { limit: usize },
    AppendCount { limit: usize },
    OpenStreamingMessages { limit: usize },
    TerminalMessage,
}

#[derive(Debug, Clone)]
pub enum VersionMutationResult {
    Applied {
        record: StoredVersionRecord,
        stream_id: String,
    },
    Duplicate {
        record: StoredVersionRecord,
        stream_id: String,
    },
    Conflict {
        current: Option<StoredVersionRecord>,
    },
    Rejected(VersionMutationRejection),
}

impl VersionWriteReservationBlock {
    pub(super) fn validate(block_size: u64) -> Result<()> {
        if block_size == 0 {
            return Err(Error::InvalidMessageFormat(
                "version delivery reservation block size must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct VersionStreamState {
    pub stream_id: Option<String>,
    pub next_delivery_serial: Option<u64>,
    pub oldest_available_delivery_serial: Option<u64>,
    pub newest_available_delivery_serial: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct VersionReplayRequest {
    pub app_id: String,
    pub channel: String,
    pub after_delivery_serial: u64,
    pub limit: usize,
}

impl VersionReplayRequest {
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0 {
            return Err(Error::InvalidMessageFormat(
                "replay limit must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredVersionRecord {
    pub app_id: String,
    pub channel: String,
    pub original_client_id: Option<String>,
    /// Commit-time facts. `None` is a backward-compatible read of records
    /// written before the envelope migration.
    #[serde(default)]
    pub envelope: Option<MessageEnvelope>,
    pub message: VersionedMessage,
}

impl StoredVersionRecord {
    pub fn message_serial(&self) -> &MessageSerial {
        &self.message.identity.message_serial
    }

    pub fn version_serial(&self) -> &VersionSerial {
        &self.message.version.serial
    }

    pub fn history_serial(&self) -> u64 {
        self.message.identity.history_serial
    }

    pub fn delivery_serial(&self) -> u64 {
        self.message.replay_position.delivery_serial
    }

    #[must_use]
    pub fn is_open_ai_stream(&self) -> bool {
        self.message
            .extras
            .as_ref()
            .and_then(|extras| extras.ai_transport_headers())
            .and_then(|headers| headers.status())
            == Some("streaming")
    }

    pub fn data_bytes(&self) -> Result<usize> {
        match self.message.data.as_ref() {
            None => Ok(0),
            Some(sockudo_protocol::messages::MessageData::String(value)) => Ok(value.len()),
            Some(value) => sonic_rs::to_vec(value)
                .map(|bytes| bytes.len())
                .map_err(Error::from),
        }
    }

    /// Apply the store-owned delivery position to a create record.
    #[must_use]
    pub fn with_delivery_position(mut self, stream_id: &str, delivery_serial: u64) -> Self {
        self.message.replay_position.delivery_serial = delivery_serial;
        if let Some(envelope) = self.envelope.as_mut() {
            envelope.stream_id = Some(stream_id.to_string());
            envelope.delivery_serial = Some(delivery_serial);
        }
        self
    }

    /// Build the next stored version after the store has validated the
    /// predecessor and selected the delivery position.
    pub fn apply_mutation(
        &self,
        request: &VersionMutationRequest,
        stream_id: &str,
        delivery_serial: u64,
    ) -> Result<Self> {
        if self.app_id != request.app_id
            || self.channel != request.channel
            || self.message_serial() != &request.message_serial
        {
            return Err(Error::InvalidMessageFormat(
                "version mutation target does not match its predecessor".to_string(),
            ));
        }

        let message = match &request.mutation {
            VersionMutation::Update(delta) => self.message.apply_mutation(
                MessageAction::Update,
                request.version.clone(),
                delivery_serial,
                delta.clone(),
            )?,
            VersionMutation::Delete(delta) => self.message.apply_mutation(
                MessageAction::Delete,
                request.version.clone(),
                delivery_serial,
                delta.clone(),
            )?,
            VersionMutation::Append(append) => self.message.apply_append(
                request.version.clone(),
                delivery_serial,
                append.clone(),
            )?,
        };

        let mut envelope = self.envelope.clone().unwrap_or_default();
        envelope.acknowledgement_id = Some(message.version.serial.as_str().to_string());
        envelope.name.clone_from(&message.name);
        envelope.data = message
            .data
            .as_ref()
            .map(MessageContent::from_message_data)
            .transpose()
            .map_err(Error::InvalidMessageFormat)?;
        envelope.publisher_client_id = message
            .version
            .client_id
            .clone()
            .or_else(|| envelope.publisher_client_id.clone());
        envelope.extras.clone_from(&message.extras);
        envelope.stream_id = Some(stream_id.to_string());
        envelope.history_serial = Some(message.identity.history_serial);
        envelope.delivery_serial = Some(delivery_serial);
        envelope.action = Some(message.action);
        envelope.message_serial = Some(message.identity.message_serial.clone());
        envelope.version = Some(VersionOperationMetadata {
            serial: message.version.serial.clone(),
            timestamp_ms: message.version.timestamp_ms,
            client_id: message.version.client_id.clone(),
            description: message.version.description.clone(),
            metadata: message.version.metadata.clone(),
            projection: if message.action == MessageAction::Append {
                VersionProjection::AppendFragment
            } else {
                VersionProjection::Aggregate
            },
        });
        envelope.idempotency.clone_from(&request.idempotency);

        Ok(Self {
            app_id: self.app_id.clone(),
            channel: self.channel.clone(),
            original_client_id: self.original_client_id.clone(),
            envelope: Some(envelope),
            message,
        })
    }
}
