use serde::{Deserialize, Serialize};
use sockudo_core::history::{
    HistoryDurableState, HistoryRetentionStats, HistoryStreamInspection, HistoryStreamRuntimeState,
};
use surrealdb_types::SurrealValue;

use super::state::{parse_history_durable_state, retained_from_stream_record};

#[derive(Debug, Clone)]
pub(super) struct HistoryStreamRecord {
    pub(super) stream_id: String,
    pub(super) next_serial: u64,
    pub(super) durable_state: HistoryDurableState,
    pub(super) durable_state_reason: Option<String>,
    pub(super) durable_state_node_id: Option<String>,
    pub(super) durable_state_changed_at_ms: Option<i64>,
    pub(super) retained: HistoryRetentionStats,
}

impl HistoryStreamRecord {
    pub(super) fn from_stored(raw: StoredStreamRecord) -> Self {
        Self {
            stream_id: raw.stream_id.clone(),
            next_serial: raw.next_serial as u64,
            durable_state: parse_history_durable_state(&raw.durable_state),
            durable_state_reason: raw.durable_state_reason.clone(),
            durable_state_node_id: raw.durable_state_node_id.clone(),
            durable_state_changed_at_ms: raw.durable_state_changed_at_ms,
            retained: retained_from_stream_record(&raw),
        }
    }

    pub(super) fn runtime_state(
        &self,
        app_id: &str,
        channel: &str,
        observed_source: &str,
    ) -> HistoryStreamRuntimeState {
        HistoryStreamRuntimeState {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: Some(self.stream_id.clone()),
            durable_state: self.durable_state,
            recovery_allowed: self.durable_state.recovery_allowed(),
            reset_required: self.durable_state.reset_required(),
            reason: self.durable_state_reason.clone(),
            node_id: self.durable_state_node_id.clone(),
            last_transition_at_ms: self.durable_state_changed_at_ms,
            authoritative_source: "durable_store".to_string(),
            observed_source: observed_source.to_string(),
        }
    }

    pub(super) fn inspection(
        &self,
        app_id: &str,
        channel: &str,
        observed_source: &str,
    ) -> HistoryStreamInspection {
        HistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: Some(self.stream_id.clone()),
            next_serial: Some(self.next_serial),
            retained: self.retained.clone(),
            state: self.runtime_state(app_id, channel, observed_source),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
pub(super) struct StoredStreamRecord {
    pub(super) app_id: String,
    pub(super) channel: String,
    pub(super) stream_id: String,
    pub(super) next_serial: i64,
    pub(super) retained_messages: i64,
    pub(super) retained_bytes: i64,
    pub(super) oldest_available_serial: Option<i64>,
    pub(super) newest_available_serial: Option<i64>,
    pub(super) oldest_available_published_at_ms: Option<i64>,
    pub(super) newest_available_published_at_ms: Option<i64>,
    pub(super) durable_state: String,
    pub(super) durable_state_reason: Option<String>,
    pub(super) durable_state_node_id: Option<String>,
    pub(super) durable_state_changed_at_ms: Option<i64>,
    pub(super) updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
pub(super) struct StoredEntryRecord {
    pub(super) app_id: String,
    pub(super) channel: String,
    pub(super) stream_id: String,
    pub(super) serial: i64,
    pub(super) published_at_ms: i64,
    pub(super) message_id: Option<String>,
    pub(super) event_name: Option<String>,
    pub(super) operation_kind: String,
    pub(super) payload_bytes: Vec<u8>,
    pub(super) payload_size_bytes: i64,
}

#[derive(Debug, Clone, Deserialize, SurrealValue)]
pub(super) struct EntryKeyRecord {
    pub(super) serial: i64,
    pub(super) published_at_ms: i64,
    pub(super) payload_size_bytes: i64,
}

#[derive(Debug, Clone, Deserialize, SurrealValue)]
pub(super) struct DurableStateRow {
    pub(super) durable_state: String,
}
