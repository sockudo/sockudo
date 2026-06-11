use scylla::DeserializeRow;
use sockudo_core::history::{
    HistoryDurableState, HistoryRetentionStats, HistoryStreamInspection, HistoryStreamRuntimeState,
};

use super::state::parse_history_durable_state;

#[derive(Debug, Clone)]
pub(super) struct HistoryStreamRecord {
    pub(super) stream_id: String,
    pub(super) next_serial: u64,
    pub(super) retained_messages: u64,
    pub(super) retained_bytes: u64,
    pub(super) oldest_serial: Option<u64>,
    pub(super) newest_serial: Option<u64>,
    pub(super) oldest_published_at_ms: Option<i64>,
    pub(super) newest_published_at_ms: Option<i64>,
    pub(super) durable_state: HistoryDurableState,
    pub(super) durable_state_reason: Option<String>,
    pub(super) durable_state_node_id: Option<String>,
    pub(super) durable_state_changed_at_ms: Option<i64>,
}

impl HistoryStreamRecord {
    pub(super) fn from_row(row: StreamRow) -> Self {
        Self {
            stream_id: row.stream_id,
            next_serial: row.next_serial as u64,
            retained_messages: row.retained_messages as u64,
            retained_bytes: row.retained_bytes as u64,
            oldest_serial: row.oldest_available_serial.map(|value| value as u64),
            newest_serial: row.newest_available_serial.map(|value| value as u64),
            oldest_published_at_ms: row.oldest_available_published_at_ms,
            newest_published_at_ms: row.newest_available_published_at_ms,
            durable_state: parse_history_durable_state(&row.durable_state),
            durable_state_reason: row.durable_state_reason,
            durable_state_node_id: row.durable_state_node_id,
            durable_state_changed_at_ms: row.durable_state_changed_at_ms,
        }
    }

    pub(super) fn retention_stats(&self) -> HistoryRetentionStats {
        HistoryRetentionStats {
            stream_id: Some(self.stream_id.clone()),
            retained_messages: self.retained_messages,
            retained_bytes: self.retained_bytes,
            oldest_serial: self.oldest_serial,
            newest_serial: self.newest_serial,
            oldest_published_at_ms: self.oldest_published_at_ms,
            newest_published_at_ms: self.newest_published_at_ms,
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
            retained: self.retention_stats(),
            state: self.runtime_state(app_id, channel, observed_source),
        }
    }
}

#[derive(Debug, DeserializeRow)]
pub(super) struct StreamRow {
    pub(super) stream_id: String,
    pub(super) next_serial: i64,
    pub(super) durable_state: String,
    pub(super) durable_state_reason: Option<String>,
    pub(super) durable_state_node_id: Option<String>,
    pub(super) durable_state_changed_at_ms: Option<i64>,
    pub(super) retained_messages: i64,
    pub(super) retained_bytes: i64,
    pub(super) oldest_available_serial: Option<i64>,
    pub(super) newest_available_serial: Option<i64>,
    pub(super) oldest_available_published_at_ms: Option<i64>,
    pub(super) newest_available_published_at_ms: Option<i64>,
}

#[derive(Debug, DeserializeRow, Clone)]
pub(super) struct EntryRow {
    pub(super) stream_id: String,
    pub(super) serial: i64,
    pub(super) published_at_ms: i64,
    pub(super) message_id: Option<String>,
    pub(super) event_name: Option<String>,
    pub(super) operation_kind: String,
    pub(super) payload_bytes: Vec<u8>,
    pub(super) payload_size_bytes: i64,
}

#[derive(Debug, DeserializeRow, Clone)]
pub(super) struct EntryKeyRow {
    pub(super) serial: i64,
    pub(super) published_at_ms: i64,
    pub(super) payload_size_bytes: i64,
}

#[derive(Debug, DeserializeRow)]
pub(super) struct DurableStateRow {
    pub(super) durable_state: String,
}

pub(super) type LwtApplyOnlyRow = (bool,);
pub(super) type LwtConditionalRow = (bool, Option<i64>, Option<String>);
pub(super) type LwtStreamRow = (
    bool,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<i64>,
);
