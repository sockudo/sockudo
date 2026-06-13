use sockudo_core::history::{
    HistoryDurableState, HistoryRetentionStats, HistoryStreamInspection, HistoryStreamRuntimeState,
};

use sqlx::{Row, mysql::MySqlRow};

use super::state::parse_history_durable_state;

#[derive(Debug, Clone)]
pub(super) struct HistoryStreamRecord {
    stream_id: String,
    next_serial: u64,
    retained_messages: u64,
    retained_bytes: u64,
    oldest_serial: Option<u64>,
    newest_serial: Option<u64>,
    oldest_published_at_ms: Option<i64>,
    newest_published_at_ms: Option<i64>,
    durable_state: HistoryDurableState,
    durable_state_reason: Option<String>,
    durable_state_node_id: Option<String>,
    durable_state_changed_at_ms: Option<i64>,
}

impl HistoryStreamRecord {
    pub(super) fn from_row(row: MySqlRow) -> Self {
        Self {
            stream_id: row.get::<String, _>("stream_id"),
            next_serial: row.get::<i64, _>("next_serial") as u64,
            retained_messages: row.get::<i64, _>("retained_messages") as u64,
            retained_bytes: row.get::<i64, _>("retained_bytes") as u64,
            oldest_serial: row
                .try_get::<Option<i64>, _>("oldest_available_serial")
                .unwrap_or(None)
                .map(|value| value as u64),
            newest_serial: row
                .try_get::<Option<i64>, _>("newest_available_serial")
                .unwrap_or(None)
                .map(|value| value as u64),
            oldest_published_at_ms: row
                .try_get::<Option<i64>, _>("oldest_available_published_at_ms")
                .unwrap_or(None),
            newest_published_at_ms: row
                .try_get::<Option<i64>, _>("newest_available_published_at_ms")
                .unwrap_or(None),
            durable_state: parse_history_durable_state(
                row.get::<String, _>("durable_state").as_str(),
            ),
            durable_state_reason: row
                .try_get::<Option<String>, _>("durable_state_reason")
                .unwrap_or(None),
            durable_state_node_id: row
                .try_get::<Option<String>, _>("durable_state_node_id")
                .unwrap_or(None),
            durable_state_changed_at_ms: row
                .try_get::<Option<i64>, _>("durable_state_changed_at_ms")
                .unwrap_or(None),
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
