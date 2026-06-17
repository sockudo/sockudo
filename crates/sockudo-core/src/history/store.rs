use super::types::*;
use crate::error::{Error, Result};
use async_trait::async_trait;

#[async_trait]
pub trait HistoryStore: Send + Sync {
    async fn reserve_publish_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryWriteReservation>;

    async fn append(&self, record: HistoryAppendRecord) -> Result<()>;

    async fn read_page(&self, request: HistoryReadRequest) -> Result<HistoryPage>;

    async fn channel_head(&self, app_id: &str, channel: &str) -> Result<HistoryRetentionStats> {
        Ok(self.stream_inspection(app_id, channel).await?.retained)
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamRuntimeState> {
        Ok(HistoryStreamRuntimeState::healthy(
            app_id, channel, None, "disabled",
        ))
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamInspection> {
        Ok(HistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: None,
            next_serial: None,
            retained: HistoryRetentionStats::default(),
            state: self.stream_runtime_state(app_id, channel).await?,
        })
    }

    async fn reset_stream(
        &self,
        _app_id: &str,
        _channel: &str,
        _reason: &str,
        _requested_by: Option<&str>,
    ) -> Result<HistoryResetResult> {
        Err(Error::Configuration(
            "Durable history reset is not supported by this store".to_string(),
        ))
    }

    async fn purge_stream(
        &self,
        _app_id: &str,
        _channel: &str,
        request: HistoryPurgeRequest,
    ) -> Result<HistoryPurgeResult> {
        request.validate()?;
        Err(Error::Configuration(
            "Durable history purge is not supported by this store".to_string(),
        ))
    }

    async fn runtime_status(&self) -> Result<HistoryRuntimeStatus> {
        Ok(HistoryRuntimeStatus::default())
    }

    /// Purge history entries older than `before_ms` (epoch milliseconds).
    /// Returns `(total_deleted, has_more)` — `has_more` is true if the batch limit was hit.
    /// Backends with native TTL (DynamoDB, ScyllaDB) return `(0, false)`.
    async fn purge_before(&self, before_ms: i64, batch_size: usize) -> Result<(u64, bool)> {
        let _ = (before_ms, batch_size);
        Ok((0, false))
    }
}

#[derive(Default)]
pub struct NoopHistoryStore;

#[async_trait]
impl HistoryStore for NoopHistoryStore {
    async fn reserve_publish_position(
        &self,
        _app_id: &str,
        _channel: &str,
    ) -> Result<HistoryWriteReservation> {
        Err(Error::Configuration(
            "Durable history is not configured".to_string(),
        ))
    }

    async fn append(&self, _record: HistoryAppendRecord) -> Result<()> {
        Err(Error::Configuration(
            "Durable history is not configured".to_string(),
        ))
    }

    async fn read_page(&self, _request: HistoryReadRequest) -> Result<HistoryPage> {
        Err(Error::Configuration(
            "Durable history is not configured".to_string(),
        ))
    }

    async fn channel_head(&self, _app_id: &str, _channel: &str) -> Result<HistoryRetentionStats> {
        Ok(HistoryRetentionStats::default())
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamRuntimeState> {
        Ok(HistoryStreamRuntimeState {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: None,
            durable_state: HistoryDurableState::ResetRequired,
            recovery_allowed: false,
            reset_required: true,
            reason: Some("durable_history_disabled".to_string()),
            node_id: None,
            last_transition_at_ms: None,
            authoritative_source: "disabled".to_string(),
            observed_source: "disabled".to_string(),
        })
    }

    async fn runtime_status(&self) -> Result<HistoryRuntimeStatus> {
        Ok(HistoryRuntimeStatus::default())
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamInspection> {
        Ok(HistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: None,
            next_serial: None,
            retained: HistoryRetentionStats::default(),
            state: self.stream_runtime_state(app_id, channel).await?,
        })
    }
}
