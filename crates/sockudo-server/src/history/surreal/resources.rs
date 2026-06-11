use sockudo_core::error::{Error, Result};
use sockudo_core::history::HistoryRetentionStats;

use super::state::{deterministic_key, retained_from_stream_record};
use super::{HistoryStreamRecord, StoredStreamRecord, SurrealHistoryStore};

impl SurrealHistoryStore {
    pub(super) fn stream_resource(&self, app_id: &str, channel: &str) -> (String, String) {
        (
            self.tables.streams.clone(),
            deterministic_key([app_id, channel].into_iter()),
        )
    }

    pub(super) fn entry_resource(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        serial: u64,
    ) -> (String, String) {
        (
            self.tables.entries.clone(),
            deterministic_key(
                [
                    app_id.to_string(),
                    channel.to_string(),
                    stream_id.to_string(),
                    format!("{serial:020}"),
                ]
                .into_iter(),
            ),
        )
    }

    pub(super) async fn load_stream_raw(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Option<StoredStreamRecord>> {
        self.db
            .select(self.stream_resource(app_id, channel))
            .await
            .map_err(|e| Error::Internal(format!("Failed to fetch SurrealDB history stream: {e}")))
    }

    pub(super) async fn load_stream_record(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Option<HistoryStreamRecord>> {
        let Some(raw) = self.load_stream_raw(app_id, channel).await? else {
            return Ok(None);
        };
        Ok(Some(HistoryStreamRecord::from_stored(raw)))
    }
    pub(super) async fn retained_stats(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryRetentionStats> {
        Ok(match self.load_stream_raw(app_id, channel).await? {
            Some(stream) => retained_from_stream_record(&stream),
            None => HistoryRetentionStats::default(),
        })
    }

    pub(super) async fn upsert_stream_raw(
        &self,
        app_id: &str,
        channel: &str,
        record: &StoredStreamRecord,
    ) -> Result<()> {
        let _: Option<StoredStreamRecord> = self
            .db
            .upsert(self.stream_resource(app_id, channel))
            .content(record.clone())
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to upsert SurrealDB history stream: {e}"))
            })?;
        Ok(())
    }
}
