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
        let mut next = record.clone();
        next.retention_revision = Some(record.retention_revision.unwrap_or(0) + 1);
        let latest = self.load_stream_raw(app_id, channel).await?;
        if latest.is_none() {
            let _: Option<StoredStreamRecord> = self
                .db
                .create(self.stream_resource(app_id, channel))
                .content(next)
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to create SurrealDB history stream during reset: {e}"
                    ))
                })?;
            return Ok(());
        }

        if let Some(latest) = &latest {
            if latest.stream_id == record.stream_id {
                next.next_serial = next.next_serial.max(latest.next_serial);
            }
        }
        let mut response = self.db.query("UPDATE ONLY type::record($table,$id) CONTENT $content WHERE (retention_revision=$revision OR (retention_revision IS NONE AND $revision=0)) AND next_serial=$expected_next RETURN AFTER")
            .bind(("table",self.tables.streams.clone())).bind(("id",self.stream_resource(app_id,channel).1))
            .bind(("content",next)).bind(("revision",record.retention_revision.unwrap_or(0)))
            .bind(("expected_next",latest.map_or(record.next_serial, |state|state.next_serial)))
            .await.map_err(|e|Error::Internal(format!("Failed to conditionally update SurrealDB history stream: {e}")))?;
        let updated: Option<StoredStreamRecord> = response.take(0usize).map_err(|e| {
            Error::Internal(format!(
                "Failed to decode SurrealDB history stream update: {e}"
            ))
        })?;
        if updated.is_none() {
            return Err(Error::Internal(
                "SurrealDB history stream changed during update".into(),
            ));
        }
        Ok(())
    }
}
