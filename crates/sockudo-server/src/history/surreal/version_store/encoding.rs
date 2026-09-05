use super::*;
use sockudo_core::version_store::EncodedVersionRecord;
use std::collections::{BTreeMap, BTreeSet};

impl SurrealVersionStore {
    // Older raw imports advanced the authoritative entry pointer without
    // refreshing the optional payload cache. Only reuse a matching cache.
    pub(super) fn cached_latest(
        message: &StoredVersionMessageRec,
    ) -> Result<Option<EncodedVersionRecord>> {
        if message.latest_payload_bytes.is_empty() {
            return Ok(None);
        }
        let encoded = EncodedVersionRecord::decode(&message.latest_payload_bytes)?;
        let record = &encoded.record;
        Ok((record.app_id == message.app_id
            && record.channel == message.channel
            && record.message_serial().as_str() == message.message_serial
            && record.version_serial().as_str() == message.latest_version_serial
            && record.history_serial() == message.history_serial as u64)
            .then_some(encoded))
    }

    pub(super) fn text_key(app_id: &str, channel: &str, snapshot_key: &str) -> String {
        deterministic_key([app_id, channel, snapshot_key].into_iter())
    }

    pub(super) async fn decode_records(
        &self,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Vec<StoredVersionRecord>> {
        let records = payloads
            .iter()
            .map(|bytes| EncodedVersionRecord::decode(bytes))
            .collect::<Result<Vec<_>>>()?;
        let requested = records
            .iter()
            .filter(|record| record.needs_snapshot())
            .map(|record| {
                Self::text_key(
                    &record.record.app_id,
                    &record.record.channel,
                    &record
                        .text
                        .as_ref()
                        .expect("snapshot reference")
                        .snapshot_key,
                )
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut snapshots = BTreeMap::new();
        for chunk in requested.chunks(100) {
            let ids: Vec<_> = chunk
                .iter()
                .map(|key| surrealdb::types::RecordId::new(self.tables.texts.clone(), key.clone()))
                .collect();
            let mut response = self
                .db
                .query("SELECT * FROM $ids")
                .bind(("ids", ids))
                .await
                .map_err(|e| Error::Internal(format!("failed to read version snapshots: {e}")))?;
            let texts: Vec<StoredVersionTextRec> = response
                .take(0usize)
                .map_err(|e| Error::Internal(format!("failed to decode version snapshots: {e}")))?;
            for text in texts {
                snapshots.insert(
                    Self::text_key(&text.app_id, &text.channel, &text.snapshot_key),
                    text.text_data,
                );
            }
        }
        records
            .into_iter()
            .map(|record| {
                let text = record.text.as_ref().and_then(|reference| {
                    snapshots.get(&Self::text_key(
                        &record.record.app_id,
                        &record.record.channel,
                        &reference.snapshot_key,
                    ))
                });
                record.materialize(text.map(String::as_str))
            })
            .collect()
    }
    pub(super) async fn purge_texts(&self, before_ms: i64, limit: usize) -> Result<(u64, bool)> {
        #[derive(Deserialize, SurrealValue)]
        struct Candidate {
            id: surrealdb::types::RecordId,
            app_id: String,
            channel: String,
            snapshot_key: String,
        }
        let mut response = self.db.query(format!(
            "SELECT id, app_id, channel, snapshot_key, updated_at_ms FROM {} WHERE retain_for_receipts = false AND updated_at_ms < $cutoff ORDER BY updated_at_ms LIMIT $limit", self.tables.texts
        )).bind(("cutoff", before_ms)).bind(("limit", limit)).await
            .map_err(|e| Error::Internal(format!("failed to select expired version snapshots: {e}")))?;
        let candidates: Vec<Candidate> = response
            .take(0usize)
            .map_err(|e| Error::Internal(format!("failed to decode expired snapshot IDs: {e}")))?;
        let has_more = candidates.len() == limit;
        let mut removed = 0;
        for candidate in candidates {
            // One statement transaction: recheck the age and all retained
            // references while deleting. A concurrent mutation pins/refreshes
            // this row in the same transaction as its new entry.
            let mut response = self.db.query(format!(
                "DELETE $id WHERE retain_for_receipts = false AND updated_at_ms < $cutoff AND array::len((SELECT VALUE id FROM {} WHERE app_id = $app_id AND channel = $channel AND text_snapshot_key = $snapshot_key LIMIT 1)) = 0 RETURN id", self.tables.entries
            )).bind(("id", candidate.id)).bind(("cutoff", before_ms)).bind(("app_id", candidate.app_id)).bind(("channel", candidate.channel)).bind(("snapshot_key", candidate.snapshot_key)).await
                .map_err(|e| Error::Internal(format!("failed to purge version snapshot: {e}")))?;
            let deleted: Vec<VersionDeletedRow> = response.take(0usize).map_err(|e| {
                Error::Internal(format!("failed to decode snapshot purge result: {e}"))
            })?;
            removed += deleted.len() as u64;
        }
        Ok((removed, has_more))
    }
}

#[derive(Deserialize, SurrealValue)]
pub(super) struct VersionDeletedRow {
    #[allow(dead_code)]
    id: surrealdb::types::RecordId,
}
