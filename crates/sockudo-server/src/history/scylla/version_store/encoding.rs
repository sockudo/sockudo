use super::*;
use sockudo_core::version_store::EncodedVersionRecord;
use std::collections::{BTreeMap, BTreeSet};

impl ScyllaVersionStore {
    pub(super) fn text_key(snapshot_key: &str) -> String {
        format!("t:{snapshot_key}")
    }

    pub(super) async fn decode_records(
        &self,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Vec<StoredVersionRecord>> {
        let records = payloads
            .iter()
            .map(|bytes| EncodedVersionRecord::decode(bytes))
            .collect::<Result<Vec<_>>>()?;
        let mut requested: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
        for record in records.iter().filter(|record| record.needs_snapshot()) {
            requested
                .entry((record.record.app_id.clone(), record.record.channel.clone()))
                .or_default()
                .insert(Self::text_key(
                    &record
                        .text
                        .as_ref()
                        .expect("snapshot reference")
                        .snapshot_key,
                ));
        }
        let mut snapshots = BTreeMap::new();
        for ((app_id, channel), keys) in requested {
            let keys: Vec<_> = keys.into_iter().collect();
            for chunk in keys.chunks(100) {
                let rows = self.session.query_unpaged(format!(
                    "SELECT commit_key, payload_bytes FROM {} WHERE app_id = ? AND channel = ? AND commit_key IN ?", self.tables.version_commits_fq()
                ), (&app_id, &channel, chunk)).await
                    .map_err(|e| Error::Internal(format!("failed to read version snapshots: {e}")))?
                    .into_rows_result().map_err(|e| Error::Internal(format!("failed to decode version snapshots: {e}")))?;
                for row in rows
                    .rows::<(String, Vec<u8>)>()
                    .map_err(|e| Error::Internal(format!("failed to decode snapshot rows: {e}")))?
                {
                    let (key, bytes) = row.map_err(|e| {
                        Error::Internal(format!("failed to decode snapshot row: {e}"))
                    })?;
                    let text = String::from_utf8(bytes)
                        .map_err(|_| Error::Internal("version snapshot is not UTF-8".into()))?;
                    snapshots.insert((app_id.clone(), channel.clone(), key), text);
                }
            }
        }
        records
            .into_iter()
            .map(|record| {
                let text = record.text.as_ref().and_then(|reference| {
                    snapshots.get(&(
                        record.record.app_id.clone(),
                        record.record.channel.clone(),
                        Self::text_key(&reference.snapshot_key),
                    ))
                });
                record.materialize(text.map(String::as_str))
            })
            .collect()
    }
}
