use super::*;
use sockudo_core::version_store::EncodedVersionRecord;
use std::collections::{BTreeMap, BTreeSet};

impl PostgresVersionStore {
    pub(super) fn text_table(&self) -> String {
        format!("{}_text", self.tables.version_entries)
    }

    pub(super) async fn decode_records(
        &self,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Vec<StoredVersionRecord>> {
        let mut connection = self.pool.acquire().await.map_err(|e| {
            Error::Internal(format!(
                "Failed to acquire version snapshot connection: {e}"
            ))
        })?;
        self.decode_records_on(&mut connection, payloads).await
    }

    pub(super) async fn decode_records_on(
        &self,
        connection: &mut sqlx::PgConnection,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Vec<StoredVersionRecord>> {
        let records = payloads
            .iter()
            .map(|bytes| EncodedVersionRecord::decode(bytes))
            .collect::<Result<Vec<_>>>()?;
        let mut requested: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
        for record in &records {
            if record.needs_snapshot() {
                requested
                    .entry((record.record.app_id.clone(), record.record.channel.clone()))
                    .or_default()
                    .insert(
                        record
                            .text
                            .as_ref()
                            .expect("snapshot reference")
                            .snapshot_key
                            .clone(),
                    );
            }
        }
        let mut snapshots = BTreeMap::new();
        for ((app_id, channel), keys) in requested {
            let keys = keys.into_iter().collect::<Vec<_>>();
            for chunk in keys.chunks(256) {
                let sql = format!(
                    "SELECT snapshot_key, text_data FROM {} WHERE app_id = $1 AND channel = $2 AND snapshot_key = ANY($3)",
                    self.text_table()
                );
                for row in sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(&app_id)
                    .bind(&channel)
                    .bind(chunk)
                    .fetch_all(&mut *connection)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("Failed to read version text snapshots: {e}"))
                    })?
                {
                    snapshots.insert(
                        (
                            app_id.clone(),
                            channel.clone(),
                            row.get::<String, _>("snapshot_key"),
                        ),
                        row.get::<String, _>("text_data"),
                    );
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
                        reference.snapshot_key.clone(),
                    ))
                });
                record.materialize(text.map(String::as_str))
            })
            .collect()
    }
}
