use super::*;
use sockudo_core::version_store::EncodedVersionRecord;
use std::collections::{BTreeMap, BTreeSet};

impl DynamoDbVersionStore {
    pub(super) fn text_key(snapshot_key: &str) -> String {
        // Legacy/imported version keys always contain '#'. Hex encoding keeps
        // auxiliary keys disjoint even when either user serial contains '#'.
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut key = String::with_capacity(5 + snapshot_key.len() * 2);
        key.push_str("SVR2:");
        for byte in snapshot_key.bytes() {
            key.push(HEX[(byte >> 4) as usize] as char);
            key.push(HEX[(byte & 15) as usize] as char);
        }
        key
    }

    pub(super) async fn decode_records(
        &self,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Vec<StoredVersionRecord>> {
        use aws_sdk_dynamodb::types::KeysAndAttributes;
        let records = payloads
            .iter()
            .map(|bytes| EncodedVersionRecord::decode(bytes))
            .collect::<Result<Vec<_>>>()?;
        let requested = records
            .iter()
            .filter(|record| record.needs_snapshot())
            .map(|record| {
                (
                    Self::app_channel_key(&record.record.app_id, &record.record.channel),
                    Self::text_key(
                        &record
                            .text
                            .as_ref()
                            .expect("snapshot reference")
                            .snapshot_key,
                    ),
                )
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut snapshots = BTreeMap::new();
        for chunk in requested.chunks(100) {
            let keys = chunk
                .iter()
                .map(|(channel, key)| {
                    HashMap::from([
                        ("app_channel".into(), Self::attr_s(channel)),
                        ("message_version_key".into(), Self::attr_s(key)),
                    ])
                })
                .collect();
            let batch = KeysAndAttributes::builder()
                .set_keys(Some(keys))
                .consistent_read(true)
                .projection_expression("app_channel, message_version_key, text_data")
                .build()
                .map_err(|e| {
                    Error::Internal(format!("Failed to build version snapshot batch: {e}"))
                })?;
            let mut pending = HashMap::from([(self.tables.version_entries.clone(), batch)]);
            let mut attempts = 0u32;
            while !pending.is_empty() {
                let result = self
                    .client
                    .batch_get_item()
                    .set_request_items(Some(pending))
                    .send()
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("Failed to read version text snapshots: {e}"))
                    })?;
                for item in result.responses.unwrap_or_default().into_values().flatten() {
                    if let (Some(channel), Some(key), Some(text)) = (
                        Self::item_str(&item, "app_channel"),
                        Self::item_str(&item, "message_version_key"),
                        Self::item_str(&item, "text_data"),
                    ) {
                        snapshots.insert((channel, key), text);
                    }
                }
                pending = result.unprocessed_keys.unwrap_or_default();
                pending.retain(|_, keys| !keys.keys().is_empty());
                if !pending.is_empty() {
                    attempts += 1;
                    if attempts >= 8 {
                        return Err(Error::Internal(
                            "version snapshots remained unprocessed after retries".into(),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        (10u64 << attempts).min(500),
                    ))
                    .await;
                }
            }
        }
        records
            .into_iter()
            .map(|record| {
                let text = record.text.as_ref().and_then(|reference| {
                    snapshots.get(&(
                        Self::app_channel_key(&record.record.app_id, &record.record.channel),
                        Self::text_key(&reference.snapshot_key),
                    ))
                });
                record.materialize(text.map(String::as_str))
            })
            .collect()
    }
}

#[cfg(test)]
mod key_tests {
    use super::*;
    #[test]
    fn snapshot_keys_cannot_collide_with_imported_version_serials() {
        for serial in ["3:msgver", "__text__#arbitrary#serial", "世界"] {
            let key = DynamoDbVersionStore::text_key(serial);
            assert!(!key.contains('#'));
            assert_ne!(key, format!("__text__#{serial}"));
        }
        assert_ne!(
            DynamoDbVersionStore::text_key("a#b"),
            DynamoDbVersionStore::text_key("ab")
        );
    }
}
