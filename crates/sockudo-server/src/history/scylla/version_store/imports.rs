use super::*;
use scylla::statement::batch::{Batch, BatchType};
use scylla::value::CqlValue;

impl ScyllaVersionStore {
    /// Import into an already-authoritative atomic chain. Legacy-only chains
    /// remain on their projection path until all retained versions are migrated.
    pub(super) async fn import_atomic_version(
        &self,
        record: &StoredVersionRecord,
    ) -> Result<Option<StoredVersionRecord>> {
        let commits = self.tables.version_commits_fq();
        let message_key = message_commit_key(record.message_serial().as_str());
        let version_key = version_commit_key(
            record.message_serial().as_str(),
            record.version_serial().as_str(),
        );
        let delivery_key = delivery_commit_key(record.delivery_serial());
        let bytes = sonic_rs::to_vec(record)?;
        let read = format!(
            "SELECT payload_bytes FROM {commits} WHERE app_id=? AND channel=? AND commit_key=?"
        );
        for _ in 0..64 {
            let rows = self.session.query_unpaged(format!("SELECT payload_bytes,latest_version_serial,latest_delivery_serial,append_count,is_open_stream FROM {commits} WHERE app_id=? AND channel=? AND commit_key=?"), (&record.app_id,&record.channel,&message_key)).await
                .map_err(|error|Error::Internal(format!("Failed to read ScyllaDB import predecessor: {error}")))?
                .into_rows_result().map_err(|error|Error::Internal(format!("Failed to decode ScyllaDB import predecessor: {error}")))?;
            let current = rows
                .maybe_first_row::<(Vec<u8>, String, i64, i64, bool)>()
                .map_err(|error| {
                    Error::Internal(format!(
                        "Failed to deserialize ScyllaDB import predecessor: {error}"
                    ))
                })?;
            let Some((current_payload, version, delivery, append_count, was_open)) = current else {
                return Ok(None);
            };
            let current = self
                .decode_records(vec![current_payload.clone()])
                .await?
                .remove(0);
            for (is_version, key) in [(true, &version_key), (false, &delivery_key)] {
                let rows = self
                    .session
                    .query_unpaged(read.as_str(), (&record.app_id, &record.channel, key))
                    .await
                    .map_err(|error| {
                        Error::Internal(format!("Failed to read ScyllaDB import identity: {error}"))
                    })?
                    .into_rows_result()
                    .map_err(|error| {
                        Error::Internal(format!(
                            "Failed to decode ScyllaDB import identity: {error}"
                        ))
                    })?;
                if let Some((payload,)) = rows.maybe_first_row::<(Vec<u8>,)>().map_err(|error| {
                    Error::Internal(format!(
                        "Failed to deserialize ScyllaDB import identity: {error}"
                    ))
                })? {
                    let existing = self.decode_records(vec![payload]).await?.remove(0);
                    // Raw imports retain the first version row. Repair legacy
                    // projections from the canonical winner on duplicate retry.
                    if is_version || sonic_rs::to_value(&existing)? == sonic_rs::to_value(record)? {
                        return Ok(Some(existing));
                    }
                    return Err(Error::InvalidMessageFormat(
                        "Conflicting ScyllaDB imported delivery serial".into(),
                    ));
                }
            }
            if current.history_serial() != record.history_serial() {
                return Err(Error::InvalidMessageFormat(
                    "Mixed history_serial values in imported version chain".into(),
                ));
            }
            let rows=self.session.query_unpaged(format!("SELECT next_delivery_serial,open_stream_count FROM {commits} WHERE app_id=? AND channel=? AND commit_key='s'"), (&record.app_id,&record.channel)).await
                .map_err(|error|Error::Internal(format!("Failed to read ScyllaDB import stream: {error}")))?
                .into_rows_result().map_err(|error|Error::Internal(format!("Failed to decode ScyllaDB import stream: {error}")))?;
            let (next_delivery, open_count) = rows.single_row::<(i64, i64)>().map_err(|error| {
                Error::Internal(format!(
                    "Failed to deserialize ScyllaDB import stream: {error}"
                ))
            })?;
            let advances = record.version_serial().as_str() > version.as_str();
            let latest = if advances { record } else { &current };
            let next_open =
                open_count + i64::from(latest.is_open_ai_stream()) - i64::from(was_open);
            let next_append = append_count
                + i64::from(
                    record.message.action
                        == sockudo_core::versioned_messages::MessageAction::Append,
                );
            let floor =
                i64::try_from(record.delivery_serial().saturating_add(1)).map_err(|_| {
                    Error::InvalidMessageFormat("Imported delivery serial is too large".into())
                })?;
            let mut batch = Batch::new(BatchType::Logged);
            batch.set_serial_consistency(Some(SerialConsistency::LocalSerial));
            batch.append_statement(Statement::new(format!("UPDATE {commits} SET next_delivery_serial=?,open_stream_count=? WHERE app_id=? AND channel=? AND commit_key='s' IF next_delivery_serial=? AND open_stream_count=?")));
            batch.append_statement(Statement::new(format!("UPDATE {commits} SET payload_bytes=?,latest_version_serial=?,latest_delivery_serial=?,action=?,append_count=?,is_open_stream=?,created_at_ms=? WHERE app_id=? AND channel=? AND commit_key=? IF latest_version_serial=? AND latest_delivery_serial=? AND append_count=?")));
            let insert = format!(
                "INSERT INTO {commits} (app_id,channel,commit_key,payload_bytes,latest_version_serial,latest_delivery_serial,history_serial,action,created_at_ms) VALUES (?,?,?,?,?,?,?,?,?) IF NOT EXISTS"
            );
            batch.append_statement(Statement::new(insert.clone()));
            batch.append_statement(Statement::new(insert));
            let text = |value: &str| Some(CqlValue::Text(value.to_owned()));
            let number = |value: i64| Some(CqlValue::BigInt(value));
            let now = sockudo_core::history::now_ms();
            let mut values = vec![
                vec![
                    number(next_delivery.max(floor)),
                    number(next_open),
                    text(&record.app_id),
                    text(&record.channel),
                    number(next_delivery),
                    number(open_count),
                ],
                vec![
                    Some(CqlValue::Blob(if advances {
                        bytes.clone()
                    } else {
                        current_payload
                    })),
                    text(latest.version_serial().as_str()),
                    number(latest.delivery_serial() as i64),
                    text(latest.message.action.as_str()),
                    number(next_append),
                    Some(CqlValue::Boolean(latest.is_open_ai_stream())),
                    number(now),
                    text(&record.app_id),
                    text(&record.channel),
                    text(&message_key),
                    text(&version),
                    number(delivery),
                    number(append_count),
                ],
            ];
            for key in [&version_key, &delivery_key] {
                values.push(vec![
                    text(&record.app_id),
                    text(&record.channel),
                    text(key),
                    Some(CqlValue::Blob(bytes.clone())),
                    text(record.version_serial().as_str()),
                    number(record.delivery_serial() as i64),
                    number(record.history_serial() as i64),
                    text(record.message.action.as_str()),
                    number(now),
                ]);
            }
            let result = self.session.batch(&batch, values).await.map_err(|error| {
                Error::Internal(format!(
                    "Failed to commit ScyllaDB imported version: {error}"
                ))
            })?;
            if version_batch_applied(result)? {
                return Ok(Some(record.clone()));
            }
            tokio::task::yield_now().await;
        }
        Err(Error::Internal(
            "ScyllaDB version import contention limit reached".into(),
        ))
    }
}
