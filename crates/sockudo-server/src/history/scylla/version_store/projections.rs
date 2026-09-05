use super::*;

impl ScyllaVersionStore {
    pub(super) async fn append_projection(
        &self,
        record: &StoredVersionRecord,
        payload: &[u8],
    ) -> Result<()> {
        let now_ms = sockudo_core::history::now_ms();
        let payload_size = payload.len() as i64;

        // Write to both entry tables for the two query access patterns.
        let insert_by_msg = format!(
            "INSERT INTO {} (app_id, channel, message_serial, version_serial, delivery_serial, history_serial, action, client_id, description, event_name, payload_bytes, payload_size_bytes, version_timestamp_ms, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS{}",
            self.tables.version_entries_by_message_fq(),
            self.ttl_suffix(),
        );
        self.session
            .query_unpaged(
                insert_by_msg.as_str(),
                (
                    &record.app_id,
                    &record.channel,
                    record.message_serial().as_str(),
                    record.version_serial().as_str(),
                    record.delivery_serial() as i64,
                    record.history_serial() as i64,
                    record.message.action.as_str(),
                    record.original_client_id.as_deref(),
                    record.message.version.description.as_deref(),
                    record.message.name.as_deref(),
                    payload,
                    payload_size,
                    record.message.version.timestamp_ms,
                    now_ms,
                ),
            )
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to insert version entry (by-message): {e}"))
            })?;

        let insert_by_delivery = format!(
            "INSERT INTO {} (app_id, channel, delivery_serial, message_serial, version_serial, history_serial, action, client_id, description, event_name, payload_bytes, payload_size_bytes, version_timestamp_ms, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS{}",
            self.tables.version_entries_by_delivery_fq(),
            self.ttl_suffix(),
        );
        self.session
            .query_unpaged(
                insert_by_delivery.as_str(),
                (
                    &record.app_id,
                    &record.channel,
                    record.delivery_serial() as i64,
                    record.message_serial().as_str(),
                    record.version_serial().as_str(),
                    record.history_serial() as i64,
                    record.message.action.as_str(),
                    record.original_client_id.as_deref(),
                    record.message.version.description.as_deref(),
                    record.message.name.as_deref(),
                    payload,
                    payload_size,
                    record.message.version.timestamp_ms,
                    now_ms,
                ),
            )
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to insert version entry (by-delivery): {e}"))
            })?;

        // Upsert version_messages. ScyllaDB has no conditional upsert like SQL; use a LWT
        // to only advance if the new version_serial is greater than the stored one.
        let select_msg_q = format!(
            "SELECT latest_version_serial FROM {} WHERE app_id = ? AND channel = ? AND message_serial = ?",
            self.tables.version_messages_fq()
        );
        let insert_msg_q = format!(
            "INSERT INTO {} (app_id, channel, message_serial, history_serial, original_client_id, latest_version_serial, latest_delivery_serial, latest_action, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS{}",
            self.tables.version_messages_fq(),
            self.ttl_suffix(),
        );
        let update_msg_q = format!(
            "UPDATE {} {}SET latest_version_serial = ?, latest_delivery_serial = ?, latest_action = ?, updated_at_ms = ? WHERE app_id = ? AND channel = ? AND message_serial = ? IF latest_version_serial < ?",
            self.tables.version_messages_fq(),
            self.update_ttl_clause(),
        );

        let existing = self
            .session
            .query_unpaged(
                select_msg_q.as_str(),
                (
                    &record.app_id,
                    &record.channel,
                    record.message_serial().as_str(),
                ),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to read version message row: {e}")))?
            .into_rows_result()
            .map_err(|e| Error::Internal(format!("Failed to decode version message row: {e}")))?;

        if let Some(row) = existing
            .maybe_first_row::<(Option<String>,)>()
            .map_err(|e| Error::Internal(format!("Failed to deserialize version message: {e}")))?
        {
            let current_serial = row.0.unwrap_or_default();
            if record.version_serial().as_str() > current_serial.as_str() {
                let mut stmt = Statement::new(update_msg_q.clone());
                stmt.set_serial_consistency(Some(SerialConsistency::LocalSerial));
                self.session
                    .query_unpaged(
                        stmt,
                        (
                            record.version_serial().as_str(),
                            record.delivery_serial() as i64,
                            record.message.action.as_str(),
                            now_ms,
                            &record.app_id,
                            &record.channel,
                            record.message_serial().as_str(),
                            record.version_serial().as_str(),
                        ),
                    )
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("Failed to update version message: {e}"))
                    })?;
            }
        } else {
            let mut stmt = Statement::new(insert_msg_q.clone());
            stmt.set_serial_consistency(Some(SerialConsistency::LocalSerial));
            self.session
                .query_unpaged(
                    stmt,
                    (
                        &record.app_id,
                        &record.channel,
                        record.message_serial().as_str(),
                        record.history_serial() as i64,
                        record.original_client_id.as_deref(),
                        record.version_serial().as_str(),
                        record.delivery_serial() as i64,
                        record.message.action.as_str(),
                        now_ms,
                        now_ms,
                    ),
                )
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to insert version message row: {e}"))
                })?;
        }

        // Update stream delivery window (best-effort, non-LWT).
        let update_stream = format!(
            "UPDATE {} SET updated_at_ms = ? WHERE app_id = ? AND channel = ?",
            self.tables.version_streams_fq()
        );
        self.session
            .query_unpaged(
                update_stream.as_str(),
                (now_ms, &record.app_id, &record.channel),
            )
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to update version stream timestamp: {e}"))
            })?;

        Ok(())
    }
}
