use super::*;

#[cfg(feature = "versioned-messages")]
#[async_trait::async_trait]
impl VersionStore for ScyllaVersionStore {
    async fn reserve_delivery_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<VersionWriteReservation> {
        let block = self.reserve_delivery_positions(app_id, channel, 1).await?;
        Ok(VersionWriteReservation {
            stream_id: block.stream_id,
            delivery_serial: block.start_delivery_serial,
        })
    }

    async fn reserve_delivery_positions(
        &self,
        app_id: &str,
        channel: &str,
        block_size: u64,
    ) -> Result<VersionWriteReservationBlock> {
        if block_size == 0 {
            return Err(Error::InvalidMessageFormat(
                "version delivery reservation block size must be greater than 0".to_string(),
            ));
        }
        let block_size_i64 = i64::try_from(block_size).map_err(|_| {
            Error::InvalidMessageFormat(
                "version delivery reservation block size is too large".to_string(),
            )
        })?;
        let select_q = format!(
            "SELECT next_delivery_serial FROM {} WHERE app_id = ? AND channel = ?",
            self.tables.version_streams_fq()
        );
        let insert_q = format!(
            "INSERT INTO {} (app_id, channel, next_delivery_serial, migration_state, updated_at_ms) VALUES (?, ?, ?, 'native_only', ?) IF NOT EXISTS",
            self.tables.version_streams_fq()
        );
        let update_q = format!(
            "UPDATE {} SET next_delivery_serial = ?, updated_at_ms = ? WHERE app_id = ? AND channel = ? IF next_delivery_serial = ?",
            self.tables.version_streams_fq()
        );

        loop {
            let rows = self
                .session
                .query_unpaged(select_q.as_str(), (app_id, channel))
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to read ScyllaDB version stream: {e}"))
                })?
                .into_rows_result()
                .map_err(|e| {
                    Error::Internal(format!("Failed to decode ScyllaDB version stream: {e}"))
                })?;

            if let Some(row) = rows.maybe_first_row::<(i64,)>().map_err(|e| {
                Error::Internal(format!("Failed to deserialize version stream: {e}"))
            })? {
                let current = row.0 as u64;
                let now_ms = sockudo_core::history::now_ms();
                let mut stmt = Statement::new(update_q.clone());
                stmt.set_serial_consistency(Some(SerialConsistency::LocalSerial));
                let result = self
                    .session
                    .query_unpaged(
                        stmt,
                        (
                            (current as i64).saturating_add(block_size_i64),
                            now_ms,
                            app_id,
                            channel,
                            current as i64,
                        ),
                    )
                    .await
                    .map_err(|e| map_scylla_lwt_error("advance version delivery serial", e))?;
                if version_lwt_applied(result)? {
                    return Ok(VersionWriteReservationBlock {
                        stream_id: format!("{}/{}", app_id, channel),
                        start_delivery_serial: current,
                        len: block_size,
                    });
                }
                continue;
            }

            let now_ms = sockudo_core::history::now_ms();
            let mut stmt = Statement::new(insert_q.clone());
            stmt.set_serial_consistency(Some(SerialConsistency::LocalSerial));
            let result = self
                .session
                .query_unpaged(
                    stmt,
                    (app_id, channel, block_size_i64.saturating_add(1), now_ms),
                )
                .await
                .map_err(|e| map_scylla_lwt_error("create version stream row", e))?;
            if version_lwt_applied(result)? {
                return Ok(VersionWriteReservationBlock {
                    stream_id: format!("{}/{}", app_id, channel),
                    start_delivery_serial: 1,
                    len: block_size,
                });
            }
        }
    }

    async fn append_version(&self, record: StoredVersionRecord) -> Result<()> {
        let now_ms = sockudo_core::history::now_ms();
        let payload = sonic_rs::to_vec(&record)
            .map_err(|e| Error::Internal(format!("Failed to serialize version record: {e}")))?;
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
                    payload.as_slice(),
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
                    payload.as_slice(),
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

    async fn get_latest(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &sockudo_core::versioned_messages::MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        // version_entries_by_message is clustered by version_serial DESC — LIMIT 1 gives the latest.
        let sql = format!(
            "SELECT payload_bytes FROM {} WHERE app_id = ? AND channel = ? AND message_serial = ? LIMIT 1",
            self.tables.version_entries_by_message_fq()
        );
        let rows = self
            .session
            .query_unpaged(sql.as_str(), (app_id, channel, message_serial.as_str()))
            .await
            .map_err(|e| Error::Internal(format!("Failed to query latest version: {e}")))?
            .into_rows_result()
            .map_err(|e| Error::Internal(format!("Failed to decode latest version: {e}")))?;

        let Some(row) = rows
            .maybe_first_row::<(Vec<u8>,)>()
            .map_err(|e| Error::Internal(format!("Failed to deserialize latest version: {e}")))?
        else {
            return Ok(None);
        };

        let record: StoredVersionRecord = sonic_rs::from_slice(&row.0)
            .map_err(|e| Error::Internal(format!("Failed to deserialize version record: {e}")))?;
        Ok(Some(record))
    }

    async fn get_versions(&self, request: VersionStoreReadRequest) -> Result<VersionStorePage> {
        request.validate()?;
        // version_entries_by_message is clustered by version_serial DESC.
        // For NewestFirst: just read in natural order (DESC). For OldestFirst: use CLUSTERING ORDER.
        // Scylla doesn't support changing order per-query, but we can ORDER BY explicitly.
        let fetch_limit = (request.limit + 1) as i32;

        let rows = if let Some(cursor) = &request.cursor {
            let (op, order) = match request.direction {
                VersionStoreDirection::NewestFirst => ("<", "DESC"),
                VersionStoreDirection::OldestFirst => (">", "ASC"),
            };
            let sql = format!(
                "SELECT payload_bytes FROM {} WHERE app_id = ? AND channel = ? AND message_serial = ? AND version_serial {} ? ORDER BY version_serial {} LIMIT ?",
                self.tables.version_entries_by_message_fq(),
                op,
                order
            );
            self.session
                .query_unpaged(
                    sql.as_str(),
                    (
                        &request.app_id,
                        &request.channel,
                        request.message_serial.as_str(),
                        cursor.version_serial.as_str(),
                        fetch_limit,
                    ),
                )
                .await
                .map_err(|e| Error::Internal(format!("Failed to query version history: {e}")))?
                .into_rows_result()
                .map_err(|e| Error::Internal(format!("Failed to decode version history: {e}")))?
        } else {
            let order = match request.direction {
                VersionStoreDirection::NewestFirst => "DESC",
                VersionStoreDirection::OldestFirst => "ASC",
            };
            let sql = format!(
                "SELECT payload_bytes FROM {} WHERE app_id = ? AND channel = ? AND message_serial = ? ORDER BY version_serial {} LIMIT ?",
                self.tables.version_entries_by_message_fq(),
                order
            );
            self.session
                .query_unpaged(
                    sql.as_str(),
                    (
                        &request.app_id,
                        &request.channel,
                        request.message_serial.as_str(),
                        fetch_limit,
                    ),
                )
                .await
                .map_err(|e| Error::Internal(format!("Failed to query version history: {e}")))?
                .into_rows_result()
                .map_err(|e| Error::Internal(format!("Failed to decode version history: {e}")))?
        };

        let raw: Vec<Vec<u8>> = rows
            .rows::<(Vec<u8>,)>()
            .map_err(|e| Error::Internal(format!("Failed to stream version rows: {e}")))?
            .map(|r| r.map(|row| row.0))
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| Error::Internal(format!("Failed to collect version rows: {e}")))?;

        let has_more = raw.len() > request.limit;
        let items: Vec<StoredVersionRecord> = raw
            .into_iter()
            .take(request.limit)
            .map(|bytes| {
                sonic_rs::from_slice(&bytes)
                    .map_err(|e| Error::Internal(format!("Failed to deserialize version: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;

        let next_cursor = if has_more {
            items.last().map(|item| VersionStoreCursor {
                version: 1,
                version_serial: item.version_serial().clone(),
                direction: request.direction,
            })
        } else {
            None
        };

        Ok(VersionStorePage {
            items,
            next_cursor,
            has_more,
        })
    }

    async fn replay_after(
        &self,
        request: VersionReplayRequest,
    ) -> Result<Vec<StoredVersionRecord>> {
        request.validate()?;
        let sql = format!(
            "SELECT payload_bytes FROM {} WHERE app_id = ? AND channel = ? AND delivery_serial > ? LIMIT ?",
            self.tables.version_entries_by_delivery_fq()
        );
        let rows = self
            .session
            .query_unpaged(
                sql.as_str(),
                (
                    &request.app_id,
                    &request.channel,
                    request.after_delivery_serial as i64,
                    request.limit as i32,
                ),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to replay version entries: {e}")))?
            .into_rows_result()
            .map_err(|e| Error::Internal(format!("Failed to decode replay rows: {e}")))?;

        rows.rows::<(Vec<u8>,)>()
            .map_err(|e| Error::Internal(format!("Failed to stream replay rows: {e}")))?
            .map(|r| {
                r.map_err(|e| Error::Internal(format!("Failed to collect replay row: {e}")))
                    .and_then(|(bytes,)| {
                        sonic_rs::from_slice(&bytes).map_err(|e| {
                            Error::Internal(format!("Failed to deserialize replay record: {e}"))
                        })
                    })
            })
            .collect()
    }

    async fn latest_by_history(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Vec<StoredVersionRecord>> {
        // Read version_messages ordered by history_serial, then fetch each entry individually.
        let msg_q = format!(
            "SELECT message_serial, latest_version_serial, history_serial FROM {} WHERE app_id = ? AND channel = ?",
            self.tables.version_messages_fq()
        );
        let msg_rows = self
            .session
            .query_unpaged(msg_q.as_str(), (app_id, channel))
            .await
            .map_err(|e| Error::Internal(format!("Failed to query version messages: {e}")))?
            .into_rows_result()
            .map_err(|e| Error::Internal(format!("Failed to decode version messages: {e}")))?;

        let mut msgs: Vec<(String, String, i64)> = msg_rows
            .rows::<(String, String, i64)>()
            .map_err(|e| Error::Internal(format!("Failed to stream version message rows: {e}")))?
            .map(|r| r.map_err(|e| Error::Internal(format!("Failed to collect msg row: {e}"))))
            .collect::<Result<Vec<_>>>()?;

        // Sort by history_serial ascending.
        msgs.sort_by_key(|(_, _, hs)| *hs);

        let entry_q = format!(
            "SELECT payload_bytes FROM {} WHERE app_id = ? AND channel = ? AND message_serial = ? AND version_serial = ?",
            self.tables.version_entries_by_message_fq()
        );
        let mut result = Vec::with_capacity(msgs.len());
        for (message_serial, latest_version_serial, _history_serial) in msgs {
            let rows = self
                .session
                .query_unpaged(
                    entry_q.as_str(),
                    (
                        app_id,
                        channel,
                        message_serial.as_str(),
                        latest_version_serial.as_str(),
                    ),
                )
                .await
                .map_err(|e| Error::Internal(format!("Failed to fetch version entry: {e}")))?
                .into_rows_result()
                .map_err(|e| Error::Internal(format!("Failed to decode version entry: {e}")))?;

            if let Some((bytes,)) = rows
                .maybe_first_row::<(Vec<u8>,)>()
                .map_err(|e| Error::Internal(format!("Failed to deserialize entry: {e}")))?
            {
                let record: StoredVersionRecord = sonic_rs::from_slice(&bytes).map_err(|e| {
                    Error::Internal(format!("Failed to deserialize version record: {e}"))
                })?;
                result.push(record);
            }
        }
        Ok(result)
    }

    async fn stream_state(&self, app_id: &str, channel: &str) -> Result<VersionStreamState> {
        let sql = format!(
            "SELECT next_delivery_serial, oldest_available_delivery_serial, newest_available_delivery_serial FROM {} WHERE app_id = ? AND channel = ?",
            self.tables.version_streams_fq()
        );
        let rows = self
            .session
            .query_unpaged(sql.as_str(), (app_id, channel))
            .await
            .map_err(|e| Error::Internal(format!("Failed to read version stream state: {e}")))?
            .into_rows_result()
            .map_err(|e| Error::Internal(format!("Failed to decode version stream state: {e}")))?;

        let Some((next_serial, oldest, newest)) = rows
            .maybe_first_row::<(Option<i64>, Option<i64>, Option<i64>)>()
            .map_err(|e| {
                Error::Internal(format!("Failed to deserialize version stream state: {e}"))
            })?
        else {
            return Ok(VersionStreamState::default());
        };

        Ok(VersionStreamState {
            stream_id: Some(format!("{}/{}", app_id, channel)),
            next_delivery_serial: next_serial.map(|v| v as u64),
            oldest_available_delivery_serial: oldest.map(|v| v as u64),
            newest_available_delivery_serial: newest.map(|v| v as u64),
        })
    }
}
