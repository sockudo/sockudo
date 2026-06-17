use super::*;

#[cfg(feature = "versioned-messages")]
#[async_trait::async_trait]
impl VersionStore for SurrealVersionStore {
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
        let record_id = deterministic_key([app_id, channel].into_iter());
        let stream_id = format!("{app_id}/{channel}");
        loop {
            let existing: Option<StoredVersionStreamRec> = self
                .db
                .select((self.tables.streams.clone(), record_id.clone()))
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to fetch SurrealDB version stream: {e}"))
                })?;

            if let Some(existing) = existing {
                let now_ms = sockudo_core::history::now_ms();
                let mut response = self
                    .db
                    .query("UPDATE ONLY type::record($table, $id) SET next_delivery_serial = $next, updated_at_ms = $now WHERE next_delivery_serial = $expected RETURN AFTER")
                    .bind(("table", self.tables.streams.clone()))
                    .bind(("id", record_id.clone()))
                    .bind(("next", existing.next_delivery_serial + block_size_i64))
                    .bind(("now", now_ms))
                    .bind(("expected", existing.next_delivery_serial))
                    .await
                    .map_err(|e| Error::Internal(format!("Failed to advance SurrealDB version delivery serial: {e}")))?;
                let updated: Option<StoredVersionStreamRec> =
                    response.take(0usize).map_err(|e| {
                        Error::Internal(format!(
                            "Failed to decode SurrealDB version serial advancement: {e}"
                        ))
                    })?;
                if updated.is_some() {
                    return Ok(VersionWriteReservationBlock {
                        stream_id: existing.stream_id,
                        start_delivery_serial: existing.next_delivery_serial as u64,
                        len: block_size,
                    });
                }
                continue;
            }

            let now_ms = sockudo_core::history::now_ms();
            let stream = StoredVersionStreamRec {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                stream_id: stream_id.clone(),
                next_delivery_serial: block_size_i64.saturating_add(1),
                oldest_delivery_serial: None,
                newest_delivery_serial: None,
                updated_at_ms: now_ms,
            };
            let create_result: Result<Option<StoredVersionStreamRec>> = self
                .db
                .create((self.tables.streams.clone(), record_id.clone()))
                .content(stream.clone())
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to create SurrealDB version stream: {e}"))
                });
            match create_result {
                Ok(Some(_)) | Ok(None) => {
                    return Ok(VersionWriteReservationBlock {
                        stream_id,
                        start_delivery_serial: 1,
                        len: block_size,
                    });
                }
                Err(err) => {
                    let err_text = err.to_string();
                    if err_text.contains("already exists")
                        || err_text.contains("already been created")
                        || err_text.contains("Database record")
                    {
                        continue;
                    }
                    return Err(err);
                }
            }
        }
    }

    async fn append_version(&self, record: StoredVersionRecord) -> Result<()> {
        let app_id = record.app_id.as_str();
        let channel = record.channel.as_str();
        let msg_serial = record.message_serial().as_str();
        let ver_serial = record.version_serial().as_str();
        let delivery_serial = record.delivery_serial();
        let history_serial = record.history_serial();
        let now_ms = sockudo_core::history::now_ms();

        let payload_bytes = sonic_rs::to_vec(&record)
            .map_err(|e| Error::Internal(format!("Failed to serialize version record: {e}")))?;

        let entry_id = deterministic_key([app_id, channel, msg_serial, ver_serial].into_iter());
        let entry = StoredVersionEntryRec {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            message_serial: msg_serial.to_string(),
            version_serial: ver_serial.to_string(),
            delivery_serial: delivery_serial as i64,
            payload_bytes,
            created_at_ms: now_ms,
        };

        // Write entry idempotently (IF NOT EXISTS via create, ignore "already exists")
        let create_entry: std::result::Result<Option<StoredVersionEntryRec>, _> = self
            .db
            .create((self.tables.entries.clone(), entry_id.clone()))
            .content(entry)
            .await;
        if let Err(e) = create_entry {
            let err_text = e.to_string();
            if !err_text.contains("already exists")
                && !err_text.contains("already been created")
                && !err_text.contains("Database record")
            {
                return Err(Error::Internal(format!(
                    "Failed to write SurrealDB version entry: {e}"
                )));
            }
        }

        // Upsert version_messages — advance latest_version_serial pointer only when newer
        let msg_id = deterministic_key([app_id, channel, msg_serial].into_iter());
        let existing_msg: Option<StoredVersionMessageRec> = self
            .db
            .select((self.tables.messages.clone(), msg_id.clone()))
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to fetch SurrealDB version message record: {e}"
                ))
            })?;

        let advance_msg = |db: &Surreal<Any>,
                           tables: &VersionStoreTables,
                           msg_id: String,
                           entry_id: String,
                           ver_serial: String,
                           now_ms: i64| {
            let db = db.clone();
            let tables = tables.clone();
            async move {
                let mut response = db
                    .query("UPDATE ONLY type::record($table, $id) SET latest_version_serial = $ver, latest_entry_key = $key, updated_at_ms = $now WHERE latest_version_serial < $ver RETURN AFTER")
                    .bind(("table", tables.messages.clone()))
                    .bind(("id", msg_id))
                    .bind(("ver", ver_serial))
                    .bind(("key", entry_id))
                    .bind(("now", now_ms))
                    .await
                    .map_err(|e| Error::Internal(format!("Failed to advance SurrealDB version message pointer: {e}")))?;
                let _: std::result::Result<Option<StoredVersionMessageRec>, _> =
                    response.take(0usize);
                Ok::<(), Error>(())
            }
        };

        match existing_msg {
            None => {
                let msg_record = StoredVersionMessageRec {
                    app_id: app_id.to_string(),
                    channel: channel.to_string(),
                    message_serial: msg_serial.to_string(),
                    latest_version_serial: ver_serial.to_string(),
                    latest_entry_key: entry_id.clone(),
                    history_serial: history_serial as i64,
                    updated_at_ms: now_ms,
                };
                let create_msg: std::result::Result<Option<StoredVersionMessageRec>, _> = self
                    .db
                    .create((self.tables.messages.clone(), msg_id.clone()))
                    .content(msg_record)
                    .await;
                if let Err(e) = create_msg {
                    let err_text = e.to_string();
                    if !err_text.contains("already exists")
                        && !err_text.contains("already been created")
                        && !err_text.contains("Database record")
                    {
                        return Err(Error::Internal(format!(
                            "Failed to create SurrealDB version message record: {e}"
                        )));
                    }
                    // Race: another writer created it first — still try to advance the pointer
                    advance_msg(
                        &self.db,
                        &self.tables,
                        msg_id,
                        entry_id,
                        ver_serial.to_string(),
                        now_ms,
                    )
                    .await?;
                }
            }
            Some(existing) if ver_serial > existing.latest_version_serial.as_str() => {
                advance_msg(
                    &self.db,
                    &self.tables,
                    msg_id,
                    entry_id,
                    ver_serial.to_string(),
                    now_ms,
                )
                .await?;
            }
            Some(_) => {
                // New version is not newer than the current pointer — nothing to update
            }
        }

        // Update version_streams window: maintain oldest/newest delivery_serial bounds
        let stream_record_id = deterministic_key([app_id, channel].into_iter());
        let _: std::result::Result<_, _> = self
            .db
            .query(
                "UPDATE ONLY type::record($table, $id) SET \
                 oldest_delivery_serial = IF oldest_delivery_serial IS NONE OR $delivery < oldest_delivery_serial THEN $delivery ELSE oldest_delivery_serial END, \
                 newest_delivery_serial = IF newest_delivery_serial IS NONE OR $delivery > newest_delivery_serial THEN $delivery ELSE newest_delivery_serial END, \
                 updated_at_ms = $now",
            )
            .bind(("table", self.tables.streams.clone()))
            .bind(("id", stream_record_id))
            .bind(("delivery", delivery_serial as i64))
            .bind(("now", now_ms))
            .await;

        Ok(())
    }

    async fn get_latest(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        let msg_id = deterministic_key([app_id, channel, message_serial.as_str()].into_iter());
        let msg_record: Option<StoredVersionMessageRec> = self
            .db
            .select((self.tables.messages.clone(), msg_id))
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to fetch SurrealDB version message: {e}"))
            })?;

        let Some(msg) = msg_record else {
            return Ok(None);
        };

        let entry: Option<StoredVersionEntryRec> = self
            .db
            .select((self.tables.entries.clone(), msg.latest_entry_key))
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to fetch SurrealDB version entry: {e}"))
            })?;

        let Some(entry) = entry else {
            return Ok(None);
        };

        let record = sonic_rs::from_slice(&entry.payload_bytes).map_err(|e| {
            Error::Internal(format!(
                "Failed to deserialize SurrealDB version entry: {e}"
            ))
        })?;
        Ok(Some(record))
    }

    async fn get_versions(&self, request: VersionStoreReadRequest) -> Result<VersionStorePage> {
        request.validate()?;

        let order = match request.direction {
            VersionStoreDirection::NewestFirst => "DESC",
            VersionStoreDirection::OldestFirst => "ASC",
        };

        let mut clauses = vec![
            "app_id = $app_id".to_string(),
            "channel = $channel".to_string(),
            "message_serial = $message_serial".to_string(),
        ];
        let mut cursor_serial_bind: Option<String> = None;

        if let Some(cursor) = request.cursor.as_ref() {
            clauses.push(match request.direction {
                VersionStoreDirection::NewestFirst => "version_serial < $cursor_serial".to_string(),
                VersionStoreDirection::OldestFirst => "version_serial > $cursor_serial".to_string(),
            });
            cursor_serial_bind = Some(cursor.version_serial.as_str().to_string());
        }

        let sql = format!(
            "SELECT app_id, channel, message_serial, version_serial, delivery_serial, payload_bytes FROM {} WHERE {} ORDER BY version_serial {} LIMIT {}",
            self.tables.entries,
            clauses.join(" AND "),
            order,
            request.limit + 1
        );

        let mut query = self
            .db
            .query(sql)
            .bind(("app_id", request.app_id.clone()))
            .bind(("channel", request.channel.clone()))
            .bind((
                "message_serial",
                request.message_serial.as_str().to_string(),
            ));

        if let Some(serial) = cursor_serial_bind {
            query = query.bind(("cursor_serial", serial));
        }

        let mut response = query.await.map_err(|e| {
            Error::Internal(format!("Failed to query SurrealDB version history: {e}"))
        })?;
        let rows: Vec<StoredVersionEntryRec> = response.take(0usize).map_err(|e| {
            Error::Internal(format!("Failed to decode SurrealDB version history: {e}"))
        })?;

        let has_more = rows.len() > request.limit;
        let items: Vec<StoredVersionRecord> = rows
            .into_iter()
            .take(request.limit)
            .map(|row| {
                sonic_rs::from_slice(&row.payload_bytes).map_err(|e| {
                    Error::Internal(format!(
                        "Failed to deserialize SurrealDB version history entry: {e}"
                    ))
                })
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

        let mut response = self
            .db
            .query(format!(
                "SELECT payload_bytes FROM {} WHERE app_id = $app_id AND channel = $channel AND delivery_serial > $after ORDER BY delivery_serial ASC LIMIT {}",
                self.tables.entries, request.limit
            ))
            .bind(("app_id", request.app_id.clone()))
            .bind(("channel", request.channel.clone()))
            .bind(("after", request.after_delivery_serial as i64))
            .await
            .map_err(|e| Error::Internal(format!("Failed to query SurrealDB version replay: {e}")))?;

        let rows: Vec<VersionPayloadRow> = response.take(0usize).map_err(|e| {
            Error::Internal(format!(
                "Failed to decode SurrealDB version replay rows: {e}"
            ))
        })?;

        rows.into_iter()
            .map(|row| {
                sonic_rs::from_slice(&row.payload_bytes).map_err(|e| {
                    Error::Internal(format!(
                        "Failed to deserialize SurrealDB version replay entry: {e}"
                    ))
                })
            })
            .collect()
    }

    async fn latest_by_history(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Vec<StoredVersionRecord>> {
        let mut response = self
            .db
            .query(format!(
                "SELECT latest_entry_key FROM {} WHERE app_id = $app_id AND channel = $channel ORDER BY history_serial ASC",
                self.tables.messages
            ))
            .bind(("app_id", app_id.to_string()))
            .bind(("channel", channel.to_string()))
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to query SurrealDB version messages for latest_by_history: {e}"
                ))
            })?;

        let msg_rows: Vec<VersionLatestKeyRow> = response.take(0usize).map_err(|e| {
            Error::Internal(format!(
                "Failed to decode SurrealDB version message rows in latest_by_history: {e}"
            ))
        })?;

        let mut result = Vec::with_capacity(msg_rows.len());
        for row in msg_rows {
            let entry: Option<StoredVersionEntryRec> = self
                .db
                .select((self.tables.entries.clone(), row.latest_entry_key))
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to fetch SurrealDB version entry in latest_by_history: {e}"
                    ))
                })?;
            if let Some(entry) = entry {
                let record: StoredVersionRecord =
                    sonic_rs::from_slice(&entry.payload_bytes).map_err(|e| {
                        Error::Internal(format!(
                            "Failed to deserialize SurrealDB version entry in latest_by_history: {e}"
                        ))
                    })?;
                result.push(record);
            }
        }
        Ok(result)
    }

    async fn stream_state(&self, app_id: &str, channel: &str) -> Result<VersionStreamState> {
        let record_id = deterministic_key([app_id, channel].into_iter());
        let record: Option<StoredVersionStreamRec> = self
            .db
            .select((self.tables.streams.clone(), record_id))
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to fetch SurrealDB version stream state: {e}"
                ))
            })?;

        let Some(stream) = record else {
            return Ok(VersionStreamState::default());
        };

        Ok(VersionStreamState {
            stream_id: Some(stream.stream_id),
            next_delivery_serial: Some(stream.next_delivery_serial as u64),
            oldest_available_delivery_serial: stream.oldest_delivery_serial.map(|v| v as u64),
            newest_available_delivery_serial: stream.newest_delivery_serial.map(|v| v as u64),
        })
    }

    async fn purge_before(&self, before_ms: i64, batch_size: usize) -> Result<(u64, bool)> {
        if batch_size == 0 {
            return Ok((0, false));
        }
        let limit = batch_size as i64;

        // SurrealDB has no LIMIT clause on DELETE, so we select a batch of
        // record ids and delete them. Two round-trips per table but each is
        // bounded by `limit`, which is what the caller asked for.
        let purge_table = |table: &str, ts_field: &'static str| {
            let db = self.db.clone();
            let table = table.to_string();
            async move {
                let select_sql =
                    format!("SELECT VALUE id FROM {table} WHERE {ts_field} < $cutoff LIMIT $limit");
                let mut response = db
                    .query(select_sql)
                    .bind(("cutoff", before_ms))
                    .bind(("limit", limit))
                    .await
                    .map_err(|e| {
                        Error::Internal(format!(
                            "Failed to select expired rows in SurrealDB {table}: {e}"
                        ))
                    })?;
                let ids: Vec<surrealdb::types::RecordId> = response.take(0usize).map_err(|e| {
                    Error::Internal(format!(
                        "Failed to decode expired row ids in SurrealDB {table}: {e}"
                    ))
                })?;
                let len = ids.len() as u64;
                if ids.is_empty() {
                    return Ok::<(u64, bool), Error>((0, false));
                }
                db.query("DELETE $ids")
                    .bind(("ids", ids))
                    .await
                    .map_err(|e| {
                        Error::Internal(format!(
                            "Failed to delete expired rows in SurrealDB {table}: {e}"
                        ))
                    })?;
                Ok((len, len as i64 == limit))
            }
        };

        let (entries_deleted, entries_more) =
            purge_table(&self.tables.entries, "created_at_ms").await?;
        let (messages_deleted, messages_more) =
            purge_table(&self.tables.messages, "updated_at_ms").await?;

        Ok((
            entries_deleted + messages_deleted,
            entries_more || messages_more,
        ))
    }
}
