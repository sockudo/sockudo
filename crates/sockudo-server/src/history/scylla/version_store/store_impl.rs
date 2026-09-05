use super::*;

#[cfg(feature = "versioned-messages")]
#[async_trait::async_trait]
impl VersionStore for ScyllaVersionStore {
    async fn ensure_stream_id(&self, app_id: &str, channel: &str) -> Result<String> {
        Ok(format!("{app_id}/{channel}"))
    }

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
        let record = self.import_atomic_version(&record).await?.unwrap_or(record);
        let payload = sonic_rs::to_vec(&record)?;
        self.append_projection(&record, &payload).await
    }

    async fn commit_create(&self, request: VersionCreateRequest) -> Result<VersionCreateResult> {
        use scylla::statement::batch::{Batch, BatchType};

        if let Some(limit) = request.limits.max_accumulated_message_bytes
            && request.record.data_bytes()? > limit
        {
            return Ok(VersionCreateResult::Rejected(
                VersionCreateRejection::AccumulatedMessageBytes { limit },
            ));
        }
        let commits = self.tables.version_commits_fq();
        let initialize = format!(
            "INSERT INTO {commits} (app_id, channel, commit_key, next_delivery_serial, open_stream_count, created_at_ms) VALUES (?, ?, 's', 1, 0, ?) IF NOT EXISTS"
        );
        let mut initialize_statement = Statement::new(initialize);
        initialize_statement.set_serial_consistency(Some(SerialConsistency::LocalSerial));
        let _ = self
            .session
            .query_unpaged(
                initialize_statement,
                (
                    &request.record.app_id,
                    &request.record.channel,
                    sockudo_core::history::now_ms(),
                ),
            )
            .await
            .map_err(|e| map_scylla_lwt_error("initialize atomic version stream", e))?;
        let stream_q = format!(
            "SELECT next_delivery_serial, open_stream_count FROM {commits} WHERE app_id = ? AND channel = ? AND commit_key = 's'"
        );
        let stream_rows = self
            .session
            .query_unpaged(stream_q, (&request.record.app_id, &request.record.channel))
            .await
            .map_err(|e| Error::Internal(format!("Failed to read atomic version stream: {e}")))?
            .into_rows_result()
            .map_err(|e| Error::Internal(format!("Failed to decode atomic version stream: {e}")))?;
        let (next_delivery, open_count) = stream_rows
            .single_row::<(i64, i64)>()
            .map_err(|e| Error::Internal(format!("Failed to deserialize version stream: {e}")))?;
        if request.record.is_open_ai_stream()
            && let Some(limit) = request.limits.max_open_streaming_messages_per_channel
            && open_count as usize >= limit
        {
            return Ok(VersionCreateResult::Rejected(
                VersionCreateRejection::OpenStreamingMessages { limit },
            ));
        }
        let message_key = message_commit_key(request.record.message_serial().as_str());
        let existing_q = format!(
            "SELECT payload_bytes FROM {commits} WHERE app_id = ? AND channel = ? AND commit_key = ?"
        );
        let existing = self
            .session
            .query_unpaged(
                existing_q,
                (
                    &request.record.app_id,
                    &request.record.channel,
                    &message_key,
                ),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to check atomic create target: {e}")))?
            .into_rows_result()
            .map_err(|e| Error::Internal(format!("Failed to decode atomic create target: {e}")))?;
        if let Some((payload,)) = existing
            .maybe_first_row::<(Vec<u8>,)>()
            .map_err(|e| Error::Internal(format!("Failed to deserialize create target: {e}")))?
        {
            let current = self.decode_records(vec![payload]).await?.remove(0);
            return Ok(VersionCreateResult::Conflict {
                current: Some(current),
            });
        }
        let stream_id = format!("{}/{}", request.record.app_id, request.record.channel);
        let record = request
            .record
            .with_delivery_position(&stream_id, next_delivery as u64);
        let payload = sonic_rs::to_vec(&record)
            .map_err(|e| Error::Internal(format!("Failed to serialize create record: {e}")))?;
        let version_key = version_commit_key(
            record.message_serial().as_str(),
            record.version_serial().as_str(),
        );
        let delivery_key = delivery_commit_key(record.delivery_serial());
        let next_open = open_count + i64::from(record.is_open_ai_stream());
        let now_ms = sockudo_core::history::now_ms();
        let mut batch = Batch::new(BatchType::Logged);
        batch.set_serial_consistency(Some(SerialConsistency::LocalSerial));
        batch.append_statement(Statement::new(format!(
            "UPDATE {commits} SET next_delivery_serial = ?, open_stream_count = ? WHERE app_id = ? AND channel = ? AND commit_key = 's' IF next_delivery_serial = ? AND open_stream_count = ?"
        )));
        batch.append_statement(Statement::new(format!(
            "INSERT INTO {commits} (app_id, channel, commit_key, payload_bytes, latest_version_serial, latest_delivery_serial, history_serial, action, append_count, is_open_stream, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?) IF NOT EXISTS"
        )));
        batch.append_statement(Statement::new(format!(
            "INSERT INTO {commits} (app_id, channel, commit_key, payload_bytes, latest_version_serial, latest_delivery_serial, history_serial, action, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS"
        )));
        batch.append_statement(Statement::new(format!(
            "INSERT INTO {commits} (app_id, channel, commit_key, payload_bytes, latest_version_serial, latest_delivery_serial, history_serial, action, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS"
        )));
        let result = self
            .session
            .batch(
                &batch,
                (
                    (
                        next_delivery + 1,
                        next_open,
                        &record.app_id,
                        &record.channel,
                        next_delivery,
                        open_count,
                    ),
                    (
                        &record.app_id,
                        &record.channel,
                        &message_key,
                        payload.as_slice(),
                        record.version_serial().as_str(),
                        record.delivery_serial() as i64,
                        record.history_serial() as i64,
                        record.message.action.as_str(),
                        record.is_open_ai_stream(),
                        now_ms,
                    ),
                    (
                        &record.app_id,
                        &record.channel,
                        &version_key,
                        payload.as_slice(),
                        record.version_serial().as_str(),
                        record.delivery_serial() as i64,
                        record.history_serial() as i64,
                        record.message.action.as_str(),
                        now_ms,
                    ),
                    (
                        &record.app_id,
                        &record.channel,
                        &delivery_key,
                        payload.as_slice(),
                        record.version_serial().as_str(),
                        record.delivery_serial() as i64,
                        record.history_serial() as i64,
                        record.message.action.as_str(),
                        now_ms,
                    ),
                ),
            )
            .await
            .map_err(|e| map_scylla_lwt_error("commit atomic version create", e))?;
        if !version_batch_applied(result)? {
            return Ok(VersionCreateResult::Conflict {
                current: self
                    .get_latest(&record.app_id, &record.channel, record.message_serial())
                    .await?,
            });
        }
        if let Err(error) = self.append_version(record.clone()).await {
            tracing::warn!(error = %error, "failed to refresh ScyllaDB version create projections");
        }
        Ok(VersionCreateResult::Applied { record, stream_id })
    }

    async fn compare_and_apply(
        &self,
        request: VersionMutationRequest,
    ) -> Result<VersionMutationResult> {
        use scylla::statement::batch::{Batch, BatchType};

        let commits = self.tables.version_commits_fq();
        if let Some(operation) = request.idempotency.as_ref() {
            let operation_key = operation_commit_key(&operation.cache_key);
            let receipt_q = format!(
                "SELECT payload_bytes, latest_version_serial FROM {commits} WHERE app_id = ? AND channel = ? AND commit_key = ?"
            );
            let receipt = self
                .session
                .query_unpaged(
                    receipt_q,
                    (&request.app_id, &request.channel, &operation_key),
                )
                .await
                .map_err(|e| Error::Internal(format!("Failed to read mutation receipt: {e}")))?
                .into_rows_result()
                .map_err(|e| Error::Internal(format!("Failed to decode mutation receipt: {e}")))?;
            if let Some((payload, fingerprint)) = receipt
                .maybe_first_row::<(Vec<u8>, String)>()
                .map_err(|e| Error::Internal(format!("Failed to deserialize receipt: {e}")))?
            {
                if fingerprint != operation.payload_fingerprint {
                    return Err(Error::IdempotencyConflict);
                }
                let record = self.decode_records(vec![payload]).await?.remove(0);
                return Ok(VersionMutationResult::Duplicate {
                    record,
                    stream_id: format!("{}/{}", request.app_id, request.channel),
                });
            }
        }
        let stream_q = format!(
            "SELECT next_delivery_serial, open_stream_count FROM {commits} WHERE app_id = ? AND channel = ? AND commit_key = 's'"
        );
        let stream = self
            .session
            .query_unpaged(stream_q, (&request.app_id, &request.channel))
            .await
            .map_err(|e| Error::Internal(format!("Failed to read atomic version stream: {e}")))?
            .into_rows_result()
            .map_err(|e| Error::Internal(format!("Failed to decode atomic version stream: {e}")))?;
        let Some((next_delivery, open_count)) = stream
            .maybe_first_row::<(i64, i64)>()
            .map_err(|e| Error::Internal(format!("Failed to deserialize version stream: {e}")))?
        else {
            return Ok(VersionMutationResult::Conflict { current: None });
        };
        let message_key = message_commit_key(request.message_serial.as_str());
        let message_q = format!(
            "SELECT payload_bytes, latest_version_serial, latest_delivery_serial, append_count, is_open_stream FROM {commits} WHERE app_id = ? AND channel = ? AND commit_key = ?"
        );
        let message = self
            .session
            .query_unpaged(message_q, (&request.app_id, &request.channel, &message_key))
            .await
            .map_err(|e| Error::Internal(format!("Failed to read mutation predecessor: {e}")))?
            .into_rows_result()
            .map_err(|e| Error::Internal(format!("Failed to decode mutation predecessor: {e}")))?;
        let Some((current_payload, expected_version, expected_delivery, append_count, was_open)) =
            message
                .maybe_first_row::<(Vec<u8>, String, i64, i64, bool)>()
                .map_err(|e| {
                    Error::Internal(format!("Failed to deserialize mutation predecessor: {e}"))
                })?
        else {
            return Ok(VersionMutationResult::Conflict { current: None });
        };
        let current_encoding =
            sockudo_core::version_store::EncodedVersionRecord::decode(&current_payload)?;
        let current = self.decode_records(vec![current_payload]).await?.remove(0);
        let delivery_serial = (next_delivery as u64).max(current.delivery_serial() + 1);
        let stream_id = format!("{}/{}", request.app_id, request.channel);
        let outcome =
            request.apply_to(&current, &stream_id, delivery_serial, append_count as usize)?;
        let VersionMutationResult::Applied { record, .. } = outcome else {
            return Ok(outcome);
        };
        let opens = !was_open && record.is_open_ai_stream();
        let closes = was_open && !record.is_open_ai_stream();
        if opens
            && let Some(limit) = request.limits.max_open_streaming_messages_per_channel
            && open_count as usize >= limit
        {
            return Ok(VersionMutationResult::Rejected(
                VersionMutationRejection::OpenStreamingMessages { limit },
            ));
        }
        let next_open = open_count + i64::from(opens) - i64::from(closes);
        let next_append = append_count
            + i64::from(matches!(
                request.mutation,
                sockudo_core::version_store::VersionMutation::Append(_)
            ));
        let plan = sockudo_core::version_store::EncodedVersionRecord::plan(
            &record,
            Some((&current, current_encoding.text.as_ref())),
        )?;
        let payload = &plan.entry_bytes;
        let version_key = version_commit_key(
            record.message_serial().as_str(),
            record.version_serial().as_str(),
        );
        let delivery_key = delivery_commit_key(delivery_serial);
        let now_ms = sockudo_core::history::now_ms();
        let mut batch = Batch::new(BatchType::Logged);
        batch.set_serial_consistency(Some(SerialConsistency::LocalSerial));
        batch.append_statement(Statement::new(format!(
            "UPDATE {commits} SET next_delivery_serial = ?, open_stream_count = ? WHERE app_id = ? AND channel = ? AND commit_key = 's' IF next_delivery_serial = ? AND open_stream_count = ?"
        )));
        batch.append_statement(Statement::new(format!(
            "UPDATE {commits} SET payload_bytes = ?, latest_version_serial = ?, latest_delivery_serial = ?, action = ?, append_count = ?, is_open_stream = ?, created_at_ms = ? WHERE app_id = ? AND channel = ? AND commit_key = ? IF latest_version_serial = ? AND latest_delivery_serial = ? AND append_count = ?"
        )));
        batch.append_statement(Statement::new(format!(
            "INSERT INTO {commits} (app_id, channel, commit_key, payload_bytes, latest_version_serial, latest_delivery_serial, history_serial, action, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS"
        )));
        batch.append_statement(Statement::new(format!(
            "INSERT INTO {commits} (app_id, channel, commit_key, payload_bytes, latest_version_serial, latest_delivery_serial, history_serial, action, created_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS"
        )));
        if plan.snapshot.is_some() {
            batch.append_statement(Statement::new(format!(
                "INSERT INTO {commits} (app_id, channel, commit_key, payload_bytes, created_at_ms) VALUES (?, ?, ?, ?, ?)"
            )));
        }
        if request.idempotency.is_some() {
            batch.append_statement(Statement::new(format!(
                "INSERT INTO {commits} (app_id, channel, commit_key, payload_bytes, latest_version_serial, created_at_ms) VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS"
            )));
        }
        let base_values = (
            (
                delivery_serial as i64 + 1,
                next_open,
                &record.app_id,
                &record.channel,
                next_delivery,
                open_count,
            ),
            (
                plan.latest_bytes.as_slice(),
                record.version_serial().as_str(),
                delivery_serial as i64,
                record.message.action.as_str(),
                next_append,
                record.is_open_ai_stream(),
                now_ms,
                &record.app_id,
                &record.channel,
                &message_key,
                expected_version.as_str(),
                expected_delivery,
                append_count,
            ),
            (
                &record.app_id,
                &record.channel,
                &version_key,
                payload.as_slice(),
                record.version_serial().as_str(),
                delivery_serial as i64,
                record.history_serial() as i64,
                record.message.action.as_str(),
                now_ms,
            ),
            (
                &record.app_id,
                &record.channel,
                &delivery_key,
                payload.as_slice(),
                record.version_serial().as_str(),
                delivery_serial as i64,
                record.history_serial() as i64,
                record.message.action.as_str(),
                now_ms,
            ),
        );
        let snapshot_key = plan
            .snapshot
            .as_ref()
            .map(|(reference, _)| Self::text_key(&reference.snapshot_key));
        let snapshot_values =
            plan.snapshot
                .as_ref()
                .zip(snapshot_key.as_ref())
                .map(|((_, text), key)| {
                    (
                        &record.app_id,
                        &record.channel,
                        key,
                        text.as_bytes(),
                        now_ms,
                    )
                });
        let operation_key = request
            .idempotency
            .as_ref()
            .map(|operation| operation_commit_key(&operation.cache_key));
        let operation_values = request
            .idempotency
            .as_ref()
            .zip(operation_key.as_ref())
            .map(|(operation, key)| {
                (
                    &record.app_id,
                    &record.channel,
                    key,
                    payload.as_slice(),
                    &operation.payload_fingerprint,
                    now_ms,
                )
            });
        let result = match (snapshot_values, operation_values) {
            (Some(snapshot), Some(operation)) => {
                self.session
                    .batch(
                        &batch,
                        (
                            base_values.0,
                            base_values.1,
                            base_values.2,
                            base_values.3,
                            snapshot,
                            operation,
                        ),
                    )
                    .await
            }
            (Some(snapshot), None) => {
                self.session
                    .batch(
                        &batch,
                        (
                            base_values.0,
                            base_values.1,
                            base_values.2,
                            base_values.3,
                            snapshot,
                        ),
                    )
                    .await
            }
            (None, Some(operation)) => {
                self.session
                    .batch(
                        &batch,
                        (
                            base_values.0,
                            base_values.1,
                            base_values.2,
                            base_values.3,
                            operation,
                        ),
                    )
                    .await
            }
            (None, None) => self.session.batch(&batch, base_values).await,
        }
        .map_err(|e| map_scylla_lwt_error("commit atomic version mutation", e))?;
        if !version_batch_applied(result)? {
            return Ok(VersionMutationResult::Conflict {
                current: self
                    .get_latest(&request.app_id, &request.channel, &request.message_serial)
                    .await?,
            });
        }
        if let Err(error) = self.append_projection(&record, payload).await {
            tracing::warn!(error = %error, "failed to refresh ScyllaDB version mutation projections");
        }
        Ok(VersionMutationResult::Applied { record, stream_id })
    }

    async fn get_latest(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &sockudo_core::versioned_messages::MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        let commit_q = format!(
            "SELECT payload_bytes FROM {} WHERE app_id = ? AND channel = ? AND commit_key = ?",
            self.tables.version_commits_fq()
        );
        let commit_rows = self
            .session
            .query_unpaged(
                commit_q,
                (app_id, channel, message_commit_key(message_serial.as_str())),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to query atomic latest version: {e}")))?
            .into_rows_result()
            .map_err(|e| Error::Internal(format!("Failed to decode atomic latest version: {e}")))?;
        if let Some((payload,)) = commit_rows
            .maybe_first_row::<(Vec<u8>,)>()
            .map_err(|e| Error::Internal(format!("Failed to deserialize atomic latest: {e}")))?
        {
            let record = self.decode_records(vec![payload]).await?.remove(0);
            return Ok(Some(record));
        }
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

        let record = self.decode_records(vec![row.0]).await?.remove(0);
        Ok(Some(record))
    }

    async fn get_latest_batch(
        &self,
        app_id: &str,
        channel: &str,
        message_serials: &[sockudo_core::versioned_messages::MessageSerial],
    ) -> Result<
        std::collections::BTreeMap<
            sockudo_core::versioned_messages::MessageSerial,
            StoredVersionRecord,
        >,
    > {
        let requested: Vec<_> = message_serials
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut result = std::collections::BTreeMap::new();
        for chunk in requested.chunks(100) {
            let keys: Vec<_> = chunk
                .iter()
                .map(|serial| message_commit_key(serial.as_str()))
                .collect();
            let sql = format!(
                "SELECT payload_bytes FROM {} WHERE app_id = ? AND channel = ? AND commit_key IN ?",
                self.tables.version_commits_fq()
            );
            let rows = self
                .session
                .query_unpaged(sql, (app_id, channel, keys))
                .await
                .map_err(|e| Error::Internal(format!("failed to read atomic latest batch: {e}")))?
                .into_rows_result()
                .map_err(|e| {
                    Error::Internal(format!("failed to decode atomic latest batch: {e}"))
                })?;
            for row in rows
                .rows::<(Vec<u8>,)>()
                .map_err(|e| Error::Internal(format!("failed to decode latest batch rows: {e}")))?
            {
                let (payload,) = row.map_err(|e| {
                    Error::Internal(format!("failed to decode latest batch row: {e}"))
                })?;
                let record = self.decode_records(vec![payload]).await?.remove(0);
                result.insert(record.message_serial().clone(), record);
            }
            // Legacy partitions remain readable during mixed-data migration.
            for serial in chunk {
                if !result.contains_key(*serial)
                    && let Some(record) = self.get_latest(app_id, channel, serial).await?
                {
                    result.insert(record.message_serial().clone(), record);
                }
            }
        }
        Ok(result)
    }

    async fn get_versions(&self, request: VersionStoreReadRequest) -> Result<VersionStorePage> {
        request.validate()?;
        let prefix = format!("v:{}:", request.message_serial.as_str());
        let upper = format!("{prefix}\u{10ffff}");
        let order = match request.direction {
            VersionStoreDirection::NewestFirst => "DESC",
            VersionStoreDirection::OldestFirst => "ASC",
        };
        let (comparison, cursor_key) = if let Some(cursor) = request.cursor.as_ref() {
            (
                match request.direction {
                    VersionStoreDirection::NewestFirst => "<",
                    VersionStoreDirection::OldestFirst => ">",
                },
                version_commit_key(
                    request.message_serial.as_str(),
                    cursor.version_serial.as_str(),
                ),
            )
        } else {
            (">=", prefix.clone())
        };
        let commit_sql = format!(
            "SELECT payload_bytes FROM {} WHERE app_id = ? AND channel = ? AND commit_key {comparison} ? AND commit_key < ? ORDER BY commit_key {order} LIMIT ?",
            self.tables.version_commits_fq()
        );
        let commit_rows = self
            .session
            .query_unpaged(
                commit_sql,
                (
                    &request.app_id,
                    &request.channel,
                    cursor_key,
                    upper,
                    (request.limit + 1) as i32,
                ),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to query atomic versions: {e}")))?
            .into_rows_result()
            .map_err(|e| Error::Internal(format!("Failed to decode atomic versions: {e}")))?;
        let atomic_payloads = commit_rows
            .rows::<(Vec<u8>,)>()
            .map_err(|e| Error::Internal(format!("Failed to stream atomic versions: {e}")))?
            .map(|row| row.map(|value| value.0))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::Internal(format!("Failed to collect atomic versions: {e}")))?;
        if !atomic_payloads.is_empty() {
            let has_more = atomic_payloads.len() > request.limit;
            let items = self
                .decode_records(atomic_payloads.into_iter().take(request.limit).collect())
                .await?;
            let next_cursor = if has_more {
                items.last().map(|item| VersionStoreCursor {
                    version: 1,
                    version_serial: item.version_serial().clone(),
                    direction: request.direction,
                })
            } else {
                None
            };
            return Ok(VersionStorePage {
                items,
                next_cursor,
                has_more,
            });
        }
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
        let items = self
            .decode_records(raw.into_iter().take(request.limit).collect())
            .await?;

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
        let after_key = delivery_commit_key(request.after_delivery_serial);
        let commit_sql = format!(
            "SELECT payload_bytes FROM {} WHERE app_id = ? AND channel = ? AND commit_key > ? AND commit_key < 'e:' ORDER BY commit_key ASC LIMIT ?",
            self.tables.version_commits_fq()
        );
        let commit_rows = self
            .session
            .query_unpaged(
                commit_sql,
                (
                    &request.app_id,
                    &request.channel,
                    after_key,
                    request.limit as i32,
                ),
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to replay atomic versions: {e}")))?
            .into_rows_result()
            .map_err(|e| Error::Internal(format!("Failed to decode atomic replay: {e}")))?;
        let atomic = commit_rows
            .rows::<(Vec<u8>,)>()
            .map_err(|e| Error::Internal(format!("failed to decode replay rows: {e}")))?
            .map(|row| row.map(|(payload,)| payload))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::Internal(format!("failed to collect replay rows: {e}")))?;
        if !atomic.is_empty() {
            return self.decode_records(atomic).await;
        }
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

        let payloads = rows
            .rows::<(Vec<u8>,)>()
            .map_err(|e| Error::Internal(format!("failed to decode replay rows: {e}")))?
            .map(|row| row.map(|(payload,)| payload))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::Internal(format!("failed to collect replay rows: {e}")))?;
        self.decode_records(payloads).await
    }

    async fn latest_by_history(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Vec<StoredVersionRecord>> {
        let commit_q = format!(
            "SELECT payload_bytes, history_serial FROM {} WHERE app_id = ? AND channel = ? AND commit_key >= 'm:' AND commit_key < 'n:'",
            self.tables.version_commits_fq()
        );
        let commit_rows = self
            .session
            .query_unpaged(commit_q, (app_id, channel))
            .await
            .map_err(|e| Error::Internal(format!("Failed to query atomic messages: {e}")))?
            .into_rows_result()
            .map_err(|e| Error::Internal(format!("Failed to decode atomic messages: {e}")))?;
        let mut atomic = commit_rows
            .rows::<(Vec<u8>, i64)>()
            .map_err(|e| Error::Internal(format!("failed to decode latest rows: {e}")))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::Internal(format!("failed to collect latest rows: {e}")))?;
        if !atomic.is_empty() {
            atomic.sort_by_key(|value| value.1);
            return self
                .decode_records(atomic.into_iter().map(|value| value.0).collect())
                .await;
        }
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
                let record = self.decode_records(vec![bytes]).await?.remove(0);
                result.push(record);
            }
        }
        Ok(result)
    }

    async fn message_count(&self, app_id: &str, channel: &str) -> Result<u64> {
        Ok(self.metadata_counts(app_id, channel, false).await?.0)
    }

    async fn active_stream_count(&self, app_id: &str, channel: &str) -> Result<usize> {
        Ok(self.metadata_counts(app_id, channel, true).await?.1)
    }

    async fn stream_state(&self, app_id: &str, channel: &str) -> Result<VersionStreamState> {
        let commit_sql = format!(
            "SELECT next_delivery_serial FROM {} WHERE app_id = ? AND channel = ? AND commit_key = 's'",
            self.tables.version_commits_fq()
        );
        let commit_rows = self
            .session
            .query_unpaged(commit_sql, (app_id, channel))
            .await
            .map_err(|e| Error::Internal(format!("Failed to read atomic stream state: {e}")))?
            .into_rows_result()
            .map_err(|e| Error::Internal(format!("Failed to decode atomic stream state: {e}")))?;
        if let Some((next_serial,)) = commit_rows
            .maybe_first_row::<(i64,)>()
            .map_err(|e| Error::Internal(format!("Failed to deserialize atomic stream: {e}")))?
        {
            let bounds_sql = format!(
                "SELECT latest_delivery_serial FROM {} WHERE app_id = ? AND channel = ? AND commit_key >= 'd:' AND commit_key < 'e:'",
                self.tables.version_commits_fq()
            );
            let bounds = self
                .session
                .query_unpaged(bounds_sql, (app_id, channel))
                .await
                .map_err(|e| Error::Internal(format!("Failed to read delivery bounds: {e}")))?
                .into_rows_result()
                .map_err(|e| Error::Internal(format!("Failed to decode delivery bounds: {e}")))?;
            let serials = bounds
                .rows::<(i64,)>()
                .map_err(|e| Error::Internal(format!("Failed to stream delivery bounds: {e}")))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| Error::Internal(format!("Failed to collect delivery bounds: {e}")))?;
            return Ok(VersionStreamState {
                stream_id: Some(format!("{app_id}/{channel}")),
                next_delivery_serial: Some(next_serial as u64),
                oldest_available_delivery_serial: serials.first().map(|value| value.0 as u64),
                newest_available_delivery_serial: serials.last().map(|value| value.0 as u64),
            });
        }
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
