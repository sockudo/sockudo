use super::*;

#[cfg(feature = "versioned-messages")]
#[async_trait::async_trait]
impl VersionStore for SurrealVersionStore {
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
                open_stream_count: 0,
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
        let app_id = record.app_id.clone();
        let channel = record.channel.clone();
        let entry_id = deterministic_key(
            [
                app_id.as_str(),
                channel.as_str(),
                record.message_serial().as_str(),
                record.version_serial().as_str(),
            ]
            .into_iter(),
        );
        let message_id = deterministic_key(
            [
                app_id.as_str(),
                channel.as_str(),
                record.message_serial().as_str(),
            ]
            .into_iter(),
        );
        let stream_id = deterministic_key([app_id.as_str(), channel.as_str()].into_iter());
        let delivery = i64::try_from(record.delivery_serial())
            .ok()
            .filter(|value| *value < i64::MAX)
            .ok_or_else(|| {
                Error::InvalidMessageFormat("imported delivery serial exceeds storage range".into())
            })?;
        let now = sockudo_core::history::now_ms();
        let payload = sonic_rs::to_vec(&record)?;
        let entry = StoredVersionEntryRec {
            app_id: app_id.clone(),
            channel: channel.clone(),
            message_serial: record.message_serial().as_str().into(),
            version_serial: record.version_serial().as_str().into(),
            delivery_serial: delivery,
            payload_bytes: payload.clone(),
            text_snapshot_key: None,
            created_at_ms: now,
        };
        let increment = i64::from(
            record.message.action == sockudo_core::versioned_messages::MessageAction::Append,
        );
        let initial_message = StoredVersionMessageRec {
            app_id: app_id.clone(),
            channel: channel.clone(),
            message_serial: record.message_serial().as_str().into(),
            latest_version_serial: record.version_serial().as_str().into(),
            state_version_serial: Some(record.version_serial().as_str().to_string()),
            latest_entry_key: entry_id.clone(),
            history_serial: record.history_serial() as i64,
            latest_payload_bytes: payload.clone(),
            append_count: increment,
            is_open_stream: record.is_open_ai_stream(),
            updated_at_ms: now,
        };
        for _ in 0..64 {
            let query = self.db.query(
                "BEGIN TRANSACTION;
                 LET $exists = SELECT VALUE id FROM type::record($entries, $entry_id);
                 IF array::len($exists) = 0 {
                    LET $previous = (SELECT * FROM type::record($messages, $message_id))[0];
                    LET $wins = $previous = NONE OR $previous.latest_version_serial < $version;
                    LET $old_open = IF $previous.is_open_stream = true THEN 1 ELSE 0 END;
                    LET $created = CREATE ONLY type::record($entries, $entry_id) CONTENT $entry;
                    IF $previous = NONE {
                        LET $message_write = CREATE ONLY type::record($messages, $message_id) CONTENT $initial_message;
                    } ELSE {
                        LET $count_write = UPDATE type::record($messages, $message_id) SET append_count = (append_count ?? 0) + $increment, updated_at_ms = $now;
                        IF $wins {
                            LET $latest_write = UPDATE type::record($messages, $message_id) SET latest_version_serial = $version, state_version_serial = $version, latest_entry_key = $entry_id, latest_payload_bytes = $payload, is_open_stream = $is_open;
                        };
                    };
                    LET $stream_write = UPSERT type::record($streams, $stream_id) SET app_id = $app_id, channel = $channel, stream_id = $public_stream_id,
                        next_delivery_serial = math::max([next_delivery_serial ?? 1, $next_delivery]),
                        open_stream_count = (open_stream_count ?? 0) + IF $wins THEN $new_open - $old_open ELSE 0 END,
                        oldest_delivery_serial = math::min([oldest_delivery_serial ?? $delivery, $delivery]),
                        newest_delivery_serial = math::max([newest_delivery_serial ?? $delivery, $delivery]), updated_at_ms = $now;
                 }; COMMIT TRANSACTION;"
            ).bind(("entries", self.tables.entries.clone())).bind(("entry_id", entry_id.clone())).bind(("entry", entry.clone()))
                .bind(("messages", self.tables.messages.clone())).bind(("message_id", message_id.clone())).bind(("initial_message", initial_message.clone()))
                .bind(("version", record.version_serial().as_str().to_string())).bind(("payload", payload.clone())).bind(("increment", increment)).bind(("is_open", record.is_open_ai_stream()))
                .bind(("streams", self.tables.streams.clone())).bind(("stream_id", stream_id.clone())).bind(("app_id", app_id.clone())).bind(("channel", channel.clone()))
                .bind(("public_stream_id", format!("{app_id}/{channel}"))).bind(("delivery", delivery)).bind(("next_delivery", delivery + 1)).bind(("new_open", i64::from(record.is_open_ai_stream()))).bind(("now", now));
            let result = query.await.and_then(|mut response| {
                let mut errors: Vec<_> = response.take_errors().into_iter().collect();
                errors.sort_by_key(|(index, _)| *index);
                if errors.is_empty() {
                    Ok(response)
                } else {
                    let actual = errors
                        .iter()
                        .position(|(_, error)| {
                            !error
                                .to_string()
                                .contains("query was not executed due to a failed transaction")
                        })
                        .unwrap_or(0);
                    Err(errors.swap_remove(actual).1)
                }
            });
            match result {
                Ok(_) => return Ok(()),
                Err(error)
                    if error
                        .to_string()
                        .to_ascii_lowercase()
                        .contains("write conflict")
                        || error.to_string().contains("already exists")
                        || error.to_string().contains("already been created") =>
                {
                    continue;
                }
                Err(error) => {
                    return Err(Error::Internal(format!(
                        "failed to import SurrealDB version transaction: {error}"
                    )));
                }
            }
        }
        Err(Error::OverCapacity)
    }

    async fn commit_create(&self, request: VersionCreateRequest) -> Result<VersionCreateResult> {
        if let Some(limit) = request.limits.max_accumulated_message_bytes
            && request.record.data_bytes()? > limit
        {
            return Ok(VersionCreateResult::Rejected(
                VersionCreateRejection::AccumulatedMessageBytes { limit },
            ));
        }
        let stream_record_id = deterministic_key(
            [
                request.record.app_id.as_str(),
                request.record.channel.as_str(),
            ]
            .into_iter(),
        );
        let existing_stream: Option<StoredVersionStreamRec> = self
            .db
            .select((self.tables.streams.clone(), stream_record_id.clone()))
            .await
            .map_err(|e| Error::Internal(format!("Failed to read version stream: {e}")))?;
        let next_delivery = existing_stream
            .as_ref()
            .map_or(1, |stream| stream.next_delivery_serial) as u64;
        let open_count = existing_stream
            .as_ref()
            .map_or(0, |stream| stream.open_stream_count) as usize;
        if request.record.is_open_ai_stream()
            && let Some(limit) = request.limits.max_open_streaming_messages_per_channel
            && open_count >= limit
        {
            return Ok(VersionCreateResult::Rejected(
                VersionCreateRejection::OpenStreamingMessages { limit },
            ));
        }
        let message_id = deterministic_key(
            [
                request.record.app_id.as_str(),
                request.record.channel.as_str(),
                request.record.message_serial().as_str(),
            ]
            .into_iter(),
        );
        if let Some(existing) = self
            .get_latest(
                &request.record.app_id,
                &request.record.channel,
                request.record.message_serial(),
            )
            .await?
        {
            return Ok(VersionCreateResult::Conflict {
                current: Some(existing),
            });
        }
        let stream_id = format!("{}/{}", request.record.app_id, request.record.channel);
        let record = request
            .record
            .with_delivery_position(&stream_id, next_delivery);
        let payload_bytes = sonic_rs::to_vec(&record)
            .map_err(|e| Error::Internal(format!("Failed to serialize create record: {e}")))?;
        let now_ms = sockudo_core::history::now_ms();
        let entry_id = deterministic_key(
            [
                record.app_id.as_str(),
                record.channel.as_str(),
                record.message_serial().as_str(),
                record.version_serial().as_str(),
            ]
            .into_iter(),
        );
        let entry = StoredVersionEntryRec {
            app_id: record.app_id.clone(),
            channel: record.channel.clone(),
            message_serial: record.message_serial().as_str().to_string(),
            version_serial: record.version_serial().as_str().to_string(),
            delivery_serial: next_delivery as i64,
            payload_bytes: payload_bytes.clone(),
            text_snapshot_key: None,
            created_at_ms: now_ms,
        };
        let message = StoredVersionMessageRec {
            app_id: record.app_id.clone(),
            channel: record.channel.clone(),
            message_serial: record.message_serial().as_str().to_string(),
            latest_version_serial: record.version_serial().as_str().to_string(),
            state_version_serial: Some(record.version_serial().as_str().to_string()),
            latest_entry_key: entry_id.clone(),
            history_serial: record.history_serial() as i64,
            latest_payload_bytes: payload_bytes,
            append_count: 0,
            is_open_stream: record.is_open_ai_stream(),
            updated_at_ms: now_ms,
        };
        let next_open = open_count + usize::from(record.is_open_ai_stream());
        let (stream_statement, stream_content) = if let Some(stream) = existing_stream {
            (
                "LET $stream_write = UPDATE type::record($stream_table, $stream_id) SET next_delivery_serial = $next_delivery, open_stream_count = $next_open, oldest_delivery_serial = IF oldest_delivery_serial IS NONE THEN $delivery ELSE oldest_delivery_serial END, newest_delivery_serial = $delivery, updated_at_ms = $now WHERE next_delivery_serial = $expected_delivery AND open_stream_count = $expected_open RETURN AFTER; IF array::len($stream_write) = 0 { THROW 'version_conflict'; };",
                Some(stream),
            )
        } else {
            (
                "LET $stream_write = CREATE ONLY type::record($stream_table, $stream_id) CONTENT $stream_content;",
                None,
            )
        };
        let stream = stream_content.unwrap_or(StoredVersionStreamRec {
            app_id: record.app_id.clone(),
            channel: record.channel.clone(),
            stream_id: stream_id.clone(),
            next_delivery_serial: 2,
            oldest_delivery_serial: Some(1),
            newest_delivery_serial: Some(1),
            open_stream_count: next_open as i64,
            updated_at_ms: now_ms,
        });
        let sql = format!(
            "BEGIN TRANSACTION; {stream_statement} LET $message_write = CREATE ONLY type::record($message_table, $message_id) CONTENT $message_content; LET $entry_write = CREATE ONLY type::record($entry_table, $entry_id) CONTENT $entry_content; COMMIT TRANSACTION;"
        );
        let response = self
            .db
            .query(sql)
            .bind(("stream_table", self.tables.streams.clone()))
            .bind(("stream_id", stream_record_id))
            .bind(("stream_content", stream))
            .bind(("next_delivery", (next_delivery + 1) as i64))
            .bind(("next_open", next_open as i64))
            .bind(("delivery", next_delivery as i64))
            .bind(("now", now_ms))
            .bind(("expected_delivery", next_delivery as i64))
            .bind(("expected_open", open_count as i64))
            .bind(("message_table", self.tables.messages.clone()))
            .bind(("message_id", message_id))
            .bind(("message_content", message))
            .bind(("entry_table", self.tables.entries.clone()))
            .bind(("entry_id", entry_id))
            .bind(("entry_content", entry))
            .await;
        match response.and_then(|response| response.check()) {
            Ok(_) => Ok(VersionCreateResult::Applied { record, stream_id }),
            Err(error)
                if error.to_string().contains("version_conflict")
                    || error
                        .to_string()
                        .to_ascii_lowercase()
                        .contains("write conflict")
                    || error.to_string().contains("already exists")
                    || error.to_string().contains("already been created") =>
            {
                Ok(VersionCreateResult::Conflict {
                    current: self
                        .get_latest(&record.app_id, &record.channel, record.message_serial())
                        .await?,
                })
            }
            Err(error) => Err(Error::Internal(format!(
                "Failed to transact SurrealDB version create: {error}"
            ))),
        }
    }

    async fn compare_and_apply(
        &self,
        request: VersionMutationRequest,
    ) -> Result<VersionMutationResult> {
        let stream_record_id =
            deterministic_key([request.app_id.as_str(), request.channel.as_str()].into_iter());
        let Some(stream): Option<StoredVersionStreamRec> = self
            .db
            .select((self.tables.streams.clone(), stream_record_id.clone()))
            .await
            .map_err(|e| Error::Internal(format!("Failed to read version stream: {e}")))?
        else {
            return Ok(VersionMutationResult::Conflict { current: None });
        };
        if let Some(operation) = request.idempotency.as_ref() {
            let receipt_id = deterministic_key(
                [
                    request.app_id.as_str(),
                    request.channel.as_str(),
                    operation.cache_key.as_str(),
                ]
                .into_iter(),
            );
            if let Some(receipt) = self
                .db
                .select::<Option<StoredVersionReceiptRec>>((
                    self.tables.receipts.clone(),
                    receipt_id,
                ))
                .await
                .map_err(|e| Error::Internal(format!("Failed to read mutation receipt: {e}")))?
            {
                if receipt.operation_fingerprint != operation.payload_fingerprint {
                    return Err(Error::IdempotencyConflict);
                }
                let record = self
                    .decode_records(vec![receipt.payload_bytes])
                    .await?
                    .remove(0);
                return Ok(VersionMutationResult::Duplicate {
                    record,
                    stream_id: stream.stream_id,
                });
            }
        }
        let message_id = deterministic_key(
            [
                request.app_id.as_str(),
                request.channel.as_str(),
                request.message_serial.as_str(),
            ]
            .into_iter(),
        );
        let Some(message): Option<StoredVersionMessageRec> = self
            .db
            .select((self.tables.messages.clone(), message_id.clone()))
            .await
            .map_err(|e| Error::Internal(format!("Failed to read mutation predecessor: {e}")))?
        else {
            return Ok(VersionMutationResult::Conflict { current: None });
        };
        let current_encoding = Self::cached_latest(&message)?;
        let current = if current_encoding.is_none() {
            self.get_latest(&request.app_id, &request.channel, &request.message_serial)
                .await?
                .ok_or_else(|| Error::Internal("Latest version entry is missing".to_string()))?
        } else {
            self.decode_records(vec![message.latest_payload_bytes.clone()])
                .await?
                .remove(0)
        };
        let delivery_serial =
            (stream.next_delivery_serial as u64).max(current.delivery_serial().saturating_add(1));
        let outcome = request.apply_to(
            &current,
            &stream.stream_id,
            delivery_serial,
            message.append_count as usize,
        )?;
        let VersionMutationResult::Applied { record, .. } = outcome else {
            return Ok(outcome);
        };
        let opens = !current.is_open_ai_stream() && record.is_open_ai_stream();
        let closes = current.is_open_ai_stream() && !record.is_open_ai_stream();
        if opens
            && let Some(limit) = request.limits.max_open_streaming_messages_per_channel
            && stream.open_stream_count as usize >= limit
        {
            return Ok(VersionMutationResult::Rejected(
                VersionMutationRejection::OpenStreamingMessages { limit },
            ));
        }
        let plan = sockudo_core::version_store::EncodedVersionRecord::plan(
            &record,
            Some((
                &current,
                current_encoding
                    .as_ref()
                    .and_then(|record| record.text.as_ref()),
            )),
        )?;
        let payload_bytes = plan.entry_bytes;
        let now_ms = sockudo_core::history::now_ms();
        let entry_id = deterministic_key(
            [
                record.app_id.as_str(),
                record.channel.as_str(),
                record.message_serial().as_str(),
                record.version_serial().as_str(),
            ]
            .into_iter(),
        );
        let entry = StoredVersionEntryRec {
            app_id: record.app_id.clone(),
            channel: record.channel.clone(),
            message_serial: record.message_serial().as_str().to_string(),
            version_serial: record.version_serial().as_str().to_string(),
            delivery_serial: delivery_serial as i64,
            payload_bytes: payload_bytes.clone(),
            text_snapshot_key: plan
                .snapshot
                .as_ref()
                .map(|(reference, _)| reference.snapshot_key.clone()),
            created_at_ms: now_ms,
        };
        let next_open = stream.open_stream_count + i64::from(opens) - i64::from(closes);
        let next_append = message.append_count
            + i64::from(matches!(
                request.mutation,
                sockudo_core::version_store::VersionMutation::Append(_)
            ));
        let (receipt_statement, receipt_id, receipt) = if let Some(operation) =
            request.idempotency.as_ref()
        {
            (
                "LET $receipt_write = CREATE ONLY type::record($receipt_table, $receipt_id) CONTENT $receipt_content;",
                deterministic_key(
                    [
                        request.app_id.as_str(),
                        request.channel.as_str(),
                        operation.cache_key.as_str(),
                    ]
                    .into_iter(),
                ),
                StoredVersionReceiptRec {
                    operation_fingerprint: operation.payload_fingerprint.clone(),
                    payload_bytes: payload_bytes.clone(),
                    created_at_ms: now_ms,
                },
            )
        } else {
            (
                "",
                "unused".to_string(),
                StoredVersionReceiptRec {
                    operation_fingerprint: String::new(),
                    payload_bytes: Vec::new(),
                    created_at_ms: now_ms,
                },
            )
        };
        let snapshot_id = plan
            .snapshot
            .as_ref()
            .map(|(reference, _)| {
                Self::text_key(&record.app_id, &record.channel, &reference.snapshot_key)
            })
            .unwrap_or_default();
        let snapshot_content =
            plan.snapshot
                .as_ref()
                .map(|(reference, text)| StoredVersionTextRec {
                    app_id: record.app_id.clone(),
                    channel: record.channel.clone(),
                    snapshot_key: reference.snapshot_key.clone(),
                    text_data: text.clone(),
                    updated_at_ms: now_ms,
                    retain_for_receipts: reference.retain_for_receipts,
                });
        let snapshot_statement = if snapshot_content.is_some() {
            "LET $snapshot_write = UPSERT ONLY type::record($text_table, $snapshot_id) CONTENT $snapshot_content;"
        } else {
            ""
        };
        let query = self
            .db
            .query(format!(
                "BEGIN TRANSACTION; LET $stream_write = UPDATE type::record($stream_table, $stream_id) SET next_delivery_serial = $next_delivery, open_stream_count = $next_open, newest_delivery_serial = $delivery, updated_at_ms = $now WHERE next_delivery_serial = $expected_delivery AND open_stream_count = $expected_open RETURN AFTER; IF array::len($stream_write) = 0 {{ THROW 'version_conflict'; }}; LET $message_write = UPDATE type::record($message_table, $message_id) SET latest_version_serial = $next_version, state_version_serial = $next_version, latest_entry_key = $entry_id, latest_payload_bytes = $payload, append_count = $next_append, is_open_stream = $is_open, updated_at_ms = $now WHERE latest_version_serial = $expected_version AND append_count = $expected_append RETURN AFTER; IF array::len($message_write) = 0 {{ THROW 'version_conflict'; }}; LET $entry_write = CREATE ONLY type::record($entry_table, $entry_id) CONTENT $entry_content; {receipt_statement} {snapshot_statement} COMMIT TRANSACTION;"
            ))
            .bind(("stream_table", self.tables.streams.clone()))
            .bind(("stream_id", stream_record_id))
            .bind(("next_delivery", (delivery_serial + 1) as i64))
            .bind(("next_open", next_open))
            .bind(("delivery", delivery_serial as i64))
            .bind(("now", now_ms))
            .bind(("expected_delivery", stream.next_delivery_serial))
            .bind(("expected_open", stream.open_stream_count))
            .bind(("message_table", self.tables.messages.clone()))
            .bind(("message_id", message_id))
            .bind(("next_version", record.version_serial().as_str().to_string()))
            .bind(("payload", plan.latest_bytes))
            .bind(("next_append", next_append))
            .bind(("is_open", record.is_open_ai_stream()))
            .bind(("expected_version", current.version_serial().as_str().to_string()))
            .bind(("expected_append", message.append_count))
            .bind(("entry_table", self.tables.entries.clone()))
            .bind(("entry_id", entry_id))
            .bind(("entry_content", entry))
            .bind(("receipt_table", self.tables.receipts.clone()))
            .bind(("receipt_id", receipt_id))
            .bind(("receipt_content", receipt))
            .bind(("text_table", self.tables.texts.clone()))
            .bind(("snapshot_id", snapshot_id))
            .bind(("snapshot_content", snapshot_content));
        match query.await.and_then(|mut response| {
            let mut errors: Vec<_> = response.take_errors().into_iter().collect();
            errors.sort_by_key(|(index, _)| *index);
            if errors.is_empty() {
                Ok(response)
            } else {
                let actual = errors
                    .iter()
                    .position(|(_, error)| {
                        !error
                            .to_string()
                            .contains("query was not executed due to a failed transaction")
                    })
                    .unwrap_or(0);
                Err(errors.swap_remove(actual).1)
            }
        }) {
            Ok(_) => Ok(VersionMutationResult::Applied {
                record,
                stream_id: stream.stream_id,
            }),
            Err(error)
                if error.to_string().contains("version_conflict")
                    || error
                        .to_string()
                        .to_ascii_lowercase()
                        .contains("write conflict")
                    || error.to_string().contains("already exists")
                    || error.to_string().contains("already been created") =>
            {
                Ok(VersionMutationResult::Conflict {
                    current: self
                        .get_latest(&request.app_id, &request.channel, &request.message_serial)
                        .await?,
                })
            }
            Err(error) => Err(Error::Internal(format!(
                "Failed to transact SurrealDB version mutation: {error}"
            ))),
        }
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

        if Self::cached_latest(&msg)?.is_some() {
            let record = self
                .decode_records(vec![msg.latest_payload_bytes])
                .await?
                .remove(0);
            return Ok(Some(record));
        }

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

        let record = self
            .decode_records(vec![entry.payload_bytes])
            .await?
            .remove(0);
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
        use futures_util::{StreamExt, TryStreamExt};
        let requested = message_serials
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        // Point selection uses deterministic record IDs and preserves legacy
        // latest-entry fallback. Bound in-flight database calls without spawning.
        let records = futures_util::stream::iter(
            requested
                .into_iter()
                .map(|serial| async move { self.get_latest(app_id, channel, &serial).await }),
        )
        .buffer_unordered(8)
        .try_collect::<Vec<_>>()
        .await?;
        Ok(records
            .into_iter()
            .flatten()
            .map(|record| (record.message_serial().clone(), record))
            .collect())
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
            "SELECT app_id, channel, message_serial, version_serial, delivery_serial, payload_bytes, created_at_ms, text_snapshot_key FROM {} WHERE {} ORDER BY version_serial {} LIMIT {}",
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
        let items = self
            .decode_records(
                rows.into_iter()
                    .take(request.limit)
                    .map(|row| row.payload_bytes)
                    .collect(),
            )
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

        let mut response = self
            .db
            .query(format!(
                "SELECT payload_bytes, delivery_serial FROM {} WHERE app_id = $app_id AND channel = $channel AND delivery_serial > $after ORDER BY delivery_serial ASC LIMIT {}",
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

        self.decode_records(rows.into_iter().map(|row| row.payload_bytes).collect())
            .await
    }

    async fn latest_by_history(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Vec<StoredVersionRecord>> {
        let mut response = self
            .db
            .query(format!(
                "SELECT latest_entry_key, history_serial FROM {} WHERE app_id = $app_id AND channel = $channel ORDER BY history_serial ASC",
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
                result.push(entry.payload_bytes);
            }
        }
        self.decode_records(result).await
    }

    async fn message_count(&self, app_id: &str, channel: &str) -> Result<u64> {
        #[derive(serde::Deserialize, surrealdb_types::SurrealValue)]
        struct CountRow {
            count: u64,
        }
        let mut response = self.db.query(format!("SELECT count() AS count FROM {} WHERE app_id = $app_id AND channel = $channel GROUP ALL", self.tables.messages))
            .bind(("app_id", app_id.to_string())).bind(("channel", channel.to_string())).await
            .map_err(|e| Error::Internal(format!("Failed to count version messages: {e}")))?;
        let rows: Vec<CountRow> = response
            .take(0usize)
            .map_err(|e| Error::Internal(format!("Failed to decode message count: {e}")))?;
        Ok(rows.first().map_or(0, |row| row.count))
    }

    async fn active_stream_count(&self, app_id: &str, channel: &str) -> Result<usize> {
        #[derive(serde::Deserialize, surrealdb_types::SurrealValue)]
        struct StateRow {
            message_serial: String,
            latest_version_serial: String,
            state_version_serial: Option<String>,
            is_open_stream: Option<bool>,
        }
        let mut after = String::new();
        let mut count = 0usize;
        loop {
            let mut response = self.db.query(format!("SELECT message_serial, latest_version_serial, state_version_serial, is_open_stream FROM {} WHERE app_id=$app AND channel=$channel AND message_serial>$after ORDER BY message_serial LIMIT 100", self.tables.messages))
                .bind(("app", app_id.to_owned())).bind(("channel", channel.to_owned())).bind(("after", after.clone()))
                .await.map_err(|e| Error::Internal(format!("failed to read active stream metadata: {e}")))?;
            let rows: Vec<StateRow> = response.take(0usize).map_err(|e| {
                Error::Internal(format!("failed to decode active stream metadata: {e}"))
            })?;
            if rows.is_empty() {
                return Ok(count);
            }
            for row in rows {
                after = row.message_serial;
                if row.state_version_serial.as_deref() == Some(row.latest_version_serial.as_str())
                    && let Some(open) = row.is_open_stream
                {
                    count += usize::from(open);
                    continue;
                }
                let serial = MessageSerial::new(after.clone())?;
                if let Some(record) = self.get_latest(app_id, channel, &serial).await? {
                    count += usize::from(record.is_open_ai_stream());
                    let id = deterministic_key([app_id, channel, serial.as_str()].into_iter());
                    self.db.query("UPDATE type::record($table, $id) SET is_open_stream=$open, state_version_serial=$version WHERE latest_version_serial=$version RETURN NONE")
                        .bind(("table", self.tables.messages.clone())).bind(("id", id))
                        .bind(("open", record.is_open_ai_stream())).bind(("version", record.version_serial().as_str().to_owned()))
                        .await.map_err(|e| Error::Internal(format!("failed to refresh active stream metadata: {e}")))?
                        .check().map_err(|e| Error::Internal(format!("failed to commit active stream metadata: {e}")))?;
                }
            }
        }
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
                let mut deleted = db
                    .query(format!(
                        "DELETE $ids WHERE {ts_field} < $cutoff RETURN BEFORE"
                    ))
                    .bind(("cutoff", before_ms))
                    .bind(("ids", ids))
                    .await
                    .map_err(|e| {
                        Error::Internal(format!(
                            "Failed to delete expired rows in SurrealDB {table}: {e}"
                        ))
                    })?;
                let deleted: Vec<VersionDeletedRow> = deleted.take(0usize).map_err(|e| {
                    Error::Internal(format!("failed to decode purged version rows: {e}"))
                })?;
                Ok((deleted.len() as u64, len as i64 == limit))
            }
        };

        let (entries_deleted, entries_more) =
            purge_table(&self.tables.entries, "created_at_ms").await?;
        let (messages_deleted, messages_more) =
            purge_table(&self.tables.messages, "updated_at_ms").await?;

        let (snapshots_deleted, snapshots_more) =
            self.purge_texts(before_ms, batch_size.min(256)).await?;
        Ok((
            entries_deleted + messages_deleted + snapshots_deleted,
            entries_more || messages_more || snapshots_more,
        ))
    }
}
