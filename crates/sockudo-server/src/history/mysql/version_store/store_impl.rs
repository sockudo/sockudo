use super::*;
use sockudo_core::version_store::EncodedVersionRecord;

#[cfg(feature = "versioned-messages")]
#[async_trait::async_trait]
impl VersionStore for MysqlVersionStore {
    async fn ensure_stream_id(&self, app_id: &str, channel: &str) -> Result<String> {
        Ok(format!("{app_id}/{channel}"))
    }

    async fn reserve_delivery_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<VersionWriteReservation> {
        let now_ms = sockudo_core::history::now_ms();

        // Insert with initial value 1 if the row doesn't exist yet.
        let insert_sql = format!(
            "INSERT IGNORE INTO `{}` (app_id, channel, next_delivery_serial, updated_at_ms) VALUES (?, ?, 1, ?)",
            self.tables.version_streams
        );
        sqlx::query(sqlx::AssertSqlSafe(insert_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .bind(now_ms)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to initialize version stream row: {e}"))
            })?;

        let mut tx = self.pool.begin().await.map_err(|e| {
            Error::Internal(format!("Failed to begin version reserve transaction: {e}"))
        })?;

        let select_sql = format!(
            "SELECT next_delivery_serial FROM `{}` WHERE app_id = ? AND channel = ? FOR UPDATE",
            self.tables.version_streams
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(select_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to read version stream for update: {e}"))
            })?;
        let reserved: i64 = row.get("next_delivery_serial");

        let update_sql = format!(
            "UPDATE `{}` SET next_delivery_serial = next_delivery_serial + 1, updated_at_ms = ? WHERE app_id = ? AND channel = ?",
            self.tables.version_streams
        );
        sqlx::query(sqlx::AssertSqlSafe(update_sql.as_str()))
            .bind(now_ms)
            .bind(app_id)
            .bind(channel)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to advance version delivery serial: {e}"))
            })?;

        tx.commit().await.map_err(|e| {
            Error::Internal(format!("Failed to commit version reserve transaction: {e}"))
        })?;

        Ok(VersionWriteReservation {
            stream_id: format!("{}/{}", app_id, channel),
            delivery_serial: reserved as u64,
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
        let now_ms = sockudo_core::history::now_ms();

        let insert_sql = format!(
            "INSERT IGNORE INTO `{}` (app_id, channel, next_delivery_serial, updated_at_ms) VALUES (?, ?, 1, ?)",
            self.tables.version_streams
        );
        sqlx::query(sqlx::AssertSqlSafe(insert_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .bind(now_ms)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to initialize version stream row: {e}"))
            })?;

        let mut tx = self.pool.begin().await.map_err(|e| {
            Error::Internal(format!("Failed to begin version reserve transaction: {e}"))
        })?;

        let select_sql = format!(
            "SELECT next_delivery_serial FROM `{}` WHERE app_id = ? AND channel = ? FOR UPDATE",
            self.tables.version_streams
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(select_sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to read version stream for update: {e}"))
            })?;
        let reserved: i64 = row.get("next_delivery_serial");

        let update_sql = format!(
            "UPDATE `{}` SET next_delivery_serial = next_delivery_serial + ?, updated_at_ms = ? WHERE app_id = ? AND channel = ?",
            self.tables.version_streams
        );
        sqlx::query(sqlx::AssertSqlSafe(update_sql.as_str()))
            .bind(block_size_i64)
            .bind(now_ms)
            .bind(app_id)
            .bind(channel)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to advance version delivery serial: {e}"))
            })?;

        tx.commit().await.map_err(|e| {
            Error::Internal(format!("Failed to commit version reserve transaction: {e}"))
        })?;

        Ok(VersionWriteReservationBlock {
            stream_id: format!("{}/{}", app_id, channel),
            start_delivery_serial: reserved as u64,
            len: block_size,
        })
    }

    async fn append_version(&self, record: StoredVersionRecord) -> Result<()> {
        let now_ms = sockudo_core::history::now_ms();
        let payload = sonic_rs::to_vec(&record)
            .map_err(|e| Error::Internal(format!("Failed to serialize version record: {e}")))?;
        let payload_size = payload.len() as i64;

        let insert_entry = format!(
            r#"INSERT IGNORE INTO `{}` (
                app_id, channel, message_serial, version_serial, delivery_serial, history_serial,
                action, client_id, description, event_name,
                payload_bytes, payload_size_bytes, version_timestamp_ms, created_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            self.tables.version_entries
        );
        sqlx::query(sqlx::AssertSqlSafe(insert_entry.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(record.message_serial().as_str())
            .bind(record.version_serial().as_str())
            .bind(record.delivery_serial() as i64)
            .bind(record.history_serial() as i64)
            .bind(record.message.action.as_str())
            .bind(record.original_client_id.as_deref())
            .bind(record.message.version.description.as_deref())
            .bind(record.message.name.as_deref())
            .bind(payload.as_slice())
            .bind(payload_size)
            .bind(record.message.version.timestamp_ms)
            .bind(now_ms)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to insert version entry: {e}")))?;

        // Upsert version_messages: advance only if the incoming version_serial is greater.
        let upsert_msg = format!(
            r#"INSERT INTO `{}` (
                app_id, channel, message_serial, history_serial, original_client_id,
                latest_version_serial, latest_delivery_serial, latest_action, is_open_stream,
                created_at_ms, updated_at_ms, state_version_serial
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                latest_delivery_serial = IF(latest_version_serial < VALUES(latest_version_serial), VALUES(latest_delivery_serial), latest_delivery_serial),
                latest_action = IF(latest_version_serial < VALUES(latest_version_serial), VALUES(latest_action), latest_action),
                is_open_stream = IF(latest_version_serial < VALUES(latest_version_serial), VALUES(is_open_stream), is_open_stream),
                updated_at_ms = IF(latest_version_serial < VALUES(latest_version_serial), VALUES(updated_at_ms), updated_at_ms),
                state_version_serial = IF(latest_version_serial < VALUES(latest_version_serial), VALUES(state_version_serial), state_version_serial),
                latest_version_serial = IF(latest_version_serial < VALUES(latest_version_serial), VALUES(latest_version_serial), latest_version_serial)"#,
            self.tables.version_messages
        );
        sqlx::query(sqlx::AssertSqlSafe(upsert_msg.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(record.message_serial().as_str())
            .bind(record.history_serial() as i64)
            .bind(record.original_client_id.as_deref())
            .bind(record.version_serial().as_str())
            .bind(record.delivery_serial() as i64)
            .bind(record.message.action.as_str())
            .bind(record.is_open_ai_stream())
            .bind(now_ms)
            .bind(now_ms)
            .bind(record.version_serial().as_str())
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to upsert version message: {e}")))?;

        // Update stream delivery window.
        let update_stream = format!(
            r#"UPDATE `{}` SET
                oldest_available_delivery_serial = CASE
                    WHEN oldest_available_delivery_serial IS NULL OR ? < oldest_available_delivery_serial
                    THEN ? ELSE oldest_available_delivery_serial END,
                newest_available_delivery_serial = CASE
                    WHEN newest_available_delivery_serial IS NULL OR ? > newest_available_delivery_serial
                    THEN ? ELSE newest_available_delivery_serial END,
                updated_at_ms = ?
            WHERE app_id = ? AND channel = ?"#,
            self.tables.version_streams
        );
        let delivery = record.delivery_serial() as i64;
        sqlx::query(sqlx::AssertSqlSafe(update_stream.as_str()))
            .bind(delivery)
            .bind(delivery)
            .bind(delivery)
            .bind(delivery)
            .bind(now_ms)
            .bind(&record.app_id)
            .bind(&record.channel)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to update version stream window: {e}")))?;

        Ok(())
    }

    async fn commit_create(&self, request: VersionCreateRequest) -> Result<VersionCreateResult> {
        let mut tx = self.pool.begin().await.map_err(|e| {
            Error::Internal(format!("Failed to begin version create transaction: {e}"))
        })?;
        let now_ms = sockudo_core::history::now_ms();
        let stream_id = format!("{}/{}", request.record.app_id, request.record.channel);
        let ensure_stream = format!(
            "INSERT IGNORE INTO `{}` (app_id, channel, next_delivery_serial, updated_at_ms) VALUES (?, ?, 1, ?)",
            self.tables.version_streams
        );
        sqlx::query(sqlx::AssertSqlSafe(ensure_stream.as_str()))
            .bind(&request.record.app_id)
            .bind(&request.record.channel)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to initialize version stream: {e}")))?;
        let lock_stream = format!(
            "SELECT next_delivery_serial FROM `{}` WHERE app_id = ? AND channel = ? FOR UPDATE",
            self.tables.version_streams
        );
        let stream = sqlx::query(sqlx::AssertSqlSafe(lock_stream.as_str()))
            .bind(&request.record.app_id)
            .bind(&request.record.channel)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to lock version stream: {e}")))?;
        let latest_sql = format!(
            "SELECT payload_bytes FROM `{}` WHERE app_id = ? AND channel = ? AND message_serial = ? ORDER BY version_serial DESC LIMIT 1",
            self.tables.version_entries
        );
        if let Some(row) = sqlx::query(sqlx::AssertSqlSafe(latest_sql.as_str()))
            .bind(&request.record.app_id)
            .bind(&request.record.channel)
            .bind(request.record.message_serial().as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to check version create target: {e}")))?
        {
            let payload: Vec<u8> = row.get("payload_bytes");
            let current = self
                .decode_records_on(&mut tx, vec![payload])
                .await?
                .remove(0);
            return Ok(VersionCreateResult::Conflict {
                current: Some(current),
            });
        }
        if let Some(limit) = request.limits.max_accumulated_message_bytes
            && request.record.data_bytes()? > limit
        {
            return Ok(VersionCreateResult::Rejected(
                VersionCreateRejection::AccumulatedMessageBytes { limit },
            ));
        }
        if request.record.is_open_ai_stream()
            && let Some(limit) = request.limits.max_open_streaming_messages_per_channel
        {
            let count_sql = format!(
                "SELECT COUNT(*) AS count FROM `{}` WHERE app_id = ? AND channel = ? AND is_open_stream = TRUE",
                self.tables.version_messages
            );
            let count = sqlx::query(sqlx::AssertSqlSafe(count_sql.as_str()))
                .bind(&request.record.app_id)
                .bind(&request.record.channel)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("Failed to count open streams: {e}")))?
                .get::<i64, _>("count") as usize;
            if count >= limit {
                return Ok(VersionCreateResult::Rejected(
                    VersionCreateRejection::OpenStreamingMessages { limit },
                ));
            }
        }
        let delivery_serial = stream.get::<i64, _>("next_delivery_serial") as u64;
        let record = request
            .record
            .with_delivery_position(&stream_id, delivery_serial);
        let payload = sonic_rs::to_vec(&record)
            .map_err(|e| Error::Internal(format!("Failed to serialize version record: {e}")))?;
        let insert_entry = format!(
            "INSERT INTO `{}` (app_id, channel, message_serial, version_serial, delivery_serial, history_serial, action, client_id, description, event_name, payload_bytes, payload_size_bytes, version_timestamp_ms, created_at_ms, operation_key, operation_fingerprint) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            self.tables.version_entries
        );
        sqlx::query(sqlx::AssertSqlSafe(insert_entry.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(record.message_serial().as_str())
            .bind(record.version_serial().as_str())
            .bind(delivery_serial as i64)
            .bind(record.history_serial() as i64)
            .bind(record.message.action.as_str())
            .bind(record.message.version.client_id.as_deref())
            .bind(record.message.version.description.as_deref())
            .bind(record.message.name.as_deref())
            .bind(payload.as_slice())
            .bind(payload.len() as i64)
            .bind(record.message.version.timestamp_ms)
            .bind(now_ms)
            .bind(None::<&str>)
            .bind(None::<&str>)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to insert create version: {e}")))?;
        let insert_message = format!(
            "INSERT INTO `{}` (app_id, channel, message_serial, history_serial, original_client_id, latest_version_serial, latest_delivery_serial, latest_action, is_open_stream, created_at_ms, updated_at_ms, state_version_serial) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            self.tables.version_messages
        );
        sqlx::query(sqlx::AssertSqlSafe(insert_message.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(record.message_serial().as_str())
            .bind(record.history_serial() as i64)
            .bind(record.original_client_id.as_deref())
            .bind(record.version_serial().as_str())
            .bind(delivery_serial as i64)
            .bind(record.message.action.as_str())
            .bind(record.is_open_ai_stream())
            .bind(now_ms)
            .bind(now_ms)
            .bind(record.version_serial().as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to insert version message: {e}")))?;
        let update_stream = format!(
            "UPDATE `{}` SET next_delivery_serial = ?, oldest_available_delivery_serial = IFNULL(oldest_available_delivery_serial, ?), newest_available_delivery_serial = ?, updated_at_ms = ? WHERE app_id = ? AND channel = ?",
            self.tables.version_streams
        );
        sqlx::query(sqlx::AssertSqlSafe(update_stream.as_str()))
            .bind((delivery_serial + 1) as i64)
            .bind(delivery_serial as i64)
            .bind(delivery_serial as i64)
            .bind(now_ms)
            .bind(&record.app_id)
            .bind(&record.channel)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to commit version stream: {e}")))?;
        tx.commit().await.map_err(|e| {
            Error::Internal(format!("Failed to commit version create transaction: {e}"))
        })?;
        Ok(VersionCreateResult::Applied { record, stream_id })
    }

    async fn compare_and_apply(
        &self,
        request: VersionMutationRequest,
    ) -> Result<VersionMutationResult> {
        let mut tx = self.pool.begin().await.map_err(|e| {
            Error::Internal(format!("Failed to begin version mutation transaction: {e}"))
        })?;
        let stream_id = format!("{}/{}", request.app_id, request.channel);
        let lock_stream = format!(
            "SELECT next_delivery_serial FROM `{}` WHERE app_id = ? AND channel = ? FOR UPDATE",
            self.tables.version_streams
        );
        let Some(stream) = sqlx::query(sqlx::AssertSqlSafe(lock_stream.as_str()))
            .bind(&request.app_id)
            .bind(&request.channel)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to lock version stream: {e}")))?
        else {
            return Ok(VersionMutationResult::Conflict { current: None });
        };
        if let Some(operation) = request.idempotency.as_ref() {
            let operation_sql = format!(
                "SELECT payload_bytes, operation_fingerprint FROM `{}` WHERE app_id = ? AND channel = ? AND operation_key = ? LIMIT 1",
                self.tables.version_entries
            );
            if let Some(row) = sqlx::query(sqlx::AssertSqlSafe(operation_sql.as_str()))
                .bind(&request.app_id)
                .bind(&request.channel)
                .bind(&operation.cache_key)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("Failed to read mutation receipt: {e}")))?
            {
                let fingerprint: Option<String> = row.get("operation_fingerprint");
                if fingerprint.as_deref() != Some(operation.payload_fingerprint.as_str()) {
                    return Err(Error::IdempotencyConflict);
                }
                let payload: Vec<u8> = row.get("payload_bytes");
                let record = self
                    .decode_records_on(&mut tx, vec![payload])
                    .await?
                    .remove(0);
                return Ok(VersionMutationResult::Duplicate { record, stream_id });
            }
        }
        let latest_sql = format!(
            "SELECT payload_bytes FROM `{}` WHERE app_id = ? AND channel = ? AND message_serial = ? ORDER BY version_serial DESC LIMIT 1",
            self.tables.version_entries
        );
        let Some(row) = sqlx::query(sqlx::AssertSqlSafe(latest_sql.as_str()))
            .bind(&request.app_id)
            .bind(&request.channel)
            .bind(request.message_serial.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to read mutation predecessor: {e}")))?
        else {
            return Ok(VersionMutationResult::Conflict { current: None });
        };
        let payload: Vec<u8> = row.get("payload_bytes");
        let predecessor_encoding = EncodedVersionRecord::decode(&payload)?;
        let current = self
            .decode_records_on(&mut tx, vec![payload])
            .await?
            .remove(0);
        let append_count = if matches!(
            request.mutation,
            sockudo_core::version_store::VersionMutation::Append(_)
        ) && request.limits.max_appends_per_message.is_some()
        {
            let cached_sql = format!(
                "SELECT append_count FROM `{}` WHERE app_id = ? AND channel = ? AND message_serial = ? FOR UPDATE",
                self.tables.version_messages
            );
            let cached: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(cached_sql.as_str()))
                .bind(&request.app_id)
                .bind(&request.channel)
                .bind(request.message_serial.as_str())
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to read message append count: {e}"))
                })?;
            if let Some(count) = cached {
                count as usize
            } else {
                // Locking read observes the current committed rows under
                // InnoDB REPEATABLE READ, including imports after our snapshot.
                let count_sql = format!(
                    "SELECT version_serial FROM `{}` WHERE app_id = ? AND channel = ? AND message_serial = ? AND action = 'message.append' FOR UPDATE",
                    self.tables.version_entries
                );
                let rows = sqlx::query(sqlx::AssertSqlSafe(count_sql.as_str()))
                    .bind(&request.app_id)
                    .bind(&request.channel)
                    .bind(request.message_serial.as_str())
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("Failed to initialize message append count: {e}"))
                    })?;
                let count = rows.len() as i64;
                let save_sql = format!(
                    "UPDATE `{}` SET append_count = ? WHERE app_id = ? AND channel = ? AND message_serial = ?",
                    self.tables.version_messages
                );
                sqlx::query(sqlx::AssertSqlSafe(save_sql.as_str()))
                    .bind(count)
                    .bind(&request.app_id)
                    .bind(&request.channel)
                    .bind(request.message_serial.as_str())
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("Failed to save message append count: {e}"))
                    })?;
                count as usize
            }
        } else {
            0
        };
        let delivery_serial = (stream.get::<i64, _>("next_delivery_serial") as u64)
            .max(current.delivery_serial().saturating_add(1));
        let outcome = request.apply_to(&current, &stream_id, delivery_serial, append_count)?;
        let VersionMutationResult::Applied { record, .. } = outcome else {
            return Ok(outcome);
        };
        if !current.is_open_ai_stream()
            && record.is_open_ai_stream()
            && let Some(limit) = request.limits.max_open_streaming_messages_per_channel
        {
            let count_sql = format!(
                "SELECT COUNT(*) AS count FROM `{}` WHERE app_id = ? AND channel = ? AND is_open_stream = TRUE",
                self.tables.version_messages
            );
            let count = sqlx::query(sqlx::AssertSqlSafe(count_sql.as_str()))
                .bind(&request.app_id)
                .bind(&request.channel)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("Failed to count open streams: {e}")))?
                .get::<i64, _>("count") as usize;
            if count >= limit {
                return Ok(VersionMutationResult::Rejected(
                    sockudo_core::version_store::VersionMutationRejection::OpenStreamingMessages {
                        limit,
                    },
                ));
            }
        }
        let plan = EncodedVersionRecord::plan(
            &record,
            Some((&current, predecessor_encoding.text.as_ref())),
        )?;
        let payload = plan.entry_bytes;
        let text_snapshot_key = plan
            .snapshot
            .as_ref()
            .map(|(reference, _)| reference.snapshot_key.as_str());
        if let Some((reference, text)) = plan.snapshot.as_ref() {
            let sql = format!(
                "INSERT INTO `{}` (app_id, channel, snapshot_key, text_data, updated_at_ms) VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE text_data = VALUES(text_data), updated_at_ms = VALUES(updated_at_ms)",
                self.text_table()
            );
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(&record.app_id)
                .bind(&record.channel)
                .bind(&reference.snapshot_key)
                .bind(text)
                .bind(sockudo_core::history::now_ms())
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to persist version text snapshot: {e}"))
                })?;
        }
        let operation_key = request
            .idempotency
            .as_ref()
            .map(|value| value.cache_key.as_str());
        let operation_fingerprint = request
            .idempotency
            .as_ref()
            .map(|value| value.payload_fingerprint.as_str());
        let now_ms = sockudo_core::history::now_ms();
        let insert_entry = format!(
            "INSERT INTO `{}` (app_id, channel, message_serial, version_serial, delivery_serial, history_serial, action, client_id, description, event_name, payload_bytes, payload_size_bytes, version_timestamp_ms, created_at_ms, operation_key, operation_fingerprint, text_snapshot_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            self.tables.version_entries
        );
        sqlx::query(sqlx::AssertSqlSafe(insert_entry.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(record.message_serial().as_str())
            .bind(record.version_serial().as_str())
            .bind(delivery_serial as i64)
            .bind(record.history_serial() as i64)
            .bind(record.message.action.as_str())
            .bind(record.message.version.client_id.as_deref())
            .bind(record.message.version.description.as_deref())
            .bind(record.message.name.as_deref())
            .bind(payload.as_slice())
            .bind(payload.len() as i64)
            .bind(record.message.version.timestamp_ms)
            .bind(now_ms)
            .bind(operation_key)
            .bind(operation_fingerprint)
            .bind(text_snapshot_key)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to insert mutation version: {e}")))?;
        let update_message = format!(
            "UPDATE `{}` SET latest_version_serial = ?, latest_delivery_serial = ?, latest_action = ?, is_open_stream = ?, state_version_serial = latest_version_serial, updated_at_ms = ? WHERE app_id = ? AND channel = ? AND message_serial = ?",
            self.tables.version_messages
        );
        sqlx::query(sqlx::AssertSqlSafe(update_message.as_str()))
            .bind(record.version_serial().as_str())
            .bind(delivery_serial as i64)
            .bind(record.message.action.as_str())
            .bind(record.is_open_ai_stream())
            .bind(now_ms)
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(record.message_serial().as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to advance version message: {e}")))?;
        let update_stream = format!(
            "UPDATE `{}` SET next_delivery_serial = ?, oldest_available_delivery_serial = IFNULL(oldest_available_delivery_serial, ?), newest_available_delivery_serial = GREATEST(IFNULL(newest_available_delivery_serial, ?), ?), updated_at_ms = ? WHERE app_id = ? AND channel = ?",
            self.tables.version_streams
        );
        sqlx::query(sqlx::AssertSqlSafe(update_stream.as_str()))
            .bind((delivery_serial + 1) as i64)
            .bind(delivery_serial as i64)
            .bind(delivery_serial as i64)
            .bind(delivery_serial as i64)
            .bind(now_ms)
            .bind(&record.app_id)
            .bind(&record.channel)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("Failed to advance version stream: {e}")))?;
        tx.commit().await.map_err(|e| {
            Error::Internal(format!(
                "Failed to commit version mutation transaction: {e}"
            ))
        })?;
        Ok(VersionMutationResult::Applied { record, stream_id })
    }

    async fn get_latest(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &sockudo_core::versioned_messages::MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        let sql = format!(
            "SELECT payload_bytes FROM `{}` WHERE app_id = ? AND channel = ? AND message_serial = ? ORDER BY version_serial DESC LIMIT 1",
            self.tables.version_entries
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .bind(message_serial.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to query latest version: {e}")))?;

        match row {
            None => Ok(None),
            Some(row) => {
                let bytes: Vec<u8> = row.get("payload_bytes");
                let record = self.decode_records(vec![bytes]).await?.remove(0);
                Ok(Some(record))
            }
        }
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
        for chunk in requested.chunks(256) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let sql = format!(
                "SELECT ve.payload_bytes FROM `{vm}` vm JOIN `{ve}` ve ON ve.app_id = vm.app_id AND ve.channel = vm.channel AND ve.message_serial = vm.message_serial AND ve.version_serial = vm.latest_version_serial WHERE vm.app_id = ? AND vm.channel = ? AND vm.message_serial IN ({placeholders})",
                vm = self.tables.version_messages,
                ve = self.tables.version_entries
            );
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(app_id)
                .bind(channel);
            for serial in chunk {
                query = query.bind(serial.as_str());
            }
            let rows = query.fetch_all(&self.pool).await.map_err(|e| {
                Error::Internal(format!("failed to read latest version batch: {e}"))
            })?;
            for record in self
                .decode_records(
                    rows.into_iter()
                        .map(|row| row.get::<Vec<u8>, _>("payload_bytes"))
                        .collect(),
                )
                .await?
            {
                result.insert(record.message_serial().clone(), record);
            }
        }
        Ok(result)
    }

    async fn get_versions(&self, request: VersionStoreReadRequest) -> Result<VersionStorePage> {
        request.validate()?;
        let fetch_limit = (request.limit + 1) as i64;

        let (order_dir, cursor_op) = match request.direction {
            VersionStoreDirection::NewestFirst => ("DESC", "<"),
            VersionStoreDirection::OldestFirst => ("ASC", ">"),
        };

        let rows = if let Some(cursor) = &request.cursor {
            let sql = format!(
                "SELECT payload_bytes FROM `{}` WHERE app_id = ? AND channel = ? AND message_serial = ? AND version_serial {} ? ORDER BY version_serial {} LIMIT ?",
                self.tables.version_entries, cursor_op, order_dir
            );
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(&request.app_id)
                .bind(&request.channel)
                .bind(request.message_serial.as_str())
                .bind(cursor.version_serial.as_str())
                .bind(fetch_limit)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| Error::Internal(format!("Failed to query version history: {e}")))?
        } else {
            let sql = format!(
                "SELECT payload_bytes FROM `{}` WHERE app_id = ? AND channel = ? AND message_serial = ? ORDER BY version_serial {} LIMIT ?",
                self.tables.version_entries, order_dir
            );
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(&request.app_id)
                .bind(&request.channel)
                .bind(request.message_serial.as_str())
                .bind(fetch_limit)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| Error::Internal(format!("Failed to query version history: {e}")))?
        };

        let has_more = rows.len() > request.limit;
        let items = self
            .decode_records(
                rows.into_iter()
                    .take(request.limit)
                    .map(|row| row.get::<Vec<u8>, _>("payload_bytes"))
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
        let sql = format!(
            "SELECT payload_bytes FROM `{}` WHERE app_id = ? AND channel = ? AND delivery_serial > ? ORDER BY delivery_serial ASC LIMIT ?",
            self.tables.version_entries
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&request.app_id)
            .bind(&request.channel)
            .bind(request.after_delivery_serial as i64)
            .bind(request.limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to replay version entries: {e}")))?;

        self.decode_records(
            rows.into_iter()
                .map(|row| row.get::<Vec<u8>, _>("payload_bytes"))
                .collect(),
        )
        .await
    }

    async fn latest_by_history(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Vec<StoredVersionRecord>> {
        let sql = format!(
            r#"SELECT ve.payload_bytes
            FROM `{vm}` vm
            JOIN `{ve}` ve ON ve.app_id = vm.app_id
                AND ve.channel = vm.channel
                AND ve.message_serial = vm.message_serial
                AND ve.version_serial = vm.latest_version_serial
            WHERE vm.app_id = ? AND vm.channel = ?
            ORDER BY vm.history_serial ASC"#,
            vm = self.tables.version_messages,
            ve = self.tables.version_entries
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to query latest by history: {e}")))?;

        self.decode_records(
            rows.into_iter()
                .map(|row| row.get::<Vec<u8>, _>("payload_bytes"))
                .collect(),
        )
        .await
    }

    async fn message_count(&self, app_id: &str, channel: &str) -> Result<u64> {
        let sql = format!(
            "SELECT COUNT(*) FROM `{messages}` m JOIN `{entries}` e ON e.app_id=m.app_id AND e.channel=m.channel AND e.message_serial=m.message_serial AND e.version_serial=m.latest_version_serial WHERE m.app_id=? AND m.channel=?",
            messages = self.tables.version_messages,
            entries = self.tables.version_entries
        );
        let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to count version messages: {e}")))?;
        Ok(count as u64)
    }

    async fn active_stream_count(&self, app_id: &str, channel: &str) -> Result<usize> {
        let mut after = String::new();
        let mut count = 0usize;
        loop {
            // Metadata is authoritative only for the corresponding latest version.
            // Joining the retained entry keeps expiry semantics identical to reads.
            let sql = format!(
                "SELECT m.message_serial, m.latest_version_serial, m.state_version_serial, m.is_open_stream FROM {messages} m JOIN {entries} e ON e.app_id=m.app_id AND e.channel=m.channel AND e.message_serial=m.message_serial AND e.version_serial=m.latest_version_serial WHERE m.app_id = ? AND m.channel = ? AND m.message_serial > ? ORDER BY m.message_serial LIMIT 100",
                messages = self.tables.version_messages,
                entries = self.tables.version_entries
            );
            let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(app_id)
                .bind(channel)
                .bind(&after)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| {
                    Error::Internal(format!("failed to read active stream metadata: {e}"))
                })?;
            if rows.is_empty() {
                return Ok(count);
            }
            for row in rows {
                after = row.get("message_serial");
                let latest: String = row.get("latest_version_serial");
                let verified: Option<String> = row.get("state_version_serial");
                if verified.as_deref() == Some(latest.as_str()) {
                    count += usize::from(row.get::<bool, _>("is_open_stream"));
                    continue;
                }
                let serial = sockudo_core::versioned_messages::MessageSerial::new(after.clone())?;
                if let Some(record) = self.get_latest(app_id, channel, &serial).await? {
                    count += usize::from(record.is_open_ai_stream());
                    // Lazily repair old imported metadata, fenced against a new writer.
                    let update = format!(
                        "UPDATE `{}` SET is_open_stream=?, state_version_serial=? WHERE app_id=? AND channel=? AND message_serial=? AND latest_version_serial=?",
                        self.tables.version_messages
                    );
                    sqlx::query(sqlx::AssertSqlSafe(update.as_str()))
                        .bind(record.is_open_ai_stream())
                        .bind(record.version_serial().as_str())
                        .bind(app_id)
                        .bind(channel)
                        .bind(record.message_serial().as_str())
                        .bind(record.version_serial().as_str())
                        .execute(&self.pool)
                        .await
                        .map_err(|e| {
                            Error::Internal(format!(
                                "failed to refresh active stream metadata: {e}"
                            ))
                        })?;
                }
            }
        }
    }

    async fn stream_state(&self, app_id: &str, channel: &str) -> Result<VersionStreamState> {
        let sql = format!(
            "SELECT next_delivery_serial, oldest_available_delivery_serial, newest_available_delivery_serial FROM `{}` WHERE app_id = ? AND channel = ?",
            self.tables.version_streams
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to read version stream state: {e}")))?;

        match row {
            None => Ok(VersionStreamState::default()),
            Some(row) => Ok(VersionStreamState {
                stream_id: Some(format!("{}/{}", app_id, channel)),
                next_delivery_serial: Some(row.get::<i64, _>("next_delivery_serial") as u64),
                oldest_available_delivery_serial: row
                    .try_get::<Option<i64>, _>("oldest_available_delivery_serial")
                    .unwrap_or(None)
                    .map(|v| v as u64),
                newest_available_delivery_serial: row
                    .try_get::<Option<i64>, _>("newest_available_delivery_serial")
                    .unwrap_or(None)
                    .map(|v| v as u64),
            }),
        }
    }

    async fn purge_before(&self, before_ms: i64, batch_size: usize) -> Result<(u64, bool)> {
        if batch_size == 0 {
            return Ok((0, false));
        }
        let limit = batch_size as i64;

        // Delete entries first — the latest-version pointer in version_messages
        // is allowed to dangle briefly until the next purge tick picks it up.
        let entries_sql = format!(
            "DELETE FROM `{}` WHERE created_at_ms < ? ORDER BY created_at_ms ASC LIMIT ?",
            self.tables.version_entries
        );
        let entries_deleted = sqlx::query(sqlx::AssertSqlSafe(entries_sql.as_str()))
            .bind(before_ms)
            .bind(limit)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to purge version entries: {e}")))?
            .rows_affected();

        let messages_sql = format!(
            "DELETE FROM `{}` WHERE updated_at_ms < ? ORDER BY updated_at_ms ASC LIMIT ?",
            self.tables.version_messages
        );
        let messages_deleted = sqlx::query(sqlx::AssertSqlSafe(messages_sql.as_str()))
            .bind(before_ms)
            .bind(limit)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to purge version messages: {e}")))?
            .rows_affected();

        let snapshots_sql = format!(
            "DELETE FROM `{snapshots}` WHERE updated_at_ms < ? AND NOT EXISTS (SELECT 1 FROM `{entries}` e WHERE e.app_id=`{snapshots}`.app_id AND e.channel=`{snapshots}`.channel AND e.text_snapshot_key=`{snapshots}`.snapshot_key) ORDER BY updated_at_ms LIMIT ?",
            snapshots = self.text_table(),
            entries = self.tables.version_entries
        );
        let snapshots_deleted = sqlx::query(sqlx::AssertSqlSafe(snapshots_sql.as_str()))
            .bind(before_ms)
            .bind(limit)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to purge version text snapshots: {e}")))?
            .rows_affected();
        let deleted = entries_deleted + messages_deleted + snapshots_deleted;
        let has_more = entries_deleted as i64 == limit
            || messages_deleted as i64 == limit
            || snapshots_deleted as i64 == limit;
        Ok((deleted, has_more))
    }
}
