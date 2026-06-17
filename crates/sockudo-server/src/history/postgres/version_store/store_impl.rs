use super::*;

#[async_trait::async_trait]
impl VersionStore for PostgresVersionStore {
    async fn reserve_delivery_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<VersionWriteReservation> {
        let now_ms = sockudo_core::history::now_ms();
        let sql = format!(
            r#"
            INSERT INTO {t} (app_id, channel, next_delivery_serial, updated_at_ms)
            VALUES ($1, $2, 2, $3)
            ON CONFLICT (app_id, channel) DO UPDATE SET
                next_delivery_serial = {t}.next_delivery_serial + 1,
                updated_at_ms = EXCLUDED.updated_at_ms
            RETURNING next_delivery_serial - 1 AS reserved_serial
            "#,
            t = self.tables.version_streams
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .bind(now_ms)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to reserve version delivery position: {e}"))
            })?;

        Ok(VersionWriteReservation {
            stream_id: format!("{}/{}", app_id, channel),
            delivery_serial: row.get::<i64, _>("reserved_serial") as u64,
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
        let initial_next = block_size_i64.saturating_add(1);
        let sql = format!(
            r#"
            INSERT INTO {t} (app_id, channel, next_delivery_serial, updated_at_ms)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (app_id, channel) DO UPDATE SET
                next_delivery_serial = {t}.next_delivery_serial + $5,
                updated_at_ms = EXCLUDED.updated_at_ms
            RETURNING next_delivery_serial - $5 AS reserved_serial
            "#,
            t = self.tables.version_streams
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .bind(initial_next)
            .bind(now_ms)
            .bind(block_size_i64)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to reserve version delivery position block: {e}"
                ))
            })?;

        Ok(VersionWriteReservationBlock {
            stream_id: format!("{}/{}", app_id, channel),
            start_delivery_serial: row.get::<i64, _>("reserved_serial") as u64,
            len: block_size,
        })
    }

    async fn append_version(&self, record: StoredVersionRecord) -> Result<()> {
        let now_ms = sockudo_core::history::now_ms();
        let payload = sonic_rs::to_vec(&record)
            .map_err(|e| Error::Internal(format!("Failed to serialize version record: {e}")))?;
        let payload_size = payload.len() as i64;

        let insert_entry = format!(
            r#"
            INSERT INTO {t} (
                app_id, channel, message_serial, version_serial, delivery_serial, history_serial,
                action, client_id, description, event_name,
                payload_bytes, payload_size_bytes, version_timestamp_ms, created_at_ms
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (app_id, channel, message_serial, version_serial) DO NOTHING
            "#,
            t = self.tables.version_entries
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

        // Upsert version_messages: only advance if the incoming version_serial is lexicographically greater.
        let upsert_msg = format!(
            r#"
            INSERT INTO {t} (
                app_id, channel, message_serial, history_serial, original_client_id,
                latest_version_serial, latest_delivery_serial, latest_action,
                created_at_ms, updated_at_ms
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
            ON CONFLICT (app_id, channel, message_serial) DO UPDATE SET
                latest_version_serial = EXCLUDED.latest_version_serial,
                latest_delivery_serial = EXCLUDED.latest_delivery_serial,
                latest_action = EXCLUDED.latest_action,
                updated_at_ms = EXCLUDED.updated_at_ms
            WHERE {t}.latest_version_serial < EXCLUDED.latest_version_serial
            "#,
            t = self.tables.version_messages
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
            .bind(now_ms)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to upsert version message: {e}")))?;

        // Update stream delivery window.
        let update_stream = format!(
            r#"
            UPDATE {t} SET
                oldest_available_delivery_serial = CASE
                    WHEN oldest_available_delivery_serial IS NULL OR $3 < oldest_available_delivery_serial
                    THEN $3 ELSE oldest_available_delivery_serial END,
                newest_available_delivery_serial = CASE
                    WHEN newest_available_delivery_serial IS NULL OR $3 > newest_available_delivery_serial
                    THEN $3 ELSE newest_available_delivery_serial END,
                updated_at_ms = $4
            WHERE app_id = $1 AND channel = $2
            "#,
            t = self.tables.version_streams
        );
        sqlx::query(sqlx::AssertSqlSafe(update_stream.as_str()))
            .bind(&record.app_id)
            .bind(&record.channel)
            .bind(record.delivery_serial() as i64)
            .bind(now_ms)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to update version stream window: {e}")))?;

        Ok(())
    }

    async fn get_latest(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &sockudo_core::versioned_messages::MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        let sql = format!(
            r#"
            SELECT payload_bytes FROM {}
            WHERE app_id = $1 AND channel = $2 AND message_serial = $3
            ORDER BY version_serial DESC
            LIMIT 1
            "#,
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
                let record: StoredVersionRecord = sonic_rs::from_slice(&bytes).map_err(|e| {
                    Error::Internal(format!("Failed to deserialize version record: {e}"))
                })?;
                Ok(Some(record))
            }
        }
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
                "SELECT payload_bytes FROM {} WHERE app_id = $1 AND channel = $2 AND message_serial = $3 AND version_serial {} $4 ORDER BY version_serial {} LIMIT $5",
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
                "SELECT payload_bytes FROM {} WHERE app_id = $1 AND channel = $2 AND message_serial = $3 ORDER BY version_serial {} LIMIT $4",
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
        let items: Vec<StoredVersionRecord> = rows
            .into_iter()
            .take(request.limit)
            .map(|row| {
                let bytes: Vec<u8> = row.get("payload_bytes");
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
            r#"
            SELECT payload_bytes FROM {}
            WHERE app_id = $1 AND channel = $2 AND delivery_serial > $3
            ORDER BY delivery_serial ASC
            LIMIT $4
            "#,
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

        rows.into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row.get("payload_bytes");
                sonic_rs::from_slice(&bytes)
                    .map_err(|e| Error::Internal(format!("Failed to deserialize version: {e}")))
            })
            .collect()
    }

    async fn latest_by_history(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Vec<StoredVersionRecord>> {
        let sql = format!(
            r#"
            SELECT ve.payload_bytes
            FROM {vm} vm
            JOIN {ve} ve ON ve.app_id = vm.app_id
                AND ve.channel = vm.channel
                AND ve.message_serial = vm.message_serial
                AND ve.version_serial = vm.latest_version_serial
            WHERE vm.app_id = $1 AND vm.channel = $2
            ORDER BY vm.history_serial ASC
            "#,
            vm = self.tables.version_messages,
            ve = self.tables.version_entries
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(app_id)
            .bind(channel)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to query latest by history: {e}")))?;

        rows.into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row.get("payload_bytes");
                sonic_rs::from_slice(&bytes)
                    .map_err(|e| Error::Internal(format!("Failed to deserialize version: {e}")))
            })
            .collect()
    }

    async fn stream_state(&self, app_id: &str, channel: &str) -> Result<VersionStreamState> {
        let sql = format!(
            "SELECT next_delivery_serial, oldest_available_delivery_serial, newest_available_delivery_serial FROM {} WHERE app_id = $1 AND channel = $2",
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

        let entries_sql = format!(
            "DELETE FROM {0} WHERE ctid IN (SELECT ctid FROM {0} WHERE created_at_ms < $1 ORDER BY created_at_ms ASC LIMIT $2)",
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
            "DELETE FROM {0} WHERE ctid IN (SELECT ctid FROM {0} WHERE updated_at_ms < $1 ORDER BY updated_at_ms ASC LIMIT $2)",
            self.tables.version_messages
        );
        let messages_deleted = sqlx::query(sqlx::AssertSqlSafe(messages_sql.as_str()))
            .bind(before_ms)
            .bind(limit)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("Failed to purge version messages: {e}")))?
            .rows_affected();

        let deleted = entries_deleted + messages_deleted;
        let has_more = entries_deleted as i64 == limit || messages_deleted as i64 == limit;
        Ok((deleted, has_more))
    }
}
