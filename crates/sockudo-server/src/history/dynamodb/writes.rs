use super::{DynamoDbHistoryStore, StoredEntryRecord, StoredStreamRecord};
use aws_sdk_dynamodb::types::{Delete, Put, TransactWriteItem, Update};
use sockudo_core::error::{Error, Result};
use sockudo_core::history::{HistoryAppendRecord, HistoryRetentionStats};

impl DynamoDbHistoryStore {
    async fn accounted_state(&self, current: &StoredStreamRecord) -> Result<StoredStreamRecord> {
        let mut next = current.clone();
        if current.retention_revision == 0 {
            let mut key = None;
            let mut count = 0;
            let mut bytes = 0;
            loop {
                let page = self
                    .client
                    .query()
                    .table_name(&self.tables.entries)
                    .key_condition_expression("stream_partition=:partition")
                    .expression_attribute_values(
                        ":partition",
                        Self::attr_string(&Self::stream_partition(
                            &current.app_id,
                            &current.channel,
                            &current.stream_id,
                        )),
                    )
                    .projection_expression("payload_size_bytes")
                    .consistent_read(true)
                    .set_exclusive_start_key(key)
                    .send()
                    .await
                    .map_err(|e| {
                        Error::Internal(format!(
                            "Failed to reconcile legacy DynamoDB history accounting: {e}"
                        ))
                    })?;
                for item in page.items() {
                    count += 1;
                    bytes += Self::item_attr_u64(item, "payload_size_bytes")?;
                }
                key = page.last_evaluated_key;
                if key.is_none() {
                    break;
                }
            }
            next.retained_messages = count;
            next.retained_bytes = bytes;
        }
        Ok(next)
    }

    fn retention_update(
        &self,
        app_id: &str,
        channel: &str,
        current: &StoredStreamRecord,
        next: &StoredStreamRecord,
    ) -> Result<Update> {
        let mut update = Update::builder().table_name(&self.tables.streams)
            .key("stream_key", Self::attr_string(&Self::stream_key(app_id, channel)))
            .condition_expression("stream_id=:stream AND next_serial=:expected_next AND (attribute_not_exists(retention_revision) OR retention_revision=:revision)")
            .expression_attribute_values(":stream", Self::attr_string(&current.stream_id))
            .expression_attribute_values(":expected_next", Self::attr_number(current.next_serial))
            .expression_attribute_values(":revision", Self::attr_number(current.retention_revision))
            .expression_attribute_values(":new_revision", Self::attr_number(current.retention_revision + 1))
            .expression_attribute_values(":count", Self::attr_number(next.retained_messages))
            .expression_attribute_values(":bytes", Self::attr_number(next.retained_bytes))
            .expression_attribute_values(":next", Self::attr_number(next.next_serial))
            .expression_attribute_values(":now", Self::attr_number(next.updated_at_ms));
        let mut set = "retention_revision=:new_revision, retained_messages=:count, retained_bytes=:bytes, next_serial=:next, updated_at_ms=:now".to_string();
        let mut remove = Vec::new();
        for (field, value) in [
            (
                "oldest_available_serial",
                next.oldest_available_serial.map(Self::attr_number),
            ),
            (
                "newest_available_serial",
                next.newest_available_serial.map(Self::attr_number),
            ),
            (
                "oldest_available_published_at_ms",
                next.oldest_available_published_at_ms.map(Self::attr_number),
            ),
            (
                "newest_available_published_at_ms",
                next.newest_available_published_at_ms.map(Self::attr_number),
            ),
        ] {
            if let Some(value) = value {
                let variable = format!(":{field}");
                set.push_str(&format!(", {field}={variable}"));
                update = update.expression_attribute_values(variable, value);
            } else {
                remove.push(field);
            }
        }
        let expression = if remove.is_empty() {
            format!("SET {set}")
        } else {
            format!("SET {set} REMOVE {}", remove.join(","))
        };
        update
            .update_expression(expression)
            .build()
            .map_err(|e| Error::Internal(format!("Failed to build DynamoDB retention update: {e}")))
    }

    // All entry removals and their count/byte deltas commit together. A changed
    // revision or missing entry retries from fresh metadata, never subtracts twice.
    async fn delete_retention_batch(
        &self,
        app_id: &str,
        channel: &str,
        current: &StoredStreamRecord,
        rows: &[StoredEntryRecord],
    ) -> Result<bool> {
        let mut next = self.accounted_state(&current).await?;
        next.retained_messages = next.retained_messages.saturating_sub(rows.len() as u64);
        next.retained_bytes = next
            .retained_bytes
            .saturating_sub(rows.iter().map(|row| row.payload_size_bytes).sum::<u64>());
        let update = self.retention_update(app_id, channel, current, &next)?;
        let mut transaction = self
            .client
            .transact_write_items()
            .transact_items(TransactWriteItem::builder().update(update).build());
        for row in rows {
            let delete = Delete::builder()
                .table_name(&self.tables.entries)
                .key(
                    "stream_partition",
                    Self::attr_string(&Self::stream_partition(app_id, channel, &current.stream_id)),
                )
                .key(
                    "serial_key",
                    Self::attr_string(&Self::serial_key(row.serial)),
                )
                .condition_expression("payload_size_bytes=:size")
                .expression_attribute_values(":size", Self::attr_number(row.payload_size_bytes))
                .build()
                .map_err(|e| {
                    Error::Internal(format!("Failed to build DynamoDB retention delete: {e}"))
                })?;
            transaction =
                transaction.transact_items(TransactWriteItem::builder().delete(delete).build());
        }
        match transaction.send().await {
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_transaction_canceled_exception()) =>
            {
                Ok(false)
            }
            Err(error) => Err(Error::Internal(format!(
                "Failed to delete DynamoDB retention batch: {error}"
            ))),
        }
    }

    pub(super) async fn delete_entries(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        rows: &[StoredEntryRecord],
    ) -> Result<()> {
        for chunk in rows.chunks(25) {
            let mut done = false;
            for _ in 0..64 {
                let current = self
                    .load_stream_raw(app_id, channel)
                    .await?
                    .ok_or_else(|| Error::Internal("Missing DynamoDB history stream".into()))?;
                if current.stream_id != stream_id {
                    return Err(Error::InvalidMessageFormat(
                        "History stream changed during purge".into(),
                    ));
                }
                let mut present = Vec::new();
                for row in chunk {
                    let response = self
                        .client
                        .get_item()
                        .table_name(&self.tables.entries)
                        .key(
                            "stream_partition",
                            Self::attr_string(&Self::stream_partition(app_id, channel, stream_id)),
                        )
                        .key(
                            "serial_key",
                            Self::attr_string(&Self::serial_key(row.serial)),
                        )
                        .consistent_read(true)
                        .projection_expression("payload_size_bytes")
                        .send()
                        .await
                        .map_err(|e| {
                            Error::Internal(format!("Failed to check DynamoDB purge row: {e}"))
                        })?;
                    if response.item.is_some() {
                        present.push(row.clone());
                    }
                }
                if present.is_empty()
                    || self
                        .delete_retention_batch(app_id, channel, &current, &present)
                        .await?
                {
                    done = true;
                    break;
                }
                tokio::task::yield_now().await;
            }
            if !done {
                return Err(Error::Internal(
                    "DynamoDB history purge contention limit reached".into(),
                ));
            }
        }
        self.refresh_retention_bounds(app_id, channel, stream_id)
            .await?;
        Ok(())
    }

    pub(super) async fn upsert_stream_raw(
        &self,
        app_id: &str,
        channel: &str,
        record: &StoredStreamRecord,
    ) -> Result<()> {
        let mut next = record.clone();
        let latest = self.load_stream_raw(app_id, channel).await?;
        if let Some(latest) = &latest {
            if latest.stream_id == record.stream_id {
                next.next_serial = next.next_serial.max(latest.next_serial);
            }
        }
        next.retention_revision += 1;
        self.client
            .put_item()
            .table_name(&self.tables.streams)
            .set_item(Some(Self::stream_item(
                &Self::stream_key(app_id, channel),
                &next,
            )))
            .condition_expression(
                "(attribute_not_exists(retention_revision) OR retention_revision=:expected) AND (attribute_not_exists(next_serial) OR next_serial=:expected_next)",
            )
            .expression_attribute_values(":expected", Self::attr_number(record.retention_revision))
            .expression_attribute_values(":expected_next",Self::attr_number(latest.map_or(record.next_serial,|state|state.next_serial)))
            .send()
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "Failed to conditionally update DynamoDB history stream: {e}"
                ))
            })?;
        Ok(())
    }

    pub(super) async fn refresh_retention_bounds(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
    ) -> Result<HistoryRetentionStats> {
        for _ in 0..64 {
            let current = self
                .load_stream_raw(app_id, channel)
                .await?
                .ok_or_else(|| Error::Internal("Missing DynamoDB history stream".into()))?;
            if current.stream_id != stream_id {
                return Err(Error::InvalidMessageFormat(
                    "History stream changed during retention".into(),
                ));
            }
            let oldest = self
                .query_retention_prefix(app_id, channel, stream_id, false)
                .await?;
            let newest = self
                .query_retention_prefix(app_id, channel, stream_id, true)
                .await?;
            let mut next = self.accounted_state(&current).await?;
            next.oldest_available_serial = oldest.first().map(|row| row.serial);
            next.newest_available_serial = newest.first().map(|row| row.serial);
            next.oldest_available_published_at_ms = oldest.first().map(|row| row.published_at_ms);
            next.newest_available_published_at_ms = newest.first().map(|row| row.published_at_ms);
            if current.retention_revision != 0
                && current.retained_messages == next.retained_messages
                && current.retained_bytes == next.retained_bytes
                && current.oldest_available_serial == next.oldest_available_serial
                && current.newest_available_serial == next.newest_available_serial
                && current.oldest_available_published_at_ms == next.oldest_available_published_at_ms
                && current.newest_available_published_at_ms == next.newest_available_published_at_ms
            {
                return Ok(HistoryRetentionStats {
                    stream_id: Some(stream_id.to_string()),
                    retained_messages: current.retained_messages,
                    retained_bytes: current.retained_bytes,
                    oldest_serial: current.oldest_available_serial,
                    newest_serial: current.newest_available_serial,
                    oldest_published_at_ms: current.oldest_available_published_at_ms,
                    newest_published_at_ms: current.newest_available_published_at_ms,
                });
            }
            let update = self.retention_update(app_id, channel, &current, &next)?;
            match self
                .client
                .transact_write_items()
                .transact_items(TransactWriteItem::builder().update(update).build())
                .send()
                .await
            {
                Ok(_) => {
                    return Ok(HistoryRetentionStats {
                        stream_id: Some(stream_id.to_string()),
                        retained_messages: next.retained_messages,
                        retained_bytes: next.retained_bytes,
                        oldest_serial: next.oldest_available_serial,
                        newest_serial: next.newest_available_serial,
                        oldest_published_at_ms: next.oldest_available_published_at_ms,
                        newest_published_at_ms: next.newest_available_published_at_ms,
                    });
                }
                Err(error)
                    if error
                        .as_service_error()
                        .is_some_and(|error| error.is_transaction_canceled_exception()) =>
                {
                    tokio::task::yield_now().await
                }
                Err(error) => {
                    return Err(Error::Internal(format!(
                        "Failed to refresh DynamoDB retention bounds: {error}"
                    )));
                }
            }
        }
        Err(Error::Internal(
            "DynamoDB retention bounds contention limit reached".into(),
        ))
    }

    pub(super) async fn persist_record(&self, record: &HistoryAppendRecord) -> Result<()> {
        let stored = StoredEntryRecord {
            app_id: record.app_id.clone(),
            channel: record.channel.clone(),
            stream_id: record.stream_id.clone(),
            serial: record.serial,
            published_at_ms: record.published_at_ms,
            message_id: record.message_id.clone(),
            event_name: record.event_name.clone(),
            operation_kind: record.operation_kind.clone(),
            payload_bytes: record.payload_bytes.to_vec(),
            payload_size_bytes: record.payload_bytes.len() as u64,
        };
        let partition = Self::stream_partition(&record.app_id, &record.channel, &record.stream_id);
        let mut inserted = false;
        for _ in 0..64 {
            let current = self
                .load_stream_raw(&record.app_id, &record.channel)
                .await?
                .ok_or_else(|| Error::Internal("Missing DynamoDB history stream".into()))?;
            if current.stream_id != record.stream_id {
                return Err(Error::InvalidMessageFormat(
                    "History append stream changed".into(),
                ));
            }
            let mut next = self.accounted_state(&current).await?;

            next.retained_messages += 1;
            next.retained_bytes += record.payload_bytes.len() as u64;
            next.next_serial = next.next_serial.max(record.serial.saturating_add(1));
            next.updated_at_ms = record.published_at_ms;
            let update = self.retention_update(&record.app_id, &record.channel, &current, &next)?;
            let put = Put::builder()
                .table_name(&self.tables.entries)
                .set_item(Some(Self::entry_item(&partition, &stored)))
                .condition_expression(
                    "attribute_not_exists(stream_partition) AND attribute_not_exists(serial_key)",
                )
                .build()
                .map_err(|e| {
                    Error::Internal(format!("Failed to build DynamoDB history append: {e}"))
                })?;
            match self
                .client
                .transact_write_items()
                .transact_items(TransactWriteItem::builder().put(put).build())
                .transact_items(TransactWriteItem::builder().update(update).build())
                .send()
                .await
            {
                Ok(_) => {
                    inserted = true;
                    break;
                }
                Err(error)
                    if error
                        .as_service_error()
                        .is_some_and(|error| error.is_transaction_canceled_exception()) =>
                {
                    let existing = self
                        .client
                        .get_item()
                        .table_name(&self.tables.entries)
                        .key("stream_partition", Self::attr_string(&partition))
                        .key(
                            "serial_key",
                            Self::attr_string(&Self::serial_key(record.serial)),
                        )
                        .consistent_read(true)
                        .send()
                        .await
                        .map_err(|e| {
                            Error::Internal(format!(
                                "Failed to inspect duplicate history append: {e}"
                            ))
                        })?;
                    if let Some(item) = existing.item {
                        let existing = Self::entry_from_item(item)?;
                        if existing.payload_bytes != stored.payload_bytes
                            || existing.message_id != stored.message_id
                            || existing.published_at_ms != stored.published_at_ms
                            || existing.event_name != stored.event_name
                            || existing.operation_kind != stored.operation_kind
                        {
                            return Err(Error::InvalidMessageFormat(
                                "Conflicting DynamoDB history append at existing serial".into(),
                            ));
                        }
                        inserted = true;
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                Err(error) => {
                    return Err(Error::Internal(format!(
                        "Failed to append DynamoDB history transaction: {error}"
                    )));
                }
            }
        }
        if !inserted {
            return Err(Error::Internal(
                "DynamoDB history append contention limit reached".into(),
            ));
        }
        let cutoff = record.published_at_ms.saturating_sub(
            record
                .retention
                .retention_window_seconds
                .saturating_mul(1000)
                .min(i64::MAX as u64) as i64,
        );
        let mut age_prefix_complete = false;
        let mut evicted_messages = 0;
        let mut evicted_bytes = 0;
        let mut conflicts = 0;
        loop {
            let current = self
                .load_stream_raw(&record.app_id, &record.channel)
                .await?
                .ok_or_else(|| Error::Internal("Missing DynamoDB history stream".into()))?;
            if current.stream_id != record.stream_id {
                return Err(Error::InvalidMessageFormat(
                    "History stream changed during retention".into(),
                ));
            }
            let rows = self
                .query_retention_prefix(&record.app_id, &record.channel, &record.stream_id, false)
                .await?;
            let mut count = current.retained_messages;
            let mut bytes = current.retained_bytes;
            let mut remove = 0;
            let mut next_age_prefix_complete = age_prefix_complete;
            for row in &rows {
                if !next_age_prefix_complete && row.published_at_ms >= cutoff {
                    next_age_prefix_complete = true;
                }
                if next_age_prefix_complete
                    && !record
                        .retention
                        .max_messages_per_channel
                        .is_some_and(|limit| count > limit as u64)
                    && !record
                        .retention
                        .max_bytes_per_channel
                        .is_some_and(|limit| bytes > limit)
                {
                    break;
                }
                count = count.saturating_sub(1);
                bytes = bytes.saturating_sub(row.payload_size_bytes);
                remove += 1;
            }
            if remove == 0 {
                break;
            }
            if self
                .delete_retention_batch(&record.app_id, &record.channel, &current, &rows[..remove])
                .await?
            {
                age_prefix_complete = next_age_prefix_complete;
                evicted_messages += remove as u64;
                evicted_bytes += rows[..remove]
                    .iter()
                    .map(|row| row.payload_size_bytes)
                    .sum::<u64>();
                conflicts = 0;
            } else {
                conflicts += 1;
                if conflicts >= 64 {
                    return Err(Error::Internal(
                        "DynamoDB retention contention limit reached".into(),
                    ));
                }
                tokio::task::yield_now().await;
            }
        }
        let retained = self
            .refresh_retention_bounds(&record.app_id, &record.channel, &record.stream_id)
            .await?;
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.update_history_retained(
                &record.app_id,
                retained.retained_messages,
                retained.retained_bytes,
            );
            if evicted_messages > 0 {
                metrics.mark_history_eviction(&record.app_id, evicted_messages, evicted_bytes);
            }
        }
        Ok(())
    }
}
