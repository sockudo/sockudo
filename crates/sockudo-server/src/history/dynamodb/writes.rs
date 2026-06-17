use sockudo_core::error::{Error, Result};
use sockudo_core::history::{HistoryAppendRecord, HistoryDirection, HistoryRetentionStats};

use super::{DynamoDbHistoryStore, StoredEntryRecord, StoredStreamRecord};

impl DynamoDbHistoryStore {
    pub(super) async fn delete_entries(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        rows: &[StoredEntryRecord],
    ) -> Result<()> {
        for row in rows {
            self.client
                .delete_item()
                .table_name(&self.tables.entries)
                .key(
                    "stream_partition",
                    Self::attr_string(&Self::stream_partition(app_id, channel, stream_id)),
                )
                .key(
                    "serial_key",
                    Self::attr_string(&Self::serial_key(row.serial)),
                )
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to delete DynamoDB history row: {e}"))
                })?;
        }
        Ok(())
    }

    pub(super) async fn upsert_stream_raw(
        &self,
        app_id: &str,
        channel: &str,
        record: &StoredStreamRecord,
    ) -> Result<()> {
        self.client
            .put_item()
            .table_name(&self.tables.streams)
            .set_item(Some(Self::stream_item(
                &Self::stream_key(app_id, channel),
                record,
            )))
            .send()
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to upsert DynamoDB history stream: {e}"))
            })?;
        Ok(())
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
        self.client
            .put_item()
            .table_name(&self.tables.entries)
            .set_item(Some(Self::entry_item(&partition, &stored)))
            .condition_expression(
                "attribute_not_exists(stream_partition) AND attribute_not_exists(serial_key)",
            )
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create DynamoDB history row: {e}")))?;

        let mut rows = self
            .query_entries(
                &record.app_id,
                &record.channel,
                &record.stream_id,
                HistoryDirection::OldestFirst,
            )
            .await?;
        let cutoff_ms = record
            .published_at_ms
            .saturating_sub((record.retention.retention_window_seconds * 1000) as i64);
        let mut to_delete = Vec::new();

        while let Some(first) = rows.first() {
            if first.published_at_ms < cutoff_ms {
                to_delete.push(rows.remove(0));
            } else {
                break;
            }
        }
        if let Some(max_messages) = record.retention.max_messages_per_channel {
            while rows.len() > max_messages {
                to_delete.push(rows.remove(0));
            }
        }
        if let Some(max_bytes) = record.retention.max_bytes_per_channel {
            let mut retained_bytes: u64 = rows.iter().map(|row| row.payload_size_bytes).sum();
            while retained_bytes > max_bytes && !rows.is_empty() {
                let removed = rows.remove(0);
                retained_bytes = retained_bytes.saturating_sub(removed.payload_size_bytes);
                to_delete.push(removed);
            }
        }
        self.delete_entries(
            &record.app_id,
            &record.channel,
            &record.stream_id,
            &to_delete,
        )
        .await?;

        if let Some(current) = self
            .load_stream_raw(&record.app_id, &record.channel)
            .await?
            && current.next_serial < record.serial.saturating_add(1)
        {
            let _ = self
                .client
                .update_item()
                .table_name(&self.tables.streams)
                .key(
                    "stream_key",
                    Self::attr_string(&Self::stream_key(&record.app_id, &record.channel)),
                )
                .condition_expression("#next_serial < :floor")
                .update_expression("SET #next_serial = :floor, updated_at_ms = :updated_at_ms")
                .expression_attribute_names("#next_serial", "next_serial")
                .expression_attribute_values(
                    ":floor",
                    Self::attr_number(record.serial.saturating_add(1)),
                )
                .expression_attribute_values(
                    ":updated_at_ms",
                    Self::attr_number(record.published_at_ms),
                )
                .send()
                .await;
        }

        let retained = HistoryRetentionStats {
            stream_id: Some(record.stream_id.clone()),
            retained_messages: rows.len() as u64,
            retained_bytes: rows.iter().map(|row| row.payload_size_bytes).sum(),
            oldest_serial: rows.first().map(|row| row.serial),
            newest_serial: rows.last().map(|row| row.serial),
            oldest_published_at_ms: rows.first().map(|row| row.published_at_ms),
            newest_published_at_ms: rows.last().map(|row| row.published_at_ms),
        };
        if let Some(current) = self
            .load_stream_raw(&record.app_id, &record.channel)
            .await?
        {
            let updated = StoredStreamRecord {
                next_serial: current.next_serial.max(record.serial.saturating_add(1)),
                retained_messages: retained.retained_messages,
                retained_bytes: retained.retained_bytes,
                oldest_available_serial: retained.oldest_serial,
                newest_available_serial: retained.newest_serial,
                oldest_available_published_at_ms: retained.oldest_published_at_ms,
                newest_available_published_at_ms: retained.newest_published_at_ms,
                updated_at_ms: record.published_at_ms,
                ..current
            };
            self.upsert_stream_raw(&record.app_id, &record.channel, &updated)
                .await?;
        }
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.update_history_retained(
                &record.app_id,
                retained.retained_messages,
                retained.retained_bytes,
            );
            if !to_delete.is_empty() {
                let evicted_bytes = to_delete.iter().map(|row| row.payload_size_bytes).sum();
                metrics.mark_history_eviction(
                    &record.app_id,
                    to_delete.len() as u64,
                    evicted_bytes,
                );
            }
        }
        Ok(())
    }
}
