use sockudo_core::error::{Error, Result};
use sockudo_core::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDirection, HistoryDurableState, HistoryItem,
    HistoryPage, HistoryPurgeMode, HistoryPurgeRequest, HistoryPurgeResult, HistoryQueryBounds,
    HistoryReadRequest, HistoryResetResult, HistoryRetentionStats, HistoryRuntimeStatus,
    HistoryStore, HistoryStreamInspection, HistoryStreamRuntimeState, HistoryWriteReservation,
};
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::info;

use super::degraded::{
    DegradeRequest, degraded_cache_key, degraded_channel_key, mark_channel_degraded,
    refresh_history_state_metrics,
};
use super::{DynamoDbHistoryStore, StoredEntryRecord, StoredStreamRecord};

#[async_trait::async_trait]
impl HistoryStore for DynamoDbHistoryStore {
    async fn reserve_publish_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryWriteReservation> {
        let stream_key = Self::stream_key(app_id, channel);
        loop {
            if let Some(existing) = self.load_stream_raw(app_id, channel).await? {
                let now_ms = sockudo_core::history::now_ms();
                let result = self
                    .client
                    .update_item()
                    .table_name(&self.tables.streams)
                    .key("stream_key", Self::attr_string(&stream_key))
                    .condition_expression("#next_serial = :expected")
                    .update_expression(
                        "SET #next_serial = :next_serial, updated_at_ms = :updated_at_ms",
                    )
                    .expression_attribute_names("#next_serial", "next_serial")
                    .expression_attribute_values(
                        ":expected",
                        Self::attr_number(existing.next_serial),
                    )
                    .expression_attribute_values(
                        ":next_serial",
                        Self::attr_number(existing.next_serial + 1),
                    )
                    .expression_attribute_values(":updated_at_ms", Self::attr_number(now_ms))
                    .send()
                    .await;
                match result {
                    Ok(_) => {
                        return Ok(HistoryWriteReservation {
                            stream_id: existing.stream_id,
                            serial: existing.next_serial,
                        });
                    }
                    Err(err) => {
                        if err.to_string().contains("ConditionalCheckFailed") {
                            continue;
                        }
                        return Err(Error::Internal(format!(
                            "Failed to advance DynamoDB history serial: {err}"
                        )));
                    }
                }
            }

            let now_ms = sockudo_core::history::now_ms();
            let stream = StoredStreamRecord {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                stream_id: uuid::Uuid::new_v4().to_string(),
                next_serial: 2,
                retained_messages: 0,
                retained_bytes: 0,
                oldest_available_serial: None,
                newest_available_serial: None,
                oldest_available_published_at_ms: None,
                newest_available_published_at_ms: None,
                durable_state: HistoryDurableState::Healthy,
                durable_state_reason: None,
                durable_state_node_id: None,
                durable_state_changed_at_ms: None,
                updated_at_ms: now_ms,
            };
            let create_result = self
                .client
                .put_item()
                .table_name(&self.tables.streams)
                .set_item(Some(Self::stream_item(&stream_key, &stream)))
                .condition_expression("attribute_not_exists(stream_key)")
                .send()
                .await;
            match create_result {
                Ok(_) => {
                    return Ok(HistoryWriteReservation {
                        stream_id: stream.stream_id,
                        serial: 1,
                    });
                }
                Err(err) => {
                    if err.to_string().contains("ConditionalCheckFailed") {
                        continue;
                    }
                    return Err(Error::Internal(format!(
                        "Failed to create DynamoDB history stream row: {err}"
                    )));
                }
            }
        }
    }

    async fn append(&self, record: HistoryAppendRecord) -> Result<()> {
        let started = Instant::now();
        if let Err(err) = self.persist_record(&record).await {
            mark_channel_degraded(
                &self.client,
                &self.tables,
                &self.degraded_channels,
                self.cache_manager.as_ref(),
                self.metrics.as_deref(),
                DegradeRequest {
                    app_id: &record.app_id,
                    channel: &record.channel,
                    reason: "durable_history_write_failed",
                    node_id: None,
                },
            )
            .await;
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.mark_history_write_failure(&record.app_id);
            }
            return Err(err);
        }
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.mark_history_write(&record.app_id);
            metrics.track_history_write_latency(
                &record.app_id,
                started.elapsed().as_secs_f64() * 1000.0,
            );
        }
        Ok(())
    }

    async fn read_page(&self, request: HistoryReadRequest) -> Result<HistoryPage> {
        request.validate()?;
        let stream_runtime = self
            .resolved_stream_runtime_state(&request.app_id, &request.channel)
            .await?;
        if !stream_runtime.recovery_allowed {
            return Err(Error::Internal(format!(
                "History stream state blocks cold reads for {}/{}: {}",
                request.app_id,
                request.channel,
                stream_runtime
                    .reason
                    .unwrap_or_else(|| stream_runtime.durable_state.as_str().to_string())
            )));
        }

        let retained = self
            .retained_stats(&request.app_id, &request.channel)
            .await?;
        if let Some(cursor) = request.cursor.as_ref() {
            if let Some(stream_id) = retained.stream_id.as_ref()
                && cursor.stream_id != *stream_id
            {
                return Err(Error::InvalidMessageFormat(
                    "Expired history cursor: channel stream changed".to_string(),
                ));
            }
            if let Some(oldest_serial) = retained.oldest_serial
                && cursor.serial < oldest_serial
            {
                return Err(Error::InvalidMessageFormat(
                    "Expired history cursor: cursor points before retained history".to_string(),
                ));
            }
        }

        let Some(stream_id) = retained.stream_id.as_deref() else {
            return Ok(HistoryPage {
                items: Vec::new(),
                next_cursor: None,
                retained,
                has_more: false,
                complete: true,
                truncated_by_retention: false,
            });
        };

        let rows = self.query_page_entries(&request, stream_id).await?;
        let filtered: Vec<HistoryItem> = rows
            .into_iter()
            .map(|row| HistoryItem {
                stream_id: row.stream_id,
                serial: row.serial,
                published_at_ms: row.published_at_ms,
                message_id: row.message_id,
                event_name: row.event_name,
                operation_kind: row.operation_kind,
                payload_size_bytes: row.payload_size_bytes as usize,
                payload_bytes: row.payload_bytes.into(),
            })
            .collect();
        let has_more = filtered.len() > request.limit;
        let items: Vec<HistoryItem> = filtered.into_iter().take(request.limit).collect();
        let next_cursor = if has_more {
            items.last().map(|item| HistoryCursor {
                version: 1,
                app_id: request.app_id.clone(),
                channel: request.channel.clone(),
                stream_id: item.stream_id.clone(),
                serial: item.serial,
                direction: request.direction,
                bounds: request.bounds.clone(),
            })
        } else {
            None
        };
        let truncated_by_retention = is_truncated_by_retention(&request.bounds, &retained);
        Ok(HistoryPage {
            items,
            next_cursor,
            retained,
            has_more,
            complete: !has_more && !truncated_by_retention,
            truncated_by_retention,
        })
    }

    async fn runtime_status(&self) -> Result<HistoryRuntimeStatus> {
        let mut degraded_channels = 0usize;
        let mut reset_required_channels = 0usize;
        let mut start_key = None;
        loop {
            let response = self
                .client
                .scan()
                .table_name(&self.tables.streams)
                .set_exclusive_start_key(start_key)
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to scan DynamoDB history runtime status: {e}"
                    ))
                })?;
            for item in response.items() {
                let stream = Self::stream_from_item(item.clone())?;
                if stream.durable_state != HistoryDurableState::Healthy {
                    degraded_channels += 1;
                }
                if stream.durable_state == HistoryDurableState::ResetRequired {
                    reset_required_channels += 1;
                }
            }
            start_key = response.last_evaluated_key;
            if start_key.is_none() {
                break;
            }
        }
        Ok(HistoryRuntimeStatus {
            enabled: true,
            backend: "dynamodb".to_string(),
            state_authority: "durable_store".to_string(),
            degraded_channels,
            reset_required_channels,
            queue_depth: self.queue_depth_total.load(Ordering::Relaxed),
        })
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamRuntimeState> {
        self.resolved_stream_runtime_state(app_id, channel).await
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamInspection> {
        self.resolved_stream_inspection(app_id, channel).await
    }

    async fn reset_stream(
        &self,
        app_id: &str,
        channel: &str,
        reason: &str,
        requested_by: Option<&str>,
    ) -> Result<HistoryResetResult> {
        if reason.trim().is_empty() {
            return Err(Error::InvalidMessageFormat(
                "Reset reason must not be empty".to_string(),
            ));
        }
        let inspection_before = self.resolved_stream_inspection(app_id, channel).await?;
        let previous_stream_id = inspection_before.stream_id.clone();
        let mut purged_messages = 0u64;
        let mut purged_bytes = 0u64;
        if let Some(stream_id) = previous_stream_id.as_deref() {
            let entries = self
                .query_entries(app_id, channel, stream_id, HistoryDirection::OldestFirst)
                .await?;
            purged_messages = entries.len() as u64;
            purged_bytes = entries.iter().map(|row| row.payload_size_bytes).sum();
            self.delete_entries(app_id, channel, stream_id, &entries)
                .await?;
        }

        let now_ms = sockudo_core::history::now_ms();
        let new_stream = StoredStreamRecord {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: uuid::Uuid::new_v4().to_string(),
            next_serial: 1,
            retained_messages: 0,
            retained_bytes: 0,
            oldest_available_serial: None,
            newest_available_serial: None,
            oldest_available_published_at_ms: None,
            newest_available_published_at_ms: None,
            durable_state: HistoryDurableState::Healthy,
            durable_state_reason: None,
            durable_state_node_id: None,
            durable_state_changed_at_ms: Some(now_ms),
            updated_at_ms: now_ms,
        };
        self.upsert_stream_raw(app_id, channel, &new_stream).await?;
        self.degraded_channels
            .remove(&degraded_channel_key(app_id, channel));
        if let Some(cache) = self.cache_manager.as_ref() {
            let _ = cache.remove(&degraded_cache_key(app_id, channel)).await;
        }
        if let Some(metrics) = self.metrics.as_deref() {
            let _ =
                refresh_history_state_metrics(&self.client, &self.tables, metrics, app_id).await;
        }
        info!(
            app_id = %app_id,
            channel = %channel,
            previous_stream_id = previous_stream_id.as_deref().unwrap_or(""),
            new_stream_id = %new_stream.stream_id,
            purged_messages,
            purged_bytes,
            reason = %reason,
            requested_by = requested_by.unwrap_or(""),
            "operator reset durable history stream"
        );
        let inspection = self.resolved_stream_inspection(app_id, channel).await?;
        Ok(HistoryResetResult {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            previous_stream_id,
            new_stream_id: new_stream.stream_id,
            purged_messages,
            purged_bytes,
            inspection,
        })
    }

    async fn purge_stream(
        &self,
        app_id: &str,
        channel: &str,
        request: HistoryPurgeRequest,
    ) -> Result<HistoryPurgeResult> {
        request.validate()?;
        let inspection_before = self.resolved_stream_inspection(app_id, channel).await?;
        let mut purged_messages = 0u64;
        let mut purged_bytes = 0u64;
        if let Some(stream_id) = inspection_before.stream_id.as_deref() {
            let entries = self
                .query_entries(app_id, channel, stream_id, HistoryDirection::OldestFirst)
                .await?;
            let to_delete: Vec<StoredEntryRecord> = entries
                .iter()
                .filter(|row| match request.mode {
                    HistoryPurgeMode::All => true,
                    HistoryPurgeMode::BeforeSerial => {
                        row.serial < request.before_serial.unwrap_or_default()
                    }
                    HistoryPurgeMode::BeforeTimeMs => {
                        row.published_at_ms < request.before_time_ms.unwrap_or_default()
                    }
                })
                .cloned()
                .collect();
            let retained_rows: Vec<StoredEntryRecord> = entries
                .into_iter()
                .filter(|row| match request.mode {
                    HistoryPurgeMode::All => false,
                    HistoryPurgeMode::BeforeSerial => {
                        row.serial >= request.before_serial.unwrap_or_default()
                    }
                    HistoryPurgeMode::BeforeTimeMs => {
                        row.published_at_ms >= request.before_time_ms.unwrap_or_default()
                    }
                })
                .collect();
            purged_messages = to_delete.len() as u64;
            purged_bytes = to_delete.iter().map(|row| row.payload_size_bytes).sum();
            self.delete_entries(app_id, channel, stream_id, &to_delete)
                .await?;
            if let Some(current) = self.load_stream_raw(app_id, channel).await? {
                let retained = HistoryRetentionStats {
                    stream_id: Some(stream_id.to_string()),
                    retained_messages: retained_rows.len() as u64,
                    retained_bytes: retained_rows.iter().map(|row| row.payload_size_bytes).sum(),
                    oldest_serial: retained_rows.first().map(|row| row.serial),
                    newest_serial: retained_rows.last().map(|row| row.serial),
                    oldest_published_at_ms: retained_rows.first().map(|row| row.published_at_ms),
                    newest_published_at_ms: retained_rows.last().map(|row| row.published_at_ms),
                };
                let updated = StoredStreamRecord {
                    retained_messages: retained.retained_messages,
                    retained_bytes: retained.retained_bytes,
                    oldest_available_serial: retained.oldest_serial,
                    newest_available_serial: retained.newest_serial,
                    oldest_available_published_at_ms: retained.oldest_published_at_ms,
                    newest_available_published_at_ms: retained.newest_published_at_ms,
                    updated_at_ms: sockudo_core::history::now_ms(),
                    ..current
                };
                self.upsert_stream_raw(app_id, channel, &updated).await?;
            }
            if let Some(metrics) = self.metrics.as_ref() {
                let retained = self.retained_stats(app_id, channel).await?;
                metrics.update_history_retained(
                    app_id,
                    retained.retained_messages,
                    retained.retained_bytes,
                );
            }
        }
        if let Some(metrics) = self.metrics.as_deref() {
            let _ =
                refresh_history_state_metrics(&self.client, &self.tables, metrics, app_id).await;
        }
        info!(
            app_id = %app_id,
            channel = %channel,
            mode = %request.mode.as_str(),
            before_serial = request.before_serial,
            before_time_ms = request.before_time_ms,
            purged_messages,
            purged_bytes,
            reason = %request.reason,
            requested_by = request.requested_by.as_deref().unwrap_or(""),
            "operator purged durable history rows"
        );
        let inspection = self.resolved_stream_inspection(app_id, channel).await?;
        Ok(HistoryPurgeResult {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            mode: request.mode,
            before_serial: request.before_serial,
            before_time_ms: request.before_time_ms,
            purged_messages,
            purged_bytes,
            inspection,
        })
    }
}

fn is_truncated_by_retention(
    bounds: &HistoryQueryBounds,
    retained: &HistoryRetentionStats,
) -> bool {
    if let (Some(start_serial), Some(oldest_serial)) = (bounds.start_serial, retained.oldest_serial)
        && start_serial < oldest_serial
    {
        return true;
    }
    if let (Some(start_time_ms), Some(oldest_time_ms)) =
        (bounds.start_time_ms, retained.oldest_published_at_ms)
        && start_time_ms < oldest_time_ms
    {
        return true;
    }
    bounds.start_serial.is_none()
        && bounds.start_time_ms.is_none()
        && retained
            .oldest_serial
            .is_some_and(|oldest_serial| oldest_serial > 1)
}

#[cfg(test)]
mod tests {
    use super::super::create_dynamodb_history_store;
    use super::*;
    use aws_sdk_dynamodb::Client;
    use sockudo_core::history_conformance::HistoryStoreConformance;
    use sockudo_core::options::{DynamoDbSettings, HistoryConfig};
    use std::sync::Arc;

    async fn is_dynamodb_available() -> bool {
        let settings = DynamoDbSettings {
            region: "us-east-1".to_string(),
            table_name: "sockudo_history_test_probe".to_string(),
            endpoint_url: Some("http://127.0.0.1:18000".to_string()),
            aws_access_key_id: Some("dummy".to_string()),
            aws_secret_access_key: Some("dummy".to_string()),
            aws_profile_name: None,
        };
        let mut aws_config_builder = aws_config::from_env()
            .region(aws_sdk_dynamodb::config::Region::new(
                settings.region.clone(),
            ))
            .endpoint_url(settings.endpoint_url.clone().unwrap());
        let credentials_provider =
            aws_sdk_dynamodb::config::Credentials::new("dummy", "dummy", None, None, "static");
        aws_config_builder = aws_config_builder.credentials_provider(credentials_provider);
        let client = Client::new(&aws_config_builder.load().await);
        client.list_tables().send().await.is_ok()
    }

    async fn build_store() -> Arc<dyn HistoryStore + Send + Sync> {
        let settings = DynamoDbSettings {
            region: "us-east-1".to_string(),
            table_name: format!("sockudo_history_{}", uuid::Uuid::new_v4().simple()),
            endpoint_url: Some("http://127.0.0.1:18000".to_string()),
            aws_access_key_id: Some("dummy".to_string()),
            aws_secret_access_key: Some("dummy".to_string()),
            aws_profile_name: None,
        };
        let config = HistoryConfig {
            enabled: true,
            backend: sockudo_core::options::HistoryBackend::DynamoDb,
            ..HistoryConfig::default()
        };

        create_dynamodb_history_store(&settings, config, None, None)
            .await
            .unwrap()
    }

    async fn seed_time_series(
        store: &Arc<dyn HistoryStore + Send + Sync>,
    ) -> sockudo_core::history::HistoryWriteReservation {
        let reservation = store.reserve_publish_position("app", "chat").await.unwrap();
        for offset in 0..5u64 {
            store
                .append(sockudo_core::history::HistoryAppendRecord {
                    app_id: "app".to_string(),
                    channel: "chat".to_string(),
                    stream_id: reservation.stream_id.clone(),
                    serial: reservation.serial + offset,
                    published_at_ms: 1_000 + offset as i64,
                    message_id: Some(format!("msg-{}", reservation.serial + offset)),
                    event_name: Some("event".to_string()),
                    operation_kind: "append".to_string(),
                    payload_bytes: tokio_util::bytes::Bytes::from(format!(
                        "payload-{}",
                        reservation.serial + offset
                    )),
                    retention: sockudo_core::history::HistoryRetentionPolicy {
                        retention_window_seconds: 3600,
                        max_messages_per_channel: None,
                        max_bytes_per_channel: None,
                    },
                })
                .await
                .unwrap();
        }
        reservation
    }

    #[tokio::test]
    async fn dynamodb_history_store_conformance_serial_and_stream_continuity() {
        if !is_dynamodb_available().await {
            eprintln!("Skipping test: DynamoDB not available");
            return;
        }
        let store = build_store().await;
        HistoryStoreConformance::assert_serial_monotonicity(store.clone())
            .await
            .unwrap();
        HistoryStoreConformance::assert_stream_id_continuity(store)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn dynamodb_history_store_conformance_pagination_and_reset_semantics() {
        if !is_dynamodb_available().await {
            eprintln!("Skipping test: DynamoDB not available");
            return;
        }
        let store = build_store().await;
        HistoryStoreConformance::assert_cursor_pagination(store.clone())
            .await
            .unwrap();
        HistoryStoreConformance::assert_purge_and_reset_semantics(store)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn dynamodb_history_store_time_bounded_pagination_uses_time_ordering() {
        if !is_dynamodb_available().await {
            eprintln!("Skipping test: DynamoDB not available");
            return;
        }
        let store = build_store().await;
        let reservation = seed_time_series(&store).await;

        let first = store
            .read_page(sockudo_core::history::HistoryReadRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                direction: sockudo_core::history::HistoryDirection::OldestFirst,
                limit: 2,
                cursor: None,
                bounds: sockudo_core::history::HistoryQueryBounds {
                    start_serial: None,
                    end_serial: None,
                    start_time_ms: Some(1_001),
                    end_time_ms: Some(1_004),
                },
            })
            .await
            .unwrap();
        assert_eq!(
            first
                .items
                .iter()
                .map(|item| item.serial)
                .collect::<Vec<_>>(),
            vec![reservation.serial + 1, reservation.serial + 2]
        );
        assert!(first.has_more);

        let second = store
            .read_page(sockudo_core::history::HistoryReadRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                direction: sockudo_core::history::HistoryDirection::OldestFirst,
                limit: 2,
                cursor: first.next_cursor,
                bounds: sockudo_core::history::HistoryQueryBounds {
                    start_serial: None,
                    end_serial: None,
                    start_time_ms: Some(1_001),
                    end_time_ms: Some(1_004),
                },
            })
            .await
            .unwrap();
        assert_eq!(
            second
                .items
                .iter()
                .map(|item| item.serial)
                .collect::<Vec<_>>(),
            vec![reservation.serial + 3, reservation.serial + 4]
        );
        assert!(!second.has_more);
    }
}
