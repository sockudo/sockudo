use sockudo_core::error::{Error, Result};
use sockudo_core::history::{
    HistoryDirection, HistoryReadRequest, HistoryRetentionStats, HistoryStreamInspection,
    HistoryStreamRuntimeState,
};

use super::degraded::{degraded_channel_key, get_cached_channel_degraded, resolve_runtime_state};
use super::{DynamoDbHistoryStore, HistoryStreamRecord, StoredEntryRecord, StoredStreamRecord};

impl DynamoDbHistoryStore {
    pub(super) async fn load_stream_raw(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Option<StoredStreamRecord>> {
        let response = self
            .client
            .get_item()
            .table_name(&self.tables.streams)
            .key(
                "stream_key",
                Self::attr_string(&Self::stream_key(app_id, channel)),
            )
            .send()
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to fetch DynamoDB history stream: {e}"))
            })?;
        match response.item {
            Some(item) => Ok(Some(Self::stream_from_item(item)?)),
            None => Ok(None),
        }
    }

    pub(super) async fn query_entries(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        direction: HistoryDirection,
    ) -> Result<Vec<StoredEntryRecord>> {
        let partition = Self::stream_partition(app_id, channel, stream_id);
        let mut items = Vec::new();
        let mut start_key = None;
        loop {
            let response = self
                .client
                .query()
                .table_name(&self.tables.entries)
                .key_condition_expression("#pk = :pk")
                .expression_attribute_names("#pk", "stream_partition")
                .expression_attribute_values(":pk", Self::attr_string(&partition))
                .scan_index_forward(matches!(direction, HistoryDirection::OldestFirst))
                .set_exclusive_start_key(start_key)
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to query DynamoDB history entries: {e}"))
                })?;
            for item in response.items() {
                items.push(Self::entry_from_item(item.clone())?);
            }
            start_key = response.last_evaluated_key;
            if start_key.is_none() {
                break;
            }
        }
        Ok(items)
    }

    pub(super) async fn query_page_entries(
        &self,
        request: &HistoryReadRequest,
        stream_id: &str,
    ) -> Result<Vec<StoredEntryRecord>> {
        if request.bounds.start_time_ms.is_some() || request.bounds.end_time_ms.is_some() {
            return self.query_time_page_entries(request, stream_id).await;
        }

        let partition = Self::stream_partition(&request.app_id, &request.channel, stream_id);
        let lower_serial = match request.direction {
            HistoryDirection::NewestFirst => request.bounds.start_serial,
            HistoryDirection::OldestFirst => {
                match (request.bounds.start_serial, request.cursor.as_ref()) {
                    (Some(value), Some(cursor)) => Some(value.max(cursor.serial + 1)),
                    (Some(value), None) => Some(value),
                    (None, Some(cursor)) => Some(cursor.serial + 1),
                    (None, None) => None,
                }
            }
        };
        let upper_serial = match request.direction {
            HistoryDirection::NewestFirst => {
                match (request.bounds.end_serial, request.cursor.as_ref()) {
                    (Some(end), Some(cursor)) => Some(end.min(cursor.serial.saturating_sub(1))),
                    (Some(end), None) => Some(end),
                    (None, Some(cursor)) => Some(cursor.serial.saturating_sub(1)),
                    (None, None) => None,
                }
            }
            HistoryDirection::OldestFirst => request.bounds.end_serial,
        };

        let mut key_condition = "#pk = :pk".to_string();
        let mut query = self
            .client
            .query()
            .table_name(&self.tables.entries)
            .expression_attribute_names("#pk", "stream_partition")
            .expression_attribute_values(":pk", Self::attr_string(&partition))
            .scan_index_forward(matches!(request.direction, HistoryDirection::OldestFirst))
            .limit(((request.limit + 1).saturating_mul(4).min(200)) as i32);

        match (lower_serial, upper_serial) {
            (Some(lower), Some(upper)) => {
                query = query.expression_attribute_names("#sk", "serial_key");
                key_condition.push_str(" AND #sk BETWEEN :lower AND :upper");
                query = query
                    .expression_attribute_values(
                        ":lower",
                        Self::attr_string(&Self::serial_key(lower)),
                    )
                    .expression_attribute_values(
                        ":upper",
                        Self::attr_string(&Self::serial_key(upper)),
                    );
            }
            (Some(lower), None) => {
                query = query.expression_attribute_names("#sk", "serial_key");
                key_condition.push_str(" AND #sk >= :lower");
                query = query.expression_attribute_values(
                    ":lower",
                    Self::attr_string(&Self::serial_key(lower)),
                );
            }
            (None, Some(upper)) => {
                query = query.expression_attribute_names("#sk", "serial_key");
                key_condition.push_str(" AND #sk <= :upper");
                query = query.expression_attribute_values(
                    ":upper",
                    Self::attr_string(&Self::serial_key(upper)),
                );
            }
            (None, None) => {}
        }
        query = query.key_condition_expression(key_condition);

        let mut filter_parts = Vec::new();
        if let Some(start_time_ms) = request.bounds.start_time_ms {
            filter_parts.push("published_at_ms >= :start_time_ms".to_string());
            query = query
                .expression_attribute_values(":start_time_ms", Self::attr_number(start_time_ms));
        }
        if let Some(end_time_ms) = request.bounds.end_time_ms {
            filter_parts.push("published_at_ms <= :end_time_ms".to_string());
            query =
                query.expression_attribute_values(":end_time_ms", Self::attr_number(end_time_ms));
        }
        if !filter_parts.is_empty() {
            query = query.filter_expression(filter_parts.join(" AND "));
        }

        let mut items = Vec::new();
        let mut start_key = None;
        while items.len() <= request.limit {
            let response = query
                .clone()
                .set_exclusive_start_key(start_key)
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to query DynamoDB paged history entries: {e:?}"
                    ))
                })?;
            for item in response.items() {
                items.push(Self::entry_from_item(item.clone())?);
                if items.len() > request.limit {
                    break;
                }
            }
            start_key = response.last_evaluated_key;
            if start_key.is_none() {
                break;
            }
        }
        Ok(items)
    }

    async fn load_entry_by_serial(
        &self,
        app_id: &str,
        channel: &str,
        stream_id: &str,
        serial: u64,
    ) -> Result<Option<StoredEntryRecord>> {
        let response = self
            .client
            .get_item()
            .table_name(&self.tables.entries)
            .key(
                "stream_partition",
                Self::attr_string(&Self::stream_partition(app_id, channel, stream_id)),
            )
            .key("serial_key", Self::attr_string(&Self::serial_key(serial)))
            .send()
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to fetch DynamoDB history cursor row: {e}"))
            })?;
        match response.item {
            Some(item) => Ok(Some(Self::entry_from_item(item)?)),
            None => Ok(None),
        }
    }

    async fn query_time_page_entries(
        &self,
        request: &HistoryReadRequest,
        stream_id: &str,
    ) -> Result<Vec<StoredEntryRecord>> {
        let partition = Self::stream_partition(&request.app_id, &request.channel, stream_id);
        let cursor_time_key = if let Some(cursor) = request.cursor.as_ref() {
            let cursor_row = self
                .load_entry_by_serial(&request.app_id, &request.channel, stream_id, cursor.serial)
                .await?
                .ok_or_else(|| {
                    Error::InvalidMessageFormat(
                        "Expired history cursor: cursor item no longer retained".to_string(),
                    )
                })?;
            Some(Self::published_at_serial_key(
                cursor_row.published_at_ms,
                cursor_row.serial,
            ))
        } else {
            None
        };

        let lower_time_key = request
            .bounds
            .start_time_ms
            .map(|value| Self::published_at_serial_key(value, 0));
        let upper_time_key = request
            .bounds
            .end_time_ms
            .map(|value| Self::published_at_serial_key(value, u64::MAX));
        let lower_key = match request.direction {
            HistoryDirection::OldestFirst => match (&lower_time_key, &cursor_time_key) {
                (Some(lower), Some(cursor)) => Some(lower.max(cursor).clone()),
                (Some(lower), None) => Some(lower.clone()),
                (None, Some(cursor)) => Some(cursor.clone()),
                (None, None) => None,
            },
            HistoryDirection::NewestFirst => lower_time_key.clone(),
        };
        let upper_key = match request.direction {
            HistoryDirection::OldestFirst => upper_time_key.clone(),
            HistoryDirection::NewestFirst => match (&upper_time_key, &cursor_time_key) {
                (Some(upper), Some(cursor)) => Some(upper.min(cursor).clone()),
                (Some(upper), None) => Some(upper.clone()),
                (None, Some(cursor)) => Some(cursor.clone()),
                (None, None) => None,
            },
        };

        let mut key_condition = "#pk = :pk".to_string();
        let mut query = self
            .client
            .query()
            .table_name(&self.tables.entries)
            .index_name(&self.tables.entries_time_index)
            .expression_attribute_names("#pk", "stream_partition")
            .expression_attribute_names("#ts", "published_at_serial_key")
            .expression_attribute_values(":pk", Self::attr_string(&partition))
            .scan_index_forward(matches!(request.direction, HistoryDirection::OldestFirst))
            .limit(((request.limit + 1).saturating_mul(4).min(200)) as i32);
        match (&lower_key, &upper_key) {
            (Some(lower), Some(upper)) => {
                key_condition.push_str(" AND #ts BETWEEN :lower_ts AND :upper_ts");
                query = query
                    .expression_attribute_values(":lower_ts", Self::attr_string(lower))
                    .expression_attribute_values(":upper_ts", Self::attr_string(upper));
            }
            (Some(lower), None) => {
                key_condition.push_str(" AND #ts >= :lower_ts");
                query = query.expression_attribute_values(":lower_ts", Self::attr_string(lower));
            }
            (None, Some(upper)) => {
                key_condition.push_str(" AND #ts <= :upper_ts");
                query = query.expression_attribute_values(":upper_ts", Self::attr_string(upper));
            }
            (None, None) => {}
        }
        query = query.key_condition_expression(key_condition);

        let mut filter_parts = Vec::new();
        if let Some(start_serial) = request.bounds.start_serial {
            filter_parts.push("serial >= :start_serial".to_string());
            query =
                query.expression_attribute_values(":start_serial", Self::attr_number(start_serial));
        }
        if let Some(end_serial) = request.bounds.end_serial {
            filter_parts.push("serial <= :end_serial".to_string());
            query = query.expression_attribute_values(":end_serial", Self::attr_number(end_serial));
        }
        if !filter_parts.is_empty() {
            query = query.filter_expression(filter_parts.join(" AND "));
        }

        let mut items = Vec::new();
        let mut start_key = None;
        while items.len() <= request.limit {
            let response = query
                .clone()
                .set_exclusive_start_key(start_key)
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "Failed to query DynamoDB time-index history entries: {e:?}"
                    ))
                })?;
            for item in response.items() {
                let entry = Self::entry_from_item(item.clone())?;
                if let Some(cursor_key) = &cursor_time_key
                    && Self::published_at_serial_key(entry.published_at_ms, entry.serial)
                        == *cursor_key
                {
                    continue;
                }
                items.push(entry);
                if items.len() > request.limit {
                    break;
                }
            }
            start_key = response.last_evaluated_key;
            if start_key.is_none() {
                break;
            }
        }
        Ok(items)
    }

    async fn load_stream_record(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Option<HistoryStreamRecord>> {
        let Some(stream) = self.load_stream_raw(app_id, channel).await? else {
            return Ok(None);
        };
        Ok(Some(HistoryStreamRecord {
            stream_id: stream.stream_id.clone(),
            next_serial: stream.next_serial,
            durable_state: stream.durable_state,
            durable_state_reason: stream.durable_state_reason.clone(),
            durable_state_node_id: stream.durable_state_node_id.clone(),
            durable_state_changed_at_ms: stream.durable_state_changed_at_ms,
            retained: retained_from_stream_record(&stream),
        }))
    }

    pub(super) async fn retained_stats(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryRetentionStats> {
        Ok(match self.load_stream_raw(app_id, channel).await? {
            Some(stream) => retained_from_stream_record(&stream),
            None => HistoryRetentionStats::default(),
        })
    }

    pub(super) async fn resolved_stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamRuntimeState> {
        let durable_record = self.load_stream_record(app_id, channel).await?;
        let durable_state = durable_record
            .as_ref()
            .map(|record| record.runtime_state(app_id, channel, "durable_store"))
            .unwrap_or_else(|| {
                HistoryStreamRuntimeState::healthy(app_id, channel, None, "durable_store")
            });
        let local_hint = self
            .degraded_channels
            .get(&degraded_channel_key(app_id, channel))
            .map(|entry| entry.value().clone());
        let cache_hint =
            get_cached_channel_degraded(self.cache_manager.as_ref(), app_id, channel).await?;
        Ok(resolve_runtime_state(durable_state, local_hint, cache_hint))
    }

    pub(super) async fn resolved_stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<HistoryStreamInspection> {
        let durable_record = self.load_stream_record(app_id, channel).await?;
        let runtime_state = self.resolved_stream_runtime_state(app_id, channel).await?;
        Ok(match durable_record {
            Some(record) => record.inspection(app_id, channel, &runtime_state.observed_source),
            None => HistoryStreamInspection {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                stream_id: None,
                next_serial: None,
                retained: HistoryRetentionStats::default(),
                state: runtime_state,
            },
        })
    }
}

fn retained_from_stream_record(stream: &StoredStreamRecord) -> HistoryRetentionStats {
    HistoryRetentionStats {
        stream_id: Some(stream.stream_id.clone()),
        retained_messages: stream.retained_messages,
        retained_bytes: stream.retained_bytes,
        oldest_serial: stream.oldest_available_serial,
        newest_serial: stream.newest_available_serial,
        oldest_published_at_ms: stream.oldest_available_published_at_ms,
        newest_published_at_ms: stream.newest_available_published_at_ms,
    }
}
