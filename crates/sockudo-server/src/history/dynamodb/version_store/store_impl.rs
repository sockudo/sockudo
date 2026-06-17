use super::*;

#[cfg(feature = "versioned-messages")]
#[async_trait::async_trait]
impl VersionStore for DynamoDbVersionStore {
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
        let app_channel = Self::app_channel_key(app_id, channel);
        loop {
            let existing = self
                .client
                .get_item()
                .table_name(&self.tables.version_streams)
                .key("app_channel", Self::attr_s(&app_channel))
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!("Failed to read version stream from DynamoDB: {e}"))
                })?
                .item;

            let now_ms = sockudo_core::history::now_ms();

            if let Some(item) = existing {
                let current = Self::item_num(&item, "next_delivery_serial").unwrap_or(1) as u64;
                let next = current.saturating_add(block_size);
                let result = self
                    .client
                    .update_item()
                    .table_name(&self.tables.version_streams)
                    .key("app_channel", Self::attr_s(&app_channel))
                    .update_expression("SET next_delivery_serial = :next, updated_at_ms = :now")
                    .condition_expression("next_delivery_serial = :expected")
                    .expression_attribute_values(":next", Self::attr_n(next))
                    .expression_attribute_values(":expected", Self::attr_n(current))
                    .expression_attribute_values(":now", Self::attr_n(now_ms))
                    .send()
                    .await;
                match result {
                    Ok(_) => {
                        return Ok(VersionWriteReservationBlock {
                            stream_id: format!("{}/{}", app_id, channel),
                            start_delivery_serial: current,
                            len: block_size,
                        });
                    }
                    Err(e) if e.to_string().contains("ConditionalCheckFailed") => continue,
                    Err(e) => {
                        return Err(Error::Internal(format!(
                            "Failed to advance DynamoDB version delivery serial: {e}"
                        )));
                    }
                }
            } else {
                let mut new_item = HashMap::new();
                new_item.insert("app_channel".to_string(), Self::attr_s(&app_channel));
                new_item.insert("app_id".to_string(), Self::attr_s(app_id));
                new_item.insert("channel".to_string(), Self::attr_s(channel));
                new_item.insert(
                    "next_delivery_serial".to_string(),
                    Self::attr_n(block_size.saturating_add(1)),
                );
                new_item.insert("migration_state".to_string(), Self::attr_s("native_only"));
                new_item.insert("updated_at_ms".to_string(), Self::attr_n(now_ms));

                let create_result = self
                    .client
                    .put_item()
                    .table_name(&self.tables.version_streams)
                    .set_item(Some(new_item))
                    .condition_expression("attribute_not_exists(app_channel)")
                    .send()
                    .await;
                match create_result {
                    Ok(_) => {
                        return Ok(VersionWriteReservationBlock {
                            stream_id: format!("{}/{}", app_id, channel),
                            start_delivery_serial: 1,
                            len: block_size,
                        });
                    }
                    Err(e) if e.to_string().contains("ConditionalCheckFailed") => continue,
                    Err(e) => {
                        return Err(Error::Internal(format!(
                            "Failed to create DynamoDB version stream row: {e}"
                        )));
                    }
                }
            }
        }
    }

    async fn append_version(&self, record: StoredVersionRecord) -> Result<()> {
        let now_ms = sockudo_core::history::now_ms();
        let payload = sonic_rs::to_vec(&record)
            .map_err(|e| Error::Internal(format!("Failed to serialize version record: {e}")))?;
        let app_channel = Self::app_channel_key(&record.app_id, &record.channel);
        let app_channel_message = Self::app_channel_message_key(
            &record.app_id,
            &record.channel,
            record.message_serial().as_str(),
        );
        let message_version_key = Self::message_version_key(
            record.message_serial().as_str(),
            record.version_serial().as_str(),
        );

        // Write the entry (idempotent via condition).
        let mut entry_item = HashMap::new();
        entry_item.insert("app_channel".to_string(), Self::attr_s(&app_channel));
        entry_item.insert(
            "message_version_key".to_string(),
            Self::attr_s(&message_version_key),
        );
        entry_item.insert(
            "app_channel_message".to_string(),
            Self::attr_s(&app_channel_message),
        );
        entry_item.insert("app_id".to_string(), Self::attr_s(&record.app_id));
        entry_item.insert("channel".to_string(), Self::attr_s(&record.channel));
        entry_item.insert(
            "message_serial".to_string(),
            Self::attr_s(record.message_serial().as_str()),
        );
        entry_item.insert(
            "version_serial".to_string(),
            Self::attr_s(record.version_serial().as_str()),
        );
        entry_item.insert(
            "delivery_serial".to_string(),
            Self::attr_n(record.delivery_serial()),
        );
        entry_item.insert(
            "history_serial".to_string(),
            Self::attr_n(record.history_serial()),
        );
        entry_item.insert(
            "action".to_string(),
            Self::attr_s(record.message.action.as_str()),
        );
        entry_item.insert("payload_bytes".to_string(), Self::attr_b(payload));
        entry_item.insert("created_at_ms".to_string(), Self::attr_n(now_ms));
        if let Some(expires_at) = self.expires_at_value() {
            entry_item.insert(Self::EXPIRES_AT_ATTR.to_string(), expires_at);
        }

        let put_result = self
            .client
            .put_item()
            .table_name(&self.tables.version_entries)
            .set_item(Some(entry_item))
            .condition_expression("attribute_not_exists(message_version_key)")
            .send()
            .await;
        if let Err(e) = put_result
            && !e.to_string().contains("ConditionalCheckFailed")
        {
            return Err(Error::Internal(format!(
                "Failed to write version entry to DynamoDB: {e}"
            )));
        }
        // Duplicate version entry — idempotent, continue.

        // Advance version_messages if this version_serial is greater.
        let (update_expr, expires_value) = if let Some(expires) = self.expires_at_value() {
            (
                "SET latest_version_serial = :vs, latest_delivery_serial = :ds, latest_action = :action, updated_at_ms = :now, history_serial = :hs, original_client_id = :oc, created_at_ms = if_not_exists(created_at_ms, :now), expires_at = :exp",
                Some(expires),
            )
        } else {
            (
                "SET latest_version_serial = :vs, latest_delivery_serial = :ds, latest_action = :action, updated_at_ms = :now, history_serial = :hs, original_client_id = :oc, created_at_ms = if_not_exists(created_at_ms, :now)",
                None,
            )
        };
        let mut update_builder = self
            .client
            .update_item()
            .table_name(&self.tables.version_messages)
            .key("app_channel", Self::attr_s(&app_channel))
            .key(
                "message_serial",
                Self::attr_s(record.message_serial().as_str()),
            )
            .update_expression(update_expr)
            .condition_expression(
                "attribute_not_exists(latest_version_serial) OR latest_version_serial < :vs",
            )
            .expression_attribute_values(":vs", Self::attr_s(record.version_serial().as_str()))
            .expression_attribute_values(":ds", Self::attr_n(record.delivery_serial()))
            .expression_attribute_values(":action", Self::attr_s(record.message.action.as_str()))
            .expression_attribute_values(":now", Self::attr_n(now_ms))
            .expression_attribute_values(":hs", Self::attr_n(record.history_serial()))
            .expression_attribute_values(
                ":oc",
                record
                    .original_client_id
                    .as_deref()
                    .map(Self::attr_s)
                    .unwrap_or(AttributeValue::Null(true)),
            );
        if let Some(expires) = expires_value {
            update_builder = update_builder.expression_attribute_values(":exp", expires);
        }
        let update_result = update_builder.send().await;
        if let Err(e) = update_result {
            // ConditionalCheckFailed means a newer version is already stored — idempotent.
            if !e.to_string().contains("ConditionalCheckFailed") {
                return Err(Error::Internal(format!(
                    "Failed to update version_messages in DynamoDB: {e}"
                )));
            }
        }

        Ok(())
    }

    async fn get_latest(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &sockudo_core::versioned_messages::MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        let app_channel_message =
            Self::app_channel_message_key(app_id, channel, message_serial.as_str());
        // Query the message GSI sorted by version_serial DESC, limit 1.
        let result = self
            .client
            .query()
            .table_name(&self.tables.version_entries)
            .index_name(&self.tables.version_entries_message_index)
            .key_condition_expression("app_channel_message = :acm")
            .expression_attribute_values(":acm", Self::attr_s(&app_channel_message))
            .scan_index_forward(false)
            .limit(1)
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to query latest version: {e}")))?;

        let items = result.items();
        if items.is_empty() {
            return Ok(None);
        }
        let bytes = items[0]
            .get("payload_bytes")
            .and_then(|v| v.as_b().ok())
            .map(|b| b.as_ref().to_vec())
            .ok_or_else(|| Error::Internal("Missing payload_bytes in version entry".to_string()))?;

        let record: StoredVersionRecord = sonic_rs::from_slice(&bytes)
            .map_err(|e| Error::Internal(format!("Failed to deserialize version record: {e}")))?;
        Ok(Some(record))
    }

    async fn get_versions(&self, request: VersionStoreReadRequest) -> Result<VersionStorePage> {
        request.validate()?;
        let app_channel_message = Self::app_channel_message_key(
            &request.app_id,
            &request.channel,
            request.message_serial.as_str(),
        );
        let scan_forward = matches!(request.direction, VersionStoreDirection::OldestFirst);
        let fetch_limit = (request.limit + 1) as i32;

        let mut query = self
            .client
            .query()
            .table_name(&self.tables.version_entries)
            .index_name(&self.tables.version_entries_message_index)
            .key_condition_expression(if request.cursor.is_some() {
                match request.direction {
                    VersionStoreDirection::NewestFirst => {
                        "app_channel_message = :acm AND version_serial < :cursor_vs"
                    }
                    VersionStoreDirection::OldestFirst => {
                        "app_channel_message = :acm AND version_serial > :cursor_vs"
                    }
                }
            } else {
                "app_channel_message = :acm"
            })
            .expression_attribute_values(":acm", Self::attr_s(&app_channel_message))
            .scan_index_forward(scan_forward)
            .limit(fetch_limit);

        if let Some(cursor) = &request.cursor {
            query = query.expression_attribute_values(
                ":cursor_vs",
                Self::attr_s(cursor.version_serial.as_str()),
            );
        }

        let result = query
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to query version history: {e}")))?;

        let all_items = result.items();
        let has_more = all_items.len() > request.limit;
        let items: Vec<StoredVersionRecord> = all_items
            .iter()
            .take(request.limit)
            .map(|item| {
                let bytes = item
                    .get("payload_bytes")
                    .and_then(|v| v.as_b().ok())
                    .map(|b| b.as_ref().to_vec())
                    .ok_or_else(|| {
                        Error::Internal("Missing payload_bytes in version entry".to_string())
                    })?;
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
        let app_channel = Self::app_channel_key(&request.app_id, &request.channel);
        let result = self
            .client
            .query()
            .table_name(&self.tables.version_entries)
            .index_name(&self.tables.version_entries_delivery_index)
            .key_condition_expression("app_channel = :ac AND delivery_serial > :after")
            .expression_attribute_values(":ac", Self::attr_s(&app_channel))
            .expression_attribute_values(":after", Self::attr_n(request.after_delivery_serial))
            .scan_index_forward(true)
            .limit(request.limit as i32)
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to replay version entries: {e}")))?;

        result
            .items()
            .iter()
            .map(|item| {
                let bytes = item
                    .get("payload_bytes")
                    .and_then(|v| v.as_b().ok())
                    .map(|b| b.as_ref().to_vec())
                    .ok_or_else(|| {
                        Error::Internal("Missing payload_bytes in version entry".to_string())
                    })?;
                sonic_rs::from_slice(&bytes)
                    .map_err(|e| Error::Internal(format!("Failed to deserialize replay: {e}")))
            })
            .collect()
    }

    async fn latest_by_history(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Vec<StoredVersionRecord>> {
        let app_channel = Self::app_channel_key(app_id, channel);
        // Scan version_messages table for this channel.
        let msg_result = self
            .client
            .query()
            .table_name(&self.tables.version_messages)
            .key_condition_expression("app_channel = :ac")
            .expression_attribute_values(":ac", Self::attr_s(&app_channel))
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to query version messages: {e}")))?;

        let mut msgs: Vec<(String, String, i64)> = msg_result
            .items()
            .iter()
            .filter_map(|item| {
                let message_serial = Self::item_str(item, "message_serial")?;
                let latest_version_serial = Self::item_str(item, "latest_version_serial")?;
                let history_serial = Self::item_num(item, "history_serial")?;
                Some((message_serial, latest_version_serial, history_serial))
            })
            .collect();

        msgs.sort_by_key(|(_, _, hs)| *hs);

        let mut result = Vec::with_capacity(msgs.len());
        for (message_serial, latest_version_serial, _hs) in msgs {
            let app_channel_message =
                Self::app_channel_message_key(app_id, channel, &message_serial);
            let entry_result = self
                .client
                .query()
                .table_name(&self.tables.version_entries)
                .index_name(&self.tables.version_entries_message_index)
                .key_condition_expression("app_channel_message = :acm AND version_serial = :vs")
                .expression_attribute_values(":acm", Self::attr_s(&app_channel_message))
                .expression_attribute_values(":vs", Self::attr_s(&latest_version_serial))
                .limit(1)
                .send()
                .await
                .map_err(|e| Error::Internal(format!("Failed to fetch version entry: {e}")))?;

            if let Some(item) = entry_result.items().first() {
                let bytes = item
                    .get("payload_bytes")
                    .and_then(|v| v.as_b().ok())
                    .map(|b| b.as_ref().to_vec())
                    .ok_or_else(|| {
                        Error::Internal("Missing payload_bytes in version entry".to_string())
                    })?;
                let record: StoredVersionRecord = sonic_rs::from_slice(&bytes).map_err(|e| {
                    Error::Internal(format!("Failed to deserialize version record: {e}"))
                })?;
                result.push(record);
            }
        }
        Ok(result)
    }

    async fn stream_state(&self, app_id: &str, channel: &str) -> Result<VersionStreamState> {
        let app_channel = Self::app_channel_key(app_id, channel);
        let result = self
            .client
            .get_item()
            .table_name(&self.tables.version_streams)
            .key("app_channel", Self::attr_s(&app_channel))
            .send()
            .await
            .map_err(|e| {
                Error::Internal(format!("Failed to read DynamoDB version stream state: {e}"))
            })?;

        let Some(item) = result.item else {
            return Ok(VersionStreamState::default());
        };

        Ok(VersionStreamState {
            stream_id: Some(format!("{}/{}", app_id, channel)),
            next_delivery_serial: Self::item_num(&item, "next_delivery_serial").map(|v| v as u64),
            oldest_available_delivery_serial: Self::item_num(
                &item,
                "oldest_available_delivery_serial",
            )
            .map(|v| v as u64),
            newest_available_delivery_serial: Self::item_num(
                &item,
                "newest_available_delivery_serial",
            )
            .map(|v| v as u64),
        })
    }
}
