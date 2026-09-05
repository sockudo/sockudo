use super::*;
use sockudo_core::version_store::EncodedVersionRecord;

#[cfg(feature = "versioned-messages")]
#[async_trait::async_trait]
impl VersionStore for DynamoDbVersionStore {
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
        let app_channel = Self::app_channel_key(&record.app_id, &record.channel);
        let entry_key = Self::message_version_key(
            record.message_serial().as_str(),
            record.version_serial().as_str(),
        );
        let payload = sonic_rs::to_vec(&record)?;
        let delivery = i64::try_from(record.delivery_serial())
            .ok()
            .filter(|value| *value < i64::MAX)
            .ok_or_else(|| {
                Error::InvalidMessageFormat("imported delivery serial exceeds storage range".into())
            })?;
        for _ in 0..64 {
            let existing_entry = self
                .client
                .get_item()
                .table_name(&self.tables.version_entries)
                .key("app_channel", Self::attr_s(&app_channel))
                .key("message_version_key", Self::attr_s(&entry_key))
                .projection_expression("message_version_key")
                .consistent_read(true)
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!("failed to read imported version identity: {e}"))
                })?
                .item;
            if existing_entry.is_some() {
                return Ok(());
            }
            let existing = self
                .client
                .get_item()
                .table_name(&self.tables.version_messages)
                .key("app_channel", Self::attr_s(&app_channel))
                .key(
                    "message_serial",
                    Self::attr_s(record.message_serial().as_str()),
                )
                .consistent_read(true)
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!("failed to read imported version predecessor: {e}"))
                })?
                .item;
            let stream = self
                .client
                .get_item()
                .table_name(&self.tables.version_streams)
                .key("app_channel", Self::attr_s(&app_channel))
                .consistent_read(true)
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!("failed to read imported version stream: {e}"))
                })?
                .item;
            let now = sockudo_core::history::now_ms();
            let previous_version = existing
                .as_ref()
                .and_then(|item| Self::item_str(item, "latest_version_serial"));
            let wins = previous_version
                .as_ref()
                .is_none_or(|version| record.version_serial().as_str() > version.as_str());
            let previous_count = existing
                .as_ref()
                .and_then(|item| Self::item_num(item, "append_count"));
            let was_open = existing
                .as_ref()
                .and_then(|item| item.get("is_open_stream"))
                .and_then(|value| value.as_bool().ok())
                .copied()
                .unwrap_or(false);
            let mut message = existing.clone().unwrap_or_default();
            message.insert("app_channel".into(), Self::attr_s(&app_channel));
            message.insert(
                "message_serial".into(),
                Self::attr_s(record.message_serial().as_str()),
            );
            message.insert(
                "append_count".into(),
                Self::attr_n(
                    previous_count.unwrap_or(0)
                        + i64::from(
                            record.message.action
                                == sockudo_core::versioned_messages::MessageAction::Append,
                        ),
                ),
            );
            if wins {
                message.insert(
                    "latest_version_serial".into(),
                    Self::attr_s(record.version_serial().as_str()),
                );
                message.insert(
                    "latest_delivery_serial".into(),
                    Self::attr_n(record.delivery_serial()),
                );
                message.insert(
                    "latest_action".into(),
                    Self::attr_s(record.message.action.as_str()),
                );
                message.insert("latest_payload_bytes".into(), Self::attr_b(payload.clone()));
                message.insert(
                    "is_open_stream".into(),
                    AttributeValue::Bool(record.is_open_ai_stream()),
                );
                message.insert(
                    "history_serial".into(),
                    Self::attr_n(record.history_serial()),
                );
                message.insert(
                    "original_client_id".into(),
                    record
                        .original_client_id
                        .as_deref()
                        .map(Self::attr_s)
                        .unwrap_or(AttributeValue::Null(true)),
                );
            }
            message.insert("updated_at_ms".into(), Self::attr_n(now));
            message
                .entry("created_at_ms".into())
                .or_insert_with(|| Self::attr_n(now));
            if let Some(expiry) = self.expires_at_value() {
                message.insert(Self::EXPIRES_AT_ATTR.into(), expiry);
            }
            let mut message_put = Put::builder()
                .table_name(&self.tables.version_messages)
                .set_item(Some(message));
            message_put = if let Some(version) = previous_version {
                let count_condition = if previous_count.is_some() {
                    "append_count = :count"
                } else {
                    "attribute_not_exists(append_count)"
                };
                let mut put = message_put
                    .condition_expression(format!(
                        "latest_version_serial = :version AND {count_condition}"
                    ))
                    .expression_attribute_values(":version", Self::attr_s(&version));
                if let Some(count) = previous_count {
                    put = put.expression_attribute_values(":count", Self::attr_n(count));
                }
                put
            } else {
                message_put.condition_expression("attribute_not_exists(message_serial)")
            };
            let next = stream
                .as_ref()
                .and_then(|item| Self::item_num(item, "next_delivery_serial"))
                .unwrap_or(1);
            let open = stream
                .as_ref()
                .and_then(|item| Self::item_num(item, "open_stream_count"));
            let mut next_stream = stream.clone().unwrap_or_default();
            next_stream.insert("app_channel".into(), Self::attr_s(&app_channel));
            next_stream.insert(
                "next_delivery_serial".into(),
                Self::attr_n(next.max(delivery + 1)),
            );
            let new_open = open.unwrap_or(0)
                + if wins {
                    i64::from(record.is_open_ai_stream()) - i64::from(was_open)
                } else {
                    0
                };
            next_stream.insert("open_stream_count".into(), Self::attr_n(new_open.max(0)));
            for (field, minimum) in [
                ("oldest_available_delivery_serial", true),
                ("newest_available_delivery_serial", false),
            ] {
                let old = stream
                    .as_ref()
                    .and_then(|item| Self::item_num(item, field))
                    .unwrap_or(delivery);
                next_stream.insert(
                    field.into(),
                    Self::attr_n(if minimum {
                        old.min(delivery)
                    } else {
                        old.max(delivery)
                    }),
                );
            }
            next_stream.insert("updated_at_ms".into(), Self::attr_n(now));
            let mut stream_put = Put::builder()
                .table_name(&self.tables.version_streams)
                .set_item(Some(next_stream));
            stream_put = if stream.is_some() {
                let open_condition = if open.is_some() {
                    "open_stream_count = :open"
                } else {
                    "attribute_not_exists(open_stream_count)"
                };
                let mut put = stream_put
                    .condition_expression(format!(
                        "next_delivery_serial = :next AND {open_condition}"
                    ))
                    .expression_attribute_values(":next", Self::attr_n(next));
                if let Some(open) = open {
                    put = put.expression_attribute_values(":open", Self::attr_n(open));
                }
                put
            } else {
                stream_put.condition_expression("attribute_not_exists(app_channel)")
            };
            let entry = Put::builder()
                .table_name(&self.tables.version_entries)
                .set_item(Some(self.entry_item(&record, None, None)?))
                .condition_expression("attribute_not_exists(message_version_key)")
                .build()
                .map_err(|e| {
                    Error::Internal(format!("failed to build imported version entry: {e}"))
                })?;
            let message_put = message_put.build().map_err(|e| {
                Error::Internal(format!("failed to build imported version metadata: {e}"))
            })?;
            let stream_put = stream_put.build().map_err(|e| {
                Error::Internal(format!("failed to build imported version stream: {e}"))
            })?;
            match self
                .client
                .transact_write_items()
                .transact_items(TransactWriteItem::builder().put(entry).build())
                .transact_items(TransactWriteItem::builder().put(message_put).build())
                .transact_items(TransactWriteItem::builder().put(stream_put).build())
                .send()
                .await
            {
                Ok(_) => return Ok(()),
                Err(error)
                    if error
                        .as_service_error()
                        .is_some_and(transaction_is_conflict) =>
                {
                    continue;
                }
                Err(error) => {
                    return Err(Error::Internal(format!(
                        "failed to import version transaction: {error}"
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
        let app_channel = Self::app_channel_key(&request.record.app_id, &request.record.channel);
        let stream_item = self
            .client
            .get_item()
            .table_name(&self.tables.version_streams)
            .key("app_channel", Self::attr_s(&app_channel))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to read version stream: {e}")))?
            .item;
        let next_delivery = stream_item
            .as_ref()
            .and_then(|item| Self::item_num(item, "next_delivery_serial"))
            .unwrap_or(1) as u64;
        let open_count = stream_item
            .as_ref()
            .and_then(|item| Self::item_num(item, "open_stream_count"))
            .unwrap_or(0) as usize;
        if request.record.is_open_ai_stream()
            && let Some(limit) = request.limits.max_open_streaming_messages_per_channel
            && open_count >= limit
        {
            return Ok(VersionCreateResult::Rejected(
                VersionCreateRejection::OpenStreamingMessages { limit },
            ));
        }
        let message_key = request.record.message_serial().as_str().to_string();
        let existing = self
            .client
            .get_item()
            .table_name(&self.tables.version_messages)
            .key("app_channel", Self::attr_s(&app_channel))
            .key("message_serial", Self::attr_s(&message_key))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to read create target: {e}")))?
            .item;
        if existing.is_some() {
            let current = self
                .get_latest(
                    &request.record.app_id,
                    &request.record.channel,
                    request.record.message_serial(),
                )
                .await?
                .ok_or_else(|| {
                    Error::Internal("Existing message has no readable version entry".to_string())
                })?;
            return Ok(VersionCreateResult::Conflict {
                current: Some(current),
            });
        }

        let stream_id = format!("{}/{}", request.record.app_id, request.record.channel);
        let record = request
            .record
            .with_delivery_position(&stream_id, next_delivery);
        let payload = sonic_rs::to_vec(&record)
            .map_err(|e| Error::Internal(format!("Failed to serialize create record: {e}")))?;
        let now_ms = sockudo_core::history::now_ms();
        let next_open = open_count + usize::from(record.is_open_ai_stream());
        let stream_write = if stream_item.is_some() {
            let update = Update::builder()
                .table_name(&self.tables.version_streams)
                .key("app_channel", Self::attr_s(&app_channel))
                .update_expression("SET next_delivery_serial = :next, open_stream_count = :new_open, updated_at_ms = :now, oldest_available_delivery_serial = if_not_exists(oldest_available_delivery_serial, :delivery), newest_available_delivery_serial = :delivery")
                .condition_expression("next_delivery_serial = :expected AND (attribute_not_exists(open_stream_count) OR open_stream_count = :open)")
                .expression_attribute_values(":next", Self::attr_n(next_delivery + 1))
                .expression_attribute_values(":expected", Self::attr_n(next_delivery))
                .expression_attribute_values(":open", Self::attr_n(open_count))
                .expression_attribute_values(":new_open", Self::attr_n(next_open))
                .expression_attribute_values(":delivery", Self::attr_n(next_delivery))
                .expression_attribute_values(":now", Self::attr_n(now_ms))
                .build()
                .map_err(|e| Error::Internal(format!("Failed to build stream update: {e}")))?;
            TransactWriteItem::builder().update(update).build()
        } else {
            let mut item = HashMap::new();
            item.insert("app_channel".to_string(), Self::attr_s(&app_channel));
            item.insert("app_id".to_string(), Self::attr_s(&record.app_id));
            item.insert("channel".to_string(), Self::attr_s(&record.channel));
            item.insert("next_delivery_serial".to_string(), Self::attr_n(2));
            item.insert("open_stream_count".to_string(), Self::attr_n(next_open));
            item.insert(
                "oldest_available_delivery_serial".to_string(),
                Self::attr_n(1),
            );
            item.insert(
                "newest_available_delivery_serial".to_string(),
                Self::attr_n(1),
            );
            item.insert("migration_state".to_string(), Self::attr_s("native_only"));
            item.insert("updated_at_ms".to_string(), Self::attr_n(now_ms));
            let put = Put::builder()
                .table_name(&self.tables.version_streams)
                .set_item(Some(item))
                .condition_expression("attribute_not_exists(app_channel)")
                .build()
                .map_err(|e| Error::Internal(format!("Failed to build stream create: {e}")))?;
            TransactWriteItem::builder().put(put).build()
        };
        let entry_put = Put::builder()
            .table_name(&self.tables.version_entries)
            .set_item(Some(self.entry_item(&record, None, None)?))
            .condition_expression("attribute_not_exists(message_version_key)")
            .build()
            .map_err(|e| Error::Internal(format!("Failed to build create entry: {e}")))?;
        let mut message_item = HashMap::new();
        message_item.insert("app_channel".to_string(), Self::attr_s(&app_channel));
        message_item.insert("message_serial".to_string(), Self::attr_s(&message_key));
        message_item.insert(
            "latest_version_serial".to_string(),
            Self::attr_s(record.version_serial().as_str()),
        );
        message_item.insert(
            "latest_delivery_serial".to_string(),
            Self::attr_n(next_delivery),
        );
        message_item.insert(
            "latest_action".to_string(),
            Self::attr_s(record.message.action.as_str()),
        );
        message_item.insert("latest_payload_bytes".to_string(), Self::attr_b(payload));
        message_item.insert("append_count".to_string(), Self::attr_n(0));
        message_item.insert(
            "is_open_stream".to_string(),
            AttributeValue::Bool(record.is_open_ai_stream()),
        );
        message_item.insert(
            "history_serial".to_string(),
            Self::attr_n(record.history_serial()),
        );
        message_item.insert("created_at_ms".to_string(), Self::attr_n(now_ms));
        message_item.insert("updated_at_ms".to_string(), Self::attr_n(now_ms));
        let message_put = Put::builder()
            .table_name(&self.tables.version_messages)
            .set_item(Some(message_item))
            .condition_expression("attribute_not_exists(message_serial)")
            .build()
            .map_err(|e| Error::Internal(format!("Failed to build message create: {e}")))?;
        let result = self
            .client
            .transact_write_items()
            .transact_items(stream_write)
            .transact_items(TransactWriteItem::builder().put(entry_put).build())
            .transact_items(TransactWriteItem::builder().put(message_put).build())
            .send()
            .await;
        match result {
            Ok(_) => Ok(VersionCreateResult::Applied { record, stream_id }),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(transaction_is_conflict) =>
            {
                let current = self
                    .get_latest(&record.app_id, &record.channel, record.message_serial())
                    .await?;
                if let Some(current) = current {
                    Ok(VersionCreateResult::Conflict {
                        current: Some(current),
                    })
                } else {
                    Ok(VersionCreateResult::Conflict { current: None })
                }
            }
            Err(error) => Err(Error::Internal(format!(
                "Failed to transact version create: {error}"
            ))),
        }
    }

    async fn compare_and_apply(
        &self,
        request: VersionMutationRequest,
    ) -> Result<VersionMutationResult> {
        let app_channel = Self::app_channel_key(&request.app_id, &request.channel);
        if let Some(operation) = request.idempotency.as_ref() {
            let receipt_key = Self::operation_receipt_key(&operation.cache_key);
            if let Some(item) = self
                .client
                .get_item()
                .table_name(&self.tables.version_entries)
                .key("app_channel", Self::attr_s(&app_channel))
                .key("message_version_key", Self::attr_s(&receipt_key))
                .consistent_read(true)
                .send()
                .await
                .map_err(|e| Error::Internal(format!("Failed to read operation receipt: {e}")))?
                .item
            {
                let fingerprint = Self::item_str(&item, "operation_fingerprint");
                if fingerprint.as_deref() != Some(operation.payload_fingerprint.as_str()) {
                    return Err(Error::IdempotencyConflict);
                }
                let bytes = item
                    .get("payload_bytes")
                    .and_then(|value| value.as_b().ok())
                    .map(|value| value.as_ref().to_vec())
                    .ok_or_else(|| Error::Internal("Receipt payload is missing".to_string()))?;
                let record = self.decode_records(vec![bytes]).await?.remove(0);
                return Ok(VersionMutationResult::Duplicate {
                    record,
                    stream_id: format!("{}/{}", request.app_id, request.channel),
                });
            }
        }
        let stream_item = self
            .client
            .get_item()
            .table_name(&self.tables.version_streams)
            .key("app_channel", Self::attr_s(&app_channel))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to read version stream: {e}")))?
            .item;
        let Some(stream_item) = stream_item else {
            return Ok(VersionMutationResult::Conflict { current: None });
        };
        let next_delivery =
            Self::item_num(&stream_item, "next_delivery_serial").unwrap_or(1) as u64;
        let open_count = Self::item_num(&stream_item, "open_stream_count").unwrap_or(0) as usize;
        let message_item = self
            .client
            .get_item()
            .table_name(&self.tables.version_messages)
            .key("app_channel", Self::attr_s(&app_channel))
            .key(
                "message_serial",
                Self::attr_s(request.message_serial.as_str()),
            )
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to read mutation predecessor: {e}")))?
            .item;
        let Some(message_item) = message_item else {
            return Ok(VersionMutationResult::Conflict { current: None });
        };
        let predecessor_encoding = message_item
            .get("latest_payload_bytes")
            .and_then(|value| value.as_b().ok())
            .map(|bytes| EncodedVersionRecord::decode(bytes.as_ref()))
            .transpose()?;
        let current = if let Some(current_bytes) = message_item
            .get("latest_payload_bytes")
            .and_then(|value| value.as_b().ok())
        {
            self.decode_records(vec![current_bytes.as_ref().to_vec()])
                .await?
                .remove(0)
        } else {
            self.get_latest(&request.app_id, &request.channel, &request.message_serial)
                .await?
                .ok_or_else(|| {
                    Error::Internal("Message has no readable version entry".to_string())
                })?
        };
        let append_count = Self::item_num(&message_item, "append_count").unwrap_or(0) as usize;
        let delivery_serial = next_delivery.max(current.delivery_serial().saturating_add(1));
        let stream_id = format!("{}/{}", request.app_id, request.channel);
        let outcome = request.apply_to(&current, &stream_id, delivery_serial, append_count)?;
        let VersionMutationResult::Applied { record, .. } = outcome else {
            return Ok(outcome);
        };
        let opens = !current.is_open_ai_stream() && record.is_open_ai_stream();
        let closes = current.is_open_ai_stream() && !record.is_open_ai_stream();
        if opens
            && let Some(limit) = request.limits.max_open_streaming_messages_per_channel
            && open_count >= limit
        {
            return Ok(VersionMutationResult::Rejected(
                VersionMutationRejection::OpenStreamingMessages { limit },
            ));
        }
        let next_open = open_count
            .saturating_add(usize::from(opens))
            .saturating_sub(usize::from(closes));
        let next_append_count = append_count
            + usize::from(matches!(
                request.mutation,
                sockudo_core::version_store::VersionMutation::Append(_)
            ));
        let now_ms = sockudo_core::history::now_ms();
        let plan = EncodedVersionRecord::plan(
            &record,
            Some((
                &current,
                predecessor_encoding
                    .as_ref()
                    .and_then(|value| value.text.as_ref()),
            )),
        )?;
        let payload = plan.latest_bytes;
        let stream_update = Update::builder()
            .table_name(&self.tables.version_streams)
            .key("app_channel", Self::attr_s(&app_channel))
            .update_expression("SET next_delivery_serial = :next, open_stream_count = :new_open, newest_available_delivery_serial = :delivery, updated_at_ms = :now")
            .condition_expression("next_delivery_serial = :expected AND (attribute_not_exists(open_stream_count) OR open_stream_count = :open)")
            .expression_attribute_values(":next", Self::attr_n(delivery_serial + 1))
            .expression_attribute_values(":expected", Self::attr_n(next_delivery))
            .expression_attribute_values(":open", Self::attr_n(open_count))
            .expression_attribute_values(":new_open", Self::attr_n(next_open))
            .expression_attribute_values(":delivery", Self::attr_n(delivery_serial))
            .expression_attribute_values(":now", Self::attr_n(now_ms))
            .build()
            .map_err(|e| Error::Internal(format!("Failed to build stream mutation: {e}")))?;
        let entry_put = Put::builder()
            .table_name(&self.tables.version_entries)
            .set_item(Some(self.entry_item(
                &record,
                request.idempotency.as_ref(),
                Some(&plan.entry_bytes),
            )?))
            .condition_expression("attribute_not_exists(message_version_key)")
            .build()
            .map_err(|e| Error::Internal(format!("Failed to build mutation entry: {e}")))?;
        let message_update = Update::builder()
            .table_name(&self.tables.version_messages)
            .key("app_channel", Self::attr_s(&app_channel))
            .key(
                "message_serial",
                Self::attr_s(request.message_serial.as_str()),
            )
            .update_expression("SET latest_version_serial = :next_vs, latest_delivery_serial = :next_ds, latest_action = :action, latest_payload_bytes = :payload, append_count = :append_count, is_open_stream = :is_open, updated_at_ms = :now")
            .condition_expression("latest_version_serial = :expected_vs AND latest_delivery_serial = :expected_ds AND (append_count = :expected_append OR attribute_not_exists(append_count))")
            .expression_attribute_values(":next_vs", Self::attr_s(record.version_serial().as_str()))
            .expression_attribute_values(":next_ds", Self::attr_n(delivery_serial))
            .expression_attribute_values(":action", Self::attr_s(record.message.action.as_str()))
            .expression_attribute_values(":payload", Self::attr_b(payload.clone()))
            .expression_attribute_values(":append_count", Self::attr_n(next_append_count))
            .expression_attribute_values(":expected_append", Self::attr_n(append_count))
            .expression_attribute_values(":is_open", AttributeValue::Bool(record.is_open_ai_stream()))
            .expression_attribute_values(":now", Self::attr_n(now_ms))
            .expression_attribute_values(":expected_vs", Self::attr_s(current.version_serial().as_str()))
            .expression_attribute_values(":expected_ds", Self::attr_n(current.delivery_serial()))
            .build()
            .map_err(|e| Error::Internal(format!("Failed to build message mutation: {e}")))?;
        let mut transaction = self
            .client
            .transact_write_items()
            .transact_items(TransactWriteItem::builder().update(stream_update).build())
            .transact_items(TransactWriteItem::builder().put(entry_put).build())
            .transact_items(TransactWriteItem::builder().update(message_update).build());
        if let Some(operation) = request.idempotency.as_ref() {
            let mut receipt = HashMap::new();
            receipt.insert("app_channel".to_string(), Self::attr_s(&app_channel));
            receipt.insert(
                "message_version_key".to_string(),
                Self::attr_s(&Self::operation_receipt_key(&operation.cache_key)),
            );
            receipt.insert(
                "operation_fingerprint".to_string(),
                Self::attr_s(&operation.payload_fingerprint),
            );
            receipt.insert(
                "payload_bytes".to_string(),
                Self::attr_b(plan.entry_bytes.clone()),
            );
            let receipt_put = Put::builder()
                .table_name(&self.tables.version_entries)
                .set_item(Some(receipt))
                .condition_expression("attribute_not_exists(message_version_key)")
                .build()
                .map_err(|e| Error::Internal(format!("Failed to build operation receipt: {e}")))?;
            transaction =
                transaction.transact_items(TransactWriteItem::builder().put(receipt_put).build());
        }
        if let Some((reference, text)) = plan.snapshot {
            let mut snapshot = HashMap::from([
                ("app_channel".to_string(), Self::attr_s(&app_channel)),
                (
                    "message_version_key".to_string(),
                    Self::attr_s(&Self::text_key(&reference.snapshot_key)),
                ),
                ("text_data".to_string(), Self::attr_s(&text)),
            ]);
            // Existing operation receipts have no TTL. Their snapshot must
            // retain the same lifetime, even after ordinary versions expire.
            if !reference.retain_for_receipts
                && let Some(expiry) = self.expires_at_value()
            {
                snapshot.insert(Self::EXPIRES_AT_ATTR.to_string(), expiry);
            }
            let put = Put::builder()
                .table_name(&self.tables.version_entries)
                .set_item(Some(snapshot))
                .build()
                .map_err(|e| {
                    Error::Internal(format!("Failed to build version text snapshot: {e}"))
                })?;
            transaction = transaction.transact_items(TransactWriteItem::builder().put(put).build());
        }
        match transaction.send().await {
            Ok(_) => Ok(VersionMutationResult::Applied { record, stream_id }),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(transaction_is_conflict) =>
            {
                Ok(VersionMutationResult::Conflict {
                    current: self
                        .get_latest(&request.app_id, &request.channel, &request.message_serial)
                        .await?,
                })
            }
            Err(error) => Err(Error::Internal(format!(
                "Failed to transact version mutation: {error}"
            ))),
        }
    }

    async fn get_latest(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &sockudo_core::versioned_messages::MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        let app_channel = Self::app_channel_key(app_id, channel);
        if let Some(item) = self
            .client
            .get_item()
            .table_name(&self.tables.version_messages)
            .key("app_channel", Self::attr_s(&app_channel))
            .key("message_serial", Self::attr_s(message_serial.as_str()))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| Error::Internal(format!("Failed to read latest message: {e}")))?
            .item
            && let Some(bytes) = item
                .get("latest_payload_bytes")
                .and_then(|value| value.as_b().ok())
        {
            let record = self
                .decode_records(vec![bytes.as_ref().to_vec()])
                .await?
                .remove(0);
            return Ok(Some(record));
        }
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

        let record = self.decode_records(vec![bytes]).await?.remove(0);
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
        use aws_sdk_dynamodb::types::KeysAndAttributes;
        use std::collections::{BTreeMap, BTreeSet, HashMap};
        let requested: Vec<_> = message_serials
            .iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let app_channel = Self::app_channel_key(app_id, channel);
        let mut result = BTreeMap::new();
        for chunk in requested.chunks(100) {
            let keys = chunk
                .iter()
                .map(|serial| {
                    HashMap::from([
                        ("app_channel".to_owned(), Self::attr_s(&app_channel)),
                        ("message_serial".to_owned(), Self::attr_s(serial.as_str())),
                    ])
                })
                .collect();
            let batch = KeysAndAttributes::builder()
                .set_keys(Some(keys))
                .consistent_read(true)
                .build()
                .map_err(|e| {
                    Error::Internal(format!("failed to build latest version batch: {e}"))
                })?;
            let mut pending = HashMap::from([(self.tables.version_messages.clone(), batch)]);
            let mut attempt = 0;
            while !pending.is_empty() {
                let response = self
                    .client
                    .batch_get_item()
                    .set_request_items(Some(pending))
                    .send()
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("failed to read latest version batch: {e}"))
                    })?;
                if let Some(responses) = response.responses {
                    for item in responses.into_values().flatten() {
                        if let Some(bytes) = item
                            .get("latest_payload_bytes")
                            .and_then(|value| value.as_b().ok())
                        {
                            let record = self
                                .decode_records(vec![bytes.as_ref().to_vec()])
                                .await?
                                .remove(0);
                            result.insert(record.message_serial().clone(), record);
                        }
                    }
                }
                pending = response.unprocessed_keys.unwrap_or_default();
                pending.retain(|_, batch| !batch.keys().is_empty());
                if !pending.is_empty() {
                    attempt += 1;
                    if attempt >= 8 {
                        return Err(Error::Internal(
                            "latest version batch remained unprocessed after retries".into(),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        (10u64 << attempt).min(500),
                    ))
                    .await;
                }
            }
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
        let payloads: Vec<Vec<u8>> = all_items
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
                Ok(bytes)
            })
            .collect::<Result<Vec<_>>>()?;
        let items = self.decode_records(payloads).await?;

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

        let payloads = result
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
                Ok(bytes)
            })
            .collect::<Result<Vec<_>>>()?;
        self.decode_records(payloads).await
    }

    async fn latest_by_history(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Vec<StoredVersionRecord>> {
        let mut continuation = None;
        let mut result = Vec::new();
        loop {
            let page = self
                .client
                .query()
                .table_name(&self.tables.version_messages)
                .key_condition_expression("app_channel = :ac")
                .expression_attribute_values(
                    ":ac",
                    Self::attr_s(&Self::app_channel_key(app_id, channel)),
                )
                .projection_expression("message_serial")
                .consistent_read(true)
                .limit(100)
                .set_exclusive_start_key(continuation)
                .send()
                .await
                .map_err(|e| Error::Internal(format!("Failed to query version messages: {e}")))?;
            let ids = page
                .items()
                .iter()
                .map(|item| {
                    Self::item_str(item, "message_serial")
                        .ok_or_else(|| {
                            Error::Internal("Version message is missing its serial".into())
                        })
                        .and_then(sockudo_core::versioned_messages::MessageSerial::new)
                })
                .collect::<Result<Vec<_>>>()?;
            result.extend(
                self.get_latest_batch(app_id, channel, &ids)
                    .await?
                    .into_values(),
            );
            continuation = page.last_evaluated_key.filter(|key| !key.is_empty());
            if continuation.is_none() {
                break;
            }
        }
        result.sort_by_key(StoredVersionRecord::history_serial);
        Ok(result)
    }

    async fn message_count(&self, app_id: &str, channel: &str) -> Result<u64> {
        let mut continuation = None;
        let mut count = 0u64;
        loop {
            let page = self
                .client
                .query()
                .table_name(&self.tables.version_messages)
                .key_condition_expression("app_channel = :ac")
                .expression_attribute_values(
                    ":ac",
                    Self::attr_s(&Self::app_channel_key(app_id, channel)),
                )
                .select(aws_sdk_dynamodb::types::Select::Count)
                .consistent_read(true)
                .set_exclusive_start_key(continuation)
                .send()
                .await
                .map_err(|e| Error::Internal(format!("Failed to count version messages: {e}")))?;
            count = count.saturating_add(page.count().max(0) as u64);
            continuation = page.last_evaluated_key.filter(|key| !key.is_empty());
            if continuation.is_none() {
                return Ok(count);
            }
        }
    }

    async fn active_stream_count(&self, app_id: &str, channel: &str) -> Result<usize> {
        let mut continuation = None;
        let mut count = 0usize;
        loop {
            let page = self
                .client
                .query()
                .table_name(&self.tables.version_messages)
                .key_condition_expression("app_channel = :ac")
                .expression_attribute_values(
                    ":ac",
                    Self::attr_s(&Self::app_channel_key(app_id, channel)),
                )
                .projection_expression("message_serial, is_open_stream")
                .consistent_read(true)
                .limit(100)
                .set_exclusive_start_key(continuation)
                .send()
                .await
                .map_err(|e| {
                    Error::Internal(format!("failed to count active version streams: {e}"))
                })?;
            for item in page.items() {
                // Every supported writer commits this boolean with its latest
                // pointer, including out-of-order raw imports. Older rows that
                // predate the metadata use an authoritative point read.
                let open = if let Some(open) = item
                    .get("is_open_stream")
                    .and_then(|value| value.as_bool().ok())
                {
                    *open
                } else {
                    let serial = Self::item_str(item, "message_serial").ok_or_else(|| {
                        Error::Internal("version message is missing its serial".into())
                    })?;
                    let serial = sockudo_core::versioned_messages::MessageSerial::new(serial)?;
                    self.get_latest(app_id, channel, &serial)
                        .await?
                        .is_some_and(|record| record.is_open_ai_stream())
                };
                count += usize::from(open);
            }
            continuation = page.last_evaluated_key.filter(|key| !key.is_empty());
            if continuation.is_none() {
                return Ok(count);
            }
        }
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
