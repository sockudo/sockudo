use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::HistoryDurableState;
use std::collections::HashMap;

use super::{DynamoDbHistoryStore, StoredEntryRecord, StoredStreamRecord};

impl DynamoDbHistoryStore {
    pub(super) fn stream_key(app_id: &str, channel: &str) -> String {
        deterministic_key([app_id, channel].into_iter())
    }

    pub(super) fn stream_partition(app_id: &str, channel: &str, stream_id: &str) -> String {
        deterministic_key([app_id, channel, stream_id].into_iter())
    }

    pub(super) fn serial_key(serial: u64) -> String {
        format!("{serial:020}")
    }

    pub(super) fn published_at_serial_key(published_at_ms: i64, serial: u64) -> String {
        format!("{published_at_ms:020}#{serial:020}")
    }

    pub(super) fn attr_string(value: &str) -> AttributeValue {
        AttributeValue::S(value.to_string())
    }

    pub(super) fn attr_number(value: impl ToString) -> AttributeValue {
        AttributeValue::N(value.to_string())
    }

    pub(super) fn stream_item(
        stream_key: &str,
        record: &StoredStreamRecord,
    ) -> HashMap<String, AttributeValue> {
        let mut item = HashMap::new();
        item.insert("stream_key".to_string(), Self::attr_string(stream_key));
        item.insert("app_id".to_string(), Self::attr_string(&record.app_id));
        item.insert("channel".to_string(), Self::attr_string(&record.channel));
        item.insert(
            "stream_id".to_string(),
            Self::attr_string(&record.stream_id),
        );
        item.insert(
            "next_serial".to_string(),
            Self::attr_number(record.next_serial),
        );
        item.insert(
            "retained_messages".to_string(),
            Self::attr_number(record.retained_messages),
        );
        item.insert(
            "retained_bytes".to_string(),
            Self::attr_number(record.retained_bytes),
        );
        item.insert(
            "durable_state".to_string(),
            Self::attr_string(record.durable_state.as_str()),
        );
        item.insert(
            "updated_at_ms".to_string(),
            Self::attr_number(record.updated_at_ms),
        );
        if let Some(reason) = &record.durable_state_reason {
            item.insert(
                "durable_state_reason".to_string(),
                Self::attr_string(reason),
            );
        }
        if let Some(node_id) = &record.durable_state_node_id {
            item.insert(
                "durable_state_node_id".to_string(),
                Self::attr_string(node_id),
            );
        }
        if let Some(changed_at) = record.durable_state_changed_at_ms {
            item.insert(
                "durable_state_changed_at_ms".to_string(),
                Self::attr_number(changed_at),
            );
        }
        if let Some(value) = record.oldest_available_serial {
            item.insert(
                "oldest_available_serial".to_string(),
                Self::attr_number(value),
            );
        }
        if let Some(value) = record.newest_available_serial {
            item.insert(
                "newest_available_serial".to_string(),
                Self::attr_number(value),
            );
        }
        if let Some(value) = record.oldest_available_published_at_ms {
            item.insert(
                "oldest_available_published_at_ms".to_string(),
                Self::attr_number(value),
            );
        }
        if let Some(value) = record.newest_available_published_at_ms {
            item.insert(
                "newest_available_published_at_ms".to_string(),
                Self::attr_number(value),
            );
        }
        item
    }

    pub(super) fn entry_item(
        stream_partition: &str,
        record: &StoredEntryRecord,
    ) -> HashMap<String, AttributeValue> {
        let mut item = HashMap::new();
        item.insert(
            "stream_partition".to_string(),
            Self::attr_string(stream_partition),
        );
        item.insert(
            "serial_key".to_string(),
            Self::attr_string(&Self::serial_key(record.serial)),
        );
        item.insert("app_id".to_string(), Self::attr_string(&record.app_id));
        item.insert("channel".to_string(), Self::attr_string(&record.channel));
        item.insert(
            "stream_id".to_string(),
            Self::attr_string(&record.stream_id),
        );
        item.insert("serial".to_string(), Self::attr_number(record.serial));
        item.insert(
            "published_at_ms".to_string(),
            Self::attr_number(record.published_at_ms),
        );
        item.insert(
            "published_at_serial_key".to_string(),
            Self::attr_string(&Self::published_at_serial_key(
                record.published_at_ms,
                record.serial,
            )),
        );
        item.insert(
            "operation_kind".to_string(),
            Self::attr_string(&record.operation_kind),
        );
        item.insert(
            "payload_bytes".to_string(),
            AttributeValue::B(Blob::new(record.payload_bytes.clone())),
        );
        item.insert(
            "payload_size_bytes".to_string(),
            Self::attr_number(record.payload_size_bytes),
        );
        if let Some(message_id) = &record.message_id {
            item.insert("message_id".to_string(), Self::attr_string(message_id));
        }
        if let Some(event_name) = &record.event_name {
            item.insert("event_name".to_string(), Self::attr_string(event_name));
        }
        item
    }

    fn item_attr_string(item: &HashMap<String, AttributeValue>, key: &str) -> Result<String> {
        match item.get(key) {
            Some(AttributeValue::S(value)) => Ok(value.clone()),
            _ => Err(Error::Internal(format!(
                "Missing or invalid DynamoDB string attribute {key}"
            ))),
        }
    }

    fn item_attr_u64(item: &HashMap<String, AttributeValue>, key: &str) -> Result<u64> {
        match item.get(key) {
            Some(AttributeValue::N(value)) => value.parse::<u64>().map_err(|e| {
                Error::Internal(format!("Invalid DynamoDB numeric attribute {key}: {e}"))
            }),
            _ => Err(Error::Internal(format!(
                "Missing or invalid DynamoDB numeric attribute {key}"
            ))),
        }
    }

    fn item_attr_i64(item: &HashMap<String, AttributeValue>, key: &str) -> Result<i64> {
        match item.get(key) {
            Some(AttributeValue::N(value)) => value.parse::<i64>().map_err(|e| {
                Error::Internal(format!("Invalid DynamoDB numeric attribute {key}: {e}"))
            }),
            _ => Err(Error::Internal(format!(
                "Missing or invalid DynamoDB numeric attribute {key}"
            ))),
        }
    }

    fn item_opt_string(item: &HashMap<String, AttributeValue>, key: &str) -> Option<String> {
        match item.get(key) {
            Some(AttributeValue::S(value)) => Some(value.clone()),
            _ => None,
        }
    }

    fn item_opt_i64(item: &HashMap<String, AttributeValue>, key: &str) -> Option<i64> {
        match item.get(key) {
            Some(AttributeValue::N(value)) => value.parse::<i64>().ok(),
            _ => None,
        }
    }

    pub(super) fn stream_from_item(
        item: HashMap<String, AttributeValue>,
    ) -> Result<StoredStreamRecord> {
        Ok(StoredStreamRecord {
            app_id: Self::item_attr_string(&item, "app_id")?,
            channel: Self::item_attr_string(&item, "channel")?,
            stream_id: Self::item_attr_string(&item, "stream_id")?,
            next_serial: Self::item_attr_u64(&item, "next_serial")?,
            retained_messages: Self::item_attr_u64(&item, "retained_messages").unwrap_or(0),
            retained_bytes: Self::item_attr_u64(&item, "retained_bytes").unwrap_or(0),
            oldest_available_serial: Self::item_opt_i64(&item, "oldest_available_serial")
                .map(|value| value as u64),
            newest_available_serial: Self::item_opt_i64(&item, "newest_available_serial")
                .map(|value| value as u64),
            oldest_available_published_at_ms: Self::item_opt_i64(
                &item,
                "oldest_available_published_at_ms",
            ),
            newest_available_published_at_ms: Self::item_opt_i64(
                &item,
                "newest_available_published_at_ms",
            ),
            durable_state: parse_history_durable_state(&Self::item_attr_string(
                &item,
                "durable_state",
            )?),
            durable_state_reason: Self::item_opt_string(&item, "durable_state_reason"),
            durable_state_node_id: Self::item_opt_string(&item, "durable_state_node_id"),
            durable_state_changed_at_ms: Self::item_opt_i64(&item, "durable_state_changed_at_ms"),
            updated_at_ms: Self::item_attr_i64(&item, "updated_at_ms")?,
        })
    }

    pub(super) fn entry_from_item(
        item: HashMap<String, AttributeValue>,
    ) -> Result<StoredEntryRecord> {
        Ok(StoredEntryRecord {
            app_id: Self::item_attr_string(&item, "app_id")?,
            channel: Self::item_attr_string(&item, "channel")?,
            stream_id: Self::item_attr_string(&item, "stream_id")?,
            serial: Self::item_attr_u64(&item, "serial")?,
            published_at_ms: Self::item_attr_i64(&item, "published_at_ms")?,
            message_id: Self::item_opt_string(&item, "message_id"),
            event_name: Self::item_opt_string(&item, "event_name"),
            operation_kind: Self::item_attr_string(&item, "operation_kind")?,
            payload_bytes: match item.get("payload_bytes") {
                Some(AttributeValue::B(value)) => value.clone().into_inner(),
                _ => {
                    return Err(Error::Internal(
                        "Missing or invalid DynamoDB binary attribute payload_bytes".to_string(),
                    ));
                }
            },
            payload_size_bytes: Self::item_attr_u64(&item, "payload_size_bytes")?,
        })
    }
}

pub(super) fn parse_history_durable_state(raw: &str) -> HistoryDurableState {
    match raw {
        "healthy" => HistoryDurableState::Healthy,
        "reset_required" => HistoryDurableState::ResetRequired,
        "degraded" => HistoryDurableState::Degraded,
        _ => HistoryDurableState::Degraded,
    }
}

fn deterministic_key<I, S>(parts: I) -> String
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let mut key = String::new();
    for part in parts {
        let part = part.as_ref();
        key.push_str(&part.len().to_string());
        key.push(':');
        key.push_str(part);
        key.push('|');
    }
    key
}
