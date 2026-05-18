use crate::error::{Error, Result};
use crate::history::now_ms;
use crate::versioned_messages::{
    MessageSerial, VersionSerial, VersionedMessage, validate_replay_continuity,
    validate_version_chain,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VersionStoreDirection {
    NewestFirst,
    OldestFirst,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionStoreCursor {
    pub version: u8,
    pub version_serial: VersionSerial,
    pub direction: VersionStoreDirection,
}

#[derive(Debug, Clone)]
pub struct VersionStoreReadRequest {
    pub app_id: String,
    pub channel: String,
    pub message_serial: MessageSerial,
    pub direction: VersionStoreDirection,
    pub limit: usize,
    pub cursor: Option<VersionStoreCursor>,
}

impl VersionStoreReadRequest {
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0 {
            return Err(Error::InvalidMessageFormat(
                "version-history limit must be greater than 0".to_string(),
            ));
        }

        if let Some(cursor) = self.cursor.as_ref() {
            if cursor.version != 1 {
                return Err(Error::InvalidMessageFormat(format!(
                    "unsupported version-history cursor version: {}",
                    cursor.version
                )));
            }
            if cursor.direction != self.direction {
                return Err(Error::InvalidMessageFormat(
                    "version-history cursor direction does not match request".to_string(),
                ));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct VersionStorePage {
    pub items: Vec<StoredVersionRecord>,
    pub next_cursor: Option<VersionStoreCursor>,
    pub has_more: bool,
}

#[derive(Debug, Clone)]
pub struct VersionWriteReservation {
    pub stream_id: String,
    pub delivery_serial: u64,
}

#[derive(Debug, Clone, Default)]
pub struct VersionStreamState {
    pub stream_id: Option<String>,
    pub next_delivery_serial: Option<u64>,
    pub oldest_available_delivery_serial: Option<u64>,
    pub newest_available_delivery_serial: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct VersionReplayRequest {
    pub app_id: String,
    pub channel: String,
    pub after_delivery_serial: u64,
    pub limit: usize,
}

impl VersionReplayRequest {
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0 {
            return Err(Error::InvalidMessageFormat(
                "replay limit must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredVersionRecord {
    pub app_id: String,
    pub channel: String,
    pub original_client_id: Option<String>,
    pub message: VersionedMessage,
}

impl StoredVersionRecord {
    pub fn message_serial(&self) -> &MessageSerial {
        &self.message.identity.message_serial
    }

    pub fn version_serial(&self) -> &VersionSerial {
        &self.message.version.serial
    }

    pub fn history_serial(&self) -> u64 {
        self.message.identity.history_serial
    }

    pub fn delivery_serial(&self) -> u64 {
        self.message.replay_position.delivery_serial
    }
}

#[async_trait]
pub trait VersionStore: Send + Sync {
    async fn reserve_delivery_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<VersionWriteReservation>;

    async fn append_version(&self, record: StoredVersionRecord) -> Result<()>;

    async fn get_latest(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &MessageSerial,
    ) -> Result<Option<StoredVersionRecord>>;

    async fn get_versions(&self, request: VersionStoreReadRequest) -> Result<VersionStorePage>;

    async fn replay_after(&self, request: VersionReplayRequest)
    -> Result<Vec<StoredVersionRecord>>;

    async fn latest_by_history(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Vec<StoredVersionRecord>>;

    async fn stream_state(&self, app_id: &str, channel: &str) -> Result<VersionStreamState>;

    /// Purge version entries whose server-side `created_at_ms` is strictly
    /// older than `before_ms`. Backends with native TTL (ScyllaDB, DynamoDB)
    /// return `(0, false)` — the storage engine handles expiry asynchronously.
    ///
    /// `batch_size` caps the rows deleted per call so transaction/lock sizes
    /// stay bounded. Returns `(rows_deleted, has_more)`; callers loop while
    /// `has_more` is true, subject to a caller-supplied per-tick budget.
    async fn purge_before(&self, before_ms: i64, batch_size: usize) -> Result<(u64, bool)> {
        let _ = (before_ms, batch_size);
        Ok((0, false))
    }
}

#[derive(Default)]
pub struct NoopVersionStore;

#[async_trait]
impl VersionStore for NoopVersionStore {
    async fn reserve_delivery_position(
        &self,
        _app_id: &str,
        _channel: &str,
    ) -> Result<VersionWriteReservation> {
        Err(Error::Configuration(
            "Versioned message storage is not configured".to_string(),
        ))
    }

    async fn append_version(&self, _record: StoredVersionRecord) -> Result<()> {
        Err(Error::Configuration(
            "Versioned message storage is not configured".to_string(),
        ))
    }

    async fn get_latest(
        &self,
        _app_id: &str,
        _channel: &str,
        _message_serial: &MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        Err(Error::Configuration(
            "Versioned message storage is not configured".to_string(),
        ))
    }

    async fn get_versions(&self, _request: VersionStoreReadRequest) -> Result<VersionStorePage> {
        Err(Error::Configuration(
            "Versioned message storage is not configured".to_string(),
        ))
    }

    async fn replay_after(
        &self,
        _request: VersionReplayRequest,
    ) -> Result<Vec<StoredVersionRecord>> {
        Err(Error::Configuration(
            "Versioned message storage is not configured".to_string(),
        ))
    }

    async fn latest_by_history(
        &self,
        _app_id: &str,
        _channel: &str,
    ) -> Result<Vec<StoredVersionRecord>> {
        Err(Error::Configuration(
            "Versioned message storage is not configured".to_string(),
        ))
    }

    async fn stream_state(&self, _app_id: &str, _channel: &str) -> Result<VersionStreamState> {
        Err(Error::Configuration(
            "Versioned message storage is not configured".to_string(),
        ))
    }
}

#[derive(Clone, Default)]
pub struct MemoryVersionStore {
    channels: Arc<RwLock<BTreeMap<String, MemoryVersionChannel>>>,
}

#[derive(Clone)]
struct MemoryVersionChannel {
    stream_id: String,
    next_delivery_serial: u64,
    messages: BTreeMap<String, Vec<StoredVersionRecord>>,
    replay: BTreeMap<u64, StoredVersionRecord>,
    // Parallel map: `delivery_serial -> server-side append time (ms)`.
    // Used by `purge_before` for TTL eviction without touching read paths.
    created_at: BTreeMap<u64, i64>,
}

impl Default for MemoryVersionChannel {
    fn default() -> Self {
        Self {
            stream_id: uuid::Uuid::new_v4().to_string(),
            next_delivery_serial: 1,
            messages: BTreeMap::new(),
            replay: BTreeMap::new(),
            created_at: BTreeMap::new(),
        }
    }
}

impl MemoryVersionStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn channel_key(app_id: &str, channel: &str) -> String {
        format!("{app_id}\0{channel}")
    }
}

#[async_trait]
impl VersionStore for MemoryVersionStore {
    async fn reserve_delivery_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<VersionWriteReservation> {
        let key = Self::channel_key(app_id, channel);
        let mut channels = self.channels.write().await;
        let channel_state = channels.entry(key).or_default();
        let reservation = VersionWriteReservation {
            stream_id: channel_state.stream_id.clone(),
            delivery_serial: channel_state.next_delivery_serial,
        };
        channel_state.next_delivery_serial = channel_state.next_delivery_serial.saturating_add(1);
        Ok(reservation)
    }

    async fn append_version(&self, record: StoredVersionRecord) -> Result<()> {
        let key = Self::channel_key(&record.app_id, &record.channel);
        let mut channels = self.channels.write().await;
        let channel_state = channels.entry(key).or_default();

        if let Some(existing) = channel_state.replay.get(&record.delivery_serial()) {
            return Err(Error::InvalidMessageFormat(format!(
                "duplicate delivery_serial {} in version replay log for {}:{} (existing message_serial {}, incoming {})",
                record.delivery_serial(),
                record.app_id,
                record.channel,
                existing.message_serial().as_str(),
                record.message_serial().as_str()
            )));
        }

        let tentative_chain = channel_state
            .messages
            .get(record.message_serial().as_str())
            .cloned()
            .unwrap_or_default();
        let mut validated_chain = tentative_chain;
        validated_chain.push(record.clone());
        validate_version_chain(
            &validated_chain
                .iter()
                .map(|entry| entry.message.clone())
                .collect::<Vec<_>>(),
        )?;

        channel_state.messages.insert(
            record.message_serial().as_str().to_string(),
            validated_chain,
        );
        channel_state
            .created_at
            .insert(record.delivery_serial(), now_ms());
        channel_state
            .replay
            .insert(record.delivery_serial(), record.clone());
        channel_state.next_delivery_serial = channel_state
            .next_delivery_serial
            .max(record.delivery_serial().saturating_add(1));

        Ok(())
    }

    async fn get_latest(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        let key = Self::channel_key(app_id, channel);
        let channels = self.channels.read().await;
        let Some(channel_state) = channels.get(&key) else {
            return Ok(None);
        };
        let Some(chain) = channel_state.messages.get(message_serial.as_str()) else {
            return Ok(None);
        };

        let latest = chain
            .iter()
            .max_by(|left, right| left.version_serial().cmp(right.version_serial()))
            .cloned()
            .ok_or_else(|| Error::InvalidMessageFormat("version chain must not be empty".into()))?;

        Ok(Some(latest))
    }

    async fn get_versions(&self, request: VersionStoreReadRequest) -> Result<VersionStorePage> {
        request.validate()?;
        let key = Self::channel_key(&request.app_id, &request.channel);
        let channels = self.channels.read().await;
        let Some(channel_state) = channels.get(&key) else {
            return Ok(VersionStorePage {
                items: Vec::new(),
                next_cursor: None,
                has_more: false,
            });
        };
        let Some(chain) = channel_state.messages.get(request.message_serial.as_str()) else {
            return Ok(VersionStorePage {
                items: Vec::new(),
                next_cursor: None,
                has_more: false,
            });
        };

        let mut items = chain.clone();
        items.sort_by(|left, right| left.version_serial().cmp(right.version_serial()));
        if matches!(request.direction, VersionStoreDirection::NewestFirst) {
            items.reverse();
        }

        let filtered: Vec<StoredVersionRecord> = items
            .into_iter()
            .filter(|item| {
                request
                    .cursor
                    .as_ref()
                    .is_none_or(|cursor| match request.direction {
                        VersionStoreDirection::NewestFirst => {
                            item.version_serial() < &cursor.version_serial
                        }
                        VersionStoreDirection::OldestFirst => {
                            item.version_serial() > &cursor.version_serial
                        }
                    })
            })
            .take(request.limit + 1)
            .collect();

        let has_more = filtered.len() > request.limit;
        let items: Vec<StoredVersionRecord> = filtered.into_iter().take(request.limit).collect();
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
        let key = Self::channel_key(&request.app_id, &request.channel);
        let channels = self.channels.read().await;
        let Some(channel_state) = channels.get(&key) else {
            return Ok(Vec::new());
        };

        let items: Vec<StoredVersionRecord> = channel_state
            .replay
            .range((request.after_delivery_serial.saturating_add(1))..)
            .map(|(_, value)| value.clone())
            .take(request.limit)
            .collect();

        validate_replay_continuity(
            &items
                .iter()
                .map(|entry| entry.message.clone())
                .collect::<Vec<_>>(),
            request.after_delivery_serial,
        )?;

        Ok(items)
    }

    async fn latest_by_history(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Vec<StoredVersionRecord>> {
        let key = Self::channel_key(app_id, channel);
        let channels = self.channels.read().await;
        let Some(channel_state) = channels.get(&key) else {
            return Ok(Vec::new());
        };

        let mut latest = channel_state
            .messages
            .values()
            .filter_map(|chain| {
                chain
                    .iter()
                    .max_by(|left, right| left.version_serial().cmp(right.version_serial()))
                    .cloned()
            })
            .collect::<Vec<_>>();

        latest.sort_by_key(StoredVersionRecord::history_serial);
        Ok(latest)
    }

    async fn stream_state(&self, app_id: &str, channel: &str) -> Result<VersionStreamState> {
        let key = Self::channel_key(app_id, channel);
        let channels = self.channels.read().await;
        let Some(channel_state) = channels.get(&key) else {
            return Ok(VersionStreamState::default());
        };

        Ok(VersionStreamState {
            stream_id: Some(channel_state.stream_id.clone()),
            next_delivery_serial: Some(channel_state.next_delivery_serial),
            oldest_available_delivery_serial: channel_state
                .replay
                .first_key_value()
                .map(|(k, _)| *k),
            newest_available_delivery_serial: channel_state
                .replay
                .last_key_value()
                .map(|(k, _)| *k),
        })
    }

    async fn purge_before(&self, before_ms: i64, batch_size: usize) -> Result<(u64, bool)> {
        if batch_size == 0 {
            return Ok((0, false));
        }
        let mut channels = self.channels.write().await;
        let mut deleted: u64 = 0;
        let mut has_more = false;

        for state in channels.values_mut() {
            let remaining = batch_size.saturating_sub(deleted as usize);
            if remaining == 0 {
                has_more = true;
                break;
            }

            let mut to_remove: Vec<u64> = Vec::new();
            for (&delivery_serial, &created_ms) in state.created_at.iter() {
                if created_ms >= before_ms {
                    break;
                }
                if to_remove.len() >= remaining {
                    has_more = true;
                    break;
                }
                to_remove.push(delivery_serial);
            }

            for delivery_serial in to_remove {
                state.created_at.remove(&delivery_serial);
                let Some(record) = state.replay.remove(&delivery_serial) else {
                    continue;
                };
                let message_key = record.message_serial().as_str().to_string();
                if let Some(chain) = state.messages.get_mut(&message_key) {
                    chain.retain(|entry| entry.version_serial() != record.version_serial());
                    if chain.is_empty() {
                        state.messages.remove(&message_key);
                    }
                }
                deleted += 1;
            }

            if !has_more
                && state
                    .created_at
                    .iter()
                    .next()
                    .is_some_and(|(_, &ts)| ts < before_ms)
            {
                has_more = true;
            }
        }

        Ok((deleted, has_more))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::versioned_messages::{
        FieldPatch, MessageAction, MessageFieldDelta, MessageSerial, VersionMetadata, VersionSerial,
    };
    use sockudo_protocol::messages::{MessageData, MessageExtras};

    fn version(serial: &str, timestamp_ms: i64) -> VersionMetadata {
        VersionMetadata {
            serial: VersionSerial::new(serial).unwrap(),
            client_id: Some("user-1".to_string()),
            timestamp_ms,
            description: None,
            metadata: None,
        }
    }

    fn base_record(
        message_serial: &str,
        history_serial: u64,
        delivery_serial: u64,
    ) -> StoredVersionRecord {
        StoredVersionRecord {
            app_id: "app".to_string(),
            channel: "chat".to_string(),
            original_client_id: Some("user-1".to_string()),
            message: VersionedMessage::new_create(
                MessageSerial::new(message_serial).unwrap(),
                version("ver:1", 1),
                history_serial,
                delivery_serial,
                Some("chat.message".to_string()),
                Some(MessageData::String("hello".to_string())),
                Some(MessageExtras {
                    headers: None,
                    ephemeral: Some(false),
                    idempotency_key: None,
                    push: None,
                    echo: None,
                }),
            ),
        }
    }

    #[tokio::test]
    async fn memory_store_returns_latest_visible_by_version_serial() {
        let store = MemoryVersionStore::new();
        let create = base_record("msg:1", 10, 1);
        store.append_version(create.clone()).await.unwrap();

        let update = StoredVersionRecord {
            message: create
                .message
                .apply_mutation(
                    MessageAction::Update,
                    version("ver:9", 2),
                    2,
                    MessageFieldDelta {
                        data: FieldPatch::Replace(MessageData::String("patched".to_string())),
                        ..Default::default()
                    },
                )
                .unwrap(),
            ..create.clone()
        };
        store.append_version(update.clone()).await.unwrap();

        let latest = store
            .get_latest("app", "chat", &MessageSerial::new("msg:1").unwrap())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(latest.version_serial().as_str(), "ver:9");
        assert_eq!(
            latest.message.data.unwrap().into_string().as_deref(),
            Some("patched")
        );
    }

    #[tokio::test]
    async fn memory_store_pages_version_history() {
        let store = MemoryVersionStore::new();
        let create = base_record("msg:1", 10, 1);
        store.append_version(create.clone()).await.unwrap();

        let update_1 = StoredVersionRecord {
            message: create
                .message
                .apply_mutation(
                    MessageAction::Update,
                    version("ver:2", 2),
                    2,
                    MessageFieldDelta::default(),
                )
                .unwrap(),
            ..create.clone()
        };
        let update_2 = StoredVersionRecord {
            message: update_1
                .message
                .apply_mutation(
                    MessageAction::Delete,
                    version("ver:3", 3),
                    3,
                    MessageFieldDelta::default(),
                )
                .unwrap(),
            ..create.clone()
        };

        store.append_version(update_1).await.unwrap();
        store.append_version(update_2).await.unwrap();

        let page = store
            .get_versions(VersionStoreReadRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                message_serial: MessageSerial::new("msg:1").unwrap(),
                direction: VersionStoreDirection::NewestFirst,
                limit: 2,
                cursor: None,
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert!(page.has_more);
        assert_eq!(page.items[0].version_serial().as_str(), "ver:3");
        assert_eq!(page.items[1].version_serial().as_str(), "ver:2");
        assert!(page.next_cursor.is_some());
    }

    #[tokio::test]
    async fn memory_store_projects_latest_by_history_order() {
        let store = MemoryVersionStore::new();
        let first = base_record("msg:1", 10, 1);
        let second = base_record("msg:2", 20, 2);
        store.append_version(second.clone()).await.unwrap();
        store.append_version(first.clone()).await.unwrap();

        let latest = store.latest_by_history("app", "chat").await.unwrap();
        assert_eq!(latest.len(), 2);
        assert_eq!(latest[0].message_serial().as_str(), "msg:1");
        assert_eq!(latest[1].message_serial().as_str(), "msg:2");
    }

    #[tokio::test]
    async fn memory_store_replays_in_delivery_order() {
        let store = MemoryVersionStore::new();
        let first = base_record("msg:1", 10, 1);
        let second = base_record("msg:2", 20, 2);
        store.append_version(first).await.unwrap();
        store.append_version(second).await.unwrap();

        let replay = store
            .replay_after(VersionReplayRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                after_delivery_serial: 0,
                limit: 10,
            })
            .await
            .unwrap();

        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].delivery_serial(), 1);
        assert_eq!(replay[1].delivery_serial(), 2);
    }

    #[tokio::test]
    async fn memory_store_reserves_delivery_positions_with_stable_stream_id() {
        let store = MemoryVersionStore::new();
        let first = store
            .reserve_delivery_position("app", "chat")
            .await
            .unwrap();
        let second = store
            .reserve_delivery_position("app", "chat")
            .await
            .unwrap();

        assert_eq!(first.stream_id, second.stream_id);
        assert_eq!(first.delivery_serial, 1);
        assert_eq!(second.delivery_serial, 2);
    }

    #[tokio::test]
    async fn memory_store_rejects_duplicate_channel_delivery_serial() {
        let store = MemoryVersionStore::new();
        let first = base_record("msg:1", 10, 1);
        let second = base_record("msg:2", 20, 1);
        store.append_version(first).await.unwrap();

        let error = store.append_version(second).await.unwrap_err();
        assert!(
            error.to_string().contains("duplicate delivery_serial"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn memory_store_rejects_invalid_append_without_corrupting_chain() {
        let store = MemoryVersionStore::new();
        let create = base_record("msg:1", 10, 1);
        store.append_version(create.clone()).await.unwrap();

        let mut invalid = StoredVersionRecord {
            message: create
                .message
                .apply_mutation(
                    MessageAction::Update,
                    version("ver:2", 2),
                    2,
                    MessageFieldDelta::default(),
                )
                .unwrap(),
            ..create.clone()
        };
        invalid.message.identity.history_serial = 99;

        let error = store.append_version(invalid).await.unwrap_err();
        assert!(
            error.to_string().contains("mixed history_serial"),
            "unexpected error: {error}"
        );

        let latest = store
            .get_latest("app", "chat", &MessageSerial::new("msg:1").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.version_serial().as_str(), "ver:1");
        assert_eq!(latest.history_serial(), 10);

        let replay = store
            .replay_after(VersionReplayRequest {
                app_id: "app".to_string(),
                channel: "chat".to_string(),
                after_delivery_serial: 0,
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].version_serial().as_str(), "ver:1");
    }
}
