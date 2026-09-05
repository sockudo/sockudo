use super::store::VersionStore;
use super::types::*;
use crate::error::{Error, Result};
use crate::history::now_ms;
use crate::versioned_messages::{
    MessageAction, MessageSerial, VersionSerial, validate_replay_continuity_iter,
    validate_version_chain,
};
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::{Bound, Deref};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Default)]
pub struct MemoryVersionStore {
    channels: Arc<RwLock<BTreeMap<String, Arc<RwLock<MemoryVersionChannel>>>>>,
}

#[derive(Clone)]
struct MemoryVersionChannel {
    stream_id: String,
    next_delivery_serial: u64,
    messages: BTreeMap<String, VersionChain>,
    replay: BTreeMap<u64, Arc<StoredRevision>>,
    // Parallel map: `delivery_serial -> server-side append time (ms)`.
    // Used by `purge_before` for TTL eviction without touching read paths.
    created_at: BTreeMap<u64, i64>,
}

// All retained records share chain identity. Imports need not arrive in serial
// order, so latest and pagination use canonical version serials, never arrival.
#[derive(Clone, Default)]
struct VersionChain {
    records: BTreeMap<VersionSerial, Arc<StoredRevision>>,
    latest: Option<Arc<StoredVersionRecord>>,
    append_count: usize,
    operations: BTreeMap<String, VecDeque<Arc<StoredRevision>>>,
}

/// A full public revision is metadata plus a prefix of an append-run snapshot.
/// The snapshot only grows while its latest prefix remains authoritative, so
/// older revisions remain immutable without copying their accumulated strings.
#[derive(Clone)]
struct StoredRevision {
    metadata: StoredVersionRecord,
    text: Option<Arc<parking_lot::RwLock<String>>>,
    text_len: usize,
    envelope_text: bool,
}

impl Deref for StoredRevision {
    type Target = StoredVersionRecord;
    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

impl StoredRevision {
    fn new(record: &StoredVersionRecord, predecessor: Option<&Self>) -> Self {
        use crate::message_envelope::MessageContent;
        use sockudo_protocol::messages::MessageData;
        let mut metadata = record.clone();
        let Some(MessageData::String(data)) = metadata.message.data.take() else {
            return Self {
                metadata: record.clone(),
                text: None,
                text_len: 0,
                envelope_text: false,
            };
        };
        let envelope_text = metadata.envelope.as_ref().is_some_and(
            |envelope| matches!(&envelope.data, Some(MessageContent::Text(text)) if text == &data),
        );
        if envelope_text && let Some(envelope) = metadata.envelope.as_mut() {
            envelope.data = None;
        }
        let text_len = data.len();
        let shared = predecessor.and_then(|previous| {
            let text = previous.text.as_ref()?;
            let fragment = record.message.append_fragment.as_deref()?;
            if record.message.action != MessageAction::Append {
                return None;
            }
            let mut snapshot = text.write();
            if snapshot.len() != previous.text_len
                || data.len() != snapshot.len().saturating_add(fragment.len())
                || !data.starts_with(snapshot.as_str())
                || !data.ends_with(fragment)
            {
                return None;
            }
            snapshot.push_str(fragment);
            Some(text.clone())
        });
        let text = Some(shared.unwrap_or_else(|| Arc::new(parking_lot::RwLock::new(data))));
        Self {
            metadata,
            text,
            text_len,
            envelope_text,
        }
    }

    fn materialize(&self) -> StoredVersionRecord {
        use crate::message_envelope::MessageContent;
        use sockudo_protocol::messages::MessageData;
        let mut record = self.metadata.clone();
        if let Some(text) = &self.text {
            let data = text.read()[..self.text_len].to_string();
            if self.envelope_text
                && let Some(envelope) = record.envelope.as_mut()
            {
                envelope.data = Some(MessageContent::Text(data.clone()));
            }
            record.message.data = Some(MessageData::String(data));
        }
        record
    }
}

impl VersionChain {
    fn validate_incoming(&self, record: &StoredVersionRecord) -> Result<()> {
        if let Some(current) = self.latest.as_ref() {
            if current.message_serial() != record.message_serial() {
                return Err(Error::InvalidMessageFormat(format!(
                    "mixed message_serial values in one version chain: {} vs {}",
                    current.message_serial().as_str(),
                    record.message_serial().as_str()
                )));
            }
            if current.history_serial() != record.history_serial() {
                return Err(Error::InvalidMessageFormat(format!(
                    "mixed history_serial values in one version chain: {} vs {}",
                    current.history_serial(),
                    record.history_serial()
                )));
            }
        }
        if self.records.contains_key(record.version_serial()) {
            return Err(Error::InvalidMessageFormat(format!(
                "duplicate version_serial {} in version chain",
                record.version_serial().as_str()
            )));
        }
        Ok(())
    }

    fn insert(&mut self, record: Arc<StoredVersionRecord>) -> Arc<StoredRevision> {
        let advances = self
            .latest
            .as_ref()
            .is_none_or(|latest| record.version_serial() > latest.version_serial());
        let predecessor = if advances {
            self.latest
                .as_ref()
                .and_then(|latest| self.records.get(latest.version_serial()))
        } else {
            None
        };
        let stored = Arc::new(StoredRevision::new(&record, predecessor.map(AsRef::as_ref)));
        self.append_count += usize::from(record.message.action == MessageAction::Append);
        if let Some(operation) = record
            .envelope
            .as_ref()
            .and_then(|envelope| envelope.idempotency.as_ref())
        {
            self.operations
                .entry(operation.cache_key.clone())
                .or_default()
                .push_back(stored.clone());
        }
        if advances {
            self.latest = Some(record);
        }
        self.records
            .insert(stored.version_serial().clone(), stored.clone());
        stored
    }

    fn remove(&mut self, record: &StoredVersionRecord) {
        self.records.remove(record.version_serial());
        self.append_count -= usize::from(record.message.action == MessageAction::Append);
        if let Some(operation) = record
            .envelope
            .as_ref()
            .and_then(|envelope| envelope.idempotency.as_ref())
            && let Some(outcomes) = self.operations.get_mut(&operation.cache_key)
        {
            outcomes.retain(|outcome| outcome.version_serial() != record.version_serial());
            if outcomes.is_empty() {
                self.operations.remove(&operation.cache_key);
            }
        }
        if self
            .latest
            .as_ref()
            .is_some_and(|latest| latest.version_serial() == record.version_serial())
        {
            self.latest = self
                .records
                .last_key_value()
                .map(|(_, record)| Arc::new(record.materialize()));
        }
    }
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

    async fn channel(&self, key: &str) -> Arc<RwLock<MemoryVersionChannel>> {
        if let Some(channel) = self.channels.read().await.get(key).cloned() {
            return channel;
        }
        self.channels
            .write()
            .await
            .entry(key.to_owned())
            .or_default()
            .clone()
    }

    fn is_terminal(record: &StoredVersionRecord) -> bool {
        matches!(
            record
                .message
                .extras
                .as_ref()
                .and_then(|extras| extras.ai_transport_headers())
                .and_then(|headers| headers.status()),
            Some("complete" | "cancelled")
        )
    }
}

#[async_trait]
impl VersionStore for MemoryVersionStore {
    async fn ensure_stream_id(&self, app_id: &str, channel: &str) -> Result<String> {
        let key = Self::channel_key(app_id, channel);
        let channel = self.channel(&key).await;
        Ok(channel.read().await.stream_id.clone())
    }

    async fn reserve_delivery_position(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<VersionWriteReservation> {
        let key = Self::channel_key(app_id, channel);
        let channel_handle = self.channel(&key).await;
        let mut state_guard = channel_handle.write().await;
        let channel_state = &mut *state_guard;
        let reservation = VersionWriteReservation {
            stream_id: channel_state.stream_id.clone(),
            delivery_serial: channel_state.next_delivery_serial,
        };
        channel_state.next_delivery_serial = channel_state.next_delivery_serial.saturating_add(1);
        Ok(reservation)
    }

    async fn reserve_delivery_positions(
        &self,
        app_id: &str,
        channel: &str,
        block_size: u64,
    ) -> Result<VersionWriteReservationBlock> {
        VersionWriteReservationBlock::validate(block_size)?;
        let key = Self::channel_key(app_id, channel);
        let channel_handle = self.channel(&key).await;
        let mut state_guard = channel_handle.write().await;
        let channel_state = &mut *state_guard;
        let block = VersionWriteReservationBlock {
            stream_id: channel_state.stream_id.clone(),
            start_delivery_serial: channel_state.next_delivery_serial,
            len: block_size,
        };
        channel_state.next_delivery_serial = channel_state
            .next_delivery_serial
            .saturating_add(block_size);
        Ok(block)
    }

    async fn append_version(&self, record: StoredVersionRecord) -> Result<()> {
        let key = Self::channel_key(&record.app_id, &record.channel);
        let channel_handle = self.channel(&key).await;
        let mut state_guard = channel_handle.write().await;
        let channel_state = &mut *state_guard;

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

        let message_serial = record.message_serial().as_str().to_owned();
        if let Some(chain) = channel_state.messages.get(&message_serial) {
            chain.validate_incoming(&record)?;
        } else {
            validate_version_chain(std::slice::from_ref(&record.message))?;
        }

        let record = Arc::new(record);
        let record = channel_state
            .messages
            .entry(message_serial)
            .or_default()
            .insert(record);
        channel_state
            .created_at
            .insert(record.delivery_serial(), now_ms());
        channel_state
            .replay
            .insert(record.delivery_serial(), Arc::clone(&record));
        channel_state.next_delivery_serial = channel_state
            .next_delivery_serial
            .max(record.delivery_serial().saturating_add(1));

        Ok(())
    }

    async fn commit_create(&self, request: VersionCreateRequest) -> Result<VersionCreateResult> {
        let key = Self::channel_key(&request.record.app_id, &request.record.channel);
        let channel_handle = self.channel(&key).await;
        let mut state_guard = channel_handle.write().await;
        let channel_state = &mut *state_guard;

        if let Some(current) = channel_state
            .messages
            .get(request.record.message_serial().as_str())
            .and_then(|chain| chain.latest.as_ref())
        {
            return Ok(VersionCreateResult::Conflict {
                current: Some(current.as_ref().clone()),
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
            let open = channel_state
                .messages
                .values()
                .filter_map(|chain| chain.latest.as_ref())
                .filter(|record| record.is_open_ai_stream())
                .count();
            if open >= limit {
                return Ok(VersionCreateResult::Rejected(
                    VersionCreateRejection::OpenStreamingMessages { limit },
                ));
            }
        }

        let delivery_serial = channel_state.next_delivery_serial;
        let record = request
            .record
            .with_delivery_position(&channel_state.stream_id, delivery_serial);
        validate_version_chain(std::slice::from_ref(&record.message))?;
        if channel_state.replay.contains_key(&delivery_serial) {
            return Err(Error::InvalidMessageFormat(format!(
                "duplicate delivery_serial {delivery_serial} in version replay log"
            )));
        }
        let mut chain = VersionChain::default();
        let stored_record = chain.insert(Arc::new(record.clone()));
        channel_state
            .messages
            .insert(record.message_serial().as_str().to_string(), chain);
        channel_state.created_at.insert(delivery_serial, now_ms());
        channel_state.replay.insert(delivery_serial, stored_record);
        channel_state.next_delivery_serial = delivery_serial.saturating_add(1);

        Ok(VersionCreateResult::Applied {
            record,
            stream_id: channel_state.stream_id.clone(),
        })
    }

    async fn compare_and_apply(
        &self,
        request: VersionMutationRequest,
    ) -> Result<VersionMutationResult> {
        let key = Self::channel_key(&request.app_id, &request.channel);
        let channel_handle = self.channels.read().await.get(&key).cloned();
        let Some(channel_handle) = channel_handle else {
            return Ok(VersionMutationResult::Conflict { current: None });
        };
        let mut state_guard = channel_handle.write().await;
        let channel_state = &mut *state_guard;
        let Some(chain) = channel_state.messages.get(request.message_serial.as_str()) else {
            return Ok(VersionMutationResult::Conflict { current: None });
        };

        if let Some(incoming) = request.idempotency.as_ref()
            && let Some(existing) = chain
                .operations
                .get(&incoming.cache_key)
                .and_then(|outcomes| outcomes.front())
        {
            let existing_idempotency = existing
                .envelope
                .as_ref()
                .and_then(|envelope| envelope.idempotency.as_ref())
                .ok_or_else(|| {
                    Error::Internal(
                        "matched mutation idempotency record disappeared during lookup".to_string(),
                    )
                })?;
            if existing_idempotency.payload_fingerprint != incoming.payload_fingerprint {
                return Err(Error::IdempotencyConflict);
            }
            return Ok(VersionMutationResult::Duplicate {
                record: existing.materialize(),
                stream_id: channel_state.stream_id.clone(),
            });
        }

        let current = chain.latest.as_ref().ok_or_else(|| {
            Error::InvalidMessageFormat("version chain must not be empty".to_string())
        })?;
        if !request.expected.matches(current) {
            return Ok(VersionMutationResult::Conflict {
                current: Some(current.as_ref().clone()),
            });
        }

        if matches!(request.mutation, VersionMutation::Append(_)) {
            if request.limits.reject_append_after_terminal && Self::is_terminal(current) {
                return Ok(VersionMutationResult::Rejected(
                    VersionMutationRejection::TerminalMessage,
                ));
            }
            if let Some(limit) = request.limits.max_appends_per_message {
                let append_count = chain.append_count;
                if append_count >= limit {
                    return Ok(VersionMutationResult::Rejected(
                        VersionMutationRejection::AppendCount { limit },
                    ));
                }
            }
        }

        let delivery_serial = channel_state
            .next_delivery_serial
            .max(current.delivery_serial().saturating_add(1));
        let record = current.apply_mutation(&request, &channel_state.stream_id, delivery_serial)?;
        if let Some(limit) = request.limits.max_accumulated_message_bytes
            && record.data_bytes()? > limit
        {
            return Ok(VersionMutationResult::Rejected(
                VersionMutationRejection::AccumulatedMessageBytes { limit },
            ));
        }
        if !current.is_open_ai_stream()
            && record.is_open_ai_stream()
            && let Some(limit) = request.limits.max_open_streaming_messages_per_channel
        {
            let open = channel_state
                .messages
                .values()
                .filter_map(|chain| chain.latest.as_ref())
                .filter(|entry| entry.is_open_ai_stream())
                .count();
            if open >= limit {
                return Ok(VersionMutationResult::Rejected(
                    VersionMutationRejection::OpenStreamingMessages { limit },
                ));
            }
        }

        chain.validate_incoming(&record)?;
        if channel_state.replay.contains_key(&delivery_serial) {
            return Err(Error::InvalidMessageFormat(format!(
                "duplicate delivery_serial {delivery_serial} in version replay log"
            )));
        }

        let stored_record = channel_state
            .messages
            .get_mut(request.message_serial.as_str())
            .ok_or_else(|| {
                Error::Internal("version chain disappeared during mutation".to_string())
            })?
            .insert(Arc::new(record.clone()));
        channel_state.created_at.insert(delivery_serial, now_ms());
        channel_state.replay.insert(delivery_serial, stored_record);
        channel_state.next_delivery_serial = delivery_serial.saturating_add(1);

        Ok(VersionMutationResult::Applied {
            record,
            stream_id: channel_state.stream_id.clone(),
        })
    }

    async fn get_latest(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        let key = Self::channel_key(app_id, channel);
        let channel_handle = self.channels.read().await.get(&key).cloned();
        let Some(channel_handle) = channel_handle else {
            return Ok(None);
        };
        let state_guard = channel_handle.read().await;
        let channel_state = &*state_guard;
        let Some(chain) = channel_state.messages.get(message_serial.as_str()) else {
            return Ok(None);
        };

        let latest = chain
            .latest
            .as_ref()
            .ok_or_else(|| Error::InvalidMessageFormat("version chain must not be empty".into()))?;

        Ok(Some(latest.as_ref().clone()))
    }

    async fn get_latest_batch(
        &self,
        app_id: &str,
        channel: &str,
        message_serials: &[MessageSerial],
    ) -> Result<BTreeMap<MessageSerial, StoredVersionRecord>> {
        if message_serials.is_empty() {
            return Ok(BTreeMap::new());
        }

        let key = Self::channel_key(app_id, channel);
        let channel_handle = self.channels.read().await.get(&key).cloned();
        let Some(channel_handle) = channel_handle else {
            return Ok(BTreeMap::new());
        };
        let state_guard = channel_handle.read().await;
        let channel_state = &*state_guard;
        let requested = message_serials.iter().collect::<BTreeSet<_>>();
        requested
            .into_iter()
            .filter_map(|message_serial| {
                channel_state
                    .messages
                    .get(message_serial.as_str())
                    .map(|chain| (message_serial, chain))
            })
            .map(|(message_serial, chain)| {
                chain
                    .latest
                    .as_ref()
                    .map(|record| (message_serial.clone(), record.as_ref().clone()))
                    .ok_or_else(|| {
                        Error::InvalidMessageFormat("version chain must not be empty".into())
                    })
            })
            .collect()
    }

    async fn get_versions(&self, request: VersionStoreReadRequest) -> Result<VersionStorePage> {
        request.validate()?;
        let key = Self::channel_key(&request.app_id, &request.channel);
        let channel_handle = self.channels.read().await.get(&key).cloned();
        let Some(channel_handle) = channel_handle else {
            return Ok(VersionStorePage {
                items: Vec::new(),
                next_cursor: None,
                has_more: false,
            });
        };
        let state_guard = channel_handle.read().await;
        let channel_state = &*state_guard;
        let Some(chain) = channel_state.messages.get(request.message_serial.as_str()) else {
            return Ok(VersionStorePage {
                items: Vec::new(),
                next_cursor: None,
                has_more: false,
            });
        };

        let cursor = request.cursor.as_ref().map(|cursor| &cursor.version_serial);
        let filtered = match request.direction {
            VersionStoreDirection::NewestFirst => chain
                .records
                .range((
                    Bound::Unbounded,
                    cursor.map_or(Bound::Unbounded, Bound::Excluded),
                ))
                .rev()
                .map(|(_, record)| record)
                .take(request.limit + 1)
                .collect::<Vec<_>>(),
            VersionStoreDirection::OldestFirst => chain
                .records
                .range((
                    cursor.map_or(Bound::Unbounded, Bound::Excluded),
                    Bound::Unbounded,
                ))
                .map(|(_, record)| record)
                .take(request.limit + 1)
                .collect::<Vec<_>>(),
        };

        let has_more = filtered.len() > request.limit;
        let items = filtered
            .into_iter()
            .take(request.limit)
            .map(|item| item.materialize())
            .collect::<Vec<_>>();
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
        let channel_handle = self.channels.read().await.get(&key).cloned();
        let Some(channel_handle) = channel_handle else {
            return Ok(Vec::new());
        };
        let state_guard = channel_handle.read().await;
        let channel_state = &*state_guard;

        let stored_items = channel_state
            .replay
            .range((request.after_delivery_serial.saturating_add(1))..)
            .map(|(_, value)| value)
            .take(request.limit)
            .collect::<Vec<_>>();

        validate_replay_continuity_iter(
            stored_items.iter().map(|entry| &entry.message),
            request.after_delivery_serial,
        )?;

        Ok(stored_items
            .into_iter()
            .map(|item| item.materialize())
            .collect())
    }

    async fn latest_by_history(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Vec<StoredVersionRecord>> {
        let key = Self::channel_key(app_id, channel);
        let channel_handle = self.channels.read().await.get(&key).cloned();
        let Some(channel_handle) = channel_handle else {
            return Ok(Vec::new());
        };
        let state_guard = channel_handle.read().await;
        let channel_state = &*state_guard;

        let mut latest = channel_state
            .messages
            .values()
            .filter_map(|chain| chain.latest.as_ref().map(|record| record.as_ref().clone()))
            .collect::<Vec<_>>();

        latest.sort_by_key(StoredVersionRecord::history_serial);
        Ok(latest)
    }

    async fn message_count(&self, app_id: &str, channel: &str) -> Result<u64> {
        let handle = self
            .channels
            .read()
            .await
            .get(&Self::channel_key(app_id, channel))
            .cloned();
        let Some(handle) = handle else {
            return Ok(0);
        };
        Ok(handle.read().await.messages.len() as u64)
    }

    async fn active_stream_count(&self, app_id: &str, channel: &str) -> Result<usize> {
        let handle = self
            .channels
            .read()
            .await
            .get(&Self::channel_key(app_id, channel))
            .cloned();
        let Some(handle) = handle else {
            return Ok(0);
        };
        Ok(handle
            .read()
            .await
            .messages
            .values()
            .filter_map(|chain| chain.latest.as_ref())
            .filter(|record| record.is_open_ai_stream())
            .count())
    }

    async fn stream_state(&self, app_id: &str, channel: &str) -> Result<VersionStreamState> {
        let key = Self::channel_key(app_id, channel);
        let channel_handle = self.channels.read().await.get(&key).cloned();
        let Some(channel_handle) = channel_handle else {
            return Ok(VersionStreamState::default());
        };
        let state_guard = channel_handle.read().await;
        let channel_state = &*state_guard;

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
        let channels: Vec<_> = self.channels.read().await.values().cloned().collect();
        let mut deleted: u64 = 0;
        let mut has_more = false;

        for channel_handle in channels {
            let mut state_guard = channel_handle.write().await;
            let state = &mut *state_guard;
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
                    chain.remove(&record);
                    if chain.records.is_empty() {
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
mod compact_tests {
    use super::*;
    use crate::message_envelope::MessageContent;
    use crate::versioned_messages::{MessageAppend, VersionMetadata, VersionedMessage};
    use sockudo_protocol::messages::MessageData;

    fn version(n: u64) -> VersionMetadata {
        VersionMetadata {
            serial: VersionSerial::new(format!("ver:{n:020}")).unwrap(),
            client_id: Some("alice".into()),
            timestamp_ms: n as i64,
            description: None,
            metadata: None,
        }
    }

    #[tokio::test]
    async fn append_snapshots_share_storage_and_survive_partial_retention() {
        let store = MemoryVersionStore::new();
        let mut current = StoredVersionRecord {
            app_id: "app".into(),
            channel: "room".into(),
            original_client_id: Some("alice".into()),
            envelope: None,
            message: VersionedMessage::new_create(
                MessageSerial::new("msg:one").unwrap(),
                version(1),
                9,
                1,
                Some("text".into()),
                Some(MessageData::String("start".into())),
                None,
            ),
        };
        store.append_version(current.clone()).await.unwrap();
        let fragment = "世界abcdef".repeat(8);
        for n in 2..=513 {
            let request = VersionMutationRequest {
                app_id: "app".into(),
                channel: "room".into(),
                message_serial: current.message_serial().clone(),
                expected: VersionPrecondition::from_record(&current),
                version: version(n),
                mutation: VersionMutation::Append(MessageAppend {
                    data_fragment: fragment.clone(),
                    extras: None,
                }),
                idempotency: None,
                limits: VersionMutationLimits::default(),
            };
            let VersionMutationResult::Applied { record, .. } =
                store.compare_and_apply(request).await.unwrap()
            else {
                panic!("append was not applied")
            };
            current = record;
        }
        let channel = store
            .channel(&MemoryVersionStore::channel_key("app", "room"))
            .await;
        {
            let state = channel.read().await;
            let chain = &state.messages["msg:one"];
            let roots = chain
                .records
                .values()
                .map(|entry| Arc::as_ptr(entry.text.as_ref().unwrap()) as usize)
                .collect::<BTreeSet<_>>();
            assert_eq!(roots.len(), 1);
            let snapshot = chain
                .records
                .first_key_value()
                .unwrap()
                .1
                .text
                .as_ref()
                .unwrap()
                .read();
            assert_eq!(snapshot.len(), "start".len() + 512 * fragment.len());
            assert!(
                chain
                    .records
                    .values()
                    .all(|entry| entry.metadata.message.data.is_none())
            );
            assert!(chain.records.values().all(|entry| {
                entry
                    .metadata
                    .envelope
                    .as_ref()
                    .is_none_or(|envelope| envelope.data.is_none())
            }));
        }
        // Random public versions preserve the exact UTF-8 prefix and envelope
        // while old records share only one growing snapshot internally.
        for n in [2, 19, 257, 513] {
            let page = store
                .get_versions(VersionStoreReadRequest {
                    app_id: "app".into(),
                    channel: "room".into(),
                    message_serial: current.message_serial().clone(),
                    direction: VersionStoreDirection::OldestFirst,
                    limit: 1,
                    cursor: Some(VersionStoreCursor {
                        version: 1,
                        version_serial: version(n - 1).serial,
                        direction: VersionStoreDirection::OldestFirst,
                    }),
                })
                .await
                .unwrap();
            let record = &page.items[0];
            let expected = format!("start{}", fragment.repeat(n as usize - 1));
            assert_eq!(
                record.message.data,
                Some(MessageData::String(expected.clone()))
            );
            assert_eq!(
                record.envelope.as_ref().unwrap().data,
                Some(MessageContent::Text(expected))
            );
            assert_eq!(
                record.message.append_fragment.as_deref(),
                Some(fragment.as_str())
            );
            assert_eq!(record.history_serial(), 9);
            assert_eq!(record.delivery_serial(), n);
        }
        {
            let mut state = channel.write().await;
            for (serial, created) in &mut state.created_at {
                if *serial <= 256 {
                    *created = 1;
                }
            }
        }
        assert_eq!(store.purge_before(2, 256).await.unwrap().0, 256);
        let restored = store
            .get_latest("app", "room", current.message_serial())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            sonic_rs::to_vec(&restored).unwrap(),
            sonic_rs::to_vec(&current).unwrap()
        );
        let page = store
            .get_versions(VersionStoreReadRequest {
                app_id: "app".into(),
                channel: "room".into(),
                message_serial: current.message_serial().clone(),
                direction: VersionStoreDirection::OldestFirst,
                limit: 1,
                cursor: None,
            })
            .await
            .unwrap();
        assert_eq!(page.items[0].delivery_serial(), 257);
        assert_eq!(
            page.items[0].message.data,
            Some(MessageData::String(format!(
                "start{}",
                fragment.repeat(256)
            )))
        );
    }
}
