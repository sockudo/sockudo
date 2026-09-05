use super::store::PresenceHistoryStore;
use super::types::{
    PresenceHistoryCursor, PresenceHistoryDirection, PresenceHistoryDurableState,
    PresenceHistoryEventCause, PresenceHistoryEventKind, PresenceHistoryItem, PresenceHistoryPage,
    PresenceHistoryQueryBounds, PresenceHistoryReadRequest, PresenceHistoryResetResult,
    PresenceHistoryRetentionStats, PresenceHistoryRuntimeStatus, PresenceHistoryStreamInspection,
    PresenceHistoryStreamRuntimeState, PresenceHistoryTransitionRecord,
};
use crate::error::{Error, Result};
use crate::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDirection, HistoryQueryBounds, HistoryReadRequest,
    HistoryRetentionPolicy, HistoryStore,
};
use crate::metrics::MetricsInterface;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DurablePresenceHistoryPayload {
    pub published_at_ms: i64,
    pub event: PresenceHistoryEventKind,
    pub cause: PresenceHistoryEventCause,
    pub user_id: String,
    pub connection_id: Option<String>,
    pub user_info: Option<Value>,
    pub dead_node_id: Option<String>,
    pub dedupe_key: String,
}

const TRANSITION_CACHE_CHANNELS: usize = 64;
const TRANSITION_CACHE_BYTES: usize = 256 * 1024;
const TRANSITION_MEMBERSHIP_RANGES: usize = 64;
const TRANSITION_MEMBERSHIP_WORDS: usize = 254;
const TRANSITION_MEMBERSHIP_RANGE_ROWS: u64 = 256;
const TRANSITION_QUERIED_USER_BYTES: usize = 64 * 1024;
const TRANSITION_LOCK_STRIPES: usize = 128;

#[derive(Clone)]
pub struct DurablePresenceHistoryStore {
    history_store: Arc<dyn HistoryStore + Send + Sync>,
    metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    transition_cache: Arc<RwLock<BTreeMap<String, Arc<Mutex<DurablePresenceTransitionCache>>>>>,
    transition_locks: Arc<[Mutex<()>; TRANSITION_LOCK_STRIPES]>,
}

struct DurablePresenceTransitionCache {
    stream_id: Option<String>,
    accounted_bytes: usize,
    membership: Vec<TransitionMembershipRange>,
    covered_through: Option<u64>,
    coverage_oldest: Option<u64>,
    coverage_valid: bool,
    queried_users: BTreeMap<String, (PresenceHistoryEventKind, u64)>,
    queried_user_bytes: usize,
    // Payload-free serial index bounds pruning work to expired transitions.
    serial_entries: BTreeMap<u64, (String, String)>,
    dedupe_keys: BTreeMap<String, u64>,
    latest_event_by_user: BTreeMap<String, (PresenceHistoryEventKind, u64)>,
}

// Exactly2KiB per range, including bounds. Merging adjacent filters with OR
// never loses an observed identity; it only increases false-positive reads.
struct TransitionMembershipRange {
    first: u64,
    last: u64,
    words: [u64; TRANSITION_MEMBERSHIP_WORDS],
}

impl TransitionMembershipRange {
    fn may_contain(&self, bits: [usize; 5]) -> bool {
        bits.iter()
            .all(|bit| self.words[bit / 64] & (1 << (bit % 64)) != 0)
    }
}

impl Default for DurablePresenceTransitionCache {
    fn default() -> Self {
        Self {
            stream_id: None,
            accounted_bytes: TRANSITION_MEMBERSHIP_RANGES
                * std::mem::size_of::<TransitionMembershipRange>()
                + 512,
            membership: Vec::with_capacity(TRANSITION_MEMBERSHIP_RANGES),
            covered_through: None,
            coverage_oldest: None,
            coverage_valid: false,
            queried_users: BTreeMap::new(),
            queried_user_bytes: 0,
            serial_entries: BTreeMap::new(),
            dedupe_keys: BTreeMap::new(),
            latest_event_by_user: BTreeMap::new(),
        }
    }
}

impl DurablePresenceTransitionCache {
    fn membership_bits(kind: u8, key: &str) -> [usize; 5] {
        let mut hash = std::hash::DefaultHasher::new();
        (kind, key).hash(&mut hash);
        let hash = hash.finish();
        [0, 13, 26, 39, 52].map(|rotation| {
            hash.rotate_left(rotation) as usize % (TRANSITION_MEMBERSHIP_WORDS * 64)
        })
    }

    #[cfg(test)]
    fn may_contain(&self, kind: u8, key: &str) -> bool {
        let bits = Self::membership_bits(kind, key);
        self.membership.iter().any(|range| range.may_contain(bits))
    }

    fn add_membership(&mut self, dedupe: &str, user: &str, serial: u64) {
        let index = self
            .membership
            .iter()
            .position(|range| serial <= range.last);
        let index = if let Some(index) = index {
            self.membership[index].first = self.membership[index].first.min(serial);
            index
        } else if self.membership.last().is_some_and(|range| {
            serial.saturating_sub(range.first) < TRANSITION_MEMBERSHIP_RANGE_ROWS
        }) {
            self.membership.len() - 1
        } else {
            if self.membership.len() == TRANSITION_MEMBERSHIP_RANGES {
                let index = self
                    .membership
                    .windows(2)
                    .enumerate()
                    .min_by_key(|(_, pair)| pair[1].last.saturating_sub(pair[0].first))
                    .map(|(index, _)| index)
                    .expect("full membership cache has adjacent ranges");
                let next = self.membership.remove(index + 1);
                let previous = &mut self.membership[index];
                previous.last = next.last;
                for (word, next_word) in previous.words.iter_mut().zip(next.words) {
                    *word |= next_word;
                }
            }
            self.membership.push(TransitionMembershipRange {
                first: serial,
                last: serial,
                words: [0; TRANSITION_MEMBERSHIP_WORDS],
            });
            self.membership.len() - 1
        };
        let range = &mut self.membership[index];
        range.last = range.last.max(serial);
        for bit in Self::membership_bits(0, dedupe)
            .into_iter()
            .chain(Self::membership_bits(1, user))
        {
            range.words[bit / 64] |= 1 << (bit % 64);
        }
    }
    fn insert(
        &mut self,
        dedupe_key: String,
        user_id: String,
        event: PresenceHistoryEventKind,
        serial: u64,
    ) -> bool {
        self.add_membership(&dedupe_key, &user_id, serial);
        if let Some(latest) = self.queried_users.get_mut(&user_id)
            && serial >= latest.1
        {
            *latest = (event, serial);
        }
        let bytes = Self::entry_bytes(&dedupe_key, &user_id);
        while self.accounted_bytes.saturating_add(bytes)
            > TRANSITION_CACHE_BYTES - TRANSITION_QUERIED_USER_BYTES
        {
            if self.serial_entries.is_empty() {
                // Oversized identities remain represented conservatively by the
                // membership filter, without retaining their string allocations.
                return true;
            }
            self.evict_oldest_recent();
        }
        if !self.serial_entries.contains_key(&serial) {
            self.accounted_bytes += bytes;
        }
        self.serial_entries
            .insert(serial, (dedupe_key.clone(), user_id.clone()));
        self.dedupe_keys
            .entry(dedupe_key)
            .and_modify(|current| *current = (*current).max(serial))
            .or_insert(serial);
        let latest = self
            .latest_event_by_user
            .entry(user_id)
            .or_insert((event, serial));
        if serial >= latest.1 {
            *latest = (event, serial);
        }
        true
    }

    fn latest_for_user(&self, user: &str) -> Option<(PresenceHistoryEventKind, u64)> {
        self.queried_users
            .get(user)
            .into_iter()
            .chain(self.latest_event_by_user.get(user))
            .copied()
            .max_by_key(|(_, serial)| *serial)
    }

    fn remember_user(&mut self, user: &str, latest: (PresenceHistoryEventKind, u64)) {
        let bytes = user.len().saturating_add(256);
        if bytes > TRANSITION_QUERIED_USER_BYTES {
            return;
        }
        if let Some(entry) = self.queried_users.get_mut(user) {
            *entry = latest;
            return;
        }
        while self.queried_user_bytes.saturating_add(bytes) > TRANSITION_QUERIED_USER_BYTES {
            let Some((evicted, _)) = self.queried_users.pop_first() else {
                break;
            };
            self.queried_user_bytes -= evicted.len().saturating_add(256);
        }
        self.queried_users.insert(user.to_owned(), latest);
        self.queried_user_bytes += bytes;
    }

    fn entry_bytes(dedupe: &str, user: &str) -> usize {
        // Two copies of each string plus conservative B-tree node/allocator overhead.
        dedupe
            .len()
            .saturating_add(user.len())
            .saturating_mul(2)
            .saturating_add(512)
    }

    fn prune_before(&mut self, oldest: u64) {
        // A wholly expired interval cannot contain a retained identity. Removing
        // it frees range slots under retention churn; partial intervals keep all
        // bits, so pruning never loses evidence for a surviving row.
        self.membership.retain(|range| range.last >= oldest);
        self.queried_users.retain(|user, (_, serial)| {
            if *serial < oldest {
                self.queried_user_bytes -= user.len().saturating_add(256);
                false
            } else {
                true
            }
        });
        self.prune_recent_before(oldest);
    }

    fn prune_recent_before(&mut self, oldest: u64) {
        while self
            .serial_entries
            .first_key_value()
            .is_some_and(|(serial, _)| *serial < oldest)
        {
            self.evict_oldest_recent();
        }
    }

    fn evict_oldest_recent(&mut self) {
        let (serial, (dedupe, user)) = self
            .serial_entries
            .pop_first()
            .expect("checked first entry");
        self.accounted_bytes = self
            .accounted_bytes
            .saturating_sub(Self::entry_bytes(&dedupe, &user));
        if self.dedupe_keys.get(&dedupe) == Some(&serial) {
            self.dedupe_keys.remove(&dedupe);
        }
        if self
            .latest_event_by_user
            .get(&user)
            .is_some_and(|(_, latest)| *latest == serial)
        {
            self.latest_event_by_user.remove(&user);
        }
    }
}

impl DurablePresenceHistoryStore {
    pub fn new(
        history_store: Arc<dyn HistoryStore + Send + Sync>,
        metrics: Option<Arc<dyn MetricsInterface + Send + Sync>>,
    ) -> Self {
        Self {
            history_store,
            metrics,
            transition_cache: Arc::new(RwLock::new(BTreeMap::new())),
            transition_locks: Arc::new(std::array::from_fn(|_| Mutex::new(()))),
        }
    }

    fn durable_channel_name(channel: &str) -> String {
        format!("[presence-history]{channel}")
    }

    fn cache_key(app_id: &str, channel: &str) -> String {
        format!("{app_id}\0{channel}")
    }

    fn history_retention(record: &PresenceHistoryTransitionRecord) -> HistoryRetentionPolicy {
        HistoryRetentionPolicy {
            retention_window_seconds: record.retention.retention_window_seconds,
            max_messages_per_channel: record.retention.max_events_per_channel,
            max_bytes_per_channel: record.retention.max_bytes_per_channel,
        }
    }

    fn presence_bounds_to_history(bounds: &PresenceHistoryQueryBounds) -> HistoryQueryBounds {
        HistoryQueryBounds {
            start_serial: bounds.start_serial,
            end_serial: bounds.end_serial,
            start_time_ms: bounds.start_time_ms,
            end_time_ms: bounds.end_time_ms,
        }
    }

    fn presence_direction_to_history(direction: PresenceHistoryDirection) -> HistoryDirection {
        match direction {
            PresenceHistoryDirection::NewestFirst => HistoryDirection::NewestFirst,
            PresenceHistoryDirection::OldestFirst => HistoryDirection::OldestFirst,
        }
    }

    fn history_cursor_from_presence(
        request: &PresenceHistoryReadRequest,
        channel: &str,
    ) -> Option<HistoryCursor> {
        request.cursor.as_ref().map(|cursor| HistoryCursor {
            version: cursor.version,
            app_id: cursor.app_id.clone(),
            channel: channel.to_string(),
            stream_id: cursor.stream_id.clone(),
            serial: cursor.serial,
            direction: Self::presence_direction_to_history(cursor.direction),
            bounds: Self::presence_bounds_to_history(&cursor.bounds),
        })
    }

    fn history_read_request(
        request: &PresenceHistoryReadRequest,
        limit: usize,
    ) -> HistoryReadRequest {
        let channel = Self::durable_channel_name(&request.channel);
        HistoryReadRequest {
            app_id: request.app_id.clone(),
            channel: channel.clone(),
            direction: Self::presence_direction_to_history(request.direction),
            limit,
            cursor: Self::history_cursor_from_presence(request, &channel),
            bounds: Self::presence_bounds_to_history(&request.bounds),
        }
    }

    fn decode_payload(bytes: &[u8]) -> Result<DurablePresenceHistoryPayload> {
        sonic_rs::from_slice(bytes).map_err(|e| {
            Error::Serialization(format!(
                "Failed to decode durable presence history payload: {e}"
            ))
        })
    }

    fn encode_payload(record: &PresenceHistoryTransitionRecord) -> Result<Bytes> {
        sonic_rs::to_vec(&DurablePresenceHistoryPayload {
            published_at_ms: record.published_at_ms,
            event: record.event_kind,
            cause: record.cause,
            user_id: record.user_id.clone(),
            connection_id: record.connection_id.clone(),
            user_info: record.user_info.clone(),
            dead_node_id: record.dead_node_id.clone(),
            dedupe_key: record.dedupe_key.clone(),
        })
        .map(Bytes::from)
        .map_err(|e| {
            Error::Serialization(format!(
                "Failed to encode durable presence history payload: {e}"
            ))
        })
    }

    fn decode_item(
        item: crate::history::HistoryItem,
    ) -> Result<(PresenceHistoryItem, DurablePresenceHistoryPayload)> {
        let payload = Self::decode_payload(item.payload_bytes.as_ref())?;
        Ok((
            PresenceHistoryItem {
                stream_id: item.stream_id,
                serial: item.serial,
                published_at_ms: payload.published_at_ms,
                event: payload.event,
                cause: payload.cause,
                user_id: payload.user_id.clone(),
                connection_id: payload.connection_id.clone(),
                dead_node_id: payload.dead_node_id.clone(),
                payload_size_bytes: item.payload_size_bytes,
                payload_bytes: item.payload_bytes,
            },
            payload,
        ))
    }

    fn retained_from_history(
        retained: crate::history::HistoryRetentionStats,
    ) -> PresenceHistoryRetentionStats {
        PresenceHistoryRetentionStats {
            stream_id: retained.stream_id,
            retained_events: retained.retained_messages,
            retained_bytes: retained.retained_bytes,
            oldest_serial: retained.oldest_serial,
            newest_serial: retained.newest_serial,
            oldest_published_at_ms: retained.oldest_published_at_ms,
            newest_published_at_ms: retained.newest_published_at_ms,
        }
    }

    fn map_runtime_state(
        channel: &str,
        state: crate::history::HistoryStreamRuntimeState,
    ) -> PresenceHistoryStreamRuntimeState {
        PresenceHistoryStreamRuntimeState {
            app_id: state.app_id,
            channel: channel.to_string(),
            stream_id: state.stream_id,
            durable_state: match state.durable_state {
                crate::history::HistoryDurableState::Healthy => {
                    PresenceHistoryDurableState::Healthy
                }
                crate::history::HistoryDurableState::Degraded => {
                    PresenceHistoryDurableState::Degraded
                }
                crate::history::HistoryDurableState::ResetRequired => {
                    PresenceHistoryDurableState::ResetRequired
                }
            },
            continuity_proven: state.recovery_allowed,
            reset_required: state.reset_required,
            reason: state.reason,
            node_id: state.node_id,
            last_transition_at_ms: state.last_transition_at_ms,
            authoritative_source: state.authoritative_source,
            observed_source: state.observed_source,
        }
    }

    async fn update_retained_metrics(&self, app_id: &str, channel: &str) -> Result<()> {
        let Some(metrics) = self.metrics.as_ref() else {
            return Ok(());
        };
        let retained = self.stream_inspection(app_id, channel).await?.retained;
        metrics.update_presence_history_retained(
            app_id,
            retained.retained_events,
            retained.retained_bytes,
        );
        Ok(())
    }

    async fn inspect_durable_channel(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<crate::history::HistoryStreamInspection> {
        self.history_store
            .stream_inspection(app_id, &Self::durable_channel_name(channel))
            .await
    }

    async fn channel_cache(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Arc<Mutex<DurablePresenceTransitionCache>> {
        let key = Self::cache_key(app_id, channel);
        if let Some(cache) = self.transition_cache.read().await.get(&key) {
            return cache.clone();
        }
        // Oversized identities remain usable without retaining them in the cache.
        if key.len() > 4096 {
            return Arc::new(Mutex::new(DurablePresenceTransitionCache::default()));
        }
        let mut caches = self.transition_cache.write().await;
        if let Some(cache) = caches.get(&key) {
            return cache.clone();
        }
        if caches.len() >= TRANSITION_CACHE_CHANNELS {
            caches.pop_first();
        }
        caches.entry(key).or_default().clone()
    }

    async fn find_existing_transition(
        &self,
        record: &PresenceHistoryTransitionRecord,
        cache: &mut DurablePresenceTransitionCache,
    ) -> Result<(bool, bool)> {
        let inspection = self
            .inspect_durable_channel(&record.app_id, &record.channel)
            .await?;
        if cache.stream_id != inspection.stream_id {
            *cache = DurablePresenceTransitionCache {
                stream_id: inspection.stream_id.clone(),
                ..Default::default()
            };
        }
        let retained = &inspection.retained;
        cache.prune_before(retained.oldest_serial.unwrap_or(u64::MAX));

        // Only a completely covered, contiguous immutable serial interval proves
        // absence. With reservation gaps or late commits, read all retained rows
        // again; neither a local miss nor matching endpoints alone is sufficient.
        let contiguous = match (retained.oldest_serial, retained.newest_serial) {
            (Some(first), Some(last)) => {
                last.checked_sub(first).and_then(|n| n.checked_add(1))
                    == Some(retained.retained_messages)
            }
            (None, None) => retained.retained_messages == 0,
            _ => false,
        };
        let coverage_reusable = contiguous
            && cache.coverage_valid
            && match (
                retained.oldest_serial,
                retained.newest_serial,
                cache.covered_through,
            ) {
                (None, None, None) => true,
                (Some(oldest), Some(newest), Some(covered)) => {
                    cache.coverage_oldest.is_some_and(|first| first <= oldest)
                        && covered <= newest
                        && covered.saturating_add(1) >= oldest
                }
                _ => false,
            };
        if !coverage_reusable {
            *cache = DurablePresenceTransitionCache {
                stream_id: inspection.stream_id.clone(),
                ..Default::default()
            };
        }
        let mut covered = cache
            .covered_through
            .zip(retained.oldest_serial)
            .and_then(|(last, first)| last.checked_sub(first).map(|n| n + 1))
            .unwrap_or(0);
        let mut found_dedupe = cache.dedupe_keys.contains_key(&record.dedupe_key);
        let mut latest_user = cache.latest_for_user(&record.user_id);
        let mut retain_cache = true;
        let mut last_read = cache.covered_through;
        if retained.retained_messages != 0 {
            let start = match cache.covered_through {
                Some(serial) => serial.checked_add(1),
                None => retained.oldest_serial,
            };
            if start.is_some() && start <= retained.newest_serial {
                let mut request = HistoryReadRequest {
                    app_id: record.app_id.clone(),
                    channel: Self::durable_channel_name(&record.channel),
                    direction: HistoryDirection::OldestFirst,
                    limit: 256,
                    cursor: None,
                    bounds: HistoryQueryBounds {
                        // Some history implementations preserve insertion order
                        // for late imports, including their reported endpoints.
                        // Such endpoints cannot bound a full-coverage fallback.
                        start_serial: contiguous.then_some(start).flatten(),
                        end_serial: contiguous.then_some(retained.newest_serial).flatten(),
                        ..Default::default()
                    },
                };
                loop {
                    let page = self.history_store.read_page(request.clone()).await?;
                    if (!page.complete && !page.has_more)
                        || page.truncated_by_retention
                        || page.retained != *retained
                    {
                        *cache = DurablePresenceTransitionCache::default();
                        return Err(Error::Internal(
                            "presence history changed or was incomplete during transition lookup"
                                .into(),
                        ));
                    }
                    // An unordered result that fits one bounded page can still
                    // prove coverage. Across pages, serial cursor ordering must
                    // hold or a late row could be skipped behind the cursor.
                    let unordered_single_page =
                        !contiguous && !page.has_more && request.cursor.is_none() && covered == 0;
                    if unordered_single_page {
                        let serials = page
                            .items
                            .iter()
                            .map(|item| item.serial)
                            .collect::<std::collections::BTreeSet<_>>();
                        if serials.len() != page.items.len() {
                            *cache = DurablePresenceTransitionCache::default();
                            return Err(Error::Internal(
                                "presence history transition lookup returned duplicate serials"
                                    .into(),
                            ));
                        }
                    }
                    for item in page.items {
                        if Some(item.stream_id.as_str()) != inspection.stream_id.as_deref() {
                            // A reset raced the inspection. Do not publish a
                            // transition using the old generation's evidence.
                            *cache = DurablePresenceTransitionCache::default();
                            return Err(Error::Internal(
                                "presence history stream changed during transition lookup".into(),
                            ));
                        }
                        let payload = Self::decode_payload(&item.payload_bytes)?;
                        if (!unordered_single_page
                            && last_read.is_some_and(|serial| item.serial <= serial))
                            || (contiguous && Some(item.serial) > retained.newest_serial)
                        {
                            *cache = DurablePresenceTransitionCache::default();
                            return Err(Error::Internal("presence history transition lookup returned invalid serial coverage".into()));
                        }
                        last_read = Some(item.serial);
                        covered += 1;
                        found_dedupe |= payload.dedupe_key == record.dedupe_key;
                        if payload.user_id == record.user_id
                            && latest_user.is_none_or(|(_, serial)| item.serial > serial)
                        {
                            latest_user = Some((payload.event, item.serial));
                        }
                        if retain_cache {
                            retain_cache = cache.insert(
                                payload.dedupe_key,
                                payload.user_id,
                                payload.event,
                                item.serial,
                            );
                        }
                    }
                    if !page.has_more {
                        break;
                    }
                    let next = page.next_cursor.ok_or_else(|| {
                        Error::Internal("presence history lookup has no continuation".into())
                    })?;
                    if request
                        .cursor
                        .as_ref()
                        .is_some_and(|cursor| cursor.serial == next.serial)
                    {
                        return Err(Error::Internal(
                            "presence history lookup did not advance".into(),
                        ));
                    }
                    request.cursor = Some(next);
                }
            }
        }
        // Complete cached coverage plus the freshly read tail permits selective
        // lookup. A negative range filter proves this range cannot contain the
        // requested identity; positives still read and verify every serial.
        if coverage_reusable
            && !found_dedupe
            && !latest_user.is_some_and(|(event, _)| event == record.event_kind)
        {
            let dedupe_bits =
                DurablePresenceTransitionCache::membership_bits(0, &record.dedupe_key);
            let user_bits = DurablePresenceTransitionCache::membership_bits(1, &record.user_id);
            let ranges: Vec<_> = cache
                .membership
                .iter()
                .enumerate()
                .filter(|(_, range)| {
                    range.may_contain(dedupe_bits)
                        || (latest_user.is_none() && range.may_contain(user_bits))
                })
                .filter_map(|(index, range)| {
                    let first = range.first.max(retained.oldest_serial?);
                    let last = range.last.min(retained.newest_serial?);
                    (first <= last).then_some((index, first, last))
                })
                .collect();
            match self
                .lookup_transition_ranges(record, &inspection, &ranges, latest_user, cache)
                .await
            {
                Ok((dedupe, latest)) => {
                    found_dedupe |= dedupe;
                    latest_user = latest;
                }
                Err(error) => {
                    *cache = DurablePresenceTransitionCache::default();
                    return Err(error);
                }
            }
        }
        let after = self
            .inspect_durable_channel(&record.app_id, &record.channel)
            .await?;
        if covered != retained.retained_messages
            || after.stream_id != inspection.stream_id
            || after.retained != *retained
        {
            *cache = DurablePresenceTransitionCache::default();
            return Err(Error::Internal(
                "presence history coverage changed during transition lookup".into(),
            ));
        }
        if let Some(latest) = latest_user {
            cache.remember_user(&record.user_id, latest);
        }
        cache.coverage_valid = contiguous;
        cache.coverage_oldest = retained.oldest_serial;
        cache.covered_through = retained.newest_serial;
        Ok((
            found_dedupe,
            latest_user.is_some_and(|(event, _)| event == record.event_kind),
        ))
    }

    async fn lookup_transition_ranges(
        &self,
        record: &PresenceHistoryTransitionRecord,
        inspection: &crate::history::HistoryStreamInspection,
        ranges: &[(usize, u64, u64)],
        mut latest_user: Option<(PresenceHistoryEventKind, u64)>,
        cache: &mut DurablePresenceTransitionCache,
    ) -> Result<(bool, Option<(PresenceHistoryEventKind, u64)>)> {
        let mut found_dedupe = false;
        for &(index, first, last) in ranges {
            let mut words = [0_u64; TRANSITION_MEMBERSHIP_WORDS];
            let mut request = HistoryReadRequest {
                app_id: record.app_id.clone(),
                channel: Self::durable_channel_name(&record.channel),
                direction: HistoryDirection::OldestFirst,
                limit: 256,
                cursor: None,
                bounds: HistoryQueryBounds {
                    start_serial: Some(first),
                    end_serial: Some(last),
                    ..Default::default()
                },
            };
            let mut count = 0_u64;
            loop {
                let page = self.history_store.read_page(request.clone()).await?;
                if (!page.complete && !page.has_more)
                    || page.truncated_by_retention
                    || page.retained != inspection.retained
                {
                    return Err(Error::Internal(
                        "presence history range changed or was incomplete during transition lookup"
                            .into(),
                    ));
                }
                for item in page.items {
                    if Some(item.stream_id.as_str()) != inspection.stream_id.as_deref()
                        || first.checked_add(count) != Some(item.serial)
                        || item.serial > last
                    {
                        return Err(Error::Internal(
                            "presence history range returned invalid serial coverage".into(),
                        ));
                    }
                    count += 1;
                    let payload = Self::decode_payload(&item.payload_bytes)?;
                    for bit in
                        DurablePresenceTransitionCache::membership_bits(0, &payload.dedupe_key)
                            .into_iter()
                            .chain(DurablePresenceTransitionCache::membership_bits(
                                1,
                                &payload.user_id,
                            ))
                    {
                        words[bit / 64] |= 1 << (bit % 64);
                    }
                    found_dedupe |= payload.dedupe_key == record.dedupe_key;
                    if payload.user_id == record.user_id
                        && latest_user.is_none_or(|(_, serial)| item.serial > serial)
                    {
                        latest_user = Some((payload.event, item.serial));
                    }
                }
                if !page.has_more {
                    break;
                }
                let next = page.next_cursor.ok_or_else(|| {
                    Error::Internal("presence history range has no continuation".into())
                })?;
                if request
                    .cursor
                    .as_ref()
                    .is_some_and(|cursor| next.serial <= cursor.serial)
                {
                    return Err(Error::Internal(
                        "presence history range lookup did not advance".into(),
                    ));
                }
                request.cursor = Some(next);
            }
            if last
                .checked_sub(first)
                .and_then(|width| width.checked_add(1))
                != Some(count)
            {
                return Err(Error::Internal(
                    "presence history range lookup returned incomplete coverage".into(),
                ));
            }
            // Exact complete retained coverage safely refines false positives,
            // including bits left behind by partial interval expiration.
            cache.membership[index] = TransitionMembershipRange { first, last, words };
        }
        Ok((found_dedupe, latest_user))
    }
}

#[async_trait]
impl PresenceHistoryStore for DurablePresenceHistoryStore {
    async fn record_transition(&self, record: PresenceHistoryTransitionRecord) -> Result<()> {
        let started = Instant::now();
        // Stable stripes preserve same-node serialization even when an active
        // disposable cache is evicted. At most128 cache owners are active; with
        // 64 retained entries, charged cache metadata is bounded by48MiB.
        let mut hash = std::hash::DefaultHasher::new();
        (&record.app_id, &record.channel).hash(&mut hash);
        let _transition = self.transition_locks[hash.finish() as usize % TRANSITION_LOCK_STRIPES]
            .lock()
            .await;
        let channel_cache = self.channel_cache(&record.app_id, &record.channel).await;
        // Serialize this channel's check and append. Stable bounded stripes
        // also serialize hash collisions; other nodes are observed via inspection.
        let mut cache = channel_cache.lock().await;
        let (found_dedupe, found_same_state) =
            self.find_existing_transition(&record, &mut cache).await?;
        if found_dedupe || found_same_state {
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.track_presence_history_write_latency(
                    &record.app_id,
                    started.elapsed().as_secs_f64() * 1000.0,
                );
            }
            return Ok(());
        }

        let reservation = self
            .history_store
            .reserve_publish_position(&record.app_id, &Self::durable_channel_name(&record.channel))
            .await;

        let reservation = match reservation {
            Ok(reservation) => reservation,
            Err(error) => {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.mark_presence_history_write_failure(&record.app_id);
                    metrics.track_presence_history_write_latency(
                        &record.app_id,
                        started.elapsed().as_secs_f64() * 1000.0,
                    );
                }
                return Err(error);
            }
        };

        let stream_id = reservation.stream_id.clone();
        let append = self
            .history_store
            .append(HistoryAppendRecord {
                app_id: record.app_id.clone(),
                channel: Self::durable_channel_name(&record.channel),
                stream_id,
                serial: reservation.serial,
                published_at_ms: record.published_at_ms,
                message_id: None,
                event_name: Some(format!("presence:{}", record.event_kind.as_str())),
                operation_kind: "append".to_string(),
                payload_bytes: Self::encode_payload(&record)?,
                retention: Self::history_retention(&record),
            })
            .await;

        match append {
            Ok(()) => {
                if cache.stream_id.as_deref() != Some(&reservation.stream_id) {
                    let empty_covered = cache.coverage_valid && cache.covered_through.is_none();
                    *cache = DurablePresenceTransitionCache {
                        stream_id: Some(reservation.stream_id.clone()),
                        coverage_valid: empty_covered,
                        ..Default::default()
                    };
                }
                let contiguous_append = cache.coverage_valid
                    && cache
                        .covered_through
                        .map_or(reservation.serial == 1, |serial| {
                            serial.checked_add(1) == Some(reservation.serial)
                        });
                cache.coverage_valid = contiguous_append;
                if contiguous_append {
                    cache.coverage_oldest.get_or_insert(reservation.serial);
                    cache.covered_through = Some(reservation.serial);
                }
                cache.insert(
                    record.dedupe_key.clone(),
                    record.user_id.clone(),
                    record.event_kind,
                    reservation.serial,
                );
                drop(cache);
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.mark_presence_history_write(&record.app_id);
                    metrics.track_presence_history_write_latency(
                        &record.app_id,
                        started.elapsed().as_secs_f64() * 1000.0,
                    );
                }
                self.update_retained_metrics(&record.app_id, &record.channel)
                    .await?;
                Ok(())
            }
            Err(error) => {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.mark_presence_history_write_failure(&record.app_id);
                    metrics.track_presence_history_write_latency(
                        &record.app_id,
                        started.elapsed().as_secs_f64() * 1000.0,
                    );
                }
                Err(error)
            }
        }
    }

    async fn read_page(&self, request: PresenceHistoryReadRequest) -> Result<PresenceHistoryPage> {
        request.validate()?;
        let history_page = self
            .history_store
            .read_page(Self::history_read_request(&request, request.limit))
            .await?;
        let runtime_state = self
            .stream_runtime_state(&request.app_id, &request.channel)
            .await?;

        let mut items = Vec::with_capacity(history_page.items.len());
        for item in history_page.items {
            let (presence_item, _) = Self::decode_item(item)?;
            items.push(presence_item);
        }

        Ok(PresenceHistoryPage {
            items,
            next_cursor: history_page
                .next_cursor
                .map(|cursor| PresenceHistoryCursor {
                    version: cursor.version,
                    app_id: cursor.app_id,
                    channel: request.channel.clone(),
                    stream_id: cursor.stream_id,
                    serial: cursor.serial,
                    direction: request.direction,
                    bounds: request.bounds.clone(),
                }),
            retained: Self::retained_from_history(history_page.retained),
            has_more: history_page.has_more,
            complete: history_page.complete && runtime_state.continuity_proven,
            truncated_by_retention: history_page.truncated_by_retention,
            degraded: !runtime_state.continuity_proven,
        })
    }

    async fn stream_runtime_state(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<PresenceHistoryStreamRuntimeState> {
        let state = self
            .history_store
            .stream_runtime_state(app_id, &Self::durable_channel_name(channel))
            .await?;
        Ok(Self::map_runtime_state(channel, state))
    }

    async fn stream_inspection(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<PresenceHistoryStreamInspection> {
        let inspection = self
            .history_store
            .stream_inspection(app_id, &Self::durable_channel_name(channel))
            .await?;
        Ok(PresenceHistoryStreamInspection {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            stream_id: inspection.stream_id,
            next_serial: inspection.next_serial,
            retained: Self::retained_from_history(inspection.retained),
            state: Self::map_runtime_state(channel, inspection.state),
        })
    }

    async fn reset_stream(
        &self,
        app_id: &str,
        channel: &str,
        reason: &str,
        requested_by: Option<&str>,
    ) -> Result<PresenceHistoryResetResult> {
        let mut hash = std::hash::DefaultHasher::new();
        (app_id, channel).hash(&mut hash);
        let _transition = self.transition_locks[hash.finish() as usize % TRANSITION_LOCK_STRIPES]
            .lock()
            .await;
        let result = self
            .history_store
            .reset_stream(
                app_id,
                &Self::durable_channel_name(channel),
                reason,
                requested_by,
            )
            .await?;
        self.transition_cache
            .write()
            .await
            .remove(&Self::cache_key(app_id, channel));
        self.update_retained_metrics(app_id, channel).await?;
        Ok(PresenceHistoryResetResult {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            previous_stream_id: result.previous_stream_id,
            new_stream_id: result.new_stream_id,
            purged_events: result.purged_messages,
            purged_bytes: result.purged_bytes,
            inspection: PresenceHistoryStreamInspection {
                app_id: app_id.to_string(),
                channel: channel.to_string(),
                stream_id: result.inspection.stream_id,
                next_serial: result.inspection.next_serial,
                retained: Self::retained_from_history(result.inspection.retained),
                state: Self::map_runtime_state(channel, result.inspection.state),
            },
        })
    }

    async fn runtime_status(&self) -> Result<PresenceHistoryRuntimeStatus> {
        let history_status = self.history_store.runtime_status().await?;
        Ok(PresenceHistoryRuntimeStatus {
            enabled: history_status.enabled,
            backend: history_status.backend,
            state_authority: history_status.state_authority,
            degraded_channels: history_status.degraded_channels,
            reset_required_channels: history_status.reset_required_channels,
            queue_depth: history_status.queue_depth,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::{MemoryHistoryStore, MemoryHistoryStoreConfig, now_ms};
    use crate::presence_history::PresenceSnapshotRequest;
    use crate::presence_history::test_support::transition;

    #[tokio::test]
    async fn durable_presence_history_round_trips_over_history_store() {
        let history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let store = DurablePresenceHistoryStore::new(history, None);
        let base = now_ms();

        store
            .record_transition(transition(
                base,
                "join-alice",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 1,
                "join-bob",
                PresenceHistoryEventKind::MemberAdded,
                "bob",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 2,
                "leave-bob",
                PresenceHistoryEventKind::MemberRemoved,
                "bob",
            ))
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: PresenceHistoryQueryBounds::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 3);
        assert_eq!(page.items[0].user_id, "alice");
        assert_eq!(page.items[1].user_id, "bob");
        assert_eq!(page.items[2].event, PresenceHistoryEventKind::MemberRemoved);

        let status = store.runtime_status().await.unwrap();
        assert_eq!(status.backend, "memory");

        let inspection = store
            .stream_inspection("app", "presence-room")
            .await
            .unwrap();
        assert_eq!(inspection.channel, "presence-room");
        assert_eq!(inspection.retained.retained_events, 3);
    }

    #[tokio::test]
    async fn durable_presence_history_dedupes_and_suppresses_same_state() {
        let history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let store = DurablePresenceHistoryStore::new(history, None);
        let base = now_ms();

        store
            .record_transition(transition(
                base,
                "join-alice-1",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 1,
                "join-alice-1",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 2,
                "join-alice-2",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 3,
                "leave-alice-1",
                PresenceHistoryEventKind::MemberRemoved,
                "alice",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 4,
                "leave-alice-2",
                PresenceHistoryEventKind::MemberRemoved,
                "alice",
            ))
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: PresenceHistoryQueryBounds::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].event, PresenceHistoryEventKind::MemberAdded);
        assert_eq!(page.items[1].event, PresenceHistoryEventKind::MemberRemoved);
    }

    #[tokio::test]
    async fn durable_presence_history_reuses_cached_latest_state() {
        let history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let store = DurablePresenceHistoryStore::new(history, None);
        let base = now_ms();

        store
            .record_transition(transition(
                base,
                "join-alice-1",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();

        {
            let cache = store.transition_cache.read().await;
            let channel = cache
                .get(&DurablePresenceHistoryStore::cache_key(
                    "app",
                    "presence-room",
                ))
                .unwrap()
                .lock()
                .await;
            assert_eq!(
                channel.latest_event_by_user.get("alice"),
                Some(&(PresenceHistoryEventKind::MemberAdded, 1))
            );
        }

        store
            .record_transition(transition(
                base + 1,
                "join-alice-2",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: PresenceHistoryQueryBounds::default(),
            })
            .await
            .unwrap();

        assert_eq!(page.items.len(), 1);
    }

    #[tokio::test]
    async fn durable_presence_history_snapshot_and_reset_follow_presence_semantics() {
        let history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let store = DurablePresenceHistoryStore::new(history, None);
        let base = now_ms();

        store
            .record_transition(transition(
                base,
                "join-alice",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 1,
                "join-bob",
                PresenceHistoryEventKind::MemberAdded,
                "bob",
            ))
            .await
            .unwrap();
        store
            .record_transition(transition(
                base + 2,
                "leave-bob",
                PresenceHistoryEventKind::MemberRemoved,
                "bob",
            ))
            .await
            .unwrap();

        let snapshot = store
            .snapshot_at(PresenceSnapshotRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                at_time_ms: None,
                at_serial: None,
            })
            .await
            .unwrap();
        assert_eq!(snapshot.members.len(), 1);
        assert_eq!(snapshot.members[0].user_id, "alice");

        let before = store
            .stream_inspection("app", "presence-room")
            .await
            .unwrap();
        let previous_stream_id = before.stream_id.clone().unwrap();

        let reset = store
            .reset_stream("app", "presence-room", "operator reset", Some("ops"))
            .await
            .unwrap();
        assert_eq!(reset.purged_events, 3);
        assert_eq!(
            reset.previous_stream_id.as_deref(),
            Some(previous_stream_id.as_str())
        );
        assert_ne!(reset.new_stream_id, previous_stream_id);

        let page = store
            .read_page(PresenceHistoryReadRequest {
                app_id: "app".to_string(),
                channel: "presence-room".to_string(),
                direction: PresenceHistoryDirection::OldestFirst,
                limit: 10,
                cursor: None,
                bounds: PresenceHistoryQueryBounds::default(),
            })
            .await
            .unwrap();
        assert!(page.items.is_empty());
    }
    #[tokio::test]
    async fn durable_presence_cache_observes_other_nodes_and_late_commits() {
        let history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let first = DurablePresenceHistoryStore::new(history.clone(), None);
        let second = DurablePresenceHistoryStore::new(history.clone(), None);
        let base = now_ms();
        first
            .record_transition(transition(
                base,
                "join",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();
        second
            .record_transition(transition(
                base + 1,
                "leave",
                PresenceHistoryEventKind::MemberRemoved,
                "alice",
            ))
            .await
            .unwrap();
        first
            .record_transition(transition(
                base + 2,
                "rejoin",
                PresenceHistoryEventKind::MemberAdded,
                "alice",
            ))
            .await
            .unwrap();
        assert_eq!(
            first
                .stream_inspection("app", "presence-room")
                .await
                .unwrap()
                .retained
                .retained_events,
            3
        );

        // An earlier reservation can commit after this node has observed a
        // newer serial. Gaps must invalidate negative cache evidence.
        let durable_channel = DurablePresenceHistoryStore::durable_channel_name("presence-room");
        let pending = history
            .reserve_publish_position("app", &durable_channel)
            .await
            .unwrap();
        first
            .record_transition(transition(
                base + 4,
                "bob-join",
                PresenceHistoryEventKind::MemberAdded,
                "bob",
            ))
            .await
            .unwrap();
        let late = transition(
            base + 3,
            "carol-join",
            PresenceHistoryEventKind::MemberAdded,
            "carol",
        );
        history
            .append(HistoryAppendRecord {
                app_id: late.app_id.clone(),
                channel: durable_channel,
                stream_id: pending.stream_id,
                serial: pending.serial,
                published_at_ms: late.published_at_ms,
                message_id: None,
                event_name: None,
                operation_kind: "append".into(),
                payload_bytes: DurablePresenceHistoryStore::encode_payload(&late).unwrap(),
                retention: DurablePresenceHistoryStore::history_retention(&late),
            })
            .await
            .unwrap();
        first
            .record_transition(transition(
                base + 5,
                "carol-duplicate",
                PresenceHistoryEventKind::MemberAdded,
                "carol",
            ))
            .await
            .unwrap();
        assert_eq!(
            first
                .stream_inspection("app", "presence-room")
                .await
                .unwrap()
                .retained
                .retained_events,
            5
        );
    }

    #[tokio::test]
    async fn durable_presence_same_node_concurrent_duplicates_append_once() {
        let history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let store = DurablePresenceHistoryStore::new(history, None);
        let base = now_ms();
        let mut jobs = tokio::task::JoinSet::new();
        for n in 0..32 {
            let store = store.clone();
            jobs.spawn(async move {
                store
                    .record_transition(transition(
                        base,
                        &format!("join-{n}"),
                        PresenceHistoryEventKind::MemberAdded,
                        "alice",
                    ))
                    .await
                    .unwrap();
            });
        }
        while let Some(job) = jobs.join_next().await {
            job.unwrap();
        }
        assert_eq!(
            store
                .stream_inspection("app", "presence-room")
                .await
                .unwrap()
                .retained
                .retained_events,
            1
        );
    }
}

#[cfg(test)]
#[path = "durable/review_tests.rs"]
mod review_tests;
