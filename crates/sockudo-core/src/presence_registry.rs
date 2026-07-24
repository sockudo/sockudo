//! Protocol-neutral current-presence authority.

use crate::error::{Error, Result};
use ahash::{AHashMap, AHasher, RandomState};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use papaya::HashMap as PapayaHashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const COORDINATION_SHARDS: usize = 4_096;
type PresenceDashMap<K, V> = DashMap<K, V, RandomState>;
type PresenceAppMap = PapayaHashMap<String, PresenceAppState, RandomState>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PresenceConnectionIdentity {
    pub app_id: String,
    pub connection_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PresenceRecord {
    pub connection_id: String,
    pub client_id: String,
    pub id: String,
    pub data: Option<Value>,
    pub encoding: Option<String>,
    pub extras: Option<Value>,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PresenceChangeAction {
    Enter,
    Update,
    Leave,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PresenceChange {
    pub action: PresenceChangeAction,
    pub member: PresenceRecord,
    /// Protocol projections may intentionally omit an id (for example a
    /// synthetic superseded-member leave) while retaining the durable member id.
    pub wire_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PresenceReplication {
    pub changes: Vec<PresenceChange>,
    /// Set only after all leaves caused by a terminal connection removal.
    pub unregister_connection: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PresenceTransition {
    pub previous: Option<PresenceRecord>,
    pub first_for_client: bool,
    pub last_for_client: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PresenceRemoval {
    pub channel: String,
    pub member: PresenceRecord,
    pub last_for_client: bool,
}

struct PresenceClientMembers {
    capacity_shard: usize,
    members: ClientMembers,
}

enum ClientMembers {
    Empty,
    One(PresenceRecord),
    Many(AHashMap<String, PresenceRecord>),
}

impl PresenceClientMembers {
    fn one(member: PresenceRecord, capacity_shard: usize) -> Self {
        Self {
            capacity_shard,
            members: ClientMembers::One(member),
        }
    }

    fn is_empty(&self) -> bool {
        matches!(self.members, ClientMembers::Empty)
    }

    fn contains_connection(&self, connection_id: &str) -> bool {
        match &self.members {
            ClientMembers::Empty => false,
            ClientMembers::One(member) => member.connection_id == connection_id,
            ClientMembers::Many(members) => members.contains_key(connection_id),
        }
    }

    fn insert(&mut self, member: PresenceRecord) -> Option<PresenceRecord> {
        match &mut self.members {
            ClientMembers::Empty => {
                self.members = ClientMembers::One(member);
                None
            }
            ClientMembers::One(current) if current.connection_id == member.connection_id => {
                Some(std::mem::replace(current, member))
            }
            ClientMembers::One(_) => {
                let ClientMembers::One(current) =
                    std::mem::replace(&mut self.members, ClientMembers::Empty)
                else {
                    unreachable!();
                };
                let mut members = AHashMap::with_capacity(2);
                members.insert(current.connection_id.clone(), current);
                members.insert(member.connection_id.clone(), member);
                self.members = ClientMembers::Many(members);
                None
            }
            ClientMembers::Many(members) => members.insert(member.connection_id.clone(), member),
        }
    }

    fn remove(&mut self, connection_id: &str) -> Option<PresenceRecord> {
        match &mut self.members {
            ClientMembers::Empty => None,
            ClientMembers::One(member) if member.connection_id == connection_id => {
                let ClientMembers::One(member) =
                    std::mem::replace(&mut self.members, ClientMembers::Empty)
                else {
                    unreachable!();
                };
                Some(member)
            }
            ClientMembers::One(_) => None,
            ClientMembers::Many(members) => {
                let removed = members.remove(connection_id);
                if members.len() == 1 {
                    let ClientMembers::Many(members) =
                        std::mem::replace(&mut self.members, ClientMembers::Empty)
                    else {
                        unreachable!();
                    };
                    let remaining = members
                        .into_values()
                        .next()
                        .expect("one presence member remains");
                    self.members = ClientMembers::One(remaining);
                }
                removed
            }
        }
    }

    fn connection_ids_except(&self, connection_id: &str) -> Vec<String> {
        match &self.members {
            ClientMembers::Empty => Vec::new(),
            ClientMembers::One(member) if member.connection_id != connection_id => {
                vec![member.connection_id.clone()]
            }
            ClientMembers::One(_) => Vec::new(),
            ClientMembers::Many(members) => members
                .keys()
                .filter(|candidate| candidate.as_str() != connection_id)
                .cloned()
                .collect(),
        }
    }

    fn extend_records(&self, output: &mut Vec<PresenceRecord>) {
        match &self.members {
            ClientMembers::Empty => {}
            ClientMembers::One(member) => output.push(member.clone()),
            ClientMembers::Many(members) => output.extend(members.values().cloned()),
        }
    }
}

struct PresenceChannelState {
    clients: PresenceDashMap<String, PresenceClientMembers>,
}

impl PresenceChannelState {
    fn new() -> Self {
        Self {
            clients: DashMap::with_hasher(RandomState::new()),
        }
    }

    fn contains_connection(&self, connection_id: &str) -> bool {
        self.clients
            .iter()
            .any(|client| client.value().contains_connection(connection_id))
    }
}

struct PresenceConnectionState {
    channels: PresenceDashMap<String, Arc<PresenceChannelState>>,
}

impl PresenceConnectionState {
    fn new() -> Self {
        Self {
            channels: DashMap::with_hasher(RandomState::new()),
        }
    }
}

struct PresenceAppState {
    connections: PapayaHashMap<String, PresenceConnectionState, RandomState>,
    channels: PresenceDashMap<String, Arc<PresenceChannelState>>,
}

impl PresenceAppState {
    fn new() -> Self {
        Self {
            connections: PapayaHashMap::with_hasher(RandomState::new()),
            channels: DashMap::with_hasher(RandomState::new()),
        }
    }
}

#[derive(Default)]
struct PresenceCapacityShard {
    active: usize,
    reserved: usize,
}

/// Bounded, concurrency-safe current-presence registry shared by protocol edges.
///
/// The registry is deliberately synchronous: it never performs I/O and its
/// connection coordination and per-client map guards are held only for short
/// in-memory state transitions. Durable history and fanout remain
/// responsibilities of the async presence service layered above it.
pub struct PresenceRegistry {
    apps: PresenceAppMap,
    connection_locks: [Mutex<()>; COORDINATION_SHARDS],
    capacity_shards: [Mutex<PresenceCapacityShard>; COORDINATION_SHARDS],
    reserved_capacity: AtomicUsize,
    capacity_batch: usize,
    max_members: usize,
    max_cached_channels: usize,
}

impl PresenceRegistry {
    #[must_use]
    pub fn new(max_members: usize) -> Self {
        let max_members = max_members.max(1);
        Self {
            apps: PapayaHashMap::with_hasher(RandomState::new()),
            connection_locks: std::array::from_fn(|_| Mutex::new(())),
            capacity_shards: std::array::from_fn(|_| Mutex::new(PresenceCapacityShard::default())),
            reserved_capacity: AtomicUsize::new(0),
            capacity_batch: (max_members / COORDINATION_SHARDS).clamp(1, 64),
            max_members,
            max_cached_channels: max_members.min(128),
        }
    }

    fn coordination_shard<T: Hash>(key: T) -> usize {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        (hasher.finish() as usize) & (COORDINATION_SHARDS - 1)
    }

    fn connection_lock(&self, app_id: &str, connection_id: &str) -> &Mutex<()> {
        &self.connection_locks[Self::coordination_shard((app_id, connection_id))]
    }

    fn has_connection(&self, app_id: &str, connection_id: &str) -> bool {
        self.apps
            .pin()
            .get(app_id)
            .is_some_and(|app| app.connections.pin().contains_key(connection_id))
    }

    pub fn register_connection(&self, app_id: &str, connection_id: &str) -> bool {
        let _guard = self.connection_lock(app_id, connection_id).lock();
        let apps = self.apps.pin();
        let app = apps.get_or_insert_with(app_id.to_string(), PresenceAppState::new);
        let connections = app.connections.pin();
        let inserted = !connections.contains_key(connection_id);
        connections.get_or_insert_with(connection_id.to_string(), PresenceConnectionState::new);
        inserted
    }

    #[must_use]
    pub fn contains_connection(&self, app_id: &str, connection_id: &str) -> bool {
        self.has_connection(app_id, connection_id)
    }

    fn capacity_shard(app_id: &str, channel: &str, client_id: &str) -> usize {
        Self::coordination_shard((app_id, channel, client_id))
    }

    fn reclaim_unused_capacity(&self) {
        for shard in &self.capacity_shards {
            let mut shard = shard.lock();
            let unused = shard.reserved - shard.active;
            if unused > 0 {
                shard.reserved = shard.active;
                self.reserved_capacity.fetch_sub(unused, Ordering::Relaxed);
            }
        }
    }

    fn reserve_member(&self, shard_index: usize) -> Result<()> {
        for attempt in 0..2 {
            let mut shard = self.capacity_shards[shard_index].lock();
            if shard.active < shard.reserved {
                shard.active += 1;
                return Ok(());
            }

            let reservation = self.reserved_capacity.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |reserved| {
                    let available = self.max_members.saturating_sub(reserved);
                    (available > 0).then_some(reserved + available.min(self.capacity_batch))
                },
            );
            if let Ok(previous) = reservation {
                let granted = (self.max_members - previous).min(self.capacity_batch);
                shard.reserved += granted;
                shard.active += 1;
                return Ok(());
            }
            drop(shard);
            if attempt == 0 {
                self.reclaim_unused_capacity();
            }
        }
        Err(Error::OverCapacity)
    }

    fn release_member(&self, shard_index: usize) {
        let mut shard = self.capacity_shards[shard_index].lock();
        debug_assert!(shard.active > 0, "presence capacity underflow");
        shard.active = shard.active.saturating_sub(1);
        if self.max_members <= COORDINATION_SHARDS && shard.active == 0 {
            let released = shard.reserved;
            shard.reserved = 0;
            self.reserved_capacity
                .fetch_sub(released, Ordering::Relaxed);
        }
    }

    fn prune_empty_channels(app: &PresenceAppState, limit: usize) {
        let mut remaining = limit;
        let candidates = app
            .channels
            .iter()
            .filter(|channel| {
                Arc::strong_count(channel.value()) == 1 && channel.value().clients.is_empty()
            })
            .map(|channel| channel.key().clone())
            .take(remaining)
            .collect::<Vec<_>>();
        for channel in candidates {
            if app
                .channels
                .remove_if(&channel, |_, state| {
                    Arc::strong_count(state) == 1 && state.clients.is_empty()
                })
                .is_some()
            {
                remaining -= 1;
                if remaining == 0 {
                    return;
                }
            }
        }
    }

    fn app_channel_state(
        &self,
        app: &PresenceAppState,
        channel: &str,
    ) -> Arc<PresenceChannelState> {
        if let Some(state) = app.channels.get(channel) {
            return Arc::clone(&state);
        }
        if app.channels.len() >= self.max_cached_channels {
            Self::prune_empty_channels(app, 16);
        }
        match app.channels.entry(channel.to_string()) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => {
                let state = Arc::new(PresenceChannelState::new());
                entry.insert(Arc::clone(&state));
                state
            }
        }
    }

    fn prune_connection_channels(
        connection: &PresenceConnectionState,
        connection_id: &str,
        limit: usize,
    ) {
        let candidates = connection
            .channels
            .iter()
            .filter(|channel| !channel.value().contains_connection(connection_id))
            .map(|channel| channel.key().clone())
            .take(limit)
            .collect::<Vec<_>>();
        for channel in candidates {
            connection.channels.remove_if(&channel, |_, state| {
                !state.contains_connection(connection_id)
            });
        }
    }

    fn apply_present(
        &self,
        app_id: &str,
        channel: &str,
        member: PresenceRecord,
    ) -> Result<PresenceTransition> {
        let _connection_guard = self.connection_lock(app_id, &member.connection_id).lock();
        let apps = self.apps.pin();
        let Some(app) = apps.get(app_id) else {
            return Err(Error::ConnectionNotFound);
        };
        let connections = app.connections.pin();
        let Some(connection) = connections.get(&member.connection_id) else {
            return Err(Error::ConnectionNotFound);
        };
        let state = loop {
            if let Some(state) = connection.channels.get(channel) {
                break state;
            }
            if connection.channels.len() >= self.max_cached_channels {
                Self::prune_connection_channels(connection, &member.connection_id, 16);
            }
            let state = self.app_channel_state(app, channel);
            connection
                .channels
                .entry(channel.to_string())
                .or_insert(state);
        };

        let client_id = member.client_id.clone();
        let (previous, first_for_client) = match state.clients.entry(client_id) {
            Entry::Occupied(mut client) => {
                let first = client.get().is_empty();
                let inserted = !client.get().contains_connection(&member.connection_id);
                if inserted {
                    self.reserve_member(client.get().capacity_shard)?;
                }
                let previous = client.get_mut().insert(member);
                (previous, inserted && first)
            }
            Entry::Vacant(client) => {
                let capacity_shard = Self::capacity_shard(app_id, channel, &member.client_id);
                self.reserve_member(capacity_shard)?;
                client.insert(PresenceClientMembers::one(member, capacity_shard));
                (None, true)
            }
        };
        Ok(PresenceTransition {
            previous,
            first_for_client,
            last_for_client: false,
        })
    }

    /// Add or replace a current member.
    pub fn enter(
        &self,
        app_id: &str,
        channel: &str,
        member: PresenceRecord,
    ) -> Result<PresenceTransition> {
        self.apply_present(app_id, channel, member)
    }

    /// Update a current member, entering it if no prior record exists.
    pub fn update(
        &self,
        app_id: &str,
        channel: &str,
        member: PresenceRecord,
    ) -> Result<PresenceTransition> {
        self.apply_present(app_id, channel, member)
    }

    fn leave_from_channel(
        &self,
        state: &PresenceChannelState,
        connection_id: &str,
        client_id: &str,
    ) -> Option<PresenceTransition> {
        let mut previous = None;
        let mut last_for_client = false;
        let mut capacity_shard = None;
        state.clients.remove_if_mut(client_id, |_, client| {
            previous = client.remove(connection_id);
            if previous.is_some() {
                capacity_shard = Some(client.capacity_shard);
            }
            last_for_client = previous.is_some() && client.is_empty();
            last_for_client
        });
        let previous = previous?;
        self.release_member(capacity_shard.expect("removed member has a capacity shard"));
        Some(PresenceTransition {
            previous: Some(previous),
            first_for_client: false,
            last_for_client,
        })
    }

    fn leave_while_connection_locked(
        &self,
        app_id: &str,
        channel: &str,
        connection_id: &str,
        client_id: &str,
    ) -> Option<PresenceTransition> {
        let apps = self.apps.pin();
        let app = apps.get(app_id)?;
        let connections = app.connections.pin();
        let connection = connections.get(connection_id)?;
        let state = connection.channels.get(channel)?;
        self.leave_from_channel(&state, connection_id, client_id)
    }

    /// Remove one `(connection, client)` member. Duplicate leaves are no-ops.
    pub fn leave(
        &self,
        app_id: &str,
        channel: &str,
        connection_id: &str,
        client_id: &str,
    ) -> Result<Option<PresenceTransition>> {
        let _connection_guard = self.connection_lock(app_id, connection_id).lock();
        let transition =
            self.leave_while_connection_locked(app_id, channel, connection_id, client_id);
        Ok(transition)
    }

    /// Remove every member owned by a connection exactly once.
    pub fn unregister_connection(&self, app_id: &str, connection_id: &str) -> Vec<PresenceRemoval> {
        let _connection_guard = self.connection_lock(app_id, connection_id).lock();
        let apps = self.apps.pin();
        let Some(app) = apps.get(app_id) else {
            return Vec::new();
        };
        let connections = app.connections.pin();
        let Some(connection) = connections.get(connection_id) else {
            return Vec::new();
        };

        let mut owned = Vec::new();
        for channel in connection.channels.iter() {
            for client in channel.value().clients.iter() {
                if client.value().contains_connection(connection_id) {
                    owned.push((channel.key().clone(), client.key().clone()));
                }
            }
        }
        owned.sort_unstable();
        let mut removals = Vec::with_capacity(owned.len());
        for (channel, client_id) in owned {
            if let Some(state) = connection.channels.get(&channel)
                && let Some(transition) = self.leave_from_channel(&state, connection_id, &client_id)
                && let Some(member) = transition.previous
            {
                removals.push(PresenceRemoval {
                    channel,
                    member,
                    last_for_client: transition.last_for_client,
                });
            }
        }
        connections.remove(connection_id);
        removals
    }

    /// Remove every member owned by a connection on one channel while keeping
    /// the connection registered for other channel attachments.
    pub fn detach_connection(
        &self,
        app_id: &str,
        channel: &str,
        connection_id: &str,
    ) -> Vec<PresenceRemoval> {
        let _connection_guard = self.connection_lock(app_id, connection_id).lock();
        let apps = self.apps.pin();
        let Some(app) = apps.get(app_id) else {
            return Vec::new();
        };
        let connections = app.connections.pin();
        let Some(connection) = connections.get(connection_id) else {
            return Vec::new();
        };
        let Some(state) = connection.channels.get(channel) else {
            return Vec::new();
        };
        let mut client_ids = state
            .clients
            .iter()
            .filter(|client| client.value().contains_connection(connection_id))
            .map(|client| client.key().clone())
            .collect::<Vec<_>>();
        client_ids.sort_unstable();
        let mut removals = Vec::with_capacity(client_ids.len());
        for client_id in client_ids {
            if let Some(transition) = self.leave_from_channel(&state, connection_id, &client_id)
                && let Some(member) = transition.previous
            {
                removals.push(PresenceRemoval {
                    channel: channel.to_string(),
                    member,
                    last_for_client: transition.last_for_client,
                });
            }
        }
        drop(state);
        connection.channels.remove(channel);
        removals
    }

    /// Remove same-client records owned by superseded connections.
    pub fn remove_client_except(
        &self,
        app_id: &str,
        channel: &str,
        client_id: &str,
        connection_id: &str,
    ) -> Vec<PresenceRemoval> {
        let apps = self.apps.pin();
        let Some(app) = apps.get(app_id) else {
            return Vec::new();
        };
        let Some(state) = app.channels.get(channel) else {
            return Vec::new();
        };
        let mut connections = state
            .clients
            .get(client_id)
            .map(|client| client.connection_ids_except(connection_id))
            .unwrap_or_default();
        connections.sort_unstable();
        let mut removals = Vec::with_capacity(connections.len());
        for old_connection_id in connections {
            if let Ok(Some(transition)) = self.leave(app_id, channel, &old_connection_id, client_id)
                && let Some(member) = transition.previous
            {
                removals.push(PresenceRemoval {
                    channel: channel.to_string(),
                    member,
                    last_for_client: transition.last_for_client,
                });
            }
        }
        removals
    }

    /// Return a deterministic current snapshot for SYNC and REST projections.
    #[must_use]
    pub fn snapshot(&self, app_id: &str, channel: &str) -> Vec<PresenceRecord> {
        let apps = self.apps.pin();
        let Some(app) = apps.get(app_id) else {
            return Vec::new();
        };
        let Some(state) = app.channels.get(channel) else {
            return Vec::new();
        };
        let mut members = Vec::new();
        for client in state.clients.iter() {
            client.value().extend_records(&mut members);
        }
        members.sort_unstable_by(|left, right| {
            (&left.connection_id, &left.client_id).cmp(&(&right.connection_id, &right.client_id))
        });
        members
    }

    #[must_use]
    pub fn member_count(&self) -> usize {
        self.capacity_shards
            .iter()
            .map(|shard| shard.lock().active)
            .sum()
    }
}

impl Default for PresenceRegistry {
    fn default() -> Self {
        Self::new(100_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    fn member(connection_id: &str, client_id: &str, serial: u64) -> PresenceRecord {
        PresenceRecord {
            connection_id: connection_id.to_string(),
            client_id: client_id.to_string(),
            id: format!("{connection_id}:{serial}:0"),
            data: Some(sonic_rs::json!({ "serial": serial })),
            encoding: Some("json".to_string()),
            extras: None,
            timestamp_ms: i64::try_from(serial).unwrap_or(i64::MAX),
        }
    }

    #[test]
    fn one_connection_can_authoritatively_hold_multiple_clients() {
        let registry = PresenceRegistry::new(128);
        registry.register_connection("app", "connection-a");

        registry
            .enter("app", "room", member("connection-a", "one", 1))
            .unwrap();
        registry
            .enter("app", "room", member("connection-a", "two", 2))
            .unwrap();

        let snapshot = registry.snapshot("app", "room");
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].id, "connection-a:1:0");
        assert_eq!(snapshot[1].id, "connection-a:2:0");
    }

    #[test]
    fn multiple_connections_preserve_first_join_and_last_leave() {
        let registry = PresenceRegistry::new(128);
        registry.register_connection("app", "connection-a");
        registry.register_connection("app", "connection-b");

        let first = registry
            .enter("app", "room", member("connection-a", "client", 1))
            .unwrap();
        let second = registry
            .enter("app", "room", member("connection-b", "client", 1))
            .unwrap();
        assert!(first.first_for_client);
        assert!(!second.first_for_client);

        let first_leave = registry
            .leave("app", "room", "connection-a", "client")
            .unwrap()
            .unwrap();
        let last_leave = registry
            .leave("app", "room", "connection-b", "client")
            .unwrap()
            .unwrap();
        assert!(!first_leave.last_for_client);
        assert!(last_leave.last_for_client);
    }

    #[test]
    fn disconnect_is_idempotent_and_removes_every_client_once() {
        let registry = PresenceRegistry::new(128);
        registry.register_connection("app", "connection-a");
        registry
            .enter("app", "one", member("connection-a", "client-one", 1))
            .unwrap();
        registry
            .enter("app", "two", member("connection-a", "client-two", 2))
            .unwrap();

        let first = registry.unregister_connection("app", "connection-a");
        let duplicate = registry.unregister_connection("app", "connection-a");

        assert_eq!(first.len(), 2);
        assert!(duplicate.is_empty());
        assert!(registry.snapshot("app", "one").is_empty());
        assert!(registry.snapshot("app", "two").is_empty());
    }

    #[test]
    fn two_node_join_leave_race_has_one_first_join_and_one_last_leave() {
        let registry = Arc::new(PresenceRegistry::new(128));
        registry.register_connection("app", "node-a-connection");
        registry.register_connection("app", "node-b-connection");
        let enter_barrier = Arc::new(Barrier::new(3));

        let enter_threads = ["node-a-connection", "node-b-connection"].map(|connection_id| {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&enter_barrier);
            std::thread::spawn(move || {
                barrier.wait();
                registry
                    .enter("app", "room", member(connection_id, "client", 1))
                    .unwrap()
                    .first_for_client
            })
        });
        enter_barrier.wait();
        let first_joins = enter_threads
            .into_iter()
            .map(|thread| usize::from(thread.join().unwrap()))
            .sum::<usize>();
        assert_eq!(first_joins, 1);

        let leave_barrier = Arc::new(Barrier::new(3));
        let leave_threads = ["node-a-connection", "node-b-connection"].map(|connection_id| {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&leave_barrier);
            std::thread::spawn(move || {
                barrier.wait();
                registry
                    .leave("app", "room", connection_id, "client")
                    .unwrap()
                    .unwrap()
                    .last_for_client
            })
        });
        leave_barrier.wait();
        let last_leaves = leave_threads
            .into_iter()
            .map(|thread| usize::from(thread.join().unwrap()))
            .sum::<usize>();
        assert_eq!(last_leaves, 1);
    }

    #[test]
    fn transient_identifiers_do_not_accumulate_coordination_locks() {
        let registry = PresenceRegistry::new(1);
        for index in 0..1_000 {
            let connection_id = format!("connection-{index}");
            registry.register_connection("app", &connection_id);
            registry
                .enter("app", "room", member(&connection_id, "client", 1))
                .unwrap();
            assert!(matches!(
                registry.enter(
                    "app",
                    "room",
                    member(&connection_id, &format!("over-capacity-{index}"), 2),
                ),
                Err(Error::OverCapacity)
            ));
            registry.unregister_connection("app", &connection_id);
            registry
                .leave(
                    "app",
                    "room",
                    &format!("unknown-{index}"),
                    &format!("unknown-{index}"),
                )
                .unwrap();
        }

        assert_eq!(registry.connection_locks.len(), COORDINATION_SHARDS);
        assert!(
            registry
                .apps
                .pin()
                .get("app")
                .is_some_and(|app| app.channels.len() <= registry.max_cached_channels)
        );
        assert_eq!(registry.member_count(), 0);
    }

    #[test]
    fn detach_removes_only_the_requested_channel_and_keeps_connection_live() {
        let registry = PresenceRegistry::new(128);
        registry.register_connection("app", "connection-a");
        registry
            .enter("app", "one", member("connection-a", "client", 1))
            .unwrap();
        registry
            .enter("app", "two", member("connection-a", "client", 2))
            .unwrap();

        let removals = registry.detach_connection("app", "one", "connection-a");

        assert_eq!(removals.len(), 1);
        assert!(registry.snapshot("app", "one").is_empty());
        assert_eq!(registry.snapshot("app", "two").len(), 1);
        assert!(registry.contains_connection("app", "connection-a"));
    }

    #[test]
    fn sustained_same_client_contention_preserves_first_and_last_transitions() {
        const CONNECTIONS: usize = 8;
        const CYCLES: usize = 500;

        let registry = Arc::new(PresenceRegistry::new(CONNECTIONS));
        for index in 0..CONNECTIONS {
            registry.register_connection("app", &format!("connection-{index}"));
        }
        let barrier = Arc::new(Barrier::new(CONNECTIONS + 1));
        let first_joins = Arc::new(AtomicUsize::new(0));
        let last_leaves = Arc::new(AtomicUsize::new(0));
        let threads = (0..CONNECTIONS)
            .map(|index| {
                let registry = Arc::clone(&registry);
                let barrier = Arc::clone(&barrier);
                let first_joins = Arc::clone(&first_joins);
                let last_leaves = Arc::clone(&last_leaves);
                std::thread::spawn(move || {
                    let connection_id = format!("connection-{index}");
                    barrier.wait();
                    for serial in 0..CYCLES {
                        let entered = registry
                            .enter(
                                "app",
                                "room",
                                member(&connection_id, "shared-client", serial as u64),
                            )
                            .unwrap();
                        if entered.first_for_client {
                            first_joins.fetch_add(1, Ordering::Relaxed);
                        }
                        std::thread::yield_now();
                        let left = registry
                            .leave("app", "room", &connection_id, "shared-client")
                            .unwrap()
                            .unwrap();
                        if left.last_for_client {
                            last_leaves.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        for thread in threads {
            thread.join().unwrap();
        }

        let first_joins = first_joins.load(Ordering::Relaxed);
        let last_leaves = last_leaves.load(Ordering::Relaxed);
        assert!(first_joins > 0);
        assert_eq!(first_joins, last_leaves);
        assert_eq!(registry.member_count(), 0);
        assert!(registry.snapshot("app", "room").is_empty());
    }

    #[test]
    fn empty_channel_cache_remains_bounded_under_identifier_churn() {
        let registry = PresenceRegistry::new(1);
        registry.register_connection("app", "connection");

        for index in 0..1_000 {
            let channel = format!("room-{index}");
            registry
                .enter("app", &channel, member("connection", "client", index))
                .unwrap();
            registry
                .leave("app", &channel, "connection", "client")
                .unwrap()
                .unwrap();
        }

        assert!(
            registry
                .apps
                .pin()
                .get("app")
                .is_some_and(|app| app.channels.len() <= registry.max_cached_channels)
        );
        let apps = registry.apps.pin();
        let app = apps.get("app").unwrap();
        let connections = app.connections.pin();
        assert!(connections.get("connection").is_some_and(|connection| {
            connection.channels.len() <= registry.max_cached_channels
        }));
        assert_eq!(registry.member_count(), 0);
    }

    #[test]
    fn concurrent_capacity_limit_has_exactly_one_winner_per_slot() {
        const CAPACITY: usize = 64;
        const CONTENDERS: usize = 128;

        let registry = Arc::new(PresenceRegistry::new(CAPACITY));
        for index in 0..CONTENDERS {
            registry.register_connection("app", &format!("connection-{index}"));
        }
        let barrier = Arc::new(Barrier::new(CONTENDERS + 1));
        let threads = (0..CONTENDERS)
            .map(|index| {
                let registry = Arc::clone(&registry);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let connection_id = format!("connection-{index}");
                    barrier.wait();
                    registry
                        .enter(
                            "app",
                            "room",
                            member(&connection_id, &format!("client-{index}"), 1),
                        )
                        .is_ok()
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let winners = threads
            .into_iter()
            .map(|thread| usize::from(thread.join().unwrap()))
            .sum::<usize>();

        assert_eq!(winners, CAPACITY);
        assert_eq!(registry.member_count(), CAPACITY);
        for index in 0..CONTENDERS {
            registry.unregister_connection("app", &format!("connection-{index}"));
        }
        assert_eq!(registry.member_count(), 0);
    }

    #[test]
    fn idle_capacity_reservations_are_reclaimed_without_false_overcapacity() {
        const CAPACITY: usize = COORDINATION_SHARDS * 2;

        let registry = PresenceRegistry::new(CAPACITY);
        registry.register_connection("app", "connection");
        let mut representative = vec![None; COORDINATION_SHARDS];
        let mut candidate = 0_u64;
        while representative.iter().any(Option::is_none) {
            let client_id = format!("client-{candidate}");
            let shard = PresenceRegistry::capacity_shard("app", "room", &client_id);
            representative[shard].get_or_insert(client_id);
            candidate += 1;
        }

        for client_id in representative.iter().flatten() {
            registry
                .enter("app", "room", member("connection", client_id, 1))
                .unwrap();
            registry
                .leave("app", "room", "connection", client_id)
                .unwrap()
                .unwrap();
        }
        assert_eq!(registry.reserved_capacity.load(Ordering::Relaxed), CAPACITY);

        let target_shard = 0;
        let mut same_shard_clients = Vec::new();
        while same_shard_clients.len() < 3 {
            let client_id = format!("reclaim-{candidate}");
            candidate += 1;
            if PresenceRegistry::capacity_shard("app", "room", &client_id) == target_shard {
                same_shard_clients.push(client_id);
            }
        }
        for client_id in &same_shard_clients {
            registry
                .enter("app", "room", member("connection", client_id, 2))
                .unwrap();
        }

        assert_eq!(registry.member_count(), same_shard_clients.len());
        registry.unregister_connection("app", "connection");
        assert_eq!(registry.member_count(), 0);
    }
}
