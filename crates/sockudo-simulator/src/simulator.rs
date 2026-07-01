use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use sockudo_core::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDirection, HistoryItem, HistoryPurgeMode,
    HistoryPurgeRequest, HistoryQueryBounds, HistoryReadRequest, HistoryRetentionPolicy,
    HistoryStore, MemoryHistoryStore, MemoryHistoryStoreConfig,
};
use sockudo_core::presence_history::{
    MemoryPresenceHistoryStore, MemoryPresenceHistoryStoreConfig, PresenceHistoryCursor,
    PresenceHistoryDirection, PresenceHistoryEventCause, PresenceHistoryEventKind,
    PresenceHistoryItem, PresenceHistoryQueryBounds, PresenceHistoryReadRequest,
    PresenceHistoryRetentionPolicy, PresenceHistoryStore, PresenceHistoryTransitionRecord,
    PresenceSnapshotRequest,
};
use sockudo_core::version_store::{
    MemoryVersionStore, StoredVersionRecord, VersionReplayRequest, VersionStore,
    VersionStoreCursor, VersionStoreDirection, VersionStoreReadRequest,
};
use sockudo_core::versioned_messages::{
    FieldPatch, MessageAction, MessageAppend, MessageFieldDelta, MessageSerial, VersionMetadata,
    VersionSerial, VersionedMessage,
};
use sockudo_protocol::messages::{MessageData, MessageExtras};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::config::SimulatorConfig;
use crate::error::{SimulatorError, SimulatorResult};
use crate::workload::{ActionWeights, WorkloadAction, WorkloadActionCounts, WorkloadGenerator};

const APP_ID: &str = "sim-app";
const BASE_TIME_MS: i64 = 1_893_456_000_000;

/// Summary emitted by a successful simulator run.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SimulationReport {
    pub seed: u64,
    pub ticks: u64,
    pub operations: u64,
    pub rejected_operations: u64,
    pub oracle_checks: u64,
    pub history_commits: u64,
    pub version_commits: u64,
    pub presence_events: u64,
    pub dropped_fanout: u64,
    pub duplicated_fanout: u64,
    pub delivered_messages: u64,
    pub recovered_messages: u64,
    pub recovery_truncations: u64,
    pub stale_deliveries: u64,
    pub node_crashes: u64,
    pub node_restarts: u64,
    pub node_partitions: u64,
    pub node_heals: u64,
    pub stream_resets: u64,
    pub purges: u64,
    pub live_nodes: usize,
    pub partitioned_nodes: usize,
    pub queued_fanout: usize,
    pub workload_weights: ActionWeights,
    pub workload_actions: WorkloadActionCounts,
}

/// Seed-deterministic simulator over Sockudo's durable core primitives.
pub struct DeterministicSimulator {
    config: SimulatorConfig,
    rng: StdRng,
    tick: u64,
    history_store: MemoryHistoryStore,
    version_store: MemoryVersionStore,
    presence_store: MemoryPresenceHistoryStore,
    nodes: Vec<NodeState>,
    clients: Vec<ClientState>,
    channels: Vec<String>,
    network: Vec<NetworkEvent>,
    shadow: Shadow,
    workload: WorkloadGenerator,
    stats: Stats,
    next_message_serial: u64,
    next_version_serial: u64,
}

impl DeterministicSimulator {
    pub fn new(config: SimulatorConfig) -> SimulatorResult<Self> {
        config.validate()?;
        let workload = WorkloadGenerator::new(config.seed, config.workload.clone())?;
        let channels = (0..config.channels)
            .map(|idx| format!("sim-channel-{idx}"))
            .collect::<Vec<_>>();
        let clients = (0..config.clients)
            .map(|idx| ClientState::new(idx, &channels))
            .collect();
        let nodes = (0..config.nodes).map(NodeState::new).collect();
        let history_store = MemoryHistoryStore::new(MemoryHistoryStoreConfig {
            retention_window: config.retention_window(),
            max_messages_per_channel: config.history_retention_messages,
            max_bytes_per_channel: None,
        });
        let presence_store = MemoryPresenceHistoryStore::new(MemoryPresenceHistoryStoreConfig {
            retention_window: config.retention_window(),
            max_events_per_channel: config.presence_retention_events,
            max_bytes_per_channel: None,
            metrics: None,
        });

        Ok(Self {
            rng: StdRng::seed_from_u64(config.seed),
            shadow: Shadow::new(
                &channels,
                config.history_retention_messages,
                config.presence_retention_events,
            ),
            config,
            tick: 0,
            history_store,
            version_store: MemoryVersionStore::new(),
            presence_store,
            nodes,
            clients,
            channels,
            network: Vec::new(),
            stats: Stats::default(),
            workload,
            next_message_serial: 1,
            next_version_serial: 1,
        })
    }

    pub async fn run(&mut self) -> SimulatorResult<SimulationReport> {
        for tick in 0..self.config.ticks {
            self.tick = tick;
            self.deliver_due_fanout();
            self.inject_faults().await?;
            self.drive_workload().await?;
            if self.config.oracle_every > 0 && tick % self.config.oracle_every == 0 {
                self.check_oracles().await?;
            }
        }

        self.quiesce().await?;
        self.recover_all_clients().await?;
        self.check_oracles().await?;
        Ok(self.report())
    }

    fn report(&self) -> SimulationReport {
        SimulationReport {
            seed: self.config.seed,
            ticks: self.tick.saturating_add(1),
            operations: self.stats.operations,
            rejected_operations: self.stats.rejected_operations,
            oracle_checks: self.stats.oracle_checks,
            history_commits: self.stats.history_commits,
            version_commits: self.stats.version_commits,
            presence_events: self.stats.presence_events,
            dropped_fanout: self.stats.dropped_fanout,
            duplicated_fanout: self.stats.duplicated_fanout,
            delivered_messages: self.stats.delivered_messages,
            recovered_messages: self.stats.recovered_messages,
            recovery_truncations: self.stats.recovery_truncations,
            stale_deliveries: self.stats.stale_deliveries,
            node_crashes: self.stats.node_crashes,
            node_restarts: self.stats.node_restarts,
            node_partitions: self.stats.node_partitions,
            node_heals: self.stats.node_heals,
            stream_resets: self.stats.stream_resets,
            purges: self.stats.purges,
            live_nodes: self.nodes.iter().filter(|node| node.alive).count(),
            partitioned_nodes: self.nodes.iter().filter(|node| node.partitioned).count(),
            queued_fanout: self.network.len(),
            workload_weights: self.workload.weights(),
            workload_actions: self.workload.selected().clone(),
        }
    }

    async fn drive_workload(&mut self) -> SimulatorResult<()> {
        self.stats.operations = self.stats.operations.saturating_add(1);
        let action = self.workload.next_action();
        if self.route_rejects() {
            self.stats.rejected_operations = self.stats.rejected_operations.saturating_add(1);
            return Ok(());
        }

        match action {
            WorkloadAction::PublishMessage => self.publish_message().await,
            WorkloadAction::CreateVersionedMessage => self.create_versioned_message().await,
            WorkloadAction::MutateVersionedMessage => self.mutate_versioned_message().await,
            WorkloadAction::PresenceTransition => self.record_presence_transition().await,
            WorkloadAction::RecoveryProbe => self.recovery_probe().await,
            WorkloadAction::PurgeHistory => self.purge_history_prefix().await,
            WorkloadAction::OracleCheck => self.check_oracles().await,
        }
    }

    fn route_rejects(&mut self) -> bool {
        if !self.nodes.iter().any(NodeState::can_accept_traffic) {
            return true;
        }
        let node_idx = self.rng.random_range(0..self.nodes.len());
        !self.nodes[node_idx].can_accept_traffic()
    }

    async fn inject_faults(&mut self) -> SimulatorResult<()> {
        if self.roll(self.config.fault.node_crash_probability) {
            self.crash_random_node();
        }
        if self.roll(self.config.fault.node_restart_probability) {
            self.restart_random_node();
        }
        if self.roll(self.config.fault.node_partition_probability) {
            self.partition_random_node();
        }
        if self.roll(self.config.fault.node_heal_probability) {
            self.heal_random_node();
        }
        if self.roll(self.config.fault.stream_reset_probability) {
            self.reset_random_stream().await?;
        }
        Ok(())
    }

    fn crash_random_node(&mut self) {
        let live = self
            .nodes
            .iter()
            .filter(|node| node.alive)
            .map(|node| node.id)
            .collect::<Vec<_>>();
        if live.len() <= 1 {
            return;
        }
        let victim = live[self.rng.random_range(0..live.len())];
        if let Some(node) = self.nodes.get_mut(victim) {
            node.alive = false;
            node.partitioned = false;
            self.stats.node_crashes = self.stats.node_crashes.saturating_add(1);
        }
    }

    fn restart_random_node(&mut self) {
        let down = self
            .nodes
            .iter()
            .filter(|node| !node.alive)
            .map(|node| node.id)
            .collect::<Vec<_>>();
        if down.is_empty() {
            return;
        }
        let node_idx = down[self.rng.random_range(0..down.len())];
        if let Some(node) = self.nodes.get_mut(node_idx) {
            node.alive = true;
            self.stats.node_restarts = self.stats.node_restarts.saturating_add(1);
        }
    }

    fn partition_random_node(&mut self) {
        let eligible = self
            .nodes
            .iter()
            .filter(|node| node.alive && !node.partitioned)
            .map(|node| node.id)
            .collect::<Vec<_>>();
        if eligible.len() <= 1 {
            return;
        }
        let node_idx = eligible[self.rng.random_range(0..eligible.len())];
        if let Some(node) = self.nodes.get_mut(node_idx) {
            node.partitioned = true;
            self.stats.node_partitions = self.stats.node_partitions.saturating_add(1);
        }
    }

    fn heal_random_node(&mut self) {
        let partitioned = self
            .nodes
            .iter()
            .filter(|node| node.partitioned)
            .map(|node| node.id)
            .collect::<Vec<_>>();
        if partitioned.is_empty() {
            return;
        }
        let node_idx = partitioned[self.rng.random_range(0..partitioned.len())];
        if let Some(node) = self.nodes.get_mut(node_idx) {
            node.partitioned = false;
            self.stats.node_heals = self.stats.node_heals.saturating_add(1);
        }
    }

    async fn reset_random_stream(&mut self) -> SimulatorResult<()> {
        let channel = self.random_channel();
        let history = self
            .history_store
            .reset_stream(
                APP_ID,
                &channel,
                "simulated operator reset",
                Some("sockudo-sim"),
            )
            .await?;
        self.shadow
            .channel_mut(&channel)
            .reset_history(history.new_stream_id.clone());

        let presence = self
            .presence_store
            .reset_stream(
                APP_ID,
                &channel,
                "simulated operator reset",
                Some("sockudo-sim"),
            )
            .await?;
        self.shadow
            .channel_mut(&channel)
            .reset_presence(presence.new_stream_id.clone());

        for client in &mut self.clients {
            client.reset_channel(&channel, history.inspection.stream_id.clone());
        }
        self.network.retain(|event| event.channel != channel);
        self.stats.stream_resets = self.stats.stream_resets.saturating_add(1);
        Ok(())
    }

    async fn publish_message(&mut self) -> SimulatorResult<()> {
        let channel = self.random_channel();
        let payload = Bytes::from(format!(
            "payload:{}:{}",
            self.tick, self.stats.history_commits
        ));
        let message = self
            .append_history(&channel, "client-event", "publish", payload)
            .await?;
        self.schedule_fanout(&channel, &message);
        Ok(())
    }

    async fn create_versioned_message(&mut self) -> SimulatorResult<()> {
        let channel = self.random_channel();
        let history = self
            .append_history(
                &channel,
                "sockudo:message.create",
                MessageAction::Create.as_str(),
                Bytes::from(format!("version-create:{}", self.tick)),
            )
            .await?;
        let delivery = self
            .version_store
            .reserve_delivery_position(APP_ID, &channel)
            .await?;
        let message_serial = MessageSerial::new(format!("msg-{:020}", self.next_message_serial))?;
        self.next_message_serial = self.next_message_serial.saturating_add(1);
        let version = self.next_version_metadata()?;
        let message = VersionedMessage::new_create(
            message_serial,
            version,
            history.serial,
            delivery.delivery_serial,
            Some("sockudo:message.create".to_string()),
            Some(MessageData::String(format!(
                "created at tick {}",
                self.tick
            ))),
            Some(MessageExtras::default()),
        );
        let record = StoredVersionRecord {
            app_id: APP_ID.to_string(),
            channel: channel.clone(),
            original_client_id: Some(format!(
                "client-{}",
                self.rng.random_range(0..self.config.clients)
            )),
            message: message.clone(),
        };
        self.version_store.append_version(record.clone()).await?;
        self.shadow.channel_mut(&channel).append_version(record);
        self.stats.version_commits = self.stats.version_commits.saturating_add(1);
        self.schedule_fanout(&channel, &history);
        Ok(())
    }

    async fn mutate_versioned_message(&mut self) -> SimulatorResult<()> {
        let channel = self.random_channel();
        let Some(current) = self
            .shadow
            .channel(&channel)
            .random_live_version(&mut self.rng)
        else {
            return self.create_versioned_message().await;
        };

        let action_roll = self.rng.random_range(0..100);
        let (action, payload) = if action_roll < 40 {
            (MessageAction::Update, "message.update")
        } else if action_roll < 75 {
            (MessageAction::Append, "message.append")
        } else {
            (MessageAction::Delete, "message.delete")
        };
        let history = self
            .append_history(
                &channel,
                action.as_str(),
                action.as_str(),
                Bytes::from(format!("{payload}:{}", self.tick)),
            )
            .await?;
        let delivery = self
            .version_store
            .reserve_delivery_position_after(
                APP_ID,
                &channel,
                current.message.replay_position.delivery_serial,
            )
            .await?;
        let version = self.next_version_metadata()?;
        let next = match action {
            MessageAction::Update => current.message.apply_mutation(
                action,
                version,
                delivery.delivery_serial,
                MessageFieldDelta {
                    data: FieldPatch::Replace(MessageData::String(format!(
                        "updated at tick {}",
                        self.tick
                    ))),
                    ..Default::default()
                },
            )?,
            MessageAction::Delete => current.message.apply_mutation(
                action,
                version,
                delivery.delivery_serial,
                MessageFieldDelta {
                    data: FieldPatch::Clear,
                    extras: FieldPatch::Clear,
                    ..Default::default()
                },
            )?,
            MessageAction::Append => current.message.apply_append(
                version,
                delivery.delivery_serial,
                MessageAppend {
                    data_fragment: format!("|{}", self.tick),
                    extras: None,
                },
            )?,
            MessageAction::Create | MessageAction::Summary => unreachable!("unsupported mutation"),
        };
        let record = StoredVersionRecord {
            app_id: APP_ID.to_string(),
            channel: channel.clone(),
            original_client_id: current.original_client_id.clone(),
            message: next,
        };
        self.version_store.append_version(record.clone()).await?;
        self.shadow.channel_mut(&channel).append_version(record);
        self.stats.version_commits = self.stats.version_commits.saturating_add(1);
        self.schedule_fanout(&channel, &history);
        Ok(())
    }

    async fn record_presence_transition(&mut self) -> SimulatorResult<()> {
        let channel = self.random_channel();
        let user_id = format!("user-{}", self.rng.random_range(0..self.config.users));
        let event = if self.rng.random::<bool>() {
            PresenceHistoryEventKind::MemberAdded
        } else {
            PresenceHistoryEventKind::MemberRemoved
        };
        let cause = match event {
            PresenceHistoryEventKind::MemberAdded | PresenceHistoryEventKind::MemberUpdated => {
                PresenceHistoryEventCause::Join
            }
            PresenceHistoryEventKind::MemberRemoved => PresenceHistoryEventCause::Disconnect,
        };
        let should_record = self
            .shadow
            .channel(&channel)
            .presence_would_record(event, &user_id);
        let record = PresenceHistoryTransitionRecord {
            app_id: APP_ID.to_string(),
            channel: channel.clone(),
            event_kind: event,
            cause,
            user_id: user_id.clone(),
            connection_id: Some(format!("socket-{user_id}")),
            user_info: None,
            dead_node_id: None,
            dedupe_key: format!("presence:{}:{}:{user_id}", self.tick, self.stats.operations),
            published_at_ms: self.timestamp_ms(),
            retention: self.presence_retention_policy(),
        };
        self.presence_store.record_transition(record).await?;
        if should_record {
            let newest = self
                .read_presence_page(&channel, PresenceHistoryDirection::NewestFirst)
                .await?;
            let Some(item) = newest.first() else {
                return Err(self.fail(format!(
                    "presence transition for {channel}/{user_id} was predicted to record but store returned no newest item"
                )));
            };
            self.shadow
                .channel_mut(&channel)
                .append_presence(item.clone());
            self.stats.presence_events = self.stats.presence_events.saturating_add(1);
        }
        Ok(())
    }

    async fn purge_history_prefix(&mut self) -> SimulatorResult<()> {
        let channel = self.random_channel();
        let shadow = self.shadow.channel(&channel);
        if shadow.history.len() < 4 {
            return Ok(());
        }
        let idx = self.rng.random_range(1..shadow.history.len());
        let before_serial = shadow.history[idx].serial;
        self.history_store
            .purge_stream(
                APP_ID,
                &channel,
                HistoryPurgeRequest {
                    mode: HistoryPurgeMode::BeforeSerial,
                    before_serial: Some(before_serial),
                    before_time_ms: None,
                    reason: "simulated retention purge".to_string(),
                    requested_by: Some("sockudo-sim".to_string()),
                },
            )
            .await?;
        self.shadow
            .channel_mut(&channel)
            .purge_history_before(before_serial);
        for client in &mut self.clients {
            client.rewind_below(&channel, before_serial.saturating_sub(1));
        }
        self.stats.purges = self.stats.purges.saturating_add(1);
        Ok(())
    }

    async fn append_history(
        &mut self,
        channel: &str,
        event_name: &str,
        operation_kind: &str,
        payload: Bytes,
    ) -> SimulatorResult<ShadowHistoryMessage> {
        let reservation = self
            .history_store
            .reserve_publish_position(APP_ID, channel)
            .await?;
        let record = HistoryAppendRecord {
            app_id: APP_ID.to_string(),
            channel: channel.to_string(),
            stream_id: reservation.stream_id.clone(),
            serial: reservation.serial,
            published_at_ms: self.timestamp_ms(),
            message_id: Some(format!("hist-{:020}", self.stats.history_commits + 1)),
            event_name: Some(event_name.to_string()),
            operation_kind: operation_kind.to_string(),
            payload_bytes: payload.clone(),
            retention: self.history_retention_policy(),
        };
        self.history_store.append(record.clone()).await?;
        let message = ShadowHistoryMessage {
            stream_id: record.stream_id,
            serial: record.serial,
            published_at_ms: record.published_at_ms,
            message_id: record.message_id,
            event_name: record.event_name,
            operation_kind: record.operation_kind,
            payload,
        };
        self.shadow
            .channel_mut(channel)
            .append_history(message.clone());
        self.stats.history_commits = self.stats.history_commits.saturating_add(1);
        Ok(message)
    }

    fn schedule_fanout(&mut self, channel: &str, message: &ShadowHistoryMessage) {
        for client_idx in 0..self.clients.len() {
            if self.roll(self.config.fault.fanout_drop_probability) {
                self.stats.dropped_fanout = self.stats.dropped_fanout.saturating_add(1);
                continue;
            }
            let event = NetworkEvent {
                deliver_at: self.tick.saturating_add(self.random_delay()),
                client_idx,
                channel: channel.to_string(),
                stream_id: message.stream_id.clone(),
                serial: message.serial,
            };
            self.network.push(event.clone());
            if self.roll(self.config.fault.fanout_duplicate_probability) {
                let mut duplicate = event;
                duplicate.deliver_at = duplicate.deliver_at.saturating_add(self.random_delay());
                self.network.push(duplicate);
                self.stats.duplicated_fanout = self.stats.duplicated_fanout.saturating_add(1);
            }
        }
    }

    fn deliver_due_fanout(&mut self) {
        let mut pending = Vec::with_capacity(self.network.len());
        let due = std::mem::take(&mut self.network);
        for event in due {
            if event.deliver_at <= self.tick {
                let accepted = self.clients[event.client_idx].deliver(&event);
                if accepted {
                    self.stats.delivered_messages = self.stats.delivered_messages.saturating_add(1);
                } else {
                    self.stats.stale_deliveries = self.stats.stale_deliveries.saturating_add(1);
                }
            } else {
                pending.push(event);
            }
        }
        self.network = pending;
    }

    async fn quiesce(&mut self) -> SimulatorResult<()> {
        let settle = self
            .config
            .fault
            .max_fanout_delay_ticks
            .saturating_add(5)
            .max(16);
        for _ in 0..settle {
            self.tick = self.tick.saturating_add(1);
            self.deliver_due_fanout();
        }
        Ok(())
    }

    async fn recovery_probe(&mut self) -> SimulatorResult<()> {
        let client_idx = self.rng.random_range(0..self.clients.len());
        let channel = self.random_channel();
        self.recover_client_channel(client_idx, &channel).await
    }

    async fn recover_all_clients(&mut self) -> SimulatorResult<()> {
        let channels = self.channels.clone();
        for client_idx in 0..self.clients.len() {
            for channel in &channels {
                self.recover_client_channel(client_idx, channel).await?;
            }
        }
        Ok(())
    }

    async fn recover_client_channel(
        &mut self,
        client_idx: usize,
        channel: &str,
    ) -> SimulatorResult<()> {
        let shadow = self.shadow.channel(channel);
        let Some(current_stream_id) = shadow.history_stream_id.clone() else {
            return Ok(());
        };
        let (client_stream_id, last_contiguous) =
            self.clients[client_idx].recovery_position(channel);
        if client_stream_id.as_deref() != Some(current_stream_id.as_str()) {
            self.clients[client_idx].reset_channel(channel, Some(current_stream_id));
            return Ok(());
        }

        let oldest = shadow.history.front().map(|message| message.serial);
        if let Some(oldest_serial) = oldest
            && last_contiguous.saturating_add(1) < oldest_serial
        {
            self.stats.recovery_truncations = self.stats.recovery_truncations.saturating_add(1);
            self.clients[client_idx].rewind_below(channel, oldest_serial.saturating_sub(1));
            return Ok(());
        }

        let recovered = self
            .read_history_bounded(channel, last_contiguous.saturating_add(1))
            .await?;
        let expected = shadow
            .history
            .iter()
            .filter(|message| message.serial > last_contiguous)
            .map(|message| message.serial)
            .collect::<Vec<_>>();
        let actual = recovered.iter().map(|item| item.serial).collect::<Vec<_>>();
        if actual != expected {
            return Err(self.fail(format!(
                "client recovery mismatch on {channel}: expected serials {expected:?}, got {actual:?}"
            )));
        }
        for item in recovered {
            let event = NetworkEvent {
                deliver_at: self.tick,
                client_idx,
                channel: channel.to_string(),
                stream_id: item.stream_id,
                serial: item.serial,
            };
            if self.clients[client_idx].deliver(&event) {
                self.stats.recovered_messages = self.stats.recovered_messages.saturating_add(1);
            }
        }
        Ok(())
    }

    async fn check_oracles(&mut self) -> SimulatorResult<()> {
        self.stats.oracle_checks = self.stats.oracle_checks.saturating_add(1);
        let channels = self.channels.clone();
        for channel in &channels {
            self.check_history_oracle(channel).await?;
            self.check_version_oracle(channel).await?;
            self.check_presence_oracle(channel).await?;
            self.check_client_recovery_oracle(channel)?;
        }
        Ok(())
    }

    async fn check_history_oracle(&self, channel: &str) -> SimulatorResult<()> {
        let shadow = self.shadow.channel(channel);
        let oldest = self
            .read_history_page(channel, HistoryDirection::OldestFirst)
            .await?;
        let newest = self
            .read_history_page(channel, HistoryDirection::NewestFirst)
            .await?;
        let expected = shadow.history.iter().collect::<Vec<_>>();

        if oldest.len() != expected.len() {
            return Err(self.fail(format!(
                "history length mismatch on {channel}: expected {}, got {}",
                expected.len(),
                oldest.len()
            )));
        }
        for (actual, expected) in oldest.iter().zip(expected.iter()) {
            self.assert_history_item(channel, actual, expected)?;
        }

        let newest_serials = newest.iter().map(|item| item.serial).collect::<Vec<_>>();
        let expected_newest = expected
            .iter()
            .rev()
            .map(|message| message.serial)
            .collect::<Vec<_>>();
        if newest_serials != expected_newest {
            return Err(self.fail(format!(
                "newest-first history mismatch on {channel}: expected {expected_newest:?}, got {newest_serials:?}"
            )));
        }

        for pair in oldest.windows(2) {
            if pair[0].serial.saturating_add(1) != pair[1].serial {
                return Err(self.fail(format!(
                    "history serial gap on {channel}: {} then {}",
                    pair[0].serial, pair[1].serial
                )));
            }
        }

        let head = self.history_store.channel_head(APP_ID, channel).await?;
        if head.retained_messages != expected.len() as u64 {
            return Err(self.fail(format!(
                "history retained_messages mismatch on {channel}: expected {}, got {}",
                expected.len(),
                head.retained_messages
            )));
        }
        let expected_bytes = expected
            .iter()
            .map(|message| message.payload.len() as u64)
            .sum::<u64>();
        if head.retained_bytes != expected_bytes {
            return Err(self.fail(format!(
                "history retained_bytes mismatch on {channel}: expected {expected_bytes}, got {}",
                head.retained_bytes
            )));
        }
        if head.stream_id != shadow.history_stream_id {
            return Err(self.fail(format!(
                "history stream mismatch on {channel}: expected {:?}, got {:?}",
                shadow.history_stream_id, head.stream_id
            )));
        }
        if head.oldest_serial != expected.first().map(|message| message.serial)
            || head.newest_serial != expected.last().map(|message| message.serial)
        {
            return Err(self.fail(format!(
                "history head serial bounds mismatch on {channel}: head={head:?}"
            )));
        }
        if shadow.history_stream_id.is_some() {
            let inspection = self
                .history_store
                .stream_inspection(APP_ID, channel)
                .await?;
            let expected_next = expected
                .last()
                .map_or(1, |message| message.serial.saturating_add(1));
            if inspection.next_serial != Some(expected_next) {
                return Err(self.fail(format!(
                    "history next_serial mismatch on {channel}: expected {expected_next}, got {:?}",
                    inspection.next_serial
                )));
            }
            if !inspection.state.recovery_allowed {
                return Err(self.fail(format!(
                    "history recovery unexpectedly disallowed on {channel}: {:?}",
                    inspection.state
                )));
            }
        }
        Ok(())
    }

    fn assert_history_item(
        &self,
        channel: &str,
        actual: &HistoryItem,
        expected: &ShadowHistoryMessage,
    ) -> SimulatorResult<()> {
        if actual.stream_id != expected.stream_id
            || actual.serial != expected.serial
            || actual.published_at_ms != expected.published_at_ms
            || actual.message_id != expected.message_id
            || actual.event_name != expected.event_name
            || actual.operation_kind != expected.operation_kind
            || actual.payload_bytes != expected.payload
        {
            return Err(self.fail(format!(
                "history item mismatch on {channel}: expected {expected:?}, got {actual:?}"
            )));
        }
        Ok(())
    }

    async fn check_version_oracle(&self, channel: &str) -> SimulatorResult<()> {
        let shadow = self.shadow.channel(channel);
        let replay = self
            .version_store
            .replay_after(VersionReplayRequest {
                app_id: APP_ID.to_string(),
                channel: channel.to_string(),
                after_delivery_serial: 0,
                limit: usize::MAX / 2,
            })
            .await?;
        let expected_replay = shadow.version_replay.values().collect::<Vec<_>>();
        if replay.len() != expected_replay.len() {
            return Err(self.fail(format!(
                "version replay length mismatch on {channel}: expected {}, got {}",
                expected_replay.len(),
                replay.len()
            )));
        }
        for (actual, expected) in replay.iter().zip(expected_replay.iter()) {
            self.assert_stored_version(channel, actual, expected)?;
        }
        for pair in replay.windows(2) {
            if pair[0].delivery_serial().saturating_add(1) != pair[1].delivery_serial() {
                return Err(self.fail(format!(
                    "version replay gap on {channel}: {} then {}",
                    pair[0].delivery_serial(),
                    pair[1].delivery_serial()
                )));
            }
        }

        let state = self.version_store.stream_state(APP_ID, channel).await?;
        if shadow.version_replay.is_empty() {
            if state.newest_available_delivery_serial.is_some() {
                return Err(self.fail(format!(
                    "empty version shadow but store has stream state on {channel}: {state:?}"
                )));
            }
        } else {
            let newest = shadow.version_replay.keys().next_back().copied();
            if state.newest_available_delivery_serial != newest
                || state.next_delivery_serial != newest.map(|serial| serial.saturating_add(1))
            {
                return Err(self.fail(format!(
                    "version stream state mismatch on {channel}: expected newest {newest:?}, got {state:?}"
                )));
            }
        }

        for (message_serial, chain) in &shadow.version_messages {
            let latest = self
                .version_store
                .get_latest(
                    APP_ID,
                    channel,
                    &MessageSerial::new(message_serial.clone())?,
                )
                .await?;
            let Some(latest) = latest else {
                return Err(self.fail(format!(
                    "missing latest version for {channel}/{message_serial}"
                )));
            };
            self.assert_stored_version(
                channel,
                &latest,
                chain.latest().ok_or_else(|| {
                    self.fail(format!("empty shadow chain for {channel}/{message_serial}"))
                })?,
            )?;

            let oldest_page = self
                .read_versions(channel, message_serial, VersionStoreDirection::OldestFirst)
                .await?;
            if oldest_page.len() != chain.versions.len() {
                return Err(self.fail(format!(
                    "version chain length mismatch on {channel}/{message_serial}: expected {}, got {}",
                    chain.versions.len(),
                    oldest_page.len()
                )));
            }
            for (actual, expected) in oldest_page.iter().zip(chain.versions.iter()) {
                self.assert_stored_version(channel, actual, expected)?;
            }
            let newest_page = self
                .read_versions(channel, message_serial, VersionStoreDirection::NewestFirst)
                .await?;
            let newest_serials = newest_page
                .iter()
                .map(|record| record.version_serial().as_str().to_string())
                .collect::<Vec<_>>();
            let expected_newest = chain
                .versions
                .iter()
                .rev()
                .map(|record| record.version_serial().as_str().to_string())
                .collect::<Vec<_>>();
            if newest_serials != expected_newest {
                return Err(self.fail(format!(
                    "newest-first version page mismatch on {channel}/{message_serial}: expected {expected_newest:?}, got {newest_serials:?}"
                )));
            }
        }

        let latest_by_history = self
            .version_store
            .latest_by_history(APP_ID, channel)
            .await?;
        let expected_latest = shadow.latest_versions_by_history();
        if latest_by_history.len() != expected_latest.len() {
            return Err(self.fail(format!(
                "latest_by_history length mismatch on {channel}: expected {}, got {}",
                expected_latest.len(),
                latest_by_history.len()
            )));
        }
        for (actual, expected) in latest_by_history.iter().zip(expected_latest.iter()) {
            self.assert_stored_version(channel, actual, expected)?;
        }
        Ok(())
    }

    fn assert_stored_version(
        &self,
        channel: &str,
        actual: &StoredVersionRecord,
        expected: &StoredVersionRecord,
    ) -> SimulatorResult<()> {
        let actual_msg = &actual.message;
        let expected_msg = &expected.message;
        if actual.app_id != expected.app_id
            || actual.channel != expected.channel
            || actual.original_client_id != expected.original_client_id
            || actual_msg.action != expected_msg.action
            || actual_msg.identity.message_serial != expected_msg.identity.message_serial
            || actual_msg.identity.history_serial != expected_msg.identity.history_serial
            || actual_msg.replay_position.delivery_serial
                != expected_msg.replay_position.delivery_serial
            || actual_msg.version.serial != expected_msg.version.serial
            || actual_msg.version.client_id != expected_msg.version.client_id
            || actual_msg.version.timestamp_ms != expected_msg.version.timestamp_ms
            || actual_msg.version.description != expected_msg.version.description
            || actual_msg.name != expected_msg.name
            || actual_msg.data != expected_msg.data
            || actual_msg.extras != expected_msg.extras
        {
            return Err(self.fail(format!(
                "stored version mismatch on {channel}: expected {expected:?}, got {actual:?}"
            )));
        }
        Ok(())
    }

    async fn check_presence_oracle(&self, channel: &str) -> SimulatorResult<()> {
        let shadow = self.shadow.channel(channel);
        let oldest = self
            .read_presence_page(channel, PresenceHistoryDirection::OldestFirst)
            .await?;
        let newest = self
            .read_presence_page(channel, PresenceHistoryDirection::NewestFirst)
            .await?;
        let expected = shadow.presence_events.iter().collect::<Vec<_>>();
        if oldest.len() != expected.len() {
            return Err(self.fail(format!(
                "presence history length mismatch on {channel}: expected {}, got {}",
                expected.len(),
                oldest.len()
            )));
        }
        for (actual, expected) in oldest.iter().zip(expected.iter()) {
            self.assert_presence_item(channel, actual, expected)?;
        }
        let newest_serials = newest.iter().map(|item| item.serial).collect::<Vec<_>>();
        let expected_newest = expected
            .iter()
            .rev()
            .map(|event| event.serial)
            .collect::<Vec<_>>();
        if newest_serials != expected_newest {
            return Err(self.fail(format!(
                "newest-first presence history mismatch on {channel}: expected {expected_newest:?}, got {newest_serials:?}"
            )));
        }
        for pair in oldest.windows(2) {
            if pair[0].serial.saturating_add(1) != pair[1].serial {
                return Err(self.fail(format!(
                    "presence serial gap on {channel}: {} then {}",
                    pair[0].serial, pair[1].serial
                )));
            }
        }

        let snapshot = self
            .presence_store
            .snapshot_at(PresenceSnapshotRequest {
                app_id: APP_ID.to_string(),
                channel: channel.to_string(),
                at_time_ms: None,
                at_serial: None,
            })
            .await?;
        let actual_members = snapshot
            .members
            .iter()
            .map(|member| member.user_id.clone())
            .collect::<BTreeSet<_>>();
        let expected_members = shadow
            .presence_members
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        if actual_members != expected_members {
            return Err(self.fail(format!(
                "presence snapshot mismatch on {channel}: expected {expected_members:?}, got {actual_members:?}"
            )));
        }
        if snapshot.events_replayed != expected.len() as u64 {
            return Err(self.fail(format!(
                "presence snapshot replay count mismatch on {channel}: expected {}, got {}",
                expected.len(),
                snapshot.events_replayed
            )));
        }

        if shadow.presence_stream_id.is_some() {
            let inspection = self
                .presence_store
                .stream_inspection(APP_ID, channel)
                .await?;
            let expected_next = expected
                .last()
                .map_or(1, |event| event.serial.saturating_add(1));
            if inspection.next_serial != Some(expected_next) {
                return Err(self.fail(format!(
                    "presence next_serial mismatch on {channel}: expected {expected_next}, got {:?}",
                    inspection.next_serial
                )));
            }
            if !inspection.state.continuity_proven {
                return Err(self.fail(format!(
                    "presence continuity unexpectedly unproven on {channel}: {:?}",
                    inspection.state
                )));
            }
        }
        Ok(())
    }

    fn assert_presence_item(
        &self,
        channel: &str,
        actual: &PresenceHistoryItem,
        expected: &ShadowPresenceEvent,
    ) -> SimulatorResult<()> {
        if actual.stream_id != expected.stream_id
            || actual.serial != expected.serial
            || actual.published_at_ms != expected.published_at_ms
            || actual.event != expected.event
            || actual.cause != expected.cause
            || actual.user_id != expected.user_id
            || actual.connection_id != expected.connection_id
            || actual.dead_node_id != expected.dead_node_id
        {
            return Err(self.fail(format!(
                "presence item mismatch on {channel}: expected {expected:?}, got {actual:?}"
            )));
        }
        Ok(())
    }

    fn check_client_recovery_oracle(&self, channel: &str) -> SimulatorResult<()> {
        let shadow = self.shadow.channel(channel);
        let Some(stream_id) = shadow.history_stream_id.as_deref() else {
            return Ok(());
        };
        let newest = shadow.history.back().map_or(0, |message| message.serial);
        let oldest = shadow
            .history
            .front()
            .map(|message| message.serial)
            .unwrap_or(1);
        for client in &self.clients {
            let (client_stream, last) = client.recovery_position(channel);
            if client_stream.as_deref() != Some(stream_id) {
                continue;
            }
            if last > newest {
                return Err(self.fail(format!(
                    "client {} is ahead of history on {channel}: last={last}, newest={newest}",
                    client.id
                )));
            }
            if last.saturating_add(1) < oldest {
                continue;
            }
            let missing = shadow
                .history
                .iter()
                .filter(|message| message.serial <= last)
                .any(|message| !client.has_delivery(channel, message.serial));
            if missing {
                return Err(self.fail(format!(
                    "client {} has contiguous recovery position {last} on {channel} but is missing a delivered serial",
                    client.id
                )));
            }
        }
        Ok(())
    }

    async fn read_history_page(
        &self,
        channel: &str,
        direction: HistoryDirection,
    ) -> SimulatorResult<Vec<HistoryItem>> {
        let mut items = Vec::new();
        let mut cursor: Option<HistoryCursor> = None;
        loop {
            let page = self
                .history_store
                .read_page(HistoryReadRequest {
                    app_id: APP_ID.to_string(),
                    channel: channel.to_string(),
                    direction,
                    limit: self.config.page_limit,
                    cursor: cursor.clone(),
                    bounds: HistoryQueryBounds::default(),
                })
                .await?;
            items.extend(page.items);
            if !page.has_more {
                break;
            }
            let Some(next) = page.next_cursor else {
                return Err(self.fail(format!(
                    "history page on {channel} had has_more=true without a cursor"
                )));
            };
            let encoded = next.encode()?;
            let decoded = HistoryCursor::decode(&encoded)?;
            if decoded != next {
                return Err(self.fail(format!("history cursor round-trip mismatch on {channel}")));
            }
            cursor = Some(next);
        }
        Ok(items)
    }

    async fn read_history_bounded(
        &self,
        channel: &str,
        start_serial: u64,
    ) -> SimulatorResult<Vec<HistoryItem>> {
        let mut items = Vec::new();
        let mut cursor: Option<HistoryCursor> = None;
        let bounds = HistoryQueryBounds {
            start_serial: Some(start_serial),
            ..Default::default()
        };
        loop {
            let page = self
                .history_store
                .read_page(HistoryReadRequest {
                    app_id: APP_ID.to_string(),
                    channel: channel.to_string(),
                    direction: HistoryDirection::OldestFirst,
                    limit: self.config.page_limit,
                    cursor: cursor.clone(),
                    bounds: bounds.clone(),
                })
                .await?;
            if page.truncated_by_retention {
                return Ok(Vec::new());
            }
            items.extend(page.items);
            if !page.has_more {
                break;
            }
            cursor = page.next_cursor;
        }
        Ok(items)
    }

    async fn read_versions(
        &self,
        channel: &str,
        message_serial: &str,
        direction: VersionStoreDirection,
    ) -> SimulatorResult<Vec<StoredVersionRecord>> {
        let mut items = Vec::new();
        let mut cursor: Option<VersionStoreCursor> = None;
        loop {
            let page = self
                .version_store
                .get_versions(VersionStoreReadRequest {
                    app_id: APP_ID.to_string(),
                    channel: channel.to_string(),
                    message_serial: MessageSerial::new(message_serial.to_string())?,
                    direction,
                    limit: self.config.page_limit,
                    cursor: cursor.clone(),
                })
                .await?;
            items.extend(page.items);
            if !page.has_more {
                break;
            }
            let Some(next) = page.next_cursor else {
                return Err(self.fail(format!(
                    "version page on {channel}/{message_serial} had has_more=true without a cursor"
                )));
            };
            cursor = Some(next);
        }
        Ok(items)
    }

    async fn read_presence_page(
        &self,
        channel: &str,
        direction: PresenceHistoryDirection,
    ) -> SimulatorResult<Vec<PresenceHistoryItem>> {
        let mut items = Vec::new();
        let mut cursor: Option<PresenceHistoryCursor> = None;
        loop {
            let page = self
                .presence_store
                .read_page(PresenceHistoryReadRequest {
                    app_id: APP_ID.to_string(),
                    channel: channel.to_string(),
                    direction,
                    limit: self.config.page_limit,
                    cursor: cursor.clone(),
                    bounds: PresenceHistoryQueryBounds::default(),
                })
                .await?;
            items.extend(page.items);
            if !page.has_more {
                break;
            }
            let Some(next) = page.next_cursor else {
                return Err(self.fail(format!(
                    "presence page on {channel} had has_more=true without a cursor"
                )));
            };
            let encoded = next.encode()?;
            let decoded = PresenceHistoryCursor::decode(&encoded)?;
            if decoded != next {
                return Err(self.fail(format!("presence cursor round-trip mismatch on {channel}")));
            }
            cursor = Some(next);
        }
        Ok(items)
    }

    fn history_retention_policy(&self) -> HistoryRetentionPolicy {
        HistoryRetentionPolicy {
            retention_window_seconds: self.config.retention_window().as_secs(),
            max_messages_per_channel: self.config.history_retention_messages,
            max_bytes_per_channel: None,
        }
    }

    fn presence_retention_policy(&self) -> PresenceHistoryRetentionPolicy {
        PresenceHistoryRetentionPolicy {
            retention_window_seconds: self.config.retention_window().as_secs(),
            max_events_per_channel: self.config.presence_retention_events,
            max_bytes_per_channel: None,
        }
    }

    fn next_version_metadata(&mut self) -> SimulatorResult<VersionMetadata> {
        let serial = VersionSerial::new(format!("ver-{:020}", self.next_version_serial))?;
        self.next_version_serial = self.next_version_serial.saturating_add(1);
        Ok(VersionMetadata {
            serial,
            client_id: Some(format!(
                "client-{}",
                self.rng.random_range(0..self.config.clients)
            )),
            timestamp_ms: self.timestamp_ms(),
            description: None,
            metadata: None,
        })
    }

    fn random_channel(&mut self) -> String {
        self.channels[self.rng.random_range(0..self.channels.len())].clone()
    }

    fn random_delay(&mut self) -> u64 {
        if self.config.fault.max_fanout_delay_ticks == 0 {
            0
        } else {
            self.rng
                .random_range(0..=self.config.fault.max_fanout_delay_ticks)
        }
    }

    fn roll(&mut self, probability: f64) -> bool {
        probability > 0.0 && self.rng.random::<f64>() < probability
    }

    fn timestamp_ms(&self) -> i64 {
        BASE_TIME_MS.saturating_add(self.tick as i64)
    }

    fn fail(&self, message: String) -> SimulatorError {
        SimulatorError::Invariant {
            seed: self.config.seed,
            tick: self.tick,
            message,
        }
    }
}

#[derive(Debug, Clone)]
struct NodeState {
    id: usize,
    alive: bool,
    partitioned: bool,
}

impl NodeState {
    fn new(id: usize) -> Self {
        Self {
            id,
            alive: true,
            partitioned: false,
        }
    }

    fn can_accept_traffic(&self) -> bool {
        self.alive && !self.partitioned
    }
}

#[derive(Debug, Clone)]
struct NetworkEvent {
    deliver_at: u64,
    client_idx: usize,
    channel: String,
    stream_id: String,
    serial: u64,
}

#[derive(Debug, Clone)]
struct ClientState {
    id: usize,
    channels: BTreeMap<String, ClientChannelState>,
}

impl ClientState {
    fn new(id: usize, channels: &[String]) -> Self {
        Self {
            id,
            channels: channels
                .iter()
                .map(|channel| (channel.clone(), ClientChannelState::default()))
                .collect(),
        }
    }

    fn deliver(&mut self, event: &NetworkEvent) -> bool {
        let state = self.channels.entry(event.channel.clone()).or_default();
        if state.stream_id.is_none() {
            state.stream_id = Some(event.stream_id.clone());
        }
        if state.stream_id.as_deref() != Some(event.stream_id.as_str()) {
            return false;
        }
        state.delivered.insert(event.serial);
        while state
            .delivered
            .contains(&state.last_contiguous.saturating_add(1))
        {
            state.last_contiguous = state.last_contiguous.saturating_add(1);
        }
        true
    }

    fn reset_channel(&mut self, channel: &str, stream_id: Option<String>) {
        self.channels.insert(
            channel.to_string(),
            ClientChannelState {
                stream_id,
                delivered: BTreeSet::new(),
                last_contiguous: 0,
            },
        );
    }

    fn rewind_below(&mut self, channel: &str, serial: u64) {
        let state = self.channels.entry(channel.to_string()).or_default();
        state.delivered.retain(|delivered| *delivered >= serial);
        state.last_contiguous = state.last_contiguous.max(serial);
    }

    fn recovery_position(&self, channel: &str) -> (Option<String>, u64) {
        self.channels
            .get(channel)
            .map(|state| (state.stream_id.clone(), state.last_contiguous))
            .unwrap_or((None, 0))
    }

    fn has_delivery(&self, channel: &str, serial: u64) -> bool {
        self.channels
            .get(channel)
            .is_some_and(|state| state.delivered.contains(&serial))
    }
}

#[derive(Debug, Clone, Default)]
struct ClientChannelState {
    stream_id: Option<String>,
    delivered: BTreeSet<u64>,
    last_contiguous: u64,
}

#[derive(Debug, Clone)]
struct Shadow {
    channels: BTreeMap<String, ShadowChannel>,
}

impl Shadow {
    fn new(
        channels: &[String],
        history_limit: Option<usize>,
        presence_limit: Option<usize>,
    ) -> Self {
        Self {
            channels: channels
                .iter()
                .map(|channel| {
                    (
                        channel.clone(),
                        ShadowChannel::new(history_limit, presence_limit),
                    )
                })
                .collect(),
        }
    }

    fn channel(&self, channel: &str) -> &ShadowChannel {
        self.channels
            .get(channel)
            .expect("simulator channel must exist in shadow")
    }

    fn channel_mut(&mut self, channel: &str) -> &mut ShadowChannel {
        self.channels
            .get_mut(channel)
            .expect("simulator channel must exist in shadow")
    }
}

#[derive(Debug, Clone)]
struct ShadowChannel {
    history_stream_id: Option<String>,
    history: VecDeque<ShadowHistoryMessage>,
    history_limit: Option<usize>,
    version_messages: BTreeMap<String, ShadowVersionChain>,
    version_replay: BTreeMap<u64, StoredVersionRecord>,
    presence_stream_id: Option<String>,
    presence_events: VecDeque<ShadowPresenceEvent>,
    presence_latest_event_by_user: BTreeMap<String, PresenceHistoryEventKind>,
    presence_members: BTreeMap<String, PresenceHistoryEventKind>,
    presence_limit: Option<usize>,
}

impl ShadowChannel {
    fn new(history_limit: Option<usize>, presence_limit: Option<usize>) -> Self {
        Self {
            history_stream_id: None,
            history: VecDeque::new(),
            history_limit,
            version_messages: BTreeMap::new(),
            version_replay: BTreeMap::new(),
            presence_stream_id: None,
            presence_events: VecDeque::new(),
            presence_latest_event_by_user: BTreeMap::new(),
            presence_members: BTreeMap::new(),
            presence_limit,
        }
    }

    fn append_history(&mut self, message: ShadowHistoryMessage) {
        self.history_stream_id = Some(message.stream_id.clone());
        self.history.push_back(message);
        if let Some(limit) = self.history_limit {
            while self.history.len() > limit {
                self.history.pop_front();
            }
        }
    }

    fn reset_history(&mut self, stream_id: String) {
        self.history_stream_id = Some(stream_id);
        self.history.clear();
    }

    fn purge_history_before(&mut self, serial: u64) {
        self.history.retain(|message| message.serial >= serial);
    }

    fn append_version(&mut self, record: StoredVersionRecord) {
        let message_serial = record.message_serial().as_str().to_string();
        self.version_replay
            .insert(record.delivery_serial(), record.clone());
        self.version_messages
            .entry(message_serial)
            .or_default()
            .versions
            .push(record);
    }

    fn random_live_version(&self, rng: &mut StdRng) -> Option<StoredVersionRecord> {
        let candidates = self
            .version_messages
            .values()
            .filter_map(ShadowVersionChain::latest)
            .filter(|record| record.message.action != MessageAction::Delete)
            .cloned()
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            None
        } else {
            Some(candidates[rng.random_range(0..candidates.len())].clone())
        }
    }

    fn latest_versions_by_history(&self) -> Vec<&StoredVersionRecord> {
        let mut latest = self
            .version_messages
            .values()
            .filter_map(ShadowVersionChain::latest)
            .collect::<Vec<_>>();
        latest.sort_by_key(|record| record.history_serial());
        latest
    }

    fn presence_would_record(&self, event: PresenceHistoryEventKind, user_id: &str) -> bool {
        self.presence_latest_event_by_user.get(user_id) != Some(&event)
    }

    fn append_presence(&mut self, item: PresenceHistoryItem) {
        self.presence_stream_id = Some(item.stream_id.clone());
        self.presence_events.push_back(ShadowPresenceEvent {
            stream_id: item.stream_id,
            serial: item.serial,
            published_at_ms: item.published_at_ms,
            event: item.event,
            cause: item.cause,
            user_id: item.user_id,
            connection_id: item.connection_id,
            dead_node_id: item.dead_node_id,
        });
        self.rebuild_presence_members();
        if let Some(limit) = self.presence_limit {
            while self.presence_events.len() > limit {
                self.presence_events.pop_front();
            }
            self.rebuild_presence_members();
        }
    }

    fn reset_presence(&mut self, stream_id: String) {
        self.presence_stream_id = Some(stream_id);
        self.presence_events.clear();
        self.presence_latest_event_by_user.clear();
        self.presence_members.clear();
    }

    fn rebuild_presence_members(&mut self) {
        self.presence_latest_event_by_user.clear();
        self.presence_members.clear();
        for event in &self.presence_events {
            self.presence_latest_event_by_user
                .insert(event.user_id.clone(), event.event);
            match event.event {
                PresenceHistoryEventKind::MemberAdded | PresenceHistoryEventKind::MemberUpdated => {
                    self.presence_members
                        .insert(event.user_id.clone(), event.event);
                }
                PresenceHistoryEventKind::MemberRemoved => {
                    self.presence_members.remove(&event.user_id);
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ShadowHistoryMessage {
    stream_id: String,
    serial: u64,
    published_at_ms: i64,
    message_id: Option<String>,
    event_name: Option<String>,
    operation_kind: String,
    payload: Bytes,
}

#[derive(Debug, Clone, Default)]
struct ShadowVersionChain {
    versions: Vec<StoredVersionRecord>,
}

impl ShadowVersionChain {
    fn latest(&self) -> Option<&StoredVersionRecord> {
        self.versions.last()
    }
}

#[derive(Debug, Clone)]
struct ShadowPresenceEvent {
    stream_id: String,
    serial: u64,
    published_at_ms: i64,
    event: PresenceHistoryEventKind,
    cause: PresenceHistoryEventCause,
    user_id: String,
    connection_id: Option<String>,
    dead_node_id: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct Stats {
    operations: u64,
    rejected_operations: u64,
    oracle_checks: u64,
    history_commits: u64,
    version_commits: u64,
    presence_events: u64,
    dropped_fanout: u64,
    duplicated_fanout: u64,
    delivered_messages: u64,
    recovered_messages: u64,
    recovery_truncations: u64,
    stale_deliveries: u64,
    node_crashes: u64,
    node_restarts: u64,
    node_partitions: u64,
    node_heals: u64,
    stream_resets: u64,
    purges: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(seed: u64) -> SimulatorConfig {
        SimulatorConfig {
            seed,
            ticks: 300,
            nodes: 4,
            clients: 4,
            channels: 3,
            users: 6,
            oracle_every: 10,
            page_limit: 3,
            history_retention_messages: Some(128),
            presence_retention_events: Some(128),
            workload: crate::WorkloadConfig::default(),
            fault: crate::FaultConfig {
                fanout_drop_probability: 0.20,
                fanout_duplicate_probability: 0.10,
                max_fanout_delay_ticks: 6,
                node_crash_probability: 0.01,
                node_restart_probability: 0.05,
                node_partition_probability: 0.01,
                node_heal_probability: 0.05,
                stream_reset_probability: 0.001,
            },
        }
    }

    #[tokio::test]
    async fn same_seed_produces_same_report() {
        let config = test_config(0xdeca_fbad);
        let mut left = DeterministicSimulator::new(config.clone()).unwrap();
        let mut right = DeterministicSimulator::new(config).unwrap();

        let left_report = left.run().await.unwrap();
        let right_report = right.run().await.unwrap();

        assert_eq!(left_report, right_report);
    }

    #[tokio::test]
    async fn dropped_fanout_recovers_from_history() {
        let mut config = test_config(0xc0ffee);
        config.ticks = 120;
        config.fault.fanout_drop_probability = 1.0;
        config.fault.fanout_duplicate_probability = 0.0;
        config.fault.stream_reset_probability = 0.0;
        let mut simulator = DeterministicSimulator::new(config).unwrap();

        let report = simulator.run().await.unwrap();

        assert_eq!(report.delivered_messages, 0);
        assert!(
            report.recovered_messages > 0,
            "history recovery should catch clients up after dropped fanout"
        );
        assert!(report.history_commits > 0);
        assert!(report.oracle_checks > 0);
    }
}
