use bytes::Bytes;
use serde::Serialize;
use sockudo_core::history::{
    HistoryAppendRecord, HistoryCursor, HistoryDirection, HistoryItem, HistoryPurgeMode,
    HistoryPurgeRequest, HistoryQueryBounds, HistoryReadRequest, HistoryRetentionPolicy,
    HistoryStore,
};
use sockudo_core::presence_history::{
    PresenceHistoryCursor, PresenceHistoryDirection, PresenceHistoryEventCause,
    PresenceHistoryEventKind, PresenceHistoryItem, PresenceHistoryQueryBounds,
    PresenceHistoryReadRequest, PresenceHistoryRetentionPolicy, PresenceHistoryStore,
    PresenceHistoryTransitionRecord, PresenceSnapshotRequest,
};
use sockudo_core::version_store::{
    StoredVersionRecord, VersionReplayRequest, VersionStore, VersionStoreCursor,
    VersionStoreDirection, VersionStoreReadRequest,
};
use sockudo_core::versioned_messages::{
    FieldPatch, MessageAction, MessageAppend, MessageFieldDelta, MessageSerial, VersionMetadata,
    VersionSerial, VersionedMessage,
};
use sockudo_protocol::messages::{ExtrasValue, MessageData, MessageExtras, PusherMessage};
use sockudo_protocol::versioned_messages::{
    MessageAction as ProtocolMessageAction, MessageVersionMetadata, apply_runtime_metadata,
    clear_runtime_append_fragment, extract_runtime_action,
};
use sockudo_protocol::wire::{deserialize_message, serialize_message};
use sockudo_protocol::{ProtocolVersion, WireFormat};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use crate::config::{SimulatorConfig, SimulatorMode};
use crate::error::{SimulatorError, SimulatorResult};
use crate::io::{
    DeterministicClock, DeterministicFaultScheduler, DeterministicNetwork, DeterministicStorage,
    ScheduledIoEvent,
};
use crate::push_lab::{PushLab, PushSimulationReport};
use crate::real_subsystems::{APP_ID, RealSubsystemHarness};
use crate::workload::{ActionWeights, WorkloadAction, WorkloadActionCounts, WorkloadGenerator};

const MAX_TRACE_EVENTS: usize = 96;

/// Summary emitted by a successful simulator run.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SimulationReport {
    pub seed: u64,
    pub ticks: u64,
    pub mode: SimulatorMode,
    pub operations: u64,
    pub rejected_operations: u64,
    pub oracle_checks: u64,
    pub protocol_oracles: ProtocolOracleReport,
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
    pub node_pauses: u64,
    pub node_resumes: u64,
    pub node_partitions: u64,
    pub node_heals: u64,
    pub node_slowdowns: u64,
    pub node_stale_marks: u64,
    pub stream_resets: u64,
    pub purges: u64,
    pub storage_dropped_writes: u64,
    pub storage_torn_writes: u64,
    pub storage_delayed_commits: u64,
    pub storage_stale_reads: u64,
    pub storage_corrupted_reads: u64,
    pub storage_recovery_checks: u64,
    pub quiesce_ticks: u64,
    pub liveness_max_quiesce_ticks: u64,
    pub live_nodes: usize,
    pub paused_nodes: usize,
    pub partitioned_nodes: usize,
    pub slow_nodes: usize,
    pub stale_nodes: usize,
    pub queued_fanout: usize,
    pub push: PushSimulationReport,
    pub recent_trace: Vec<String>,
    pub io_trace: Vec<String>,
    pub workload_weights: ActionWeights,
    pub workload_actions: WorkloadActionCounts,
}

/// Protocol-aware oracle counters emitted in the simulator report.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct ProtocolOracleReport {
    pub v1_deliveries_checked: u64,
    pub v1_suppressed_deliveries: u64,
    pub v2_deliveries_checked: u64,
    pub v2_recovery_cursors_checked: u64,
    pub duplicate_identity_checks: u64,
    pub serial_monotonicity_checks: u64,
    pub attach_rewind_gap_checks: u64,
    pub presence_edge_checks: u64,
}

/// Seed-deterministic simulator over Sockudo's durable core primitives.
pub struct DeterministicSimulator {
    config: SimulatorConfig,
    clock: DeterministicClock,
    scheduler: DeterministicFaultScheduler,
    real: RealSubsystemHarness,
    nodes: Vec<NodeState>,
    clients: Vec<ClientState>,
    channels: Vec<String>,
    network: DeterministicNetwork<NetworkEvent>,
    push: PushLab,
    storage: DeterministicStorage,
    storage_visibility: StorageVisibility,
    shadow: Shadow,
    workload: WorkloadGenerator,
    stats: Stats,
    trace: VecDeque<String>,
    next_message_serial: u64,
    next_version_serial: u64,
    next_presence_connection: u64,
    quiesce_started_at: Option<u64>,
}

impl DeterministicSimulator {
    pub fn new(config: SimulatorConfig) -> SimulatorResult<Self> {
        config.validate()?;
        let workload = WorkloadGenerator::new(config.seed, config.workload.clone())?;
        let channels = (0..config.channels)
            .map(|idx| format!("sim-channel-{idx}"))
            .collect::<Vec<_>>();
        let clients = (0..config.clients)
            .map(|idx| {
                let protocol = if idx.is_multiple_of(2) {
                    ProtocolVersion::V2
                } else {
                    ProtocolVersion::V1
                };
                ClientState::new(idx, protocol, &channels)
            })
            .collect();
        let nodes = (0..config.nodes).map(NodeState::new).collect();
        let push = PushLab::new(&channels, config.clients, config.push.clone())?;
        let real = RealSubsystemHarness::new(&config);

        Ok(Self {
            clock: DeterministicClock::default(),
            scheduler: DeterministicFaultScheduler::new(config.seed),
            shadow: Shadow::new(
                &channels,
                config.history_retention_messages,
                config.presence_retention_events,
            ),
            config,
            real,
            nodes,
            clients,
            channels,
            network: DeterministicNetwork::default(),
            push,
            storage: DeterministicStorage,
            storage_visibility: StorageVisibility::default(),
            stats: Stats::default(),
            workload,
            trace: VecDeque::new(),
            next_message_serial: 1,
            next_version_serial: 1,
            next_presence_connection: 1,
            quiesce_started_at: None,
        })
    }

    pub async fn run(&mut self) -> SimulatorResult<SimulationReport> {
        for tick in 0..self.config.ticks {
            self.clock.set_tick(tick);
            self.storage_visibility.flush(tick);
            self.deliver_due_fanout()?;
            self.push.on_tick(self.tick(), &mut self.scheduler).await?;
            self.inject_faults().await?;
            self.drive_workload().await?;
            if self.config.oracle_every > 0 && tick % self.config.oracle_every == 0 {
                self.check_oracles().await?;
            }
        }

        self.quiesce().await?;
        self.recover_all_clients().await?;
        self.check_oracles().await?;
        self.push
            .check_oracles(true, self.config.page_limit)
            .await
            .map_err(|message| self.fail(message))?;
        self.check_liveness_oracles()?;
        Ok(self.report())
    }

    fn report(&self) -> SimulationReport {
        let tick = self.tick();
        SimulationReport {
            seed: self.config.seed,
            ticks: tick.saturating_add(1),
            mode: self.config.mode,
            operations: self.stats.operations,
            rejected_operations: self.stats.rejected_operations,
            oracle_checks: self.stats.oracle_checks,
            protocol_oracles: self.stats.protocol.report(),
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
            node_pauses: self.stats.node_pauses,
            node_resumes: self.stats.node_resumes,
            node_partitions: self.stats.node_partitions,
            node_heals: self.stats.node_heals,
            node_slowdowns: self.stats.node_slowdowns,
            node_stale_marks: self.stats.node_stale_marks,
            stream_resets: self.stats.stream_resets,
            purges: self.stats.purges,
            storage_dropped_writes: self.stats.storage_dropped_writes,
            storage_torn_writes: self.stats.storage_torn_writes,
            storage_delayed_commits: self.stats.storage_delayed_commits,
            storage_stale_reads: self.stats.storage_stale_reads,
            storage_corrupted_reads: self.stats.storage_corrupted_reads,
            storage_recovery_checks: self.stats.storage_recovery_checks,
            quiesce_ticks: self.quiesce_ticks(),
            liveness_max_quiesce_ticks: self.config.liveness.max_quiesce_ticks,
            live_nodes: self.nodes.iter().filter(|node| node.alive).count(),
            paused_nodes: self.nodes.iter().filter(|node| node.paused).count(),
            partitioned_nodes: self.nodes.iter().filter(|node| node.partitioned).count(),
            slow_nodes: self.nodes.iter().filter(|node| node.is_slow(tick)).count(),
            stale_nodes: self.nodes.iter().filter(|node| node.is_stale(tick)).count(),
            queued_fanout: self.network.len(),
            push: self.push.report(),
            recent_trace: self.trace.iter().cloned().collect(),
            io_trace: self.scheduler.recent_trace(),
            workload_weights: self.workload.weights(),
            workload_actions: self.workload.selected().clone(),
        }
    }

    async fn drive_workload(&mut self) -> SimulatorResult<()> {
        self.stats.operations = self.stats.operations.saturating_add(1);
        let action = self.workload.next_action();
        self.scheduler
            .record(self.tick(), format!("operation {}", action.as_str()));
        if self.route_rejects() {
            self.stats.rejected_operations = self.stats.rejected_operations.saturating_add(1);
            self.scheduler
                .record(self.tick(), "operation route_rejected");
            return Ok(());
        }

        match action {
            WorkloadAction::PublishMessage => self.publish_message().await,
            WorkloadAction::CreateVersionedMessage => self.create_versioned_message().await,
            WorkloadAction::MutateVersionedMessage => self.mutate_versioned_message().await,
            WorkloadAction::PresenceTransition => self.record_presence_transition().await,
            WorkloadAction::RecoveryProbe => self.recovery_probe().await,
            WorkloadAction::PurgeHistory => self.purge_history_prefix().await,
            WorkloadAction::PushRegisterDevice => {
                self.push
                    .register_or_update_device(self.tick(), &mut self.scheduler)
                    .await
            }
            WorkloadAction::PushDeleteDevice => {
                self.push
                    .delete_device(self.tick(), &mut self.scheduler)
                    .await
            }
            WorkloadAction::PushSubscribe => {
                self.push
                    .subscribe_device(self.tick(), &mut self.scheduler)
                    .await
            }
            WorkloadAction::PushUnsubscribe => {
                self.push
                    .unsubscribe_device(self.tick(), &mut self.scheduler)
                    .await
            }
            WorkloadAction::PushPublish => {
                self.push
                    .publish_now(self.tick(), &mut self.scheduler)
                    .await
            }
            WorkloadAction::PushScheduledPublish => {
                self.push
                    .schedule_publish(self.tick(), &mut self.scheduler)
                    .await
            }
            WorkloadAction::PushProviderFeedback => {
                self.push
                    .duplicate_provider_feedback(self.tick(), &mut self.scheduler);
                Ok(())
            }
            WorkloadAction::PushRepair => {
                self.push.repair_now(self.tick(), &mut self.scheduler);
                Ok(())
            }
            WorkloadAction::OracleCheck => self.check_oracles().await,
        }
    }

    fn route_rejects(&mut self) -> bool {
        let tick = self.tick();
        if !self.nodes.iter().any(|node| node.can_accept_traffic(tick)) {
            return true;
        }
        let node_idx = self.scheduler.usize_below(self.nodes.len());
        if !self.nodes[node_idx].can_accept_traffic(tick) {
            return true;
        }
        if let Some(node) = self.nodes.get_mut(node_idx) {
            node.accepted_operations = node.accepted_operations.saturating_add(1);
        }
        false
    }

    async fn inject_faults(&mut self) -> SimulatorResult<()> {
        if self.roll("node_crash", self.config.fault.node_crash_probability) {
            self.crash_random_node();
        }
        if self.roll("node_restart", self.config.fault.node_restart_probability)
            && self.restart_random_node()
        {
            self.check_durable_recovery_after_restart().await?;
        }
        if self.roll("node_pause", self.config.fault.node_pause_probability) {
            self.pause_random_node();
        }
        if self.roll("node_resume", self.config.fault.node_resume_probability) {
            self.resume_random_node();
        }
        if self.roll(
            "node_partition",
            self.config.fault.node_partition_probability,
        ) {
            self.partition_random_node();
        }
        if self.roll("node_heal", self.config.fault.node_heal_probability) {
            self.heal_random_node();
        }
        if self.roll("node_slow", self.config.fault.node_slow_probability) {
            self.slow_random_node();
        }
        if self.roll("node_stale", self.config.fault.node_stale_probability) {
            self.stale_random_node();
        }
        if self.roll("stream_reset", self.config.fault.stream_reset_probability) {
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
        let victim = live[self.scheduler.usize_below(live.len())];
        if let Some(node) = self.nodes.get_mut(victim) {
            node.alive = false;
            node.partitioned = false;
            self.stats.node_crashes = self.stats.node_crashes.saturating_add(1);
            self.trace(format!("node {victim} crashed"));
        }
    }

    fn restart_random_node(&mut self) -> bool {
        let down = self
            .nodes
            .iter()
            .filter(|node| !node.alive)
            .map(|node| node.id)
            .collect::<Vec<_>>();
        if down.is_empty() {
            return false;
        }
        let node_idx = down[self.scheduler.usize_below(down.len())];
        if let Some(node) = self.nodes.get_mut(node_idx) {
            node.alive = true;
            node.paused = false;
            self.stats.node_restarts = self.stats.node_restarts.saturating_add(1);
            self.trace(format!("node {node_idx} restarted"));
            return true;
        }
        false
    }

    fn pause_random_node(&mut self) {
        let eligible = self
            .nodes
            .iter()
            .filter(|node| node.alive && !node.paused)
            .map(|node| node.id)
            .collect::<Vec<_>>();
        if eligible.len() <= 1 {
            return;
        }
        let node_idx = eligible[self.scheduler.usize_below(eligible.len())];
        if let Some(node) = self.nodes.get_mut(node_idx) {
            node.paused = true;
            self.stats.node_pauses = self.stats.node_pauses.saturating_add(1);
            self.trace(format!("node {node_idx} paused"));
        }
    }

    fn resume_random_node(&mut self) {
        let paused = self
            .nodes
            .iter()
            .filter(|node| node.paused)
            .map(|node| node.id)
            .collect::<Vec<_>>();
        if paused.is_empty() {
            return;
        }
        let node_idx = paused[self.scheduler.usize_below(paused.len())];
        if let Some(node) = self.nodes.get_mut(node_idx) {
            node.paused = false;
            self.stats.node_resumes = self.stats.node_resumes.saturating_add(1);
            self.trace(format!("node {node_idx} resumed"));
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
        let node_idx = eligible[self.scheduler.usize_below(eligible.len())];
        if let Some(node) = self.nodes.get_mut(node_idx) {
            node.partitioned = true;
            self.stats.node_partitions = self.stats.node_partitions.saturating_add(1);
            self.trace(format!("node {node_idx} partitioned"));
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
        let node_idx = partitioned[self.scheduler.usize_below(partitioned.len())];
        if let Some(node) = self.nodes.get_mut(node_idx) {
            node.partitioned = false;
            self.stats.node_heals = self.stats.node_heals.saturating_add(1);
            self.trace(format!("node {node_idx} healed"));
        }
    }

    fn slow_random_node(&mut self) {
        let eligible = self
            .nodes
            .iter()
            .filter(|node| node.alive)
            .map(|node| node.id)
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            return;
        }
        let node_idx = eligible[self.scheduler.usize_below(eligible.len())];
        let slow_until = self.tick().saturating_add(self.random_delay().max(3));
        if let Some(node) = self.nodes.get_mut(node_idx) {
            node.slow_until = slow_until;
            self.stats.node_slowdowns = self.stats.node_slowdowns.saturating_add(1);
            self.trace(format!("node {node_idx} slow until {slow_until}"));
        }
    }

    fn stale_random_node(&mut self) {
        let eligible = self
            .nodes
            .iter()
            .filter(|node| node.alive)
            .map(|node| node.id)
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            return;
        }
        let node_idx = eligible[self.scheduler.usize_below(eligible.len())];
        let stale_until = self.tick().saturating_add(self.random_delay().max(2));
        if let Some(node) = self.nodes.get_mut(node_idx) {
            node.stale_until = stale_until;
            self.stats.node_stale_marks = self.stats.node_stale_marks.saturating_add(1);
            self.trace(format!("node {node_idx} stale until {stale_until}"));
        }
    }

    async fn reset_random_stream(&mut self) -> SimulatorResult<()> {
        let channel = self.random_channel();
        let history = self
            .real
            .history
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
        self.storage_visibility.reset_history(&channel);

        let presence = self
            .real
            .presence
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
        self.storage_visibility.reset_presence(&channel);

        for client in &mut self.clients {
            client.reset_channel(&channel, history.inspection.stream_id.clone());
        }
        self.network.retain(|event| event.channel != channel);
        self.stats.stream_resets = self.stats.stream_resets.saturating_add(1);
        self.trace(format!("stream reset on {channel}"));
        Ok(())
    }

    async fn publish_message(&mut self) -> SimulatorResult<()> {
        let channel = self.random_channel();
        let tick = self.tick();
        let payload = Bytes::from(format!("payload:{}:{}", tick, self.stats.history_commits));
        let Some(message) = self
            .append_history(&channel, "client-event", "publish", payload)
            .await?
        else {
            return Ok(());
        };
        self.schedule_fanout(&channel, &message);
        self.trace(format!(
            "history publish {channel} serial={}",
            message.serial
        ));
        Ok(())
    }

    async fn create_versioned_message(&mut self) -> SimulatorResult<()> {
        let channel = self.random_channel();
        let tick = self.tick();
        let Some(mut history) = self
            .append_history(
                &channel,
                "sockudo:message.create",
                MessageAction::Create.as_str(),
                Bytes::from(format!("version-create:{tick}")),
            )
            .await?
        else {
            return Ok(());
        };
        if self.storage_write_torn("version_create") {
            self.schedule_fanout(&channel, &history);
            self.trace(format!(
                "versioned create torn after history {channel} history_serial={}",
                history.serial
            ));
            return Ok(());
        }
        let delivery = self
            .real
            .version
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
            Some(MessageData::String(format!("created at tick {}", tick))),
            Some(MessageExtras::default()),
        );
        let record = StoredVersionRecord {
            app_id: APP_ID.to_string(),
            channel: channel.clone(),
            original_client_id: Some(format!(
                "client-{}",
                self.scheduler.usize_below(self.config.clients)
            )),
            message: message.clone(),
        };
        self.real.version.append_version(record.clone()).await?;
        self.record_version_visibility(&channel, record.delivery_serial());
        history.version_delivery_serial = Some(record.delivery_serial());
        self.shadow.channel_mut(&channel).append_version(record);
        self.shadow
            .channel_mut(&channel)
            .link_version_history(history.serial, history.version_delivery_serial);
        self.stats.version_commits = self.stats.version_commits.saturating_add(1);
        self.schedule_fanout(&channel, &history);
        self.trace(format!(
            "versioned create {channel} history_serial={}",
            history.serial
        ));
        Ok(())
    }

    async fn mutate_versioned_message(&mut self) -> SimulatorResult<()> {
        let channel = self.random_channel();
        let tick = self.tick();
        let Some(current) = self
            .shadow
            .channel(&channel)
            .random_live_version(&mut self.scheduler, tick)
        else {
            return self.create_versioned_message().await;
        };

        let action_roll = self.scheduler.u32_below(100);
        let (action, payload) = if action_roll < 40 {
            (MessageAction::Update, "message.update")
        } else if action_roll < 75 {
            (MessageAction::Append, "message.append")
        } else {
            (MessageAction::Delete, "message.delete")
        };
        let Some(mut history) = self
            .append_history(
                &channel,
                action.as_str(),
                action.as_str(),
                Bytes::from(format!("{payload}:{tick}")),
            )
            .await?
        else {
            return Ok(());
        };
        if self.storage_write_torn("version_mutation") {
            self.schedule_fanout(&channel, &history);
            self.trace(format!(
                "versioned mutation torn after history {channel} history_serial={}",
                history.serial
            ));
            return Ok(());
        }
        let delivery = self
            .real
            .version
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
                        tick
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
                    data_fragment: format!("|{tick}"),
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
        self.real.version.append_version(record.clone()).await?;
        self.record_version_visibility(&channel, record.delivery_serial());
        history.version_delivery_serial = Some(record.delivery_serial());
        self.shadow.channel_mut(&channel).append_version(record);
        self.shadow
            .channel_mut(&channel)
            .link_version_history(history.serial, history.version_delivery_serial);
        self.stats.version_commits = self.stats.version_commits.saturating_add(1);
        self.schedule_fanout(&channel, &history);
        self.trace(format!(
            "versioned mutation {channel} history_serial={}",
            history.serial
        ));
        Ok(())
    }

    async fn record_presence_transition(&mut self) -> SimulatorResult<()> {
        let channel = self.random_channel();
        let tick = self.tick();
        let user_id = format!("user-{}", self.scheduler.usize_below(self.config.users));
        let active_connections = self.shadow.channel(&channel).presence_connections(&user_id);
        let action = if active_connections.is_empty() {
            if self.scheduler.ratio(1, 5) {
                PresenceConnectionAction::Leave
            } else {
                PresenceConnectionAction::Join
            }
        } else if self.scheduler.ratio(2, 5) {
            PresenceConnectionAction::Join
        } else {
            PresenceConnectionAction::Leave
        };
        let connection_id = match action {
            PresenceConnectionAction::Join => self.next_presence_connection_id(&user_id),
            PresenceConnectionAction::Leave => {
                let connection_idx = if active_connections.is_empty() {
                    0
                } else {
                    self.scheduler.usize_below(active_connections.len())
                };
                active_connections
                    .get(connection_idx)
                    .cloned()
                    .unwrap_or_else(|| format!("socket-{user_id}-ghost-{tick}"))
            }
        };
        let before_connections = active_connections.len();
        let after_connections = match action {
            PresenceConnectionAction::Join => before_connections.saturating_add(1),
            PresenceConnectionAction::Leave if before_connections > 0 => {
                before_connections.saturating_sub(1)
            }
            PresenceConnectionAction::Leave => before_connections,
        };
        let expected_event = match (before_connections, after_connections, action) {
            (0, after, PresenceConnectionAction::Join) if after > 0 => {
                Some(PresenceHistoryEventKind::MemberAdded)
            }
            (before, 0, PresenceConnectionAction::Leave) if before > 0 => {
                Some(PresenceHistoryEventKind::MemberRemoved)
            }
            _ => None,
        };

        if self.storage_write_dropped("presence_transition") {
            return Ok(());
        }

        if let Some(event) = expected_event {
            let cause = match event {
                PresenceHistoryEventKind::MemberAdded | PresenceHistoryEventKind::MemberUpdated => {
                    PresenceHistoryEventCause::Join
                }
                PresenceHistoryEventKind::MemberRemoved => PresenceHistoryEventCause::Disconnect,
            };
            let record = PresenceHistoryTransitionRecord {
                app_id: APP_ID.to_string(),
                channel: channel.clone(),
                event_kind: event,
                cause,
                user_id: user_id.clone(),
                connection_id: Some(connection_id.clone()),
                user_info: None,
                dead_node_id: None,
                dedupe_key: format!(
                    "presence:{tick}:{}:{user_id}:{connection_id}",
                    self.stats.operations
                ),
                published_at_ms: self.timestamp_ms(),
                retention: self.presence_retention_policy(),
            };
            self.real.presence.record_transition(record).await?;
            let newest = self
                .read_presence_page(&channel, PresenceHistoryDirection::NewestFirst)
                .await?;
            let Some(item) = newest.first() else {
                return Err(self.fail(format!(
                    "presence transition for {channel}/{user_id} was predicted to record but store returned no newest item"
                )));
            };
            self.shadow.channel_mut(&channel).append_presence(
                item.clone(),
                before_connections,
                after_connections,
            );
            self.record_presence_visibility(&channel, item.serial);
            self.stats.presence_events = self.stats.presence_events.saturating_add(1);
            self.trace(format!(
                "presence {:?} {channel}/{user_id} connection={connection_id} before={before_connections} after={after_connections} serial={}",
                event, item.serial
            ));
        } else {
            self.trace(format!(
                "presence {:?} {channel}/{user_id} connection={connection_id} before={before_connections} after={after_connections} no_edge",
                action
            ));
        }
        self.shadow.channel_mut(&channel).apply_presence_connection(
            &user_id,
            &connection_id,
            action,
        );
        Ok(())
    }

    async fn purge_history_prefix(&mut self) -> SimulatorResult<()> {
        let channel = self.random_channel();
        let shadow = self.shadow.channel(&channel);
        if shadow.history.len() < 4 {
            return Ok(());
        }
        let idx = self
            .scheduler
            .usize_below(shadow.history.len().saturating_sub(1))
            .saturating_add(1);
        let before_serial = shadow.history[idx].serial;
        self.real
            .history
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
        self.trace(format!(
            "history purge {channel} before_serial={before_serial}"
        ));
        Ok(())
    }

    async fn append_history(
        &mut self,
        channel: &str,
        event_name: &str,
        operation_kind: &str,
        payload: Bytes,
    ) -> SimulatorResult<Option<ShadowHistoryMessage>> {
        if self.storage_write_dropped(operation_kind) {
            return Ok(None);
        }
        let reservation = self
            .real
            .history
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
        self.real.history.append(record.clone()).await?;
        let message = ShadowHistoryMessage {
            stream_id: record.stream_id,
            serial: record.serial,
            published_at_ms: record.published_at_ms,
            message_id: record.message_id,
            idempotency_key: None,
            version_delivery_serial: None,
            event_name: record.event_name,
            operation_kind: record.operation_kind,
            payload,
        };
        self.shadow
            .channel_mut(channel)
            .append_history(message.clone());
        self.record_history_visibility(channel, message.serial);
        self.stats.history_commits = self.stats.history_commits.saturating_add(1);
        Ok(Some(message))
    }

    fn storage_write_dropped(&mut self, boundary: &str) -> bool {
        let tick = self.tick();
        if self.storage.write_is_dropped(
            &mut self.scheduler,
            tick,
            boundary,
            self.config.fault.storage.dropped_write_probability,
        ) {
            self.stats.storage_dropped_writes = self.stats.storage_dropped_writes.saturating_add(1);
            self.trace(format!("storage dropped write at {boundary}"));
            return true;
        }
        false
    }

    fn storage_write_torn(&mut self, boundary: &str) -> bool {
        let tick = self.tick();
        if self.storage.write_is_torn(
            &mut self.scheduler,
            tick,
            boundary,
            self.config.fault.storage.torn_write_probability,
        ) {
            self.stats.storage_torn_writes = self.stats.storage_torn_writes.saturating_add(1);
            self.trace(format!("storage torn write at {boundary}"));
            return true;
        }
        false
    }

    fn record_history_visibility(&mut self, channel: &str, serial: u64) {
        let tick = self.tick();
        let visible_at = self.storage.commit_visible_at(
            &mut self.scheduler,
            tick,
            "history",
            self.config.fault.storage.delayed_commit_probability,
            self.config.fault.storage.max_commit_delay_ticks,
        );
        if visible_at > tick {
            self.stats.storage_delayed_commits =
                self.stats.storage_delayed_commits.saturating_add(1);
        }
        self.storage_visibility
            .record_history(channel, serial, visible_at);
        self.storage_visibility.flush(tick);
    }

    fn record_version_visibility(&mut self, channel: &str, delivery_serial: u64) {
        let tick = self.tick();
        let visible_at = self.storage.commit_visible_at(
            &mut self.scheduler,
            tick,
            "version",
            self.config.fault.storage.delayed_commit_probability,
            self.config.fault.storage.max_commit_delay_ticks,
        );
        if visible_at > tick {
            self.stats.storage_delayed_commits =
                self.stats.storage_delayed_commits.saturating_add(1);
        }
        self.storage_visibility
            .record_version(channel, delivery_serial, visible_at);
        self.storage_visibility.flush(tick);
    }

    fn record_presence_visibility(&mut self, channel: &str, serial: u64) {
        let tick = self.tick();
        let visible_at = self.storage.commit_visible_at(
            &mut self.scheduler,
            tick,
            "presence",
            self.config.fault.storage.delayed_commit_probability,
            self.config.fault.storage.max_commit_delay_ticks,
        );
        if visible_at > tick {
            self.stats.storage_delayed_commits =
                self.stats.storage_delayed_commits.saturating_add(1);
        }
        self.storage_visibility
            .record_presence(channel, serial, visible_at);
        self.storage_visibility.flush(tick);
    }

    fn drain_storage_visibility(&mut self) {
        let tick = self
            .storage_visibility
            .next_due_tick()
            .unwrap_or(self.tick());
        self.clock.advance_to(tick);
        self.storage_visibility.force_visible();
    }

    fn schedule_fanout(&mut self, channel: &str, message: &ShadowHistoryMessage) {
        let tick = self.tick();
        for client_idx in 0..self.clients.len() {
            if self.roll("fanout_drop", self.config.fault.fanout_drop_probability) {
                self.stats.dropped_fanout = self.stats.dropped_fanout.saturating_add(1);
                continue;
            }
            let source_node = self.scheduler.usize_below(self.nodes.len());
            if !self.nodes[source_node].can_fanout(tick) {
                self.stats.dropped_fanout = self.stats.dropped_fanout.saturating_add(1);
                continue;
            }
            let slow_delay = if self.nodes[source_node].is_slow(tick) {
                self.random_delay()
            } else {
                0
            };
            let event = NetworkEvent {
                deliver_at: self
                    .tick()
                    .saturating_add(self.random_delay())
                    .saturating_add(slow_delay),
                client_idx,
                channel: channel.to_string(),
                stream_id: message.stream_id.clone(),
                serial: message.serial,
                source_node,
                message: message.clone(),
                source: DeliverySource::LiveFanout,
            };
            self.network.schedule(event.clone());
            if self.roll(
                "fanout_duplicate",
                self.config.fault.fanout_duplicate_probability,
            ) {
                let mut duplicate = event;
                duplicate.deliver_at = duplicate.deliver_at.saturating_add(self.random_delay());
                self.network.schedule(duplicate);
                self.stats.duplicated_fanout = self.stats.duplicated_fanout.saturating_add(1);
            }
        }
    }

    fn deliver_due_fanout(&mut self) -> SimulatorResult<()> {
        for event in self.network.drain_due(self.tick()) {
            self.check_delivery_protocol_oracles(&event)?;
            let accepted = self.clients[event.client_idx].deliver(&event);
            if accepted {
                self.stats.delivered_messages = self.stats.delivered_messages.saturating_add(1);
                if let Some(node) = self.nodes.get_mut(event.source_node) {
                    node.fanout_deliveries = node.fanout_deliveries.saturating_add(1);
                }
            } else {
                self.stats.stale_deliveries = self.stats.stale_deliveries.saturating_add(1);
            }
        }
        Ok(())
    }

    async fn quiesce(&mut self) -> SimulatorResult<()> {
        self.quiesce_started_at = Some(self.tick());
        let max_steps = self.network.len();
        for _ in 0..max_steps {
            let Some(next_delivery_tick) = self.network.next_delivery_tick() else {
                let tick = self.push.quiesce(self.tick(), &mut self.scheduler).await?;
                self.clock.set_tick(tick);
                self.storage_visibility.flush(tick);
                self.drain_storage_visibility();
                return Ok(());
            };
            self.clock.advance_by(1);
            self.clock.advance_to(next_delivery_tick);
            self.storage_visibility.flush(self.tick());
            self.deliver_due_fanout()?;
        }
        if !self.network.is_empty() {
            return Err(self.fail(format!(
                "quiesce did not drain fanout queue after {max_steps} steps (remaining={})",
                self.network.len()
            )));
        }
        let tick = self.push.quiesce(self.tick(), &mut self.scheduler).await?;
        self.clock.set_tick(tick);
        self.storage_visibility.flush(tick);
        self.drain_storage_visibility();
        Ok(())
    }

    fn check_liveness_oracles(&self) -> SimulatorResult<()> {
        if self.config.mode != SimulatorMode::Liveness {
            return Ok(());
        }

        let quiesce_ticks = self.quiesce_ticks();
        if quiesce_ticks > self.config.liveness.max_quiesce_ticks {
            return Err(self.fail(format!(
                "liveness quiesce budget exceeded: quiesce_ticks={quiesce_ticks} max={}",
                self.config.liveness.max_quiesce_ticks
            )));
        }
        if self.config.liveness.require_recovery_after_drop
            && self.stats.dropped_fanout > 0
            && self.stats.history_commits > 0
            && self.stats.recovered_messages == 0
        {
            return Err(self.fail(
                "liveness expected durable recovery after dropped live fanout, but no messages recovered"
                    .to_string(),
            ));
        }
        let push = self.push.report();
        let queue_loss = push
            .queue_produce_lost
            .saturating_add(push.queue_ack_lost)
            .saturating_add(push.queue_lease_timeouts);
        if self.config.liveness.require_repair_after_queue_loss
            && queue_loss > 0
            && push.accepted_publishes > 0
            && push.repair_requeued == 0
        {
            return Err(self.fail(
                "liveness expected push repair after queue loss, but repair never requeued work"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn quiesce_ticks(&self) -> u64 {
        self.quiesce_started_at
            .map_or(0, |started_at| self.tick().saturating_sub(started_at))
    }

    async fn recovery_probe(&mut self) -> SimulatorResult<()> {
        let client_idx = self.scheduler.usize_below(self.clients.len());
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
        let Some(current_stream_id) = self.shadow.channel(channel).history_stream_id.clone() else {
            return Ok(());
        };
        let (client_stream_id, last_contiguous) =
            self.clients[client_idx].recovery_position(channel);
        if client_stream_id.as_deref() != Some(current_stream_id.as_str()) {
            self.clients[client_idx].reset_channel(channel, Some(current_stream_id));
            return Ok(());
        }

        let oldest = self
            .shadow
            .channel(channel)
            .history
            .front()
            .map(|message| message.serial);
        if let Some(oldest_serial) = oldest
            && last_contiguous.saturating_add(1) < oldest_serial
        {
            self.stats.recovery_truncations = self.stats.recovery_truncations.saturating_add(1);
            self.clients[client_idx].rewind_below(channel, oldest_serial.saturating_sub(1));
            return Ok(());
        }

        let recovered = self
            .read_history_bounded_for_recovery(channel, last_contiguous.saturating_add(1))
            .await?;
        if recovered.corrupted {
            return Ok(());
        }
        if recovered.truncated_by_retention {
            self.stats.recovery_truncations = self.stats.recovery_truncations.saturating_add(1);
            return Ok(());
        }
        let expected = self
            .shadow
            .channel(channel)
            .history
            .iter()
            .filter(|message| {
                message.serial > last_contiguous && message.serial <= recovered.visible_serial
            })
            .map(|message| message.serial)
            .collect::<Vec<_>>();
        let actual = recovered
            .items
            .iter()
            .map(|item| item.serial)
            .collect::<Vec<_>>();
        if actual != expected {
            return Err(self.fail(format!(
                "client recovery mismatch on {channel}: expected serials {expected:?}, got {actual:?}"
            )));
        }
        for item in recovered.items {
            let message = ShadowHistoryMessage::from_history_item(item);
            let event = NetworkEvent {
                deliver_at: self.tick(),
                client_idx,
                channel: channel.to_string(),
                stream_id: message.stream_id.clone(),
                serial: message.serial,
                source_node: 0,
                message,
                source: DeliverySource::Recovery,
            };
            self.check_delivery_protocol_oracles(&event)?;
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
            self.check_serial_monotonicity_oracle(channel)?;
            self.check_committed_identity_oracle(channel)?;
            self.check_attach_rewind_gaplessness_oracle(channel).await?;
            self.check_client_recovery_oracle(channel)?;
            self.check_v2_recovery_cursor_oracle(channel)?;
        }
        self.push
            .check_oracles(false, self.config.page_limit)
            .await
            .map_err(|message| self.fail(message))?;
        Ok(())
    }

    async fn check_durable_recovery_after_restart(&mut self) -> SimulatorResult<()> {
        self.stats.storage_recovery_checks = self.stats.storage_recovery_checks.saturating_add(1);
        let channels = self.channels.clone();
        for channel in &channels {
            self.check_history_oracle(channel).await?;
            self.check_version_oracle(channel).await?;
            self.check_presence_oracle(channel).await?;
            self.check_serial_monotonicity_oracle(channel)?;
            self.check_committed_identity_oracle(channel)?;
            self.check_attach_rewind_gaplessness_oracle(channel).await?;
            self.check_client_recovery_oracle(channel)?;
            self.check_v2_recovery_cursor_oracle(channel)?;
        }
        self.push
            .check_oracles(false, self.config.page_limit)
            .await
            .map_err(|message| self.fail(message))?;
        Ok(())
    }

    fn check_delivery_protocol_oracles(&mut self, event: &NetworkEvent) -> SimulatorResult<()> {
        self.check_protocol_v1_delivery(event)?;
        self.check_protocol_v2_delivery(event)
    }

    fn check_protocol_v1_delivery(&mut self, event: &NetworkEvent) -> SimulatorResult<()> {
        let base = self.delivery_message(event);
        let rendered = self.v1_compatible_message(&base);
        let runtime_action = extract_runtime_action(&base);
        if matches!(
            runtime_action,
            Some(
                ProtocolMessageAction::Update
                    | ProtocolMessageAction::Delete
                    | ProtocolMessageAction::Append
                    | ProtocolMessageAction::Summary
            )
        ) {
            if rendered.is_some() {
                return Err(self.fail(format!(
                    "V1 rendered V2-only mutation for client {} on {} serial={} source={:?}",
                    event.client_idx, event.channel, event.serial, event.source
                )));
            }
            self.stats.protocol.v1_suppressed_deliveries = self
                .stats
                .protocol
                .v1_suppressed_deliveries
                .saturating_add(1);
            return Ok(());
        }

        let Some(rendered) = rendered else {
            return Err(self.fail(format!(
                "V1 unexpectedly suppressed deliverable message on {} serial={} source={:?}",
                event.channel, event.serial, event.source
            )));
        };
        let rendered = self.serialize_round_trip(rendered)?;
        let value = serde_json::to_value(&rendered)?;
        for key in [
            "serial",
            "stream_id",
            "message_id",
            "tags",
            "sequence",
            "conflation_key",
            "idempotency_key",
            "extras",
            "__delta_seq",
            "__conflation_key",
        ] {
            if value.get(key).is_some() {
                return Err(self.fail(format!(
                    "V1 delivery leaked V2-only field '{key}' on {} serial={} payload={value}",
                    event.channel, event.serial
                )));
            }
        }
        if rendered.event.as_deref().is_some_and(|name| {
            name.starts_with("sockudo:") || name.starts_with("sockudo_internal:")
        }) {
            return Err(self.fail(format!(
                "V1 delivery leaked Sockudo protocol prefix on {} serial={} event={:?}",
                event.channel, event.serial, rendered.event
            )));
        }
        self.stats.protocol.v1_deliveries_checked =
            self.stats.protocol.v1_deliveries_checked.saturating_add(1);
        Ok(())
    }

    fn check_protocol_v2_delivery(&mut self, event: &NetworkEvent) -> SimulatorResult<()> {
        let mut message = self.delivery_message(event);
        clear_runtime_append_fragment(&mut message);
        message.rewrite_prefix(ProtocolVersion::V2);
        message.idempotency_key = None;
        let rendered = self.serialize_round_trip(message)?;
        if rendered.stream_id.as_deref() != Some(event.stream_id.as_str())
            || rendered.serial != Some(event.serial)
            || rendered.message_id.as_deref() != event.message.message_id.as_deref()
        {
            return Err(self.fail(format!(
                "V2 continuity fields mismatch on {} serial={}: rendered stream={:?} serial={:?} message_id={:?} expected stream={} message_id={:?}",
                event.channel,
                event.serial,
                rendered.stream_id,
                rendered.serial,
                rendered.message_id,
                event.stream_id,
                event.message.message_id
            )));
        }
        if rendered.idempotency_key.is_some() {
            return Err(self.fail(format!(
                "V2 delivery leaked internal idempotency key on {} serial={}",
                event.channel, event.serial
            )));
        }
        if rendered.extras.is_none() {
            return Err(self.fail(format!(
                "V2 delivery dropped extras envelope on {} serial={}",
                event.channel, event.serial
            )));
        }
        self.stats.protocol.v2_deliveries_checked =
            self.stats.protocol.v2_deliveries_checked.saturating_add(1);
        Ok(())
    }

    fn delivery_message(&self, event: &NetworkEvent) -> PusherMessage {
        let mut headers = HashMap::from([(
            "simulator".to_string(),
            ExtrasValue::String("protocol-oracle".to_string()),
        )]);
        let mut message = PusherMessage {
            event: event.message.event_name.clone(),
            channel: Some(event.channel.clone()),
            data: Some(MessageData::String(
                String::from_utf8_lossy(&event.message.payload).to_string(),
            )),
            name: Some(event.message.operation_kind.clone()),
            user_id: None,
            tags: Some(BTreeMap::from([(
                "simulator".to_string(),
                "true".to_string(),
            )])),
            sequence: Some(event.serial),
            conflation_key: Some(event.channel.clone()),
            message_id: event.message.message_id.clone(),
            stream_id: Some(event.stream_id.clone()),
            serial: Some(event.serial),
            idempotency_key: Some(format!("internal-{}", event.serial)),
            extras: Some(MessageExtras {
                headers: Some(std::mem::take(&mut headers)),
                ephemeral: None,
                idempotency_key: event.message.idempotency_key.clone(),
                push: None,
                echo: Some(true),
                ai: None,
            }),
            delta_sequence: Some(event.serial),
            delta_conflation_key: Some(event.channel.clone()),
        };

        if let Some(action) = protocol_action_from_operation(&event.message.operation_kind) {
            let shadow = self.shadow.channel(&event.channel);
            let (message_serial, version) = self
                .delivery_version_record(event, shadow)
                .map(|record| {
                    (
                        record.message_serial().as_str().to_string(),
                        MessageVersionMetadata {
                            serial: record.version_serial().as_str().to_string(),
                            client_id: record.message.version.client_id.clone(),
                            timestamp_ms: record.message.version.timestamp_ms,
                            description: record.message.version.description.clone(),
                            metadata: record.message.version.metadata.clone(),
                        },
                    )
                })
                .unwrap_or_else(|| {
                    (
                        format!("torn-msg-{:020}", event.serial),
                        MessageVersionMetadata {
                            serial: format!("torn-ver-{:020}", event.serial),
                            client_id: None,
                            timestamp_ms: event.message.published_at_ms,
                            description: None,
                            metadata: None,
                        },
                    )
                });
            apply_runtime_metadata(
                &mut message,
                action,
                &message_serial,
                &version,
                Some(event.serial),
            );
        }
        message
    }

    fn delivery_version_record<'a>(
        &self,
        event: &NetworkEvent,
        shadow: &'a ShadowChannel,
    ) -> Option<&'a StoredVersionRecord> {
        event
            .message
            .version_delivery_serial
            .and_then(|delivery_serial| shadow.version_replay.get(&delivery_serial))
            .or_else(|| shadow.version_by_history_serial(event.serial))
    }

    fn v1_compatible_message(&self, message: &PusherMessage) -> Option<PusherMessage> {
        let runtime_action = extract_runtime_action(message);
        match runtime_action {
            Some(ProtocolMessageAction::Create) | None => {}
            Some(_) => return None,
        }

        let mut v1_message = message.clone();
        if runtime_action == Some(ProtocolMessageAction::Create) {
            v1_message.rewrite_prefix(ProtocolVersion::V1);
        }
        v1_message.serial = None;
        v1_message.message_id = None;
        v1_message.stream_id = None;
        v1_message.tags = None;
        v1_message.sequence = None;
        v1_message.conflation_key = None;
        v1_message.idempotency_key = None;
        v1_message.extras = None;
        v1_message.delta_sequence = None;
        v1_message.delta_conflation_key = None;
        Some(v1_message)
    }

    fn serialize_round_trip(&self, message: PusherMessage) -> SimulatorResult<PusherMessage> {
        let bytes = serialize_message(&message, WireFormat::Json)
            .map_err(|error| self.fail(format!("protocol oracle serialization failed: {error}")))?;
        deserialize_message(&bytes, WireFormat::Json)
            .map_err(|error| self.fail(format!("protocol oracle deserialization failed: {error}")))
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

        let head = self.real.history.channel_head(APP_ID, channel).await?;
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
            let inspection = self.real.history.stream_inspection(APP_ID, channel).await?;
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
            .real
            .version
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

        let state = self.real.version.stream_state(APP_ID, channel).await?;
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
                .real
                .version
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

        let latest_by_history = self.real.version.latest_by_history(APP_ID, channel).await?;
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

    async fn check_presence_oracle(&mut self, channel: &str) -> SimulatorResult<()> {
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
            match expected.event {
                PresenceHistoryEventKind::MemberAdded => {
                    if expected.connections_before != 0 || expected.connections_after == 0 {
                        return Err(self.fail(format!(
                            "presence first-join oracle failed on {channel}/{}: before={} after={} serial={}",
                            expected.user_id,
                            expected.connections_before,
                            expected.connections_after,
                            expected.serial
                        )));
                    }
                }
                PresenceHistoryEventKind::MemberRemoved => {
                    if expected.connections_before != 1 || expected.connections_after != 0 {
                        return Err(self.fail(format!(
                            "presence last-leave oracle failed on {channel}/{}: before={} after={} serial={}",
                            expected.user_id,
                            expected.connections_before,
                            expected.connections_after,
                            expected.serial
                        )));
                    }
                }
                PresenceHistoryEventKind::MemberUpdated => {}
            }
            self.stats.protocol.presence_edge_checks =
                self.stats.protocol.presence_edge_checks.saturating_add(1);
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
            .real
            .presence
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
                .real
                .presence
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

    fn check_v2_recovery_cursor_oracle(&mut self, channel: &str) -> SimulatorResult<()> {
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
            if client.protocol != ProtocolVersion::V2 {
                continue;
            }
            let Some(state) = client.channels.get(channel) else {
                continue;
            };
            if state.stream_id.as_deref() != Some(stream_id) {
                continue;
            }
            if state.last_contiguous > newest {
                return Err(self.fail(format!(
                    "V2 recovery cursor for client {} is ahead of {channel}: last={} newest={newest}",
                    client.id, state.last_contiguous
                )));
            }
            if state.last_contiguous.saturating_add(1) >= oldest {
                let recomputed = contiguous_prefix_from(&state.delivered, oldest.saturating_sub(1));
                if recomputed != state.last_contiguous {
                    return Err(self.fail(format!(
                        "V2 recovery cursor for client {} is not the delivered contiguous prefix on {channel}: cursor={} recomputed={recomputed} delivered={:?}",
                        client.id, state.last_contiguous, state.delivered
                    )));
                }
            }
            self.stats.protocol.v2_recovery_cursors_checked = self
                .stats
                .protocol
                .v2_recovery_cursors_checked
                .saturating_add(1);
        }
        Ok(())
    }

    fn check_committed_identity_oracle(&mut self, channel: &str) -> SimulatorResult<()> {
        let shadow = self.shadow.channel(channel);
        let mut history_ids: BTreeMap<String, u64> = BTreeMap::new();
        for message in &shadow.history {
            let identity = message
                .message_id
                .clone()
                .unwrap_or_else(|| format!("{}:{}:{}", message.stream_id, channel, message.serial));
            if let Some(previous) = history_ids.insert(identity.clone(), message.serial)
                && message.idempotency_key.is_none()
            {
                return Err(self.fail(format!(
                    "duplicate committed history identity on {channel}: identity={identity} previous_serial={previous} duplicate_serial={}",
                    message.serial
                )));
            }
            self.stats.protocol.duplicate_identity_checks = self
                .stats
                .protocol
                .duplicate_identity_checks
                .saturating_add(1);
        }

        let mut version_serials = BTreeMap::new();
        let mut delivery_serials = BTreeMap::new();
        for record in shadow.version_replay.values() {
            let version_serial = record.version_serial().as_str().to_string();
            if let Some(previous) =
                version_serials.insert(version_serial.clone(), record.delivery_serial())
            {
                return Err(self.fail(format!(
                    "duplicate committed version identity on {channel}: version_serial={version_serial} previous_delivery={previous} duplicate_delivery={}",
                    record.delivery_serial()
                )));
            }
            if let Some(previous) =
                delivery_serials.insert(record.delivery_serial(), version_serial.clone())
            {
                return Err(self.fail(format!(
                    "duplicate committed delivery identity on {channel}: delivery_serial={} previous_version={previous} duplicate_version={version_serial}",
                    record.delivery_serial()
                )));
            }
            self.stats.protocol.duplicate_identity_checks = self
                .stats
                .protocol
                .duplicate_identity_checks
                .saturating_add(1);
        }
        Ok(())
    }

    fn check_serial_monotonicity_oracle(&mut self, channel: &str) -> SimulatorResult<()> {
        let shadow = self.shadow.channel(channel);
        for pair in shadow.history.iter().collect::<Vec<_>>().windows(2) {
            if pair[0].serial.saturating_add(1) != pair[1].serial {
                return Err(self.fail(format!(
                    "history serial monotonicity failed on {channel}: {} then {}",
                    pair[0].serial, pair[1].serial
                )));
            }
            self.stats.protocol.serial_monotonicity_checks = self
                .stats
                .protocol
                .serial_monotonicity_checks
                .saturating_add(1);
        }
        for pair in shadow.presence_events.iter().collect::<Vec<_>>().windows(2) {
            if pair[0].serial.saturating_add(1) != pair[1].serial {
                return Err(self.fail(format!(
                    "presence serial monotonicity failed on {channel}: {} then {}",
                    pair[0].serial, pair[1].serial
                )));
            }
            self.stats.protocol.serial_monotonicity_checks = self
                .stats
                .protocol
                .serial_monotonicity_checks
                .saturating_add(1);
        }
        for pair in shadow
            .version_replay
            .values()
            .collect::<Vec<_>>()
            .windows(2)
        {
            if pair[0].delivery_serial().saturating_add(1) != pair[1].delivery_serial() {
                return Err(self.fail(format!(
                    "version delivery serial monotonicity failed on {channel}: {} then {}",
                    pair[0].delivery_serial(),
                    pair[1].delivery_serial()
                )));
            }
            self.stats.protocol.serial_monotonicity_checks = self
                .stats
                .protocol
                .serial_monotonicity_checks
                .saturating_add(1);
        }
        for (message_serial, chain) in &shadow.version_messages {
            for pair in chain.versions.windows(2) {
                let left = version_serial_ordinal(pair[0].version_serial().as_str());
                let right = version_serial_ordinal(pair[1].version_serial().as_str());
                if left >= right {
                    return Err(self.fail(format!(
                        "version serial monotonicity failed on {channel}/{message_serial}: {} then {}",
                        pair[0].version_serial().as_str(),
                        pair[1].version_serial().as_str()
                    )));
                }
                if pair[0].history_serial() != pair[1].history_serial() {
                    return Err(self.fail(format!(
                        "version chain history identity changed on {channel}/{message_serial}: {} then {}",
                        pair[0].history_serial(),
                        pair[1].history_serial()
                    )));
                }
                self.stats.protocol.serial_monotonicity_checks = self
                    .stats
                    .protocol
                    .serial_monotonicity_checks
                    .saturating_add(1);
            }
        }
        Ok(())
    }

    async fn check_attach_rewind_gaplessness_oracle(
        &mut self,
        channel: &str,
    ) -> SimulatorResult<()> {
        let shadow = self.shadow.channel(channel);
        let Some(newest) = shadow.history.back().map(|message| message.serial) else {
            return Ok(());
        };
        let rewind_count = self.config.page_limit.min(shadow.history.len()).max(1);
        let start_serial = newest.saturating_sub(rewind_count as u64).saturating_add(1);
        let expected = shadow
            .history
            .iter()
            .filter(|message| message.serial >= start_serial && message.serial <= newest)
            .map(|message| message.serial)
            .collect::<Vec<_>>();
        let page = self
            .real
            .history
            .read_page(HistoryReadRequest {
                app_id: APP_ID.to_string(),
                channel: channel.to_string(),
                direction: HistoryDirection::OldestFirst,
                limit: rewind_count,
                cursor: None,
                bounds: HistoryQueryBounds {
                    start_serial: Some(start_serial),
                    end_serial: Some(newest),
                    ..Default::default()
                },
            })
            .await?;
        let actual = page
            .items
            .iter()
            .map(|item| item.serial)
            .collect::<Vec<_>>();
        if page.truncated_by_retention {
            return Ok(());
        }
        if actual != expected {
            return Err(self.fail(format!(
                "attach/rewind gaplessness mismatch on {channel}: promised range {start_serial}..={newest}, expected {expected:?}, got {actual:?}"
            )));
        }
        for pair in actual.windows(2) {
            if pair[0].saturating_add(1) != pair[1] {
                return Err(self.fail(format!(
                    "attach/rewind gap on {channel}: {} then {}",
                    pair[0], pair[1]
                )));
            }
        }
        self.stats.protocol.attach_rewind_gap_checks = self
            .stats
            .protocol
            .attach_rewind_gap_checks
            .saturating_add(1);
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
                .real
                .history
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

    async fn read_history_bounded_for_recovery(
        &mut self,
        channel: &str,
        start_serial: u64,
    ) -> SimulatorResult<RecoveryRead> {
        let tick = self.tick();
        if self.storage.read_is_corrupted(
            &mut self.scheduler,
            tick,
            "history_recovery",
            self.config.fault.storage.corrupt_read_probability,
        ) {
            self.stats.storage_corrupted_reads =
                self.stats.storage_corrupted_reads.saturating_add(1);
            self.trace(format!(
                "storage corrupted recovery read on {channel} from serial {start_serial}"
            ));
            return Ok(RecoveryRead {
                corrupted: true,
                ..RecoveryRead::default()
            });
        }

        let mut visible_serial = self.storage_visibility.visible_history_serial(channel);
        if self.storage.read_is_stale(
            &mut self.scheduler,
            tick,
            "history_recovery",
            self.config.fault.storage.stale_read_probability,
        ) {
            self.stats.storage_stale_reads = self.stats.storage_stale_reads.saturating_add(1);
            if visible_serial >= start_serial {
                let rewind = self.scheduler.u64_inclusive(
                    visible_serial
                        .saturating_sub(start_serial)
                        .saturating_add(1),
                );
                visible_serial = visible_serial.saturating_sub(rewind);
            }
            self.trace(format!(
                "storage stale recovery read on {channel} visible_serial={visible_serial}"
            ));
        }
        if visible_serial < start_serial {
            return Ok(RecoveryRead {
                visible_serial,
                ..RecoveryRead::default()
            });
        }

        let mut items = Vec::new();
        let mut cursor: Option<HistoryCursor> = None;
        let bounds = HistoryQueryBounds {
            start_serial: Some(start_serial),
            end_serial: Some(visible_serial),
            ..Default::default()
        };
        loop {
            let page = self
                .real
                .history
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
                return Ok(RecoveryRead {
                    visible_serial,
                    truncated_by_retention: true,
                    ..RecoveryRead::default()
                });
            }
            items.extend(page.items);
            if !page.has_more {
                break;
            }
            let Some(next) = page.next_cursor else {
                return Err(self.fail(format!(
                    "history recovery page on {channel} had has_more=true without a cursor"
                )));
            };
            let encoded = next.encode()?;
            let decoded = HistoryCursor::decode(&encoded)?;
            if decoded != next {
                return Err(self.fail(format!(
                    "history recovery cursor round-trip mismatch on {channel}"
                )));
            }
            cursor = Some(next);
        }
        Ok(RecoveryRead {
            items,
            visible_serial,
            corrupted: false,
            truncated_by_retention: false,
        })
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
                .real
                .version
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
            let encoded = serde_json::to_string(&next)?;
            let decoded: VersionStoreCursor = serde_json::from_str(&encoded)?;
            if decoded != next {
                return Err(self.fail(format!(
                    "version cursor round-trip mismatch on {channel}/{message_serial}"
                )));
            }
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
                .real
                .presence
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
                self.scheduler.usize_below(self.config.clients)
            )),
            timestamp_ms: self.timestamp_ms(),
            description: None,
            metadata: None,
        })
    }

    fn next_presence_connection_id(&mut self, user_id: &str) -> String {
        let id = self.next_presence_connection;
        self.next_presence_connection = self.next_presence_connection.saturating_add(1);
        format!("socket-{user_id}-{id:020}")
    }

    fn random_channel(&mut self) -> String {
        self.channels[self.scheduler.usize_below(self.channels.len())].clone()
    }

    fn random_delay(&mut self) -> u64 {
        if self.config.fault.max_fanout_delay_ticks == 0 {
            0
        } else {
            self.scheduler
                .u64_inclusive(self.config.fault.max_fanout_delay_ticks)
        }
    }

    fn roll(&mut self, label: &str, probability: f64) -> bool {
        self.scheduler.roll(self.tick(), label, probability)
    }

    fn tick(&self) -> u64 {
        self.clock.tick()
    }

    fn timestamp_ms(&self) -> i64 {
        self.clock.timestamp_ms()
    }

    fn fail(&self, message: String) -> SimulatorError {
        let mut message = message;
        message.push_str("\nreplay command:");
        message.push_str("\n  ");
        message.push_str(&self.replay_command());
        if let Ok(config_json) = serde_json::to_string(&self.config) {
            message.push_str("\nconfig_json:");
            message.push_str("\n  ");
            message.push_str(&config_json);
        }
        let push_trace = self.push.recent_trace();
        let trace = self
            .trace
            .iter()
            .cloned()
            .chain(push_trace)
            .collect::<Vec<_>>();
        if !trace.is_empty() {
            message.push_str("\nrecent trace:");
            for event in trace.iter().rev().take(30).rev() {
                message.push_str("\n  ");
                message.push_str(event);
            }
        }
        SimulatorError::Invariant {
            seed: self.config.seed,
            tick: self.tick(),
            message,
        }
    }

    fn replay_command(&self) -> String {
        format!(
            "cargo run -p sockudo-simulator --bin sockudo-sim -- --seed {} --ticks {} --mode {}",
            self.config.seed,
            self.config.ticks,
            match self.config.mode {
                SimulatorMode::Safety => "safety",
                SimulatorMode::Liveness => "liveness",
            }
        )
    }

    fn trace(&mut self, event: String) {
        if self.trace.len() == MAX_TRACE_EVENTS {
            self.trace.pop_front();
        }
        self.trace
            .push_back(format!("tick={} {event}", self.tick()));
    }
}

#[derive(Debug, Clone, Default)]
struct StorageVisibility {
    history: BTreeMap<String, VisibilityWindow>,
    version: BTreeMap<String, VisibilityWindow>,
    presence: BTreeMap<String, VisibilityWindow>,
}

impl StorageVisibility {
    fn record_history(&mut self, channel: &str, serial: u64, visible_at: u64) {
        self.history
            .entry(channel.to_string())
            .or_default()
            .record(serial, visible_at);
    }

    fn record_version(&mut self, channel: &str, delivery_serial: u64, visible_at: u64) {
        self.version
            .entry(channel.to_string())
            .or_default()
            .record(delivery_serial, visible_at);
    }

    fn record_presence(&mut self, channel: &str, serial: u64, visible_at: u64) {
        self.presence
            .entry(channel.to_string())
            .or_default()
            .record(serial, visible_at);
    }

    fn reset_history(&mut self, channel: &str) {
        self.history.remove(channel);
    }

    fn reset_presence(&mut self, channel: &str) {
        self.presence.remove(channel);
    }

    fn visible_history_serial(&self, channel: &str) -> u64 {
        self.history
            .get(channel)
            .map_or(0, VisibilityWindow::visible_serial)
    }

    fn flush(&mut self, tick: u64) {
        for window in self
            .history
            .values_mut()
            .chain(self.version.values_mut())
            .chain(self.presence.values_mut())
        {
            window.flush(tick);
        }
    }

    fn next_due_tick(&self) -> Option<u64> {
        self.history
            .values()
            .chain(self.version.values())
            .chain(self.presence.values())
            .filter_map(VisibilityWindow::max_due_tick)
            .max()
    }

    fn force_visible(&mut self) {
        for window in self
            .history
            .values_mut()
            .chain(self.version.values_mut())
            .chain(self.presence.values_mut())
        {
            window.force_visible();
        }
    }
}

#[derive(Debug, Clone, Default)]
struct VisibilityWindow {
    visible_serial: u64,
    pending: BTreeMap<u64, u64>,
}

impl VisibilityWindow {
    fn record(&mut self, serial: u64, visible_at: u64) {
        if serial <= self.visible_serial {
            return;
        }
        self.pending.insert(serial, visible_at);
    }

    fn visible_serial(&self) -> u64 {
        self.visible_serial
    }

    fn flush(&mut self, tick: u64) {
        loop {
            let next_serial = self.visible_serial.saturating_add(1);
            let Some(&visible_at) = self.pending.get(&next_serial) else {
                break;
            };
            if visible_at > tick {
                break;
            }
            self.pending.remove(&next_serial);
            self.visible_serial = next_serial;
        }
    }

    fn max_due_tick(&self) -> Option<u64> {
        self.pending.values().copied().max()
    }

    fn force_visible(&mut self) {
        if let Some(last) = self.pending.keys().next_back().copied() {
            self.visible_serial = self.visible_serial.max(last);
        }
        self.pending.clear();
    }
}

#[derive(Debug, Clone)]
struct NodeState {
    id: usize,
    alive: bool,
    paused: bool,
    partitioned: bool,
    slow_until: u64,
    stale_until: u64,
    accepted_operations: u64,
    fanout_deliveries: u64,
}

impl NodeState {
    fn new(id: usize) -> Self {
        Self {
            id,
            alive: true,
            paused: false,
            partitioned: false,
            slow_until: 0,
            stale_until: 0,
            accepted_operations: 0,
            fanout_deliveries: 0,
        }
    }

    fn can_accept_traffic(&self, tick: u64) -> bool {
        self.alive && !self.paused && !self.partitioned && !self.is_stale(tick)
    }

    fn can_fanout(&self, tick: u64) -> bool {
        self.alive && !self.paused && !self.partitioned && !self.is_stale(tick)
    }

    fn is_slow(&self, tick: u64) -> bool {
        self.slow_until > tick
    }

    fn is_stale(&self, tick: u64) -> bool {
        self.stale_until > tick
    }
}

#[derive(Debug, Clone)]
struct NetworkEvent {
    deliver_at: u64,
    client_idx: usize,
    channel: String,
    stream_id: String,
    serial: u64,
    source_node: usize,
    message: ShadowHistoryMessage,
    source: DeliverySource,
}

impl ScheduledIoEvent for NetworkEvent {
    fn deliver_at(&self) -> u64 {
        self.deliver_at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliverySource {
    LiveFanout,
    Recovery,
}

#[derive(Debug, Default)]
struct RecoveryRead {
    items: Vec<HistoryItem>,
    visible_serial: u64,
    corrupted: bool,
    truncated_by_retention: bool,
}

#[derive(Debug, Clone)]
struct ClientState {
    id: usize,
    protocol: ProtocolVersion,
    channels: BTreeMap<String, ClientChannelState>,
}

impl ClientState {
    fn new(id: usize, protocol: ProtocolVersion, channels: &[String]) -> Self {
        Self {
            id,
            protocol,
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
        while state
            .delivered
            .contains(&state.last_contiguous.saturating_add(1))
        {
            state.last_contiguous = state.last_contiguous.saturating_add(1);
        }
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
    version_delivery_by_history_serial: BTreeMap<u64, u64>,
    presence_stream_id: Option<String>,
    presence_events: VecDeque<ShadowPresenceEvent>,
    presence_latest_event_by_user: BTreeMap<String, PresenceHistoryEventKind>,
    presence_members: BTreeMap<String, PresenceHistoryEventKind>,
    presence_active_connections: BTreeMap<String, BTreeSet<String>>,
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
            version_delivery_by_history_serial: BTreeMap::new(),
            presence_stream_id: None,
            presence_events: VecDeque::new(),
            presence_latest_event_by_user: BTreeMap::new(),
            presence_members: BTreeMap::new(),
            presence_active_connections: BTreeMap::new(),
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
        self.version_delivery_by_history_serial.clear();
    }

    fn purge_history_before(&mut self, serial: u64) {
        self.history.retain(|message| message.serial >= serial);
        self.version_delivery_by_history_serial
            .retain(|history_serial, _| *history_serial >= serial);
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

    fn link_version_history(&mut self, history_serial: u64, delivery_serial: Option<u64>) {
        let Some(delivery_serial) = delivery_serial else {
            return;
        };
        self.version_delivery_by_history_serial
            .insert(history_serial, delivery_serial);
        if let Some(message) = self
            .history
            .iter_mut()
            .find(|message| message.serial == history_serial)
        {
            message.version_delivery_serial = Some(delivery_serial);
        }
    }

    fn random_live_version(
        &self,
        scheduler: &mut DeterministicFaultScheduler,
        _tick: u64,
    ) -> Option<StoredVersionRecord> {
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
            Some(candidates[scheduler.usize_below(candidates.len())].clone())
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

    fn version_by_history_serial(&self, history_serial: u64) -> Option<&StoredVersionRecord> {
        self.version_delivery_by_history_serial
            .get(&history_serial)
            .and_then(|delivery_serial| self.version_replay.get(delivery_serial))
    }

    fn presence_connections(&self, user_id: &str) -> Vec<String> {
        self.presence_active_connections
            .get(user_id)
            .map(|connections| connections.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn append_presence(
        &mut self,
        item: PresenceHistoryItem,
        connections_before: usize,
        connections_after: usize,
    ) {
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
            connections_before,
            connections_after,
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
        self.presence_active_connections.clear();
    }

    fn apply_presence_connection(
        &mut self,
        user_id: &str,
        connection_id: &str,
        action: PresenceConnectionAction,
    ) {
        match action {
            PresenceConnectionAction::Join => {
                self.presence_active_connections
                    .entry(user_id.to_string())
                    .or_default()
                    .insert(connection_id.to_string());
            }
            PresenceConnectionAction::Leave => {
                if let Some(connections) = self.presence_active_connections.get_mut(user_id) {
                    connections.remove(connection_id);
                    if connections.is_empty() {
                        self.presence_active_connections.remove(user_id);
                    }
                }
            }
        }
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
    idempotency_key: Option<String>,
    version_delivery_serial: Option<u64>,
    event_name: Option<String>,
    operation_kind: String,
    payload: Bytes,
}

impl ShadowHistoryMessage {
    fn from_history_item(item: HistoryItem) -> Self {
        Self {
            stream_id: item.stream_id,
            serial: item.serial,
            published_at_ms: item.published_at_ms,
            message_id: item.message_id,
            idempotency_key: None,
            version_delivery_serial: None,
            event_name: item.event_name,
            operation_kind: item.operation_kind,
            payload: item.payload_bytes,
        }
    }
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

fn protocol_action_from_operation(operation: &str) -> Option<ProtocolMessageAction> {
    match operation {
        "message.create" => Some(ProtocolMessageAction::Create),
        "message.update" => Some(ProtocolMessageAction::Update),
        "message.delete" => Some(ProtocolMessageAction::Delete),
        "message.append" => Some(ProtocolMessageAction::Append),
        "message.summary" => Some(ProtocolMessageAction::Summary),
        _ => None,
    }
}

fn contiguous_prefix_from(delivered: &BTreeSet<u64>, baseline: u64) -> u64 {
    let mut prefix = baseline;
    while delivered.contains(&prefix.saturating_add(1)) {
        prefix = prefix.saturating_add(1);
    }
    prefix
}

fn version_serial_ordinal(serial: &str) -> u64 {
    serial
        .rsplit_once('-')
        .and_then(|(_, suffix)| suffix.parse::<u64>().ok())
        .unwrap_or(0)
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
    connections_before: usize,
    connections_after: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PresenceConnectionAction {
    Join,
    Leave,
}

#[derive(Debug, Default, Clone)]
struct Stats {
    operations: u64,
    rejected_operations: u64,
    oracle_checks: u64,
    protocol: ProtocolOracleStats,
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
    node_pauses: u64,
    node_resumes: u64,
    node_partitions: u64,
    node_heals: u64,
    node_slowdowns: u64,
    node_stale_marks: u64,
    stream_resets: u64,
    purges: u64,
    storage_dropped_writes: u64,
    storage_torn_writes: u64,
    storage_delayed_commits: u64,
    storage_stale_reads: u64,
    storage_corrupted_reads: u64,
    storage_recovery_checks: u64,
}

#[derive(Debug, Default, Clone)]
struct ProtocolOracleStats {
    v1_deliveries_checked: u64,
    v1_suppressed_deliveries: u64,
    v2_deliveries_checked: u64,
    v2_recovery_cursors_checked: u64,
    duplicate_identity_checks: u64,
    serial_monotonicity_checks: u64,
    attach_rewind_gap_checks: u64,
    presence_edge_checks: u64,
}

impl ProtocolOracleStats {
    fn report(&self) -> ProtocolOracleReport {
        ProtocolOracleReport {
            v1_deliveries_checked: self.v1_deliveries_checked,
            v1_suppressed_deliveries: self.v1_suppressed_deliveries,
            v2_deliveries_checked: self.v2_deliveries_checked,
            v2_recovery_cursors_checked: self.v2_recovery_cursors_checked,
            duplicate_identity_checks: self.duplicate_identity_checks,
            serial_monotonicity_checks: self.serial_monotonicity_checks,
            attach_rewind_gap_checks: self.attach_rewind_gap_checks,
            presence_edge_checks: self.presence_edge_checks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(seed: u64) -> SimulatorConfig {
        SimulatorConfig {
            seed,
            ticks: 300,
            mode: crate::SimulatorMode::Safety,
            liveness: crate::LivenessConfig::default(),
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
                node_pause_probability: 0.01,
                node_resume_probability: 0.05,
                node_partition_probability: 0.01,
                node_heal_probability: 0.05,
                node_slow_probability: 0.01,
                node_stale_probability: 0.01,
                stream_reset_probability: 0.001,
                storage: crate::StorageFaultConfig::default(),
            },
            push: crate::PushLabConfig::default(),
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
        assert_eq!(left_report.queued_fanout, 0);
    }

    #[tokio::test]
    async fn same_seed_replays_operation_fault_trace_and_report() {
        let mut config = test_config(0x105e_ed10);
        config.ticks = 32;
        config.workload.weights = ActionWeights {
            publish_message: 1,
            create_versioned_message: 0,
            mutate_versioned_message: 0,
            presence_transition: 0,
            recovery_probe: 0,
            purge_history: 0,
            push_register_device: 0,
            push_delete_device: 0,
            push_subscribe: 0,
            push_unsubscribe: 0,
            push_publish: 0,
            push_scheduled_publish: 0,
            push_provider_feedback: 0,
            push_repair: 0,
            oracle_check: 0,
        };
        config.fault.fanout_drop_probability = 1.0;
        config.fault.fanout_duplicate_probability = 0.0;
        config.fault.node_crash_probability = 0.0;
        config.fault.node_restart_probability = 0.0;
        config.fault.node_pause_probability = 0.0;
        config.fault.node_resume_probability = 0.0;
        config.fault.node_partition_probability = 0.0;
        config.fault.node_heal_probability = 0.0;
        config.fault.node_slow_probability = 0.0;
        config.fault.node_stale_probability = 0.0;
        config.fault.stream_reset_probability = 0.0;

        let mut left = DeterministicSimulator::new(config.clone()).unwrap();
        let mut right = DeterministicSimulator::new(config).unwrap();

        let left_report = left.run().await.unwrap();
        let right_report = right.run().await.unwrap();

        assert_eq!(left_report.io_trace, right_report.io_trace);
        assert_eq!(left_report.recent_trace, right_report.recent_trace);
        assert_eq!(left_report, right_report);
        assert!(
            left_report
                .io_trace
                .iter()
                .any(|event| event.contains("operation publish_message")),
            "operation trace should be replayable"
        );
        assert!(
            left_report
                .io_trace
                .iter()
                .any(|event| event.contains("fault fanout_drop")),
            "fault trace should be replayable"
        );
        assert!(left_report.history_commits > 0);
        assert!(left_report.dropped_fanout > 0);
        assert_eq!(left_report.queued_fanout, 0);
    }

    #[tokio::test]
    async fn same_seed_replays_across_real_push_boundary() {
        let mut config = test_config(0x5150_600d);
        config.ticks = 180;
        config.workload.weights = ActionWeights {
            publish_message: 0,
            create_versioned_message: 0,
            mutate_versioned_message: 0,
            presence_transition: 0,
            recovery_probe: 0,
            purge_history: 0,
            push_register_device: 24,
            push_delete_device: 4,
            push_subscribe: 20,
            push_unsubscribe: 5,
            push_publish: 32,
            push_scheduled_publish: 10,
            push_provider_feedback: 4,
            push_repair: 6,
            oracle_check: 3,
        };
        config.push.queue_produce_lost_probability = 0.04;
        config.push.queue_ack_lost_probability = 0.02;
        config.push.provider_retryable_probability = 0.18;
        config.push.provider_invalid_token_probability = 0.03;

        let mut left = DeterministicSimulator::new(config.clone()).unwrap();
        let mut right = DeterministicSimulator::new(config).unwrap();

        let left_report = left.run().await.unwrap();
        let right_report = right.run().await.unwrap();

        assert_eq!(left_report, right_report);
        assert!(left_report.push.accepted_publishes > 0);
        assert_eq!(
            left_report.push.durable_statuses,
            left_report.push.durable_publish_logs
        );
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

    #[tokio::test]
    async fn seed_corpus_small_medium_and_disaster_heavy_passes() {
        let corpus = [
            (12648430, 120, 0.05, 0.01),
            (3735928559, 240, 0.10, 0.03),
            (16045690984503098046, 360, 0.18, 0.06),
        ];

        for (seed, ticks, drop_probability, queue_loss) in corpus {
            let mut config = test_config(seed);
            config.ticks = ticks;
            config.fault.fanout_drop_probability = drop_probability;
            config.push.queue_produce_lost_probability = queue_loss;
            config.push.queue_ack_lost_probability = queue_loss / 2.0;
            config.push.provider_retryable_probability = 0.16;
            config.push.provider_lost_response_probability = 0.04;

            let report = DeterministicSimulator::new(config)
                .unwrap()
                .run()
                .await
                .unwrap();

            assert_eq!(report.queued_fanout, 0);
            assert_eq!(report.push.queued_items, 0);
            assert_eq!(report.push.pending_schedules, 0);
            assert_eq!(report.push.outstanding_deliveries, 0);
        }
    }

    #[tokio::test]
    async fn storage_fault_seed_exercises_durable_recovery_oracles() {
        let mut config = test_config(0x5702_a6e5_fa17_0001);
        config.ticks = 360;
        config.oracle_every = 7;
        config.fault.fanout_drop_probability = 0.35;
        config.fault.fanout_duplicate_probability = 0.04;
        config.fault.node_crash_probability = 0.035;
        config.fault.node_restart_probability = 0.180;
        config.fault.node_pause_probability = 0.0;
        config.fault.node_resume_probability = 0.0;
        config.fault.node_partition_probability = 0.0;
        config.fault.node_heal_probability = 0.0;
        config.fault.node_slow_probability = 0.0;
        config.fault.node_stale_probability = 0.0;
        config.fault.stream_reset_probability = 0.0;
        config.fault.storage = crate::StorageFaultConfig {
            dropped_write_probability: 0.060,
            torn_write_probability: 0.120,
            stale_read_probability: 0.220,
            corrupt_read_probability: 0.080,
            delayed_commit_probability: 0.300,
            max_commit_delay_ticks: 12,
        };
        config.workload.weights = ActionWeights {
            publish_message: 24,
            create_versioned_message: 18,
            mutate_versioned_message: 24,
            presence_transition: 18,
            recovery_probe: 18,
            purge_history: 0,
            push_register_device: 8,
            push_delete_device: 1,
            push_subscribe: 8,
            push_unsubscribe: 1,
            push_publish: 10,
            push_scheduled_publish: 2,
            push_provider_feedback: 2,
            push_repair: 3,
            oracle_check: 3,
        };

        let report = DeterministicSimulator::new(config)
            .unwrap()
            .run()
            .await
            .unwrap();

        assert!(report.storage_dropped_writes > 0);
        assert!(report.storage_torn_writes > 0);
        assert!(report.storage_delayed_commits > 0);
        assert!(report.storage_stale_reads > 0);
        assert!(report.storage_corrupted_reads > 0);
        assert!(report.storage_recovery_checks > 0);
        assert!(report.history_commits > 0);
        assert!(report.version_commits > 0);
        assert!(report.presence_events > 0);
        assert_eq!(report.queued_fanout, 0);
        assert_eq!(report.push.outstanding_deliveries, 0);
    }

    #[tokio::test]
    async fn liveness_mode_enforces_bounded_quiesce() {
        let mut config = test_config(0x11fe_11fe);
        config.mode = crate::SimulatorMode::Liveness;
        config.liveness.max_quiesce_ticks = 2_000;
        config.ticks = 250;

        let report = DeterministicSimulator::new(config)
            .unwrap()
            .run()
            .await
            .unwrap();

        assert_eq!(report.mode, crate::SimulatorMode::Liveness);
        assert!(report.quiesce_ticks <= report.liveness_max_quiesce_ticks);
        assert_eq!(report.queued_fanout, 0);
        assert_eq!(report.push.outstanding_deliveries, 0);
    }

    #[tokio::test]
    #[ignore = "nightly disaster burn-in profile"]
    async fn nightly_disaster_burn_in_seed_passes() {
        let mut config = test_config(0x05ee_dd15_a57e_57ab);
        config.ticks = 50_000;
        config.nodes = 9;
        config.clients = 24;
        config.channels = 12;
        config.fault.fanout_drop_probability = 0.14;
        config.fault.fanout_duplicate_probability = 0.08;
        config.fault.node_crash_probability = 0.006;
        config.fault.node_partition_probability = 0.008;
        config.push.queue_produce_lost_probability = 0.05;
        config.push.queue_ack_lost_probability = 0.03;
        config.push.queue_lease_timeout_probability = 0.025;
        config.push.write_fail_after_commit_probability = 0.015;
        config.push.provider_retryable_probability = 0.18;
        config.push.provider_permanent_rejection_probability = 0.07;
        config.push.provider_invalid_token_probability = 0.05;
        config.push.provider_lost_response_probability = 0.04;

        let report = DeterministicSimulator::new(config)
            .unwrap()
            .run()
            .await
            .unwrap();

        assert_eq!(report.queued_fanout, 0);
        assert_eq!(report.push.outstanding_deliveries, 0);
    }
}
