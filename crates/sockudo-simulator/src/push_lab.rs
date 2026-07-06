use rand::Rng;
use rand::rngs::StdRng;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::error::{SimulatorError, SimulatorResult};

const MAX_TRACE_EVENTS: usize = 80;
const DEFAULT_PUSH_DEVICES: usize = 32;
const DEFAULT_MAX_RETRIES: u32 = 4;
const DEFAULT_REPAIR_EVERY_TICKS: u64 = 17;

/// Configuration for the simulator's Sockudo-side push disaster model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushLabConfig {
    pub devices: usize,
    pub max_retries: u32,
    pub repair_every_ticks: u64,
    pub max_queue_delay_ticks: u64,
    pub max_provider_delay_ticks: u64,
    pub queue_produce_lost_probability: f64,
    pub queue_duplicate_probability: f64,
    pub queue_ack_lost_probability: f64,
    pub queue_lease_timeout_probability: f64,
    pub backend_outage_probability: f64,
    pub backend_recovery_probability: f64,
    pub write_fail_before_commit_probability: f64,
    pub write_fail_after_commit_probability: f64,
    pub response_lost_probability: f64,
    pub read_stale_probability: f64,
    pub provider_retryable_probability: f64,
    pub provider_permanent_rejection_probability: f64,
    pub provider_invalid_token_probability: f64,
    pub provider_lost_response_probability: f64,
    pub provider_delayed_result_probability: f64,
    pub provider_duplicate_result_probability: f64,
}

impl Default for PushLabConfig {
    fn default() -> Self {
        Self {
            devices: DEFAULT_PUSH_DEVICES,
            max_retries: DEFAULT_MAX_RETRIES,
            repair_every_ticks: DEFAULT_REPAIR_EVERY_TICKS,
            max_queue_delay_ticks: 9,
            max_provider_delay_ticks: 11,
            queue_produce_lost_probability: 0.015,
            queue_duplicate_probability: 0.020,
            queue_ack_lost_probability: 0.010,
            queue_lease_timeout_probability: 0.010,
            backend_outage_probability: 0.002,
            backend_recovery_probability: 0.080,
            write_fail_before_commit_probability: 0.006,
            write_fail_after_commit_probability: 0.004,
            response_lost_probability: 0.006,
            read_stale_probability: 0.010,
            provider_retryable_probability: 0.100,
            provider_permanent_rejection_probability: 0.040,
            provider_invalid_token_probability: 0.025,
            provider_lost_response_probability: 0.020,
            provider_delayed_result_probability: 0.060,
            provider_duplicate_result_probability: 0.030,
        }
    }
}

impl PushLabConfig {
    pub(crate) fn validate(&self) -> SimulatorResult<()> {
        if self.devices == 0 {
            return Err(SimulatorError::Config(
                "push devices must be greater than 0".into(),
            ));
        }
        if self.max_retries == 0 {
            return Err(SimulatorError::Config(
                "push max_retries must be greater than 0".into(),
            ));
        }
        for (name, value) in [
            (
                "queue_produce_lost_probability",
                self.queue_produce_lost_probability,
            ),
            (
                "queue_duplicate_probability",
                self.queue_duplicate_probability,
            ),
            (
                "queue_ack_lost_probability",
                self.queue_ack_lost_probability,
            ),
            (
                "queue_lease_timeout_probability",
                self.queue_lease_timeout_probability,
            ),
            (
                "backend_outage_probability",
                self.backend_outage_probability,
            ),
            (
                "backend_recovery_probability",
                self.backend_recovery_probability,
            ),
            (
                "write_fail_before_commit_probability",
                self.write_fail_before_commit_probability,
            ),
            (
                "write_fail_after_commit_probability",
                self.write_fail_after_commit_probability,
            ),
            ("response_lost_probability", self.response_lost_probability),
            ("read_stale_probability", self.read_stale_probability),
            (
                "provider_retryable_probability",
                self.provider_retryable_probability,
            ),
            (
                "provider_permanent_rejection_probability",
                self.provider_permanent_rejection_probability,
            ),
            (
                "provider_invalid_token_probability",
                self.provider_invalid_token_probability,
            ),
            (
                "provider_lost_response_probability",
                self.provider_lost_response_probability,
            ),
            (
                "provider_delayed_result_probability",
                self.provider_delayed_result_probability,
            ),
            (
                "provider_duplicate_result_probability",
                self.provider_duplicate_result_probability,
            ),
        ] {
            if !(0.0..=1.0).contains(&value) {
                return Err(SimulatorError::Config(format!(
                    "{name} must be between 0.0 and 1.0"
                )));
            }
        }
        Ok(())
    }
}

/// Push-specific report embedded in the simulator JSON output.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct PushSimulationReport {
    pub registered_devices: u64,
    pub active_devices: usize,
    pub subscriptions: usize,
    pub accepted_publishes: u64,
    pub duplicate_publishes: u64,
    pub scheduled_publishes: u64,
    pub durable_statuses: usize,
    pub durable_publish_logs: usize,
    pub planned_deliveries: u64,
    pub provider_sends: u64,
    pub delivery_results: u64,
    pub accepted_results: u64,
    pub retryable_results: u64,
    pub rejected_results: u64,
    pub expired_results: u64,
    pub cancelled_results: u64,
    pub retries_scheduled: u64,
    pub retries_attempted: u64,
    pub dead_letters: u64,
    pub invalid_tokens: u64,
    pub queue_produce_lost: u64,
    pub queue_duplicates: u64,
    pub queue_ack_lost: u64,
    pub queue_lease_timeouts: u64,
    pub repair_scans: u64,
    pub repair_requeued: u64,
    pub backend_outages: u64,
    pub backend_recoveries: u64,
    pub write_fail_before_commit: u64,
    pub write_fail_after_commit: u64,
    pub response_lost_after_commit: u64,
    pub stale_reads: u64,
    pub queued_items: usize,
    pub pending_schedules: usize,
    pub outstanding_deliveries: usize,
    pub recent_trace: Vec<String>,
}

/// Deterministic model of Sockudo-side push durability and provider fallibility.
#[derive(Debug, Clone)]
pub struct PushLab {
    config: PushLabConfig,
    clients: usize,
    channels: Vec<String>,
    devices: BTreeMap<String, SimDevice>,
    publishes: BTreeMap<String, PushPublish>,
    idempotency: BTreeMap<String, String>,
    queue: VecDeque<QueueItem>,
    schedules: Vec<ScheduledPush>,
    backend_outage: bool,
    quiescing: bool,
    next_device_id: u64,
    next_publish_id: u64,
    next_queue_id: u64,
    stats: PushStats,
    trace: VecDeque<String>,
}

impl PushLab {
    pub(crate) fn new(
        channels: &[String],
        clients: usize,
        config: PushLabConfig,
    ) -> SimulatorResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            clients,
            channels: channels.to_vec(),
            devices: BTreeMap::new(),
            publishes: BTreeMap::new(),
            idempotency: BTreeMap::new(),
            queue: VecDeque::new(),
            schedules: Vec::new(),
            backend_outage: false,
            quiescing: false,
            next_device_id: 1,
            next_publish_id: 1,
            next_queue_id: 1,
            stats: PushStats::default(),
            trace: VecDeque::new(),
        })
    }

    pub(crate) fn on_tick(&mut self, tick: u64, rng: &mut StdRng) {
        self.maybe_toggle_backend(tick, rng);
        self.release_due_schedules(tick, rng);
        self.process_queue(tick, rng);
        if self.config.repair_every_ticks > 0 && tick.is_multiple_of(self.config.repair_every_ticks)
        {
            self.repair(tick, rng);
        }
    }

    pub(crate) fn register_or_update_device(&mut self, tick: u64, rng: &mut StdRng) {
        if self.backend_outage || self.write_fails_before_commit(tick, rng, "device_upsert") {
            return;
        }
        let provider = PushProvider::random(rng);
        let device_id = if self.devices.len() < self.config.devices {
            let id = format!("device-{:020}", self.next_device_id);
            self.next_device_id = self.next_device_id.saturating_add(1);
            id
        } else {
            self.random_device_id(rng)
                .unwrap_or_else(|| "device-00000000000000000001".to_string())
        };
        let client_id = format!("client-{}", rng.random_range(0..self.clients));
        if !self.devices.contains_key(&device_id) {
            self.stats.registered_devices = self.stats.registered_devices.saturating_add(1);
            self.devices.insert(
                device_id.clone(),
                SimDevice {
                    id: device_id.clone(),
                    client_id: client_id.clone(),
                    provider,
                    state: DeviceState::Active,
                    subscriptions: BTreeSet::new(),
                    visible_removed_at: None,
                },
            );
        }
        if let Some(device) = self.devices.get_mut(&device_id) {
            device.client_id = client_id;
            device.provider = provider;
            device.state = DeviceState::Active;
            device.visible_removed_at = None;
        }
        self.after_commit_fault(tick, rng, "device_upsert");
        self.trace(tick, format!("push device upserted {device_id}"));
    }

    pub(crate) fn delete_device(&mut self, tick: u64, rng: &mut StdRng) {
        let Some(device_id) = self.random_device_id(rng) else {
            self.register_or_update_device(tick, rng);
            return;
        };
        if self.backend_outage || self.write_fails_before_commit(tick, rng, "device_delete") {
            return;
        }
        if let Some(device) = self.devices.get_mut(&device_id) {
            device.state = DeviceState::Deleted;
            device.subscriptions.clear();
            device.visible_removed_at = Some(tick);
            self.after_commit_fault(tick, rng, "device_delete");
            self.trace(tick, format!("push device deleted {device_id}"));
        }
    }

    pub(crate) fn subscribe_device(&mut self, tick: u64, rng: &mut StdRng) {
        let device_id = self.ensure_active_device(tick, rng);
        let channel = self.random_channel(rng);
        if self.backend_outage || self.write_fails_before_commit(tick, rng, "subscribe") {
            return;
        }
        if let Some(device) = self.devices.get_mut(&device_id) {
            device.state = DeviceState::Active;
            device.visible_removed_at = None;
            device.subscriptions.insert(channel.clone());
            self.after_commit_fault(tick, rng, "subscribe");
            self.trace(tick, format!("push subscribed {device_id} to {channel}"));
        }
    }

    pub(crate) fn unsubscribe_device(&mut self, tick: u64, rng: &mut StdRng) {
        let Some(device_id) = self.random_device_id(rng) else {
            return;
        };
        let channel = self.random_channel(rng);
        if self.backend_outage || self.write_fails_before_commit(tick, rng, "unsubscribe") {
            return;
        }
        if let Some(device) = self.devices.get_mut(&device_id) {
            device.subscriptions.remove(&channel);
            if device.subscriptions.is_empty() {
                device.visible_removed_at = Some(tick);
            }
            self.after_commit_fault(tick, rng, "unsubscribe");
            self.trace(
                tick,
                format!("push unsubscribed {device_id} from {channel}"),
            );
        }
    }

    pub(crate) fn publish_now(&mut self, tick: u64, rng: &mut StdRng) {
        let channel = self.random_channel(rng);
        self.ensure_subscriber(tick, rng, &channel);
        self.accept_publish(tick, rng, channel, None);
    }

    pub(crate) fn schedule_publish(&mut self, tick: u64, rng: &mut StdRng) {
        let channel = self.random_channel(rng);
        self.ensure_subscriber(tick, rng, &channel);
        let delay = self.random_queue_delay(rng).saturating_add(1);
        let publish_id = self.next_publish_key();
        let idempotency_key = format!("schedule:{publish_id}");
        self.schedules.push(ScheduledPush {
            due_tick: tick.saturating_add(delay),
            channel: channel.clone(),
            publish_id,
            idempotency_key,
        });
        self.stats.scheduled_publishes = self.stats.scheduled_publishes.saturating_add(1);
        self.trace(
            tick,
            format!(
                "push scheduled publish for {channel} due at {}",
                tick + delay
            ),
        );
    }

    pub(crate) fn duplicate_provider_feedback(&mut self, tick: u64, rng: &mut StdRng) {
        let candidates = self
            .publishes
            .values()
            .flat_map(|publish| {
                publish
                    .deliveries
                    .values()
                    .filter(|delivery| delivery.last_result.is_some())
                    .map(|delivery| {
                        (
                            publish.id.clone(),
                            delivery.id.clone(),
                            delivery.last_result.unwrap_or(ProviderResult::Accepted),
                        )
                    })
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return;
        }
        let (publish_id, delivery_id, result) =
            candidates[rng.random_range(0..candidates.len())].clone();
        let item = QueueItem {
            id: self.next_queue_id(),
            deliver_at: tick.saturating_add(self.random_provider_delay(rng)),
            stage: QueueStage::ProviderResult {
                publish_id,
                delivery_id,
                result,
            },
        };
        self.queue.push_back(item);
        self.trace(tick, "push duplicate provider feedback queued".to_string());
    }

    pub(crate) fn repair_now(&mut self, tick: u64, rng: &mut StdRng) {
        self.repair(tick, rng);
    }

    pub(crate) fn quiesce(&mut self, mut tick: u64, rng: &mut StdRng) -> u64 {
        let previous_quiescing = self.quiescing;
        self.quiescing = true;
        let max_steps = self
            .queue
            .len()
            .saturating_add(self.schedules.len())
            .saturating_add(self.outstanding_deliveries())
            .saturating_mul(self.config.max_retries as usize + 1)
            .saturating_add(10_000);
        for _ in 0..max_steps {
            self.prune_obsolete_queue();
            if self.queue.is_empty()
                && self.schedules.is_empty()
                && self.outstanding_deliveries() == 0
            {
                self.quiescing = previous_quiescing;
                return tick;
            }
            tick = tick.saturating_add(1);
            self.backend_outage = false;
            self.release_due_schedules(tick, rng);
            self.repair(tick, rng);
            self.process_queue(tick, rng);
            self.prune_obsolete_queue();
        }
        self.quiescing = previous_quiescing;
        tick
    }

    pub(crate) fn check_oracles(&self, final_quiesce: bool) -> Result<(), String> {
        for (key, publish_id) in &self.idempotency {
            if !self.publishes.contains_key(publish_id) {
                return Err(format!(
                    "push idempotency key {key} points to missing publish {publish_id}"
                ));
            }
        }

        for publish in self.publishes.values() {
            if !publish.durable_status {
                return Err(format!(
                    "accepted push publish {} is missing durable status",
                    publish.id
                ));
            }
            if !publish.durable_log {
                return Err(format!(
                    "accepted push publish {} is missing durable publish log",
                    publish.id
                ));
            }
            let terminal = publish.terminal_deliveries();
            if terminal > publish.planned {
                return Err(format!(
                    "push publish {} terminal deliveries exceed planned count: terminal={} planned={}",
                    publish.id, terminal, publish.planned
                ));
            }
            if publish.counters.total() > publish.planned {
                return Err(format!(
                    "push publish {} counters exceed planned count: counters={:?} planned={}",
                    publish.id, publish.counters, publish.planned
                ));
            }
            if publish.state == PublishState::Terminal && terminal != publish.planned {
                return Err(format!(
                    "push publish {} is terminal with incomplete deliveries: terminal={} planned={}",
                    publish.id, terminal, publish.planned
                ));
            }
            for delivery in publish.deliveries.values() {
                if delivery.provider_sent_after_visible_removal {
                    return Err(format!(
                        "push delivery {} for publish {} was sent after device became unsubscribed/deleted/invalid",
                        delivery.id, publish.id
                    ));
                }
                if matches!(delivery.state, DeliveryState::RetryScheduled)
                    && delivery.attempts > self.config.max_retries
                {
                    return Err(format!(
                        "push delivery {} for publish {} scheduled retry beyond max attempts",
                        delivery.id, publish.id
                    ));
                }
            }
            if final_quiesce && publish.outstanding_deliveries() > 0 {
                return Err(format!(
                    "push publish {} still has {} outstanding deliveries after quiesce",
                    publish.id,
                    publish.outstanding_deliveries()
                ));
            }
        }

        if final_quiesce && !self.queue.is_empty() {
            return Err(format!(
                "push queues did not drain at quiesce: {} item(s) remain",
                self.queue.len()
            ));
        }
        if final_quiesce && !self.schedules.is_empty() {
            return Err(format!(
                "push schedules did not drain at quiesce: {} item(s) remain",
                self.schedules.len()
            ));
        }
        Ok(())
    }

    pub(crate) fn report(&self) -> PushSimulationReport {
        PushSimulationReport {
            registered_devices: self.stats.registered_devices,
            active_devices: self
                .devices
                .values()
                .filter(|device| device.state == DeviceState::Active)
                .count(),
            subscriptions: self
                .devices
                .values()
                .map(|device| device.subscriptions.len())
                .sum(),
            accepted_publishes: self.stats.accepted_publishes,
            duplicate_publishes: self.stats.duplicate_publishes,
            scheduled_publishes: self.stats.scheduled_publishes,
            durable_statuses: self
                .publishes
                .values()
                .filter(|publish| publish.durable_status)
                .count(),
            durable_publish_logs: self
                .publishes
                .values()
                .filter(|publish| publish.durable_log)
                .count(),
            planned_deliveries: self.publishes.values().map(|publish| publish.planned).sum(),
            provider_sends: self.stats.provider_sends,
            delivery_results: self.stats.delivery_results,
            accepted_results: self.stats.accepted_results,
            retryable_results: self.stats.retryable_results,
            rejected_results: self.stats.rejected_results,
            expired_results: self.stats.expired_results,
            cancelled_results: self.stats.cancelled_results,
            retries_scheduled: self.stats.retries_scheduled,
            retries_attempted: self.stats.retries_attempted,
            dead_letters: self.stats.dead_letters,
            invalid_tokens: self.stats.invalid_tokens,
            queue_produce_lost: self.stats.queue_produce_lost,
            queue_duplicates: self.stats.queue_duplicates,
            queue_ack_lost: self.stats.queue_ack_lost,
            queue_lease_timeouts: self.stats.queue_lease_timeouts,
            repair_scans: self.stats.repair_scans,
            repair_requeued: self.stats.repair_requeued,
            backend_outages: self.stats.backend_outages,
            backend_recoveries: self.stats.backend_recoveries,
            write_fail_before_commit: self.stats.write_fail_before_commit,
            write_fail_after_commit: self.stats.write_fail_after_commit,
            response_lost_after_commit: self.stats.response_lost_after_commit,
            stale_reads: self.stats.stale_reads,
            queued_items: self.queue.len(),
            pending_schedules: self.schedules.len(),
            outstanding_deliveries: self.outstanding_deliveries(),
            recent_trace: self.trace.iter().cloned().collect(),
        }
    }

    pub(crate) fn recent_trace(&self) -> Vec<String> {
        self.trace.iter().cloned().collect()
    }

    fn accept_publish(
        &mut self,
        tick: u64,
        rng: &mut StdRng,
        channel: String,
        requested: Option<(String, String)>,
    ) {
        if self.backend_outage || self.write_fails_before_commit(tick, rng, "push_publish") {
            return;
        }
        let (publish_id, idempotency_key) = requested.unwrap_or_else(|| {
            let publish_id = if !self.idempotency.is_empty() && rng.random_ratio(1, 8) {
                self.idempotency
                    .values()
                    .next()
                    .cloned()
                    .unwrap_or_else(|| self.next_publish_key())
            } else {
                self.next_publish_key()
            };
            let idempotency_key = format!("push-idem:{publish_id}");
            (publish_id, idempotency_key)
        });
        if let Some(existing) = self.idempotency.get(&idempotency_key) {
            if existing == &publish_id {
                self.stats.duplicate_publishes = self.stats.duplicate_publishes.saturating_add(1);
                self.trace(
                    tick,
                    format!("push duplicate idempotency {idempotency_key} -> {publish_id}"),
                );
            }
            return;
        }

        let planned_targets = self.eligible_targets(&channel);
        let planned = planned_targets.len() as u64;
        let deliveries = planned_targets
            .into_iter()
            .enumerate()
            .map(|(idx, target)| {
                let id = format!("{publish_id}:delivery-{idx:06}");
                (
                    id.clone(),
                    Delivery {
                        id,
                        device_id: target.device_id,
                        provider: target.provider,
                        state: DeliveryState::Pending,
                        attempts: 0,
                        terminal: None,
                        last_result: None,
                        result_keys: BTreeSet::new(),
                        provider_sent_after_visible_removal: false,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        self.publishes.insert(
            publish_id.clone(),
            PushPublish {
                id: publish_id.clone(),
                channel: channel.clone(),
                idempotency_key: idempotency_key.clone(),
                durable_status: true,
                durable_log: true,
                state: PublishState::Queued,
                planned,
                counters: PushCounters::default(),
                deliveries,
            },
        );
        self.idempotency.insert(idempotency_key, publish_id.clone());
        self.stats.accepted_publishes = self.stats.accepted_publishes.saturating_add(1);
        self.after_commit_fault(tick, rng, "push_publish");
        self.enqueue(
            tick,
            rng,
            QueueStage::PublishLog {
                publish_id: publish_id.clone(),
            },
        );
        self.trace(
            tick,
            format!("push publish accepted {publish_id} channel={channel} planned={planned}"),
        );
    }

    fn release_due_schedules(&mut self, tick: u64, rng: &mut StdRng) {
        let mut pending = Vec::with_capacity(self.schedules.len());
        let schedules = std::mem::take(&mut self.schedules);
        for schedule in schedules {
            if schedule.due_tick <= tick {
                self.accept_publish(
                    tick,
                    rng,
                    schedule.channel,
                    Some((schedule.publish_id, schedule.idempotency_key)),
                );
            } else {
                pending.push(schedule);
            }
        }
        self.schedules = pending;
    }

    fn process_queue(&mut self, tick: u64, rng: &mut StdRng) {
        if self.backend_outage {
            return;
        }

        let mut pending = VecDeque::with_capacity(self.queue.len());
        let queue = std::mem::take(&mut self.queue);
        for item in queue {
            if item.deliver_at > tick {
                pending.push_back(item);
                continue;
            }
            if !self.quiescing && self.roll(self.config.queue_lease_timeout_probability, rng) {
                let mut duplicate = item.clone();
                duplicate.id = self.next_queue_id();
                duplicate.deliver_at = tick.saturating_add(self.random_queue_delay(rng).max(1));
                pending.push_back(duplicate);
                self.stats.queue_lease_timeouts = self.stats.queue_lease_timeouts.saturating_add(1);
            }
            self.apply_queue_item(tick, rng, item.clone());
            if !self.quiescing && self.roll(self.config.queue_ack_lost_probability, rng) {
                let mut redelivery = item;
                redelivery.id = self.next_queue_id();
                redelivery.deliver_at = tick.saturating_add(self.random_queue_delay(rng).max(1));
                pending.push_back(redelivery);
                self.stats.queue_ack_lost = self.stats.queue_ack_lost.saturating_add(1);
            }
        }
        pending.append(&mut self.queue);
        self.queue = pending;
    }

    fn apply_queue_item(&mut self, tick: u64, rng: &mut StdRng, item: QueueItem) {
        match item.stage {
            QueueStage::PublishLog { publish_id } => {
                let Some(delivery_ids) = self.publishes.get(&publish_id).map(|publish| {
                    publish
                        .deliveries
                        .values()
                        .filter(|delivery| matches!(delivery.state, DeliveryState::Pending))
                        .map(|delivery| delivery.id.clone())
                        .collect::<Vec<_>>()
                }) else {
                    return;
                };
                for delivery_id in delivery_ids {
                    self.enqueue(
                        tick,
                        rng,
                        QueueStage::Delivery {
                            publish_id: publish_id.clone(),
                            delivery_id,
                        },
                    );
                }
                if let Some(publish) = self.publishes.get_mut(&publish_id) {
                    publish.state = if publish.planned == 0 {
                        PublishState::Terminal
                    } else {
                        PublishState::Running
                    };
                }
            }
            QueueStage::Delivery {
                publish_id,
                delivery_id,
            } => self.dispatch_delivery(tick, rng, &publish_id, &delivery_id),
            QueueStage::ProviderResult {
                publish_id,
                delivery_id,
                result,
            } => self.apply_provider_result(tick, rng, &publish_id, &delivery_id, result),
        }
    }

    fn dispatch_delivery(
        &mut self,
        tick: u64,
        rng: &mut StdRng,
        publish_id: &str,
        delivery_id: &str,
    ) {
        let Some((device_id, provider, attempts, terminal)) = self
            .publishes
            .get(publish_id)
            .and_then(|publish| publish.deliveries.get(delivery_id))
            .map(|delivery| {
                (
                    delivery.device_id.clone(),
                    delivery.provider,
                    delivery.attempts,
                    delivery.terminal,
                )
            })
        else {
            return;
        };
        if terminal.is_some() {
            return;
        }
        if !self.device_is_eligible_for_publish(publish_id, &device_id, rng) {
            self.apply_terminal(publish_id, delivery_id, ProviderResult::Cancelled, tick);
            return;
        }
        if attempts >= self.config.max_retries {
            self.dead_letter(publish_id, delivery_id, tick);
            return;
        }

        if let Some(publish) = self.publishes.get_mut(publish_id)
            && let Some(delivery) = publish.deliveries.get_mut(delivery_id)
        {
            delivery.attempts = delivery.attempts.saturating_add(1);
            if delivery.attempts > 1 {
                self.stats.retries_attempted = self.stats.retries_attempted.saturating_add(1);
            }
        }
        self.stats.provider_sends = self.stats.provider_sends.saturating_add(1);

        let result = self.provider_result(provider, rng);
        if result == ProviderResult::LostResponseAfterAccepted {
            self.apply_provider_result(
                tick,
                rng,
                publish_id,
                delivery_id,
                ProviderResult::Retryable,
            );
            return;
        }
        if self.roll(self.config.provider_delayed_result_probability, rng) {
            self.enqueue(
                tick.saturating_add(self.random_provider_delay(rng).max(1)),
                rng,
                QueueStage::ProviderResult {
                    publish_id: publish_id.to_string(),
                    delivery_id: delivery_id.to_string(),
                    result,
                },
            );
        } else {
            self.apply_provider_result(tick, rng, publish_id, delivery_id, result);
        }
        if self.roll(self.config.provider_duplicate_result_probability, rng) {
            self.enqueue(
                tick.saturating_add(self.random_provider_delay(rng).max(1)),
                rng,
                QueueStage::ProviderResult {
                    publish_id: publish_id.to_string(),
                    delivery_id: delivery_id.to_string(),
                    result,
                },
            );
        }
    }

    fn apply_provider_result(
        &mut self,
        tick: u64,
        rng: &mut StdRng,
        publish_id: &str,
        delivery_id: &str,
        result: ProviderResult,
    ) {
        let Some((attempt, duplicate)) = self
            .publishes
            .get_mut(publish_id)
            .and_then(|publish| publish.deliveries.get_mut(delivery_id))
            .map(|delivery| {
                let key = format!("{delivery_id}:{}:{result:?}", delivery.attempts);
                let duplicate = !delivery.result_keys.insert(key);
                (delivery.attempts, duplicate)
            })
        else {
            return;
        };
        if duplicate {
            return;
        }

        match result {
            ProviderResult::Accepted
            | ProviderResult::Rejected
            | ProviderResult::Expired
            | ProviderResult::Cancelled => {
                self.apply_terminal(publish_id, delivery_id, result, tick);
            }
            ProviderResult::InvalidToken => {
                let device_id = self
                    .publishes
                    .get(publish_id)
                    .and_then(|publish| publish.deliveries.get(delivery_id))
                    .map(|delivery| delivery.device_id.clone());
                if let Some(device_id) = device_id
                    && let Some(device) = self.devices.get_mut(&device_id)
                {
                    device.state = DeviceState::Invalid;
                    device.subscriptions.clear();
                    device.visible_removed_at = Some(tick);
                    self.stats.invalid_tokens = self.stats.invalid_tokens.saturating_add(1);
                }
                self.apply_terminal(publish_id, delivery_id, ProviderResult::Expired, tick);
            }
            ProviderResult::Retryable | ProviderResult::LostResponseAfterAccepted => {
                self.stats.retryable_results = self.stats.retryable_results.saturating_add(1);
                if attempt >= self.config.max_retries {
                    self.dead_letter(publish_id, delivery_id, tick);
                    return;
                }
                if let Some(publish) = self.publishes.get_mut(publish_id)
                    && let Some(delivery) = publish.deliveries.get_mut(delivery_id)
                {
                    delivery.state = DeliveryState::RetryScheduled;
                    delivery.last_result = Some(ProviderResult::Retryable);
                }
                self.stats.retries_scheduled = self.stats.retries_scheduled.saturating_add(1);
                self.enqueue(
                    tick.saturating_add(self.random_queue_delay(rng).max(1)),
                    rng,
                    QueueStage::Delivery {
                        publish_id: publish_id.to_string(),
                        delivery_id: delivery_id.to_string(),
                    },
                );
            }
        }
    }

    fn apply_terminal(
        &mut self,
        publish_id: &str,
        delivery_id: &str,
        result: ProviderResult,
        tick: u64,
    ) {
        let Some(publish) = self.publishes.get_mut(publish_id) else {
            return;
        };
        let Some(delivery) = publish.deliveries.get_mut(delivery_id) else {
            return;
        };
        if delivery.terminal.is_some() {
            return;
        }
        delivery.state = DeliveryState::Terminal;
        delivery.terminal = Some(result);
        delivery.last_result = Some(result);
        publish.counters.record(result);
        publish.refresh_state();
        self.stats.delivery_results = self.stats.delivery_results.saturating_add(1);
        match result {
            ProviderResult::Accepted => {
                self.stats.accepted_results = self.stats.accepted_results.saturating_add(1);
            }
            ProviderResult::Rejected => {
                self.stats.rejected_results = self.stats.rejected_results.saturating_add(1);
            }
            ProviderResult::Expired | ProviderResult::InvalidToken => {
                self.stats.expired_results = self.stats.expired_results.saturating_add(1);
            }
            ProviderResult::Cancelled => {
                self.stats.cancelled_results = self.stats.cancelled_results.saturating_add(1);
            }
            ProviderResult::Retryable | ProviderResult::LostResponseAfterAccepted => {}
        }
        self.trace(
            tick,
            format!("push delivery terminal {publish_id}/{delivery_id} -> {result:?}"),
        );
    }

    fn dead_letter(&mut self, publish_id: &str, delivery_id: &str, tick: u64) {
        let Some(publish) = self.publishes.get_mut(publish_id) else {
            return;
        };
        let Some(delivery) = publish.deliveries.get_mut(delivery_id) else {
            return;
        };
        if delivery.terminal.is_some() {
            return;
        }
        delivery.state = DeliveryState::Terminal;
        delivery.terminal = Some(ProviderResult::Rejected);
        delivery.last_result = Some(ProviderResult::Rejected);
        publish.counters.dead_lettered = publish.counters.dead_lettered.saturating_add(1);
        publish.refresh_state();
        self.stats.delivery_results = self.stats.delivery_results.saturating_add(1);
        self.stats.dead_letters = self.stats.dead_letters.saturating_add(1);
        self.trace(
            tick,
            format!("push delivery dead-lettered {publish_id}/{delivery_id}"),
        );
    }

    fn repair(&mut self, tick: u64, rng: &mut StdRng) {
        if self.backend_outage {
            return;
        }
        let publish_ids = self.publishes.keys().cloned().collect::<Vec<_>>();
        for publish_id in publish_ids {
            self.stats.repair_scans = self.stats.repair_scans.saturating_add(1);
            let Some(publish) = self.publishes.get(&publish_id) else {
                continue;
            };
            if publish.state == PublishState::Queued
                && !self.queue_contains_publish_log(&publish_id)
            {
                self.enqueue(
                    tick,
                    rng,
                    QueueStage::PublishLog {
                        publish_id: publish_id.clone(),
                    },
                );
                self.stats.repair_requeued = self.stats.repair_requeued.saturating_add(1);
                continue;
            }
            let missing = publish
                .deliveries
                .values()
                .filter(|delivery| {
                    matches!(
                        delivery.state,
                        DeliveryState::Pending | DeliveryState::RetryScheduled
                    ) && !self.queue_contains_delivery(&publish_id, &delivery.id)
                })
                .map(|delivery| delivery.id.clone())
                .collect::<Vec<_>>();
            for delivery_id in missing {
                self.enqueue(
                    tick,
                    rng,
                    QueueStage::Delivery {
                        publish_id: publish_id.clone(),
                        delivery_id,
                    },
                );
                self.stats.repair_requeued = self.stats.repair_requeued.saturating_add(1);
            }
        }
    }

    fn enqueue(&mut self, tick: u64, rng: &mut StdRng, stage: QueueStage) {
        if !self.quiescing && self.roll(self.config.queue_produce_lost_probability, rng) {
            self.stats.queue_produce_lost = self.stats.queue_produce_lost.saturating_add(1);
            self.trace(tick, format!("push queue produce lost for {stage:?}"));
            return;
        }
        let item = QueueItem {
            id: self.next_queue_id(),
            deliver_at: tick.saturating_add(self.random_queue_delay(rng)),
            stage,
        };
        self.queue.push_back(item.clone());
        if !self.quiescing && self.roll(self.config.queue_duplicate_probability, rng) {
            let mut duplicate = item;
            duplicate.id = self.next_queue_id();
            duplicate.deliver_at = duplicate
                .deliver_at
                .saturating_add(self.random_queue_delay(rng).max(1));
            self.queue.push_back(duplicate);
            self.stats.queue_duplicates = self.stats.queue_duplicates.saturating_add(1);
        }
    }

    fn eligible_targets(&self, channel: &str) -> Vec<PushTarget> {
        self.devices
            .values()
            .filter(|device| {
                device.state == DeviceState::Active && device.subscriptions.contains(channel)
            })
            .map(|device| PushTarget {
                device_id: device.id.clone(),
                provider: device.provider,
            })
            .collect()
    }

    fn device_is_eligible_for_publish(
        &mut self,
        publish_id: &str,
        device_id: &str,
        rng: &mut StdRng,
    ) -> bool {
        if self.roll(self.config.read_stale_probability, rng) {
            self.stats.stale_reads = self.stats.stale_reads.saturating_add(1);
            return false;
        }
        let channel = self
            .publishes
            .get(publish_id)
            .map(|publish| publish.channel.clone());
        let Some(channel) = channel else {
            return false;
        };
        self.devices.get(device_id).is_some_and(|device| {
            device.state == DeviceState::Active && device.subscriptions.contains(&channel)
        })
    }

    fn maybe_toggle_backend(&mut self, tick: u64, rng: &mut StdRng) {
        if self.backend_outage {
            if self.roll(self.config.backend_recovery_probability, rng) {
                self.backend_outage = false;
                self.stats.backend_recoveries = self.stats.backend_recoveries.saturating_add(1);
                self.trace(tick, "push backend recovered".to_string());
            }
        } else if self.roll(self.config.backend_outage_probability, rng) {
            self.backend_outage = true;
            self.stats.backend_outages = self.stats.backend_outages.saturating_add(1);
            self.trace(tick, "push backend outage".to_string());
        }
    }

    fn write_fails_before_commit(&mut self, tick: u64, rng: &mut StdRng, boundary: &str) -> bool {
        if self.roll(self.config.write_fail_before_commit_probability, rng) {
            self.stats.write_fail_before_commit =
                self.stats.write_fail_before_commit.saturating_add(1);
            self.trace(
                tick,
                format!("push write failed before commit at {boundary}"),
            );
            true
        } else {
            false
        }
    }

    fn after_commit_fault(&mut self, tick: u64, rng: &mut StdRng, boundary: &str) {
        if self.roll(self.config.write_fail_after_commit_probability, rng) {
            self.stats.write_fail_after_commit =
                self.stats.write_fail_after_commit.saturating_add(1);
            self.trace(
                tick,
                format!("push write failed after commit at {boundary}"),
            );
        }
        if self.roll(self.config.response_lost_probability, rng) {
            self.stats.response_lost_after_commit =
                self.stats.response_lost_after_commit.saturating_add(1);
            self.trace(
                tick,
                format!("push response lost after commit at {boundary}"),
            );
        }
    }

    fn provider_result(&mut self, _provider: PushProvider, rng: &mut StdRng) -> ProviderResult {
        if self.roll(self.config.provider_lost_response_probability, rng) {
            ProviderResult::LostResponseAfterAccepted
        } else if self.roll(self.config.provider_invalid_token_probability, rng) {
            ProviderResult::InvalidToken
        } else if self.roll(self.config.provider_permanent_rejection_probability, rng) {
            ProviderResult::Rejected
        } else if self.roll(self.config.provider_retryable_probability, rng) {
            ProviderResult::Retryable
        } else {
            ProviderResult::Accepted
        }
    }

    fn ensure_subscriber(&mut self, tick: u64, rng: &mut StdRng, channel: &str) {
        if self.devices.values().any(|device| {
            device.state == DeviceState::Active && device.subscriptions.contains(channel)
        }) {
            return;
        }
        let device_id = self.ensure_active_device(tick, rng);
        if let Some(device) = self.devices.get_mut(&device_id) {
            device.subscriptions.insert(channel.to_string());
        }
    }

    fn ensure_active_device(&mut self, tick: u64, rng: &mut StdRng) -> String {
        if let Some(device) = self
            .devices
            .values()
            .find(|device| device.state == DeviceState::Active)
        {
            return device.id.clone();
        }
        self.register_or_update_device(tick, rng);
        self.random_device_id(rng)
            .unwrap_or_else(|| "device-00000000000000000001".to_string())
    }

    fn random_device_id(&self, rng: &mut StdRng) -> Option<String> {
        if self.devices.is_empty() {
            return None;
        }
        let idx = rng.random_range(0..self.devices.len());
        self.devices.keys().nth(idx).cloned()
    }

    fn random_channel(&self, rng: &mut StdRng) -> String {
        self.channels[rng.random_range(0..self.channels.len())].clone()
    }

    fn next_publish_key(&mut self) -> String {
        let publish_id = format!("push-{:020}", self.next_publish_id);
        self.next_publish_id = self.next_publish_id.saturating_add(1);
        publish_id
    }

    fn next_queue_id(&mut self) -> u64 {
        let id = self.next_queue_id;
        self.next_queue_id = self.next_queue_id.saturating_add(1);
        id
    }

    fn random_queue_delay(&self, rng: &mut StdRng) -> u64 {
        if self.config.max_queue_delay_ticks == 0 {
            0
        } else {
            rng.random_range(0..=self.config.max_queue_delay_ticks)
        }
    }

    fn random_provider_delay(&self, rng: &mut StdRng) -> u64 {
        if self.config.max_provider_delay_ticks == 0 {
            0
        } else {
            rng.random_range(0..=self.config.max_provider_delay_ticks)
        }
    }

    fn queue_contains_publish_log(&self, publish_id: &str) -> bool {
        self.queue.iter().any(|item| {
            matches!(
                &item.stage,
                QueueStage::PublishLog { publish_id: queued } if queued == publish_id
            )
        })
    }

    fn queue_contains_delivery(&self, publish_id: &str, delivery_id: &str) -> bool {
        self.queue.iter().any(|item| {
            matches!(
                &item.stage,
                QueueStage::Delivery {
                    publish_id: queued_publish,
                    delivery_id: queued_delivery,
                } if queued_publish == publish_id && queued_delivery == delivery_id
            )
        })
    }

    fn prune_obsolete_queue(&mut self) {
        let publishes = &self.publishes;
        self.queue.retain(|item| match &item.stage {
            QueueStage::PublishLog { publish_id } => publishes
                .get(publish_id)
                .is_some_and(|publish| publish.state != PublishState::Terminal),
            QueueStage::Delivery {
                publish_id,
                delivery_id,
            }
            | QueueStage::ProviderResult {
                publish_id,
                delivery_id,
                ..
            } => publishes
                .get(publish_id)
                .and_then(|publish| publish.deliveries.get(delivery_id))
                .is_some_and(|delivery| delivery.terminal.is_none()),
        });
    }

    fn outstanding_deliveries(&self) -> usize {
        self.publishes
            .values()
            .map(PushPublish::outstanding_deliveries)
            .sum()
    }

    fn roll(&self, probability: f64, rng: &mut StdRng) -> bool {
        probability > 0.0 && rng.random::<f64>() < probability
    }

    fn trace(&mut self, tick: u64, event: String) {
        if self.trace.len() == MAX_TRACE_EVENTS {
            self.trace.pop_front();
        }
        self.trace.push_back(format!("tick={tick} {event}"));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PushProvider {
    Fcm,
    Apns,
    WebPush,
    Hms,
    Wns,
}

impl PushProvider {
    fn random(rng: &mut StdRng) -> Self {
        match rng.random_range(0..5) {
            0 => Self::Fcm,
            1 => Self::Apns,
            2 => Self::WebPush,
            3 => Self::Hms,
            _ => Self::Wns,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SimDevice {
    id: String,
    client_id: String,
    provider: PushProvider,
    state: DeviceState,
    subscriptions: BTreeSet<String>,
    visible_removed_at: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeviceState {
    Active,
    Invalid,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PushPublish {
    id: String,
    channel: String,
    idempotency_key: String,
    durable_status: bool,
    durable_log: bool,
    state: PublishState,
    planned: u64,
    counters: PushCounters,
    deliveries: BTreeMap<String, Delivery>,
}

impl PushPublish {
    fn terminal_deliveries(&self) -> u64 {
        self.deliveries
            .values()
            .filter(|delivery| delivery.terminal.is_some())
            .count() as u64
    }

    fn outstanding_deliveries(&self) -> usize {
        self.deliveries
            .values()
            .filter(|delivery| delivery.terminal.is_none())
            .count()
    }

    fn refresh_state(&mut self) {
        self.state = if self.terminal_deliveries() == self.planned {
            PublishState::Terminal
        } else {
            PublishState::Running
        };
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishState {
    Queued,
    Running,
    Terminal,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PushCounters {
    succeeded: u64,
    failed: u64,
    expired: u64,
    cancelled: u64,
    dead_lettered: u64,
}

impl PushCounters {
    fn record(&mut self, result: ProviderResult) {
        match result {
            ProviderResult::Accepted => {
                self.succeeded = self.succeeded.saturating_add(1);
            }
            ProviderResult::Rejected => {
                self.failed = self.failed.saturating_add(1);
            }
            ProviderResult::Expired | ProviderResult::InvalidToken => {
                self.expired = self.expired.saturating_add(1);
            }
            ProviderResult::Cancelled => {
                self.cancelled = self.cancelled.saturating_add(1);
            }
            ProviderResult::Retryable | ProviderResult::LostResponseAfterAccepted => {}
        }
    }

    fn total(&self) -> u64 {
        self.succeeded
            .saturating_add(self.failed)
            .saturating_add(self.expired)
            .saturating_add(self.cancelled)
            .saturating_add(self.dead_lettered)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Delivery {
    id: String,
    device_id: String,
    provider: PushProvider,
    state: DeliveryState,
    attempts: u32,
    terminal: Option<ProviderResult>,
    last_result: Option<ProviderResult>,
    result_keys: BTreeSet<String>,
    provider_sent_after_visible_removal: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryState {
    Pending,
    RetryScheduled,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderResult {
    Accepted,
    Retryable,
    Rejected,
    InvalidToken,
    Expired,
    Cancelled,
    LostResponseAfterAccepted,
}

#[derive(Debug, Clone)]
struct PushTarget {
    device_id: String,
    provider: PushProvider,
}

#[derive(Debug, Clone)]
struct ScheduledPush {
    due_tick: u64,
    channel: String,
    publish_id: String,
    idempotency_key: String,
}

#[derive(Debug, Clone)]
struct QueueItem {
    id: u64,
    deliver_at: u64,
    stage: QueueStage,
}

#[derive(Debug, Clone)]
enum QueueStage {
    PublishLog {
        publish_id: String,
    },
    Delivery {
        publish_id: String,
        delivery_id: String,
    },
    ProviderResult {
        publish_id: String,
        delivery_id: String,
        result: ProviderResult,
    },
}

#[derive(Debug, Clone, Default)]
struct PushStats {
    registered_devices: u64,
    accepted_publishes: u64,
    duplicate_publishes: u64,
    scheduled_publishes: u64,
    provider_sends: u64,
    delivery_results: u64,
    accepted_results: u64,
    retryable_results: u64,
    rejected_results: u64,
    expired_results: u64,
    cancelled_results: u64,
    retries_scheduled: u64,
    retries_attempted: u64,
    dead_letters: u64,
    invalid_tokens: u64,
    queue_produce_lost: u64,
    queue_duplicates: u64,
    queue_ack_lost: u64,
    queue_lease_timeouts: u64,
    repair_scans: u64,
    repair_requeued: u64,
    backend_outages: u64,
    backend_recoveries: u64,
    write_fail_before_commit: u64,
    write_fail_after_commit: u64,
    response_lost_after_commit: u64,
    stale_reads: u64,
}
