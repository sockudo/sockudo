use serde::{Deserialize, Serialize};
use sockudo_push::{PublishLifecycleState, PushProviderKind};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::error::{SimulatorError, SimulatorResult};
use crate::io::{DeterministicFaultScheduler, DeterministicQueue, DeterministicStorage};
use crate::real_subsystems::RealPushHarness;

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
    pub status_transitions: u64,
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
#[derive(Clone)]
pub struct PushLab {
    config: PushLabConfig,
    real: RealPushHarness,
    clients: usize,
    channels: Vec<String>,
    devices: BTreeMap<String, SimDevice>,
    publishes: BTreeMap<String, PushPublish>,
    idempotency: BTreeMap<String, String>,
    queue: DeterministicQueue<QueueItem>,
    schedules: Vec<ScheduledPush>,
    storage: DeterministicStorage,
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
            real: RealPushHarness::new(),
            clients,
            channels: channels.to_vec(),
            devices: BTreeMap::new(),
            publishes: BTreeMap::new(),
            idempotency: BTreeMap::new(),
            queue: DeterministicQueue::default(),
            schedules: Vec::new(),
            storage: DeterministicStorage,
            backend_outage: false,
            quiescing: false,
            next_device_id: 1,
            next_publish_id: 1,
            next_queue_id: 1,
            stats: PushStats::default(),
            trace: VecDeque::new(),
        })
    }

    pub(crate) async fn on_tick(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
    ) -> SimulatorResult<()> {
        self.maybe_toggle_backend(tick, scheduler);
        self.release_due_schedules(tick, scheduler).await?;
        self.process_queue(tick, scheduler).await?;
        if self.config.repair_every_ticks > 0 && tick.is_multiple_of(self.config.repair_every_ticks)
        {
            self.repair(tick, scheduler);
        }
        Ok(())
    }

    pub(crate) async fn register_or_update_device(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
    ) -> SimulatorResult<()> {
        if self.backend_outage || self.write_fails_before_commit(tick, scheduler, "device_upsert") {
            return Ok(());
        }
        let provider = random_provider(scheduler);
        let device_id = if self.devices.len() < self.config.devices {
            let id = format!("device-{:020}", self.next_device_id);
            self.next_device_id = self.next_device_id.saturating_add(1);
            id
        } else {
            self.random_device_id(scheduler)
                .unwrap_or_else(|| "device-00000000000000000001".to_string())
        };
        let client_id = format!("client-{}", scheduler.usize_below(self.clients));
        self.real
            .upsert_device(&device_id, &client_id, provider, tick)
            .await?;
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
        self.after_commit_fault(tick, scheduler, "device_upsert");
        self.trace(tick, format!("push device upserted {device_id}"));
        Ok(())
    }

    pub(crate) async fn delete_device(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
    ) -> SimulatorResult<()> {
        let Some(device_id) = self.random_device_id(scheduler) else {
            self.register_or_update_device(tick, scheduler).await?;
            return Ok(());
        };
        if self.backend_outage || self.write_fails_before_commit(tick, scheduler, "device_delete") {
            return Ok(());
        }
        self.real.delete_device(&device_id).await?;
        if let Some(device) = self.devices.get_mut(&device_id) {
            device.state = DeviceState::Deleted;
            device.subscriptions.clear();
            device.visible_removed_at = Some(tick);
            self.after_commit_fault(tick, scheduler, "device_delete");
            self.trace(tick, format!("push device deleted {device_id}"));
        }
        Ok(())
    }

    pub(crate) async fn subscribe_device(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
    ) -> SimulatorResult<()> {
        let device_id = self.ensure_active_device(tick, scheduler).await?;
        let channel = self.random_channel(scheduler);
        if self.backend_outage || self.write_fails_before_commit(tick, scheduler, "subscribe") {
            return Ok(());
        }
        if let Some(device) = self.devices.get_mut(&device_id) {
            self.real.upsert_subscription(&channel, &device_id).await?;
            device.state = DeviceState::Active;
            device.visible_removed_at = None;
            device.subscriptions.insert(channel.clone());
            self.after_commit_fault(tick, scheduler, "subscribe");
            self.trace(tick, format!("push subscribed {device_id} to {channel}"));
        }
        Ok(())
    }

    pub(crate) async fn unsubscribe_device(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
    ) -> SimulatorResult<()> {
        let Some(device_id) = self.random_device_id(scheduler) else {
            return Ok(());
        };
        let channel = self.random_channel(scheduler);
        if self.backend_outage || self.write_fails_before_commit(tick, scheduler, "unsubscribe") {
            return Ok(());
        }
        self.real.delete_subscription(&channel, &device_id).await?;
        if let Some(device) = self.devices.get_mut(&device_id) {
            device.subscriptions.remove(&channel);
            if device.subscriptions.is_empty() {
                device.visible_removed_at = Some(tick);
            }
            self.after_commit_fault(tick, scheduler, "unsubscribe");
            self.trace(
                tick,
                format!("push unsubscribed {device_id} from {channel}"),
            );
        }
        Ok(())
    }

    pub(crate) async fn publish_now(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
    ) -> SimulatorResult<()> {
        let channel = self.random_channel(scheduler);
        self.ensure_subscriber(tick, scheduler, &channel).await?;
        self.accept_publish(tick, scheduler, channel, None).await
    }

    pub(crate) async fn schedule_publish(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
    ) -> SimulatorResult<()> {
        let channel = self.random_channel(scheduler);
        self.ensure_subscriber(tick, scheduler, &channel).await?;
        let delay = self.random_queue_delay(scheduler).saturating_add(1);
        let publish_id = self.next_publish_key();
        let idempotency_key = format!("schedule:{publish_id}");
        let due_tick = tick.saturating_add(delay);
        self.real
            .put_scheduled_publish(&publish_id, &channel, due_tick)
            .await?;
        scheduler.record(
            tick,
            format!("schedule push.scheduled_publish publish={publish_id} due_tick={due_tick}"),
        );
        self.schedules.push(ScheduledPush {
            due_tick,
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
        Ok(())
    }

    pub(crate) fn duplicate_provider_feedback(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
    ) {
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
            candidates[scheduler.usize_below(candidates.len())].clone();
        let item = QueueItem {
            id: self.next_queue_id(),
            deliver_at: tick.saturating_add(self.random_provider_delay(scheduler)),
            stage: QueueStage::ProviderResult {
                publish_id,
                delivery_id,
                result,
            },
        };
        scheduler.record(
            tick,
            format!(
                "schedule push.provider_feedback_duplicate item={} deliver_at={}",
                item.id, item.deliver_at
            ),
        );
        self.queue.push_back(item);
        self.trace(tick, "push duplicate provider feedback queued".to_string());
    }

    pub(crate) fn repair_now(&mut self, tick: u64, scheduler: &mut DeterministicFaultScheduler) {
        self.repair(tick, scheduler);
    }

    pub(crate) async fn quiesce(
        &mut self,
        mut tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
    ) -> SimulatorResult<u64> {
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
                return Ok(tick);
            }
            tick = scheduler.timer_advance(tick, "push.quiesce", tick.saturating_add(1));
            self.backend_outage = false;
            self.release_due_schedules(tick, scheduler).await?;
            self.repair(tick, scheduler);
            self.process_queue(tick, scheduler).await?;
            self.prune_obsolete_queue();
        }
        self.quiescing = previous_quiescing;
        Ok(tick)
    }

    pub(crate) async fn check_oracles(
        &self,
        final_quiesce: bool,
        page_limit: usize,
    ) -> Result<(), String> {
        for (key, publish_id) in &self.idempotency {
            if !self.publishes.contains_key(publish_id) {
                return Err(format!(
                    "push idempotency key {key} points to missing publish {publish_id}"
                ));
            }
        }

        let expected_devices = self
            .devices
            .values()
            .filter(|device| device.state == DeviceState::Active)
            .map(|device| device.id.clone())
            .collect::<BTreeSet<_>>();
        let actual_devices = self
            .real
            .active_device_ids(page_limit)
            .await
            .map_err(|error| format!("real push device scan failed: {error}"))?;
        if actual_devices != expected_devices {
            return Err(format!(
                "real push devices mismatch: expected {expected_devices:?}, got {actual_devices:?}"
            ));
        }

        let expected_subscriptions = self
            .devices
            .values()
            .flat_map(|device| {
                device
                    .subscriptions
                    .iter()
                    .map(|channel| (channel.clone(), device.id.clone()))
            })
            .collect::<BTreeSet<_>>();
        let actual_subscriptions = self
            .real
            .subscription_keys(page_limit)
            .await
            .map_err(|error| format!("real push subscription scan failed: {error}"))?;
        if actual_subscriptions != expected_subscriptions {
            return Err(format!(
                "real push subscriptions mismatch: expected {expected_subscriptions:?}, got {actual_subscriptions:?}"
            ));
        }

        let expected_publish_ids = self.publishes.keys().cloned().collect::<BTreeSet<_>>();
        let actual_publish_log_ids = self
            .real
            .publish_log_ids(page_limit)
            .await
            .map_err(|error| format!("real push publish-log scan failed: {error}"))?;
        if actual_publish_log_ids != expected_publish_ids {
            return Err(format!(
                "real push publish logs mismatch: expected {expected_publish_ids:?}, got {actual_publish_log_ids:?}"
            ));
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
            let status = self
                .real
                .publish_status(&publish.id)
                .await
                .map_err(|error| {
                    format!("real push status read failed for {}: {error}", publish.id)
                })?
                .ok_or_else(|| format!("real push status missing for {}", publish.id))?;
            if status.counters.planned != publish.planned {
                return Err(format!(
                    "real push planned count mismatch for {}: expected {}, got {}",
                    publish.id, publish.planned, status.counters.planned
                ));
            }
            if status.state != PublishLifecycleState::Queued {
                return Err(format!(
                    "real push status for {} changed outside simulator worker model: expected Queued, got {:?}",
                    publish.id, status.state
                ));
            }
            let idempotency_target = self
                .real
                .publish_idempotency_target(&publish.id)
                .await
                .map_err(|error| {
                    format!(
                        "real push idempotency read failed for {}: {error}",
                        publish.id
                    )
                })?;
            if idempotency_target.as_deref() != Some(publish.id.as_str()) {
                return Err(format!(
                    "real push idempotency mismatch for {}: got {idempotency_target:?}",
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
            let expected_state = publish.expected_lifecycle_state();
            if publish.status_state != expected_state {
                return Err(format!(
                    "push publish {} lifecycle state mismatch: expected {:?} from counters {:?}, got {:?}",
                    publish.id, expected_state, publish.counters, publish.status_state
                ));
            }
            for transition in &publish.status_transitions {
                if !valid_status_transition(transition.from, transition.to) {
                    return Err(format!(
                        "push publish {} invalid status transition at tick {}: {:?} -> {:?} ({})",
                        publish.id,
                        transition.tick,
                        transition.from,
                        transition.to,
                        transition.reason
                    ));
                }
            }
            if publish.state == PublishState::Terminal && !is_terminal_status(publish.status_state)
            {
                return Err(format!(
                    "push publish {} is terminal but lifecycle state is {:?}",
                    publish.id, publish.status_state
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

        for schedule in &self.schedules {
            let exists = self
                .real
                .scheduled_publish_exists(&schedule.publish_id)
                .await
                .map_err(|error| {
                    format!(
                        "real push schedule read failed for {}: {error}",
                        schedule.publish_id
                    )
                })?;
            if !exists {
                return Err(format!(
                    "real push schedule missing for pending publish {}",
                    schedule.publish_id
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
            status_transitions: self
                .publishes
                .values()
                .map(|publish| publish.status_transitions.len() as u64)
                .sum(),
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

    async fn accept_publish(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
        channel: String,
        requested: Option<(String, String)>,
    ) -> SimulatorResult<()> {
        if self.backend_outage || self.write_fails_before_commit(tick, scheduler, "push_publish") {
            return Ok(());
        }
        let (publish_id, idempotency_key) = requested.unwrap_or_else(|| {
            let publish_id = if !self.idempotency.is_empty() && scheduler.ratio(1, 8) {
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
            return Ok(());
        }

        let planned_targets = self.eligible_targets(&channel);
        let planned = planned_targets.len() as u64;
        let accepted = self
            .real
            .accept_channel_publish(&publish_id, &channel, planned, tick)
            .await?;
        if accepted.duplicate {
            self.stats.duplicate_publishes = self.stats.duplicate_publishes.saturating_add(1);
            self.trace(
                tick,
                format!("push duplicate real pipeline accept -> {publish_id}"),
            );
            return Ok(());
        }
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
                status_state: PublishLifecycleState::Queued,
                status_transitions: Vec::new(),
                planned,
                counters: PushCounters::default(),
                deliveries,
            },
        );
        self.idempotency.insert(idempotency_key, publish_id.clone());
        self.stats.accepted_publishes = self.stats.accepted_publishes.saturating_add(1);
        self.after_commit_fault(tick, scheduler, "push_publish");
        self.enqueue(
            tick,
            scheduler,
            QueueStage::PublishLog {
                publish_id: publish_id.clone(),
            },
        );
        self.trace(
            tick,
            format!("push publish accepted {publish_id} channel={channel} planned={planned}"),
        );
        Ok(())
    }

    async fn release_due_schedules(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
    ) -> SimulatorResult<()> {
        let mut pending = Vec::with_capacity(self.schedules.len());
        let mut due = Vec::new();
        let schedules = std::mem::take(&mut self.schedules);
        for schedule in schedules {
            if schedule.due_tick <= tick {
                due.push(schedule);
            } else {
                pending.push(schedule);
            }
        }
        scheduler.shuffle_scheduled(tick, "push.schedules.due", &mut due);
        for schedule in due {
            self.real
                .delete_scheduled_publish(&schedule.publish_id)
                .await?;
            self.accept_publish(
                tick,
                scheduler,
                schedule.channel,
                Some((schedule.publish_id, schedule.idempotency_key)),
            )
            .await?;
        }
        self.schedules = pending;
        Ok(())
    }

    async fn process_queue(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
    ) -> SimulatorResult<()> {
        if self.backend_outage {
            return Ok(());
        }

        let mut pending = VecDeque::with_capacity(self.queue.len());
        let mut due = Vec::new();
        let queue = self.queue.take_all();
        for item in queue {
            if item.deliver_at > tick {
                pending.push_back(item);
                continue;
            }
            due.push(item);
        }
        scheduler.shuffle_scheduled(tick, "push.queue.due", &mut due);
        for item in due {
            if !self.quiescing
                && self.roll(
                    tick,
                    scheduler,
                    "queue.lease_timeout",
                    self.config.queue_lease_timeout_probability,
                )
            {
                let mut duplicate = item.clone();
                duplicate.id = self.next_queue_id();
                duplicate.deliver_at =
                    tick.saturating_add(self.random_queue_delay(scheduler).max(1));
                pending.push_back(duplicate);
                self.stats.queue_lease_timeouts = self.stats.queue_lease_timeouts.saturating_add(1);
            }
            self.apply_queue_item(tick, scheduler, item.clone()).await?;
            if !self.quiescing
                && self.roll(
                    tick,
                    scheduler,
                    "queue.ack_lost",
                    self.config.queue_ack_lost_probability,
                )
            {
                let mut redelivery = item;
                redelivery.id = self.next_queue_id();
                redelivery.deliver_at =
                    tick.saturating_add(self.random_queue_delay(scheduler).max(1));
                pending.push_back(redelivery);
                self.stats.queue_ack_lost = self.stats.queue_ack_lost.saturating_add(1);
            }
        }
        self.queue.append_to(&mut pending);
        self.queue.replace(pending);
        Ok(())
    }

    async fn apply_queue_item(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
        item: QueueItem,
    ) -> SimulatorResult<()> {
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
                    return Ok(());
                };
                for delivery_id in delivery_ids {
                    self.enqueue(
                        tick,
                        scheduler,
                        QueueStage::Delivery {
                            publish_id: publish_id.clone(),
                            delivery_id,
                        },
                    );
                }
                if let Some(publish) = self.publishes.get_mut(&publish_id) {
                    if publish.terminal_deliveries() == publish.planned {
                        publish.state = PublishState::Terminal;
                        let next = publish.expected_lifecycle_state();
                        publish.transition_status(tick, next, "publish_log_after_terminal");
                    } else {
                        publish.state = if publish.planned == 0 {
                            PublishState::Terminal
                        } else {
                            PublishState::Running
                        };
                        let next = if publish.planned == 0 {
                            PublishLifecycleState::Succeeded
                        } else {
                            PublishLifecycleState::Dispatching
                        };
                        publish.transition_status(tick, next, "publish_log_planned");
                    };
                }
            }
            QueueStage::Delivery {
                publish_id,
                delivery_id,
            } => {
                self.dispatch_delivery(tick, scheduler, &publish_id, &delivery_id)
                    .await?
            }
            QueueStage::ProviderResult {
                publish_id,
                delivery_id,
                result,
            } => {
                self.apply_provider_result(tick, scheduler, &publish_id, &delivery_id, result)
                    .await?;
            }
        }
        Ok(())
    }

    async fn dispatch_delivery(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
        publish_id: &str,
        delivery_id: &str,
    ) -> SimulatorResult<()> {
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
            return Ok(());
        };
        if terminal.is_some() {
            return Ok(());
        }
        if !self.device_is_eligible_for_publish(tick, publish_id, &device_id, scheduler) {
            self.apply_terminal(publish_id, delivery_id, ProviderResult::Cancelled, tick);
            return Ok(());
        }
        if attempts >= self.config.max_retries {
            self.dead_letter(publish_id, delivery_id, tick);
            return Ok(());
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

        let result = self.provider_result(tick, provider, scheduler);
        if result == ProviderResult::LostResponseAfterAccepted {
            self.apply_provider_result(
                tick,
                scheduler,
                publish_id,
                delivery_id,
                ProviderResult::Retryable,
            )
            .await?;
            return Ok(());
        }
        if self.roll(
            tick,
            scheduler,
            "provider.delayed_result",
            self.config.provider_delayed_result_probability,
        ) {
            self.enqueue(
                tick.saturating_add(self.random_provider_delay(scheduler).max(1)),
                scheduler,
                QueueStage::ProviderResult {
                    publish_id: publish_id.to_string(),
                    delivery_id: delivery_id.to_string(),
                    result,
                },
            );
        } else {
            self.apply_provider_result(tick, scheduler, publish_id, delivery_id, result)
                .await?;
        }
        if self.roll(
            tick,
            scheduler,
            "provider.duplicate_result",
            self.config.provider_duplicate_result_probability,
        ) {
            self.enqueue(
                tick.saturating_add(self.random_provider_delay(scheduler).max(1)),
                scheduler,
                QueueStage::ProviderResult {
                    publish_id: publish_id.to_string(),
                    delivery_id: delivery_id.to_string(),
                    result,
                },
            );
        }
        Ok(())
    }

    async fn apply_provider_result(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
        publish_id: &str,
        delivery_id: &str,
        result: ProviderResult,
    ) -> SimulatorResult<()> {
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
            return Ok(());
        };
        if duplicate {
            return Ok(());
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
                if let Some(device_id) = device_id {
                    self.real.delete_device(&device_id).await?;
                    if let Some(device) = self.devices.get_mut(&device_id) {
                        device.state = DeviceState::Invalid;
                        device.subscriptions.clear();
                        device.visible_removed_at = Some(tick);
                        self.stats.invalid_tokens = self.stats.invalid_tokens.saturating_add(1);
                    }
                }
                self.apply_terminal(publish_id, delivery_id, ProviderResult::Expired, tick);
            }
            ProviderResult::Retryable | ProviderResult::LostResponseAfterAccepted => {
                self.stats.retryable_results = self.stats.retryable_results.saturating_add(1);
                if attempt >= self.config.max_retries {
                    self.dead_letter(publish_id, delivery_id, tick);
                    return Ok(());
                }
                if let Some(publish) = self.publishes.get_mut(publish_id)
                    && let Some(delivery) = publish.deliveries.get_mut(delivery_id)
                {
                    delivery.state = DeliveryState::RetryScheduled;
                    delivery.last_result = Some(ProviderResult::Retryable);
                }
                self.stats.retries_scheduled = self.stats.retries_scheduled.saturating_add(1);
                self.enqueue(
                    tick.saturating_add(self.random_queue_delay(scheduler).max(1)),
                    scheduler,
                    QueueStage::Delivery {
                        publish_id: publish_id.to_string(),
                        delivery_id: delivery_id.to_string(),
                    },
                );
            }
        }
        Ok(())
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
        if publish.state == PublishState::Terminal {
            let next = publish.expected_lifecycle_state();
            publish.transition_status(tick, next, "delivery_terminal");
        }
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
        if publish.state == PublishState::Terminal {
            let next = publish.expected_lifecycle_state();
            publish.transition_status(tick, next, "delivery_dead_lettered");
        }
        self.stats.delivery_results = self.stats.delivery_results.saturating_add(1);
        self.stats.dead_letters = self.stats.dead_letters.saturating_add(1);
        self.trace(
            tick,
            format!("push delivery dead-lettered {publish_id}/{delivery_id}"),
        );
    }

    fn repair(&mut self, tick: u64, scheduler: &mut DeterministicFaultScheduler) {
        if self.backend_outage {
            return;
        }
        let publish_ids = self.publishes.keys().cloned().collect::<Vec<_>>();
        for publish_id in publish_ids {
            self.stats.repair_scans = self.stats.repair_scans.saturating_add(1);
            let Some(publish) = self.publishes.get(&publish_id) else {
                continue;
            };
            if publish.state == PublishState::Queued {
                if !self.queue_contains_publish_log(&publish_id) {
                    self.enqueue(
                        tick,
                        scheduler,
                        QueueStage::PublishLog {
                            publish_id: publish_id.clone(),
                        },
                    );
                    self.stats.repair_requeued = self.stats.repair_requeued.saturating_add(1);
                }
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
                    scheduler,
                    QueueStage::Delivery {
                        publish_id: publish_id.clone(),
                        delivery_id,
                    },
                );
                self.stats.repair_requeued = self.stats.repair_requeued.saturating_add(1);
            }
        }
    }

    fn enqueue(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
        stage: QueueStage,
    ) {
        if !self.quiescing
            && self.roll(
                tick,
                scheduler,
                "queue.produce_lost",
                self.config.queue_produce_lost_probability,
            )
        {
            self.stats.queue_produce_lost = self.stats.queue_produce_lost.saturating_add(1);
            self.trace(tick, format!("push queue produce lost for {stage:?}"));
            return;
        }
        let item = QueueItem {
            id: self.next_queue_id(),
            deliver_at: tick.saturating_add(self.random_queue_delay(scheduler)),
            stage,
        };
        scheduler.record(
            tick,
            format!(
                "schedule push.queue item={} deliver_at={} stage={:?}",
                item.id, item.deliver_at, item.stage
            ),
        );
        self.queue.push_back(item.clone());
        if !self.quiescing
            && self.roll(
                tick,
                scheduler,
                "queue.duplicate",
                self.config.queue_duplicate_probability,
            )
        {
            let mut duplicate = item;
            duplicate.id = self.next_queue_id();
            duplicate.deliver_at = duplicate
                .deliver_at
                .saturating_add(self.random_queue_delay(scheduler).max(1));
            scheduler.record(
                tick,
                format!(
                    "schedule push.queue.duplicate item={} deliver_at={} stage={:?}",
                    duplicate.id, duplicate.deliver_at, duplicate.stage
                ),
            );
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
        tick: u64,
        publish_id: &str,
        device_id: &str,
        scheduler: &mut DeterministicFaultScheduler,
    ) -> bool {
        if self.storage.read_is_stale(
            scheduler,
            tick,
            "push_delivery_target",
            self.config.read_stale_probability,
        ) {
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

    fn maybe_toggle_backend(&mut self, tick: u64, scheduler: &mut DeterministicFaultScheduler) {
        if self.backend_outage {
            if self.roll(
                tick,
                scheduler,
                "storage.backend_recovery",
                self.config.backend_recovery_probability,
            ) {
                self.backend_outage = false;
                self.stats.backend_recoveries = self.stats.backend_recoveries.saturating_add(1);
                self.trace(tick, "push backend recovered".to_string());
            }
        } else if self.roll(
            tick,
            scheduler,
            "storage.backend_outage",
            self.config.backend_outage_probability,
        ) {
            self.backend_outage = true;
            self.stats.backend_outages = self.stats.backend_outages.saturating_add(1);
            self.trace(tick, "push backend outage".to_string());
        }
    }

    fn write_fails_before_commit(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
        boundary: &str,
    ) -> bool {
        if self.storage.write_fails_before_commit(
            scheduler,
            tick,
            boundary,
            self.config.write_fail_before_commit_probability,
        ) {
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

    fn after_commit_fault(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
        boundary: &str,
    ) {
        let fault = self.storage.after_commit(
            scheduler,
            tick,
            boundary,
            self.config.write_fail_after_commit_probability,
            self.config.response_lost_probability,
        );
        if fault.write_failed {
            self.stats.write_fail_after_commit =
                self.stats.write_fail_after_commit.saturating_add(1);
            self.trace(
                tick,
                format!("push write failed after commit at {boundary}"),
            );
        }
        if fault.response_lost {
            self.stats.response_lost_after_commit =
                self.stats.response_lost_after_commit.saturating_add(1);
            self.trace(
                tick,
                format!("push response lost after commit at {boundary}"),
            );
        }
    }

    fn provider_result(
        &mut self,
        tick: u64,
        _provider: PushProviderKind,
        scheduler: &mut DeterministicFaultScheduler,
    ) -> ProviderResult {
        if self.roll(
            tick,
            scheduler,
            "provider.lost_response",
            self.config.provider_lost_response_probability,
        ) {
            ProviderResult::LostResponseAfterAccepted
        } else if self.roll(
            tick,
            scheduler,
            "provider.invalid_token",
            self.config.provider_invalid_token_probability,
        ) {
            ProviderResult::InvalidToken
        } else if self.roll(
            tick,
            scheduler,
            "provider.permanent_rejection",
            self.config.provider_permanent_rejection_probability,
        ) {
            ProviderResult::Rejected
        } else if self.roll(
            tick,
            scheduler,
            "provider.retryable",
            self.config.provider_retryable_probability,
        ) {
            ProviderResult::Retryable
        } else {
            ProviderResult::Accepted
        }
    }

    async fn ensure_subscriber(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
        channel: &str,
    ) -> SimulatorResult<()> {
        if self.devices.values().any(|device| {
            device.state == DeviceState::Active && device.subscriptions.contains(channel)
        }) {
            return Ok(());
        }
        let device_id = self.ensure_active_device(tick, scheduler).await?;
        if let Some(device) = self.devices.get_mut(&device_id) {
            self.real.upsert_subscription(channel, &device_id).await?;
            device.subscriptions.insert(channel.to_string());
        }
        Ok(())
    }

    async fn ensure_active_device(
        &mut self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
    ) -> SimulatorResult<String> {
        if let Some(device) = self
            .devices
            .values()
            .find(|device| device.state == DeviceState::Active)
        {
            return Ok(device.id.clone());
        }
        self.register_or_update_device(tick, scheduler).await?;
        Ok(self
            .devices
            .values()
            .find(|device| device.state == DeviceState::Active)
            .map(|device| device.id.clone())
            .unwrap_or_else(|| "device-00000000000000000001".to_string()))
    }

    fn random_device_id(&self, scheduler: &mut DeterministicFaultScheduler) -> Option<String> {
        if self.devices.is_empty() {
            return None;
        }
        let idx = scheduler.usize_below(self.devices.len());
        self.devices.keys().nth(idx).cloned()
    }

    fn random_channel(&self, scheduler: &mut DeterministicFaultScheduler) -> String {
        self.channels[scheduler.usize_below(self.channels.len())].clone()
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

    fn random_queue_delay(&self, scheduler: &mut DeterministicFaultScheduler) -> u64 {
        if self.config.max_queue_delay_ticks == 0 {
            0
        } else {
            scheduler.u64_inclusive(self.config.max_queue_delay_ticks)
        }
    }

    fn random_provider_delay(&self, scheduler: &mut DeterministicFaultScheduler) -> u64 {
        if self.config.max_provider_delay_ticks == 0 {
            0
        } else {
            scheduler.u64_inclusive(self.config.max_provider_delay_ticks)
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

    fn roll(
        &self,
        tick: u64,
        scheduler: &mut DeterministicFaultScheduler,
        label: &str,
        probability: f64,
    ) -> bool {
        scheduler.roll(tick, label, probability)
    }

    fn trace(&mut self, tick: u64, event: String) {
        if self.trace.len() == MAX_TRACE_EVENTS {
            self.trace.pop_front();
        }
        self.trace.push_back(format!("tick={tick} {event}"));
    }
}

fn random_provider(scheduler: &mut DeterministicFaultScheduler) -> PushProviderKind {
    match scheduler.u32_below(5) {
        0 => PushProviderKind::Fcm,
        1 => PushProviderKind::Apns,
        2 => PushProviderKind::WebPush,
        3 => PushProviderKind::Hms,
        _ => PushProviderKind::Wns,
    }
}

fn valid_status_transition(from: PublishLifecycleState, to: PublishLifecycleState) -> bool {
    match from {
        PublishLifecycleState::Queued => matches!(
            to,
            PublishLifecycleState::Planning
                | PublishLifecycleState::Dispatching
                | PublishLifecycleState::Succeeded
                | PublishLifecycleState::Failed
                | PublishLifecycleState::Expired
                | PublishLifecycleState::DeadLettered
                | PublishLifecycleState::PartiallySucceeded
        ),
        PublishLifecycleState::Planning => matches!(
            to,
            PublishLifecycleState::Dispatching
                | PublishLifecycleState::Failed
                | PublishLifecycleState::Expired
                | PublishLifecycleState::DeadLettered
        ),
        PublishLifecycleState::Dispatching
        | PublishLifecycleState::Throttled
        | PublishLifecycleState::QuotaExceeded => is_terminal_status(to),
        state if is_terminal_status(state) => false,
        PublishLifecycleState::Cancelled
        | PublishLifecycleState::Expired
        | PublishLifecycleState::Failed
        | PublishLifecycleState::DeadLettered
        | PublishLifecycleState::Succeeded
        | PublishLifecycleState::PartiallySucceeded => false,
    }
}

fn is_terminal_status(state: PublishLifecycleState) -> bool {
    matches!(
        state,
        PublishLifecycleState::Cancelled
            | PublishLifecycleState::Expired
            | PublishLifecycleState::Failed
            | PublishLifecycleState::DeadLettered
            | PublishLifecycleState::Succeeded
            | PublishLifecycleState::PartiallySucceeded
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SimDevice {
    id: String,
    client_id: String,
    provider: PushProviderKind,
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
    status_state: PublishLifecycleState,
    status_transitions: Vec<StatusTransition>,
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

    fn transition_status(&mut self, tick: u64, next: PublishLifecycleState, reason: &'static str) {
        if self.status_state == next {
            return;
        }
        let previous = self.status_state;
        self.status_state = next;
        self.status_transitions.push(StatusTransition {
            tick,
            from: previous,
            to: next,
            reason,
        });
    }

    fn expected_lifecycle_state(&self) -> PublishLifecycleState {
        if self.state == PublishState::Queued {
            return PublishLifecycleState::Queued;
        }
        if self.planned == 0 {
            return PublishLifecycleState::Succeeded;
        }
        let succeeded = self.counters.succeeded;
        let failed = self.counters.failed.saturating_add(self.counters.cancelled);
        let expired = self.counters.expired;
        let dead_lettered = self.counters.dead_lettered;
        let terminal = succeeded
            .saturating_add(failed)
            .saturating_add(expired)
            .saturating_add(dead_lettered);
        if terminal < self.planned {
            return PublishLifecycleState::Dispatching;
        }
        if failed == 0 && expired == 0 && dead_lettered == 0 {
            PublishLifecycleState::Succeeded
        } else if succeeded > 0 {
            PublishLifecycleState::PartiallySucceeded
        } else if expired > 0 {
            PublishLifecycleState::Expired
        } else if dead_lettered > 0 {
            PublishLifecycleState::DeadLettered
        } else {
            PublishLifecycleState::Failed
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StatusTransition {
    tick: u64,
    from: PublishLifecycleState,
    to: PublishLifecycleState,
    reason: &'static str,
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
    provider: PushProviderKind,
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
    provider: PushProviderKind,
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
