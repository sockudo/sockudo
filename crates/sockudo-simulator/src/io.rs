use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::VecDeque;

const BASE_TIME_MS: i64 = 1_893_456_000_000;
const MAX_IO_TRACE_EVENTS: usize = 128;

/// Seeded logical clock for simulator timers and durable timestamps.
#[derive(Debug, Clone)]
pub(crate) struct DeterministicClock {
    tick: u64,
    base_time_ms: i64,
}

impl Default for DeterministicClock {
    fn default() -> Self {
        Self {
            tick: 0,
            base_time_ms: BASE_TIME_MS,
        }
    }
}

impl DeterministicClock {
    pub(crate) fn tick(&self) -> u64 {
        self.tick
    }

    pub(crate) fn set_tick(&mut self, tick: u64) {
        self.tick = tick;
    }

    pub(crate) fn advance_to(&mut self, tick: u64) {
        self.tick = self.tick.max(tick);
    }

    pub(crate) fn advance_by(&mut self, ticks: u64) {
        self.tick = self.tick.saturating_add(ticks);
    }

    pub(crate) fn timestamp_ms(&self) -> i64 {
        self.base_time_ms.saturating_add(self.tick as i64)
    }
}

/// Seeded scheduler for all simulator IO choices and fault rolls.
#[derive(Debug, Clone)]
pub(crate) struct DeterministicFaultScheduler {
    rng: StdRng,
    trace: VecDeque<String>,
}

impl DeterministicFaultScheduler {
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            trace: VecDeque::new(),
        }
    }

    pub(crate) fn record(&mut self, tick: u64, event: impl Into<String>) {
        if self.trace.len() == MAX_IO_TRACE_EVENTS {
            self.trace.pop_front();
        }
        self.trace
            .push_back(format!("tick={tick} {}", event.into()));
    }

    pub(crate) fn recent_trace(&self) -> Vec<String> {
        self.trace.iter().cloned().collect()
    }

    pub(crate) fn roll(&mut self, tick: u64, label: &str, probability: f64) -> bool {
        let fired = probability > 0.0 && self.rng.random::<f64>() < probability;
        if fired {
            self.record(tick, format!("fault {label}"));
        }
        fired
    }

    pub(crate) fn bool(&mut self) -> bool {
        self.rng.random::<bool>()
    }

    pub(crate) fn ratio(&mut self, numerator: u32, denominator: u32) -> bool {
        self.rng.random_ratio(numerator, denominator)
    }

    pub(crate) fn usize_below(&mut self, upper: usize) -> usize {
        debug_assert!(upper > 0, "deterministic choice requires a non-empty set");
        self.rng.random_range(0..upper)
    }

    pub(crate) fn u64_inclusive(&mut self, max: u64) -> u64 {
        self.rng.random_range(0..=max)
    }

    pub(crate) fn u32_below(&mut self, upper: u32) -> u32 {
        debug_assert!(upper > 0, "deterministic choice requires a non-empty set");
        self.rng.random_range(0..upper)
    }
}

/// Event queued against a logical simulator tick.
pub(crate) trait ScheduledIoEvent {
    fn deliver_at(&self) -> u64;
}

/// Deterministic fanout delivery queue.
#[derive(Debug, Clone)]
pub(crate) struct DeterministicNetwork<E> {
    events: Vec<E>,
}

impl<E> Default for DeterministicNetwork<E> {
    fn default() -> Self {
        Self { events: Vec::new() }
    }
}

impl<E> DeterministicNetwork<E> {
    pub(crate) fn len(&self) -> usize {
        self.events.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub(crate) fn schedule(&mut self, event: E) {
        self.events.push(event);
    }

    pub(crate) fn retain(&mut self, predicate: impl FnMut(&E) -> bool) {
        self.events.retain(predicate);
    }
}

impl<E: ScheduledIoEvent> DeterministicNetwork<E> {
    pub(crate) fn drain_due(&mut self, tick: u64) -> Vec<E> {
        let mut due = Vec::new();
        let mut pending = Vec::with_capacity(self.events.len());
        for event in std::mem::take(&mut self.events) {
            if event.deliver_at() <= tick {
                due.push(event);
            } else {
                pending.push(event);
            }
        }
        self.events = pending;
        due
    }

    pub(crate) fn next_delivery_tick(&self) -> Option<u64> {
        self.events.iter().map(ScheduledIoEvent::deliver_at).min()
    }
}

/// Deterministic worker queue for modeled async subsystems.
#[derive(Debug, Clone)]
pub(crate) struct DeterministicQueue<E> {
    items: VecDeque<E>,
}

impl<E> Default for DeterministicQueue<E> {
    fn default() -> Self {
        Self {
            items: VecDeque::new(),
        }
    }
}

impl<E> DeterministicQueue<E> {
    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub(crate) fn push_back(&mut self, item: E) {
        self.items.push_back(item);
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &E> {
        self.items.iter()
    }

    pub(crate) fn retain(&mut self, predicate: impl FnMut(&E) -> bool) {
        self.items.retain(predicate);
    }

    pub(crate) fn take_all(&mut self) -> VecDeque<E> {
        std::mem::take(&mut self.items)
    }

    pub(crate) fn append_to(&mut self, items: &mut VecDeque<E>) {
        items.append(&mut self.items);
    }

    pub(crate) fn replace(&mut self, items: VecDeque<E>) {
        self.items = items;
    }
}

/// Deterministic storage fault decisions around real memory-store calls.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DeterministicStorage;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AfterCommitFault {
    pub(crate) write_failed: bool,
    pub(crate) response_lost: bool,
}

impl DeterministicStorage {
    pub(crate) fn write_fails_before_commit(
        &self,
        scheduler: &mut DeterministicFaultScheduler,
        tick: u64,
        boundary: &str,
        probability: f64,
    ) -> bool {
        let label = format!("storage.{boundary}.write_before_commit");
        scheduler.roll(tick, &label, probability)
    }

    pub(crate) fn write_is_dropped(
        &self,
        scheduler: &mut DeterministicFaultScheduler,
        tick: u64,
        boundary: &str,
        probability: f64,
    ) -> bool {
        let label = format!("storage.{boundary}.dropped_write");
        scheduler.roll(tick, &label, probability)
    }

    pub(crate) fn write_is_torn(
        &self,
        scheduler: &mut DeterministicFaultScheduler,
        tick: u64,
        boundary: &str,
        probability: f64,
    ) -> bool {
        let label = format!("storage.{boundary}.torn_write");
        scheduler.roll(tick, &label, probability)
    }

    pub(crate) fn after_commit(
        &self,
        scheduler: &mut DeterministicFaultScheduler,
        tick: u64,
        boundary: &str,
        write_fail_probability: f64,
        response_lost_probability: f64,
    ) -> AfterCommitFault {
        let write_label = format!("storage.{boundary}.write_after_commit");
        let response_label = format!("storage.{boundary}.response_lost_after_commit");
        AfterCommitFault {
            write_failed: scheduler.roll(tick, &write_label, write_fail_probability),
            response_lost: scheduler.roll(tick, &response_label, response_lost_probability),
        }
    }

    pub(crate) fn read_is_stale(
        &self,
        scheduler: &mut DeterministicFaultScheduler,
        tick: u64,
        boundary: &str,
        probability: f64,
    ) -> bool {
        let label = format!("storage.{boundary}.stale_read");
        scheduler.roll(tick, &label, probability)
    }

    pub(crate) fn read_is_corrupted(
        &self,
        scheduler: &mut DeterministicFaultScheduler,
        tick: u64,
        boundary: &str,
        probability: f64,
    ) -> bool {
        let label = format!("storage.{boundary}.corrupt_read");
        scheduler.roll(tick, &label, probability)
    }

    pub(crate) fn commit_visible_at(
        &self,
        scheduler: &mut DeterministicFaultScheduler,
        tick: u64,
        boundary: &str,
        probability: f64,
        max_delay_ticks: u64,
    ) -> u64 {
        if max_delay_ticks == 0
            || !scheduler.roll(
                tick,
                &format!("storage.{boundary}.delayed_commit_visibility"),
                probability,
            )
        {
            return tick;
        }
        let delay = scheduler.u64_inclusive(max_delay_ticks).max(1);
        let visible_at = tick.saturating_add(delay);
        scheduler.record(
            tick,
            format!("storage.{boundary}.commit_visible_at tick={visible_at} delay={delay}"),
        );
        visible_at
    }
}
