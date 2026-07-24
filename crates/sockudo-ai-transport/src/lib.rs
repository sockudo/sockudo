//! AI Transport egress and observability helpers.

pub mod observability;

use ahash::AHashMap;
use parking_lot::Mutex;
use sockudo_core::message_envelope::MessageEnvelope;
use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::PusherMessage;
use sockudo_protocol::versioned_messages::{
    MessageAction, extract_runtime_action, extract_runtime_append_fragment,
    extract_runtime_message_serial, set_runtime_append_fragment,
};
use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

const DEFAULT_SHARDS: usize = 64;
const TERMINAL_COMPLETE: &str = "complete";
const TERMINAL_CANCELLED: &str = "cancelled";

/// Allowed append rollup windows in milliseconds.
pub const ALLOWED_APPEND_ROLLUP_WINDOWS_MS: [u64; 5] = [0, 20, 40, 100, 500];

/// Runtime settings for the append rollup engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RollupConfig {
    pub enabled: bool,
    pub window_ms: u64,
    pub orphan_ttl_ms: u64,
    pub shards: usize,
    pub max_active_streams_per_channel: usize,
}

impl Default for RollupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            window_ms: 40,
            orphan_ttl_ms: 1_000,
            shards: DEFAULT_SHARDS,
            max_active_streams_per_channel: 1024,
        }
    }
}

/// One message ready for egress after rollup processing.
#[derive(Debug, Clone, PartialEq)]
pub struct RollupDelivery {
    pub app_id: String,
    pub channel: String,
    pub message: PusherMessage,
    pub reason: RollupDeliveryReason,
    pub coalesced: usize,
    pub latency_ms: u64,
    pub context: DeferredFanoutContext,
}

/// Fanout facts that must survive a deferred rollup delivery.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DeferredFanoutContext {
    pub exclude_socket: Option<SocketId>,
    pub start_time_ms: Option<f64>,
    pub force_full_message: bool,
    pub envelope: Option<MessageEnvelope>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollupDeliveryReason {
    Immediate,
    Deadline,
    TerminalFlush,
    Terminal,
    Bypass,
    Orphan,
    ShutdownDrain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollupStats {
    pub appends_received: u64,
    pub appends_delivered: u64,
    pub active_streams: usize,
}

/// Exact active-stream change produced while holding the owning channel shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveStreamDelta {
    pub app_id: String,
    pub delta: isize,
}

/// Bounded work returned from the due-only scheduler.
#[derive(Debug, Default)]
pub struct SchedulerPoll {
    pub deliveries: Vec<RollupDelivery>,
    pub active_stream_deltas: Vec<ActiveStreamDelta>,
    pub examined_entries: usize,
    pub popped_entries: usize,
}

/// A generation-checked due-work claim. The payload remains in the engine until
/// the adapter holds the channel ordering permit and calls `claim_due`.
#[derive(Debug, Clone)]
pub struct DueStreamToken {
    key: Arc<StreamKey>,
    generation: u64,
    kind: DeadlineKind,
}

impl DueStreamToken {
    #[must_use]
    pub fn app_id(&self) -> &str {
        &self.key.app_id
    }

    #[must_use]
    pub fn channel(&self) -> &str {
        &self.key.channel
    }
}

#[derive(Debug, Default)]
pub struct DueTokenPoll {
    pub tokens: Vec<DueStreamToken>,
    pub examined_entries: usize,
    pub popped_entries: usize,
}

/// Diagnostic scheduler state. This method is not used to publish gauges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RollupTrackingStats {
    pub active_streams: usize,
    pub pending_payload_streams: usize,
    pub scheduled_deadlines: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StreamKey {
    app_id: String,
    channel: String,
    message_serial: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ChannelKey {
    app_id: String,
    channel: String,
}

#[derive(Debug)]
struct PendingBatch {
    first_seen_ms: u64,
    last_seen_ms: u64,
    latest_message: PusherMessage,
    context: DeferredFanoutContext,
    pending_fragment: Option<String>,
    appended_count: usize,
}

impl PendingBatch {
    fn new(
        first_seen_ms: u64,
        now_ms: u64,
        latest_message: PusherMessage,
        context: DeferredFanoutContext,
    ) -> Self {
        let mut pending = Self {
            first_seen_ms,
            last_seen_ms: now_ms,
            latest_message,
            context,
            pending_fragment: None,
            appended_count: 0,
        };
        pending.capture_fragment();
        pending
    }

    fn push(&mut self, now_ms: u64, message: PusherMessage, context: DeferredFanoutContext) {
        self.last_seen_ms = now_ms;
        self.latest_message = message;
        self.context = context;
        self.capture_fragment();
    }

    fn capture_fragment(&mut self) {
        let next_fragment =
            extract_runtime_append_fragment(&self.latest_message).map(str::to_string);
        if let Some(next_fragment) = next_fragment {
            match self.pending_fragment.as_mut() {
                Some(pending) => pending.push_str(&next_fragment),
                None => self.pending_fragment = Some(next_fragment),
            }
            if let Some(pending) = self.pending_fragment.as_ref() {
                set_runtime_append_fragment(&mut self.latest_message, pending.clone());
            }
        } else {
            self.pending_fragment = None;
        }
        self.appended_count += 1;
        if let Some(envelope) = self.context.envelope.as_mut() {
            envelope.extras = self.latest_message.extras.clone();
        }
    }

    fn into_delivery(
        self,
        reason: RollupDeliveryReason,
        app_id: &str,
        channel: &str,
    ) -> RollupDelivery {
        let coalesced = self.appended_count.max(1);
        RollupDelivery {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            message: self.latest_message,
            reason,
            coalesced,
            latency_ms: self.last_seen_ms.saturating_sub(self.first_seen_ms),
            context: self.context,
        }
    }
}

struct TrackedStream {
    first_seen_ms: u64,
    last_seen_ms: u64,
    window_deadline_ms: u64,
    flush_generation: u64,
    orphan_generation: u64,
    pending: Option<Box<PendingBatch>>,
}

impl TrackedStream {
    fn new(now_ms: u64, window_ms: u64) -> Self {
        Self {
            first_seen_ms: now_ms,
            last_seen_ms: now_ms,
            window_deadline_ms: now_ms.saturating_add(window_ms),
            flush_generation: 1,
            orphan_generation: 1,
            pending: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeadlineKind {
    Flush,
    Orphan,
}

#[derive(Debug, Clone)]
struct DeadlineEntry {
    due_ms: u64,
    sequence: u64,
    generation: u64,
    key: Arc<StreamKey>,
    kind: DeadlineKind,
}

impl PartialEq for DeadlineEntry {
    fn eq(&self, other: &Self) -> bool {
        self.due_ms == other.due_ms && self.sequence == other.sequence
    }
}

impl Eq for DeadlineEntry {}

impl PartialOrd for DeadlineEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for DeadlineEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .due_ms
            .cmp(&self.due_ms)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

#[derive(Default)]
struct ShardState {
    streams: AHashMap<Arc<StreamKey>, TrackedStream>,
    active_by_channel: AHashMap<ChannelKey, usize>,
    flush_deadlines: BinaryHeap<DeadlineEntry>,
    orphan_deadlines: BinaryHeap<DeadlineEntry>,
    next_sequence: u64,
    stale_deadlines: usize,
}

struct Shard {
    state: Mutex<ShardState>,
}

/// Coalesces versioned `message.append` fan-out without changing persistence.
///
/// The engine is synchronous by design. Callers take the returned deliveries
/// outside the engine lock and perform async I/O separately.
pub struct RollupEngine {
    config: RollupConfig,
    shards: Vec<Shard>,
    active_streams: AtomicUsize,
    next_poll_shard: AtomicUsize,
    appends_received: AtomicU64,
    appends_delivered: AtomicU64,
}

impl RollupEngine {
    #[must_use]
    pub fn new(config: RollupConfig) -> Self {
        let shard_count = config.shards.max(1);
        let shards = (0..shard_count)
            .map(|_| Shard {
                state: Mutex::new(ShardState::default()),
            })
            .collect();
        Self {
            config,
            shards,
            active_streams: AtomicUsize::new(0),
            next_poll_shard: AtomicUsize::new(0),
            appends_received: AtomicU64::new(0),
            appends_delivered: AtomicU64::new(0),
        }
    }

    #[must_use]
    pub fn config(&self) -> RollupConfig {
        self.config
    }

    /// Process a versioned message and return zero, one, or two egress deliveries.
    #[must_use]
    pub fn ingest(
        &self,
        app_id: &str,
        channel: &str,
        message: PusherMessage,
        now_ms: u64,
    ) -> Vec<RollupDelivery> {
        self.ingest_with_context(
            app_id,
            channel,
            message,
            now_ms,
            DeferredFanoutContext::default(),
        )
        .deliveries
    }

    /// Process a message while retaining all facts needed by a deferred fanout.
    #[must_use]
    pub fn ingest_with_context(
        &self,
        app_id: &str,
        channel: &str,
        message: PusherMessage,
        now_ms: u64,
        context: DeferredFanoutContext,
    ) -> SchedulerPoll {
        let mut result = SchedulerPoll::default();
        let Some(action) = extract_runtime_action(&message) else {
            result
                .deliveries
                .push(bypass(app_id, channel, message, context));
            return result;
        };
        let Some(message_serial) = extract_runtime_message_serial(&message).map(ToOwned::to_owned)
        else {
            result
                .deliveries
                .push(bypass(app_id, channel, message, context));
            return result;
        };

        if !self.config.enabled || self.config.window_ms == 0 {
            if action == MessageAction::Append {
                self.mark_received();
                self.mark_delivered(1);
            }
            result
                .deliveries
                .push(bypass(app_id, channel, message, context));
            return result;
        }

        let key = StreamKey {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            message_serial,
        };
        let channel_key = ChannelKey {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
        };
        let shard = self.shard(&key);
        let mut state = shard.state.lock();

        if matches!(action, MessageAction::Update | MessageAction::Delete) {
            if let Some(tracked) = self.remove_stream(&mut state, &key, &channel_key) {
                result.active_stream_deltas.push(ActiveStreamDelta {
                    app_id: app_id.to_string(),
                    delta: -1,
                });
                if let Some(pending) = tracked.pending {
                    result.deliveries.push(pending.into_delivery(
                        RollupDeliveryReason::TerminalFlush,
                        app_id,
                        channel,
                    ));
                    self.mark_delivered(1);
                }
            }
            result.deliveries.push(delivery(
                app_id,
                channel,
                message,
                context,
                RollupDeliveryReason::Terminal,
                1,
                0,
            ));
            return result;
        }

        if action != MessageAction::Append {
            result
                .deliveries
                .push(bypass(app_id, channel, message, context));
            return result;
        }

        self.mark_received();
        if is_terminal_append(&message) {
            if let Some(tracked) = self.remove_stream(&mut state, &key, &channel_key) {
                result.active_stream_deltas.push(ActiveStreamDelta {
                    app_id: app_id.to_string(),
                    delta: -1,
                });
                if let Some(pending) = tracked.pending {
                    result.deliveries.push(pending.into_delivery(
                        RollupDeliveryReason::TerminalFlush,
                        app_id,
                        channel,
                    ));
                    self.mark_delivered(1);
                }
            }
            result.deliveries.push(delivery(
                app_id,
                channel,
                message,
                context,
                RollupDeliveryReason::Terminal,
                1,
                0,
            ));
            self.mark_delivered(1);
            return result;
        }

        let mut next_flush = None;
        if let Some(tracked) = state.streams.get_mut(&key) {
            let effective_now_ms = now_ms.max(tracked.last_seen_ms);
            tracked.last_seen_ms = effective_now_ms;
            if effective_now_ms >= tracked.window_deadline_ms {
                if let Some(pending) = tracked.pending.take() {
                    result.deliveries.push(pending.into_delivery(
                        RollupDeliveryReason::Deadline,
                        app_id,
                        channel,
                    ));
                    self.mark_delivered(1);
                }
                tracked.window_deadline_ms = effective_now_ms.saturating_add(self.config.window_ms);
                tracked.flush_generation = tracked.flush_generation.wrapping_add(1);
                next_flush = Some((tracked.window_deadline_ms, tracked.flush_generation));
                result.deliveries.push(delivery(
                    app_id,
                    channel,
                    message,
                    context,
                    RollupDeliveryReason::Immediate,
                    1,
                    0,
                ));
                self.mark_delivered(1);
            } else {
                match tracked.pending.as_mut() {
                    Some(pending) => pending.push(now_ms, message, context),
                    None => {
                        tracked.pending = Some(Box::new(PendingBatch::new(
                            tracked.first_seen_ms,
                            effective_now_ms,
                            message,
                            context,
                        )));
                    }
                }
            }
        } else {
            if !try_increment_channel(
                &mut state.active_by_channel,
                &channel_key,
                self.config.max_active_streams_per_channel,
            ) {
                self.mark_delivered(1);
                result
                    .deliveries
                    .push(bypass(app_id, channel, message, context));
                return result;
            }
            let key = Arc::new(key.clone());
            let tracked = TrackedStream::new(now_ms, self.config.window_ms);
            let flush_deadline = tracked.window_deadline_ms;
            let flush_generation = tracked.flush_generation;
            let orphan_deadline = now_ms.saturating_add(self.config.orphan_ttl_ms);
            let orphan_generation = tracked.orphan_generation;
            state.streams.insert(Arc::clone(&key), tracked);
            schedule_deadline(
                &mut state,
                DeadlineKind::Flush,
                Arc::clone(&key),
                flush_deadline,
                flush_generation,
            );
            schedule_deadline(
                &mut state,
                DeadlineKind::Orphan,
                key,
                orphan_deadline,
                orphan_generation,
            );
            self.active_streams.fetch_add(1, Ordering::AcqRel);
            result.active_stream_deltas.push(ActiveStreamDelta {
                app_id: app_id.to_string(),
                delta: 1,
            });
            result.deliveries.push(delivery(
                app_id,
                channel,
                message,
                context,
                RollupDeliveryReason::Immediate,
                1,
                0,
            ));
            self.mark_delivered(1);
        }

        if let Some((due_ms, generation)) = next_flush
            && let Some(key) = state
                .streams
                .get_key_value(&key)
                .map(|(key, _)| Arc::clone(key))
        {
            state.stale_deadlines = state.stale_deadlines.saturating_add(1);
            schedule_deadline(&mut state, DeadlineKind::Flush, key, due_ms, generation);
            compact_deadlines_if_needed(&mut state);
        }

        result
    }

    /// Flush streams whose rollup window has elapsed.
    #[must_use]
    pub fn flush_due(&self, now_ms: u64) -> Vec<RollupDelivery> {
        self.poll_kind(now_ms, usize::MAX, DeadlineKind::Flush)
            .deliveries
    }

    /// Flush and drop streams abandoned past the configured orphan TTL.
    #[must_use]
    pub fn sweep_orphans(&self, now_ms: u64) -> Vec<RollupDelivery> {
        self.poll_kind(now_ms, usize::MAX, DeadlineKind::Orphan)
            .deliveries
    }

    /// Poll due flush and orphan deadlines without scanning active streams.
    #[must_use]
    pub fn poll_due(&self, now_ms: u64, limit: usize) -> SchedulerPoll {
        self.poll_deadlines(now_ms, limit, None)
    }

    /// Discover due work without removing stream state. The adapter claims each
    /// token only after entering its per-channel ordering boundary.
    #[must_use]
    pub fn poll_due_tokens(&self, now_ms: u64, limit: usize) -> DueTokenPoll {
        self.poll_deadline_tokens(now_ms, limit, None)
    }

    /// Atomically claim due work. A terminal mutation or newer generation makes
    /// an older token stale and produces no delivery.
    #[must_use]
    pub fn claim_due(&self, token: DueStreamToken) -> SchedulerPoll {
        let mut result = SchedulerPoll::default();
        let shard = self.shard(token.key.as_ref());
        let mut state = shard.state.lock();
        let Some(tracked) = state.streams.get(token.key.as_ref()) else {
            return result;
        };
        let current_generation = match token.kind {
            DeadlineKind::Flush => tracked.flush_generation,
            DeadlineKind::Orphan => tracked.orphan_generation,
        };
        if current_generation != token.generation {
            return result;
        }
        let channel_key = ChannelKey {
            app_id: token.key.app_id.clone(),
            channel: token.key.channel.clone(),
        };
        if let Some(tracked) = self.remove_stream(&mut state, &token.key, &channel_key) {
            result.active_stream_deltas.push(ActiveStreamDelta {
                app_id: token.key.app_id.clone(),
                delta: -1,
            });
            if let Some(pending) = tracked.pending {
                let reason = match token.kind {
                    DeadlineKind::Flush => RollupDeliveryReason::Deadline,
                    DeadlineKind::Orphan => RollupDeliveryReason::Orphan,
                };
                result.deliveries.push(pending.into_delivery(
                    reason,
                    &token.key.app_id,
                    &token.key.channel,
                ));
                self.mark_delivered(1);
            }
        }
        result
    }

    /// Drain all pending coalesced content during graceful shutdown.
    #[must_use]
    pub fn drain_pending(&self, limit: usize) -> SchedulerPoll {
        let mut result = SchedulerPoll::default();
        if limit == 0 {
            return result;
        }
        for shard in &self.shards {
            if result.active_stream_deltas.len() >= limit {
                break;
            }
            let mut state = shard.state.lock();
            let remaining = limit.saturating_sub(result.active_stream_deltas.len());
            let keys = state
                .streams
                .keys()
                .take(remaining)
                .cloned()
                .collect::<Vec<_>>();
            for key in keys {
                let channel_key = ChannelKey {
                    app_id: key.app_id.clone(),
                    channel: key.channel.clone(),
                };
                if let Some(tracked) = self.remove_stream(&mut state, &key, &channel_key) {
                    result.active_stream_deltas.push(ActiveStreamDelta {
                        app_id: key.app_id.clone(),
                        delta: -1,
                    });
                    if let Some(pending) = tracked.pending {
                        result.deliveries.push(pending.into_delivery(
                            RollupDeliveryReason::ShutdownDrain,
                            &key.app_id,
                            &key.channel,
                        ));
                    }
                }
            }
        }
        self.mark_delivered(result.deliveries.len() as u64);
        result
    }

    #[must_use]
    pub fn active_streams(&self) -> usize {
        self.active_streams.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn stats(&self) -> RollupStats {
        RollupStats {
            appends_received: self.appends_received.load(Ordering::Relaxed),
            appends_delivered: self.appends_delivered.load(Ordering::Relaxed),
            active_streams: self.active_streams(),
        }
    }

    #[must_use]
    pub fn tracking_stats(&self) -> RollupTrackingStats {
        let mut pending_payload_streams = 0;
        let mut scheduled_deadlines = 0;
        for shard in &self.shards {
            let state = shard.state.lock();
            pending_payload_streams += state
                .streams
                .values()
                .filter(|stream| stream.pending.is_some())
                .count();
            scheduled_deadlines += state.flush_deadlines.len() + state.orphan_deadlines.len();
        }
        RollupTrackingStats {
            active_streams: self.active_streams(),
            pending_payload_streams,
            scheduled_deadlines,
        }
    }

    /// Inline scheduler bytes per active stream, excluding keys and payload allocations.
    #[must_use]
    pub const fn tracking_overhead_bytes_per_stream() -> usize {
        std::mem::size_of::<TrackedStream>()
            + (2 * std::mem::size_of::<DeadlineEntry>())
            + std::mem::size_of::<Arc<StreamKey>>()
    }

    fn poll_kind(&self, now_ms: u64, limit: usize, kind: DeadlineKind) -> SchedulerPoll {
        self.poll_deadlines(now_ms, limit, Some(kind))
    }

    fn poll_deadlines(
        &self,
        now_ms: u64,
        limit: usize,
        only_kind: Option<DeadlineKind>,
    ) -> SchedulerPoll {
        let tokens = self.poll_deadline_tokens(now_ms, limit, only_kind);
        let mut result = SchedulerPoll {
            examined_entries: tokens.examined_entries,
            popped_entries: tokens.popped_entries,
            ..SchedulerPoll::default()
        };
        for token in tokens.tokens {
            let claimed = self.claim_due(token);
            result.deliveries.extend(claimed.deliveries);
            result
                .active_stream_deltas
                .extend(claimed.active_stream_deltas);
        }
        result
    }

    fn poll_deadline_tokens(
        &self,
        now_ms: u64,
        limit: usize,
        only_kind: Option<DeadlineKind>,
    ) -> DueTokenPoll {
        let mut result = DueTokenPoll::default();
        if limit == 0 {
            return result;
        }
        let start = self.next_poll_shard.fetch_add(1, Ordering::Relaxed) % self.shards.len();
        for offset in 0..self.shards.len() {
            if result.popped_entries >= limit {
                break;
            }
            let shard = &self.shards[(start + offset) % self.shards.len()];
            let mut state = shard.state.lock();
            if only_kind != Some(DeadlineKind::Orphan) {
                self.poll_shard_tokens(&mut state, now_ms, limit, DeadlineKind::Flush, &mut result);
            }
            if result.popped_entries < limit && only_kind != Some(DeadlineKind::Flush) {
                self.poll_shard_tokens(
                    &mut state,
                    now_ms,
                    limit,
                    DeadlineKind::Orphan,
                    &mut result,
                );
            }
            compact_deadlines_if_needed(&mut state);
        }
        result
    }

    fn poll_shard_tokens(
        &self,
        state: &mut ShardState,
        now_ms: u64,
        limit: usize,
        kind: DeadlineKind,
        result: &mut DueTokenPoll,
    ) {
        loop {
            if result.popped_entries >= limit {
                return;
            }
            result.examined_entries = result.examined_entries.saturating_add(1);
            let next = match kind {
                DeadlineKind::Flush => state.flush_deadlines.peek(),
                DeadlineKind::Orphan => state.orphan_deadlines.peek(),
            };
            let Some(next) = next else {
                return;
            };
            if next.due_ms > now_ms {
                return;
            }
            let entry = match kind {
                DeadlineKind::Flush => state.flush_deadlines.pop(),
                DeadlineKind::Orphan => state.orphan_deadlines.pop(),
            }
            .expect("peeked deadline must be present");
            result.popped_entries = result.popped_entries.saturating_add(1);

            let Some(tracked) = state.streams.get(entry.key.as_ref()) else {
                continue;
            };
            let current_generation = match kind {
                DeadlineKind::Flush => tracked.flush_generation,
                DeadlineKind::Orphan => tracked.orphan_generation,
            };
            if entry.generation != current_generation {
                continue;
            }

            match kind {
                DeadlineKind::Flush => {
                    if tracked.pending.is_none() {
                        continue;
                    }
                    result.tokens.push(DueStreamToken {
                        key: entry.key,
                        generation: entry.generation,
                        kind,
                    });
                }
                DeadlineKind::Orphan => {
                    let actual_due = tracked
                        .last_seen_ms
                        .saturating_add(self.config.orphan_ttl_ms);
                    if actual_due > now_ms {
                        let tracked = state
                            .streams
                            .get_mut(entry.key.as_ref())
                            .expect("stream observed while shard is locked");
                        tracked.orphan_generation = tracked.orphan_generation.wrapping_add(1);
                        let generation = tracked.orphan_generation;
                        schedule_deadline(
                            state,
                            DeadlineKind::Orphan,
                            Arc::clone(&entry.key),
                            actual_due,
                            generation,
                        );
                        continue;
                    }
                    result.tokens.push(DueStreamToken {
                        key: entry.key,
                        generation: entry.generation,
                        kind,
                    });
                }
            }
        }
    }

    fn remove_stream(
        &self,
        state: &mut ShardState,
        key: &StreamKey,
        channel_key: &ChannelKey,
    ) -> Option<TrackedStream> {
        let (_, tracked) = state.streams.remove_entry(key)?;
        decrement_channel(&mut state.active_by_channel, channel_key);
        state.stale_deadlines = state.stale_deadlines.saturating_add(2);
        self.decrement_active_total();
        compact_deadlines_if_needed(state);
        Some(tracked)
    }

    fn decrement_active_total(&self) {
        let previous = self.active_streams.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "active stream total underflow");
    }

    #[inline]
    fn shard(&self, key: &StreamKey) -> &Shard {
        let index = fast_shard_index(key, self.shards.len());
        &self.shards[index]
    }

    #[inline]
    fn mark_received(&self) {
        self.appends_received.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn mark_delivered(&self, count: u64) {
        self.appends_delivered.fetch_add(count, Ordering::Relaxed);
    }
}

fn schedule_deadline(
    state: &mut ShardState,
    kind: DeadlineKind,
    key: Arc<StreamKey>,
    due_ms: u64,
    generation: u64,
) {
    let sequence = state.next_sequence;
    state.next_sequence = state.next_sequence.wrapping_add(1);
    let entry = DeadlineEntry {
        due_ms,
        sequence,
        generation,
        key,
        kind,
    };
    match kind {
        DeadlineKind::Flush => state.flush_deadlines.push(entry),
        DeadlineKind::Orphan => state.orphan_deadlines.push(entry),
    }
}

fn compact_deadlines_if_needed(state: &mut ShardState) {
    let total = state.flush_deadlines.len() + state.orphan_deadlines.len();
    if state.stale_deadlines < 64 || state.stale_deadlines.saturating_mul(2) < total {
        return;
    }
    let mut flush = std::mem::take(&mut state.flush_deadlines);
    flush.retain(|entry| {
        state.streams.get(entry.key.as_ref()).is_some_and(|stream| {
            entry.kind == DeadlineKind::Flush
                && entry.generation == stream.flush_generation
                && entry.due_ms == stream.window_deadline_ms
        })
    });
    let mut orphan = std::mem::take(&mut state.orphan_deadlines);
    orphan.retain(|entry| {
        state.streams.get(entry.key.as_ref()).is_some_and(|stream| {
            entry.kind == DeadlineKind::Orphan && entry.generation == stream.orphan_generation
        })
    });
    state.flush_deadlines = flush;
    state.orphan_deadlines = orphan;
    state.stale_deadlines = 0;
}

fn try_increment_channel(
    active: &mut AHashMap<ChannelKey, usize>,
    key: &ChannelKey,
    limit: usize,
) -> bool {
    let count = active.entry(key.clone()).or_insert(0);
    if *count >= limit {
        return false;
    }
    *count += 1;
    true
}

fn decrement_channel(active: &mut AHashMap<ChannelKey, usize>, key: &ChannelKey) {
    if let Some(count) = active.get_mut(key) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            active.remove(key);
        }
    }
}

#[inline]
#[must_use]
pub fn is_allowed_append_rollup_window_ms(window_ms: u64) -> bool {
    ALLOWED_APPEND_ROLLUP_WINDOWS_MS.contains(&window_ms)
}

fn bypass(
    app_id: &str,
    channel: &str,
    message: PusherMessage,
    context: DeferredFanoutContext,
) -> RollupDelivery {
    delivery(
        app_id,
        channel,
        message,
        context,
        RollupDeliveryReason::Bypass,
        1,
        0,
    )
}

fn delivery(
    app_id: &str,
    channel: &str,
    message: PusherMessage,
    context: DeferredFanoutContext,
    reason: RollupDeliveryReason,
    coalesced: usize,
    latency_ms: u64,
) -> RollupDelivery {
    RollupDelivery {
        app_id: app_id.to_string(),
        channel: channel.to_string(),
        message,
        reason,
        coalesced,
        latency_ms,
        context,
    }
}

fn is_terminal_append(message: &PusherMessage) -> bool {
    message
        .ai_transport_headers()
        .and_then(|headers| headers.status())
        .is_some_and(|status| matches!(status, TERMINAL_COMPLETE | TERMINAL_CANCELLED))
}

#[inline]
fn fast_shard_index(key: &StreamKey, shards: usize) -> usize {
    let mut hash = 0xcbf29ce484222325_u64;
    for bytes in [key.app_id.as_bytes(), key.channel.as_bytes()] {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    (hash as usize) % shards
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_protocol::messages::{AiExtras, MessageData, MessageExtras};
    use sockudo_protocol::versioned_messages::{MessageVersionMetadata, apply_runtime_metadata};
    use std::collections::HashMap;

    fn append(serial: &str, data: &str, now: i64) -> PusherMessage {
        let mut message = PusherMessage {
            event: Some(MessageAction::Append.v2_event_name()),
            channel: Some("ai:room".to_string()),
            data: Some(MessageData::String(data.to_string())),
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        };
        apply_runtime_metadata(
            &mut message,
            MessageAction::Append,
            serial,
            &MessageVersionMetadata {
                serial: format!("v{now}"),
                client_id: None,
                timestamp_ms: now,
                description: None,
                metadata: None,
            },
            Some(now as u64),
        );
        message
    }

    fn append_with_fragment(
        serial: &str,
        aggregate: &str,
        fragment: &str,
        now: i64,
    ) -> PusherMessage {
        let mut message = append(serial, aggregate, now);
        set_runtime_append_fragment(&mut message, fragment);
        message
    }

    fn terminal_append(serial: &str, data: &str) -> PusherMessage {
        let mut message = append(serial, data, 99);
        let mut transport = HashMap::new();
        transport.insert("status".to_string(), "complete".to_string());
        message.extras.get_or_insert_with(MessageExtras::default).ai = Some(AiExtras {
            transport: Some(transport),
            codec: None,
        });
        message
    }

    fn update(serial: &str) -> PusherMessage {
        let mut message = append(serial, "final", 101);
        message.event = Some(MessageAction::Update.v2_event_name());
        apply_runtime_metadata(
            &mut message,
            MessageAction::Update,
            serial,
            &MessageVersionMetadata {
                serial: "v101".to_string(),
                client_id: None,
                timestamp_ms: 101,
                description: None,
                metadata: None,
            },
            Some(101),
        );
        message
    }

    fn string_data(message: &PusherMessage) -> &str {
        message
            .data
            .as_ref()
            .and_then(MessageData::as_string)
            .unwrap()
    }

    #[test]
    fn first_append_delivers_immediately_and_arms_window() {
        let engine = RollupEngine::new(RollupConfig::default());
        let out = engine.ingest("app", "ai:room", append("m1", "a", 1), 0);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].reason, RollupDeliveryReason::Immediate);
        assert_eq!(string_data(&out[0].message), "a");
        assert_eq!(engine.active_streams(), 1);
    }

    #[test]
    fn appends_within_window_are_coalesced_until_deadline() {
        let engine = RollupEngine::new(RollupConfig::default());
        assert_eq!(
            engine
                .ingest("app", "ai:room", append("m1", "a", 1), 0)
                .len(),
            1
        );
        assert!(
            engine
                .ingest("app", "ai:room", append("m1", "ab", 2), 10)
                .is_empty()
        );
        assert!(
            engine
                .ingest("app", "ai:room", append("m1", "abc", 3), 20)
                .is_empty()
        );

        let out = engine.flush_due(40);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].reason, RollupDeliveryReason::Deadline);
        assert_eq!(string_data(&out[0].message), "abc");
        assert_eq!(out[0].coalesced, 2);
        assert_eq!(engine.active_streams(), 0);
    }

    #[test]
    fn coalesced_appends_preserve_combined_append_fragment() {
        let engine = RollupEngine::new(RollupConfig::default());
        let first = engine.ingest("app", "ai:room", append_with_fragment("m1", "a", "a", 1), 0);
        assert_eq!(first.len(), 1);

        assert!(
            engine
                .ingest(
                    "app",
                    "ai:room",
                    append_with_fragment("m1", "ab", "b", 2),
                    10,
                )
                .is_empty()
        );
        assert!(
            engine
                .ingest(
                    "app",
                    "ai:room",
                    append_with_fragment("m1", "abc", "c", 3),
                    20,
                )
                .is_empty()
        );

        let out = engine.flush_due(40);
        assert_eq!(out.len(), 1);
        assert_eq!(string_data(&out[0].message), "abc");
        assert_eq!(extract_runtime_append_fragment(&out[0].message), Some("bc"));
    }

    #[test]
    fn terminal_append_flushes_pending_before_terminal() {
        let engine = RollupEngine::new(RollupConfig::default());
        assert_eq!(
            engine
                .ingest("app", "ai:room", append("m1", "a", 1), 0)
                .len(),
            1
        );
        assert!(
            engine
                .ingest("app", "ai:room", append("m1", "ab", 2), 10)
                .is_empty()
        );

        let out = engine.ingest("app", "ai:room", terminal_append("m1", "abc"), 11);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].reason, RollupDeliveryReason::TerminalFlush);
        assert_eq!(string_data(&out[0].message), "ab");
        assert_eq!(out[1].reason, RollupDeliveryReason::Terminal);
        assert_eq!(string_data(&out[1].message), "abc");
        assert_eq!(engine.active_streams(), 0);
    }

    #[test]
    fn terminal_append_does_not_redeliver_the_immediate_first_append() {
        let engine = RollupEngine::new(RollupConfig::default());
        let first = engine.ingest("app", "ai:room", append("m1", "a", 1), 0);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].reason, RollupDeliveryReason::Immediate);

        let terminal = engine.ingest("app", "ai:room", terminal_append("m1", "a"), 1);
        assert_eq!(terminal.len(), 1);
        assert_eq!(terminal[0].reason, RollupDeliveryReason::Terminal);
        assert_eq!(string_data(&terminal[0].message), "a");
        assert_eq!(engine.active_streams(), 0);
    }

    #[test]
    fn update_flushes_pending_before_terminal() {
        let engine = RollupEngine::new(RollupConfig::default());
        assert_eq!(
            engine
                .ingest("app", "ai:room", append("m1", "a", 1), 0)
                .len(),
            1
        );
        assert!(
            engine
                .ingest("app", "ai:room", append("m1", "ab", 2), 10)
                .is_empty()
        );

        let out = engine.ingest("app", "ai:room", update("m1"), 11);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].reason, RollupDeliveryReason::TerminalFlush);
        assert_eq!(out[1].reason, RollupDeliveryReason::Terminal);
        assert_eq!(engine.active_streams(), 0);
    }

    #[test]
    fn zero_window_bypasses_rollup() {
        let engine = RollupEngine::new(RollupConfig {
            window_ms: 0,
            ..RollupConfig::default()
        });

        let first = engine.ingest("app", "ai:room", append("m1", "a", 1), 0);
        let second = engine.ingest("app", "ai:room", append("m1", "ab", 2), 1);

        assert_eq!(first[0].reason, RollupDeliveryReason::Bypass);
        assert_eq!(second[0].reason, RollupDeliveryReason::Bypass);
        assert_eq!(engine.active_streams(), 0);
    }

    #[test]
    fn orphan_sweep_flushes_and_removes_state() {
        let engine = RollupEngine::new(RollupConfig {
            orphan_ttl_ms: 50,
            ..RollupConfig::default()
        });
        assert_eq!(
            engine
                .ingest("app", "ai:room", append("m1", "a", 1), 0)
                .len(),
            1
        );
        assert!(
            engine
                .ingest("app", "ai:room", append("m1", "ab", 2), 10)
                .is_empty()
        );

        let out = engine.sweep_orphans(60);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].reason, RollupDeliveryReason::Orphan);
        assert_eq!(string_data(&out[0].message), "ab");
        assert_eq!(engine.active_streams(), 0);
    }

    #[test]
    fn orphan_sweep_drops_first_only_state_without_duplicate_delivery() {
        let engine = RollupEngine::new(RollupConfig {
            orphan_ttl_ms: 50,
            ..RollupConfig::default()
        });
        assert_eq!(
            engine
                .ingest("app", "ai:room", append("m1", "a", 1), 0)
                .len(),
            1
        );

        let out = engine.sweep_orphans(60);
        assert!(out.is_empty());
        assert_eq!(engine.active_streams(), 0);
    }

    #[test]
    fn active_stream_cap_bypasses_new_rollup_state() {
        let engine = RollupEngine::new(RollupConfig {
            max_active_streams_per_channel: 1,
            ..RollupConfig::default()
        });

        let first = engine.ingest("app", "ai:room", append("m1", "a", 1), 0);
        let second = engine.ingest("app", "ai:room", append("m2", "b", 2), 1);

        assert_eq!(first[0].reason, RollupDeliveryReason::Immediate);
        assert_eq!(second[0].reason, RollupDeliveryReason::Bypass);
        assert_eq!(engine.active_streams(), 1);

        let terminal = engine.ingest("app", "ai:room", terminal_append("m1", "a"), 2);
        assert_eq!(terminal.len(), 1);
        assert_eq!(terminal[0].reason, RollupDeliveryReason::Terminal);
        assert_eq!(engine.active_streams(), 0);

        let third = engine.ingest("app", "ai:room", append("m2", "bc", 3), 3);
        assert_eq!(third[0].reason, RollupDeliveryReason::Immediate);
        assert_eq!(engine.active_streams(), 1);
    }

    #[test]
    fn allowed_window_values_match_wire_contract() {
        for value in [0, 20, 40, 100, 500] {
            assert!(is_allowed_append_rollup_window_ms(value));
        }
        for value in [1, 39, 250, 501] {
            assert!(!is_allowed_append_rollup_window_ms(value));
        }
    }
}
