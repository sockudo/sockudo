//! AI Transport egress and observability helpers.

pub mod observability;

use ahash::AHashMap;
use sockudo_protocol::messages::PusherMessage;
use sockudo_protocol::versioned_messages::{
    MessageAction, extract_runtime_action, extract_runtime_message_serial,
};
use std::hash::Hash;
use std::sync::Mutex;

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollupDeliveryReason {
    Immediate,
    Deadline,
    TerminalFlush,
    Terminal,
    Bypass,
    Orphan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollupStats {
    pub appends_received: u64,
    pub appends_delivered: u64,
    pub active_streams: usize,
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

#[derive(Debug, Clone)]
struct PendingStream {
    first_seen_ms: u64,
    last_seen_ms: u64,
    deadline_ms: u64,
    latest_message: PusherMessage,
    appended_count: usize,
}

impl PendingStream {
    fn new(first_seen_ms: u64, deadline_ms: u64, latest_message: PusherMessage) -> Self {
        Self {
            first_seen_ms,
            last_seen_ms: first_seen_ms,
            deadline_ms,
            latest_message,
            appended_count: 0,
        }
    }

    fn push(&mut self, now_ms: u64, message: PusherMessage) {
        self.last_seen_ms = now_ms;
        self.latest_message = message;
        self.appended_count += 1;
    }

    fn flush(
        &mut self,
        reason: RollupDeliveryReason,
        app_id: &str,
        channel: &str,
    ) -> RollupDelivery {
        let coalesced = self.appended_count.max(1);
        RollupDelivery {
            app_id: app_id.to_string(),
            channel: channel.to_string(),
            message: self.latest_message.clone(),
            reason,
            coalesced,
            latency_ms: self.last_seen_ms.saturating_sub(self.first_seen_ms),
        }
    }
}

struct Shard {
    streams: Mutex<AHashMap<StreamKey, PendingStream>>,
}

/// Coalesces versioned `message.append` fan-out without changing persistence.
///
/// The engine is synchronous by design. Callers take the returned deliveries
/// outside the engine lock and perform async I/O separately.
pub struct RollupEngine {
    config: RollupConfig,
    shards: Vec<Shard>,
    active_by_channel: Mutex<AHashMap<ChannelKey, usize>>,
    appends_received: std::sync::atomic::AtomicU64,
    appends_delivered: std::sync::atomic::AtomicU64,
}

impl RollupEngine {
    #[must_use]
    pub fn new(config: RollupConfig) -> Self {
        let shard_count = config.shards.max(1);
        let shards = (0..shard_count)
            .map(|_| Shard {
                streams: Mutex::new(AHashMap::new()),
            })
            .collect();
        Self {
            config,
            shards,
            active_by_channel: Mutex::new(AHashMap::new()),
            appends_received: std::sync::atomic::AtomicU64::new(0),
            appends_delivered: std::sync::atomic::AtomicU64::new(0),
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
        let Some(action) = extract_runtime_action(&message) else {
            return vec![bypass(app_id, channel, message)];
        };
        let Some(message_serial) = extract_runtime_message_serial(&message).map(ToOwned::to_owned)
        else {
            return vec![bypass(app_id, channel, message)];
        };

        if !self.config.enabled || self.config.window_ms == 0 {
            if action == MessageAction::Append {
                self.mark_received();
                self.mark_delivered(1);
            }
            return vec![bypass(app_id, channel, message)];
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
        let mut deliveries = Vec::with_capacity(2);
        let mut streams = shard.streams.lock().unwrap_or_else(|err| err.into_inner());

        if matches!(action, MessageAction::Update | MessageAction::Delete) {
            if let Some(mut pending) = streams.remove(&key) {
                self.decrement_channel(&channel_key);
                deliveries.push(pending.flush(
                    RollupDeliveryReason::TerminalFlush,
                    app_id,
                    channel,
                ));
                self.mark_delivered(1);
            }
            deliveries.push(delivery(
                app_id,
                channel,
                message,
                RollupDeliveryReason::Terminal,
                1,
                0,
            ));
            return deliveries;
        }

        if action != MessageAction::Append {
            return vec![bypass(app_id, channel, message)];
        }

        self.mark_received();
        if is_terminal_append(&message) {
            if let Some(mut pending) = streams.remove(&key) {
                self.decrement_channel(&channel_key);
                deliveries.push(pending.flush(
                    RollupDeliveryReason::TerminalFlush,
                    app_id,
                    channel,
                ));
                self.mark_delivered(1);
            }
            deliveries.push(delivery(
                app_id,
                channel,
                message,
                RollupDeliveryReason::Terminal,
                1,
                0,
            ));
            self.mark_delivered(1);
            return deliveries;
        }

        if let Some(pending) = streams.get_mut(&key) {
            if now_ms >= pending.deadline_ms {
                let flushed = pending.flush(RollupDeliveryReason::Deadline, app_id, channel);
                self.mark_delivered(1);
                *pending =
                    PendingStream::new(now_ms, now_ms + self.config.window_ms, message.clone());
                deliveries.push(flushed);
                deliveries.push(delivery(
                    app_id,
                    channel,
                    message,
                    RollupDeliveryReason::Immediate,
                    1,
                    0,
                ));
                self.mark_delivered(1);
            } else {
                pending.push(now_ms, message);
            }
        } else {
            if !self.try_increment_channel(&channel_key) {
                self.mark_delivered(1);
                return vec![bypass(app_id, channel, message)];
            }
            streams.insert(
                key,
                PendingStream::new(now_ms, now_ms + self.config.window_ms, message.clone()),
            );
            deliveries.push(delivery(
                app_id,
                channel,
                message,
                RollupDeliveryReason::Immediate,
                1,
                0,
            ));
            self.mark_delivered(1);
        }

        deliveries
    }

    /// Flush streams whose rollup window has elapsed.
    #[must_use]
    pub fn flush_due(&self, now_ms: u64) -> Vec<RollupDelivery> {
        self.collect_matching(
            now_ms,
            |pending, now| {
                if pending.appended_count == 0 {
                    false
                } else {
                    now >= pending.deadline_ms
                }
            },
            RollupDeliveryReason::Deadline,
        )
    }

    /// Flush and drop streams abandoned past the configured orphan TTL.
    #[must_use]
    pub fn sweep_orphans(&self, now_ms: u64) -> Vec<RollupDelivery> {
        let mut deliveries = Vec::new();
        for shard in &self.shards {
            let mut streams = shard.streams.lock().unwrap_or_else(|err| err.into_inner());
            let keys = streams
                .iter()
                .filter_map(|(key, pending)| {
                    (now_ms.saturating_sub(pending.last_seen_ms) >= self.config.orphan_ttl_ms)
                        .then_some(key.clone())
                })
                .collect::<Vec<_>>();
            for key in keys {
                if let Some(mut pending) = streams.remove(&key) {
                    self.decrement_channel(&ChannelKey {
                        app_id: key.app_id.clone(),
                        channel: key.channel.clone(),
                    });
                    if pending.appended_count > 0 {
                        deliveries.push(pending.flush(
                            RollupDeliveryReason::Orphan,
                            &key.app_id,
                            &key.channel,
                        ));
                    }
                }
            }
        }
        self.mark_delivered(deliveries.len() as u64);
        deliveries
    }

    #[must_use]
    pub fn active_streams(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .streams
                    .lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .len()
            })
            .sum()
    }

    #[must_use]
    pub fn stats(&self) -> RollupStats {
        RollupStats {
            appends_received: self
                .appends_received
                .load(std::sync::atomic::Ordering::Relaxed),
            appends_delivered: self
                .appends_delivered
                .load(std::sync::atomic::Ordering::Relaxed),
            active_streams: self.active_streams(),
        }
    }

    fn collect_matching<F>(
        &self,
        now_ms: u64,
        should_flush: F,
        reason: RollupDeliveryReason,
    ) -> Vec<RollupDelivery>
    where
        F: Fn(&PendingStream, u64) -> bool,
    {
        let mut deliveries = Vec::new();
        for shard in &self.shards {
            let mut streams = shard.streams.lock().unwrap_or_else(|err| err.into_inner());
            let keys = streams
                .iter()
                .filter_map(|(key, pending)| should_flush(pending, now_ms).then_some(key.clone()))
                .collect::<Vec<_>>();
            for key in keys {
                if let Some(mut pending) = streams.remove(&key) {
                    self.decrement_channel(&ChannelKey {
                        app_id: key.app_id.clone(),
                        channel: key.channel.clone(),
                    });
                    deliveries.push(pending.flush(reason, &key.app_id, &key.channel));
                }
            }
        }
        self.mark_delivered(deliveries.len() as u64);
        deliveries
    }

    #[inline]
    fn shard(&self, key: &StreamKey) -> &Shard {
        let index = fast_shard_index(key, self.shards.len());
        &self.shards[index]
    }

    #[inline]
    fn mark_received(&self) {
        self.appends_received
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline]
    fn mark_delivered(&self, count: u64) {
        self.appends_delivered
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    }

    fn try_increment_channel(&self, key: &ChannelKey) -> bool {
        let mut active = self
            .active_by_channel
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let count = active.entry(key.clone()).or_insert(0);
        if *count >= self.config.max_active_streams_per_channel {
            return false;
        }
        *count += 1;
        true
    }

    fn decrement_channel(&self, key: &ChannelKey) {
        let mut active = self
            .active_by_channel
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if let Some(count) = active.get_mut(key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                active.remove(key);
            }
        }
    }
}

#[inline]
#[must_use]
pub fn is_allowed_append_rollup_window_ms(window_ms: u64) -> bool {
    ALLOWED_APPEND_ROLLUP_WINDOWS_MS.contains(&window_ms)
}

fn bypass(app_id: &str, channel: &str, message: PusherMessage) -> RollupDelivery {
    delivery(app_id, channel, message, RollupDeliveryReason::Bypass, 1, 0)
}

fn delivery(
    app_id: &str,
    channel: &str,
    message: PusherMessage,
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
    for bytes in [
        key.app_id.as_bytes(),
        key.channel.as_bytes(),
        key.message_serial.as_bytes(),
    ] {
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
        assert_eq!(terminal[0].reason, RollupDeliveryReason::TerminalFlush);
        assert_eq!(terminal[1].reason, RollupDeliveryReason::Terminal);
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
