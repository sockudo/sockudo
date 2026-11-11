use super::ConnectionHandler;
use ahash::AHashMap;
use bytes::Bytes;
use serde::Deserialize;
use sockudo_core::app::App;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::{HistoryDirection, HistoryQueryBounds, HistoryReadRequest};
use sockudo_core::websocket::SocketId;
use sockudo_protocol::messages::{MessageData, PusherMessage};
use sonic_rs::{JsonValueMutTrait, json};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use crate::replay_buffer::ReplayLookup;

#[derive(Debug, Clone)]
struct ResumePosition {
    serial: u64,
    stream_id: Option<String>,
    last_message_id: Option<String>,
    legacy_serial_only: bool,
}

#[derive(Debug, Clone)]
struct ResumeFailure {
    code: &'static str,
    reason: &'static str,
    expected_stream_id: Option<String>,
    current_stream_id: Option<String>,
    oldest_available_serial: Option<u64>,
    newest_available_serial: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ResumeDeliveryPosition {
    stream_id: Option<String>,
    serial: u64,
    last_message_id: Option<String>,
}

/// The last position a recovery replay delivered for a channel.
#[derive(Debug, Clone)]
pub(crate) struct ReplayedThrough {
    stream_id: Option<String>,
    serial: u64,
}

impl ReplayedThrough {
    /// Whether a held live message was already delivered by the replay. Serials are only
    /// comparable within one stream generation, so a message from another stream never is.
    fn covers(&self, stream_id: Option<&str>, serial: Option<u64>) -> bool {
        stream_id == self.stream_id.as_deref() && serial.is_some_and(|serial| serial <= self.serial)
    }
}

struct ResumeDelivery {
    delivered: usize,
    position: Option<ResumeDeliveryPosition>,
}

#[derive(Debug, Clone)]
enum ResumeOutcome {
    Recovered {
        source: &'static str,
        messages: Vec<Bytes>,
    },
    Failed(ResumeFailure),
}

fn map_history_read_error(position: &ResumePosition, error: Error) -> ResumeFailure {
    match error {
        Error::InvalidMessageFormat(message) if message.contains("channel stream changed") => {
            ResumeFailure {
                code: "stream_reset",
                reason: "durable_stream_changed_during_reconnect",
                expected_stream_id: position.stream_id.clone(),
                current_stream_id: None,
                oldest_available_serial: None,
                newest_available_serial: None,
            }
        }
        Error::InvalidMessageFormat(message)
            if message.contains("cursor points before retained history") =>
        {
            ResumeFailure {
                code: "position_expired",
                reason: "retention_boundary_crossed_during_reconnect",
                expected_stream_id: position.stream_id.clone(),
                current_stream_id: None,
                oldest_available_serial: None,
                newest_available_serial: None,
            }
        }
        _ => ResumeFailure {
            code: "persistence_unavailable",
            reason: "durable_history_read_failed",
            expected_stream_id: position.stream_id.clone(),
            current_stream_id: None,
            oldest_available_serial: None,
            newest_available_serial: None,
        },
    }
}

fn continuity_failure(
    position: &ResumePosition,
    reason: &'static str,
    current_stream_id: Option<String>,
    oldest_available_serial: Option<u64>,
    newest_available_serial: Option<u64>,
) -> ResumeFailure {
    ResumeFailure {
        code: "continuity_unverifiable",
        reason,
        expected_stream_id: position.stream_id.clone(),
        current_stream_id,
        oldest_available_serial,
        newest_available_serial,
    }
}

fn recovery_window_failure(
    position: &ResumePosition,
    current_stream_id: Option<String>,
    oldest_available_serial: Option<u64>,
    newest_available_serial: Option<u64>,
) -> ResumeFailure {
    ResumeFailure {
        code: "position_expired",
        reason: "recovery_window_exceeded",
        expected_stream_id: position.stream_id.clone(),
        current_stream_id,
        oldest_available_serial,
        newest_available_serial,
    }
}

/// How long a cold resume waits for reserved serials to be committed.
const DURABLE_WRITE_CATCH_UP_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);
const DURABLE_WRITE_CATCH_UP_POLL: std::time::Duration = std::time::Duration::from_millis(10);

/// Highest serial reserved for publishing on the durable stream, which can be
/// ahead of the committed `newest_serial` while appends are in flight.
fn reserved_durable_head(
    inspection: &sockudo_core::history::HistoryStreamInspection,
) -> Option<u64> {
    let reserved = inspection
        .next_serial
        .map(|next| next.saturating_sub(1))
        .filter(|&serial| serial > 0);
    match (reserved, inspection.retained.newest_serial) {
        (Some(reserved), Some(newest)) => Some(reserved.max(newest)),
        (reserved, newest) => reserved.or(newest),
    }
}

fn exceeds_recovery_window(count: u64, max_replay_messages: usize) -> bool {
    count > u64::try_from(max_replay_messages).unwrap_or(u64::MAX)
}

/// Upper bound on resume positions parked per socket while their subscriptions are pending.
pub(crate) const MAX_PENDING_RESUME_CHANNELS: usize = 100;
/// Parked resume positions older than this fail closed instead of replaying.
pub(crate) const PENDING_RESUME_TTL: Duration = Duration::from_secs(30);
/// Channel names longer than this are never parked (Pusher caps names at 200 bytes).
const MAX_PENDING_RESUME_CHANNEL_BYTES: usize = 256;
/// Bound on the client-supplied identifiers a parked position retains, so an idle socket
/// cannot hold arbitrary bytes. Stream ids are UUIDs or `{app_id}/{channel}` (bounded by the
/// channel cap and always accepted); `last_message_id` only suppresses one duplicate
/// identity during durable recovery, so an oversized one is dropped rather than refusing
/// the whole position (the publish API may accept longer message ids).
const MAX_PENDING_RESUME_ID_BYTES: usize = 128;

/// A resume position received before the socket held the matching subscription.
#[derive(Debug, Clone)]
pub(crate) struct PendingResume {
    position: ResumePosition,
    received_at: Instant,
    /// Lets the periodic sweep tell the socket its parked position expired.
    app_id: String,
}

/// Per-socket parked resume positions keyed by channel. Bounded by
/// [`MAX_PENDING_RESUME_CHANNELS`] and removed on disconnect.
pub(crate) type PendingResumes = AHashMap<String, PendingResume>;

/// Generic failure for channels the socket may not read. It deliberately carries no
/// stream metadata so it cannot be used to probe channels (GHSA-r5q6-rw3h-q24r).
fn access_denied_failure(code: &'static str, reason: &'static str) -> ResumeFailure {
    ResumeFailure {
        code,
        reason,
        expected_stream_id: None,
        current_stream_id: None,
        oldest_available_serial: None,
        newest_available_serial: None,
    }
}

impl ConnectionHandler {
    /// Handle a `pusher:resume` / `sockudo:resume` event from a reconnecting client.
    ///
    /// V2 clients should send `channel_positions` with `{ stream_id, serial }`.
    /// Legacy `channel_serials` (and positions without `stream_id`) are accepted for the
    /// hot-buffer fast path only.
    ///
    /// Recovery is only served for channels the socket is subscribed to. Clients send
    /// `resume` right after re-subscribing, while private/presence subscribes may still be
    /// waiting on the auth endpoint, so positions for channels that are not subscribed yet
    /// are parked (bounded per socket and in time) and replayed once that channel's
    /// subscription succeeds. They are dropped if the subscription fails or the socket
    /// disconnects.
    pub async fn handle_resume(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        message: &PusherMessage,
    ) -> Result<()> {
        let replay_buffer = match &self.replay_buffer {
            Some(rb) => rb,
            None => {
                self.send_message_to_socket(
                    &app_config.id,
                    socket_id,
                    PusherMessage::error(
                        4301,
                        "Connection recovery is not enabled".to_string(),
                        None,
                    ),
                )
                .await?;
                return Ok(());
            }
        };

        let channel_positions = parse_resume_positions(message)?;
        if channel_positions.is_empty() {
            self.send_message_to_socket(
                &app_config.id,
                socket_id,
                PusherMessage::error(
                    4302,
                    "No channel recovery positions provided".to_string(),
                    None,
                ),
            )
            .await?;
            return Ok(());
        }

        let Some(connection) = self
            .connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
        else {
            return Err(Error::ConnectionNotFound);
        };

        // Positions parked by an earlier resume that expired before their subscription
        // arrived get an explicit outcome instead of disappearing.
        let mut failed = Vec::new();
        for channel in self.take_expired_pending_resumes(socket_id) {
            let failure = access_denied_failure("not_subscribed", "pending_resume_expired");
            self.record_resume_failure(socket_id, app_config, &channel, &failure, &mut failed)
                .await;
        }

        let mut ready = Vec::new();
        let mut pending = Vec::new();
        let mut rejected = Vec::new();
        for (channel, position) in channel_positions {
            if crate::v2_broadcast::has_matching_subscription(&connection, &channel) {
                if self
                    .resume_access_allowed(socket_id, app_config, &channel)
                    .await
                {
                    ready.push((channel, position));
                } else {
                    rejected.push((channel, "channel_access_denied"));
                }
            } else if self.park_pending_resume(socket_id, &app_config.id, &channel, position) {
                pending.push(channel);
            } else {
                rejected.push((channel, "pending_resume_limit_exceeded"));
            }
        }

        for (channel, reason) in &rejected {
            let failure = access_denied_failure("not_subscribed", reason);
            self.record_resume_failure(socket_id, app_config, channel, &failure, &mut failed)
                .await;
        }
        if !rejected.is_empty() || !pending.is_empty() {
            debug!(
                app_id = %app_config.id,
                socket_id = %socket_id,
                rejected = rejected.len(),
                pending = pending.len(),
                "resume positions for unsubscribed channels were not replayed"
            );
        }

        self.run_resume(
            socket_id,
            app_config,
            &connection,
            replay_buffer,
            ready,
            failed,
            pending,
        )
        .await
    }

    /// Replay parked resume positions once a subscription succeeded, or drop them when it
    /// failed. Called after every subscribe attempt on sockets that have parked positions.
    pub(crate) async fn resume_pending_after_subscribe(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        channel: &str,
        subscribed: bool,
    ) -> Result<()> {
        if !subscribed {
            let dropped = self.take_pending_resumes(socket_id, |pending| pending == channel);
            if !dropped.is_empty() {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.mark_history_recovery_failure(&app_config.id, "subscription_failed");
                }
                debug!(
                    app_id = %app_config.id,
                    socket_id = %socket_id,
                    channel,
                    "dropped pending resume after failed subscription"
                );
            }
            return Ok(());
        }

        let Some(replay_buffer) = self.replay_buffer.as_ref() else {
            return Ok(());
        };
        let Some(connection) = self
            .connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
        else {
            self.pending_resumes.remove(socket_id);
            return Ok(());
        };

        // A wildcard subscribe can satisfy several parked public channels at once.
        let matched = self.take_pending_resumes(socket_id, |pending| {
            pending == channel
                || crate::v2_broadcast::has_matching_subscription(&connection, pending)
        });
        if matched.is_empty() {
            return Ok(());
        }

        let mut ready = Vec::new();
        let mut failed = Vec::new();
        for (pending_channel, entry) in matched {
            let reason = if entry.received_at.elapsed() > PENDING_RESUME_TTL {
                Some("pending_resume_expired")
            } else if !crate::v2_broadcast::has_matching_subscription(&connection, &pending_channel)
                || !self
                    .resume_access_allowed(socket_id, app_config, &pending_channel)
                    .await
            {
                Some("channel_access_denied")
            } else {
                None
            };
            match reason {
                Some(reason) => {
                    let failure = access_denied_failure("not_subscribed", reason);
                    self.record_resume_failure(
                        socket_id,
                        app_config,
                        &pending_channel,
                        &failure,
                        &mut failed,
                    )
                    .await;
                }
                None => ready.push((pending_channel, entry.position)),
            }
        }

        self.run_resume(
            socket_id,
            app_config,
            &connection,
            replay_buffer,
            ready,
            failed,
            Vec::new(),
        )
        .await
    }

    /// Same channel-level checks `channel_history` applies, beyond holding the
    /// subscription: user-limited channel access and the connection's capabilities.
    async fn resume_access_allowed(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        channel: &str,
    ) -> bool {
        self.validate_v2_channel_access(socket_id, app_config, channel)
            .await
            .is_ok()
            && self
                .validate_v2_capability(socket_id, app_config, channel, "subscribe")
                .await
                .is_ok()
    }

    /// Whether `channel` has a parked resume on this socket. The subscribe path then leaves
    /// its continuity gate for the recovery replay to drain, so held live traffic is released
    /// after the older replayed messages instead of before them.
    pub(crate) fn has_pending_resume(&self, socket_id: &SocketId, channel: &str) -> bool {
        self.pending_resumes
            .get(socket_id)
            .is_some_and(|pending| pending.contains_key(channel))
    }

    /// Parked channels a subscribe to `subscribed` will satisfy: the exact channel, or every
    /// parked public channel a wildcard pattern matches. Bounded by
    /// [`MAX_PENDING_RESUME_CHANNELS`].
    pub(crate) fn pending_channels_for_subscription(
        &self,
        socket_id: &SocketId,
        subscribed: &str,
    ) -> Vec<String> {
        let is_pattern = sockudo_core::utils::is_wildcard_subscription_pattern(subscribed);
        self.pending_resumes
            .get(socket_id)
            .map(|pending| {
                pending
                    .keys()
                    .filter(|channel| {
                        channel.as_str() == subscribed
                            || (is_pattern
                                && sockudo_core::utils::channel_allows_wildcard_delivery(channel)
                                && sockudo_core::utils::wildcard_pattern_matches(
                                    channel, subscribed,
                                ))
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    fn park_pending_resume(
        &self,
        socket_id: &SocketId,
        app_id: &str,
        channel: &str,
        mut position: ResumePosition,
    ) -> bool {
        if channel.is_empty() || channel.len() > MAX_PENDING_RESUME_CHANNEL_BYTES {
            return false;
        }
        if position.stream_id.as_deref().is_some_and(|stream_id| {
            stream_id.len() > MAX_PENDING_RESUME_ID_BYTES
                && !is_channel_scoped_stream_id(stream_id, app_id, channel)
        }) {
            // No store issues such a stream id, so the position could never match anyway.
            return false;
        }
        if position
            .last_message_id
            .as_ref()
            .is_some_and(|id| id.len() > MAX_PENDING_RESUME_ID_BYTES)
        {
            position.last_message_id = None;
        }
        let mut pending = self.pending_resumes.entry(*socket_id).or_default();
        if !pending.contains_key(channel) && pending.len() >= MAX_PENDING_RESUME_CHANNELS {
            return false;
        }
        pending.insert(
            channel.to_string(),
            PendingResume {
                position,
                received_at: Instant::now(),
                app_id: app_id.to_string(),
            },
        );
        true
    }

    /// Remove this socket's parked positions that are older than [`PENDING_RESUME_TTL`] and
    /// return their channels so the caller can report them.
    fn take_expired_pending_resumes(&self, socket_id: &SocketId) -> Vec<String> {
        let Some(mut pending) = self.pending_resumes.get_mut(socket_id) else {
            return Vec::new();
        };
        let mut expired = Vec::new();
        pending.retain(|channel, entry| {
            let live = entry.received_at.elapsed() <= PENDING_RESUME_TTL;
            if !live {
                expired.push(channel.clone());
            }
            live
        });
        let now_empty = pending.is_empty();
        drop(pending);
        if now_empty {
            self.pending_resumes
                .remove_if(socket_id, |_, pending| pending.is_empty());
        }
        expired
    }

    /// Drop parked resume positions older than [`PENDING_RESUME_TTL`] on every socket, so
    /// idle connections do not retain them until disconnect, and send each affected socket a
    /// `resume_failed` (`pending_resume_expired`) so a channel reported as `pending` always
    /// gets an outcome. Called periodically by the server's recovery maintenance task;
    /// returns how many positions were dropped.
    pub async fn evict_expired_pending_resumes(&self) -> usize {
        self.evict_pending_resumes_older_than(PENDING_RESUME_TTL)
            .await
    }

    /// [`Self::evict_expired_pending_resumes`] with an explicit age limit.
    pub async fn evict_pending_resumes_older_than(&self, ttl: Duration) -> usize {
        let mut expired: Vec<(SocketId, String, String)> = Vec::new();
        self.pending_resumes.retain(|socket_id, pending| {
            pending.retain(|channel, entry| {
                let live = entry.received_at.elapsed() <= ttl;
                if !live {
                    expired.push((*socket_id, entry.app_id.clone(), channel.clone()));
                }
                live
            });
            !pending.is_empty()
        });

        let failure = access_denied_failure("not_subscribed", "pending_resume_expired");
        for (socket_id, app_id, channel) in &expired {
            // The socket may have disconnected meanwhile; there is nobody left to tell.
            let _ = self
                .send_resume_failed(socket_id, app_id, channel, &failure)
                .await;
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.mark_history_recovery_failure(app_id, failure.code);
            }
        }
        expired.len()
    }

    fn take_pending_resumes(
        &self,
        socket_id: &SocketId,
        mut matches: impl FnMut(&str) -> bool,
    ) -> Vec<(String, PendingResume)> {
        let Some(mut pending) = self.pending_resumes.get_mut(socket_id) else {
            return Vec::new();
        };
        let keys: Vec<String> = pending
            .keys()
            .filter(|channel| matches(channel))
            .cloned()
            .collect();
        let taken = keys
            .into_iter()
            .filter_map(|channel| pending.remove(&channel).map(|entry| (channel, entry)))
            .collect();
        let now_empty = pending.is_empty();
        drop(pending);
        if now_empty {
            self.pending_resumes
                .remove_if(socket_id, |_, pending| pending.is_empty());
        }
        taken
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_resume(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        connection: &sockudo_core::websocket::WebSocketRef,
        replay_buffer: &crate::replay_buffer::ReplayBuffer,
        ready: Vec<(String, ResumePosition)>,
        mut failed: Vec<sonic_rs::Value>,
        pending: Vec<String>,
    ) -> Result<()> {
        let mut recovered = Vec::new();
        let mut gated_channels = HashSet::new();
        let mut replayed_through: HashMap<String, ReplayedThrough> = HashMap::new();
        for (channel, _) in &ready {
            connection.start_rewind_gate(channel.clone());
            gated_channels.insert(channel.clone());
        }

        for (channel, position) in ready {
            let outcome = self
                .resume_channel(socket_id, app_config, replay_buffer, &channel, &position)
                .await;

            match outcome {
                ResumeOutcome::Recovered { source, messages } => {
                    let delivery = self
                        .send_replayed_messages(socket_id, &app_config.id, &channel, messages)
                        .await;
                    let delivery = match delivery {
                        Ok(delivery) => delivery,
                        Err(failure) => {
                            self.record_resume_failure(
                                socket_id,
                                app_config,
                                &channel,
                                &failure,
                                &mut failed,
                            )
                            .await;
                            continue;
                        }
                    };
                    if let Some(metrics) = self.metrics.as_ref() {
                        metrics.mark_history_recovery_success(&app_config.id, source);
                    }
                    if let Some(position) = delivery.position.as_ref() {
                        replayed_through.insert(
                            channel.clone(),
                            ReplayedThrough {
                                stream_id: position.stream_id.clone(),
                                serial: position.serial,
                            },
                        );
                    }
                    recovered.push(json!({
                        "channel": channel,
                        "source": source,
                        "replayed": delivery.delivered,
                        "position": delivery.position,
                    }));
                }
                ResumeOutcome::Failed(failure) => {
                    self.record_resume_failure(
                        socket_id,
                        app_config,
                        &channel,
                        &failure,
                        &mut failed,
                    )
                    .await;
                }
            }
        }

        let mut summary = json!({
            "recovered": recovered,
            "failed": failed,
        });
        if !pending.is_empty()
            && let Some(object) = summary.as_object_mut()
        {
            object.insert("pending", json!(pending));
        }
        let success_msg = PusherMessage {
            event: Some(sockudo_protocol::constants::EVENT_RESUME_SUCCESS.to_string()),
            channel: None,
            data: Some(MessageData::String(summary.to_string())),
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
        if let Err(error) = self
            .send_message_to_socket(&app_config.id, socket_id, success_msg)
            .await
        {
            for channel in &gated_channels {
                let _ = connection.finish_rewind_gate(channel).await;
            }
            connection.shutdown();
            return Err(error);
        }

        for channel in gated_channels {
            let through = replayed_through.get(&channel);
            self.drain_recovery_gate(connection, socket_id, &app_config.id, &channel, through)
                .await?;
        }

        Ok(())
    }

    async fn resume_channel(
        &self,
        _socket_id: &SocketId,
        app_config: &App,
        replay_buffer: &crate::replay_buffer::ReplayBuffer,
        channel: &str,
        position: &ResumePosition,
    ) -> ResumeOutcome {
        let max_replay_messages = app_config
            .resolved_connection_recovery(&self.server_options().connection_recovery)
            .max_buffer_size;
        match replay_buffer.get_messages_after_position(
            &app_config.id,
            channel,
            position.stream_id.as_deref(),
            position.serial,
        ) {
            ReplayLookup::Recovered(messages) => {
                if messages.len() > max_replay_messages {
                    return ResumeOutcome::Failed(recovery_window_failure(
                        position,
                        position.stream_id.clone(),
                        None,
                        messages.last().and_then(|bytes| {
                            sonic_rs::from_slice::<PusherMessage>(bytes)
                                .ok()
                                .and_then(|message| message.serial)
                        }),
                    ));
                }
                let count = messages.len();
                if self
                    .hot_replay_reaches_durable_head(app_config, channel, position, count)
                    .await
                {
                    return ResumeOutcome::Recovered {
                        source: "hot",
                        messages,
                    };
                }
                warn!(
                    app_id = %app_config.id,
                    channel,
                    requested_serial = position.serial,
                    hot_replayed = count,
                    "hot replay ends before the durable reserved head; attempting durable recovery"
                );
            }
            ReplayLookup::StreamReset { current_stream_id } => {
                return ResumeOutcome::Failed(ResumeFailure {
                    code: "stream_reset",
                    reason: "stream_id_mismatch",
                    expected_stream_id: position.stream_id.clone(),
                    current_stream_id,
                    oldest_available_serial: None,
                    newest_available_serial: None,
                });
            }
            ReplayLookup::Ahead { newest_serial } => {
                warn!(
                    app_id = %app_config.id,
                    channel,
                    requested_serial = position.serial,
                    newest_serial,
                    "resume position is ahead of the node-local replay buffer; consulting durable recovery"
                );
            }
            ReplayLookup::ContinuityGap {
                expected_serial,
                observed_serial,
            } => {
                warn!(
                    app_id = %app_config.id,
                    channel,
                    expected_serial,
                    observed_serial,
                    "hot replay continuity check failed; attempting durable recovery"
                );
            }
            ReplayLookup::Expired => {}
        }

        if position.legacy_serial_only {
            return ResumeOutcome::Failed(ResumeFailure {
                code: "continuity_unverifiable",
                reason: "legacy_serial_only_position_cannot_use_cold_recovery",
                expected_stream_id: None,
                current_stream_id: None,
                oldest_available_serial: None,
                newest_available_serial: None,
            });
        }

        if self.server_options().versioned_messages.enabled {
            let stream_state = match self
                .version_store()
                .stream_state(&app_config.id, channel)
                .await
            {
                Ok(state) => state,
                Err(e) => {
                    warn!(error = %e, "version store state read failed");
                    return ResumeOutcome::Failed(ResumeFailure {
                        code: "persistence_unavailable",
                        reason: "version_store_state_unavailable",
                        expected_stream_id: position.stream_id.clone(),
                        current_stream_id: None,
                        oldest_available_serial: None,
                        newest_available_serial: None,
                    });
                }
            };

            if stream_state.stream_id.as_deref() != position.stream_id.as_deref() {
                return ResumeOutcome::Failed(ResumeFailure {
                    code: "stream_reset",
                    reason: "version_stream_id_mismatch",
                    expected_stream_id: position.stream_id.clone(),
                    current_stream_id: stream_state.stream_id.clone(),
                    oldest_available_serial: stream_state.oldest_available_delivery_serial,
                    newest_available_serial: stream_state.newest_available_delivery_serial,
                });
            }

            if stream_state
                .newest_available_delivery_serial
                .is_some_and(|newest| position.serial > newest)
            {
                return ResumeOutcome::Failed(ResumeFailure {
                    code: "continuity_unverifiable",
                    reason: "position_ahead_of_version_stream",
                    expected_stream_id: position.stream_id.clone(),
                    current_stream_id: stream_state.stream_id.clone(),
                    oldest_available_serial: stream_state.oldest_available_delivery_serial,
                    newest_available_serial: stream_state.newest_available_delivery_serial,
                });
            }

            if stream_state.newest_available_delivery_serial.is_none()
                && let Some(next_serial) = stream_state.next_delivery_serial
            {
                let requested_next = position.serial.saturating_add(1);
                if requested_next < next_serial {
                    return ResumeOutcome::Failed(ResumeFailure {
                        code: "position_expired",
                        reason: "version_replay_empty_after_prior_writes",
                        expected_stream_id: position.stream_id.clone(),
                        current_stream_id: stream_state.stream_id.clone(),
                        oldest_available_serial: None,
                        newest_available_serial: None,
                    });
                }
                if requested_next > next_serial {
                    return ResumeOutcome::Failed(ResumeFailure {
                        code: "continuity_unverifiable",
                        reason: "position_ahead_of_version_stream",
                        expected_stream_id: position.stream_id.clone(),
                        current_stream_id: stream_state.stream_id.clone(),
                        oldest_available_serial: None,
                        newest_available_serial: None,
                    });
                }
            }
            if stream_state.newest_available_delivery_serial.is_none()
                && stream_state.next_delivery_serial.is_none()
            {
                return ResumeOutcome::Failed(ResumeFailure {
                    code: "continuity_unverifiable",
                    reason: "version_stream_head_unavailable",
                    expected_stream_id: position.stream_id.clone(),
                    current_stream_id: stream_state.stream_id.clone(),
                    oldest_available_serial: None,
                    newest_available_serial: None,
                });
            }

            if stream_state
                .oldest_available_delivery_serial
                .is_some_and(|oldest| position.serial.saturating_add(1) < oldest)
            {
                return ResumeOutcome::Failed(ResumeFailure {
                    code: "position_expired",
                    reason: "version_replay_floor_is_ahead_of_requested_serial",
                    expected_stream_id: position.stream_id.clone(),
                    current_stream_id: stream_state.stream_id.clone(),
                    oldest_available_serial: stream_state.oldest_available_delivery_serial,
                    newest_available_serial: stream_state.newest_available_delivery_serial,
                });
            }

            if let Some(newest) = stream_state.newest_available_delivery_serial
                && exceeds_recovery_window(
                    newest.saturating_sub(position.serial),
                    max_replay_messages,
                )
            {
                return ResumeOutcome::Failed(recovery_window_failure(
                    position,
                    stream_state.stream_id.clone(),
                    stream_state.oldest_available_delivery_serial,
                    stream_state.newest_available_delivery_serial,
                ));
            }

            let replay_limit = stream_state
                .newest_available_delivery_serial
                .map(|newest| newest.saturating_sub(position.serial) as usize)
                .unwrap_or(0)
                .max(1);

            let records = match self
                .version_store()
                .replay_after(sockudo_core::version_store::VersionReplayRequest {
                    app_id: app_config.id.clone(),
                    channel: channel.to_string(),
                    after_delivery_serial: position.serial,
                    limit: replay_limit,
                })
                .await
            {
                Ok(records) => records,
                Err(e) => {
                    warn!(error = %e, "version store replay failed");
                    return ResumeOutcome::Failed(ResumeFailure {
                        code: "persistence_unavailable",
                        reason: "version_store_replay_failed",
                        expected_stream_id: position.stream_id.clone(),
                        current_stream_id: stream_state.stream_id.clone(),
                        oldest_available_serial: stream_state.oldest_available_delivery_serial,
                        newest_available_serial: stream_state.newest_available_delivery_serial,
                    });
                }
            };

            let mut expected_serial = position.serial.saturating_add(1);
            for record in &records {
                if record.delivery_serial() != expected_serial {
                    return ResumeOutcome::Failed(ResumeFailure {
                        code: "continuity_unverifiable",
                        reason: "version_replay_is_not_contiguous",
                        expected_stream_id: position.stream_id.clone(),
                        current_stream_id: stream_state.stream_id.clone(),
                        oldest_available_serial: stream_state.oldest_available_delivery_serial,
                        newest_available_serial: stream_state.newest_available_delivery_serial,
                    });
                }
                expected_serial = expected_serial.saturating_add(1);
            }
            if stream_state
                .newest_available_delivery_serial
                .is_some_and(|newest| expected_serial <= newest)
            {
                return ResumeOutcome::Failed(ResumeFailure {
                    code: "continuity_unverifiable",
                    reason: "version_replay_ended_before_stream_head",
                    expected_stream_id: position.stream_id.clone(),
                    current_stream_id: stream_state.stream_id.clone(),
                    oldest_available_serial: stream_state.oldest_available_delivery_serial,
                    newest_available_serial: stream_state.newest_available_delivery_serial,
                });
            }

            let mut messages = Vec::with_capacity(records.len());
            for record in records {
                let message =
                    self.build_runtime_message_from_record(&record, stream_state.stream_id.clone());
                let bytes =
                    sonic_rs::to_vec(&message)
                        .map(Bytes::from)
                        .map_err(|_| ResumeFailure {
                            code: "persistence_unavailable",
                            reason: "version_store_payload_serialization_failed",
                            expected_stream_id: position.stream_id.clone(),
                            current_stream_id: stream_state.stream_id.clone(),
                            oldest_available_serial: stream_state.oldest_available_delivery_serial,
                            newest_available_serial: stream_state.newest_available_delivery_serial,
                        });
                match bytes {
                    Ok(bytes) => messages.push(bytes),
                    Err(failure) => return ResumeOutcome::Failed(failure),
                }
            }

            return ResumeOutcome::Recovered {
                source: "cold",
                messages,
            };
        }

        let history_policy = app_config.resolved_history(channel, &self.server_options().history);
        if !history_policy.enabled {
            return ResumeOutcome::Failed(ResumeFailure {
                code: "position_expired",
                reason: "hot_replay_expired_and_durable_history_disabled",
                expected_stream_id: position.stream_id.clone(),
                current_stream_id: None,
                oldest_available_serial: None,
                newest_available_serial: None,
            });
        }

        let stream_runtime = match self
            .history_store()
            .stream_runtime_state(&app_config.id, channel)
            .await
        {
            Ok(state) => state,
            Err(e) => {
                warn!(error = %e, "history store state read failed");
                return ResumeOutcome::Failed(ResumeFailure {
                    code: "persistence_unavailable",
                    reason: "durable_history_state_unavailable",
                    expected_stream_id: position.stream_id.clone(),
                    current_stream_id: None,
                    oldest_available_serial: None,
                    newest_available_serial: None,
                });
            }
        };
        if !stream_runtime.recovery_allowed {
            return ResumeOutcome::Failed(ResumeFailure {
                code: if stream_runtime.reset_required {
                    "stream_reset"
                } else {
                    "persistence_unavailable"
                },
                reason: if stream_runtime.reset_required {
                    "durable_stream_reset_required"
                } else {
                    "history_stream_degraded"
                },
                expected_stream_id: position.stream_id.clone(),
                current_stream_id: stream_runtime.stream_id.clone(),
                oldest_available_serial: None,
                newest_available_serial: None,
            });
        }

        match self
            .collect_resume_from_history(
                app_config,
                channel,
                position,
                history_policy.max_page_size,
                max_replay_messages,
            )
            .await
        {
            Ok(messages) => ResumeOutcome::Recovered {
                source: "cold",
                messages,
            },
            Err(failure) => ResumeOutcome::Failed(failure),
        }
    }

    /// The replay buffer only holds this node's publishes, so a contiguous hot
    /// replay can still end before serials another node reserved and
    /// broadcast while the client was away. With durable history the shared
    /// reserved head is the authority; a shorter hot replay must go cold.
    async fn hot_replay_reaches_durable_head(
        &self,
        app_config: &App,
        channel: &str,
        position: &ResumePosition,
        replayed: usize,
    ) -> bool {
        if position.legacy_serial_only
            || self.server_options().versioned_messages.enabled
            || !app_config
                .resolved_history(channel, &self.server_options().history)
                .enabled
        {
            return true;
        }
        let replayed_through = position
            .serial
            .saturating_add(u64::try_from(replayed).unwrap_or(u64::MAX));
        match self
            .history_store()
            .stream_inspection(&app_config.id, channel)
            .await
        {
            // A stream with no reserved or committed serial has no shared head
            // to check against.
            Ok(inspection) => match reserved_durable_head(&inspection) {
                None => true,
                Some(reserved) => {
                    inspection
                        .stream_id
                        .as_deref()
                        .or(inspection.retained.stream_id.as_deref())
                        == position.stream_id.as_deref()
                        && reserved <= replayed_through
                }
            },
            Err(error) => {
                warn!(error = %error, "history store inspection failed during hot resume");
                false
            }
        }
    }

    async fn collect_resume_from_history(
        &self,
        app_config: &App,
        channel: &str,
        position: &ResumePosition,
        max_page_size: usize,
        max_replay_messages: usize,
    ) -> std::result::Result<Vec<Bytes>, ResumeFailure> {
        // Freeze the durable head before pagination. History cursors embed their query bounds, so
        // changing the end bound after page one would invalidate an otherwise healthy cursor.
        let mut frozen_inspection = self
            .history_store()
            .stream_inspection(&app_config.id, channel)
            .await
            .map_err(|error| map_history_read_error(position, error))?;
        // Publishes are broadcast once their serial is reserved, while the
        // durable append commits asynchronously. Recovering only up to the
        // committed head would skip a reserved serial the client never saw
        // live (e.g. published by another node while it was disconnected), so
        // wait a bounded time for in-flight writes and fail closed otherwise.
        // A stream with no committed rows (never written, or fully purged) is
        // left to the empty-stream checks below, which already fail closed.
        if frozen_inspection.retained.newest_serial.is_some()
            && let Some(reserved_head) = reserved_durable_head(&frozen_inspection)
            && position.serial < reserved_head
        {
            let deadline = tokio::time::Instant::now() + DURABLE_WRITE_CATCH_UP_TIMEOUT;
            while frozen_inspection
                .stream_id
                .as_deref()
                .or(frozen_inspection.retained.stream_id.as_deref())
                == position.stream_id.as_deref()
                && frozen_inspection
                    .retained
                    .newest_serial
                    .is_none_or(|newest| newest < reserved_head)
            {
                if tokio::time::Instant::now() >= deadline {
                    return Err(ResumeFailure {
                        code: "persistence_unavailable",
                        reason: "durable_history_write_in_flight",
                        expected_stream_id: position.stream_id.clone(),
                        current_stream_id: frozen_inspection.retained.stream_id,
                        oldest_available_serial: frozen_inspection.retained.oldest_serial,
                        newest_available_serial: frozen_inspection.retained.newest_serial,
                    });
                }
                tokio::time::sleep(DURABLE_WRITE_CATCH_UP_POLL).await;
                frozen_inspection = self
                    .history_store()
                    .stream_inspection(&app_config.id, channel)
                    .await
                    .map_err(|error| map_history_read_error(position, error))?;
            }
        }
        let frozen_head = frozen_inspection.retained;
        if frozen_head.stream_id.as_deref() != position.stream_id.as_deref() {
            return Err(ResumeFailure {
                code: "stream_reset",
                reason: "durable_stream_id_mismatch",
                expected_stream_id: position.stream_id.clone(),
                current_stream_id: frozen_head.stream_id,
                oldest_available_serial: frozen_head.oldest_serial,
                newest_available_serial: frozen_head.newest_serial,
            });
        }
        if frozen_head
            .newest_serial
            .is_some_and(|newest| position.serial > newest)
        {
            return Err(continuity_failure(
                position,
                "position_ahead_of_durable_stream",
                frozen_head.stream_id,
                frozen_head.oldest_serial,
                frozen_head.newest_serial,
            ));
        }
        if frozen_head.newest_serial.is_none()
            && let Some(next_serial) = frozen_inspection.next_serial
        {
            let requested_next = position.serial.saturating_add(1);
            if requested_next < next_serial {
                return Err(ResumeFailure {
                    code: "position_expired",
                    reason: "durable_history_empty_after_prior_writes",
                    expected_stream_id: position.stream_id.clone(),
                    current_stream_id: frozen_head.stream_id,
                    oldest_available_serial: None,
                    newest_available_serial: None,
                });
            }
            if requested_next > next_serial {
                return Err(continuity_failure(
                    position,
                    "position_ahead_of_durable_stream",
                    frozen_head.stream_id,
                    None,
                    None,
                ));
            }
        }
        if frozen_head.newest_serial.is_none() && frozen_inspection.next_serial.is_none() {
            return Err(continuity_failure(
                position,
                "durable_stream_head_unavailable",
                frozen_head.stream_id,
                None,
                None,
            ));
        }
        if frozen_head
            .oldest_serial
            .is_some_and(|oldest| position.serial.saturating_add(1) < oldest)
        {
            return Err(ResumeFailure {
                code: "position_expired",
                reason: "retained_history_floor_is_ahead_of_requested_serial",
                expected_stream_id: position.stream_id.clone(),
                current_stream_id: frozen_head.stream_id,
                oldest_available_serial: frozen_head.oldest_serial,
                newest_available_serial: frozen_head.newest_serial,
            });
        }

        let Some(target_newest_serial) = frozen_head.newest_serial else {
            return Ok(Vec::new());
        };
        if target_newest_serial == position.serial {
            return Ok(Vec::new());
        }
        if exceeds_recovery_window(
            target_newest_serial.saturating_sub(position.serial),
            max_replay_messages,
        ) {
            return Err(recovery_window_failure(
                position,
                frozen_head.stream_id,
                frozen_head.oldest_serial,
                frozen_head.newest_serial,
            ));
        }

        let mut cursor = None;
        let mut recovered_messages = Vec::new();
        let bounds = HistoryQueryBounds {
            start_serial: Some(position.serial.saturating_add(1)),
            end_serial: Some(target_newest_serial),
            start_time_ms: None,
            end_time_ms: None,
        };
        let mut first_page = true;
        let mut expected_serial = position.serial.saturating_add(1);
        let mut previous_cursor_serial = None;

        let gap = target_newest_serial.saturating_sub(position.serial) as usize;
        recovered_messages.reserve(gap.min(max_page_size * 4));

        loop {
            let page = self
                .history_store()
                .read_page(HistoryReadRequest {
                    app_id: app_config.id.clone(),
                    channel: channel.to_string(),
                    direction: HistoryDirection::OldestFirst,
                    limit: max_page_size,
                    cursor: cursor.clone(),
                    bounds: bounds.clone(),
                })
                .await
                .map_err(|error| map_history_read_error(position, error))?;

            if first_page {
                first_page = false;

                if page.retained.stream_id.as_deref() != position.stream_id.as_deref() {
                    return Err(ResumeFailure {
                        code: "stream_reset",
                        reason: "durable_stream_id_mismatch",
                        expected_stream_id: position.stream_id.clone(),
                        current_stream_id: page.retained.stream_id.clone(),
                        oldest_available_serial: page.retained.oldest_serial,
                        newest_available_serial: page.retained.newest_serial,
                    });
                }

                if page.truncated_by_retention {
                    return Err(ResumeFailure {
                        code: "position_expired",
                        reason: "retained_history_floor_is_ahead_of_requested_serial",
                        expected_stream_id: position.stream_id.clone(),
                        current_stream_id: page.retained.stream_id.clone(),
                        oldest_available_serial: page.retained.oldest_serial,
                        newest_available_serial: page.retained.newest_serial,
                    });
                }
            } else if page.retained.stream_id.as_deref() != position.stream_id.as_deref() {
                return Err(ResumeFailure {
                    code: "stream_reset",
                    reason: "durable_stream_changed_during_reconnect",
                    expected_stream_id: position.stream_id.clone(),
                    current_stream_id: page.retained.stream_id.clone(),
                    oldest_available_serial: page.retained.oldest_serial,
                    newest_available_serial: page.retained.newest_serial,
                });
            }

            // The retention window filters every page, so an expired entry can
            // surface after the first one when publish timestamps are not in
            // serial order. Report it as expiry, not as a continuity gap.
            if page.truncated_by_retention {
                return Err(ResumeFailure {
                    code: "position_expired",
                    reason: "retention_boundary_crossed_during_reconnect",
                    expected_stream_id: position.stream_id.clone(),
                    current_stream_id: page.retained.stream_id.clone(),
                    oldest_available_serial: page.retained.oldest_serial,
                    newest_available_serial: page.retained.newest_serial,
                });
            }

            let count_this_page = page.items.len();
            let page_items = page.items;
            let projected_messages = self
                .history_items_to_realtime_messages(app_config, channel, &page_items)
                .await
                .map_err(|_| ResumeFailure {
                    code: "persistence_unavailable",
                    reason: "durable_history_payload_projection_failed",
                    expected_stream_id: position.stream_id.clone(),
                    current_stream_id: page.retained.stream_id.clone(),
                    oldest_available_serial: page.retained.oldest_serial,
                    newest_available_serial: page.retained.newest_serial,
                })?;
            let mut last_item_serial = None;
            for (item, message) in page_items.into_iter().zip(projected_messages) {
                if item.stream_id.as_str() != position.stream_id.as_deref().unwrap_or_default() {
                    return Err(continuity_failure(
                        position,
                        "durable_history_item_stream_mismatch",
                        Some(item.stream_id),
                        page.retained.oldest_serial,
                        Some(target_newest_serial),
                    ));
                }
                if item.serial != expected_serial || item.serial > target_newest_serial {
                    return Err(continuity_failure(
                        position,
                        "durable_history_is_not_contiguous",
                        page.retained.stream_id.clone(),
                        page.retained.oldest_serial,
                        Some(target_newest_serial),
                    ));
                }
                last_item_serial = Some(item.serial);
                expected_serial = expected_serial.saturating_add(1);
                if position
                    .last_message_id
                    .as_ref()
                    .is_some_and(|last_id| item.message_id.as_ref() == Some(last_id))
                {
                    warn!(
                        app_id = %app_config.id,
                        channel,
                        serial = item.serial,
                        "suppressed duplicate message identity during durable recovery"
                    );
                    continue;
                }
                // Durable history owns its storage envelope. Recovery owns a
                // V2 egress boundary, so project each retained item back into
                // the typed realtime message before serializing wire bytes.
                let bytes =
                    sonic_rs::to_vec(&message)
                        .map(Bytes::from)
                        .map_err(|_| ResumeFailure {
                            code: "persistence_unavailable",
                            reason: "durable_history_payload_serialization_failed",
                            expected_stream_id: position.stream_id.clone(),
                            current_stream_id: page.retained.stream_id.clone(),
                            oldest_available_serial: page.retained.oldest_serial,
                            newest_available_serial: page.retained.newest_serial,
                        })?;
                recovered_messages.push(bytes);
            }

            if !page.has_more {
                if !page.complete || expected_serial <= target_newest_serial {
                    return Err(continuity_failure(
                        position,
                        "durable_history_ended_before_stream_head",
                        page.retained.stream_id.clone(),
                        page.retained.oldest_serial,
                        Some(target_newest_serial),
                    ));
                }
                break;
            }

            let next_cursor = page.next_cursor.ok_or_else(|| {
                continuity_failure(
                    position,
                    "durable_history_cursor_missing",
                    page.retained.stream_id.clone(),
                    page.retained.oldest_serial,
                    Some(target_newest_serial),
                )
            })?;
            if count_this_page == 0
                || next_cursor.version != 1
                || next_cursor.app_id != app_config.id
                || next_cursor.channel != channel
                || Some(next_cursor.serial) != last_item_serial
                || previous_cursor_serial.is_some_and(|previous| next_cursor.serial <= previous)
                || next_cursor.stream_id.as_str()
                    != position.stream_id.as_deref().unwrap_or_default()
                || next_cursor.direction != HistoryDirection::OldestFirst
                || next_cursor.bounds != bounds
            {
                return Err(continuity_failure(
                    position,
                    "durable_history_cursor_did_not_advance",
                    page.retained.stream_id.clone(),
                    page.retained.oldest_serial,
                    Some(target_newest_serial),
                ));
            }
            previous_cursor_serial = Some(next_cursor.serial);
            cursor = Some(next_cursor);
        }

        Ok(recovered_messages)
    }

    async fn record_resume_failure(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        channel: &str,
        failure: &ResumeFailure,
        failed: &mut Vec<sonic_rs::Value>,
    ) {
        let _ = self
            .send_resume_failed(socket_id, &app_config.id, channel, failure)
            .await;
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.mark_history_recovery_failure(&app_config.id, failure.code);
        }
        failed.push(json!({
            "channel": channel,
            "code": failure.code,
            "reason": failure.reason,
            "expected_stream_id": failure.expected_stream_id,
            "current_stream_id": failure.current_stream_id,
            "oldest_available_serial": failure.oldest_available_serial,
            "newest_available_serial": failure.newest_available_serial,
        }));
    }

    /// Release live messages held while a channel was being recovered. Messages of the
    /// replayed stream at or before `replayed_through` were already delivered by the replay
    /// and are skipped; messages of any other stream (e.g. after a reset) are always sent.
    pub(crate) async fn drain_recovery_gate(
        &self,
        connection: &sockudo_core::websocket::WebSocketRef,
        socket_id: &SocketId,
        app_id: &str,
        channel: &str,
        replayed_through: Option<&ReplayedThrough>,
    ) -> Result<()> {
        loop {
            let batch = connection
                .drain_rewind_gate_batch(channel)
                .await
                .map_err(|error| {
                    warn!(channel, error = %error, "recovery continuity gate failed");
                    connection.shutdown();
                    error
                })?;
            let Some(mut buffered) = batch else {
                break;
            };
            buffered.sort_by_key(|message| message.serial.unwrap_or(u64::MAX));
            let mut messages = buffered
                .into_iter()
                .filter(|buffered_message| {
                    // Already part of the replay; a live copy would duplicate it.
                    !replayed_through.is_some_and(|through| {
                        through.covers(
                            buffered_message.message.stream_id.as_deref(),
                            buffered_message.serial,
                        )
                    })
                })
                .map(|message| std::sync::Arc::unwrap_or_clone(message.message))
                .collect::<Vec<_>>();
            // Gate drains must apply the same subscription predicate and tag
            // filter as live fanout; fanout buffers gated sockets before its
            // predicates run.
            crate::v2_broadcast::retain_messages_allowed_by_subscription(
                connection,
                channel,
                &mut messages,
                self.tag_filtering_enabled(),
            );

            for message in messages {
                let message =
                    crate::v2_broadcast::apply_append_mode(message, connection.append_mode);
                self.send_message_to_socket(app_id, socket_id, message)
                    .await
                    .map_err(|error| {
                        warn!(channel, error = %error, "failed to drain recovery continuity gate");
                        connection.shutdown();
                        error
                    })?;
            }
        }

        Ok(())
    }

    async fn send_resume_failed(
        &self,
        socket_id: &SocketId,
        app_id: &str,
        channel: &str,
        failure: &ResumeFailure,
    ) -> Result<()> {
        let fail_msg = PusherMessage {
            event: Some(sockudo_protocol::constants::EVENT_RESUME_FAILED.to_string()),
            channel: Some(channel.to_string()),
            data: Some(MessageData::String(
                json!({
                    "code": failure.code,
                    "reason": failure.reason,
                    "expected_stream_id": failure.expected_stream_id,
                    "current_stream_id": failure.current_stream_id,
                    "oldest_available_serial": failure.oldest_available_serial,
                    "newest_available_serial": failure.newest_available_serial,
                })
                .to_string(),
            )),
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
        self.send_message_to_socket(app_id, socket_id, fail_msg)
            .await
    }

    async fn send_replayed_messages(
        &self,
        socket_id: &SocketId,
        app_id: &str,
        channel: &str,
        messages: Vec<Bytes>,
    ) -> std::result::Result<ResumeDelivery, ResumeFailure> {
        let Some(connection) = self
            .connection_manager
            .get_connection(socket_id, app_id)
            .await
        else {
            return Err(replay_delivery_failure());
        };

        // Never deliver recovery payloads without a live subscription, and never bypass the
        // per-message subscription filter (GHSA-r5q6-rw3h-q24r).
        if !crate::v2_broadcast::has_matching_subscription(&connection, channel) {
            return Err(access_denied_failure(
                "not_subscribed",
                "channel_not_subscribed",
            ));
        }

        let mut delivered = 0;
        let mut position = None;
        for bytes in messages {
            let message: PusherMessage = sonic_rs::from_slice(&bytes).map_err(|error| {
                warn!(channel, error = %error, "failed to decode recovery payload");
                replay_delivery_failure()
            })?;
            position = recovery_position(&message).or(position);
            if !crate::v2_broadcast::socket_allows_message(&connection, channel, &message, true) {
                continue;
            }
            self.send_message_to_socket(app_id, socket_id, message)
                .await
                .map_err(|error| {
                    warn!(channel, error = %error, "failed to deliver recovery payload");
                    replay_delivery_failure()
                })?;
            delivered += 1;
        }
        Ok(ResumeDelivery {
            delivered,
            position,
        })
    }
}

fn recovery_position(message: &PusherMessage) -> Option<ResumeDeliveryPosition> {
    Some(ResumeDeliveryPosition {
        stream_id: message.stream_id.clone(),
        serial: message.serial?,
        last_message_id: message.message_id.clone(),
    })
}

/// Durable version stores identify a channel's stream as `{app_id}/{channel}`.
fn is_channel_scoped_stream_id(stream_id: &str, app_id: &str, channel: &str) -> bool {
    stream_id
        .strip_prefix(app_id)
        .and_then(|rest| rest.strip_prefix('/'))
        .is_some_and(|rest| rest == channel)
}

fn replay_delivery_failure() -> ResumeFailure {
    ResumeFailure {
        code: "persistence_unavailable",
        reason: "failed_to_deliver_recovery_payload",
        expected_stream_id: None,
        current_stream_id: None,
        oldest_available_serial: None,
        newest_available_serial: None,
    }
}

fn parse_resume_positions(message: &PusherMessage) -> Result<HashMap<String, ResumePosition>> {
    let data_str = match &message.data {
        Some(MessageData::String(s)) => s.clone(),
        Some(MessageData::Json(v)) => v.to_string(),
        _ => {
            return Err(Error::InvalidMessageFormat(
                "Missing data in resume message".to_string(),
            ));
        }
    };

    #[derive(Deserialize)]
    struct ChannelPosition {
        serial: u64,
        stream_id: Option<String>,
        last_message_id: Option<String>,
    }

    #[derive(Deserialize)]
    struct ResumeData {
        channel_positions: Option<HashMap<String, ChannelPosition>>,
        channel_serials: Option<HashMap<String, u64>>,
    }

    let resume_data: ResumeData = sonic_rs::from_str(&data_str)
        .map_err(|e| Error::InvalidMessageFormat(format!("Invalid resume data JSON: {e}")))?;

    if let Some(channel_positions) = resume_data.channel_positions {
        return Ok(channel_positions
            .into_iter()
            .map(|(channel, position)| {
                (
                    channel,
                    ResumePosition {
                        serial: position.serial,
                        // Without a stream_id continuity cannot be proven: treat it like
                        // the legacy serial-only form (hot buffer only, never cold).
                        legacy_serial_only: position.stream_id.is_none(),
                        stream_id: position.stream_id,
                        last_message_id: position.last_message_id,
                    },
                )
            })
            .collect());
    }

    resume_data
        .channel_serials
        .map(|positions| {
            positions
                .into_iter()
                .map(|(channel, serial)| {
                    (
                        channel,
                        ResumePosition {
                            serial,
                            stream_id: None,
                            last_message_id: None,
                            legacy_serial_only: true,
                        },
                    )
                })
                .collect()
        })
        .ok_or_else(|| {
            Error::InvalidMessageFormat(
                "Missing channel_positions or channel_serials in resume data".to_string(),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConnectionManager;
    use crate::local_adapter::LocalAdapter;
    use async_trait::async_trait;
    use bytes::Bytes;
    use sockudo_app::memory_app_manager::MemoryAppManager;
    use sockudo_core::app::AppManager;
    use sockudo_core::cache::CacheManager;
    use sockudo_core::history::{
        HistoryAppendRecord, HistoryDurableState, HistoryPage, HistoryPurgeMode,
        HistoryPurgeRequest, HistoryReadRequest, HistoryRuntimeStatus, HistoryStore,
        HistoryStreamInspection, HistoryStreamRuntimeState, HistoryWriteReservation,
        MemoryHistoryStore, MemoryHistoryStoreConfig,
    };
    use sockudo_core::metrics::MetricsInterface;
    use sockudo_core::options::ServerOptions;
    use sonic_rs::Value;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::RwLock;

    #[test]
    fn terminal_recovery_position_advances_over_suppressed_messages() {
        let mut message = PusherMessage::channel_event("ignored", "chat", json!({}));
        message.stream_id = Some("stream-1".into());
        message.serial = Some(42);
        message.message_id = Some("message-42".into());

        let position = recovery_position(&message).expect("serial creates a position");
        assert_eq!(position.stream_id.as_deref(), Some("stream-1"));
        assert_eq!(position.serial, 42);
        assert_eq!(position.last_message_id.as_deref(), Some("message-42"));
    }

    struct TestCache;

    #[async_trait]
    impl CacheManager for TestCache {
        async fn has(&self, _key: &str) -> Result<bool> {
            Ok(false)
        }
        async fn get(&self, _key: &str) -> Result<Option<String>> {
            Ok(None)
        }
        async fn set(&self, _key: &str, _value: &str, _ttl_seconds: u64) -> Result<()> {
            Ok(())
        }
        async fn remove(&self, _key: &str) -> Result<()> {
            Ok(())
        }
        async fn disconnect(&self) -> Result<()> {
            Ok(())
        }
        async fn ttl(&self, _key: &str) -> Result<Option<Duration>> {
            Ok(None)
        }
    }

    struct TestMetrics;

    #[async_trait]
    impl MetricsInterface for TestMetrics {
        async fn init(&self) -> Result<()> {
            Ok(())
        }
        fn mark_new_connection(&self, _app_id: &str, _socket_id: &SocketId) {}
        fn mark_disconnection(&self, _app_id: &str, _socket_id: &SocketId) {}
        fn mark_connection_error(&self, _app_id: &str, _error_type: &str) {}
        fn mark_rate_limit_check(&self, _app_id: &str, _limiter_type: &str) {}
        fn mark_rate_limit_check_with_context(
            &self,
            _app_id: &str,
            _limiter_type: &str,
            _request_context: &str,
        ) {
        }
        fn mark_rate_limit_triggered(&self, _app_id: &str, _limiter_type: &str) {}
        fn mark_rate_limit_triggered_with_context(
            &self,
            _app_id: &str,
            _limiter_type: &str,
            _request_context: &str,
        ) {
        }
        fn mark_channel_subscription(&self, _app_id: &str, _channel_type: &str) {}
        fn mark_channel_unsubscription(&self, _app_id: &str, _channel_type: &str) {}

        fn mark_api_message(
            &self,
            _app_id: &str,
            _incoming_message_size: usize,
            _sent_message_size: usize,
        ) {
        }
        fn mark_ws_message_sent(&self, _app_id: &str, _sent_message_size: usize) {}
        fn mark_ws_messages_sent_batch(
            &self,
            _app_id: &str,
            _sent_message_size: usize,
            _count: usize,
        ) {
        }
        fn mark_ws_message_received(&self, _app_id: &str, _message_size: usize) {}
        fn track_horizontal_adapter_resolve_time(&self, _app_id: &str, _time_ms: f64) {}
        fn track_horizontal_adapter_resolved_promises(
            &self,
            _app_id: &str,
            _resolved: bool,
            _request_type: &str,
        ) {
        }
        fn mark_horizontal_adapter_request_sent(&self, _app_id: &str) {}
        fn mark_horizontal_adapter_request_received(&self, _app_id: &str) {}
        fn mark_horizontal_adapter_response_received(&self, _app_id: &str) {}
        fn track_broadcast_latency(
            &self,
            _app_id: &str,
            _channel_name: &str,
            _recipient_count: usize,
            _latency_ms: f64,
        ) {
        }
        fn track_horizontal_delta_compression(
            &self,
            _app_id: &str,
            _channel_name: &str,
            _enabled: bool,
        ) {
        }
        fn track_delta_compression_bandwidth(
            &self,
            _app_id: &str,
            _channel_name: &str,
            _original_bytes: usize,
            _compressed_bytes: usize,
        ) {
        }
        fn track_delta_compression_full_message(&self, _app_id: &str, _channel_name: &str) {}
        fn track_delta_compression_delta_message(&self, _app_id: &str, _channel_name: &str) {}
        async fn get_metrics_as_plaintext(&self) -> String {
            String::new()
        }
        async fn get_metrics_as_json(&self) -> Value {
            sonic_rs::json!({})
        }
        async fn clear(&self) {}
    }

    #[derive(Clone)]
    struct StatefulHistoryStore {
        inner: Arc<MemoryHistoryStore>,
        states: Arc<RwLock<HashMap<String, HistoryStreamRuntimeState>>>,
    }

    impl StatefulHistoryStore {
        fn new(inner: Arc<MemoryHistoryStore>) -> Self {
            Self {
                inner,
                states: Arc::new(RwLock::new(HashMap::new())),
            }
        }

        fn key(app_id: &str, channel: &str) -> String {
            format!("{app_id}\0{channel}")
        }

        async fn set_state(&self, app_id: &str, channel: &str, state: HistoryStreamRuntimeState) {
            self.states
                .write()
                .await
                .insert(Self::key(app_id, channel), state);
        }
    }

    #[async_trait]
    impl HistoryStore for StatefulHistoryStore {
        async fn reserve_publish_position(
            &self,
            app_id: &str,
            channel: &str,
        ) -> sockudo_core::error::Result<HistoryWriteReservation> {
            self.inner.reserve_publish_position(app_id, channel).await
        }

        async fn append(&self, record: HistoryAppendRecord) -> sockudo_core::error::Result<()> {
            self.inner.append(record).await
        }

        async fn read_page(
            &self,
            request: HistoryReadRequest,
        ) -> sockudo_core::error::Result<HistoryPage> {
            let state = self
                .stream_runtime_state(&request.app_id, &request.channel)
                .await?;
            if !state.recovery_allowed {
                return Err(sockudo_core::error::Error::Internal(
                    "history_stream_state_blocks_reads".to_string(),
                ));
            }
            self.inner.read_page(request).await
        }

        async fn stream_inspection(
            &self,
            app_id: &str,
            channel: &str,
        ) -> sockudo_core::error::Result<HistoryStreamInspection> {
            self.inner.stream_inspection(app_id, channel).await
        }

        async fn runtime_status(&self) -> sockudo_core::error::Result<HistoryRuntimeStatus> {
            let states = self.states.read().await;
            let degraded_channels = states
                .values()
                .filter(|state| !state.recovery_allowed)
                .count();
            let reset_required_channels =
                states.values().filter(|state| state.reset_required).count();
            Ok(HistoryRuntimeStatus {
                enabled: true,
                backend: "memory".to_string(),
                state_authority: "shared_test_state".to_string(),
                degraded_channels,
                reset_required_channels,
                queue_depth: 0,
            })
        }

        async fn stream_runtime_state(
            &self,
            app_id: &str,
            channel: &str,
        ) -> sockudo_core::error::Result<HistoryStreamRuntimeState> {
            if let Some(state) = self
                .states
                .read()
                .await
                .get(&Self::key(app_id, channel))
                .cloned()
            {
                return Ok(state);
            }
            self.inner.stream_runtime_state(app_id, channel).await
        }
    }

    #[derive(Clone)]
    struct FaultyStateHistoryStore {
        inner: Arc<MemoryHistoryStore>,
    }

    #[async_trait]
    impl HistoryStore for FaultyStateHistoryStore {
        async fn reserve_publish_position(
            &self,
            app_id: &str,
            channel: &str,
        ) -> sockudo_core::error::Result<HistoryWriteReservation> {
            self.inner.reserve_publish_position(app_id, channel).await
        }

        async fn append(&self, record: HistoryAppendRecord) -> sockudo_core::error::Result<()> {
            self.inner.append(record).await
        }

        async fn read_page(
            &self,
            request: HistoryReadRequest,
        ) -> sockudo_core::error::Result<HistoryPage> {
            self.inner.read_page(request).await
        }

        async fn stream_inspection(
            &self,
            app_id: &str,
            channel: &str,
        ) -> sockudo_core::error::Result<HistoryStreamInspection> {
            self.inner.stream_inspection(app_id, channel).await
        }

        async fn runtime_status(&self) -> sockudo_core::error::Result<HistoryRuntimeStatus> {
            self.inner.runtime_status().await
        }

        async fn stream_runtime_state(
            &self,
            _app_id: &str,
            _channel: &str,
        ) -> sockudo_core::error::Result<HistoryStreamRuntimeState> {
            Err(Error::Cache(
                "simulated_cache_marker_read_failed".to_string(),
            ))
        }
    }

    #[derive(Clone)]
    struct CursorExpiryHistoryStore {
        inner: Arc<MemoryHistoryStore>,
        read_count: Arc<AtomicUsize>,
        expire_reason: &'static str,
    }

    #[async_trait]
    impl HistoryStore for CursorExpiryHistoryStore {
        async fn reserve_publish_position(
            &self,
            app_id: &str,
            channel: &str,
        ) -> sockudo_core::error::Result<HistoryWriteReservation> {
            self.inner.reserve_publish_position(app_id, channel).await
        }

        async fn append(&self, record: HistoryAppendRecord) -> sockudo_core::error::Result<()> {
            self.inner.append(record).await
        }

        async fn read_page(
            &self,
            request: HistoryReadRequest,
        ) -> sockudo_core::error::Result<HistoryPage> {
            let current = self.read_count.fetch_add(1, Ordering::Relaxed);
            if current >= 1 && request.cursor.is_some() {
                return Err(Error::InvalidMessageFormat(self.expire_reason.to_string()));
            }
            self.inner.read_page(request).await
        }

        async fn stream_inspection(
            &self,
            app_id: &str,
            channel: &str,
        ) -> sockudo_core::error::Result<HistoryStreamInspection> {
            self.inner.stream_inspection(app_id, channel).await
        }

        async fn runtime_status(&self) -> sockudo_core::error::Result<HistoryRuntimeStatus> {
            self.inner.runtime_status().await
        }

        async fn stream_runtime_state(
            &self,
            app_id: &str,
            channel: &str,
        ) -> sockudo_core::error::Result<HistoryStreamRuntimeState> {
            self.inner.stream_runtime_state(app_id, channel).await
        }
    }

    #[derive(Clone)]
    struct MissingCursorHistoryStore {
        inner: Arc<MemoryHistoryStore>,
    }

    #[async_trait]
    impl HistoryStore for MissingCursorHistoryStore {
        async fn reserve_publish_position(
            &self,
            app_id: &str,
            channel: &str,
        ) -> sockudo_core::error::Result<HistoryWriteReservation> {
            self.inner.reserve_publish_position(app_id, channel).await
        }

        async fn append(&self, record: HistoryAppendRecord) -> sockudo_core::error::Result<()> {
            self.inner.append(record).await
        }

        async fn read_page(
            &self,
            request: HistoryReadRequest,
        ) -> sockudo_core::error::Result<HistoryPage> {
            let mut page = self.inner.read_page(request).await?;
            if page.has_more {
                page.next_cursor = None;
            }
            Ok(page)
        }

        async fn stream_inspection(
            &self,
            app_id: &str,
            channel: &str,
        ) -> sockudo_core::error::Result<HistoryStreamInspection> {
            self.inner.stream_inspection(app_id, channel).await
        }

        async fn runtime_status(&self) -> sockudo_core::error::Result<HistoryRuntimeStatus> {
            self.inner.runtime_status().await
        }

        async fn stream_runtime_state(
            &self,
            app_id: &str,
            channel: &str,
        ) -> sockudo_core::error::Result<HistoryStreamRuntimeState> {
            self.inner.stream_runtime_state(app_id, channel).await
        }
    }

    fn build_handler(history_enabled: bool) -> ConnectionHandler {
        build_handler_with_history_store_and_page_size(
            history_enabled,
            Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default())),
            100,
        )
    }

    fn build_handler_with_history_store(
        history_enabled: bool,
        history_store: Arc<dyn sockudo_core::history::HistoryStore + Send + Sync>,
    ) -> ConnectionHandler {
        build_handler_with_history_store_and_page_size(history_enabled, history_store, 100)
    }

    fn build_handler_with_history_store_and_page_size(
        history_enabled: bool,
        history_store: Arc<dyn sockudo_core::history::HistoryStore + Send + Sync>,
        max_page_size: usize,
    ) -> ConnectionHandler {
        let mut options = ServerOptions::default();
        options.connection_recovery.enabled = true;
        options.history.enabled = history_enabled;
        options.history.max_page_size = max_page_size;

        ConnectionHandler::builder(
            Arc::new(MemoryAppManager::new()) as Arc<dyn AppManager + Send + Sync>,
            Arc::new(LocalAdapter::new()) as Arc<dyn ConnectionManager + Send + Sync>,
            Arc::new(TestCache),
            options,
        )
        .metrics(Arc::new(TestMetrics))
        .history_store(history_store)
        .build()
    }

    async fn append_history(
        handler: &ConnectionHandler,
        app_id: &str,
        channel: &str,
        serials: &[u64],
        stream_id: &str,
    ) {
        let base_ts = sockudo_core::history::now_ms();
        let entries = serials
            .iter()
            .map(|serial| (*serial, base_ts + *serial as i64))
            .collect::<Vec<_>>();
        append_history_at(handler, app_id, channel, &entries, stream_id).await;
    }

    async fn append_history_at(
        handler: &ConnectionHandler,
        app_id: &str,
        channel: &str,
        entries: &[(u64, i64)],
        stream_id: &str,
    ) {
        for (serial, published_at_ms) in entries {
            handler
                .history_store()
                .append(HistoryAppendRecord {
                    app_id: app_id.to_string(),
                    channel: channel.to_string(),
                    stream_id: stream_id.to_string(),
                    serial: *serial,
                    published_at_ms: *published_at_ms,
                    message_id: Some(format!("msg-{serial}")),
                    event_name: Some("evt".to_string()),
                    operation_kind: "append".to_string(),
                    payload_bytes: Bytes::from(
                        sonic_rs::to_vec(&PusherMessage {
                            event: Some("evt".to_string()),
                            channel: Some(channel.to_string()),
                            data: None,
                            name: None,
                            user_id: None,
                            tags: None,
                            sequence: None,
                            conflation_key: None,
                            message_id: Some(format!("msg-{serial}")),
                            stream_id: Some(stream_id.to_string()),
                            serial: Some(*serial),
                            idempotency_key: None,
                            extras: None,
                            delta_sequence: None,
                            delta_conflation_key: None,
                        })
                        .unwrap(),
                    ),
                    retention: sockudo_core::history::HistoryRetentionPolicy {
                        retention_window_seconds: 3600,
                        max_messages_per_channel: None,
                        max_bytes_per_channel: None,
                    },
                })
                .await
                .unwrap();
        }
    }

    fn test_app(app_id: &str) -> App {
        App::from_policy(
            app_id.to_string(),
            "key".to_string(),
            "secret".to_string(),
            true,
            Default::default(),
        )
    }

    fn resume_message(payload: sonic_rs::Value) -> PusherMessage {
        PusherMessage {
            event: Some(sockudo_protocol::constants::EVENT_RESUME.to_string()),
            channel: None,
            data: Some(MessageData::String(payload.to_string())),
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
        }
    }

    #[test]
    fn parse_resume_positions_supports_legacy_serials() {
        let positions = parse_resume_positions(&resume_message(json!({
            "channel_serials": { "chat": 42 }
        })))
        .unwrap();
        let position = positions.get("chat").unwrap();
        assert_eq!(position.serial, 42);
        assert!(position.stream_id.is_none());
        assert!(position.legacy_serial_only);
    }

    #[test]
    fn parse_resume_positions_supports_continuity_tokens() {
        let positions = parse_resume_positions(&resume_message(json!({
            "channel_positions": {
                "chat": {
                    "serial": 42,
                    "stream_id": "stream-1",
                    "last_message_id": "msg-42"
                }
            }
        })))
        .unwrap();
        let position = positions.get("chat").unwrap();
        assert_eq!(position.serial, 42);
        assert_eq!(position.stream_id.as_deref(), Some("stream-1"));
        assert_eq!(position.last_message_id.as_deref(), Some("msg-42"));
        assert!(!position.legacy_serial_only);
    }

    #[test]
    fn positions_without_stream_id_are_hot_buffer_only() {
        let positions = parse_resume_positions(&resume_message(json!({
            "channel_positions": { "chat": { "serial": 42 } }
        })))
        .unwrap();
        assert!(positions.get("chat").unwrap().legacy_serial_only);
    }

    fn parked_position() -> ResumePosition {
        ResumePosition {
            serial: 1,
            stream_id: Some("stream".to_string()),
            last_message_id: None,
            legacy_serial_only: false,
        }
    }

    #[tokio::test]
    async fn pending_resumes_are_bounded_per_socket_and_in_time() {
        let handler = build_handler(false);
        let socket_id = SocketId::new();

        for index in 0..MAX_PENDING_RESUME_CHANNELS {
            assert!(handler.park_pending_resume(
                &socket_id,
                "app",
                &format!("private-{index}"),
                parked_position()
            ));
        }
        assert!(
            !handler.park_pending_resume(&socket_id, "app", "private-overflow", parked_position()),
            "parking beyond the per-socket cap must be rejected"
        );
        assert!(
            handler.park_pending_resume(&socket_id, "app", "private-0", parked_position()),
            "re-parking an existing channel replaces it"
        );
        assert!(!handler.park_pending_resume(
            &socket_id,
            "app",
            &"x".repeat(MAX_PENDING_RESUME_CHANNEL_BYTES + 1),
            parked_position()
        ));
        assert!(!handler.park_pending_resume(&socket_id, "app", "", parked_position()));

        // `handle_resume` takes (and reports) expired entries before parking new ones, so
        // they never count against the cap.
        let expired_at = Instant::now()
            .checked_sub(PENDING_RESUME_TTL + Duration::from_secs(1))
            .unwrap();
        for entry in handler
            .pending_resumes
            .get_mut(&socket_id)
            .unwrap()
            .values_mut()
        {
            entry.received_at = expired_at;
        }
        assert_eq!(
            handler.take_expired_pending_resumes(&socket_id).len(),
            MAX_PENDING_RESUME_CHANNELS
        );
        assert!(!handler.pending_resumes.contains_key(&socket_id));
        assert!(handler.park_pending_resume(
            &socket_id,
            "app",
            "private-overflow",
            parked_position()
        ));
        assert_eq!(handler.pending_resumes.get(&socket_id).unwrap().len(), 1);

        let taken =
            handler.take_pending_resumes(&socket_id, |channel| channel == "private-overflow");
        assert_eq!(taken.len(), 1);
        assert!(
            !handler.pending_resumes.contains_key(&socket_id),
            "an emptied socket entry is removed"
        );
    }

    #[tokio::test]
    async fn pending_resumes_reject_oversized_ids_and_expire_without_traffic() {
        let handler = build_handler(false);
        let socket_id = SocketId::new();
        let long_id = "x".repeat(MAX_PENDING_RESUME_ID_BYTES + 1);

        let mut oversized_stream = parked_position();
        oversized_stream.stream_id = Some(long_id.clone());
        assert!(!handler.park_pending_resume(&socket_id, "app", "private-a", oversized_stream));

        // Durable version stores use `{app_id}/{channel}`, which may exceed the id cap.
        let long_channel = format!("private-{}", "c".repeat(190));
        let mut channel_scoped = parked_position();
        channel_scoped.stream_id = Some(format!("app/{long_channel}"));
        assert!(handler.park_pending_resume(&socket_id, "app", &long_channel, channel_scoped));
        let mut other_channel_scope = parked_position();
        other_channel_scope.stream_id = Some(format!("app/{long_channel}-other"));
        assert!(!handler.park_pending_resume(
            &socket_id,
            "app",
            &long_channel,
            other_channel_scope
        ));
        assert!(
            handler
                .take_pending_resumes(&socket_id, |channel| channel == long_channel)
                .len()
                == 1
        );

        // An oversized optional last_message_id is dropped, not the whole position.
        let mut oversized_message_id = parked_position();
        oversized_message_id.last_message_id = Some(long_id);
        assert!(handler.park_pending_resume(&socket_id, "app", "private-a", oversized_message_id));
        let parked = handler.take_pending_resumes(&socket_id, |channel| channel == "private-a");
        assert_eq!(parked.len(), 1);
        assert_eq!(parked[0].1.position.last_message_id, None);
        assert_eq!(parked[0].1.position.serial, parked_position().serial);
        assert_eq!(parked[0].1.position.stream_id, parked_position().stream_id);

        assert!(handler.park_pending_resume(&socket_id, "app", "private-a", parked_position()));
        assert!(handler.park_pending_resume(&socket_id, "app", "private-b", parked_position()));
        assert_eq!(handler.evict_expired_pending_resumes().await, 0);

        let expired_at = Instant::now()
            .checked_sub(PENDING_RESUME_TTL + Duration::from_secs(1))
            .unwrap();
        handler
            .pending_resumes
            .get_mut(&socket_id)
            .unwrap()
            .get_mut("private-a")
            .unwrap()
            .received_at = expired_at;
        assert_eq!(handler.evict_expired_pending_resumes().await, 1);
        assert!(handler.has_pending_resume(&socket_id, "private-b"));

        handler
            .pending_resumes
            .get_mut(&socket_id)
            .unwrap()
            .get_mut("private-b")
            .unwrap()
            .received_at = expired_at;
        assert_eq!(handler.evict_expired_pending_resumes().await, 1);
        assert!(
            !handler.pending_resumes.contains_key(&socket_id),
            "a socket with no parked positions left is removed"
        );
    }

    #[test]
    fn replayed_through_only_covers_the_replayed_stream() {
        let through = ReplayedThrough {
            stream_id: Some("stream-a".to_string()),
            serial: 5,
        };
        assert!(through.covers(Some("stream-a"), Some(5)));
        assert!(through.covers(Some("stream-a"), Some(1)));
        assert!(!through.covers(Some("stream-a"), Some(6)));
        assert!(!through.covers(Some("stream-a"), None));
        assert!(
            !through.covers(Some("stream-b"), Some(1)),
            "a reset stream restarts serials and must never be deduplicated against the old one"
        );
        assert!(!through.covers(None, Some(1)));
    }

    #[tokio::test]
    async fn hot_recovery_success_uses_replay_buffer() {
        let handler = build_handler(true);
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        replay_buffer.store(
            "app",
            "chat",
            Some("stream-1"),
            2,
            Bytes::from_static(b"two"),
        );
        replay_buffer.store(
            "app",
            "chat",
            Some("stream-1"),
            3,
            Bytes::from_static(b"three"),
        );
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Recovered { source, messages } => {
                let count = messages.len();
                assert_eq!(source, "hot");
                assert_eq!(count, 2);
            }
            other => panic!("expected hot recovery, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cold_recovery_success_uses_durable_history() {
        let handler = build_handler(true);
        append_history(&handler, "app", "chat", &[2, 3], "stream-1").await;
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Recovered { source, messages } => {
                let count = messages.len();
                assert_eq!(source, "cold");
                assert_eq!(count, 2);
            }
            other => panic!("expected cold recovery, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn durable_recovery_rejects_gap_larger_than_configured_window() {
        let handler = build_handler(true);
        append_history(&handler, "app", "chat", &[2, 3], "stream-1").await;
        let mut app = test_app("app");
        app.policy_mut().connection_recovery =
            Some(sockudo_core::app::AppConnectionRecoveryConfig {
                enabled: Some(true),
                buffer_ttl_seconds: None,
                max_buffer_size: Some(1),
            });

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                handler.replay_buffer().unwrap(),
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => {
                assert_eq!(failure.code, "position_expired");
                assert_eq!(failure.reason, "recovery_window_exceeded");
                assert_eq!(failure.newest_available_serial, Some(3));
            }
            other => panic!("expected bounded recovery failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cold_recovery_keeps_frozen_bounds_across_multiple_pages() {
        let store = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let handler = build_handler_with_history_store_and_page_size(true, store, 1);
        append_history(&handler, "app", "chat", &[2, 3, 4], "stream-1").await;
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Recovered { source, messages } => {
                let count = messages.len();
                assert_eq!(source, "cold");
                assert_eq!(count, 3);
            }
            other => panic!("expected paginated cold recovery, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn hot_position_ahead_falls_back_to_authoritative_durable_history() {
        let handler = build_handler(true);
        append_history(&handler, "app", "chat", &[1, 2, 3, 4], "stream-1").await;
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        replay_buffer.store(
            "app",
            "chat",
            Some("stream-1"),
            1,
            Bytes::from_static(b"hot-1"),
        );
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 2,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Recovered { source, messages } => {
                let count = messages.len();
                assert_eq!(source, "cold");
                assert_eq!(count, 2);
            }
            other => panic!("expected durable fallback, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fully_evicted_durable_history_does_not_look_like_a_new_empty_stream() {
        let store = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let handler = build_handler_with_history_store(true, store.clone());
        append_history(&handler, "app", "chat", &[1, 2], "stream-1").await;
        store
            .purge_stream(
                "app",
                "chat",
                HistoryPurgeRequest {
                    mode: HistoryPurgeMode::All,
                    before_serial: None,
                    before_time_ms: None,
                    reason: "test retention".to_owned(),
                    requested_by: None,
                },
            )
            .await
            .unwrap();
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 0,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => {
                assert_eq!(failure.code, "position_expired");
                assert_eq!(failure.reason, "durable_history_empty_after_prior_writes");
            }
            other => panic!("expected fully-evicted history failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cold_recovery_rejects_non_contiguous_history() {
        let handler = build_handler(true);
        append_history(&handler, "app", "chat", &[2, 4], "stream-1").await;
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => {
                assert_eq!(failure.code, "continuity_unverifiable");
                assert_eq!(failure.reason, "durable_history_is_not_contiguous");
            }
            other => panic!("expected continuity failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn recovery_rejects_position_ahead_of_durable_stream() {
        let handler = build_handler(true);
        append_history(&handler, "app", "chat", &[2, 3], "stream-1").await;
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 4,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => {
                assert_eq!(failure.code, "continuity_unverifiable");
                assert_eq!(failure.reason, "position_ahead_of_durable_stream");
            }
            other => panic!("expected ahead-of-stream failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn recovery_rejects_has_more_page_without_cursor() {
        let inner = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let store = Arc::new(MissingCursorHistoryStore { inner });
        let handler = build_handler_with_history_store_and_page_size(true, store, 1);
        append_history(&handler, "app", "chat", &[2, 3], "stream-1").await;
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => {
                assert_eq!(failure.code, "continuity_unverifiable");
                assert_eq!(failure.reason, "durable_history_cursor_missing");
            }
            other => panic!("expected cursor failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn epoch_mismatch_fails_explicitly() {
        let handler = build_handler(true);
        append_history(&handler, "app", "chat", &[2, 3], "stream-new").await;
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-old".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => assert_eq!(failure.code, "stream_reset"),
            other => panic!("expected failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn retention_gap_fails_explicitly() {
        let handler = build_handler(true);
        append_history(&handler, "app", "chat", &[5, 6], "stream-1").await;
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => assert_eq!(failure.code, "position_expired"),
            other => panic!("expected retention-gap failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn per_channel_recovery_outcomes_are_independent() {
        let handler = build_handler(true);
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        replay_buffer.store(
            "app",
            "good",
            Some("stream-good"),
            2,
            Bytes::from_static(b"two"),
        );
        append_history(&handler, "app", "bad", &[5, 6], "stream-bad").await;
        let app = test_app("app");

        let good = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "good",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-good".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;
        let bad = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "bad",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-bad".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        assert!(matches!(good, ResumeOutcome::Recovered { .. }));
        assert!(matches!(bad, ResumeOutcome::Failed(_)));
    }

    #[tokio::test]
    async fn shared_history_store_allows_cross_node_cold_recovery() {
        let shared_history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let node_a = build_handler_with_history_store(true, shared_history.clone());
        let node_b = build_handler_with_history_store(true, shared_history);
        let app = test_app("app");

        append_history(&node_a, "app", "chat", &[2, 3], "stream-1").await;

        let outcome = node_b
            .resume_channel(
                &SocketId::new(),
                &app,
                node_b.replay_buffer().unwrap(),
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Recovered { source, messages } => {
                let count = messages.len();
                assert_eq!(source, "cold");
                assert_eq!(count, 2);
            }
            other => panic!("expected shared-store cold recovery, got {other:?}"),
        }
    }

    fn resume_from(serial: u64) -> ResumePosition {
        ResumePosition {
            serial,
            stream_id: Some("stream-1".to_string()),
            last_message_id: None,
            legacy_serial_only: false,
        }
    }

    #[tokio::test]
    async fn hot_replay_short_of_durable_head_falls_back_to_cold() {
        // Node B's hot buffer only saw its own publishes (2, 3); node A
        // published 4 while the client was away (sockudo/sockudo#535).
        let shared_history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let node_a = build_handler_with_history_store(true, shared_history.clone());
        let node_b = build_handler_with_history_store(true, shared_history);
        append_history(&node_a, "app", "chat", &[1, 2, 3, 4], "stream-1").await;
        let replay_buffer = node_b.replay_buffer().unwrap().clone();
        for serial in [2, 3] {
            replay_buffer.store(
                "app",
                "chat",
                Some("stream-1"),
                serial,
                Bytes::from_static(b"hot"),
            );
        }

        let outcome = node_b
            .resume_channel(
                &SocketId::new(),
                &test_app("app"),
                &replay_buffer,
                "chat",
                &resume_from(1),
            )
            .await;

        match outcome {
            ResumeOutcome::Recovered {
                source, messages, ..
            } => {
                let count = messages.len();
                assert_eq!(source, "cold");
                assert_eq!(count, 3, "serial 4 must be replayed");
            }
            other => panic!("expected cold recovery through the durable head, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cold_resume_waits_for_reserved_serial_to_commit() {
        let shared_history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let publisher = Arc::new(build_handler_with_history_store(
            true,
            shared_history.clone(),
        ));
        let node_b = build_handler_with_history_store(true, shared_history.clone());
        append_history(&publisher, "app", "chat", &[1, 2, 3], "stream-1").await;
        // Serial 4 is reserved and broadcast, but its durable append lands later.
        let reservation = shared_history
            .reserve_publish_position("app", "chat")
            .await
            .unwrap();
        assert_eq!(reservation.serial, 4);
        let delayed = Arc::clone(&publisher);
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            append_history(&delayed, "app", "chat", &[4], "stream-1").await;
        });

        let outcome = node_b
            .resume_channel(
                &SocketId::new(),
                &test_app("app"),
                node_b.replay_buffer().unwrap(),
                "chat",
                &resume_from(1),
            )
            .await;
        writer.await.unwrap();

        match outcome {
            ResumeOutcome::Recovered {
                source, messages, ..
            } => {
                let count = messages.len();
                assert_eq!(source, "cold");
                assert_eq!(count, 3, "the in-flight serial 4 must be replayed");
            }
            other => panic!("expected recovery including the in-flight serial, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cold_resume_fails_closed_when_reserved_serial_never_commits() {
        let shared_history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let node_b = build_handler_with_history_store(true, shared_history.clone());
        append_history(&node_b, "app", "chat", &[1, 2, 3], "stream-1").await;
        shared_history
            .reserve_publish_position("app", "chat")
            .await
            .unwrap();

        let outcome = node_b
            .resume_channel(
                &SocketId::new(),
                &test_app("app"),
                node_b.replay_buffer().unwrap(),
                "chat",
                &resume_from(1),
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => {
                assert_eq!(failure.code, "persistence_unavailable");
                assert_eq!(failure.reason, "durable_history_write_in_flight");
            }
            other => panic!("expected fail-closed resume, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn shared_degraded_state_blocks_cross_node_cold_recovery() {
        let shared_history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let shared_state = Arc::new(StatefulHistoryStore::new(shared_history));
        let node_a = build_handler_with_history_store(
            true,
            shared_state.clone() as Arc<dyn HistoryStore + Send + Sync>,
        );
        let node_b = build_handler_with_history_store(
            true,
            shared_state.clone() as Arc<dyn HistoryStore + Send + Sync>,
        );
        let app = test_app("app");

        append_history(&node_a, "app", "chat", &[2, 3], "stream-1").await;
        shared_state
            .set_state(
                "app",
                "chat",
                HistoryStreamRuntimeState {
                    app_id: "app".to_string(),
                    channel: "chat".to_string(),
                    stream_id: Some("stream-1".to_string()),
                    durable_state: HistoryDurableState::Degraded,
                    recovery_allowed: false,
                    reset_required: false,
                    reason: Some("durable_history_write_failed".to_string()),
                    node_id: Some("node-a".to_string()),
                    last_transition_at_ms: Some(sockudo_core::history::now_ms()),
                    authoritative_source: "shared_test_state".to_string(),
                    observed_source: "shared_test_state".to_string(),
                },
            )
            .await;

        let outcome = node_b
            .resume_channel(
                &SocketId::new(),
                &app,
                node_b.replay_buffer().unwrap(),
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => {
                assert_eq!(failure.code, "persistence_unavailable");
                assert_eq!(failure.reason, "history_stream_degraded");
                assert_eq!(failure.current_stream_id.as_deref(), Some("stream-1"));
            }
            other => panic!("expected degraded-state failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn reset_required_state_fails_cold_recovery_as_stream_reset() {
        let shared_history = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let shared_state = Arc::new(StatefulHistoryStore::new(shared_history));
        let handler = build_handler_with_history_store(
            true,
            shared_state.clone() as Arc<dyn HistoryStore + Send + Sync>,
        );
        let app = test_app("app");

        append_history(&handler, "app", "chat", &[2, 3], "stream-2").await;
        shared_state
            .set_state(
                "app",
                "chat",
                HistoryStreamRuntimeState {
                    app_id: "app".to_string(),
                    channel: "chat".to_string(),
                    stream_id: Some("stream-2".to_string()),
                    durable_state: HistoryDurableState::ResetRequired,
                    recovery_allowed: false,
                    reset_required: true,
                    reason: Some("operator_reset_required".to_string()),
                    node_id: Some("ops".to_string()),
                    last_transition_at_ms: Some(sockudo_core::history::now_ms()),
                    authoritative_source: "shared_test_state".to_string(),
                    observed_source: "shared_test_state".to_string(),
                },
            )
            .await;

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                handler.replay_buffer().unwrap(),
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-2".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => {
                assert_eq!(failure.code, "stream_reset");
                assert_eq!(failure.reason, "durable_stream_reset_required");
            }
            other => panic!("expected reset-required failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn history_state_lookup_failure_fails_closed_as_persistence_unavailable() {
        let store = Arc::new(FaultyStateHistoryStore {
            inner: Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default())),
        });
        let handler = build_handler_with_history_store(true, store.clone());
        append_history(&handler, "app", "chat", &[2, 3], "stream-1").await;
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => {
                assert_eq!(failure.code, "persistence_unavailable");
                assert_eq!(failure.reason, "durable_history_state_unavailable");
            }
            other => panic!("expected state lookup failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn retention_boundary_crossed_mid_reconnect_fails_as_position_expired() {
        let inner = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let store = Arc::new(CursorExpiryHistoryStore {
            inner,
            read_count: Arc::new(AtomicUsize::new(0)),
            expire_reason: "Expired history cursor: cursor points before retained history",
        });
        let handler = build_handler_with_history_store_and_page_size(true, store.clone(), 1);
        append_history(&handler, "app", "chat", &[2, 3], "stream-1").await;
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => {
                assert_eq!(failure.code, "position_expired");
                assert_eq!(
                    failure.reason,
                    "retention_boundary_crossed_during_reconnect"
                );
            }
            other => panic!("expected retention-boundary failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn expired_entry_after_first_page_fails_as_position_expired() {
        // Serial 4 expired while 2, 3 and 5 are fresh: publish timestamps are
        // not in serial order, so the expired entry lands on the second page.
        let inner = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let store = Arc::new(sockudo_core::history::RetentionWindowHistoryStore::new(
            inner,
            Arc::new(MemoryAppManager::new()),
            sockudo_core::options::HistoryConfig {
                enabled: true,
                retention_window_seconds: 120,
                ..Default::default()
            },
        ));
        let handler = build_handler_with_history_store_and_page_size(true, store, 2);
        let now = sockudo_core::history::now_ms();
        append_history_at(
            &handler,
            "app",
            "chat",
            &[
                (2, now - 2_000),
                (3, now - 1_500),
                (4, now - 130_000),
                (5, now - 1_000),
            ],
            "stream-1",
        )
        .await;
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => {
                assert_eq!(failure.code, "position_expired");
                assert_eq!(
                    failure.reason,
                    "retention_boundary_crossed_during_reconnect"
                );
            }
            other => panic!("expected expiry on the second page, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn stream_change_mid_reconnect_fails_as_stream_reset() {
        let inner = Arc::new(MemoryHistoryStore::new(MemoryHistoryStoreConfig::default()));
        let store = Arc::new(CursorExpiryHistoryStore {
            inner,
            read_count: Arc::new(AtomicUsize::new(0)),
            expire_reason: "Expired history cursor: channel stream changed",
        });
        let handler = build_handler_with_history_store_and_page_size(true, store.clone(), 1);
        append_history(&handler, "app", "chat", &[2, 3], "stream-1").await;
        let replay_buffer = handler.replay_buffer().unwrap().clone();
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                &replay_buffer,
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Failed(failure) => {
                assert_eq!(failure.code, "stream_reset");
                assert_eq!(failure.reason, "durable_stream_changed_during_reconnect");
            }
            other => panic!("expected stream-change failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn replay_buffer_loss_falls_back_to_cold_history() {
        let handler = build_handler(true);
        append_history(&handler, "app", "chat", &[2, 3], "stream-1").await;
        let app = test_app("app");

        let outcome = handler
            .resume_channel(
                &SocketId::new(),
                &app,
                handler.replay_buffer().unwrap(),
                "chat",
                &ResumePosition {
                    serial: 1,
                    stream_id: Some("stream-1".to_string()),
                    last_message_id: None,
                    legacy_serial_only: false,
                },
            )
            .await;

        match outcome {
            ResumeOutcome::Recovered { source, messages } => {
                let count = messages.len();
                assert_eq!(source, "cold");
                assert_eq!(count, 2);
            }
            other => panic!("expected cold fallback after replay-buffer loss, got {other:?}"),
        }
    }
}
